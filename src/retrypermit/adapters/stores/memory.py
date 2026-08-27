from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, TypeVar

from pydantic import BaseModel

from retrypermit.domain.enums import (
    FailedStage,
    InboxStatus,
    MessageState,
    PolicyStatus,
    ReceiptKind,
    ReceiptStatus,
    ReplayStatus,
    RunStatus,
)
from retrypermit.domain.errors import (
    LeaseNotOwnedError,
    NotFoundError,
    PayloadHashMismatchError,
    PolicyNotActiveError,
    PolicyValidationError,
    RunConflictError,
)
from retrypermit.domain.hashing import payload_hash
from retrypermit.domain.inbox import (
    DeliveryAttempt,
    DeliveryRecord,
    DeliveryRegistration,
    InboxItem,
    InboxLease,
)
from retrypermit.domain.messages import (
    MessageAnalysisUpdate,
    MessageRecord,
    SeedMessage,
)
from retrypermit.domain.policy import PolicyVersion
from retrypermit.domain.receipts import Receipt, Transition, TransitionResult
from retrypermit.domain.replay import (
    DownstreamEffect,
    DownstreamSubmission,
    ReplayLease,
    ReplayLedgerEntry,
)
from retrypermit.domain.runs import DemoRun
from retrypermit.domain.state_machine import ensure_transition, is_terminal
from retrypermit.domain.triage import Repair
from retrypermit.ports.clock import Clock, SystemClock

ModelT = TypeVar("ModelT", bound=BaseModel)


def _copy(model: ModelT) -> ModelT:
    return model.model_copy(deep=True)


class MemoryStore:
    """Atomic local implementation of the same operations required from Firestore.

    The process lock is deliberately scoped around invariant-bearing methods. Local
    mode remains non-durable and must be disclosed as such by the API/UI.
    """

    def __init__(
        self,
        *,
        clock: Clock | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        self._lock = asyncio.Lock()
        self._policies: dict[tuple[str, str], PolicyVersion] = {}
        self._active_policy: dict[str, str] = {}
        self._runs: dict[str, DemoRun] = {}
        self._messages: dict[str, dict[str, MessageRecord]] = {}
        self._transitions: dict[tuple[str, str], list[Transition]] = {}
        self._receipts: dict[tuple[str, str], dict[str, Receipt]] = {}
        self._deliveries: dict[tuple[str, str], DeliveryRecord] = {}
        self._delivery_attempts: dict[tuple[str, str], list[DeliveryAttempt]] = {}
        self._inbox: dict[tuple[str, str], InboxItem] = {}
        self._ledgers: dict[tuple[str, str], ReplayLedgerEntry] = {}
        self._effects: dict[tuple[str, str], DownstreamEffect] = {}

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{self._id_factory()}"

    def _message_locked(self, run_id: str, message_id: str) -> MessageRecord:
        try:
            return self._messages[run_id][message_id]
        except KeyError as exc:
            raise NotFoundError(f"message {run_id}/{message_id} not found") from exc

    def _run_locked(self, run_id: str) -> DemoRun:
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise NotFoundError(f"run {run_id} not found") from exc

    async def install_policy(self, policy: PolicyVersion) -> PolicyVersion:
        async with self._lock:
            key = (policy.tenant_id, policy.version)
            existing = self._policies.get(key)
            if existing is not None:
                if (
                    existing.source_hash != policy.source_hash
                    or existing.extracted_policy_hash != policy.extracted_policy_hash
                ):
                    raise PolicyValidationError(
                        "a policy version cannot be replaced with different content"
                    )
                return _copy(existing)
            if policy.status == PolicyStatus.ACTIVE:
                raise PolicyValidationError(
                    "policy installation cannot bypass transactional activation"
                )
            self._policies[key] = _copy(policy)
            return _copy(policy)

    async def activate_policy(
        self, tenant_id: str, version: str, actor: str
    ) -> PolicyVersion:
        async with self._lock:
            key = (tenant_id, version)
            try:
                candidate = self._policies[key]
            except KeyError as exc:
                raise NotFoundError(f"policy {tenant_id}/{version} not found") from exc
            if candidate.status not in (PolicyStatus.APPROVED, PolicyStatus.ACTIVE):
                raise PolicyValidationError("only an approved policy can be activated")
            now = self._clock.now()
            previous_version = self._active_policy.get(tenant_id)
            if previous_version and previous_version != version:
                previous_key = (tenant_id, previous_version)
                previous = self._policies[previous_key]
                self._policies[previous_key] = previous.model_copy(
                    update={"status": PolicyStatus.APPROVED}
                )
            activated = candidate.model_copy(
                update={
                    "status": PolicyStatus.ACTIVE,
                    "activated_at": candidate.activated_at or now,
                    "activated_by": candidate.activated_by or actor,
                }
            )
            self._policies[key] = activated
            self._active_policy[tenant_id] = version
            return _copy(activated)

    async def get_active_policy(self, tenant_id: str) -> PolicyVersion:
        async with self._lock:
            version = self._active_policy.get(tenant_id)
            if version is None:
                raise PolicyNotActiveError(f"tenant {tenant_id} has no active policy")
            policy = self._policies[(tenant_id, version)]
            if policy.status != PolicyStatus.ACTIVE:
                raise PolicyNotActiveError("active policy pointer is inconsistent")
            return _copy(policy)

    async def reset_run(
        self,
        *,
        tenant_id: str,
        policy_version: str,
        seeds: Sequence[SeedMessage],
        run_id: str | None = None,
    ) -> DemoRun:
        async with self._lock:
            if self._active_policy.get(tenant_id) != policy_version:
                raise PolicyNotActiveError(
                    "a run must be seeded with the tenant's currently active policy"
                )
            active_run_ids = {
                existing.run_id
                for existing in self._runs.values()
                if existing.tenant_id == tenant_id and existing.active
            }
            for (effect_run_id, idempotency_key), _effect in self._effects.items():
                if effect_run_id not in active_run_ids:
                    continue
                ledger = self._ledgers.get((effect_run_id, idempotency_key))
                if ledger is None or ledger.status != ReplayStatus.CONFIRMED:
                    raise RunConflictError(
                        "reset is blocked until an existing downstream effect is confirmed"
                    )
            new_run_id = run_id or self._id("run")
            if new_run_id in self._runs:
                raise RunConflictError(f"run {new_run_id} already exists")
            now = self._clock.now()
            run = DemoRun(
                run_id=new_run_id,
                tenant_id=tenant_id,
                status=RunStatus.READY,
                active=True,
                active_policy_version=policy_version,
                seeded_message_count=len(seeds),
                created_at=now,
                updated_at=now,
            )
            messages: dict[str, MessageRecord] = {}
            for seed in seeds:
                if seed.source_message_id in messages:
                    raise RunConflictError(
                        f"duplicate source message id {seed.source_message_id}"
                    )
                try:
                    amount = Decimal(str(seed.payload["amount"]))
                    currency = str(seed.payload["currency"]).upper()
                except (KeyError, InvalidOperation, ValueError) as exc:
                    raise RunConflictError(
                        f"invalid seed payload {seed.source_message_id}"
                    ) from exc
                original_hash = payload_hash(seed.payload)
                messages[seed.source_message_id] = MessageRecord(
                    run_id=new_run_id,
                    tenant_id=tenant_id,
                    message_id=seed.source_message_id,
                    source_topic=seed.source_topic,
                    original_payload=seed.payload,
                    original_payload_hash=original_hash,
                    amount=amount,
                    currency=currency,
                    active_policy_version=policy_version,
                    created_at=now,
                    updated_at=now,
                )
            for existing_id, existing in list(self._runs.items()):
                if existing.tenant_id == tenant_id and existing.active:
                    self._runs[existing_id] = existing.model_copy(
                        update={
                            "active": False,
                            "status": RunStatus.INACTIVE,
                            "updated_at": now,
                            "expires_at": now + timedelta(days=30),
                        }
                    )
            self._runs[new_run_id] = run
            self._messages[new_run_id] = messages
            for message_id in messages:
                self._transitions[(new_run_id, message_id)] = []
                self._receipts[(new_run_id, message_id)] = {}
            return _copy(run)

    async def get_active_run(self, tenant_id: str) -> DemoRun:
        async with self._lock:
            matches = [
                run
                for run in self._runs.values()
                if run.tenant_id == tenant_id and run.active
            ]
            if len(matches) != 1:
                raise NotFoundError(f"tenant {tenant_id} has no single active run")
            return _copy(matches[0])

    async def get_run(self, run_id: str) -> DemoRun:
        async with self._lock:
            return _copy(self._run_locked(run_id))

    async def mark_run_running(self, run_id: str) -> DemoRun:
        async with self._lock:
            run = self._run_locked(run_id)
            if not run.active:
                raise RunConflictError("inactive runs cannot be started")
            if run.status == RunStatus.COMPLETE:
                return _copy(run)
            now = self._clock.now()
            updated = run.model_copy(
                update={
                    "status": RunStatus.RUNNING,
                    "started_at": run.started_at or now,
                    "updated_at": now,
                }
            )
            self._runs[run_id] = updated
            return _copy(updated)

    async def get_message(self, run_id: str, message_id: str) -> MessageRecord:
        async with self._lock:
            return _copy(self._message_locked(run_id, message_id))

    async def list_messages(self, run_id: str) -> list[MessageRecord]:
        async with self._lock:
            if run_id not in self._messages:
                raise NotFoundError(f"run {run_id} not found")
            return [_copy(value) for _, value in sorted(self._messages[run_id].items())]

    async def update_analysis(
        self, run_id: str, message_id: str, analysis: MessageAnalysisUpdate
    ) -> MessageRecord:
        async with self._lock:
            message = self._message_locked(run_id, message_id)
            updated = message.model_copy(
                update={
                    "failure_class": analysis.failure_class,
                    "confidence": analysis.confidence,
                    "runbook_clause": analysis.runbook_clause,
                    "runbook_page": analysis.runbook_page,
                    "decision_summary": analysis.decision_summary,
                    "evidence": list(analysis.evidence),
                    "contains_injection_attempt": analysis.contains_injection_attempt,
                    "recommended_action": analysis.recommended_action,
                    "proposed_repairs": [
                        repair.model_copy(deep=True)
                        for repair in analysis.proposed_repairs
                    ],
                    "updated_at": self._clock.now(),
                }
            )
            self._messages[run_id][message_id] = updated
            return _copy(updated)

    async def set_repaired_payload(
        self,
        run_id: str,
        message_id: str,
        payload: dict[str, Any],
        repaired_hash: str,
        applied_repairs: list[Repair],
    ) -> MessageRecord:
        if payload_hash(payload) != repaired_hash:
            raise PayloadHashMismatchError("repaired payload hash is invalid")
        async with self._lock:
            message = self._message_locked(run_id, message_id)
            updated = message.model_copy(
                update={
                    "repaired_payload": payload,
                    "repaired_payload_hash": repaired_hash,
                    "applied_repairs": applied_repairs,
                    "updated_at": self._clock.now(),
                }
            )
            self._messages[run_id][message_id] = updated
            return _copy(updated)

    def _transition_locked(
        self,
        *,
        run_id: str,
        message_id: str,
        new_state: MessageState,
        triggering_event: str,
        trace_id: str,
        decision_summary: str | None,
        runbook_clause: str | None,
        error_code: str | None,
        failed_stage: FailedStage | None,
        next_attempt_at: datetime | None,
    ) -> TransitionResult:
        message = self._message_locked(run_id, message_id)
        ensure_transition(message.current_state, new_state, message.failed_stage)
        now = self._clock.now()
        receipt_id = self._id("rcpt")
        transition = Transition(
            sequence=message.next_transition_sequence,
            run_id=run_id,
            message_id=message_id,
            previous_state=message.current_state,
            new_state=new_state,
            timestamp=now,
            triggering_event=triggering_event,
            decision_summary=decision_summary,
            runbook_clause=runbook_clause,
            receipt_id=receipt_id,
            error_code=error_code,
            trace_id=trace_id,
        )
        receipt = Receipt(
            receipt_id=receipt_id,
            operation_id=self._id("op"),
            run_id=run_id,
            message_id=message_id,
            kind=ReceiptKind.TRANSITION,
            status=ReceiptStatus.CONFIRMED,
            stage="state_transition",
            trace_id=trace_id,
            policy_version=message.active_policy_version,
            runbook_clause=runbook_clause,
            error_code=error_code,
            details={
                "previous_state": message.current_state.value,
                "new_state": new_state.value,
                "triggering_event": triggering_event,
            },
            created_at=now,
        )
        updated_failed_stage = failed_stage
        if new_state not in (MessageState.FAILED_RETRYABLE, MessageState.FAILED_FINAL):
            updated_failed_stage = None
        updated = message.model_copy(
            update={
                "current_state": new_state,
                "failed_stage": updated_failed_stage,
                "next_attempt_at": next_attempt_at,
                "last_error_code": error_code,
                "decision_summary": decision_summary or message.decision_summary,
                "runbook_clause": runbook_clause or message.runbook_clause,
                "next_transition_sequence": message.next_transition_sequence + 1,
                "updated_at": now,
            }
        )
        self._messages[run_id][message_id] = updated
        self._transitions[(run_id, message_id)].append(transition)
        self._receipts[(run_id, message_id)][receipt.receipt_id] = receipt
        return TransitionResult(transition=_copy(transition), receipt=_copy(receipt))

    async def transition_message(
        self,
        *,
        run_id: str,
        message_id: str,
        new_state: MessageState,
        triggering_event: str,
        trace_id: str,
        decision_summary: str | None = None,
        runbook_clause: str | None = None,
        error_code: str | None = None,
        failed_stage: FailedStage | None = None,
        next_attempt_at: datetime | None = None,
    ) -> TransitionResult:
        async with self._lock:
            result = self._transition_locked(
                run_id=run_id,
                message_id=message_id,
                new_state=new_state,
                triggering_event=triggering_event,
                trace_id=trace_id,
                decision_summary=decision_summary,
                runbook_clause=runbook_clause,
                error_code=error_code,
                failed_stage=failed_stage,
                next_attempt_at=next_attempt_at,
            )
            if is_terminal(new_state):
                self._maybe_complete_run_locked(run_id)
            return result

    async def list_transitions(self, run_id: str, message_id: str) -> list[Transition]:
        async with self._lock:
            self._message_locked(run_id, message_id)
            return [_copy(item) for item in self._transitions[(run_id, message_id)]]

    async def record_receipt(self, receipt: Receipt) -> Receipt:
        async with self._lock:
            self._message_locked(receipt.run_id, receipt.message_id)
            bucket = self._receipts[(receipt.run_id, receipt.message_id)]
            existing = bucket.get(receipt.receipt_id)
            if existing is not None:
                if existing != receipt:
                    raise RunConflictError("receipt id already has different content")
                return _copy(existing)
            bucket[receipt.receipt_id] = _copy(receipt)
            return _copy(receipt)

    async def list_receipts(self, run_id: str, message_id: str) -> list[Receipt]:
        async with self._lock:
            self._message_locked(run_id, message_id)
            return [
                _copy(item)
                for item in sorted(
                    self._receipts[(run_id, message_id)].values(),
                    key=lambda receipt: (receipt.created_at, receipt.receipt_id),
                )
            ]

    async def register_delivery(
        self,
        *,
        run_id: str,
        delivery_key: str,
        source_message_id: str,
        transport_message_id: str,
        source_topic: str,
        payload: dict[str, Any],
        trace_id: str,
    ) -> DeliveryRegistration:
        async with self._lock:
            message = self._message_locked(run_id, source_message_id)
            actual_hash = payload_hash(payload)
            if actual_hash != message.original_payload_hash:
                raise PayloadHashMismatchError(
                    "delivery payload differs from the seeded source message"
                )
            if source_topic != message.source_topic:
                raise PayloadHashMismatchError(
                    "delivery source topic differs from the seeded source message"
                )
            key = (run_id, delivery_key)
            now = self._clock.now()
            existing = self._deliveries.get(key)
            duplicate = existing is not None
            if existing is not None:
                if (
                    existing.run_id != run_id
                    or existing.delivery_key != delivery_key
                    or existing.source_message_id != source_message_id
                    or existing.transport_message_id != transport_message_id
                    or existing.source_topic != source_topic
                    or existing.payload_hash != actual_hash
                ):
                    raise PayloadHashMismatchError(
                        "duplicate delivery key has different source identity or payload"
                    )
                delivery = existing.model_copy(
                    update={
                        "seen_count": existing.seen_count + 1,
                        "last_seen_at": now,
                        "last_trace_id": trace_id,
                    }
                )
            else:
                delivery = DeliveryRecord(
                    run_id=run_id,
                    delivery_key=delivery_key,
                    source_message_id=source_message_id,
                    transport_message_id=transport_message_id,
                    source_topic=source_topic,
                    payload_hash=actual_hash,
                    first_seen_at=now,
                    last_seen_at=now,
                    last_trace_id=trace_id,
                )
            attempt = DeliveryAttempt(
                attempt_id=self._id("delivery"),
                run_id=run_id,
                delivery_key=delivery_key,
                trace_id=trace_id,
                received_at=now,
                duplicate=duplicate,
            )
            inbox = self._inbox.get(key)
            if inbox is None:
                inbox = InboxItem(
                    run_id=run_id,
                    inbox_id=delivery_key,
                    delivery_key=delivery_key,
                    message_id=source_message_id,
                    payload_hash=actual_hash,
                    status=InboxStatus.PENDING,
                    created_at=now,
                    updated_at=now,
                )
            elif (
                inbox.run_id != run_id
                or inbox.delivery_key != delivery_key
                or inbox.message_id != source_message_id
                or inbox.payload_hash != actual_hash
            ):
                raise PayloadHashMismatchError(
                    "inbox item has different source identity or payload"
                )
            if message.transport_message_id is None:
                self._messages[run_id][source_message_id] = message.model_copy(
                    update={
                        "transport_message_id": transport_message_id,
                        "updated_at": now,
                    }
                )
            self._deliveries[key] = delivery
            self._delivery_attempts.setdefault(key, []).append(attempt)
            self._inbox[key] = inbox
            return DeliveryRegistration(
                delivery=_copy(delivery),
                attempt=_copy(attempt),
                inbox=_copy(inbox),
                duplicate=duplicate,
            )

    async def claim_inbox(
        self, run_id: str, inbox_id: str, owner: str, lease_duration: timedelta
    ) -> InboxLease:
        if lease_duration <= timedelta(0):
            raise RunConflictError("inbox lease duration must be positive")
        async with self._lock:
            key = (run_id, inbox_id)
            try:
                item = self._inbox[key]
            except KeyError as exc:
                raise NotFoundError(f"inbox {run_id}/{inbox_id} not found") from exc
            now = self._clock.now()
            if item.status in (InboxStatus.COMPLETED, InboxStatus.FAILED_FINAL):
                return InboxLease(item=_copy(item), acquired=False)
            lease_active = (
                item.lease_expires_at is not None and item.lease_expires_at > now
            )
            if lease_active:
                return InboxLease(item=_copy(item), acquired=False)
            takeover = (
                item.lease_owner is not None
                and item.lease_expires_at is not None
                and item.lease_expires_at <= now
            )
            updated = item.model_copy(
                update={
                    "status": InboxStatus.PROCESSING,
                    "lease_owner": owner,
                    "lease_expires_at": now + lease_duration,
                    "attempt_count": item.attempt_count + 1,
                    "updated_at": now,
                }
            )
            self._inbox[key] = updated
            return InboxLease(item=_copy(updated), acquired=True, takeover=takeover)

    async def update_inbox_status(
        self,
        run_id: str,
        inbox_id: str,
        status: InboxStatus,
        *,
        task_name: str | None = None,
        scheduled_at: datetime | None = None,
        next_attempt_at: datetime | None = None,
        error_code: str | None = None,
        owner: str | None = None,
        expected_attempt_count: int | None = None,
    ) -> InboxItem:
        async with self._lock:
            key = (run_id, inbox_id)
            try:
                item = self._inbox[key]
            except KeyError as exc:
                raise NotFoundError(f"inbox {run_id}/{inbox_id} not found") from exc
            now = self._clock.now()
            if item.status in (InboxStatus.COMPLETED, InboxStatus.FAILED_FINAL):
                return _copy(item)
            if (
                expected_attempt_count is not None
                and item.attempt_count != expected_attempt_count
            ):
                return _copy(item)
            lease_active = (
                item.status == InboxStatus.PROCESSING
                and item.lease_expires_at is not None
                and item.lease_expires_at > now
            )
            if status == InboxStatus.ENQUEUED:
                if lease_active:
                    return _copy(item)
            elif status in (InboxStatus.RETRYABLE, InboxStatus.COMPLETED):
                if not lease_active or item.lease_owner != owner:
                    raise LeaseNotOwnedError(
                        f"{status.value.lower()} inbox update requires the owning worker"
                    )
            elif status == InboxStatus.FAILED_FINAL:
                if item.status == InboxStatus.PROCESSING:
                    if owner is not None:
                        if not lease_active or item.lease_owner != owner:
                            raise LeaseNotOwnedError(
                                "final inbox update requires a live owning worker"
                            )
                    elif self._run_locked(run_id).active or lease_active:
                        raise LeaseNotOwnedError(
                            "ownerless finalization requires an inactive run and expired lease"
                        )
                elif owner is None and self._run_locked(run_id).active:
                    raise RunConflictError(
                        "an ownerless final inbox update requires an inactive run"
                    )
                elif owner is not None:
                    raise LeaseNotOwnedError(
                        "final inbox update requires a PROCESSING item"
                    )
            clear_lease = status in {
                InboxStatus.ENQUEUED,
                InboxStatus.RETRYABLE,
                InboxStatus.COMPLETED,
                InboxStatus.FAILED_FINAL,
            }
            updated = item.model_copy(
                update={
                    "status": status,
                    "task_name": task_name if task_name is not None else item.task_name,
                    "scheduled_at": (
                        scheduled_at if scheduled_at is not None else item.scheduled_at
                    ),
                    "next_attempt_at": next_attempt_at,
                    "last_error_code": error_code,
                    "lease_owner": None if clear_lease else item.lease_owner,
                    "lease_expires_at": None if clear_lease else item.lease_expires_at,
                    "updated_at": now,
                }
            )
            self._inbox[key] = updated
            return _copy(updated)

    async def acquire_replay_lease(
        self,
        *,
        run_id: str,
        message_id: str,
        idempotency_key: str,
        repaired_payload_hash: str,
        action_version: str,
        owner: str,
        lease_duration: timedelta,
    ) -> ReplayLease:
        if lease_duration <= timedelta(0):
            raise RunConflictError("replay lease duration must be positive")
        async with self._lock:
            message = self._message_locked(run_id, message_id)
            if (
                message.repaired_payload is None
                or message.repaired_payload_hash != repaired_payload_hash
            ):
                raise PayloadHashMismatchError(
                    "replay payload differs from the persisted repaired message"
                )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError("replay lease requires a REPLAYING message")
            key = (run_id, idempotency_key)
            now = self._clock.now()
            entry = self._ledgers.get(key)
            if entry is None:
                entry = ReplayLedgerEntry(
                    run_id=run_id,
                    message_id=message_id,
                    idempotency_key=idempotency_key,
                    payload_hash=repaired_payload_hash,
                    status=ReplayStatus.PENDING,
                    action_version=action_version,
                    created_at=now,
                    updated_at=now,
                )
            elif entry.payload_hash != repaired_payload_hash:
                raise PayloadHashMismatchError(
                    "idempotency key already exists with another payload hash"
                )
            if entry.message_id != message_id:
                raise PayloadHashMismatchError(
                    "idempotency key already belongs to another message"
                )
            if entry.action_version != action_version:
                raise PayloadHashMismatchError(
                    "idempotency key already belongs to another action version"
                )
            if entry.status == ReplayStatus.CONFIRMED:
                return ReplayLease(
                    entry=_copy(entry), acquired=False, already_confirmed=True
                )
            lease_active = (
                entry.lease_expires_at is not None and entry.lease_expires_at > now
            )
            if entry.status == ReplayStatus.FAILED_FINAL:
                return ReplayLease(entry=_copy(entry), acquired=False)
            if lease_active:
                return ReplayLease(entry=_copy(entry), acquired=False)
            takeover = (
                entry.lease_owner is not None
                and entry.lease_expires_at is not None
                and entry.lease_expires_at <= now
            )
            entry = entry.model_copy(
                update={
                    "lease_owner": owner,
                    "lease_expires_at": now + lease_duration,
                    "updated_at": now,
                }
            )
            self._ledgers[key] = entry
            self._messages[run_id][message_id] = message.model_copy(
                update={"idempotency_key": idempotency_key, "updated_at": now}
            )
            return ReplayLease(entry=_copy(entry), acquired=True, takeover=takeover)

    async def start_replay_attempt(
        self, run_id: str, idempotency_key: str, owner: str
    ) -> ReplayLedgerEntry:
        async with self._lock:
            key = (run_id, idempotency_key)
            try:
                entry = self._ledgers[key]
            except KeyError as exc:
                raise NotFoundError(
                    f"replay ledger {idempotency_key} not found"
                ) from exc
            now = self._clock.now()
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or entry.lease_expires_at <= now
            ):
                raise LeaseNotOwnedError("replay attempt requires a live owned lease")
            if entry.status == ReplayStatus.CONFIRMED:
                return _copy(entry)
            updated = entry.model_copy(
                update={
                    "status": ReplayStatus.ATTEMPTED,
                    "attempt_count": entry.attempt_count + 1,
                    "updated_at": now,
                }
            )
            self._ledgers[key] = updated
            message = self._message_locked(run_id, entry.message_id)
            self._messages[run_id][entry.message_id] = message.model_copy(
                update={"attempts": updated.attempt_count, "updated_at": now}
            )
            return _copy(updated)

    async def create_or_get_downstream_effect(
        self,
        *,
        run_id: str,
        message_id: str,
        idempotency_key: str,
        repaired_payload_hash: str,
        owner: str,
    ) -> DownstreamSubmission:
        async with self._lock:
            run = self._run_locked(run_id)
            message = self._message_locked(run_id, message_id)
            key = (run_id, idempotency_key)
            now = self._clock.now()
            try:
                ledger = self._ledgers[key]
            except KeyError as exc:
                raise NotFoundError(
                    f"replay ledger {idempotency_key} not found"
                ) from exc
            active_policy = self._active_policy.get(message.tenant_id)
            if not run.active or run.status != RunStatus.RUNNING:
                raise RunConflictError(
                    "downstream effects require an active running run"
                )
            if (
                active_policy != message.active_policy_version
                or run.active_policy_version != message.active_policy_version
            ):
                raise RunConflictError("the run policy is no longer active")
            if (
                message.current_state != MessageState.REPLAYING
                or message.repaired_payload is None
                or message.repaired_payload_hash != repaired_payload_hash
                or message.idempotency_key != idempotency_key
            ):
                raise PayloadHashMismatchError(
                    "message does not match the authorized downstream submission"
                )
            if (
                ledger.message_id != message_id
                or ledger.payload_hash != repaired_payload_hash
                or ledger.status != ReplayStatus.ATTEMPTED
                or ledger.lease_owner != owner
                or ledger.lease_expires_at is None
                or ledger.lease_expires_at <= now
            ):
                raise LeaseNotOwnedError(
                    "downstream submission requires a live attempted replay lease"
                )
            existing = self._effects.get(key)
            if existing is not None:
                if existing.payload_hash != repaired_payload_hash:
                    raise PayloadHashMismatchError(
                        "downstream key already has a different payload hash"
                    )
                updated = existing.model_copy(
                    update={
                        "submission_count": existing.submission_count + 1,
                        "updated_at": now,
                    }
                )
                self._effects[key] = updated
                return DownstreamSubmission(effect=_copy(updated), created=False)
            reference = (
                "rp_order_"
                + hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()[:16]
            )
            effect = DownstreamEffect(
                run_id=run_id,
                message_id=message_id,
                idempotency_key=idempotency_key,
                payload_hash=repaired_payload_hash,
                downstream_reference=reference,
                created_at=now,
                updated_at=now,
            )
            self._effects[key] = effect
            return DownstreamSubmission(effect=_copy(effect), created=True)

    async def confirm_replay(
        self,
        *,
        run_id: str,
        message_id: str,
        idempotency_key: str,
        owner: str,
        downstream_reference: str,
        trace_id: str,
        result_receipt: Receipt,
    ) -> ReplayLedgerEntry:
        async with self._lock:
            key = (run_id, idempotency_key)
            try:
                entry = self._ledgers[key]
                effect = self._effects[key]
            except KeyError as exc:
                raise NotFoundError(
                    "ledger and downstream effect are both required"
                ) from exc
            message = self._message_locked(run_id, message_id)
            if (
                entry.run_id != run_id
                or entry.message_id != message_id
                or entry.idempotency_key != idempotency_key
                or effect.run_id != run_id
                or effect.message_id != message_id
                or effect.idempotency_key != idempotency_key
                or message.run_id != run_id
                or message.message_id != message_id
            ):
                raise PayloadHashMismatchError(
                    "ledger or effect belongs to another message"
                )
            if effect.downstream_reference != downstream_reference:
                raise PayloadHashMismatchError(
                    "downstream reference does not match effect"
                )
            if effect.payload_hash != entry.payload_hash:
                raise PayloadHashMismatchError(
                    "ledger and downstream payload hashes differ"
                )
            if (
                message.idempotency_key != idempotency_key
                or message.repaired_payload_hash != entry.payload_hash
                or message.downstream_reference not in (None, downstream_reference)
            ):
                raise PayloadHashMismatchError(
                    "message does not match the confirmed replay effect"
                )
            if (
                result_receipt.run_id != run_id
                or result_receipt.message_id != message_id
                or result_receipt.kind != ReceiptKind.REPLAY_RESULT
                or result_receipt.status != ReceiptStatus.CONFIRMED
                or result_receipt.stage != "replay"
                or result_receipt.trace_id != trace_id
                or result_receipt.idempotency_key != idempotency_key
                or result_receipt.downstream_reference != downstream_reference
                or result_receipt.payload_hash != entry.payload_hash
                or result_receipt.attempt != entry.attempt_count
                or result_receipt.policy_version != message.active_policy_version
                or (
                    message.runbook_clause is not None
                    and result_receipt.runbook_clause != message.runbook_clause
                )
                or result_receipt.error_code is not None
                or result_receipt.retryable
                or not result_receipt.operation_id
            ):
                raise RunConflictError("replay confirmation receipt is inconsistent")
            receipt_bucket = self._receipts[(run_id, message_id)]
            existing_receipt = receipt_bucket.get(result_receipt.receipt_id)
            if existing_receipt is not None and existing_receipt != result_receipt:
                raise RunConflictError("replay confirmation receipt id conflicts")
            if entry.status == ReplayStatus.CONFIRMED:
                if existing_receipt is None:
                    raise RunConflictError(
                        "confirmed replay ledger is missing its result receipt"
                    )
                return _copy(entry)
            if entry.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "confirmation requires an ATTEMPTED replay ledger"
                )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError("confirmation requires a REPLAYING message")
            if existing_receipt is not None:
                raise RunConflictError(
                    "replay result receipt must be committed with confirmation"
                )
            now = self._clock.now()
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or entry.lease_expires_at <= now
            ):
                raise LeaseNotOwnedError("confirmation requires a live owned lease")
            confirmed = entry.model_copy(
                update={
                    "status": ReplayStatus.CONFIRMED,
                    "downstream_reference": downstream_reference,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": None,
                    "next_attempt_at": None,
                    "updated_at": now,
                }
            )
            self._ledgers[key] = confirmed
            self._messages[run_id][message_id] = message.model_copy(
                update={
                    "downstream_reference": downstream_reference,
                    "updated_at": now,
                }
            )
            self._transition_locked(
                run_id=run_id,
                message_id=message_id,
                new_state=MessageState.REPLAYED,
                triggering_event="downstream_confirmed",
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                error_code=None,
                failed_stage=None,
                next_attempt_at=None,
            )
            receipt_bucket[result_receipt.receipt_id] = _copy(result_receipt)
            self._maybe_complete_run_locked(run_id)
            return _copy(confirmed)

    async def mark_replay_retryable(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        owner: str,
        error_code: str,
        next_attempt_at: datetime,
        trace_id: str,
        result_receipt: Receipt,
    ) -> ReplayLedgerEntry:
        async with self._lock:
            key = (run_id, idempotency_key)
            try:
                entry = self._ledgers[key]
            except KeyError as exc:
                raise NotFoundError(
                    f"replay ledger {idempotency_key} not found"
                ) from exc
            message = self._message_locked(run_id, entry.message_id)
            self._validate_failed_replay_receipt_locked(
                entry=entry,
                message=message,
                receipt=result_receipt,
                error_code=error_code,
                retryable=True,
                trace_id=trace_id,
            )
            existing_receipt = self._receipts[(run_id, entry.message_id)].get(
                result_receipt.receipt_id
            )
            if entry.status == ReplayStatus.RETRYABLE:
                if (
                    existing_receipt == result_receipt
                    and message.current_state == MessageState.FAILED_RETRYABLE
                ):
                    return _copy(entry)
                raise RunConflictError("retryable replay result is inconsistent")
            if entry.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "retryable replay update requires an ATTEMPTED ledger"
                )
            if existing_receipt is not None:
                raise RunConflictError(
                    "replay result receipt must be committed with the retryable update"
                )
            now = self._clock.now()
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or entry.lease_expires_at <= now
            ):
                raise LeaseNotOwnedError("retryable update requires a live owned lease")
            updated = entry.model_copy(
                update={
                    "status": ReplayStatus.RETRYABLE,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": error_code,
                    "next_attempt_at": next_attempt_at,
                    "updated_at": now,
                }
            )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError("retryable replay requires a REPLAYING message")
            self._transition_locked(
                run_id=run_id,
                message_id=entry.message_id,
                new_state=MessageState.FAILED_RETRYABLE,
                triggering_event="replay_failed_retryable",
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                error_code=error_code,
                failed_stage=FailedStage.REPLAY,
                next_attempt_at=next_attempt_at,
            )
            self._ledgers[key] = updated
            self._receipts[(run_id, entry.message_id)][result_receipt.receipt_id] = (
                _copy(result_receipt)
            )
            return _copy(updated)

    async def mark_replay_failed_final(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        owner: str,
        error_code: str,
        trace_id: str,
        result_receipt: Receipt,
        new_state: MessageState = MessageState.FAILED_FINAL,
        triggering_event: str = "retry_budget_exhausted",
    ) -> ReplayLedgerEntry:
        async with self._lock:
            key = (run_id, idempotency_key)
            try:
                entry = self._ledgers[key]
            except KeyError as exc:
                raise NotFoundError(
                    f"replay ledger {idempotency_key} not found"
                ) from exc
            if new_state not in (MessageState.FAILED_FINAL, MessageState.ESCALATED):
                raise RunConflictError("final replay state must be terminal")
            message = self._message_locked(run_id, entry.message_id)
            self._validate_failed_replay_receipt_locked(
                entry=entry,
                message=message,
                receipt=result_receipt,
                error_code=error_code,
                retryable=False,
                trace_id=trace_id,
            )
            existing_receipt = self._receipts[(run_id, entry.message_id)].get(
                result_receipt.receipt_id
            )
            if entry.status == ReplayStatus.FAILED_FINAL:
                if (
                    existing_receipt == result_receipt
                    and message.current_state == new_state
                ):
                    return _copy(entry)
                raise RunConflictError("final replay result is inconsistent")
            if entry.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "final replay update requires an ATTEMPTED ledger"
                )
            if existing_receipt is not None:
                raise RunConflictError(
                    "replay result receipt must be committed with the final update"
                )
            now = self._clock.now()
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or entry.lease_expires_at <= now
            ):
                raise LeaseNotOwnedError(
                    "final failure update requires a live owned lease"
                )
            updated = entry.model_copy(
                update={
                    "status": ReplayStatus.FAILED_FINAL,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": error_code,
                    "next_attempt_at": None,
                    "updated_at": now,
                }
            )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError("final replay failure requires REPLAYING")
            self._transition_locked(
                run_id=run_id,
                message_id=entry.message_id,
                new_state=new_state,
                triggering_event=triggering_event,
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                error_code=error_code,
                failed_stage=(
                    FailedStage.REPLAY
                    if new_state == MessageState.FAILED_FINAL
                    else None
                ),
                next_attempt_at=None,
            )
            self._ledgers[key] = updated
            self._receipts[(run_id, entry.message_id)][result_receipt.receipt_id] = (
                _copy(result_receipt)
            )
            self._maybe_complete_run_locked(run_id)
            return _copy(updated)

    def _validate_failed_replay_receipt_locked(
        self,
        *,
        entry: ReplayLedgerEntry,
        message: MessageRecord,
        receipt: Receipt,
        error_code: str,
        retryable: bool,
        trace_id: str,
    ) -> None:
        if (
            receipt.run_id != entry.run_id
            or receipt.message_id != entry.message_id
            or receipt.kind != ReceiptKind.REPLAY_RESULT
            or receipt.status != ReceiptStatus.FAILED
            or receipt.stage != "replay"
            or receipt.attempt != entry.attempt_count
            or receipt.trace_id != trace_id
            or receipt.policy_version != message.active_policy_version
            or receipt.runbook_clause != message.runbook_clause
            or receipt.payload_hash != entry.payload_hash
            or receipt.idempotency_key != entry.idempotency_key
            or receipt.downstream_reference is not None
            or receipt.error_code != error_code
            or receipt.retryable is not retryable
            or not receipt.operation_id
        ):
            raise RunConflictError("failed replay result receipt is inconsistent")
        existing = self._receipts[(entry.run_id, entry.message_id)].get(
            receipt.receipt_id
        )
        if existing is not None and existing != receipt:
            raise RunConflictError("failed replay result receipt id conflicts")

    def _maybe_complete_run_locked(self, run_id: str) -> None:
        run = self._run_locked(run_id)
        if run.status != RunStatus.RUNNING:
            return
        if self._messages[run_id] and all(
            is_terminal(message.current_state)
            for message in self._messages[run_id].values()
        ):
            now = self._clock.now()
            self._runs[run_id] = run.model_copy(
                update={
                    "status": RunStatus.COMPLETE,
                    "completed_at": now,
                    "updated_at": now,
                }
            )

    async def get_replay_ledger(
        self, run_id: str, idempotency_key: str
    ) -> ReplayLedgerEntry:
        async with self._lock:
            try:
                return _copy(self._ledgers[(run_id, idempotency_key)])
            except KeyError as exc:
                raise NotFoundError(
                    f"replay ledger {idempotency_key} not found"
                ) from exc

    async def get_downstream_effect(
        self, run_id: str, idempotency_key: str
    ) -> DownstreamEffect:
        async with self._lock:
            try:
                return _copy(self._effects[(run_id, idempotency_key)])
            except KeyError as exc:
                raise NotFoundError(
                    f"downstream effect {idempotency_key} not found"
                ) from exc

    async def list_downstream_effects(self, run_id: str) -> list[DownstreamEffect]:
        async with self._lock:
            return [
                _copy(effect)
                for (effect_run_id, _), effect in sorted(self._effects.items())
                if effect_run_id == run_id
            ]

    async def recovery_candidates(
        self, now: datetime
    ) -> tuple[list[InboxItem], list[ReplayLedgerEntry], list[MessageRecord]]:
        async with self._lock:
            inbox = [
                _copy(item)
                for item in self._inbox.values()
                if self._runs[item.run_id].active
                and (
                    item.status == InboxStatus.PENDING
                    or (
                        item.status == InboxStatus.ENQUEUED
                        and (
                            item.scheduled_at is None
                            or item.scheduled_at <= now - timedelta(minutes=5)
                        )
                    )
                    or (
                        item.status == InboxStatus.PROCESSING
                        and item.lease_expires_at is not None
                        and item.lease_expires_at <= now
                    )
                    or (
                        item.status == InboxStatus.RETRYABLE
                        and item.next_attempt_at is not None
                        and item.next_attempt_at <= now
                    )
                )
            ]
            ledgers = [
                _copy(entry)
                for entry in self._ledgers.values()
                if self._runs[entry.run_id].active
                and entry.status
                not in (ReplayStatus.CONFIRMED, ReplayStatus.FAILED_FINAL)
                and (
                    (
                        entry.lease_expires_at is not None
                        and entry.lease_expires_at <= now
                    )
                    or (
                        entry.status == ReplayStatus.RETRYABLE
                        and entry.next_attempt_at is not None
                        and entry.next_attempt_at <= now
                    )
                )
            ]
            messages = [
                _copy(message)
                for run_messages in self._messages.values()
                for message in run_messages.values()
                if self._runs[message.run_id].active
                and message.current_state == MessageState.FAILED_RETRYABLE
                and message.next_attempt_at is not None
                and message.next_attempt_at <= now
            ]
            return inbox, ledgers, messages
