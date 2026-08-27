from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, TypeVar

from google.cloud.firestore_v1 import AsyncClient, AsyncTransaction, async_transactional
from google.cloud.firestore_v1.base_query import FieldFilter
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


def _firestore_value(value: Any) -> Any:
    """Recursively serialize domain values without stringifying timestamps."""

    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return _firestore_value(value.value)
    if isinstance(value, BaseModel):
        return _firestore_value(value.model_dump(mode="python"))
    if isinstance(value, Mapping):
        return {str(key): _firestore_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_firestore_value(item) for item in value]
    return value


def _dump(model: BaseModel) -> dict[str, Any]:
    """Return recursively serialized values accepted natively by Firestore."""

    dumped = _firestore_value(model)
    if not isinstance(dumped, dict):
        raise TypeError("a Firestore document must serialize to a mapping")
    return dumped


def _load(model_type: type[ModelT], data: dict[str, Any] | None) -> ModelT:
    if data is None:
        raise NotFoundError(f"{model_type.__name__} document has no data")
    return model_type.model_validate(data)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _document_id(value: str, label: str) -> str:
    """Fail closed instead of allowing user input to alter a Firestore path."""

    if (
        not value
        or "/" in value
        or value in {".", ".."}
        or len(value.encode("utf-8")) > 1_500
    ):
        raise RunConflictError(f"{label} is not a safe Firestore document id")
    return value


class FirestoreStore:
    """Firestore implementation of the RetryPermit Store protocol.

    Tenant policy versions and the active pointers live below ``tenants``. Every
    operational record is rooted below its immutable ``demo_runs/{run_id}``
    document so reset rotates runs instead of deleting audit evidence.
    """

    def __init__(
        self,
        client: AsyncClient | None = None,
        *,
        project: str | None = None,
        database: str | None = None,
        clock: Clock | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._client = client or AsyncClient(project=project, database=database)
        self._clock = clock or SystemClock()
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)

    @property
    def client(self) -> AsyncClient:
        return self._client

    def _now(self) -> datetime:
        return _utc(self._clock.now())

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{self._id_factory()}"

    def _tenant_ref(self, tenant_id: str):
        return self._client.collection("tenants").document(
            _document_id(tenant_id, "tenant_id")
        )

    def _policy_ref(self, tenant_id: str, version: str):
        return (
            self._tenant_ref(tenant_id)
            .collection("policy_versions")
            .document(_document_id(version, "policy version"))
        )

    def _run_ref(self, run_id: str):
        return self._client.collection("demo_runs").document(
            _document_id(run_id, "run_id")
        )

    def _message_ref(self, run_id: str, message_id: str):
        return (
            self._run_ref(run_id)
            .collection("messages")
            .document(_document_id(message_id, "message_id"))
        )

    def _delivery_ref(self, run_id: str, delivery_key: str):
        return (
            self._run_ref(run_id)
            .collection("deliveries")
            .document(_document_id(delivery_key, "delivery_key"))
        )

    def _inbox_ref(self, run_id: str, inbox_id: str):
        return (
            self._run_ref(run_id)
            .collection("inbox")
            .document(_document_id(inbox_id, "inbox_id"))
        )

    def _ledger_ref(self, run_id: str, idempotency_key: str):
        return (
            self._run_ref(run_id)
            .collection("replay_ledger")
            .document(_document_id(idempotency_key, "idempotency_key"))
        )

    def _effect_ref(self, run_id: str, idempotency_key: str):
        return (
            self._run_ref(run_id)
            .collection("downstream_effects")
            .document(_document_id(idempotency_key, "idempotency_key"))
        )

    async def install_policy(self, policy: PolicyVersion) -> PolicyVersion:
        if policy.status == PolicyStatus.ACTIVE:
            raise PolicyValidationError(
                "policy installation cannot bypass transactional activation"
            )
        policy_ref = self._policy_ref(policy.tenant_id, policy.version)
        tenant_ref = self._tenant_ref(policy.tenant_id)

        @async_transactional
        async def install(transaction: AsyncTransaction) -> PolicyVersion:
            now = self._now()
            snapshot = await policy_ref.get(transaction=transaction)
            if snapshot.exists:
                existing = _load(PolicyVersion, snapshot.to_dict())
                if (
                    existing.source_hash != policy.source_hash
                    or existing.extracted_policy_hash != policy.extracted_policy_hash
                ):
                    raise PolicyValidationError(
                        "a policy version cannot be replaced with different content"
                    )
                return existing
            transaction.set(policy_ref, _dump(policy))
            transaction.set(
                tenant_ref,
                {
                    "tenant_id": policy.tenant_id,
                    "updated_at": now,
                },
                merge=True,
            )
            return policy

        return await install(self._client.transaction())

    async def activate_policy(
        self, tenant_id: str, version: str, actor: str
    ) -> PolicyVersion:
        tenant_ref = self._tenant_ref(tenant_id)
        candidate_ref = self._policy_ref(tenant_id, version)

        @async_transactional
        async def activate(transaction: AsyncTransaction) -> PolicyVersion:
            now = self._now()
            tenant_snapshot = await tenant_ref.get(transaction=transaction)
            candidate_snapshot = await candidate_ref.get(transaction=transaction)
            if not candidate_snapshot.exists:
                raise NotFoundError(f"policy {tenant_id}/{version} not found")
            candidate = _load(PolicyVersion, candidate_snapshot.to_dict())
            if candidate.status not in (PolicyStatus.APPROVED, PolicyStatus.ACTIVE):
                raise PolicyValidationError("only an approved policy can be activated")

            tenant_data = tenant_snapshot.to_dict() or {}
            previous_version = tenant_data.get("active_policy_version")
            previous_ref = None
            previous = None
            if previous_version and previous_version != version:
                previous_ref = self._policy_ref(tenant_id, str(previous_version))
                previous_snapshot = await previous_ref.get(transaction=transaction)
                if previous_snapshot.exists:
                    previous = _load(PolicyVersion, previous_snapshot.to_dict())

            activated = candidate.model_copy(
                update={
                    "status": PolicyStatus.ACTIVE,
                    "activated_at": candidate.activated_at or now,
                    "activated_by": candidate.activated_by or actor,
                }
            )
            if previous_ref is not None and previous is not None:
                retired = previous.model_copy(update={"status": PolicyStatus.APPROVED})
                transaction.set(previous_ref, _dump(retired))
            transaction.set(candidate_ref, _dump(activated))
            transaction.set(
                tenant_ref,
                {
                    "tenant_id": tenant_id,
                    "active_policy_version": version,
                    "updated_at": now,
                },
                merge=True,
            )
            return activated

        return await activate(self._client.transaction())

    async def get_active_policy(self, tenant_id: str) -> PolicyVersion:
        tenant_snapshot = await self._tenant_ref(tenant_id).get()
        tenant_data = tenant_snapshot.to_dict() or {}
        version = tenant_data.get("active_policy_version")
        if not version:
            raise PolicyNotActiveError(f"tenant {tenant_id} has no active policy")
        snapshot = await self._policy_ref(tenant_id, str(version)).get()
        if not snapshot.exists:
            raise PolicyNotActiveError("active policy pointer is inconsistent")
        policy = _load(PolicyVersion, snapshot.to_dict())
        if policy.status != PolicyStatus.ACTIVE:
            raise PolicyNotActiveError("active policy pointer is inconsistent")
        return policy

    def _build_seed_messages(
        self,
        *,
        tenant_id: str,
        policy_version: str,
        seeds: Sequence[SeedMessage],
        run_id: str,
        now: datetime,
    ) -> list[MessageRecord]:
        messages: list[MessageRecord] = []
        seen: set[str] = set()
        for seed in seeds:
            if seed.source_message_id in seen:
                raise RunConflictError(
                    f"duplicate source message id {seed.source_message_id}"
                )
            _document_id(seed.source_message_id, "source message id")
            seen.add(seed.source_message_id)
            try:
                amount = Decimal(str(seed.payload["amount"]))
                currency = str(seed.payload["currency"]).upper()
            except (KeyError, InvalidOperation, ValueError) as exc:
                raise RunConflictError(
                    f"invalid seed payload {seed.source_message_id}"
                ) from exc
            messages.append(
                MessageRecord(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    message_id=seed.source_message_id,
                    source_topic=seed.source_topic,
                    original_payload=seed.payload,
                    original_payload_hash=payload_hash(seed.payload),
                    amount=amount,
                    currency=currency,
                    active_policy_version=policy_version,
                    created_at=now,
                    updated_at=now,
                )
            )
        return messages

    async def reset_run(
        self,
        *,
        tenant_id: str,
        policy_version: str,
        seeds: Sequence[SeedMessage],
        run_id: str | None = None,
    ) -> DemoRun:
        new_run_id = run_id or self._id("run")
        _document_id(new_run_id, "run_id")
        tenant_ref = self._tenant_ref(tenant_id)
        policy_ref = self._policy_ref(tenant_id, policy_version)
        run_ref = self._run_ref(new_run_id)

        @async_transactional
        async def rotate(transaction: AsyncTransaction) -> DemoRun:
            now = self._now()
            tenant_snapshot = await tenant_ref.get(transaction=transaction)
            policy_snapshot = await policy_ref.get(transaction=transaction)
            new_run_snapshot = await run_ref.get(transaction=transaction)
            tenant_runs_query = self._client.collection("demo_runs").where(
                filter=FieldFilter("tenant_id", "==", tenant_id)
            )
            tenant_runs_stream = await transaction.get(tenant_runs_query)
            tenant_run_snapshots = [snapshot async for snapshot in tenant_runs_stream]
            active_runs: list[tuple[Any, DemoRun]] = []
            effect_ledgers: list[tuple[DemoRun, DownstreamEffect, Any]] = []
            for previous_snapshot in tenant_run_snapshots:
                previous = _load(DemoRun, previous_snapshot.to_dict())
                if not previous.active:
                    continue
                if previous_snapshot.id != previous.run_id:
                    raise RunConflictError(
                        "reset is blocked by an inconsistent active run identity"
                    )
                active_runs.append((previous_snapshot, previous))
                effects_query = previous_snapshot.reference.collection(
                    "downstream_effects"
                ).order_by("__name__")
                effects_stream = await transaction.get(effects_query)
                effect_snapshots = [snapshot async for snapshot in effects_stream]
                for effect_snapshot in effect_snapshots:
                    effect = _load(DownstreamEffect, effect_snapshot.to_dict())
                    if (
                        effect_snapshot.id != effect.idempotency_key
                        or effect.run_id != previous_snapshot.id
                    ):
                        raise RunConflictError(
                            "reset is blocked by an inconsistent downstream effect identity"
                        )
                    ledger_ref = previous_snapshot.reference.collection(
                        "replay_ledger"
                    ).document(effect_snapshot.id)
                    ledger_snapshot = await ledger_ref.get(transaction=transaction)
                    effect_ledgers.append((previous, effect, ledger_snapshot))

            tenant_data = tenant_snapshot.to_dict() or {}
            if tenant_data.get("active_policy_version") != policy_version:
                raise PolicyNotActiveError(
                    "a run must be seeded with the tenant's currently active policy"
                )
            if not policy_snapshot.exists:
                raise PolicyNotActiveError("active policy pointer is inconsistent")
            policy = _load(PolicyVersion, policy_snapshot.to_dict())
            if policy.status != PolicyStatus.ACTIVE:
                raise PolicyNotActiveError("active policy pointer is inconsistent")
            if new_run_snapshot.exists:
                raise RunConflictError(f"run {new_run_id} already exists")

            for previous, effect, ledger_snapshot in effect_ledgers:
                if not ledger_snapshot.exists:
                    raise RunConflictError(
                        "reset is blocked until an existing downstream effect is confirmed"
                    )
                ledger = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
                if (
                    effect.run_id != previous.run_id
                    or ledger.run_id != previous.run_id
                    or ledger.idempotency_key != effect.idempotency_key
                    or ledger.message_id != effect.message_id
                    or ledger.payload_hash != effect.payload_hash
                    or ledger.downstream_reference != effect.downstream_reference
                    or ledger.status != ReplayStatus.CONFIRMED
                ):
                    raise RunConflictError(
                        "reset is blocked until an existing downstream effect is confirmed"
                    )

            messages = self._build_seed_messages(
                tenant_id=tenant_id,
                policy_version=policy_version,
                seeds=seeds,
                run_id=new_run_id,
                now=now,
            )
            run = DemoRun(
                run_id=new_run_id,
                tenant_id=tenant_id,
                status=RunStatus.READY,
                active=True,
                active_policy_version=policy_version,
                seeded_message_count=len(messages),
                created_at=now,
                updated_at=now,
            )
            for previous_snapshot, previous in active_runs:
                inactive = previous.model_copy(
                    update={
                        "active": False,
                        "status": RunStatus.INACTIVE,
                        "updated_at": now,
                        "expires_at": now + timedelta(days=30),
                    }
                )
                transaction.set(previous_snapshot.reference, _dump(inactive))
            transaction.set(run_ref, _dump(run))
            for message in messages:
                transaction.set(
                    self._message_ref(new_run_id, message.message_id), _dump(message)
                )
            transaction.set(
                tenant_ref,
                {
                    "tenant_id": tenant_id,
                    "active_policy_version": policy_version,
                    "updated_at": now,
                },
                merge=True,
            )
            return run

        return await rotate(self._client.transaction())

    async def get_active_run(self, tenant_id: str) -> DemoRun:
        query = self._client.collection("demo_runs").where(
            filter=FieldFilter("tenant_id", "==", tenant_id)
        )
        matches = [
            _load(DemoRun, snapshot.to_dict())
            async for snapshot in query.stream()
            if (snapshot.to_dict() or {}).get("active") is True
        ]
        if len(matches) != 1:
            raise NotFoundError(f"tenant {tenant_id} has no single active run")
        return matches[0]

    async def get_run(self, run_id: str) -> DemoRun:
        snapshot = await self._run_ref(run_id).get()
        if not snapshot.exists:
            raise NotFoundError(f"run {run_id} not found")
        return _load(DemoRun, snapshot.to_dict())

    async def mark_run_running(self, run_id: str) -> DemoRun:
        run_ref = self._run_ref(run_id)

        @async_transactional
        async def mark(transaction: AsyncTransaction) -> DemoRun:
            now = self._now()
            snapshot = await run_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise NotFoundError(f"run {run_id} not found")
            run = _load(DemoRun, snapshot.to_dict())
            if not run.active:
                raise RunConflictError("inactive runs cannot be started")
            if run.status == RunStatus.COMPLETE:
                return run
            updated = run.model_copy(
                update={
                    "status": RunStatus.RUNNING,
                    "started_at": run.started_at or now,
                    "updated_at": now,
                }
            )
            transaction.set(run_ref, _dump(updated))
            return updated

        return await mark(self._client.transaction())

    async def get_message(self, run_id: str, message_id: str) -> MessageRecord:
        snapshot = await self._message_ref(run_id, message_id).get()
        if not snapshot.exists:
            raise NotFoundError(f"message {run_id}/{message_id} not found")
        return _load(MessageRecord, snapshot.to_dict())

    async def list_messages(self, run_id: str) -> list[MessageRecord]:
        run_snapshot = await self._run_ref(run_id).get()
        if not run_snapshot.exists:
            raise NotFoundError(f"run {run_id} not found")
        messages = [
            _load(MessageRecord, snapshot.to_dict())
            async for snapshot in self._run_ref(run_id).collection("messages").stream()
        ]
        return sorted(messages, key=lambda item: item.message_id)

    async def update_analysis(
        self, run_id: str, message_id: str, analysis: MessageAnalysisUpdate
    ) -> MessageRecord:
        message_ref = self._message_ref(run_id, message_id)

        @async_transactional
        async def update(transaction: AsyncTransaction) -> MessageRecord:
            now = self._now()
            snapshot = await message_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise NotFoundError(f"message {run_id}/{message_id} not found")
            message = _load(MessageRecord, snapshot.to_dict())
            updated = message.model_copy(
                update={**analysis.model_dump(), "updated_at": now}
            )
            transaction.set(message_ref, _dump(updated))
            return updated

        return await update(self._client.transaction())

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
        message_ref = self._message_ref(run_id, message_id)

        @async_transactional
        async def update(transaction: AsyncTransaction) -> MessageRecord:
            now = self._now()
            snapshot = await message_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise NotFoundError(f"message {run_id}/{message_id} not found")
            message = _load(MessageRecord, snapshot.to_dict())
            updated = message.model_copy(
                update={
                    "repaired_payload": payload,
                    "repaired_payload_hash": repaired_hash,
                    "applied_repairs": applied_repairs,
                    "updated_at": now,
                }
            )
            transaction.set(message_ref, _dump(updated))
            return updated

        return await update(self._client.transaction())

    def _transition_models(
        self,
        *,
        message: MessageRecord,
        new_state: MessageState,
        triggering_event: str,
        trace_id: str,
        decision_summary: str | None,
        runbook_clause: str | None,
        error_code: str | None,
        failed_stage: FailedStage | None,
        next_attempt_at: datetime | None,
        now: datetime,
        receipt_id: str,
        operation_id: str,
    ) -> tuple[MessageRecord, Transition, Receipt]:
        ensure_transition(message.current_state, new_state, message.failed_stage)
        transition = Transition(
            sequence=message.next_transition_sequence,
            run_id=message.run_id,
            message_id=message.message_id,
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
            operation_id=operation_id,
            run_id=message.run_id,
            message_id=message.message_id,
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
        if new_state not in (
            MessageState.FAILED_RETRYABLE,
            MessageState.FAILED_FINAL,
        ):
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
        return updated, transition, receipt

    def _write_transition(
        self,
        transaction: AsyncTransaction,
        *,
        message_ref,
        message: MessageRecord,
        transition: Transition,
        receipt: Receipt,
    ) -> None:
        transition_ref = message_ref.collection("transitions").document(
            f"{transition.sequence:020d}"
        )
        receipt_ref = message_ref.collection("receipts").document(
            _document_id(receipt.receipt_id, "receipt_id")
        )
        transaction.set(message_ref, _dump(message))
        transaction.set(transition_ref, _dump(transition))
        transaction.set(receipt_ref, _dump(receipt))

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
        receipt_id = self._id("rcpt")
        operation_id = self._id("op")
        message_ref = self._message_ref(run_id, message_id)
        run_ref = self._run_ref(run_id)

        @async_transactional
        async def transition(transaction: AsyncTransaction) -> TransitionResult:
            now = self._now()
            snapshot = await message_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise NotFoundError(f"message {run_id}/{message_id} not found")
            message = _load(MessageRecord, snapshot.to_dict())
            run = None
            message_snapshots: list[Any] = []
            if is_terminal(new_state):
                run_snapshot = await run_ref.get(transaction=transaction)
                if not run_snapshot.exists:
                    raise NotFoundError(f"run {run_id} not found")
                run = _load(DemoRun, run_snapshot.to_dict())
                messages_stream = await transaction.get(
                    run_ref.collection("messages").order_by("__name__")
                )
                message_snapshots = [candidate async for candidate in messages_stream]
            updated, event, receipt = self._transition_models(
                message=message,
                new_state=new_state,
                triggering_event=triggering_event,
                trace_id=trace_id,
                decision_summary=decision_summary,
                runbook_clause=runbook_clause,
                error_code=error_code,
                failed_stage=failed_stage,
                next_attempt_at=next_attempt_at,
                now=now,
                receipt_id=receipt_id,
                operation_id=operation_id,
            )
            self._write_transition(
                transaction,
                message_ref=message_ref,
                message=updated,
                transition=event,
                receipt=receipt,
            )
            if run is not None and run.status == RunStatus.RUNNING:
                terminal = bool(message_snapshots)
                for candidate_snapshot in message_snapshots:
                    candidate = (
                        updated
                        if candidate_snapshot.id == message_id
                        else _load(MessageRecord, candidate_snapshot.to_dict())
                    )
                    if not is_terminal(candidate.current_state):
                        terminal = False
                        break
                if terminal:
                    completed = run.model_copy(
                        update={
                            "status": RunStatus.COMPLETE,
                            "completed_at": now,
                            "updated_at": now,
                        }
                    )
                    transaction.set(run_ref, _dump(completed))
            return TransitionResult(transition=event, receipt=receipt)

        return await transition(self._client.transaction())

    async def list_transitions(self, run_id: str, message_id: str) -> list[Transition]:
        message_ref = self._message_ref(run_id, message_id)
        if not (await message_ref.get()).exists:
            raise NotFoundError(f"message {run_id}/{message_id} not found")
        transitions = [
            _load(Transition, snapshot.to_dict())
            async for snapshot in message_ref.collection("transitions").stream()
        ]
        return sorted(transitions, key=lambda item: item.sequence)

    async def record_receipt(self, receipt: Receipt) -> Receipt:
        message_ref = self._message_ref(receipt.run_id, receipt.message_id)
        receipt_ref = message_ref.collection("receipts").document(
            _document_id(receipt.receipt_id, "receipt_id")
        )

        @async_transactional
        async def record(transaction: AsyncTransaction) -> Receipt:
            message_snapshot = await message_ref.get(transaction=transaction)
            receipt_snapshot = await receipt_ref.get(transaction=transaction)
            if not message_snapshot.exists:
                raise NotFoundError(
                    f"message {receipt.run_id}/{receipt.message_id} not found"
                )
            if receipt_snapshot.exists:
                existing = _load(Receipt, receipt_snapshot.to_dict())
                if existing != receipt:
                    raise RunConflictError("receipt id already has different content")
                return existing
            transaction.set(receipt_ref, _dump(receipt))
            return receipt

        return await record(self._client.transaction())

    async def list_receipts(self, run_id: str, message_id: str) -> list[Receipt]:
        message_ref = self._message_ref(run_id, message_id)
        if not (await message_ref.get()).exists:
            raise NotFoundError(f"message {run_id}/{message_id} not found")
        receipts = [
            _load(Receipt, snapshot.to_dict())
            async for snapshot in message_ref.collection("receipts").stream()
        ]
        return sorted(receipts, key=lambda item: (item.created_at, item.receipt_id))

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
        actual_hash = payload_hash(payload)
        attempt_id = self._id("delivery")
        message_ref = self._message_ref(run_id, source_message_id)
        delivery_ref = self._delivery_ref(run_id, delivery_key)
        inbox_ref = self._inbox_ref(run_id, delivery_key)
        attempt_ref = delivery_ref.collection("attempts").document(attempt_id)

        @async_transactional
        async def register(transaction: AsyncTransaction) -> DeliveryRegistration:
            now = self._now()
            message_snapshot = await message_ref.get(transaction=transaction)
            delivery_snapshot = await delivery_ref.get(transaction=transaction)
            inbox_snapshot = await inbox_ref.get(transaction=transaction)
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{source_message_id} not found")
            message = _load(MessageRecord, message_snapshot.to_dict())
            if actual_hash != message.original_payload_hash:
                raise PayloadHashMismatchError(
                    "delivery payload differs from the seeded source message"
                )
            if source_topic != message.source_topic:
                raise RunConflictError(
                    "delivery source topic differs from the seeded source message"
                )

            duplicate = delivery_snapshot.exists
            if duplicate:
                existing = _load(DeliveryRecord, delivery_snapshot.to_dict())
                if existing.payload_hash != actual_hash:
                    raise PayloadHashMismatchError(
                        "duplicate delivery key has a different payload hash"
                    )
                if (
                    existing.source_message_id != source_message_id
                    or existing.transport_message_id != transport_message_id
                    or existing.source_topic != source_topic
                ):
                    raise RunConflictError(
                        "duplicate delivery key has different transport identity"
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
                attempt_id=attempt_id,
                run_id=run_id,
                delivery_key=delivery_key,
                trace_id=trace_id,
                received_at=now,
                duplicate=duplicate,
            )
            if inbox_snapshot.exists:
                inbox = _load(InboxItem, inbox_snapshot.to_dict())
                if inbox.payload_hash != actual_hash:
                    raise PayloadHashMismatchError(
                        "inbox item has a different payload hash"
                    )
            else:
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

            transaction.set(delivery_ref, _dump(delivery))
            transaction.set(attempt_ref, _dump(attempt))
            if not inbox_snapshot.exists:
                transaction.set(inbox_ref, _dump(inbox))
            if message.transport_message_id is None:
                updated_message = message.model_copy(
                    update={
                        "transport_message_id": transport_message_id,
                        "updated_at": now,
                    }
                )
                transaction.set(message_ref, _dump(updated_message))
            return DeliveryRegistration(
                delivery=delivery,
                attempt=attempt,
                inbox=inbox,
                duplicate=duplicate,
            )

        return await register(self._client.transaction())

    async def claim_inbox(
        self, run_id: str, inbox_id: str, owner: str, lease_duration: timedelta
    ) -> InboxLease:
        if lease_duration <= timedelta(0):
            raise RunConflictError("inbox lease duration must be positive")
        inbox_ref = self._inbox_ref(run_id, inbox_id)

        @async_transactional
        async def claim(transaction: AsyncTransaction) -> InboxLease:
            now = self._now()
            snapshot = await inbox_ref.get(transaction=transaction)
            if not snapshot.exists:
                raise NotFoundError(f"inbox {run_id}/{inbox_id} not found")
            item = _load(InboxItem, snapshot.to_dict())
            if item.status in (InboxStatus.COMPLETED, InboxStatus.FAILED_FINAL):
                return InboxLease(item=item, acquired=False)
            lease_active = (
                item.lease_expires_at is not None and _utc(item.lease_expires_at) > now
            )
            if lease_active:
                return InboxLease(item=item, acquired=False)
            takeover = (
                item.lease_owner is not None
                and item.lease_expires_at is not None
                and _utc(item.lease_expires_at) <= now
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
            transaction.set(inbox_ref, _dump(updated))
            return InboxLease(item=updated, acquired=True, takeover=takeover)

        return await claim(self._client.transaction())

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
        inbox_ref = self._inbox_ref(run_id, inbox_id)
        run_ref = self._run_ref(run_id)

        @async_transactional
        async def update(transaction: AsyncTransaction) -> InboxItem:
            now = self._now()
            inbox_snapshot = await inbox_ref.get(transaction=transaction)
            run_snapshot = await run_ref.get(transaction=transaction)
            if not inbox_snapshot.exists:
                raise NotFoundError(f"inbox {run_id}/{inbox_id} not found")
            if not run_snapshot.exists:
                raise NotFoundError(f"run {run_id} not found")
            item = _load(InboxItem, inbox_snapshot.to_dict())
            run = _load(DemoRun, run_snapshot.to_dict())

            if (
                expected_attempt_count is not None
                and item.attempt_count != expected_attempt_count
            ):
                return item

            # A late scheduler or worker callback cannot regress a durable terminal
            # outcome. Returning the persisted item also makes repeats idempotent.
            if item.status in (InboxStatus.COMPLETED, InboxStatus.FAILED_FINAL):
                return item

            processing_lease_active = bool(
                item.status == InboxStatus.PROCESSING
                and item.lease_expires_at is not None
                and _utc(item.lease_expires_at) > now
            )

            if status == InboxStatus.ENQUEUED:
                if processing_lease_active:
                    return item
            elif status in (InboxStatus.RETRYABLE, InboxStatus.COMPLETED):
                if (
                    owner is None
                    or not processing_lease_active
                    or item.lease_owner != owner
                ):
                    raise LeaseNotOwnedError(
                        f"{status.value.lower()} inbox update requires a live owning worker"
                    )
            elif status == InboxStatus.FAILED_FINAL:
                if owner is not None:
                    if not processing_lease_active or item.lease_owner != owner:
                        raise LeaseNotOwnedError(
                            "final inbox update requires a live owning worker"
                        )
                else:
                    lease_expired_or_absent = (
                        item.lease_owner is None and item.lease_expires_at is None
                    ) or (
                        item.lease_expires_at is not None
                        and _utc(item.lease_expires_at) <= now
                    )
                    if run.active and item.status != InboxStatus.PROCESSING:
                        raise RunConflictError(
                            "an ownerless final inbox update requires an inactive run"
                        )
                    if run.active or not lease_expired_or_absent:
                        raise LeaseNotOwnedError(
                            "ownerless finalization requires an inactive run and expired lease"
                        )
            elif status == InboxStatus.PROCESSING:
                raise RunConflictError("claim_inbox owns PROCESSING transitions")
            elif status == InboxStatus.PENDING and item.status != InboxStatus.PENDING:
                raise RunConflictError("inbox status cannot regress to PENDING")

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
                        _utc(scheduled_at)
                        if scheduled_at is not None
                        else item.scheduled_at
                    ),
                    "next_attempt_at": (
                        _utc(next_attempt_at) if next_attempt_at is not None else None
                    ),
                    "last_error_code": error_code,
                    "lease_owner": None if clear_lease else item.lease_owner,
                    "lease_expires_at": (
                        None if clear_lease else item.lease_expires_at
                    ),
                    "updated_at": now,
                }
            )
            transaction.set(inbox_ref, _dump(updated))
            return updated

        return await update(self._client.transaction())

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
        message_ref = self._message_ref(run_id, message_id)
        ledger_ref = self._ledger_ref(run_id, idempotency_key)

        @async_transactional
        async def acquire(transaction: AsyncTransaction) -> ReplayLease:
            now = self._now()
            message_snapshot = await message_ref.get(transaction=transaction)
            ledger_snapshot = await ledger_ref.get(transaction=transaction)
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{message_id} not found")
            message = _load(MessageRecord, message_snapshot.to_dict())
            if (
                message.current_state != MessageState.REPLAYING
                or message.repaired_payload is None
                or message.repaired_payload_hash != repaired_payload_hash
                or payload_hash(message.repaired_payload) != repaired_payload_hash
            ):
                raise PayloadHashMismatchError(
                    "replay payload differs from the repaired message"
                )
            if message.idempotency_key not in (None, idempotency_key):
                raise PayloadHashMismatchError(
                    "message already has another idempotency key"
                )

            if ledger_snapshot.exists:
                entry = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
                if (
                    entry.run_id != run_id
                    or entry.idempotency_key != idempotency_key
                    or entry.payload_hash != repaired_payload_hash
                ):
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
            else:
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

            if entry.status == ReplayStatus.CONFIRMED:
                return ReplayLease(entry=entry, acquired=False, already_confirmed=True)
            if entry.status == ReplayStatus.FAILED_FINAL:
                return ReplayLease(entry=entry, acquired=False)
            lease_active = (
                entry.lease_expires_at is not None
                and _utc(entry.lease_expires_at) > now
            )
            if lease_active:
                return ReplayLease(entry=entry, acquired=False)
            takeover = (
                entry.lease_owner is not None
                and entry.lease_expires_at is not None
                and _utc(entry.lease_expires_at) <= now
            )
            updated_entry = entry.model_copy(
                update={
                    "lease_owner": owner,
                    "lease_expires_at": now + lease_duration,
                    "updated_at": now,
                }
            )
            updated_message = message.model_copy(
                update={"idempotency_key": idempotency_key, "updated_at": now}
            )
            transaction.set(ledger_ref, _dump(updated_entry))
            transaction.set(message_ref, _dump(updated_message))
            return ReplayLease(entry=updated_entry, acquired=True, takeover=takeover)

        return await acquire(self._client.transaction())

    async def start_replay_attempt(
        self, run_id: str, idempotency_key: str, owner: str
    ) -> ReplayLedgerEntry:
        ledger_ref = self._ledger_ref(run_id, idempotency_key)

        @async_transactional
        async def start(transaction: AsyncTransaction) -> ReplayLedgerEntry:
            now = self._now()
            ledger_snapshot = await ledger_ref.get(transaction=transaction)
            if not ledger_snapshot.exists:
                raise NotFoundError(f"replay ledger {idempotency_key} not found")
            entry = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
            if entry.run_id != run_id or entry.idempotency_key != idempotency_key:
                raise PayloadHashMismatchError(
                    "replay ledger identity does not match its document path"
                )
            message_ref = self._message_ref(run_id, entry.message_id)
            message_snapshot = await message_ref.get(transaction=transaction)
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{entry.message_id} not found")
            if entry.status == ReplayStatus.CONFIRMED:
                return entry
            if entry.status == ReplayStatus.FAILED_FINAL:
                raise RunConflictError("a final replay failure cannot be retried")
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or _utc(entry.lease_expires_at) <= now
            ):
                raise LeaseNotOwnedError("replay attempt requires a live owned lease")
            updated = entry.model_copy(
                update={
                    "status": ReplayStatus.ATTEMPTED,
                    "attempt_count": entry.attempt_count + 1,
                    "updated_at": now,
                }
            )
            message = _load(MessageRecord, message_snapshot.to_dict())
            updated_message = message.model_copy(
                update={"attempts": updated.attempt_count, "updated_at": now}
            )
            transaction.set(ledger_ref, _dump(updated))
            transaction.set(message_ref, _dump(updated_message))
            return updated

        return await start(self._client.transaction())

    async def create_or_get_downstream_effect(
        self,
        *,
        run_id: str,
        message_id: str,
        idempotency_key: str,
        repaired_payload_hash: str,
        owner: str,
    ) -> DownstreamSubmission:
        run_ref = self._run_ref(run_id)
        message_ref = self._message_ref(run_id, message_id)
        ledger_ref = self._ledger_ref(run_id, idempotency_key)
        effect_ref = self._effect_ref(run_id, idempotency_key)
        reference = (
            "rp_order_"
            + hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()[:16]
        )

        @async_transactional
        async def submit(transaction: AsyncTransaction) -> DownstreamSubmission:
            now = self._now()
            run_snapshot = await run_ref.get(transaction=transaction)
            message_snapshot = await message_ref.get(transaction=transaction)
            ledger_snapshot = await ledger_ref.get(transaction=transaction)
            effect_snapshot = await effect_ref.get(transaction=transaction)
            if not run_snapshot.exists:
                raise NotFoundError(f"run {run_id} not found")
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{message_id} not found")
            if not ledger_snapshot.exists:
                raise NotFoundError(f"replay ledger {idempotency_key} not found")
            run = _load(DemoRun, run_snapshot.to_dict())
            message = _load(MessageRecord, message_snapshot.to_dict())
            ledger = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
            tenant_ref = self._tenant_ref(run.tenant_id)
            tenant_snapshot = await tenant_ref.get(transaction=transaction)
            tenant_data = tenant_snapshot.to_dict() or {}
            active_policy_version = tenant_data.get("active_policy_version")
            if not active_policy_version:
                raise PolicyNotActiveError(
                    f"tenant {run.tenant_id} has no active policy"
                )
            policy_ref = self._policy_ref(run.tenant_id, str(active_policy_version))
            policy_snapshot = await policy_ref.get(transaction=transaction)
            if not policy_snapshot.exists:
                raise PolicyNotActiveError("active policy pointer is inconsistent")
            policy = _load(PolicyVersion, policy_snapshot.to_dict())

            if (
                run.run_id != run_id
                or not run.active
                or run.status != RunStatus.RUNNING
            ):
                raise RunConflictError(
                    "downstream effects require an active RUNNING demo run"
                )
            if (
                run.active_policy_version != active_policy_version
                or message.active_policy_version != active_policy_version
                or message.run_id != run_id
                or message.message_id != message_id
                or message.tenant_id != run.tenant_id
                or policy.version != active_policy_version
                or policy.tenant_id != run.tenant_id
                or policy.status != PolicyStatus.ACTIVE
            ):
                raise PolicyNotActiveError(
                    "run and message policy must equal the tenant's active policy"
                )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError("downstream effects require a REPLAYING message")
            if message.idempotency_key != idempotency_key:
                raise PayloadHashMismatchError(
                    "message idempotency key differs from the replay key"
                )
            if (
                message.repaired_payload is None
                or message.repaired_payload_hash != repaired_payload_hash
                or payload_hash(message.repaired_payload) != repaired_payload_hash
            ):
                raise PayloadHashMismatchError(
                    "message repaired payload differs from the replay payload"
                )
            if (
                ledger.run_id != run_id
                or ledger.message_id != message_id
                or ledger.idempotency_key != idempotency_key
                or ledger.payload_hash != repaired_payload_hash
            ):
                raise PayloadHashMismatchError(
                    "ledger does not match the downstream submission"
                )
            if ledger.action_version != policy.definition.action_version:
                raise PolicyNotActiveError(
                    "replay action version differs from the active policy"
                )
            if ledger.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "downstream effects require an ATTEMPTED replay ledger"
                )
            if (
                ledger.lease_owner != owner
                or ledger.lease_expires_at is None
                or _utc(ledger.lease_expires_at) <= now
            ):
                raise LeaseNotOwnedError(
                    "downstream effects require a live owned replay lease"
                )
            if effect_snapshot.exists:
                existing = _load(DownstreamEffect, effect_snapshot.to_dict())
                if (
                    existing.run_id != run_id
                    or existing.payload_hash != repaired_payload_hash
                    or existing.message_id != message_id
                    or existing.idempotency_key != idempotency_key
                ):
                    raise PayloadHashMismatchError(
                        "downstream key already has a different payload hash"
                    )
                updated = existing.model_copy(
                    update={
                        "submission_count": existing.submission_count + 1,
                        "updated_at": now,
                    }
                )
                transaction.set(effect_ref, _dump(updated))
                return DownstreamSubmission(effect=updated, created=False)
            effect = DownstreamEffect(
                run_id=run_id,
                message_id=message_id,
                idempotency_key=idempotency_key,
                payload_hash=repaired_payload_hash,
                downstream_reference=reference,
                created_at=now,
                updated_at=now,
            )
            transaction.set(effect_ref, _dump(effect))
            return DownstreamSubmission(effect=effect, created=True)

        return await submit(self._client.transaction())

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
        ledger_ref = self._ledger_ref(run_id, idempotency_key)
        effect_ref = self._effect_ref(run_id, idempotency_key)
        message_ref = self._message_ref(run_id, message_id)
        run_ref = self._run_ref(run_id)
        transition_receipt_id = self._id("rcpt")
        transition_operation_id = self._id("op")
        result_receipt_ref = message_ref.collection("receipts").document(
            _document_id(result_receipt.receipt_id, "receipt_id")
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
            or result_receipt.error_code is not None
            or result_receipt.retryable
            or not result_receipt.operation_id
        ):
            raise RunConflictError(
                "confirmed replay result receipt metadata is inconsistent"
            )
        if result_receipt.receipt_id == transition_receipt_id:
            raise RunConflictError(
                "result and transition receipts require distinct identifiers"
            )

        @async_transactional
        async def confirm(transaction: AsyncTransaction) -> ReplayLedgerEntry:
            now = self._now()
            ledger_snapshot = await ledger_ref.get(transaction=transaction)
            effect_snapshot = await effect_ref.get(transaction=transaction)
            message_snapshot = await message_ref.get(transaction=transaction)
            run_snapshot = await run_ref.get(transaction=transaction)
            result_receipt_snapshot = await result_receipt_ref.get(
                transaction=transaction
            )
            if not ledger_snapshot.exists or not effect_snapshot.exists:
                raise NotFoundError("ledger and downstream effect are both required")
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{message_id} not found")
            if not run_snapshot.exists:
                raise NotFoundError(f"run {run_id} not found")
            entry = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
            effect = _load(DownstreamEffect, effect_snapshot.to_dict())
            message = _load(MessageRecord, message_snapshot.to_dict())
            run = _load(DemoRun, run_snapshot.to_dict())
            if (
                run.run_id != run_id
                or message.run_id != run_id
                or message.message_id != message_id
                or entry.run_id != run_id
                or entry.message_id != message_id
                or entry.idempotency_key != idempotency_key
                or effect.run_id != run_id
                or effect.message_id != message_id
                or effect.idempotency_key != idempotency_key
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
                result_receipt.payload_hash != entry.payload_hash
                or result_receipt.attempt != entry.attempt_count
                or result_receipt.policy_version != message.active_policy_version
                or (
                    message.runbook_clause is not None
                    and result_receipt.runbook_clause != message.runbook_clause
                )
            ):
                raise RunConflictError(
                    "replay result receipt does not match persisted replay metadata"
                )
            if entry.status == ReplayStatus.CONFIRMED:
                if not result_receipt_snapshot.exists:
                    raise RunConflictError(
                        "confirmed replay ledger is missing its result receipt"
                    )
                existing_receipt = _load(Receipt, result_receipt_snapshot.to_dict())
                if existing_receipt != result_receipt:
                    raise RunConflictError(
                        "confirmed replay receipt differs from the persisted receipt"
                    )
                return entry
            if entry.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "confirmation requires an ATTEMPTED replay ledger"
                )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError("confirmation requires a REPLAYING message")
            if result_receipt_snapshot.exists:
                raise RunConflictError(
                    "replay result receipt must be committed with confirmation"
                )
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or _utc(entry.lease_expires_at) <= now
            ):
                raise LeaseNotOwnedError("confirmation requires a live owned lease")

            messages_stream = await transaction.get(
                run_ref.collection("messages").order_by("__name__")
            )
            message_snapshots = [snapshot async for snapshot in messages_stream]
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
            updated_message = message.model_copy(
                update={
                    "downstream_reference": downstream_reference,
                    "updated_at": now,
                }
            )
            updated_message, transition, transition_receipt = self._transition_models(
                message=updated_message,
                new_state=MessageState.REPLAYED,
                triggering_event="downstream_confirmed",
                trace_id=trace_id,
                decision_summary=updated_message.decision_summary,
                runbook_clause=updated_message.runbook_clause,
                error_code=None,
                failed_stage=None,
                next_attempt_at=None,
                now=now,
                receipt_id=transition_receipt_id,
                operation_id=transition_operation_id,
            )

            transaction.set(ledger_ref, _dump(confirmed))
            self._write_transition(
                transaction,
                message_ref=message_ref,
                message=updated_message,
                transition=transition,
                receipt=transition_receipt,
            )
            transaction.set(result_receipt_ref, _dump(result_receipt))

            if run.status == RunStatus.RUNNING and message_snapshots:
                terminal = True
                for snapshot in message_snapshots:
                    candidate = (
                        updated_message
                        if snapshot.id == message_id
                        else _load(MessageRecord, snapshot.to_dict())
                    )
                    if not is_terminal(candidate.current_state):
                        terminal = False
                        break
                if terminal:
                    completed = run.model_copy(
                        update={
                            "status": RunStatus.COMPLETE,
                            "completed_at": now,
                            "updated_at": now,
                        }
                    )
                    transaction.set(run_ref, _dump(completed))
            return confirmed

        return await confirm(self._client.transaction())

    @staticmethod
    def _validate_failed_replay_receipt(
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
        retry_at = _utc(next_attempt_at)
        ledger_ref = self._ledger_ref(run_id, idempotency_key)
        transition_receipt_id = self._id("rcpt")
        transition_operation_id = self._id("op")
        _document_id(result_receipt.receipt_id, "receipt_id")
        if result_receipt.receipt_id == transition_receipt_id:
            raise RunConflictError(
                "result and transition receipts require distinct identifiers"
            )

        @async_transactional
        async def mark(transaction: AsyncTransaction) -> ReplayLedgerEntry:
            now = self._now()
            ledger_snapshot = await ledger_ref.get(transaction=transaction)
            if not ledger_snapshot.exists:
                raise NotFoundError(f"replay ledger {idempotency_key} not found")
            entry = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
            if entry.run_id != run_id or entry.idempotency_key != idempotency_key:
                raise PayloadHashMismatchError(
                    "replay ledger identity does not match its document path"
                )
            message_ref = self._message_ref(run_id, entry.message_id)
            result_receipt_ref = message_ref.collection("receipts").document(
                result_receipt.receipt_id
            )
            message_snapshot = await message_ref.get(transaction=transaction)
            result_receipt_snapshot = await result_receipt_ref.get(
                transaction=transaction
            )
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{entry.message_id} not found")
            message = _load(MessageRecord, message_snapshot.to_dict())
            if (
                message.run_id != run_id
                or message.message_id != entry.message_id
                or message.idempotency_key != idempotency_key
                or message.repaired_payload_hash != entry.payload_hash
            ):
                raise PayloadHashMismatchError(
                    "retryable replay update does not match its message"
                )
            self._validate_failed_replay_receipt(
                entry=entry,
                message=message,
                receipt=result_receipt,
                error_code=error_code,
                retryable=True,
                trace_id=trace_id,
            )
            if entry.status == ReplayStatus.RETRYABLE:
                if not result_receipt_snapshot.exists:
                    raise RunConflictError(
                        "retryable replay ledger is missing its result receipt"
                    )
                existing_receipt = _load(Receipt, result_receipt_snapshot.to_dict())
                if (
                    existing_receipt != result_receipt
                    or message.current_state != MessageState.FAILED_RETRYABLE
                ):
                    raise RunConflictError("retryable replay result is inconsistent")
                return entry
            if entry.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "retryable replay update requires an ATTEMPTED ledger"
                )
            if result_receipt_snapshot.exists:
                raise RunConflictError(
                    "replay result receipt must be committed with the retryable update"
                )
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or _utc(entry.lease_expires_at) <= now
            ):
                raise LeaseNotOwnedError("retryable update requires a live owned lease")
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError(
                    "retryable replay update does not match the REPLAYING message"
                )
            updated_entry = entry.model_copy(
                update={
                    "status": ReplayStatus.RETRYABLE,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": error_code,
                    "next_attempt_at": retry_at,
                    "updated_at": now,
                }
            )
            updated_message, transition, transition_receipt = self._transition_models(
                message=message,
                new_state=MessageState.FAILED_RETRYABLE,
                triggering_event="replay_failed_retryable",
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                error_code=error_code,
                failed_stage=FailedStage.REPLAY,
                next_attempt_at=retry_at,
                now=now,
                receipt_id=transition_receipt_id,
                operation_id=transition_operation_id,
            )
            transaction.set(ledger_ref, _dump(updated_entry))
            self._write_transition(
                transaction,
                message_ref=message_ref,
                message=updated_message,
                transition=transition,
                receipt=transition_receipt,
            )
            transaction.set(result_receipt_ref, _dump(result_receipt))
            return updated_entry

        return await mark(self._client.transaction())

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
        if new_state not in (MessageState.FAILED_FINAL, MessageState.ESCALATED):
            raise RunConflictError(
                "final replay failure must transition to FAILED_FINAL or ESCALATED"
            )
        ledger_ref = self._ledger_ref(run_id, idempotency_key)
        run_ref = self._run_ref(run_id)
        transition_receipt_id = self._id("rcpt")
        transition_operation_id = self._id("op")
        _document_id(result_receipt.receipt_id, "receipt_id")
        if result_receipt.receipt_id == transition_receipt_id:
            raise RunConflictError(
                "result and transition receipts require distinct identifiers"
            )

        @async_transactional
        async def mark(transaction: AsyncTransaction) -> ReplayLedgerEntry:
            now = self._now()
            ledger_snapshot = await ledger_ref.get(transaction=transaction)
            run_snapshot = await run_ref.get(transaction=transaction)
            if not ledger_snapshot.exists:
                raise NotFoundError(f"replay ledger {idempotency_key} not found")
            if not run_snapshot.exists:
                raise NotFoundError(f"run {run_id} not found")
            entry = _load(ReplayLedgerEntry, ledger_snapshot.to_dict())
            run = _load(DemoRun, run_snapshot.to_dict())
            if entry.run_id != run_id or entry.idempotency_key != idempotency_key:
                raise PayloadHashMismatchError(
                    "replay ledger identity does not match its document path"
                )
            message_ref = self._message_ref(run_id, entry.message_id)
            result_receipt_ref = message_ref.collection("receipts").document(
                result_receipt.receipt_id
            )
            message_snapshot = await message_ref.get(transaction=transaction)
            result_receipt_snapshot = await result_receipt_ref.get(
                transaction=transaction
            )
            if not message_snapshot.exists:
                raise NotFoundError(f"message {run_id}/{entry.message_id} not found")
            messages_stream = await transaction.get(
                run_ref.collection("messages").order_by("__name__")
            )
            message_snapshots = [snapshot async for snapshot in messages_stream]
            message = _load(MessageRecord, message_snapshot.to_dict())
            if (
                message.run_id != run_id
                or message.message_id != entry.message_id
                or message.idempotency_key != idempotency_key
                or message.repaired_payload_hash != entry.payload_hash
            ):
                raise PayloadHashMismatchError(
                    "final replay update does not match its message"
                )
            self._validate_failed_replay_receipt(
                entry=entry,
                message=message,
                receipt=result_receipt,
                error_code=error_code,
                retryable=False,
                trace_id=trace_id,
            )
            if entry.status == ReplayStatus.FAILED_FINAL:
                if not result_receipt_snapshot.exists:
                    raise RunConflictError(
                        "final replay ledger is missing its result receipt"
                    )
                existing_receipt = _load(Receipt, result_receipt_snapshot.to_dict())
                if (
                    existing_receipt != result_receipt
                    or message.current_state != new_state
                ):
                    raise RunConflictError("final replay result is inconsistent")
                return entry
            if entry.status != ReplayStatus.ATTEMPTED:
                raise RunConflictError(
                    "final replay update requires an ATTEMPTED ledger"
                )
            if result_receipt_snapshot.exists:
                raise RunConflictError(
                    "replay result receipt must be committed with the final update"
                )
            if (
                entry.lease_owner != owner
                or not entry.lease_expires_at
                or _utc(entry.lease_expires_at) <= now
            ):
                raise LeaseNotOwnedError(
                    "final failure update requires a live owned lease"
                )
            if message.current_state != MessageState.REPLAYING:
                raise RunConflictError(
                    "final replay update does not match the REPLAYING message"
                )
            updated_entry = entry.model_copy(
                update={
                    "status": ReplayStatus.FAILED_FINAL,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": error_code,
                    "next_attempt_at": None,
                    "updated_at": now,
                }
            )
            updated_message, transition, transition_receipt = self._transition_models(
                message=message,
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
                now=now,
                receipt_id=transition_receipt_id,
                operation_id=transition_operation_id,
            )
            transaction.set(ledger_ref, _dump(updated_entry))
            self._write_transition(
                transaction,
                message_ref=message_ref,
                message=updated_message,
                transition=transition,
                receipt=transition_receipt,
            )
            transaction.set(result_receipt_ref, _dump(result_receipt))

            if run.status == RunStatus.RUNNING and message_snapshots:
                terminal = True
                for snapshot in message_snapshots:
                    candidate = (
                        updated_message
                        if snapshot.id == entry.message_id
                        else _load(MessageRecord, snapshot.to_dict())
                    )
                    if not is_terminal(candidate.current_state):
                        terminal = False
                        break
                if terminal:
                    completed = run.model_copy(
                        update={
                            "status": RunStatus.COMPLETE,
                            "completed_at": now,
                            "updated_at": now,
                        }
                    )
                    transaction.set(run_ref, _dump(completed))
            return updated_entry

        return await mark(self._client.transaction())

    async def get_replay_ledger(
        self, run_id: str, idempotency_key: str
    ) -> ReplayLedgerEntry:
        snapshot = await self._ledger_ref(run_id, idempotency_key).get()
        if not snapshot.exists:
            raise NotFoundError(f"replay ledger {idempotency_key} not found")
        return _load(ReplayLedgerEntry, snapshot.to_dict())

    async def get_downstream_effect(
        self, run_id: str, idempotency_key: str
    ) -> DownstreamEffect:
        snapshot = await self._effect_ref(run_id, idempotency_key).get()
        if not snapshot.exists:
            raise NotFoundError(f"downstream effect {idempotency_key} not found")
        return _load(DownstreamEffect, snapshot.to_dict())

    async def list_downstream_effects(self, run_id: str) -> list[DownstreamEffect]:
        effects = [
            _load(DownstreamEffect, snapshot.to_dict())
            async for snapshot in self._run_ref(run_id)
            .collection("downstream_effects")
            .stream()
        ]
        return sorted(effects, key=lambda item: item.idempotency_key)

    async def recovery_candidates(
        self, now: datetime
    ) -> tuple[list[InboxItem], list[ReplayLedgerEntry], list[MessageRecord]]:
        cutoff = _utc(now)
        enqueued_cutoff = cutoff - timedelta(minutes=5)
        inbox_candidates: list[InboxItem] = []
        ledger_candidates: list[ReplayLedgerEntry] = []
        message_candidates: list[MessageRecord] = []

        async for run_snapshot in self._client.collection("demo_runs").stream():
            run = _load(DemoRun, run_snapshot.to_dict())
            if not run.active:
                continue
            run_ref = self._run_ref(run.run_id)
            async for snapshot in run_ref.collection("inbox").stream():
                item = _load(InboxItem, snapshot.to_dict())
                if (
                    item.status == InboxStatus.PENDING
                    or (
                        item.status == InboxStatus.ENQUEUED
                        and (
                            item.scheduled_at is None
                            or _utc(item.scheduled_at) <= enqueued_cutoff
                        )
                    )
                    or (
                        item.status == InboxStatus.PROCESSING
                        and item.lease_expires_at is not None
                        and _utc(item.lease_expires_at) <= cutoff
                    )
                    or (
                        item.status == InboxStatus.RETRYABLE
                        and item.next_attempt_at is not None
                        and _utc(item.next_attempt_at) <= cutoff
                    )
                ):
                    inbox_candidates.append(item)
            async for snapshot in run_ref.collection("replay_ledger").stream():
                entry = _load(ReplayLedgerEntry, snapshot.to_dict())
                if entry.status in (
                    ReplayStatus.CONFIRMED,
                    ReplayStatus.FAILED_FINAL,
                ):
                    continue
                if (
                    entry.lease_expires_at is not None
                    and _utc(entry.lease_expires_at) <= cutoff
                ) or (
                    entry.status == ReplayStatus.RETRYABLE
                    and entry.next_attempt_at is not None
                    and _utc(entry.next_attempt_at) <= cutoff
                ):
                    ledger_candidates.append(entry)
            async for snapshot in run_ref.collection("messages").stream():
                message = _load(MessageRecord, snapshot.to_dict())
                if (
                    message.current_state == MessageState.FAILED_RETRYABLE
                    and message.next_attempt_at is not None
                    and _utc(message.next_attempt_at) <= cutoff
                ):
                    message_candidates.append(message)

        inbox_candidates.sort(key=lambda item: (item.run_id, item.inbox_id))
        ledger_candidates.sort(key=lambda item: (item.run_id, item.idempotency_key))
        message_candidates.sort(key=lambda item: (item.run_id, item.message_id))
        return inbox_candidates, ledger_candidates, message_candidates
