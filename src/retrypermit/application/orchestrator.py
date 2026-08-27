from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from retrypermit.application.repair_service import repair_payload
from retrypermit.application.triage_validator import authorize_triage
from retrypermit.domain.enums import (
    FailedStage,
    MessageState,
    ReceiptKind,
    ReceiptStatus,
)
from retrypermit.domain.errors import (
    PayloadHashMismatchError,
    RepairRejectedError,
    RunConflictError,
)
from retrypermit.domain.hashing import derive_idempotency_key
from retrypermit.domain.messages import MessageAnalysisUpdate, MessageRecord
from retrypermit.domain.policy import PolicyVersion
from retrypermit.domain.receipts import Receipt
from retrypermit.domain.state_machine import is_terminal
from retrypermit.domain.triage import PolicyClauseContext, Triage, TriageRequest
from retrypermit.ports.clock import Clock, SystemClock
from retrypermit.ports.model_provider import ModelProvider
from retrypermit.ports.store import Store


logger = logging.getLogger("retrypermit.orchestrator")


class RetryPermitOrchestrator:
    """Deterministic executor around a proposal-only model boundary."""

    def __init__(
        self,
        store: Store,
        model_provider: ModelProvider,
        *,
        clock: Clock | None = None,
        id_factory: Callable[[], str] | None = None,
        lease_duration: timedelta = timedelta(seconds=30),
        retry_delay: timedelta = timedelta(seconds=5),
        model_timeout_seconds: float = 20.0,
    ) -> None:
        self._store = store
        self._model = model_provider
        self._clock = clock or SystemClock()
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._model_timeout_seconds = model_timeout_seconds

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{self._id_factory()}"

    async def run_all(self, run_id: str) -> list[MessageRecord]:
        await self._store.mark_run_running(run_id)
        messages = await self._store.list_messages(run_id)
        return list(
            await asyncio.gather(
                *(self.run_message(run_id, message.message_id) for message in messages)
            )
        )

    async def run_message(
        self,
        run_id: str,
        message_id: str,
        *,
        worker_id: str | None = None,
        trace_id: str | None = None,
    ) -> MessageRecord:
        worker = worker_id or self._id("worker")
        trace = trace_id or self._id("trace")
        await self._store.mark_run_running(run_id)
        logger.info(
            "message_processing_started",
            extra={
                "trace_id": trace,
                "run_id": run_id,
                "message_id": message_id,
                "tool_name": "deterministic_orchestrator",
                "worker_id": worker,
            },
        )

        for _ in range(16):
            message = await self._store.get_message(run_id, message_id)
            if is_terminal(message.current_state):
                return message
            if message.current_state == MessageState.FAILED_RETRYABLE:
                if (
                    message.next_attempt_at
                    and message.next_attempt_at > self._clock.now()
                ):
                    return message
                if message.failed_stage == FailedStage.TRIAGE:
                    target = MessageState.TRIAGING
                elif message.failed_stage == FailedStage.REPLAY:
                    target = MessageState.REPLAYING
                else:
                    raise RunConflictError(
                        "retryable message has no valid failed_stage"
                    )
                await self._store.transition_message(
                    run_id=run_id,
                    message_id=message_id,
                    new_state=target,
                    triggering_event="retry_due",
                    trace_id=trace,
                )
                continue
            if message.current_state == MessageState.RECEIVED:
                await self._store.transition_message(
                    run_id=run_id,
                    message_id=message_id,
                    new_state=MessageState.TRIAGING,
                    triggering_event="processing_started",
                    trace_id=trace,
                )
                continue
            if message.current_state == MessageState.TRIAGING:
                terminal = await self._triage_stage(message, trace)
                if terminal:
                    return await self._store.get_message(run_id, message_id)
                continue
            if message.current_state == MessageState.TRIAGED:
                await self._store.transition_message(
                    run_id=run_id,
                    message_id=message_id,
                    new_state=MessageState.PLANNED,
                    triggering_event="triage_persisted",
                    trace_id=trace,
                    decision_summary=message.decision_summary,
                    runbook_clause=message.runbook_clause,
                )
                continue
            if message.current_state == MessageState.PLANNED:
                terminal = await self._planning_stage(message, trace)
                if terminal:
                    return await self._store.get_message(run_id, message_id)
                continue
            if message.current_state == MessageState.REPAIRING:
                terminal = await self._repair_stage(message, trace)
                if terminal:
                    return await self._store.get_message(run_id, message_id)
                continue
            if message.current_state == MessageState.REPLAYING:
                await self._replay_stage(message, worker, trace)
                return await self._store.get_message(run_id, message_id)
            raise RunConflictError(
                f"unsupported orchestration state {message.current_state}"
            )
        raise RunConflictError("orchestration exceeded the bounded state-step count")

    async def _triage_stage(self, message: MessageRecord, trace_id: str) -> bool:
        policy = await self._active_policy_for(message)
        prior_receipts = await self._store.list_receipts(
            message.run_id, message.message_id
        )
        attempt = 1 + sum(
            receipt.kind == ReceiptKind.TRIAGE_ATTEMPT for receipt in prior_receipts
        )
        operation_id = self._id("op")
        await self._record_receipt(
            message,
            kind=ReceiptKind.TRIAGE_ATTEMPT,
            status=ReceiptStatus.STARTED,
            stage="triage",
            trace_id=trace_id,
            operation_id=operation_id,
            attempt=attempt,
        )
        request = TriageRequest(
            tenant_id=message.tenant_id,
            run_id=message.run_id,
            message_id=message.message_id,
            payload=message.original_payload,
            policy_version=policy.version,
            allowed_repairs=policy.definition.migration_rules,
            clause_ids=[clause.clause_id for clause in policy.definition.clauses],
            policy_clauses=[
                PolicyClauseContext(
                    clause_id=clause.clause_id,
                    page=clause.page,
                    text=clause.text,
                    failure_classes=clause.failure_classes,
                    authorized_actions=clause.authorized_actions,
                )
                for clause in policy.definition.clauses
            ],
        )
        model_started = time.perf_counter()
        try:
            raw = await asyncio.wait_for(
                self._model.triage(request), timeout=self._model_timeout_seconds
            )
            triage = Triage.model_validate(raw)
        except TimeoutError:
            will_retry = attempt < policy.definition.retry_limit
            await self._record_receipt(
                message,
                kind=ReceiptKind.TRIAGE_RESULT,
                status=ReceiptStatus.FAILED,
                stage="triage",
                trace_id=trace_id,
                operation_id=operation_id,
                attempt=attempt,
                error_code="MODEL_TIMEOUT",
                retryable=will_retry,
                details={
                    "duration_ms": round(
                        (time.perf_counter() - model_started) * 1000, 3
                    )
                },
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.FAILED_RETRYABLE,
                triggering_event="triage_timeout",
                trace_id=trace_id,
                error_code="MODEL_TIMEOUT",
                failed_stage=FailedStage.TRIAGE,
                next_attempt_at=self._clock.now() + self._retry_delay,
            )
            if attempt >= policy.definition.retry_limit:
                await self._store.transition_message(
                    run_id=message.run_id,
                    message_id=message.message_id,
                    new_state=MessageState.FAILED_FINAL,
                    triggering_event="triage_retry_budget_exhausted",
                    trace_id=trace_id,
                    error_code="MODEL_TIMEOUT",
                    failed_stage=FailedStage.TRIAGE,
                )
            return True
        except Exception:
            # Do not persist raw model exceptions or hidden model reasoning.
            await self._record_receipt(
                message,
                kind=ReceiptKind.TRIAGE_RESULT,
                status=ReceiptStatus.FAILED,
                stage="triage",
                trace_id=trace_id,
                operation_id=operation_id,
                attempt=attempt,
                error_code="INVALID_MODEL_OUTPUT",
                retryable=False,
                details={
                    "duration_ms": round(
                        (time.perf_counter() - model_started) * 1000, 3
                    )
                },
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.ESCALATED,
                triggering_event="invalid_model_output",
                trace_id=trace_id,
                error_code="INVALID_MODEL_OUTPUT",
            )
            return True

        await self._record_receipt(
            message,
            kind=ReceiptKind.TRIAGE_RESULT,
            status=ReceiptStatus.CONFIRMED,
            stage="triage",
            trace_id=trace_id,
            operation_id=operation_id,
            attempt=attempt,
            details={
                "failure_class": triage.failure_class.value,
                "confidence": triage.confidence,
                "runbook_clause": triage.runbook_clause,
                "runbook_page": triage.runbook_page,
                "evidence": triage.evidence,
                "contains_injection_attempt": triage.contains_injection_attempt,
                "proposed_repairs": [
                    item.model_dump(mode="json") for item in triage.proposed_repairs
                ],
                "recommended_action": triage.recommended_action.value,
                "decision_summary": triage.decision_summary,
                "duration_ms": round((time.perf_counter() - model_started) * 1000, 3),
            },
        )
        await self._store.update_analysis(
            message.run_id,
            message.message_id,
            MessageAnalysisUpdate(
                failure_class=triage.failure_class,
                confidence=triage.confidence,
                runbook_clause=triage.runbook_clause,
                runbook_page=triage.runbook_page,
                decision_summary=triage.decision_summary,
                evidence=triage.evidence,
                contains_injection_attempt=triage.contains_injection_attempt,
                recommended_action=triage.recommended_action,
                proposed_repairs=triage.proposed_repairs,
            ),
        )
        await self._store.transition_message(
            run_id=message.run_id,
            message_id=message.message_id,
            new_state=MessageState.TRIAGED,
            triggering_event="triage_completed",
            trace_id=trace_id,
            decision_summary=triage.decision_summary,
            runbook_clause=triage.runbook_clause,
        )
        return False

    async def _planning_stage(self, message: MessageRecord, trace_id: str) -> bool:
        policy = await self._active_policy_for(message)
        triage = self._triage_from_message(message)
        decision = authorize_triage(message.original_payload, triage, policy)
        operation_id = self._id("op")
        await self._record_receipt(
            message,
            kind=ReceiptKind.POLICY,
            status=(
                ReceiptStatus.CONFIRMED
                if decision.authorized
                else ReceiptStatus.REJECTED
            ),
            stage="authorization",
            trace_id=trace_id,
            operation_id=operation_id,
            error_code=None if decision.authorized else decision.error_codes[0],
            details={
                "authorized": decision.authorized,
                "error_codes": decision.error_codes,
                "effective_cap": str(decision.effective_cap),
                "amount": str(decision.amount) if decision.amount is not None else None,
            },
        )
        if not decision.authorized:
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.ESCALATED,
                triggering_event="authorization_denied",
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                error_code=decision.error_codes[0],
            )
            return True
        await self._store.transition_message(
            run_id=message.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPAIRING,
            triggering_event="authorization_confirmed",
            trace_id=trace_id,
            decision_summary=message.decision_summary,
            runbook_clause=message.runbook_clause,
        )
        return False

    async def _repair_stage(self, message: MessageRecord, trace_id: str) -> bool:
        policy = await self._active_policy_for(message)
        operation_id = self._id("op")
        repair_started = time.perf_counter()
        try:
            result = repair_payload(
                message.original_payload, message.proposed_repairs, policy
            )
        except RepairRejectedError as exc:
            await self._record_receipt(
                message,
                kind=ReceiptKind.REPAIR_RESULT,
                status=ReceiptStatus.REJECTED,
                stage="repair",
                trace_id=trace_id,
                operation_id=operation_id,
                error_code=exc.code,
                details={
                    "duration_ms": round(
                        (time.perf_counter() - repair_started) * 1000, 3
                    )
                },
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.ESCALATED,
                triggering_event="repair_rejected",
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                error_code=exc.code,
            )
            return True
        await self._store.set_repaired_payload(
            message.run_id,
            message.message_id,
            result.repaired_payload,
            result.repaired_payload_hash,
            result.applied_repairs,
        )
        await self._record_receipt(
            message,
            kind=ReceiptKind.REPAIR_RESULT,
            status=ReceiptStatus.CONFIRMED,
            stage="repair",
            trace_id=trace_id,
            operation_id=operation_id,
            payload_hash=result.repaired_payload_hash,
            details={
                "before_hash": result.original_payload_hash,
                "after_hash": result.repaired_payload_hash,
                "applied_repairs": [
                    item.model_dump(mode="json") for item in result.applied_repairs
                ],
                "rejected_repairs": [],
                "duration_ms": round((time.perf_counter() - repair_started) * 1000, 3),
            },
        )
        await self._store.transition_message(
            run_id=message.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPLAYING,
            triggering_event="repair_validated",
            trace_id=trace_id,
            decision_summary=message.decision_summary,
            runbook_clause=message.runbook_clause,
        )
        return False

    async def _replay_stage(
        self, message: MessageRecord, worker_id: str, trace_id: str
    ) -> None:
        policy = await self._active_policy_for(message)
        if message.repaired_payload is None or message.repaired_payload_hash is None:
            raise RunConflictError(
                "replay requires a persisted repaired payload and hash"
            )
        key = derive_idempotency_key(
            message.tenant_id,
            message.source_topic,
            message.message_id,
            policy.definition.action_version,
        )
        try:
            lease = await self._store.acquire_replay_lease(
                run_id=message.run_id,
                message_id=message.message_id,
                idempotency_key=key,
                repaired_payload_hash=message.repaired_payload_hash,
                action_version=policy.definition.action_version,
                owner=worker_id,
                lease_duration=self._lease_duration,
            )
        except PayloadHashMismatchError:
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.ESCALATED,
                triggering_event="idempotency_payload_mismatch",
                trace_id=trace_id,
                error_code="IDEMPOTENCY_PAYLOAD_MISMATCH",
            )
            return
        if not lease.acquired:
            return

        entry = await self._store.start_replay_attempt(message.run_id, key, worker_id)
        operation_id = self._id("op")
        await self._record_receipt(
            message,
            kind=ReceiptKind.REPLAY_ATTEMPT,
            status=ReceiptStatus.STARTED,
            stage="replay",
            trace_id=trace_id,
            operation_id=operation_id,
            attempt=entry.attempt_count,
            payload_hash=message.repaired_payload_hash,
            idempotency_key=key,
        )
        downstream_started = time.perf_counter()
        try:
            submission = await self._store.create_or_get_downstream_effect(
                run_id=message.run_id,
                message_id=message.message_id,
                idempotency_key=key,
                repaired_payload_hash=message.repaired_payload_hash,
                owner=worker_id,
            )
        except PayloadHashMismatchError as exc:
            failed_receipt = Receipt(
                receipt_id=self._id("rcpt"),
                operation_id=operation_id,
                run_id=message.run_id,
                message_id=message.message_id,
                kind=ReceiptKind.REPLAY_RESULT,
                status=ReceiptStatus.FAILED,
                stage="replay",
                trace_id=trace_id,
                attempt=entry.attempt_count,
                policy_version=message.active_policy_version,
                runbook_clause=message.runbook_clause,
                payload_hash=message.repaired_payload_hash,
                idempotency_key=key,
                error_code=exc.code,
                retryable=False,
                details={
                    "duration_ms": round(
                        (time.perf_counter() - downstream_started) * 1000, 3
                    )
                },
                created_at=self._clock.now(),
            )
            await self._store.mark_replay_failed_final(
                run_id=message.run_id,
                idempotency_key=key,
                owner=worker_id,
                error_code=exc.code,
                trace_id=trace_id,
                result_receipt=failed_receipt,
                new_state=MessageState.ESCALATED,
                triggering_event="downstream_payload_mismatch",
            )
            self._log_receipt(failed_receipt, state=MessageState.ESCALATED)
            return
        except Exception:
            error_code = "DOWNSTREAM_UNAVAILABLE"
            retryable = entry.attempt_count < policy.definition.retry_limit
            failed_receipt = Receipt(
                receipt_id=self._id("rcpt"),
                operation_id=operation_id,
                run_id=message.run_id,
                message_id=message.message_id,
                kind=ReceiptKind.REPLAY_RESULT,
                status=ReceiptStatus.FAILED,
                stage="replay",
                trace_id=trace_id,
                attempt=entry.attempt_count,
                policy_version=message.active_policy_version,
                runbook_clause=message.runbook_clause,
                payload_hash=message.repaired_payload_hash,
                idempotency_key=key,
                error_code=error_code,
                retryable=retryable,
                details={
                    "duration_ms": round(
                        (time.perf_counter() - downstream_started) * 1000, 3
                    )
                },
                created_at=self._clock.now(),
            )
            if entry.attempt_count >= policy.definition.retry_limit:
                await self._store.mark_replay_failed_final(
                    run_id=message.run_id,
                    idempotency_key=key,
                    owner=worker_id,
                    error_code=error_code,
                    trace_id=trace_id,
                    result_receipt=failed_receipt,
                )
                result_state = MessageState.FAILED_FINAL
            else:
                next_attempt = self._clock.now() + self._retry_delay
                await self._store.mark_replay_retryable(
                    run_id=message.run_id,
                    idempotency_key=key,
                    owner=worker_id,
                    error_code=error_code,
                    next_attempt_at=next_attempt,
                    trace_id=trace_id,
                    result_receipt=failed_receipt,
                )
                result_state = MessageState.FAILED_RETRYABLE
            self._log_receipt(failed_receipt, state=result_state)
            return

        result_receipt = Receipt(
            receipt_id=self._id("rcpt"),
            operation_id=operation_id,
            run_id=message.run_id,
            message_id=message.message_id,
            kind=ReceiptKind.REPLAY_RESULT,
            status=ReceiptStatus.CONFIRMED,
            stage="replay",
            trace_id=trace_id,
            attempt=entry.attempt_count,
            policy_version=message.active_policy_version,
            runbook_clause=message.runbook_clause,
            payload_hash=message.repaired_payload_hash,
            idempotency_key=key,
            downstream_reference=submission.effect.downstream_reference,
            details={
                "effect_created": submission.created,
                "submission_count": submission.effect.submission_count,
                "duration_ms": round(
                    (time.perf_counter() - downstream_started) * 1000, 3
                ),
            },
            created_at=self._clock.now(),
        )
        await self._store.confirm_replay(
            run_id=message.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            owner=worker_id,
            downstream_reference=submission.effect.downstream_reference,
            trace_id=trace_id,
            result_receipt=result_receipt,
        )
        self._log_receipt(result_receipt, state=MessageState.REPLAYED)

    async def _active_policy_for(self, message: MessageRecord) -> PolicyVersion:
        policy = await self._store.get_active_policy(message.tenant_id)
        if policy.version != message.active_policy_version:
            raise RunConflictError(
                "the run's policy is no longer active; deterministic replanning is required"
            )
        return policy

    @staticmethod
    def _triage_from_message(message: MessageRecord) -> Triage:
        if (
            message.failure_class is None
            or message.confidence is None
            or message.runbook_clause is None
            or message.runbook_page is None
            or message.recommended_action is None
            or message.decision_summary is None
        ):
            raise RunConflictError("planned message lacks persisted triage fields")
        return Triage(
            failure_class=message.failure_class,
            confidence=message.confidence,
            runbook_clause=message.runbook_clause,
            runbook_page=message.runbook_page,
            evidence=message.evidence,
            contains_injection_attempt=message.contains_injection_attempt,
            proposed_repairs=message.proposed_repairs,
            recommended_action=message.recommended_action,
            decision_summary=message.decision_summary,
        )

    async def _record_receipt(
        self,
        message: MessageRecord,
        *,
        kind: ReceiptKind,
        status: ReceiptStatus,
        stage: str,
        trace_id: str,
        operation_id: str,
        attempt: int = 1,
        payload_hash: str | None = None,
        idempotency_key: str | None = None,
        downstream_reference: str | None = None,
        error_code: str | None = None,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> Receipt:
        receipt = await self._store.record_receipt(
            Receipt(
                receipt_id=self._id("rcpt"),
                operation_id=operation_id,
                run_id=message.run_id,
                message_id=message.message_id,
                kind=kind,
                status=status,
                stage=stage,
                attempt=attempt,
                trace_id=trace_id,
                policy_version=message.active_policy_version,
                runbook_clause=message.runbook_clause,
                payload_hash=payload_hash,
                idempotency_key=idempotency_key,
                downstream_reference=downstream_reference,
                error_code=error_code,
                retryable=retryable,
                details=details or {},
                created_at=self._clock.now(),
            )
        )
        self._log_receipt(receipt)
        return receipt

    @staticmethod
    def _log_receipt(receipt: Receipt, *, state: MessageState | None = None) -> None:
        logger.info(
            "receipt_persisted",
            extra={
                "trace_id": receipt.trace_id,
                "run_id": receipt.run_id,
                "message_id": receipt.message_id,
                "state": state.value if state is not None else None,
                "failed_stage": (
                    receipt.stage
                    if receipt.status == ReceiptStatus.FAILED
                    and receipt.stage
                    in {
                        FailedStage.TRIAGE.value,
                        FailedStage.REPLAY.value,
                    }
                    else None
                ),
                "tool_name": receipt.stage,
                "receipt_id": receipt.receipt_id,
                "policy_version": receipt.policy_version,
                "runbook_clause": receipt.runbook_clause,
                "attempt": receipt.attempt,
                "duration_ms": receipt.details.get("duration_ms"),
                "outcome": receipt.status.value,
                "error_code": receipt.error_code,
            },
        )
