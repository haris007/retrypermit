from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from retrypermit.application.repair_service import repair_payload
from retrypermit.application.triage_validator import (
    authorize_policy_action,
    authorize_triage,
    invalid_data_resolution,
    is_transient_inventory_503,
    payload_contains_injection,
)
from retrypermit.domain.enums import (
    FailedStage,
    FailureClass,
    MessageState,
    RecommendedAction,
    ReceiptKind,
    ReceiptStatus,
)
from retrypermit.domain.errors import (
    AmbiguousDownstreamOutcomeError,
    PayloadHashMismatchError,
    RepairRejectedError,
    RunConflictError,
)
from retrypermit.domain.hashing import derive_idempotency_key, payload_hash
from retrypermit.domain.messages import MessageAnalysisUpdate, MessageRecord
from retrypermit.domain.policy import PolicyVersion
from retrypermit.domain.receipts import Receipt
from retrypermit.domain.state_machine import is_terminal
from retrypermit.domain.triage import PolicyClauseContext, Triage, TriageRequest
from retrypermit.ports.clock import Clock, SystemClock
from retrypermit.ports.downstream import Downstream
from retrypermit.ports.model_provider import ModelProvider
from retrypermit.ports.store import Store


logger = logging.getLogger("retrypermit.orchestrator")


class RetryPermitOrchestrator:
    """Deterministic executor around a proposal-only model boundary."""

    def __init__(
        self,
        store: Store,
        model_provider: ModelProvider,
        downstream: Downstream,
        *,
        clock: Clock | None = None,
        id_factory: Callable[[], str] | None = None,
        lease_duration: timedelta = timedelta(seconds=30),
        retry_base_delay: timedelta = timedelta(seconds=1),
        retry_max_delay: timedelta = timedelta(seconds=10),
        jitter_source: Callable[[], float] | None = None,
        model_timeout_seconds: float = 20.0,
        downstream_timeout_seconds: float = 10.0,
        demo_recheck_delay: timedelta | None = None,
        simulated_transient_recovery_delay: timedelta = timedelta(seconds=10),
        triage_concurrency: int = 4,
    ) -> None:
        self._store = store
        self._model = model_provider
        self._downstream = downstream
        self._clock = clock or SystemClock()
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        self._lease_duration = lease_duration
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay
        self._jitter_source = jitter_source or random.random
        self._model_timeout_seconds = model_timeout_seconds
        self._downstream_timeout_seconds = downstream_timeout_seconds
        self._demo_recheck_delay = demo_recheck_delay
        self._simulated_transient_recovery_delay = simulated_transient_recovery_delay
        self._triage_semaphore = asyncio.Semaphore(triage_concurrency)
        self._policy_cache: dict[tuple[str, str], PolicyVersion] = {}

    def invalidate_policy_cache(self) -> None:
        self._policy_cache.clear()

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{self._id_factory()}"

    def _retry_delay(self, attempt: int) -> timedelta:
        """Bounded exponential backoff with up to 25 percent jitter."""

        base_seconds = max(0.0, self._retry_base_delay.total_seconds())
        max_seconds = max(base_seconds, self._retry_max_delay.total_seconds())
        exponential = min(max_seconds, base_seconds * (2 ** max(0, attempt - 1)))
        jittered = min(max_seconds, exponential * (1 + 0.25 * self._jitter_source()))
        return timedelta(seconds=jittered)

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
            if message.current_state == MessageState.DEFERRED:
                if message.recheck_at and message.recheck_at > self._clock.now():
                    return message
                attempt = message.recheck_attempts + 1
                await self._record_receipt(
                    message,
                    kind=ReceiptKind.RECHECK,
                    status=ReceiptStatus.STARTED,
                    stage="recheck",
                    trace_id=trace,
                    operation_id=self._id("op"),
                    attempt=attempt,
                    details={"scheduled_for": message.recheck_at},
                )
                await self._store.transition_message(
                    run_id=run_id,
                    message_id=message_id,
                    new_state=MessageState.RECHECKING,
                    triggering_event="scheduled_recheck_started",
                    trace_id=trace,
                    recheck_attempts=attempt,
                )
                continue
            if message.current_state == MessageState.RECHECKING:
                terminal = await self._recheck_stage(message, trace)
                if terminal:
                    return await self._store.get_message(run_id, message_id)
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
            approved_currencies=policy.definition.approved_currencies,
            effective_replay_cap=str(policy.definition.effective_replay_cap),
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
            async with self._triage_semaphore:
                raw = await asyncio.wait_for(
                    self._model.triage(request), timeout=self._model_timeout_seconds
                )
            triage = Triage.model_validate(raw)
            if triage.contains_injection_attempt or payload_contains_injection(
                message.original_payload
            ):
                clause = policy.definition.clause("RP-D-1")
                triage = Triage(
                    failure_class=FailureClass.PROMPT_INJECTION,
                    confidence=triage.confidence,
                    runbook_clause="RP-D-1",
                    runbook_page=clause.page if clause is not None else 2,
                    evidence=list(
                        dict.fromkeys(
                            [
                                *triage.evidence,
                                "Deterministic guard detected an instruction in untrusted payload data.",
                            ]
                        )
                    )[:12],
                    contains_injection_attempt=True,
                    proposed_repairs=[],
                    recommended_action=RecommendedAction.QUARANTINE,
                    decision_summary=(
                        "Refuse the embedded instruction and quarantine the message; "
                        "payload text cannot grant replay authority."
                    ),
                    proposed_fix=None,
                )
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
                next_attempt_at=self._clock.now() + self._retry_delay(attempt),
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
                "proposed_fix": triage.proposed_fix,
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
                proposed_fix=triage.proposed_fix,
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
        if message.failure_class == FailureClass.TRANSIENT_DOWNSTREAM:
            action = authorize_policy_action(
                triage,
                policy,
                failure_class=FailureClass.TRANSIENT_DOWNSTREAM,
                action=RecommendedAction.DEFER,
            )
            errors = list(action.error_codes)
            if not is_transient_inventory_503(message.original_payload):
                errors.append("TRANSIENT_SIGNATURE_MISMATCH")
            if errors:
                await self._store.transition_message(
                    run_id=message.run_id,
                    message_id=message.message_id,
                    new_state=MessageState.ESCALATED,
                    triggering_event="deferral_authorization_denied",
                    trace_id=trace_id,
                    error_code=errors[0],
                    withheld_reason="The transient failure did not match deterministic policy evidence.",
                    severity="high",
                    sla="15 minutes",
                    escalation_recipient="synthetic-ops@retrypermit.example",
                )
                return True
            if message.transient_recovered:
                original = message.original_payload
                await self._store.set_repaired_payload(
                    message.run_id,
                    message.message_id,
                    original,
                    payload_hash(original),
                    [],
                )
                await self._store.transition_message(
                    run_id=message.run_id,
                    message_id=message.message_id,
                    new_state=MessageState.REPLAYING,
                    triggering_event="transient_dependency_recovered",
                    trace_id=trace_id,
                    decision_summary="Inventory recovered; replay the unchanged validated payload.",
                    runbook_clause=message.runbook_clause,
                )
                return False
            recheck_at = self._clock.now() + self._recheck_delay(policy)
            reason = "Simulated inventory dependency returned HTTP 503."
            await self._record_receipt(
                message,
                kind=ReceiptKind.DEFERRAL,
                status=ReceiptStatus.CONFIRMED,
                stage="deferral",
                trace_id=trace_id,
                operation_id=self._id("op"),
                details={
                    "reason": reason,
                    "recheck_at": recheck_at,
                    "attempt_count": message.recheck_attempts,
                    "policy_clause": message.runbook_clause,
                    "demo_interval_override_seconds": (
                        self._demo_recheck_delay.total_seconds()
                        if self._demo_recheck_delay is not None
                        else None
                    ),
                },
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.DEFERRED,
                triggering_event="transient_dependency_deferred",
                trace_id=trace_id,
                decision_summary=message.decision_summary,
                runbook_clause=message.runbook_clause,
                next_attempt_at=recheck_at,
                deferral_reason=reason,
                recheck_at=recheck_at,
            )
            return True
        if message.failure_class == FailureClass.INVALID_DATA:
            action = authorize_policy_action(
                triage,
                policy,
                failure_class=FailureClass.INVALID_DATA,
                action=RecommendedAction.ESCALATE,
            )
            resolution = invalid_data_resolution(message.original_payload, policy)
            errors = list(action.error_codes)
            if resolution is None:
                errors.append("INVALID_DATA_SIGNATURE_MISMATCH")
                reason = "The proposal did not match a deterministic invalid-data rule."
                proposed_fix = (
                    message.proposed_fix or "Investigate and resubmit corrected data."
                )
            else:
                reason, proposed_fix = resolution
            await self._record_receipt(
                message,
                kind=ReceiptKind.ESCALATION,
                status=ReceiptStatus.CONFIRMED,
                stage="withheld_escalation",
                trace_id=trace_id,
                operation_id=self._id("op"),
                error_code=errors[0] if errors else None,
                details={
                    "proposed_fix": proposed_fix,
                    "withheld_reason": reason,
                    "severity": "medium",
                    "sla": "4 business hours",
                    "recipient": "synthetic-order-ops@retrypermit.example",
                },
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.ESCALATED,
                triggering_event="invalid_data_withheld",
                trace_id=trace_id,
                error_code=errors[0] if errors else "BUSINESS_CONFIRMATION_REQUIRED",
                proposed_fix=proposed_fix,
                withheld_reason=reason,
                severity="medium",
                sla="4 business hours",
                escalation_recipient="synthetic-order-ops@retrypermit.example",
            )
            return True
        if (
            message.failure_class == FailureClass.PROMPT_INJECTION
            or payload_contains_injection(message.original_payload)
        ):
            action = authorize_policy_action(
                triage,
                policy,
                failure_class=FailureClass.PROMPT_INJECTION,
                action=RecommendedAction.QUARANTINE,
            )
            reason = (
                "Untrusted payload text attempted to override policy and request replay. "
                "The instruction was treated as inert data."
            )
            await self._record_receipt(
                message,
                kind=ReceiptKind.REFUSAL,
                status=ReceiptStatus.CONFIRMED,
                stage="quarantine",
                trace_id=trace_id,
                operation_id=self._id("op"),
                error_code=(action.error_codes[0] if action.error_codes else None),
                details={
                    "refusal": reason,
                    "deterministic_pattern_detected": payload_contains_injection(
                        message.original_payload
                    ),
                    "model_flagged": message.contains_injection_attempt,
                    "downstream_effect_permitted": False,
                },
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.QUARANTINED,
                triggering_event="untrusted_instruction_refused",
                trace_id=trace_id,
                error_code="UNTRUSTED_INSTRUCTION_DETECTED",
                quarantine_reason=reason,
            )
            return True
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

    def _recheck_delay(self, policy: PolicyVersion) -> timedelta:
        if self._demo_recheck_delay is not None:
            return self._demo_recheck_delay
        return timedelta(seconds=policy.definition.transient_recheck_seconds)

    async def _recheck_stage(self, message: MessageRecord, trace_id: str) -> bool:
        policy = await self._active_policy_for(message)
        run = await self._store.get_run(message.run_id)
        recovery_anchor = run.started_at or message.created_at
        recovered = self._clock.now() >= (
            recovery_anchor + self._simulated_transient_recovery_delay
        )
        if recovered:
            await self._record_receipt(
                message,
                kind=ReceiptKind.RECHECK,
                status=ReceiptStatus.CONFIRMED,
                stage="recheck",
                trace_id=trace_id,
                operation_id=self._id("op"),
                attempt=message.recheck_attempts,
                details={"dependency": "inventory", "available": True},
            )
            await self._store.transition_message(
                run_id=message.run_id,
                message_id=message.message_id,
                new_state=MessageState.PLANNED,
                triggering_event="transient_dependency_available",
                trace_id=trace_id,
                transient_recovered=True,
            )
            return False
        recheck_at = self._clock.now() + self._recheck_delay(policy)
        await self._record_receipt(
            message,
            kind=ReceiptKind.RECHECK,
            status=ReceiptStatus.FAILED,
            stage="recheck",
            trace_id=trace_id,
            operation_id=self._id("op"),
            attempt=message.recheck_attempts,
            error_code="INVENTORY_STILL_UNAVAILABLE",
            retryable=True,
            details={
                "dependency": "inventory",
                "available": False,
                "next_recheck_at": recheck_at,
            },
        )
        await self._store.transition_message(
            run_id=message.run_id,
            message_id=message.message_id,
            new_state=MessageState.DEFERRED,
            triggering_event="transient_dependency_still_unavailable",
            trace_id=trace_id,
            error_code="INVENTORY_STILL_UNAVAILABLE",
            next_attempt_at=recheck_at,
            deferral_reason="Simulated inventory dependency is still unavailable.",
            recheck_at=recheck_at,
        )
        return True

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
            submission = await asyncio.wait_for(
                self._downstream.submit(
                    run_id=message.run_id,
                    message_id=message.message_id,
                    payload=message.repaired_payload,
                    payload_hash=message.repaired_payload_hash,
                    idempotency_key=key,
                    owner=worker_id,
                ),
                timeout=self._downstream_timeout_seconds,
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
        except Exception as exc:
            if isinstance(exc, AmbiguousDownstreamOutcomeError):
                error_code = exc.code
            elif isinstance(exc, TimeoutError):
                error_code = "DOWNSTREAM_TIMEOUT_AFTER_EFFECT_UNKNOWN"
            else:
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
                next_attempt = self._clock.now() + self._retry_delay(
                    entry.attempt_count
                )
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
        # Cache immutable definitions, never authority. Another worker may have
        # activated a different policy without invalidating this process's cache.
        active_version = await self._store.get_active_policy_version(message.tenant_id)
        if active_version != message.active_policy_version:
            self.invalidate_policy_cache()
            raise RunConflictError(
                "the run's policy is no longer active; deterministic replanning is required"
            )
        cache_key = (message.tenant_id, message.active_policy_version)
        cached = self._policy_cache.get(cache_key)
        if cached is not None:
            return cached
        policy = await self._store.get_active_policy(message.tenant_id)
        if policy.version != message.active_policy_version:
            raise RunConflictError(
                "the run's policy is no longer active; deterministic replanning is required"
            )
        self._policy_cache[cache_key] = policy
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
            proposed_fix=message.proposed_fix,
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
