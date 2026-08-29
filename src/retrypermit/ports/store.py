from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol, Sequence

from retrypermit.domain.enums import FailedStage, InboxStatus, MessageState
from retrypermit.domain.inbox import DeliveryRegistration, InboxItem, InboxLease
from retrypermit.domain.messages import (
    MessageAnalysisUpdate,
    MessageRecord,
    SeedMessage,
)
from retrypermit.domain.policy import PolicyVersion
from retrypermit.domain.policy_events import PolicyActivationEvent, PolicyApprovalEvent
from retrypermit.domain.receipts import Receipt, Transition, TransitionResult
from retrypermit.domain.replay import (
    DownstreamEffect,
    DownstreamSubmission,
    ReplayLease,
    ReplayLedgerEntry,
)
from retrypermit.domain.runs import DemoRun
from retrypermit.domain.triage import Repair


class Store(Protocol):
    async def install_policy(self, policy: PolicyVersion) -> PolicyVersion: ...

    async def approve_policy(
        self, tenant_id: str, version: str, actor: str
    ) -> PolicyVersion: ...

    async def activate_policy(
        self, tenant_id: str, version: str, actor: str
    ) -> PolicyVersion: ...

    async def get_active_policy(self, tenant_id: str) -> PolicyVersion: ...

    async def list_policies(self, tenant_id: str) -> list[PolicyVersion]: ...

    async def list_policy_approval_events(
        self, tenant_id: str
    ) -> list[PolicyApprovalEvent]: ...

    async def list_policy_activation_events(
        self, tenant_id: str
    ) -> list[PolicyActivationEvent]: ...

    async def reset_run(
        self,
        *,
        tenant_id: str,
        policy_version: str,
        seeds: Sequence[SeedMessage],
        run_id: str | None = None,
    ) -> DemoRun: ...

    async def get_active_run(self, tenant_id: str) -> DemoRun: ...

    async def get_run(self, run_id: str) -> DemoRun: ...

    async def set_run_inject_failure(self, run_id: str, enabled: bool) -> DemoRun: ...

    async def mark_run_running(self, run_id: str) -> DemoRun: ...

    async def get_message(self, run_id: str, message_id: str) -> MessageRecord: ...

    async def list_messages(self, run_id: str) -> list[MessageRecord]: ...

    async def update_analysis(
        self, run_id: str, message_id: str, analysis: MessageAnalysisUpdate
    ) -> MessageRecord: ...

    async def set_repaired_payload(
        self,
        run_id: str,
        message_id: str,
        payload: dict[str, Any],
        repaired_hash: str,
        applied_repairs: list[Repair],
    ) -> MessageRecord: ...

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
        deferral_reason: str | None = None,
        recheck_at: datetime | None = None,
        recheck_attempts: int | None = None,
        transient_recovered: bool | None = None,
        proposed_fix: str | None = None,
        withheld_reason: str | None = None,
        severity: str | None = None,
        sla: str | None = None,
        escalation_recipient: str | None = None,
        quarantine_reason: str | None = None,
    ) -> TransitionResult: ...

    async def list_transitions(
        self, run_id: str, message_id: str
    ) -> list[Transition]: ...

    async def record_receipt(self, receipt: Receipt) -> Receipt: ...

    async def list_receipts(self, run_id: str, message_id: str) -> list[Receipt]: ...

    async def count_recorded_deliveries(self, run_id: str, message_id: str) -> int: ...

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
    ) -> DeliveryRegistration: ...

    async def claim_inbox(
        self, run_id: str, inbox_id: str, owner: str, lease_duration: timedelta
    ) -> InboxLease: ...

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
    ) -> InboxItem: ...

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
    ) -> ReplayLease: ...

    async def start_replay_attempt(
        self, run_id: str, idempotency_key: str, owner: str
    ) -> ReplayLedgerEntry: ...

    async def create_or_get_downstream_effect(
        self,
        *,
        run_id: str,
        message_id: str,
        idempotency_key: str,
        repaired_payload_hash: str,
        owner: str,
    ) -> DownstreamSubmission: ...

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
    ) -> ReplayLedgerEntry: ...

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
    ) -> ReplayLedgerEntry: ...

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
    ) -> ReplayLedgerEntry: ...

    async def get_replay_ledger(
        self, run_id: str, idempotency_key: str
    ) -> ReplayLedgerEntry: ...

    async def get_downstream_effect(
        self, run_id: str, idempotency_key: str
    ) -> DownstreamEffect: ...

    async def list_downstream_effects(self, run_id: str) -> list[DownstreamEffect]: ...

    async def recovery_candidates(
        self, now: datetime
    ) -> tuple[list[InboxItem], list[ReplayLedgerEntry], list[MessageRecord]]: ...
