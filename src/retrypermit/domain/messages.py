from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import (
    FailedStage,
    FailureClass,
    MessageState,
    RecommendedAction,
)
from retrypermit.domain.triage import Repair


class MessageRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    tenant_id: str
    message_id: str
    transport_message_id: str | None = None
    source_topic: str
    current_state: MessageState = MessageState.RECEIVED
    original_payload: dict[str, Any]
    repaired_payload: dict[str, Any] | None = None
    original_payload_hash: str
    repaired_payload_hash: str | None = None
    failure_class: FailureClass | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    amount: Decimal
    currency: str
    attempts: int = Field(default=0, ge=0)
    failed_stage: FailedStage | None = None
    runbook_clause: str | None = None
    runbook_page: int | None = None
    active_policy_version: str
    decision_summary: str | None = None
    evidence: list[str] = Field(default_factory=list)
    contains_injection_attempt: bool = False
    recommended_action: RecommendedAction | None = None
    proposed_repairs: list[Repair] = Field(default_factory=list)
    applied_repairs: list[Repair] = Field(default_factory=list)
    last_error_code: str | None = None
    idempotency_key: str | None = None
    downstream_reference: str | None = None
    next_attempt_at: datetime | None = None
    next_transition_sequence: int = Field(default=1, ge=1)
    created_at: datetime
    updated_at: datetime


class SeedMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_message_id: str
    source_topic: str = "orders.dlq"
    payload: dict[str, Any]


class MessageAnalysisUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_class: FailureClass
    confidence: float = Field(ge=0.0, le=1.0)
    runbook_clause: str
    runbook_page: int
    decision_summary: str
    evidence: list[str]
    contains_injection_attempt: bool
    recommended_action: RecommendedAction
    proposed_repairs: list[Repair]
