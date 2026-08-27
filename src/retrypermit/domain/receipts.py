from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import MessageState, ReceiptKind, ReceiptStatus


class Receipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    receipt_id: str
    operation_id: str
    run_id: str
    message_id: str
    kind: ReceiptKind
    status: ReceiptStatus
    stage: str
    attempt: int = Field(default=1, ge=1)
    trace_id: str
    policy_version: str | None = None
    runbook_clause: str | None = None
    payload_hash: str | None = None
    idempotency_key: str | None = None
    downstream_reference: str | None = None
    error_code: str | None = None
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class Transition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: int = Field(ge=1)
    run_id: str
    message_id: str
    previous_state: MessageState
    new_state: MessageState
    timestamp: datetime
    triggering_event: str
    decision_summary: str | None = None
    runbook_clause: str | None = None
    receipt_id: str
    error_code: str | None = None
    trace_id: str


class TransitionResult(BaseModel):
    transition: Transition
    receipt: Receipt
