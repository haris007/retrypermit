from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import ReplayStatus


class ReplayLedgerEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    message_id: str
    idempotency_key: str
    payload_hash: str
    status: ReplayStatus
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    attempt_count: int = Field(default=0, ge=0)
    downstream_reference: str | None = None
    action_version: str
    last_error_code: str | None = None
    next_attempt_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ReplayLease(BaseModel):
    entry: ReplayLedgerEntry
    acquired: bool
    takeover: bool = False
    already_confirmed: bool = False


class DownstreamEffect(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    message_id: str
    idempotency_key: str
    payload_hash: str
    downstream_reference: str
    created_at: datetime
    updated_at: datetime
    submission_count: int = Field(default=1, ge=1)


class DownstreamSubmission(BaseModel):
    effect: DownstreamEffect
    created: bool
