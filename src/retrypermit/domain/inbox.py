from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import InboxStatus


class DeliveryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    delivery_key: str
    source_message_id: str
    transport_message_id: str
    source_topic: str
    payload_hash: str
    seen_count: int = Field(default=1, ge=1)
    first_seen_at: datetime
    last_seen_at: datetime
    last_trace_id: str


class DeliveryAttempt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attempt_id: str
    run_id: str
    delivery_key: str
    trace_id: str
    received_at: datetime
    duplicate: bool


class InboxItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    inbox_id: str
    delivery_key: str
    message_id: str
    payload_hash: str
    status: InboxStatus
    task_name: str | None = None
    scheduled_at: datetime | None = None
    next_attempt_at: datetime | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    attempt_count: int = Field(default=0, ge=0)
    last_error_code: str | None = None
    created_at: datetime
    updated_at: datetime


class DeliveryRegistration(BaseModel):
    delivery: DeliveryRecord
    attempt: DeliveryAttempt
    inbox: InboxItem
    duplicate: bool


class InboxLease(BaseModel):
    item: InboxItem
    acquired: bool
    takeover: bool = False
