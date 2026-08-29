from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ErrorBody(BaseModel):
    code: str
    message: str
    trace_id: str
    retryable: bool


class ErrorResponse(BaseModel):
    error: ErrorBody


class PubSubMessage(BaseModel):
    data: str
    message_id: str = Field(alias="messageId")
    attributes: dict[str, str] = Field(default_factory=dict)
    publish_time: str | None = Field(default=None, alias="publishTime")

    model_config = {"populate_by_name": True}


class PubSubEnvelope(BaseModel):
    message: PubSubMessage
    subscription: str
    delivery_attempt: int | None = Field(default=None, alias="deliveryAttempt")

    model_config = {"populate_by_name": True}


class DeliveryPayload(BaseModel):
    """Application payload carried inside an untrusted Pub/Sub data field."""

    run_id: str = Field(min_length=1, max_length=256)
    message_id: str = Field(min_length=1, max_length=256)
    source_topic: str = Field(default="orders.dlq", min_length=1, max_length=256)
    payload: dict[str, Any]


class TaskProcessRequest(BaseModel):
    run_id: str
    inbox_id: str


class TaskRecoverRequest(BaseModel):
    run_id: str
    message_id: str


class RecoveryRequest(BaseModel):
    limit: int = Field(default=100, ge=1, le=500)


class DemoActionResponse(BaseModel):
    run_id: str
    status: str
    message_count: int = 0
    details: dict[str, Any] = Field(default_factory=dict)


class PolicyMutationRequest(BaseModel):
    actor: str = Field(default="retrypermit-demo-admin", min_length=1, max_length=128)


class DownstreamOrderRequest(BaseModel):
    run_id: str
    message_id: str
    payload: dict[str, Any]
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    idempotency_key: str = Field(pattern=r"^[0-9a-f]{64}$")
