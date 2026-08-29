from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import ReplayStatus


class EffectivelyOnceProof(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    message_id: str
    recorded_pubsub_delivery_count: int = Field(ge=0)
    execution_attempt_count: int = Field(ge=0)
    downstream_request_count: int = Field(ge=0)
    unique_downstream_effect_count: int = Field(ge=0)
    final_downstream_reference: str | None = None
    ledger_status: ReplayStatus
    live_query: bool = True
    delivery_count_label: str = "deliveries recorded by RetryPermit"
