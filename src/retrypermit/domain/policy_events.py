from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class PolicyApprovalEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    tenant_id: str
    policy_version: str
    actor: str
    created_at: datetime


class PolicyActivationEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    tenant_id: str
    previous_active_version: str | None = None
    new_active_version: str
    actor: str
    created_at: datetime
