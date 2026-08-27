from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import RunStatus


class DemoRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    tenant_id: str
    status: RunStatus
    active: bool
    inject_failure: bool = False
    active_policy_version: str
    seeded_message_count: int = Field(default=0, ge=0)
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None
