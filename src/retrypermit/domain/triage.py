from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from retrypermit.domain.enums import (
    FailureClass,
    RecommendedAction,
    RepairKind,
)


class Repair(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: RepairKind
    source_field: str | None = None
    target_field: str = Field(min_length=1, max_length=128)
    target_type: str | None = Field(default=None, max_length=64)
    default_value: str | int | float | bool | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> "Repair":
        if self.kind in (RepairKind.RENAME_FIELD, RepairKind.COERCE_TYPE):
            if not self.source_field:
                raise ValueError(f"{self.kind.value} requires source_field")
        if self.kind == RepairKind.RENAME_FIELD:
            if self.source_field == self.target_field:
                raise ValueError("rename_field source and target must differ")
            if self.target_type is not None:
                raise ValueError("rename_field does not accept target_type")
        if self.kind == RepairKind.COERCE_TYPE and not self.target_type:
            raise ValueError("coerce_type requires target_type")
        if self.kind == RepairKind.SET_DEFAULT and self.source_field is not None:
            raise ValueError("set_default does not accept source_field")
        return self

    def signature(self) -> tuple[Any, ...]:
        return (
            self.kind.value,
            self.source_field,
            self.target_field,
            self.target_type,
            self.default_value,
        )


class Triage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_class: FailureClass
    confidence: float = Field(ge=0.0, le=1.0)
    runbook_clause: str = Field(min_length=1, max_length=128)
    runbook_page: int = Field(ge=1, le=10_000)
    evidence: list[Annotated[str, Field(min_length=1, max_length=300)]] = Field(
        default_factory=list, max_length=12
    )
    contains_injection_attempt: bool
    proposed_repairs: list[Repair] = Field(default_factory=list, max_length=16)
    recommended_action: RecommendedAction
    decision_summary: str = Field(min_length=1, max_length=1_000)
    proposed_fix: str | None = Field(default=None, max_length=1_000)


class PolicyClauseContext(BaseModel):
    """Validated policy evidence the model may cite, never authority by itself."""

    model_config = ConfigDict(extra="forbid")

    clause_id: str
    page: int = Field(ge=1)
    text: str
    failure_classes: list[FailureClass]
    authorized_actions: list[RecommendedAction]


class TriageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    run_id: str
    message_id: str
    payload: dict[str, Any]
    policy_version: str
    allowed_repairs: list[Repair]
    clause_ids: list[str]
    policy_clauses: list[PolicyClauseContext]
