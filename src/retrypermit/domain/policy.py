from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from retrypermit.domain.enums import FailureClass, PolicyStatus, RecommendedAction
from retrypermit.domain.hashing import canonical_json_bytes
from retrypermit.domain.triage import Repair

SYSTEM_MAX_AUTOREPLAY_AMOUNT = Decimal("2500.00")


class FailureSignature(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_class: FailureClass
    required_fields: list[str] = Field(min_length=1)
    description: str = Field(min_length=1, max_length=1_000)


class PolicyClause(BaseModel):
    model_config = ConfigDict(extra="forbid")

    clause_id: str = Field(min_length=1, max_length=128)
    page: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=256)
    text: str = Field(min_length=1, max_length=2_000)
    failure_classes: list[FailureClass] = Field(min_length=1)
    authorized_actions: list[RecommendedAction] = Field(min_length=1)
    allowed_repairs: list[Repair] = Field(default_factory=list)


class PolicyDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)
    auto_replay_cap: Decimal = Field(gt=0)
    approved_currencies: list[str] = Field(min_length=1)
    retry_limit: int = Field(ge=0, le=20)
    action_version: str = Field(min_length=1, max_length=128)
    failure_signatures: list[FailureSignature] = Field(min_length=1)
    migration_rules: list[Repair] = Field(min_length=1)
    clauses: list[PolicyClause] = Field(min_length=1)

    @field_validator("approved_currencies")
    @classmethod
    def normalize_currencies(cls, value: list[str]) -> list[str]:
        normalized = [item.upper() for item in value]
        if len(set(normalized)) != len(normalized):
            raise ValueError("approved currencies must be unique")
        if any(len(item) != 3 or not item.isalpha() for item in normalized):
            raise ValueError("currencies must be three-letter alphabetic codes")
        return normalized

    @model_validator(mode="after")
    def validate_uniqueness_and_authority(self) -> "PolicyDefinition":
        clause_ids = [clause.clause_id for clause in self.clauses]
        if len(set(clause_ids)) != len(clause_ids):
            raise ValueError("policy clause identifiers must be unique")
        rule_signatures = [rule.signature() for rule in self.migration_rules]
        if len(set(rule_signatures)) != len(rule_signatures):
            raise ValueError("migration rules must be unique")
        allowed = set(rule_signatures)
        for clause in self.clauses:
            if any(
                repair.signature() not in allowed for repair in clause.allowed_repairs
            ):
                raise ValueError("clause contains a repair absent from migration_rules")
        return self

    @property
    def effective_replay_cap(self) -> Decimal:
        return min(self.auto_replay_cap, SYSTEM_MAX_AUTOREPLAY_AMOUNT)

    def clause(self, clause_id: str) -> PolicyClause | None:
        return next(
            (clause for clause in self.clauses if clause.clause_id == clause_id), None
        )


class PolicyVersion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    definition: PolicyDefinition
    source_kind: str = Field(min_length=1, max_length=64)
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    extracted_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: PolicyStatus
    extracted_at: datetime
    approved_at: datetime | None = None
    approved_by: str | None = None
    activated_at: datetime | None = None
    activated_by: str | None = None

    @model_validator(mode="after")
    def validate_status_metadata(self) -> "PolicyVersion":
        if self.status in (PolicyStatus.APPROVED, PolicyStatus.ACTIVE):
            if self.approved_at is None or not self.approved_by:
                raise ValueError("approved policies require approval metadata")
        if self.status == PolicyStatus.ACTIVE:
            if self.activated_at is None or not self.activated_by:
                raise ValueError("active policies require activation metadata")
        expected_hash = policy_definition_hash(self.definition)
        if self.extracted_policy_hash != expected_hash:
            raise ValueError("extracted policy hash does not match policy definition")
        return self

    @property
    def version(self) -> str:
        return self.definition.version

    @property
    def tenant_id(self) -> str:
        return self.definition.tenant_id


def policy_definition_hash(definition: PolicyDefinition) -> str:
    import hashlib

    return hashlib.sha256(
        canonical_json_bytes(definition.model_dump(mode="json"))
    ).hexdigest()
