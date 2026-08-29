from __future__ import annotations

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import (
    FailureClass,
    PolicyStatus,
    RecommendedAction,
)
from retrypermit.domain.policy import PolicyVersion
from retrypermit.domain.triage import Triage


class AuthorizationDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorized: bool
    error_codes: list[str] = Field(default_factory=list)
    effective_cap: Decimal
    amount: Decimal | None = None


class PolicyActionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorized: bool
    error_codes: list[str] = Field(default_factory=list)


def authorize_policy_action(
    triage: Triage,
    policy: PolicyVersion,
    *,
    failure_class: FailureClass,
    action: RecommendedAction,
) -> PolicyActionDecision:
    """Validate non-replay authority without treating confidence as permission."""

    errors: list[str] = []
    if policy.status != PolicyStatus.ACTIVE:
        errors.append("POLICY_NOT_ACTIVE")
    if triage.failure_class != failure_class:
        errors.append("FAILURE_CLASS_MISMATCH")
    if triage.recommended_action != action:
        errors.append("ACTION_MISMATCH")
    clause = policy.definition.clause(triage.runbook_clause)
    if clause is None:
        errors.append("UNKNOWN_POLICY_CLAUSE")
    else:
        if clause.page != triage.runbook_page:
            errors.append("POLICY_PAGE_MISMATCH")
        if failure_class not in clause.failure_classes:
            errors.append("CLAUSE_CLASS_MISMATCH")
        if action not in clause.authorized_actions:
            errors.append("CLAUSE_ACTION_MISMATCH")
    return PolicyActionDecision(
        authorized=not errors,
        error_codes=list(dict.fromkeys(errors)),
    )


def payload_contains_injection(value: object) -> bool:
    """Conservative deterministic detector over every untrusted string value."""

    if isinstance(value, dict):
        return any(payload_contains_injection(item) for item in value.values())
    if isinstance(value, list):
        return any(payload_contains_injection(item) for item in value)
    if not isinstance(value, str):
        return False
    normalized = " ".join(value.casefold().split())
    return (
        "ignore previous instructions" in normalized
        or "approve and replay" in normalized
        or "system prompt" in normalized
    )


def is_transient_inventory_503(payload: dict[str, object]) -> bool:
    context = payload.get("failure_context")
    return (
        isinstance(context, dict)
        and str(context.get("dependency", "")).casefold() == "inventory"
        and context.get("status") == 503
    )


def invalid_data_resolution(
    payload: dict[str, object], policy: PolicyVersion
) -> tuple[str, str] | None:
    products = payload.get("products")
    if isinstance(products, list):
        for item in products:
            if isinstance(item, dict):
                quantity = item.get("quantity")
                if isinstance(quantity, (int, float)) and not isinstance(
                    quantity, bool
                ):
                    if quantity < 0:
                        return (
                            "A negative quantity cannot be repaired without business intent.",
                            "Confirm the intended non-negative quantity and resubmit the order.",
                        )
    currency = str(payload.get("currency", "")).upper()
    if currency not in policy.definition.approved_currencies:
        return (
            f"Currency {currency or '(missing)'} is not approved by the active policy.",
            "Confirm the intended currency and resubmit with an approved ISO currency; do not coerce it automatically.",
        )
    return None


def authorize_triage(
    payload: dict[str, object], triage: Triage, policy: PolicyVersion
) -> AuthorizationDecision:
    """Treat the proposal as evidence; recompute every authorization fact."""

    errors: list[str] = []
    definition = policy.definition
    if policy.status != PolicyStatus.ACTIVE:
        errors.append("POLICY_NOT_ACTIVE")
    if triage.confidence < 0.70:
        errors.append("LOW_CONFIDENCE")
    if triage.failure_class != FailureClass.SCHEMA_DRIFT:
        errors.append("UNSUPPORTED_FAILURE_CLASS")
    if triage.recommended_action != RecommendedAction.REPLAY:
        errors.append("REPLAY_NOT_RECOMMENDED")
    if triage.contains_injection_attempt:
        errors.append("UNTRUSTED_INSTRUCTION_DETECTED")

    clause = definition.clause(triage.runbook_clause)
    if clause is None:
        errors.append("UNKNOWN_POLICY_CLAUSE")
    else:
        if clause.page != triage.runbook_page:
            errors.append("POLICY_PAGE_MISMATCH")
        if triage.failure_class not in clause.failure_classes:
            errors.append("CLAUSE_CLASS_MISMATCH")
        if triage.recommended_action not in clause.authorized_actions:
            errors.append("CLAUSE_ACTION_MISMATCH")
        allowed_by_clause = {repair.signature() for repair in clause.allowed_repairs}
        if any(
            repair.signature() not in allowed_by_clause
            for repair in triage.proposed_repairs
        ):
            errors.append("CLAUSE_REPAIR_MISMATCH")

    allowed_globally = {repair.signature() for repair in definition.migration_rules}
    if any(
        repair.signature() not in allowed_globally for repair in triage.proposed_repairs
    ):
        errors.append("TRANSFORM_NOT_ALLOWLISTED")
    if not triage.proposed_repairs:
        errors.append("NO_REPAIR_PROPOSED")

    signature = next(
        (
            item
            for item in definition.failure_signatures
            if item.failure_class == FailureClass.SCHEMA_DRIFT
        ),
        None,
    )
    if signature is None or any(
        field not in payload for field in signature.required_fields
    ):
        errors.append("PAYLOAD_SIGNATURE_MISMATCH")
    amount_value = payload.get("amount")
    if (
        "customer_id" not in payload
        or "customerId" in payload
        or isinstance(amount_value, bool)
        or not isinstance(amount_value, (int, float, Decimal))
    ):
        errors.append("PAYLOAD_SIGNATURE_MISMATCH")

    amount: Decimal | None = None
    try:
        amount = Decimal(str(amount_value))
        if not amount.is_finite() or amount <= 0:
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        errors.append("INVALID_AMOUNT")
    currency = str(payload.get("currency", "")).upper()
    if currency not in definition.approved_currencies:
        errors.append("CURRENCY_NOT_APPROVED")
    if amount is not None and amount > definition.effective_replay_cap:
        errors.append("AMOUNT_OVER_EFFECTIVE_CAP")

    return AuthorizationDecision(
        authorized=not errors,
        error_codes=list(dict.fromkeys(errors)),
        effective_cap=definition.effective_replay_cap,
        amount=amount,
    )
