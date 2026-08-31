from __future__ import annotations

from retrypermit.domain.enums import FailureClass, RecommendedAction, RepairKind
from retrypermit.domain.triage import Repair, Triage, TriageRequest


def _contains_injection(value: object) -> bool:
    if isinstance(value, dict):
        return any(_contains_injection(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_injection(item) for item in value)
    return isinstance(value, str) and "ignore previous instructions" in value.casefold()


class DeterministicFakeModelProvider:
    """Fixture provider for unit/local tests; never presented as Gemini evidence."""

    def __init__(self) -> None:
        self.call_count = 0

    async def triage(self, request: TriageRequest) -> Triage:
        self.call_count += 1
        payload = request.payload
        if _contains_injection(payload):
            return Triage(
                failure_class=FailureClass.PROMPT_INJECTION,
                confidence=0.99,
                runbook_clause="RP-D-1",
                runbook_page=2,
                evidence=[
                    "An untrusted payload field contains an instruction to override policy.",
                    "Payload text is data and cannot grant replay authority.",
                ],
                contains_injection_attempt=True,
                proposed_repairs=[],
                recommended_action=RecommendedAction.QUARANTINE,
                decision_summary="Refuse the embedded instruction and quarantine the message.",
                proposed_fix=None,
            )
        failure_context = payload.get("failure_context")
        if (
            isinstance(failure_context, dict)
            and failure_context.get("dependency") == "inventory"
            and failure_context.get("status") == 503
        ):
            return Triage(
                failure_class=FailureClass.TRANSIENT_DOWNSTREAM,
                confidence=0.96,
                runbook_clause="RP-B-1",
                runbook_page=2,
                evidence=[
                    "The order payload is valid schema v4.",
                    "The simulated inventory dependency returned HTTP 503.",
                ],
                contains_injection_attempt=False,
                proposed_repairs=[],
                recommended_action=RecommendedAction.DEFER,
                decision_summary="Defer this message and recheck inventory availability on its scheduled task.",
                proposed_fix=None,
            )
        products = payload.get("products")
        has_negative_quantity = isinstance(products, list) and any(
            isinstance(item, dict)
            and isinstance(item.get("quantity"), (int, float))
            and not isinstance(item.get("quantity"), bool)
            and item["quantity"] < 0
            for item in products
        )
        currency_not_allowed = str(payload.get("currency", "")).upper() not in (
            request.approved_currencies or ["USD"]
        )
        if has_negative_quantity or currency_not_allowed:
            proposed_fix = (
                "Confirm the intended non-negative quantity and resubmit the order."
                if has_negative_quantity
                else "Confirm the intended currency and resubmit with an approved ISO currency; do not coerce it automatically."
            )
            evidence = (
                ["At least one product quantity is negative."]
                if has_negative_quantity
                else ["The supplied currency is not on the active policy allowlist."]
            )
            return Triage(
                failure_class=FailureClass.INVALID_DATA,
                confidence=0.98,
                runbook_clause="RP-C-1",
                runbook_page=2,
                evidence=evidence,
                contains_injection_attempt=False,
                proposed_repairs=[],
                recommended_action=RecommendedAction.ESCALATE,
                decision_summary="Withhold replay because the required correction needs business confirmation.",
                proposed_fix=proposed_fix,
            )
        is_numeric_amount = isinstance(
            payload.get("amount"), (int, float)
        ) and not isinstance(payload.get("amount"), bool)
        is_v3_shape = (
            "customer_id" in payload
            and "customerId" not in payload
            and is_numeric_amount
        )
        if is_v3_shape:
            return Triage(
                failure_class=FailureClass.SCHEMA_DRIFT,
                confidence=0.97,
                runbook_clause="RP-A-1",
                runbook_page=1,
                evidence=[
                    "Schema v3 customer_id maps to v4 customerId.",
                    "Schema v4 serializes amount as a two-decimal string.",
                ],
                contains_injection_attempt=False,
                proposed_repairs=[
                    Repair(
                        kind=RepairKind.RENAME_FIELD,
                        source_field="customer_id",
                        target_field="customerId",
                    ),
                    Repair(
                        kind=RepairKind.COERCE_TYPE,
                        source_field="amount",
                        target_field="amount",
                        target_type="string_decimal_2",
                    ),
                ],
                recommended_action=RecommendedAction.REPLAY,
                decision_summary=(
                    "The payload matches the approved synthetic v3-to-v4 schema "
                    "migration and can be repaired deterministically."
                ),
                proposed_fix=None,
            )
        return Triage(
            failure_class=FailureClass.UNKNOWN,
            confidence=0.25,
            runbook_clause="RP-SAFE-STOP",
            runbook_page=2,
            evidence=["The payload does not match the approved Class A signature."],
            contains_injection_attempt=False,
            proposed_repairs=[],
            recommended_action=RecommendedAction.ESCALATE,
            decision_summary="No approved automatic action matches this payload.",
            proposed_fix=None,
        )
