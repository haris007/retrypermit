from __future__ import annotations

from retrypermit.domain.enums import FailureClass, RecommendedAction, RepairKind
from retrypermit.domain.triage import Repair, Triage, TriageRequest


class DeterministicFakeModelProvider:
    """Fixture provider for unit/local tests; never presented as Gemini evidence."""

    def __init__(self) -> None:
        self.call_count = 0

    async def triage(self, request: TriageRequest) -> Triage:
        self.call_count += 1
        payload = request.payload
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
        )
