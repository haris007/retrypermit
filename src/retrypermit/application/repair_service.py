from __future__ import annotations

import copy
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from retrypermit.domain.enums import RepairKind
from retrypermit.domain.errors import RepairRejectedError
from retrypermit.domain.hashing import payload_hash
from retrypermit.domain.policy import PolicyVersion
from retrypermit.domain.triage import Repair


class RepairResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    original_payload_hash: str
    repaired_payload_hash: str
    repaired_payload: dict[str, Any]
    applied_repairs: list[Repair]
    rejected_repairs: list[Repair] = Field(default_factory=list)


def repair_payload(
    payload: dict[str, Any], repairs: list[Repair], policy: PolicyVersion
) -> RepairResult:
    allowed = {rule.signature() for rule in policy.definition.migration_rules}
    unauthorized = [repair for repair in repairs if repair.signature() not in allowed]
    if unauthorized:
        raise RepairRejectedError("one or more transforms are not allowlisted")
    if len({repair.signature() for repair in repairs}) != len(repairs):
        raise RepairRejectedError("duplicate transforms are not accepted")

    result = copy.deepcopy(payload)
    for repair in repairs:
        if repair.kind == RepairKind.RENAME_FIELD:
            assert repair.source_field is not None
            if repair.source_field not in result:
                raise RepairRejectedError(
                    f"source field {repair.source_field} is absent"
                )
            if repair.target_field in result:
                raise RepairRejectedError(
                    f"target field {repair.target_field} already exists"
                )
            result[repair.target_field] = result.pop(repair.source_field)
        elif repair.kind == RepairKind.COERCE_TYPE:
            assert repair.source_field is not None
            if repair.source_field not in result:
                raise RepairRejectedError(
                    f"source field {repair.source_field} is absent"
                )
            value = result[repair.source_field]
            if repair.target_type == "string_decimal_2":
                if isinstance(value, bool):
                    raise RepairRejectedError("boolean cannot be coerced to an amount")
                try:
                    decimal_value = Decimal(str(value))
                except (InvalidOperation, ValueError) as exc:
                    raise RepairRejectedError(
                        "amount is not decimal-compatible"
                    ) from exc
                if not decimal_value.is_finite():
                    raise RepairRejectedError("amount must be finite")
                coerced: Any = format(
                    decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                    ".2f",
                )
            elif repair.target_type == "string":
                coerced = str(value)
            elif repair.target_type == "integer":
                if isinstance(value, bool):
                    raise RepairRejectedError("boolean cannot be coerced to integer")
                coerced = int(value)
            else:
                raise RepairRejectedError(
                    f"unsupported deterministic target type {repair.target_type}"
                )
            if (
                repair.target_field != repair.source_field
                and repair.target_field in result
            ):
                raise RepairRejectedError(
                    f"target field {repair.target_field} already exists"
                )
            if repair.target_field != repair.source_field:
                result.pop(repair.source_field)
            result[repair.target_field] = coerced
        elif repair.kind == RepairKind.SET_DEFAULT:
            result.setdefault(repair.target_field, repair.default_value)
        else:  # pragma: no cover - enum validation makes this unreachable
            raise RepairRejectedError("unsupported transform kind")

    _validate_v4_payload(result)
    return RepairResult(
        original_payload_hash=payload_hash(payload),
        repaired_payload_hash=payload_hash(result),
        repaired_payload=result,
        applied_repairs=repairs,
    )


def _validate_v4_payload(payload: dict[str, Any]) -> None:
    required = ("order_id", "customerId", "amount", "currency", "products")
    if any(field not in payload for field in required):
        raise RepairRejectedError("repaired payload is missing a required v4 field")
    if "customer_id" in payload:
        raise RepairRejectedError("legacy customer_id remains after repair")
    if not isinstance(payload["customerId"], str) or not payload["customerId"]:
        raise RepairRejectedError("customerId must be a nonempty string")
    amount = payload["amount"]
    if not isinstance(amount, str) or len(amount.rpartition(".")[2]) != 2:
        raise RepairRejectedError("amount must be a two-decimal string")
    try:
        parsed = Decimal(amount)
    except InvalidOperation as exc:
        raise RepairRejectedError("amount is not a valid decimal string") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise RepairRejectedError("amount must be finite and positive")
    if not isinstance(payload["products"], list) or not payload["products"]:
        raise RepairRejectedError("products must be a nonempty list")
