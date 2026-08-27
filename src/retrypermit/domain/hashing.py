from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def derive_idempotency_key(
    tenant_id: str,
    source_topic: str,
    message_id: str,
    action_version: str,
) -> str:
    """Derive an application-namespaced key; never use a bare message id."""

    namespace = "\x1f".join(
        ("retrypermit", tenant_id, source_topic, message_id, "replay", action_version)
    )
    return hashlib.sha256(namespace.encode("utf-8")).hexdigest()
