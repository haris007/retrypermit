from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from pydantic import TypeAdapter

from retrypermit.config import PROJECT_ROOT
from retrypermit.domain.enums import PolicyStatus
from retrypermit.domain.messages import SeedMessage
from retrypermit.domain.policy import (
    PolicyDefinition,
    PolicyVersion,
    policy_definition_hash,
)

DEFAULT_POLICY_PATH = PROJECT_ROOT / "fixtures" / "policies" / "runbook-v1.json"
DEFAULT_MESSAGES_PATH = PROJECT_ROOT / "fixtures" / "messages" / "class-a.json"


class SeededPolicyProvider:
    """Loads the checked-in, validated, explicitly preapproved Phase 1 policy."""

    def __init__(
        self,
        path: str | Path = DEFAULT_POLICY_PATH,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def load(self) -> PolicyVersion:
        source = self._path.read_bytes()
        raw = json.loads(source)
        definition = PolicyDefinition.model_validate(raw)
        now = self._clock()
        return PolicyVersion(
            definition=definition,
            source_kind="seeded_json",
            source_hash=hashlib.sha256(source).hexdigest(),
            extracted_policy_hash=policy_definition_hash(definition),
            status=PolicyStatus.APPROVED,
            extracted_at=now,
            approved_at=now,
            approved_by="retrypermit-seeded-fixture",
        )


def load_seed_messages(path: str | Path = DEFAULT_MESSAGES_PATH) -> list[SeedMessage]:
    raw = json.loads(Path(path).read_bytes())
    return TypeAdapter(list[SeedMessage]).validate_python(raw)
