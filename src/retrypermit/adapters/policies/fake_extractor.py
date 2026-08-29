from __future__ import annotations

from pathlib import Path

from retrypermit.config import PROJECT_ROOT
from retrypermit.domain.policy import PolicyDefinition


class DeterministicPolicyExtractor:
    """Local/test extractor; explicitly not evidence of a Gemini PDF call."""

    def __init__(
        self,
        definition_path: Path = (
            PROJECT_ROOT / "fixtures" / "policies" / "runbook-v2.json"
        ),
    ) -> None:
        self._definition_path = definition_path

    async def extract(self, pdf_bytes: bytes) -> PolicyDefinition:
        if not pdf_bytes.startswith(b"%PDF-"):
            raise ValueError("policy source is not a PDF")
        return PolicyDefinition.model_validate_json(
            self._definition_path.read_text(encoding="utf-8")
        )
