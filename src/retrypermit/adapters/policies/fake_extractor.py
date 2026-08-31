from __future__ import annotations

from pathlib import Path

from retrypermit.config import PROJECT_ROOT
from retrypermit.domain.policy import PolicyDefinition


class DeterministicPolicyExtractor:
    """Local/test extractor; explicitly not evidence of a Gemini PDF call."""

    def __init__(
        self,
        definition_path: Path | None = None,
    ) -> None:
        self._definition_path = definition_path

    async def extract(self, pdf_bytes: bytes) -> PolicyDefinition:
        if not pdf_bytes.startswith(b"%PDF-"):
            raise ValueError("policy source is not a PDF")
        definition_path = self._definition_path
        if definition_path is None:
            # Match exact bundled bytes, never a caller-controlled filename or
            # instructions inside an arbitrary upload. This is still a fixture,
            # not a PDF parser or evidence of a real Gemini extraction.
            fixture_dir = PROJECT_ROOT / "fixtures"
            v3_pdf = fixture_dir / "dlq-runbook-v3.pdf"
            version = (
                "runbook-v3"
                if v3_pdf.exists() and pdf_bytes == v3_pdf.read_bytes()
                else "runbook-v2"
            )
            definition_path = fixture_dir / "policies" / f"{version}.json"
        return PolicyDefinition.model_validate_json(
            definition_path.read_text(encoding="utf-8")
        )
