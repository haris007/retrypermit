import asyncio
import hashlib
from datetime import UTC, datetime

from retrypermit.domain.enums import PolicyStatus
from retrypermit.domain.policy import (
    PolicyDefinition,
    PolicyVersion,
    policy_definition_hash,
)
from retrypermit.domain.errors import PolicyNotActiveError, PolicyValidationError
from retrypermit.ports.policy_extractor import PolicyExtractor
from retrypermit.ports.policy_provider import PolicyProvider
from retrypermit.ports.store import Store


class PolicyService:
    def __init__(
        self,
        store: Store,
        provider: PolicyProvider,
        extractor: PolicyExtractor | None = None,
        *,
        extraction_timeout_seconds: float = 45.0,
    ) -> None:
        self._store = store
        self._provider = provider
        self._extractor = extractor
        self._extraction_timeout_seconds = extraction_timeout_seconds

    async def bootstrap(
        self, *, actor: str = "retrypermit-system-bootstrap"
    ) -> PolicyVersion:
        """Keep the active policy, or install and activate the seed on first boot."""

        try:
            return await self._store.get_active_policy("synthetic-demo")
        except PolicyNotActiveError:
            pass

        candidate = await self._provider.load()
        installed = await self._store.install_policy(candidate)
        return await self._store.activate_policy(
            installed.tenant_id, installed.version, actor
        )

    async def extract_pdf(self, pdf_bytes: bytes) -> PolicyVersion:
        if self._extractor is None:
            raise PolicyValidationError("PDF policy extraction is not configured")
        if len(pdf_bytes) > 5_000_000:
            raise PolicyValidationError("policy PDF exceeds the 5 MB limit")
        if not pdf_bytes.startswith(b"%PDF-"):
            raise PolicyValidationError("policy source must be a valid PDF")
        try:
            extracted = await asyncio.wait_for(
                self._extractor.extract(pdf_bytes),
                timeout=self._extraction_timeout_seconds,
            )
            definition = PolicyDefinition.model_validate(extracted)
        except Exception as exc:
            raise PolicyValidationError(
                "policy PDF did not produce a valid strict policy"
            ) from exc
        if definition.tenant_id != "synthetic-demo":
            raise PolicyValidationError(
                "extracted policy tenant does not match the synthetic demo"
            )
        candidate = PolicyVersion(
            definition=definition,
            source_kind="gemini_pdf_extraction",
            source_hash=hashlib.sha256(pdf_bytes).hexdigest(),
            extracted_policy_hash=policy_definition_hash(definition),
            status=PolicyStatus.PENDING_APPROVAL,
            extracted_at=datetime.now(UTC),
        )
        return await self._store.install_policy(candidate)

    async def approve(self, tenant_id: str, version: str, actor: str) -> PolicyVersion:
        return await self._store.approve_policy(tenant_id, version, actor)

    async def activate(self, tenant_id: str, version: str, actor: str) -> PolicyVersion:
        return await self._store.activate_policy(tenant_id, version, actor)

    async def list(self, tenant_id: str) -> list[PolicyVersion]:
        return await self._store.list_policies(tenant_id)
