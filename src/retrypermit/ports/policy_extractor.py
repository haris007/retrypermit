from typing import Protocol

from retrypermit.domain.policy import PolicyDefinition


class PolicyExtractor(Protocol):
    async def extract(self, pdf_bytes: bytes) -> PolicyDefinition: ...
