from typing import Protocol

from retrypermit.domain.triage import Triage, TriageRequest


class ModelProvider(Protocol):
    """A proposal-only boundary. Implementations receive no effectful tools."""

    async def triage(self, request: TriageRequest) -> Triage: ...
