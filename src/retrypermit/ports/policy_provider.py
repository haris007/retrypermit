from typing import Protocol

from retrypermit.domain.policy import PolicyVersion


class PolicyProvider(Protocol):
    async def load(self) -> PolicyVersion: ...
