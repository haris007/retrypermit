from retrypermit.domain.policy import PolicyVersion
from retrypermit.ports.policy_provider import PolicyProvider
from retrypermit.ports.store import Store


class PolicyService:
    def __init__(self, store: Store, provider: PolicyProvider) -> None:
        self._store = store
        self._provider = provider

    async def bootstrap(
        self, *, actor: str = "retrypermit-system-bootstrap"
    ) -> PolicyVersion:
        """Install and transactionally activate the approved seeded policy."""

        candidate = await self._provider.load()
        installed = await self._store.install_policy(candidate)
        return await self._store.activate_policy(
            installed.tenant_id, installed.version, actor
        )
