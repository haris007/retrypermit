from __future__ import annotations

from collections.abc import Sequence

from retrypermit.adapters.policies.seeded_json import load_seed_messages
from retrypermit.domain.messages import SeedMessage
from retrypermit.domain.runs import DemoRun
from retrypermit.ports.policy_provider import PolicyProvider
from retrypermit.ports.store import Store

from .policy_service import PolicyService


class DemoService:
    def __init__(
        self,
        store: Store,
        policy_provider: PolicyProvider,
        *,
        seeds: Sequence[SeedMessage] | None = None,
    ) -> None:
        self._store = store
        self._policy_service = PolicyService(store, policy_provider)
        self._seeds = list(seeds) if seeds is not None else load_seed_messages()

    async def bootstrap(self) -> None:
        await self._policy_service.bootstrap()

    async def reset(self, *, run_id: str | None = None) -> DemoRun:
        policy = await self._store.get_active_policy(self._seeds_tenant_id)
        return await self._store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=self._seeds,
            run_id=run_id,
        )

    async def get_active(self) -> DemoRun:
        return await self._store.get_active_run(self._seeds_tenant_id)

    @property
    def _seeds_tenant_id(self) -> str:
        # Phase 1 fixtures are intentionally a single disclosed synthetic tenant.
        return "synthetic-demo"
