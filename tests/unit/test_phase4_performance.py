import asyncio

from retrypermit.adapters.downstream.store_backed import StoreBackedDownstream
from retrypermit.adapters.models.fake import DeterministicFakeModelProvider
from retrypermit.adapters.policies.seeded_json import SeededPolicyProvider
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.application.demo_service import DemoService
from retrypermit.application.orchestrator import RetryPermitOrchestrator


class ObservedFakeProvider(DeterministicFakeModelProvider):
    def __init__(self) -> None:
        super().__init__()
        self.active = 0
        self.maximum_active = 0

    async def triage(self, request):
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            await asyncio.sleep(0.01)
            return await super().triage(request)
        finally:
            self.active -= 1


def test_triage_is_concurrent_but_bounded_at_four() -> None:
    async def scenario() -> None:
        store = MemoryStore()
        demo = DemoService(store, SeededPolicyProvider())
        await demo.bootstrap()
        run = await demo.reset(run_id="phase4-concurrency")
        provider = ObservedFakeProvider()
        orchestrator = RetryPermitOrchestrator(
            store,
            provider,
            StoreBackedDownstream(store),
            triage_concurrency=4,
        )

        await orchestrator.run_all(run.run_id)

        assert provider.call_count == 12
        assert provider.maximum_active == 4

    asyncio.run(scenario())


def test_active_policy_is_cached_and_explicitly_invalidated() -> None:
    async def scenario() -> None:
        store = MemoryStore()
        demo = DemoService(store, SeededPolicyProvider())
        await demo.bootstrap()
        run = await demo.reset(run_id="phase4-policy-cache")
        original_get_active_policy = store.get_active_policy
        calls = 0

        async def counted_get_active_policy(tenant_id: str):
            nonlocal calls
            calls += 1
            return await original_get_active_policy(tenant_id)

        store.get_active_policy = counted_get_active_policy  # type: ignore[method-assign]
        orchestrator = RetryPermitOrchestrator(
            store,
            DeterministicFakeModelProvider(),
            StoreBackedDownstream(store),
        )

        await orchestrator.run_message(run.run_id, "synthetic-order-001")
        await orchestrator.run_message(run.run_id, "synthetic-order-002")
        assert calls == 1

        orchestrator.invalidate_policy_cache()
        await orchestrator.run_message(run.run_id, "synthetic-order-003")
        assert calls == 2

    asyncio.run(scenario())
