import asyncio
from datetime import timedelta

from retrypermit.adapters.downstream.store_backed import StoreBackedDownstream
from retrypermit.adapters.models.fake import DeterministicFakeModelProvider
from retrypermit.adapters.policies.seeded_json import (
    SeededPolicyProvider,
    load_seed_messages,
)
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.application.demo_service import DemoService
from retrypermit.application.orchestrator import RetryPermitOrchestrator
from retrypermit.domain.enums import (
    MessageState,
    ReceiptKind,
    ReceiptStatus,
    ReplayStatus,
    RunStatus,
)


def test_phase3_initial_pass_routes_all_four_classes_without_unsafe_effects() -> None:
    async def scenario() -> None:
        store = MemoryStore()
        demo = DemoService(store, SeededPolicyProvider())
        await demo.bootstrap()
        run = await demo.reset(run_id="six-class-a")
        provider = DeterministicFakeModelProvider()
        messages = await RetryPermitOrchestrator(
            store, provider, StoreBackedDownstream(store)
        ).run_all(run.run_id)

        assert provider.call_count == 12
        assert (
            sum(message.current_state == MessageState.REPLAYED for message in messages)
            == 6
        )
        assert (
            sum(message.current_state == MessageState.ESCALATED for message in messages)
            == 2
        )
        assert (
            sum(message.current_state == MessageState.DEFERRED for message in messages)
            == 3
        )
        assert (
            sum(
                message.current_state == MessageState.QUARANTINED
                for message in messages
            )
            == 1
        )
        assert len(await store.list_downstream_effects(run.run_id)) == 6
        assert (await store.get_run(run.run_id)).status == RunStatus.RUNNING
        for message in messages:
            if message.current_state == MessageState.ESCALATED:
                assert message.last_error_code == "BUSINESS_CONFIRMATION_REQUIRED"
                continue
            if message.current_state in {
                MessageState.DEFERRED,
                MessageState.QUARANTINED,
            }:
                continue
            ledger = await store.get_replay_ledger(
                run.run_id, message.idempotency_key or ""
            )
            assert ledger.status == ReplayStatus.CONFIRMED
            receipts = await store.list_receipts(run.run_id, message.message_id)
            assert any(item.kind == ReceiptKind.TRIAGE_ATTEMPT for item in receipts)
            assert any(item.kind == ReceiptKind.TRIAGE_RESULT for item in receipts)
            assert any(item.kind == ReceiptKind.REPLAY_ATTEMPT for item in receipts)
            assert any(item.kind == ReceiptKind.REPLAY_RESULT for item in receipts)

    asyncio.run(scenario())


def test_repeated_orchestration_does_not_create_another_effect() -> None:
    async def scenario() -> None:
        store = MemoryStore()
        demo = DemoService(store, SeededPolicyProvider())
        await demo.bootstrap()
        run = await demo.reset(run_id="repeat")
        provider = DeterministicFakeModelProvider()
        orchestrator = RetryPermitOrchestrator(
            store, provider, StoreBackedDownstream(store)
        )
        message_id = (await store.list_messages(run.run_id))[0].message_id
        first = await orchestrator.run_message(run.run_id, message_id)
        second = await orchestrator.run_message(run.run_id, message_id)
        assert first.downstream_reference == second.downstream_reference
        assert len(await store.list_downstream_effects(run.run_id)) == 1
        assert provider.call_count == 1

    asyncio.run(scenario())


def test_low_confidence_proposal_escalates_without_an_effect() -> None:
    class LowConfidenceProvider(DeterministicFakeModelProvider):
        async def triage(self, request):
            proposal = await super().triage(request)
            return proposal.model_copy(update={"confidence": 0.69})

    async def scenario() -> None:
        store = MemoryStore()
        demo = DemoService(store, SeededPolicyProvider())
        await demo.bootstrap()
        run = await demo.reset(run_id="low-confidence")
        message_id = (await store.list_messages(run.run_id))[0].message_id
        message = await RetryPermitOrchestrator(
            store, LowConfidenceProvider(), StoreBackedDownstream(store)
        ).run_message(run.run_id, message_id)
        assert message.current_state == MessageState.ESCALATED
        assert message.last_error_code == "LOW_CONFIDENCE"
        assert await store.list_downstream_effects(run.run_id) == []

    asyncio.run(scenario())


def test_triage_timeouts_stop_at_the_policy_retry_limit() -> None:
    class TimeoutProvider:
        async def triage(self, request):
            raise TimeoutError

    async def scenario() -> None:
        store = MemoryStore()
        demo = DemoService(store, SeededPolicyProvider())
        await demo.bootstrap()
        policy = await store.get_active_policy("synthetic-demo")
        seed = load_seed_messages()[0]
        run = await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=[seed],
            run_id="bounded-triage-timeout",
        )
        orchestrator = RetryPermitOrchestrator(
            store,
            TimeoutProvider(),
            StoreBackedDownstream(store),
            retry_base_delay=timedelta(0),
            retry_max_delay=timedelta(0),
        )

        for expected_attempt in range(1, policy.definition.retry_limit + 1):
            message = await orchestrator.run_message(run.run_id, seed.source_message_id)
            expected_state = (
                MessageState.FAILED_FINAL
                if expected_attempt == policy.definition.retry_limit
                else MessageState.FAILED_RETRYABLE
            )
            assert message.current_state == expected_state

        receipts = await store.list_receipts(run.run_id, seed.source_message_id)
        attempts = [
            receipt
            for receipt in receipts
            if receipt.kind == ReceiptKind.TRIAGE_ATTEMPT
        ]
        results = [
            receipt for receipt in receipts if receipt.kind == ReceiptKind.TRIAGE_RESULT
        ]
        assert [receipt.attempt for receipt in attempts] == [1, 2, 3]
        assert [receipt.attempt for receipt in results] == [1, 2, 3]
        assert results[-1].status == ReceiptStatus.FAILED
        assert not results[-1].retryable
        assert await store.list_downstream_effects(run.run_id) == []
        assert (await store.get_run(run.run_id)).status == RunStatus.COMPLETE

    asyncio.run(scenario())
