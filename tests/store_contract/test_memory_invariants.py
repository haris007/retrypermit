import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from retrypermit.adapters.policies.seeded_json import (
    SeededPolicyProvider,
    load_seed_messages,
)
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.application.repair_service import repair_payload
from retrypermit.domain.enums import (
    FailedStage,
    InboxStatus,
    MessageState,
    ReceiptKind,
    ReceiptStatus,
    ReplayStatus,
    RunStatus,
)
from retrypermit.domain.errors import (
    InvalidTransitionError,
    LeaseNotOwnedError,
    PayloadHashMismatchError,
    RunConflictError,
)
from retrypermit.domain.hashing import derive_idempotency_key
from retrypermit.domain.receipts import Receipt
from retrypermit.domain.state_machine import ensure_transition


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 26, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.value

    def advance(self, duration: timedelta) -> None:
        self.value += duration


async def _seed_store(clock: MutableClock) -> tuple[MemoryStore, object, object]:
    store = MemoryStore(clock=clock)
    candidate = await SeededPolicyProvider(clock=clock.now).load()
    await store.install_policy(candidate)
    policy = await store.activate_policy(candidate.tenant_id, candidate.version, "test")
    run = await store.reset_run(
        tenant_id=candidate.tenant_id,
        policy_version=candidate.version,
        seeds=load_seed_messages(),
        run_id="contract-run",
    )
    return store, policy, run


async def _prepare_replaying_message(store, policy, run):
    await store.mark_run_running(run.run_id)
    message = (await store.list_messages(run.run_id))[0]
    for state in (
        MessageState.TRIAGING,
        MessageState.TRIAGED,
        MessageState.PLANNED,
        MessageState.REPAIRING,
    ):
        await store.transition_message(
            run_id=run.run_id,
            message_id=message.message_id,
            new_state=state,
            triggering_event="test",
            trace_id="trace",
        )
    repaired = repair_payload(
        message.original_payload, policy.definition.migration_rules, policy
    )
    await store.set_repaired_payload(
        run.run_id,
        message.message_id,
        repaired.repaired_payload,
        repaired.repaired_payload_hash,
        repaired.applied_repairs,
    )
    await store.transition_message(
        run_id=run.run_id,
        message_id=message.message_id,
        new_state=MessageState.REPLAYING,
        triggering_event="test",
        trace_id="trace",
    )
    key = derive_idempotency_key(
        message.tenant_id,
        message.source_topic,
        message.message_id,
        policy.definition.action_version,
    )
    return message, repaired, key


def test_run_reset_isolates_and_preserves_history() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, first = await _seed_store(clock)
        with pytest.raises(RunConflictError):
            await store.reset_run(
                tenant_id=policy.tenant_id,
                policy_version=policy.version,
                seeds=load_seed_messages(),
                run_id=first.run_id,
            )
        assert (await store.get_run(first.run_id)).active
        second = await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=load_seed_messages(),
            run_id="second-run",
        )
        historical = await store.get_run(first.run_id)
        assert not historical.active
        assert historical.status == RunStatus.INACTIVE
        assert second.active
        assert len(await store.list_messages(first.run_id)) == 6
        assert len(await store.list_messages(second.run_id)) == 6

    asyncio.run(scenario())


def test_duplicate_delivery_is_recorded_transactionally() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, _, run = await _seed_store(clock)
        seed = load_seed_messages()[0]

        async def deliver(index: int):
            return await store.register_delivery(
                run_id=run.run_id,
                delivery_key="delivery-key",
                source_message_id=seed.source_message_id,
                transport_message_id="pubsub-message",
                source_topic=seed.source_topic,
                payload=seed.payload,
                trace_id=f"trace-{index}",
            )

        results = await asyncio.gather(*(deliver(index) for index in range(10)))
        assert sorted(item.delivery.seen_count for item in results) == list(
            range(1, 11)
        )
        assert sum(not item.duplicate for item in results) == 1
        with pytest.raises(PayloadHashMismatchError):
            await store.register_delivery(
                run_id=run.run_id,
                delivery_key="delivery-key",
                source_message_id=seed.source_message_id,
                transport_message_id="different-pubsub-message",
                source_topic=seed.source_topic,
                payload=seed.payload,
                trace_id="identity-mismatch",
            )

    asyncio.run(scenario())


def test_non_positive_inbox_and_replay_leases_are_rejected() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, run = await _seed_store(clock)
        seed = load_seed_messages()[0]
        registration = await store.register_delivery(
            run_id=run.run_id,
            delivery_key="invalid-inbox-lease",
            source_message_id=seed.source_message_id,
            transport_message_id="invalid-inbox-lease-transport",
            source_topic=seed.source_topic,
            payload=seed.payload,
            trace_id="lease-validation",
        )
        for invalid_duration in (timedelta(0), timedelta(seconds=-1)):
            with pytest.raises(RunConflictError):
                await store.claim_inbox(
                    run.run_id,
                    registration.inbox.inbox_id,
                    "worker",
                    invalid_duration,
                )

        message, repaired, key = await _prepare_replaying_message(store, policy, run)
        for invalid_duration in (timedelta(0), timedelta(seconds=-1)):
            with pytest.raises(RunConflictError):
                await store.acquire_replay_lease(
                    run_id=run.run_id,
                    message_id=message.message_id,
                    idempotency_key=key,
                    repaired_payload_hash=repaired.repaired_payload_hash,
                    action_version=policy.definition.action_version,
                    owner="worker",
                    lease_duration=invalid_duration,
                )

    asyncio.run(scenario())


def test_abandoned_lease_recovers_and_downstream_effect_stays_unique() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, run = await _seed_store(clock)
        await store.mark_run_running(run.run_id)
        message = (await store.list_messages(run.run_id))[0]
        for state in (
            MessageState.TRIAGING,
            MessageState.TRIAGED,
            MessageState.PLANNED,
            MessageState.REPAIRING,
        ):
            await store.transition_message(
                run_id=run.run_id,
                message_id=message.message_id,
                new_state=state,
                triggering_event="test",
                trace_id="trace",
            )
        repaired = repair_payload(
            message.original_payload, policy.definition.migration_rules, policy
        )
        await store.set_repaired_payload(
            run.run_id,
            message.message_id,
            repaired.repaired_payload,
            repaired.repaired_payload_hash,
            repaired.applied_repairs,
        )
        await store.transition_message(
            run_id=run.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPLAYING,
            triggering_event="test",
            trace_id="trace",
        )
        key = derive_idempotency_key(
            message.tenant_id,
            message.source_topic,
            message.message_id,
            policy.definition.action_version,
        )
        first = await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="worker-one",
            lease_duration=timedelta(seconds=10),
        )
        assert first.acquired
        reentrant = await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="worker-one",
            lease_duration=timedelta(seconds=10),
        )
        assert not reentrant.acquired
        await store.start_replay_attempt(run.run_id, key, "worker-one")
        effect_one = await store.create_or_get_downstream_effect(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            owner="worker-one",
        )
        with pytest.raises(RunConflictError):
            await store.reset_run(
                tenant_id=policy.tenant_id,
                policy_version=policy.version,
                seeds=load_seed_messages(),
                run_id="reset-must-wait-for-confirmation",
            )
        assert (await store.get_run(run.run_id)).active

        clock.advance(timedelta(seconds=11))
        second = await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="worker-two",
            lease_duration=timedelta(seconds=10),
        )
        assert second.acquired and second.takeover
        await store.start_replay_attempt(run.run_id, key, "worker-two")
        with pytest.raises(LeaseNotOwnedError):
            await store.create_or_get_downstream_effect(
                run_id=run.run_id,
                message_id=message.message_id,
                idempotency_key=key,
                repaired_payload_hash=repaired.repaired_payload_hash,
                owner="worker-one",
            )
        effect_two = await store.create_or_get_downstream_effect(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            owner="worker-two",
        )
        assert not effect_two.created
        assert (
            effect_two.effect.downstream_reference
            == effect_one.effect.downstream_reference
        )
        await store.confirm_replay(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            owner="worker-two",
            downstream_reference=effect_two.effect.downstream_reference,
            trace_id="trace-two",
            result_receipt=Receipt(
                receipt_id="replay-result",
                operation_id="replay-operation",
                run_id=run.run_id,
                message_id=message.message_id,
                kind=ReceiptKind.REPLAY_RESULT,
                status=ReceiptStatus.CONFIRMED,
                stage="replay",
                attempt=2,
                trace_id="trace-two",
                policy_version=policy.version,
                runbook_clause="RP-A-1",
                payload_hash=repaired.repaired_payload_hash,
                idempotency_key=key,
                downstream_reference=effect_two.effect.downstream_reference,
                created_at=clock.now(),
            ),
        )
        assert len(await store.list_downstream_effects(run.run_id)) == 1
        assert (
            await store.get_message(run.run_id, message.message_id)
        ).current_state == MessageState.REPLAYED
        receipts = await store.list_receipts(run.run_id, message.message_id)
        assert any(
            item.receipt_id == "replay-result"
            and item.kind == ReceiptKind.REPLAY_RESULT
            and item.status == ReceiptStatus.CONFIRMED
            for item in receipts
        )
        replacement = await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=load_seed_messages(),
            run_id="reset-after-confirmation",
        )
        assert replacement.active
        assert not (await store.get_run(run.run_id)).active

    asyncio.run(scenario())


def test_inbox_lease_is_not_reentrant_and_stale_updates_cannot_regress() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, run = await _seed_store(clock)
        seed = load_seed_messages()[0]
        registration = await store.register_delivery(
            run_id=run.run_id,
            delivery_key="inbox-fence",
            source_message_id=seed.source_message_id,
            transport_message_id="transport-fence",
            source_topic=seed.source_topic,
            payload=seed.payload,
            trace_id="delivery-trace",
        )
        await store.update_inbox_status(
            run.run_id,
            registration.inbox.inbox_id,
            InboxStatus.ENQUEUED,
            task_name="task-one",
            scheduled_at=clock.now(),
        )
        first = await store.claim_inbox(
            run.run_id,
            registration.inbox.inbox_id,
            "worker-one",
            timedelta(seconds=10),
        )
        assert first.acquired
        reentrant = await store.claim_inbox(
            run.run_id,
            registration.inbox.inbox_id,
            "worker-one",
            timedelta(seconds=10),
        )
        assert not reentrant.acquired
        delayed_schedule = await store.update_inbox_status(
            run.run_id,
            registration.inbox.inbox_id,
            InboxStatus.ENQUEUED,
            task_name="delayed-task",
            scheduled_at=clock.now(),
        )
        assert delayed_schedule.status == InboxStatus.PROCESSING
        assert delayed_schedule.lease_owner == "worker-one"

        clock.advance(timedelta(seconds=11))
        second = await store.claim_inbox(
            run.run_id,
            registration.inbox.inbox_id,
            "worker-two",
            timedelta(seconds=10),
        )
        assert second.acquired and second.takeover
        with pytest.raises(LeaseNotOwnedError):
            await store.update_inbox_status(
                run.run_id,
                registration.inbox.inbox_id,
                InboxStatus.RETRYABLE,
                owner="worker-one",
                next_attempt_at=clock.now(),
            )
        current = store._inbox[(run.run_id, registration.inbox.inbox_id)]
        assert current.status == InboxStatus.PROCESSING
        assert current.lease_owner == "worker-two"

        generation_registration = await store.register_delivery(
            run_id=run.run_id,
            delivery_key="generation-fence",
            source_message_id=seed.source_message_id,
            transport_message_id="transport-generation",
            source_topic=seed.source_topic,
            payload=seed.payload,
            trace_id="generation-trace",
        )
        generation_lease = await store.claim_inbox(
            run.run_id,
            generation_registration.inbox.inbox_id,
            "generation-worker",
            timedelta(seconds=10),
        )
        assert generation_lease.item.attempt_count == 1
        retryable = await store.update_inbox_status(
            run.run_id,
            generation_registration.inbox.inbox_id,
            InboxStatus.RETRYABLE,
            owner="generation-worker",
            next_attempt_at=clock.now(),
        )
        late_schedule = await store.update_inbox_status(
            run.run_id,
            generation_registration.inbox.inbox_id,
            InboxStatus.ENQUEUED,
            task_name="stale-generation-zero",
            scheduled_at=clock.now(),
            expected_attempt_count=0,
        )
        assert late_schedule == retryable
        assert late_schedule.status == InboxStatus.RETRYABLE

        inactive_registration = await store.register_delivery(
            run_id=run.run_id,
            delivery_key="inactive-inbox",
            source_message_id=seed.source_message_id,
            transport_message_id="transport-inactive",
            source_topic=seed.source_topic,
            payload=seed.payload,
            trace_id="inactive-trace",
        )
        await store.claim_inbox(
            run.run_id,
            inactive_registration.inbox.inbox_id,
            "abandoned-worker",
            timedelta(seconds=10),
        )
        await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=load_seed_messages(),
            run_id="inbox-replacement-run",
        )
        with pytest.raises(LeaseNotOwnedError):
            await store.update_inbox_status(
                run.run_id,
                inactive_registration.inbox.inbox_id,
                InboxStatus.FAILED_FINAL,
            )
        clock.advance(timedelta(seconds=11))
        finalized = await store.update_inbox_status(
            run.run_id,
            inactive_registration.inbox.inbox_id,
            InboxStatus.FAILED_FINAL,
        )
        assert finalized.status == InboxStatus.FAILED_FINAL

    asyncio.run(scenario())


def test_reset_fences_an_inflight_worker_before_effect_creation() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, run = await _seed_store(clock)
        await store.mark_run_running(run.run_id)
        message = (await store.list_messages(run.run_id))[0]
        for state in (
            MessageState.TRIAGING,
            MessageState.TRIAGED,
            MessageState.PLANNED,
            MessageState.REPAIRING,
        ):
            await store.transition_message(
                run_id=run.run_id,
                message_id=message.message_id,
                new_state=state,
                triggering_event="test",
                trace_id="trace",
            )
        repaired = repair_payload(
            message.original_payload, policy.definition.migration_rules, policy
        )
        await store.set_repaired_payload(
            run.run_id,
            message.message_id,
            repaired.repaired_payload,
            repaired.repaired_payload_hash,
            repaired.applied_repairs,
        )
        await store.transition_message(
            run_id=run.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPLAYING,
            triggering_event="test",
            trace_id="trace",
        )
        key = derive_idempotency_key(
            message.tenant_id,
            message.source_topic,
            message.message_id,
            policy.definition.action_version,
        )
        await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="stale-worker",
            lease_duration=timedelta(seconds=30),
        )
        await store.start_replay_attempt(run.run_id, key, "stale-worker")

        await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=load_seed_messages(),
            run_id="replacement-run",
        )
        with pytest.raises(RunConflictError):
            await store.create_or_get_downstream_effect(
                run_id=run.run_id,
                message_id=message.message_id,
                idempotency_key=key,
                repaired_payload_hash=repaired.repaired_payload_hash,
                owner="stale-worker",
            )
        assert await store.list_downstream_effects(run.run_id) == []

    asyncio.run(scenario())


def test_same_key_with_different_payload_hash_fails_safely() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, run = await _seed_store(clock)
        await store.mark_run_running(run.run_id)
        message = (await store.list_messages(run.run_id))[0]
        for state in (
            MessageState.TRIAGING,
            MessageState.TRIAGED,
            MessageState.PLANNED,
            MessageState.REPAIRING,
        ):
            await store.transition_message(
                run_id=run.run_id,
                message_id=message.message_id,
                new_state=state,
                triggering_event="test",
                trace_id="trace",
            )
        repaired = repair_payload(
            message.original_payload, policy.definition.migration_rules, policy
        )
        await store.set_repaired_payload(
            run.run_id,
            message.message_id,
            repaired.repaired_payload,
            repaired.repaired_payload_hash,
            repaired.applied_repairs,
        )
        await store.transition_message(
            run_id=run.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPLAYING,
            triggering_event="test",
            trace_id="trace",
        )
        key = derive_idempotency_key(
            message.tenant_id,
            message.source_topic,
            message.message_id,
            policy.definition.action_version,
        )
        await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="one",
            lease_duration=timedelta(seconds=10),
        )
        with pytest.raises(PayloadHashMismatchError):
            await store.acquire_replay_lease(
                run_id=run.run_id,
                message_id=message.message_id,
                idempotency_key=key,
                repaired_payload_hash="b" * 64,
                action_version=policy.definition.action_version,
                owner="two",
                lease_duration=timedelta(seconds=10),
            )

    asyncio.run(scenario())


def test_failed_stage_cannot_route_triage_failure_into_replay() -> None:
    with pytest.raises(InvalidTransitionError):
        ensure_transition(
            MessageState.FAILED_RETRYABLE,
            MessageState.REPLAYING,
            FailedStage.TRIAGE,
        )


def test_failed_replay_result_and_state_change_commit_together() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, _ = await _seed_store(clock)
        seed = load_seed_messages()[0]
        run = await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=[seed],
            run_id="failed-receipt-run",
        )
        await store.mark_run_running(run.run_id)
        message = await store.get_message(run.run_id, seed.source_message_id)
        for state in (
            MessageState.TRIAGING,
            MessageState.TRIAGED,
            MessageState.PLANNED,
            MessageState.REPAIRING,
        ):
            await store.transition_message(
                run_id=run.run_id,
                message_id=message.message_id,
                new_state=state,
                triggering_event="test",
                trace_id="failure-trace",
            )
        repaired = repair_payload(
            message.original_payload, policy.definition.migration_rules, policy
        )
        await store.set_repaired_payload(
            run.run_id,
            message.message_id,
            repaired.repaired_payload,
            repaired.repaired_payload_hash,
            repaired.applied_repairs,
        )
        await store.transition_message(
            run_id=run.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPLAYING,
            triggering_event="test",
            trace_id="failure-trace",
        )
        key = derive_idempotency_key(
            message.tenant_id,
            message.source_topic,
            message.message_id,
            policy.definition.action_version,
        )
        await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="retry-worker",
            lease_duration=timedelta(seconds=10),
        )
        first_attempt = await store.start_replay_attempt(
            run.run_id, key, "retry-worker"
        )
        retry_receipt = Receipt(
            receipt_id="failed-result-retryable",
            operation_id="failed-operation-one",
            run_id=run.run_id,
            message_id=message.message_id,
            kind=ReceiptKind.REPLAY_RESULT,
            status=ReceiptStatus.FAILED,
            stage="replay",
            attempt=first_attempt.attempt_count,
            trace_id="failure-trace",
            policy_version=policy.version,
            payload_hash=repaired.repaired_payload_hash,
            idempotency_key=key,
            error_code="DOWNSTREAM_UNAVAILABLE",
            retryable=True,
            created_at=clock.now(),
        )
        retry_at = clock.now() + timedelta(seconds=5)

        async def mark_retry(receipt: Receipt):
            return await store.mark_replay_retryable(
                run_id=run.run_id,
                idempotency_key=key,
                owner="retry-worker",
                error_code="DOWNSTREAM_UNAVAILABLE",
                next_attempt_at=retry_at,
                trace_id="failure-trace",
                result_receipt=receipt,
            )

        invalid_receipts = (
            retry_receipt.model_copy(update={"operation_id": ""}),
            retry_receipt.model_copy(update={"trace_id": "different-trace"}),
            retry_receipt.model_copy(
                update={"downstream_reference": "unexpected-reference"}
            ),
        )
        for invalid_receipt in invalid_receipts:
            with pytest.raises(RunConflictError):
                await mark_retry(invalid_receipt)

        prepersisted_retry_receipt = retry_receipt.model_copy(
            update={
                "receipt_id": "prepersisted-retryable-result",
                "operation_id": "prepersisted-retryable-operation",
            }
        )
        await store.record_receipt(prepersisted_retry_receipt)
        with pytest.raises(RunConflictError):
            await mark_retry(prepersisted_retry_receipt)
        assert (await store.get_replay_ledger(run.run_id, key)).status == (
            ReplayStatus.ATTEMPTED
        )
        assert (
            await store.get_message(run.run_id, message.message_id)
        ).current_state == MessageState.REPLAYING

        retryable = await mark_retry(retry_receipt)
        assert retryable.status == ReplayStatus.RETRYABLE
        assert await mark_retry(retry_receipt) == retryable
        assert retry_receipt in await store.list_receipts(
            run.run_id, message.message_id
        )

        clock.advance(timedelta(seconds=5))
        await store.transition_message(
            run_id=run.run_id,
            message_id=message.message_id,
            new_state=MessageState.REPLAYING,
            triggering_event="retry_due",
            trace_id="failure-trace-two",
        )
        await store.acquire_replay_lease(
            run_id=run.run_id,
            message_id=message.message_id,
            idempotency_key=key,
            repaired_payload_hash=repaired.repaired_payload_hash,
            action_version=policy.definition.action_version,
            owner="final-worker",
            lease_duration=timedelta(seconds=10),
        )
        final_attempt = await store.start_replay_attempt(
            run.run_id, key, "final-worker"
        )
        final_receipt = retry_receipt.model_copy(
            update={
                "receipt_id": "failed-result-final",
                "operation_id": "failed-operation-two",
                "attempt": final_attempt.attempt_count,
                "trace_id": "failure-trace-two",
                "retryable": False,
                "created_at": clock.now(),
            }
        )

        async def mark_final(
            receipt: Receipt,
            new_state: MessageState = MessageState.FAILED_FINAL,
        ):
            return await store.mark_replay_failed_final(
                run_id=run.run_id,
                idempotency_key=key,
                owner="final-worker",
                error_code="DOWNSTREAM_UNAVAILABLE",
                trace_id="failure-trace-two",
                result_receipt=receipt,
                new_state=new_state,
            )

        prepersisted_final_receipt = final_receipt.model_copy(
            update={
                "receipt_id": "prepersisted-final-result",
                "operation_id": "prepersisted-final-operation",
            }
        )
        await store.record_receipt(prepersisted_final_receipt)
        with pytest.raises(RunConflictError):
            await mark_final(prepersisted_final_receipt)
        assert (await store.get_replay_ledger(run.run_id, key)).status == (
            ReplayStatus.ATTEMPTED
        )

        final = await mark_final(final_receipt)
        assert final.status == ReplayStatus.FAILED_FINAL
        assert await mark_final(final_receipt) == final
        with pytest.raises(RunConflictError):
            await mark_final(final_receipt, MessageState.ESCALATED)
        assert final_receipt in await store.list_receipts(
            run.run_id, message.message_id
        )
        assert (await store.get_run(run.run_id)).status == RunStatus.COMPLETE

    asyncio.run(scenario())


def test_generic_terminal_transition_completes_a_running_run() -> None:
    async def scenario() -> None:
        clock = MutableClock()
        store, policy, _ = await _seed_store(clock)
        seed = load_seed_messages()[0]
        run = await store.reset_run(
            tenant_id=policy.tenant_id,
            policy_version=policy.version,
            seeds=[seed],
            run_id="terminal-transition-run",
        )
        await store.mark_run_running(run.run_id)
        await store.transition_message(
            run_id=run.run_id,
            message_id=seed.source_message_id,
            new_state=MessageState.TRIAGING,
            triggering_event="test",
            trace_id="terminal-trace",
        )
        await store.transition_message(
            run_id=run.run_id,
            message_id=seed.source_message_id,
            new_state=MessageState.ESCALATED,
            triggering_event="invalid_model_output",
            trace_id="terminal-trace",
            error_code="INVALID_MODEL_OUTPUT",
        )
        completed = await store.get_run(run.run_id)
        assert completed.status == RunStatus.COMPLETE
        assert completed.completed_at == clock.now()

    asyncio.run(scenario())
