from datetime import UTC, datetime
from decimal import Decimal

import pytest

from retrypermit.adapters.downstream.store_backed import StoreBackedDownstream
from retrypermit.adapters.models.fake import DeterministicFakeModelProvider
from retrypermit.adapters.policies.seeded_json import (
    SeededPolicyProvider,
    load_seed_messages,
)
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.application.orchestrator import RetryPermitOrchestrator
from retrypermit.domain.enums import MessageState, PolicyStatus
from retrypermit.domain.hashing import derive_idempotency_key
from retrypermit.domain.messages import SeedMessage
from retrypermit.domain.policy import PolicyVersion, policy_definition_hash
from retrypermit.domain.triage import PolicyClauseContext, TriageRequest


def test_idempotency_key_is_stable_and_namespaced() -> None:
    first = derive_idempotency_key("tenant-a", "orders.dlq", "message-1", "action-v1")
    second = derive_idempotency_key("tenant-a", "orders.dlq", "message-1", "action-v1")
    assert first == second
    assert len(first) == 64
    assert first != "message-1"
    assert first != derive_idempotency_key(
        "tenant-b", "orders.dlq", "message-1", "action-v1"
    )
    assert first != derive_idempotency_key(
        "tenant-a", "orders.retry", "message-1", "action-v1"
    )


@pytest.mark.asyncio
async def test_fake_provider_classifies_the_approved_schema_drift() -> None:
    candidate = await SeededPolicyProvider().load()
    seed = load_seed_messages()[0]
    request = TriageRequest(
        tenant_id=candidate.tenant_id,
        run_id="classification",
        message_id=seed.source_message_id,
        payload=seed.payload,
        policy_version=candidate.version,
        allowed_repairs=candidate.definition.migration_rules,
        clause_ids=[item.clause_id for item in candidate.definition.clauses],
        policy_clauses=[
            PolicyClauseContext(
                clause_id=item.clause_id,
                page=item.page,
                text=item.text,
                failure_classes=item.failure_classes,
                authorized_actions=item.authorized_actions,
            )
            for item in candidate.definition.clauses
        ],
    )
    result = await DeterministicFakeModelProvider().triage(request)
    assert result.failure_class.value == "schema_drift"
    assert result.recommended_action.value == "replay"
    assert result.runbook_clause == "RP-A-1"


@pytest.mark.asyncio
async def test_over_500_escalates_under_v1_and_replays_under_active_v2() -> None:
    provider = SeededPolicyProvider()
    v1 = await provider.load()
    seed = SeedMessage(
        source_message_id="policy-cap-order",
        payload={
            "order_id": "ORDER-POLICY-CAP",
            "customer_id": "CUSTOMER-POLICY",
            "amount": 750.0,
            "currency": "USD",
            "products": [{"sku": "DEMO-POLICY", "quantity": 1}],
        },
    )
    store = MemoryStore()
    await store.install_policy(v1)
    await store.activate_policy(v1.tenant_id, v1.version, "test")
    v1_run = await store.reset_run(
        tenant_id=v1.tenant_id,
        policy_version=v1.version,
        seeds=[seed],
        run_id="policy-v1-cap",
    )
    orchestrator = RetryPermitOrchestrator(
        store,
        DeterministicFakeModelProvider(),
        StoreBackedDownstream(store),
    )
    v1_result = await orchestrator.run_message(v1_run.run_id, seed.source_message_id)
    assert v1_result.current_state == MessageState.ESCALATED
    assert await store.list_downstream_effects(v1_run.run_id) == []

    definition_v2 = v1.definition.model_copy(
        update={
            "version": "runbook-v2-test",
            "name": "RetryPermit synthetic test policy v2",
            "auto_replay_cap": Decimal("2000.00"),
            "action_version": "schema-v3-to-v4-v2-test",
        }
    )
    now = datetime.now(UTC)
    v2 = PolicyVersion(
        definition=definition_v2,
        source_kind="test_fixture",
        source_hash="2" * 64,
        extracted_policy_hash=policy_definition_hash(definition_v2),
        status=PolicyStatus.APPROVED,
        extracted_at=now,
        approved_at=now,
        approved_by="test",
    )
    await store.install_policy(v2)
    await store.activate_policy(v2.tenant_id, v2.version, "test")
    v2_run = await store.reset_run(
        tenant_id=v2.tenant_id,
        policy_version=v2.version,
        seeds=[seed],
        run_id="policy-v2-cap",
    )
    v2_result = await orchestrator.run_message(v2_run.run_id, seed.source_message_id)
    assert v2_result.current_state == MessageState.REPLAYED
    assert len(await store.list_downstream_effects(v2_run.run_id)) == 1


def test_system_cap_remains_2500_for_any_policy() -> None:
    import asyncio

    v1 = asyncio.run(SeededPolicyProvider().load())
    raised = v1.definition.model_copy(update={"auto_replay_cap": Decimal("999999")})
    assert raised.effective_replay_cap == Decimal("2500.00")
