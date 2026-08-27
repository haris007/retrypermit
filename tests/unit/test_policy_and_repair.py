import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from retrypermit.adapters.models.fake import DeterministicFakeModelProvider
from retrypermit.adapters.policies.seeded_json import (
    SeededPolicyProvider,
    load_seed_messages,
)
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.application.repair_service import repair_payload
from retrypermit.application.triage_validator import authorize_triage
from retrypermit.domain.enums import RepairKind
from retrypermit.domain.errors import RepairRejectedError
from retrypermit.domain.policy import PolicyDefinition
from retrypermit.domain.triage import PolicyClauseContext, Repair, TriageRequest


def test_policy_cap_is_clamped_and_invalid_clause_repair_is_rejected() -> None:
    path = Path("fixtures/policies/runbook-v1.json")
    raw = json.loads(path.read_text())
    raw["auto_replay_cap"] = "9999.00"
    policy = PolicyDefinition.model_validate(raw)
    assert policy.effective_replay_cap == Decimal("2500.00")

    raw["clauses"][0]["allowed_repairs"][0]["target_field"] = "invented"
    with pytest.raises(ValidationError):
        PolicyDefinition.model_validate(raw)


def test_allowlisted_repair_and_invented_transform_rejection() -> None:
    async def scenario() -> None:
        candidate = await SeededPolicyProvider().load()
        store = MemoryStore()
        await store.install_policy(candidate)
        policy = await store.activate_policy(
            candidate.tenant_id, candidate.version, "test"
        )
        payload = load_seed_messages()[0].payload
        result = repair_payload(payload, policy.definition.migration_rules, policy)
        assert "customer_id" not in result.repaired_payload
        assert result.repaired_payload["customerId"] == "CUSTOMER-ALDER"
        assert result.repaired_payload["amount"] == "329.00"

        invented = Repair(
            kind=RepairKind.RENAME_FIELD,
            source_field="order_id",
            target_field="inventedOrderId",
        )
        with pytest.raises(RepairRejectedError):
            repair_payload(payload, [invented], policy)

    asyncio.run(scenario())


def test_low_confidence_cannot_authorize_replay() -> None:
    async def scenario() -> None:
        candidate = await SeededPolicyProvider().load()
        store = MemoryStore()
        await store.install_policy(candidate)
        policy = await store.activate_policy(
            candidate.tenant_id, candidate.version, "test"
        )
        seed = load_seed_messages()[0]
        request = TriageRequest(
            tenant_id=candidate.tenant_id,
            run_id="test",
            message_id=seed.source_message_id,
            payload=seed.payload,
            policy_version=candidate.version,
            allowed_repairs=policy.definition.migration_rules,
            clause_ids=[clause.clause_id for clause in policy.definition.clauses],
            policy_clauses=[
                PolicyClauseContext(
                    clause_id=clause.clause_id,
                    page=clause.page,
                    text=clause.text,
                    failure_classes=clause.failure_classes,
                    authorized_actions=clause.authorized_actions,
                )
                for clause in policy.definition.clauses
            ],
        )
        triage = await DeterministicFakeModelProvider().triage(request)
        low_confidence = triage.model_copy(update={"confidence": 0.69})
        decision = authorize_triage(seed.payload, low_confidence, policy)
        assert not decision.authorized
        assert "LOW_CONFIDENCE" in decision.error_codes

    asyncio.run(scenario())
