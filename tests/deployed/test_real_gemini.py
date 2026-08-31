import os

import pytest

from retrypermit.adapters.models.adk_gemini import AdkGeminiProvider
from retrypermit.adapters.policies.seeded_json import (
    SeededPolicyProvider,
    load_seed_messages,
)
from retrypermit.domain.triage import PolicyClauseContext, TriageRequest


@pytest.mark.gcp
@pytest.mark.asyncio
async def test_real_adk_gemini_structured_triage() -> None:
    """Unmocked credentialed verification; intentionally skipped without opt-in."""

    if os.getenv("RUN_GCP_TESTS") != "1":
        pytest.skip("set RUN_GCP_TESTS=1 with ADC and a dedicated project")
    policy = await SeededPolicyProvider().load()
    seed = load_seed_messages()[0]
    request = TriageRequest(
        tenant_id=policy.tenant_id,
        run_id="real-model-verification",
        message_id=seed.source_message_id,
        payload=seed.payload,
        policy_version=policy.version,
        allowed_repairs=policy.definition.migration_rules,
        approved_currencies=policy.definition.approved_currencies,
        effective_replay_cap=str(policy.definition.effective_replay_cap),
        clause_ids=[item.clause_id for item in policy.definition.clauses],
        policy_clauses=[
            PolicyClauseContext(
                clause_id=item.clause_id,
                page=item.page,
                text=item.text,
                failure_classes=item.failure_classes,
                authorized_actions=item.authorized_actions,
            )
            for item in policy.definition.clauses
        ],
    )
    triage = await AdkGeminiProvider(
        os.getenv("GEMINI_MODEL", "gemini-3.7-flash")
    ).triage(request)
    assert triage.failure_class.value == "schema_drift"
    assert triage.runbook_clause in request.clause_ids
