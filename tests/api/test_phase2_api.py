import pytest
from fastapi.testclient import TestClient

from retrypermit.config import Settings
from retrypermit.main import create_app


ADMIN_TOKEN = "test-only-admin-token-long-enough"


@pytest.fixture
def client():
    settings = Settings(
        _env_file=None,
        demo_admin_token=ADMIN_TOKEN,
        demo_mode=True,
        use_in_memory_store=True,
        use_fake_model=True,
        allow_local_service_auth=True,
        replay_retry_base_seconds=0,
        replay_retry_max_seconds=0,
        demo_recheck_seconds=0.01,
        simulated_transient_recovery_seconds=0.01,
    )
    with TestClient(create_app(settings)) as value:
        yield value


def test_start_with_failure_and_live_proof(client: TestClient) -> None:
    response = client.post(
        "/api/demo/start-with-failure",
        json={},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert response.status_code == 200
    assert response.json()["details"]["inject_failure"] is True

    proof = client.get("/api/messages/synthetic-order-001/proof")
    assert proof.status_code == 200
    body = proof.json()["proof"]
    assert body["delivery_count_label"] == "deliveries recorded by RetryPermit"
    assert body["recorded_pubsub_delivery_count"] == 1
    assert body["execution_attempt_count"] == 2
    assert body["downstream_request_count"] == 2
    assert body["unique_downstream_effect_count"] == 1
    assert body["ledger_status"] == "CONFIRMED"


def test_pdf_extract_approve_activate_routes_are_admin_guarded(
    client: TestClient,
) -> None:
    pdf = open("fixtures/dlq-runbook-v2.pdf", "rb").read()
    denied = client.post(
        "/api/policies/extract",
        files={"file": ("runbook-v2.pdf", pdf, "application/pdf")},
    )
    assert denied.status_code == 401

    extracted = client.post(
        "/api/policies/extract",
        files={"file": ("runbook-v2.pdf", pdf, "application/pdf")},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert extracted.status_code == 200
    assert extracted.json()["policy"]["status"] == "PENDING_APPROVAL"

    unapproved = client.post(
        "/api/policies/runbook-v2/activate",
        json={"actor": "test-admin"},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert unapproved.status_code == 422

    approved = client.post(
        "/api/policies/runbook-v2/approve",
        json={"actor": "test-admin"},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert approved.status_code == 200
    activated = client.post(
        "/api/policies/runbook-v2/activate",
        json={"actor": "test-admin"},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert activated.status_code == 200
    policies = client.get("/api/policies").json()
    assert policies["active_policy"]["definition"]["version"] == "runbook-v2"
    assert policies["approval_events"][-1]["policy_version"] == "runbook-v2"
    assert policies["activation_events"][-1]["new_active_version"] == "runbook-v2"

    reset = client.post(
        "/api/demo/reset",
        json={},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert reset.status_code == 200
    started = client.post(
        "/api/demo/start",
        json={},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    assert started.status_code == 200
    stats = client.get("/api/stats").json()
    assert stats["resolved_count"] == 9
    assert stats["dlq_depth"] == 3
