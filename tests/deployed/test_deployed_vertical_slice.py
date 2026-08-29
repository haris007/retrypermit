import os
import time
from pathlib import Path

import httpx
import pytest
from google.cloud import firestore


def _wait_for_stats(
    client: httpx.Client,
    run_id: str,
    *,
    resolved: int,
    dlq_depth: int,
) -> dict:
    # Production policies use a five-minute Class B recheck. Keep the deployed
    # acceptance honest instead of enabling the local ten-second demo override.
    timeout_seconds = int(os.getenv("RETRYPERMIT_DEPLOYED_RUN_TIMEOUT_SECONDS", "420"))
    deadline = time.monotonic() + timeout_seconds
    stats = {}
    while time.monotonic() < deadline:
        stats = (
            client.get("/api/stats", params={"run_id": run_id})
            .raise_for_status()
            .json()
        )
        if stats["resolved_count"] == resolved and stats["dlq_depth"] == dlq_depth:
            return stats
        time.sleep(2)
    return stats


@pytest.mark.gcp
@pytest.mark.deployed
def test_deployed_vertical_slice_uses_real_transport_and_model() -> None:
    """Unmocked Cloud Run/Pub/Sub/Tasks/Firestore/Gemini acceptance test."""

    base_url = os.getenv("RETRYPERMIT_DEPLOYED_URL", "").rstrip("/")
    admin_token = os.getenv("RETRYPERMIT_DEPLOYED_ADMIN_TOKEN", "")
    project = os.getenv("RETRYPERMIT_GCP_PROJECT", "")
    if not base_url or not admin_token or not project:
        pytest.skip("set deployed URL, admin token, and GCP project after deployment")
    assert base_url.startswith("https://")
    assert "localhost" not in base_url and "127.0.0.1" not in base_url
    headers = {"X-Admin-Token": admin_token}
    with httpx.Client(base_url=base_url, timeout=60) as client:
        health = client.get("/health").raise_for_status().json()
        assert health["mode"] == "Google Cloud"
        assert health["environment"] == "cloud"
        assert health["store"] == "Firestore"
        assert health["model_provider"].startswith("Google ADK / ")
        assert "fixture" not in health["model_provider"].lower()
        assert health["synthetic_data"] is True
        assert health["simulated_downstream"] is True
        client.post(
            "/api/policies/runbook-v1/activate",
            headers=headers,
            json={"actor": "deployed-phase5-test"},
        ).raise_for_status()
        failure_run_id = (
            client.post("/api/demo/reset", headers=headers, json={})
            .raise_for_status()
            .json()["run_id"]
        )
        client.post(
            "/api/demo/start-with-failure", headers=headers, json={}
        ).raise_for_status()
        stats = _wait_for_stats(client, failure_run_id, resolved=9, dlq_depth=3)
        assert stats["resolved_count"] == 9
        assert stats["dlq_depth"] == 3
        assert stats["deferred_total"] == 3
        assert stats["escalated_count"] == 2
        assert stats["quarantined_count"] == 1
        assert stats["durable_delivery"] is True
        assert stats["model"] != "deterministic-fixture"
        failure_messages = (
            client.get("/api/messages", params={"run_id": failure_run_id})
            .raise_for_status()
            .json()["messages"]
        )
        assert [item["current_state"] for item in failure_messages].count(
            "REPLAYED"
        ) == 9
        assert [item["current_state"] for item in failure_messages].count(
            "ESCALATED"
        ) == 2
        assert [item["current_state"] for item in failure_messages].count(
            "QUARANTINED"
        ) == 1
        proof = (
            client.get(
                "/api/messages/synthetic-order-001/proof",
                params={"run_id": failure_run_id},
            )
            .raise_for_status()
            .json()["proof"]
        )
        assert proof["delivery_count_label"] == "deliveries recorded by RetryPermit"
        assert proof["execution_attempt_count"] == 2
        assert proof["downstream_request_count"] == 2
        assert proof["unique_downstream_effect_count"] == 1
        assert proof["ledger_status"] == "CONFIRMED"

        policies = client.get("/api/policies").raise_for_status().json()
        versions = {item["definition"]["version"] for item in policies["items"]}
        if "runbook-v2" not in versions:
            with Path("fixtures/dlq-runbook-v2.pdf").open("rb") as pdf_file:
                extracted = client.post(
                    "/api/policies/extract",
                    headers=headers,
                    files={"file": ("dlq-runbook-v2.pdf", pdf_file, "application/pdf")},
                ).raise_for_status()
            assert extracted.json()["policy"]["status"] == "PENDING_APPROVAL"
        client.post(
            "/api/policies/runbook-v2/approve",
            headers=headers,
            json={"actor": "deployed-phase5-test"},
        ).raise_for_status()
        client.post(
            "/api/policies/runbook-v2/activate",
            headers=headers,
            json={"actor": "deployed-phase5-test"},
        ).raise_for_status()
        v2_run_id = (
            client.post("/api/demo/reset", headers=headers, json={})
            .raise_for_status()
            .json()["run_id"]
        )
        client.post("/api/demo/start", headers=headers, json={}).raise_for_status()
        v2_stats = _wait_for_stats(client, v2_run_id, resolved=9, dlq_depth=3)
        assert v2_stats["resolved_count"] == 9
        assert v2_stats["dlq_depth"] == 3
        assert v2_stats["deferred_total"] == 3
        assert v2_stats["escalated_count"] == 2
        assert v2_stats["quarantined_count"] == 1
        messages = (
            client.get("/api/messages", params={"run_id": v2_run_id})
            .raise_for_status()
            .json()["messages"]
        )
        assert len(messages) == 12
        assert {item["current_state"] for item in messages} == {
            "REPLAYED",
            "ESCALATED",
            "QUARANTINED",
        }
        idempotency_keys: set[str] = set()
        for message in messages:
            receipts = (
                client.get(
                    f"/api/messages/{message['message_id']}/receipts",
                    params={"run_id": v2_run_id},
                )
                .raise_for_status()
                .json()["items"]
            )
            kinds = {item["kind"] for item in receipts}
            assert {
                "pubsub_publish_attempt",
                "pubsub_publish_result",
                "delivery",
                "task_schedule_attempt",
                "task_schedule_result",
                "triage_attempt",
                "triage_result",
            } <= kinds
            if message["current_state"] == "REPLAYED":
                key = message["idempotency_key"]
                assert key
                idempotency_keys.add(key)
                assert {"replay_attempt", "replay_result"} <= kinds
                assert any(
                    item["kind"] == "replay_result" and item["status"] == "CONFIRMED"
                    for item in receipts
                )
                effect = (
                    client.get(
                        f"/mock-downstream/orders/{key}",
                        params={"run_id": v2_run_id},
                    )
                    .raise_for_status()
                    .json()["effect"]
                )
                assert effect["message_id"] == message["message_id"]
                assert effect["idempotency_key"] == key
            elif message["current_state"] == "ESCALATED":
                assert "escalation" in kinds
                assert message["idempotency_key"] is None
            else:
                assert "refusal" in kinds
                assert message["idempotency_key"] is None

    assert len(idempotency_keys) == 9

    # This query does not trust the application response. It independently proves
    # that the acceptance run and one unique effect per key reached Firestore.
    db = firestore.Client(project=project)
    failure_run_ref = db.collection("demo_runs").document(failure_run_id)
    assert len(list(failure_run_ref.collection("messages").stream())) == 12
    assert len(list(failure_run_ref.collection("downstream_effects").stream())) == 9

    run_ref = db.collection("demo_runs").document(v2_run_id)
    run_snapshot = run_ref.get()
    assert run_snapshot.exists
    assert run_snapshot.to_dict()["status"] == "COMPLETE"
    assert len(list(run_ref.collection("messages").stream())) == 12
    effect_snapshots = list(run_ref.collection("downstream_effects").stream())
    assert len(effect_snapshots) == 9
    assert {snapshot.id for snapshot in effect_snapshots} == idempotency_keys
