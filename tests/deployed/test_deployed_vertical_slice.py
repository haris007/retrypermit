import os
import time

import httpx
import pytest
from google.cloud import firestore


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
    with httpx.Client(base_url=base_url, timeout=30) as client:
        health = client.get("/health").raise_for_status().json()
        assert health["mode"] == "Google Cloud"
        assert health["environment"] == "cloud"
        assert health["store"] == "Firestore"
        assert health["model_provider"].startswith("Google ADK / ")
        assert "fixture" not in health["model_provider"].lower()
        assert health["synthetic_data"] is True
        assert health["simulated_downstream"] is True
        run_id = (
            client.post("/api/demo/reset", headers=headers, json={})
            .raise_for_status()
            .json()["run_id"]
        )
        client.post("/api/demo/start", headers=headers, json={}).raise_for_status()
        deadline = time.monotonic() + 90
        stats = {}
        while time.monotonic() < deadline:
            stats = (
                client.get("/api/stats", params={"run_id": run_id})
                .raise_for_status()
                .json()
            )
            if stats["resolved_count"] == 6:
                break
            time.sleep(2)
        assert stats["resolved_count"] == 6
        assert stats["dlq_depth"] == 0
        assert stats["durable_delivery"] is True
        assert stats["model"] != "deterministic-fixture"
        messages = (
            client.get("/api/messages", params={"run_id": run_id})
            .raise_for_status()
            .json()["messages"]
        )
        assert {item["current_state"] for item in messages} == {"REPLAYED"}
        idempotency_keys: set[str] = set()
        for message in messages:
            key = message["idempotency_key"]
            idempotency_keys.add(key)
            receipts = (
                client.get(
                    f"/api/messages/{message['message_id']}/receipts",
                    params={"run_id": run_id},
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
                "replay_attempt",
                "replay_result",
            } <= kinds
            assert any(
                item["kind"] == "replay_result" and item["status"] == "CONFIRMED"
                for item in receipts
            )
            effect = (
                client.get(
                    f"/mock-downstream/orders/{key}",
                    params={"run_id": run_id},
                )
                .raise_for_status()
                .json()["effect"]
            )
            assert effect["message_id"] == message["message_id"]
            assert effect["idempotency_key"] == key

    assert len(idempotency_keys) == 6

    # This query does not trust the application response. It independently proves
    # that the acceptance run and one unique effect per key reached Firestore.
    db = firestore.Client(project=project)
    run_ref = db.collection("demo_runs").document(run_id)
    run_snapshot = run_ref.get()
    assert run_snapshot.exists
    assert run_snapshot.to_dict()["status"] == "COMPLETE"
    assert len(list(run_ref.collection("messages").stream())) == 6
    effect_snapshots = list(run_ref.collection("downstream_effects").stream())
    assert len(effect_snapshots) == 6
    assert {snapshot.id for snapshot in effect_snapshots} == idempotency_keys
