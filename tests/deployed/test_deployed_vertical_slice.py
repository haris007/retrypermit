"""Explicitly opted-in acceptance against the real deployed Google Cloud stack."""

import hashlib
import json
import os
import time
from pathlib import Path

import httpx
import pytest
from google.cloud import firestore

from retrypermit.domain.policy import PolicyDefinition, policy_definition_hash

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
ACTOR = {"actor": "authorized-hackathon-final-verification"}
EXPECTED = {
    "replayed_count": 9,
    "escalated_count": 2,
    "quarantined_count": 1,
    "dlq_depth": 3,
}


def _fingerprints(policies):
    return {
        p["definition"]["version"]: (
            p["definition"],
            p["source_hash"],
            p["extracted_policy_hash"],
        )
        for p in policies["items"]
    }


def _prepare_v3(client, headers):
    """Real PDF extraction; never approve a proposal that differs from reviewed JSON."""
    expected = PolicyDefinition.model_validate_json(
        (FIXTURES / "policies/runbook-v3.json").read_text()
    )
    pdf = (FIXTURES / "dlq-runbook-v3.pdf").read_bytes()
    policies = client.get("/api/policies").raise_for_status().json()
    candidate = next(
        (p for p in policies["items"] if p["definition"]["version"] == "runbook-v3"),
        None,
    )
    if candidate is None:
        response = client.post(
            "/api/policies/extract",
            headers=headers,
            files={"file": ("dlq-runbook-v3.pdf", pdf, "application/pdf")},
        ).raise_for_status()
        candidate = response.json()["policy"]
        assert candidate["status"] == "PENDING_APPROVAL"
        assert (
            client.post(
                "/api/policies/runbook-v3/activate", headers=headers, json=ACTOR
            ).status_code
            == 422
        )
        print(
            "Real Gemini PDF extraction created a pending v3; unapproved activation denied.",
            flush=True,
        )
    assert candidate["definition"]["version"] == "runbook-v3"
    assert candidate["source_kind"] == "gemini_pdf_extraction"
    assert candidate["source_hash"] == hashlib.sha256(pdf).hexdigest()
    actual = PolicyDefinition.model_validate(candidate["definition"])
    assert actual == expected, (
        "STOP: extracted proposal differs from reviewed v3; do not approve or overwrite"
    )
    assert candidate["extracted_policy_hash"] == policy_definition_hash(expected)
    if candidate["status"] == "PENDING_APPROVAL":
        client.post(
            "/api/policies/runbook-v3/approve", headers=headers, json=ACTOR
        ).raise_for_status()
    print(
        f"v3 reviewed and approved: policy_hash={candidate['extracted_policy_hash']}",
        flush=True,
    )


def _wait_for_completion(client, headers, run_id, forbidden_version):
    # Production Class B uses 300 seconds. No local ten-second override or manual
    # recheck endpoint may stand in for real scheduled Cloud Tasks delivery.
    deadline = time.monotonic() + int(
        os.getenv("RETRYPERMIT_DEPLOYED_RUN_TIMEOUT_SECONDS", "540")
    )
    next_report = 0.0
    switch_denied = False
    while time.monotonic() < deadline:
        stats = (
            client.get("/api/stats", params={"run_id": run_id})
            .raise_for_status()
            .json()
        )
        if time.monotonic() >= next_report:
            print(
                json.dumps(
                    {
                        "progress": run_id,
                        "elapsed_seconds": stats["elapsed_seconds"],
                        "states": stats["state_counts"],
                    }
                ),
                flush=True,
            )
            next_report = time.monotonic() + 30
        if stats["deferred_count"] > 0 and not switch_denied:
            response = client.post(
                f"/api/policies/{forbidden_version}/activate",
                headers=headers,
                json=ACTOR,
            )
            assert response.status_code == 409, (
                "A deferred run must retain its active policy"
            )
            switch_denied = True
            print(
                f"{run_id}: policy switch correctly rejected while deferred.",
                flush=True,
            )
        if stats["run_status"] == "COMPLETE":
            assert switch_denied, (
                "No observed deferred interval; scheduled-recheck claim unproven"
            )
            assert {key: stats[key] for key in EXPECTED} == EXPECTED, stats
            assert stats["deferred_total"] == 3
            assert stats["durable_delivery"] is True
            assert stats["model"] != "deterministic-fixture"
            assert stats["demo_recheck_seconds"] is None
            return stats
        time.sleep(2)
    pytest.fail(f"Cloud run did not complete before timeout: {stats}")


def _verify_run(client, headers, run_id, inject_failure, *, repeat_start=True):
    messages = (
        client.get("/api/messages", params={"run_id": run_id})
        .raise_for_status()
        .json()["messages"]
    )
    assert len(messages) == 12
    keys = set()
    for message in messages:
        message_id = message["message_id"]
        receipts = (
            client.get(
                f"/api/messages/{message_id}/receipts", params={"run_id": run_id}
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
        transitions = (
            client.get(
                f"/api/messages/{message_id}/transitions", params={"run_id": run_id}
            )
            .raise_for_status()
            .json()["items"]
        )
        states = [item["new_state"] for item in transitions]
        number = int(message_id.rsplit("-", 1)[1])
        if number <= 9:
            assert message["current_state"] == "REPLAYED"
            key = message["idempotency_key"]
            assert key
            keys.add(key)
            assert any(
                item["kind"] == "replay_result" and item["status"] == "CONFIRMED"
                for item in receipts
            )
            effect = (
                client.get(f"/mock-downstream/orders/{key}", params={"run_id": run_id})
                .raise_for_status()
                .json()["effect"]
            )
            assert effect["message_id"] == message_id
            assert effect["idempotency_key"] == key
            if number >= 7:
                assert (
                    states.index("DEFERRED")
                    < states.index("RECHECKING")
                    < states.index("REPLAYED")
                )
                assert message["recheck_attempts"] >= 1
                assert message["transient_recovered"] is True
                assert "recheck" in kinds
        elif number <= 11:
            assert message["current_state"] == "ESCALATED"
            assert message["failure_class"] == "invalid_data"
            assert message["proposed_fix"]
            assert message["idempotency_key"] is None
            assert "escalation" in kinds
            assert "replay_attempt" not in kinds
        else:
            assert message["current_state"] == "QUARANTINED"
            assert message["contains_injection_attempt"] is True
            assert message["idempotency_key"] is None
            assert "refusal" in kinds
            assert "replay_attempt" not in kinds
    proof = (
        client.get("/api/messages/synthetic-order-001/proof", params={"run_id": run_id})
        .raise_for_status()
        .json()["proof"]
    )
    assert proof["delivery_count_label"] == "deliveries recorded by RetryPermit"
    assert proof["execution_attempt_count"] == (2 if inject_failure else 1)
    assert proof["downstream_request_count"] == (2 if inject_failure else 1)
    assert proof["unique_downstream_effect_count"] == 1
    assert proof["ledger_status"] == "CONFIRMED"
    # Repeating Start is checked during a fresh live run, never while validating
    # archived evidence: the Start endpoint intentionally targets the active run.
    if repeat_start:
        endpoint = (
            "/api/demo/start-with-failure" if inject_failure else "/api/demo/start"
        )
        before = client.get(
            "/api/messages/synthetic-order-001/receipts", params={"run_id": run_id}
        ).json()["items"]
        assert (
            client.post(endpoint, headers=headers, json={})
            .raise_for_status()
            .json()["status"]
            == "COMPLETE"
        )
        after = client.get(
            "/api/messages/synthetic-order-001/receipts", params={"run_id": run_id}
        ).json()["items"]
        assert after == before
    print(
        json.dumps(
            {
                "verified_run": run_id,
                "injected_lost_response": inject_failure,
                "effect_proof": proof,
            }
        ),
        flush=True,
    )
    assert len(keys) == 9
    return keys


def _verify_firestore(project, runs):
    # Reset archives the prior COMPLETE run as INACTIVE, retaining completed_at
    # and all child evidence. Only the latest run stays active and COMPLETE.
    db = firestore.Client(project=project)
    for index, (run_id, version, keys) in enumerate(runs):
        ref = db.collection("demo_runs").document(run_id)
        record = ref.get().to_dict()
        latest = index == len(runs) - 1
        assert record["status"] == ("COMPLETE" if latest else "INACTIVE")
        assert record["active"] is latest
        assert record["completed_at"] is not None
        assert record["active_policy_version"] == version
        assert record["inject_failure"] is (version == "runbook-v1")
        assert len(list(ref.collection("messages").stream())) == 12
        effects = list(ref.collection("downstream_effects").stream())
        assert len(effects) == 9
        assert {effect.id for effect in effects} == keys
        print(
            f"Independent Firestore proof: {run_id}; 12 messages, 9 unique effects, "
            f"completed_at={record['completed_at']}, status={record['status']}.",
            flush=True,
        )


def _revalidate_existing(client, project, run_ids):
    """Read-only continuation of a recorded live test, never a fresh-run claim."""
    assert len(run_ids) == 2 and len(set(run_ids)) == 2
    print(
        "READ-ONLY evidence revalidation: no new Gemini calls, runs or policy mutations.",
        flush=True,
    )
    runs = []
    for run_id, version, injected in zip(
        run_ids, ("runbook-v1", "runbook-v3"), (True, False), strict=True
    ):
        stats = (
            client.get("/api/stats", params={"run_id": run_id})
            .raise_for_status()
            .json()
        )
        assert {key: stats[key] for key in EXPECTED} == EXPECTED
        assert stats["active_policy_version"] == version
        assert stats["durable_delivery"] is True
        assert stats["demo_recheck_seconds"] is None
        assert stats["model"] != "deterministic-fixture"
        keys = _verify_run(client, {}, run_id, injected, repeat_start=False)
        runs.append((run_id, version, keys))
    policies = client.get("/api/policies").raise_for_status().json()
    active = policies["active_policy"]
    expected = PolicyDefinition.model_validate_json(
        (FIXTURES / "policies/runbook-v3.json").read_text()
    )
    assert PolicyDefinition.model_validate(active["definition"]) == expected
    assert active["source_kind"] == "gemini_pdf_extraction"
    assert (
        active["source_hash"]
        == hashlib.sha256((FIXTURES / "dlq-runbook-v3.pdf").read_bytes()).hexdigest()
    )
    assert active["extracted_policy_hash"] == policy_definition_hash(expected)
    assert any(e["policy_version"] == "runbook-v3" for e in policies["approval_events"])
    _verify_firestore(project, runs)


@pytest.mark.gcp
@pytest.mark.deployed
def test_deployed_vertical_slice_uses_real_transport_and_model():
    base_url = os.getenv("RETRYPERMIT_DEPLOYED_URL", "").rstrip("/")
    admin_token = os.getenv("RETRYPERMIT_DEPLOYED_ADMIN_TOKEN", "")
    project = os.getenv("RETRYPERMIT_GCP_PROJECT", "")
    if (
        os.getenv("RUN_GCP_TESTS") != "1"
        or not base_url
        or not admin_token
        or not project
    ):
        pytest.skip(
            "explicit RUN_GCP_TESTS=1, deployed URL, admin token and project required"
        )
    assert (
        base_url.startswith("https://")
        and "localhost" not in base_url
        and "127.0.0.1" not in base_url
    )
    headers = {"X-Admin-Token": admin_token}
    runs = []
    with httpx.Client(base_url=base_url, timeout=90) as client:
        health = client.get("/health").raise_for_status().json()
        assert health["mode"] == "Google Cloud" and health["environment"] == "cloud"
        assert health["store"] == "Firestore"
        assert health["model_provider"].startswith("Google ADK / ")
        assert "fixture" not in health["model_provider"].lower()
        assert health["synthetic_data"] and health["simulated_downstream"]
        existing = os.getenv("RETRYPERMIT_VERIFY_EXISTING_RUN_IDS", "")
        if existing:
            _revalidate_existing(client, project, existing.split(","))
            return
        assert client.post("/api/demo/reset", json={}).status_code == 401
        assert (
            client.post(
                "/internal/recovery/sweep", headers=headers, json={}
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/internal/recovery/sweep",
                headers={"X-RetryPermit-Local-Service": "1"},
                json={},
            ).status_code
            == 401
        )
        before = _fingerprints(client.get("/api/policies").raise_for_status().json())
        _prepare_v3(client, headers)
        for version, inject_failure, forbidden in (
            ("runbook-v1", True, "runbook-v3"),
            ("runbook-v3", False, "runbook-v1"),
        ):
            client.post(
                f"/api/policies/{version}/activate", headers=headers, json=ACTOR
            ).raise_for_status()
            run_id = (
                client.post("/api/demo/reset", headers=headers, json={})
                .raise_for_status()
                .json()["run_id"]
            )
            endpoint = (
                "/api/demo/start-with-failure" if inject_failure else "/api/demo/start"
            )
            client.post(endpoint, headers=headers, json={}).raise_for_status()
            stats = _wait_for_completion(client, headers, run_id, forbidden)
            assert stats["active_policy_version"] == version
            keys = _verify_run(client, headers, run_id, inject_failure)
            runs.append((run_id, version, keys))
        policies = client.get("/api/policies").raise_for_status().json()
        after = _fingerprints(policies)
        for version, fingerprint in before.items():
            assert after[version] == fingerprint, (
                "Existing immutable policy was changed"
            )
        assert policies["active_policy"]["definition"]["version"] == "runbook-v3"
        assert any(
            e["policy_version"] == "runbook-v3" for e in policies["approval_events"]
        )

    _verify_firestore(project, runs)
