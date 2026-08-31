import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from retrypermit.adapters.downstream.store_backed import StoreBackedDownstream
from retrypermit.adapters.models.fake import DeterministicFakeModelProvider
from retrypermit.adapters.pubsub.google_publisher import RecordingPublisher
from retrypermit.application.orchestrator import RetryPermitOrchestrator
from retrypermit.config import Settings
from retrypermit.domain.enums import MessageState
from retrypermit.domain.errors import RunConflictError
from retrypermit.main import create_app


HEADERS = {"X-Admin-Token": "local-reliability-tests-only"}


@pytest.fixture
def client():
    settings = Settings(
        _env_file=None,
        app_env="local",
        demo_mode=True,
        use_in_memory_store=True,
        use_fake_model=True,
        demo_admin_token=HEADERS["X-Admin-Token"],
        replay_retry_base_seconds=0,
        replay_retry_max_seconds=0,
        demo_recheck_seconds=0.01,
        simulated_transient_recovery_seconds=0.01,
    )
    with TestClient(create_app(settings)) as value:
        yield value


def _approve_v2(client):
    response = client.post(
        "/api/policies/extract",
        headers=HEADERS,
        files={
            "file": (
                "runbook-v2.pdf",
                Path("fixtures/dlq-runbook-v2.pdf").read_bytes(),
                "application/pdf",
            )
        },
    )
    assert response.status_code == 200
    response = client.post(
        "/api/policies/runbook-v2/approve",
        headers=HEADERS,
        json={"actor": "local-regression"},
    )
    assert response.status_code == 200


async def _deliver_one(runtime, run_id, message_id):
    message = await runtime.store.get_message(run_id, message_id)
    registration = await runtime.delivery.register(
        run_id=run_id,
        subscription="local-regression",
        source_message_id=message_id,
        transport_message_id=f"local-regression-{message_id}",
        source_topic=message.source_topic,
        payload=message.original_payload,
        trace_id="local-regression",
    )
    return await runtime.delivery.process_inbox(
        run_id=run_id,
        inbox_id=registration.inbox.inbox_id,
        worker_id="local-regression-worker",
        trace_id="local-regression",
    )


def test_policy_switch_is_rejected_while_a_message_is_deferred(client):
    runtime = client.app.state.runtime
    run_id = client.get("/api/stats").json()["run_id"]
    first = client.portal.call(_deliver_one, runtime, run_id, "synthetic-order-007")
    assert first.current_state == MessageState.DEFERRED
    _approve_v2(client)
    events_before = client.get("/api/policies").json()["activation_events"]
    response = client.post(
        "/api/policies/runbook-v2/activate",
        headers=HEADERS,
        json={"actor": "local-regression"},
    )
    assert response.status_code == 409
    policies = client.get("/api/policies").json()
    assert policies["active_policy"]["definition"]["version"] == "runbook-v1"
    assert policies["activation_events"] == events_before
    # The rejection must leave the original run resumable and its authority intact.
    response = client.post("/api/demo/start", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETE"
    assert client.get("/api/stats").json()["replayed_count"] == 9
    assert (
        client.post(
            "/api/policies/runbook-v2/activate",
            headers=HEADERS,
            json={"actor": "local-regression"},
        ).status_code
        == 200
    )


def test_policy_switch_is_rejected_after_an_ambiguous_effect(client):
    runtime = client.app.state.runtime
    run_id = client.get("/api/stats").json()["run_id"]
    client.portal.call(runtime.store.set_run_inject_failure, run_id, True)
    first = client.portal.call(_deliver_one, runtime, run_id, "synthetic-order-001")
    assert first.current_state == MessageState.FAILED_RETRYABLE
    assert len(client.portal.call(runtime.store.list_downstream_effects, run_id)) == 1
    _approve_v2(client)
    assert (
        client.post(
            "/api/policies/runbook-v2/activate",
            headers=HEADERS,
            json={"actor": "local-regression"},
        ).status_code
        == 409
    )
    response = client.post("/api/demo/start-with-failure", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETE"
    proof = client.get("/api/messages/synthetic-order-001/proof").json()["proof"]
    assert proof["execution_attempt_count"] == 2
    assert proof["unique_downstream_effect_count"] == 1
    assert proof["ledger_status"] == "CONFIRMED"


def test_second_worker_revalidates_cached_policy_and_ready_run_must_reset(client):
    runtime = client.app.state.runtime
    run_id = client.get("/api/stats").json()["run_id"]
    message = client.portal.call(
        runtime.store.get_message, run_id, "synthetic-order-007"
    )
    worker = RetryPermitOrchestrator(
        runtime.store,
        DeterministicFakeModelProvider(),
        StoreBackedDownstream(runtime.store),
    )
    assert (
        client.portal.call(worker._active_policy_for, message).version == "runbook-v1"
    )
    _approve_v2(client)
    assert (
        client.post(
            "/api/policies/runbook-v2/activate",
            headers=HEADERS,
            json={"actor": "local-regression"},
        ).status_code
        == 200
    )
    with pytest.raises(RunConflictError, match="no longer active"):
        client.portal.call(worker._active_policy_for, message)
    assert worker._policy_cache == {}
    assert client.post("/api/demo/start", headers=HEADERS).status_code == 409
    assert runtime.publisher.messages == []
    assert client.post("/api/demo/reset", headers=HEADERS).status_code == 200
    new_run_id = client.get("/api/stats").json()["run_id"]
    new_message = client.portal.call(
        runtime.store.get_message, new_run_id, "synthetic-order-007"
    )
    assert (
        client.portal.call(worker._active_policy_for, new_message).version
        == "runbook-v2"
    )


class FailSecondPublishOnce(RecordingPublisher):
    def __init__(self):
        super().__init__()
        self.failed = False

    async def publish(self, **kwargs):
        if len(self.messages) == 1 and not self.failed:
            self.failed = True
            raise RuntimeError("Synthetic partial publication failure")
        return await super().publish(**kwargs)


@pytest.mark.parametrize("start_first", [False, True])
def test_start_and_policy_activation_cannot_both_win(client, start_first):
    runtime = client.app.state.runtime
    run_id = client.get("/api/stats").json()["run_id"]
    _approve_v2(client)

    async def race():
        start = runtime.store.mark_run_running(run_id, inject_failure=True)
        activate = runtime.store.activate_policy(
            "synthetic-demo", "runbook-v2", "local-race"
        )
        operations = [start, activate] if start_first else [activate, start]
        results = await asyncio.gather(*operations, return_exceptions=True)
        assert sum(isinstance(result, RunConflictError) for result in results) == 1
        run = await runtime.store.get_run(run_id)
        active = await runtime.store.get_active_policy_version("synthetic-demo")
        if start_first:
            assert run.status.value == "RUNNING"
            assert active == run.active_policy_version == "runbook-v1"
            assert run.inject_failure is True
        else:
            assert run.status.value == "READY"
            assert active == "runbook-v2"
            assert run.inject_failure is False

    client.portal.call(race)


@pytest.mark.parametrize("inject_failure", [False, True])
def test_start_resumes_after_partial_publication_and_preserves_effects(
    client, inject_failure
):
    runtime = client.app.state.runtime
    publisher = FailSecondPublishOnce()
    runtime.publisher = publisher
    endpoint = "/api/demo/start-with-failure" if inject_failure else "/api/demo/start"
    first = client.post(endpoint, headers=HEADERS)
    assert first.status_code == 503
    assert len(publisher.messages) == 1
    run_id = client.get("/api/stats").json()["run_id"]
    before = client.portal.call(runtime.store.get_run, run_id)
    assert before.status.value == "RUNNING"
    assert before.inject_failure is inject_failure

    # A conflicting retry must not publish or silently change the mode.
    other = "/api/demo/start" if inject_failure else "/api/demo/start-with-failure"
    assert client.post(other, headers=HEADERS).status_code == 409
    assert len(publisher.messages) == 1

    retry = client.post(endpoint, headers=HEADERS)
    assert retry.status_code == 200
    assert retry.json()["status"] == "COMPLETE"
    after = client.portal.call(runtime.store.get_run, run_id)
    assert after.started_at == before.started_at
    assert after.inject_failure is inject_failure
    stats = client.get("/api/stats").json()
    assert stats["replayed_count"] == 9
    assert stats["escalated_count"] == 2
    assert stats["quarantined_count"] == 1
    assert len(client.portal.call(runtime.store.list_downstream_effects, run_id)) == 9
    proof = client.get("/api/messages/synthetic-order-001/proof").json()["proof"]
    assert proof["unique_downstream_effect_count"] == 1
    assert proof["execution_attempt_count"] == (2 if inject_failure else 1)
    publications = len(publisher.messages)
    assert client.post(endpoint, headers=HEADERS).json()["status"] == "COMPLETE"
    assert len(publisher.messages) == publications
