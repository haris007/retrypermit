import base64
import json

import pytest
from fastapi.testclient import TestClient

from retrypermit.adapters.tasks.cloud_tasks import RecordingTaskScheduler
from retrypermit.application.delivery_service import delivery_key
from retrypermit.config import Settings
from retrypermit.domain.enums import InboxStatus, MessageState, ReceiptKind, RunStatus
from retrypermit.main import create_app


ADMIN_TOKEN = "test-only-admin-token-long-enough"
LOCAL_SERVICE_HEADERS = {"X-RetryPermit-Local-Service": "1"}


@pytest.fixture
def client():
    settings = Settings(
        _env_file=None,
        demo_admin_token=ADMIN_TOKEN,
        demo_mode=True,
        use_in_memory_store=True,
        use_fake_model=True,
        allow_local_service_auth=True,
    )
    with TestClient(create_app(settings)) as value:
        yield value


def _pubsub_envelope(run_id: str, message: dict, transport_id: str = "ps-001"):
    inner = {
        "run_id": run_id,
        "message_id": message["message_id"],
        "source_topic": message["source_topic"],
        "payload": message["original_payload"],
    }
    return {
        "message": {
            "data": base64.b64encode(json.dumps(inner).encode()).decode(),
            "messageId": transport_id,
        },
        "subscription": "projects/demo/subscriptions/orders-dlq-push",
    }


def test_health_admin_auth_reset_and_history_preservation(client: TestClient) -> None:
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["mode"] == "Local / in-memory"
    assert health.json()["synthetic_data"] is True
    assert health.json()["simulated_downstream"] is True

    denied = client.post("/api/demo/reset", json={})
    assert denied.status_code == 401
    assert denied.json()["error"]["code"] == "AUTH_REQUIRED"
    assert denied.json()["error"]["trace_id"]

    runtime = client.app.state.runtime
    first_run = client.get("/api/stats").json()["run_id"]
    reset = client.post(
        "/api/demo/reset", json={}, headers={"X-Admin-Token": ADMIN_TOKEN}
    )
    assert reset.status_code == 200
    second_run = reset.json()["run_id"]
    assert second_run != first_run
    assert reset.json()["message_count"] == 6
    historical = _run(runtime, first_run)
    assert historical.status == RunStatus.INACTIVE
    assert historical.active is False


def test_api_start_runs_six_messages_end_to_end(client: TestClient) -> None:
    response = client.post(
        "/api/demo/start", json={}, headers={"X-Admin-Token": ADMIN_TOKEN}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETE"
    stats = client.get("/api/stats").json()
    assert stats["resolved_count"] == 6
    assert stats["dlq_depth"] == 0
    assert stats["durable_delivery"] is False

    messages = client.get("/api/messages").json()["messages"]
    assert len(messages) == 6
    assert {item["current_state"] for item in messages} == {"REPLAYED"}
    for message in messages:
        receipts = client.get(f"/api/messages/{message['message_id']}/receipts").json()[
            "items"
        ]
        kinds = {item["kind"] for item in receipts}
        assert "pubsub_publish_attempt" in kinds
        assert "pubsub_publish_result" in kinds
        assert any(
            item["kind"] == "replay_result" and item["status"] == "CONFIRMED"
            for item in receipts
        )


def test_duplicate_pubsub_push_and_task_produce_one_downstream_effect(
    client: TestClient,
) -> None:
    runtime = client.app.state.runtime
    stats = client.get("/api/stats").json()
    message = client.get("/api/messages").json()["messages"][0]
    envelope = _pubsub_envelope(stats["run_id"], message)

    first = client.post("/pubsub/dlq", json=envelope, headers=LOCAL_SERVICE_HEADERS)
    second = client.post("/pubsub/dlq", json=envelope, headers=LOCAL_SERVICE_HEADERS)
    assert first.status_code == second.status_code == 204

    inbox_id = delivery_key(envelope["subscription"], "ps-001")
    task_body = {"run_id": stats["run_id"], "inbox_id": inbox_id}
    first_task = client.post(
        "/internal/tasks/process",
        json=task_body,
        headers={**LOCAL_SERVICE_HEADERS, "X-CloudTasks-TaskName": "task-one"},
    )
    second_task = client.post(
        "/internal/tasks/process",
        json=task_body,
        headers={**LOCAL_SERVICE_HEADERS, "X-CloudTasks-TaskName": "task-one"},
    )
    assert first_task.status_code == second_task.status_code == 200
    assert first_task.json()["status"] == "REPLAYED"
    assert second_task.json()["status"] == "REPLAYED"

    effects = _effects(runtime, stats["run_id"])
    assert len(effects) == 1
    assert effects[0].submission_count == 1
    stored = _message(runtime, stats["run_id"], message["message_id"])
    assert stored.current_state == MessageState.REPLAYED
    receipts = _receipts(runtime, stats["run_id"], message["message_id"])
    delivery_receipts = [item for item in receipts if item.kind == ReceiptKind.DELIVERY]
    assert len(delivery_receipts) == 2
    assert delivery_receipts[-1].details["duplicate"] is True


def test_pubsub_ack_waits_for_durable_task_schedule(client: TestClient) -> None:
    runtime = client.app.state.runtime
    stats = client.get("/api/stats").json()
    message = client.get("/api/messages").json()["messages"][0]
    envelope = _pubsub_envelope(stats["run_id"], message, "ps-schedule-gap")

    class FailingScheduler:
        async def schedule(self, **_kwargs):
            raise RuntimeError("synthetic scheduler outage")

    runtime.delivery._scheduler = FailingScheduler()
    failed = client.post("/pubsub/dlq", json=envelope, headers=LOCAL_SERVICE_HEADERS)
    assert failed.status_code == 503
    assert failed.json()["error"]["code"] == "TASK_SCHEDULE_FAILED"

    key = delivery_key(envelope["subscription"], "ps-schedule-gap")
    inbox = runtime.store._inbox[(stats["run_id"], key)]
    assert inbox.status == InboxStatus.PENDING
    assert inbox.task_name is None

    replacement = RecordingTaskScheduler()
    runtime.delivery._scheduler = replacement
    recovered = client.post("/pubsub/dlq", json=envelope, headers=LOCAL_SERVICE_HEADERS)
    assert recovered.status_code == 204
    assert len(replacement.tasks) == 1
    inbox = runtime.store._inbox[(stats["run_id"], key)]
    assert inbox.status == InboxStatus.ENQUEUED
    receipts = _receipts(runtime, stats["run_id"], message["message_id"])
    results = [
        item for item in receipts if item.kind == ReceiptKind.TASK_SCHEDULE_RESULT
    ]
    assert {item.status.value for item in results} == {"FAILED", "CONFIRMED"}


def test_pubsub_and_task_routes_require_service_identity(client: TestClient) -> None:
    stats = client.get("/api/stats").json()
    message = client.get("/api/messages").json()["messages"][0]
    denied = client.post("/pubsub/dlq", json=_pubsub_envelope(stats["run_id"], message))
    assert denied.status_code == 401
    assert denied.json()["error"]["code"] == "AUTH_REQUIRED"


def _run(runtime, run_id: str):
    import asyncio

    return asyncio.run(runtime.store.get_run(run_id))


def _message(runtime, run_id: str, message_id: str):
    import asyncio

    return asyncio.run(runtime.store.get_message(run_id, message_id))


def _effects(runtime, run_id: str):
    import asyncio

    return asyncio.run(runtime.store.list_downstream_effects(run_id))


def _receipts(runtime, run_id: str, message_id: str):
    import asyncio

    return asyncio.run(runtime.store.list_receipts(run_id, message_id))
