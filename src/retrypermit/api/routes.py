from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, File, Query, Request, Response, UploadFile
from pydantic import ValidationError

from retrypermit.api.auth import require_admin, verify_service_identity
from retrypermit.api.schemas import (
    DeliveryPayload,
    DemoActionResponse,
    DownstreamOrderRequest,
    PolicyMutationRequest,
    PubSubEnvelope,
    RecoveryRequest,
    TaskProcessRequest,
    TaskRecoverRequest,
)
from retrypermit.domain.enums import (
    InboxStatus,
    MessageState,
    ReceiptKind,
    ReceiptStatus,
    RunStatus,
)
from retrypermit.domain.errors import NotFoundError, PolicyValidationError
from retrypermit.domain.receipts import Receipt
from retrypermit.domain.state_machine import is_terminal
from retrypermit.errors import ConflictError, DependencyUnavailable, RetryPermitError
from retrypermit.runtime import Runtime


def _runtime(request: Request) -> Runtime:
    return request.app.state.runtime


def _trace_id(request: Request) -> str:
    return str(getattr(request.state, "trace_id", uuid.uuid4().hex))


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


async def _selected_run(runtime: Runtime, run_id: str | None):
    if run_id:
        return await runtime.store.get_run(run_id)
    return await runtime.demo.get_active()


async def _record_publish_receipt(
    runtime: Runtime,
    *,
    message,
    operation_id: str,
    status: ReceiptStatus,
    kind: ReceiptKind,
    trace_id: str,
    transport_message_id: str | None = None,
    error_code: str | None = None,
) -> None:
    await runtime.store.record_receipt(
        Receipt(
            receipt_id=f"rcpt_{uuid.uuid4().hex}",
            operation_id=operation_id,
            run_id=message.run_id,
            message_id=message.message_id,
            kind=kind,
            status=status,
            stage="pubsub_publish",
            trace_id=trace_id,
            policy_version=message.active_policy_version,
            payload_hash=message.original_payload_hash,
            error_code=error_code,
            retryable=status == ReceiptStatus.FAILED,
            details={
                "topic": runtime.settings.pubsub_topic,
                "transport_message_id": transport_message_id,
                "local_inline": runtime.settings.use_in_memory_store,
            },
            created_at=datetime.now(UTC),
        )
    )


async def _start_active_demo(
    runtime: Runtime, *, trace_id: str, inject_failure: bool
) -> DemoActionResponse:
    run = await runtime.demo.get_active()
    run = await runtime.store.set_run_inject_failure(run.run_id, inject_failure)
    await runtime.store.mark_run_running(run.run_id)
    current = await runtime.store.list_messages(run.run_id)
    local_inbox_ids: list[str] = []

    for message in current:
        if message.current_state != MessageState.RECEIVED:
            continue
        operation_id = f"op_{uuid.uuid4().hex}"
        await _record_publish_receipt(
            runtime,
            message=message,
            operation_id=operation_id,
            status=ReceiptStatus.STARTED,
            kind=ReceiptKind.PUBSUB_PUBLISH_ATTEMPT,
            trace_id=trace_id,
        )
        body = {
            "run_id": run.run_id,
            "message_id": message.message_id,
            "source_topic": message.source_topic,
            "payload": message.original_payload,
        }
        try:
            transport_id = await runtime.publisher.publish(
                topic=runtime.settings.pubsub_topic,
                payload=body,
                attributes={
                    "run_id": run.run_id,
                    "source_message_id": message.message_id,
                    "synthetic": "true",
                },
            )
        except Exception as exc:
            await _record_publish_receipt(
                runtime,
                message=message,
                operation_id=operation_id,
                status=ReceiptStatus.FAILED,
                kind=ReceiptKind.PUBSUB_PUBLISH_RESULT,
                trace_id=trace_id,
                error_code="PUBSUB_PUBLISH_FAILED",
            )
            raise DependencyUnavailable(
                "PUBSUB_PUBLISH_FAILED",
                "A synthetic message could not be published.",
                trace_id,
            ) from exc
        await _record_publish_receipt(
            runtime,
            message=message,
            operation_id=operation_id,
            status=ReceiptStatus.CONFIRMED,
            kind=ReceiptKind.PUBSUB_PUBLISH_RESULT,
            trace_id=trace_id,
            transport_message_id=transport_id,
        )

        if runtime.settings.use_in_memory_store:
            registration = await runtime.delivery.register(
                run_id=run.run_id,
                subscription="local-inline-bypass",
                source_message_id=message.message_id,
                transport_message_id=transport_id,
                source_topic=message.source_topic,
                payload=message.original_payload,
                trace_id=trace_id,
            )
            local_inbox_ids.append(registration.inbox.inbox_id)

    if local_inbox_ids:
        await asyncio.gather(
            *(
                runtime.delivery.process_inbox(
                    run_id=run.run_id,
                    inbox_id=inbox_id,
                    worker_id=f"local_{uuid.uuid4().hex}",
                    trace_id=trace_id,
                )
                for inbox_id in local_inbox_ids
            )
        )

    # The local fake executes recorded task intents in schedule order. This is a
    # deterministic demo harness for per-message tasks, not a production sweep loop.
    if runtime.settings.use_in_memory_store:
        handled_tasks: set[str] = set()
        for _ in range(96):
            latest = await runtime.store.list_messages(run.run_id)
            if all(is_terminal(item.current_state) for item in latest):
                break
            recorded = getattr(runtime.scheduler, "tasks", {})
            pending = [
                (name, task)
                for name, task in recorded.items()
                if name not in handled_tasks
                and task.get("body", {}).get("run_id") == run.run_id
            ]
            if not pending:
                break
            task_name, task = min(
                pending,
                key=lambda item: (item[1]["schedule_time"], item[0]),
            )
            delay = max(
                0.0,
                (task["schedule_time"] - datetime.now(UTC)).total_seconds(),
            )
            if delay:
                await asyncio.sleep(delay)
            handled_tasks.add(task_name)
            body = task["body"]
            if task["endpoint"] == "/internal/tasks/process":
                await runtime.delivery.process_inbox(
                    run_id=body["run_id"],
                    inbox_id=body["inbox_id"],
                    worker_id=f"local_task_{uuid.uuid4().hex}",
                    trace_id=trace_id,
                )
            elif task["endpoint"] == "/internal/tasks/recover-message":
                await runtime.delivery.process_recovery(
                    run_id=body["run_id"],
                    message_id=body["message_id"],
                    worker_id=f"local_recovery_{uuid.uuid4().hex}",
                    trace_id=trace_id,
                )

    final_run = await runtime.store.get_run(run.run_id)
    return DemoActionResponse(
        run_id=run.run_id,
        status=final_run.status.value,
        message_count=len(current),
        details={
            "transport": (
                "local recorded-task simulation, non-durable"
                if runtime.settings.use_in_memory_store
                else "Google Cloud Pub/Sub"
            ),
            "inject_failure": inject_failure,
            "failure_semantics": (
                "one simulated 503 after a real idempotent effect is committed"
                if inject_failure
                else "none"
            ),
            "synthetic_data": True,
            "simulated_downstream": True,
            "recheck_strategy": "per-message scheduled tasks; recovery scheduler is a safety net",
            "demo_recheck_seconds": runtime.settings.demo_recheck_seconds,
        },
    )


def register_routes(app: FastAPI) -> None:
    @app.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        runtime = _runtime(request)
        return {
            "status": "ok",
            "service": "RetryPermit",
            "mode": runtime.settings.mode_label,
            "environment": runtime.settings.app_env,
            "store": (
                "in-memory (non-durable)"
                if runtime.settings.use_in_memory_store
                else "Firestore"
            ),
            "model_provider": (
                "deterministic fixture"
                if runtime.settings.use_fake_model
                else f"Google ADK / {runtime.settings.gemini_model}"
            ),
            "synthetic_data": True,
            "simulated_downstream": True,
            "cloud_deployment_verified": runtime.settings.cloud_deployment_verified,
        }

    @app.get("/api/stats")
    async def stats(
        request: Request, run_id: str | None = Query(default=None)
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        run = await _selected_run(runtime, run_id)
        messages = await runtime.store.list_messages(run.run_id)
        resolved = sum(
            message.current_state == MessageState.REPLAYED for message in messages
        )
        state_counts = {
            state.value: sum(message.current_state == state for message in messages)
            for state in MessageState
        }
        deferred_total = sum(
            message.failure_class is not None
            and message.failure_class.value == "transient_downstream"
            for message in messages
        )
        end = run.completed_at or datetime.now(UTC)
        elapsed = (
            max(0.0, (end - run.started_at).total_seconds()) if run.started_at else 0.0
        )
        return {
            "run_id": run.run_id,
            "run_status": run.status.value,
            "mode": runtime.settings.mode_label,
            "total": len(messages),
            "dlq_depth": len(messages) - resolved,
            "resolved_count": resolved,
            "replayed_count": state_counts[MessageState.REPLAYED.value],
            "deferred_count": state_counts[MessageState.DEFERRED.value],
            "deferred_total": deferred_total,
            "escalated_count": state_counts[MessageState.ESCALATED.value],
            "quarantined_count": state_counts[MessageState.QUARANTINED.value],
            "state_counts": state_counts,
            "elapsed_seconds": round(elapsed, 3),
            "active_policy_version": run.active_policy_version,
            "synthetic_data": True,
            "simulated_downstream": True,
            "durable_delivery": not runtime.settings.use_in_memory_store,
            "model": (
                "deterministic-fixture"
                if runtime.settings.use_fake_model
                else runtime.settings.gemini_model
            ),
            "demo_recheck_seconds": (
                runtime.settings.demo_recheck_seconds
                if runtime.settings.demo_mode
                else None
            ),
            "recheck_mode": "per-message scheduled task with Scheduler recovery net",
        }

    @app.get("/api/messages")
    async def messages(
        request: Request, run_id: str | None = Query(default=None)
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        run = await _selected_run(runtime, run_id)
        items = await runtime.store.list_messages(run.run_id)
        return {"run_id": run.run_id, "messages": [_dump(item) for item in items]}

    @app.get("/api/messages/{message_id}")
    async def message_detail(
        message_id: str,
        request: Request,
        run_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        run = await _selected_run(runtime, run_id)
        message = await runtime.store.get_message(run.run_id, message_id)
        return {"message": _dump(message)}

    @app.get("/api/messages/{message_id}/transitions")
    async def transitions(
        message_id: str,
        request: Request,
        run_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        run = await _selected_run(runtime, run_id)
        items = await runtime.store.list_transitions(run.run_id, message_id)
        return {
            "run_id": run.run_id,
            "message_id": message_id,
            "items": [_dump(item) for item in items],
        }

    @app.get("/api/messages/{message_id}/receipts")
    async def receipts(
        message_id: str,
        request: Request,
        run_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        run = await _selected_run(runtime, run_id)
        items = await runtime.store.list_receipts(run.run_id, message_id)
        return {
            "run_id": run.run_id,
            "message_id": message_id,
            "items": [_dump(item) for item in items],
        }

    @app.get("/api/messages/{message_id}/proof")
    async def message_proof(
        message_id: str,
        request: Request,
        run_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        run = await _selected_run(runtime, run_id)
        proof = await runtime.proof.prove_effectively_once(run.run_id, message_id)
        return {"proof": proof.model_dump(mode="json")}

    @app.get("/api/policies")
    async def policies(request: Request) -> dict[str, Any]:
        runtime = _runtime(request)
        active = await runtime.store.get_active_policy(runtime.settings.tenant_id)
        items = []
        for policy in await runtime.policies.list(runtime.settings.tenant_id):
            item = policy.model_dump(mode="json")
            item["effective_replay_cap"] = str(policy.definition.effective_replay_cap)
            items.append(item)
        return {
            "active_policy": next(
                item
                for item in items
                if item["definition"]["version"] == active.version
            ),
            "items": items,
            "approval_events": [
                event.model_dump(mode="json")
                for event in await runtime.store.list_policy_approval_events(
                    runtime.settings.tenant_id
                )
            ],
            "activation_events": [
                event.model_dump(mode="json")
                for event in await runtime.store.list_policy_activation_events(
                    runtime.settings.tenant_id
                )
            ],
        }

    @app.post("/api/policies/extract")
    async def extract_policy(
        request: Request, file: UploadFile = File(...)
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        if file.content_type != "application/pdf":
            raise PolicyValidationError("policy upload must use application/pdf")
        pdf_bytes = await file.read(5_000_001)
        policy = await runtime.policies.extract_pdf(pdf_bytes)
        return {
            "policy": policy.model_dump(mode="json"),
            "effective_replay_cap": str(policy.definition.effective_replay_cap),
            "authority": "pending approval; cannot authorize an action",
        }

    @app.post("/api/policies/{version}/approve")
    async def approve_policy(
        version: str, body: PolicyMutationRequest, request: Request
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        policy = await runtime.policies.approve(
            runtime.settings.tenant_id, version, body.actor
        )
        return {"policy": policy.model_dump(mode="json")}

    @app.post("/api/policies/{version}/activate")
    async def activate_policy(
        version: str, body: PolicyMutationRequest, request: Request
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        policy = await runtime.policies.activate(
            runtime.settings.tenant_id, version, body.actor
        )
        runtime.orchestrator.invalidate_policy_cache()
        return {"policy": policy.model_dump(mode="json")}

    @app.post("/api/demo/reset", response_model=DemoActionResponse)
    async def reset_demo(request: Request) -> DemoActionResponse:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        run = await runtime.demo.reset()
        return DemoActionResponse(
            run_id=run.run_id,
            status=run.status.value,
            message_count=run.seeded_message_count,
            details={
                "history_preserved": True,
                "synthetic_data": True,
                "simulated_downstream": True,
            },
        )

    @app.post("/api/demo/seed", response_model=DemoActionResponse)
    async def seed_demo(request: Request) -> DemoActionResponse:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        try:
            run = await runtime.demo.get_active()
            current = await runtime.store.list_messages(run.run_id)
            ready = (
                run.status == RunStatus.READY
                and len(current) == run.seeded_message_count
                and all(item.current_state == MessageState.RECEIVED for item in current)
            )
            if not ready:
                run = await runtime.demo.reset()
        except NotFoundError:
            run = await runtime.demo.reset()
        return DemoActionResponse(
            run_id=run.run_id,
            status=run.status.value,
            message_count=run.seeded_message_count,
            details={"already_seeded": ready if "ready" in locals() else False},
        )

    @app.post("/api/demo/start", response_model=DemoActionResponse)
    async def start_demo(request: Request) -> DemoActionResponse:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        return await _start_active_demo(
            runtime, trace_id=_trace_id(request), inject_failure=False
        )

    @app.post("/api/demo/start-with-failure", response_model=DemoActionResponse)
    async def start_demo_with_failure(request: Request) -> DemoActionResponse:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        return await _start_active_demo(
            runtime, trace_id=_trace_id(request), inject_failure=True
        )

    @app.post("/api/recheck/run")
    async def run_recheck(request: Request, body: RecoveryRequest) -> dict[str, Any]:
        """Manual test nudge; production recovery remains Scheduler -> task intents."""

        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        result = await runtime.delivery.schedule_recovery(
            limit=body.limit, trace_id=_trace_id(request)
        )
        return {
            **result.model_dump(mode="json"),
            "execution": "task intents scheduled; no sweep-side business effect",
        }

    @app.post("/pubsub/dlq", status_code=204)
    async def pubsub_dlq(request: Request, envelope: PubSubEnvelope) -> Response:
        runtime = _runtime(request)
        verify_service_identity(
            request,
            settings=runtime.settings,
            expected_email=runtime.settings.pubsub_push_service_account,
        )
        trace_id = _trace_id(request)
        try:
            raw = base64.b64decode(envelope.message.data, validate=True)
            decoded = json.loads(raw.decode("utf-8"))
            payload = DeliveryPayload.model_validate(decoded)
        except (
            binascii.Error,
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValidationError,
        ) as exc:
            raise RetryPermitError(
                code="INVALID_PUBSUB_PAYLOAD",
                message="The Pub/Sub data field is not a valid RetryPermit delivery.",
                retryable=False,
                status_code=400,
                trace_id=trace_id,
            ) from exc
        if payload.source_topic != runtime.settings.pubsub_topic:
            raise ConflictError(
                "SOURCE_TOPIC_MISMATCH",
                "The event source topic does not match the configured DLQ topic.",
                trace_id,
            )
        registration = await runtime.delivery.register(
            run_id=payload.run_id,
            subscription=envelope.subscription,
            source_message_id=payload.message_id,
            transport_message_id=envelope.message.message_id,
            source_topic=payload.source_topic,
            payload=payload.payload,
            trace_id=trace_id,
        )
        run = await runtime.store.get_run(payload.run_id)
        if not run.active:
            await runtime.store.update_inbox_status(
                payload.run_id,
                registration.inbox.inbox_id,
                InboxStatus.FAILED_FINAL,
                error_code="RUN_INACTIVE",
            )
            return Response(status_code=204)
        await runtime.delivery.ensure_processing_task(registration, trace_id=trace_id)
        return Response(status_code=204)

    @app.post("/internal/tasks/process")
    async def process_task(
        request: Request, body: TaskProcessRequest
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        verify_service_identity(
            request,
            settings=runtime.settings,
            expected_email=runtime.settings.cloud_tasks_service_account,
        )
        run = await runtime.store.get_run(body.run_id)
        if not run.active:
            await runtime.store.update_inbox_status(
                body.run_id,
                body.inbox_id,
                InboxStatus.FAILED_FINAL,
                error_code="RUN_INACTIVE",
            )
            return {"status": "ignored_inactive_run", "run_id": body.run_id}
        task_header = request.headers.get("x-cloudtasks-taskname", uuid.uuid4().hex)
        worker_id = (
            "task_"
            + hashlib.sha256(task_header.encode()).hexdigest()[:20]
            + "_"
            + uuid.uuid4().hex[:12]
        )
        message = await runtime.delivery.process_inbox(
            run_id=body.run_id,
            inbox_id=body.inbox_id,
            worker_id=worker_id,
            trace_id=_trace_id(request),
        )
        return {
            "status": message.current_state.value,
            "run_id": body.run_id,
            "message_id": message.message_id,
        }

    @app.post("/internal/tasks/recover-message")
    async def recover_message_task(
        request: Request, body: TaskRecoverRequest
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        verify_service_identity(
            request,
            settings=runtime.settings,
            expected_email=runtime.settings.cloud_tasks_service_account,
        )
        run = await runtime.store.get_run(body.run_id)
        if not run.active:
            return {"status": "ignored_inactive_run", "run_id": body.run_id}
        task_header = request.headers.get("x-cloudtasks-taskname", uuid.uuid4().hex)
        worker_id = (
            "recovery_"
            + hashlib.sha256(task_header.encode()).hexdigest()[:20]
            + "_"
            + uuid.uuid4().hex[:12]
        )
        message = await runtime.delivery.process_recovery(
            run_id=body.run_id,
            message_id=body.message_id,
            worker_id=worker_id,
            trace_id=_trace_id(request),
        )
        return {
            "status": message.current_state.value,
            "run_id": body.run_id,
            "message_id": body.message_id,
        }

    @app.post("/internal/recovery/sweep")
    async def recovery_sweep(request: Request, body: RecoveryRequest) -> dict[str, Any]:
        runtime = _runtime(request)
        verify_service_identity(
            request,
            settings=runtime.settings,
            expected_email=runtime.settings.recovery_service_account,
        )
        result = await runtime.delivery.schedule_recovery(
            limit=body.limit, trace_id=_trace_id(request)
        )
        return result.model_dump(mode="json")

    @app.post("/mock-downstream/orders")
    async def submit_mock_order(
        request: Request, body: DownstreamOrderRequest
    ) -> dict[str, Any]:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        message = await runtime.store.get_message(body.run_id, body.message_id)
        if (
            message.idempotency_key != body.idempotency_key
            or message.repaired_payload_hash != body.payload_hash
            or message.repaired_payload != body.payload
        ):
            raise ConflictError(
                "DOWNSTREAM_AUTHORIZATION_MISMATCH",
                "The request does not match the deterministically authorized replay.",
                _trace_id(request),
            )
        # The irreversible create path is deliberately unavailable to an HTTP
        # caller: it requires the orchestrator's live fenced replay lease. This
        # endpoint provides idempotent inspection/resubmission proof only after the
        # deterministic executor has created the simulated effect.
        effect = await runtime.store.get_downstream_effect(
            body.run_id, body.idempotency_key
        )
        return {
            "created": False,
            "effect": effect.model_dump(mode="json"),
            "synthetic_data": True,
            "simulated_downstream": True,
        }

    @app.get("/mock-downstream/orders/{idempotency_key}")
    async def get_mock_order(
        idempotency_key: str,
        request: Request,
        run_id: str = Query(min_length=1),
    ) -> dict[str, Any]:
        effect = await _runtime(request).store.get_downstream_effect(
            run_id, idempotency_key
        )
        return {
            "effect": effect.model_dump(mode="json"),
            "synthetic_data": True,
            "simulated_downstream": True,
        }

    @app.post("/mock-downstream/reset", response_model=DemoActionResponse)
    async def reset_mock_downstream(request: Request) -> DemoActionResponse:
        runtime = _runtime(request)
        require_admin(request, runtime.settings)
        run = await runtime.demo.reset()
        return DemoActionResponse(
            run_id=run.run_id,
            status=run.status.value,
            message_count=run.seeded_message_count,
            details={
                "history_preserved": True,
                "reset_mechanism": "run namespace rotation",
            },
        )
