from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Protocol

from pydantic import BaseModel

from retrypermit.domain.enums import (
    InboxStatus,
    ReceiptKind,
    ReceiptStatus,
)
from retrypermit.domain.inbox import DeliveryRegistration, InboxItem
from retrypermit.domain.messages import MessageRecord
from retrypermit.domain.receipts import Receipt
from retrypermit.domain.state_machine import is_terminal
from retrypermit.errors import DependencyUnavailable
from retrypermit.ports.clock import Clock, SystemClock
from retrypermit.ports.store import Store
from retrypermit.ports.task_scheduler import TaskScheduler


logger = logging.getLogger("retrypermit.delivery")


class MessageProcessor(Protocol):
    async def run_message(
        self,
        run_id: str,
        message_id: str,
        *,
        worker_id: str | None = None,
        trace_id: str | None = None,
    ) -> MessageRecord: ...


class RecoveryScheduleResult(BaseModel):
    inbox_candidates: int
    replay_ledger_candidates: int
    retryable_message_candidates: int
    tasks_scheduled: int


def delivery_key(subscription: str, transport_message_id: str) -> str:
    material = "\x1f".join(
        ("retrypermit", "pubsub-delivery", subscription, transport_message_id)
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class DeliveryService:
    """Durable delivery/task handshake shared by ingress and recovery routes."""

    def __init__(
        self,
        store: Store,
        scheduler: TaskScheduler,
        processor: MessageProcessor,
        *,
        clock: Clock | None = None,
        id_factory: Callable[[], str] | None = None,
        inbox_lease: timedelta = timedelta(seconds=60),
        retry_delay: timedelta = timedelta(seconds=5),
    ) -> None:
        self._store = store
        self._scheduler = scheduler
        self._processor = processor
        self._clock = clock or SystemClock()
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        self._inbox_lease = inbox_lease
        self._retry_delay = retry_delay

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{self._id_factory()}"

    async def register(
        self,
        *,
        run_id: str,
        subscription: str,
        source_message_id: str,
        transport_message_id: str,
        source_topic: str,
        payload: dict[str, object],
        trace_id: str,
    ) -> DeliveryRegistration:
        key = delivery_key(subscription, transport_message_id)
        registration = await self._store.register_delivery(
            run_id=run_id,
            delivery_key=key,
            source_message_id=source_message_id,
            transport_message_id=transport_message_id,
            source_topic=source_topic,
            payload=payload,
            trace_id=trace_id,
        )
        message = await self._store.get_message(run_id, source_message_id)
        delivery_receipt = await self._store.record_receipt(
            Receipt(
                receipt_id=self._id("rcpt"),
                operation_id=registration.attempt.attempt_id,
                run_id=run_id,
                message_id=source_message_id,
                kind=ReceiptKind.DELIVERY,
                status=ReceiptStatus.CONFIRMED,
                stage="pubsub_delivery",
                trace_id=trace_id,
                policy_version=message.active_policy_version,
                payload_hash=registration.delivery.payload_hash,
                details={
                    "delivery_key": key,
                    "transport_message_id": transport_message_id,
                    "duplicate": registration.duplicate,
                    "seen_count": registration.delivery.seen_count,
                },
                created_at=self._clock.now(),
            )
        )
        logger.info(
            "delivery_persisted",
            extra={
                "trace_id": trace_id,
                "run_id": run_id,
                "message_id": source_message_id,
                "delivery_id": key,
                "state": registration.inbox.status.value,
                "tool_name": "pubsub_delivery",
                "receipt_id": delivery_receipt.receipt_id,
                "policy_version": message.active_policy_version,
                "outcome": ReceiptStatus.CONFIRMED.value,
                "duplicate": registration.duplicate,
            },
        )
        return registration

    async def ensure_processing_task(
        self, registration: DeliveryRegistration, *, trace_id: str
    ) -> str | None:
        inbox = registration.inbox
        if inbox.status in (InboxStatus.COMPLETED, InboxStatus.FAILED_FINAL):
            return inbox.task_name
        if inbox.task_name and inbox.status in (
            InboxStatus.ENQUEUED,
            InboxStatus.PROCESSING,
        ):
            return inbox.task_name
        task_name = self._task_name(
            "process", inbox.run_id, inbox.inbox_id, inbox.attempt_count
        )
        return await self._schedule_for_message(
            run_id=inbox.run_id,
            message_id=inbox.message_id,
            task_name=task_name,
            endpoint="/internal/tasks/process",
            body={"run_id": inbox.run_id, "inbox_id": inbox.inbox_id},
            trace_id=trace_id,
            schedule_time=self._clock.now(),
            inbox=inbox,
        )

    async def process_inbox(
        self,
        *,
        run_id: str,
        inbox_id: str,
        worker_id: str,
        trace_id: str,
    ) -> MessageRecord:
        lease = await self._store.claim_inbox(
            run_id, inbox_id, worker_id, self._inbox_lease
        )
        if not lease.acquired:
            return await self._store.get_message(run_id, lease.item.message_id)
        try:
            message = await self._processor.run_message(
                run_id,
                lease.item.message_id,
                worker_id=worker_id,
                trace_id=trace_id,
            )
        except Exception:
            await self._store.update_inbox_status(
                run_id,
                inbox_id,
                InboxStatus.RETRYABLE,
                next_attempt_at=self._clock.now() + self._retry_delay,
                error_code="PROCESSING_FAILED",
                owner=worker_id,
            )
            raise
        if is_terminal(message.current_state):
            await self._store.update_inbox_status(
                run_id, inbox_id, InboxStatus.COMPLETED, owner=worker_id
            )
        else:
            retry_at = message.next_attempt_at or self._clock.now() + self._retry_delay
            retry_inbox = await self._store.update_inbox_status(
                run_id,
                inbox_id,
                InboxStatus.RETRYABLE,
                next_attempt_at=retry_at,
                error_code=message.last_error_code or "PROCESSING_INCOMPLETE",
                owner=worker_id,
            )
            task_name = self._task_name(
                "process", run_id, inbox_id, retry_inbox.attempt_count
            )
            await self._schedule_for_message(
                run_id=run_id,
                message_id=message.message_id,
                task_name=task_name,
                endpoint="/internal/tasks/process",
                body={"run_id": run_id, "inbox_id": inbox_id},
                trace_id=trace_id,
                schedule_time=retry_at,
                inbox=retry_inbox,
            )
        logger.info(
            "inbox_processing_finished",
            extra={
                "trace_id": trace_id,
                "run_id": run_id,
                "message_id": message.message_id,
                "delivery_id": inbox_id,
                "state": message.current_state.value,
                "failed_stage": (
                    message.failed_stage.value if message.failed_stage else None
                ),
                "tool_name": "inbox_worker",
                "attempt": lease.item.attempt_count,
                "outcome": (
                    "terminal" if is_terminal(message.current_state) else "retryable"
                ),
                "error_code": message.last_error_code,
            },
        )
        return message

    async def process_recovery(
        self,
        *,
        run_id: str,
        message_id: str,
        worker_id: str,
        trace_id: str,
    ) -> MessageRecord:
        return await self._processor.run_message(
            run_id, message_id, worker_id=worker_id, trace_id=trace_id
        )

    async def schedule_recovery(
        self, *, limit: int, trace_id: str
    ) -> RecoveryScheduleResult:
        now = self._clock.now()
        inbox_items, ledgers, messages = await self._store.recovery_candidates(now)
        inbox_items = inbox_items[:limit]
        remaining = max(0, limit - len(inbox_items))
        recovery_pairs: dict[tuple[str, str], int] = {}
        for entry in ledgers:
            recovery_pairs[(entry.run_id, entry.message_id)] = max(
                recovery_pairs.get((entry.run_id, entry.message_id), 0),
                entry.attempt_count,
            )
        for message in messages:
            recovery_pairs[(message.run_id, message.message_id)] = max(
                recovery_pairs.get((message.run_id, message.message_id), 0),
                message.attempts,
            )

        scheduled = 0
        for item in inbox_items:
            task_name = self._task_name(
                "inbox-recovery", item.run_id, item.inbox_id, item.attempt_count
            )
            await self._schedule_for_message(
                run_id=item.run_id,
                message_id=item.message_id,
                task_name=task_name,
                endpoint="/internal/tasks/process",
                body={"run_id": item.run_id, "inbox_id": item.inbox_id},
                trace_id=trace_id,
                schedule_time=now,
                inbox=item,
            )
            scheduled += 1

        inbox_message_pairs = {(item.run_id, item.message_id) for item in inbox_items}
        for (run_id, message_id), attempt in list(recovery_pairs.items())[:remaining]:
            if (run_id, message_id) in inbox_message_pairs:
                continue
            task_name = self._task_name("message-recovery", run_id, message_id, attempt)
            await self._schedule_for_message(
                run_id=run_id,
                message_id=message_id,
                task_name=task_name,
                endpoint="/internal/tasks/recover-message",
                body={"run_id": run_id, "message_id": message_id},
                trace_id=trace_id,
                schedule_time=now,
            )
            scheduled += 1

        return RecoveryScheduleResult(
            inbox_candidates=len(inbox_items),
            replay_ledger_candidates=len(ledgers),
            retryable_message_candidates=len(messages),
            tasks_scheduled=scheduled,
        )

    async def _schedule_for_message(
        self,
        *,
        run_id: str,
        message_id: str,
        task_name: str,
        endpoint: str,
        body: dict[str, str],
        trace_id: str,
        schedule_time,
        inbox: InboxItem | None = None,
    ) -> str:
        message = await self._store.get_message(run_id, message_id)
        operation_id = self._id("op")
        common = {
            "run_id": run_id,
            "message_id": message_id,
            "kind": ReceiptKind.TASK_SCHEDULE_ATTEMPT,
            "stage": "task_schedule",
            "trace_id": trace_id,
            "policy_version": message.active_policy_version,
            "details": {"task_name": task_name, "endpoint": endpoint},
            "created_at": self._clock.now(),
        }
        await self._store.record_receipt(
            Receipt(
                receipt_id=self._id("rcpt"),
                operation_id=operation_id,
                status=ReceiptStatus.STARTED,
                **common,
            )
        )
        try:
            scheduled_name = await self._scheduler.schedule(
                task_name=task_name,
                endpoint=endpoint,
                body=body,
                schedule_time=schedule_time,
            )
        except Exception as exc:
            await self._store.record_receipt(
                Receipt(
                    receipt_id=self._id("rcpt"),
                    operation_id=operation_id,
                    run_id=run_id,
                    message_id=message_id,
                    kind=ReceiptKind.TASK_SCHEDULE_RESULT,
                    status=ReceiptStatus.FAILED,
                    stage="task_schedule",
                    trace_id=trace_id,
                    policy_version=message.active_policy_version,
                    error_code="TASK_SCHEDULE_FAILED",
                    retryable=True,
                    details={"task_name": task_name, "endpoint": endpoint},
                    created_at=self._clock.now(),
                )
            )
            raise DependencyUnavailable(
                "TASK_SCHEDULE_FAILED",
                "Durable processing could not be scheduled; delivery must be retried.",
                trace_id,
            ) from exc
        schedule_result_receipt = await self._store.record_receipt(
            Receipt(
                receipt_id=self._id("rcpt"),
                operation_id=operation_id,
                run_id=run_id,
                message_id=message_id,
                kind=ReceiptKind.TASK_SCHEDULE_RESULT,
                status=ReceiptStatus.CONFIRMED,
                stage="task_schedule",
                trace_id=trace_id,
                policy_version=message.active_policy_version,
                details={
                    "task_name": task_name,
                    "scheduled_name": scheduled_name,
                    "endpoint": endpoint,
                },
                created_at=self._clock.now(),
            )
        )
        if inbox is not None:
            await self._store.update_inbox_status(
                run_id,
                inbox.inbox_id,
                InboxStatus.ENQUEUED,
                task_name=scheduled_name,
                scheduled_at=schedule_time,
                expected_attempt_count=inbox.attempt_count,
            )
        logger.info(
            "task_schedule_confirmed",
            extra={
                "trace_id": trace_id,
                "run_id": run_id,
                "message_id": message_id,
                "delivery_id": inbox.inbox_id if inbox is not None else None,
                "tool_name": "cloud_tasks_schedule",
                "receipt_id": schedule_result_receipt.receipt_id,
                "policy_version": message.active_policy_version,
                "attempt": inbox.attempt_count if inbox is not None else None,
                "outcome": ReceiptStatus.CONFIRMED.value,
                "task_name": scheduled_name,
            },
        )
        return scheduled_name

    @staticmethod
    def _task_name(prefix: str, run_id: str, identity: str, generation: int) -> str:
        material = "\x1f".join(
            ("retrypermit", prefix, run_id, identity, str(generation))
        )
        digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
        return f"{prefix}-{digest[:48]}"
