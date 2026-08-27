from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

from retrypermit.config import Settings


class CloudTaskScheduler:
    """Create deterministic, OIDC-authenticated processing tasks."""

    def __init__(
        self,
        settings: Settings,
        client: tasks_v2.CloudTasksAsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or tasks_v2.CloudTasksAsyncClient()

    async def schedule(
        self,
        *,
        task_name: str,
        endpoint: str,
        body: dict[str, Any],
        schedule_time: datetime,
    ) -> str:
        parent = self.settings.task_parent
        full_name = f"{parent}/tasks/{task_name}"
        when = schedule_time.astimezone(UTC)
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(when)
        task = tasks_v2.Task(
            name=full_name,
            schedule_time=timestamp,
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=f"{self.settings.service_base_url.rstrip('/')}/{endpoint.lstrip('/')}",
                headers={"Content-Type": "application/json"},
                body=json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
                oidc_token=tasks_v2.OidcToken(
                    service_account_email=self.settings.cloud_tasks_service_account,
                    audience=self.settings.oidc_audience,
                ),
            ),
        )
        try:
            created = await self.client.create_task(
                request=tasks_v2.CreateTaskRequest(parent=parent, task=task)
            )
            return created.name
        except AlreadyExists:
            # Deterministic task names make the scheduling handshake recoverable.
            return full_name


class RecordingTaskScheduler:
    """Local/test scheduler. It records intent but is explicitly non-durable."""

    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}

    async def schedule(
        self,
        *,
        task_name: str,
        endpoint: str,
        body: dict[str, Any],
        schedule_time: datetime,
    ) -> str:
        self.tasks.setdefault(
            task_name,
            {
                "endpoint": endpoint,
                "body": body,
                "schedule_time": schedule_time,
            },
        )
        return f"local/tasks/{task_name}"
