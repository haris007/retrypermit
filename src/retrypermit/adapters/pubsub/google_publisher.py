from __future__ import annotations

import asyncio
import json
from typing import Any

from google.cloud import pubsub_v1

from retrypermit.config import Settings


class GooglePubSubPublisher:
    def __init__(
        self,
        settings: Settings,
        client: pubsub_v1.PublisherClient | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or pubsub_v1.PublisherClient()

    async def publish(
        self,
        *,
        topic: str,
        payload: dict[str, Any],
        attributes: dict[str, str],
    ) -> str:
        topic_path = self.client.topic_path(self.settings.google_cloud_project, topic)
        data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        future = self.client.publish(topic_path, data, **attributes)
        return await asyncio.to_thread(future.result, timeout=30)


class RecordingPublisher:
    """Local publisher that records messages without claiming durable transport."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def publish(
        self,
        *,
        topic: str,
        payload: dict[str, Any],
        attributes: dict[str, str],
    ) -> str:
        message_id = f"local-{len(self.messages) + 1:04d}"
        self.messages.append(
            {
                "message_id": message_id,
                "topic": topic,
                "payload": payload,
                "attributes": attributes,
            }
        )
        return message_id
