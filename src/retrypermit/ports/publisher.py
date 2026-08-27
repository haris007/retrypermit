from typing import Any, Protocol


class Publisher(Protocol):
    async def publish(
        self, *, topic: str, payload: dict[str, Any], attributes: dict[str, str]
    ) -> str: ...
