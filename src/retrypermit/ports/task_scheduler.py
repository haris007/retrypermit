from datetime import datetime
from typing import Any, Protocol


class TaskScheduler(Protocol):
    async def schedule(
        self,
        *,
        task_name: str,
        endpoint: str,
        body: dict[str, Any],
        schedule_time: datetime,
    ) -> str: ...
