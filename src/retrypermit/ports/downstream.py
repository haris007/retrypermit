from typing import Any, Protocol

from retrypermit.domain.replay import DownstreamSubmission


class Downstream(Protocol):
    async def submit(
        self,
        *,
        run_id: str,
        message_id: str,
        payload: dict[str, Any],
        payload_hash: str,
        idempotency_key: str,
        owner: str,
    ) -> DownstreamSubmission: ...
