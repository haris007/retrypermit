from __future__ import annotations

from typing import Any

from retrypermit.domain.hashing import payload_hash as calculate_payload_hash
from retrypermit.domain.replay import DownstreamSubmission
from retrypermit.domain.errors import (
    AmbiguousDownstreamOutcomeError,
    PayloadHashMismatchError,
)
from retrypermit.ports.store import Store


FAILURE_INJECTION_MESSAGE_ID = "synthetic-order-001"


class StoreBackedDownstream:
    """The synthetic order processor's independent idempotency boundary.

    The replay ledger is deliberately not consulted here. The store operation owns
    a separate, transactionally unique downstream-effect record, which is the proof
    that repeated submissions produced one business effect.
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    async def submit(
        self,
        *,
        run_id: str,
        message_id: str,
        payload: dict[str, Any],
        payload_hash: str,
        idempotency_key: str,
        owner: str,
    ) -> DownstreamSubmission:
        if calculate_payload_hash(payload) != payload_hash:
            raise PayloadHashMismatchError("downstream payload hash is invalid")
        submission = await self._store.create_or_get_downstream_effect(
            run_id=run_id,
            message_id=message_id,
            idempotency_key=idempotency_key,
            repaired_payload_hash=payload_hash,
            owner=owner,
        )
        run = await self._store.get_run(run_id)
        if (
            run.inject_failure
            and message_id == FAILURE_INJECTION_MESSAGE_ID
            and submission.created
        ):
            # The effect is deliberately committed before the response is lost. A
            # retry with the same idempotency key returns the original reference.
            raise AmbiguousDownstreamOutcomeError(
                "simulated 503 after the downstream effect was committed"
            )
        return submission
