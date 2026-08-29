from retrypermit.domain.errors import RunConflictError
from retrypermit.domain.proof import EffectivelyOnceProof
from retrypermit.ports.store import Store


class ProofService:
    """Build proof from live store queries, never from a cached summary."""

    def __init__(self, store: Store) -> None:
        self._store = store

    async def prove_effectively_once(
        self, run_id: str, message_id: str
    ) -> EffectivelyOnceProof:
        message = await self._store.get_message(run_id, message_id)
        if not message.idempotency_key:
            raise RunConflictError("message has not entered the replay ledger")
        ledger = await self._store.get_replay_ledger(run_id, message.idempotency_key)
        effects = [
            effect
            for effect in await self._store.list_downstream_effects(run_id)
            if effect.message_id == message_id
        ]
        references = {effect.downstream_reference for effect in effects}
        if len(references) > 1:
            raise RunConflictError(
                "message has more than one downstream business reference"
            )
        return EffectivelyOnceProof(
            run_id=run_id,
            message_id=message_id,
            recorded_pubsub_delivery_count=(
                await self._store.count_recorded_deliveries(run_id, message_id)
            ),
            execution_attempt_count=ledger.attempt_count,
            downstream_request_count=sum(effect.submission_count for effect in effects),
            unique_downstream_effect_count=len(effects),
            final_downstream_reference=(next(iter(references)) if references else None),
            ledger_status=ledger.status,
        )
