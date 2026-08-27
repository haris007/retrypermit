from datetime import datetime

from pydantic import BaseModel

from retrypermit.domain.inbox import InboxItem
from retrypermit.domain.messages import MessageRecord
from retrypermit.domain.replay import ReplayLedgerEntry
from retrypermit.ports.store import Store


class RecoveryBatch(BaseModel):
    inbox_items: list[InboxItem]
    replay_ledgers: list[ReplayLedgerEntry]
    retryable_messages: list[MessageRecord]


class RecoveryService:
    def __init__(self, store: Store) -> None:
        self._store = store

    async def scan(self, now: datetime) -> RecoveryBatch:
        inbox, ledgers, messages = await self._store.recovery_candidates(now)
        return RecoveryBatch(
            inbox_items=inbox,
            replay_ledgers=ledgers,
            retryable_messages=messages,
        )
