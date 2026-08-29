from __future__ import annotations

from retrypermit.domain.enums import FailedStage, MessageState
from retrypermit.domain.errors import InvalidTransitionError

_ALLOWED: dict[MessageState, set[MessageState]] = {
    MessageState.RECEIVED: {MessageState.TRIAGING},
    MessageState.TRIAGING: {
        MessageState.TRIAGED,
        MessageState.FAILED_RETRYABLE,
        MessageState.ESCALATED,
    },
    MessageState.TRIAGED: {MessageState.PLANNED},
    MessageState.PLANNED: {
        MessageState.DEFERRED,
        MessageState.REPAIRING,
        MessageState.REPLAYING,
        MessageState.ESCALATED,
        MessageState.QUARANTINED,
    },
    MessageState.DEFERRED: {MessageState.RECHECKING},
    MessageState.RECHECKING: {MessageState.PLANNED, MessageState.DEFERRED},
    MessageState.REPAIRING: {MessageState.REPLAYING, MessageState.ESCALATED},
    MessageState.REPLAYING: {
        MessageState.REPLAYED,
        MessageState.FAILED_RETRYABLE,
        MessageState.FAILED_FINAL,
        MessageState.ESCALATED,
    },
    MessageState.FAILED_RETRYABLE: {
        MessageState.TRIAGING,
        MessageState.REPLAYING,
        MessageState.FAILED_FINAL,
    },
    MessageState.REPLAYED: set(),
    MessageState.FAILED_FINAL: set(),
    MessageState.ESCALATED: set(),
    MessageState.QUARANTINED: set(),
}


def ensure_transition(
    previous: MessageState,
    new: MessageState,
    failed_stage: FailedStage | None = None,
) -> None:
    if new not in _ALLOWED[previous]:
        raise InvalidTransitionError(
            f"{previous.value} cannot transition to {new.value}"
        )
    if previous == MessageState.FAILED_RETRYABLE:
        expected = {
            FailedStage.TRIAGE: MessageState.TRIAGING,
            FailedStage.REPLAY: MessageState.REPLAYING,
        }.get(failed_stage)
        if new != MessageState.FAILED_FINAL and new != expected:
            raise InvalidTransitionError(
                f"failed stage {failed_stage!s} cannot resume at {new.value}"
            )


def is_terminal(state: MessageState) -> bool:
    return state in {
        MessageState.REPLAYED,
        MessageState.FAILED_FINAL,
        MessageState.ESCALATED,
        MessageState.QUARANTINED,
    }
