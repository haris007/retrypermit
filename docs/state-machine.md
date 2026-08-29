# RetryPermit state machine

Every message transition is explicit, validated, sequence-numbered, and recorded. Terminal states are `REPLAYED`, `ESCALATED`, and `QUARANTINED`; a retryable failure is not terminal.

```mermaid
stateDiagram-v2
    [*] --> RECEIVED
    RECEIVED --> TRIAGING: worker claims message
    TRIAGING --> TRIAGED: proposal validated
    TRIAGING --> FAILED_RETRYABLE: model timeout / retryable triage error
    FAILED_RETRYABLE --> TRIAGING: failed_stage=TRIAGE and retry due

    TRIAGED --> PLANNED: deterministic plan
    PLANNED --> DEFERRED: transient dependency unavailable
    DEFERRED --> RECHECKING: per-message task due
    RECHECKING --> DEFERRED: dependency still unavailable
    RECHECKING --> PLANNED: dependency recovered

    PLANNED --> REPAIRING: policy-authorized transform required
    PLANNED --> REPLAYING: payload valid without repair
    PLANNED --> ESCALATED: invalid / unknown / unauthorized
    PLANNED --> QUARANTINED: instruction-like untrusted content
    REPAIRING --> REPLAYING: repaired payload passes deterministic validation
    REPAIRING --> ESCALATED: repair rejected or exceeds effective cap

    REPLAYING --> REPLAYED: downstream receipt CONFIRMED
    REPLAYING --> FAILED_RETRYABLE: lost response / 503 / timeout within budget
    FAILED_RETRYABLE --> REPLAYING: failed_stage=REPLAY and retry due
    FAILED_RETRYABLE --> ESCALATED: stage retry budget exhausted
```

## Why `failed_stage` matters

`FAILED_RETRYABLE` is shared by errors from different work. RetryPermit persists `failed_stage` so recovery resumes the interrupted stage:

| `failed_stage` | Resume path | What is not repeated unnecessarily |
| --- | --- | --- |
| `TRIAGE` | `FAILED_RETRYABLE → TRIAGING` | No replay is attempted before a valid proposal exists |
| `REPLAY` | `FAILED_RETRYABLE → REPLAYING` | The same approved plan and idempotency key are reused |

Retry exhaustion produces an explicit escalation rather than an infinite loop.

## Outcome paths

- **Class A:** `RECEIVED → TRIAGING → TRIAGED → PLANNED → REPAIRING → REPLAYING → REPLAYED`.
- **Class B:** `... → PLANNED → DEFERRED → RECHECKING → PLANNED → REPLAYING → REPLAYED`.
- **Class C:** `... → PLANNED → ESCALATED`; no downstream call is allowed.
- **Class D:** `... → PLANNED → QUARANTINED`; instruction text is inert and no downstream call is allowed.
- **Lost response:** `... → REPLAYING → FAILED_RETRYABLE → REPLAYING → REPLAYED`, using the same key.

## State invariants

1. The payload hash bound to a replay key cannot change on retry.
2. A confirmed ledger record points to exactly one downstream reference.
3. A terminal Class C or D message has no downstream effect.
4. Policy extraction cannot activate a policy.
5. Recovery can recreate work but cannot bypass the state machine or authorization checks.
6. The application safety ceiling clamps every runbook value.
