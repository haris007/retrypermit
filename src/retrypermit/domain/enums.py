from enum import StrEnum


class MessageState(StrEnum):
    RECEIVED = "RECEIVED"
    TRIAGING = "TRIAGING"
    TRIAGED = "TRIAGED"
    PLANNED = "PLANNED"
    DEFERRED = "DEFERRED"
    RECHECKING = "RECHECKING"
    REPAIRING = "REPAIRING"
    REPLAYING = "REPLAYING"
    REPLAYED = "REPLAYED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"
    ESCALATED = "ESCALATED"
    QUARANTINED = "QUARANTINED"


class FailedStage(StrEnum):
    TRIAGE = "triage"
    REPLAY = "replay"


class FailureClass(StrEnum):
    SCHEMA_DRIFT = "schema_drift"
    TRANSIENT_DOWNSTREAM = "transient_downstream"
    INVALID_DATA = "invalid_data"
    PROMPT_INJECTION = "prompt_injection"
    UNKNOWN = "unknown"


class RecommendedAction(StrEnum):
    REPLAY = "replay"
    DEFER = "defer"
    QUARANTINE = "quarantine"
    ESCALATE = "escalate"


class RepairKind(StrEnum):
    RENAME_FIELD = "rename_field"
    COERCE_TYPE = "coerce_type"
    SET_DEFAULT = "set_default"


class ReplayStatus(StrEnum):
    PENDING = "PENDING"
    ATTEMPTED = "ATTEMPTED"
    CONFIRMED = "CONFIRMED"
    RETRYABLE = "RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"


class InboxStatus(StrEnum):
    PENDING = "PENDING"
    ENQUEUED = "ENQUEUED"
    PROCESSING = "PROCESSING"
    RETRYABLE = "RETRYABLE"
    COMPLETED = "COMPLETED"
    FAILED_FINAL = "FAILED_FINAL"


class ReceiptKind(StrEnum):
    TRANSITION = "transition"
    TRIAGE_ATTEMPT = "triage_attempt"
    TRIAGE_RESULT = "triage_result"
    REPAIR_RESULT = "repair_result"
    REPLAY_ATTEMPT = "replay_attempt"
    REPLAY_RESULT = "replay_result"
    PUBSUB_PUBLISH_ATTEMPT = "pubsub_publish_attempt"
    PUBSUB_PUBLISH_RESULT = "pubsub_publish_result"
    TASK_SCHEDULE_ATTEMPT = "task_schedule_attempt"
    TASK_SCHEDULE_RESULT = "task_schedule_result"
    DELIVERY = "delivery"
    POLICY = "policy"
    DEFERRAL = "deferral"
    RECHECK = "recheck"
    ESCALATION = "escalation"
    REFUSAL = "refusal"


class ReceiptStatus(StrEnum):
    STARTED = "STARTED"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    IN_PROGRESS = "IN_PROGRESS"


class PolicyStatus(StrEnum):
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED = "APPROVED"
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"


class RunStatus(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    INACTIVE = "INACTIVE"
