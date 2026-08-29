class RetryPermitError(Exception):
    """Base class for safe, deterministic domain failures."""

    code = "RETRYPERMIT_ERROR"


class NotFoundError(RetryPermitError):
    code = "NOT_FOUND"


class InvalidTransitionError(RetryPermitError):
    code = "INVALID_STATE_TRANSITION"


class PolicyValidationError(RetryPermitError):
    code = "INVALID_POLICY"


class PolicyNotActiveError(RetryPermitError):
    code = "POLICY_NOT_ACTIVE"


class AuthorizationDeniedError(RetryPermitError):
    code = "ACTION_NOT_AUTHORIZED"


class RepairRejectedError(RetryPermitError):
    code = "REPAIR_REJECTED"


class PayloadHashMismatchError(RetryPermitError):
    code = "IDEMPOTENCY_PAYLOAD_MISMATCH"


class LeaseNotOwnedError(RetryPermitError):
    code = "LEASE_NOT_OWNED"


class RunConflictError(RetryPermitError):
    code = "RUN_CONFLICT"


class AmbiguousDownstreamOutcomeError(RetryPermitError):
    """The downstream effect exists, but its response was not received."""

    code = "DOWNSTREAM_RESPONSE_LOST_AFTER_EFFECT"
