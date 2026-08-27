from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4


@dataclass(slots=True)
class RetryPermitError(Exception):
    code: str
    message: str
    retryable: bool = False
    status_code: int = 400
    trace_id: str = ""

    def __post_init__(self) -> None:
        if not self.trace_id:
            self.trace_id = uuid4().hex
        # dataclass(slots=True) creates a replacement class, so zero-argument
        # super() can retain the pre-transform __class__ cell on some Python
        # versions. Calling Exception directly is stable and keeps args useful.
        Exception.__init__(self, self.message)


class AuthenticationError(RetryPermitError):
    def __init__(
        self, message: str = "Authentication is required.", trace_id: str = ""
    ):
        super().__init__("AUTH_REQUIRED", message, False, 401, trace_id)


class AuthorizationError(RetryPermitError):
    def __init__(
        self, message: str = "This identity is not authorized.", trace_id: str = ""
    ):
        super().__init__("AUTH_FORBIDDEN", message, False, 403, trace_id)


class ConflictError(RetryPermitError):
    def __init__(self, code: str, message: str, trace_id: str = ""):
        super().__init__(code, message, False, 409, trace_id)


class DependencyUnavailable(RetryPermitError):
    def __init__(self, code: str, message: str, trace_id: str = ""):
        super().__init__(code, message, True, 503, trace_id)
