from __future__ import annotations

import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from retrypermit.api.routes import register_routes
from retrypermit.config import Settings, get_settings
from retrypermit.domain.errors import (
    NotFoundError,
    PolicyValidationError,
    RepairRejectedError,
    RetryPermitError as DomainError,
)
from retrypermit.errors import RetryPermitError as ApiError
from retrypermit.observability import configure_logging
from retrypermit.runtime import Runtime, build_runtime


logger = logging.getLogger("retrypermit.api")
_TRACE_PATTERN = re.compile(r"^[0-9a-fA-F]{16,32}$")


def _request_trace_id(request: Request) -> str:
    value = str(getattr(request.state, "trace_id", ""))
    return value or uuid.uuid4().hex


def _error_response(
    *, code: str, message: str, trace_id: str, retryable: bool, status_code: int
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "trace_id": trace_id,
                "retryable": retryable,
            }
        },
        headers={"X-Trace-Id": trace_id},
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.runtime = await build_runtime(resolved)
        logger.info(
            "runtime_ready",
            extra={
                "mode": resolved.mode_label,
                "environment": resolved.app_env,
                "synthetic_data": True,
                "simulated_downstream": True,
            },
        )
        yield
        runtime: Runtime = application.state.runtime
        client = getattr(runtime.store, "client", None)
        close = getattr(client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result

    application = FastAPI(
        title="RetryPermit",
        version="0.1.0",
        description="Policy-governed recovery for failed synthetic events.",
        lifespan=lifespan,
    )

    @application.middleware("http")
    async def trace_requests(request: Request, call_next):
        started = time.perf_counter()
        cloud_trace = request.headers.get("x-cloud-trace-context", "").partition("/")[0]
        request_id = request.headers.get("x-request-id", "")
        candidate = cloud_trace if _TRACE_PATTERN.fullmatch(cloud_trace) else request_id
        request.state.trace_id = (
            candidate[:64] if candidate and len(candidate) <= 64 else uuid.uuid4().hex
        )
        response = await call_next(request)
        response.headers.setdefault("X-Trace-Id", request.state.trace_id)
        logger.info(
            "http_request",
            extra={
                "trace_id": request.state.trace_id,
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
        )
        return response

    @application.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        trace_id = exc.trace_id or _request_trace_id(request)
        return _error_response(
            code=exc.code,
            message=exc.message,
            trace_id=trace_id,
            retryable=exc.retryable,
            status_code=exc.status_code,
        )

    @application.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
        if isinstance(exc, NotFoundError):
            status_code = 404
        elif isinstance(exc, (PolicyValidationError, RepairRejectedError)):
            status_code = 422
        else:
            status_code = 409
        return _error_response(
            code=exc.code,
            message=str(exc) or "The deterministic operation was rejected.",
            trace_id=_request_trace_id(request),
            retryable=False,
            status_code=status_code,
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        logger.warning(
            "request_validation_failed",
            extra={
                "trace_id": _request_trace_id(request),
                "error_count": len(exc.errors()),
            },
        )
        return _error_response(
            code="INVALID_REQUEST",
            message="The request did not match the required schema.",
            trace_id=_request_trace_id(request),
            retryable=False,
            status_code=422,
        )

    @application.exception_handler(HTTPException)
    async def http_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
        return _error_response(
            code="HTTP_ERROR",
            message=str(exc.detail),
            trace_id=_request_trace_id(request),
            retryable=exc.status_code >= 500,
            status_code=exc.status_code,
        )

    @application.exception_handler(Exception)
    async def unexpected_error_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.exception(
            "unhandled_error",
            extra={
                "trace_id": _request_trace_id(request),
                "error_code": "INTERNAL_ERROR",
            },
        )
        return _error_response(
            code="INTERNAL_ERROR",
            message="RetryPermit could not complete the operation.",
            trace_id=_request_trace_id(request),
            retryable=True,
            status_code=500,
        )

    register_routes(application)

    frontend = Path(resolved.frontend_dist_path)
    assets = frontend / "assets"
    if assets.is_dir():
        application.mount("/assets", StaticFiles(directory=assets), name="assets")

    @application.get("/", include_in_schema=False)
    async def index():
        index_path = frontend / "index.html"
        if index_path.is_file():
            return FileResponse(index_path)
        return {
            "service": "RetryPermit",
            "status": "frontend_not_built",
            "next": "Run `make web` to build the local operations console.",
        }

    return application


app = create_app()
