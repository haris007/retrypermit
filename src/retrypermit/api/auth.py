from __future__ import annotations

import secrets
from dataclasses import dataclass

from fastapi import Request
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from retrypermit.config import Settings
from retrypermit.errors import AuthenticationError, AuthorizationError


@dataclass(frozen=True, slots=True)
class ServiceIdentity:
    email: str
    audience: str


def require_admin(request: Request, settings: Settings) -> None:
    configured = settings.demo_admin_token
    supplied = request.headers.get("x-admin-token", "")
    if not configured:
        raise AuthenticationError(
            "Administrative controls are disabled until DEMO_ADMIN_TOKEN is configured."
        )
    if not supplied:
        raise AuthenticationError("Provide the administrator token in X-Admin-Token.")
    if not secrets.compare_digest(supplied, configured):
        raise AuthorizationError("The administrator token is not valid.")


def verify_service_identity(
    request: Request,
    *,
    settings: Settings,
    expected_email: str,
) -> ServiceIdentity:
    local_marker = request.headers.get("x-retrypermit-local-service", "")
    if local_marker == "1" and settings.demo_mode and settings.allow_local_service_auth:
        return ServiceIdentity(email="local-demo", audience="local-demo")

    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise AuthenticationError("A Google-signed OIDC bearer token is required.")
    if not settings.oidc_audience:
        raise AuthorizationError("OIDC_AUDIENCE is not configured.")
    if not expected_email:
        raise AuthorizationError(
            "The expected service-account identity is not configured."
        )

    try:
        claims = id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            audience=settings.oidc_audience,
        )
    except Exception as exc:  # Google auth exposes several verification exceptions.
        raise AuthenticationError("The OIDC token could not be verified.") from exc

    email = str(claims.get("email", ""))
    verified = claims.get("email_verified") is True
    if not verified or not secrets.compare_digest(email, expected_email):
        raise AuthorizationError("The OIDC service-account identity is not authorized.")
    return ServiceIdentity(email=email, audience=str(claims.get("aud", "")))
