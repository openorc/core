"""Authenticated-Profile request dependency (the #52 boundary at the #162 transport).

The first authenticated API dependency: extracts the bearer credential from
the ``Authorization`` header through the declared ``SupabaseSessionJwtBearer``
security scheme, verifies it through the #52 authentication application
service (Supabase Auth token verification plus canonical Profile resolution),
and hands the resolved Profile to authorized routes.

- The dependency is synchronous: FastAPI runs sync dependencies off the
  event loop, so the blocking token verification (JWKS retrieval, an
  external call) and the Profile-resolution database work never run on the
  event loop. The security-scheme sub-dependency only reads the header.
- The declared scheme is what the generated OpenAPI advertises (issue #369):
  the Supabase Auth session JWT obtained through GitHub-only sign-in,
  supplied as an HTTP Bearer credential. Publishable and secret Supabase
  keys are not user JWTs, and no new login/token system is introduced.
- Failures surface as the typed application vocabulary and are mapped to
  HTTP by the application-level application-error handler: invalid/missing
  credentials to a uniform 401, JWKS retrieval failures to 503, and an
  unconfigured Supabase project URL to a fail-closed 503 (like the webhook
  secret boundary).
- One verifier instance per (project URL, audience) per process is reused so
  the adapter's JWKS cache works across requests; it never stores token
  material.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Annotated

from fastapi import Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from openorc.adapters.supabase import SupabaseAccessTokenVerifier
from openorc.config import Settings
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services.authentication import authenticate
from openorc.services.errors import AuthenticationError, IntegrationNotConfiguredError

__all__ = [
    "AuthenticatedSession",
    "authenticated_profile",
    "authenticated_session",
    "supabase_session_jwt_bearer",
]

_UNAUTHENTICATED_MESSAGE = "authentication failed: the access token is missing or malformed"

# The declared security scheme (issue #369): the Supabase Auth session JWT
# obtained through GitHub-only sign-in, supplied as an HTTP Bearer credential.
# ``auto_error=False`` keeps every credential failure inside this module's
# typed uniform-401 contract — the scheme never produces its own HTTP
# exception, and no credential material is ever stored or reflected back.
supabase_session_jwt_bearer = HTTPBearer(
    scheme_name="SupabaseSessionJwtBearer",
    bearerFormat="JWT",
    description=(
        "Supabase Auth session JWT obtained through GitHub sign-in. "
        "Publishable and secret Supabase API keys are not user JWTs."
    ),
    auto_error=False,
)


def authenticated_profile(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Security(supabase_session_jwt_bearer)
    ] = None,
) -> Profile:
    """Resolve the authenticated Profile for one request (#52 boundary).

    The returned Profile is ordinary authenticated identity, never itself an
    authorization capability: Workspace ownership is established by the
    application services each route composes.
    """
    settings: Settings = request.app.state.settings
    return authenticate(
        get_database_pool(settings),
        _verifier_for(settings),
        token=_extract_bearer_token(credentials),
    ).profile


@dataclass(frozen=True, slots=True)
class AuthenticatedSession:
    """The authenticated Profile plus its verified credential expiry.

    The dependency's response to the authorized Workspace SSE stream boundary
    (issue #399): the same single #52 authentication resolution, with the
    principal's verified ``exp`` surfaced so a live stream terminates no later
    than credential validity. The expiry is an ordinary verified claim value —
    never credential material — and the Profile is ordinary authenticated
    identity, never itself an authorization capability.
    """

    profile: Profile
    expires_at: datetime


def authenticated_session(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Security(supabase_session_jwt_bearer)
    ] = None,
) -> AuthenticatedSession:
    """Resolve the authenticated Profile and its verified credential expiry (#399).

    The same single ``authenticate`` boundary as :func:`authenticated_profile`
    (one verification, one Profile resolution); the Workspace stream route is
    the first consumer of the verified token validity bound. No new
    authentication subsystem and no token-refresh mechanism exist: a stream
    outliving its credential is prevented by terminating at the expiry, and
    the client's documented reconnect re-authenticates.
    """
    settings: Settings = request.app.state.settings
    user = authenticate(
        get_database_pool(settings),
        _verifier_for(settings),
        token=_extract_bearer_token(credentials),
    )
    return AuthenticatedSession(profile=user.profile, expires_at=user.principal.expires_at)


def _extract_bearer_token(credentials: HTTPAuthorizationCredentials | None) -> str:
    """Extract the bearer-token value under the uniform failure contract.

    A missing header, a non-bearer scheme (the ``Bearer`` capitalization is
    required, as before), and an empty token are the same uniform
    authentication failure: no header content is ever reflected back.
    """
    if credentials is None or credentials.scheme != "Bearer":
        raise AuthenticationError(_UNAUTHENTICATED_MESSAGE)
    token = credentials.credentials.strip()
    if not token:
        raise AuthenticationError(_UNAUTHENTICATED_MESSAGE)
    return token


def _verifier_for(settings: Settings) -> SupabaseAccessTokenVerifier:
    """Build (or reuse) the process-level access-token verifier."""
    project_url = (settings.supabase_url or "").strip()
    if not project_url:
        raise IntegrationNotConfiguredError(
            "the Supabase project URL is not configured for this process"
        )
    return _cached_verifier(project_url, settings.supabase_jwt_audience)


@lru_cache(maxsize=8)
def _cached_verifier(project_url: str, audience: str) -> SupabaseAccessTokenVerifier:
    # One verifier instance per (project URL, audience) per process: the
    # adapter's JWKS cache is instance-scoped, so per-request construction
    # would refetch signing keys on every request. The inputs are deployment
    # configuration, never caller-supplied material.
    return SupabaseAccessTokenVerifier(project_url=project_url, audience=audience)
