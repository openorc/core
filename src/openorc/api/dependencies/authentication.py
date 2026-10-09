"""Authenticated-Profile request dependency (the #52 boundary at the #162 transport).

The first authenticated API dependency: extracts the bearer token from the
``Authorization`` header, verifies it through the #52 authentication
application service (Supabase Auth token verification plus canonical Profile
resolution), and hands the resolved Profile to authorized routes.

- The dependency is synchronous: FastAPI runs sync dependencies off the
  event loop, so the blocking token verification (JWKS retrieval, an
  external call) and the Profile-resolution database work never run on the
  event loop.
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

from functools import lru_cache
from typing import Annotated

from fastapi import Header, Request

from openorc.adapters.supabase import SupabaseAccessTokenVerifier
from openorc.config import Settings
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services.authentication import authenticate
from openorc.services.errors import AuthenticationError, IntegrationNotConfiguredError

__all__ = ["authenticated_profile"]

_BEARER_SCHEME = "Bearer "


def authenticated_profile(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
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
        token=_extract_bearer_token(authorization),
    ).profile


def _extract_bearer_token(authorization: str | None) -> str:
    """Extract the bearer-token value from the Authorization header value.

    Missing, non-bearer, and empty-token headers are the same uniform
    authentication failure: no header content is ever reflected back.
    """
    if authorization is None or not authorization.startswith(_BEARER_SCHEME):
        raise AuthenticationError("authentication failed: the access token is missing or malformed")
    token = authorization[len(_BEARER_SCHEME) :].strip()
    if not token:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")
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
