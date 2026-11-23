"""Thin Profile GitHub user-authorization transport (issue #373).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369):
the authenticated Profile's GitHub App user-to-server authorization status,
flow start (authorization and reauthorization), and revocation. The OAuth
provider callback ingress is a separate route OUTSIDE this versioned
namespace (``api/routers/github_user_authorization_callback.py``), exactly
like the webhook ingress.

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request parsing, delegation to the shared application
services, and the service-to-wire response mapping. It owns no
authorization policy, no correlation semantics, no credential handling, and
no persistence behavior — those belong to the service layer. The user-flow
external boundaries are composed per request from deployment settings,
failing closed (503) when the deployment has not configured them. Blocking
database work runs off the event loop (sync route handlers; the v1
persistence surface is synchronous). The addressed errors are the typed
application vocabulary, mapped to HTTP by the application-level
application-error handler; no response ever carries credential material
(references, verifiers, correlation states, or tokens).
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_CONFLICT_409,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.dependencies.github_user_flow import resolve_user_flow_composition
from openorc.api.schemas.github_user_authorization import (
    GitHubUserAuthorizationFlowStartRead,
    GitHubUserAuthorizationRevokedRead,
    GitHubUserAuthorizationStatusRead,
    PendingFlowRead,
)
from openorc.config import Settings
from openorc.domain.github_user_authorization import GitHubUserAuthorization
from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlowPurpose,
)
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import github_user_authorization_flows

router = APIRouter(tags=["github-user-authorization"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_STATUS_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    503: DOCUMENTED_UNAVAILABLE_503,
}
_START_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    409: DOCUMENTED_CONFLICT_409,
    503: DOCUMENTED_UNAVAILABLE_503,
}
_REVOKE_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    503: DOCUMENTED_UNAVAILABLE_503,
}

# --- status read -------------------------------------------------------------


@router.get(
    "/profile/github-user-authorization",
    response_model=GitHubUserAuthorizationStatusRead,
    operation_id="read_profile_github_user_authorization",
    responses=_STATUS_RESPONSES,
)
def read_user_authorization(
    request: Request,
    profile: AuthenticatedProfile,
) -> GitHubUserAuthorizationStatusRead:
    """Read the authenticated Profile's GitHub authorization status.

    The durable currentness state (missing/active/revoked are deliberately
    distinguishable), the stable proven GitHub user ID, presentation-only
    login metadata, the current authorization/refresh-expiry instants, and
    the pending flow's purpose and window when one is outstanding. This is
    recorded durable state, never live GitHub freshness, and it never
    carries credential material.
    """
    settings: Settings = request.app.state.settings
    status = github_user_authorization_flows.read_github_user_authorization_status(
        get_database_pool(settings), profile_id=profile.id
    )
    return _to_status_wire(status)


# --- flow start --------------------------------------------------------------


@router.post(
    "/profile/github-user-authorization/flows",
    response_model=GitHubUserAuthorizationFlowStartRead,
    status_code=201,
    operation_id="start_github_user_authorization_flow",
    responses=_START_RESPONSES,
)
def start_user_authorization_flow(
    request: Request,
    profile: AuthenticatedProfile,
) -> GitHubUserAuthorizationFlowStartRead:
    """Start one pending GitHub user-authorization flow for the Profile.

    Supersedes any prior pending flow (at most one per Profile), captures
    the durable revocation evidence the eventual completion is fenced
    against, and returns the documented GitHub authorize URL for the Owner's
    browser. Starting over an already-active authorization is a conflict:
    reauthorization is the supported action there.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    composition = resolve_user_flow_composition(settings)
    start = github_user_authorization_flows.start_github_user_authorization_flow(
        pool,
        profile_id=profile.id,
        purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
        user_flow_client_id=composition.user_flow_client_id,
    )
    return _to_start_wire(start)


@router.post(
    "/profile/github-user-authorization/reauthorization",
    response_model=GitHubUserAuthorizationFlowStartRead,
    status_code=201,
    operation_id="start_github_user_reauthorization_flow",
    responses=_START_RESPONSES,
)
def start_user_reauthorization_flow(
    request: Request,
    profile: AuthenticatedProfile,
) -> GitHubUserAuthorizationFlowStartRead:
    """Start one pending GitHub reauthorization flow over an existing row.

    The reauthorization intent over the Profile's recorded authorization
    (active or revoked — re-establishment is the recovery path). The
    completion is fenced against any Owner revocation committed after this
    authenticated start, including an idempotent revoke of an
    already-revoked authorization. Starting with no authorization row at all
    is a conflict: authorization is the supported action there.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    composition = resolve_user_flow_composition(settings)
    start = github_user_authorization_flows.start_github_user_authorization_flow(
        pool,
        profile_id=profile.id,
        purpose=GitHubUserAuthorizationFlowPurpose.REAUTHORIZE,
        user_flow_client_id=composition.user_flow_client_id,
    )
    return _to_start_wire(start)


# --- revoke ------------------------------------------------------------------


@router.delete(
    "/profile/github-user-authorization",
    response_model=GitHubUserAuthorizationRevokedRead,
    operation_id="revoke_profile_github_user_authorization",
    responses=_REVOKE_RESPONSES,
)
def revoke_user_authorization(
    request: Request,
    profile: AuthenticatedProfile,
) -> GitHubUserAuthorizationRevokedRead:
    """Revoke the authenticated Profile's GitHub authorization (Owner action).

    Deletes the refresh credential, marks the durable authorization revoked,
    and invalidates the Profile's pending authorization flow. Revoking an
    already-revoked authorization is externally idempotent and still
    advances the durable revocation fence, so every successful Owner
    revocation command invalidates reauthorization flows that began before
    it. Revoking with no authorization at all is a not-found outcome.
    """
    settings: Settings = request.app.state.settings
    revoked = github_user_authorization_flows.revoke_profile_github_user_authorization(
        get_database_pool(settings), profile_id=profile.id
    )
    return _to_revoked_wire(revoked)


# --- wire mapping ------------------------------------------------------------


def _to_status_wire(
    status: github_user_authorization_flows.GitHubUserAuthorizationStatusRead,
) -> GitHubUserAuthorizationStatusRead:
    """Map the service status projection onto the wire (safe fields only)."""
    return GitHubUserAuthorizationStatusRead(
        status=status.status,  # type: ignore[arg-type]
        github_user_id=status.github_user_id,
        github_login=status.github_login,
        authorized_at=status.authorized_at,
        refresh_expires_at=status.refresh_expires_at,
        pending_flow=(
            None
            if status.pending_flow is None
            else PendingFlowRead(
                purpose=status.pending_flow.purpose.value,
                started_at=status.pending_flow.started_at,
                expires_at=status.pending_flow.expires_at,
            )
        ),
    )


def _to_start_wire(
    start: github_user_authorization_flows.GitHubUserAuthorizationFlowStart,
) -> GitHubUserAuthorizationFlowStartRead:
    """Map the started-flow projection onto the wire (safe fields only)."""
    return GitHubUserAuthorizationFlowStartRead(
        authorize_url=start.authorize_url,
        purpose=start.purpose.value,
        expires_at=start.expires_at,
    )


def _to_revoked_wire(
    revoked: GitHubUserAuthorization,
) -> GitHubUserAuthorizationRevokedRead:
    """Map the revoked authorization onto the wire (safe durable facts only).

    The revocation instant is CHECK-required for a revoked row; the
    fallback keeps the projection total even if a caller ever observes a
    fresh-from-service row before the durable instant normalizes.
    """
    return GitHubUserAuthorizationRevokedRead(
        github_user_id=revoked.github_user_id,
        github_login=revoked.github_login,
        revoked_at=(revoked.revoked_at if revoked.revoked_at is not None else revoked.updated_at),
    )
