"""Thin Workspace GitHub installation mapping transport (issue #372).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
Owner-authorized application services, and the domain-to-wire response
mapping. It owns no authorization policy, no claim/observation trust
boundary, and no persistence behavior — those belong to the service layer.
The claim command carries the stable external installation identity alone:
an installation claim is unverified Owner configuration, never a GitHub
observation, and no observation fact is accepted or echoed by any operation
here. Blocking database work runs off the event loop (sync route handlers;
the v1 persistence surface is synchronous). The addressed errors are the
typed application vocabulary, mapped to HTTP by the application-level
application-error handler.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_CONFLICT_409,
    DOCUMENTED_INVALID_COMMAND_422,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.github_installations import (
    GitHubInstallationClaimCreate,
    GitHubInstallationObservationRead,
    GitHubInstallationRead,
)
from openorc.config import Settings
from openorc.domain.github_installations import GitHubInstallation
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import github_installation_claims

router = APIRouter(tags=["github-installations"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_INSTALLATIONS_PATH = "/workspaces/{workspace_id}/github/installations"
_INSTALLATION_PATH = "/workspaces/{workspace_id}/github/installations/{installation_id}"

_CLAIM_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}
_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    503: DOCUMENTED_UNAVAILABLE_503,
}


@router.post(
    _INSTALLATIONS_PATH,
    response_model=GitHubInstallationRead,
    status_code=201,
    operation_id="record_workspace_installation_claim",
    responses=_CLAIM_RESPONSES,
)
def record_installation_claim(
    workspace_id: UUID,
    payload: GitHubInstallationClaimCreate,
    request: Request,
    profile: AuthenticatedProfile,
) -> GitHubInstallationRead:
    """Record one Owner installation claim for the Workspace.

    The claim carries the stable external installation ID alone: it is
    unverified Owner configuration persisted in claim state, never a GitHub
    observation, and it never implies access or permission. Re-claiming an
    already-claimed or already-established installation is idempotent and
    returns the durable record unchanged.
    """
    settings: Settings = request.app.state.settings
    installation = github_installation_claims.record_installation_claim(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        github_installation_id=payload.github_installation_id,
    )
    return _to_wire(installation)


@router.get(
    _INSTALLATIONS_PATH,
    response_model=list[GitHubInstallationRead],
    operation_id="list_workspace_installation_claims",
    responses=_READ_RESPONSES,
)
def list_installation_claims(
    workspace_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> list[GitHubInstallationRead]:
    """List the Workspace's installation mapping records.

    Each record carries its recorded state verbatim: a null ``observed``
    block is the unverified claim state, and observed facts appear only once
    trusted GitHub reconciliation has recorded them. Recorded status is not
    live GitHub freshness and never implies access or permission.
    """
    settings: Settings = request.app.state.settings
    installations = github_installation_claims.list_workspace_installation_claims(
        get_database_pool(settings), profile_id=profile.id, workspace_id=workspace_id
    )
    return [_to_wire(installation) for installation in installations]


@router.get(
    _INSTALLATION_PATH,
    response_model=GitHubInstallationRead,
    operation_id="read_workspace_installation_claim",
    responses=_READ_RESPONSES,
)
def read_installation_claim(
    workspace_id: UUID,
    installation_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> GitHubInstallationRead:
    """Read one installation mapping record of the Workspace.

    A foreign or absent installation is the uniform not-found outcome.
    """
    settings: Settings = request.app.state.settings
    installation = github_installation_claims.read_installation_claim(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        installation_id=installation_id,
    )
    return _to_wire(installation)


def _to_wire(installation: GitHubInstallation) -> GitHubInstallationRead:
    """Map the domain record onto the wire DTO (claim and observed blocks only).

    The ``observed`` block is present only when trusted GitHub reconciliation
    has established the record; no credential material and no derived
    usability/authorization status exists on this surface.
    """
    return GitHubInstallationRead(
        id=installation.id,
        workspace_id=installation.workspace_id,
        github_installation_id=installation.identity.github_installation_id,
        observed=(
            None
            if installation.account is None
            else GitHubInstallationObservationRead(
                github_account_id=installation.account.github_account_id,
                account_login=installation.account.login,
                account_type=installation.account.type,
                suspended_at=installation.suspended_at,
            )
        ),
        created_at=installation.created_at,
        updated_at=installation.updated_at,
    )
