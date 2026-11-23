"""Thin Workspace Repository mapping transport (issue #372).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
Owner-authorized application services, and the domain-to-wire response
mapping. It owns no authorization policy, no claim/observation trust
boundary, and no persistence behavior — those belong to the service layer.
The claim command carries the stable external repository identity and its
owning Project alone: a repository claim is unverified Owner configuration,
never a GitHub observation, and no observation fact is accepted or echoed by
any operation here. Route binding/unbinding delegate to the existing issue
#57 route services unchanged. Blocking database work runs off the event loop
(sync route handlers; the v1 persistence surface is synchronous). The
addressed errors are the typed application vocabulary, mapped to HTTP by the
application-level application-error handler.
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
from openorc.api.schemas.repositories import (
    InstallationRouteBinding,
    RepositoryClaimCreate,
    RepositoryInstallationRouteRead,
    RepositoryObservationRead,
    RepositoryRead,
)
from openorc.config import Settings
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import github_installations, repository_onboarding
from openorc.services.repository_onboarding import WorkspaceRepositoryStatus

router = APIRouter(tags=["repositories"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_REPOSITORIES_PATH = "/workspaces/{workspace_id}/repositories"
_REPOSITORY_PATH = "/workspaces/{workspace_id}/repositories/{repository_id}"
_ROUTE_PATH = "/workspaces/{workspace_id}/repositories/{repository_id}/installation-route"

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
_BIND_RESPONSES = _CLAIM_RESPONSES
_UNBIND_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    503: DOCUMENTED_UNAVAILABLE_503,
}


@router.post(
    _REPOSITORIES_PATH,
    response_model=RepositoryRead,
    status_code=201,
    operation_id="claim_workspace_repository",
    responses=_CLAIM_RESPONSES,
)
def claim_repository(
    workspace_id: UUID,
    payload: RepositoryClaimCreate,
    request: Request,
    profile: AuthenticatedProfile,
) -> RepositoryRead:
    """Claim one stable GitHub repository identity into the Workspace.

    The claim carries the stable external repository ID and its owning
    Project alone: it is unverified Owner configuration persisted in claim
    state — no observed presentation fact is accepted here — and it never
    implies access or permission. A duplicate stable identity within one
    Workspace is a conflict; the same identity in another Workspace is an
    independent record.
    """
    settings: Settings = request.app.state.settings
    repository = repository_onboarding.claim_workspace_repository(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        project_id=payload.project_id,
        github_repository_id=payload.github_repository_id,
    )
    return _to_wire(WorkspaceRepositoryStatus(repository=repository, installation=None))


@router.get(
    _REPOSITORIES_PATH,
    response_model=list[RepositoryRead],
    operation_id="list_workspace_repositories",
    responses=_READ_RESPONSES,
)
def list_repositories(
    workspace_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> list[RepositoryRead]:
    """List the Workspace's Repository mapping statuses.

    Each record carries its recorded state verbatim: a null ``observed``
    block is the unverified claim state, and the configured route reference
    is a configuration fact that never implies access or permission.
    """
    settings: Settings = request.app.state.settings
    statuses = repository_onboarding.list_workspace_repositories(
        get_database_pool(settings), profile_id=profile.id, workspace_id=workspace_id
    )
    return [_to_wire(status) for status in statuses]


@router.get(
    _REPOSITORY_PATH,
    response_model=RepositoryRead,
    operation_id="read_workspace_repository",
    responses=_READ_RESPONSES,
)
def read_repository(
    workspace_id: UUID,
    repository_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> RepositoryRead:
    """Read one Repository mapping status of the Workspace.

    A foreign or absent repository is the uniform not-found outcome; the
    recorded state is durable evidence, never live GitHub freshness.
    """
    settings: Settings = request.app.state.settings
    status = repository_onboarding.read_workspace_repository(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
    )
    return _to_wire(status)


@router.put(
    _ROUTE_PATH,
    response_model=RepositoryRead,
    operation_id="bind_repository_installation_route",
    responses=_BIND_RESPONSES,
)
def bind_installation_route(
    workspace_id: UUID,
    repository_id: UUID,
    payload: InstallationRouteBinding,
    request: Request,
    profile: AuthenticatedProfile,
) -> RepositoryRead:
    """Bind or rebind the Repository's explicit installation route.

    Delegates to the issue #57 route service: the addressed installation must
    be a record of the authorized Workspace — a route into another Workspace
    is the uniform not-found outcome and is unrepresentable durably. Binding
    changes the route configuration only and makes no claim about current
    repository access.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    github_installations.bind_repository_installation(
        pool,
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_installation_id=payload.installation_id,
    )
    # The projected response is the repository's current recorded status,
    # composed through the shared read service — never a router-side
    # reconstruction.
    return _to_wire(
        repository_onboarding.read_workspace_repository(
            pool,
            profile_id=profile.id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
    )


@router.delete(
    _ROUTE_PATH,
    response_model=RepositoryRead,
    operation_id="unbind_repository_installation_route",
    responses=_UNBIND_RESPONSES,
)
def unbind_installation_route(
    workspace_id: UUID,
    repository_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> RepositoryRead:
    """Clear the Repository's installation route without deleting identity.

    Delegates to the issue #57 route service: the route configuration is
    simply absent and the Repository is not usable for GitHub operations
    until it is explicitly bound again. Deleting or disconnecting OpenOrc
    configuration never uninstalls the GitHub App.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    github_installations.unbind_repository_installation(
        pool,
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
    )
    # The projected response is the repository's current recorded status,
    # composed through the shared read service — never a router-side
    # reconstruction.
    return _to_wire(
        repository_onboarding.read_workspace_repository(
            pool,
            profile_id=profile.id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
    )


def _to_wire(status: WorkspaceRepositoryStatus) -> RepositoryRead:
    """Map the domain status onto the wire DTO (claim, observed, and route only).

    The ``observed`` block is present only when trusted GitHub reconciliation
    has established the record; the route reference is configuration only.
    No credential material and no derived usability/authorization status
    exists on this surface.
    """
    repository = status.repository
    return RepositoryRead(
        id=repository.id,
        project_id=repository.project_id,
        workspace_id=repository.workspace_id,
        github_repository_id=repository.identity.github_repository_id,
        observed=(
            None
            if repository.metadata is None
            else RepositoryObservationRead(
                owner_login=repository.metadata.owner_login,
                name=repository.metadata.name,
                html_url=repository.metadata.html_url,
                is_private=repository.metadata.is_private,
                default_branch=repository.metadata.default_branch,
            )
        ),
        installation_route=(
            None
            if status.installation is None
            else RepositoryInstallationRouteRead(
                installation_id=status.installation.id,
                github_installation_id=status.installation.identity.github_installation_id,
            )
        ),
        created_at=repository.created_at,
        updated_at=repository.updated_at,
    )
