"""Thin Workspace discovery, creation and configuration transport (issue #370).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
application services, and the domain-to-wire response mapping. It owns no
authorization policy, no Workspace-configuration semantics, and no
persistence behavior — those belong to the service layer. Discovery/creation
compose the narrowly-scoped lifecycle service; the settings updates delegate
to the existing issue #53/#240 configuration service unchanged. The issue
#374 Workspace deletion delegates to the issue #97 administrative-deletion
service unchanged. Blocking database work runs off the event loop (sync
route handlers; the v1 persistence surface is synchronous). The addressed
errors are the typed
application vocabulary, mapped to HTTP by the application-level
application-error handler.
"""

from __future__ import annotations

from typing import Annotated
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
from openorc.api.schemas.workspaces import (
    DefaultTaskBudgetChange,
    DefaultTaskBudgetSetting,
    GuidanceChange,
    GuidanceSetting,
    ReviewIterationLimitChange,
    ReviewIterationLimitSetting,
    WorkspaceCreate,
    WorkspaceRead,
)
from openorc.config import Settings
from openorc.domain.ownership import Profile, Workspace
from openorc.persistence.pool import get_database_pool
from openorc.services import administrative_deletion, workspace_configuration
from openorc.services.workspace_lifecycle import (
    create_workspace_for_profile,
    list_owned_workspaces,
)

router = APIRouter(tags=["workspaces"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]


@router.get(
    "/profile/workspaces",
    response_model=list[WorkspaceRead],
    operation_id="list_profile_workspaces",
    responses={
        401: DOCUMENTED_AUTH_401,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def list_profile_workspaces(
    request: Request,
    profile: AuthenticatedProfile,
) -> list[WorkspaceRead]:
    """List the Workspaces owned by the authenticated Profile."""
    settings: Settings = request.app.state.settings
    workspaces = list_owned_workspaces(get_database_pool(settings), profile_id=profile.id)
    return [_to_wire(workspace) for workspace in workspaces]


@router.post(
    "/profile/workspaces",
    response_model=WorkspaceRead,
    status_code=201,
    operation_id="create_profile_workspace",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def create_profile_workspace(
    payload: WorkspaceCreate,
    request: Request,
    profile: AuthenticatedProfile,
) -> WorkspaceRead:
    """Create a Workspace owned by the authenticated Profile.

    The name is the only creation input; the owner is always the
    authenticated Profile (canonical v1 ownership — no owner assignment).
    """
    settings: Settings = request.app.state.settings
    workspace = create_workspace_for_profile(
        get_database_pool(settings),
        profile_id=profile.id,
        name=payload.name,
    )
    return _to_wire(workspace)


@router.get(
    "/workspaces/{workspace_id}",
    response_model=WorkspaceRead,
    operation_id="get_workspace",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def read_workspace(
    workspace_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> WorkspaceRead:
    """Read one Workspace the authenticated Profile owns, with its settings."""
    settings: Settings = request.app.state.settings
    workspace = workspace_configuration.get_workspace_configuration(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
    )
    return _to_wire(workspace)


@router.put(
    "/workspaces/{workspace_id}/configuration/review-iteration-limit",
    response_model=ReviewIterationLimitChange,
    operation_id="set_workspace_review_iteration_limit",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def replace_review_iteration_limit(
    workspace_id: UUID,
    payload: ReviewIterationLimitSetting,
    request: Request,
    profile: AuthenticatedProfile,
) -> ReviewIterationLimitChange:
    """Set the review-loop iteration limit (affects future ReviewLoops only)."""
    settings: Settings = request.app.state.settings
    result = workspace_configuration.set_review_iteration_limit(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        review_iteration_limit=payload.review_iteration_limit,
    )
    return ReviewIterationLimitChange(
        workspace_id=result.workspace_id,
        changed=result.changed,
        previous_review_iteration_limit=result.previous_review_iteration_limit,
        new_review_iteration_limit=result.new_review_iteration_limit,
    )


@router.put(
    "/workspaces/{workspace_id}/configuration/guidance",
    response_model=GuidanceChange,
    operation_id="set_workspace_guidance",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def replace_guidance(
    workspace_id: UUID,
    payload: GuidanceSetting,
    request: Request,
    profile: AuthenticatedProfile,
) -> GuidanceChange:
    """Replace the Workspace guidance prose (blank means no guidance)."""
    settings: Settings = request.app.state.settings
    result = workspace_configuration.set_guidance(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        guidance=payload.guidance,
    )
    return GuidanceChange(workspace_id=result.workspace_id, changed=result.changed)


@router.put(
    "/workspaces/{workspace_id}/configuration/default-task-budget",
    response_model=DefaultTaskBudgetChange,
    operation_id="set_workspace_default_task_budget",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def replace_default_task_budget(
    workspace_id: UUID,
    payload: DefaultTaskBudgetSetting,
    request: Request,
    profile: AuthenticatedProfile,
) -> DefaultTaskBudgetChange:
    """Set the default Task USD budget (explicit null disables spending policy)."""
    settings: Settings = request.app.state.settings
    result = workspace_configuration.set_default_task_budget(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        default_task_budget_usd=payload.default_task_budget_usd,
    )
    return DefaultTaskBudgetChange(
        workspace_id=result.workspace_id,
        changed=result.changed,
        previous_default_task_budget_usd=result.previous_default_task_budget_usd,
        new_default_task_budget_usd=result.new_default_task_budget_usd,
    )


@router.delete(
    "/workspaces/{workspace_id}",
    response_model=WorkspaceRead,
    operation_id="delete_workspace",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def delete_workspace(
    workspace_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> WorkspaceRead:
    """Delete the Workspace with every OpenOrc-owned Vault secret it references.

    The Workspace-root lock is held across the whole composition, so no
    Connection can enter the aggregate after the cleanup set is
    established, and every enumerated Connection's referenced OpenOrc-owned
    credential is deleted in the same transaction. This deletes OpenOrc's
    control state only — never GitHub or runtime-owned artifacts. The
    narrower Connection/Project/Repository deletions are the smaller
    alternatives; this is the Workspace-wide irreversible operation.
    """
    settings: Settings = request.app.state.settings
    deleted = administrative_deletion.delete_workspace_record(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
    )
    return _to_wire(deleted)


def _to_wire(workspace: Workspace) -> WorkspaceRead:
    """Map the domain Workspace onto the wire DTO (safe identity/settings only)."""
    return WorkspaceRead(
        id=workspace.id,
        owner_profile_id=workspace.owner_profile_id,
        name=workspace.name,
        created_at=workspace.created_at,
        updated_at=workspace.updated_at,
        review_iteration_limit=workspace.review_iteration_limit,
        guidance=workspace.guidance,
        default_task_budget_usd=workspace.default_task_budget_usd,
    )
