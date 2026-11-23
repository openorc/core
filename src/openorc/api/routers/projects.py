"""Thin Workspace Project transport (issue #372).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
application services, and the domain-to-wire response mapping. A Project
name is genuine Owner-authored configuration, never a GitHub observation.
The issue #374 Project deletion delegates to the issue #97
administrative-deletion service unchanged.
Blocking database work runs off the event loop (sync route handlers; the v1
persistence surface is synchronous). The addressed errors are the typed
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
from openorc.api.schemas.projects import ProjectCreate, ProjectRead
from openorc.config import Settings
from openorc.domain.ownership import Profile, Project
from openorc.persistence.pool import get_database_pool
from openorc.services import administrative_deletion, repository_onboarding

router = APIRouter(tags=["projects"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]


@router.get(
    "/workspaces/{workspace_id}/projects",
    response_model=list[ProjectRead],
    operation_id="list_workspace_projects",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def list_projects(
    workspace_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> list[ProjectRead]:
    """List the Workspace's Projects, deterministically ordered."""
    settings: Settings = request.app.state.settings
    projects = repository_onboarding.list_workspace_projects(
        get_database_pool(settings), profile_id=profile.id, workspace_id=workspace_id
    )
    return [_to_wire(project) for project in projects]


@router.post(
    "/workspaces/{workspace_id}/projects",
    response_model=ProjectRead,
    status_code=201,
    operation_id="create_workspace_project",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def create_project(
    workspace_id: UUID,
    payload: ProjectCreate,
    request: Request,
    profile: AuthenticatedProfile,
) -> ProjectRead:
    """Create one Project in the Workspace owned by the authenticated Profile.

    The name is the only creation input: genuine Owner-authored configuration,
    never a GitHub observation.
    """
    settings: Settings = request.app.state.settings
    project = repository_onboarding.create_workspace_project(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        name=payload.name,
    )
    return _to_wire(project)


@router.delete(
    "/workspaces/{workspace_id}/projects/{project_id}",
    response_model=ProjectRead,
    operation_id="delete_workspace_project",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def delete_project(
    workspace_id: UUID,
    project_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> ProjectRead:
    """Delete one Project with its Task subtrees within the authorized Workspace.

    The Project's Repositories and their Task subtrees go with it; the
    Workspace, sibling Projects, and Workspace-level Connections and their
    credentials are untouched. This deletes OpenOrc's control aggregate
    only — the external GitHub repositories are never touched.
    """
    settings: Settings = request.app.state.settings
    deleted = administrative_deletion.delete_project_record(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        project_id=project_id,
    )
    return _to_wire(deleted)


def _to_wire(project: Project) -> ProjectRead:
    """Map the domain Project onto the wire DTO (identity and name only)."""
    return ProjectRead(
        id=project.id,
        workspace_id=project.workspace_id,
        name=project.name,
        created_at=project.created_at,
        updated_at=project.updated_at,
    )
