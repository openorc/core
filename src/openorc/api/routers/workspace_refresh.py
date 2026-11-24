"""Thin scoped GitHub refresh transport (issue #375).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction, path
parsing, delegation to the Owner-authorized admission/status services, and
the wire mapping. It owns no authorization policy, no admission containment
semantics, and no execution-submission meaning. A 202 acknowledges
authorization and established F1 submission acceptance only — never
delivery, execution, or completion; the typed refusal/uncertainty/conflict
vocabulary maps to HTTP by the application-level handler. Blocking database
work runs off the event loop (sync route handlers; the v1 persistence
surface is synchronous).
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
from openorc.api.schemas.refresh import (
    GitHubRefreshAdmissionRead,
    GitHubRefreshStatusRead,
    GitHubRefreshSummaryRead,
    GitHubRefreshUnitErrorRead,
)
from openorc.config import Settings
from openorc.domain.github_refreshes import GitHubRefreshRecord, GitHubRefreshSummary
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import github_refresh
from openorc.workers.github_operations import F1RefreshExecutionSubmission

router = APIRouter(tags=["github-refresh"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_WORKSPACE_REFRESH_PATH = "/workspaces/{workspace_id}/refresh"
_REPOSITORY_REFRESH_PATH = "/workspaces/{workspace_id}/repositories/{repository_id}/refresh"
_REFRESH_STATUS_PATH = "/workspaces/{workspace_id}/refresh/{refresh_id}"

_ADMIT_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}
_STATUS_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}


def _record_to_admission_wire(record: GitHubRefreshRecord) -> GitHubRefreshAdmissionRead:
    return GitHubRefreshAdmissionRead(
        refresh_id=record.id,
        workspace_id=record.workspace_id,
        repository_id=record.repository_id,
        status=record.status.value,
    )


def _summary_to_wire(summary: GitHubRefreshSummary | None) -> GitHubRefreshSummaryRead | None:
    if summary is None:
        return None
    return GitHubRefreshSummaryRead(
        repositories_checked=summary.repositories_checked,
        issues_discovered=summary.issues_discovered,
        issues_updated=summary.issues_updated,
        issues_closed=summary.issues_closed,
        pull_requests_checked=summary.pull_requests_checked,
    )


@router.post(
    _WORKSPACE_REFRESH_PATH,
    response_model=GitHubRefreshAdmissionRead,
    status_code=202,
    operation_id="request_workspace_github_refresh",
    responses=_ADMIT_RESPONSES,
)
def request_workspace_refresh(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
) -> GitHubRefreshAdmissionRead:
    """Request one authorized on-demand GitHub refresh of the whole Workspace.

    Admits the durable refresh record (with the per-scope active-overlap
    containment) and submits its execution through F1. The 202 body is the
    accepted record: authorization and submission acceptance only — never
    delivery, execution, or completion. Poll the status resource for the
    truthful lifecycle.
    """
    settings: Settings = request.app.state.settings
    record = github_refresh.request_workspace_github_refresh_for_owner(
        get_database_pool(settings),
        F1RefreshExecutionSubmission(request.app.state.operation_submitter),
        profile_id=profile.id,
        workspace_id=workspace_id,
    )
    return _record_to_admission_wire(record)


@router.post(
    _REPOSITORY_REFRESH_PATH,
    response_model=GitHubRefreshAdmissionRead,
    status_code=202,
    operation_id="request_repository_github_refresh",
    responses=_ADMIT_RESPONSES,
)
def request_repository_refresh(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    repository_id: UUID,
) -> GitHubRefreshAdmissionRead:
    """Request one authorized on-demand refresh of one Workspace Repository.

    The Repository-scope admission composes the same Owner authorization,
    containment, and F1 submission semantics as the Workspace scope, bounded
    to the addressed Repository's durable aggregate.
    """
    settings: Settings = request.app.state.settings
    record = github_refresh.request_repository_github_refresh_for_owner(
        get_database_pool(settings),
        F1RefreshExecutionSubmission(request.app.state.operation_submitter),
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
    )
    return _record_to_admission_wire(record)


@router.get(
    _REFRESH_STATUS_PATH,
    response_model=GitHubRefreshStatusRead,
    operation_id="get_workspace_github_refresh",
    responses=_STATUS_RESPONSES,
)
def get_workspace_refresh_status(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    refresh_id: UUID,
) -> GitHubRefreshStatusRead:
    """Return one refresh record's truthful status within the Workspace.

    The stored record presented verbatim, with an expired
    ``accepted``/``running`` record presented as ``unknown``. Timestamps and
    summary stay null until established; the errors are the bounded
    sanitized per-unit classifications. Reads never mutate.
    """
    settings: Settings = request.app.state.settings
    projection = github_refresh.get_workspace_github_refresh_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        refresh_id=refresh_id,
    )
    return GitHubRefreshStatusRead(
        refresh_id=projection.id,
        workspace_id=projection.workspace_id,
        repository_id=projection.repository_id,
        status=projection.status.value,
        started_at=projection.started_at,
        completed_at=projection.completed_at,
        summary=_summary_to_wire(projection.summary),
        errors=[
            GitHubRefreshUnitErrorRead(
                family=error.family,
                code=error.code,
                workspace_id=error.workspace_id,
                repository_id=error.repository_id,
                github_issue_id=error.github_issue_id,
                delivery_guid=error.delivery_guid,
            )
            for error in projection.errors
        ],
    )
