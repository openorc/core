"""Thin Owner Task-command transport: issue intake and queue-first Start (issue #378).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction, path
and body parsing, delegation to the Owner-authorized command services, and
the authoritative post-write snapshot wire mapping. It owns no authorization
policy, no eligibility semantics, no queue or wake orchestration, and no
audit emission — those belong to the service layer (the #377 implementation
owns the ``task_started`` audit fact inside the Start service's arbitration
transaction; no event is ever emitted from HTTP). Blocking database and
GitHub work runs off the event loop (sync route handlers; the v1
persistence surface is synchronous). The addressed errors are the typed
application vocabulary, mapped to HTTP by the application-level handler.

The command responses are the services' authoritative post-write snapshots
exactly as returned — never a re-derived current-state projection (the
existing Task-read resource owns subsequent retrieval) and never a
command-status ledger. The Start response documents the committed
``queued`` entry and the separately-typed post-commit queue-wake delivery
evidence; successful Start means enqueue, never FIFO admission, READY
sessions, or completed planning.
"""

from __future__ import annotations

from decimal import Decimal
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
from openorc.api.dependencies.github_app_client import resolve_github_app_client
from openorc.api.schemas.tasks import (
    RepositoryIssueTaskCreatedRead,
    TaskCommandSnapshotRead,
    TaskStartCommand,
    TaskStartedRead,
    TaskStartSnapshotRead,
)
from openorc.config import Settings
from openorc.domain.ownership import Profile
from openorc.domain.tasks import Task
from openorc.persistence.pool import get_database_pool
from openorc.services import task_intake, task_start
from openorc.workers.task_queue_operations import F1TaskQueueWakeSubmission

router = APIRouter(tags=["tasks"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_ISSUE_TASKS_PATH = (
    "/workspaces/{workspace_id}/repositories/{repository_id}/issues/{issue_id}/tasks"
)
_TASK_START_PATH = "/workspaces/{workspace_id}/tasks/{task_id}/start"

_COMMAND_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}


def _decimal_to_wire(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _command_snapshot_to_wire(task: Task) -> TaskCommandSnapshotRead:
    """Map one authoritative post-write Task snapshot onto the command wire.

    The mapping is verbatim over the service-returned snapshot — the exact
    facts the guarded write committed, including the Task's actual returned
    status. No current-state re-derivation happens here.
    """
    return TaskCommandSnapshotRead(
        task_id=task.id,
        workspace_id=task.workspace_id,
        repository_id=task.repository_id,
        github_issue_id=task.github_issue_id,
        github_issue_number=task.github_issue_number,
        status=task.status.value,
        state_token=task.state_token,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


@router.post(
    _ISSUE_TASKS_PATH,
    response_model=RepositoryIssueTaskCreatedRead,
    operation_id="create_repository_issue_task",
    responses=_COMMAND_RESPONSES,
)
def create_repository_issue_task(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    repository_id: UUID,
    issue_id: UUID,
) -> RepositoryIssueTaskCreatedRead:
    """Intake one addressed GitHub issue as a Task (issue-level ``create_task``).

    Addressed by the exact issue projection UUID within the Workspace
    Repository. The command freshly revalidates the authenticated Owner's
    exact Workspace/Repository authority and the full GitHub eligibility
    (open issue, no blocking dependency, usable installation route, no
    current Task for the stable identity, operational account) — an offered
    action is not authorization, and intake requires no pre-existing Task
    or state token. A successful intake creates a ``ready_to_plan`` Task
    with the atomic ``task_created`` fact and never freezes a requirements
    baseline or budget, never enqueues, and never begins planning. A
    concurrent duplicate converges onto the existing current Task
    (``created: false``), preserving exactly one current/non-archived Task
    per GitHub issue.
    """
    settings: Settings = request.app.state.settings
    intake = task_intake.intake_repository_task_for_issue_projection_for_owner(
        get_database_pool(settings),
        resolve_github_app_client(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        issue_id=issue_id,
    )
    return RepositoryIssueTaskCreatedRead(
        task=_command_snapshot_to_wire(intake.task),
        created=intake.created,
    )


@router.post(
    _TASK_START_PATH,
    response_model=TaskStartedRead,
    operation_id="start_task",
    responses=_COMMAND_RESPONSES,
)
def start_task(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    task_id: UUID,
    command: TaskStartCommand,
) -> TaskStartedRead:
    """Start one addressed Task through the queue-first Owner Start.

    Addressed by the exact current Task and its opaque ``state_token``. The
    command freshly revalidates the exact Workspace/Task authority and the
    full GitHub eligibility, then commits ``ready_to_plan → queued`` with
    the frozen requirements fingerprint, the then-current Workspace Task
    budget, and the original queue-entry timestamp in the same guarded
    write (the atomic Owner-attributed ``task_started`` audit fact is the
    #377 implementation's, recorded in that same transaction). The response
    is the committed post-write snapshot even when FIFO admission advances
    the Task before it is written: successful Start means enqueue, never
    FIFO admission, READY sessions, or completed planning — the ONE FIFO
    processor owns every admission. ``queue_wake_outcome`` is the
    separately-typed post-commit queue-wake delivery evidence; a refused,
    uncertain, or unwired wake never fails or reverses the committed queue
    entry, and the durable ``queued`` facts own recovery.
    """
    settings: Settings = request.app.state.settings
    outcome = task_start.start_task_for_owner(
        get_database_pool(settings),
        resolve_github_app_client(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=command.state_token,
        queue_wake=F1TaskQueueWakeSubmission(request.app.state.operation_submitter),
    )
    task = outcome.task
    return TaskStartedRead(
        task=TaskStartSnapshotRead(
            task_id=task.id,
            workspace_id=task.workspace_id,
            repository_id=task.repository_id,
            github_issue_id=task.github_issue_id,
            github_issue_number=task.github_issue_number,
            status=task.status.value,
            state_token=task.state_token,
            created_at=task.created_at,
            updated_at=task.updated_at,
            queued_at=task.queued_at,
            source_requirements_fingerprint=task.source_requirements_fingerprint,
            authorized_task_budget_usd=_decimal_to_wire(task.authorized_task_budget_usd),
        ),
        queue_wake_outcome=(
            None if outcome.queue_wake_outcome is None else outcome.queue_wake_outcome.value
        ),
    )
