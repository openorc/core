"""Authoritative Task intake services (issue #60).

Create a Task only after a fresh authoritative GitHub check, immediately
before the short creation transaction. The critical path is strictly
ordered to minimize the race window the fresh-observation contract exists
for:

1. resolve the explicit currently usable installation route;
2. fresh B3 issue reconciliation (issue existence/open state + the
   requirements fingerprint, refreshing the projection) — external GitHub
   I/O with no database transaction open;
3. fresh blocked-by dependency observation, mirrored in its own short
   transaction — authoritative input to eligibility; an unobservable
   blocking state fails closed;
4. the final short transaction: eligibility against the facts just mirrored
   (issue open; empty blocked-by edge set ⇒ not blocked — parent/sub-issue
   hierarchy is categorically absent from the predicate), the current-Task
   re-check, then ``create_task`` with the just-reconciled fingerprint. A
   concurrent duplicate converges on the existing database partial-unique
   invariant and is classified from re-read durable state, never from the
   constraint name. The ``task_created`` event is recorded atomically with
   the creation;
5. ONLY after successful creation, the non-gating hierarchy
   synchronization runs as an independent reconciliation: its outcome
   (including a GitHub-error failure that preserves the prior mirror) never
   alters the already-determined intake result.

Eligibility is exactly: the issue exists and is open; GitHub does not
currently report it blocked by an issue dependency; the Repository has an
explicit currently usable GitHub installation route; no current
non-archived Task exists for the stable Repository + GitHub issue ID. No
new Workspace/Project policy is invented here beyond the existing
ownership/route resolvers. Cancellation/reopen semantics follow the Phase 1
currentness model: a cancelled archived Task releases the issue; a fresh
Task follows a completed one only when authoritative GitHub state shows the
issue open again.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.github import GitHubAppClient
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.github_issues import GitHubIssueState
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence.events import record_workflow_event
from openorc.persistence.github_issue_relations import list_github_issue_blocked_by_edges
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import create_task, find_current_task_for_issue
from openorc.services import github_issue_relations, github_reconciliation
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_repository

__all__ = [
    "RepositoryTaskIntake",
    "intake_repository_task",
    "intake_repository_task_for_owner",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE = "openorc.services.task_intake"
_INTAKE_SPAN_NAME = "task_intake.intake_repository_task"
_OWNER_INTAKE_SPAN_NAME = "task_intake.intake_repository_task_for_owner"

_OPENORC_ACTOR = WorkflowActorContext(WorkflowEventActor.OPENORC, None)


@dataclass(frozen=True, slots=True)
class RepositoryTaskIntake:
    """The durable outcome of one authoritative Task intake invocation.

    ``task`` is the current Task attempt with its immutable source baseline;
    ``created`` is true exactly for the invocation whose insert won the
    current-Task uniqueness invariant (a concurrent duplicate intake
    classifies onto the existing current Task with ``created=False`` rather
    than producing two current Tasks).
    """

    task: Task
    created: bool


def _require_uuid_command(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_issue_number_command(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidCommandError("issue_number must be a positive integer")


def intake_repository_task(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
) -> RepositoryTaskIntake:
    """Create a Task only after a fresh authoritative GitHub check.

    The trusted, system-capable intake primitive for the later
    webhook-intake and repeatable-reconciliation layers: durably resolved
    Workspace/Repository identity, no authenticated actor, fail closed
    uniformly. Raises ``NotFoundError`` uniformly for an absent or
    foreign-Workspace repository, an unconfigured/foreign route, or a fresh
    authoritative check reporting the issue closed; ``ConflictError`` when
    GitHub currently reports the issue blocked, when the addressed number is
    a pull request, or for the classified duplicate-intake outcome;
    ``AuthorizationError``/``ExternalOperationFailedError``/
    ``ExternalOperationUncertainError`` for the classified GitHub outcomes —
    in particular, an unobservable blocking state fails intake closed.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _INTAKE_SPAN_NAME) as span:
        annotate_span(span, operation=_INTAKE_SPAN_NAME, workspace_id=str(workspace_id))
        # (1) The explicit currently usable installation route (fail closed).
        github_issue_relations._resolve_system_repository_installation_route(
            pool, workspace_id=workspace_id, repository_id=repository_id
        )
        # (2) Fresh B3 issue reconciliation: existence, open state, and the
        # requirements fingerprint, with no database transaction open.
        reconciliation = github_reconciliation.reconcile_repository_issue(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
        )
        projection = reconciliation.issue
        if projection.state is not GitHubIssueState.OPEN:
            # The fresh authoritative check: a closed issue is not startable.
            raise NotFoundError("the addressed GitHub issue is not open")
        # (3) Fresh blocked-by dependency observation, mirrored in its own
        # short transaction — the authoritative eligibility input.
        dependency_sync = github_issue_relations.synchronize_repository_issue_dependencies(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=projection.identity.github_issue_id,
        )
        if dependency_sync.blocked:
            raise ConflictError(
                "GitHub currently reports the addressed issue as blocked by an issue dependency"
            )
        # (4) The final short creation transaction: eligibility against the
        # fresh facts just mirrored, then the race-classified create.
        task, created = _apply_intake_creation(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=projection.identity.github_issue_id,
            github_issue_number=projection.issue_number,
            requirements_fingerprint=projection.requirements_fingerprint,
        )
        # (5) The non-gating hierarchy sync: strictly AFTER creation. Its
        # outcome never alters the already-determined intake result.
        _synchronize_hierarchy_after_creation(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=projection.identity.github_issue_id,
        )
        return RepositoryTaskIntake(task=task, created=created)


def _apply_intake_creation(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    github_issue_number: int,
    requirements_fingerprint: str,
) -> tuple[Task, bool]:
    """Evaluate eligibility against fresh mirrored facts and create the Task.

    One short transaction: the account-deletion barrier first, the
    not-blocked check against the facts just mirrored, the current-Task
    re-check, then the insert carrying the immutable source baseline and the
    ``task_created`` event. A lost current-Task uniqueness race is
    classified from re-read durable state, never from the constraint name.
    """
    with composed_transaction(pool) as tx_pool:
        workspace = get_workspace(tx_pool, workspace_id)
        if workspace is None:
            raise NotFoundError("the requested workspace is not available")
        require_account_operational(tx_pool, profile_id=workspace.owner_profile_id)
        blockers = list_github_issue_blocked_by_edges(
            tx_pool, repository_id=repository_id, github_issue_id=github_issue_id
        )
        if blockers:
            raise ConflictError(
                "GitHub currently reports the addressed issue as blocked by an issue dependency"
            )
        current = find_current_task_for_issue(
            tx_pool, repository_id=repository_id, github_issue_id=github_issue_id
        )
        if current is not None:
            # The classified duplicate-intake outcome: converge on the
            # existing current Task rather than producing two.
            return current, False
        try:
            task = create_task(
                tx_pool,
                workspace_id=workspace_id,
                repository_id=repository_id,
                github_issue_id=github_issue_id,
                github_issue_number=github_issue_number,
                source_requirements_fingerprint=requirements_fingerprint,
                status=TaskStatus.READY_TO_PLAN,
            )
        except UniqueViolation:
            # A concurrent intake committed first: the current-Task partial
            # unique index won. Classify from re-read durable state.
            winner = find_current_task_for_issue(
                tx_pool, repository_id=repository_id, github_issue_id=github_issue_id
            )
            if winner is None:  # pragma: no cover - unresolvable concurrent state
                raise ConflictError(
                    "the Task creation could not be serialized against current durable state"
                ) from None
            return winner, False
        record_workflow_event(
            tx_pool,
            workspace_id=workspace_id,
            task_id=task.id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=_OPENORC_ACTOR.actor_type,
            actor_id=_OPENORC_ACTOR.actor_id,
        )
        return task, True


def _synchronize_hierarchy_after_creation(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
    github_issue_id: int,
) -> None:
    """Run the non-gating hierarchy sync after successful creation.

    Hierarchy is presentation state. A failure here is logged selectively as
    degraded relationship projection and is deliberately swallowed: it must
    never turn a determined, committed intake into an error for the caller.
    """
    try:
        github_issue_relations.synchronize_repository_issue_hierarchy(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=github_issue_id,
        )
    except ApplicationError as error:
        logger.warning(
            "hierarchy synchronization after task intake failed (non-gating; mirror preserved): %s",
            type(error).__name__,
        )


def intake_repository_task_for_owner(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
) -> RepositoryTaskIntake:
    """Intake a Task under the authenticated Owner authorization boundary."""
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _OWNER_INTAKE_SPAN_NAME) as span:
        annotate_span(span, operation=_OWNER_INTAKE_SPAN_NAME, workspace_id=str(workspace_id))
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        return intake_repository_task(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
        )
