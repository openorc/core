"""Repositories for Task persistence.

Explicit SQL repositories over the ``openorc`` schema for the Task aggregate
root (Phase 1, issue #21). Rows map to transport-independent domain objects
from :mod:`openorc.domain.tasks`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Identity and lifecycle: ``github_issue_id`` is the stable external identity
backing a Task; one current (non-archived) Task exists per
``(repository_id, github_issue_id)`` and the partial unique index keeps it
that way. Archival is a separate fact from the terminal outcome, so
``archive_task`` — not ``update_task_status`` — is the only terminal
transition path: it atomically sets the terminal ``status``, stamps
``archived_at``, and rotates ``state_token``.

Optimistic concurrency: every authoritative Task-state mutation is
conditional on the caller's expected ``state_token`` and replaces the token
in the same statement. A mutation returns ``None`` when the expected token no
longer matches (or the row is missing/already archived): the operation is
stale and must not be retried blindly. Translating that outcome into typed
application errors is a service-layer concern, not a persistence one.

Violated database invariants (uniqueness, foreign keys, CHECK constraints)
surface as driver exceptions (for example ``psycopg.errors.UniqueViolation``,
``ForeignKeyViolation``, and ``CheckViolation``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.tasks import Task, TaskDomainError, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "archive_task",
    "bind_canonical_branch",
    "create_task",
    "find_current_task_by_branch",
    "find_current_task_for_issue",
    "get_task",
    "list_issue_attempts",
    "list_workspace_tasks",
    "set_current_plan_revision",
    "update_task_status",
]

_TASK_COLUMNS = (
    "id, workspace_id, repository_id, github_issue_id, github_issue_number, "
    "status, archived_at, canonical_feature_branch, state_token, "
    "current_plan_revision_id, current_owner_gate_id, created_at, updated_at"
)


def _task_from_row(row: Sequence[Any]) -> Task:
    archived_at = row[6]
    return Task(
        id=row[0],
        workspace_id=row[1],
        repository_id=row[2],
        github_issue_id=row[3],
        github_issue_number=row[4],
        status=TaskStatus(row[5]),
        archived_at=None if archived_at is None else normalize_utc(archived_at),
        canonical_feature_branch=row[7],
        state_token=row[8],
        current_plan_revision_id=row[9],
        current_owner_gate_id=row[10],
        created_at=normalize_utc(row[11]),
        updated_at=normalize_utc(row[12]),
    )


def create_task(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    github_issue_number: int,
    status: TaskStatus = TaskStatus.READY_TO_PLAN,
) -> Task:
    """Insert one Task attempt for a stable GitHub issue within a Repository.

    A fresh Task starts nonterminal (default ``ready_to_plan``) with a NULL
    canonical branch: terminal attempts only arise through
    :func:`archive_task`, the canonical branch is only bound once later
    workflow/runtime logic has verified it (:func:`bind_canonical_branch`),
    and at most one non-archived Task may exist per
    ``(repository_id, github_issue_id)`` (the database partial unique index
    enforces it).
    """
    if status.is_terminal:
        raise TaskDomainError(
            "a Task cannot be created with a terminal status; use archive_task on an existing Task"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.tasks "
            "(workspace_id, repository_id, github_issue_id, github_issue_number, status) "
            "values (%s, %s, %s, %s, %s) "
            f"returning {_TASK_COLUMNS}",
            (
                workspace_id,
                repository_id,
                github_issue_id,
                github_issue_number,
                status.value,
            ),
        ).fetchone()
    assert row is not None
    return _task_from_row(row)


def get_task(pool: DatabasePool, task_id: UUID) -> Task | None:
    """Return one Task by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s", (task_id,)
        ).fetchone()
    return None if row is None else _task_from_row(row)


def list_workspace_tasks(
    pool: DatabasePool, *, workspace_id: UUID, include_archived: bool = False
) -> list[Task]:
    """List a Workspace's Tasks.

    Defaults to the current (non-archived) attempts only; pass
    ``include_archived=True`` for full history. Serves the active-Workspace
    and status query shapes behind the ``tasks_workspace_*`` indexes.
    """
    predicate = "" if include_archived else " and archived_at is null"
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            f"where workspace_id = %s{predicate} order by created_at, id",
            (workspace_id,),
        ).fetchall()
    return [_task_from_row(row) for row in rows]


def list_issue_attempts(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> list[Task]:
    """List every attempt (archived and current) for one stable issue."""
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where repository_id = %s and github_issue_id = %s "
            "order by created_at, id",
            (repository_id, github_issue_id),
        ).fetchall()
    return [_task_from_row(row) for row in rows]


def find_current_task_for_issue(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> Task | None:
    """Return the one current Task for a stable issue, or ``None``.

    Serves current-Task resolution through the partial unique index
    ``tasks_repository_issue_current_uniq``.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where repository_id = %s and github_issue_id = %s "
            "and archived_at is null",
            (repository_id, github_issue_id),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def find_current_task_by_branch(
    pool: DatabasePool, *, repository_id: UUID, canonical_feature_branch: str
) -> Task | None:
    """Return the current Task owning a canonical branch, or ``None``.

    Serves canonical-branch ownership lookup through the partial unique
    index ``tasks_repository_canonical_branch_current_uniq``.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where repository_id = %s and canonical_feature_branch = %s "
            "and archived_at is null",
            (repository_id, canonical_feature_branch),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def update_task_status(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    status: TaskStatus,
) -> Task | None:
    """Move a current Task to a nonterminal status, rotating ``state_token``.

    Nonterminal transitions only: terminal statuses (``cancelled``/
    ``completed``) raise :class:`TaskDomainError` — terminal transitions go
    exclusively through :func:`archive_task`, which atomically sets the
    terminal status, stamps ``archived_at``, and rotates the token.

    The update is conditional on ``expected_state_token`` and replaces the
    token atomically. Returns the updated Task, or ``None`` when the token
    no longer matches or the Task is missing/already archived (a stale
    operation that must not be retried blindly).
    """
    if not isinstance(status, TaskStatus) or status.is_terminal:
        raise TaskDomainError(
            "update_task_status only accepts nonterminal Task statuses; "
            "terminal transitions go exclusively through archive_task"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set status = %s, state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            f"returning {_TASK_COLUMNS}",
            (status.value, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def archive_task(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    terminal_status: TaskStatus,
) -> Task | None:
    """Archive a Task attempt with its terminal outcome, atomically.

    The only terminal transition path: sets ``terminal_status`` (must be
    ``cancelled`` or ``completed``), stamps ``archived_at``, and rotates
    ``state_token`` in one statement. Archival is independent from terminal
    outcome — the outcome stays distinguishable through ``status`` — and both
    CANCELLED and COMPLETED attempts become archived history.

    The update is conditional on ``expected_state_token``. Returns the
    updated Task, or ``None`` when the token no longer matches or the Task
    is missing/already archived (a stale operation that must not be retried
    blindly).
    """
    if not isinstance(terminal_status, TaskStatus) or not terminal_status.is_terminal:
        raise TaskDomainError(
            "archive_task requires a terminal status (cancelled or completed); "
            "nonterminal transitions use update_task_status"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set status = %s, archived_at = now(), "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            f"returning {_TASK_COLUMNS}",
            (terminal_status.value, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def bind_canonical_branch(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    canonical_feature_branch: str,
) -> Task | None:
    """Bind a Task's canonical feature branch once, rotating the token.

    Canonical branch ownership is a one-time, Task-level fact: the branch is
    created NULL and bound exactly once, after later workflow/runtime logic
    has verified the Producer-created branch. The conditional UPDATE applies
    only while ``canonical_feature_branch IS NULL``, so a rebinding attempt
    is a rejected no-op — a current Task never releases or switches its
    canonical branch. A Task's branch is exclusive within its Repository
    (enforced by the partial unique index); retries, later Executions, and
    PR remediation for the Task continue on the same branch. Archival is
    what releases branch ownership for future Tasks.

    The update is conditional on ``expected_state_token``. Returns the
    updated Task, or ``None`` when the token no longer matches, the Task is
    missing/already archived, or the branch is already bound (none of these
    outcomes may be retried blindly; stale-branch translation is a
    service-layer concern).
    """
    if not isinstance(canonical_feature_branch, str) or not canonical_feature_branch.strip():
        raise TaskDomainError("canonical_feature_branch must be a non-empty string")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set canonical_feature_branch = %s, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            "and canonical_feature_branch is null "
            f"returning {_TASK_COLUMNS}",
            (canonical_feature_branch, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def set_current_plan_revision(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    plan_revision_id: UUID,
) -> Task | None:
    """Point the Task's current-plan pointer at one exact PlanRevision.

    The current-plan pointer is a nullable Task-level fact: it identifies
    the authoritative current PlanRevision without duplicating the
    revision's content or review outcomes on the Task row (one fact, one
    home). The composite foreign key ``tasks_current_plan_revision_fk``
    durably enforces same-Task/Workspace consistency: the pointer may only
    reference a PlanRevision whose ``(id, task_id, workspace_id)`` matches
    the Task, so cross-Task or cross-Workspace corruption attempts raise
    ``ForeignKeyViolation`` (translating driver exceptions into typed
    application errors is a service-layer concern). Moving the pointer to a
    newer same-Task revision leaves every older revision intact; the
    pointer is never cleared implicitly (archival is what ends a Task's
    attempt).

    The update is conditional on ``expected_state_token`` and rotates the
    token like every other authoritative Task-state mutation. Returns the
    updated Task, or ``None`` when the token no longer matches or the Task
    is missing/already archived (a stale operation that must not be retried
    blindly).
    """
    if not isinstance(plan_revision_id, UUID):
        raise TaskDomainError("plan_revision_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set current_plan_revision_id = %s, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            f"returning {_TASK_COLUMNS}",
            (plan_revision_id, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else _task_from_row(row)
