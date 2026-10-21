"""Repositories for Execution persistence.

Explicit SQL repositories over the ``openorc`` schema for Execution, the
historical attempt record inside a Task's Producer session (Phase 1, issue
#24). Rows map to transport-independent domain objects from
:mod:`openorc.domain.executions`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Executions are attempt/history records, not generic CRUD rows:

- Creation anchors the attempt to the Task's Producer session. The
  composite foreign key keeps the session's Task/Workspace scope in
  agreement (``ForeignKeyViolation`` otherwise), and the creation
  transaction requires the referenced binding's role to be exactly
  ``producer`` — a Reviewer session can never anchor an Execution.
- A retry, recovery, or continuation is a fresh row with the next
  caller-supplied per-Task ``execution_number`` (the
  ``unique (task_id, execution_number)`` constraint is the durable backstop
  and doubles as the attempt-history lookup). The identity constraint
  never enforces single-active-execution exclusivity.
- The E9 remediation claim (issue #84): creation optionally binds the
  attempt to one finalized CHANGES_REQUESTED ReviewIteration
  (``review_iteration_id``). The composite foreign key keeps the claimed
  iteration in the same Task/Workspace scope, and the partial unique index
  on the column makes a second claim for the same iteration a
  ``UniqueViolation`` — the durable one-shot remediation invariant,
  independent of any Task state token.
- Status transitions are guarded conditional updates: the update applies
  only while the row's current status is the caller's ``expected_status``,
  returning ``None`` otherwise (never retried blindly; typed-error
  translation is a service-layer concern). The four final statuses
  (succeeded, failed_transient, failed_final, cancelled) are absorbing: a
  finalized Execution is immutable history, and its status is never
  rewritten to manufacture a different past. Which transitions are legal
  between nonfinal statuses, and when retries happen, belong to later
  orchestration — persistence encodes no transition graph.
- There is no canonical-branch ownership here and no Git SHA field of any
  kind: the canonical feature branch remains a Task-level exclusive
  ownership fact, and committed repository truth is GitHub's, reconciled
  later through GitHub.
- The E6 additive operations (issue #81) are focused one-shot primitives on
  the same guarded-conditional discipline: the dispatch claim moves a
  queued attempt to running and initializes the qualifying-activity marker
  in the same statement; the completion persists the validated canonical
  implementation_result payload and stamps succeeded in one statement; the
  activity marker advances only while the row is running. None of them
  ever rewrites a finalized row.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.executions import (
    FINAL_EXECUTION_STATUSES,
    Execution,
    ExecutionDomainError,
    ExecutionStatus,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "claim_execution_running",
    "complete_execution_with_result",
    "create_execution",
    "get_execution",
    "list_task_executions",
    "record_execution_activity",
    "update_execution_status",
]

_EXECUTION_COLUMNS = (
    "id, workspace_id, task_id, producer_session_id, execution_number, status, "
    "created_at, updated_at, implementation_result, last_qualifying_activity_at, "
    "review_iteration_id"
)


def _execution_from_row(row: Sequence[Any]) -> Execution:
    return Execution(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        producer_session_id=row[3],
        execution_number=row[4],
        status=ExecutionStatus(row[5]),
        created_at=normalize_utc(row[6]),
        updated_at=normalize_utc(row[7]),
        implementation_result=row[8],
        last_qualifying_activity_at=None if row[9] is None else normalize_utc(row[9]),
        review_iteration_id=row[10],
    )


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ExecutionDomainError(f"Execution.{name} must be a positive integer")


def create_execution(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    producer_session_id: UUID,
    execution_number: int,
    review_iteration_id: UUID | None = None,
) -> Execution:
    """Insert one attempt record as a ``queued`` Execution for a Task.

    The attempt lives inside the Task's Producer session: the creation
    transaction reads the referenced TaskAgentSession and requires its role
    to be exactly ``producer`` — a Reviewer session is rejected with
    ``ExecutionDomainError`` (scope agreement is durably enforced by the
    composite foreign key). The caller supplies the next per-Task
    ``execution_number``; a duplicate number raises ``UniqueViolation`` as
    the durable backstop. Attempts are history: a retry, recovery, or
    continuation is another row, never a rewrite of a finalized attempt.

    ``review_iteration_id`` (issue #84) optionally binds the attempt as the
    one-shot remediation claim of one finalized CHANGES_REQUESTED
    ReviewIteration: the composite foreign key keeps the claimed iteration
    in the same Task/Workspace scope, and the partial unique index on the
    column means a second claim for the same iteration raises
    ``UniqueViolation`` — completed, known-failed, and uncertain prior
    claims alike. The default ``None`` leaves ordinary (non-remediation)
    attempts unclaimed, exactly as before.
    """
    _require_positive_int(execution_number, "execution_number")
    if review_iteration_id is not None and not isinstance(review_iteration_id, UUID):
        raise ExecutionDomainError("create_execution requires a UUID review_iteration_id (or None)")
    with transaction(pool) as conn:
        role_row = conn.execute(
            "select role from openorc.task_agent_sessions where id = %s",
            (producer_session_id,),
        ).fetchone()
        if role_row is None or role_row[0] != "producer":
            raise ExecutionDomainError(
                "create_execution requires the referenced TaskAgentSession to "
                "be the Task's producer session; Reviewer sessions never "
                "anchor an Execution"
            )
        row = conn.execute(
            "insert into openorc.executions "
            "(workspace_id, task_id, producer_session_id, execution_number, status, "
            "review_iteration_id) "
            "values (%s, %s, %s, %s, %s, %s) "
            f"returning {_EXECUTION_COLUMNS}",
            (
                workspace_id,
                task_id,
                producer_session_id,
                execution_number,
                ExecutionStatus.QUEUED.value,
                review_iteration_id,
            ),
        ).fetchone()
    assert row is not None
    return _execution_from_row(row)


def get_execution(pool: DatabasePool, *, execution_id: UUID) -> Execution | None:
    """Return one Execution by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_EXECUTION_COLUMNS} from openorc.executions where id = %s",
            (execution_id,),
        ).fetchone()
    return None if row is None else _execution_from_row(row)


def list_task_executions(pool: DatabasePool, *, task_id: UUID) -> list[Execution]:
    """List a Task's complete attempt history, in attempt order.

    Every attempt of the Task — active and finalized alike — is retained:
    finalized attempts are immutable history, and the ordering is identity,
    never exclusivity.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_EXECUTION_COLUMNS} from openorc.executions "
            "where task_id = %s order by execution_number",
            (task_id,),
        ).fetchall()
    return [_execution_from_row(row) for row in rows]


def update_execution_status(
    pool: DatabasePool,
    *,
    execution_id: UUID,
    expected_status: ExecutionStatus,
    next_status: ExecutionStatus,
) -> Execution | None:
    """Apply one guarded status transition to an unfinalized Execution.

    The update applies only while the row's current status is
    ``expected_status`` and stamps ``updated_at`` atomically with it.
    ``None`` means the Execution is missing or no longer carries the
    expected status — a rejected no-op that must not be retried blindly. A
    finalized Execution is absorbing history: a transition out of any of
    the four final statuses raises ``ExecutionDomainError`` before the
    statement runs, so finalized history is never rewritten.
    """
    if not isinstance(expected_status, ExecutionStatus) or not isinstance(
        next_status, ExecutionStatus
    ):
        raise ExecutionDomainError(
            "update_execution_status requires ExecutionStatus values for the "
            "expected and next statuses"
        )
    if expected_status is next_status:
        raise ExecutionDomainError("update_execution_status requires a different next_status")
    if expected_status in FINAL_EXECUTION_STATUSES:
        raise ExecutionDomainError(
            f"a finalized Execution ({expected_status.value}) is absorbing "
            "history; its status is never rewritten"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.executions "
            "set status = %s, updated_at = now() "
            "where id = %s and status = %s "
            f"returning {_EXECUTION_COLUMNS}",
            (next_status.value, execution_id, expected_status.value),
        ).fetchone()
    return None if row is None else _execution_from_row(row)


def claim_execution_running(pool: DatabasePool, *, execution_id: UUID) -> Execution | None:
    """Apply the E6 dispatch claim to a queued attempt (issue #81).

    One conditional statement moves the attempt from ``queued`` to
    ``running`` and initializes ``last_qualifying_activity_at`` with the
    database dispatch instant in the same write, so the F5 activity marker
    is born atomically with the claim that authorizes activity writes.
    ``None`` means the attempt is missing or no longer queued — a rejected
    no-op that must never be retried blindly.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.executions "
            "set status = 'running', last_qualifying_activity_at = now(), updated_at = now() "
            "where id = %s and status = 'queued' "
            f"returning {_EXECUTION_COLUMNS}",
            (execution_id,),
        ).fetchone()
    return None if row is None else _execution_from_row(row)


def complete_execution_with_result(
    pool: DatabasePool,
    *,
    execution_id: UUID,
    implementation_result: Mapping[str, object],
) -> Execution | None:
    """One-shot E6 finalize: persist the validated result and stamp success.

    A single conditional statement attaches the validated canonical
    ``implementation_result`` payload and moves the attempt from ``running``
    to ``succeeded``, so the result and the terminal status can never
    commit apart. ``None`` means the attempt is missing or no longer
    running — duplicate or stale finalization is a rejected no-op, and a
    finalized Execution is absorbing history that is never rewritten.
    """
    if not isinstance(implementation_result, Mapping):
        raise ExecutionDomainError(
            "complete_execution_with_result requires a canonical JSON-object "
            "implementation_result payload"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.executions "
            "set status = 'succeeded', implementation_result = %s, updated_at = now() "
            "where id = %s and status = 'running' "
            f"returning {_EXECUTION_COLUMNS}",
            (Jsonb(dict(implementation_result)), execution_id),
        ).fetchone()
    return None if row is None else _execution_from_row(row)


def record_execution_activity(pool: DatabasePool, *, execution_id: UUID) -> Execution | None:
    """Advance the F5 qualifying-activity marker with database time.

    The marker moves monotonically (database ``now()``) and only while the
    attempt is still ``running``: a finalized Execution absorbs history and
    late callbacks are dropped as a rejected no-op. No activity content is
    persisted — the caller decides qualification; this statement only
    stamps the instant.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.executions "
            "set last_qualifying_activity_at = now(), updated_at = now() "
            "where id = %s and status = 'running' "
            f"returning {_EXECUTION_COLUMNS}",
            (execution_id,),
        ).fetchone()
    return None if row is None else _execution_from_row(row)
