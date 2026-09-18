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
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

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
    "create_execution",
    "get_execution",
    "list_task_executions",
    "update_execution_status",
]

_EXECUTION_COLUMNS = (
    "id, workspace_id, task_id, producer_session_id, execution_number, status, "
    "created_at, updated_at"
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
    """
    _require_positive_int(execution_number, "execution_number")
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
            "(workspace_id, task_id, producer_session_id, execution_number, status) "
            "values (%s, %s, %s, %s, %s) "
            f"returning {_EXECUTION_COLUMNS}",
            (
                workspace_id,
                task_id,
                producer_session_id,
                execution_number,
                ExecutionStatus.QUEUED.value,
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
