"""Repositories for one-shot external-effect intent persistence (issue #228).

Explicit SQL repositories over the ``openorc`` schema for the two narrow
one-shot intent records the Owner Abort and unified Task Cancel commands
register:

- ``openorc.task_interrupt_intents`` — the exact-session ClineBox activity
  Interrupt intent of one Owner Abort, created unsettled and advanced
  exactly once to a terminal outcome record (or annotated in place while
  still unsettled with a safe dispatch fact).
- ``openorc.task_pull_request_close_intents`` — the Task cancellation's
  one-shot PR-close obligation, created pending and advanced exactly once
  to a terminal outcome record.

Rows map to transport-independent domain objects from
:mod:`openorc.domain.intents`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary. Outcomes are canonical
JSON objects through the domain canonicalizer and an explicit ``Jsonb``
adapter.

The forward-only discipline is enforced by conditional UPDATEs: an outcome
write applies only while the row is still nonterminal (``unsettled`` /
``pending``) and only to a terminal target status, so a raced duplicate
recording is a rejected no-op (``None``) that must not be retried blindly —
translating that into typed application errors is a service-layer concern.
The database partial unique index (one unsettled interrupt intent per
session binding) and ``unique (task_id)`` (one close intent per Task) are
the durable one-shot backstops.

Violated database invariants surface as driver exceptions; translating
driver exceptions into typed application errors is a service-layer concern.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.connections import WorkflowRole
from openorc.domain.intents import (
    TERMINAL_TASK_INTERRUPT_INTENT_STATUSES,
    TERMINAL_TASK_PULL_REQUEST_CLOSE_INTENT_STATUSES,
    TaskInterruptIntent,
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntent,
    TaskPullRequestCloseIntentStatus,
    canonical_intent_outcome,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "annotate_unsettled_task_interrupt_intent",
    "create_task_interrupt_intent",
    "create_task_pull_request_close_intent",
    "get_task_interrupt_intent",
    "get_task_pull_request_close_intent_for_task",
    "list_task_interrupt_intents",
    "list_unsettled_task_interrupt_intents",
    "record_task_interrupt_intent_outcome",
    "record_task_pull_request_close_intent_outcome",
]

_TASK_INTERRUPT_INTENT_COLUMNS = (
    "id, workspace_id, task_id, task_agent_session_id, workflow_role, "
    "external_session_id, requested_by_profile_id, status, outcome, "
    "created_at, updated_at"
)
_TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS = (
    "id, workspace_id, task_id, task_pull_request_id, github_pr_id, "
    "github_pr_number, requested_by_profile_id, status, outcome, "
    "created_at, updated_at"
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"intent {name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"intent {name} must be a non-empty string")


def _interrupt_intent_from_row(row: Sequence[Any]) -> TaskInterruptIntent:
    return TaskInterruptIntent(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        task_agent_session_id=row[3],
        workflow_role=WorkflowRole(row[4]),
        external_session_id=row[5],
        requested_by_profile_id=row[6],
        status=TaskInterruptIntentStatus(row[7]),
        outcome=row[8],
        created_at=normalize_utc(row[9]),
        updated_at=normalize_utc(row[10]),
    )


def _close_intent_from_row(row: Sequence[Any]) -> TaskPullRequestCloseIntent:
    return TaskPullRequestCloseIntent(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        task_pull_request_id=row[3],
        github_pr_id=row[4],
        github_pr_number=row[5],
        requested_by_profile_id=row[6],
        status=TaskPullRequestCloseIntentStatus(row[7]),
        outcome=row[8],
        created_at=normalize_utc(row[9]),
        updated_at=normalize_utc(row[10]),
    )


def create_task_interrupt_intent(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    workflow_role: WorkflowRole,
    external_session_id: str,
    requested_by_profile_id: UUID,
) -> TaskInterruptIntent:
    """Insert one unsettled exact-session Interrupt intent.

    Created inside the caller's transaction (the Owner Abort's blocking
    composition). The one-shot unsettled-per-session discipline is the
    database partial unique index: a second unsettled intent for the same
    session binding raises ``UniqueViolation`` for service classification —
    a duplicate dispatch can never replace a pending uncertainty.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    if not isinstance(workflow_role, WorkflowRole):
        raise ValueError("create_task_interrupt_intent requires a WorkflowRole")
    _require_nonblank_str(external_session_id, "external_session_id")
    _require_uuid(requested_by_profile_id, "requested_by_profile_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_interrupt_intents "
            "(workspace_id, task_id, task_agent_session_id, workflow_role, "
            "external_session_id, requested_by_profile_id, status) "
            "values (%s, %s, %s, %s, %s, %s, 'unsettled') "
            f"returning {_TASK_INTERRUPT_INTENT_COLUMNS}",
            (
                workspace_id,
                task_id,
                task_agent_session_id,
                workflow_role.value,
                external_session_id,
                requested_by_profile_id,
            ),
        ).fetchone()
    assert row is not None
    return _interrupt_intent_from_row(row)


def get_task_interrupt_intent(
    pool: DatabasePool, *, task_interrupt_intent_id: UUID
) -> TaskInterruptIntent | None:
    """Return one Interrupt intent by id, or ``None`` when it does not exist."""
    _require_uuid(task_interrupt_intent_id, "task_interrupt_intent_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "select "
            + _TASK_INTERRUPT_INTENT_COLUMNS
            + " from openorc.task_interrupt_intents where id = %s",
            (task_interrupt_intent_id,),
        ).fetchone()
    return None if row is None else _interrupt_intent_from_row(row)


def list_task_interrupt_intents(pool: DatabasePool, *, task_id: UUID) -> list[TaskInterruptIntent]:
    """List a Task's complete Interrupt-intent history, oldest first.

    Every intent of the Task — unsettled and terminal alike — is retained:
    terminal intents are the durable outcome records the workflow's safety
    decisions (for example the Cancel eligibility veto) read.
    """
    _require_uuid(task_id, "task_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            "select "
            + _TASK_INTERRUPT_INTENT_COLUMNS
            + " from openorc.task_interrupt_intents where task_id = %s order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_interrupt_intent_from_row(row) for row in rows]


def list_unsettled_task_interrupt_intents(
    pool: DatabasePool, *, task_id: UUID
) -> list[TaskInterruptIntent]:
    """List a Task's unsettled Interrupt intents, oldest first.

    The authoritative outstanding-interruption read: an unsettled intent
    means the admitted interruption's outcome is not durably accounted for,
    so neither Cancel may apply over it nor may a duplicate Abort re-issue
    the Interrupt.
    """
    _require_uuid(task_id, "task_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            "select " + _TASK_INTERRUPT_INTENT_COLUMNS + " from openorc.task_interrupt_intents "
            "where task_id = %s and status = 'unsettled' order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_interrupt_intent_from_row(row) for row in rows]


def annotate_unsettled_task_interrupt_intent(
    pool: DatabasePool,
    *,
    task_interrupt_intent_id: UUID,
    outcome: Any,
) -> TaskInterruptIntent | None:
    """Record one safe dispatch fact on an unsettled Interrupt intent.

    The acknowledged-but-still-unsettled case: the intent's status stays
    ``unsettled`` while its ``outcome`` gains the safe machine fact. The
    write applies only while the intent is unsettled; a raced terminal
    outcome is a rejected no-op (``None``).
    """
    _require_uuid(task_interrupt_intent_id, "task_interrupt_intent_id")
    canonical_outcome = canonical_intent_outcome(outcome)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_interrupt_intents "
            "set outcome = %s, updated_at = now() "
            "where id = %s and status = 'unsettled' "
            f"returning {_TASK_INTERRUPT_INTENT_COLUMNS}",
            (Jsonb(dict(canonical_outcome)), task_interrupt_intent_id),
        ).fetchone()
    return None if row is None else _interrupt_intent_from_row(row)


def record_task_interrupt_intent_outcome(
    pool: DatabasePool,
    *,
    task_interrupt_intent_id: UUID,
    status: TaskInterruptIntentStatus,
    outcome: Any,
) -> TaskInterruptIntent | None:
    """Advance one unsettled Interrupt intent to its terminal outcome, once.

    The one-shot forward-only transition: applies only while the intent is
    unsettled and the target status is terminal. ``None`` means the intent
    is missing or already terminal (a rejected no-op that must not be
    retried blindly).
    """
    _require_uuid(task_interrupt_intent_id, "task_interrupt_intent_id")
    if status not in TERMINAL_TASK_INTERRUPT_INTENT_STATUSES:
        raise ValueError(
            "record_task_interrupt_intent_outcome requires a terminal "
            "TaskInterruptIntentStatus; unsettled intents are annotated in place"
        )
    canonical_outcome = canonical_intent_outcome(outcome)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_interrupt_intents "
            "set status = %s, outcome = %s, updated_at = now() "
            "where id = %s and status = 'unsettled' "
            f"returning {_TASK_INTERRUPT_INTENT_COLUMNS}",
            (status.value, Jsonb(dict(canonical_outcome)), task_interrupt_intent_id),
        ).fetchone()
    return None if row is None else _interrupt_intent_from_row(row)


def create_task_pull_request_close_intent(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_pull_request_id: UUID,
    github_pr_id: int,
    github_pr_number: int,
    requested_by_profile_id: UUID,
) -> TaskPullRequestCloseIntent:
    """Insert one pending PR-close obligation.

    Created inside the caller's transaction (the cancellation's terminal
    composition), atomically with the archival it serves. ``unique
    (task_id)`` is the durable one-shot backstop: a Task cancels at most
    once and its canonical ``TaskPullRequest`` is ``unique (task_id)``, so
    a duplicate obligation raises ``UniqueViolation`` for service
    classification.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(task_pull_request_id, "task_pull_request_id")
    if isinstance(github_pr_id, bool) or not isinstance(github_pr_id, int) or github_pr_id <= 0:
        raise ValueError("github_pr_id must be a positive integer")
    if (
        isinstance(github_pr_number, bool)
        or not isinstance(github_pr_number, int)
        or github_pr_number <= 0
    ):
        raise ValueError("github_pr_number must be a positive integer")
    _require_uuid(requested_by_profile_id, "requested_by_profile_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_pull_request_close_intents "
            "(workspace_id, task_id, task_pull_request_id, github_pr_id, "
            "github_pr_number, requested_by_profile_id, status) "
            "values (%s, %s, %s, %s, %s, %s, 'pending') "
            f"returning {_TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS}",
            (
                workspace_id,
                task_id,
                task_pull_request_id,
                github_pr_id,
                github_pr_number,
                requested_by_profile_id,
            ),
        ).fetchone()
    assert row is not None
    return _close_intent_from_row(row)


def get_task_pull_request_close_intent_for_task(
    pool: DatabasePool, *, task_id: UUID
) -> TaskPullRequestCloseIntent | None:
    """Return a Task's close obligation, or ``None`` when none exists.

    The obligation is observable across crashes between the terminal
    archival and the external closure attempt: the durable pending row is
    the fact, never an audit event.
    """
    _require_uuid(task_id, "task_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "select "
            + _TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS
            + " from openorc.task_pull_request_close_intents where task_id = %s",
            (task_id,),
        ).fetchone()
    return None if row is None else _close_intent_from_row(row)


def record_task_pull_request_close_intent_outcome(
    pool: DatabasePool,
    *,
    task_pull_request_close_intent_id: UUID,
    status: TaskPullRequestCloseIntentStatus,
    outcome: Any,
) -> TaskPullRequestCloseIntent | None:
    """Advance one pending PR-close obligation to its terminal outcome, once.

    The one-shot forward-only transition: applies only while the intent is
    pending and the target status is terminal. ``None`` means the intent is
    missing or already terminal (a rejected no-op that must not be retried
    blindly); the caller records the observed outcome it actually holds and
    never manufactures a substitute.
    """
    _require_uuid(task_pull_request_close_intent_id, "task_pull_request_close_intent_id")
    if status not in TERMINAL_TASK_PULL_REQUEST_CLOSE_INTENT_STATUSES:
        raise ValueError(
            "record_task_pull_request_close_intent_outcome requires a terminal "
            "TaskPullRequestCloseIntentStatus; pending obligations stay pending"
        )
    canonical_outcome = canonical_intent_outcome(outcome)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_pull_request_close_intents "
            "set status = %s, outcome = %s, updated_at = now() "
            "where id = %s and status = 'pending' "
            f"returning {_TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS}",
            (status.value, Jsonb(dict(canonical_outcome)), task_pull_request_close_intent_id),
        ).fetchone()
    return None if row is None else _close_intent_from_row(row)
