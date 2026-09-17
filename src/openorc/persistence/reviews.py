"""Repositories for ReviewLoop and ReviewIteration persistence.

Explicit SQL repositories over the ``openorc`` schema for the governed
review loop and its immutable-once-finalized iterations (Phase 1, issue
#23). Rows map to transport-independent domain objects from
:mod:`openorc.domain.reviews`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Lifecycle and immutability: ``create_review_loop`` establishes a loop as
OPEN with the caller-supplied effective ``iteration_limit`` (the v1 default
constant lives at the configured setting boundary, not as a database
default). ``close_review_loop`` is the loop's only lifecycle transition —
an absorbing, conditional UPDATE from OPEN to CLOSED stamping the semantic
``closed_at``. A ReviewIteration is created unfinalized (all four result
facts NULL) and finalized exactly once by ``record_review_iteration_result``,
which applies only while the iteration is unfinalized and atomically sets
outcome, summary, findings, and ``decided_at`` in one statement. There is
no rewrite path: a finalized iteration can never change, and revisions and
results are not rewritten to manufacture a different past.

The durable backstops are database constraints: iteration numbering is
unique per ReviewLoop (``UniqueViolation``), the reviewed PlanRevision must
belong to the same Task and Workspace as the loop (``ForeignKeyViolation``),
and the settled Reviewer-result coherence (ACCEPTED clears with zero
findings; CHANGES_REQUESTED retains at least one) is CHECK-enforced.
Translating driver exceptions into typed application errors is a
service-layer concern, not a persistence one.

Every jsonb write goes through ``psycopg.types.json.Jsonb`` at this
boundary: psycopg 3 does not adapt plain containers to jsonb without an
explicit wrapper.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopDomainError,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
    canonical_review_findings,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "close_review_loop",
    "create_review_iteration",
    "create_review_loop",
    "get_review_iteration",
    "get_review_loop",
    "list_review_loop_iterations",
    "list_task_review_loops",
    "record_review_iteration_result",
]

_REVIEW_LOOP_COLUMNS = (
    "id, workspace_id, task_id, purpose, iteration_limit, status, closed_at, created_at"
)
_REVIEW_ITERATION_COLUMNS = (
    "id, workspace_id, task_id, review_loop_id, iteration_number, plan_revision_id, "
    "outcome, summary, findings, decided_at, created_at"
)


def _review_loop_from_row(row: Sequence[Any]) -> ReviewLoop:
    closed_at = row[6]
    return ReviewLoop(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        purpose=ReviewLoopPurpose(row[3]),
        iteration_limit=row[4],
        status=ReviewLoopStatus(row[5]),
        closed_at=None if closed_at is None else normalize_utc(closed_at),
        created_at=normalize_utc(row[7]),
    )


def _review_iteration_from_row(row: Sequence[Any]) -> ReviewIteration:
    decided_at = row[9]
    return ReviewIteration(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        review_loop_id=row[3],
        iteration_number=row[4],
        plan_revision_id=row[5],
        outcome=None if row[6] is None else ReviewOutcome(row[6]),
        summary=row[7],
        # findings arrives as the psycopg-decoded jsonb array (a Python
        # list) or None; the domain canonicalizer re-validates canonical
        # form on construction.
        findings=row[8],
        decided_at=None if decided_at is None else normalize_utc(decided_at),
        created_at=normalize_utc(row[10]),
    )


def _require_purpose(purpose: object) -> None:
    if not isinstance(purpose, ReviewLoopPurpose):
        raise ReviewLoopDomainError(
            "ReviewLoop.purpose must be a ReviewLoopPurpose (planning or pr_review)"
        )


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReviewLoopDomainError(f"ReviewLoop.{name} must be a positive integer")


def create_review_loop(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    purpose: ReviewLoopPurpose,
    iteration_limit: int,
) -> ReviewLoop:
    """Establish one review loop for a Task as OPEN.

    v1 purposes are exactly ``planning`` and ``pr_review``. The caller
    supplies the effective ``iteration_limit`` the loop will use — the v1
    default (``DEFAULT_REVIEW_LOOP_ITERATION_LIMIT``) and any later
    Workspace-level configurability live at the configured setting boundary,
    and the effective limit is stored on the loop and preserved for
    historical reconstruction. The loop starts OPEN with no ``closed_at``;
    closure (:func:`close_review_loop`) is the only lifecycle transition.
    """
    _require_purpose(purpose)
    _require_positive_int(iteration_limit, "iteration_limit")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.review_loops "
            "(workspace_id, task_id, purpose, iteration_limit, status) "
            "values (%s, %s, %s, %s, 'open') "
            f"returning {_REVIEW_LOOP_COLUMNS}",
            (workspace_id, task_id, purpose.value, iteration_limit),
        ).fetchone()
    assert row is not None
    return _review_loop_from_row(row)


def get_review_loop(pool: DatabasePool, *, review_loop_id: UUID) -> ReviewLoop | None:
    """Return one review loop by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_REVIEW_LOOP_COLUMNS} from openorc.review_loops where id = %s",
            (review_loop_id,),
        ).fetchone()
    return None if row is None else _review_loop_from_row(row)


def list_task_review_loops(
    pool: DatabasePool, *, task_id: UUID, status: ReviewLoopStatus | None = None
) -> list[ReviewLoop]:
    """List a Task's review loops, optionally filtered by lifecycle status.

    Serves the ``review_loops_task_status_idx`` lookup (for example, the
    open planning loop); pass ``status=None`` for the Task's full loop
    history.
    """
    predicate = "" if status is None else " and status = %s"
    params: tuple[Any, ...] = (task_id,) if status is None else (task_id, status.value)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_REVIEW_LOOP_COLUMNS} from openorc.review_loops "
            f"where task_id = %s{predicate} order by created_at, id",
            params,
        ).fetchall()
    return [_review_loop_from_row(row) for row in rows]


def close_review_loop(pool: DatabasePool, *, review_loop_id: UUID) -> ReviewLoop | None:
    """Close a review loop, stamping the semantic ``closed_at``.

    The loop's only lifecycle transition: an absorbing, conditional UPDATE
    applying only while the loop is OPEN. Closure policy (when a loop
    closes, e.g. after acceptance or after its iteration limit is reached)
    belongs to later orchestration; this is the durable transition
    primitive, and CLOSED is absorbing.

    Returns the updated loop, or ``None`` when the loop is missing or
    already closed (a rejected no-op that must not be retried blindly).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.review_loops "
            "set status = 'closed', closed_at = now() "
            "where id = %s and status = 'open' "
            f"returning {_REVIEW_LOOP_COLUMNS}",
            (review_loop_id,),
        ).fetchone()
    return None if row is None else _review_loop_from_row(row)


def create_review_iteration(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    review_loop_id: UUID,
    iteration_number: int,
    plan_revision_id: UUID,
) -> ReviewIteration:
    """Create one unfinalized review iteration bound to its exact subject.

    The iteration starts unfinalized: all four result facts (outcome,
    summary, findings, decided_at) are NULL until
    :func:`record_review_iteration_result` finalizes them atomically. The
    subject is the exact PlanRevision under review; the composite foreign
    keys durably enforce that the subject belongs to the same Task and
    Workspace as the loop (a cross-Task subject raises
    ``ForeignKeyViolation``), and the ``unique (review_loop_id,
    iteration_number)`` constraint makes a duplicate iteration number
    impossible (``UniqueViolation``).
    """
    _require_positive_int(iteration_number, "iteration_number")
    if not isinstance(plan_revision_id, UUID):
        raise ReviewLoopDomainError(
            "ReviewIteration.plan_revision_id must be a UUID (the exact subject reviewed)"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.review_iterations "
            "(workspace_id, task_id, review_loop_id, iteration_number, plan_revision_id) "
            "values (%s, %s, %s, %s, %s) "
            f"returning {_REVIEW_ITERATION_COLUMNS}",
            (workspace_id, task_id, review_loop_id, iteration_number, plan_revision_id),
        ).fetchone()
    assert row is not None
    return _review_iteration_from_row(row)


def get_review_iteration(
    pool: DatabasePool, *, review_iteration_id: UUID
) -> ReviewIteration | None:
    """Return one review iteration by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_REVIEW_ITERATION_COLUMNS} from openorc.review_iterations where id = %s",
            (review_iteration_id,),
        ).fetchone()
    return None if row is None else _review_iteration_from_row(row)


def list_review_loop_iterations(
    pool: DatabasePool, *, review_loop_id: UUID
) -> list[ReviewIteration]:
    """List a loop's iterations in numbering order.

    The full iteration history — unfinalized and finalized alike — is
    retained: finalized results are immutable historical evidence that a
    later REVIEW_RESOLUTION consumes, and numbering is unique per loop
    (the ``unique (review_loop_id, iteration_number)`` constraint doubles
    as the iteration lookup).
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_REVIEW_ITERATION_COLUMNS} from openorc.review_iterations "
            "where review_loop_id = %s order by iteration_number",
            (review_loop_id,),
        ).fetchall()
    return [_review_iteration_from_row(row) for row in rows]


def record_review_iteration_result(
    pool: DatabasePool,
    *,
    review_iteration_id: UUID,
    outcome: ReviewOutcome,
    summary: str,
    findings: Sequence[Any],
) -> ReviewIteration | None:
    """Finalize an iteration with its complete Reviewer result, atomically.

    Records the settled ``review_result`` facts — ``outcome``, the
    Reviewer's non-empty ``summary``, the protocol-settled ``findings``
    array (persisted verbatim as historical evidence; per-item protocol
    validation is Phase 2), and the semantic ``decided_at`` — in one
    statement, applying only while the iteration is unfinalized (``outcome
    IS NULL``). A finalized iteration can never change: there is no rewrite
    path, and results are not rewritten to manufacture a different past.
    The caller supplies an outcome that is a reviewer judgment;
    provider/runtime/protocol failures are not outcomes.

    The findings array is canonicalized through the domain canonicalizer
    (plain-list canonical JSON form) and written through the explicit
    ``Jsonb`` adapter. The database CHECKs backstop the settled coherence:
    ACCEPTED clears with zero findings and CHANGES_REQUESTED retains at
    least one.

    Returns the finalized iteration, or ``None`` when the iteration is
    missing or already finalized (a rejected no-op that must not be retried
    blindly).
    """
    if not isinstance(outcome, ReviewOutcome):
        raise ReviewLoopDomainError(
            "record_review_iteration_result requires a ReviewOutcome (a reviewer "
            "judgment); provider/runtime/protocol failures are not outcomes"
        )
    if not isinstance(summary, str) or not summary.strip():
        raise ReviewLoopDomainError("summary must be a non-empty string")
    canonical_findings = canonical_review_findings(findings)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.review_iterations "
            "set outcome = %s, summary = %s, findings = %s, decided_at = now() "
            "where id = %s and outcome is null "
            f"returning {_REVIEW_ITERATION_COLUMNS}",
            (outcome.value, summary, Jsonb(canonical_findings), review_iteration_id),
        ).fetchone()
    return None if row is None else _review_iteration_from_row(row)
