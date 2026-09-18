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

Iteration creation is guarded by the loop's current lifecycle,
concurrency-safely: ``create_review_iteration`` locks the referenced loop
row ``FOR UPDATE`` inside the creation transaction and inserts only while
the loop is OPEN, so an iteration can never commit after the loop has
closed (a concurrent close needs the same row lock). This is lifecycle
integrity, not ReviewLoop orchestration.

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
    "task_pull_request_id, reviewed_head_sha, outcome, summary, findings, "
    "decided_at, created_at"
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
    decided_at = row[11]
    return ReviewIteration(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        review_loop_id=row[3],
        iteration_number=row[4],
        plan_revision_id=row[5],
        task_pull_request_id=row[6],
        reviewed_head_sha=row[7],
        outcome=None if row[8] is None else ReviewOutcome(row[8]),
        summary=row[9],
        # findings arrives as the psycopg-decoded jsonb array (a Python
        # list) or None; the domain canonicalizer re-validates canonical
        # form on construction.
        findings=row[10],
        decided_at=None if decided_at is None else normalize_utc(decided_at),
        created_at=normalize_utc(row[12]),
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
    plan_revision_id: UUID | None = None,
    task_pull_request_id: UUID | None = None,
    reviewed_head_sha: str | None = None,
) -> ReviewIteration | None:
    """Create one unfinalized review iteration bound to its exact subject.

    The subject is exactly one form, matching the loop's purpose: a
    ``planning`` loop binds the exact PlanRevision under review
    (``plan_revision_id``); a ``pr_review`` loop binds the exact
    TaskPullRequest plus the exact reviewed head SHA (``task_pull_request_id``
    plus non-empty ``reviewed_head_sha``). Reviewer acceptance identity is
    the TaskPullRequest plus the exact reviewed head SHA: the exact reviewed
    head is immutable iteration history, so PR identity stays stable while
    the PR's current head moves across remediation rounds. Partial PR
    forms (PR without head SHA, or head SHA without PR) are not subjects
    and are rejected.

    The write is guarded by the loop's current lifecycle, concurrency-safely:
    the referenced loop row is locked ``FOR UPDATE`` inside the creation
    transaction and the insert applies only while the loop is OPEN. A CLOSED
    or missing loop rejects the creation with ``None`` — CLOSED accepts no
    further iterations, and a loop cannot close concurrently between the
    check and the commit because ``close_review_loop`` needs the same row
    lock (either the iteration commits while the loop is OPEN, or the close
    wins and the creation is rejected). This is lifecycle integrity, not
    ReviewLoop orchestration.

    The same lock read carries the loop's ``purpose``, and purpose/subject
    agreement is enforced here: a PR subject in a ``planning`` loop, or a
    PlanRevision subject in a ``pr_review`` loop, raises
    ``ReviewLoopDomainError`` and commits nothing (a deterministic caller
    error, like the producer-role requirement on execution creation — the
    loop row is locked, so the check cannot race). The same-Task/Workspace
    and exact-subject foreign keys are unchanged: against an OPEN,
    purpose-matching loop, an iteration whose Task/Workspace disagrees with
    the loop, or whose subject belongs to another Task, still raises
    ``ForeignKeyViolation``; a duplicate iteration number still raises
    ``UniqueViolation``.
    """
    _require_positive_int(iteration_number, "iteration_number")
    has_plan = isinstance(plan_revision_id, UUID)
    has_pr = (
        isinstance(task_pull_request_id, UUID)
        and isinstance(reviewed_head_sha, str)
        and bool(reviewed_head_sha.strip())
    )
    if has_plan and (task_pull_request_id is not None or reviewed_head_sha is not None):
        raise ReviewLoopDomainError(
            "a planning iteration binds only the exact PlanRevision subject: "
            "no TaskPullRequest and no reviewed head SHA"
        )
    if not has_plan and not has_pr:
        raise ReviewLoopDomainError(
            "a PR-review iteration binds the exact TaskPullRequest plus a "
            "non-empty reviewed_head_sha (the exact reviewed head); a planning "
            "iteration binds the exact PlanRevision"
        )
    with transaction(pool) as conn:
        # Lifecycle + purpose guard, held to commit: locking the loop row
        # FOR UPDATE makes the OPEN check, the purpose/subject agreement
        # check, and the insert atomic against a concurrent
        # close_review_loop (which needs the same row lock), so an
        # iteration can never commit after the loop has closed. A missing
        # loop is equally "no OPEN loop to accept the iteration".
        loop_row = conn.execute(
            "select status, purpose from openorc.review_loops where id = %s for update",
            (review_loop_id,),
        ).fetchone()
        if loop_row is None or loop_row[0] != "open":
            return None
        # Purpose/subject agreement (issue #25): the loop's purpose decides
        # the subject form, so a PR subject can never be inserted into a
        # planning loop and vice versa. A mismatch is a deterministic
        # caller error, not a rejected no-op: nothing was written.
        purpose = loop_row[1]
        if purpose == ReviewLoopPurpose.PLANNING.value and not has_plan:
            raise ReviewLoopDomainError(
                "a planning review loop accepts only PlanRevision subjects: "
                "a PR-review subject (TaskPullRequest plus exact reviewed head "
                "SHA) belongs to a pr_review loop"
            )
        if purpose == ReviewLoopPurpose.PR_REVIEW.value and not has_pr:
            raise ReviewLoopDomainError(
                "a pr_review loop accepts only PR-review subjects (the exact "
                "TaskPullRequest plus the exact reviewed head SHA): a "
                "PlanRevision subject belongs to a planning loop"
            )
        row = conn.execute(
            "insert into openorc.review_iterations "
            "(workspace_id, task_id, review_loop_id, iteration_number, plan_revision_id, "
            "task_pull_request_id, reviewed_head_sha) "
            "values (%s, %s, %s, %s, %s, %s, %s) "
            f"returning {_REVIEW_ITERATION_COLUMNS}",
            (
                workspace_id,
                task_id,
                review_loop_id,
                iteration_number,
                plan_revision_id,
                task_pull_request_id,
                reviewed_head_sha,
            ),
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
