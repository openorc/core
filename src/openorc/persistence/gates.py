"""Repositories for OwnerGate persistence.

Explicit SQL repositories over the ``openorc`` schema for OwnerGate, the
durable human-authority decision record (Phase 1, issue #24). Rows map to
transport-independent domain objects from :mod:`openorc.domain.gates`;
instants returned from Postgres are normalized to timezone-aware UTC at
this boundary.

Resolution is an authoritative Task transition, not a gate-only update:
:func:`resolve_owner_gate` succeeds only for the gate that is still the
Task's current gate (``tasks.current_owner_gate_id``) with the caller's
expected Task ``state_token``, and applies one atomic effect — resolve the
gate one-shot, clear the pointer, rotate the token, advance the Task
timestamp. A gate that is current but token-stale, or pending but no
longer current (superseded or never installed), is a stale operation:
nothing is applied to either the gate or the Task, and the gate remains
pending. A gate that is already resolved (or missing) is a one-shot no-op.
The outcome contract (:class:`OwnerGateResolution`) makes stale and no-op
explicitly distinguishable; translating them into application behavior is
a service-layer concern, and neither may be retried blindly.

Normal lifecycle: one pending gate is installed as current (through
:func:`openorc.persistence.tasks.set_current_owner_gate`, which refuses to
replace an existing current gate), resolved while current, and only then
may another pending gate be installed. Resolved gates are immutable
historical records; a pending gate that loses currency stays pending as
history and is never rewritten into an outcome. Currency can also be lost
without any terminal stamp: :func:`supersede_current_owner_gate` (issue
#219) clears the current pointer and returns the Task to planning while
the addressed pending gate stays pending as history — the
Reviewer-triggered continuation of an accepted-plan implementation gate.

Subject construction is validated here to mirror the domain/DB coherence
(a transaction cannot commit a subject shape the domain rejects): each
gate type binds exactly its subject form — the exact review-cleared
PlanRevision for implementation authorization, the exact head SHA (with
no PR binding) for the pre-PR authorization gate, the exact
TaskPullRequest plus the exact head SHA for merge decision, exactly one
subject for REVIEW_RESOLUTION (planning PlanRevision, or PR subject as
TaskPullRequest plus exact head SHA), and — for the three exceptional
types (issue #258: block_resolution, github_issue_change,
github_state_mismatch) — exactly the unresolved TaskBlock, with every
other subject form NULL and ordinary types never carrying a TaskBlock
subject.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from openorc.domain.gates import (
    OwnerGate,
    OwnerGateDomainError,
    OwnerGateStatus,
    OwnerGateType,
)
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import _TASK_COLUMNS, _task_from_row
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "OwnerGateResolution",
    "OwnerGateResolutionOutcome",
    "OwnerGateSupersession",
    "OwnerGateSupersessionDestination",
    "OwnerGateSupersessionOutcome",
    "create_owner_gate",
    "get_owner_gate",
    "list_task_owner_gates",
    "resolve_owner_gate",
    "supersede_current_owner_gate",
]

_OWNER_GATE_COLUMNS = (
    "id, workspace_id, task_id, gate_type, status, plan_revision_id, "
    "subject_head_sha, task_pull_request_id, task_block_id, decided_at, created_at"
)


class OwnerGateResolutionOutcome(StrEnum):
    """The explicit outcome of one :func:`resolve_owner_gate` attempt.

    ``RESOLVED`` applied the authoritative Task transition; ``STALE``
    applied nothing (stale token, or the gate is no longer the Task's
    current gate); and ``NO_OP`` applied nothing because the gate was
    already resolved or missing.
    """

    RESOLVED = "resolved"
    STALE = "stale"
    NO_OP = "no_op"


@dataclass(frozen=True, slots=True)
class OwnerGateResolution:
    """The outcome of one resolve attempt.

    ``gate`` is the gate row as seen by the attempt (the resolved gate for
    ``RESOLVED``, the still-pending gate for ``STALE``, the
    already-resolved gate for ``NO_OP``, or ``None`` when the gate is
    missing). ``task`` is the post-mutation Task row when the pointer was
    cleared and the token rotated, and ``None`` when nothing was applied.
    """

    outcome: OwnerGateResolutionOutcome
    gate: OwnerGate | None
    task: Task | None


def _owner_gate_from_row(row: Sequence[Any]) -> OwnerGate:
    decided_at = row[9]
    return OwnerGate(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        gate_type=OwnerGateType(row[3]),
        status=OwnerGateStatus(row[4]),
        plan_revision_id=row[5],
        subject_head_sha=row[6],
        task_pull_request_id=row[7],
        task_block_id=row[8],
        decided_at=None if decided_at is None else normalize_utc(decided_at),
        created_at=normalize_utc(row[10]),
    )


def _require_subject(
    gate_type: OwnerGateType,
    plan_revision_id: UUID | None,
    subject_head_sha: str | None,
    task_pull_request_id: UUID | None,
    task_block_id: UUID | None,
) -> None:
    """Mirror the per-type exact-subject coherence at this boundary.

    ``pr_authorization`` is the pre-PR exact-head gate (no PR binding:
    it happens before the canonical PR exists); ``merge_decision`` and the
    PR form of ``review_resolution`` bind the canonical TaskPullRequest
    plus the exact head SHA. The three exceptional types (issue #258) bind
    exactly one unresolved TaskBlock as their only subject; the ordinary
    types never carry a TaskBlock subject.
    """
    if gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        if (
            not isinstance(plan_revision_id, UUID)
            or subject_head_sha is not None
            or task_pull_request_id is not None
            or task_block_id is not None
        ):
            raise OwnerGateDomainError(
                "an implementation_authorization gate requires the exact "
                "review-cleared PlanRevision subject and no head SHA"
            )
    elif gate_type is OwnerGateType.PR_AUTHORIZATION:
        if (
            plan_revision_id is not None
            or not isinstance(subject_head_sha, str)
            or not subject_head_sha.strip()
            or task_pull_request_id is not None
            or task_block_id is not None
        ):
            raise OwnerGateDomainError(
                "a pr_authorization gate requires the exact head-SHA subject "
                "with no PlanRevision and no PR binding (it happens before the "
                "canonical PR exists)"
            )
    elif gate_type is OwnerGateType.MERGE_DECISION:
        if (
            plan_revision_id is not None
            or not isinstance(subject_head_sha, str)
            or not subject_head_sha.strip()
            or not isinstance(task_pull_request_id, UUID)
            or task_block_id is not None
        ):
            raise OwnerGateDomainError(
                "a merge_decision gate requires the exact TaskPullRequest plus "
                "the exact head SHA and no PlanRevision"
            )
    elif gate_type is OwnerGateType.REVIEW_RESOLUTION:  # exactly one subject form.
        # Nullable subject ids are validated independently of subject-form
        # selection: a wrong-type non-NULL id is a caller error before any
        # SQL runs and can never masquerade as an absent subject that
        # silently selects the other subject form.
        if plan_revision_id is not None and not isinstance(plan_revision_id, UUID):
            raise OwnerGateDomainError(
                "create_owner_gate requires a UUID plan_revision_id (or None)"
            )
        if task_pull_request_id is not None and not isinstance(task_pull_request_id, UUID):
            raise OwnerGateDomainError(
                "create_owner_gate requires a UUID task_pull_request_id (or None)"
            )
        if task_block_id is not None:
            raise OwnerGateDomainError("a review_resolution gate never binds a TaskBlock subject")
        if plan_revision_id is not None:
            # Planning-exhaustion form: the PlanRevision only.
            if subject_head_sha is not None or task_pull_request_id is not None:
                raise OwnerGateDomainError(
                    "a review_resolution gate on the planning subject binds "
                    "exactly the exhausted PlanRevision: no head SHA and no "
                    "PR binding"
                )
        else:
            # PR-review-exhaustion form: the TaskPullRequest plus the exact
            # head SHA — never a bare head SHA. Presence is ``is not None``
            # (the type was validated above).
            if (
                task_pull_request_id is None
                or not isinstance(subject_head_sha, str)
                or not subject_head_sha.strip()
            ):
                raise OwnerGateDomainError(
                    "a review_resolution gate on the PR-review subject binds "
                    "exactly the TaskPullRequest plus the exact head SHA"
                )
    else:  # The three exceptional types: the unresolved TaskBlock only.
        if not isinstance(task_block_id, UUID):
            raise OwnerGateDomainError(
                f"an {gate_type.value} gate requires the exact unresolved "
                "TaskBlock subject (task_block_id)"
            )
        if (
            plan_revision_id is not None
            or subject_head_sha is not None
            or task_pull_request_id is not None
        ):
            raise OwnerGateDomainError(
                f"an {gate_type.value} gate binds exactly the unresolved "
                "TaskBlock subject: no PlanRevision, head SHA, or PR binding"
            )


def create_owner_gate(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    gate_type: OwnerGateType,
    plan_revision_id: UUID | None = None,
    subject_head_sha: str | None = None,
    task_pull_request_id: UUID | None = None,
    task_block_id: UUID | None = None,
) -> OwnerGate:
    """Insert one pending OwnerGate decision record for a Task.

    The gate is created ``pending`` (inserted explicitly — the migration
    declares no lifecycle default) with no closed stamp; the authoritative
    resolution is a separate, later operation. Same-type
    re-decisions are new rows, never rewrites of resolved gates: resolved
    gates are immutable history and are never recycled. The per-type
    exact-subject coherence is validated here (mirroring the domain and the
    database CHECK) so a transaction cannot commit a subject shape the
    domain rejects; the plan-revision, TaskPullRequest, and (issue #258)
    TaskBlock subjects must additionally belong to the same Task and
    Workspace (``ForeignKeyViolation`` is the durable backstop). The
    exceptional gate types bind exactly one TaskBlock subject — the
    composed installer verifies the block is unresolved, since the foreign
    key establishes identity and scope only. Installation as the Task's
    current gate is a separate authoritative Task mutation
    (:func:`openorc.persistence.tasks.set_current_owner_gate`).
    """
    if not isinstance(gate_type, OwnerGateType):
        raise OwnerGateDomainError(
            "create_owner_gate requires an OwnerGateType "
            "(implementation_authorization, pr_authorization, merge_decision, "
            "review_resolution, block_resolution, github_issue_change, or "
            "github_state_mismatch)"
        )
    _require_subject(
        gate_type, plan_revision_id, subject_head_sha, task_pull_request_id, task_block_id
    )
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.owner_gates "
            "(workspace_id, task_id, gate_type, status, plan_revision_id, "
            "subject_head_sha, task_pull_request_id, task_block_id) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_OWNER_GATE_COLUMNS}",
            (
                workspace_id,
                task_id,
                gate_type.value,
                OwnerGateStatus.PENDING.value,
                plan_revision_id,
                subject_head_sha,
                task_pull_request_id,
                task_block_id,
            ),
        ).fetchone()
    assert row is not None
    return _owner_gate_from_row(row)


def get_owner_gate(pool: DatabasePool, *, owner_gate_id: UUID) -> OwnerGate | None:
    """Return one OwnerGate by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates where id = %s",
            (owner_gate_id,),
        ).fetchone()
    return None if row is None else _owner_gate_from_row(row)


def list_task_owner_gates(pool: DatabasePool, *, task_id: UUID) -> list[OwnerGate]:
    """List a Task's complete gate history, in deterministic (created_at, id) order.

    Every gate of the Task — pending and resolved alike — is retained
    history: the Task's current-gate pointer identifies the gate it is
    waiting on without deleting or rewriting older gates, and a pending
    gate that lost currency remains pending history.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates "
            "where task_id = %s order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_owner_gate_from_row(row) for row in rows]


def resolve_owner_gate(
    pool: DatabasePool,
    *,
    owner_gate_id: UUID,
    outcome: OwnerGateStatus,
    expected_task_state_token: UUID,
) -> OwnerGateResolution:
    """Resolve the Task's current gate as one authoritative Task transition.

    Success requires the gate to be pending, to still be the Task's current
    gate, and the caller's ``expected_task_state_token`` to still match a
    non-archived Task. Under that condition one atomic effect applies: the
    gate resolves one-shot to ``outcome`` with its ``decided_at`` stamp, the
    Task's ``current_owner_gate_id`` clears, and the Task's ``state_token``
    rotates with its ``updated_at`` advancing. The Task row is locked
    ``FOR UPDATE`` first (every gate write happens under that lock), and
    both updates carry their own conditional guards as durable backstops.

    ``outcome`` is any terminal OwnerGateStatus: the Owner-driven
    APPROVED/REJECTED/CANCELLED resolutions and the Reviewer-driven
    REVIEW_RESOLUTION settlement (``outcome=SETTLED``, issue #211). The
    database CHECK restricts ``settled`` to review_resolution gates; the
    calling service owns the exact-subject proof that only a settlement
    seam applies it.

    A gate that is current but token-stale, or pending but no longer the
    Task's current gate (superseded or never installed), is a stale
    operation: nothing is applied to either the gate or the Task, the gate
    remains pending, and ``STALE`` is returned for service-layer recovery
    translation. A gate that is already resolved or missing is a one-shot
    ``NO_OP``. Neither outcome may be retried blindly; a pending gate is
    never rewritten into an outcome after losing currency.

    Normal lifecycle: install one pending gate as current, resolve it while
    current, then install another — cancellation is itself a resolution
    (``outcome=CANCELLED``), never a pointer replacement.
    """
    if not isinstance(outcome, OwnerGateStatus) or outcome is OwnerGateStatus.PENDING:
        raise OwnerGateDomainError(
            "resolve_owner_gate requires a terminal OwnerGateStatus outcome "
            "(approved, rejected, cancelled, or settled)"
        )
    if not isinstance(expected_task_state_token, UUID):
        raise OwnerGateDomainError("expected_task_state_token must be a UUID")
    with transaction(pool) as conn:
        gate_row = conn.execute(
            f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates where id = %s",
            (owner_gate_id,),
        ).fetchone()
        if gate_row is None:
            return OwnerGateResolution(
                outcome=OwnerGateResolutionOutcome.NO_OP, gate=None, task=None
            )
        gate = _owner_gate_from_row(gate_row)
        if gate.status is not OwnerGateStatus.PENDING:
            return OwnerGateResolution(
                outcome=OwnerGateResolutionOutcome.NO_OP, gate=gate, task=None
            )
        # Lock the Task row first: currency and token are verified under the
        # lock, so a concurrent pointer move or resolution serializes here.
        task_row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s for update",
            (gate.task_id,),
        ).fetchone()
        if task_row is None:
            return OwnerGateResolution(
                outcome=OwnerGateResolutionOutcome.STALE, gate=gate, task=None
            )
        task = _task_from_row(task_row)
        if (
            task.current_owner_gate_id != gate.id
            or task.state_token != expected_task_state_token
            or task.archived_at is not None
        ):
            return OwnerGateResolution(
                outcome=OwnerGateResolutionOutcome.STALE, gate=gate, task=None
            )
        resolved_row = conn.execute(
            "update openorc.owner_gates "
            "set status = %s, decided_at = now() "
            "where id = %s and status = 'pending' "
            f"returning {_OWNER_GATE_COLUMNS}",
            (outcome.value, owner_gate_id),
        ).fetchone()
        assert resolved_row is not None
        cleared_task_row = conn.execute(
            "update openorc.tasks "
            "set current_owner_gate_id = null, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            "and current_owner_gate_id = %s "
            f"returning {_TASK_COLUMNS}",
            (gate.task_id, expected_task_state_token, gate.id),
        ).fetchone()
        assert cleared_task_row is not None
        return OwnerGateResolution(
            outcome=OwnerGateResolutionOutcome.RESOLVED,
            gate=_owner_gate_from_row(resolved_row),
            task=_task_from_row(cleared_task_row),
        )


class OwnerGateSupersessionDestination(StrEnum):
    """The guarded Task-status destination of a gate supersession.

    Issue #219 introduced the supersession with exactly one destination: the
    Task returns to ``planning`` so the accepted plan can be revised. Issue
    #84 adds the PR-review destination: a Reviewer change request at a
    pending PR/head MERGE_DECISION gate returns the Task to ``reviewing`` so
    the same canonical PR can be remediated and re-reviewed. The vocabulary
    is closed — every other destination is rejected — and the transition
    guards stay identical for both: the Task must be a non-archived
    ``waiting_for_owner`` current-gate holder.
    """

    PLANNING = "planning"
    REVIEWING = "reviewing"


class OwnerGateSupersessionOutcome(StrEnum):
    """The explicit outcome of one :func:`supersede_current_owner_gate` attempt.

    ``SUPERSEDED`` applied the authoritative Task transition: the pending
    gate lost currency and stays pending as history. ``STALE`` applied
    nothing (stale token, the gate is no longer pending, or the gate is no
    longer the Task's current gate). There is deliberately no one-shot
    ``NO_OP``: a supersession never rewrites the gate row, so a replayed
    attempt can only find the gate no longer current and classify as
    ``STALE``.
    """

    SUPERSEDED = "superseded"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class OwnerGateSupersession:
    """The outcome of one current-gate supersession attempt.

    ``task`` is the post-transition Task only for ``SUPERSEDED``. The
    superseded ``gate`` is returned unchanged in both outcomes: it stays
    exactly the durable pending record it was, with no ``decided_at``
    stamp and no terminal status.
    """

    outcome: OwnerGateSupersessionOutcome
    gate: OwnerGate | None
    task: Task | None


def supersede_current_owner_gate(
    pool: DatabasePool,
    *,
    owner_gate_id: UUID,
    expected_task_state_token: UUID,
    destination: OwnerGateSupersessionDestination = OwnerGateSupersessionDestination.PLANNING,
) -> OwnerGateSupersession:
    """Supersede the Task's current gate as one authoritative Task transition.

    Issue #219's narrow Reviewer-triggered continuation primitive: the
    exact current pending gate loses currency WITHOUT any terminal stamp —
    the gate row is never touched and stays pending as history (the
    established superseded-pending-gate property) — while the Task's
    ``current_owner_gate_id`` clears, the Task's status moves from
    ``waiting_for_owner`` to the guarded ``destination`` (``planning`` for
    the accepted-plan continuation, ``reviewing`` for the issue #84 PR-review
    continuation), and the ``state_token`` rotates with its ``updated_at``
    advancing, all in one atomic effect.

    Success requires the gate to be pending, still the Task's current
    gate, and the caller's ``expected_task_state_token`` to still match a
    non-archived ``waiting_for_owner`` Task. The Task row is locked
    ``FOR UPDATE`` first (every gate write happens under that lock) and
    the Task update carries its own conditional guards as a durable
    backstop, so a concurrent resolution or supersession serializes here
    and only one continuation can win. This is never an Owner resolution:
    it stamps no ``decided_at``, writes no terminal gate status, and
    belongs to the E3/E4/E9 continuation seam that separately records the
    Reviewer's iteration and the safe audit trail.
    """
    if not isinstance(expected_task_state_token, UUID):
        raise OwnerGateDomainError("expected_task_state_token must be a UUID")
    if not isinstance(destination, OwnerGateSupersessionDestination):
        raise OwnerGateDomainError(
            "supersede_current_owner_gate requires an "
            "OwnerGateSupersessionDestination (planning or reviewing)"
        )
    with transaction(pool) as conn:
        gate_row = conn.execute(
            f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates where id = %s",
            (owner_gate_id,),
        ).fetchone()
        if gate_row is None:
            return OwnerGateSupersession(
                outcome=OwnerGateSupersessionOutcome.STALE, gate=None, task=None
            )
        gate = _owner_gate_from_row(gate_row)
        if gate.status is not OwnerGateStatus.PENDING:
            return OwnerGateSupersession(
                outcome=OwnerGateSupersessionOutcome.STALE, gate=gate, task=None
            )
        # Lock the Task row first: currency and token are verified under the
        # lock, so a concurrent pointer move or resolution serializes here.
        task_row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s for update",
            (gate.task_id,),
        ).fetchone()
        if task_row is None:
            return OwnerGateSupersession(
                outcome=OwnerGateSupersessionOutcome.STALE, gate=gate, task=None
            )
        task = _task_from_row(task_row)
        if (
            task.current_owner_gate_id != gate.id
            or task.state_token != expected_task_state_token
            or task.archived_at is not None
            or task.status is not TaskStatus.WAITING_FOR_OWNER
        ):
            return OwnerGateSupersession(
                outcome=OwnerGateSupersessionOutcome.STALE, gate=gate, task=None
            )
        superseded_task_row = conn.execute(
            "update openorc.tasks "
            "set current_owner_gate_id = null, status = %s, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            "and current_owner_gate_id = %s and status = 'waiting_for_owner' "
            f"returning {_TASK_COLUMNS}",
            (destination.value, gate.task_id, expected_task_state_token, gate.id),
        ).fetchone()
        assert superseded_task_row is not None
        return OwnerGateSupersession(
            outcome=OwnerGateSupersessionOutcome.SUPERSEDED,
            gate=gate,
            task=_task_from_row(superseded_task_row),
        )
