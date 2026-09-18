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
history and is never rewritten into an outcome.

Subject construction is validated here to mirror the domain/DB coherence
(a transaction cannot commit a subject shape the domain rejects): each
gate type binds exactly its subject form — the exact review-cleared
PlanRevision for implementation authorization, the exact head SHA (with
no PR binding) for the pre-PR authorization gate, the exact
TaskPullRequest plus the exact head SHA for merge decision, exactly one
subject for REVIEW_RESOLUTION (planning PlanRevision, or PR subject as
TaskPullRequest plus exact head SHA).
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
from openorc.domain.tasks import Task
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import _TASK_COLUMNS, _task_from_row
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "OwnerGateResolution",
    "OwnerGateResolutionOutcome",
    "create_owner_gate",
    "get_owner_gate",
    "list_task_owner_gates",
    "resolve_owner_gate",
]

_OWNER_GATE_COLUMNS = (
    "id, workspace_id, task_id, gate_type, status, plan_revision_id, "
    "subject_head_sha, task_pull_request_id, decided_at, created_at"
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
    decided_at = row[8]
    return OwnerGate(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        gate_type=OwnerGateType(row[3]),
        status=OwnerGateStatus(row[4]),
        plan_revision_id=row[5],
        subject_head_sha=row[6],
        task_pull_request_id=row[7],
        decided_at=None if decided_at is None else normalize_utc(decided_at),
        created_at=normalize_utc(row[9]),
    )


def _require_subject(
    gate_type: OwnerGateType,
    plan_revision_id: UUID | None,
    subject_head_sha: str | None,
    task_pull_request_id: UUID | None,
) -> None:
    """Mirror the per-type exact-subject coherence at this boundary.

    ``pr_authorization`` is the pre-PR exact-head gate (no PR binding:
    it happens before the canonical PR exists); ``merge_decision`` and the
    PR form of ``review_resolution`` bind the canonical TaskPullRequest
    plus the exact head SHA.
    """
    if gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        if (
            not isinstance(plan_revision_id, UUID)
            or subject_head_sha is not None
            or task_pull_request_id is not None
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
        ):
            raise OwnerGateDomainError(
                "a merge_decision gate requires the exact TaskPullRequest plus "
                "the exact head SHA and no PlanRevision"
            )
    else:  # REVIEW_RESOLUTION: exactly one subject form.
        if isinstance(plan_revision_id, UUID):
            # Planning-exhaustion form: the PlanRevision only.
            if subject_head_sha is not None or task_pull_request_id is not None:
                raise OwnerGateDomainError(
                    "a review_resolution gate on the planning subject binds "
                    "exactly the exhausted PlanRevision: no head SHA and no "
                    "PR binding"
                )
        else:
            # PR-review-exhaustion form: the TaskPullRequest plus the exact
            # head SHA — never a bare head SHA.
            if (
                not isinstance(task_pull_request_id, UUID)
                or not isinstance(subject_head_sha, str)
                or not subject_head_sha.strip()
            ):
                raise OwnerGateDomainError(
                    "a review_resolution gate on the PR-review subject binds "
                    "exactly the TaskPullRequest plus the exact head SHA"
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
) -> OwnerGate:
    """Insert one pending OwnerGate decision record for a Task.

    The gate is created ``pending`` (inserted explicitly — the migration
    declares no lifecycle default) with no closed stamp; the authoritative
    resolution is a separate, later operation. Same-type
    re-decisions are new rows, never rewrites of resolved gates: resolved
    gates are immutable history and are never recycled. The per-type
    exact-subject coherence is validated here (mirroring the domain and the
    database CHECK) so a transaction cannot commit a subject shape the
    domain rejects; the plan-revision and TaskPullRequest subjects must
    additionally belong to the same Task and Workspace
    (``ForeignKeyViolation`` is the durable backstop). Installation as the
    Task's current gate is a separate authoritative Task mutation
    (:func:`openorc.persistence.tasks.set_current_owner_gate`).
    """
    if not isinstance(gate_type, OwnerGateType):
        raise OwnerGateDomainError(
            "create_owner_gate requires an OwnerGateType "
            "(implementation_authorization, pr_authorization, merge_decision, "
            "or review_resolution)"
        )
    _require_subject(gate_type, plan_revision_id, subject_head_sha, task_pull_request_id)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.owner_gates "
            "(workspace_id, task_id, gate_type, status, plan_revision_id, "
            "subject_head_sha, task_pull_request_id) "
            "values (%s, %s, %s, %s, %s, %s, %s) "
            f"returning {_OWNER_GATE_COLUMNS}",
            (
                workspace_id,
                task_id,
                gate_type.value,
                OwnerGateStatus.PENDING.value,
                plan_revision_id,
                subject_head_sha,
                task_pull_request_id,
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
            "(approved, rejected, or cancelled)"
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
