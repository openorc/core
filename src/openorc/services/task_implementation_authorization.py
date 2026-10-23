"""E5: the durable implementation-authorization boundary (issue #80).

E5 is the Owner authority seam between Reviewer plan acceptance and
implementation. Reviewer ACCEPTED only makes the exact current PlanRevision
*eligible* for Owner authorization — it is never human authorization and
never closes the planning ReviewLoop. This module owns the three durable
consequences of that boundary for the one current, non-archived Task:

- ``open_implementation_authorization`` presents the pending
  IMPLEMENTATION_AUTHORIZATION gate as the natural continuation of accepted
  formal review. It derives every clearance fact from the canonical tables
  inside one short composed transaction — never from a caller's free-text
  acceptance or a detached outcome object — and installs one pending
  exact-plan gate as the Task's current gate. Both accepted shapes take
  this one path: ordinary E3b ``ACCEPTED_CLEAR`` (Task still ``planning``,
  no gate) and mediated E3c ``SETTLED_ACCEPTED`` (Task still
  ``waiting_for_owner`` with the settled gate's pointer cleared). The
  Owner's choices at the presented gate are Approve (below), ordinary E4
  discussion on the same still-pending gate (no E5 seam: discussion keeps
  it pending and approvable, and a standalone Reviewer ACCEPTED there is
  advisory), and Cancel (below). When #219's change-request handoff retires
  a presented gate, a later distinct Reviewer-accepted PlanRevision earns a
  NEW gate through this same path; the retired gate is never approvable.
- ``approve_implementation_authorization`` is the one irreversible planning
  boundary: in one short transaction it resolves the exact current pending
  gate APPROVED with truthful OWNER provenance, closes the still-OPEN
  planning ReviewLoop exactly once, moves the Task to ``implementing``
  under a rotated token, and records the safe audit events — commit or
  roll back together, so a forced late failure leaves no partial
  approval/closure/transition/event. The approved gate's exact subject and
  the persisted acceptance evidence are the implementation authority E6
  (issue #81) consumes; this module returns the durable handoff facts and
  performs no runtime realization, no external send, and no Execution.
  A worker crash after approval cannot cause a second approval: the
  resolution is one-shot, so a replay classifies as a stale operation and
  never bypasses E6's separate delivery/reconciliation contract.
- ``cancel_implementation_authorization`` cancels the Task at its exact
  current pending implementation gate: the gate resolves CANCELLED with
  truthful OWNER provenance and the Task archives CANCELLED with its safe
  ``TASK_CANCELLED`` event in one atomic composition. Owner cancellation is
  a terminal Task outcome, never a rejection of the plan and never a
  ``REJECTED`` verdict — there is deliberately no Owner "Decline" — and no
  GitHub artifact is touched. Broad cross-stage cancellation, active
  runtime abort, and cleanup coordination belong to E10 (issue #85); this
  seam only guarantees an exact pending gate cannot survive its Task's
  cancellation.

Rejections fail closed on the typed application vocabulary without any
write: malformed commands are ``InvalidCommandError``; missing and
cross-Workspace/Task subjects are the uniform ``NotFoundError``; stale
tokens and moved or gone subjects (archived Task, superseded or already
resolved gate, moved plan, vanished loop, raced presentation) are
``StaleOperationError``; genuine current-state conflicts (no valid
Reviewer acceptance evidence, an unresolved trailing attempt, a newer
change request, an existing current gate, a wrong pre-gate state) are
``ConflictError``. Facts are derived from the established records only:
no new gate types, gate statuses, columns, tables, or status vocabulary
exist, and no database transaction spans external I/O (E5 performs none).
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.gates import (
    OwnerGateResolution,
    OwnerGateResolutionOutcome,
    create_owner_gate,
)
from openorc.persistence.gates import (
    resolve_owner_gate as resolve_gate_records,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_cancellation import cancel_task_composition
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "CancelledImplementationAuthorization",
    "ImplementationAuthorization",
    "ImplementationGateOpened",
    "approve_implementation_authorization",
    "cancel_implementation_authorization",
    "open_implementation_authorization",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_implementation_authorization"
_OPEN_SPAN_NAME = "task_implementation_authorization.open_implementation_authorization"
_APPROVE_SPAN_NAME = "task_implementation_authorization.approve_implementation_authorization"
_CANCEL_SPAN_NAME = "task_implementation_authorization.cancel_implementation_authorization"

# OpenOrc's own coordination fact — presenting the exact accepted plan and
# its pending gate to the Owner — is a safe control-plane fact, never an
# Owner decision. The Owner's Approve/Cancel decisions carry the
# authenticated Owner logical identity through
# :func:`event_coordination.owner_actor` instead.
_OPENORC_ACTOR = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)


@dataclass(frozen=True, slots=True)
class ImplementationGateOpened:
    """The presented implementation gate's durable handoff.

    Carries the post-presentation Task (``waiting_for_owner`` under its
    rotated ``state_token``), the exact pending gate binding the exact
    current PlanRevision, and the persisted acceptance evidence: the
    still-OPEN planning ReviewLoop and the latest adjudicating finalized
    ACCEPTED ReviewIteration that made the plan eligible.
    """

    task: Task
    owner_gate: OwnerGate
    plan_revision: PlanRevision
    review_iteration: ReviewIteration
    review_loop: ReviewLoop


@dataclass(frozen=True, slots=True)
class ImplementationAuthorization:
    """The Owner-approved implementation authority's durable handoff.

    Carries the resolved APPROVED gate — whose exact subject and persisted
    acceptance evidence are the implementation authority — the post-write
    ``implementing`` Task with its rotated ``state_token``, and the closed
    planning ReviewLoop. Sufficient for E6 (issue #81) to address the
    original READY Producer binding; E5 itself realizes nothing.
    """

    task: Task
    owner_gate: OwnerGate
    plan_revision: PlanRevision
    review_iteration: ReviewIteration
    review_loop: ReviewLoop


@dataclass(frozen=True, slots=True)
class CancelledImplementationAuthorization:
    """The cancel-at-gate outcome's durable handoff.

    Carries the terminal ``cancelled`` archived Task with its rotated
    token and the exact pending gate resolved CANCELLED with truthful
    OWNER provenance. A #219-superseded pending gate is unrelated history
    and is never rewritten here.
    """

    task: Task
    owner_gate: OwnerGate


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_open_planning_loop(
    transaction_pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> ReviewLoop:
    """Resolve and re-lock the Task's one OPEN planning ReviewLoop.

    The same derivation E3c's settlement uses: the loop is resolved from
    durable state (never caller input) and re-read ``FOR UPDATE`` — the
    same lock that serialized iteration creation and closure — so the
    authorization composition serializes against concurrent loop-lifecycle
    moves. No OPEN planning loop means the clearance window is gone (for
    example a replayed presentation after approval): a stale operation.
    More than one cannot arise through the established loop establishment
    and is refused rather than silently picked.
    """
    open_planning = [
        loop
        for loop in review_records.list_task_review_loops(
            transaction_pool, task_id=task_id, status=ReviewLoopStatus.OPEN
        )
        if loop.purpose is ReviewLoopPurpose.PLANNING
    ]
    if len(open_planning) > 1:
        raise ConflictError(
            "the task carries more than one open planning review loop; "
            "the implementation authorization is refused"
        )
    if not open_planning:
        raise StaleOperationError(
            "the planning review loop is no longer open; the implementation authorization is stale"
        )
    loop = review_records.get_review_loop(
        transaction_pool, review_loop_id=open_planning[0].id, for_update=True
    )
    if (
        loop is None
        or loop.id != open_planning[0].id
        or loop.workspace_id != workspace_id
        or loop.task_id != task_id
    ):
        raise StaleOperationError(
            "the planning review loop has moved on; the implementation authorization is stale"
        )
    if loop.purpose is not ReviewLoopPurpose.PLANNING:
        raise ConflictError("the open review loop is not a planning review loop")
    if loop.status is not ReviewLoopStatus.OPEN:
        raise StaleOperationError(
            "the planning review loop is no longer open; the implementation authorization is stale"
        )
    return loop


def _require_accepted_clearance(
    transaction_pool: DatabasePool, *, loop: ReviewLoop, revision: PlanRevision
) -> ReviewIteration:
    """Derive the exact accepted-plan clearance evidence from persisted state.

    The latest adjudicating iteration — the latest finalized one of the
    locked loop's persisted history — must be a Reviewer ACCEPTED bound to
    the exact current PlanRevision. A trailing unfinalized attempt, a newer
    change request, a stale/historical earlier acceptance, or cross-Task/
    Workspace evidence yields no clearance: nothing is reconstructed from
    Owner prose, from an earlier iteration, or from a detached outcome
    object, and no write is attempted.
    """
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    for iteration in iterations:
        if iteration.workspace_id != loop.workspace_id or iteration.task_id != loop.task_id:
            raise ConflictError(
                "the planning loop carries cross-scope iteration history; "
                "the implementation authorization is refused"
            )
    if not iterations:
        raise ConflictError(
            "the planning loop carries no adjudicating review; no accepted plan clearance exists"
        )
    if iterations[-1].outcome is None:
        raise ConflictError(
            "the latest review attempt is still unresolved; no accepted plan clearance exists"
        )
    adjudicating = iterations[-1]
    if adjudicating.outcome is not ReviewOutcome.ACCEPTED:
        raise ConflictError(
            "the latest adjudicating review is a change request; no accepted plan clearance exists"
        )
    if adjudicating.plan_revision_id != revision.id:
        raise StaleOperationError(
            "the accepted review no longer addresses the task's current plan "
            "revision; the clearance is stale"
        )
    return adjudicating


def _require_resolution_applied(resolution: OwnerGateResolution) -> tuple[OwnerGate, Task]:
    """Translate the persistence resolution outcome into the typed vocabulary.

    ``RESOLVED`` returns the resolved gate and post-write Task (pointer
    cleared, token rotated, ``decided_at`` stamped). A missing gate is the
    uniform ``NotFoundError``; ``NO_OP`` (already resolved) and ``STALE``
    (no longer current or token-moved) are stale operations that applied
    nothing and are never retried blindly.
    """
    if resolution.outcome is OwnerGateResolutionOutcome.RESOLVED:
        if resolution.gate is None or resolution.task is None:
            # Unreachable through the conditional resolution write; fail
            # closed rather than proceed with a half-proved resolution.
            raise StaleOperationError("the owner gate resolution applied no durable effect")
        return resolution.gate, resolution.task
    if resolution.gate is None:
        raise NotFoundError("the requested owner gate is not available for this task")
    if resolution.outcome is OwnerGateResolutionOutcome.NO_OP:
        raise StaleOperationError(
            "the owner gate has already been resolved; the operation is stale"
        )
    raise StaleOperationError(
        "the owner gate is no longer the task's current gate; the operation is stale"
    )


def open_implementation_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
) -> ImplementationGateOpened:
    """Present the exact current accepted plan's implementation gate.

    The Owner-authorized continuation of accepted formal review (no
    additional Owner decision is required to *present* the gate): one
    short composed transaction validates the command, the account
    operational barrier, and exact Workspace/Task Owner scope; locks the
    exact non-archived Task row; requires the expected Task token; requires
    the pre-gate state — ``planning`` (ordinary E3b acceptance) or
    ``waiting_for_owner`` with no current gate (E3c settlement) — the exact
    current PlanRevision, exactly one OPEN planning ReviewLoop, and the
    latest adjudicating finalized Reviewer ACCEPTED bound to that exact
    revision; then creates and installs one pending exact-plan
    IMPLEMENTATION_AUTHORIZATION gate as the Task's current gate, moves a
    still-``planning`` Task to ``waiting_for_owner``, and records the safe
    OPENORC audit facts. Atomic rollback on any failure: replay/race can
    never create a second current gate or an orphaned gate row.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _OPEN_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_OPEN_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            # Serialize the presentation against concurrent Task mutations on
            # the Task row before any currentness decision (existing Task
            # persistence; no new lock, index, or queue machinery).
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.current_owner_gate_id is not None:
                raise ConflictError(
                    "the task already carries a current owner gate; the "
                    "implementation authorization gate cannot be opened"
                )
            if task.status not in (TaskStatus.PLANNING, TaskStatus.WAITING_FOR_OWNER):
                raise ConflictError(
                    "the task is not in a pre-gate planning state; the "
                    "implementation authorization gate cannot be opened"
                )
            if task.current_plan_revision_id is None:
                raise ConflictError(
                    "the task carries no current plan revision; the "
                    "implementation authorization gate cannot be opened"
                )
            revision = require_current_plan_revision(
                transaction_pool,
                task=task,
                plan_revision_id=task.current_plan_revision_id,
            )
            loop = _require_open_planning_loop(
                transaction_pool, workspace_id=workspace_id, task_id=task_id
            )
            iteration = _require_accepted_clearance(transaction_pool, loop=loop, revision=revision)
            gate = create_owner_gate(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
                plan_revision_id=revision.id,
            )
            installed = task_records.set_current_owner_gate(
                transaction_pool,
                task_id,
                expected_state_token=task.state_token,
                owner_gate_id=gate.id,
            )
            if installed is None:
                raise StaleOperationError(
                    "the implementation authorization gate could not be installed "
                    "as the task's current gate; the presentation is stale"
                )
            if installed.status is TaskStatus.PLANNING:
                waiting = task_records.update_task_status(
                    transaction_pool,
                    task_id,
                    expected_state_token=installed.state_token,
                    status=TaskStatus.WAITING_FOR_OWNER,
                )
                if waiting is None:
                    raise StaleOperationError(
                        "the task could not move to waiting_for_owner; the presentation is stale"
                    )
            else:
                waiting = installed
            event_coordination.record_owner_gate_created_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=gate.id,
                actor=_OPENORC_ACTOR,
            )
            event_coordination.record_plan_ready_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                plan_revision_id=revision.id,
                actor=_OPENORC_ACTOR,
            )
            return ImplementationGateOpened(
                task=waiting,
                owner_gate=gate,
                plan_revision=revision,
                review_iteration=iteration,
                review_loop=loop,
            )


def approve_implementation_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> ImplementationAuthorization:
    """Resolve the exact current implementation gate APPROVED — the one
    irreversible planning boundary.

    One short composed transaction: the account operational barrier, exact
    Owner Workspace/Task scope, the Task row locked ``FOR UPDATE``
    (serializing the approval against every concurrent Task mutation), the
    exact current non-archived ``waiting_for_owner`` Task under the
    caller's expected token, the exact current pending
    IMPLEMENTATION_AUTHORIZATION gate, the gate's exact-plan subject equal
    to the current PlanRevision, the re-locked OPEN planning loop, and the
    latest adjudicating finalized Reviewer ACCEPTED bound to that exact
    revision. Every fact is rechecked while serialized on the Task: a stale
    token, a switched plan, a #219-retired gate, a raced cancellation, or a
    non-current gate applies nothing. Then, atomically: the gate resolves
    APPROVED with truthful OWNER provenance, the planning ReviewLoop closes
    exactly once (only now), the Task moves to ``implementing`` with a
    rotated token, and the safe audit events record the resolution and the
    authorization. Gate approval + loop closure + Task transition + audit
    commit or roll back together; no adapter is ever realized or sent.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _APPROVE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_APPROVE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.WAITING_FOR_OWNER:
                raise ConflictError("the addressed task is not waiting on its owner gate")
            gate = require_current_owner_gate(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            if gate.gate_type is not OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
                raise ConflictError(
                    "the addressed owner gate is not an implementation authorization gate"
                )
            if gate.plan_revision_id is None:
                # Unreachable through the domain/persistence subject-form
                # validation; fail closed rather than authorize a formless
                # subject.
                raise ConflictError(
                    "the implementation authorization gate carries no plan revision subject"
                )
            revision = require_current_plan_revision(
                transaction_pool, task=task, plan_revision_id=gate.plan_revision_id
            )
            loop = _require_open_planning_loop(
                transaction_pool, workspace_id=workspace_id, task_id=task_id
            )
            iteration = _require_accepted_clearance(transaction_pool, loop=loop, revision=revision)
            resolution = resolve_gate_records(
                transaction_pool,
                owner_gate_id=gate.id,
                outcome=OwnerGateStatus.APPROVED,
                expected_task_state_token=expected_state_token,
            )
            approved_gate, approved_task = _require_resolution_applied(resolution)
            closed_loop = review_records.close_review_loop(transaction_pool, review_loop_id=loop.id)
            if closed_loop is None:
                # The loop was re-locked OPEN above in this transaction; fail
                # closed rather than record a closure-less authorization.
                raise StaleOperationError(
                    "the planning review loop is no longer open; the approval is stale"
                )
            implementing = task_records.update_task_status(
                transaction_pool,
                task_id,
                expected_state_token=approved_task.state_token,
                status=TaskStatus.IMPLEMENTING,
            )
            if implementing is None:
                raise StaleOperationError(
                    "the task could not move to implementing; the approval is stale"
                )
            owner_actor = event_coordination.owner_actor(profile_id)
            event_coordination.record_owner_gate_resolved_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=approved_gate.id,
                actor=owner_actor,
                outcome=OwnerGateStatus.APPROVED,
            )
            event_coordination.record_implementation_authorized_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                plan_revision_id=revision.id,
                actor=owner_actor,
            )
            return ImplementationAuthorization(
                task=implementing,
                owner_gate=approved_gate,
                plan_revision=revision,
                review_iteration=iteration,
                review_loop=closed_loop,
            )


def cancel_implementation_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> CancelledImplementationAuthorization:
    """Cancel the Task at its exact current pending implementation gate.

    The narrow terminal-outcome orchestration keeping an exact pending gate
    from remaining approvable after cancellation (broad cross-stage
    cancellation belongs to E10, issue #85). One short composed
    transaction: the same authorization/currentness/gate composition as
    approval, then — atomically — the exact pending gate resolves
    CANCELLED with truthful OWNER provenance (pointer cleared, token
    rotated, ``decided_at`` stamped) and the Task archives CANCELLED under
    the post-resolution token with its safe OWNER ``TASK_CANCELLED``
    event. Owner cancellation is a terminal outcome, never a rejection:
    no ``REJECTED`` verdict exists, and no GitHub artifact is touched. The
    gate resolution happens first because it rotates the token the
    archival consumes; both effects and their events commit or roll back
    together, so the gate is never approvable again.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _CANCEL_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_CANCEL_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # Since E10b (issue #228) the gate-cancellation entrypoints delegate
        # to the ONE shared cancellation composition: the same
        # authorization/currentness/gate composition, the exact pending
        # gate resolved CANCELLED with truthful OWNER provenance, and the
        # terminal ``cancelled`` archival with its safe OWNER
        # ``TASK_CANCELLED`` event in one composition. Owner cancellation
        # remains a terminal Task outcome — never a rejection of the
        # implementation — and no GitHub artifact is touched.
        composition = cancel_task_composition(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate_id=owner_gate_id,
            expected_gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        )
        owner_gate = composition.owner_gate
        assert owner_gate is not None
        return CancelledImplementationAuthorization(
            task=composition.task,
            owner_gate=owner_gate,
        )
