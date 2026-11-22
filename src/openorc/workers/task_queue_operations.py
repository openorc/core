"""F11 Task-queue wake operation composition (issue #257).

The production handler for the one workspace-scoped Task-queue wake kind,
the F1-backed wake submission bridge the capacity-relevant services arrange
post-commit, and the bounded post-restart queue recovery. The handler
invokes the ONE durable FIFO admission processor
(:func:`openorc.services.task_start.reevaluate_queued_tasks`) with
process-local dependencies and — strictly after the processor's committed
admissions — submits exactly one current-token planning-bootstrap
continuation per admitted candidate, rehoming the former Start-outcome
bootstrap submission so no alternate direct-Start bootstrap route remains.
The handler never decides Task state, currentness, capacity, or
authorization, and carries no workflow rules of its own.

Composition rules enforced by this module:

- Wake execution: the handler resolves the executing process's database pool
  and continuation submitter, runs the bounded sweep (optionally resumed
  after the payload's keyset continuation cursor — a paging hint, never
  authority), submits one typed ``TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN``
  per ``ADMITTED`` candidate from that outcome's exact post-claim facts, and
  arranges one bounded continuation wake when the sweep reached its candidate
  budget before exhausting the queued set. A known refusal or uncertain
  submission never fails the already-committed admissions, is never retried
  here, and is never converted into a known non-delivery: the durable
  ``queued``/``planning`` facts own recovery. Sweep failures are this
  boundary's sanitized execution-error containment — the durable state owns
  recovery, and a duplicate wake rederives everything harmlessly.
- Wake submission arrangement: the bridge implements the shared
  :class:`~openorc.services.task_queue_wake.TaskQueueWakeSubmission` seam
  over a supplied process-local F1 :class:`OperationSubmitter` — the serving
  process's bounded local executor in regular execution mode, the canonical
  queue submission in queue mode. Each arrangement becomes exactly one
  ``TASK_QUEUE_WAKE`` invocation carrying only the scoped Workspace
  identifier (plus an optional continuation cursor), and the F1 submission
  outcome maps 1:1 onto the shared wake outcome vocabulary. The bridge owns
  no transport mechanics of its own and no durable state: a known refusal or
  uncertain submission is returned as such — the arranging service treats
  the committed state as authoritative and never replays.
- Recovery (bounded, scheduling-only): :func:`arrange_task_queue_recovery`
  — wired into the serving process's startup in both execution modes —
  derives restart candidates from the persisted ``queued`` Tasks and the
  unclaimed ``planning`` bootstrap facts alone. Lost wakes are re-arranged
  per Workspace; a lost F1 bootstrap handoff is submitted only where the
  read-only recovery classifier positively proves the E1b one-shot claim
  never consumed the bootstrap. No claimed, admitted, or uncertain runtime
  turn is ever replayed, reconstructed, or falsely settled — the existing
  detached correlation and Level 1/2/3 machinery own every consumed effect,
  and the accepted silent-worker-death limitation stands.
- This kind is not enrolled in the F5b Level 2 Task operational-failure
  triage or Level 3 gating: like the GitHub and terminal-finalization
  families, its failures stay contained by this boundary's sanitized
  execution-error reporting and the durable state's own outstanding-work
  semantics. No retry, block, or disposition logic lives here.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from openorc.domain.tasks import Task
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import (
    list_task_queue_recovery_workspaces,
    list_unclaimed_planning_bootstrap_tasks,
)
from openorc.persistence.time import normalize_utc
from openorc.services.task_queue_wake import (
    TaskQueueWakeSubmissionOutcome,
    arrange_task_queue_wake,
)
from openorc.services.task_start import (
    QueuedCandidateDisposition,
    find_unclaimed_bootstrap_handoff,
    reevaluate_queued_tasks,
)
from openorc.workers.invocation import (
    InvalidInvocationError,
    OperationDependencies,
    OperationHandler,
    OperationInvocation,
    OperationKind,
    OperationSubmitter,
    QueueWakePayload,
    SubmissionOutcome,
    SubmissionResult,
    TaskPlanningBootstrapAndInitialPlanPayload,
    TaskScope,
    WorkflowRoleRef,
)

__all__ = [
    "F1TaskQueueWakeSubmission",
    "arrange_task_queue_recovery",
    "build_task_queue_operation_handlers",
]

logger = logging.getLogger(__name__)

# The bounded recovery pass enumerates at most this many candidate subjects
# per trigger; the next supported recovery trigger revisits the rest. A
# paging bound only — never scheduling authority.
_RECOVERY_SUBJECT_BUDGET = 50


def _submit_wake_invocation(
    submitter: OperationSubmitter, payload: QueueWakePayload
) -> SubmissionResult:
    """Submit one wake invocation through the supplied F1 submitter."""
    return submitter.submit(
        OperationInvocation(kind=OperationKind.TASK_QUEUE_WAKE, payload=payload)
    )


def _submit_planning_bootstrap_continuation(
    submitter: OperationSubmitter,
    *,
    task: Task,
    profile_id: UUID,
) -> SubmissionResult:
    """Submit the typed planning-bootstrap continuation for one admitted Task.

    The rehomed F1 planning-bootstrap submission (issue #257): the admitted
    outcome's exact post-claim Task — its rotated ``state_token`` — and the
    accountable Workspace Owner identity compose the existing typed
    invocation, which the executing service re-proves from durable state.
    The returned submission outcome is transport information only, never
    Task truth: a known refusal or uncertain submission never fails the
    already-committed admission, is never retried here, and is never
    converted into a known non-delivery — the durable recovery seams own it.
    """
    return submitter.submit(
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
            payload=TaskPlanningBootstrapAndInitialPlanPayload(
                task=TaskScope(
                    workspace_id=str(task.workspace_id),
                    task_id=str(task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(profile_id),
                expected_state_token=str(task.state_token),
            ),
        )
    )


def _handle_task_queue_wake(payload: Any, *, dependencies: OperationDependencies) -> None:
    """Run one bounded Task-queue re-evaluation wake (F11, issue #257).

    The handler only decodes and delegates: the ONE FIFO admission processor
    rederives every candidate's durable eligibility, capacity, and
    currentness from Postgres; the executing process's continuation submitter
    owns the post-commit bootstrap handoffs and the bounded continuation
    wake. The wake is an invitation to rederive queue eligibility — never
    admission authority — so a duplicate or replayed wake rederives
    everything harmlessly, and a second sweep observes ``no_longer_queued``
    for any candidate an earlier wake already admitted (no second admission,
    no duplicated bootstrap work).
    """
    wake_payload = cast(QueueWakePayload, payload)
    workspace_id = UUID(wake_payload.workspace_id)
    pool = dependencies.database_pool()
    submitter = dependencies.continuation_submitter()
    after_queued_at = (
        None
        if wake_payload.after_queued_at is None
        else normalize_utc(datetime.fromisoformat(wake_payload.after_queued_at))
    )
    after_task_id = None if wake_payload.after_task_id is None else UUID(wake_payload.after_task_id)
    sweep = reevaluate_queued_tasks(
        pool,
        workspace_id=workspace_id,
        after_queued_at=after_queued_at,
        after_task_id=after_task_id,
    )
    for outcome in sweep.outcomes:
        if outcome.disposition is not QueuedCandidateDisposition.ADMITTED:
            continue
        if outcome.admitted_task is None or outcome.owner_profile_id is None:
            # The processor's contract guarantees the post-claim facts on an
            # admitted outcome; a missing fact is a durable-invariant failure
            # that must never synthesize a handoff.
            raise InvalidInvocationError(
                "the queue processor returned an admitted candidate without its post-claim facts"
            )
        result = _submit_planning_bootstrap_continuation(
            submitter, task=outcome.admitted_task, profile_id=outcome.owner_profile_id
        )
        if result.outcome is not SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
            logger.warning(
                "the admitted candidate's planning-bootstrap continuation was not "
                "accepted for delivery (task=%s, outcome=%s, reason=%s); the durable "
                "state owns recovery",
                outcome.admitted_task.id,
                result.outcome.value,
                result.refusal_reason.value if result.refusal_reason is not None else "none",
            )
    if sweep.exhausted or sweep.continuation_cursor is None:
        return
    # Bounded continuation: one wake past the examined tail reaches the
    # unexamined candidates instead of revisiting the same prefix. The
    # cursor is a paging hint only, and a lost continuation wake is harmless
    # — a later wake or recovery trigger rederives the queue from Postgres.
    continuation = _submit_wake_invocation(
        submitter,
        QueueWakePayload(
            workspace_id=str(workspace_id),
            after_queued_at=sweep.continuation_cursor[0].isoformat(),
            after_task_id=str(sweep.continuation_cursor[1]),
        ),
    )
    if continuation.outcome is not SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
        logger.warning(
            "the bounded queue continuation wake was not accepted for delivery "
            "(workspace=%s, outcome=%s); the durable queued facts own recovery",
            workspace_id,
            continuation.outcome.value,
        )


def build_task_queue_operation_handlers() -> dict[OperationKind, OperationHandler]:
    """The explicit F11 wake kind-to-handler table (issue #257).

    Merged into the production handler table next to the F5a Task-workflow,
    F9 Task-less GitHub, and F10 terminal-finalization families; deliberately
    not routed through the F5b-coordinated Task-workflow handler wrapper.
    """
    return {
        OperationKind.TASK_QUEUE_WAKE: _handle_task_queue_wake,
    }


class F1TaskQueueWakeSubmission:
    """The F1-backed Task-queue wake submission bridge (issue #257).

    Implements the shared
    :class:`~openorc.services.task_queue_wake.TaskQueueWakeSubmission` seam
    over a supplied process-local F1 :class:`OperationSubmitter`: each
    post-commit arrangement becomes exactly one ``TASK_QUEUE_WAKE``
    invocation carrying only the scoped Workspace identifier, and the F1
    submission outcome maps 1:1 onto the shared wake outcome vocabulary. A
    submission error is an unknown outcome — never a known non-delivery,
    never a replay — and acceptance is never a queue decision. The arranging
    service owns the containment: the committed capacity-relevant state is
    authoritative either way.
    """

    def __init__(self, submitter: OperationSubmitter) -> None:
        self._submitter = submitter

    def submit(self, *, workspace_id: UUID) -> TaskQueueWakeSubmissionOutcome:
        """Submit one Task-queue wake through the supplied F1 submitter."""
        result = _submit_wake_invocation(
            self._submitter, QueueWakePayload(workspace_id=str(workspace_id))
        )
        if result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
            return TaskQueueWakeSubmissionOutcome.ACCEPTED_FOR_DELIVERY
        if result.outcome is SubmissionOutcome.KNOWN_REFUSED:
            return TaskQueueWakeSubmissionOutcome.KNOWN_REFUSED
        return TaskQueueWakeSubmissionOutcome.UNCERTAIN


def arrange_task_queue_recovery(
    pool: DatabasePool,
    submitter: OperationSubmitter,
    *,
    subject_budget: int = _RECOVERY_SUBJECT_BUDGET,
) -> None:
    """One bounded post-restart Task-queue recovery pass (issue #257).

    The supported recovery trigger wired into the serving process's startup
    in both execution modes — beside the queue-mode hosting process's
    durable restart reconciliation, and beside the regular-mode serving
    process's local-executor ownership: derive the candidates from the
    persisted ``queued`` Tasks and the unclaimed ``planning`` bootstrap
    facts alone, and arrange the corresponding bounded wakes and bootstrap
    handoffs through the executing process's submitter. Every candidate is
    re-proven from committed state at arrangement time; a candidate whose
    durable facts moved on submits nothing. Wake and bootstrap submissions
    are contained — a refused, uncertain, or lost arrangement never fails
    the recovery pass, never replays a claimed or uncertain runtime turn,
    and the next supported trigger revisits the rest.
    """
    bridge = F1TaskQueueWakeSubmission(submitter)
    for workspace_id in list_task_queue_recovery_workspaces(pool, limit=subject_budget):
        arrange_task_queue_wake(
            bridge,
            workspace_id=workspace_id,
            origin="task_queue_recovery",
        )
    for task in list_unclaimed_planning_bootstrap_tasks(pool, limit=subject_budget):
        _arrange_recovered_bootstrap_continuation(
            pool, submitter, workspace_id=task.workspace_id, task_id=task.id
        )


def _arrange_recovered_bootstrap_continuation(
    pool: DatabasePool,
    submitter: OperationSubmitter,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> None:
    """Arrange one recovered planning-bootstrap handoff, re-proven from state.

    The recovery classifier positively proves the E1b one-shot bootstrap
    claim never consumed the handoff before any submission; a candidate
    whose durable facts moved on between enumeration and arrangement submits
    nothing. The workspace read resolves the accountable Owner identity; a
    missing workspace submits nothing.
    """
    try:
        task = find_unclaimed_bootstrap_handoff(pool, workspace_id=workspace_id, task_id=task_id)
    except Exception as error:  # noqa: BLE001 - contained recovery classification
        logger.warning(
            "the bootstrap-handoff recovery classification failed (error type %s) "
            "for task %s; the durable state owns recovery",
            type(error).__name__,
            task_id,
        )
        return
    if task is None:
        return
    workspace = get_workspace(pool, workspace_id=workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    result = _submit_planning_bootstrap_continuation(
        submitter, task=task, profile_id=workspace.owner_profile_id
    )
    if result.outcome is not SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
        logger.warning(
            "the recovered planning-bootstrap handoff was not accepted for delivery "
            "(task=%s, outcome=%s); the durable state owns recovery",
            task.id,
            result.outcome.value,
        )
