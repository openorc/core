"""E3b: record bounded planning review decisions atomically (issue #210).

E3b is the planning-loop decision and atomic finalization seam the E3a
dispatch (:class:`openorc.services.task_planning_review_dispatch`) feeds:
given E3a's internal continuation input — the exact reserved
exact-current-plan ReviewIteration, the validated typed ``ReviewResult``,
and the exact READY Reviewer binding that received the REVIEW — it
rechecks every exact subject and, in ONE short composed transaction,
finalizes the result once (outcome, summary, ordered findings,
``decided_at``), records the safe REVIEWER ``PLAN_REVIEWED`` audit event,
and commits the review's durable next decision. Technical evaluation
belongs to the Reviewer; OpenOrc owns only the deterministic decision and
its workflow consequences.

Scope discipline (Phase 2E): this seam never assesses plan semantics,
never settles REVIEW_RESOLUTION gates (E3c), never creates
IMPLEMENTATION_AUTHORIZATION (E5), and never implements Owner discussion
(E4). A stale, duplicate, or uncertain external attempt yields no verdict,
no gate, and no event: an already-finalized attempt, a moved loop or Task
subject, or a non-READY Reviewer binding fail closed with the typed
vocabulary and apply nothing.

Durable decision semantics:

- The OPEN loop's snapshotted ``iteration_limit`` is the number of
  autonomous Producer revisions, not a cap on ReviewIterations
  (:func:`openorc.domain.reviews.autonomous_budget_exhausted`). With the
  default of five, five ``CHANGES_REQUESTED`` reviews each still permit one
  E2 revision and the next REVIEW; a sixth pauses for the Owner. The
  stored snapshot is authoritative — Workspace setting edits affect future
  loops only. No new persistent budget counters exist: the budget state is
  derived from the loop's ordered finalized iteration history.
- Owner-mediated mode is sticky and derived from durable OwnerGate
  history: once any REVIEW_RESOLUTION gate exists for the Task, every
  further ``CHANGES_REQUESTED`` immediately creates a NEW exact-plan
  REVIEW_RESOLUTION gate instead of refreshing the autonomous allowance.
  After E3c's formal Owner-mediated settlement, the same OPEN loop
  continues; the sticky rule derives from recorded history, never from an
  in-memory flag.
- ``REVIEW_LIMIT_REACHED`` is recorded on the FIRST exhaustion only — the
  exhaustion that occurs while no REVIEW_RESOLUTION gate exists in
  durable history. Later Owner-mediated disagreements create fresh gates
  and record ``PLAN_REVIEWED``/``OWNER_GATE_CREATED`` without
  re-representing the already-exhausted budget as newly reached.
- ACCEPTED at any round closes the ReviewLoop and yields the exact
  accepted PlanRevision and finalized iteration evidence for E5 — without
  creating IMPLEMENTATION_AUTHORIZATION and without mutating the Task.
- A within-budget ``CHANGES_REQUESTED`` keeps the Task PLANNING and the
  loop OPEN; the exact finalized findings are routed to merged E2
  (:func:`openorc.services.task_plan_production.revise_task_plan`) OUTSIDE
  any database transaction. Only E2's newly persisted current
  PlanRevision and post-update token may launch E3a's next ordinary REVIEW
  (the amended E3a admission guard refuses re-reviewing an
  already-reviewed revision). An uncertain E2 operation is never resent
  here: the finalized review stays durable and recovery belongs to E2's
  own seam and later orchestration.

The purpose-independent budget/recording seams
(:func:`openorc.domain.reviews.count_changes_requested`,
:func:`openorc.domain.reviews.autonomous_budget_exhausted`, and the
conditional one-shot result recording) are shared with the later PR-review
loop deliberately and minimally; there is no generic decision engine.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGate, OwnerGateType
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
    autonomous_budget_exhausted,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.gates import create_owner_gate, list_task_owner_gates
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.models import (
    REVIEW_OUTCOME_ACCEPTED,
    REVIEW_OUTCOME_CHANGES_REQUESTED,
    ReviewResult,
)
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_plan_production import TaskPlanProductionOutcome, revise_task_plan
from openorc.services.task_planning_review_dispatch import TaskPlanningReviewDispatchOutcome
from openorc.services.task_subject_guards import (
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "PlanningReviewDecision",
    "TaskPlanningReviewDecisionOutcome",
    "finalize_planning_review_decision",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_planning_review_decision"
_DECISION_SPAN_NAME = "task_planning_review_decision.finalize_planning_review_decision"

# The safe logical actor identities this seam records events under: the
# exact REVIEWER session carries the Reviewer's decision; OpenOrc carries
# its own coordination facts (gate creation, first exhaustion).
_OPENORC_ACTOR = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)

# The settled protocol outcome vocabulary mapped onto the domain outcome
# enum; the protocol parser guarantees exactly these two spellings.
_PROTOCOL_REVIEW_OUTCOMES: Mapping[str, ReviewOutcome] = {
    REVIEW_OUTCOME_ACCEPTED: ReviewOutcome.ACCEPTED,
    REVIEW_OUTCOME_CHANGES_REQUESTED: ReviewOutcome.CHANGES_REQUESTED,
}


class PlanningReviewDecision(StrEnum):
    """The durable next decision committed with one finalized planning review.

    ``ACCEPTED_CLEAR`` closes the ReviewLoop and yields the accepted plan
    evidence to E5. ``REVISION_ROUTED`` records an in-budget change request
    whose finalized findings were routed to E2 (the outcome always carries
    the produced revision continuation). ``EXHAUSTED`` records the
    Owner-resolution pause with its exact-plan REVIEW_RESOLUTION gate.
    """

    ACCEPTED_CLEAR = "accepted_clear"
    REVISION_ROUTED = "revision_routed"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True, slots=True)
class TaskPlanningReviewDecisionOutcome:
    """The durable result of one bounded planning review decision.

    ``review_iteration`` is the finalized attempt (complete result facts,
    immutable from here on) and ``plan_revision`` the exact reviewed
    (current) PlanRevision. ``review_loop`` is the loop's post-mutation
    state (CLOSED after acceptance; OPEN otherwise). ``task`` is the
    post-mutation Task where one applied — ``waiting_for_owner`` with the
    installed gate pointer and a rotated token when exhausted, the
    revision-produced Task carried by ``revision_production`` when routed —
    and the verified unchanged PLANNING Task after acceptance. E5 consumes
    the accepted evidence; E3c consumes ``resolution_gate``.
    """

    decision: PlanningReviewDecision
    review_iteration: ReviewIteration
    plan_revision: PlanRevision
    review_loop: ReviewLoop
    task: Task
    revision_production: TaskPlanProductionOutcome | None
    resolution_gate: OwnerGate | None


@dataclass(frozen=True, slots=True)
class _Finalization:
    """The committed Phase-1 facts awaiting the external consequence."""

    decision: _Phase1Decision
    finalized_iteration: ReviewIteration
    reviewed_revision: PlanRevision
    loop: ReviewLoop
    task: Task
    resolution_gate: OwnerGate | None


class _Phase1Decision(StrEnum):
    """The decision committed inside the finalization transaction."""

    ACCEPTED = "accepted"
    ROUTE_REVISION = "route_revision"
    EXHAUSTED = "exhausted"


# --- command validation -------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_runtime(value: object) -> AgentRuntimeAdapter:
    if not isinstance(value, AgentRuntimeAdapter):
        raise InvalidCommandError("runtime must be an AgentRuntimeAdapter")
    return value


# The GitHub adapter capabilities the routing path of this seam actually
# consumes: the authoritative repository observation and the repository
# base-branch observation E2's revision production audits against.
_GITHUB_ROUTE_CAPABILITIES = ("get_installation_repository", "get_repository_branch")


def _require_github(value: object) -> GitHubAppClient:
    """Classify a malformed GitHub adapter dependency as an invalid command.

    ``GitHubAppClient`` is a deliberately structural Protocol seam — services
    never import the concrete HTTP client, and supported fakes implement only
    the capabilities their seam consumes — so the entry boundary verifies the
    adapter capabilities this seam's routing path actually consumes rather
    than demanding full-protocol conformance from every fake. The check runs
    before any transaction or durable effect: an invalid dependency can never
    strand an already-finalized review, because E2's revision production would
    otherwise fail with an unclassified error after the result became
    immutable (issue #210).
    """
    for capability in _GITHUB_ROUTE_CAPABILITIES:
        if not callable(getattr(value, capability, None)):
            raise InvalidCommandError("github must be a GitHubAppClient")
    return cast(GitHubAppClient, value)


def _require_dispatch_outcome(value: object) -> TaskPlanningReviewDispatchOutcome:
    """Require the exact E3a continuation input, not coarse review facts.

    Only the outcome object the successful E3a dispatch returned — the
    reserved exact-subject attempt, the validated typed ``ReviewResult``,
    and the exact READY Reviewer binding — authorizes the decision
    finalization. A bare result, iteration id, or verdict never does.
    """
    if not isinstance(value, TaskPlanningReviewDispatchOutcome):
        raise InvalidCommandError(
            "dispatch must be the successful TaskPlanningReviewDispatchOutcome "
            "returned by the planning review dispatch"
        )
    return value


# --- entry subject/session guards ---------------------------------------------


def _require_decision_subject(task: Task, attempt: TaskPlanningReviewDispatchOutcome) -> None:
    """Prove the supplied dispatch outcome addresses exactly this Task.

    The continuation input's loop and iteration must name the addressed
    Task's own identity and Workspace, and the iteration must belong to its
    loop. A mismatched continuation is a malformed command, not a subject
    to decide against.
    """
    loop = attempt.review_loop
    iteration = attempt.review_iteration
    if loop.task_id != task.id or loop.workspace_id != task.workspace_id:
        raise InvalidCommandError(
            "the supplied dispatch outcome loop does not belong to the addressed task"
        )
    if iteration.task_id != task.id or iteration.workspace_id != task.workspace_id:
        raise InvalidCommandError(
            "the supplied dispatch outcome iteration does not belong to the addressed task"
        )
    if iteration.review_loop_id != loop.id:
        raise InvalidCommandError(
            "the supplied dispatch outcome iteration does not belong to its loop"
        )


def _require_decision_loop(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    attempt: TaskPlanningReviewDispatchOutcome,
) -> ReviewLoop:
    """Re-lock and re-prove the reserved attempt's planning loop.

    The loop row is re-read ``FOR UPDATE`` — the same lock that serialized
    the reservation — so the decision finalization serializes against
    concurrent dispatches and closures. A moved, non-planning, or
    already-closed loop is a stale-or-conflicting subject that applies no
    verdict, event, or gate.
    """
    loop = review_records.get_review_loop(
        transaction_pool, review_loop_id=attempt.review_loop.id, for_update=True
    )
    if (
        loop is None
        or loop.id != attempt.review_loop.id
        or loop.workspace_id != workspace_id
        or loop.task_id != attempt.review_iteration.task_id
    ):
        raise StaleOperationError(
            "the reserved review attempt's planning review loop has moved on; the decision is stale"
        )
    if loop.purpose is not ReviewLoopPurpose.PLANNING:
        raise ConflictError("the reserved review attempt is not a planning review")
    if loop.status is not ReviewLoopStatus.OPEN:
        raise StaleOperationError(
            "the planning review loop has already closed; the decision is stale"
        )
    return loop


def _require_reserved_unfinalized_attempt(
    transaction_pool: DatabasePool,
    *,
    loop: ReviewLoop,
    attempt: TaskPlanningReviewDispatchOutcome,
    revision: PlanRevision,
) -> ReviewIteration:
    """Re-prove the reserved attempt under the loop lock.

    The persisted iteration must be exactly the reserved numbered attempt,
    must address exactly the Task's current PlanRevision the dispatch
    proved, and must still be unfinalized: an already-finalized attempt is
    a duplicate delivery that never yields a second result, event, or gate,
    and an attempt for a superseded revision is stale.
    """
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    persisted = next(
        (iteration for iteration in iterations if iteration.id == attempt.review_iteration.id),
        None,
    )
    if persisted is None:
        raise NotFoundError("the reserved review attempt is not available in this loop")
    if persisted.iteration_number != attempt.review_iteration.iteration_number:
        raise InvalidCommandError(
            "the supplied dispatch outcome iteration does not match the persisted attempt"
        )
    if persisted.plan_revision_id != revision.id:
        raise StaleOperationError(
            "the reserved review attempt no longer addresses the task's current "
            "plan revision; the decision is stale"
        )
    if persisted.outcome is not None:
        raise StaleOperationError(
            "the reserved review attempt has already been finalized; a duplicate "
            "decision never applies"
        )
    return persisted


def _require_decision_reviewer(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_session: TaskAgentSession,
) -> TaskAgentSession:
    """Re-prove the exact admitted READY Reviewer binding that got the REVIEW.

    The durable ``(Task, REVIEWER)`` binding must still be the exact one the
    dispatch resolved — same binding id, still READY, and still carrying the
    same immutable external session identity. Session loss is a blocking
    condition, never a silent replacement; a moved or ended binding applies
    no verdict.
    """
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.id != expected_session.id:
        raise InvalidCommandError(
            "the supplied dispatch outcome names a different reviewer session binding"
        )
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; the decision finalization "
            "uses the exact admitted session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None or (
        binding.external_session_id != expected_session.external_session_id
    ):
        raise InvalidCommandError(
            "the supplied dispatch outcome's reviewer session identity does not "
            "match the admitted binding"
        )
    return binding


# --- result mapping -----------------------------------------------------------


def _reviewer_outcome(result: ReviewResult) -> ReviewOutcome:
    """Map the validated typed result onto the domain outcome vocabulary."""
    outcome = _PROTOCOL_REVIEW_OUTCOMES.get(result.outcome)
    if outcome is None:
        raise InvalidCommandError(
            "the review result carries an outcome outside the settled reviewer vocabulary"
        )
    return outcome


def _finding_documents(result: ReviewResult) -> list[dict[str, str]]:
    """The typed findings as their canonical JSON documents, order preserved.

    The findings array is persisted verbatim as historical evidence; the
    domain canonicalizer validates canonical JSON form on the way in.
    """
    return [{"summary": finding.summary, "details": finding.details} for finding in result.findings]


def _owner_mediated_history(gates: Sequence[OwnerGate]) -> bool:
    """Whether durable gate history already carries a REVIEW_RESOLUTION decision.

    The one durable stickiness source (issue #210): once any
    REVIEW_RESOLUTION gate exists for the Task — any status, any subject
    form — the Task is in Owner-mediated mode and every further change
    request immediately exhausts instead of refreshing the autonomous
    allowance. Derived per decision from recorded history; no in-memory
    flag and no persistent marker exist.
    """
    return any(gate.gate_type is OwnerGateType.REVIEW_RESOLUTION for gate in gates)


def _reviewer_actor(reviewer: TaskAgentSession) -> event_coordination.WorkflowActorContext:
    """The safe REVIEWER logical actor identity: the exact session UUID."""
    return event_coordination.WorkflowActorContext(WorkflowEventActor.REVIEWER, str(reviewer.id))


# --- the finalization transaction ----------------------------------------------


def _finalize_decision_transaction(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    attempt: TaskPlanningReviewDispatchOutcome,
) -> _Finalization:
    """Record the result and its durable next decision in one transaction.

    Composition (database only; the external consequence follows outside):
    the account operational barrier, exact Owner Workspace/Task scope, the
    Task row locked ``FOR UPDATE`` (serializing the decision against every
    concurrent Task mutation), the exact current ``planning`` Task under the
    caller's expected token, the continuation subject coherence checks, the
    exact current PlanRevision, the re-locked OPEN planning loop, the still
    unfinalized reserved attempt, and the exact READY Reviewer binding —
    then the one-shot conditional result recording, the decision facts
    derived from persisted state, the safe audit events, and the decision's
    durable effects.
    """
    with composed_transaction(pool) as transaction_pool:
        require_account_operational(transaction_pool, profile_id=profile_id)
        require_workspace_task(
            transaction_pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
        )
        # Serialize the decision against concurrent Task mutations on the
        # Task row before any currentness decision (existing Task
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
        if task.status is not TaskStatus.PLANNING:
            raise ConflictError("the addressed task is not in its coarse planning state")
        _require_decision_subject(task, attempt)
        attempt_revision_id = attempt.review_iteration.plan_revision_id
        if attempt_revision_id is None:
            # A planning iteration always binds the exact PlanRevision; a
            # PR-subject continuation is never a planning decision input.
            raise InvalidCommandError(
                "the reserved review attempt does not bind a PlanRevision subject"
            )
        persisted_revision = require_current_plan_revision(
            transaction_pool,
            task=task,
            plan_revision_id=attempt_revision_id,
        )
        locked_loop = _require_decision_loop(
            transaction_pool, workspace_id=workspace_id, attempt=attempt
        )
        persisted_attempt = _require_reserved_unfinalized_attempt(
            transaction_pool, loop=locked_loop, attempt=attempt, revision=persisted_revision
        )
        reviewer = _require_decision_reviewer(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_session=attempt.reviewer_session,
        )

        finalized = review_records.record_review_iteration_result(
            transaction_pool,
            review_iteration_id=persisted_attempt.id,
            outcome=_reviewer_outcome(attempt.review_result),
            summary=attempt.review_result.summary,
            findings=_finding_documents(attempt.review_result),
        )
        if finalized is None:
            # The conditional write rejected: the attempt was finalized (or
            # removed) concurrently. A duplicate or raced delivery never
            # yields a second result, event, or gate, and is never retried
            # blindly.
            raise StaleOperationError(
                "the reserved review attempt is no longer unfinalized; the decision is stale"
            )
        finalized_outcome = finalized.outcome
        if finalized_outcome is None:
            # Unreachable through the one-shot conditional write (a returned
            # row carries its complete result atomically); fail closed
            # rather than record a verdict-less event or decision.
            raise ConflictError("the finalized review iteration carries no outcome")

        # The decision facts are derived from persisted state under the
        # loop lock, never from caller input: the refreshed finalized
        # iteration history decides the budget state, and durable gate
        # history decides stickiness and first exhaustion.
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=locked_loop.id
        )
        budget_exhausted = autonomous_budget_exhausted(locked_loop, iterations)
        owner_mediated = _owner_mediated_history(
            list_task_owner_gates(transaction_pool, task_id=task_id)
        )

        event_coordination.record_plan_reviewed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=finalized.id,
            actor=_reviewer_actor(reviewer),
            outcome=finalized_outcome,
        )

        return _apply_decision(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task=task,
            locked_loop=locked_loop,
            finalized=finalized,
            finalized_outcome=finalized_outcome,
            persisted_revision=persisted_revision,
            budget_exhausted=budget_exhausted,
            owner_mediated=owner_mediated,
        )


def _apply_decision(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task: Task,
    locked_loop: ReviewLoop,
    finalized: ReviewIteration,
    finalized_outcome: ReviewOutcome,
    persisted_revision: PlanRevision,
    budget_exhausted: bool,
    owner_mediated: bool,
) -> _Finalization:
    """Apply the derived decision's durable effects in the same transaction.

    ACCEPTED closes the loop (the accepted evidence returns to the caller
    for E5). An exhausted change request — past the snapshotted autonomous
    budget, or with Owner mediation already sticky from durable gate
    history — creates and installs the exact-plan pending
    REVIEW_RESOLUTION gate and moves the Task to ``waiting_for_owner`` in
    one atomic composition, recording ``REVIEW_LIMIT_REACHED`` on the first
    exhaustion only. A within-budget change request leaves the Task
    PLANNING and the loop OPEN for the E2 routing that follows outside.
    """
    if finalized_outcome is ReviewOutcome.ACCEPTED:
        closed_loop = review_records.close_review_loop(
            transaction_pool, review_loop_id=locked_loop.id
        )
        if closed_loop is None:
            raise StaleOperationError(
                "the planning review loop is no longer open; the decision is stale"
            )
        return _Finalization(
            _Phase1Decision.ACCEPTED,
            finalized,
            persisted_revision,
            closed_loop,
            task,
            None,
        )

    if budget_exhausted or owner_mediated:
        gate = create_owner_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            gate_type=OwnerGateType.REVIEW_RESOLUTION,
            plan_revision_id=persisted_revision.id,
        )
        installed = task_records.set_current_owner_gate(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            owner_gate_id=gate.id,
        )
        if installed is None:
            raise StaleOperationError(
                "the resolution gate could not be installed as the task's "
                "current gate; the decision is stale"
            )
        event_coordination.record_owner_gate_created_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=gate.id,
            actor=_OPENORC_ACTOR,
        )
        waiting = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=installed.state_token,
            status=TaskStatus.WAITING_FOR_OWNER,
        )
        if waiting is None:
            raise StaleOperationError(
                "the task could not move to waiting_for_owner; the decision is stale"
            )
        if not owner_mediated:
            # First exhaustion only: the loop crosses its autonomous
            # budget exactly once, and the durable gate history check is
            # what distinguishes that moment from every later
            # Owner-mediated disagreement.
            event_coordination.record_review_limit_reached_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                review_loop_id=locked_loop.id,
                actor=_OPENORC_ACTOR,
            )
        return _Finalization(
            _Phase1Decision.EXHAUSTED,
            finalized,
            persisted_revision,
            locked_loop,
            waiting,
            gate,
        )

    # Within budget: the Task stays PLANNING and the loop stays OPEN. No
    # durable effect beyond the finalized result and its event; the exact
    # finalized findings route to E2 outside this transaction.
    return _Finalization(
        _Phase1Decision.ROUTE_REVISION,
        finalized,
        persisted_revision,
        locked_loop,
        task,
        None,
    )


# --- the decision operation -----------------------------------------------------


def finalize_planning_review_decision(
    pool: DatabasePool,
    runtime: AgentRuntimeAdapter,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    dispatch: TaskPlanningReviewDispatchOutcome,
) -> TaskPlanningReviewDecisionOutcome:
    """Finalize one reserved planning review and commit its durable decision.

    Consumes the exact successful E3a continuation input: the reserved
    exact-current-plan ReviewIteration, the validated typed
    ``ReviewResult``, and the exact READY Reviewer binding. Every exact
    subject the dispatch proved is rechecked in one short composed
    transaction, the result is finalized once, and the durable next
    decision commits with its safe audit events.

    On an in-budget ``CHANGES_REQUESTED``, the exact finalized findings
    route to merged E2 (:func:`revise_task_plan`) with NO database
    transaction open; only E2's newly persisted current PlanRevision and
    post-update token may launch E3a's next ordinary REVIEW, and an
    uncertain E2 operation is never resent here. On exhaustion the
    exact-plan pending REVIEW_RESOLUTION gate is created and installed, the
    Task moves to ``waiting_for_owner``, and exhausted findings are never
    forwarded. On acceptance the loop closes and the accepted evidence is
    yielded for E5 without creating IMPLEMENTATION_AUTHORIZATION.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DECISION_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        runtime_adapter = _require_runtime(runtime)
        github_client = _require_github(github)
        attempt = _require_dispatch_outcome(dispatch)
        annotate_span(
            span,
            operation=_DECISION_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: decision finalization (database only). ---
        finalization = _finalize_decision_transaction(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            attempt=attempt,
        )

        # --- Phase 2: the external consequence, with NO transaction open. ---
        if finalization.decision is _Phase1Decision.ROUTE_REVISION:
            production = revise_task_plan(
                pool,
                runtime_adapter,
                github_client,
                workspace_id=workspace_id,
                task_id=task_id,
                profile_id=profile_id,
                expected_state_token=expected_state_token,
                prior_plan_revision_id=finalization.reviewed_revision.id,
                review_iteration_id=finalization.finalized_iteration.id,
            )
            return TaskPlanningReviewDecisionOutcome(
                decision=PlanningReviewDecision.REVISION_ROUTED,
                review_iteration=finalization.finalized_iteration,
                plan_revision=finalization.reviewed_revision,
                review_loop=finalization.loop,
                task=production.task,
                revision_production=production,
                resolution_gate=None,
            )

        return TaskPlanningReviewDecisionOutcome(
            decision=(
                PlanningReviewDecision.ACCEPTED_CLEAR
                if finalization.decision is _Phase1Decision.ACCEPTED
                else PlanningReviewDecision.EXHAUSTED
            ),
            review_iteration=finalization.finalized_iteration,
            plan_revision=finalization.reviewed_revision,
            review_loop=finalization.loop,
            task=finalization.task,
            revision_production=None,
            resolution_gate=finalization.resolution_gate,
        )
