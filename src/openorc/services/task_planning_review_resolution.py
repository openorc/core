"""E3c: settle Reviewer-driven REVIEW_RESOLUTION gates (issue #211).

E3c is the narrow Owner-mediated gate-settlement seam E4's formal-result
recognition feeds: at a Task's pending current REVIEW_RESOLUTION gate, a
schema-valid standalone Reviewer ``review_result`` — recognized and
transported by E4 (issue #79), never parsed or negotiated here — is an
authoritative continuation of the **same OPEN planning ReviewLoop**. This
service rechecks every exact subject and, in ONE short composed
transaction, allocates and finalizes a NEW next-numbered ReviewIteration
for the unchanged exact PlanRevision, records the safe REVIEWER
``PLAN_REVIEWED`` audit event, settles the gate with the terminal
``SETTLED`` status (Reviewer provenance, never an Owner APPROVED verdict),
clears the exact current-gate pointer, rotates the Task ``state_token``,
and commits the settlement's durable next decision.

Durable settlement semantics:

- ``SETTLED`` (issue #211) is the Reviewer-driven terminal gate status,
  representable only for ``review_resolution`` gates (database CHECK) and
  reachable only through this seam; the Owner-facing resolution service
  refuses it, so ordinary Owner APPROVED/REJECTED/CANCELLED semantics are
  unchanged and no Owner override exists.
- A formal ACCEPTED closes the loop and yields the same exact cleared
  PlanRevision and the new finalized iteration as the accepted evidence
  for E5's normal IMPLEMENTATION_AUTHORIZATION flow (#80). This seam
  never issues that gate and never authorizes implementation.
- A formal CHANGES_REQUESTED settles the gate, moves the Task
  ``waiting_for_owner`` → ``planning``, and routes ONLY the newly
  finalized findings to merged E2
  (:func:`openorc.services.task_plan_production.revise_task_plan`)
  OUTSIDE any transaction — the historic exhausted findings are never
  rerouted, and an uncertain E2 operation is never resent (the finalized
  settlement stays durable; recovery belongs to E2's own seam and later
  orchestration). Only E2's newly persisted revision and post-update
  token may launch E3a's next ordinary REVIEW; the following
  disagreement immediately creates a NEW REVIEW_RESOLUTION gate because
  Owner-mediated stickiness derives from durable gate history, which now
  includes the settled gate, and never refreshes the spent autonomous
  budget.
- No iteration is pre-reserved for advisory chat: the settlement
  iteration is allocated and finalized atomically at settlement time,
  and earlier CHANGES_REQUESTED history stays immutable.

Scope discipline (Phase 2E): this seam never assesses plan semantics,
never dispatches REVIEW (E3a), never decides the ordinary bounded budget
(E3b), never implements discussion transport (E4), never creates
IMPLEMENTATION_AUTHORIZATION (E5), and never touches PR/head subjects
(E9, which reuses the same purpose-independent settlement primitive). A
stale, foreign, replayed, superseded, non-READY, or non-formal input
never settles the gate: it fails closed with the typed vocabulary and
applies nothing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
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
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.gates import OwnerGateResolutionOutcome
from openorc.persistence.gates import resolve_owner_gate as resolve_gate_records
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
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "PlanningReviewResolutionDecision",
    "TaskPlanningReviewResolutionOutcome",
    "settle_planning_review_resolution",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_planning_review_resolution"
_SETTLEMENT_SPAN_NAME = "task_planning_review_resolution.settle_planning_review_resolution"

# The settled protocol outcome vocabulary mapped onto the domain outcome
# enum; the protocol funnel guarantees exactly these two spellings, and a
# validated typed ``ReviewResult`` is the only accepted settlement input.
_PROTOCOL_REVIEW_OUTCOMES: Mapping[str, ReviewOutcome] = {
    REVIEW_OUTCOME_ACCEPTED: ReviewOutcome.ACCEPTED,
    REVIEW_OUTCOME_CHANGES_REQUESTED: ReviewOutcome.CHANGES_REQUESTED,
}


class PlanningReviewResolutionDecision(StrEnum):
    """The durable next decision committed with one settled resolution gate.

    ``SETTLED_ACCEPTED`` closes the ReviewLoop and yields the exact
    cleared PlanRevision/iteration evidence for E5's normal
    IMPLEMENTATION_AUTHORIZATION flow. ``CONTINUATION_ROUTED`` records the
    settlement whose newly finalized findings were routed to E2 (the
    outcome carries the produced revision continuation).
    """

    SETTLED_ACCEPTED = "settled_accepted"
    CONTINUATION_ROUTED = "continuation_routed"


@dataclass(frozen=True, slots=True)
class TaskPlanningReviewResolutionOutcome:
    """The durable result of one settled REVIEW_RESOLUTION gate.

    ``review_iteration`` is the NEW finalized settlement iteration
    (immutable from here on) for the unchanged exact ``plan_revision`` —
    the Task's still-current revision. ``review_loop`` is the loop's
    post-mutation state (CLOSED after acceptance; OPEN otherwise). ``task``
    is the post-settlement Task (``waiting_for_owner`` with a rotated
    token and no current gate after acceptance; ``planning`` with a
    rotated token when routed). ``resolution_gate`` is the settled gate.
    E5 consumes the accepted evidence; the caller launches the next
    ordinary E3a REVIEW from ``revision_production``.
    """

    decision: PlanningReviewResolutionDecision
    review_iteration: ReviewIteration
    plan_revision: PlanRevision
    review_loop: ReviewLoop
    task: Task
    resolution_gate: OwnerGate
    revision_production: TaskPlanProductionOutcome | None


@dataclass(frozen=True, slots=True)
class _Settlement:
    """The committed Phase-1 facts awaiting the external consequence."""

    decision: PlanningReviewResolutionDecision
    finalized_iteration: ReviewIteration
    settled_revision: PlanRevision
    loop: ReviewLoop
    task: Task
    settled_gate: OwnerGate


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
    strand an already-settled gate, because E2's revision production would
    otherwise fail with an unclassified error after the settlement became
    immutable (issue #210).
    """
    for capability in _GITHUB_ROUTE_CAPABILITIES:
        if not callable(getattr(value, capability, None)):
            raise InvalidCommandError("github must be a GitHubAppClient")
    return cast(GitHubAppClient, value)


def _require_formal_result(value: object) -> ReviewResult:
    """Require the validated typed ReviewResult — the only settlement input.

    E4's future formal-result recognition supplies the schema-valid typed
    result; this seam performs no parsing, no chat transport, and no
    verdict inference. A bare dict, string, or dispatch outcome never
    settles a gate.
    """
    if not isinstance(value, ReviewResult):
        raise InvalidCommandError(
            "review_result must be a validated protocol ReviewResult; "
            "formal-result recognition (E4) owns recognition and transport"
        )
    return value


# --- result mapping -----------------------------------------------------------


def _resolution_outcome(result: ReviewResult) -> ReviewOutcome:
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


def _reviewer_actor(reviewer: TaskAgentSession) -> event_coordination.WorkflowActorContext:
    """The safe REVIEWER logical actor identity: the exact session UUID."""
    return event_coordination.WorkflowActorContext(WorkflowEventActor.REVIEWER, str(reviewer.id))


# --- settlement subject guards -------------------------------------------------


def _require_settlement_gate(task: Task, transaction_pool: DatabasePool) -> OwnerGate:
    """Prove the Task's current gate is the exact-plan resolution gate.

    The settled gate is THE Task's current gate — resolved from the
    Task's pointer, never a caller-supplied id — and must be a pending
    REVIEW_RESOLUTION gate in the planning subject form (the exact
    PlanRevision, no head SHA and no PR binding). A PR/head-subject gate
    belongs to E9's later settlement and is refused here; any other gate
    type never settles through this seam.
    """
    if task.current_owner_gate_id is None:
        raise ConflictError("the task has no current owner gate to settle")
    gate = require_current_owner_gate(
        transaction_pool, task=task, owner_gate_id=task.current_owner_gate_id
    )
    if gate.gate_type is not OwnerGateType.REVIEW_RESOLUTION:
        raise ConflictError(
            "only a pending review_resolution gate settles through the planning settlement seam"
        )
    if (
        gate.plan_revision_id is None
        or gate.subject_head_sha is not None
        or gate.task_pull_request_id is not None
    ):
        raise ConflictError(
            "planning settlement settles only the plan-subject review_resolution "
            "gate; PR/head-subject settlement belongs to later PR-review work"
        )
    if gate.plan_revision_id != task.current_plan_revision_id:
        raise StaleOperationError(
            "the resolution gate no longer addresses the task's current plan "
            "revision; the settlement is stale"
        )
    return gate


def _require_open_planning_loop(
    transaction_pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> ReviewLoop:
    """Resolve and re-lock the Task's one OPEN planning ReviewLoop.

    The settlement continues the same OPEN loop the exhausted reviews
    ran in; the loop is resolved from durable state (never caller input)
    and re-read ``FOR UPDATE`` — the same lock that serialized the
    reservations and closures — so the settlement serializes against
    concurrent dispatches and closures. No OPEN planning loop is a stale
    settlement (for example a replayed settlement after acceptance);
    more than one cannot arise through the established loop
    establishment and is refused rather than silently picked.
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
            "the task carries more than one open planning review loop; the settlement is refused"
        )
    if not open_planning:
        raise StaleOperationError(
            "the planning review loop is no longer open; the settlement is stale"
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
        raise StaleOperationError("the planning review loop has moved on; the settlement is stale")
    if loop.purpose is not ReviewLoopPurpose.PLANNING:
        raise ConflictError("the open review loop is not a planning review loop")
    if loop.status is not ReviewLoopStatus.OPEN:
        raise StaleOperationError(
            "the planning review loop has already closed; the settlement is stale"
        )
    return loop


def _require_resolution_reviewer(
    transaction_pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Prove the exact existing admitted READY Reviewer binding.

    E4's discussion ran in the Task's existing admitted Reviewer session;
    the settlement verifies that exact durable binding after the external
    send — same binding, still READY, never recreated or reinitialized.
    Session loss is a blocking condition, never a silent replacement; a
    moved or ended binding settles nothing.
    """
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; the settlement uses "
            "the exact admitted session only and never recreates or "
            "reinitializes one"
        )
    return binding


def _reserve_and_finalize_settlement_iteration(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    loop: ReviewLoop,
    revision: PlanRevision,
    result: ReviewResult,
    outcome: ReviewOutcome,
) -> ReviewIteration:
    """Allocate and finalize the NEW settlement iteration atomically.

    The caller holds the loop row lock ``FOR UPDATE``; the next number is
    derived from the locked loop's persisted history (never a caller
    input, never a persistent counter), the unique per-loop numbering and
    the loop-OPEN creation guard are the durable backstops, and the
    one-shot conditional result recording finalizes the allocation in the
    same transaction. There is deliberately no pre-reservation for
    advisory chat: the iteration exists only with its complete result.
    """
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    next_number = (
        max(
            (iteration.iteration_number for iteration in iterations),
            default=0,
        )
        + 1
    )
    try:
        reserved = review_records.create_review_iteration(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=next_number,
            plan_revision_id=revision.id,
        )
    except UniqueViolation as exc:
        raise StaleOperationError(
            "a competing review iteration for this loop committed first; the settlement is stale"
        ) from exc
    if reserved is None:
        # The loop-OPEN creation guard rejected the write. The caller holds
        # the loop lock, so this is a concurrent close racing the
        # settlement: fail closed as a stale operation.
        raise StaleOperationError(
            "the planning review loop is no longer open at settlement time; the settlement is stale"
        )
    finalized = review_records.record_review_iteration_result(
        transaction_pool,
        review_iteration_id=reserved.id,
        outcome=outcome,
        summary=result.summary,
        findings=_finding_documents(result),
    )
    if finalized is None:
        # Unreachable for a fresh allocation (the conditional write applies
        # to an iteration this transaction just created); fail closed
        # rather than record a verdict-less event or decision.
        raise StaleOperationError(
            "the settlement iteration is no longer unfinalized; the settlement is stale"
        )
    return finalized


# --- the settlement transaction -------------------------------------------------


def _settle_resolution_transaction(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    result: ReviewResult,
    record_discussion_message_events: bool,
) -> _Settlement:
    """Settle the gate and commit the durable next decision in one transaction.

    Composition (database only; the external consequence follows outside):
    the account operational barrier, exact Owner Workspace/Task scope, the
    Task row locked ``FOR UPDATE`` (serializing the settlement against
    every concurrent Task mutation), the exact current ``waiting_for_owner``
    Task under the caller's expected token, the exact current pending
    plan-subject REVIEW_RESOLUTION gate, the unchanged exact current
    PlanRevision, the re-locked OPEN planning loop, the exact existing
    admitted READY Reviewer binding, the NEW finalized settlement
    iteration, the safe audit events, the gate's one-shot SETTLED stamp
    with pointer clear and token rotation, and the settlement's durable
    next decision. When ``record_discussion_message_events`` is set (E4's
    formal-exchange path, issue #79), the two direction-only
    OWNER ↔ Reviewer discussion message events ride this same transaction,
    so a failed event write rolls the entire settlement back.
    """
    with composed_transaction(pool) as transaction_pool:
        require_account_operational(transaction_pool, profile_id=profile_id)
        require_workspace_task(
            transaction_pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
        )
        # Serialize the settlement against concurrent Task mutations on the
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
        if task.status is not TaskStatus.WAITING_FOR_OWNER:
            raise ConflictError("the addressed task is not waiting on its owner gate")
        gate = _require_settlement_gate(task, transaction_pool)
        if gate.plan_revision_id is None:  # the planning-subject form proved above
            raise ConflictError("the resolution gate carries no plan revision subject")
        revision = require_current_plan_revision(
            transaction_pool,
            task=task,
            plan_revision_id=gate.plan_revision_id,
        )
        locked_loop = _require_open_planning_loop(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        reviewer = _require_resolution_reviewer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        outcome = _resolution_outcome(result)
        finalized = _reserve_and_finalize_settlement_iteration(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            loop=locked_loop,
            revision=revision,
            result=result,
            outcome=outcome,
        )
        finalized_outcome = finalized.outcome
        if finalized_outcome is None:
            # Unreachable through the one-shot conditional write (a returned
            # row carries its complete result atomically); fail closed
            # rather than record a verdict-less event or decision.
            raise ConflictError("the finalized review iteration carries no outcome")

        event_coordination.record_plan_reviewed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=finalized.id,
            actor=_reviewer_actor(reviewer),
            outcome=finalized_outcome,
        )

        settlement = resolve_gate_records(
            transaction_pool,
            owner_gate_id=gate.id,
            outcome=OwnerGateStatus.SETTLED,
            expected_task_state_token=expected_state_token,
        )
        if (
            settlement.outcome is not OwnerGateResolutionOutcome.RESOLVED
            or settlement.gate is None
            or settlement.task is None
        ):
            # The exact-subject guards above make this unreachable in
            # practice; classify it as stale rather than proceed with a
            # half-proved settlement.
            raise StaleOperationError(
                "the resolution gate could not be settled; the settlement is stale"
            )
        settled_gate = settlement.gate
        settled_task = settlement.task

        event_coordination.record_owner_gate_resolved_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=settled_gate.id,
            actor=_reviewer_actor(reviewer),
            outcome=OwnerGateStatus.SETTLED,
            review_iteration_id=finalized.id,
            review_outcome=finalized_outcome,
        )

        if record_discussion_message_events:
            # E4's formal-exchange audit rides this same transaction (issue
            # #79): two direction-only discussion message events for the
            # exchange that produced this result — the Owner message routed
            # to the Reviewer and the Reviewer reply that carried the formal
            # result — both subject-bound to the settled gate. A failed
            # event write rolls the whole settlement back, so the discussion
            # facts and the authoritative settlement commit or vanish
            # together and no partial success is representable.
            event_coordination.record_owner_reviewer_discussion_message_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=settled_gate.id,
                actor=event_coordination.owner_actor(profile_id),
                direction=event_coordination.OwnerReviewerDiscussionDirection.OWNER_TO_REVIEWER,
            )
            event_coordination.record_owner_reviewer_discussion_message_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=settled_gate.id,
                actor=_reviewer_actor(reviewer),
                direction=event_coordination.OwnerReviewerDiscussionDirection.REVIEWER_TO_OWNER,
            )

        if finalized_outcome is ReviewOutcome.ACCEPTED:
            closed_loop = review_records.close_review_loop(
                transaction_pool, review_loop_id=locked_loop.id
            )
            if closed_loop is None:
                raise StaleOperationError(
                    "the planning review loop is no longer open; the settlement is stale"
                )
            return _Settlement(
                PlanningReviewResolutionDecision.SETTLED_ACCEPTED,
                finalized,
                revision,
                closed_loop,
                settled_task,
                settled_gate,
            )

        # The planning disagreement continues: back to planning under the
        # post-settlement rotated token, with the loop left OPEN.
        planning_task = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=settled_task.state_token,
            status=TaskStatus.PLANNING,
        )
        if planning_task is None:
            raise StaleOperationError(
                "the task could not return to planning; the settlement is stale"
            )
        return _Settlement(
            PlanningReviewResolutionDecision.CONTINUATION_ROUTED,
            finalized,
            revision,
            locked_loop,
            planning_task,
            settled_gate,
        )


# --- the settlement operation -----------------------------------------------------


def settle_planning_review_resolution(
    pool: DatabasePool,
    runtime: AgentRuntimeAdapter | None,
    github: GitHubAppClient | None,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    review_result: ReviewResult,
    record_discussion_message_events: bool = False,
) -> TaskPlanningReviewResolutionOutcome:
    """Settle the Task's exact current REVIEW_RESOLUTION gate with a formal result.

    Consumes E4's future formal-result recognition input: the validated
    typed ``ReviewResult`` recognized at the Task's pending current
    REVIEW_RESOLUTION gate. Every exact subject is rechecked in one short
    composed transaction — authenticated Owner Task scope, current
    token/subject, the unchanged exact current PlanRevision, the OPEN
    planning loop, and the exact existing admitted READY Reviewer binding
    (verified after E4's external send, which happens before this entry) —
    and the settlement commits atomically: the NEW next-numbered finalized
    settlement iteration for the unchanged revision, the safe REVIEWER
    audit events, the gate's ``SETTLED`` stamp with Reviewer provenance,
    the cleared current-gate pointer, and the rotated Task token.

    On a formal ACCEPTED the loop closes and the accepted evidence returns
    for E5's normal IMPLEMENTATION_AUTHORIZATION flow — no gate is issued
    and no implementation is authorized here. On a formal
    CHANGES_REQUESTED the Task returns to ``planning`` and the exact
    newly finalized findings route to merged E2
    (:func:`revise_task_plan`) with NO database transaction open; only
    E2's newly persisted current PlanRevision and post-update token may
    launch E3a's next ordinary REVIEW, and an uncertain E2 operation is
    never resent here.

    ``record_discussion_message_events`` (issue #79) records E4's two
    direction-only Owner ↔ Reviewer discussion message events inside this
    settlement's transaction when the settlement is dispatched through
    E4's formal-result path, so the exchange audit and the authoritative
    settlement commit atomically; the default leaves the event sequence
    exactly as before this parameter existed.

    The E2 continuation dependencies (the Producer-side runtime adapter
    and the GitHub client) are proven before the settlement commits
    exactly when the validated result routes to E2 — the same
    no-stranding guarantee as before, now scoped to the routing outcome.
    A formal ACCEPTED never touches them, so E4's formal acceptance can
    dispatch this settlement without acquiring a Producer attachment or
    a GitHub client.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _SETTLEMENT_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        result = _require_formal_result(review_result)
        # The routing decision is fully determined by the validated result
        # outcome, so the E2 dependencies are proven BEFORE the settlement
        # transaction exactly when the result routes to E2; an invalid or
        # missing dependency can never strand an already-settled gate.
        routes_to_e2 = _resolution_outcome(result) is ReviewOutcome.CHANGES_REQUESTED
        runtime_adapter = _require_runtime(runtime) if routes_to_e2 else None
        github_client = _require_github(github) if routes_to_e2 else None
        annotate_span(
            span,
            operation=_SETTLEMENT_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: gate settlement (database only). ---
        settlement = _settle_resolution_transaction(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            result=result,
            record_discussion_message_events=record_discussion_message_events,
        )

        # --- Phase 2: the external consequence, with NO transaction open. ---
        if settlement.decision is PlanningReviewResolutionDecision.CONTINUATION_ROUTED:
            if runtime_adapter is None or github_client is None:
                # Unreachable: the CONTINUATION_ROUTED decision is derived
                # from the same validated outcome that proved both E2
                # dependencies at entry. Refuse rather than proceed with a
                # half-proved continuation.
                raise ConflictError("the E2 continuation dependencies are unavailable")
            production = revise_task_plan(
                pool,
                runtime_adapter,
                github_client,
                workspace_id=workspace_id,
                task_id=task_id,
                profile_id=profile_id,
                expected_state_token=settlement.task.state_token,
                prior_plan_revision_id=settlement.settled_revision.id,
                review_iteration_id=settlement.finalized_iteration.id,
            )
            return TaskPlanningReviewResolutionOutcome(
                decision=PlanningReviewResolutionDecision.CONTINUATION_ROUTED,
                review_iteration=settlement.finalized_iteration,
                plan_revision=settlement.settled_revision,
                review_loop=settlement.loop,
                task=production.task,
                resolution_gate=settlement.settled_gate,
                revision_production=production,
            )

        return TaskPlanningReviewResolutionOutcome(
            decision=PlanningReviewResolutionDecision.SETTLED_ACCEPTED,
            review_iteration=settlement.finalized_iteration,
            plan_revision=settlement.settled_revision,
            review_loop=settlement.loop,
            task=settlement.task,
            resolution_gate=settlement.settled_gate,
            revision_production=None,
        )
