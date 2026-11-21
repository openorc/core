"""E9 §2/§4: atomic PR review decisions, settlement, and merge-gate seam
(issue #84).

Three focused seams share one transactional discipline (database only, the
Task row ``FOR UPDATE`` taken FIRST, every exact subject re-proven under the
lock, external consequences strictly outside):

- ``settle_observed_pr_review`` is the ordinary decision seam the
  correlation seam feeds (issue #338): given the reserved exact-head attempt
  and the validated typed ``ReviewResult`` observed on the admitted detached
  REVIEW turn, it rechecks every subject and in
  ONE short composed transaction finalizes the iteration once, records the
  safe REVIEWER-attributed ``PR_REVIEWED`` event whose primary subject is the
  ReviewIteration, and commits the durable next decision. A within-budget
  ``CHANGES_REQUESTED`` keeps the Task ``REVIEWING`` and the loop OPEN and
  the ONE completion handoff dispatches the Producer remediation as one
  detached turn outside the transaction; an
  exhausted or sticky ``CHANGES_REQUESTED`` atomically creates and installs
  one exact PR/head ``REVIEW_RESOLUTION`` and moves ``REVIEWING →
  WAITING_FOR_OWNER`` without routing; an ``ACCEPTED`` atomically creates and
  installs one exact-head ``MERGE_DECISION`` and moves to
  ``WAITING_FOR_OWNER`` with the loop deliberately left OPEN (only a
  GitHub-confirmed merge or terminal cancellation closes it, in E10).
- ``settle_observed_pr_review_resolution`` is the PR/head-subject sibling of E3c's
  Reviewer-driven settlement, consumed through E4's formal-result routing: a
  new, standalone, schema-valid ``review_result`` at the Task's current
  PR/head ``REVIEW_RESOLUTION`` gate appends a NEW next-numbered finalized
  same-head iteration, records the REVIEWER ``PR_REVIEWED`` audit and — when
  routed through E4 — the two direction-only discussion events, settles that
  exact gate through the purpose-independent ``resolve_owner_gate``
  ``SETTLED`` primitive with truthful Reviewer provenance, clears the current
  pointer, and rotates the token. A formal ``ACCEPTED`` proceeds to a NEW
  ordinary exact-head ``MERGE_DECISION``; a formal ``CHANGES_REQUESTED``
  returns the Task to ``REVIEWING`` and the ONE completion handoff dispatches
  the new remediation round as one detached turn outside the transaction.
  Later disagreement
  immediately reopens resolution; prior results are immutable.
- ``retire_observed_pr_merge_decision_with_changes`` is issue #219's narrow
  supersession pattern applied to the merge gate: at the Task's current
  pending PR/head ``MERGE_DECISION`` gate, ordinary discussion, malformed or
  embedded JSON, and even a standalone formal ``ACCEPTED`` stay advisory
  (E4's conversation; the gate stays pending and approvable while its head is
  unchanged). Only a standalone valid ``CHANGES_REQUESTED`` is actionable:
  atomically append the NEW same-head PR iteration, record the REVIEWER
  ``PR_REVIEWED`` audit and the safe E4 events, retire G1 as no-longer-current
  through the guarded ``supersede_current_owner_gate`` ``reviewing``
  destination (the gate row stays pending history — never Owner-stamped,
  never SETTLED, no false gate-resolution event, no intervening
  REVIEW_RESOLUTION), clear the pointer, rotate the token, move
  ``WAITING_FOR_OWNER → REVIEWING``, and route only the new result to the
  remediation seam outside the transaction.

Budget and stickiness (deliberately PR-scoped):

- The OPEN PR loop's snapshotted ``iteration_limit`` is the number of
  autonomous Producer remediations, not a cap on ReviewIterations
  (:func:`openorc.domain.reviews.autonomous_budget_exhausted`). The stored
  snapshot is authoritative; the budget never resets; planning-loop history
  is never consulted.
- Owner-mediated stickiness derives ONLY from this canonical PR's PR/head
  ``REVIEW_RESOLUTION``/``MERGE_DECISION`` gate history — deliberately
  narrower than E3b's all-gates helper. After any Owner-facing PR gate
  exists, every further ``CHANGES_REQUESTED`` immediately creates a fresh
  resolution gate instead of refreshing the autonomous allowance.
- ``REVIEW_LIMIT_REACHED`` is recorded on the FIRST exhaustion only — the
  exhaustion that occurs while no PR/head REVIEW_RESOLUTION or MERGE_DECISION
  gate exists in durable history. Later disagreements create fresh gates and
  never re-represent the already-exhausted budget as newly reached.

Race and dependency discipline (mirroring merged #219): the Producer-side
continuation dependencies — the Producer runtime adapter, the GitHub client,
and the deferred Reviewer attachment factory for the re-entered dispatch —
are validated at entry exactly when the validated result routes to
remediation, so an unusable dependency can never strand an already-finalized
or already-superseded gate. The Task row lock serializes a raced Owner
decision/cancellation; sends happen only outside transactions; a failed late
event write rolls the iteration + gate/pointer/status/audit effects together.
A stale, duplicate, cancelled/merged, changed-head, or uncertain continuation
creates no second verdict, gate, or event.

GitHub checks/statuses remain GitHub-owned read-only observations through
existing B7 interfaces; no merge policy, CI gating, or second checks store
exists here. The Owner merge operation, expected-head merge enforcement, and
terminal composition belong to E10 (issue #85).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.pull_requests import TaskPullRequest
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
from openorc.persistence import gates as gate_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.gates import (
    OwnerGateResolutionOutcome,
    OwnerGateSupersessionDestination,
    OwnerGateSupersessionOutcome,
)
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
from openorc.services.task_plan_production import settle_resumed_task_record
from openorc.services.task_pr_remediation import (
    ReviewerRuntimeAttachmentFactory,
)
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_task,
    require_task_pull_request_head,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "PlanningReviewDecision",
    "PrMergeGateChangeRequestDecision",
    "PrReviewDecision",
    "PrReviewGateResolutionDecision",
    "TaskPrMergeGateChangeRequestOutcome",
    "TaskPrReviewDecisionOutcome",
    "TaskPrReviewGateResolutionOutcome",
    "retire_observed_pr_merge_decision_with_changes",
    "settle_observed_pr_review",
    "settle_observed_pr_review_resolution",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_review_decision"

logger = logging.getLogger(__name__)

_OPENORC_ACTOR = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)

_PROTOCOL_REVIEW_OUTCOMES = {
    REVIEW_OUTCOME_ACCEPTED: ReviewOutcome.ACCEPTED,
    REVIEW_OUTCOME_CHANGES_REQUESTED: ReviewOutcome.CHANGES_REQUESTED,
}

_REVIEWER_ROLE = WorkflowRole.REVIEWER

_STICKY_PR_GATE_TYPES: Final[tuple[OwnerGateType, ...]] = (
    OwnerGateType.REVIEW_RESOLUTION,
    OwnerGateType.MERGE_DECISION,
)


class PrReviewDecision(StrEnum):
    """The classified durable next decision of one ordinary PR review."""

    REMEDIATION_ROUTED = "remediation_routed"
    RESOLUTION_INSTALLED = "resolution_installed"
    ACCEPTED_MERGE_DECISION = "accepted_merge_decision"


class PrReviewGateResolutionDecision(StrEnum):
    """The classified durable next decision of one PR/head resolution settlement."""

    SETTLED_ACCEPTED = "settled_accepted"
    CONTINUATION_ROUTED = "continuation_routed"


class PrMergeGateChangeRequestDecision(StrEnum):
    """The classified durable outcome of one actionable merge-gate change request."""

    MERGE_GATE_RETIRED = "merge_gate_retired"


# Historical alias kept for symmetry with the planning enum name.
PlanningReviewDecision = PrReviewDecision


@dataclass(frozen=True, slots=True)
class TaskPrReviewDecisionOutcome:
    """The safe durable result of one ordinary PR review decision.

    ``decision`` classifies the committed next decision. ``remediation`` is
    the outcome of the routed Producer remediation round (carrying the
    re-entered dispatch for the confirmed new head) when
    ``REMEDIATION_ROUTED``; ``gate`` is the newly installed exact-head
    ``MERGE_DECISION`` or ``REVIEW_RESOLUTION`` gate otherwise.
    """

    decision: PrReviewDecision
    review_iteration: ReviewIteration
    review_loop: ReviewLoop
    task: Task
    pull_request: TaskPullRequest
    gate: OwnerGate | None
    remediation: object | None


@dataclass(frozen=True, slots=True)
class TaskPrReviewGateResolutionOutcome:
    """The safe durable result of one PR/head resolution settlement.

    ``merge_decision_gate`` is the newly installed exact-head MERGE_DECISION
    after a formal acceptance; ``remediation`` is the routed Producer
    remediation round after a formal change request.
    """

    decision: PrReviewGateResolutionDecision
    review_iteration: ReviewIteration
    review_loop: ReviewLoop
    task: Task
    pull_request: TaskPullRequest
    settled_gate: OwnerGate
    merge_decision_gate: OwnerGate | None
    remediation: object | None


@dataclass(frozen=True, slots=True)
class TaskPrMergeGateChangeRequestOutcome:
    """The safe durable result of one actionable merge-gate change request.

    The historical MERGE_DECISION gate remains pending history — never Owner
    stamped, never Reviewer SETTLED, never a false gate-resolution event —
    while the Task returns to ``REVIEWING`` under the rotated token and the
    remediation round is routed outside the transaction.
    """

    decision: PrMergeGateChangeRequestDecision
    review_iteration: ReviewIteration
    review_loop: ReviewLoop
    task: Task
    pull_request: TaskPullRequest
    retired_gate: OwnerGate
    remediation: object | None


def _require_producer_runtime(value: object) -> AgentRuntimeAdapter:
    if not callable(getattr(value, "send", None)) or not callable(
        getattr(value, "realize_control", None)
    ):
        raise InvalidCommandError("producer_runtime must be an AgentRuntimeAdapter")
    return cast(AgentRuntimeAdapter, value)


def _require_github(value: object) -> GitHubAppClient:
    if not callable(getattr(value, "get_installation_repository", None)) or not callable(
        getattr(value, "get_repository_pull_request", None)
    ):
        raise InvalidCommandError("github must be a GitHubAppClient")
    return cast(GitHubAppClient, value)


def _require_reviewer_factory(
    value: object,
) -> ReviewerRuntimeAttachmentFactory:
    if not callable(value):
        raise InvalidCommandError(
            "reviewer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Reviewer binding"
        )
    return cast(ReviewerRuntimeAttachmentFactory, value)


# --- command validation and shared subject helpers -----------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_formal_result(value: object) -> ReviewResult:
    if not isinstance(value, ReviewResult):
        raise InvalidCommandError("review_result must be the validated typed ReviewResult")
    return value


def _resolution_outcome(result: ReviewResult) -> ReviewOutcome:
    outcome = _PROTOCOL_REVIEW_OUTCOMES.get(result.outcome)
    if outcome is None:
        raise InvalidCommandError(
            "the review result carries an outcome outside the settled reviewer vocabulary"
        )
    return outcome


def _finding_documents(result: ReviewResult) -> list[dict[str, str]]:
    """The typed findings as their canonical JSON documents, order preserved."""
    return [{"summary": finding.summary, "details": finding.details} for finding in result.findings]


def _reviewer_actor(reviewer: TaskAgentSession) -> event_coordination.WorkflowActorContext:
    """The safe REVIEWER logical actor identity: the exact session UUID."""
    return event_coordination.WorkflowActorContext(WorkflowEventActor.REVIEWER, str(reviewer.id))


def _require_ready_reviewer(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Resolve the exact admitted READY Reviewer role/session binding."""
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; the exact admitted session "
            "only is used and never recreated or reinitialized"
        )
    if binding.external_session_id is None:
        raise ConflictError("the reviewer session binding has no external session identity")
    return binding


def _require_open_pr_loop(
    transaction_pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> ReviewLoop:
    """Resolve the Task's single OPEN ``PR_REVIEW`` loop.

    The PR loop must already exist for every decision seam: the dispatch
    establishes it once. Planning loops are never selected or consulted; a
    closed loop refuses every decision.
    """
    existing = [
        loop
        for loop in review_records.list_task_review_loops(transaction_pool, task_id=task_id)
        if loop.purpose is ReviewLoopPurpose.PR_REVIEW
    ]
    if len(existing) != 1:
        raise NotFoundError("the requested pr_review loop is not available for this task")
    loop = existing[0]
    if loop.status is not ReviewLoopStatus.OPEN:
        raise ConflictError(
            "the pr_review loop is not open; closed loops are never reopened or replaced"
        )
    return loop


def _pr_owner_mediated_history(
    transaction_pool: DatabasePool, *, task_id: UUID, task_pull_request_id: UUID
) -> bool:
    """Whether this canonical PR carries PR/head REVIEW_RESOLUTION/MERGE history.

    The deliberately PR-scoped stickiness derivation: any
    ``REVIEW_RESOLUTION`` or ``MERGE_DECISION`` gate bound to exactly this
    canonical TaskPullRequest (any head) makes every further disagreement
    immediately return to a fresh resolution gate. Planning gates and other
    subjects are never consulted, and no in-memory flag exists.
    """
    gates = gate_records.list_task_owner_gates(transaction_pool, task_id=task_id)
    return any(
        gate.gate_type in _STICKY_PR_GATE_TYPES
        and gate.task_pull_request_id == task_pull_request_id
        for gate in gates
    )


def _reserve_and_finalize_decision_iteration(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    loop: ReviewLoop,
    iterations: Sequence[ReviewIteration],
    task_pull_request_id: UUID,
    reviewed_head_sha: str,
    result: ReviewResult,
    outcome: ReviewOutcome,
) -> ReviewIteration:
    """Allocate and finalize the NEW settlement iteration atomically.

    The caller holds the loop row lock ``FOR UPDATE``; the next number is
    derived from the locked loop's persisted history, the unique per-loop
    numbering and the loop-OPEN creation guard are the durable backstops, and
    the one-shot conditional result recording finalizes the allocation in the
    same transaction.
    """
    next_number = max((iteration.iteration_number for iteration in iterations), default=0) + 1
    try:
        reserved = review_records.create_review_iteration(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=next_number,
            task_pull_request_id=task_pull_request_id,
            reviewed_head_sha=reviewed_head_sha,
        )
    except UniqueViolation as exc:
        raise StaleOperationError(
            "a competing review iteration for this loop committed first; the continuation is stale"
        ) from exc
    if reserved is None:
        raise StaleOperationError("the pr_review loop is no longer open; the continuation is stale")
    finalized = review_records.record_review_iteration_result(
        transaction_pool,
        review_iteration_id=reserved.id,
        outcome=outcome,
        summary=result.summary,
        findings=_finding_documents(result),
    )
    if finalized is None:
        raise StaleOperationError(
            "the continuation iteration is no longer unfinalized; the continuation is stale"
        )
    return finalized


# --- the ordinary decision transaction -----------------------------------------------


def _finalize_pr_decision_transaction(
    pool: DatabasePool,
    *,
    profile_id: UUID | None,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    review_result: ReviewResult,
    review_iteration_id: UUID,
    expected_reviewer_session_id: UUID,
    task_resume_intent_id: UUID | None = None,
) -> tuple:
    """Finalize the reserved PR attempt and commit the next decision atomically.

    Composition (database only): the accountable-Profile barriers exactly when
    the caller carries one (the blocking entries; the detached settlement's
    scope is the receipt's authenticated Workspace resolution instead, issue
    #338), the Task row locked ``FOR UPDATE``, the exact current ``REVIEWING``
    Task at the caller's token with no current OwnerGate, the exact reserved
    attempt re-derived from durable state — never from a process-local
    continuation object — the canonical PR at exactly the reserved attempt's
    reviewed head, the re-locked OPEN PR loop, the exact READY Reviewer
    binding, the one-shot finalization, the safe REVIEWER ``PR_REVIEWED``
    event, and the budget/stickiness-derived durable next decision — all in
    one transaction.
    """
    with composed_transaction(pool) as transaction_pool:
        if profile_id is not None:
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
        if task.status is not TaskStatus.REVIEWING:
            raise ConflictError(
                "the addressed task is not reviewing; the pr review decision is not available"
            )
        if task.current_owner_gate_id is not None:
            raise ConflictError(
                "the task carries a current owner gate; the ordinary pr review "
                "decision is not available"
            )
        # The exact reserved attempt is re-derived from durable state — never
        # from a process-local continuation object (issue #338): it must exist
        # in this scope, still be unfinalized, and be exactly one PR/head
        # iteration whose reviewed head and canonical PR subject this
        # composition proves under the lock.
        reserved = review_records.get_review_iteration(
            transaction_pool, review_iteration_id=review_iteration_id
        )
        if reserved is None or reserved.workspace_id != workspace_id or reserved.task_id != task_id:
            raise StaleOperationError(
                "the reserved review attempt is not available in this task; the decision is stale"
            )
        if reserved.outcome is not None:
            raise ConflictError(
                "the reserved review attempt is already finalized; a second verdict, "
                "gate, or event is never created"
            )
        if (
            reserved.plan_revision_id is not None
            or reserved.task_pull_request_id is None
            or reserved.reviewed_head_sha is None
        ):
            raise ConflictError(
                "the reserved review attempt is not a pr/head iteration; the decision is stale"
            )
        head_sha = reserved.reviewed_head_sha
        pull_request = require_task_pull_request_head(
            transaction_pool, task=task, expected_head_sha=head_sha
        )
        if pull_request.id != reserved.task_pull_request_id:
            raise StaleOperationError(
                "the canonical pull request identity changed since the dispatch; "
                "the decision is stale"
            )
        loop = review_records.get_review_loop(
            transaction_pool,
            review_loop_id=reserved.review_loop_id,
            for_update=True,
        )
        if loop is None or loop.id != reserved.review_loop_id:
            raise StaleOperationError("the pr_review loop has moved on; the decision is stale")
        if loop.status is not ReviewLoopStatus.OPEN:
            raise ConflictError("the pr_review loop is not open; the decision applies nothing")
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        if (
            next((iteration for iteration in iterations if iteration.id == reserved.id), None)
            is None
        ):
            raise StaleOperationError(
                "the reserved review attempt is not available in this loop; the decision is stale"
            )
        reviewer = _require_ready_reviewer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        if reviewer.id != expected_reviewer_session_id:
            raise StaleOperationError(
                "the reviewer session binding has moved on; the decision is stale"
            )
        outcome = _resolution_outcome(review_result)
        finalized = review_records.record_review_iteration_result(
            transaction_pool,
            review_iteration_id=reserved.id,
            outcome=outcome,
            summary=review_result.summary,
            findings=_finding_documents(review_result),
        )
        if task_resume_intent_id is not None:
            # The F8g atomic settlement seam (issue #292): the resumed turn's
            # result was durably processed by its original workflow owner in
            # THIS transaction, so the resume record settles or the whole
            # finalization rolls back together.
            settle_resumed_task_record(
                transaction_pool, task_resume_intent_id=task_resume_intent_id
            )
        if finalized is None:
            raise StaleOperationError(
                "the review attempt is no longer unfinalized; the decision is stale"
            )
        event_coordination.record_pr_reviewed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=finalized.id,
            actor=_reviewer_actor(reviewer),
            outcome=outcome,
        )
        # The budget derives from the loop's ordered finalized iteration
        # history, now including this finalization; the stored snapshot is
        # authoritative and the budget never resets.
        history = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        exhausted = autonomous_budget_exhausted(loop=loop, iterations=history)
        sticky = _pr_owner_mediated_history(
            transaction_pool, task_id=task_id, task_pull_request_id=pull_request.id
        )

        if outcome is ReviewOutcome.ACCEPTED:
            gate = gate_records.create_owner_gate(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                gate_type=OwnerGateType.MERGE_DECISION,
                subject_head_sha=head_sha,
                task_pull_request_id=pull_request.id,
            )
            installed = task_records.set_current_owner_gate(
                transaction_pool,
                task_id,
                expected_state_token=task.state_token,
                owner_gate_id=gate.id,
            )
            if installed is None:
                raise StaleOperationError(
                    "the merge decision gate could not be installed; the decision is stale"
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
            return (
                PrReviewDecision.ACCEPTED_MERGE_DECISION,
                finalized,
                loop,
                waiting,
                pull_request,
                gate,
                reviewer,
            )

        if outcome is ReviewOutcome.CHANGES_REQUESTED and not exhausted and not sticky:
            # Within-budget disagreement: the Task stays REVIEWING and the
            # OPEN loop continues; the finalized result routes to the Producer
            # remediation OUTSIDE this transaction.
            return (
                PrReviewDecision.REMEDIATION_ROUTED,
                finalized,
                loop,
                task,
                pull_request,
                None,
                reviewer,
            )

        # Exhausted or sticky disagreement: the exact PR/head resolution gate
        # is created and installed atomically and nothing routes to the
        # Producer from this decision.
        gate = gate_records.create_owner_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            gate_type=OwnerGateType.REVIEW_RESOLUTION,
            subject_head_sha=head_sha,
            task_pull_request_id=pull_request.id,
        )
        installed = task_records.set_current_owner_gate(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            owner_gate_id=gate.id,
        )
        if installed is None:
            raise StaleOperationError(
                "the resolution gate could not be installed; the decision is stale"
            )
        event_coordination.record_owner_gate_created_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=gate.id,
            actor=_OPENORC_ACTOR,
        )
        if not sticky:
            # First exhaustion only: the loop crosses its autonomous budget
            # exactly once, and the durable PR gate history is what
            # distinguishes that moment from every later disagreement.
            event_coordination.record_review_limit_reached_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                review_loop_id=loop.id,
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
        return (
            PrReviewDecision.RESOLUTION_INSTALLED,
            finalized,
            loop,
            waiting,
            pull_request,
            gate,
            reviewer,
        )


def settle_observed_pr_review(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    review_iteration_id: UUID,
    expected_reviewer_session_id: UUID,
    result: ReviewResult,
) -> TaskPrReviewDecisionOutcome | None:
    """Finalize one observed PR/head review through the owning service.

    The detached-run settlement's owning-service composition (issue #338):
    the correlation seam funnels the attributed terminal observation through
    the shared formal contract and hands the validated ``ReviewResult`` here
    inside its own settlement transaction. The settlement re-derives every
    subject from durable state — the exact current ``REVIEWING`` Task, the
    exact reserved unfinalized PR/head iteration still addressing the
    canonical pull request at its reviewed head in the still-OPEN PR loop,
    and the exact READY Reviewer binding the receipt anchored — and runs the
    one shared decision composition atomically: the one-shot conditional
    result recording, the persisted-state budget and sticky Owner-mediation
    derivation, the safe audit events, and the committed durable next
    decision (the exact-head ``MERGE_DECISION`` on acceptance — never a
    merge; the exact PR/head ``REVIEW_RESOLUTION`` on exhaustion or
    stickiness). No Producer turn is started here: the in-budget
    ``CHANGES_REQUESTED`` continuation is the ONE completion handoff's from
    committed state, never this seam's. A moved-on subject settles
    result-not-applied; a duplicate, late, or stale REVIEW can never decide
    twice.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(review_iteration_id, "review_iteration_id")
    _require_uuid_command(expected_reviewer_session_id, "expected_reviewer_session_id")
    validated = _require_formal_result(result)
    _resolution_outcome(validated)
    try:
        task = task_records.get_task(transaction_pool, task_id=task_id)
        if task is None or task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        (
            decision,
            finalized,
            loop,
            settled_task,
            pull_request,
            gate,
            _reviewer,
        ) = _finalize_pr_decision_transaction(
            transaction_pool,
            profile_id=None,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=task.state_token,
            review_result=validated,
            review_iteration_id=review_iteration_id,
            expected_reviewer_session_id=expected_reviewer_session_id,
        )
    except (NotFoundError, ConflictError, StaleOperationError):
        # The subject set moved on between the dispatch and the observed
        # completion (a rotated token, a lost session, a superseded attempt,
        # a moved-on loop): the validated result can no longer apply, so
        # nothing is rewritten and the receipt settles as result-not-applied.
        # Never a fabricated decision, never a rewrite of moved-on history.
        return None
    return TaskPrReviewDecisionOutcome(
        decision=decision,
        review_iteration=finalized,
        review_loop=loop,
        task=settled_task,
        pull_request=pull_request,
        gate=gate,
        remediation=None,
    )


# --- the PR/head resolution settlement (E4 formal continuation) -----------------------


def _settle_pr_resolution_transaction(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    result: ReviewResult,
    record_discussion_message_events: bool,
) -> tuple:
    """Settle the exact PR/head resolution gate and commit the next decision.

    Composition (database only): the account barrier, the exact Owner scope,
    the Task row locked ``FOR UPDATE``, the exact current
    ``waiting_for_owner`` Task at the caller's token, the exact current
    pending PR/head REVIEW_RESOLUTION gate (resolved from the Task's current
    pointer, never a caller-supplied id), the unchanged canonical PR at the
    gate's exact head, the OPEN PR loop, the exact admitted READY Reviewer
    binding, the NEW next-numbered finalized same-head iteration with the
    REVIEWER ``PR_REVIEWED`` audit, the purpose-independent ``SETTLED`` stamp
    with truthful Reviewer provenance, the cleared pointer, the rotated
    token, and — when routed through E4 — the two direction-only discussion
    events. A failed late event write rolls the whole settlement back.
    """
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
        if task.current_owner_gate_id is None:
            raise StaleOperationError(
                "the task carries no current owner gate; the settlement is stale"
            )
        gate = require_current_owner_gate(
            transaction_pool, task=task, owner_gate_id=task.current_owner_gate_id
        )
        if gate.gate_type is not OwnerGateType.REVIEW_RESOLUTION:
            raise ConflictError("only the current review_resolution gate settles through this seam")
        if (
            gate.plan_revision_id is not None
            or gate.subject_head_sha is None
            or gate.task_pull_request_id is None
        ):
            # The planning-subject form belongs to E3c's settlement; this seam
            # is exclusively the PR/head form.
            raise ConflictError(
                "the review_resolution gate does not carry the canonical "
                "pull-request/head subject form"
            )
        head_sha = gate.subject_head_sha
        pull_request = require_task_pull_request_head(
            transaction_pool, task=task, expected_head_sha=head_sha
        )
        if pull_request.id != gate.task_pull_request_id:
            raise StaleOperationError(
                "the resolution gate no longer addresses the task's canonical pull "
                "request; the settlement is stale"
            )
        loop = _require_open_pr_loop(transaction_pool, workspace_id=workspace_id, task_id=task_id)
        reviewer = _require_ready_reviewer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        outcome = _resolution_outcome(result)
        finalized = _reserve_and_finalize_decision_iteration(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            loop=loop,
            iterations=iterations,
            task_pull_request_id=pull_request.id,
            reviewed_head_sha=head_sha,
            result=result,
            outcome=outcome,
        )
        event_coordination.record_pr_reviewed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=finalized.id,
            actor=_reviewer_actor(reviewer),
            outcome=outcome,
        )
        settlement = gate_records.resolve_owner_gate(
            transaction_pool,
            owner_gate_id=gate.id,
            outcome=OwnerGateStatus.SETTLED,
            expected_task_state_token=task.state_token,
        )
        if settlement.outcome is not OwnerGateResolutionOutcome.RESOLVED:
            raise StaleOperationError(
                "the resolution gate could not be settled; the settlement is stale"
            )
        settled_task = settlement.task
        assert settled_task is not None
        event_coordination.record_owner_gate_resolved_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=gate.id,
            actor=_reviewer_actor(reviewer),
            outcome=OwnerGateStatus.SETTLED,
            review_iteration_id=finalized.id,
            review_outcome=outcome,
        )
        if record_discussion_message_events:
            event_coordination.record_owner_reviewer_discussion_message_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=gate.id,
                actor=event_coordination.owner_actor(profile_id),
                direction=event_coordination.OwnerReviewerDiscussionDirection.OWNER_TO_REVIEWER,
            )
            event_coordination.record_owner_reviewer_discussion_message_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=gate.id,
                actor=_reviewer_actor(reviewer),
                direction=event_coordination.OwnerReviewerDiscussionDirection.REVIEWER_TO_OWNER,
            )

        if outcome is ReviewOutcome.ACCEPTED:
            # A formal acceptance proceeds to a NEW ordinary exact-head
            # MERGE_DECISION for the still-current head. The loop stays OPEN,
            # the Task stays waiting_for_owner, and the pointer install
            # rotates the token.
            merge_gate = gate_records.create_owner_gate(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                gate_type=OwnerGateType.MERGE_DECISION,
                subject_head_sha=head_sha,
                task_pull_request_id=pull_request.id,
            )
            installed = task_records.set_current_owner_gate(
                transaction_pool,
                task_id,
                expected_state_token=settled_task.state_token,
                owner_gate_id=merge_gate.id,
            )
            if installed is None:
                raise StaleOperationError(
                    "the merge decision gate could not be installed; the settlement is stale"
                )
            event_coordination.record_owner_gate_created_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=merge_gate.id,
                actor=_OPENORC_ACTOR,
            )
            return (
                PrReviewGateResolutionDecision.SETTLED_ACCEPTED,
                finalized,
                loop,
                installed,
                pull_request,
                gate,
                merge_gate,
                reviewer,
            )

        # The PR disagreement continues: back to reviewing under the
        # post-settlement rotated token, with the loop left OPEN.
        reviewing = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=settled_task.state_token,
            status=TaskStatus.REVIEWING,
        )
        if reviewing is None:
            raise StaleOperationError(
                "the task could not return to reviewing; the settlement is stale"
            )
        return (
            PrReviewGateResolutionDecision.CONTINUATION_ROUTED,
            finalized,
            loop,
            reviewing,
            pull_request,
            gate,
            None,
            reviewer,
        )


def settle_observed_pr_review_resolution(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    result: ReviewResult,
    record_discussion_message_events: bool = True,
) -> tuple:
    """Settle the exact PR/head REVIEW_RESOLUTION gate inside the caller's transaction.

    The detached discussion settlement's entry point (issue #339): the exact
    durable settlement transaction of :func:`settle_observed_pr_review_resolution`,
    composed inside the caller's already-open transaction (the nested
    composition becomes a SAVEPOINT), so the gate's ``SETTLED`` stamp, the
    NEW finalized same-head iteration, the audit events, and the
    direction-only discussion audit commit atomically with the caller's
    receipt settlement. A formal ACCEPTED installs a NEW exact-head
    MERGE_DECISION inside the same transaction; a formal CHANGES_REQUESTED
    returns the Task to ``REVIEWING`` with no Producer routing here. The
    blocking wrapper's external consequences belong to this seam's own
    post-commit continuation — the detached completion handoff re-derives the
    Producer remediation dispatch from committed state — so no runtime or
    GitHub dependency is proven here and nothing external is invoked.
    """
    return _settle_pr_resolution_transaction(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        result=result,
        record_discussion_message_events=record_discussion_message_events,
    )


def retire_observed_pr_merge_decision_with_changes(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    result: ReviewResult,
    record_discussion_message_events: bool = True,
) -> tuple:
    """Retire the pending PR/head MERGE_DECISION gate inside the caller's transaction.

    The detached discussion settlement's entry point (issue #339): the exact
    durable retirement transaction of
    :func:`retire_observed_pr_merge_decision_with_changes`, composed inside the
    caller's already-open transaction (the nested composition becomes a
    SAVEPOINT), so the NEW finalized same-head iteration, the audit events,
    the guarded gate supersession, the pointer clear, and the token rotation
    commit atomically with the caller's receipt settlement. The blocking
    wrapper's external consequences belong to this seam's own post-commit
    continuation — the detached completion handoff re-derives the Producer
    remediation dispatch from committed state — so no runtime or GitHub
    dependency is proven here and nothing external is invoked.
    """
    return _retire_pr_merge_decision_transaction(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        result=result,
        record_discussion_message_events=record_discussion_message_events,
    )


# --- the merge-gate change-request retirement (E4 formal continuation) ----------------


def _retire_pr_merge_decision_transaction(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    result: ReviewResult,
    record_discussion_message_events: bool,
) -> tuple:
    """Append the actionable iteration and retire the pending merge gate.

    Composition (database only): the account barrier, the exact Owner scope,
    the Task row locked ``FOR UPDATE``, the exact current
    ``waiting_for_owner`` Task at the caller's token, the exact current
    pending PR/head MERGE_DECISION gate (resolved from the Task's current
    pointer), the unchanged canonical PR at the gate's exact head, the OPEN
    PR loop, the exact admitted READY Reviewer binding, the NEW next-numbered
    finalized same-head iteration with the REVIEWER ``PR_REVIEWED`` audit,
    the guarded ``reviewing``-destination supersession (the gate row stays
    pending history — no Owner stamp, no SETTLED, no gate-resolution event,
    no intervening REVIEW_RESOLUTION), the pointer clear, the token rotation,
    and — when routed through E4 — the two direction-only discussion events.
    """
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
        if task.current_owner_gate_id is None:
            raise StaleOperationError(
                "the task carries no current owner gate; the retirement is stale"
            )
        gate = require_current_owner_gate(
            transaction_pool, task=task, owner_gate_id=task.current_owner_gate_id
        )
        if gate.gate_type is not OwnerGateType.MERGE_DECISION:
            raise ConflictError("only the current merge_decision gate retires through this seam")
        if (
            gate.plan_revision_id is not None
            or gate.subject_head_sha is None
            or gate.task_pull_request_id is None
        ):
            raise ConflictError(
                "the merge_decision gate does not carry the canonical "
                "pull-request/head subject form"
            )
        head_sha = gate.subject_head_sha
        pull_request = require_task_pull_request_head(
            transaction_pool, task=task, expected_head_sha=head_sha
        )
        if pull_request.id != gate.task_pull_request_id:
            raise StaleOperationError(
                "the merge gate no longer addresses the task's canonical pull request; "
                "the retirement is stale"
            )
        loop = _require_open_pr_loop(transaction_pool, workspace_id=workspace_id, task_id=task_id)
        reviewer = _require_ready_reviewer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        outcome = _resolution_outcome(result)
        finalized = _reserve_and_finalize_decision_iteration(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            loop=loop,
            iterations=iterations,
            task_pull_request_id=pull_request.id,
            reviewed_head_sha=head_sha,
            result=result,
            outcome=outcome,
        )
        event_coordination.record_pr_reviewed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=finalized.id,
            actor=_reviewer_actor(reviewer),
            outcome=outcome,
        )
        supersession = gate_records.supersede_current_owner_gate(
            transaction_pool,
            owner_gate_id=gate.id,
            expected_task_state_token=task.state_token,
            destination=OwnerGateSupersessionDestination.REVIEWING,
        )
        if supersession.outcome is not OwnerGateSupersessionOutcome.SUPERSEDED:
            raise StaleOperationError(
                "the merge gate could not be retired; the retirement is stale"
            )
        superseded_task = supersession.task
        assert superseded_task is not None
        if record_discussion_message_events:
            event_coordination.record_owner_reviewer_discussion_message_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=gate.id,
                actor=event_coordination.owner_actor(profile_id),
                direction=event_coordination.OwnerReviewerDiscussionDirection.OWNER_TO_REVIEWER,
            )
            event_coordination.record_owner_reviewer_discussion_message_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=gate.id,
                actor=_reviewer_actor(reviewer),
                direction=event_coordination.OwnerReviewerDiscussionDirection.REVIEWER_TO_OWNER,
            )
        return finalized, loop, superseded_task, pull_request, gate, reviewer
