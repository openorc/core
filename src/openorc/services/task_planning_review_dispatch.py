"""E3a: dispatch the exact-plan REVIEW evaluation to the admitted Reviewer (issue #209).

E3a is the runtime-facing, exact-plan REVIEW attempt seam the planning
decision loop (E3b) consumes: given E2's actual merged return contract
(:class:`openorc.services.task_plan_production.TaskPlanProductionOutcome` —
the persisted current PlanRevision and the post-pointer-update Task carrying
its rotated ``state_token``), it reserves exactly one next-numbered
unfinalized ReviewIteration for that exact plan and sends ONE REVIEW
interaction to the exact admitted READY Reviewer session, returning the
reserved attempt and the validated typed ``ReviewResult`` as an internal
continuation input for E3b.

Scope discipline (Phase 2E): OpenOrc coordinates messages, validated JSON,
persistence, authority, and safety; the agents make technical judgments.
This seam never assesses plan semantics, never finalizes a verdict, never
records a ReviewIteration result, and never decides budget or exhaustion:
E3b must record the ReviewIteration/result and its workflow consequence in
one transaction, so E3a deliberately performs no finalization and no Task
mutation (the Task token is consumed, never rotated, here).
REVIEW_RESOLUTION settlement, Owner discussion, and generalized durable
dispatch/replay are later-layer work (#85/#87–#90).

Phase composition (one operation):

1. entry validation + atomic reservation — one short database-only
   transaction: the account operational barrier, exact Owner Workspace/Task
   scope, the Task row locked ``FOR UPDATE`` (serializing concurrent REVIEW
   dispatches for the same Task through existing Task persistence — no new
   lock, index, or queue machinery), the exact current ``planning`` Task
   under E2's post-update ``state_token`` (never archived), the supplied
   PlanRevision proven to be exactly the Task's current revision and
   content-identical to its persisted row (a stale or foreign plan refuses
   dispatch), the exact admitted READY Reviewer role/session resolved from
   durable state (never rebound, recreated, or reinitialized), the durable
   Repository/issue identity, and the loop/iteration reservation:

   - on first review, establish exactly one OPEN ``PLANNING``-purpose
     ReviewLoop for the Task attempt, snapshotting the Workspace's current
     positive ``review_iteration_limit`` (the v1 default of 5 flows through
     the Workspace setting boundary); subsequent plans and Owner-mediated
     continuation reuse the same loop, so the stored budget survives later
     Workspace setting edits. No additional loop table or index;
   - with the loop row locked ``FOR UPDATE``, refuse another ordinary REVIEW
     while an earlier attempt in the loop is still unresolved (an
     unfinalized iteration is never bypassed, superseded, or rewritten);
   - then atomically reserve exactly one next-numbered unfinalized
     ReviewIteration for the exact current PlanRevision — the unique
     per-loop numbering and the loop-OPEN guard are the durable backstops,
     and no PR-head fields exist.

2. the ONE interaction send — the established #129 ``ReviewInteraction``
   with an exact ``PlanReviewSubject`` (the persisted revision id, the
   durable repository address, the Task's issue number, and the persisted
   plan content verbatim) is sent once to the exact Reviewer session
   through the #67 ``realize_control(..., expected_family=REVIEW_RESULT_FAMILY)``
   funnel with NO database transaction open, carrying a deterministically
   composed model-visible message of the same subject facts. There is no
   separate REVIEW Markdown, no semantic rubric, no protocol instruction
   prose, and no Cline-specific path; the message is composed inline by
   this service (no new prompt template or control asset). External sends
   occur outside short database transactions by construction: the exact
   Task/token/current-plan/session checks above happen before dispatch and
   are repeated by the E3b decision finalization;

3. the internal continuation input — the reserved exact-subject attempt
   (loop, unfinalized iteration) plus the typed ``ReviewResult`` and the
   resolved READY Reviewer binding. A known failure or an uncertain runtime
   outcome raises a typed application error and leaves the reserved attempt
   unresolved: no verdict is inferred, no replacement attempt is created,
   and nothing is blindly resent. Only a schema-valid, exact-subject typed
   ``ReviewResult`` is ever returned to E3b.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.ownership import Repository
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_repository, get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.interaction import PlanReviewSubject, ReviewInteraction
from openorc.protocol.models import REVIEW_RESULT_FAMILY, ReviewResult
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_subject_guards import (
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "TaskPlanningReviewDispatchOutcome",
    "dispatch_planning_review",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_planning_review_dispatch"
_DISPATCH_SPAN_NAME = "task_planning_review_dispatch.dispatch_planning_review"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TaskPlanningReviewDispatchOutcome:
    """The internal continuation input E3b's decision finalization consumes.

    ``review_loop``/``review_iteration`` are the exact reserved review
    attempt: the Task's OPEN planning loop and the next-numbered
    unfinalized iteration bound to the exact reviewed PlanRevision.
    ``review_result`` is the validated typed ``ReviewResult`` the Reviewer
    returned — it is input for E3b, never an authoritative verdict: E3b
    records the iteration result and its workflow consequence in one
    transaction. ``reviewer_session`` is the exact durable READY binding
    whose external session identity received the REVIEW, so E3b can recheck
    the same binding.
    """

    review_loop: ReviewLoop
    review_iteration: ReviewIteration
    review_result: ReviewResult
    reviewer_session: TaskAgentSession


# --- command validation -----------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_runtime(value: object) -> AgentRuntimeAdapter:
    if not isinstance(value, AgentRuntimeAdapter):
        raise InvalidCommandError("runtime must be an AgentRuntimeAdapter")
    return value


def _require_plan_production(value: object) -> TaskPlanProductionOutcome:
    """Require the exact E2 handoff, not coarse planning facts.

    Coarse ``PLANNING`` state, READY sessions, or a bare revision UUID are
    never preparation proof: only the outcome object the successful E2
    plan-production operation returned — carrying the persisted current
    PlanRevision and the post-update Task with its rotated ``state_token`` —
    authorizes the exact-plan REVIEW dispatch.
    """
    if not isinstance(value, TaskPlanProductionOutcome):
        raise InvalidCommandError(
            "plan_production must be the successful TaskPlanProductionOutcome "
            "returned by plan production"
        )
    return value


# --- entry subject/session guards -------------------------------------------


def _require_production_subject(task: Task, production: TaskPlanProductionOutcome) -> None:
    """Prove the supplied E2 outcome addresses exactly this Task/revision.

    The continuation input must name the addressed Task's own identity and
    a revision belonging to the same Task and Workspace. A mismatched
    outcome is a malformed continuation, not a subject to dispatch against.
    """
    if production.task.id != task.id:
        raise InvalidCommandError("the supplied plan-production outcome addresses a different task")
    revision = production.plan_revision
    if revision.task_id != task.id or revision.workspace_id != task.workspace_id:
        raise InvalidCommandError(
            "the supplied plan-production outcome revision does not belong to the addressed task"
        )


def _require_persisted_revision_coherence(persisted: PlanRevision, supplied: PlanRevision) -> None:
    """Fail closed when the supplied revision is not the persisted current one.

    ``require_current_plan_revision`` already proved the durable row is the
    Task's exact current revision; this coherence check rejects a caller
    that substituted a different revision object under the same identity.
    """
    if persisted.content != supplied.content:
        raise ConflictError(
            "the supplied plan-production outcome does not match the persisted "
            "current plan revision; the continuation input is not the persisted plan"
        )


def _require_ready_reviewer(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Resolve the exact admitted READY Reviewer role/session binding.

    Review dispatch reuses the original admitted Reviewer binding only: a
    missing, cross-Workspace, or cross-Task binding is the uniform
    not-found outcome; a CONNECTING/LOST/ENDED binding is a terminal or
    not-yet-established conflict. No rebinding, second session, or fresh
    initialization ever happens here — session loss is a blocking/recovery
    condition, never a silent replacement.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; review dispatch uses the "
            "exact admitted session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None:
        raise ConflictError("the reviewer session binding has no external session identity")
    return binding


def _require_task_repository(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> Repository:
    """Resolve the Task's durable Repository record within the Workspace.

    The durable reconciled record supplies the authoritative stable
    repository/issue identity for the REVIEW subject; mutable presentation
    metadata is never sole identity, and a missing or cross-Workspace
    repository is the uniform not-found outcome.
    """
    repository = get_repository(pool, repository_id)
    if repository is None or repository.workspace_id != workspace_id:
        raise NotFoundError("the requested repository is not available in this workspace")
    return repository


# --- loop establishment and iteration reservation ----------------------------


def _require_or_establish_planning_loop(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    iteration_limit: int,
) -> ReviewLoop:
    """Resolve the Task attempt's single OPEN planning ReviewLoop.

    On first review the loop is established as OPEN with the Workspace's
    current positive ``review_iteration_limit`` snapshot (the v1 default
    flows through the Workspace setting boundary; the stored budget is
    preserved for the loop's lifetime regardless of later Workspace edits).
    Subsequent plans and Owner-mediated continuation reuse the same loop.
    More than one planning loop for one Task is an invariant violation, and
    an already-closed planning loop while the Task is still planning is a
    fail-closed conflict — neither is ever silently repaired.
    """
    task_loops = review_records.list_task_review_loops(pool, task_id=task_id)
    planning_loops = [loop for loop in task_loops if loop.purpose is ReviewLoopPurpose.PLANNING]
    if len(planning_loops) > 1:
        raise ConflictError(
            "this task has more than one planning review loop; the one-loop "
            "invariant is violated and the state is never repaired implicitly"
        )
    if planning_loops:
        loop = planning_loops[0]
        if loop.status is not ReviewLoopStatus.OPEN:
            raise ConflictError(
                "this task's planning review loop has already closed; reopening "
                "or replacing it is later-phase review lifecycle machinery"
            )
        return loop
    return review_records.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=iteration_limit,
    )


def _require_no_unresolved_attempt(iterations: Sequence[ReviewIteration]) -> None:
    """Refuse another ordinary REVIEW while an earlier attempt is unresolved.

    Any unfinalized iteration in the loop is an earlier REVIEW attempt
    whose outcome is unknown-or-unrecorded. Dispatching a second REVIEW
    would bypass it (and the E3b finalization that must settle it), so the
    dispatch is refused; the unresolved attempt is never superseded,
    rewritten, or silently replaced.
    """
    for iteration in iterations:
        if iteration.outcome is None:
            raise ConflictError(
                "an earlier REVIEW attempt in this planning loop is still "
                "unresolved; a new REVIEW cannot bypass it and the attempt "
                "is never superseded, rewritten, or replaced"
            )


def _reserve_iteration(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    loop: ReviewLoop,
    iterations: Sequence[ReviewIteration],
    plan_revision_id: UUID,
) -> ReviewIteration:
    """Atomically reserve one next-numbered unfinalized iteration.

    The caller holds the loop row lock ``FOR UPDATE`` in its transaction,
    so concurrent dispatches serialize before numbering; the unique
    per-loop ``iteration_number`` constraint and the loop-OPEN creation
    guard are the durable backstops. A competing committed reservation is
    a stale operation — never a second attempt to bypass the first.
    """
    next_number = (
        max(
            (iteration.iteration_number for iteration in iterations),
            default=0,
        )
        + 1
    )
    try:
        reserved = review_records.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=next_number,
            plan_revision_id=plan_revision_id,
        )
    except UniqueViolation as exc:
        raise StaleOperationError(
            "a competing review iteration for this loop committed first; the dispatch is stale"
        ) from exc
    if reserved is None:
        # The loop-OPEN creation guard rejected the write. The caller holds
        # the loop lock, so this is a concurrent close racing the dispatch:
        # fail closed as a stale operation.
        raise StaleOperationError(
            "the planning review loop is no longer open at reservation time; the dispatch is stale"
        )
    return reserved


# --- the ONE REVIEW send -----------------------------------------------------


def _compose_review_message(
    *,
    repository_full_name: str,
    issue_number: int,
    revision: PlanRevision,
) -> str:
    """The model-visible REVIEW message: the persisted subject, structurally.

    The message carries exactly the authoritative Repository/issue identity
    and the persisted PlanRevision content verbatim — the same facts the
    typed ``PlanReviewSubject`` carries, so message and subject agree — with
    no rubric, no scoring or remediation prose, no protocol instruction
    text, and no runtime-specific path. It is composed inline by this
    service: no new prompt template and no control asset exist for REVIEW.
    """
    return (
        f"Plan review for {repository_full_name} issue #{issue_number}.\n"
        f"Review plan revision {revision.revision_number} "
        f"(plan_revision_id {revision.id}).\n\n"
        f"{revision.content}"
    )


def _realize_review_control(
    runtime: AgentRuntimeAdapter,
    session_id: str,
    *,
    interaction: ReviewInteraction,
    message: str,
) -> ReviewResult:
    """Send ONE established REVIEW interaction to the exact Reviewer session.

    The #67 shared funnel performs the formal validation; the service never
    adds an E-specific parser, inspects review semantics, or manages runtime
    modes. Known failure and uncertain outcome stay distinct, and neither is
    ever retried or resent here: an uncertain send leaves the reserved
    attempt unresolved for the E3b finalization to reconcile.
    """
    try:
        outcome = runtime.realize_control(
            session_id, interaction, message=message, expected_family=REVIEW_RESULT_FAMILY
        )
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "planning review dispatch ended with an uncertain runtime outcome; "
            "the reserved review attempt remains unresolved"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the agent runtime review interaction is unknown; "
            "the reserved review attempt remains unresolved"
        ) from error
    except AgentRuntimeError as error:
        raise ExternalOperationFailedError(
            "the agent runtime review interaction failed; the reserved review "
            "attempt remains unresolved"
        ) from error
    if not isinstance(outcome, ReviewResult):
        # A formal send with an expected family returns a typed formal
        # result or raises; anything else is a known non-answer.
        raise ExternalOperationFailedError(
            "the agent runtime did not return a typed review result; the "
            "reserved review attempt remains unresolved"
        )
    return outcome


# --- the dispatch operation --------------------------------------------------


def dispatch_planning_review(
    pool: DatabasePool,
    runtime: AgentRuntimeAdapter,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    plan_production: TaskPlanProductionOutcome,
) -> TaskPlanningReviewDispatchOutcome:
    """Reserve one exact-plan REVIEW attempt and send it to the Reviewer.

    Consumes the exact successful E2 plan-production outcome
    (:class:`TaskPlanProductionOutcome`): the post-update Task token
    authorizes the continuation, the persisted current PlanRevision is the
    exact REVIEW subject, and the exact admitted READY Reviewer binding is
    the only dispatch target. On first review the OPEN planning ReviewLoop
    is established with the Workspace's current budget snapshot; the
    next-numbered unfinalized ReviewIteration is reserved atomically before
    the external send, so a lost or uncertain send leaves a durable,
    unresolved attempt instead of a silent hole. The typed ``ReviewResult``
    is returned as E3b's continuation input — never finalized here.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        runtime_adapter = _require_runtime(runtime)
        production = _require_plan_production(plan_production)
        annotate_span(
            span,
            operation=_DISPATCH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: entry validation + atomic reservation (database only). ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            # Serialize concurrent REVIEW dispatches for this Task on the
            # Task row before any currentness decision (existing Task
            # persistence; no new lock, index, or queue machinery).
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=production.task.state_token,
            )
            if task.status is not TaskStatus.PLANNING:
                raise ConflictError("the addressed task is not in its coarse planning state")
            _require_production_subject(task, production)
            persisted_revision = require_current_plan_revision(
                transaction_pool, task=task, plan_revision_id=production.plan_revision.id
            )
            _require_persisted_revision_coherence(persisted_revision, production.plan_revision)
            reviewer = _require_ready_reviewer(
                transaction_pool, workspace_id=workspace_id, task_id=task_id
            )
            workspace = get_workspace(transaction_pool, workspace_id)
            if workspace is None:
                raise NotFoundError("the requested workspace is not available")

            workspace = get_workspace(transaction_pool, workspace_id)
            if workspace is None:
                raise NotFoundError("the requested workspace is not available")
            repository = _require_task_repository(
                transaction_pool,
                workspace_id=workspace_id,
                repository_id=task.repository_id,
            )
            loop = _require_or_establish_planning_loop(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                iteration_limit=workspace.review_iteration_limit,
            )
            locked_loop = review_records.get_review_loop(
                transaction_pool, review_loop_id=loop.id, for_update=True
            )
            if locked_loop is None or locked_loop.id != loop.id:
                raise StaleOperationError(
                    "the planning review loop has moved on; the dispatch is stale"
                )
            iterations = review_records.list_review_loop_iterations(
                transaction_pool, review_loop_id=loop.id
            )
            _require_no_unresolved_attempt(iterations)
            reserved = _reserve_iteration(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                loop=loop,
                iterations=iterations,
                plan_revision_id=persisted_revision.id,
            )

        # --- Phase 2: the ONE REVIEW send, with NO database transaction open. ---
        repository_full_name = f"{repository.metadata.owner_login}/{repository.metadata.name}"
        subject = PlanReviewSubject(
            plan_revision_id=str(persisted_revision.id),
            repository_full_name=repository_full_name,
            issue_number=task.github_issue_number,
            plan=persisted_revision.content,
        )
        interaction = ReviewInteraction(subject=subject)
        message = _compose_review_message(
            repository_full_name=repository_full_name,
            issue_number=task.github_issue_number,
            revision=persisted_revision,
        )
        review_result = _realize_review_control(
            runtime_adapter,
            reviewer.external_session_id or "",
            interaction=interaction,
            message=message,
        )

        # --- Phase 3: the internal continuation input for E3b. ---
        # No verdict is recorded and no workflow consequence is applied here:
        # E3b records the iteration result and its consequence in one
        # transaction, rechecking the exact Task/token/current-plan/session
        # subject this dispatch proved.
        return TaskPlanningReviewDispatchOutcome(
            review_loop=locked_loop,
            review_iteration=reserved,
            review_result=review_result,
            reviewer_session=reviewer,
        )
