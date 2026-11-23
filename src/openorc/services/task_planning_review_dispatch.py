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
     unfinalized iteration is never bypassed, superseded, or rewritten),
     and refuse another ordinary REVIEW of a plan revision the loop has
     already reviewed at all (issue #210): only E2's newly persisted next
     revision and rotated token may launch the next ordinary REVIEW, so a
     replayed still-current production outcome can never re-review the
     same plan while revision production is pending — E3c's formal
     Owner-mediated evaluations are a separate later seam and are exempt;
   - then atomically reserve exactly one next-numbered unfinalized
     ReviewIteration for the exact current PlanRevision — the unique
     per-loop numbering and the loop-OPEN guard are the durable backstops,
     and no PR-head fields exist.

2. the deferred Reviewer attachment (issue #268) — strictly after the
   reservation commit and the invocation's attempt-evidence recording, the
   exact admitted READY Reviewer attachment is acquired through the
   caller-supplied deferred factory bound to the Phase-1 resolved session
   identity. An attachment failure is therefore attributable to the exact
   reserved ReviewIteration and reaches the Level 2 operational-failure
   handoff with its own identity, instead of stranding the planning Task
   without a disposition. Immediately after acquisition, one short
   database-only transaction re-proves the exact reserved subjects under
   the Task row ``FOR UPDATE`` — the exact current ``planning`` Task at the
   expected token with no current OwnerGate, the exact current PlanRevision,
   and the exact READY Reviewer binding; any moved subject refuses the
   dispatch as a never-dispatched dead letter bound to the reserved
   subjects, leaving the reserved iteration unfinalized for recovery. The
   transaction closes before the send; no database transaction spans the
   attachment or the send;

3. the ONE interaction send — the established #129 ``ReviewInteraction``
   with an exact ``PlanReviewSubject`` (the persisted revision id, the
   durable repository address, the Task's issue number, and the persisted
   plan content verbatim) is sent once to the exact Reviewer session
   through the #67 ``realize_control(..., expected_family=REVIEW_RESULT_FAMILY)``
   funnel with NO database transaction open, carrying a deterministically
   composed model-visible message of the same subject facts. There is no
   separate REVIEW Markdown, no semantic rubric, no protocol instruction
   prose, and no Cline-specific path; the message is composed inline by
   this service (no new prompt template or control asset). The exact
   Task/token/current-plan/session checks are proven in the reservation
   transaction, re-proven immediately before the send, and repeated by the
   E3b decision finalization;

4. the internal continuation input — the reserved exact-subject attempt
   (loop, unfinalized iteration) plus the typed ``ReviewResult`` and the
   resolved READY Reviewer binding. A known failure, an attachment failure,
   or an uncertain runtime outcome raises a typed application error and
   leaves the reserved attempt unresolved: no verdict is inferred, no
   replacement attempt is created, and nothing is blindly resent. Only a
   schema-valid, exact-subject typed ``ReviewResult`` is ever returned to
   E3b.

Detached regular-mode dispatch (issue #336): the same Phase-1 reservation
(:func:`_reserve_review_attempt`, shared with the blocking entry) commits
the reserved attempt, and the ONE detached-turn receipt companion of that
exact iteration — operation ``reviewer_review``, attempt kind
``review_iteration``, expecting the canonical ``review_result`` family —
commits in one short transaction strictly before the positive observation
registration and the verbatim admission of the composed canonical REVIEW
message on the exact Reviewer session. No ``send`` and no blocking wait
exists in this path, and the dispatching worker exits after the
acknowledgement attach. The independently observed completion routes
through the correlation seam to the owning E3b decision settlement;
proven pre-effect failures refuse the receipt and report through the
existing Level 1/2 machinery attributable to the reserved iteration, and
an uncertain admission leaves the receipt conservatively claimed and is
never re-dispatched.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.ownership import Repository
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
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
from openorc.persistence.detached_turns import refuse_task_detached_turn_receipt
from openorc.persistence.ownership import get_repository, get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.models import REVIEW_RESULT_FAMILY, ReviewResult
from openorc.services import task_detached_turn_correlation as turn_correlation
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_installations import require_observed_repository_metadata
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_detached_turn_observation import ObservationRegistrationRefusedError
from openorc.services.task_plan_production import (
    PlanProductionObservationRegistrar,
    TaskPlanProductionOutcome,
)
from openorc.services.task_subject_guards import (
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "DispatchedPlanningReview",
    "ReviewerRuntimeAttachmentFactory",
    "TaskPlanningReviewDispatchOutcome",
    "dispatch_planning_review_detached",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_planning_review_dispatch"
_DISPATCH_DETACHED_SPAN_NAME = "task_planning_review_dispatch.dispatch_planning_review_detached"

# Safe refusal-kind tokens for a receipt's durable refusal outcome after a
# proven pre-effect detached-dispatch failure (no exception text or error
# content ever enters durable state).
_REGISTRATION_REFUSAL_KIND = "observation_registration_refused"
_ADMISSION_REFUSAL_KIND = "admission_refused"

logger = logging.getLogger(__name__)

# The deferred Reviewer attachment seam (F5e, issue #268): the service decides
# WHEN the adapter is acquired — strictly after the Phase-1 reservation
# transaction has committed, so an attachment failure is attributable to the
# exact reserved ReviewIteration — and the caller owns HOW the production
# attachment is composed (the #190 BOUND ``attach_cline_runtime`` composition
# bound to the exact resolved Reviewer session identity).
ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]


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


def _require_reviewer_factory(value: object) -> ReviewerRuntimeAttachmentFactory:
    if not callable(value):
        raise InvalidCommandError(
            "reviewer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Reviewer binding"
        )
    return cast(ReviewerRuntimeAttachmentFactory, value)


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
    repository is the uniform not-found outcome. A claim-state repository
    (issue #372 Owner onboarding, no trusted GitHub observation yet) fails
    closed as a conflict instead of supplying unverified claim data.
    """
    repository = get_repository(pool, repository_id)
    if repository is None or repository.workspace_id != workspace_id:
        raise NotFoundError("the requested repository is not available in this workspace")
    require_observed_repository_metadata(repository)
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


def _require_no_iteration_for_current_revision(
    iterations: Sequence[ReviewIteration], revision: PlanRevision
) -> None:
    """Refuse another ordinary REVIEW of a revision the loop already reviewed.

    The exact current PlanRevision may be dispatched for ordinary REVIEW
    only while no iteration in the loop binds it yet (issue #210): after a
    finalization, only E2's newly persisted next revision and rotated
    token may launch the next ordinary REVIEW. A replayed production
    outcome whose token is still current can therefore never reserve a
    second REVIEW of the same plan while revision production is pending —
    which also protects an in-flight E2 operation from being superseded by
    a newer same-plan iteration under the #207 latest-same-plan guard.
    E3c's formal Owner-mediated evaluations are a separate later operation
    and may append iterations against an unchanged subject through their
    own seam; this guard constrains the ordinary REVIEW seam only.
    """
    for iteration in iterations:
        if iteration.plan_revision_id == revision.id:
            raise ConflictError(
                "the current plan revision already has a review iteration in "
                "this planning loop; only its next persisted revision may "
                "launch another ordinary REVIEW"
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


# --- the dispatch operation --------------------------------------------------


def _revalidate_reserved_subjects(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    reserved_revision: PlanRevision,
    reviewer_session: TaskAgentSession,
) -> Task:
    """The post-attachment currentness proof: database only, before the send.

    Attachment acquisition takes wall-clock time, so the exact reserved
    subjects are re-proven under the Task row ``FOR UPDATE`` after the
    Reviewer attachment is acquired and immediately before the send: the
    exact current non-archived ``PLANNING`` Task at the caller's expected
    token with no current OwnerGate, the exact current PlanRevision still
    the reserved one, and the exact admitted READY Reviewer binding.
    Cancellation, a gate transition, a produced successor revision, or
    Reviewer-session loss during acquisition refuses the dispatch as a
    never-dispatched dead letter bound to the reserved subjects: no message
    is sent, no verdict/event/gate exists, and the reserved iteration stays
    unfinalized as an explicit recoverable blocked state — never silently
    bypassed and never re-dispatched blind. The transaction closes before
    the send; no runtime I/O occurs inside it.

    The entire proof runs inside ONE composed transaction: the Task row lock
    taken by ``get_task_for_update`` is held to the transaction's commit, so
    the token/status, revision currency, and Reviewer-binding checks all read
    one transaction-scoped connection and a concurrent Task transition can
    never interleave between them.
    """
    with composed_transaction(pool) as transaction_pool:
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
            raise ConflictError(
                "the addressed task is no longer planning; the dispatch is refused "
                "without sending and the reserved attempt stays unfinalized for recovery"
            )
        if task.current_owner_gate_id is not None:
            raise ConflictError(
                "the task carries a current owner gate; the dispatch is refused without "
                "sending and the reserved attempt stays unfinalized for recovery"
            )
        require_current_plan_revision(
            transaction_pool, task=task, plan_revision_id=reserved_revision.id
        )
        reviewer = _require_ready_reviewer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        if reviewer.id != reviewer_session.id:
            raise StaleOperationError(
                "the reviewer session binding has moved on after the review "
                "reservation; the dispatch is refused without sending and the "
                "reserved attempt stays unfinalized for recovery"
            )
        return task


# --- the shared reservation and the detached regular-mode dispatch (issue #336) ---


@dataclass(frozen=True, slots=True)
class _ReservedReviewAttempt:
    """The committed Phase-1 reservation facts both dispatch entries consume."""

    task: Task
    revision: PlanRevision
    reviewer: TaskAgentSession
    repository_full_name: str
    loop: ReviewLoop
    iteration: ReviewIteration


def _reserve_review_attempt(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    plan_production: TaskPlanProductionOutcome,
) -> _ReservedReviewAttempt:
    """Reserve exactly one next-numbered unfinalized ReviewIteration.

    The shared Phase-1 entry validation and atomic reservation of both
    dispatch entries (issues #209/#336): the account operational barrier,
    exact Owner Workspace/Task scope, the Task row locked ``FOR UPDATE``
    (serializing concurrent REVIEW dispatches for the same Task through
    existing Task persistence), the exact current ``planning`` Task under
    E2's post-update ``state_token``, the supplied PlanRevision proven to be
    exactly the Task's current revision and content-identical to its
    persisted row, the exact admitted READY Reviewer role/session resolved
    from durable state, the durable Repository identity, and the
    loop/iteration reservation — the OPEN planning loop established on
    first review with the Workspace's snapshotted budget, the loop row
    re-locked ``FOR UPDATE``, the no-unresolved-attempt and
    no-iteration-for-current-revision guards, and the next-numbered
    unfinalized iteration reserved atomically.
    """
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
            expected_state_token=plan_production.task.state_token,
        )
        if task.status is not TaskStatus.PLANNING:
            raise ConflictError("the addressed task is not in its coarse planning state")
        _require_production_subject(task, plan_production)
        persisted_revision = require_current_plan_revision(
            transaction_pool, task=task, plan_revision_id=plan_production.plan_revision.id
        )
        _require_persisted_revision_coherence(persisted_revision, plan_production.plan_revision)
        reviewer = _require_ready_reviewer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
        workspace = get_workspace(transaction_pool, workspace_id)
        if workspace is None:
            raise NotFoundError("the requested workspace is not available")
        repository = _require_task_repository(
            transaction_pool,
            workspace_id=workspace_id,
            repository_id=task.repository_id,
        )
        # The REVIEW dispatch composes OBSERVED presentation facts only: a
        # claim-state repository (issue #372 Owner onboarding) carries no
        # trusted GitHub observation yet and fails closed instead of misusing
        # claim data.
        metadata = require_observed_repository_metadata(repository)
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
        _require_no_iteration_for_current_revision(iterations, persisted_revision)
        reserved = _reserve_iteration(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            loop=loop,
            iterations=iterations,
            plan_revision_id=persisted_revision.id,
        )
    return _ReservedReviewAttempt(
        task=task,
        revision=persisted_revision,
        reviewer=reviewer,
        repository_full_name=f"{metadata.owner_login}/{metadata.name}",
        loop=locked_loop,
        iteration=reserved,
    )


@dataclass(frozen=True, slots=True)
class DispatchedPlanningReview:
    """The typed detached planning-REVIEW dispatch outcome (issue #336).

    The authoritative facts exactly as the durable state reads after the
    dispatch: the post-dispatch Task (its token unrotated by the receipt),
    the reserved unfinalized ReviewIteration whose receipt companion was
    committed, the claimed detached-turn receipt, and the exact Reviewer
    binding the canonical REVIEW instruction was admitted on. No runtime
    result and no validated ``review_result`` exists yet: the independently
    observed completion owns the decision settlement, and the ONE post-commit
    completion handoff owns the workflow continuation.
    """

    task: Task
    review_iteration: ReviewIteration
    receipt: TaskDetachedTurnReceipt
    reviewer_session: TaskAgentSession


def _refuse_review_dispatch_quietly(
    pool: DatabasePool,
    receipt: TaskDetachedTurnReceipt,
    *,
    refusal_kind: str,
) -> None:
    """Mark a claimed review receipt refused after a proven pre-effect failure.

    Best-effort containment: a persistence failure here must never mask the
    typed refusal the caller raises. The claimed receipt stays the honest
    unresolved state under the accepted v1 recovery bounds and the failure
    is logged selectively. There is no send claim to mark — the reserved
    ReviewIteration is the durable attempt identity.
    """
    try:
        refused = refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
            refusal_kind=refusal_kind,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused review-dispatch receipt could not be durably marked (%s); "
            "the claimed receipt stays the honest unresolved state",
            type(error).__name__,
        )
        refused = None
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the review-dispatch receipt was no longer claimed when its pre-effect "
            "refusal was recorded"
        )


def _dispatch_review_turn(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    reserved: _ReservedReviewAttempt,
    message: str,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    observation_registrar: PlanProductionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
) -> DispatchedPlanningReview:
    """Admit the canonical REVIEW instruction as one detached agent turn.

    The shared detached-dispatch mechanics of the planning REVIEW family
    (issue #336), mirroring the established detached-dispatch shape: the
    runtime lease and the detached capability are acquired and duck-typed
    fail-closed BEFORE any durable dispatch effect, the post-attachment
    currentness proof re-proves the exact reserved subjects under the Task
    row lock and closes before the receipt, the detached-turn receipt
    companion of the exact reserved ReviewIteration commits in ONE short
    transaction strictly before any runtime effect, one positively-required
    observation registration gates the admission, one detached-turn
    admission carries the composed canonical instruction verbatim, and the
    acknowledgement attach consumes any staged early completion exactly
    once. No ``send`` and no blocking wait exists in this path, and no
    database transaction is ever held across a runtime call.

    Proven pre-effect failures refuse the receipt and raise the typed
    operational-failure classes the F5b coordinator triages under the
    reserved iteration's own family. An uncertain admission leaves the
    receipt conservatively claimed and is never re-dispatched. The
    dispatching worker returns after the acknowledgement attach; the
    observed completion owns the decision settlement.
    """
    reviewer = reserved.reviewer
    reviewer_external_session_id = reviewer.external_session_id
    if reviewer_external_session_id is None or not reviewer_external_session_id.strip():
        raise ConflictError("the reviewer session binding carries no external session identity")
    register = getattr(observation_registrar, "register_session", None)
    if not callable(register):
        raise InvalidCommandError(
            "the planning-review observation registrar does not implement the "
            "established-session registration seam"
        )
    with ExitStack() as attachment_stack:
        runtime = attachment_stack.enter_context(reviewer_runtime_factory(reviewer))
        # The detached capability is duck-typed fail-closed BEFORE any durable
        # dispatch effect: a runtime without it cannot serve the detached
        # dispatch, and no receipt was ever recorded.
        admit = getattr(runtime, "admit_detached_turn", None)
        if not callable(admit):
            raise ExternalOperationFailedError(
                "the reviewer runtime does not implement the detached turn "
                "capability; the review dispatch failed before any effect"
            )
        admit_detached_turn = cast(Callable[[str, str], AgentTurnAdmission], admit)

        # The post-attachment currentness proof re-proves the reserved
        # subjects under the Task row lock and closes before the receipt —
        # the same proof the blocking send performs immediately before its
        # one interaction send.
        _revalidate_reserved_subjects(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=reserved.task.state_token,
            reserved_revision=reserved.revision,
            reviewer_session=reviewer,
        )

        # --- The one short dispatch transaction: the detached-turn receipt
        # companion of the exact reserved ReviewIteration, committed
        # strictly before any runtime effect. The single-outstanding-receipt
        # invariant and the receipt lifecycle are durable from here. ---
        with composed_transaction(pool) as transaction_pool:
            receipt = turn_correlation.record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=reviewer.id,
                operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
                original_attempt_kind=OriginalAttemptKind.REVIEW_ITERATION,
                original_attempt_id=reserved.iteration.id,
                expected_formal_family=REVIEW_RESULT_FAMILY,
            )

        # --- The positive pre-admission observation registration: the
        # positively-required gate. A refusal is a proven pre-effect failure:
        # the just-recorded receipt is marked refused, so a never-dispatched
        # turn cannot hold the session's single-outstanding slot. ---
        try:
            register(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=reviewer.id,
                connection_id=reviewer.connection_id,
                external_session_id=reviewer_external_session_id,
            )
        except ObservationRegistrationRefusedError as error:
            _refuse_review_dispatch_quietly(pool, receipt, refusal_kind=_REGISTRATION_REFUSAL_KIND)
            raise ExternalOperationFailedError(
                "the planning-review observation registration was refused before "
                "any admission; the instruction never dispatched"
            ) from error
        except Exception as error:
            logger.warning(
                "the planning-review observation registration failed "
                "(%s) before any admission; the receipt is refused and the "
                "instruction never dispatched",
                type(error).__name__,
            )
            _refuse_review_dispatch_quietly(pool, receipt, refusal_kind=_REGISTRATION_REFUSAL_KIND)
            raise ExternalOperationFailedError(
                "the planning-review observation registration failed before any "
                "admission; the instruction never dispatched"
            ) from error

        # --- The admission (external, no transaction held). One
        # detached-turn admission of the composed canonical instruction
        # verbatim. ---
        try:
            admission = admit_detached_turn(reviewer_external_session_id, message)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "the detached planning-review admission ended with an uncertain "
                "outcome; the receipt stays conservatively claimed and is never "
                "re-dispatched"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the planning-review admission is unknown; the "
                "receipt stays conservatively claimed and the instruction is "
                "never re-dispatched"
            ) from error
        except AgentRuntimeError as error:
            # A positively identified refusal (unreachable before effect,
            # runtime rejection, exact-session absence): proven pre-effect,
            # so the receipt is marked refused and the dispatch fails known.
            _refuse_review_dispatch_quietly(pool, receipt, refusal_kind=_ADMISSION_REFUSAL_KIND)
            raise ExternalOperationRejectedError(
                "the planning-review admission was refused before any effect; "
                "the receipt is refused and the instruction never dispatched"
            ) from error

        # --- The acknowledgement attach (one short transaction). A staged
        # early completion is consumed exactly once here and routed through
        # the ordinary settlement, so a validated review result that finished
        # before the acknowledgement was persisted is correlated, never
        # ignored or duplicated. ---
        try:
            outcome: turn_correlation.TurnSettlementOutcome = (
                turn_correlation.attach_acknowledged_turn(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    receipt_id=receipt.id,
                    admitted_turn_reference=admission.turn_reference,
                )
            )
        except ApplicationError:
            raise
        except Exception as error:
            raise ExternalOperationFailedError(
                "the planning-review acknowledgement could not be durably "
                "attached after its known admission; authoritative recovery "
                "is required and the instruction is never re-dispatched"
            ) from error

    # The attachment is released; the handoff is strictly post-dispatch and
    # never holds the runtime lease.
    if outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_VALIDATED_RESULT:
        # A staged early completion settled the iteration and the receipt
        # within this dispatch: the ONE post-commit completion handoff owns
        # the family routing. Strictly post-commit, contained, never a
        # workflow authority; a failed routing leaves the durable state to
        # the existing recovery semantics.
        try:
            completion_handoff.handoff_settled_completion(
                workspace_id=workspace_id,
                task_id=task_id,
                operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
            )
        except Exception as error:  # noqa: BLE001 - contained handoff
            logger.warning(
                "the post-dispatch planning-review completion handoff failed "
                "(%s); the durable state owns recovery",
                type(error).__name__,
            )
    if (
        outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_INTERRUPTED_TURN
        and outcome.receipt is not None
    ):
        # A staged interrupted completion settled the receipt within this
        # dispatch (issue #342): the ONE post-commit completion handoff owns
        # the Interrupt settlement — the attached adapter's D6b guard
        # release and the annotated intent's outcome recording — exactly as
        # it owns the validated families' continuations. Strictly
        # post-attach, contained best-effort inside the handoff, never a
        # workflow authority; a failed settlement leaves the durable
        # annotated intent exactly as committed.
        completion_handoff.handoff_settled_interrupt(
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=outcome.receipt.task_agent_session_id,
            external_session_id=reviewer_external_session_id,
        )
    reloaded_task = task_records.get_task(pool, task_id=task_id)
    if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    return DispatchedPlanningReview(
        task=reloaded_task,
        review_iteration=reserved.iteration,
        receipt=outcome.receipt if outcome.receipt is not None else receipt,
        reviewer_session=reviewer,
    )


def dispatch_planning_review_detached(
    pool: DatabasePool,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    plan_production: TaskPlanProductionOutcome,
    observation_registrar: PlanProductionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> DispatchedPlanningReview:
    """Dispatch one exact-plan REVIEW as one detached agent turn.

    The detached counterpart of the ordinary REVIEW dispatch (issues
    #325/#336): the same shared Phase-1 reservation — the exact current
    PlanRevision, the exact admitted READY Reviewer session, the loop
    guards, and the next-numbered unfinalized ReviewIteration committed
    atomically — followed by the composed canonical REVIEW message admitted
    verbatim as one independently completed agent turn on the exact
    Reviewer session, with the iteration's detached-turn receipt companion
    (expecting the canonical ``review_result`` family) committed strictly
    before the positive observation registration and the admission. No
    ``send`` and no blocking wait exists in this path, and the dispatching
    worker returns after the acknowledgement attach. The independently
    observed completion settles the validated ``review_result`` through the
    owning E3b decision settlement, and the ONE post-commit completion
    handoff owns the workflow continuation from committed state.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_DETACHED_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        reviewer_factory = _require_reviewer_factory(reviewer_runtime_factory)
        production = _require_plan_production(plan_production)
        annotate_span(
            span,
            operation=_DISPATCH_DETACHED_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: the shared entry validation + atomic reservation. ---
        reserved = _reserve_review_attempt(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            plan_production=production,
        )

        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's committed
            # reserved review iteration identity (#262). Recorded immediately
            # after the reservation commit, before anything below — the
            # attachment acquisition included — can fail (issue #268).
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
                original_attempt_kind=OriginalAttemptKind.REVIEW_ITERATION,
                original_attempt_id=reserved.iteration.id,
                phase="planning_review_dispatch",
            )

        message = _compose_review_message(
            repository_full_name=reserved.repository_full_name,
            issue_number=reserved.task.github_issue_number,
            revision=reserved.revision,
        )
        return _dispatch_review_turn(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            reserved=reserved,
            message=message,
            reviewer_runtime_factory=reviewer_factory,
            observation_registrar=observation_registrar,
            completion_handoff=completion_handoff,
        )
