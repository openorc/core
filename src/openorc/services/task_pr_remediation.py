"""E9 §3: the durable one-shot Producer PR-remediation claim (issue #84).

A finalized routable ``CHANGES_REQUESTED`` iteration of the OPEN PR_REVIEW
loop is remediable exactly once, ever. This module is the PR-specific
Producer continuation seam the PR decision and settlement services route to
outside their transactions: it allocates the next numbered Execution attempt
on the Task's original admitted Producer ACT session with a durable claim
association (``executions.review_iteration_id``, unique-enforced by the
partial unique index), sends ONE ``ReviseInteraction`` carrying the canonical
``review_result`` JSON, records the validated completion once with E6's exact
known-failure versus uncertain/potentially-dispatched distinction, and — only
after a durably recorded completion — verifies the Producer's branch claim,
reconciles the same canonical PR through B7, requires a GitHub-confirmed new
head, and re-enters the ordinary PR-review dispatch for that new head.

Authority boundaries:

- **The claim is durable before any send and never rolled back on any send
  outcome.** The claim transaction (database only, under the Task row lock)
  proves the exact subjects — the account barrier, the exact Owner scope, the
  non-archived ``REVIEWING`` Task at the caller's expected token, the canonical
  PR at the addressed iteration's exact head, the READY Producer binding, the
  OPEN PR_REVIEW loop owning the addressed iteration, and that the addressed
  iteration is a finalized CHANGES_REQUESTED result and the latest-numbered
  finalized result on its head (E2's supersession pattern) — then inserts the
  next attempt with ``review_iteration_id`` bound. A ``UniqueViolation`` on
  the partial unique index means a prior claim exists (completed, known
  failed, or uncertain alike) and classifies as a stale operation that
  applies nothing — the one-shot invariant is database-enforced, independent
  of any Task state token, so even a retry presenting the Task's freshly
  rotated current token cannot claim a second attempt.
- **No token rotation happens at claim time.** Remediation continues the
  same ``REVIEWING`` state under the same token; the one-shot property comes
  from the durable association, never from token mechanics.
- **E6's initial-attempt-only service is never called.** The initial
  authorized implementation (``task_authorized_implementation_execution``)
  refuses any second attempt by design; this module owns the narrow
  remediation continuation with the same result-recording discipline: a
  known pre-effect failure finalizes ``failed_transient``/``failed_final``
  truthfully; every post-claim uncertainty leaves the attempt conservatively
  running (the potentially-dispatched record) and is never resent; a
  validated completion is recorded exactly once, conditionally, and never
  rewritten.
- **The Producer reports on the existing canonical branch; the branch claim
  is verified, never treated as head/PR authority.** After a recorded
  completion, the same canonical PR is reconciled through B7 with no
  transaction open and must report a confirmed NEW head before the next
  ordinary REVIEW is dispatched. A missing push (unchanged head), a wrong
  branch claim, a closed/unmerged PR, or a conflicting identity fails closed
  — success is never invented, another PR is never adopted, and an uncertain
  command is never resent. Any appropriately observed head-change seam stays
  GitHub-owned reconciliation, never a shadow truth.
- **No remediation prompt and no historic exhausted findings.** The routed
  artifact is the exact validated ``review_result`` JSON of the addressed
  iteration's result, structurally serialized; nothing else is added.

The next ordinary review dispatch, decision finalization, gate settlement,
and merge decision belong to the sibling E9 modules; generalized recovery
belongs to E10 (issue #85).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from typing import Final, Protocol, cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeInterruptedTurnError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTransportError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.events import WorkflowEventActor
from openorc.domain.executions import Execution
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
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
from openorc.persistence import executions as execution_records
from openorc.persistence import pull_requests as pull_request_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.detached_turns import refuse_task_detached_turn_receipt
from openorc.persistence.executions import ExecutionStatus
from openorc.persistence.ownership import get_repository
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.interaction import PrReviewSubject, ReviseInteraction
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    REVIEW_OUTCOME_CHANGES_REQUESTED,
    SCHEMA_VERSION,
    ImplementationResult,
    ReviewFinding,
    ReviewResult,
)
from openorc.services import event_coordination
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
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.review_result_envelope import serialize_review_result
from openorc.services.task_abort import settle_observed_task_interrupt
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_detached_turn_observation import ObservationRegistrationRefusedError
from openorc.services.task_exceptional_blocking import install_exceptional_block_gate
from openorc.services.task_level3_blocking_catalog import classify_blocking_case
from openorc.services.task_pr_composition import TaskPrPublished
from openorc.services.task_pr_review_dispatch import (
    TaskPrReviewDispatchOutcome,
    dispatch_pr_review,
)
from openorc.services.task_pull_request_reconciliation import (
    TaskPullRequestReconciliation,
    reconcile_task_pull_request,
)
from openorc.services.task_subject_guards import (
    require_canonical_task_pull_request,
    require_current_task,
    require_task_pull_request_head,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "DispatchedPrRemediation",
    "PrRemediationObservationRegistrar",
    "ProducerRuntimeAttachmentFactory",
    "ReviewerRuntimeAttachmentFactory",
    "SettledPrRemediationVerification",
    "TaskPrRemediationOutcome",
    "complete_resumed_pr_remediation_result",
    "continue_resumed_pr_remediation_chain",
    "dispatch_pr_remediation_detached",
    "route_pr_remediation",
    "verify_settled_pr_remediation_push",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_remediation"
_REMEDIATION_SPAN_NAME = "task_pr_remediation.route_pr_remediation"
_DISPATCH_DETACHED_SPAN_NAME = "task_pr_remediation.dispatch_pr_remediation_detached"

# The positively established pre-effect refusal kinds the detached dispatch's
# receipt containment records: the observation registration's typed refusal
# and the admission's proven pre-effect runtime rejection.
_REGISTRATION_REFUSAL_KIND: Final[str] = "observation_registration_refused"
_ADMISSION_REFUSAL_KIND: Final[str] = "admission_refused"

logger = logging.getLogger(__name__)

# The validated completion status the conditional one-shot recording accepts
# (the canonical ``implementation_result`` envelope's completion value, as
# E7's envelope re-derivation pins it).
_IMPLEMENTATION_COMPLETED_STATUS = "COMPLETED"

# The safe failure-kind tokens shared with E6's attempt history (the only
# context the ``execution_failed`` audit carries).
_AUTHORITY_STALE_FAILURE_KIND: Final[str] = "authority_stale"
_RUNTIME_PRE_EFFECT_FAILURE_KIND: Final[str] = "runtime_pre_effect"

# The deferred Reviewer attachment seam for the re-entered ordinary dispatch:
# the service decides WHEN the adapter is acquired; the caller owns HOW the
# production attachment is composed (the #190 BOUND composition).
ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]


@dataclass(frozen=True, slots=True)
class TaskPrRemediationOutcome:
    """The durable outcome of one Producer PR-remediation round.

    ``execution`` is the claimed attempt in its recorded final state
    (``succeeded`` after a validated completion; ``failed_transient``/
    ``failed_final`` after a known pre-effect failure; still ``running`` —
    the potentially-dispatched record — after any post-claim uncertainty).
    ``new_head_sha`` is the GitHub-confirmed fresh head when the completion
    was recorded and the reconciliation confirmed the remediation push;
    ``dispatch`` is the re-entered ordinary PR-review dispatch outcome for
    that new head. A fail-closed verification after a recorded completion
    returns a typed error instead of an outcome — the recorded result is
    never rewritten into invented success.
    """

    task: Task
    producer_session: TaskAgentSession
    addressed_iteration: ReviewIteration
    review_loop: ReviewLoop
    execution: Execution
    new_head_sha: str | None
    dispatch: TaskPrReviewDispatchOutcome | None


# The claimed attempt is a full Execution record; typed precisely for callers.
def _require_producer_runtime(value: object) -> AgentRuntimeAdapter:
    if not callable(getattr(value, "send", None)) or not callable(
        getattr(value, "realize_control", None)
    ):
        raise InvalidCommandError("producer_runtime must be an AgentRuntimeAdapter")
    return cast(AgentRuntimeAdapter, value)


def _require_github(value: object) -> GitHubAppClient:
    for capability in ("get_installation_repository", "get_repository_pull_request"):
        if not callable(getattr(value, capability, None)):
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


def _require_uuid_command(value: object, name: str) -> UUID:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_review_result(value: object) -> ReviewResult:
    if not isinstance(value, ReviewResult):
        raise InvalidCommandError("review_result must be the validated typed ReviewResult")
    if value.outcome != REVIEW_OUTCOME_CHANGES_REQUESTED:
        raise InvalidCommandError(
            "only a CHANGES_REQUESTED review result routes to Producer remediation"
        )
    return value


def _require_addressed_iteration(value: object) -> ReviewIteration:
    if not isinstance(value, ReviewIteration):
        raise InvalidCommandError(
            "addressed_iteration must be the finalized CHANGES_REQUESTED ReviewIteration"
        )
    return value


def _require_ready_producer(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Resolve the exact admitted READY Producer role/session binding.

    Remediation continues the original admitted Producer ACT session only:
    a missing, cross-Workspace, or cross-Task binding is the uniform
    not-found outcome; a CONNECTING/LOST/ENDED binding is a conflict. No
    rebinding, second session, or fresh initialization ever happens here.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.PRODUCER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested producer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the producer session binding is not READY; remediation uses the exact "
            "admitted session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None:
        raise ConflictError("the producer session binding has no external session identity")
    return binding


# --- the durable one-shot claim transaction ------------------------------------------


def _claim_remediation_in_transaction(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    addressed_iteration: ReviewIteration,
) -> tuple[Task, TaskPullRequest, ReviewLoop, TaskAgentSession, Execution]:
    """Claim the one-shot remediation attempt durably inside the caller's
    transaction.

    The claim composition under the caller's already-taken Task row lock: the
    exact subjects are proved and the next numbered attempt is inserted with
    the ``review_iteration_id`` claim association and claimed ``running`` in
    the same transaction. The claim is never rolled back on any send outcome;
    the detached dispatch composes it with its detached-turn receipt companion
    in ONE short transaction (issue #338), so a receipt failure claims nothing
    at all.
    """
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
            "the addressed task is not reviewing; producer remediation is not available"
        )
    producer = _require_ready_producer(transaction_pool, workspace_id=workspace_id, task_id=task_id)
    # The canonical PR at the addressed iteration's exact head: the
    # addressed result was produced against that head, and remediation
    # continues it. A moved head is a stale operation that claims nothing.
    head_sha = addressed_iteration.reviewed_head_sha
    if head_sha is None:
        raise ConflictError("the addressed review iteration does not carry a PR/head subject")
    pull_request = require_task_pull_request_head(
        transaction_pool, task=task, expected_head_sha=head_sha
    )
    if pull_request.id != addressed_iteration.task_pull_request_id:
        raise StaleOperationError(
            "the addressed review iteration does not belong to the task's canonical "
            "pull request; the operation is stale"
        )
    # The OPEN PR_REVIEW loop owning the addressed iteration; planning
    # loops are never selected or consulted.
    loops = [
        loop
        for loop in review_records.list_task_review_loops(transaction_pool, task_id=task_id)
        if loop.purpose is ReviewLoopPurpose.PR_REVIEW
        and loop.id == addressed_iteration.review_loop_id
    ]
    if len(loops) != 1:
        raise NotFoundError("the requested review loop is not available for this task")
    loop = review_records.get_review_loop(
        transaction_pool, review_loop_id=loops[0].id, for_update=True
    )
    if loop is None or loop.status is not ReviewLoopStatus.OPEN:
        raise ConflictError("the addressed review loop is not open; remediation applies nothing")
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    stored = next(
        (iteration for iteration in iterations if iteration.id == addressed_iteration.id),
        None,
    )
    if stored is None:
        raise NotFoundError("the addressed review iteration is not available in this loop")
    if stored.outcome is None:
        raise ConflictError("the addressed review iteration is not finalized; nothing is routable")
    if stored.outcome is not ReviewOutcome.CHANGES_REQUESTED:
        raise ConflictError(
            "only a finalized CHANGES_REQUESTED review iteration routes to producer remediation"
        )
    finalized_on_head = [
        iteration
        for iteration in iterations
        if iteration.outcome is not None
        and iteration.task_pull_request_id == pull_request.id
        and iteration.reviewed_head_sha == head_sha
    ]
    if not finalized_on_head or stored.id != finalized_on_head[-1].id:
        # E2's supersession pattern: a superseded iteration is never
        # remediable; its result has been superseded on its own head.
        raise StaleOperationError(
            "the addressed review iteration is superseded by a newer finalized "
            "result on the same head; the operation is stale"
        )
    history = execution_records.list_task_executions(transaction_pool, task_id=task_id)
    next_number = max((execution.execution_number for execution in history), default=0) + 1
    try:
        execution = execution_records.create_execution(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=producer.id,
            execution_number=next_number,
            review_iteration_id=addressed_iteration.id,
        )
    except UniqueViolation as exc:
        # The partial unique index refused a second claim for the same
        # iteration: a prior attempt exists — completed, known-failed, or
        # uncertain alike. The one-shot invariant is durable and token-
        # independent; the replayed claim applies nothing.
        raise StaleOperationError(
            "this finalized review iteration was already claimed for remediation; "
            "the one-shot claim is durable and a second attempt is never allocated"
        ) from exc
    claimed = execution_records.claim_execution_running(transaction_pool, execution_id=execution.id)
    if claimed is None:
        raise StaleOperationError(
            "the reserved remediation attempt could not be claimed; the dispatch claim is stale"
        )
    event_coordination.record_execution_started_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        execution_id=claimed.id,
        actor=event_coordination.producer_actor(producer.id),
    )
    return task, pull_request, loop, producer, claimed


def _claim_remediation_transaction(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    addressed_iteration: ReviewIteration,
) -> tuple[Task, TaskPullRequest, ReviewLoop, TaskAgentSession, Execution]:
    """Claim the one-shot remediation attempt durably, before any send.

    One short database-only transaction under the Task row lock: the exact
    subjects are proved and the next numbered attempt is inserted with the
    ``review_iteration_id`` claim association and claimed ``running`` in the
    same transaction. The claim is never rolled back on any send outcome.
    """
    with composed_transaction(pool) as transaction_pool:
        return _claim_remediation_in_transaction(
            transaction_pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            addressed_iteration=addressed_iteration,
        )


# --- the send and the conditional one-shot result recording ---------------------------


def _finalize_pre_effect_failure(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    execution_id: UUID,
    producer_session_id: UUID,
    next_status: ExecutionStatus,
    failure_kind: str,
) -> None:
    """Truthfully finalize a known failure that provably preceded any effect.

    Mirrors E6's pre-effect discipline: the conditional transition applies
    only to the claimed ``running`` attempt, the safe ``execution_failed``
    audit carries the failure-kind token only, and a raced finalization is a
    rejected no-op — never a rewrite of possibly-dispatched history.
    """
    with composed_transaction(pool) as transaction_pool:
        failed = execution_records.update_execution_status(
            transaction_pool,
            execution_id=execution_id,
            expected_status=ExecutionStatus.RUNNING,
            next_status=next_status,
        )
        if failed is None:
            return
        event_coordination.record_execution_failed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            execution_id=execution_id,
            actor=event_coordination.producer_actor(producer_session_id),
            failure_kind=failure_kind,
        )


def _revalidate_remediation_subjects_for_send(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    producer_session: TaskAgentSession,
    head_sha: str,
) -> Task:
    """The pre-send subject proof: the remediation subject is still current.

    Attachment takes wall-clock time, so the exact claimed subjects are
    re-proven under the Task row ``FOR UPDATE`` after acquisition and before
    the REVISE realization: the exact current non-archived ``REVIEWING``
    Task at the caller's token, the exact admitted READY Producer binding,
    and the canonical PR still carrying exactly the addressed iteration's
    reviewed head — the head is the remediation subject and must still be
    current before any work is dispatched.
    """
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
            "the addressed task is no longer reviewing; the remediation subjects have moved on"
        )
    producer = _require_ready_producer(transaction_pool, workspace_id=workspace_id, task_id=task_id)
    if producer.id != producer_session.id:
        raise StaleOperationError(
            "the producer session binding has moved on; the operation is stale"
        )
    require_task_pull_request_head(transaction_pool, task=task, expected_head_sha=head_sha)
    return task


def _revalidate_claimed_subjects_for_completion(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    producer_session: TaskAgentSession,
    expected_pull_request_id: UUID,
) -> Task:
    """The completion subject proof: Task, Producer binding, PR identity.

    Records the validated completion against the already-claimed Execution
    without requiring the addressed iteration's reviewed head to remain the
    persisted reconciled head: the authorized Producer is expected to commit
    and push before returning ``implementation_result``, and GitHub may
    reconcile the new head through a webhook or another B7 invocation while
    the Producer works. The Task row lock re-proves the exact current
    non-archived ``REVIEWING`` Task at the caller's token, the Producer
    binding still belongs to this Task and is exactly the claimed session
    (READY is deliberately not required: the send already happened), and the
    canonical PR record is still exactly the addressed one — by identity,
    never by head equality. Any moved subject is a stale operation: nothing
    is applied, nothing is fabricated, and no retry permission is granted.
    """
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
            "the addressed task is no longer reviewing; the remediation subjects have moved on"
        )
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=WorkflowRole.PRODUCER)
    if binding is None or binding.task_id != task_id or binding.workspace_id != workspace_id:
        raise NotFoundError("the producer session binding is not available for this task")
    if binding.id != producer_session.id:
        raise StaleOperationError(
            "the producer session binding has moved on; the completion is stale"
        )
    pull_request = require_canonical_task_pull_request(transaction_pool, task=task)
    if pull_request.id != expected_pull_request_id:
        raise StaleOperationError(
            "the canonical pull request identity changed during remediation; "
            "the completion is stale"
        )
    return task


def _canonical_result_payload(result: ImplementationResult) -> dict[str, object]:
    """The exact validated canonical envelope persisted on the attempt."""
    payload: dict[str, object] = {
        "type": IMPLEMENTATION_RESULT_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "status": result.status,
        "branch": result.branch,
        "summary": result.summary,
        "changes": list(result.changes),
        "validation": list(result.validation),
    }
    if result.notes is not None:
        payload["notes"] = result.notes
    return payload


def _record_remediation_completion(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    execution_id: UUID,
    producer_session: TaskAgentSession,
    expected_pull_request_id: UUID,
    result: ImplementationResult,
) -> tuple[Task, Execution]:
    """Record the validated completion exactly once, conditionally.

    One short transaction re-proving the Task, the Producer binding, and the
    canonical PR identity — deliberately without requiring the addressed
    head to remain the persisted reconciled head, because a legitimate
    Producer push may already have been reconciled by GitHub — and applying
    the one-shot conditional result recording with the safe
    ``execution_completed`` audit. Any revalidation failure here leaves the
    attempt conservatively running — after effects may have begun, history
    is never rewritten and no false outcome is fabricated.
    """
    with composed_transaction(pool) as transaction_pool:
        current_task = _revalidate_claimed_subjects_for_completion(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            producer_session=producer_session,
            expected_pull_request_id=expected_pull_request_id,
        )
        finalized = _finalize_remediation_attempt(
            transaction_pool,
            task=current_task,
            execution_id=execution_id,
            producer_session_id=producer_session.id,
            result=result,
        )
    return current_task, finalized


def _finalize_remediation_attempt(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    execution_id: UUID,
    producer_session_id: UUID,
    result: ImplementationResult,
) -> Execution:
    """The one-shot completion of one running remediation attempt.

    The shared finalize core of the ordinary E9 remediation dispatch and the
    Owner Resume routing's boundary-C composition (issue #292): the
    conditional ``running`` → ``succeeded`` transition with the validated
    canonical result payload and the truthful ``execution_completed`` audit
    commit atomically; a concurrent finalize wins the conditional write and
    the stale caller fails closed. The attempt is never rewritten afterwards.
    """
    finalized = execution_records.complete_execution_with_result(
        transaction_pool,
        execution_id=execution_id,
        implementation_result=_canonical_result_payload(result),
    )
    if finalized is None:
        raise StaleOperationError(
            "the remediation attempt is no longer running; finalization is stale"
        )
    event_coordination.record_execution_completed_event(
        transaction_pool,
        workspace_id=task.workspace_id,
        task_id=task.id,
        execution_id=execution_id,
        actor=event_coordination.producer_actor(producer_session_id),
    )
    return finalized


def complete_resumed_pr_remediation_result(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    execution_id: UUID,
    result: ImplementationResult,
) -> tuple[Task, Execution]:
    """Complete the resumed PR-remediation attempt with its formal result.

    The Owner Resume routing's boundary-C composition for the interrupted
    E9 remediation execution (issue #292): the exact Task/token/status
    currentness is re-proven from durable state under the caller's Task row
    lock, the addressed Execution must still be the Task's running
    remediation attempt anchored to the Producer session and claiming its
    exact review iteration with the canonical PR present, and the resumed
    formal result completes it through the one shared finalize. No new
    attempt is ever created and the interrupted attempt's history is
    preserved: the resumed turn continues the SAME logical operation.
    """
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
        raise StaleOperationError(
            "the task is no longer reviewing; the resumed remediation result is stale"
        )
    producer_session = get_task_agent_session(
        transaction_pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    if (
        producer_session is None
        or producer_session.workspace_id != workspace_id
        or producer_session.external_session_id is None
    ):
        raise ConflictError("the producer session binding is not available for this task")
    execution = execution_records.get_execution(transaction_pool, execution_id=execution_id)
    if (
        execution is None
        or execution.workspace_id != workspace_id
        or execution.task_id != task_id
        or execution.producer_session_id != producer_session.id
        or execution.status is not ExecutionStatus.RUNNING
        or execution.review_iteration_id is None
    ):
        raise StaleOperationError(
            "the addressed remediation attempt is not the task's running "
            "producer remediation attempt; the resumed result cannot complete it"
        )
    iteration = review_records.get_review_iteration(
        transaction_pool, review_iteration_id=execution.review_iteration_id
    )
    if iteration is None or iteration.workspace_id != workspace_id or iteration.task_id != task_id:
        raise StaleOperationError(
            "the remediation attempt's claimed review iteration is not available; "
            "the resumed result cannot complete it"
        )
    pull_request = pull_request_records.get_task_pull_request_for_task(
        transaction_pool, task_id=task_id
    )
    if pull_request is None or pull_request.workspace_id != workspace_id:
        raise StaleOperationError(
            "the remediation attempt's canonical pull request is not available; "
            "the resumed result cannot complete it"
        )
    finalized = _finalize_remediation_attempt(
        transaction_pool,
        task=task,
        execution_id=execution.id,
        producer_session_id=producer_session.id,
        result=result,
    )
    return task, finalized


def _send_remediation_revision(
    pool: DatabasePool,
    runtime: AgentRuntimeAdapter,
    session_id: str,
    *,
    repository_full_name: str,
    pull_request: TaskPullRequest,
    task: Task,
    review_result: ReviewResult,
) -> ImplementationResult:
    """Send ONE established REVISE interaction to the exact Producer session.

    The subject is the exact ``PrReviewSubject`` of the addressed iteration's
    canonical PR/head; the entire routed message is the canonical
    ``review_result`` JSON of the validated result — no remediation prompt,
    no copy of historic exhausted findings, and no rewriting. The #67 shared
    funnel performs the formal validation. Once the realization is attempted,
    implementation work may have begun: an uncertain outcome and a response
    that is not the validated ``implementation_result`` both leave the
    claimed attempt conservatively running (the potentially-dispatched
    record), and a known runtime failure finalizes it truthfully as a
    pre-effect known failure — never resent, never fabricated.

    The pre-effect classification is narrowed to the positively proven
    classes (issue #87): ``AgentRuntimeTransportError`` and
    ``AgentRuntimeProtocolFailureError`` can arise only after a send has done
    work, so they are unresolved outcomes that keep the claimed attempt on
    its potentially-dispatched ``RUNNING`` record — never a proven
    non-delivery and never a safe retry.
    """
    subject = PrReviewSubject(
        repository_full_name=repository_full_name,
        pull_request_number=pull_request.github_pr_number,
        head_sha=pull_request.head_sha,
        issue_number=task.github_issue_number,
    )
    interaction = ReviseInteraction(subject=subject, review_result=review_result)
    try:
        outcome = runtime.realize_control(
            session_id,
            interaction,
            message=serialize_review_result(review_result),
            expected_family=IMPLEMENTATION_RESULT_FAMILY,
        )
    except AgentRuntimeInterruptedTurnError as error:
        settle_observed_task_interrupt(
            pool, runtime, task_id=task.id, external_session_id=session_id
        )
        logger.warning(
            "pr remediation ended with the runtime interrupting the send; the claimed "
            "attempt remains potentially dispatched"
        )
        raise ExternalOperationUncertainError(
            "the pr remediation send was interrupted by the runtime; the attempt "
            "remains potentially dispatched and is never resent"
        ) from error
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "pr remediation ended with an uncertain runtime outcome; the claimed "
            "attempt remains potentially dispatched"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the agent runtime pr remediation interaction is unknown; "
            "the attempt remains potentially dispatched and is never resent"
        ) from error
    except (AgentRuntimeTransportError, AgentRuntimeProtocolFailureError) as error:
        logger.warning(
            "pr remediation ended with a post-send runtime failure; the claimed "
            "attempt remains potentially dispatched"
        )
        raise ExternalOperationUncertainError(
            "the pr remediation send may have been delivered but its response could "
            "not be interpreted; the attempt remains potentially dispatched and is "
            "never resent"
        ) from error
    except AgentRuntimeError as error:
        # The remaining normalized taxonomy classes (configuration rejection,
        # runtime unavailability, and a not-live addressed session) are
        # positively proven to precede any effect.
        raise ExternalOperationFailedError(
            "the agent runtime pr remediation interaction failed as a known "
            "non-delivery; the attempt is finalized truthfully and never resent"
        ) from error
    if not isinstance(outcome, ImplementationResult):
        raise ExternalOperationUncertainError(
            "the pr remediation interaction ended without a validated implementation "
            "completion after possible implementation work; the attempt remains "
            "unresolved and is never resent"
        )
    return outcome


def _repository_full_name(repository: object) -> str:
    """The durable routed repository address for the interaction subject."""
    metadata = getattr(repository, "metadata", None)
    owner = getattr(metadata, "owner_login", None)
    name = getattr(metadata, "name", None)
    if (
        not isinstance(owner, str)
        or not owner.strip()
        or not isinstance(name, str)
        or not name.strip()
    ):
        raise ConflictError(
            "the routed repository carries no usable owner/name address; the "
            "remediation subject cannot be composed"
        )
    return f"{owner}/{name}"


# --- the fresh exact-head verification and re-dispatch --------------------------------


def _verify_remediation_push_and_redispatch(
    pool: DatabasePool,
    github: GitHubAppClient,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    producer_session: TaskAgentSession,
    addressed_iteration: ReviewIteration,
    review_loop: ReviewLoop,
    finalized_execution: Execution,
    result: ImplementationResult,
    repository_full_name: str,
) -> TaskPrRemediationOutcome:
    """Verify the branch claim, reconcile for a fresh head, and re-dispatch.

    Runs only after the completion is durably recorded: the Producer's
    ``branch`` claim is verified against the Task's existing canonical branch
    (never treated as head/PR authority), the SAME canonical PR is reconciled
    authoritatively through B7 with no transaction open, and a GitHub-
    confirmed NEW head is required before the next ordinary review dispatch
    is entered for that head with the new evidence. A missing push, a wrong
    branch claim, a closed/unmerged PR, or a conflicting identity fails
    closed — success is never invented, another PR is never adopted, and the
    recorded completion is never rewritten.
    """
    # The Producer's branch claim is a routing claim only: it must name the
    # Task's existing canonical branch, which is the durable branch fact —
    # GitHub reconciliation, not this claim, supplies the head.
    task = task_records.get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.canonical_feature_branch is None or result.branch != task.canonical_feature_branch:
        raise ConflictError(
            "the producer completion claims a branch other than the task's bound "
            "canonical feature branch; no push is verifiable and no new review is "
            "launched"
        )
    reconciliation: TaskPullRequestReconciliation = reconcile_task_pull_request(
        pool,
        github,
        workspace_id=workspace_id,
        task_id=task_id,
    )
    reconciled = reconciliation.pull_request
    if reconciled.id != addressed_iteration.task_pull_request_id:
        raise ConflictError(
            "the reconciled pull request is not the addressed canonical record; "
            "another pull request is never adopted"
        )
    if reconciled.state is not TaskPullRequestState.OPEN:
        raise ConflictError(
            "the canonical pull request is no longer open; closed-PR and merge "
            "recovery belong to the later recovery layer"
        )
    new_head = reconciled.head_sha
    if new_head == addressed_iteration.reviewed_head_sha:
        raise ExternalOperationFailedError(
            "the producer completion is recorded but the canonical pull request "
            "head is unchanged; no push is confirmed and no new review is launched"
        )
    # Re-enter the ordinary PR-review dispatch for the confirmed new head,
    # reusing E8's typed publication facts as the triggering command shape —
    # never re-invoking E8's composition/publication seam.
    dispatch = dispatch_pr_review(
        pool,
        reviewer_runtime_factory,
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        published=TaskPrPublished(task=task, pull_request=reconciled),
    )
    return TaskPrRemediationOutcome(
        task=dispatch.task,
        producer_session=producer_session,
        addressed_iteration=addressed_iteration,
        review_loop=review_loop,
        execution=finalized_execution,
        new_head_sha=new_head,
        dispatch=dispatch,
    )


# --- the remediation operation ---------------------------------------------------------


def route_pr_remediation(
    pool: DatabasePool,
    producer_runtime: AgentRuntimeAdapter,
    github: GitHubAppClient,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    review_result: ReviewResult,
    addressed_iteration: ReviewIteration,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> TaskPrRemediationOutcome:
    """Claim, dispatch, record, and continue one Producer PR-remediation round.

    The one-shot claim commits durably before any send and is never rolled
    back; the ONE ``ReviseInteraction`` carries only the canonical
    ``review_result`` JSON on the original Producer ACT session; the validated
    completion is recorded exactly once with E6's known-failure versus
    uncertain distinction; and only a recorded completion proceeds to the
    fresh exact-head verification, the B7 reconciliation of the same canonical
    PR, and the re-entered ordinary review dispatch for the GitHub-confirmed
    new head. A replayed claim for an already-claimed iteration — whatever the
    prior outcome, and whatever token a retry presents — applies nothing.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _REMEDIATION_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        producer_adapter = _require_producer_runtime(producer_runtime)
        github_client = _require_github(github)
        _require_reviewer_factory(reviewer_runtime_factory)
        result = _require_review_result(review_result)
        addressed = _require_addressed_iteration(addressed_iteration)
        annotate_span(
            span,
            operation=_REMEDIATION_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: the durable one-shot claim (database only). ---
        (
            task,
            pull_request,
            review_loop,
            producer_session,
            claimed,
        ) = _claim_remediation_transaction(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            addressed_iteration=addressed,
        )
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's reserved
            # remediation Execution identity (#262).
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
                original_attempt_kind=OriginalAttemptKind.EXECUTION,
                original_attempt_id=claimed.id,
                phase="pr_remediation_execution",
            )
        execution_id = claimed.id
        head_sha = addressed.reviewed_head_sha or ""

        # --- Phase 2: the ONE send, with NO database transaction open. ---
        repository = get_repository(pool, task.repository_id)
        if repository is None or repository.workspace_id != workspace_id:
            raise NotFoundError("the requested repository is not available in this workspace")
        repository_full_name = _repository_full_name(repository)
        # R1: authority revalidation after acquisition, before realization.
        try:
            with composed_transaction(pool) as transaction_pool:
                _revalidate_remediation_subjects_for_send(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    producer_session=producer_session,
                    head_sha=head_sha,
                )
        except (NotFoundError, ConflictError, StaleOperationError):
            # Authority moved on during composition, before any runtime
            # effect: the never-dispatched attempt finalizes truthfully.
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution_id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_FINAL,
                failure_kind=_AUTHORITY_STALE_FAILURE_KIND,
            )
            raise
        try:
            completed = _send_remediation_revision(
                pool,
                producer_adapter,
                producer_session.external_session_id or "",
                repository_full_name=repository_full_name,
                pull_request=pull_request,
                task=task,
                review_result=result,
            )
        except ExternalOperationFailedError:
            # A known non-delivery: the claimed attempt finalizes truthfully.
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution_id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise
        # --- the one-shot completion recording (no transaction open across
        # the send; the finalize repeats the subject proof). ---
        _, finalized_execution = _record_remediation_completion(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            execution_id=execution_id,
            producer_session=producer_session,
            expected_pull_request_id=pull_request.id,
            result=completed,
        )

        # --- Phase 3: the fresh exact-head verification and re-dispatch. ---
        return _verify_remediation_push_and_redispatch(
            pool,
            github_client,
            reviewer_runtime_factory,
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=profile_id,
            expected_state_token=expected_state_token,
            producer_session=producer_session,
            addressed_iteration=addressed,
            review_loop=review_loop,
            finalized_execution=finalized_execution,
            result=completed,
            repository_full_name=repository_full_name,
        )


def continue_resumed_pr_remediation_chain(
    pool: DatabasePool,
    github: GitHubAppClient,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    finalized_execution: Execution,
    result: ImplementationResult,
) -> TaskPrRemediationOutcome:
    """Re-enter the ordinary remediation chain after the resumed completion.

    The Owner Resume routing's post-completion continuation for the
    interrupted E9 remediation execution (issue #292): the exact claimed
    review iteration, its review loop, the Producer binding, and the routed
    repository address are re-derived from durable state, and the ordinary
    fresh exact-head verification and re-dispatch chain runs unchanged for
    the finalized attempt — never a second chain composition.
    """
    producer_session = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.PRODUCER)
    if (
        producer_session is None
        or producer_session.workspace_id != workspace_id
        or finalized_execution.producer_session_id != producer_session.id
    ):
        raise ConflictError("the finalized remediation attempt's producer binding is not available")
    if finalized_execution.review_iteration_id is None:
        raise ConflictError("the finalized remediation attempt claims no review iteration")
    addressed_iteration = review_records.get_review_iteration(
        pool, review_iteration_id=finalized_execution.review_iteration_id
    )
    if (
        addressed_iteration is None
        or addressed_iteration.workspace_id != workspace_id
        or addressed_iteration.task_id != task_id
    ):
        raise ConflictError(
            "the finalized remediation attempt's claimed review iteration is not available"
        )
    review_loop = review_records.get_review_loop(
        pool, review_loop_id=addressed_iteration.review_loop_id
    )
    if review_loop is None or review_loop.workspace_id != workspace_id:
        raise ConflictError("the claimed review iteration's review loop is not available")
    pull_request = pull_request_records.get_task_pull_request_for_task(pool, task_id=task_id)
    if pull_request is None or pull_request.workspace_id != workspace_id:
        raise ConflictError("the remediation chain's canonical pull request is not available")
    task = task_records.get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id or task.repository_id is None:
        raise ConflictError("the remediation chain's task is not available")
    repository = get_repository(pool, task.repository_id)
    if repository is None or repository.workspace_id != workspace_id:
        raise ConflictError("the remediation chain's repository is not available")
    return _verify_remediation_push_and_redispatch(
        pool,
        github,
        reviewer_runtime_factory,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=profile_id,
        expected_state_token=task.state_token,
        producer_session=producer_session,
        addressed_iteration=addressed_iteration,
        review_loop=review_loop,
        finalized_execution=finalized_execution,
        result=result,
        repository_full_name=_repository_full_name(repository),
    )


# --- the detached Producer remediation dispatch (issues #325/#338) --------------------


# The deferred Producer attachment seam for the detached remediation enqueue:
# the service decides WHEN the adapter is acquired; the caller owns HOW the
# production attachment is composed (the #190 BOUND composition on the exact
# Task checkout).
ProducerRuntimeAttachmentFactory = Callable[[], AbstractContextManager[AgentRuntimeAdapter]]


class PrRemediationObservationRegistrar(Protocol):
    """The structural registration seam of the process's detached-turn observer.

    The same positively-required pre-admission gate the detached
    establishment, implementation, plan, review, and composition dispatches
    consume (issues #323/#333/#334/#335/#336/#337): the remediation enqueue
    registers the exact bound external Producer session before any admission,
    so an unreplayable terminal ``implementation_result`` can never be
    produced without active observation. Failures are the observer's typed
    refusals, routed through the pre-effect failure machinery.
    """

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Register one exact bound session for detached-turn observation."""
        ...


@dataclass(frozen=True, slots=True)
class DispatchedPrRemediation:
    """The typed detached E9 remediation dispatch outcome (issue #338).

    The authoritative facts exactly as the durable state reads after the
    dispatch: the post-dispatch Task, the claimed ``running`` remediation
    Execution whose receipt companion was committed, the claimed
    detached-turn receipt, the exact Producer binding the canonical REVISE
    instruction was admitted on, and the addressed finalized
    ``CHANGES_REQUESTED`` iteration. No runtime result and no validated
    ``implementation_result`` exists yet: the independently observed
    completion settles the attempt through the existing #322 E9
    execution-settlement route, and the ONE post-commit completion handoff
    owns the workflow continuation.
    """

    task: Task
    execution: Execution
    receipt: TaskDetachedTurnReceipt
    producer_session: TaskAgentSession
    addressed_iteration: ReviewIteration


def _review_result_from_iteration(iteration: ReviewIteration) -> ReviewResult:
    """Rebuild the persisted protocol-validated review result verbatim.

    The finalized CHANGES_REQUESTED iteration's summary and findings are the
    remediation input (issue #338): values and ordering are preserved
    exactly, with no interpretation, rewriting, or free-text mutation. A
    persisted findings document that is no longer in the canonical
    ``review_result`` finding shape fails closed instead of being repaired
    or inferred.
    """
    findings: list[ReviewFinding] = []
    for item in iteration.findings or ():
        if (
            not isinstance(item, dict)
            or set(item.keys()) != {"summary", "details"}
            or not isinstance(item.get("summary"), str)
            or not str(item.get("summary", "")).strip()
            or not isinstance(item.get("details"), str)
            or not str(item.get("details", "")).strip()
        ):
            raise ConflictError(
                "the persisted review findings are not in the canonical review_result "
                "finding shape; no remediation turn is dispatched"
            )
        findings.append(ReviewFinding(summary=item["summary"], details=item["details"]))
    return ReviewResult(
        outcome=REVIEW_OUTCOME_CHANGES_REQUESTED,
        summary=iteration.summary or "",
        findings=tuple(findings),
    )


def _refuse_pr_remediation_dispatch_quietly(
    pool: DatabasePool,
    receipt: TaskDetachedTurnReceipt,
    *,
    refusal_kind: str,
) -> None:
    """Mark a claimed remediation receipt refused after a proven pre-effect failure.

    Best-effort containment: a persistence failure here must never mask the
    typed refusal the caller raises. The claimed receipt stays the honest
    unresolved state under the accepted v1 recovery bounds and the failure
    is logged selectively. The claimed Execution keeps its own durable
    attempt identity and is finalized by the caller's pre-effect failure
    path, never by this containment.
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
            "the refused remediation-dispatch receipt could not be durably marked (%s); "
            "the claimed receipt stays the honest unresolved state",
            type(error).__name__,
        )
        refused = None
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the remediation-dispatch receipt was no longer claimed when its "
            "pre-effect refusal was recorded"
        )


def _dispatch_pr_remediation_turn(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    addressed_iteration: ReviewIteration,
    message: str,
    compose_claim_and_receipt: Callable[
        [DatabasePool], tuple[Task, TaskAgentSession, Execution, TaskDetachedTurnReceipt]
    ],
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    observation_registrar: PrRemediationObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None,
) -> DispatchedPrRemediation:
    """Admit the canonical REVISE instruction as one detached agent turn.

    The detached dispatch mechanics of the E9 remediation family (issue
    #338), mirroring the established plan-family dispatch shape: the runtime
    lease and the detached capability are acquired and duck-typed fail-closed
    BEFORE any durable dispatch effect, the exact one-shot E9 claim and its
    detached-turn receipt companion (expecting the canonical
    ``implementation_result`` family) commit in ONE short transaction
    strictly before any runtime effect — so a receipt failure claims nothing
    at all and the admission race and the single-outstanding-turn invariant
    are durable from the first possible effect — one positively-required
    observation registration gates the admission, one detached-turn admission
    carries the composed canonical REVISE message verbatim on the exact Task
    checkout, and the acknowledgement attach consumes any staged early
    completion exactly once. No ``send`` and no blocking wait exists in this
    path, and no database transaction is ever held across a runtime call.

    The proven/uncertain failure classification is exactly the ordinary
    remediation dispatch's: only positively proven pre-effect failures refuse
    the receipt and finalize the claimed ``running`` attempt truthfully
    (``failed_transient``; an authority movement after the claim finalizes
    ``failed_final``); an uncertain admission leaves the attempt
    conservatively running and the receipt conservatively claimed and is
    never re-dispatched. The dispatching worker returns after the
    acknowledgement attach; the observed completion settles the attempt
    through the existing #322 E9 execution-settlement route.
    """
    register = getattr(observation_registrar, "register_session", None)
    if not callable(register):
        raise InvalidCommandError(
            "the remediation observation registrar does not implement the "
            "established-session registration seam"
        )
    with ExitStack() as attachment_stack:
        runtime = attachment_stack.enter_context(producer_runtime_factory())
        # The detached capability is duck-typed fail-closed BEFORE any durable
        # dispatch effect: a runtime without it cannot serve the detached
        # dispatch, and no claim or receipt was ever recorded.
        admit = getattr(runtime, "admit_detached_turn", None)
        if not callable(admit):
            raise ExternalOperationFailedError(
                "the producer runtime does not implement the detached turn "
                "capability; the remediation dispatch failed before any effect"
            )
        admit_detached_turn = cast(Callable[[str, str], AgentTurnAdmission], admit)

        # --- The one short dispatch transaction: the exact one-shot E9 claim
        # and its detached-turn receipt companion, committed atomically
        # strictly before any runtime effect. A replayed or raced continuation
        # never claims or dispatches again: the claim's one-shot subject guard
        # and the single-outstanding-receipt invariant are durable from here,
        # and a receipt failure rolls the whole claim back. ---
        task, producer_session, execution, receipt = compose_claim_and_receipt(pool)
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's committed
            # remediation Execution identity (#262). Recorded immediately
            # after the claim-and-receipt commit, before anything below — the
            # R1 revalidation included — can fail (issue #268).
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
                original_attempt_kind=OriginalAttemptKind.EXECUTION,
                original_attempt_id=execution.id,
                phase="pr_remediation_execution",
            )
        producer_external_session_id = producer_session.external_session_id
        if producer_external_session_id is None or not producer_external_session_id.strip():
            # Unreachable for the claim's own READY-Producer guard; kept
            # explicit with the claim-coupled cleanup for fail-closed safety.
            _refuse_pr_remediation_dispatch_quietly(
                pool, receipt, refusal_kind=_ADMISSION_REFUSAL_KIND
            )
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution.id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise ConflictError(
                "the claimed producer session binding carries no external session identity"
            )
        # --- R1: authority revalidation after acquisition, before any
        # runtime interaction. ---
        try:
            with composed_transaction(pool) as transaction_pool:
                _revalidate_remediation_subjects_for_send(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    producer_session=producer_session,
                    head_sha=addressed_iteration.reviewed_head_sha or "",
                )
        except (NotFoundError, ConflictError, StaleOperationError):
            # Authority moved on after the claim, before any runtime effect:
            # the never-dispatched attempt finalizes truthfully and the
            # just-recorded receipt is refused, so a never-dispatched turn
            # cannot hold the session's single-outstanding slot.
            _refuse_pr_remediation_dispatch_quietly(
                pool, receipt, refusal_kind=_ADMISSION_REFUSAL_KIND
            )
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution.id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_FINAL,
                failure_kind=_AUTHORITY_STALE_FAILURE_KIND,
            )
            raise
        # --- the positive pre-admission observation registration: the
        # positively-required gate. A refusal is a proven pre-effect failure:
        # the just-recorded receipt is marked refused and the never-dispatched
        # attempt finalizes truthfully. ---
        try:
            register(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session.id,
                connection_id=producer_session.connection_id,
                external_session_id=producer_external_session_id,
            )
        except ObservationRegistrationRefusedError:
            _refuse_pr_remediation_dispatch_quietly(
                pool, receipt, refusal_kind=_REGISTRATION_REFUSAL_KIND
            )
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution.id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise
        except Exception as error:
            from openorc.services.task_detached_turn_observation import (  # noqa: PLC0415
                ObservationRegistrationRefusalReason,
            )

            _refuse_pr_remediation_dispatch_quietly(
                pool, receipt, refusal_kind=_REGISTRATION_REFUSAL_KIND
            )
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution.id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise ObservationRegistrationRefusedError(
                "the remediation observation registration failed before any "
                "admission; the dispatch is a proven pre-effect failure",
                reason=ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value,
            ) from error
        # --- the admission (external, no transaction held). One detached-turn
        # admission of the composed canonical REVISE message verbatim on the
        # exact Task checkout. ---
        try:
            admission = admit_detached_turn(producer_external_session_id, message)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "the detached remediation admission ended with an uncertain "
                "outcome; the receipt stays conservatively claimed and the "
                "attempt is never re-dispatched"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the remediation admission is unknown; the "
                "receipt stays conservatively claimed and the attempt is "
                "never re-dispatched"
            ) from error
        except AgentRuntimeError as error:
            # A positively identified refusal (unreachable before effect,
            # runtime rejection, exact-session absence): proven pre-effect,
            # so the receipt is marked refused and the attempt fails known.
            _refuse_pr_remediation_dispatch_quietly(
                pool, receipt, refusal_kind=_ADMISSION_REFUSAL_KIND
            )
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=execution.id,
                producer_session_id=producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise ExternalOperationRejectedError(
                "the remediation admission was refused before any effect; "
                "the receipt is refused and the attempt never dispatched"
            ) from error
        # --- the acknowledgement attach (one short transaction). A staged
        # early completion is consumed exactly once here and routed through
        # the ordinary settlement, so a validated result that finished before
        # the acknowledgement was persisted is correlated, never ignored or
        # duplicated. ---
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
                "the remediation acknowledgement could not be durably attached "
                "after its known admission; authoritative recovery is required "
                "and the attempt is never re-dispatched"
            ) from error

    # The attachment is released; the handoff is strictly post-dispatch and
    # never holds the runtime lease.
    if outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_VALIDATED_RESULT:
        # A staged early completion settled the attempt and the receipt
        # within this dispatch: the ONE post-commit completion handoff owns
        # the family routing. Strictly post-commit, contained, never a
        # workflow authority; a failed routing leaves the durable state to
        # the existing recovery semantics.
        try:
            completion_handoff.handoff_settled_completion(
                workspace_id=workspace_id,
                task_id=task_id,
                operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
            )
        except Exception as error:  # noqa: BLE001 - contained handoff
            logger.warning(
                "the post-dispatch remediation completion handoff failed (%s); "
                "the durable state owns recovery",
                type(error).__name__,
            )
    reloaded_task = task_records.get_task(pool, task_id=task_id)
    if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    return DispatchedPrRemediation(
        task=reloaded_task,
        execution=execution,
        receipt=outcome.receipt if outcome.receipt is not None else receipt,
        producer_session=producer_session,
        addressed_iteration=addressed_iteration,
    )


def dispatch_pr_remediation_detached(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    addressed_iteration: ReviewIteration,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    observation_registrar: PrRemediationObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> DispatchedPrRemediation:
    """Dispatch one routed Producer remediation as one detached agent turn.

    The detached counterpart of the ordinary remediation route (issues
    #325/#338): the exact one-shot E9 claim — bound to the finalized
    ``CHANGES_REQUESTED`` iteration and the original Producer session — and
    its detached-turn receipt companion commit in ONE short transaction that
    is never rolled back on any dispatch outcome and never leaves a claimed
    attempt without its correlation receipt, followed by the canonical
    ``review_result`` JSON of the **persisted** finalized iteration (rebuilt
    verbatim, never from transient state) admitted as one independently
    completed agent turn on the original Producer session at the exact Task
    checkout. No ``send`` and no blocking wait exists in this path, and the
    dispatching worker exits after the acknowledgement attach. The
    independently observed completion settles the attempt through the
    existing #322 E9 execution-settlement route, and the ONE post-commit
    completion handoff owns the continuation from committed state. A replayed
    claim for an already-claimed iteration — whatever the prior outcome, and
    whatever token a retry presents — applies nothing.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_DETACHED_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        addressed = _require_addressed_iteration(addressed_iteration)
        annotate_span(
            span,
            operation=_DISPATCH_DETACHED_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        def compose_claim_and_receipt(
            transaction_pool: DatabasePool,
        ) -> tuple[Task, TaskAgentSession, Execution, TaskDetachedTurnReceipt]:
            _task, _pull_request, _review_loop, producer_session, claimed = (
                _claim_remediation_in_transaction(
                    transaction_pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    addressed_iteration=addressed,
                )
            )
            receipt = turn_correlation.record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session.id,
                operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
                original_attempt_kind=OriginalAttemptKind.EXECUTION,
                original_attempt_id=claimed.id,
            )
            return _task, producer_session, claimed, receipt

        # --- the composed canonical REVISE message from the persisted
        # finalized iteration, then the detached admission. ---
        message = serialize_review_result(_review_result_from_iteration(addressed))
        return _dispatch_pr_remediation_turn(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            addressed_iteration=addressed,
            message=message,
            compose_claim_and_receipt=compose_claim_and_receipt,
            producer_runtime_factory=producer_runtime_factory,
            observation_registrar=observation_registrar,
            completion_handoff=completion_handoff,
            attempt_evidence=attempt_evidence,
        )


# --- the settled-remediation continuation verification (issue #338) -------------------


# The OPENORC logical actor of the verification's Level 3 block installation:
# the classification is OpenOrc's own typed observation, never a runtime- or
# Owner-attributed decision.
_OPENORC_ACTOR: Final[event_coordination.WorkflowActorContext] = (
    event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)
)


@dataclass(frozen=True, slots=True)
class SettledPrRemediationVerification:
    """The GitHub-verified continuation facts of one settled remediation round.

    Returned only when a durably SUCCEEDED remediation Execution is recorded
    for the latest finalized ``CHANGES_REQUESTED`` PR iteration, the recorded
    completion's branch claim corresponds to the Task's bound canonical
    branch, and B7 reconciles the SAME canonical pull request to a
    GitHub-confirmed NEW head with no review iteration yet reserved for that
    head — the exact facts the ONE completion handoff needs to submit the
    next typed ``TASK_PR_REVIEW`` for the confirmed head. ``None`` means no
    continuation is owed from committed state — nothing settled to continue,
    a moved-on or superseded subject, a continuation already owned by another
    path, or a round whose verification installed its atomic Level 3 block.
    """

    task: Task
    pull_request: TaskPullRequest
    addressed_iteration: ReviewIteration
    finalized_execution: Execution
    new_head_sha: str


def _block_unverifiable_remediation_round(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    context: dict[str, str],
) -> Task | None:
    """Atomically block the Task on an unverifiable settled remediation round.

    The established E8 stale-publication block shape (issue #338): one short
    composed transaction — the approving Profile's account-operational
    barrier re-proved first, the locked Task's exact currentness re-proven,
    and the F3 exceptional blocking foundation (#258) installing — atomically
    — the ``stale_operation`` TaskBlock, its pending Level 3-selected OwnerGate
    bound to it as the only subject (F8b, issue #287), the ``reviewing →
    blocked`` transition with one rotated token, and the ``task_blocked`` /
    ``owner_gate_created`` audits under the truthful OPENORC actor.

    The unverifiable-round fact is deliberately presented to the Level 3
    blocking-case catalog (F8a, issue #286) as no supported condition — it is
    not one of the settled Type A cases — so the policy selects the Type B
    ``BLOCK_RESOLUTION`` diagnostic gate; this caller never chooses a gate
    type. The caller's ``context`` carries only the exact proven machine
    facts of the detection site (issue #280) for later guarded recovery.

    A subject that moved on between the detection and the installation
    applies nothing and returns ``None`` — the continuation is that subject's
    own seam's, never this guard's. A succeeded remediation Execution is
    never rewritten, never labelled failed, and never re-dispatched: the
    recorded round and its one-shot semantics stand unchanged.
    """
    try:
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                return None
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.REVIEWING or task.current_owner_gate_id is not None:
                return None
            gate_type = classify_blocking_case(None, context, TaskBlockReason.STALE_OPERATION)
            installation = install_exceptional_block_gate(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                expected_status=task.status,
                expected_current_owner_gate_id=task.current_owner_gate_id,
                gate_type=gate_type,
                block_reason=TaskBlockReason.STALE_OPERATION,
                block_context=context,
                actor=_OPENORC_ACTOR,
            )
            return installation.task
    except (NotFoundError, StaleOperationError):
        # The subject moved on between the detection and the installation:
        # the durable state is another seam's, and no block applies.
        return None


def verify_settled_pr_remediation_push(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
) -> SettledPrRemediationVerification | None:
    """Re-prove a settled remediation round and verify its confirmed new head.

    The detached remediation completion's owning continuation guard (issue
    #338), reusing the ordinary chain's verification policy with every
    subject re-derived from committed state — never from a transient
    settlement object. This is a direct control guard, not a Level 1
    operation: a succeeded remediation Execution is never a Level 2 failure
    identity (the execution-family contracts deliberately reject succeeded
    attempts), so its positively detected verification mismatches reach
    Level 3 directly through the shared exceptional-blocking foundation and
    its uncertain or known-failed GitHub reads stay typed errors whose
    durable state the existing Owner Resume recovery owns.

    **Phase 1 — the continuation subject set (database only):** the exact
    current non-archived ``REVIEWING`` Task at the caller's token with no
    current OwnerGate, the single OPEN ``PR_REVIEW`` loop, its
    latest-numbered finalized ``CHANGES_REQUESTED`` PR iteration still the
    loop's latest iteration (a later routed iteration means the remediation
    already resolved), and the one durably SUCCEEDED remediation Execution
    claimed for exactly that iteration with a recorded canonical
    ``implementation_result`` — the one-shot claim association guarantees at
    most one. Any missing, moved-on, or superseded subject means no
    continuation is owed and ``None`` is returned.

    **Phase 2 — the recorded branch claim:** the recorded canonical envelope
    must be the completed ``implementation_result`` family whose ``branch``
    claim names exactly the Task's bound canonical feature branch. The claim
    is routing evidence, never head/PR authority; a mismatching recorded
    completion installs the atomic Level 3 block and is never rewritten into
    invented success.

    **Phase 3 — the authoritative head verification (no transaction open
    across GitHub I/O):** the SAME canonical pull request is reconciled
    through B7, must still be exactly the addressed iteration's record and
    OPEN, and must report a GitHub-confirmed NEW head — a missing push
    (unchanged head) installs the atomic Level 3 block. A review iteration
    already reserved for the confirmed head (a concurrently continued
    subject) is a no-op instead of dispatching twice.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    github_client = _require_github(github)

    # --- Phase 1: the continuation subject set (database only). ---
    with composed_transaction(pool) as transaction_pool:
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            return None
        try:
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
        except (NotFoundError, StaleOperationError):
            # The subject moved on between the handoff's pre-check and this
            # transaction: the continuation is moot, never an error.
            return None
        if task.status is not TaskStatus.REVIEWING or task.current_owner_gate_id is not None:
            # A gate or a moved-on status owns the workflow: the continuation
            # is that seam's, never this verification's.
            return None
        loops = [
            loop
            for loop in review_records.list_task_review_loops(transaction_pool, task_id=task_id)
            if loop.workspace_id == workspace_id and loop.purpose is ReviewLoopPurpose.PR_REVIEW
        ]
        if len(loops) != 1 or loops[0].status is not ReviewLoopStatus.OPEN:
            return None
        loop = loops[0]
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        change_requests = [
            iteration
            for iteration in iterations
            if iteration.outcome is ReviewOutcome.CHANGES_REQUESTED
            and iteration.task_pull_request_id is not None
        ]
        if not change_requests:
            return None
        addressed = change_requests[-1]
        if iterations[-1].id != addressed.id:
            # A later routed iteration means this remediation already resolved
            # (the E2 supersession rule the Level 2 remediation subject
            # contract proves): the continuation is superseded.
            return None
        claimed = [
            execution
            for execution in execution_records.list_task_executions(
                transaction_pool, task_id=task_id
            )
            if execution.review_iteration_id == addressed.id
        ]
        if len(claimed) != 1:
            # The one-shot claim association admits at most one execution per
            # iteration; anything else is a durable invariant violation.
            raise ConflictError(
                "the addressed review iteration does not carry exactly one claimed "
                "remediation execution; the continuation cannot choose a subject"
            )
        execution = claimed[0]
        if (
            execution.status is not ExecutionStatus.SUCCEEDED
            or execution.implementation_result is None
        ):
            # Nothing durably settled to continue (still running, known
            # failed, or uncertain): the settlement routes own those states.
            return None

    # --- Phase 2: the recorded branch claim (the routing evidence). ---
    payload = execution.implementation_result
    if (
        not isinstance(payload, dict)
        or payload.get("type") != IMPLEMENTATION_RESULT_FAMILY
        or payload.get("status") != _IMPLEMENTATION_COMPLETED_STATUS
    ):
        _block_unverifiable_remediation_round(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            context={
                "detection": "noncanonical_recorded_result",
                "execution_id": str(execution.id),
                "review_iteration_id": str(addressed.id),
            },
        )
        return None
    branch_claim = payload.get("branch")
    if (
        not isinstance(branch_claim, str)
        or task.canonical_feature_branch is None
        or branch_claim != task.canonical_feature_branch
    ):
        _block_unverifiable_remediation_round(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            context={
                "detection": "wrong_branch_claim",
                "execution_id": str(execution.id),
                "review_iteration_id": str(addressed.id),
                "claimed_branch": branch_claim if isinstance(branch_claim, str) else "",
                "canonical_feature_branch": task.canonical_feature_branch or "",
            },
        )
        return None

    # --- Phase 3: the authoritative head verification (GitHub I/O, no
    # transaction held across it). An uncertain or known-failed read stays a
    # typed error — never a block, never a proven workflow mismatch — and
    # the durable state owns recovery. ---
    reconciliation: TaskPullRequestReconciliation = reconcile_task_pull_request(
        pool,
        github_client,
        workspace_id=workspace_id,
        task_id=task_id,
    )
    reconciled = reconciliation.pull_request
    if reconciled.id != addressed.task_pull_request_id:
        _block_unverifiable_remediation_round(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            context={
                "detection": "canonical_pull_request_mismatch",
                "execution_id": str(execution.id),
                "review_iteration_id": str(addressed.id),
                "addressed_pull_request_id": str(addressed.task_pull_request_id),
                "reconciled_pull_request_id": str(reconciled.id),
            },
        )
        return None
    if reconciled.state is not TaskPullRequestState.OPEN:
        _block_unverifiable_remediation_round(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            context={
                "detection": "pull_request_not_open",
                "execution_id": str(execution.id),
                "review_iteration_id": str(addressed.id),
                "pull_request_state": reconciled.state.value,
            },
        )
        return None
    new_head = reconciled.head_sha
    if new_head == addressed.reviewed_head_sha:
        _block_unverifiable_remediation_round(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            context={
                "detection": "no_confirmed_push",
                "execution_id": str(execution.id),
                "review_iteration_id": str(addressed.id),
                "addressed_head_sha": addressed.reviewed_head_sha or "",
                "reconciled_head_sha": new_head,
            },
        )
        return None
    # A concurrently continued subject — the review for the confirmed head is
    # already reserved — owns its own state; this continuation applies nothing.
    current_iterations = review_records.list_review_loop_iterations(pool, review_loop_id=loop.id)
    if any(
        iteration.task_pull_request_id == reconciled.id and iteration.reviewed_head_sha == new_head
        for iteration in current_iterations
    ):
        return None
    reloaded_task = task_records.get_task(pool, task_id)
    if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    return SettledPrRemediationVerification(
        task=reloaded_task,
        pull_request=reconciled,
        addressed_iteration=addressed,
        finalized_execution=execution,
        new_head_sha=new_head,
    )
