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
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Final, cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTransportError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import Execution
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
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
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
    ReviewResult,
)
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.review_result_envelope import serialize_review_result
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
    "ReviewerRuntimeAttachmentFactory",
    "TaskPrRemediationOutcome",
    "route_pr_remediation",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_remediation"
_REMEDIATION_SPAN_NAME = "task_pr_remediation.route_pr_remediation"

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
        producer = _require_ready_producer(
            transaction_pool, workspace_id=workspace_id, task_id=task_id
        )
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
            raise ConflictError(
                "the addressed review loop is not open; remediation applies nothing"
            )
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
            raise ConflictError(
                "the addressed review iteration is not finalized; nothing is routable"
            )
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
        claimed = execution_records.claim_execution_running(
            transaction_pool, execution_id=execution.id
        )
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
            workspace_id=workspace_id,
            task_id=task_id,
            execution_id=execution_id,
            actor=event_coordination.producer_actor(producer_session.id),
        )
    return current_task, finalized


def _send_remediation_revision(
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
