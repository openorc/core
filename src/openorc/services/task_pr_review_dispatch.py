"""E9 §1: exact PR admission and ordinary Reviewer dispatch (issue #84).

E9 continues the same persistent Producer ↔ Reviewer conversation over the
one canonical GitHub PR and its exact reconciled head. This module is the
PR-review dispatch seam the PR decision loop (``task_pr_review_decision``)
consumes: given E8's durable ``TaskPrPublished`` facts (the post-transition
``reviewing`` Task and the one canonical ``TaskPullRequest``), it reconciles
the canonical PR authoritatively, resolves the latest appropriate successful
Producer ``implementation_result`` as durable evidence, reserves exactly one
next-numbered unfinalized PR/head ReviewIteration, and sends ONE
``ReviewInteraction`` to the exact admitted READY Reviewer session whose
entire model-visible message is the canonical structural JSON serialization
of the validated persisted ``implementation_result`` — no prefix, no PR
number/SHA prose, no rubric, no new prompt template.

Authority boundaries (mirroring the merged planning seams E3a/E3b/E3c/E4 and
E8):

- **Every authority-bearing fact is re-derived from current persistence.**
  The typed ``TaskPrPublished`` command is the triggering facts, never the
  authority: the canonical PR, the Task, the Reviewer session, and the PR
  loop are reloaded, and the exact head is the freshly reconciled GitHub
  head SHA — never the Producer's claim and never the command's stale copy.
  A later remediation/head-update dispatch reuses the same canonical PR;
  E8's composition/publication seam is never re-invoked.
- **Authenticated admission precedes the system-capable reconciliation.**
  The account-operational barrier and the exact authenticated Owner
  Workspace/Task scope are established in one short database-only
  transaction that is closed before ``reconcile_task_pull_request`` is
  invoked, so an invocation targeting another Owner's Task can never trigger
  a GitHub read or mutate the target PR's persisted reconciliation snapshot
  (fail-closed at the authorization boundary; cross-Workspace/cross-Task
  subjects are uniformly the not-found vocabulary). Phase B re-proves both
  under the Task/loop locks, so the admission phase gates external
  reachability only and replaces nothing.
- **The reconciled head is the review subject.** A head change observed here
  is review-subject invalidation of earlier head-bound currency; base-branch
  movement alone never is. A GitHub-observed changed head never fabricates a
  new Producer completion: ordinary dispatch for a new head requires a
  corresponding later successful ``implementation_result`` — the initial E6
  result for the initial head, otherwise a successful Execution result
  recorded for the latest-numbered finalized PR iteration (the remediation
  of that result). Stale evidence never dispatches.
- **One ordinary dispatch per head.** Under the Task row ``FOR UPDATE`` and
  the loop row ``FOR UPDATE``, ordinary dispatch refuses unresolved earlier
  attempts, duplicate/replayed dispatch for a head the loop has already
  ordinarily reviewed, and attempts on a superseded head. Additional
  same-head iterations are allowed only through E4's separate formal
  discussion-continuation path (the settlement seam in
  ``task_pr_review_decision``), never through this seam. A single OPEN
  ``PR_REVIEW`` loop is established once if absent, snapshotting the current
  Workspace ``review_iteration_limit``; a closed/duplicate loop never
  reopens or gets replaced, and planning loops are never selected or
  consulted for the PR budget.
- **The one send is outside transactions.** The exact Reviewer attachment is
  acquired through the deferred factory bound to the Phase-1 external
  session identity only after authorization has committed; the persisted
  reconciled head is re-read immediately before the send (a head that moved
  after the reservation makes the dispatch a never-dispatched dead letter
  bound to a superseded head: no verdict, no event, no gate, no second
  send); and no database transaction spans the attachment or the send.
  Known failure and uncertain outcome stay distinct; an uncertain send
  leaves the reserved iteration unresolved and is never blindly replayed.

Review-subject settlement, Owner discussion, remediation routing, and the
merge decision belong to the sibling E9 modules and later layers (E10,
issue #85).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.ownership import Repository
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
from openorc.persistence import gates as gate_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.executions import ExecutionStatus
from openorc.persistence.ownership import get_repository, get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.interaction import PrReviewSubject, ReviewInteraction
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    SCHEMA_VERSION,
    ReviewResult,
)
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_pr_composition import TaskPrPublished
from openorc.services.task_pull_request_reconciliation import (
    TaskPullRequestReconciliation,
    reconcile_task_pull_request,
)
from openorc.services.task_subject_guards import (
    require_current_task,
    require_task_pull_request_head,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "ReviewerRuntimeAttachmentFactory",
    "TaskPrReviewDispatchOutcome",
    "dispatch_pr_review",
    "serialize_implementation_result_message",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_review_dispatch"
_DISPATCH_SPAN_NAME = "task_pr_review_dispatch.dispatch_pr_review"

logger = logging.getLogger(__name__)

# The deferred Reviewer attachment seam: the service decides WHEN the adapter
# is acquired; the caller owns HOW the production attachment is composed (the
# #190 BOUND ``attach_cline_runtime`` composition). No adapter is ever
# acquired before the Phase-A0 authorization transaction has committed.
ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]

# The GitHub adapter capabilities the reconciliation path of this seam
# actually consumes through B7: the stable-ID installation repository
# observation and the authoritative pull-request read.
_GITHUB_RECONCILE_CAPABILITIES = (
    "get_installation_repository",
    "get_repository_pull_request",
)


@dataclass(frozen=True, slots=True)
class TaskPrReviewDispatchOutcome:
    """The internal continuation input for the PR review decision.

    Mirrors the planning dispatch contract: the reserved exact-subject
    attempt (the OPEN ``PR_REVIEW`` loop, the unfinalized PR/head iteration),
    the validated typed ``ReviewResult`` the Reviewer returned, and the exact
    READY Reviewer binding that received the REVIEW. No verdict is recorded
    and no workflow consequence is applied here — the decision module records
    the iteration result and its consequence in one transaction, rechecking
    every exact subject this dispatch proved.
    """

    review_loop: ReviewLoop
    review_iteration: ReviewIteration
    review_result: ReviewResult
    reviewer_session: TaskAgentSession
    task: Task
    pull_request: TaskPullRequest
    reconciled_head_sha: str


# --- command validation ------------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
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


def _require_github(value: object) -> GitHubAppClient:
    """Verify the GitHub capabilities this seam's reconciliation consumes.

    ``GitHubAppClient`` is a deliberately structural Protocol seam — services
    never import the concrete HTTP client and supported fakes implement only
    the capabilities their seam consumes — so the entry boundary verifies the
    B7 capabilities rather than demanding full-protocol conformance from
    every fake. The check runs before any transaction or durable effect.
    """
    for capability in _GITHUB_RECONCILE_CAPABILITIES:
        if not callable(getattr(value, capability, None)):
            raise InvalidCommandError("github must be a GitHubAppClient")
    return cast(GitHubAppClient, value)


def _require_published(value: object) -> TaskPrPublished:
    """Require E8's typed publication facts as the triggering command.

    The facts are triggering, never authority: every authority-bearing fact
    is re-derived from current persistence and from the authoritative
    reconciliation. Only the shape is validated here, before telemetry.
    """
    if not isinstance(value, TaskPrPublished):
        raise InvalidCommandError(
            "published must be the TaskPrPublished facts of the canonical PR publication"
        )
    if not isinstance(value.task, Task) or not isinstance(value.pull_request, TaskPullRequest):
        raise InvalidCommandError(
            "published must carry the post-publication Task and canonical TaskPullRequest facts"
        )
    return value


def _require_ready_reviewer(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Resolve the exact admitted READY Reviewer role/session binding.

    PR review reuses the original admitted Reviewer binding only: a missing,
    cross-Workspace, or cross-Task binding is the uniform not-found outcome;
    a CONNECTING/LOST/ENDED binding is a conflict. No rebinding, second
    session, or fresh initialization ever happens here — session loss is a
    blocking/recovery condition, never a silent replacement.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; the review uses the exact "
            "admitted session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None:
        raise ConflictError("the reviewer session binding has no external session identity")
    return binding


# --- the canonical PR loop ---------------------------------------------------------


def _require_or_establish_pr_loop(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    iteration_limit: int,
) -> ReviewLoop:
    """Resolve the Task's single OPEN ``PR_REVIEW`` loop, establishing it once.

    The PR loop is established exactly once per Task on first PR dispatch,
    snapshotting the Workspace's current positive ``review_iteration_limit``
    (the stored snapshot is authoritative — later Workspace setting edits
    affect future loops only). Planning loops are never selected or consulted
    for the PR budget; a closed PR loop is never reopened or replaced.
    """
    existing = [
        loop
        for loop in review_records.list_task_review_loops(transaction_pool, task_id=task_id)
        if loop.purpose is ReviewLoopPurpose.PR_REVIEW
    ]
    if existing:
        if len(existing) > 1:
            raise ConflictError(
                "the task carries more than one pr_review loop; the canonical "
                "single-loop invariant is violated"
            )
        loop = existing[0]
        if loop.status is not ReviewLoopStatus.OPEN:
            raise ConflictError(
                "the task's pr_review loop is closed; closed loops are never reopened or replaced"
            )
        return loop
    return review_records.create_review_loop(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=iteration_limit,
    )


def _require_no_unresolved_attempt(iterations: Sequence[ReviewIteration]) -> None:
    """Refuse another ordinary REVIEW while an earlier attempt is unresolved.

    Any unfinalized iteration in the loop is an earlier review attempt whose
    outcome is unknown-or-unrecorded. Dispatching a second REVIEW would
    bypass it (and the decision finalization that must settle it), so the
    dispatch is refused; the unresolved attempt is never superseded,
    rewritten, or silently replaced.
    """
    for iteration in iterations:
        if iteration.outcome is None:
            raise ConflictError(
                "an earlier PR review attempt in this loop is still unresolved; a new "
                "REVIEW cannot bypass it and the attempt is never superseded, "
                "rewritten, or replaced"
            )


def _require_no_iteration_for_current_head(
    iterations: Sequence[ReviewIteration], pull_request: TaskPullRequest, head_sha: str
) -> None:
    """Refuse another ordinary REVIEW of a head the loop has already reviewed.

    The exact reconciled head may be dispatched for ordinary REVIEW only
    while no iteration in the loop binds it yet: after a finalization, only a
    GitHub-confirmed new head with corresponding remediation evidence may
    launch the next ordinary REVIEW. Additional same-head iterations are
    allowed only through E4's separate formal discussion-continuation path,
    which never uses this seam.
    """
    for iteration in iterations:
        if (
            iteration.task_pull_request_id == pull_request.id
            and iteration.reviewed_head_sha == head_sha
        ):
            raise ConflictError(
                "the canonical pull request head already has a review iteration in "
                "this pr_review loop; only a GitHub-confirmed new head with "
                "corresponding remediation evidence may launch another ordinary REVIEW"
            )


def _reserve_iteration(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    loop: ReviewLoop,
    iterations: Sequence[ReviewIteration],
    task_pull_request_id: UUID,
    reviewed_head_sha: str,
) -> ReviewIteration:
    """Atomically reserve one next-numbered unfinalized PR/head iteration.

    The caller holds the loop row lock ``FOR UPDATE`` in its transaction, so
    concurrent dispatches serialize before numbering; the unique per-loop
    ``iteration_number`` constraint and the loop-OPEN creation guard are the
    durable backstops. A competing committed reservation is a stale
    operation — never a second attempt to bypass the first.
    """
    next_number = max((iteration.iteration_number for iteration in iterations), default=0) + 1
    try:
        reserved = review_records.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=next_number,
            task_pull_request_id=task_pull_request_id,
            reviewed_head_sha=reviewed_head_sha,
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
            "the pr_review loop is no longer open at reservation time; the dispatch is stale"
        )
    return reserved


# --- durable Producer completion evidence -------------------------------------------


def _resolve_initial_authorized_head(transaction_pool: DatabasePool, *, task_id: UUID) -> str:
    """Derive the initial authorized PR head from durable E7 authorization.

    The initial head is the exact subject of the Task's durably ``APPROVED``
    ``PR_AUTHORIZATION`` gate — the authorization whose approval authorized
    E8's publication of that head — re-derived from gate history inside the
    caller's Task-locked transaction. The E8 command's publication facts are
    a typed trigger only and are never consulted here: a caller-supplied
    snapshot of the currently reconciled head after an unrelated GitHub head
    change can never upgrade the stale initial completion into evidence.
    The durable ``pr_authorization`` subject form binds exactly one head SHA
    and no PR binding, so an incoherent history (no approval, or more than
    one) refuses the dispatch.
    """
    pr_gates = [
        gate
        for gate in gate_records.list_task_owner_gates(transaction_pool, task_id=task_id)
        if gate.gate_type is OwnerGateType.PR_AUTHORIZATION
    ]
    approved = [gate for gate in pr_gates if gate.status is OwnerGateStatus.APPROVED]
    if len(approved) != 1:
        raise ConflictError(
            "the task's pr authorization history does not bind exactly one durably "
            "approved initial head; the initial dispatch is refused"
        )
    initial_head = approved[0].subject_head_sha
    if initial_head is None:
        # Unreachable for the domain's exact pr_authorization subject form;
        # fail closed rather than derive an absent authority.
        raise ConflictError(
            "the approved pr authorization gate carries no head-SHA subject; the "
            "initial dispatch is refused"
        )
    return initial_head


def _validate_dispatch_evidence_envelope(
    payload: object, *, canonical_feature_branch: str
) -> dict[str, object]:
    """Re-derive the persisted completion's canonical envelope and branch.

    Mirrors E7's durable-fact re-derivation of the qualifying E6 result
    (never a parser rerun, never a trust of an in-memory typed result): the
    envelope must be the ``implementation_result`` family at the pinned
    protocol version with a completed status and a usable branch claim, and
    the branch claim must correspond to the Task's currently bound canonical
    feature branch. A wrong-branch result never authorizes a ReviewIteration
    regardless of its Execution status or the current GitHub head — this
    closes the bypass where a wrong-branch remediation completion that
    correctly refused its own redispatch would otherwise be picked up by a
    later dispatch as qualifying evidence.
    """
    if not isinstance(payload, dict):
        raise ConflictError(
            "the persisted implementation completion is not the canonical "
            "implementation_result envelope; no review dispatch is produced"
        )
    if payload.get("type") != IMPLEMENTATION_RESULT_FAMILY:
        raise ConflictError(
            "the persisted implementation completion is not a validated "
            "implementation_result envelope; no review dispatch is produced"
        )
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ConflictError(
            "the persisted implementation completion carries an unsupported schema "
            "version; no review dispatch is produced"
        )
    if payload.get("status") != "COMPLETED":
        raise ConflictError(
            "the persisted implementation completion is not a completed "
            "implementation; no review dispatch is produced"
        )
    branch = payload.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        raise ConflictError(
            "the persisted implementation completion carries no usable branch claim; "
            "no review dispatch is produced"
        )
    if branch != canonical_feature_branch:
        raise ConflictError(
            "the persisted implementation completion claims a branch other than the "
            "task's bound canonical feature branch; it never qualifies as review "
            "evidence"
        )
    return payload


def _resolve_dispatch_evidence(
    transaction_pool: DatabasePool,
    *,
    task_id: UUID,
    canonical_feature_branch: str,
    reconciled_head_sha: str,
    initial_authorized_head_sha: str,
    loop: ReviewLoop | None,
) -> dict[str, object]:
    """Resolve the latest appropriate successful Producer completion evidence.

    The durable evidence for the reconciled head is the latest successful
    Execution with a non-NULL validated ``implementation_result`` whose
    canonical envelope re-derives and whose branch claim corresponds to the
    Task's bound canonical feature branch: the initial E6 result for the
    durably authorized initial head, otherwise a successful Execution
    recorded for the latest-numbered finalized PR iteration (its one-shot
    remediation claim). A GitHub-observed changed head never fabricates a
    Producer completion, and stale or wrong-branch evidence never
    dispatches: a changed head without corresponding remediation evidence —
    or a completed-but-wrong-branch result — refuses with no reservation, no
    matter what head the caller's triggering facts carry.
    """
    history = [
        execution
        for execution in execution_records.list_task_executions(transaction_pool, task_id=task_id)
        if execution.status is ExecutionStatus.SUCCEEDED
        and execution.implementation_result is not None
    ]
    if not history:
        raise ConflictError(
            "no successful Producer implementation completion is recorded for this "
            "task; there is no durable evidence to dispatch for review"
        )
    evidence = history[-1]
    payload = _validate_dispatch_evidence_envelope(
        evidence.implementation_result, canonical_feature_branch=canonical_feature_branch
    )
    iterations = (
        []
        if loop is None
        else review_records.list_review_loop_iterations(transaction_pool, review_loop_id=loop.id)
    )
    finalized = [iteration for iteration in iterations if iteration.outcome is not None]
    if evidence.review_iteration_id is None:
        # Initial evidence: the initial E6 completion. It is valid exactly
        # while the reconciled head is still the durably authorized initial
        # head — a GitHub-observed head change with no remediation round
        # never dispatches the stale initial result, whatever head the
        # caller's triggering facts carry.
        if reconciled_head_sha != initial_authorized_head_sha:
            raise StaleOperationError(
                "the pull request head changed after the initial implementation "
                "completion with no corresponding successful remediation; the "
                "stale evidence is never dispatched"
            )
        return payload
    claimed = next(
        (iteration for iteration in finalized if iteration.id == evidence.review_iteration_id),
        None,
    )
    if (
        claimed is None
        or claimed.outcome is not ReviewOutcome.CHANGES_REQUESTED
        or (finalized and claimed.id != finalized[-1].id)
    ):
        # The claimed iteration is not the latest finalized CHANGES_REQUESTED
        # result of this loop: the evidence is superseded and never dispatches.
        raise StaleOperationError(
            "the recorded remediation completion is superseded by a newer finalized "
            "review result; the stale evidence is never dispatched"
        )
    return payload


def serialize_implementation_result_message(payload: dict[str, object]) -> str:
    """The model-visible PR-review message: the persisted result, structurally.

    The entire message is the canonical structural JSON serialization of the
    validated persisted ``implementation_result`` envelope — every
    substantive field preserved verbatim, with no prefix, no PR number or
    SHA prose, no rubric, no protocol instruction text, and no new prompt
    template. The assigned PR/head identity rides only in the typed
    ``PrReviewSubject`` interaction context, which the Reviewer never
    establishes authority by echoing.
    """
    message: dict[str, object] = {
        "type": IMPLEMENTATION_RESULT_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "status": payload.get("status"),
        "branch": payload.get("branch"),
        "summary": payload.get("summary"),
        "changes": payload.get("changes", []),
        "validation": payload.get("validation", []),
    }
    if payload.get("notes") is not None:
        message["notes"] = payload.get("notes")
    return json.dumps(message)


# --- eligibility --------------------------------------------------------------------


def _require_review_eligible_pull_request(
    *,
    pull_request: TaskPullRequest,
    task: Task,
    repository: Repository,
) -> None:
    """Require the freshly reconciled canonical PR to be eligible for review.

    The record must remain open and unmerged, with the Task's bound canonical
    head branch and the durable workflow base routing (the routed Repository
    record's default branch — never the Producer's claim). A closed-unmerged
    PR, a confirmed merge, a retargeted base, or a changed head branch each
    fails closed with zero durable workflow effects — never replacement,
    adoption, or dispatch. Generalized closed-PR and merge recovery belongs
    to E10 (issue #85).
    """
    if pull_request.state is not TaskPullRequestState.OPEN:
        if pull_request.merged_at is None:
            raise ConflictError(
                "the canonical pull request is closed and unmerged; closed-PR "
                "recovery belongs to the later recovery layer and this dispatch "
                "applies nothing"
            )
        raise ConflictError(
            "the canonical pull request is already merged; merge confirmation "
            "and task completion belong to the later recovery layer"
        )
    if task.canonical_feature_branch is None:
        raise ConflictError(
            "the task carries no bound canonical feature branch; the review subject is incomplete"
        )
    if pull_request.head_ref != task.canonical_feature_branch:
        raise StaleOperationError(
            "the pull request head branch no longer matches the task's canonical "
            "feature branch; the review subject is invalidated and the dispatch "
            "applies nothing"
        )
    default_branch = repository.metadata.default_branch
    if default_branch is None or not default_branch.strip():
        raise ConflictError(
            "the routed repository carries no default branch; the base routing cannot be verified"
        )
    if pull_request.base_ref != default_branch:
        raise StaleOperationError(
            "the pull request base no longer matches the workflow base routing; "
            "the dispatch applies nothing"
        )


# --- the ONE send -------------------------------------------------------------------


def _realize_pr_review_control(
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
    attempt unresolved for the decision finalization to reconcile.
    """
    try:
        outcome = runtime.realize_control(
            session_id, interaction, message=message, expected_family=REVIEW_RESULT_FAMILY
        )
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "pr review dispatch ended with an uncertain runtime outcome; "
            "the reserved review attempt remains unresolved"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the agent runtime pr review interaction is unknown; "
            "the reserved review attempt remains unresolved"
        ) from error
    except AgentRuntimeError as error:
        raise ExternalOperationFailedError(
            "the agent runtime pr review interaction failed; the reserved review "
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


def _revalidate_reserved_subjects(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    expected_pull_request_id: UUID,
    reserved_head_sha: str,
    reviewer_session: TaskAgentSession,
) -> Task:
    """The post-attachment currentness proof: database only, before the send.

    Attachment acquisition takes wall-clock time, so the exact reserved
    subjects are re-proven under the Task row ``FOR UPDATE`` after the
    Reviewer attachment is acquired and immediately before the send: the
    exact current non-archived ``REVIEWING`` Task at the caller's expected
    token with no current OwnerGate, the canonical PR still carrying exactly
    the reserved head and identity, and the exact admitted READY Reviewer
    binding. Cancellation, a gate transition, or Reviewer-session loss
    during acquisition refuses the dispatch as a never-dispatched dead
    letter bound to the reserved subjects: no message is sent, no
    verdict/event/gate exists, and the reserved iteration stays unfinalized
    as an explicit recoverable blocked state — never silently bypassed and
    never re-dispatched blind. The transaction closes before the send; no
    GitHub I/O occurs inside it.

    The entire proof runs inside ONE composed transaction: the Task row lock
    taken by ``get_task_for_update`` is held to the transaction's commit, so
    the token/status, PR identity/head, and Reviewer-binding checks all read
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
        if task.status is not TaskStatus.REVIEWING:
            raise ConflictError(
                "the addressed task is no longer reviewing; the dispatch is refused "
                "without sending and the reserved attempt stays unfinalized for recovery"
            )
        if task.current_owner_gate_id is not None:
            raise ConflictError(
                "the task carries a current owner gate; the dispatch is refused without "
                "sending and the reserved attempt stays unfinalized for recovery"
            )
        pull_request = require_task_pull_request_head(
            transaction_pool, task=task, expected_head_sha=reserved_head_sha
        )
        if pull_request.id != expected_pull_request_id:
            raise StaleOperationError(
                "the canonical pull request identity changed after the review "
                "reservation; the dispatch is refused without sending and the "
                "reserved attempt stays unfinalized for recovery"
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


# --- the dispatch operation ---------------------------------------------------------


def dispatch_pr_review(
    pool: DatabasePool,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    published: TaskPrPublished,
) -> TaskPrReviewDispatchOutcome:
    """Dispatch the canonical PR's ordinary review to the admitted Reviewer.

    Ordered phases (authorization before every adapter acquisition):

    **Phase A0 — authenticated admission (database only, closed before any
    external call):** the account-operational barrier and the exact
    authenticated Owner Workspace/Task scope. Nothing else; no durable
    workflow effects. A foreign or cross-Owner invocation is rejected here,
    before any GitHub read or PR-snapshot write can occur.

    **Phase A — authoritative reconciliation (no transaction open):** B7
    reconciles the canonical PR against GitHub; the reconciled record must be
    open/unmerged, on the canonical head branch, on the workflow base routing,
    with the fresh reconciled head SHA. The latest appropriate successful
    ``implementation_result`` is resolved as durable evidence for the head.

    **Phase B — reservation transaction (database only):** the account
    barrier and Owner scope are re-proved under the Task row ``FOR UPDATE``;
    the Task must be non-archived ``REVIEWING`` at the expected token with no
    current OwnerGate and a bound canonical feature branch; the canonical PR
    must still carry exactly the reconciled head; the exact admitted READY
    Reviewer session is resolved; the single OPEN ``PR_REVIEW`` loop is
    established once if absent (snapshotting the Workspace limit); and, under
    the loop row lock, one next-numbered unfinalized PR/head iteration is
    reserved — after refusing unresolved earlier attempts and duplicate
    dispatch for an already-reviewed head.

    **Phase C — the post-attachment currentness proof (one short
    database-only transaction, no GitHub I/O):** after the exact Reviewer
    attachment is acquired, the reserved Task/token/``REVIEWING`` status,
    the canonical PR identity/head, and the exact Reviewer binding are
    re-proven under the Task row ``FOR UPDATE``; any moved subject fails
    closed without sending.

    **Phase D — the ONE send (outside transactions):** with the revalidation
    transaction closed, ONE ``ReviewInteraction`` with an exact
    ``PrReviewSubject`` is realized with ``expected_family=REVIEW_RESULT_FAMILY``
    on the original external session, the entire message being the serialized
    persisted ``implementation_result``. Known failure and uncertain outcome
    stay distinct; an uncertain send leaves the reserved iteration unresolved.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        reviewer_factory = _require_reviewer_factory(reviewer_runtime_factory)
        github_client = _require_github(github)
        published_facts = _require_published(published)
        annotate_span(
            span,
            operation=_DISPATCH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase A0: authenticated admission (database only, closed first). ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )

        # --- Phase A: authoritative reconciliation (no transaction open). ---
        reconciliation: TaskPullRequestReconciliation = reconcile_task_pull_request(
            pool,
            github_client,
            workspace_id=workspace_id,
            task_id=task_id,
        )
        reconciled = reconciliation.pull_request
        task = task_records.get_task(pool, task_id)
        if task is None or task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        if reconciled.task_id != task.id or reconciled.workspace_id != workspace_id:
            raise NotFoundError("the requested pull request record is not available for this task")
        if published_facts.pull_request.id != reconciled.id:
            raise NotFoundError(
                "the addressed publication facts do not name the task's canonical "
                "pull request record"
            )
        repository = get_repository(pool, task.repository_id)
        if repository is None or repository.workspace_id != workspace_id:
            raise NotFoundError("the requested repository is not available in this workspace")
        _require_review_eligible_pull_request(
            pull_request=reconciled, task=task, repository=repository
        )
        head_sha = reconciled.head_sha

        # --- Phase B: reservation transaction (database only). ---
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
                    "the addressed task is not reviewing; the ordinary PR review "
                    "dispatch is not available"
                )
            if task.current_owner_gate_id is not None:
                raise ConflictError(
                    "the task carries a current owner gate; the ordinary PR review "
                    "dispatch is not available"
                )
            # The persisted reconciled head is revalidated under the Task lock:
            # a concurrent durable change rolls the reservation back — no
            # reservation commits against a moved subject.
            pull_request = require_task_pull_request_head(
                transaction_pool, task=task, expected_head_sha=head_sha
            )
            if (
                pull_request.id != reconciled.id
                or pull_request.github_pr_id != reconciled.github_pr_id
            ):
                raise StaleOperationError(
                    "the canonical pull request identity changed during dispatch; "
                    "the operation is stale"
                )
            reviewer = _require_ready_reviewer(
                transaction_pool, workspace_id=workspace_id, task_id=task_id
            )
            workspace = get_workspace(transaction_pool, workspace_id)
            if workspace is None:
                raise NotFoundError("the requested workspace is not available")
            loop = _require_or_establish_pr_loop(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                iteration_limit=workspace.review_iteration_limit,
            )
            locked_loop = review_records.get_review_loop(
                transaction_pool, review_loop_id=loop.id, for_update=True
            )
            if locked_loop is None or locked_loop.id != loop.id:
                raise StaleOperationError("the pr_review loop has moved on; the dispatch is stale")
            iterations = review_records.list_review_loop_iterations(
                transaction_pool, review_loop_id=locked_loop.id
            )
            _require_no_unresolved_attempt(iterations)
            _require_no_iteration_for_current_head(iterations, pull_request, head_sha)
            payload = _resolve_dispatch_evidence(
                transaction_pool,
                task_id=task_id,
                canonical_feature_branch=task.canonical_feature_branch or "",
                reconciled_head_sha=head_sha,
                initial_authorized_head_sha=_resolve_initial_authorized_head(
                    transaction_pool, task_id=task_id
                ),
                loop=locked_loop,
            )
            reserved = _reserve_iteration(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                loop=locked_loop,
                iterations=iterations,
                task_pull_request_id=pull_request.id,
                reviewed_head_sha=head_sha,
            )

        # --- Phases C and D: the ONE send, with NO database transaction open. ---
        repository_full_name = f"{repository.metadata.owner_login}/{repository.metadata.name}"
        subject = PrReviewSubject(
            repository_full_name=repository_full_name,
            pull_request_number=reconciled.github_pr_number,
            head_sha=head_sha,
            issue_number=task.github_issue_number,
        )
        interaction = ReviewInteraction(subject=subject)
        message = serialize_implementation_result_message(payload)
        with reviewer_factory(reviewer) as runtime:
            # Phase C: the post-attachment currentness proof — one short
            # database-only transaction re-proving the exact reserved
            # Task/token/status, PR identity/head, and Reviewer binding after
            # acquisition and immediately before the send. The transaction
            # closes before the send; no database transaction spans it.
            _revalidate_reserved_subjects(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                expected_pull_request_id=pull_request.id,
                reserved_head_sha=head_sha,
                reviewer_session=reviewer,
            )
            review_result = _realize_pr_review_control(
                runtime,
                reviewer.external_session_id or "",
                interaction=interaction,
                message=message,
            )

        # --- the internal continuation input for the decision. ---
        # No verdict is recorded and no workflow consequence is applied here:
        # the decision module records the iteration result and its consequence
        # in one transaction, rechecking the exact Task/token/PR/head/session
        # subjects this dispatch proved.
        return TaskPrReviewDispatchOutcome(
            review_loop=locked_loop,
            review_iteration=reserved,
            review_result=review_result,
            reviewer_session=reviewer,
            task=task,
            pull_request=pull_request,
            reconciled_head_sha=head_sha,
        )
