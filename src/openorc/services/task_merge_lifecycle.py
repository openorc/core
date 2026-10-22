"""E10a: exact-head merge authorization and GitHub-confirmed completion (issue #227).

Two focused seams share one transactional discipline (database-only
compositions, the Task row lock taken for consequential transitions, external
consequences strictly outside, every exact subject re-proven under the lock):

- ``request_task_merge`` is the ONE Owner merge command: at the Task's exact
  current pending ``MERGE_DECISION`` gate it proves the same Profile's
  Workspace/Task Owner authority, the gate's exact canonical
  ``TaskPullRequest`` + Reviewer-accepted head binding, the OPEN ``PR_REVIEW``
  loop, and — through authoritative B7 reconciliation and the routed
  installation's issue read — that the PR is still open/unmerged and the
  source issue is still open. One short composed transaction then resolves
  the gate ``APPROVED`` and records the OWNER ``OWNER_GATE_RESOLVED`` +
  ``MERGE_REQUESTED`` audit pair: the durable one-shot claim after which no
  replay, duplicate approval, or stale token can issue another GitHub merge
  request. The existing ``pull_request_merge.request_pull_request_merge``
  primitive is called exactly once with the gate's accepted SHA and the
  approving Profile, its final pre-send transaction re-proving the durable
  claim through the supplied ``claim_revalidation`` hook. Every outcome is
  classified by which external effect actually occurred: a confirmed merge
  completes the Task through the shared completion seam; a GitHub-owned
  refusal and a definitive pre-delivery failure are distinguished by the
  primitive's machine-readable error subtype (only the former records
  ``MERGE_REJECTED_BY_GITHUB``), and after an authoritative no-merge proof a
  NEW pending exact-head ``MERGE_DECISION`` gate requires fresh Owner
  authorization; an expected-head mismatch blocks ``STALE_OPERATION``; an
  uncertain or unconfirmable outcome blocks ``EXTERNAL_OPERATION_UNCERTAIN``
  and is never repeated.

- ``complete_confirmed_task_merge`` is the ONE GitHub-authoritative completion
  seam, shared by the Owner-initiated path and the B6 reconciliation
  consequence (webhook dispatch and the periodic sweep invoke it after a
  successfully reconciled merged canonical PR). It requires no
  ``MERGE_DECISION`` and no user OAuth token and cannot itself call GitHub:
  it reloads the authoritative persisted PR/current-Task facts under lock —
  webhook-asserted inputs are never trusted — and composes ONE atomic
  transition: close the OPEN ``PR_REVIEW`` loop (absent is legitimate; the
  planning loop is never touched), archive the Task ``COMPLETED`` through the
  guarded archival primitive (which releases the current-issue mapping and
  leaves no actionable current gate on the archived Task without inventing
  any Owner gate event), and commit ``PR_MERGED`` (GITHUB) plus the
  terminal ``TASK_COMPLETED`` (OPENORC) exactly once. Archived ``CANCELLED``
  history is immutable; an already-``COMPLETED`` archive is an idempotent
  no-op, so duplicate webhook/sweep/Owner completion races produce at most
  one archival, loop closure, and event set.

E10a does not own general cancellation/PR closing (#228), closed-unmerged
terminal consequences (#229), reason-specific blocked-Task recovery (F4b
#230), checkout deletion (F4c #183), or delivery machinery (F1–F5).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)
from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import gates as gate_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request
from openorc.services import (
    event_coordination,
    pull_request_merge,
    task_mutations,
    task_pull_request_reconciliation,
)
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    MergeConfirmedReconciliationPendingError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.github_owner_write_authorization import (
    require_owner_write_authorization,
)
from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_subject_guards import (
    require_canonical_task_pull_request,
    require_current_owner_gate,
    require_current_task,
    require_task_pull_request_head,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "TaskMergeCompletionKind",
    "TaskMergeCompletionOutcome",
    "TaskMergeOutcome",
    "TaskMergeOutcomeKind",
    "complete_confirmed_task_merge",
    "request_task_merge",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_merge_lifecycle"
_REQUEST_MERGE_SPAN_NAME = "task_merge_lifecycle.request_task_merge"
_COMPLETE_MERGE_SPAN_NAME = "task_merge_lifecycle.complete_confirmed_task_merge"

_OPENORC_ACTOR: Final = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)
_GITHUB_ACTOR: Final = event_coordination.WorkflowActorContext(WorkflowEventActor.GITHUB, None)

_GITHUB_ISSUE_OPEN_STATE: Final = "open"


class TaskMergeOutcomeKind(StrEnum):
    """The classified workflow outcomes of one Owner merge command.

    Every kind states which external effect actually occurred: a
    GitHub-confirmed merge completed the Task; a definitive no-merge outcome
    installed a NEW pending exact-head ``MERGE_DECISION`` gate (fresh Owner
    authorization required); an expected-head mismatch blocked
    ``STALE_OPERATION``; an unconfirmable outcome blocked
    ``EXTERNAL_OPERATION_UNCERTAIN``. Hard precondition failures raise typed
    errors instead of returning an outcome.
    """

    COMPLETED = "completed"
    RETRY_GATE_INSTALLED = "retry_gate_installed"
    BLOCKED_STALE_OPERATION = "blocked_stale_operation"
    BLOCKED_UNCERTAIN = "blocked_uncertain"


@dataclass(frozen=True, slots=True)
class TaskMergeOutcome:
    """The typed result of one Owner merge command.

    ``task`` is the post-transition Task; ``pull_request`` is the
    authoritative reconciled canonical PR the classification is grounded in.
    ``retry_gate`` is the NEW pending exact-head ``MERGE_DECISION`` gate for
    the retry outcome; ``task_block`` is the applied blocking condition for
    the blocked outcomes; ``merge_commit_sha`` carries GitHub's confirmed
    merge commit where the merge is known applied.
    """

    kind: TaskMergeOutcomeKind
    task: Task
    pull_request: TaskPullRequest
    retry_gate: OwnerGate | None = None
    task_block: TaskBlock | None = None
    merge_commit_sha: str | None = None


class TaskMergeCompletionKind(StrEnum):
    """The classified outcomes of one confirmed-merge completion invocation.

    ``COMPLETED`` applied the atomic terminal transition now;
    ``ALREADY_COMPLETED`` is the idempotent no-op a duplicate webhook, sweep,
    or Owner completion race observes on the already-archived ``COMPLETED``
    Task — no second archival, loop closure, or event set.
    """

    COMPLETED = "completed"
    ALREADY_COMPLETED = "already_completed"


@dataclass(frozen=True, slots=True)
class TaskMergeCompletionOutcome:
    """The typed result of one confirmed-merge completion invocation."""

    kind: TaskMergeCompletionKind
    task: Task
    pull_request: TaskPullRequest


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _is_confirmed_merged(pull_request: TaskPullRequest) -> bool:
    """Whether the canonical PR record affirmatively shows a GitHub merge.

    The exact stable GitHub PR identity is enforced by B7 reconciliation;
    completion requires the reconciled record to be CLOSED **and** carry its
    merge instant — a ``MERGED`` merge-API answer without such reconciled
    durable evidence is never enough.
    """
    return pull_request.state is TaskPullRequestState.CLOSED and pull_request.merged_at is not None


def _translate_github_issue_read_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified issue-read outcomes into the typed vocabulary.

    Authorization absence is the safe classified ``AuthorizationError``; a
    rate limit is a known provider condition, deliberately not an
    authorization absence; uncertain outcomes stay uncertain.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the addressed repository or issue is not accessible through the "
            "routed github installation"
        )
    if isinstance(error, GitHubOutcomeUncertainError):
        return ExternalOperationUncertainError(
            "the outcome of the authoritative GitHub issue read is unknown"
        )
    assert isinstance(
        error,
        (GitHubAuthenticationRejectedError, GitHubRateLimitedError, GitHubRequestRejectedError),
    )
    return ExternalOperationFailedError(
        "the authoritative GitHub issue read failed as a known provider condition"
    )


def _require_open_pr_review_loop(transaction_pool: DatabasePool, *, task: Task) -> None:
    """Prove the Task's single OPEN ``PR_REVIEW`` loop is still available.

    The Owner merge command addresses the PR-review loop E9 kept deliberately
    OPEN through the merge decision; a missing or non-unique open PR-review
    loop is a stale operation that applies nothing. The already-closed
    planning loop is never consulted.
    """
    open_loops = review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.OPEN
    )
    pr_loops = [loop for loop in open_loops if loop.purpose is ReviewLoopPurpose.PR_REVIEW]
    if len(pr_loops) != 1:
        raise StaleOperationError(
            "the task's open pr review loop is not uniquely available; the merge request is stale"
        )


def _require_source_issue_open(pool: DatabasePool, github: GitHubAppClient, *, task: Task) -> None:
    """Prove the Task's source issue is still open through the routed installation.

    The fresh stable repository identity is observed first (the documented
    stable-ID installation listing), and the issue read is addressed through
    that fresh owner/name — never stored mutable metadata. The returned
    observation must bind to the Task's exact stable issue identity, must not
    be a pull request viewed through the issues endpoint, and must report the
    open state. A GitHub-confirmed closed issue refuses the merge WITHOUT
    consuming, resolving, or rotating the pending merge decision — the Owner
    may reopen the issue and request the merge again through the same gate
    while it remains current. A missing, mismatched, unavailable, or
    uncertain read fails closed without treating the issue as open. Issue
    openness is an Owner-merge guard only: an independently confirmed
    external PR merge stays authoritative even when the issue is closed.
    """
    route = resolve_system_repository_installation_route(
        pool, workspace_id=task.workspace_id, repository_id=task.repository_id
    )
    try:
        repository_observation = github.get_installation_repository(
            github_installation_id=route.github_installation_id,
            github_repository_id=route.repository.identity.github_repository_id,
        )
        issue_observation = github.get_repository_issue(
            github_installation_id=route.github_installation_id,
            owner_login=repository_observation.owner_login,
            repository_name=repository_observation.name,
            issue_number=task.github_issue_number,
        )
    except (
        GitHubAuthorizationRejectedError,
        GitHubAuthenticationRejectedError,
        GitHubRateLimitedError,
        GitHubRequestRejectedError,
        GitHubOutcomeUncertainError,
    ) as error:
        raise _translate_github_issue_read_outcome(error) from error
    if issue_observation.is_pull_request:
        raise ConflictError(
            "the addressed issue number reports a pull request through the issues "
            "endpoint; the merge guard fails closed"
        )
    if issue_observation.github_issue_id != task.github_issue_id:
        raise ConflictError(
            "the addressed issue number reports a different stable identity; "
            "the merge guard fails closed"
        )
    if issue_observation.state != _GITHUB_ISSUE_OPEN_STATE:
        raise ConflictError(
            "the source issue is closed on GitHub; the merge request is refused "
            "and the pending merge decision stays usable after a reopen"
        )


def _merge_claim_revalidation(
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_post_approval_token: UUID,
    task_pull_request_id: UUID,
    expected_head_sha: str,
) -> Callable[[DatabasePool], None]:
    """Build the durable one-shot claim revalidation supplied to the merge primitive.

    The returned hook runs inside the primitive's final pre-send composed
    transaction and re-proves, under the Task row lock, the exact durable
    facts the one-shot approval claim committed: the Task is still current at
    the post-approval token, the addressed gate is still the APPROVED
    ``MERGE_DECISION`` bound to the exact canonical PR and accepted head, and
    the canonical PR is still open/unmerged at that head. Every failure is a
    typed stale operation: the hook grants no merge authority of its own and
    never mutates anything.
    """

    def validate(claim_pool: DatabasePool) -> None:
        locked = task_records.get_task_for_update(claim_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        if locked.archived_at is not None or locked.state_token != expected_post_approval_token:
            raise StaleOperationError(
                "the approved merge claim is no longer current; the merge request is stale"
            )
        gate = gate_records.get_owner_gate(claim_pool, owner_gate_id=owner_gate_id)
        if (
            gate is None
            or gate.workspace_id != workspace_id
            or gate.task_id != task_id
            or gate.status is not OwnerGateStatus.APPROVED
            or gate.task_pull_request_id != task_pull_request_id
            or gate.subject_head_sha != expected_head_sha
        ):
            raise StaleOperationError(
                "the approved merge claim no longer binds the addressed gate; "
                "the merge request is stale"
            )
        pull_request = get_task_pull_request(claim_pool, task_pull_request_id=task_pull_request_id)
        if (
            pull_request is None
            or pull_request.workspace_id != workspace_id
            or pull_request.task_id != task_id
            or pull_request.state is not TaskPullRequestState.OPEN
            or pull_request.merged_at is not None
            or pull_request.head_sha != expected_head_sha
        ):
            raise StaleOperationError(
                "the canonical pull request is no longer open at the claimed head; "
                "the merge request is stale"
            )

    return validate


def _apply_merge_staleness_block(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate_id: UUID,
    task_pull_request_id: UUID,
    expected_head_sha: str,
    observed_head_sha: str,
) -> tuple[TaskBlock, Task]:
    """Atomically block the current Task on merge-request staleness (issue #227).

    One short composed transaction re-proves the account-operational barrier
    and the exact Profile's Owner authority FIRST (the admission check does
    not remain authoritative across the intervening GitHub calls), then —
    under the Task row lock — creates the ``stale_operation`` TaskBlock with
    the safe exact-subject recovery context, moves the Task to ``blocked``
    with a rotated token, and records the ``task_blocked`` audit. A stale
    subject applies nothing and raises. This is the E10a-specific use of the
    existing TaskBlock/status/event primitives, not a second blocking engine.
    """
    require_owner_write_authorization(
        pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    with composed_transaction(pool) as transaction_pool:
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        block = block_records.create_task_block(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            reason=TaskBlockReason.STALE_OPERATION,
            context={
                "owner_gate_id": str(owner_gate_id),
                "task_pull_request_id": str(task_pull_request_id),
                "expected_head_sha": expected_head_sha,
                "observed_head_sha": observed_head_sha,
            },
        )
        blocked = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            status=TaskStatus.BLOCKED,
        )
        if blocked is None:
            raise StaleOperationError(
                "the task could not move to blocked; the merge-staleness block is stale"
            )
        event_coordination.record_task_blocked_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_block_id=block.id,
            actor=_OPENORC_ACTOR,
        )
        return block, blocked


def _install_retry_merge_gate(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    task_pull_request_id: UUID,
    subject_head_sha: str,
) -> tuple[OwnerGate, Task]:
    """Install a NEW pending exact-head ``MERGE_DECISION`` gate (issue #227).

    Used only after a definitive no-merge outcome (a GitHub-owned refusal or
    a proven pre-delivery failure) and a fresh authoritative reconciliation
    proving the same canonical PR remains open/unmerged at the unchanged
    accepted head. One short composed transaction re-proves the
    account-operational barrier and Owner authority FIRST, then creates the
    new pending gate bound to the exact canonical PR + head and installs it
    as the Task's current gate through the guarded primitives — the prior
    APPROVED gate is immutable history and is never reused. Fresh Owner
    authorization is required for another merge attempt; nothing here
    re-sends a merge request or rewrites review evidence.
    """
    require_owner_write_authorization(
        pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    with composed_transaction(pool) as transaction_pool:
        require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        gate = gate_records.create_owner_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            gate_type=OwnerGateType.MERGE_DECISION,
            plan_revision_id=None,
            subject_head_sha=subject_head_sha,
            task_pull_request_id=task_pull_request_id,
        )
        updated = task_mutations.set_current_owner_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate_id=gate.id,
        )
        event_coordination.record_owner_gate_created_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=gate.id,
            actor=_OPENORC_ACTOR,
        )
        return gate, updated


def complete_confirmed_task_merge(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskMergeCompletionOutcome:
    """Complete a still-current Task whose canonical PR is GitHub-confirmed merged.

    The ONE shared completion seam for Owner-initiated and externally
    performed merges. Callers must have just reconciled the canonical PR
    authoritatively (the B6 webhook/sweep paths, or the merge primitive's
    post-success reconciliation); the inputs are never trusted from a
    webhook assertion — the authoritative persisted PR/current-Task facts
    are reloaded here under lock and re-proved. Requires no
    ``MERGE_DECISION`` and no user OAuth token, and cannot itself call
    GitHub. On the still-current, locked, non-archived Task — whatever its
    PR-associated nonterminal status, including ``BLOCKED`` — one atomic
    composition closes the OPEN ``PR_REVIEW`` loop (absent is legitimate;
    the planning loop is never touched), archives the Task ``COMPLETED``
    through the guarded archival primitive, and commits ``PR_MERGED``
    (GITHUB) plus the terminal ``TASK_COMPLETED`` (OPENORC) exactly once.

    Archived ``CANCELLED`` history is immutable — a later merge of its old
    PR never resurrects or rewrites it. An already-archived ``COMPLETED``
    Task is the idempotent ``ALREADY_COMPLETED`` no-op, so duplicate
    webhook/sweep/Owner completion races produce at most one archival, loop
    closure, and event set. A canonical PR not confirmed merged in durable
    state fails closed and applies nothing.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with application_span(_SERVICE_TRACER_SCOPE, _COMPLETE_MERGE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_COMPLETE_MERGE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier (#97), derived from
            # durable Workspace ownership rather than an authenticated
            # caller: the FIRST lock acquisition of this write phase.
            workspace = get_workspace(transaction_pool, workspace_id)
            if workspace is None:
                raise NotFoundError("the requested workspace is not available")
            require_account_operational(transaction_pool, profile_id=workspace.owner_profile_id)
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            if locked.archived_at is not None:
                if locked.status is TaskStatus.CANCELLED:
                    raise StaleOperationError(
                        "the task is archived as cancelled; a later merge never "
                        "rewrites its terminal outcome"
                    )
                # Already archived COMPLETED: the idempotent duplicate-race
                # no-op — no second archival, loop closure, or event set.
                pull_request = require_canonical_task_pull_request(transaction_pool, task=locked)
                return TaskMergeCompletionOutcome(
                    kind=TaskMergeCompletionKind.ALREADY_COMPLETED,
                    task=locked,
                    pull_request=pull_request,
                )
            pull_request = require_canonical_task_pull_request(transaction_pool, task=locked)
            if not _is_confirmed_merged(pull_request):
                raise StaleOperationError(
                    "the canonical pull request is not confirmed merged in durable "
                    "state; completion applies nothing"
                )
            open_loops = review_records.list_task_review_loops(
                transaction_pool, task_id=task_id, status=ReviewLoopStatus.OPEN
            )
            for loop in open_loops:
                if loop.purpose is ReviewLoopPurpose.PR_REVIEW:
                    review_records.close_review_loop(transaction_pool, review_loop_id=loop.id)
            completed = task_mutations.archive_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=locked.state_token,
                terminal_status=TaskStatus.COMPLETED,
                actor=_OPENORC_ACTOR,
            )
            event_coordination.record_pr_merged_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_pull_request_id=pull_request.id,
                actor=_GITHUB_ACTOR,
            )
            return TaskMergeCompletionOutcome(
                kind=TaskMergeCompletionKind.COMPLETED,
                task=completed,
                pull_request=pull_request,
            )


def request_task_merge(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> TaskMergeOutcome:
    """Command the gate's exact GitHub merge as the current Owner merge decision.

    The ONE Owner merge command (issue #227). The addressed gate must be the
    Task's exact current PENDING ``MERGE_DECISION`` bound to the canonical
    ``TaskPullRequest`` and the Reviewer-accepted head SHA; no Owner-selected
    SHA/PR is accepted and no Reviewer override exists. Before any gate
    consumption the canonical PR is reconciled authoritatively and the source
    issue is proven open through the routed installation. One short composed
    transaction then resolves the gate ``APPROVED`` and records the OWNER
    ``OWNER_GATE_RESOLVED`` + ``MERGE_REQUESTED`` audit pair — the durable
    one-shot claim — and the existing exact-head merge primitive is called
    exactly once under the claim-revalidation hook. Outcomes are classified
    by which external effect actually occurred; see
    :class:`TaskMergeOutcomeKind`.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _REQUEST_MERGE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_REQUEST_MERGE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase A: the exact Profile's Owner authority and exact subjects ---
        require_owner_write_authorization(
            pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
        )
        with composed_transaction(pool) as transaction_pool:
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.WAITING_FOR_OWNER:
                raise StaleOperationError(
                    "the task is not waiting for an owner merge decision; "
                    "the merge request is stale"
                )
            gate = require_current_owner_gate(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            if gate.gate_type is not OwnerGateType.MERGE_DECISION:
                raise StaleOperationError(
                    "the addressed owner gate is not a merge decision gate; "
                    "the merge request is stale"
                )
            gate_head_sha = gate.subject_head_sha
            if gate_head_sha is None:
                raise StaleOperationError(
                    "the merge decision gate carries no exact head subject; "
                    "the merge request is stale"
                )
            pull_request = require_task_pull_request_head(
                transaction_pool, task=task, expected_head_sha=gate_head_sha
            )
            if gate.task_pull_request_id != pull_request.id:
                raise StaleOperationError(
                    "the merge decision gate addresses a different canonical pull "
                    "request record; the merge request is stale"
                )
            _require_open_pr_review_loop(transaction_pool, task=task)

        # --- Phase B: authoritative PR reconciliation (external, no
        # transaction open) — the current facts the whole classification is
        # grounded in, before the gate is consumed ---
        reconciliation = task_pull_request_reconciliation.reconcile_task_pull_request(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
        reconciled = reconciliation.pull_request
        if _is_confirmed_merged(reconciled):
            # A confirmed merged PR takes the external-completion path, not
            # another merge request; the pending gate is never consumed here
            # and no Owner approval event is invented.
            completion = complete_confirmed_task_merge(
                pool, workspace_id=workspace_id, task_id=task_id
            )
            return TaskMergeOutcome(
                kind=TaskMergeOutcomeKind.COMPLETED,
                task=completion.task,
                pull_request=completion.pull_request,
            )
        if reconciled.state is TaskPullRequestState.CLOSED:
            # A confirmed closed-unmerged PR does not merge or reopen; its
            # terminal cancellation consequence is owned by E10c (#229). The
            # gate stays pending and untouched.
            raise ConflictError(
                "the canonical pull request is closed and unmerged; the merge "
                "request is refused without consuming the pending merge decision"
            )
        if reconciled.head_sha != gate_head_sha:
            # The reviewed head moved: the gate is stale. Fail closed with the
            # narrow merge-staleness block consequence where the Task is still
            # current — never a new merge gate on the unreviewed head.
            staleness_block, blocked = _apply_merge_staleness_block(
                pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                owner_gate_id=owner_gate_id,
                task_pull_request_id=pull_request.id,
                expected_head_sha=gate_head_sha,
                observed_head_sha=reconciled.head_sha,
            )
            return TaskMergeOutcome(
                kind=TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION,
                task=blocked,
                pull_request=reconciled,
                task_block=staleness_block,
            )

        # --- Phase C: the issue-open guard (external, no transaction open) ---
        _require_source_issue_open(pool, github, task=task)

        # --- Phase D: re-lock/revalidate after the external preflight, then
        # the one-shot approval/claim in ONE short composed transaction ---
        with composed_transaction(pool) as transaction_pool:
            current = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            gate = require_current_owner_gate(
                transaction_pool, task=current, owner_gate_id=owner_gate_id
            )
            if (
                gate.gate_type is not OwnerGateType.MERGE_DECISION
                or gate.task_pull_request_id != pull_request.id
                or gate.subject_head_sha != reconciled.head_sha
            ):
                raise StaleOperationError(
                    "the merge decision gate no longer binds the reconciled pull "
                    "request subject; the merge request is stale"
                )
            require_task_pull_request_head(
                transaction_pool, task=current, expected_head_sha=reconciled.head_sha
            )
            _require_open_pr_review_loop(transaction_pool, task=current)
            resolution = task_mutations.resolve_owner_gate(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                owner_gate_id=owner_gate_id,
                outcome=OwnerGateStatus.APPROVED,
                actor=event_coordination.owner_actor(profile_id),
            )
            post_approval = resolution.task
            event_coordination.record_merge_requested_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_pull_request_id=pull_request.id,
                owner_gate_id=owner_gate_id,
                actor=event_coordination.owner_actor(profile_id),
            )

        # --- Phase E: exactly ONE merge request under the durable one-shot
        # claim, then classification by which external effect occurred ---
        try:
            merge_result = pull_request_merge.request_pull_request_merge(
                pool,
                github,
                user_token_resolver,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_head_sha=reconciled.head_sha,
                claim_revalidation=_merge_claim_revalidation(
                    workspace_id=workspace_id,
                    task_id=task_id,
                    owner_gate_id=owner_gate_id,
                    expected_post_approval_token=post_approval.state_token,
                    task_pull_request_id=pull_request.id,
                    expected_head_sha=reconciled.head_sha,
                ),
            )
        except MergeConfirmedReconciliationPendingError as error:
            return _classify_after_unconfirmed_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=post_approval.state_token,
                task_pull_request_id=pull_request.id,
                expected_head_sha=reconciled.head_sha,
                merge_commit_sha=error.merge_commit_sha,
            )
        except ExternalOperationUncertainError:
            return _classify_after_unconfirmed_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=post_approval.state_token,
                task_pull_request_id=pull_request.id,
                expected_head_sha=reconciled.head_sha,
                merge_commit_sha=None,
            )

        except ExternalOperationRejectedError:
            return _settle_after_definitive_no_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=post_approval.state_token,
                task_pull_request_id=pull_request.id,
                subject_head_sha=reconciled.head_sha,
                github_rejected=True,
            )
        except (ExternalOperationFailedError, AuthorizationError):
            # Plain known failures are pre-delivery conditions: credential
            # resolution, authentication/authorization rejection, rate
            # limiting. Proven non-delivery — never a GitHub-owned refusal.
            return _settle_after_definitive_no_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=post_approval.state_token,
                task_pull_request_id=pull_request.id,
                subject_head_sha=reconciled.head_sha,
                github_rejected=False,
            )
        except StaleOperationError:
            return _classify_after_stale_merge_request(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=post_approval.state_token,
                task_pull_request_id=pull_request.id,
                subject_head_sha=reconciled.head_sha,
            )
        completion = complete_confirmed_task_merge(pool, workspace_id=workspace_id, task_id=task_id)
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=completion.task,
            pull_request=completion.pull_request,
            merge_commit_sha=merge_result.merge_commit_sha,
        )


def _apply_uncertain_merge_block(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate_id: UUID,
    task_pull_request_id: UUID,
    expected_head_sha: str,
    merge_commit_sha: str | None,
) -> tuple[TaskBlock, Task]:
    """Atomically block the current Task on an unconfirmable merge outcome (issue #227).

    Same composed-transaction discipline as the merge-staleness block: the
    account-operational barrier and the exact Profile's Owner authority are
    re-proven FIRST after the intervening GitHub calls, then — under the Task
    row lock — the ``external_operation_uncertain`` TaskBlock is created with
    durable safe context tied to the consumed gate, PR, and head (plus the
    preserved confirmed merge commit where GitHub answered a known success),
    the Task moves to ``blocked`` with a rotated token, and the
    ``task_blocked`` audit records the condition. The uncertain merge request
    is never repeated; recovery belongs to the later F2/F4/F4b layers.
    """
    require_owner_write_authorization(
        pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    block_context: dict[str, str] = {
        "owner_gate_id": str(owner_gate_id),
        "task_pull_request_id": str(task_pull_request_id),
        "expected_head_sha": expected_head_sha,
    }
    if merge_commit_sha is not None:
        block_context["merge_commit_sha"] = merge_commit_sha
    with composed_transaction(pool) as transaction_pool:
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        block = block_records.create_task_block(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            reason=TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN,
            context=block_context,
        )
        blocked = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            status=TaskStatus.BLOCKED,
        )
        if blocked is None:
            raise StaleOperationError(
                "the task could not move to blocked; the uncertain-merge block is stale"
            )
        event_coordination.record_task_blocked_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_block_id=block.id,
            actor=_OPENORC_ACTOR,
        )
        return block, blocked


def _classify_after_unconfirmed_merge(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    task_pull_request_id: UUID,
    expected_head_sha: str,
    merge_commit_sha: str | None,
) -> TaskMergeOutcome:
    """Classify a merge request whose durable effect is confirmed or unknown.

    A confirmed merge with failed reconciliation preserves the confirmed
    outcome evidence; an uncertain outcome never carries any. Both paths
    NEVER repeat the merge mutation: one authoritative reconciliation is
    re-attempted (a read, safe after any send), a confirmed merged canonical
    PR takes the shared completion path, and anything still inconclusive —
    including a reconciliation that itself fails — blocks
    ``EXTERNAL_OPERATION_UNCERTAIN`` with durable safe context tied to the
    consumed gate, PR, and head for the later recovery layers.
    """
    try:
        reconciliation = task_pull_request_reconciliation.reconcile_task_pull_request(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
    except ApplicationError:
        reconciliation = None
    if reconciliation is not None and _is_confirmed_merged(reconciliation.pull_request):
        completion = complete_confirmed_task_merge(pool, workspace_id=workspace_id, task_id=task_id)
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=completion.task,
            pull_request=completion.pull_request,
            merge_commit_sha=merge_commit_sha,
        )
    uncertainty_block, blocked = _apply_uncertain_merge_block(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        owner_gate_id=owner_gate_id,
        task_pull_request_id=task_pull_request_id,
        expected_head_sha=expected_head_sha,
        merge_commit_sha=merge_commit_sha,
    )
    if reconciliation is not None:
        classified_pull_request = reconciliation.pull_request
    else:
        classified_pull_request = get_task_pull_request(
            pool, task_pull_request_id=task_pull_request_id
        )
        if classified_pull_request is None:
            raise NotFoundError("the requested pull request record is not available for this task")
    return TaskMergeOutcome(
        kind=TaskMergeOutcomeKind.BLOCKED_UNCERTAIN,
        task=blocked,
        pull_request=classified_pull_request,
        task_block=uncertainty_block,
        merge_commit_sha=merge_commit_sha,
    )


def _settle_after_definitive_no_merge(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    task_pull_request_id: UUID,
    subject_head_sha: str,
    github_rejected: bool,
) -> TaskMergeOutcome:
    """Settle a definitive no-merge outcome with truthful provenance (issue #227).

    A GitHub-owned refusal records ``MERGE_REJECTED_BY_GITHUB``; a proven
    pre-delivery failure records no GitHub-refusal event — the durable trace
    is the one-shot ``MERGE_REQUESTED`` claim plus the recovery gate. Fresh
    authoritative reconciliation then decides the recovery: a merged
    canonical PR completes the Task (the refusal raced an already-merged
    PR); a closed-unmerged PR is refused without any retry gate (#229 owns
    its terminal consequence); a moved head blocks ``STALE_OPERATION`` (no
    Reviewer acceptance ever transfers to the unreviewed head); only the
    same PR open/unmerged at the unchanged accepted head installs the NEW
    pending exact-head ``MERGE_DECISION`` gate requiring fresh Owner
    authorization. A failing reconciliation proof fails closed and mints no
    retry gate. The original accepted review evidence is preserved.
    """
    if github_rejected:
        event_coordination.record_merge_rejected_by_github_event(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_pull_request_id=task_pull_request_id,
            actor=_GITHUB_ACTOR,
        )
    reconciliation = task_pull_request_reconciliation.reconcile_task_pull_request(
        pool, github, workspace_id=workspace_id, task_id=task_id
    )
    reconciled = reconciliation.pull_request
    if _is_confirmed_merged(reconciled):
        completion = complete_confirmed_task_merge(pool, workspace_id=workspace_id, task_id=task_id)
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=completion.task,
            pull_request=completion.pull_request,
        )
    if reconciled.state is TaskPullRequestState.CLOSED:
        raise ConflictError(
            "the canonical pull request is closed and unmerged; no retry merge "
            "gate is minted and the terminal consequence is owned by E10c (#229)"
        )
    if reconciled.head_sha != subject_head_sha:
        staleness_block, blocked = _apply_merge_staleness_block(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate_id=owner_gate_id,
            task_pull_request_id=task_pull_request_id,
            expected_head_sha=subject_head_sha,
            observed_head_sha=reconciled.head_sha,
        )
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION,
            task=blocked,
            pull_request=reconciled,
            task_block=staleness_block,
        )
    retry_gate, updated = _install_retry_merge_gate(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        task_pull_request_id=task_pull_request_id,
        subject_head_sha=subject_head_sha,
    )
    return TaskMergeOutcome(
        kind=TaskMergeOutcomeKind.RETRY_GATE_INSTALLED,
        task=updated,
        pull_request=reconciled,
        retry_gate=retry_gate,
    )


def _classify_after_stale_merge_request(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    task_pull_request_id: UUID,
    subject_head_sha: str,
) -> TaskMergeOutcome:
    """Classify a stale merge-request outcome (issue #227).

    The typed staleness reaches this classifier from two distinct phases —
    a pre-send claim-revalidation failure (nothing was delivered) and the
    post-send expected-head mismatch (the request was delivered and refused
    by GitHub's own head guard). The classification is grounded in one fresh
    authoritative reconciliation either way: a merged canonical PR takes the
    shared completion path; a closed-unmerged PR is refused without any
    retry gate; a moved head blocks ``STALE_OPERATION`` where the Task is
    still current and never transfers the prior Reviewer acceptance to the
    unreviewed head; a PR still open at the exact claimed head means the
    staleness was workflow state (an archived Task or a lost claim), not a
    head change, and the standard stale semantics apply nothing.
    """
    reconciliation = task_pull_request_reconciliation.reconcile_task_pull_request(
        pool, github, workspace_id=workspace_id, task_id=task_id
    )
    reconciled = reconciliation.pull_request
    if _is_confirmed_merged(reconciled):
        completion = complete_confirmed_task_merge(pool, workspace_id=workspace_id, task_id=task_id)
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=completion.task,
            pull_request=completion.pull_request,
        )
    if reconciled.state is TaskPullRequestState.CLOSED:
        raise ConflictError(
            "the canonical pull request is closed and unmerged; no retry merge "
            "gate is minted and the terminal consequence is owned by E10c (#229)"
        )
    if reconciled.head_sha != subject_head_sha:
        staleness_block, blocked = _apply_merge_staleness_block(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate_id=owner_gate_id,
            task_pull_request_id=task_pull_request_id,
            expected_head_sha=subject_head_sha,
            observed_head_sha=reconciled.head_sha,
        )
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION,
            task=blocked,
            pull_request=reconciled,
            task_block=staleness_block,
        )
    raise StaleOperationError(
        "the approved merge claim is no longer current; the merge request is stale"
    )
