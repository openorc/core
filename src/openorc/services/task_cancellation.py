"""E10b: the unified Owner Task-cancellation command (issue #228).

``cancel_task`` is the ONE Owner-authorized Cancel command: from a current
safely inactive Task it archives the Task terminal ``CANCELLED``, releases
its issue's one-current-Task mapping, resolves its exact current pending
OwnerGate (when one exists), closes its OPEN ReviewLoops, and — when the
canonical open/unmerged pull request may require closure — requests that
closure once under the recorded Profile's user authorization. Cancel never
calls the runtime abort: the two Owner controls are mutually exclusive by
authoritative workflow eligibility.

Eligibility is rechecked on every invocation from durable state, never from
a caller-supplied claim: the exact current non-archived Task under the
caller's ``state_token``, in one of the safely inactive states —
``ready_to_plan``, ``queued``, ``waiting_for_owner`` at its exact current
pending OwnerGate of any type, or ``blocked`` with every activity
interruption positively settled. Autonomous progression
(``planning``/``implementing``/``reviewing``) is inapplicable — the Owner
uses Abort first — and an outstanding runtime work fact (an unsettled or
unresolved Interrupt intent, an open Execution, a pending RuntimeRequest)
vetoes Cancel. The veto composes the durable outstanding-work facts
available in v1; per-send durable dispatch identity arrives with F2
(#87/#89).

The command's ordering is load-bearing (issue #228 §4):

- **Before any irreversible mutation**, the available authoritative
  canonical-PR facts are established: a fresh B7 reconciliation outside any
  database transaction classifies the canonical pull request. A PR already
  authoritatively confirmed merged at the point of decision never becomes
  an Owner ``CANCELLED`` claim — it takes E10a's existing
  ``complete_confirmed_task_merge`` completion path. A read failure still
  allows cancellation with a durable unresolved PR-close obligation.
- **One shared short decision transaction** (shared by the delegating E5/E7
  gate-cancellation entrypoints): re-prove authorization, currentness,
  eligibility, and the accounted-runtime veto under the Task row lock;
  resolve the exact current pending gate ``CANCELLED`` with truthful OWNER
  provenance (when one exists — no artificial gate is ever created);
  close the OPEN ReviewLoops as actually present; archive ``CANCELLED``
  through the guarded archival primitive (which clears ``queued_at`` and
  the current-gate pointer and releases the issue mapping); commit the one
  OWNER ``task_cancelled`` event; and — only when the close obligation
  exists — durably record the narrowly scoped one-shot PR-close intent
  atomically with the archival, so a crash immediately after archival
  leaves the obligation observable.
- **After the commit**, the one-shot closure attempt: a fresh
  authoritative observation; an already closed-unmerged PR records
  no-action with no write; a PR observed merged after terminal
  cancellation preserves the archived outcome and never rewrites history;
  an open PR at the same stable identity is closed exactly once under the
  recorded Profile's user credential (the #143 accountability machinery,
  including the single bounded definitive-401 recovery), and the observed
  outcome is recorded. Pre-delivery conditions keep the obligation pending
  for the later reconciliation machinery; a definitive rejection records
  ``failed``; an unconfirmable outcome records ``unknown`` — never blindly
  replayed.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubProfileUserAccessToken,
    GitHubPullRequestCloseOperation,
    GitHubPullRequestCloseResult,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntent,
    TaskPullRequestCloseIntentStatus,
)
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.reviews import ReviewLoop, ReviewLoopStatus
from openorc.domain.runtime_requests import RuntimeRequestStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import intents as intent_records
from openorc.persistence import reviews as review_records
from openorc.persistence import runtime_requests as runtime_request_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request_for_task
from openorc.services import event_coordination
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_installation_route import (
    ResolvedRepositoryInstallationRoute,
    resolve_system_repository_installation_route,
)
from openorc.services.github_owner_write_authorization import (
    resolve_owner_write_credential,
    resolve_owner_write_credential_after_rejection,
    translate_owner_write_failure,
)
from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_merge_lifecycle import complete_confirmed_task_merge
from openorc.services.task_pull_request_reconciliation import reconcile_task_pull_request
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "TaskCancellationComposition",
    "TaskCancellationOutcome",
    "TaskCancellationOutcomeKind",
    "cancel_task",
    "cancel_task_composition",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_cancellation"
_CANCEL_SPAN_NAME = "task_cancellation.cancel_task"
_COMPOSITION_SPAN_NAME = "task_cancellation.cancel_task_composition"

# Cancel is inapplicable while autonomous progression is authorized: the
# Owner uses the Abort control first (issue #228).
_AUTONOMOUS_INELIGIBLE_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.PLANNING,
        TaskStatus.IMPLEMENTING,
        TaskStatus.REVIEWING,
    }
)

# The Execution statuses whose runtime work is still outstanding.
_OPEN_EXECUTION_STATUSES: frozenset[ExecutionStatus] = frozenset(
    {
        ExecutionStatus.QUEUED,
        ExecutionStatus.RUNNING,
        ExecutionStatus.PAUSED,
        ExecutionStatus.PAUSED_FOR_APPROVAL,
    }
)


class _CloseObligation(StrEnum):
    """The canonical-PR closure obligation established before cancellation.

    ``none`` — no canonical pull request, or one authoritatively observed
    closed-unmerged before the decision. ``close`` — observed open/unmerged
    at the same stable identity, so the close intent is recorded atomically
    with the archival. ``unresolved`` — the authoritative observation
    failed, so the obligation is recorded conservatively pending.
    """

    NONE = "none"
    CLOSE = "close"
    UNRESOLVED = "unresolved"


class TaskCancellationOutcomeKind(StrEnum):
    """The classified outcome of one unified Cancel command.

    ``cancelled`` applied the terminal archival (with its gate resolution,
    loop closures, and close obligation as applicable); ``completed``
    routed to E10a's shared completion because the canonical PR was already
    authoritatively confirmed merged at the point of decision.
    """

    CANCELLED = "cancelled"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class TaskCancellationOutcome:
    """The typed result of one unified Cancel command."""

    kind: TaskCancellationOutcomeKind
    task: Task
    owner_gate: OwnerGate | None
    closed_loops: tuple[ReviewLoop, ...]
    pull_request: TaskPullRequest | None
    close_intent: TaskPullRequestCloseIntent | None


@dataclass(frozen=True, slots=True)
class TaskCancellationComposition:
    """The typed result of the shared database-only cancellation composition."""

    task: Task
    owner_gate: OwnerGate | None
    closed_loops: tuple[ReviewLoop, ...]


@dataclass(frozen=True, slots=True)
class _CancelComposition:
    """The internal result of one composed cancellation, close intent included."""

    task: Task
    owner_gate: OwnerGate | None
    closed_loops: tuple[ReviewLoop, ...]
    close_intent: TaskPullRequestCloseIntent | None


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _is_confirmed_merged(pull_request: TaskPullRequest) -> bool:
    """Whether the durable record authoritatively carries a confirmed merge."""
    return pull_request.state is TaskPullRequestState.CLOSED and pull_request.merged_at is not None


def _require_cancel_eligible_status(task: Task) -> None:
    """Refuse the statuses where Cancel is inapplicable by workflow authority."""
    if task.status in _AUTONOMOUS_INELIGIBLE_STATUSES:
        raise ConflictError(
            "cancel is inapplicable while autonomous progression is authorized; "
            "the owner abort control applies first"
        )


def _require_runtime_work_accounted(transaction_pool: DatabasePool, *, task_id: UUID) -> None:
    """Veto cancellation while any durable runtime work fact is outstanding.

    The authoritative durable outstanding-work facts available in v1: an
    Interrupt intent that is not positively settled (unsettled, definitively
    failed — the interrupted work continues unaccounted — or unknown), an
    open Execution, and a pending RuntimeRequest. Per-send durable dispatch
    identity arrives with F2 (#87/#89).
    """
    for intent in intent_records.list_task_interrupt_intents(transaction_pool, task_id=task_id):
        if intent.status is not TaskInterruptIntentStatus.SETTLED:
            raise ConflictError(
                "the task's activity interruption is not settled; the owner abort "
                "settlement or reconciliation must account for it first"
            )
    for execution in execution_records.list_task_executions(transaction_pool, task_id=task_id):
        if execution.status in _OPEN_EXECUTION_STATUSES:
            raise ConflictError("the task has an open execution; its runtime work is unaccounted")
    for request in runtime_request_records.list_task_runtime_requests(
        transaction_pool, task_id=task_id
    ):
        if request.status is RuntimeRequestStatus.PENDING:
            raise ConflictError(
                "the task has a pending runtime request; a runtime turn is unaccounted"
            )


def _resolve_close_obligation(
    observed: TaskPullRequest,
) -> _CloseObligation:
    """Classify one authoritative pre-decision PR observation."""
    if _is_confirmed_merged(observed):
        # Not reachable through this helper (the caller routes to the
        # completion path first); kept total for classification clarity.
        return _CloseObligation.NONE
    if observed.state is TaskPullRequestState.OPEN:
        return _CloseObligation.CLOSE
    return _CloseObligation.NONE


def _cancel_composition(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate_id: UUID | None,
    close_obligation: _CloseObligation,
    expected_gate_type: OwnerGateType | None = None,
) -> _CancelComposition:
    """The ONE shared atomic cancellation composition (issue #228 §3).

    One short composed transaction: the account-operational barrier and the
    exact Profile-owned Workspace/Task scope first, the Task row lock, the
    currentness re-proof under the caller's expected token, the eligibility
    classification and the accounted-runtime veto, then — atomically — the
    exact current pending gate's ``CANCELLED`` resolution with truthful
    OWNER provenance (only when one exists; no artificial gate is created
    for blocked/queue states), the closure of every OPEN ReviewLoop as
    actually present, the guarded terminal ``CANCELLED`` archival (which
    clears ``queued_at`` and the current-gate pointer and releases the
    issue's one-current-Task mapping), the OWNER ``task_cancelled`` audit,
    and — only when the close obligation exists — the durable one-shot
    PR-close intent. Everything commits or rolls back together; a stale
    request mutates nothing.
    """
    owner_actor = event_coordination.owner_actor(profile_id)
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
        _require_cancel_eligible_status(task)
        _require_runtime_work_accounted(transaction_pool, task_id=task_id)
        cancelled_gate: OwnerGate | None = None
        if task.status is TaskStatus.WAITING_FOR_OWNER:
            gate_id = owner_gate_id if owner_gate_id is not None else task.current_owner_gate_id
            if gate_id is None:
                raise ConflictError(
                    "the waiting task names no current owner gate to cancel through"
                )
            gate = require_current_owner_gate(transaction_pool, task=task, owner_gate_id=gate_id)
            if expected_gate_type is not None and gate.gate_type is not expected_gate_type:
                # The delegating gate entrypoint's exact-type discipline: the
                # unified command resolves the current pending gate of any
                # type, while a gate-scoped entrypoint admits only its own.
                raise ConflictError(
                    f"only the current {expected_gate_type.value} gate cancels through this seam"
                )
            resolution = gate_records.resolve_owner_gate(
                transaction_pool,
                owner_gate_id=gate.id,
                outcome=OwnerGateStatus.CANCELLED,
                expected_task_state_token=expected_state_token,
            )
            if resolution.outcome is gate_records.OwnerGateResolutionOutcome.RESOLVED:
                cancelled_gate = resolution.gate
                cancelled_task = resolution.task
                if cancelled_gate is None or cancelled_task is None:
                    raise StaleOperationError(
                        "the owner gate resolution did not apply; the cancellation is stale"
                    )
            elif resolution.gate is None:
                raise NotFoundError("the requested owner gate is not available for this task")
            elif resolution.outcome is gate_records.OwnerGateResolutionOutcome.NO_OP:
                raise StaleOperationError(
                    "the owner gate has already been resolved; the cancellation is stale"
                )
            else:
                raise StaleOperationError(
                    "the owner gate is no longer the task's current gate; the cancellation is stale"
                )
            event_coordination.record_owner_gate_resolved_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=cancelled_gate.id,
                actor=owner_actor,
                outcome=OwnerGateStatus.CANCELLED,
            )
            working_token = cancelled_task.state_token
        else:
            working_token = task.state_token

        open_loops = review_records.list_task_review_loops(
            transaction_pool, task_id=task_id, status=ReviewLoopStatus.OPEN
        )
        closed_loops: list[ReviewLoop] = []
        for loop in open_loops:
            closed = review_records.close_review_loop(transaction_pool, review_loop_id=loop.id)
            # A ``None`` here means the loop was already closed concurrently:
            # the cancellation's desired end state already holds, so this is
            # a truthful no-op, never a retry.
            if closed is not None:
                closed_loops.append(closed)
        archived = task_records.archive_task(
            transaction_pool,
            task_id,
            expected_state_token=working_token,
            terminal_status=TaskStatus.CANCELLED,
        )
        if archived is None:
            raise StaleOperationError("the task could not be cancelled; the cancellation is stale")
        event_coordination.record_task_terminal_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            actor=owner_actor,
            terminal_status=TaskStatus.CANCELLED,
        )
        close_intent: TaskPullRequestCloseIntent | None = None
        if close_obligation is not _CloseObligation.NONE:
            surviving = get_task_pull_request_for_task(transaction_pool, task_id=task_id)
            if surviving is None or surviving.workspace_id != archived.workspace_id:
                raise ConflictError(
                    "the canonical pull request record moved during cancellation; "
                    "the cancellation is stale"
                )
            close_intent = intent_records.create_task_pull_request_close_intent(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_pull_request_id=surviving.id,
                github_pr_id=surviving.github_pr_id,
                github_pr_number=surviving.github_pr_number,
                requested_by_profile_id=profile_id,
            )
    return _CancelComposition(
        task=archived,
        owner_gate=cancelled_gate,
        closed_loops=tuple(closed_loops),
        close_intent=close_intent,
    )


def cancel_task_composition(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate_id: UUID,
    expected_gate_type: OwnerGateType | None = None,
) -> TaskCancellationComposition:
    """The shared database-only cancellation composition for gate entrypoints.

    The ONE implementation of the common cancellation composition (issue
    #228 §3): the delegating E5/E7 gate-cancellation entrypoints call this
    with their caller-addressed exact current pending gate (and, when their
    contract requires it, the gate type their seam admits). It performs no
    external call and records no PR-close obligation — those gate states
    carry no canonical pull request in the v1 flow, and no GitHub artifact
    is ever touched.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _COMPOSITION_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_COMPOSITION_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        composition = _cancel_composition(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate_id=owner_gate_id,
            close_obligation=_CloseObligation.NONE,
            expected_gate_type=expected_gate_type,
        )
        return TaskCancellationComposition(
            task=composition.task,
            owner_gate=composition.owner_gate,
            closed_loops=composition.closed_loops,
        )


def _record_close_outcome(
    pool: DatabasePool,
    intent: TaskPullRequestCloseIntent,
    *,
    status: TaskPullRequestCloseIntentStatus,
    outcome: dict[str, object],
) -> TaskPullRequestCloseIntent:
    """Record one safe terminal outcome on the pending close obligation.

    A raced terminal outcome is left untouched: the durable record the flow
    already holds is the truth, and no outcome is ever manufactured.
    """
    recorded = intent_records.record_task_pull_request_close_intent_outcome(
        pool,
        task_pull_request_close_intent_id=intent.id,
        status=status,
        outcome=outcome,
    )
    return recorded if recorded is not None else intent


def _close_request_with_bounded_recovery(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    credential: GitHubProfileUserAccessToken,
    profile_id: UUID,
    route: ResolvedRepositoryInstallationRoute,
    owner_login: str,
    repository_name: str,
    pull_number: int,
) -> GitHubPullRequestCloseResult:
    """Exactly one Owner-accountable close request with bounded 401 recovery.

    The close mutation carries the accountable Profile's user credential
    (issue #143). A DEFINITIVE 401-style authentication rejection — GitHub
    did not perform the closure — performs the single bounded recovery the
    #142 lifecycle allows and retries the close EXACTLY once; the retry's
    classification propagates unchanged, so a second rejection is the known
    failure. Uncertain outcomes are never refreshed, retried, or replayed.
    """
    try:
        return github.close_pull_request(
            credential=credential,
            github_installation_id=route.github_installation_id,
            owner_login=owner_login,
            repository_name=repository_name,
            pull_number=pull_number,
        )
    except GitHubAuthenticationRejectedError:
        recovered = resolve_owner_write_credential_after_rejection(
            pool, user_token_resolver, github, profile_id=profile_id, route=route
        )
        return github.close_pull_request(
            credential=recovered,
            github_installation_id=route.github_installation_id,
            owner_login=owner_login,
            repository_name=repository_name,
            pull_number=pull_number,
        )


def _attempt_close_intent(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    workspace_id: UUID,
    task_id: UUID,
    intent: TaskPullRequestCloseIntent,
) -> tuple[TaskPullRequestCloseIntent, TaskPullRequest | None]:
    """The one-shot post-archival closure attempt (issue #228 §4).

    A fresh authoritative observation first: an already closed-unmerged PR
    records no-action with no write; a PR observed merged after terminal
    cancellation preserves the archived outcome and never rewrites history;
    an open PR at the same stable identity is closed exactly once under the
    recorded Profile's user credential. Pre-delivery conditions (route,
    credential, intersection, access observation) keep the obligation
    pending for the later reconciliation machinery; a definitive rejection
    records ``failed``; an unconfirmable outcome records ``unknown`` — the
    close request is never blindly replayed.
    """
    try:
        reconciliation = reconcile_task_pull_request(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
    except ApplicationError:
        # The authoritative read failed: the pending obligation stays
        # observable; no outcome is fabricated and no write is attempted.
        return intent, None
    observed = reconciliation.pull_request
    if observed.github_pr_id != intent.github_pr_id:
        # The durable stable identity moved: this admitted close request can
        # never safely proceed against a different PR.
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.FAILED,
                outcome={"observation": "identity_changed"},
            ),
            observed,
        )
    if observed.state is TaskPullRequestState.CLOSED:
        observed_state = "merged" if observed.merged_at is not None else "closed_unmerged"
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.NO_ACTION,
                outcome={"observed_state": observed_state},
            ),
            observed,
        )
    task = task_records.get_task(pool, task_id)
    if task is None:
        raise NotFoundError("the requested task is not available in this workspace")
    try:
        route = resolve_system_repository_installation_route(
            pool, workspace_id=workspace_id, repository_id=task.repository_id
        )
        # The recorded Profile's accountability, re-proven before credential
        # resolution (issue #143): the account-wide barrier composes first.
        require_account_operational(pool, profile_id=intent.requested_by_profile_id)
        credential = resolve_owner_write_credential(
            pool,
            user_token_resolver,
            github,
            profile_id=intent.requested_by_profile_id,
            route=route,
        )
        repository_observation = github.get_installation_repository(
            github_installation_id=route.github_installation_id,
            github_repository_id=route.repository.identity.github_repository_id,
        )
    except ApplicationError:
        # A pre-delivery condition: the admitted close request never went
        # out, so the pending obligation stays unresolved — never blindly
        # replayed here.
        return intent, None
    return _deliver_close_request(
        pool,
        github,
        user_token_resolver,
        workspace_id=workspace_id,
        task_id=task_id,
        intent=intent,
        credential=credential,
        route=route,
        repository_observation=repository_observation,
    )


def _deliver_close_request(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    workspace_id: UUID,
    task_id: UUID,
    intent: TaskPullRequestCloseIntent,
    credential: GitHubProfileUserAccessToken,
    route: ResolvedRepositoryInstallationRoute,
    repository_observation: GitHubRepositoryObservation,
) -> tuple[TaskPullRequestCloseIntent, TaskPullRequest | None]:
    """Deliver the one close request under the recorded Profile's credential.

    A definitive rejection records ``failed``; an uncertain outcome records
    ``unknown`` and is never replayed; a known success is confirmed through
    fresh authoritative reconciliation — never the close answer alone.
    """
    try:
        try:
            close_result = _close_request_with_bounded_recovery(
                pool,
                github,
                user_token_resolver,
                credential=credential,
                profile_id=intent.requested_by_profile_id,
                route=route,
                owner_login=repository_observation.owner_login,
                repository_name=repository_observation.name,
                pull_number=intent.github_pr_number,
            )
        except GitHubOutcomeUncertainError as error:
            raise ExternalOperationUncertainError(
                "the outcome of the canonical pull request close request is unknown"
            ) from error
        except (
            GitHubAuthenticationRejectedError,
            GitHubAuthorizationRejectedError,
            GitHubRateLimitedError,
            GitHubRequestRejectedError,
        ) as error:
            # The user-authenticated close's own classification (issue
            # #143): honest about the accountable Profile's user credential,
            # never dressed up as a routed-installation credential failure.
            raise translate_owner_write_failure(error, operation="close request") from error
    except ExternalOperationUncertainError:
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.UNKNOWN,
                outcome={"close": "uncertain"},
            ),
            None,
        )
    except ApplicationError:
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.FAILED,
                outcome={"close": "failed"},
            ),
            None,
        )
    if close_result.outcome is GitHubPullRequestCloseOperation.REJECTED:
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.FAILED,
                outcome={"close": "rejected"},
            ),
            None,
        )
    # Known success: the observed outcome is confirmed through fresh
    # authoritative reconciliation — never the close answer alone.
    try:
        confirmation = reconcile_task_pull_request(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
    except ApplicationError:
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.UNKNOWN,
                outcome={"close": "unconfirmed"},
            ),
            None,
        )
    confirmed = confirmation.pull_request
    if confirmed.state is not TaskPullRequestState.CLOSED:
        return (
            _record_close_outcome(
                pool,
                intent,
                status=TaskPullRequestCloseIntentStatus.UNKNOWN,
                outcome={"observed_state": "open"},
            ),
            confirmed,
        )
    confirmed_state = "merged" if confirmed.merged_at is not None else "closed_unmerged"
    return (
        _record_close_outcome(
            pool,
            intent,
            status=TaskPullRequestCloseIntentStatus.CLOSE_REQUESTED,
            outcome={"confirmed": True, "observed_state": confirmed_state},
        ),
        confirmed,
    )


def cancel_task(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
) -> TaskCancellationOutcome:
    """Cancel the Task from a current safely inactive state (issue #228).

    The ONE Owner-authorized unified Cancel command. The available
    authoritative canonical-PR facts are established BEFORE any irreversible
    mutation — a PR already authoritatively confirmed merged at the point of
    decision routes to E10a's shared completion path instead of an Owner
    ``CANCELLED`` claim, and a read failure still allows cancellation with a
    durable unresolved PR-close obligation. One shared short transaction
    then applies the whole terminal composition atomically, and the one-shot
    closure attempt follows strictly after the commit. Stale, archived,
    already-terminal, autonomous-phase, and unaccounted-runtime requests
    mutate nothing.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _CANCEL_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_CANCEL_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase A: authorization, eligibility, and the available
        # authoritative canonical-PR facts, before any irreversible mutation ---
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
            _require_cancel_eligible_status(task)
            _require_runtime_work_accounted(transaction_pool, task_id=task_id)
            pull_request = get_task_pull_request_for_task(transaction_pool, task_id=task_id)
            if pull_request is not None and pull_request.workspace_id != task.workspace_id:
                raise NotFoundError(
                    "the requested pull request record is not available for this task"
                )
        close_obligation = _CloseObligation.NONE
        reconciled: TaskPullRequest | None = None
        if pull_request is not None:
            # Fresh authoritative observation with no database transaction
            # open: the decision facts precede any irreversible mutation.
            try:
                reconciliation = reconcile_task_pull_request(
                    pool, github, workspace_id=workspace_id, task_id=task_id
                )
            except ApplicationError:
                close_obligation = _CloseObligation.UNRESOLVED
            else:
                reconciled = reconciliation.pull_request
                if _is_confirmed_merged(reconciled):
                    # The PR was already authoritatively confirmed merged at
                    # the point of decision: E10a's completion path, never an
                    # Owner CANCELLED claim over a merged PR.
                    completion = complete_confirmed_task_merge(
                        pool, workspace_id=workspace_id, task_id=task_id
                    )
                    return TaskCancellationOutcome(
                        kind=TaskCancellationOutcomeKind.COMPLETED,
                        task=completion.task,
                        owner_gate=None,
                        closed_loops=(),
                        pull_request=completion.pull_request,
                        close_intent=None,
                    )
                close_obligation = _resolve_close_obligation(reconciled)
        # --- Phase B: the one shared short decision transaction ---
        composition = _cancel_composition(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate_id=None,
            close_obligation=close_obligation,
        )
        # --- Phase C: the one-shot closure attempt under the recorded
        # Profile's authorization, strictly after the commit ---
        final_intent = composition.close_intent
        final_pull_request = reconciled
        if final_intent is not None:
            final_intent, observed = _attempt_close_intent(
                pool,
                github,
                user_token_resolver,
                workspace_id=workspace_id,
                task_id=task_id,
                intent=final_intent,
            )
            if observed is not None:
                final_pull_request = observed
        return TaskCancellationOutcome(
            kind=TaskCancellationOutcomeKind.CANCELLED,
            task=composition.task,
            owner_gate=composition.owner_gate,
            closed_loops=composition.closed_loops,
            pull_request=final_pull_request,
            close_intent=final_intent,
        )
