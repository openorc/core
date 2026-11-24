"""Authorized Task read services (issue #376).

The read-side composition behind the authorized Task list/detail surface:
Owner-authorized, database-only, and strictly side-effect free. It composes
the coherent durable Task observation (``persistence.task_reads``) with the
ownership-separated GitHub/OpenOrc facts and the backend-generated action
projection — following the G2a issue-read surface's conventions (issue
#375) and never a second policy engine, a separate read store, or a
generalized read-model framework.

Ownership separation is structural: ``github`` carries the GitHub-owned
native facts (the linked issue's selected G2a projection summary and the
canonical TaskPullRequest's observed provider facts), while ``openorc``
carries OpenOrc identity and workflow facts. Clients render information and
offered actions; they never implement the OpenOrc state machine.

Backend-generated actions are computed dynamically in application/domain
code from the same coherent durable observation, reusing the authoritative
eligibility predicates the command services apply — never stored, never a
client-side state machine, never a new independent eligibility engine.
Every offered action — gate-scoped and Task-level alike — is evaluated
under the same durable prerequisites the command boundaries apply: the
account-operational and terminalization barriers, the #342 unsettled-current-Interrupt
veto, the guarded blocked-Task continuation derivations, the discussion
prerequisites, and the durable GitHub-projection start eligibility. Offered
actions are neither permission nor a promise of success: the G3 command
routes (issue #93) revalidate everything freshly at execution. No read ever
calls GitHub or a runtime provider, mutates workflow, or initiates a
refresh.

Every read composes inside ONE ``composed_read_transaction`` — a
transaction-scoped REPEATABLE READ READ ONLY snapshot — so the state token,
the exact current gate, the block, the subject facts, and every
eligibility fact the offered menus evaluate are one durable observation
instead of READ COMMITTED's per-statement views: a concurrent workflow
transition committing between the composition's statements can never
produce a response mixing an old state token with newly resolved facts.

Presentation bounding is transparent (the G2a bound-plus-total pattern): a
potentially large detail collection presents at most
:data:`TASK_DETAIL_PRESENTATION_BOUND` entries in deterministic order with
its durable total as the companion count, so a client distinguishes a
complete collection from a transparently bounded one. Presentation bounds
never restrict the durable evidence the action guards evaluate — the
bounding applies only to the rendered projection, computed after every
eligibility fact has been evaluated. The complete collection of any record
family remains reachable through the separate paginated history endpoint
(:mod:`openorc.services.task_history`).

Cross-Workspace isolation is absolute (the G2a rule): a foreign-Workspace
or absent subject is the uniform not-found outcome, never a probing oracle.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from openorc.domain.actions import OwnerGateActionToken, TaskActionToken
from openorc.domain.blocks import TaskBlock
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import (
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.executions import Execution
from openorc.domain.gates import (
    ExceptionalOwnerGateAction,
    OwnerGate,
    OwnerGateStatus,
    OwnerGateType,
    exceptional_owner_gate_actions,
)
from openorc.domain.github_issues import GitHubIssueProjection
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntentStatus,
    TaskResumeIntentStatus,
)
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.planning import PlanRevision
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.reviews import ReviewIteration, ReviewLoop, ReviewLoopStatus
from openorc.domain.runtime_requests import RuntimeRequest, RuntimeRequestStatus
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence.blocks import list_current_task_blocks_for_tasks
from openorc.persistence.detached_turns import (
    list_task_detached_turn_receipts_for_original_attempt,
)
from openorc.persistence.gates import list_owner_gates_by_ids
from openorc.persistence.github_issues import (
    find_github_issue_by_id,
    find_github_issue_projections_by_identity,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request_for_task
from openorc.persistence.sessions import (
    list_task_agent_sessions_for_tasks,
    sum_task_retained_session_costs_for_tasks,
)
from openorc.persistence.task_reads import TaskObservationRecord, load_task_observation
from openorc.persistence.tasks import (
    count_workspace_queued_tasks_before,
    list_issue_tasks_page,
    list_repository_tasks_page,
    list_workspace_tasks_page,
)
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import account_operationally_offered
from openorc.services.task_abort import abort_durably_offered
from openorc.services.task_cancellation import cancellation_durably_offered
from openorc.services.task_owner_resume import blocked_continuation_durably_offered
from openorc.services.task_pending_runtime_interactions import (
    list_pending_task_runtime_interactions,
)
from openorc.services.task_source_change_ignore import ignore_durably_offered
from openorc.services.task_start import start_durably_offered
from openorc.services.transaction_composition import composed_read_transaction
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_repository,
    require_workspace_task,
)

__all__ = [
    "ISSUE_ATTEMPTS_PAGE_LIMIT",
    "TASK_DETAIL_PRESENTATION_BOUND",
    "TASK_PAGE_LIMIT",
    "TaskBlockSummary",
    "TaskCostFacts",
    "TaskDetail",
    "TaskExecutionFacts",
    "TaskGateSummary",
    "TaskOutstandingIntentFacts",
    "TaskPendingInteractionFacts",
    "TaskReviewIterationFacts",
    "TaskReviewLoopFacts",
    "TaskSessionFacts",
    "TaskSummaryItem",
    "TaskTerminalizationFacts",
    "decode_task_page_cursor",
    "terminalization_facts",
    "encode_task_page_cursor",
    "get_task_for_owner",
    "list_issue_tasks_for_owner",
    "list_repository_tasks_for_owner",
    "list_workspace_tasks_for_owner",
]

# The bounded list page: one authorized list request returns at most this
# many items, advanced by the opaque keyset cursor (the G2a convention).
TASK_PAGE_LIMIT = 50
ISSUE_ATTEMPTS_PAGE_LIMIT = 50
# The transparent detail-collection presentation bound: a collection with
# more entries presents the first bound in deterministic order with its
# durable total as the companion count. Presentation never restricts the
# durable evidence the action guards evaluate.
TASK_DETAIL_PRESENTATION_BOUND = 50

_READS_TRACER_SCOPE = "openorc.services.task_reads"
_LIST_SPAN_NAME = "task_reads.list_tasks_for_owner"
_DETAIL_SPAN_NAME = "task_reads.get_task_for_owner"

# The ordinary gate menus (issue #376's agreed public contract): the fixed
# per-type decision vocabulary, guarded by currentness/authority and durable
# prerequisites — never a registry and never an action engine.
_ORDINARY_GATE_MENUS: dict[OwnerGateType, tuple[str, ...]] = {
    OwnerGateType.IMPLEMENTATION_AUTHORIZATION: (
        OwnerGateActionToken.APPROVE.value,
        OwnerGateActionToken.DISCUSS.value,
        OwnerGateActionToken.CANCEL_TASK.value,
    ),
    OwnerGateType.PR_AUTHORIZATION: (
        OwnerGateActionToken.APPROVE.value,
        OwnerGateActionToken.CANCEL_TASK.value,
    ),
    OwnerGateType.MERGE_DECISION: (
        OwnerGateActionToken.APPROVE.value,
        OwnerGateActionToken.DISCUSS.value,
        OwnerGateActionToken.CANCEL_TASK.value,
    ),
    OwnerGateType.REVIEW_RESOLUTION: (
        OwnerGateActionToken.DISCUSS.value,
        OwnerGateActionToken.CANCEL_TASK.value,
    ),
}

# The exceptional menus' wire tokens, mapped from the domain's fixed
# decision vocabulary (domain.gates.exceptional_owner_gate_actions).
# The exceptional menus' wire tokens, mapped from the domain's fixed
# decision vocabulary (domain.gates.exceptional_owner_gate_actions).
_EXCEPTIONAL_ACTION_TOKENS: dict[ExceptionalOwnerGateAction, str] = {
    ExceptionalOwnerGateAction.IGNORE: OwnerGateActionToken.IGNORE.value,
    ExceptionalOwnerGateAction.CONTINUE: OwnerGateActionToken.CONTINUE.value,
    ExceptionalOwnerGateAction.RESUME: OwnerGateActionToken.RESUME.value,
    ExceptionalOwnerGateAction.CANCEL_TASK: OwnerGateActionToken.CANCEL_TASK.value,
}


@dataclass(frozen=True, slots=True)
class TaskCostFacts:
    """The Task's derived expenditure facts (issues #240/#291, #376).

    ``retained_cost_usd`` is the role-agnostic sum of the retained known
    per-session checkpoints — ``None`` when no session has ever reported a
    cost (unknown, never zero). ``is_partial`` records that at least one
    session binding expected to contribute carries no retained reading: a
    known subtotal, never a final provider bill. The budget is the frozen
    Task authorization; the effective threshold is the operative spending
    threshold (the Owner Continue override when present, else the frozen
    budget); ``None`` means spending policy is disabled, never zero.
    """

    retained_cost_usd: Decimal | None
    authorized_task_budget_usd: Decimal | None
    effective_cost_threshold_usd: Decimal | None
    is_partial: bool


@dataclass(frozen=True, slots=True)
class TaskGateSummary:
    """The exact current pending OwnerGate and its offered menu (#376).

    ``actions`` is the backend-generated gate-scoped menu: the gate type's
    canonical ordinary or exceptional decisions, guarded by the same durable
    prerequisites the command boundaries apply — the account-operational and
    terminalization barriers, the per-action eligibility predicates
    (``discuss`` only where the #339 discussion prerequisites positively
    hold, ``cancel_task`` only where the accounted-runtime veto permits it,
    ``resume``/``continue``/``ignore`` only where their guarded
    blocked-Task continuation derivations positively hold). A resolved or
    historical gate never appears here.
    """

    gate: OwnerGate
    actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TaskBlockSummary:
    """The exact unresolved TaskBlock (summary or typed detail, #376)."""

    block: TaskBlock


@dataclass(frozen=True, slots=True)
class TaskTerminalizationFacts:
    """The Task's terminalization commitment and cleanup evidence (#376)."""

    outcome: TaskStatus
    origin: str
    established_at: datetime
    cleanup_send_claimed_at: datetime | None
    cleanup_send_settled_at: datetime | None


@dataclass(frozen=True, slots=True)
class TaskSummaryItem:
    """One ownership-separated Task summary (the shared list DTO, #376).

    The same summary the detail resource extends: the linked GitHub issue's
    selected G2a native-field summary (``None`` fields honestly
    unprojected), the canonical TaskPullRequest when it exists, the derived
    cost block, the exact current gate/block, the Task-level actions, and
    the durable queue context when queued.
    """

    task: Task
    github_issue: GitHubIssueProjection | None
    pull_request: TaskPullRequest | None
    cost: TaskCostFacts
    current_gate: TaskGateSummary | None
    current_block: TaskBlockSummary | None
    actions: tuple[str, ...]
    queue_position: int | None


@dataclass(frozen=True, slots=True)
class TaskSessionFacts:
    """One Task/role session binding's authorized presentation facts (#376).

    ``configured_provider``/``configured_model`` are the effective
    configuration captured for this exact Task session at initialization —
    never silently substituted from the current mutable Workspace role
    binding; a session initialized before the capture (or an unconfigured
    route) honestly reads ``None``. The reported fields are runtime
    observations. No raw configuration snapshot, prompt, secret, auth
    reference, or native control fact is ever exposed.
    """

    session: TaskAgentSession
    configured_provider: str | None
    configured_model: str | None


@dataclass(frozen=True, slots=True)
class TaskExecutionFacts:
    """One relevant current/unsettled Execution's safe projection (#376).

    ``result`` is the safe typed projection of the E6 validated canonical
    implementation result when one is attached (the validated Owner-authored
    completion fields only); an unsettled attempt carries ``None``. No
    native run identity, transcript, or provider payload is ever exposed.
    """

    execution: Execution
    result: dict[str, object] | None


@dataclass(frozen=True, slots=True)
class TaskReviewIterationFacts:
    """One ReviewIteration's authorized presentation facts (#376)."""

    iteration: ReviewIteration


@dataclass(frozen=True, slots=True)
class TaskReviewLoopFacts:
    """One open/current ReviewLoop with its presented iterations (#376).

    ``presented_iterations`` always carries the relevant current
    exact-subject iteration and the latest finalized outcome/findings needed
    to understand the current workflow and gate; additional iterations are
    presented up to the explicit bound with ``iteration_total`` as the
    companion count — the autonomous revision budget (``iteration_limit``)
    governs Producer revisions, never the Reviewer evaluation history, which
    the paginated history endpoint presents in full.
    """

    loop: ReviewLoop
    presented_iterations: tuple[TaskReviewIterationFacts, ...]
    iteration_total: int
    iterations_bounded: bool


@dataclass(frozen=True, slots=True)
class TaskPendingInteractionFacts:
    """One outstanding runtime interaction's safe typed facts (#376).

    Exactly the #376 pending-interaction surface: the closed operation and
    role vocabulary values, the exact bound session identity, the safe typed
    subject, and the committed start instant — never raw claim rows, runtime
    payloads, or a proof of live activity.
    """

    operation: str
    role: str
    task_agent_session_id: UUID | None
    subject: dict[str, object]
    started_at: datetime | None


@dataclass(frozen=True, slots=True)
class TaskOutstandingIntentFacts:
    """One outstanding intent's safe typed facts (#376).

    ``kind`` carries the closed intent vocabulary (``interrupt``, ``resume`",
    ``pr_close``); ``status`` is the intent's own durable status value.
    """

    id: UUID
    kind: str
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class TaskDetail:
    """The composed Task detail (issue #376).

    Every bounded collection carries its companion durable total of exactly
    the same relevant population. ``pending_runtime_interactions`` is the one
    collection whose population is invariant-bounded instead: at most one
    bootstrap interaction (the single planning-bootstrap evidence), at most
    one unsettled dispatch per session binding (the one-shot claim discipline
    behind the single-outstanding-run invariant, over the v1 two session
    bindings), at most one current-subject review attempt, and at most one
    RUNNING execution in the eligible phase — a bounded projection of current
    work, never a history-sized collection, so no companion total applies.
    """

    """The ownership-separated Task detail (issue #376).

    Extends the shared summary with the current typed records: the block's
    typed reason context, the current immutable plan revision, the session
    bindings, the relevant current/unsettled Executions, the open review
    loops, the pending runtime interactions/requests, and the outstanding
    detached-turn receipt and intent evidence.
    """

    summary: TaskSummaryItem
    current_plan: PlanRevision | None
    sessions: tuple[TaskSessionFacts, ...]
    executions: tuple[TaskExecutionFacts, ...]
    executions_total: int
    planning_bootstrap_stage: str | None
    review_loops: tuple[TaskReviewLoopFacts, ...]
    pending_runtime_interactions: tuple[TaskPendingInteractionFacts, ...]
    pending_runtime_requests: tuple[RuntimeRequest, ...]
    pending_runtime_requests_total: int
    outstanding_receipts: tuple[TaskDetachedTurnReceipt, ...]
    outstanding_receipts_total: int
    outstanding_intents: tuple[TaskOutstandingIntentFacts, ...]
    outstanding_intents_total: int


def encode_task_page_cursor(last_created_at: datetime, last_task_id: UUID) -> str:
    """Return the opaque keyset cursor for one page's last ``(created_at, id)`` key."""
    micros = int(last_created_at.astimezone(UTC).timestamp() * 1_000_000)
    raw = f"t:{micros}:{last_task_id}".encode("ascii")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_task_page_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode one opaque Task page cursor, or raise the typed invalid command."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("ascii")
        prefix, micros_text, task_id_text = raw.split(":", 2)
        if prefix != "t":
            raise ValueError("unexpected cursor prefix")
        return datetime.fromtimestamp(int(micros_text) / 1_000_000, tz=UTC), UUID(task_id_text)
    except (ValueError, UnicodeDecodeError) as error:
        raise InvalidCommandError("the page cursor is not a valid Task page cursor") from error


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _configured_pair(session: TaskAgentSession) -> tuple[str | None, str | None]:
    """Extract the session's captured configured provider/model, fail-closed.

    The pair is read from the durable ``effective_config_snapshot``'s #376
    capture keys — the effective configuration captured for that exact Task
    session at initialization. A missing, non-canonical, or incomplete pair
    fails closed as ``None``: honest unknown, never a substitute from the
    current mutable Workspace role binding.
    """
    snapshot = session.effective_config_snapshot
    if not isinstance(snapshot, Mapping):
        return None, None
    provider = snapshot.get("configured_provider")
    model = snapshot.get("configured_model")
    if (provider is None) != (model is None):
        return None, None
    if provider is None or model is None:
        return None, None
    if not isinstance(provider, str) or not isinstance(model, str):
        return None, None
    return provider, model


def _cost_facts(task: Task, observation: TaskObservationRecord) -> TaskCostFacts:
    """Compose the derived cost block from the existing checkpoint facts."""
    known_total = observation.session_costs_known_total_usd
    if known_total is not None and not isinstance(known_total, Decimal):
        known_total = None
    return TaskCostFacts(
        retained_cost_usd=known_total,
        authorized_task_budget_usd=task.authorized_task_budget_usd,
        effective_cost_threshold_usd=task.effective_cost_threshold_usd,
        is_partial=observation.session_costs_has_unobserved,
    )


def _account_operational(tx_view: DatabasePool, *, profile_id: UUID) -> bool:
    """Whether the authenticated Owner's account-operational barrier holds.

    The read-side non-locking predicate: the same durable deletion-state
    semantics as the guarded mutation barrier, without the ``FOR KEY SHARE``
    clause PostgreSQL prohibits inside the composed read-only snapshot
    transaction (#376). A missing Profile and any existing deletion attempt
    are simply not offered; the commands revalidate under the locking barrier.
    """
    return account_operationally_offered(tx_view, profile_id=profile_id)


def _discuss_offered(
    tx_view: DatabasePool,
    *,
    task: Task,
    gate: OwnerGate,
    sessions: Sequence[TaskAgentSession],
) -> bool:
    """Whether the #339 discussion prerequisites positively hold (#376).

    ``discuss`` is offered only when the exact current gate's Reviewer
    discussion can actually be prepared on the durable facts: an admitted
    READY Reviewer session binding exists, and no outstanding gate-bound
    discussion receipt occupies the single-outstanding slot. The command
    revalidates everything freshly at execution.
    """
    reviewer_ready = any(
        session.role is WorkflowRole.REVIEWER
        and session.lifecycle_status is TaskSessionLifecycleStatus.READY
        for session in sessions
    )
    if not reviewer_ready:
        return False
    receipts = list_task_detached_turn_receipts_for_original_attempt(
        tx_view,
        workspace_id=task.workspace_id,
        task_id=task.id,
        original_attempt_kind=OriginalAttemptKind.OWNER_GATE_DISCUSSION,
        original_attempt_id=gate.id,
    )
    return not any(
        receipt.status
        in (TaskDetachedTurnReceiptStatus.CLAIMED, TaskDetachedTurnReceiptStatus.ADMITTED)
        for receipt in receipts
    )


def _gate_menu(
    tx_view: DatabasePool,
    *,
    task: Task,
    gate: OwnerGate,
    sessions: Sequence[TaskAgentSession],
    account_operational: bool,
) -> tuple[str, ...]:
    """Generate the exact current gate's offered menu from durable facts (#376).

    The canonical ordinary menus and the domain's fixed exceptional menus are
    the decision vocabulary; the offered subset is guarded by the same
    durable prerequisites the command boundaries apply, never the gate type
    alone: the account-operational and terminalization barriers (a committed
    terminalization bars every ordinary command, the gate-scoped ones
    included), the #342 unsettled-current-Interrupt accounted-runtime veto on
    every offered cancel, the #339 discussion prerequisites, and the guarded
    blocked-Task continuation derivations behind resume/continue. A
    non-pending gate (resolved history) never advertises a menu.
    """
    if gate.status is not OwnerGateStatus.PENDING or gate.task_id != task.id:
        return ()
    if not account_operational or task.terminalization_outcome is not None:
        # The same ordinary-command barriers the Task-level projection
        # applies: a claimed account deletion or a committed terminalization
        # bars every offered command, the gate menu included.
        return ()
    if task.archived_at is not None:
        return ()
    if gate.gate_type in _ORDINARY_GATE_MENUS:
        menu = list(_ORDINARY_GATE_MENUS[gate.gate_type])
        if OwnerGateActionToken.DISCUSS.value in menu and not _discuss_offered(
            tx_view, task=task, gate=gate, sessions=sessions
        ):
            menu.remove(OwnerGateActionToken.DISCUSS.value)
        if OwnerGateActionToken.CANCEL_TASK.value in menu and not cancellation_durably_offered(
            tx_view, task=task
        ):
            menu.remove(OwnerGateActionToken.CANCEL_TASK.value)
        return tuple(menu)
    try:
        exceptional = exceptional_owner_gate_actions(gate.gate_type)
    except Exception:
        # Ordinary gate types carry no exceptional menu; the fixed domain
        # vocabulary fails closed rather than inventing an action.
        return ()
    return tuple(
        _EXCEPTIONAL_ACTION_TOKENS[action]
        for action in exceptional
        if _exceptional_action_offered(tx_view, task=task, gate=gate, action=action)
    )


def _exceptional_action_offered(
    tx_view: DatabasePool,
    *,
    task: Task,
    gate: OwnerGate,
    action: ExceptionalOwnerGateAction,
) -> bool:
    """Whether one exceptional decision's durable prerequisites hold (#376).

    Each decision reuses its own command boundary's authoritative predicate:
    cancel through the shared cancellation guard, resume and continue
    through the guarded blocked-Task continuation derivation (the #342
    unsettled-current-Interrupt precondition included), and ignore through
    the Ignore verification's durable Phase-1 checks. Unknown vocabulary is
    never offered.
    """
    if action is ExceptionalOwnerGateAction.CANCEL_TASK:
        return cancellation_durably_offered(tx_view, task=task)
    if action in (
        ExceptionalOwnerGateAction.RESUME,
        ExceptionalOwnerGateAction.CONTINUE,
    ):
        return blocked_continuation_durably_offered(tx_view, task=task, gate=gate)
    if action is ExceptionalOwnerGateAction.IGNORE:
        return ignore_durably_offered(tx_view, task=task, gate=gate)
    return False


def _task_level_actions(
    tx_view: DatabasePool,
    *,
    task: Task,
    account_operational: bool,
    gate_offers_cancel: bool,
) -> tuple[str, ...]:
    """Generate the Task-level offered command tokens from durable facts (#376).

    ``start`` only for an eligible current ``ready_to_plan`` attempt whose
    durable GitHub-projection queue eligibility positively holds (the #76
    guard re-derived from durable facts, never a fresh external call);
    ``abort`` only under the established eligible runtime/workflow
    conditions (the authoritative in-flight fact); ``cancel_task`` only
    under the existing safe-inactivity/accounted-interrupt/gate rules — and
    never duplicated when the current gate's menu already offers it;
    ``purge`` only for archived attempts. Once the terminalization
    commitment is committed, ordinary commands are barred even when the
    primary status is not yet terminal; the only ever-offered action past
    commitment is the archived attempt's ``purge``. No generic retry, no
    native RuntimeRequest approval, no action after commitment.
    """
    if not account_operational:
        return ()
    if task.terminalization_outcome is not None:
        # Ordinary commands are barred once the terminalization commitment
        # is committed, even if the primary status is not yet terminal.
        return (TaskActionToken.PURGE.value,) if task.archived_at is not None else ()
    if task.archived_at is not None:
        return (TaskActionToken.PURGE.value,)
    actions: list[str] = []
    if task.status is TaskStatus.READY_TO_PLAN and start_durably_offered(tx_view, task=task):
        actions.append(TaskActionToken.START.value)
    if abort_durably_offered(tx_view, task=task):
        actions.append(TaskActionToken.ABORT.value)
    if not gate_offers_cancel and cancellation_durably_offered(tx_view, task=task):
        actions.append(TaskActionToken.CANCEL_TASK.value)
    return tuple(actions)


def terminalization_facts(task: Task) -> TaskTerminalizationFacts | None:
    """Expose the set-once terminalization commitment when present (#376)."""
    if task.terminalization_outcome is None or task.terminalization_origin is None:
        return None
    assert task.terminalization_established_at is not None
    return TaskTerminalizationFacts(
        outcome=task.terminalization_outcome,
        origin=task.terminalization_origin.value,
        established_at=task.terminalization_established_at,
        cleanup_send_claimed_at=task.cleanup_send_claimed_at,
        cleanup_send_settled_at=task.cleanup_send_settled_at,
    )


def _review_loop_facts(
    *,
    loop: ReviewLoop,
    iterations: Sequence[ReviewIteration],
    task: Task,
    pull_request: TaskPullRequest | None,
) -> TaskReviewLoopFacts:
    """Compose one open loop's presented iterations (issue #376, rev-3 rule).

    The relevant current exact-subject iteration (the loop's unfinalized
    iteration whose subject matches the Task's current subject) and the
    latest finalized iteration carrying the outcome/findings the current
    workflow and gate need are ALWAYS presented, never bounded out.
    Additional iterations are presented newest-first up to the explicit
    presentation bound with the loop's durable total as the companion count;
    the full history remains on the paginated history endpoint. The bound
    applies only to the rendered projection — the action guards evaluate the
    complete durable evidence.
    """
    if loop.status is not ReviewLoopStatus.OPEN:
        return TaskReviewLoopFacts(
            loop=loop,
            presented_iterations=(),
            iteration_total=len(iterations),
            iterations_bounded=False,
        )

    def _subject_current(iteration: ReviewIteration) -> bool:
        if loop.purpose.value == "planning":
            return iteration.plan_revision_id == task.current_plan_revision_id
        if pull_request is None:
            return False
        return (
            iteration.task_pull_request_id == pull_request.id
            and iteration.reviewed_head_sha == pull_request.head_sha
        )

    always: list[ReviewIteration] = []
    for iteration in iterations:
        if iteration.outcome is None and _subject_current(iteration):
            always.append(iteration)
    finalized = [iteration for iteration in iterations if iteration.outcome is not None]
    if finalized:
        always.append(finalized[-1])
    always_ids = {iteration.id for iteration in always}
    remainder = [
        iteration
        for iteration in sorted(
            (item for item in iterations if item.id not in always_ids),
            key=lambda item: (item.created_at, item.id),
            reverse=True,
        )
    ]
    bounded = remainder[:TASK_DETAIL_PRESENTATION_BOUND]
    presented = tuple(
        TaskReviewIterationFacts(iteration=iteration) for iteration in [*always, *bounded]
    )
    return TaskReviewLoopFacts(
        loop=loop,
        presented_iterations=presented,
        iteration_total=len(iterations),
        iterations_bounded=len(remainder) > TASK_DETAIL_PRESENTATION_BOUND,
    )


# The safe typed fields of the E6 validated canonical implementation result
# (issue #81): the Owner-authored completion facts only — never a native run
# identity, transcript, or provider payload.
_IMPLEMENTATION_RESULT_SAFE_KEYS = ("status", "branch", "summary", "changes", "validation", "notes")


def _execution_facts(execution: Execution) -> TaskExecutionFacts:
    """Project one Execution's safe typed facts (issue #376)."""
    result = execution.implementation_result
    safe_result: dict[str, object] | None = None
    if isinstance(result, dict):
        safe_result = {
            key: result[key] for key in _IMPLEMENTATION_RESULT_SAFE_KEYS if key in result
        }
    return TaskExecutionFacts(execution=execution, result=safe_result)


def _outstanding_intents(
    observation: TaskObservationRecord,
) -> tuple[tuple[TaskOutstandingIntentFacts, ...], int]:
    """Compose the outstanding intent evidence and its durable total (#376).

    The unresolved interrupt/resume/PR-close authorizations and any relevant
    cleanup obligation, exposed as separate typed records — never raw rows,
    never raw outcome payloads. The companion total counts exactly the same
    outstanding population durably (the bounded loaded windows plus the
    single-record close obligation, unique per Task), so a client always
    distinguishes a complete collection from a transparently bounded one.
    """
    facts: list[TaskOutstandingIntentFacts] = []
    for intent in observation.interrupt_intents:
        if intent.status is not TaskInterruptIntentStatus.SETTLED:
            facts.append(
                TaskOutstandingIntentFacts(
                    id=intent.id,
                    kind="interrupt",
                    status=intent.status.value,
                    created_at=intent.created_at,
                    updated_at=intent.updated_at,
                )
            )
    for intent in observation.resume_intents:
        if intent.status in (TaskResumeIntentStatus.AUTHORIZED, TaskResumeIntentStatus.DISPATCHED):
            facts.append(
                TaskOutstandingIntentFacts(
                    id=intent.id,
                    kind="resume",
                    status=intent.status.value,
                    created_at=intent.created_at,
                    updated_at=intent.updated_at,
                )
            )
    close_intent = observation.pull_request_close_intent
    if close_intent is not None and close_intent.status is TaskPullRequestCloseIntentStatus.PENDING:
        facts.append(
            TaskOutstandingIntentFacts(
                id=close_intent.id,
                kind="pr_close",
                status=close_intent.status.value,
                created_at=close_intent.created_at,
                updated_at=close_intent.updated_at,
            )
        )
    # The interrupt/resume facts come from the bounded loaded windows; their
    # durable totals come from the loader's own counts of the same status
    # populations. The close obligation is a single record per Task (the
    # unique(task_id) intent), presented exactly when it is pending.
    return tuple(facts), (
        observation.outstanding_interrupt_intents_total
        + observation.outstanding_resume_intents_total
        + (
            1
            if observation.pull_request_close_intent is not None
            and observation.pull_request_close_intent.status
            is TaskPullRequestCloseIntentStatus.PENDING
            else 0
        )
    )


def _compose_summary(
    tx_view: DatabasePool,
    *,
    task: Task,
    observation: TaskObservationRecord,
    issue: GitHubIssueProjection | None,
    pull_request: TaskPullRequest | None,
    account_operational: bool,
) -> TaskSummaryItem:
    """Compose one ownership-separated Task summary (issue #376)."""
    sessions = observation.sessions
    gate = observation.current_gate
    gate_actions: tuple[str, ...] = ()
    if gate is not None:
        gate_actions = _gate_menu(
            tx_view,
            task=task,
            gate=gate,
            sessions=sessions,
            account_operational=account_operational,
        )
    gate_offers_cancel = OwnerGateActionToken.CANCEL_TASK.value in gate_actions
    actions = _task_level_actions(
        tx_view,
        task=task,
        account_operational=account_operational,
        gate_offers_cancel=gate_offers_cancel,
    )
    current_block: TaskBlockSummary | None = None
    if observation.current_blocks:
        # The exact unresolved block: the blocking composition guarantees at
        # most one unresolved block per Task (the partial unique index); the
        # newest-ordered first entry is the operative blocking condition.
        current_block = TaskBlockSummary(block=observation.current_blocks[0])
    queue_position: int | None = None
    if task.status is TaskStatus.QUEUED and task.queued_at is not None:
        queue_position = count_workspace_queued_tasks_before(
            tx_view,
            workspace_id=task.workspace_id,
            queued_at=task.queued_at,
            task_id=task.id,
        )
    return TaskSummaryItem(
        task=task,
        github_issue=issue,
        pull_request=pull_request,
        cost=_cost_facts(task, observation),
        current_gate=(None if gate is None else TaskGateSummary(gate=gate, actions=gate_actions)),
        current_block=current_block,
        actions=actions,
        queue_position=queue_position,
    )


def _validate_page_filters(statuses: Sequence[TaskStatus], archival: str) -> None:
    """Validate the #376 list filters (typed vocabulary, canonical archival)."""
    if not statuses:
        raise InvalidCommandError("the status filter must name at least one canonical status")
    for status in statuses:
        if not isinstance(status, TaskStatus):
            raise InvalidCommandError("the status filter must use canonical TaskStatus values")
    if archival not in ("current", "archived", "all"):
        raise InvalidCommandError("the archival filter must be one of current, archived, all")


def _validate_page_cursor(cursor: str | None) -> tuple[datetime, UUID] | None:
    """Validate the opaque page cursor."""
    if cursor is None:
        return None
    return decode_task_page_cursor(cursor)


def list_workspace_tasks_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    statuses: Sequence[TaskStatus],
    archival: str = "current",
    cursor: str | None = None,
) -> tuple[list[TaskSummaryItem], str | None]:
    """List one Workspace's Tasks for the authenticated Owner (issue #376).

    Owner-authorized, database-only, and side-effect free: bounded,
    deterministically ordered (``(created_at, id)`` keyset), and advanced by
    the opaque ``next_cursor`` when more items exist. The archival filter
    defaults to current (non-archived) attempts. Every item carries the
    ownership-separated summary and the backend-generated actions.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _validate_page_filters(statuses, archival)
    after = _validate_page_cursor(cursor)
    with application_span(_READS_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_LIST_SPAN_NAME)
        require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
        with composed_read_transaction(pool) as tx_view:
            tasks = list_workspace_tasks_page(
                tx_view,
                workspace_id=workspace_id,
                statuses=statuses,
                archival=archival,
                after_created_at=None if after is None else after[0],
                after_task_id=None if after is None else after[1],
                limit=TASK_PAGE_LIMIT + 1,
            )
            has_more = len(tasks) > TASK_PAGE_LIMIT
            tasks = tasks[:TASK_PAGE_LIMIT]
            items = [
                _compose_list_item(tx_view, task=task, profile_id=profile_id) for task in tasks
            ]
            next_cursor = (
                encode_task_page_cursor(tasks[-1].created_at, tasks[-1].id)
                if has_more and tasks
                else None
            )
        return items, next_cursor


def list_repository_tasks_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    statuses: Sequence[TaskStatus],
    archival: str = "current",
    cursor: str | None = None,
) -> tuple[list[TaskSummaryItem], str | None]:
    """List one Workspace Repository's Tasks for the authenticated Owner (#376).

    Owner-authorized, database-only, and side-effect free; the archival
    filter defaults to current (non-archived) attempts.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _validate_page_filters(statuses, archival)
    after = _validate_page_cursor(cursor)
    with application_span(_READS_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_LIST_SPAN_NAME)
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        with composed_read_transaction(pool) as tx_view:
            tasks = list_repository_tasks_page(
                tx_view,
                workspace_id=workspace_id,
                repository_id=repository_id,
                statuses=statuses,
                archival=archival,
                after_created_at=None if after is None else after[0],
                after_task_id=None if after is None else after[1],
                limit=TASK_PAGE_LIMIT + 1,
            )
            has_more = len(tasks) > TASK_PAGE_LIMIT
            tasks = tasks[:TASK_PAGE_LIMIT]
            items = [
                _compose_list_item(tx_view, task=task, profile_id=profile_id) for task in tasks
            ]
            next_cursor = (
                encode_task_page_cursor(tasks[-1].created_at, tasks[-1].id)
                if has_more and tasks
                else None
            )
        return items, next_cursor


def list_issue_tasks_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    issue_id: UUID,
    statuses: Sequence[TaskStatus],
    archival: str = "all",
    cursor: str | None = None,
) -> tuple[list[TaskSummaryItem], str | None]:
    """List every attempt for one G2a issue projection (issue #376).

    The exact G2a issue projection UUID is the only address; a foreign
    Workspace or absent projection is the uniform not-found outcome. Every
    Task is one attempt — there is no separate Attempt entity — and the
    list includes archived attempts by default.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_uuid_command(issue_id, "issue_id")
    _validate_page_filters(statuses, archival)
    after = _validate_page_cursor(cursor)
    with application_span(_READS_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_LIST_SPAN_NAME)
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        with composed_read_transaction(pool) as tx_view:
            projection = find_github_issue_by_id(
                tx_view, workspace_id=workspace_id, repository_id=repository_id, issue_id=issue_id
            )
            if projection is None:
                raise NotFoundError(
                    "the requested github issue is not available in this repository"
                )
            tasks = list_issue_tasks_page(
                tx_view,
                workspace_id=workspace_id,
                repository_id=repository_id,
                github_issue_id=projection.identity.github_issue_id,
                statuses=statuses,
                archival=archival,
                after_created_at=None if after is None else after[0],
                after_task_id=None if after is None else after[1],
                limit=ISSUE_ATTEMPTS_PAGE_LIMIT + 1,
            )
            has_more = len(tasks) > ISSUE_ATTEMPTS_PAGE_LIMIT
            tasks = tasks[:ISSUE_ATTEMPTS_PAGE_LIMIT]
            items = [
                _compose_list_item(tx_view, task=task, profile_id=profile_id) for task in tasks
            ]
            next_cursor = (
                encode_task_page_cursor(tasks[-1].created_at, tasks[-1].id)
                if has_more and tasks
                else None
            )
        return items, next_cursor


def _compose_list_item(
    tx_view: DatabasePool,
    *,
    task: Task,
    profile_id: UUID,
) -> TaskSummaryItem:
    """Compose one list item's ownership-separated summary (issue #376).

    The page-level batched reads (issues, canonical PRs, current gates,
    costs, current blocks) ran before this composer; the per-Task action
    eligibility reuses the authoritative predicates inside the same composed
    snapshot transaction, so every offered action is computed from the same
    coherent durable observation the commands themselves revalidate against.
    The Task's actual session bindings supply the discussion-prerequisite
    evidence, exactly as the detail projection supplies them: an eligible
    READY Reviewer is never suppressed because the list is a summary.
    """
    issue = find_github_issue_projections_by_identity(
        tx_view, repository_id=task.repository_id, github_issue_ids=[task.github_issue_id]
    ).get(task.github_issue_id)
    pull_request = get_task_pull_request_for_task(tx_view, task_id=task.id)
    gate = (
        None
        if task.current_owner_gate_id is None
        else list_owner_gates_by_ids(tx_view, owner_gate_ids=[task.current_owner_gate_id]).get(
            task.current_owner_gate_id
        )
    )
    blocks = list_current_task_blocks_for_tasks(tx_view, task_ids=[task.id]).get(task.id, [])
    costs = sum_task_retained_session_costs_for_tasks(tx_view, task_ids=[task.id]).get(task.id)
    sessions = list_task_agent_sessions_for_tasks(tx_view, task_ids=[task.id]).get(task.id, [])
    observation = TaskObservationRecord(
        task=task,
        current_gate=gate,
        current_blocks=tuple(blocks),
        current_plan=None,
        sessions=tuple(sessions),
        executions=(),
        executions_total=0,
        review_loops=(),
        review_iterations=(),
        runtime_requests=(),
        pending_runtime_requests_total=0,
        interrupt_intents=(),
        outstanding_interrupt_intents_total=0,
        resume_intents=(),
        outstanding_resume_intents_total=0,
        pull_request_close_intent=None,
        receipts=(),
        outstanding_receipts_total=0,
        send_claims=(),
        planning_bootstrap=None,
        pull_request=pull_request,
        issue_projection=issue,
        session_costs_known_total_usd=None if costs is None else costs.known_total_usd,
        session_costs_has_unobserved=False if costs is None else costs.has_unobserved_sessions,
    )
    return _compose_summary(
        tx_view,
        task=task,
        observation=observation,
        issue=issue,
        pull_request=pull_request,
        account_operational=_account_operational(tx_view, profile_id=profile_id),
    )


def get_task_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
) -> TaskDetail:
    """Return one Task's composed detail for the authenticated Owner (#376).

    Owner-authorized, database-only, and side-effect free. The addressed
    internal Task UUID is the only address; a foreign-Workspace or absent
    Task is the uniform not-found outcome, never a probing oracle. The
    whole detail — current state token, gate, block, subject facts, and the
    backend-generated actions — composes from ONE coherent durable
    observation (the service's composed transaction).
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with application_span(_READS_TRACER_SCOPE, _DETAIL_SPAN_NAME) as span:
        annotate_span(span, operation=_DETAIL_SPAN_NAME)
        require_workspace_task(
            pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
        )
        with composed_read_transaction(pool) as tx_view:
            observation = load_task_observation(tx_view, workspace_id=workspace_id, task_id=task_id)
            if observation is None:
                raise NotFoundError("the requested task is not available in this workspace")
            task = observation.task
            sessions = observation.sessions
            pending_interactions = list_pending_task_runtime_interactions(
                tx_view, workspace_id=workspace_id, task_id=task_id
            )
            summary = _compose_summary(
                tx_view,
                task=task,
                observation=observation,
                issue=observation.issue_projection,
                pull_request=observation.pull_request,
                account_operational=_account_operational(tx_view, profile_id=profile_id),
            )
            current_plan = observation.current_plan
            session_facts = tuple(
                TaskSessionFacts(
                    session=session,
                    configured_provider=_configured_pair(session)[0],
                    configured_model=_configured_pair(session)[1],
                )
                for session in sessions
            )
            executions_total = observation.executions_total
            execution_facts = tuple(
                _execution_facts(execution) for execution in observation.executions
            )
            iterations_by_loop: dict[UUID, list[ReviewIteration]] = {}
            for iteration in observation.review_iterations:
                iterations_by_loop.setdefault(iteration.review_loop_id, []).append(iteration)
            review_loop_facts = tuple(
                _review_loop_facts(
                    loop=loop,
                    iterations=iterations_by_loop.get(loop.id, []),
                    task=task,
                    pull_request=observation.pull_request,
                )
                for loop in observation.review_loops
                if loop.status is ReviewLoopStatus.OPEN
            )
            intent_facts, intents_total = _outstanding_intents(observation)
            detail = TaskDetail(
                summary=summary,
                current_plan=current_plan,
                sessions=session_facts,
                executions=execution_facts,
                executions_total=executions_total,
                planning_bootstrap_stage=(
                    None
                    if observation.planning_bootstrap is None
                    or observation.planning_bootstrap.stage is None
                    else observation.planning_bootstrap.stage.value
                ),
                review_loops=review_loop_facts,
                pending_runtime_interactions=tuple(
                    TaskPendingInteractionFacts(
                        operation=interaction.operation.value,
                        role=interaction.role.value,
                        task_agent_session_id=interaction.task_agent_session_id,
                        subject=dict(interaction.subject),
                        started_at=interaction.started_at,
                    )
                    for interaction in pending_interactions
                ),
                pending_runtime_requests=tuple(
                    request
                    for request in observation.runtime_requests
                    if request.status is RuntimeRequestStatus.PENDING
                ),
                pending_runtime_requests_total=observation.pending_runtime_requests_total,
                outstanding_receipts=observation.receipts,
                outstanding_receipts_total=observation.outstanding_receipts_total,
                outstanding_intents=intent_facts,
                outstanding_intents_total=intents_total,
            )
        return detail
