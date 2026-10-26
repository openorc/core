"""E10b: the Owner activity-interruption command (issue #228).

``abort_task_activity`` is the ONE Owner-authorized Abort command: while the
Task is autonomously working, it revokes OpenOrc workflow progression by
moving the Task to ``blocked``, then requests the Cline-specific Interrupt on
the existing exact ClineBox session(s). Abort is an OpenOrc workflow action;
``ClineAdapter.abort_activity`` is the Cline-specific Interrupt mechanism. It
is not a Task cancellation, a session deletion, a resume command, or a
recovery path — F4b (#230) owns later non-cancellation recovery.

The command has exactly two phases, distinguished by authoritative durable
state and never by Cline's instantaneous busy/status field:

- **Dispatch** (current Task in an autonomous workflow phase — ``planning``,
  ``implementing``, ``reviewing`` — or at a pending OwnerGate with a
  demonstrably in-flight runtime interaction): one short composed
  transaction proves the account-operational barrier and the exact
  Profile-owned Workspace/Task scope, takes the Task row lock, resolves the
  exact existing bound Producer/Reviewer ``TaskAgentSession`` targets the
  caller's admitted activity context names, refuses any already-outstanding
  interruption, then atomically creates the ``owner_action_required``
  TaskBlock (with the safe ``owner_abort`` context), moves the Task to
  ``blocked`` under its token (a pending gate stays truthful pending
  history, unusable while blocked), records the OWNER ``task_blocked``
  audit, and registers the one-shot exact-session Interrupt intents. AFTER
  the commit — never inside a transaction — the existing
  ``ClineAdapter.abort_activity`` is invoked exactly once per exact target,
  and each safe outcome (acknowledged / definitively failed / uncertain) is
  recorded on its durable intent. An SDK acknowledgement proves request
  acceptance, never Cline quiescence: an acknowledged interrupt stays
  ``unsettled`` until positively settled.
- **Settlement-or-refusal** (current Task already ``blocked`` by this
  control): a duplicate or post-restart Abort command never re-interrupts
  blindly. Without positive settlement evidence the command refuses and
  surfaces the unresolved interruption; with the caller-attested positive
  evidence (both D6b preconditions) the existing
  ``ClineAdapter.settle_abort`` is invoked outside any transaction and a
  positively settled interruption is recorded on its durable intent — the
  only state in which Cancel becomes available again. A refused settlement
  keeps every guard latched and the intent unsettled.

The Task stays ``blocked`` regardless of the Interrupt outcome; late formal
results can never advance it. Terminal session identities are preserved:
the command never calls ``stop``/``start``/``send``, never allocates a
replacement session, and never sends an agent prompt.

**Trusted internal admission (issue #240).** The same blocking machinery is
the one Owner-authorized cost-limit control:
:func:`apply_owner_authorized_cost_limit_block` /
:func:`establish_owner_authorized_cost_limit_block` compose the identical
atomic blocking composition — one ``COST_LIMIT`` TaskBlock with the safe
``owner_abort`` control context, the guarded ``blocked`` transition, the
OWNER ``task_blocked`` audit, and the exact one-shot Interrupt intents —
driven by the Task's persisted standing budget decision instead of a live
Owner command. The accountable Owner Profile is derived from the durable
Workspace row (never from a telemetry event), the threshold is re-verified
under the Task row lock, the exact in-flight targets are derived from
durable Task/session/work facts (open Executions anchored to the bound
session; OPEN ReviewLoops carrying an unfinalized iteration), and a Task
idle at a pending Owner gate enters ``BLOCKED`` with no gratuitous
Interrupt. There is deliberately no second abort implementation: settlement,
dispatch, and outcome recording are exactly E10b's, and the interactive
Owner Abort command's contract is unchanged. F4b (issue #230) owns
``COST_LIMIT`` resolution; nothing here resumes or retries the blocked Task.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from openorc.adapters.agent_runtime import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.cline import ClineAdapter, ClineSettlementRefusal
from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import Execution, ExecutionStatus
from openorc.domain.intents import TaskInterruptIntent, TaskInterruptIntentStatus
from openorc.domain.reviews import ReviewLoopStatus
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import executions as execution_records
from openorc.persistence import intents as intent_records
from openorc.persistence import ownership as ownership_records
from openorc.persistence import reviews as review_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "CostLimitBlockOutcomeKind",
    "OwnerAuthorizedCostLimitBlock",
    "TaskAbortOutcome",
    "TaskAbortOutcomeKind",
    "TaskInterruptOutcome",
    "TaskInterruptSettlement",
    "abort_task_activity",
    "apply_owner_authorized_cost_limit_block",
    "dispatch_registered_task_interrupt",
    "establish_owner_authorized_cost_limit_block",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_abort"
_ABORT_SPAN_NAME = "task_abort.abort_task_activity"

# The workflow phases whose authorized autonomous progression (or in-flight
# gate interaction) an Owner Abort may stop. Every other current status has
# no authorized active runtime work for this control to interrupt.
_DISPATCH_ELIGIBLE_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.PLANNING,
        TaskStatus.IMPLEMENTING,
        TaskStatus.REVIEWING,
        TaskStatus.WAITING_FOR_OWNER,
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


class TaskAbortOutcomeKind(StrEnum):
    """The classified outcome of one Owner Abort command.

    ``interrupted`` applied the blocking composition and dispatched the
    exact-session Interrupts; ``settled`` positively settled every
    outstanding interruption of the already-blocked Task; ``unresolved``
    attempted settlement and the adapter's guard discipline refused — the
    interruption stays outstanding and surfaced.
    """

    INTERRUPTED = "interrupted"
    SETTLED = "settled"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class TaskInterruptSettlement:
    """The caller-attested positive evidence for settling one interruption.

    Both D6b preconditions must hold affirmatively: the interrupted runtime
    work has settled or cannot still execute, and the applicable logical
    control has been accounted for by its owning caller. Negative or absent
    evidence is never a settlement command — the absence of an
    adapter-local latch or an SDK status reading is never settlement
    evidence.
    """

    runtime_work_settled: bool
    operation_accounted_for: bool

    def __post_init__(self) -> None:
        if self.runtime_work_settled is not True or self.operation_accounted_for is not True:
            raise InvalidCommandError(
                "settlement requires positive caller evidence that the interrupted "
                "runtime work has settled or cannot still execute and that the "
                "logical control is accounted for"
            )


@dataclass(frozen=True, slots=True)
class TaskInterruptOutcome:
    """The safe durable outcome of one exact-session interrupt handling."""

    task_interrupt_intent_id: UUID
    workflow_role: WorkflowRole
    status: TaskInterruptIntentStatus
    detail: str


@dataclass(frozen=True, slots=True)
class TaskAbortOutcome:
    """The typed result of one Owner Abort command."""

    kind: TaskAbortOutcomeKind
    task: Task
    task_block: TaskBlock | None
    interrupt_outcomes: tuple[TaskInterruptOutcome, ...]
    accounted_executions: tuple[Execution, ...] = ()


@dataclass(frozen=True, slots=True)
class _InterruptTarget:
    """One exact admitted Task/role session target of the interruption."""

    task_agent_session_id: UUID
    workflow_role: WorkflowRole
    external_session_id: str


@dataclass(frozen=True, slots=True)
class _DispatchPlan:
    """The committed blocking composition awaiting its post-commit dispatch."""

    task: Task
    task_block: TaskBlock
    targets: tuple[tuple[TaskInterruptIntent, _InterruptTarget], ...]


@dataclass(frozen=True, slots=True)
class _SettlementPlan:
    """The outstanding interruptions of an already-blocked Task to settle."""

    task: Task
    targets: tuple[tuple[TaskInterruptIntent, str], ...]


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _validate_target_roles(target_roles: Sequence[WorkflowRole]) -> tuple[WorkflowRole, ...]:
    """Validate the admitted activity context's roles as a command shape.

    The sequence may be empty only for the settlement phase of an
    already-blocked Task; the dispatch phase requires at least one exact
    target role. Duplicates are collapsed deterministically.
    """
    if not isinstance(target_roles, (list, tuple)):
        raise InvalidCommandError("target_roles must be a sequence of workflow roles")
    for role in target_roles:
        if not isinstance(role, WorkflowRole):
            raise InvalidCommandError("target_roles must contain only WorkflowRole values")
    return tuple(dict.fromkeys(target_roles))


def _resolve_interrupt_target(
    transaction_pool: DatabasePool, *, task: Task, role: WorkflowRole
) -> _InterruptTarget:
    """Resolve the exact existing bound Task/role session one abort targets.

    The addressed role's durable binding must exist in the Task's Workspace,
    be a READY bound session, and carry its immutable external session
    identity — a binding that never initialized, was lost, or ended cannot
    be interrupted. The binding identity is re-proven, never allocated or
    replaced.
    """
    session = session_records.get_task_agent_session(transaction_pool, task_id=task.id, role=role)
    if session is None or session.task_id != task.id or session.workspace_id != task.workspace_id:
        raise NotFoundError("the requested agent session is not available for this task")
    if session.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the addressed session binding is not a ready external session to interrupt"
        )
    if session.external_session_id is None or not session.external_session_id.strip():
        raise ConflictError("the addressed session binding carries no external session identity")
    return _InterruptTarget(
        task_agent_session_id=session.id,
        workflow_role=role,
        external_session_id=session.external_session_id,
    )


def _has_admitted_in_flight_activity(
    transaction_pool: DatabasePool, *, task: Task, target: _InterruptTarget
) -> bool:
    """Whether the admitted role/session demonstrates durable in-flight activity.

    The phase-specific eligibility fact shared by the interactive Owner
    Abort and the cost-policy control (issues #228/#240): a Producer target
    demonstrates in-flight activity by owning a durably OPEN Execution
    anchored to that exact session (``producer_session_id``), a Reviewer
    target by owning an OPEN ReviewLoop carrying an unfinalized
    ReviewIteration — the dispatched review turn still in flight. Never the
    primary Task status alone, and never an SDK status reading.
    """
    if target.workflow_role is WorkflowRole.PRODUCER:
        executions = execution_records.list_task_executions(transaction_pool, task_id=task.id)
        return any(
            execution.producer_session_id == target.task_agent_session_id
            and execution.status in _OPEN_EXECUTION_STATUSES
            for execution in executions
        )
    loops = review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.OPEN
    )
    for loop in loops:
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        if any(iteration.outcome is None for iteration in iterations):
            return True
    return False


def _require_admitted_in_flight_activity(
    transaction_pool: DatabasePool, *, task: Task, target: _InterruptTarget
) -> None:
    """Bind the admitted role/session to demonstrated in-flight activity.

    The phase-specific eligibility contract (issue #228): the autonomous
    workflow phases (``planning``/``implementing``/``reviewing``) are
    eligible for Owner Abort against their appropriate existing bound
    runtime session, while a WAITING_FOR_OWNER Task must demonstrate
    in-flight activity — an ordinary pending gate does not establish active
    work, so interrupting over one is refused. The demonstration reuses the
    existing durable facts only, through
    :func:`_has_admitted_in_flight_activity`.
    """
    if not _has_admitted_in_flight_activity(transaction_pool, task=task, target=target):
        raise ConflictError(
            "the addressed session owns no durably admitted in-flight runtime activity to interrupt"
        )


def _owner_abort_block_context(targets: tuple[_InterruptTarget, ...]) -> dict[str, object]:
    """The safe typed ``owner_abort`` TaskBlock context.

    Only internal Task/session identities and role names — never transcript
    content, prompts, credentials, or raw provider errors.
    """
    return {
        "control": "owner_abort",
        "target_roles": [target.workflow_role.value for target in targets],
        "task_agent_session_ids": [str(target.task_agent_session_id) for target in targets],
    }


def _establish_abort_plan(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    target_roles: tuple[WorkflowRole, ...],
    settlement: TaskInterruptSettlement | None,
) -> _DispatchPlan | _SettlementPlan:
    """The authoritative pre-external composition of one Abort command.

    One short composed transaction (before any Cline I/O): the
    account-operational barrier and exact Owner scope first, the Task row
    lock, the currentness re-proof, then the phase classification. The
    dispatch branch applies the whole blocking composition atomically; the
    settlement branch only reads the outstanding intents (their settlement
    happens outside the transaction through the adapter).
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
        if task.status is TaskStatus.BLOCKED:
            unsettled = intent_records.list_unsettled_task_interrupt_intents(
                transaction_pool, task_id=task_id
            )
            if not unsettled:
                raise ConflictError(
                    "the blocked task carries no owner activity interruption to settle"
                )
            if settlement is None:
                raise ConflictError(
                    "the owner activity interruption is unresolved; it must be "
                    "positively settled or reconciled before another abort command"
                )
            return _SettlementPlan(
                task=task,
                targets=tuple((intent, intent.external_session_id) for intent in unsettled),
            )
        if task.status not in _DISPATCH_ELIGIBLE_STATUSES:
            raise ConflictError(
                "the addressed task has no authorized active workflow activity to abort"
            )
        if settlement is not None:
            raise InvalidCommandError(
                "settlement evidence is only admitted for a task already blocked by the owner abort"
            )
        targets = tuple(
            _resolve_interrupt_target(transaction_pool, task=task, role=role)
            for role in target_roles
        )
        if intent_records.list_unsettled_task_interrupt_intents(transaction_pool, task_id=task_id):
            raise StaleOperationError(
                "an owner activity interruption is already outstanding; it must be "
                "settled or reconciled before another abort"
            )
        if task.status is TaskStatus.WAITING_FOR_OWNER:
            # A pending OwnerGate does not by itself establish active work:
            # interrupting over an ordinary idle gate is refused, while a
            # demonstrably in-flight runtime interaction at the gate (a
            # paused-for-approval Execution, an unfinalized review turn) is
            # handled by the same safety rule as the autonomous phases. The
            # autonomous phases are eligible against their appropriate
            # existing bound runtime session without further proof.
            for target in targets:
                _require_admitted_in_flight_activity(transaction_pool, task=task, target=target)
        return _apply_owner_authorized_block(
            transaction_pool,
            task=task,
            owner_profile_id=profile_id,
            reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
            context=_owner_abort_block_context(targets),
            targets=targets,
        )


def _apply_owner_authorized_block(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    owner_profile_id: UUID,
    reason: TaskBlockReason,
    context: Mapping[str, object],
    targets: tuple[_InterruptTarget, ...],
) -> _DispatchPlan:
    """The atomic Owner-attributed blocking composition, before any external I/O.

    The one shared composition of the interactive Owner Abort (issue #228)
    and the persisted standing cost-policy control (issue #240), inside the
    caller's open composed transaction: one current TaskBlock with its
    reason/recovery context, the guarded ``blocked`` transition under the
    Task's exact state token, the OWNER ``task_blocked`` audit, and the
    exact one-shot Interrupt intents for the resolved targets — created
    BEFORE any Cline I/O and committed or rolled back together. Failure of
    any step rolls back every blocking decision fact.
    """
    block = block_records.create_task_block(
        transaction_pool,
        workspace_id=task.workspace_id,
        task_id=task.id,
        reason=reason,
        context=context,
    )
    blocked = task_records.update_task_status(
        transaction_pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.BLOCKED,
    )
    if blocked is None:
        raise StaleOperationError(
            "the task could not move to blocked; the owner-authorized control is stale"
        )
    event_coordination.record_task_blocked_event(
        transaction_pool,
        workspace_id=task.workspace_id,
        task_id=task.id,
        task_block_id=block.id,
        actor=event_coordination.owner_actor(owner_profile_id),
    )
    intents = tuple(
        (
            intent_records.create_task_interrupt_intent(
                transaction_pool,
                workspace_id=task.workspace_id,
                task_id=task.id,
                task_agent_session_id=target.task_agent_session_id,
                workflow_role=target.workflow_role,
                external_session_id=target.external_session_id,
                requested_by_profile_id=owner_profile_id,
            ),
            target,
        )
        for target in targets
    )
    return _DispatchPlan(task=blocked, task_block=block, targets=intents)


class CostLimitBlockOutcomeKind(StrEnum):
    """The classified outcome of one persisted standing cost-limit blocking.

    ``blocked_with_interrupts`` composed the block and registered exact
    Interrupt intents for durably in-flight sessions (dispatch happens
    post-commit, outside any transaction); ``blocked_idle`` composed the
    block for a Task with no durably demonstrable in-flight runtime
    activity — no gratuitous Interrupt exists; ``already_blocked`` found the
    Task already blocked (one durable block stands, never a duplicate);
    ``not_applicable`` found no current budget-controlled blocking decision
    under the lock (stale, foreign, archived, unauthorized, or the
    threshold no longer true).
    """

    BLOCKED_WITH_INTERRUPTS = "blocked_with_interrupts"
    BLOCKED_IDLE = "blocked_idle"
    ALREADY_BLOCKED = "already_blocked"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class OwnerAuthorizedCostLimitBlock:
    """The typed result of one persisted standing cost-limit blocking.

    ``task``/``task_block`` are present only for the two blocking kinds.
    ``interrupt_dispatches`` carries the exact post-commit obligations as
    ``(intent, external_session_id)`` pairs: dispatch them through
    :func:`dispatch_registered_task_interrupt` strictly after the
    composition committed and with no database transaction open.
    """

    kind: CostLimitBlockOutcomeKind
    task: Task | None = None
    task_block: TaskBlock | None = None
    interrupt_dispatches: tuple[tuple[TaskInterruptIntent, str], ...] = ()


def _resolve_cost_limit_interrupt_targets(
    transaction_pool: DatabasePool, *, task: Task
) -> tuple[_InterruptTarget, ...]:
    """Derive the exact in-flight interrupt targets from durable facts only.

    The cost-policy control never trusts Cline's transient status and never
    hardcodes a role list: every READY bound session of the Task — the
    role-agnostic binding read — is examined against the shared durable
    in-flight facts (a Producer-bound open Execution anchored to that exact
    session; a Reviewer-owned OPEN ReviewLoop carrying an unfinalized
    iteration). Sessions without demonstrated in-flight activity are never
    gratuitously interrupted: where the Task is idle (typically at a pending
    Owner gate), the block composes with zero targets and the pending gate
    stays truthful pending history for F4b.
    """
    targets: list[_InterruptTarget] = []
    for session in session_records.list_task_agent_sessions(transaction_pool, task_id=task.id):
        if session.lifecycle_status is not TaskSessionLifecycleStatus.READY:
            continue
        if session.external_session_id is None or not session.external_session_id.strip():
            continue
        target = _InterruptTarget(
            task_agent_session_id=session.id,
            workflow_role=session.role,
            external_session_id=session.external_session_id,
        )
        if _has_admitted_in_flight_activity(transaction_pool, task=task, target=target):
            targets.append(target)
    return tuple(targets)


def _cost_limit_block_context(
    *,
    targets: tuple[_InterruptTarget, ...],
    observed_cost_usd: Decimal,
    authorized_budget_usd: Decimal,
    has_unobserved_sessions: bool,
) -> dict[str, object]:
    """The safe typed ``owner_abort`` TaskBlock context of one cost-limit block.

    It is still an Owner-authorized ``owner_abort`` control — the standing
    budget decision — so the context records that control identity, the
    exact threshold facts (observed known total and authorized budget as
    exact decimal strings; never floats), whether any constituent reading
    was unknown, and the interrupted session identities. Never transcript
    content, prompts, credentials, or raw provider errors.
    """
    return {
        "control": "owner_abort",
        "reason_provenance": "owner_authorized_task_budget",
        "observed_cost_usd": str(observed_cost_usd),
        "authorized_budget_usd": str(authorized_budget_usd),
        "aggregate_includes_unobserved_sessions": has_unobserved_sessions,
        "target_roles": [target.workflow_role.value for target in targets],
        "task_agent_session_ids": [str(target.task_agent_session_id) for target in targets],
    }


def apply_owner_authorized_cost_limit_block(
    transaction_pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> OwnerAuthorizedCostLimitBlock:
    """Compose the Owner-authorized cost-limit block inside an open transaction.

    The trusted internal admission of the persisted standing budget decision
    (issue #240) — services only, never an API/RQ surface. Requires an open
    composed transaction (call it inside one; it acquires the Task row lock
    first and every subsequent read/write keeps that ordering). Everything
    is verified against the locked durable state: the current non-archived
    Task in the exact Workspace, its non-NULL authorized budget, the
    DB-derived accountable Workspace Owner Profile (behind the same
    account-operational barrier every Owner-attributed mutation composes),
    and the threshold re-checked against the role-agnostic retained-cost
    aggregate under the lock. An already-blocked Task never receives a
    duplicate block, duplicate intent, or new interrupt; an
    archived/terminal, foreign, or unauthorized Task and a threshold no
    longer true under the lock are classified no-ops, never errors. A
    detected cost-limit policy never silently settles or cancels existing
    work.
    """
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id or locked.archived_at is not None:
        return OwnerAuthorizedCostLimitBlock(kind=CostLimitBlockOutcomeKind.NOT_APPLICABLE)
    if locked.status is TaskStatus.BLOCKED:
        return OwnerAuthorizedCostLimitBlock(kind=CostLimitBlockOutcomeKind.ALREADY_BLOCKED)
    if locked.status not in _DISPATCH_ELIGIBLE_STATUSES:
        return OwnerAuthorizedCostLimitBlock(kind=CostLimitBlockOutcomeKind.NOT_APPLICABLE)
    budget = locked.authorized_task_budget_usd
    if budget is None:
        # No standing spending authorization exists (disabled, or a
        # pre-migration started Task): no policy decision is ever invented.
        return OwnerAuthorizedCostLimitBlock(kind=CostLimitBlockOutcomeKind.NOT_APPLICABLE)
    workspace = ownership_records.get_workspace(transaction_pool, workspace_id=workspace_id)
    if workspace is None:
        # The locked Task proves its Workspace; a missing row here would be
        # a durable invariant violation, never guessed around.
        raise ConflictError("the task's workspace is no longer available; the cost policy is stale")
    # The accountable Owner is the durable Workspace Owner, never a
    # telemetry event's provenance; the account-operational barrier composes
    # so a claimed deletion still latches this Owner-attributed mutation.
    require_account_operational(transaction_pool, profile_id=workspace.owner_profile_id)
    costs = session_records.sum_task_retained_session_costs(transaction_pool, task_id=task_id)
    if costs.known_total_usd is None or costs.known_total_usd < budget:
        # The threshold is not true under the lock: refuse to manufacture a
        # blocking decision from earlier, weaker observations.
        return OwnerAuthorizedCostLimitBlock(kind=CostLimitBlockOutcomeKind.NOT_APPLICABLE)
    targets = _resolve_cost_limit_interrupt_targets(transaction_pool, task=locked)
    plan = _apply_owner_authorized_block(
        transaction_pool,
        task=locked,
        owner_profile_id=workspace.owner_profile_id,
        reason=TaskBlockReason.COST_LIMIT,
        context=_cost_limit_block_context(
            targets=targets,
            observed_cost_usd=costs.known_total_usd,
            authorized_budget_usd=budget,
            has_unobserved_sessions=costs.has_unobserved_sessions,
        ),
        targets=targets,
    )
    return OwnerAuthorizedCostLimitBlock(
        kind=(
            CostLimitBlockOutcomeKind.BLOCKED_WITH_INTERRUPTS
            if targets
            else CostLimitBlockOutcomeKind.BLOCKED_IDLE
        ),
        task=plan.task,
        task_block=plan.task_block,
        interrupt_dispatches=tuple(
            (intent, target.external_session_id) for intent, target in plan.targets
        ),
    )


def establish_owner_authorized_cost_limit_block(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> OwnerAuthorizedCostLimitBlock:
    """Compose the Owner-authorized cost-limit block in one short transaction.

    The pre-dispatch seam for #88 F3a: one short composed transaction
    verifies the current Task/budget/threshold under the Task row lock and,
    exactly when the persisted standing decision applies, commits the
    ``COST_LIMIT`` block, the guarded ``blocked`` transition, the OWNER
    audit, and the exact one-shot Interrupt intents together. Returns the
    typed outcome; dispatch obligations are handled strictly after the
    commit through :func:`dispatch_registered_task_interrupt` — never
    inside any transaction.
    """
    with application_span(
        _SERVICE_TRACER_SCOPE, "task_abort.establish_owner_authorized_cost_limit_block"
    ) as span:
        annotate_span(
            span,
            operation="task_abort.establish_owner_authorized_cost_limit_block",
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            return apply_owner_authorized_cost_limit_block(
                transaction_pool, workspace_id=workspace_id, task_id=task_id
            )


def dispatch_registered_task_interrupt(
    pool: DatabasePool,
    cline_adapter: ClineAdapter,
    *,
    intent: TaskInterruptIntent,
    external_session_id: str,
) -> TaskInterruptOutcome:
    """Dispatch one registered Interrupt intent post-commit and record its outcome.

    The trusted internal seam over the exact same E10b dispatch machinery
    (issue #228) for the persisted standing cost-policy control (issue
    #240): strictly after the blocking composition committed, with no
    database transaction open, the adapter's classified outcomes map onto
    the durable intent exactly as the interactive Owner Abort records them.
    """
    return _dispatch_interrupt(
        pool,
        cline_adapter,
        intent=intent,
        target=_InterruptTarget(
            task_agent_session_id=intent.task_agent_session_id,
            workflow_role=intent.workflow_role,
            external_session_id=external_session_id,
        ),
    )


def _account_for_interrupted_executions(
    pool: DatabasePool, *, task_id: UUID
) -> tuple[Execution, ...]:
    """The exact-attempt accounting of positively established cessation.

    Called only after every outstanding interruption settled positively: the
    Task's durably open Execution attempts transition to the existing
    ``cancelled`` final status through the guarded conditional primitive, so
    the implementation work the Owner interrupted is accounted for and the
    later Cancel eligibility veto no longer sees outstanding runtime work.
    Truthful history is preserved: a raced finalization (the guarded update
    returning ``None``) is left exactly as the other writer recorded it, no
    completion result is invented, and no interruption state machine exists.
    """
    accounted: list[Execution] = []
    for execution in execution_records.list_task_executions(pool, task_id=task_id):
        if execution.status not in _OPEN_EXECUTION_STATUSES:
            continue
        updated = execution_records.update_execution_status(
            pool,
            execution_id=execution.id,
            expected_status=execution.status,
            next_status=ExecutionStatus.CANCELLED,
        )
        if updated is not None:
            accounted.append(updated)
    return tuple(accounted)


def _interrupt_outcome(
    intent: TaskInterruptIntent,
    *,
    status: TaskInterruptIntentStatus,
    detail: str,
) -> TaskInterruptOutcome:
    return TaskInterruptOutcome(
        task_interrupt_intent_id=intent.id,
        workflow_role=intent.workflow_role,
        status=status,
        detail=detail,
    )


def _dispatch_interrupt(
    pool: DatabasePool,
    cline_adapter: ClineAdapter,
    *,
    intent: TaskInterruptIntent,
    target: _InterruptTarget,
) -> TaskInterruptOutcome:
    """Dispatch one exact-session Interrupt and record its safe durable outcome.

    Called strictly after the blocking composition committed, with no
    database transaction open. The adapter's classified outcomes map onto
    the durable intent: a definitive non-delivery records ``failed``, an
    uncertain outcome records ``unknown`` (never replayed), and an
    acknowledgement records the safe dispatch fact while the intent stays
    ``unsettled`` — an SDK acknowledgement proves request acceptance, never
    Cline quiescence. A raced terminal outcome is left untouched: the
    durable record the flow already holds is the truth.
    """
    try:
        cline_adapter.abort_activity(target.external_session_id)
    except AgentRuntimeUncertainOutcomeError:
        recorded = intent_records.record_task_interrupt_intent_outcome(
            pool,
            task_interrupt_intent_id=intent.id,
            status=TaskInterruptIntentStatus.UNKNOWN,
            outcome={"dispatch": "uncertain"},
        )
        return _interrupt_outcome(
            intent,
            status=recorded.status if recorded is not None else intent.status,
            detail="uncertain",
        )
    except AgentRuntimeError:
        recorded = intent_records.record_task_interrupt_intent_outcome(
            pool,
            task_interrupt_intent_id=intent.id,
            status=TaskInterruptIntentStatus.FAILED,
            outcome={"dispatch": "failed"},
        )
        return _interrupt_outcome(
            intent,
            status=recorded.status if recorded is not None else intent.status,
            detail="failed",
        )
    intent_records.annotate_unsettled_task_interrupt_intent(
        pool,
        task_interrupt_intent_id=intent.id,
        outcome={"dispatch": "acknowledged"},
    )
    return _interrupt_outcome(
        intent,
        status=TaskInterruptIntentStatus.UNSETTLED,
        detail="acknowledged",
    )


@dataclass(frozen=True, slots=True)
class _SettlementResult:
    """One adapter-level settlement attempt, not yet durably recorded."""

    intent: TaskInterruptIntent
    kind: str  # "settled" | "refused" | "unavailable"
    detail: str
    refusal: str | None


def _settle_interrupt_attempt(
    cline_adapter: ClineAdapter,
    *,
    intent: TaskInterruptIntent,
    external_session_id: str,
    settlement: TaskInterruptSettlement,
) -> _SettlementResult:
    """Attempt one D6b settlement under positive caller-attested evidence.

    The adapter's settlement discipline is invoked exactly as documented and
    its outcome is CLASSIFIED only: no durable write happens here, so the
    settlement records and the interrupted-Execution accounting can commit
    in one short transaction afterwards. A raced terminal outcome is left
    untouched by the recording step.
    """
    try:
        result = cline_adapter.settle_abort(
            external_session_id,
            runtime_work_settled=settlement.runtime_work_settled,
            operation_accounted_for=settlement.operation_accounted_for,
        )
    except AgentRuntimeError:
        return _SettlementResult(
            intent=intent, kind="unavailable", detail="unavailable", refusal=None
        )
    if result.settled:
        return _SettlementResult(intent=intent, kind="settled", detail="settled", refusal=None)
    refusal = (
        result.refusal.value if isinstance(result.refusal, ClineSettlementRefusal) else "refused"
    )
    return _SettlementResult(
        intent=intent, kind="refused", detail=f"refused:{refusal}", refusal=refusal
    )


def _record_settlement_outcomes(
    pool: DatabasePool,
    *,
    task_id: UUID,
    results: tuple[_SettlementResult, ...],
) -> tuple[tuple[TaskInterruptOutcome, ...], tuple[Execution, ...]]:
    """Record the settlement outcomes and the execution accounting atomically.

    ONE short composed transaction after the positive D6b settlement was
    established outside it: the settled intents advance to ``settled``, the
    refused ones are annotated in place (every guard stays latched), the
    unavailable attempts record ``unknown``, and — only when every
    outstanding interruption settled positively — the exact-attempt
    Execution accounting applies through the guarded conditional primitive.
    Everything commits or rolls back together, so a crash can never leave a
    settled interruption with an unaccounted open Execution (which would
    veto Cancel while the settlement entrypoint could not run again); the
    still-unsettled durable state remains truthfully re-settleable instead.
    A raced terminal outcome is left untouched: the durable record the flow
    already holds is the truth, and no outcome is ever manufactured.
    """
    with composed_transaction(pool) as transaction_pool:
        outcomes: list[TaskInterruptOutcome] = []
        for result in results:
            if result.kind == "settled":
                recorded = intent_records.record_task_interrupt_intent_outcome(
                    transaction_pool,
                    task_interrupt_intent_id=result.intent.id,
                    status=TaskInterruptIntentStatus.SETTLED,
                    outcome={"settlement": "confirmed"},
                )
                outcomes.append(
                    _interrupt_outcome(
                        result.intent,
                        status=recorded.status if recorded is not None else result.intent.status,
                        detail=result.detail,
                    )
                )
            elif result.kind == "unavailable":
                recorded = intent_records.record_task_interrupt_intent_outcome(
                    transaction_pool,
                    task_interrupt_intent_id=result.intent.id,
                    status=TaskInterruptIntentStatus.UNKNOWN,
                    outcome={"settlement": "unavailable"},
                )
                outcomes.append(
                    _interrupt_outcome(
                        result.intent,
                        status=recorded.status if recorded is not None else result.intent.status,
                        detail=result.detail,
                    )
                )
            else:
                assert result.refusal is not None
                intent_records.annotate_unsettled_task_interrupt_intent(
                    transaction_pool,
                    task_interrupt_intent_id=result.intent.id,
                    outcome={"settlement_refusal": result.refusal},
                )
                outcomes.append(
                    _interrupt_outcome(
                        result.intent,
                        status=TaskInterruptIntentStatus.UNSETTLED,
                        detail=result.detail,
                    )
                )
        all_settled = all(result.kind == "settled" for result in results)
        accounted_executions: tuple[Execution, ...] = ()
        if all_settled:
            # Positive cessation established for every outstanding
            # interruption: the exact-attempt accounting commits in the same
            # transaction as the settlement records, so the later Cancel
            # eligibility veto can never see a settled interruption with an
            # unaccounted open Execution.
            accounted_executions = _account_for_interrupted_executions(
                transaction_pool, task_id=task_id
            )
    return tuple(outcomes), accounted_executions


def abort_task_activity(
    pool: DatabasePool,
    cline_adapter: ClineAdapter,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    target_roles: Sequence[WorkflowRole],
    settlement: TaskInterruptSettlement | None = None,
) -> TaskAbortOutcome:
    """Abort the Task's authorized active workflow activity with an Interrupt.

    The ONE Owner-authorized Abort command (issue #228): one short composed
    transaction proves the account/Owner/currentness/role guards and applies
    the atomic ``owner_action_required`` block + ``blocked`` transition +
    OWNER ``task_blocked`` audit + durable one-shot Interrupt intents BEFORE
    any Cline I/O; the existing ``ClineAdapter.abort_activity`` is then
    invoked exactly once per exact admitted target, strictly outside any
    transaction, and each safe outcome is recorded on its durable intent.
    For an already-blocked Task the same command settles an outstanding
    interruption under positive caller-attested D6b evidence — never
    re-interrupting blindly — or refuses and surfaces the unresolved
    interruption. The Task stays ``blocked`` regardless of the Interrupt
    outcome; a late formal result can never advance it.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    roles = _validate_target_roles(target_roles)
    if settlement is not None and not isinstance(settlement, TaskInterruptSettlement):
        raise InvalidCommandError("settlement must be a TaskInterruptSettlement when supplied")
    if settlement is None and not roles:
        raise InvalidCommandError("abort requires at least one admitted workflow-role target")
    with application_span(_SERVICE_TRACER_SCOPE, _ABORT_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_ABORT_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        plan = _establish_abort_plan(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            target_roles=roles,
            settlement=settlement,
        )
        if isinstance(plan, _DispatchPlan):
            outcomes = tuple(
                _dispatch_interrupt(pool, cline_adapter, intent=intent, target=target)
                for intent, target in plan.targets
            )
            return TaskAbortOutcome(
                kind=TaskAbortOutcomeKind.INTERRUPTED,
                task=plan.task,
                task_block=plan.task_block,
                interrupt_outcomes=outcomes,
            )
        # A settlement plan exists only when the caller supplied the
        # positive settlement evidence (the transaction refuses otherwise).
        assert settlement is not None
        results = tuple(
            _settle_interrupt_attempt(
                cline_adapter,
                intent=intent,
                external_session_id=external_session_id,
                settlement=settlement,
            )
            for intent, external_session_id in plan.targets
        )
        all_settled = all(result.kind == "settled" for result in results)
        outcomes, accounted_executions = _record_settlement_outcomes(
            pool, task_id=task_id, results=results
        )
        return TaskAbortOutcome(
            kind=TaskAbortOutcomeKind.SETTLED if all_settled else TaskAbortOutcomeKind.UNRESOLVED,
            task=plan.task,
            task_block=None,
            interrupt_outcomes=outcomes,
            accounted_executions=accounted_executions,
        )
