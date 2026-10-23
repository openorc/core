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
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
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
    "TaskAbortOutcome",
    "TaskAbortOutcomeKind",
    "TaskInterruptOutcome",
    "TaskInterruptSettlement",
    "abort_task_activity",
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


def _require_admitted_in_flight_activity(
    transaction_pool: DatabasePool, *, task: Task, target: _InterruptTarget
) -> None:
    """Bind the admitted role/session to its durable in-flight workflow activity.

    The exact-active-target eligibility contract (issue #228): the addressed
    session must own the admitted workflow interaction as proven by existing
    durable state — never by the primary Task status or an SDK status
    reading. A Producer session is bound to its durably open Execution
    (``producer_session_id`` is the Execution's own session anchor); a
    Reviewer session is bound to the Task's OPEN ReviewLoop carrying an
    unfinalized ReviewIteration — the dispatched review turn still in
    flight. A pending OwnerGate therefore admits Abort only when such an
    in-flight fact exists (a paused-for-approval Execution, for example);
    an ordinary exhausted-loop gate with none is idle and refuses.
    """
    if target.workflow_role is WorkflowRole.PRODUCER:
        executions = execution_records.list_task_executions(transaction_pool, task_id=task.id)
        if any(
            execution.producer_session_id == target.task_agent_session_id
            and execution.status in _OPEN_EXECUTION_STATUSES
            for execution in executions
        ):
            return
        raise ConflictError(
            "the addressed producer session owns no durably admitted in-flight "
            "implementation activity to interrupt"
        )
    loops = review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.OPEN
    )
    for loop in loops:
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        if any(iteration.outcome is None for iteration in iterations):
            return
    raise ConflictError(
        "the addressed reviewer session owns no durably admitted in-flight "
        "review activity to interrupt"
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
        for target in targets:
            _require_admitted_in_flight_activity(transaction_pool, task=task, target=target)
        block = block_records.create_task_block(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
            context=_owner_abort_block_context(targets),
        )
        blocked = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            status=TaskStatus.BLOCKED,
        )
        if blocked is None:
            raise StaleOperationError(
                "the task could not move to blocked; the owner abort is stale"
            )
        event_coordination.record_task_blocked_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_block_id=block.id,
            actor=event_coordination.owner_actor(profile_id),
        )
        intents = tuple(
            (
                intent_records.create_task_interrupt_intent(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    task_agent_session_id=target.task_agent_session_id,
                    workflow_role=target.workflow_role,
                    external_session_id=target.external_session_id,
                    requested_by_profile_id=profile_id,
                ),
                target,
            )
            for target in targets
        )
        return _DispatchPlan(task=blocked, task_block=block, targets=intents)


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


def _settle_interrupt(
    pool: DatabasePool,
    cline_adapter: ClineAdapter,
    *,
    intent: TaskInterruptIntent,
    external_session_id: str,
    settlement: TaskInterruptSettlement,
) -> TaskInterruptOutcome:
    """Settle one outstanding interruption under positive caller-attested evidence.

    The adapter's D6b settlement discipline is invoked exactly as documented:
    both preconditions come from the caller's attestation, the refusal paths
    keep every guard latched, and only a positively settled outcome advances
    the durable intent to ``settled`` — the state in which Cancel becomes
    available again. A raced terminal outcome is left untouched.
    """
    try:
        result = cline_adapter.settle_abort(
            external_session_id,
            runtime_work_settled=settlement.runtime_work_settled,
            operation_accounted_for=settlement.operation_accounted_for,
        )
    except AgentRuntimeError:
        recorded = intent_records.record_task_interrupt_intent_outcome(
            pool,
            task_interrupt_intent_id=intent.id,
            status=TaskInterruptIntentStatus.UNKNOWN,
            outcome={"settlement": "unavailable"},
        )
        return _interrupt_outcome(
            intent,
            status=recorded.status if recorded is not None else intent.status,
            detail="unavailable",
        )
    if result.settled:
        recorded = intent_records.record_task_interrupt_intent_outcome(
            pool,
            task_interrupt_intent_id=intent.id,
            status=TaskInterruptIntentStatus.SETTLED,
            outcome={"settlement": "confirmed"},
        )
        return _interrupt_outcome(
            intent,
            status=recorded.status if recorded is not None else intent.status,
            detail="settled",
        )
    refusal = (
        result.refusal.value if isinstance(result.refusal, ClineSettlementRefusal) else "refused"
    )
    intent_records.annotate_unsettled_task_interrupt_intent(
        pool,
        task_interrupt_intent_id=intent.id,
        outcome={"settlement_refusal": refusal},
    )
    return _interrupt_outcome(
        intent,
        status=TaskInterruptIntentStatus.UNSETTLED,
        detail=f"refused:{refusal}",
    )


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
        outcomes = tuple(
            _settle_interrupt(
                pool,
                cline_adapter,
                intent=intent,
                external_session_id=external_session_id,
                settlement=settlement,
            )
            for intent, external_session_id in plan.targets
        )
        all_settled = all(
            outcome.status is TaskInterruptIntentStatus.SETTLED for outcome in outcomes
        )
        accounted_executions: tuple[Execution, ...] = ()
        if all_settled:
            # Positive cessation established for every outstanding
            # interruption: the exact-attempt accounting of the interrupted
            # runtime work applies now, so the later Cancel eligibility veto
            # no longer sees outstanding runtime work.
            accounted_executions = _account_for_interrupted_executions(pool, task_id=task_id)
        return TaskAbortOutcome(
            kind=TaskAbortOutcomeKind.SETTLED if all_settled else TaskAbortOutcomeKind.UNRESOLVED,
            task=plan.task,
            task_block=None,
            interrupt_outcomes=outcomes,
            accounted_executions=accounted_executions,
        )
