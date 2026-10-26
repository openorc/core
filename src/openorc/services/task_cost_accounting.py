"""Task cost accounting and spending-policy evaluation service (issue #240).

The focused read/checkpoint/check application service behind OpenOrc's
best-effort, Cline-reported Task cost accounting. It owns three things and
nothing else:

- **Checkpointing** one exact bound Task/role/Connection session's
  aggregate reported USD cost as the session's high-water durable fact
  (never the root/lead-agent scope, never a substituted or invented
  value).
- **Deriving** the Task's observed expenditure from the role-agnostic
  retained-cost aggregate over every TaskAgentSession matching the Task —
  never duplicated onto the Task row.
- **Enforcing** the Owner-configured budget by entering the existing
  ``BLOCKED`` state through the shared E10b Owner-attributed blocking
  composition when the retained known total reaches the Task's non-NULL
  authorized budget under the Task row lock.

Boundary discipline:

- The caller supplies durable Task/session identifiers and the Cline
  typed aggregate value (an exact ``Decimal`` extracted from
  ``ClineActivitySnapshot.aggregate_usage`` — never a raw SDK object).
  Snapshot reads are SDK I/O and happen OUTSIDE any database transaction;
  the checkpoint and the budget evaluation compose in one fresh short
  ``composed_transaction`` under the Task row lock, acquired first so
  Task-then-session lock ordering stays consistent with every other Task
  mutation.
- Absent, unusable, stale, or foreign observations are unknown truth:
  they checkpoint nothing, block nothing, and never authorize anything.
  Unavailable readings are deliberately nonblocking (no speculative
  fail-closed cost gate exists).
- A reached threshold is authoritative-but-soft: the blocking decision is
  re-verified under the lock by the shared composition, and an
  already-blocked, archived, terminal, or unauthorized Task never receives
  a duplicate block, duplicate intent, or new interrupt.
- This service neither resumes nor retries the blocked Task:
  ``COST_LIMIT`` resolution belongs entirely to F4b (issue #230). The
  pre-dispatch gate (:func:`check_task_budget_before_dispatch`) is the
  seam #88 F3a wires into Cline-backed workflow invocations in both
  ``regular`` and ``queue`` modes before a new model-backed autonomous
  dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from openorc.domain.blocks import TaskBlock
from openorc.domain.intents import TaskInterruptIntent
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import task_abort
from openorc.services.errors import InvalidCommandError
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "BudgetGateDecision",
    "BudgetGateDecisionKind",
    "SessionCostObservation",
    "SessionCostObservationKind",
    "check_task_budget_before_dispatch",
    "observe_session_usage",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_cost_accounting"
_OBSERVE_SPAN_NAME = "task_cost_accounting.observe_session_usage"
_GATE_SPAN_NAME = "task_cost_accounting.check_task_budget_before_dispatch"

# The workflow phases in which a reached budget threshold enforces the
# blocking decision. Checkpointing truthful cost is valid for every Task
# state; enforcement is not — a queued or terminal Task has no active work
# whose standing authorization could interrupt, and the shared blocking
# composition re-verifies the same eligibility under its own lock.
_ENFORCEMENT_ELIGIBLE_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.PLANNING,
        TaskStatus.IMPLEMENTING,
        TaskStatus.REVIEWING,
        TaskStatus.WAITING_FOR_OWNER,
    }
)


class SessionCostObservationKind(StrEnum):
    """The classified outcome of one session usage observation.

    ``checkpointed`` retained the reading and the budget is not exhausted;
    ``blocked`` retained the reading and composed the atomic cost-limit
    block under the lock (post-commit interrupt dispatches are attached);
    ``not_observed`` carried no usable aggregate reading (unknown — nothing
    written, nothing blocked); ``ignored_foreign`` was stale, foreign, or
    not-higher and matched no checkpointable row; ``budget_disabled`` and
    ``task_not_current`` retained the truthful reading but no standing
    policy applies; ``already_blocked`` found the Task already blocked and
    never duplicated any blocking fact.
    """

    CHECKPOINTED = "checkpointed"
    BLOCKED = "blocked"
    NOT_OBSERVED = "not_observed"
    IGNORED_FOREIGN = "ignored_foreign"
    BUDGET_DISABLED = "budget_disabled"
    TASK_NOT_CURRENT = "task_not_current"
    ALREADY_BLOCKED = "already_blocked"


@dataclass(frozen=True, slots=True)
class SessionCostObservation:
    """The typed result of one session usage observation.

    ``task`` is the locked current Task when one exists. ``task_block`` and
    ``interrupt_dispatches`` are present only for ``blocked``; the dispatch
    obligations are ``(intent, external_session_id)`` pairs to be dispatched
    strictly after the observation transaction committed and with no
    database transaction open.
    """

    kind: SessionCostObservationKind
    task: Task | None = None
    task_block: TaskBlock | None = None
    interrupt_dispatches: tuple[tuple[TaskInterruptIntent, str], ...] = ()


class BudgetGateDecisionKind(StrEnum):
    """The classified pre-dispatch budget gate decision.

    ``allow`` permits the existing authorized work to continue; ``blocked``
    composed the atomic cost-limit block under the lock (the caller refuses
    the dispatch and handles the attached interrupt dispatches post-commit);
    ``already_blocked`` found the Task already blocked; ``budget_disabled``
    found no standing spending authorization; ``task_not_current`` found the
    subject stale, foreign, archived, or outside the enforcement phases.
    """

    ALLOW = "allow"
    BLOCKED = "blocked"
    ALREADY_BLOCKED = "already_blocked"
    BUDGET_DISABLED = "budget_disabled"
    NOT_CURRENT = "task_not_current"


@dataclass(frozen=True, slots=True)
class BudgetGateDecision:
    """The typed pre-dispatch budget gate result.

    ``observed_known_total_usd`` and ``has_unobserved_sessions`` carry the
    aggregate facts behind an ``allow`` decision so callers can distinguish
    a complete aggregate from one with unknown constituents; they are
    observational diagnostics, never workflow authority.
    """

    kind: BudgetGateDecisionKind
    task: Task | None = None
    task_block: TaskBlock | None = None
    interrupt_dispatches: tuple[tuple[TaskInterruptIntent, str], ...] = ()
    observed_known_total_usd: Decimal | None = None
    has_unobserved_sessions: bool = False


def _require_uuid_command(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_aggregate_total_cost(value: object) -> None:
    """Validate the typed aggregate reading before any state is touched."""
    if value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, Decimal)
        or not value.is_finite()
        or value < 0
    ):
        raise InvalidCommandError(
            "aggregate_total_cost must be None or a nonnegative finite USD amount "
            "(an exact decimal extracted from the typed activity snapshot)"
        )


def _blocked_observation(
    result: task_abort.OwnerAuthorizedCostLimitBlock, fallback_task: Task | None
) -> SessionCostObservation:
    """Map the shared blocking composition result into an observation outcome."""
    if result.kind in (
        task_abort.CostLimitBlockOutcomeKind.BLOCKED_WITH_INTERRUPTS,
        task_abort.CostLimitBlockOutcomeKind.BLOCKED_IDLE,
    ):
        return SessionCostObservation(
            kind=SessionCostObservationKind.BLOCKED,
            task=result.task,
            task_block=result.task_block,
            interrupt_dispatches=result.interrupt_dispatches,
        )
    if result.kind is task_abort.CostLimitBlockOutcomeKind.ALREADY_BLOCKED:
        return SessionCostObservation(
            kind=SessionCostObservationKind.ALREADY_BLOCKED,
            task=result.task if result.task is not None else fallback_task,
        )
    # Under the same transaction and lock the re-verified threshold cannot
    # have retreated; a NOT_APPLICABLE here is a defensive classification:
    # the checkpoint stands, no block is manufactured.
    return SessionCostObservation(kind=SessionCostObservationKind.CHECKPOINTED, task=fallback_task)


def observe_session_usage(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    connection_id: UUID,
    aggregate_total_cost: Decimal | None,
) -> SessionCostObservation:
    """Checkpoint one session's aggregate reported cost and evaluate its Task budget.

    One fresh short composed transaction, under the Task row lock acquired
    first: the exact-scope high-water checkpoint (only when the binding
    tuple and a strictly greater amount both match), then the budget
    evaluation against the role-agnostic retained-cost aggregate. A reached
    threshold composes the shared Owner-authorized cost-limit block in the
    SAME transaction — block, ``blocked`` transition, OWNER audit, and
    exact one-shot Interrupt intents commit or roll back together — with
    the interrupt dispatches returned for post-commit handling. A ``None``
    reading is unknown truth: nothing is written and nothing is blocked.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(task_agent_session_id, "task_agent_session_id")
    _require_uuid_command(connection_id, "connection_id")
    _require_aggregate_total_cost(aggregate_total_cost)
    with application_span(_SERVICE_TRACER_SCOPE, _OBSERVE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_OBSERVE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            # The Task lock is the first acquisition, keeping the
            # Task-then-session ordering consistent with every other Task
            # mutation and with the blocking composition below.
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                return SessionCostObservation(kind=SessionCostObservationKind.IGNORED_FOREIGN)
            if aggregate_total_cost is None:
                # An absent or unusable aggregate reading is unknown —
                # never zero, never blocking, and nothing is written.
                return SessionCostObservation(
                    kind=SessionCostObservationKind.NOT_OBSERVED, task=locked
                )
            checkpointed = session_records.checkpoint_task_agent_session_cost(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=task_agent_session_id,
                connection_id=connection_id,
                retained_cost_usd=aggregate_total_cost,
            )
            if checkpointed is None:
                # A stale, foreign, or not-higher reading matched no row:
                # truthful no-op, never another Task's accounting.
                return SessionCostObservation(
                    kind=SessionCostObservationKind.IGNORED_FOREIGN, task=locked
                )
            if locked.status is TaskStatus.BLOCKED:
                return SessionCostObservation(
                    kind=SessionCostObservationKind.ALREADY_BLOCKED, task=locked
                )
            if locked.authorized_task_budget_usd is None:
                return SessionCostObservation(
                    kind=SessionCostObservationKind.BUDGET_DISABLED, task=locked
                )
            if locked.status not in _ENFORCEMENT_ELIGIBLE_STATUSES:
                return SessionCostObservation(
                    kind=SessionCostObservationKind.TASK_NOT_CURRENT, task=locked
                )
            costs = session_records.sum_task_retained_session_costs(
                transaction_pool, task_id=task_id
            )
            if (
                costs.known_total_usd is None
                or costs.known_total_usd < locked.authorized_task_budget_usd
            ):
                return SessionCostObservation(
                    kind=SessionCostObservationKind.CHECKPOINTED, task=locked
                )
            return _blocked_observation(
                task_abort.apply_owner_authorized_cost_limit_block(
                    transaction_pool, workspace_id=workspace_id, task_id=task_id
                ),
                fallback_task=locked,
            )


def check_task_budget_before_dispatch(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> BudgetGateDecision:
    """The cost-aware pre-dispatch gate before new model-backed autonomous work.

    The seam #88 F3a wires into Cline-backed workflow invocations in both
    ``regular`` and ``queue`` modes: one short composed transaction under
    the Task row lock evaluates the retained known session cost sum against
    the Task's authorized budget and — exactly when the threshold is
    reached — composes the shared atomic cost-limit block in the same
    transaction. ``allow`` permits the existing authorized work; ``blocked``
    refuses the dispatch and carries the post-commit interrupt dispatches.
    Unknown constituents never change the verdict: the threshold is a known
    lower bound against the budget, and missing readings are deliberately
    nonblocking (no speculative fail-closed cost gate exists).
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with application_span(_SERVICE_TRACER_SCOPE, _GATE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_GATE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                return BudgetGateDecision(kind=BudgetGateDecisionKind.NOT_CURRENT)
            if locked.status is TaskStatus.BLOCKED:
                return BudgetGateDecision(kind=BudgetGateDecisionKind.ALREADY_BLOCKED, task=locked)
            if locked.status not in _ENFORCEMENT_ELIGIBLE_STATUSES:
                return BudgetGateDecision(kind=BudgetGateDecisionKind.NOT_CURRENT, task=locked)
            if locked.authorized_task_budget_usd is None:
                return BudgetGateDecision(kind=BudgetGateDecisionKind.BUDGET_DISABLED, task=locked)
            costs = session_records.sum_task_retained_session_costs(
                transaction_pool, task_id=task_id
            )
            if (
                costs.known_total_usd is None
                or costs.known_total_usd < locked.authorized_task_budget_usd
            ):
                return BudgetGateDecision(
                    kind=BudgetGateDecisionKind.ALLOW,
                    task=locked,
                    observed_known_total_usd=costs.known_total_usd,
                    has_unobserved_sessions=costs.has_unobserved_sessions,
                )
            result = task_abort.apply_owner_authorized_cost_limit_block(
                transaction_pool, workspace_id=workspace_id, task_id=task_id
            )
            if result.kind in (
                task_abort.CostLimitBlockOutcomeKind.BLOCKED_WITH_INTERRUPTS,
                task_abort.CostLimitBlockOutcomeKind.BLOCKED_IDLE,
            ):
                return BudgetGateDecision(
                    kind=BudgetGateDecisionKind.BLOCKED,
                    task=result.task,
                    task_block=result.task_block,
                    interrupt_dispatches=result.interrupt_dispatches,
                )
            if result.kind is task_abort.CostLimitBlockOutcomeKind.ALREADY_BLOCKED:
                return BudgetGateDecision(
                    kind=BudgetGateDecisionKind.ALREADY_BLOCKED,
                    task=result.task if result.task is not None else locked,
                )
            # Defensive: under the same lock the threshold cannot have
            # retreated after the evaluation above.
            return BudgetGateDecision(
                kind=BudgetGateDecisionKind.ALLOW,
                task=locked,
                observed_known_total_usd=costs.known_total_usd,
                has_unobserved_sessions=costs.has_unobserved_sessions,
            )
