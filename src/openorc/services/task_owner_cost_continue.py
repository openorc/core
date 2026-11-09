"""F8f: the Owner Continue action for the resolved COST_LIMIT episode (issue #291).

``continue_task_after_cost_limit`` is the ONE Owner-authorized Continue
command for a Task blocked by an exactly-evidenced cost-limit stop: it
refreshes the Task's spending allowance and reauthorizes the original Task's
suspended work through the SAME guarded continuation the Owner Resume uses —
one short composed transaction re-proves the account/Owner/currentness/
episode guards via the shared authorization prologue, derives the one
durably identifiable interrupted workflow operation with its result routing,
and atomically resolves the exact current pending ``COST_LIMIT`` gate
APPROVED with truthful OWNER provenance, resolves its sole unresolved
TaskBlock, restores the guarded pre-block Task status under the rotated
token WITH the refreshed cumulative spending threshold in the same
conditional statement, records the truthful OWNER audit including the
immutable grant facts on the resolved gate's event, and commits the one-shot
session-resume record.

The refreshed allowance is measured from the LATEST retained known cumulative
Task spend under the lock plus the Task's FROZEN original
``authorized_task_budget_usd`` — one fresh full Task-budget allowance from
the currently observed spend, including any prior overshoot. Example:
original $10, observed $12, Continue => new threshold $22. Repeat Continues
apply the same rule to their latest observed spend; they never reset history
and never reduce a checkpoint. The original budget is immutable, unaffected
by later Workspace-default edits.

The command handles ONLY an actually interrupted turn: the block context
names interrupted session targets and the shared prologue derives their
routable operation. A zero-target block context refuses — no manufactured
pre-send continuation, no idle restoration, and no reinstallation of
superseded ordinary Owner gates. Nothing is dispatched here: the authorized
continuation is submitted through the executing process's operation-invocation
seam after this composition commits. Unsafe, stale, or unclassifiable state
refuses the whole action and the Task stays ``blocked``; a later Resume
delivery failure is a new operational-failure episode and never reopens the
resolved gate or revokes the committed grant.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Final
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.domain.blocking_cases import Level3BlockingCondition
from openorc.domain.blocks import TaskBlock
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import TaskResumeIntent
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.gates import resolve_owner_gate as resolve_gate_records
from openorc.persistence.intents import create_task_resume_intent
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, InvalidCommandError
from openorc.services.event_coordination import (
    OwnerGateCostAllowanceRefresh,
    owner_actor,
    record_owner_gate_resolved_event,
)
from openorc.services.task_implementation_authorization import require_resolution_applied
from openorc.services.task_owner_resume import _authorize_blocked_task_continuation
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "AuthorizedTaskCostContinue",
    "continue_task_after_cost_limit",
]

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_owner_cost_continue"
_CONTINUE_SPAN_NAME: Final[str] = "task_owner_cost_continue.continue_task_after_cost_limit"


@dataclass(frozen=True, slots=True)
class AuthorizedTaskCostContinue:
    """The one atomic effect of a successful Owner Continue authorization.

    ``task`` is the post-authorization Task in its restored workflow status
    under the rotated token, carrying the refreshed effective cumulative
    spending threshold; ``resolved_owner_gate`` is the exceptional
    ``COST_LIMIT`` gate resolved APPROVED with truthful OWNER provenance;
    ``resolved_task_block`` is the resolved blocking condition whose context
    named the interrupted sessions; ``effective_cost_threshold_usd`` is the
    committed refreshed allowance (latest retained known spend + the frozen
    original budget); ``resume`` is the committed one-shot session-resume
    record whose dispatch continues the exact interrupted session/operation.
    """

    task: Task
    resolved_owner_gate: OwnerGate
    resolved_task_block: TaskBlock
    effective_cost_threshold_usd: Decimal
    resume: TaskResumeIntent


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def continue_task_after_cost_limit(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> AuthorizedTaskCostContinue:
    """Refresh the spending allowance and reauthorize the interrupted work.

    The ONE Owner-authorized Continue command for the current pending
    ``COST_LIMIT`` gate (issue #291, F8f): the shared guarded authorization
    prologue proves the account/Owner/currentness/episode guards and derives
    the interrupted operation's routing; this composition then commits, in
    ONE database-only transaction, the refreshed effective cumulative
    spending threshold (latest retained known cumulative spend + the frozen
    original authorized budget, both re-derived under the Task row lock),
    the gate's APPROVED resolution with truthful OWNER provenance, the sole
    unresolved TaskBlock's resolution, the guarded pre-block Task status
    restore under the rotated token with the refreshed threshold in the SAME
    statement, the truthful OWNER audit whose event carries the immutable
    grant facts against the exact resolved gate, and the one-shot
    session-resume record. Unsafe, stale, or unclassifiable state —
    including an unknown latest spend, a missing authorized budget, or a
    zero-target block context — refuses the whole action: nothing is
    applied and the Task stays ``blocked``.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _CONTINUE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_CONTINUE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            subject = _authorize_blocked_task_continuation(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
                gate_type=OwnerGateType.COST_LIMIT,
                blocking_condition=Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
                action="continue",
            )
            budget = subject.task.authorized_task_budget_usd
            if budget is None:
                # A proven cost-limit block always enforced a present frozen
                # budget; a missing one is an unexpected durable state the
                # action never grants against.
                raise ConflictError(
                    "the blocked task carries no authorized task budget; the "
                    "continue action is refused"
                )
            costs = session_records.sum_task_retained_session_costs(
                transaction_pool, task_id=task_id
            )
            if costs.known_total_usd is None:
                # The latest retained known cumulative spend is unknown: the
                # refreshed threshold cannot be derived truthfully, so the
                # action grants nothing (no partial grant, no transition).
                raise ConflictError(
                    "the task's latest retained known cumulative spend is unknown; "
                    "the continue action is refused"
                )
            refreshed_threshold = costs.known_total_usd + budget
            resolution = resolve_gate_records(
                transaction_pool,
                owner_gate_id=subject.gate.id,
                outcome=OwnerGateStatus.APPROVED,
                expected_task_state_token=subject.task.state_token,
            )
            resolved_gate, gate_task = require_resolution_applied(resolution)
            resolved_block = block_records.resolve_task_block(
                transaction_pool, task_block_id=subject.block.id
            )
            if resolved_block is None:
                raise ConflictError(
                    "the cost_limit block has already resolved; the continue action is stale"
                )
            restored = task_records.restore_blocked_task_status(
                transaction_pool,
                task_id,
                expected_state_token=gate_task.state_token,
                restored_status=subject.routing.restore_status,
                effective_cost_threshold_usd=refreshed_threshold,
            )
            if restored is None:
                raise ConflictError(
                    "the blocked task could not be restored; the continue action is stale"
                )
            record_owner_gate_resolved_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=resolved_gate.id,
                actor=owner_actor(profile_id),
                outcome=OwnerGateStatus.APPROVED,
                cost_allowance_refresh=OwnerGateCostAllowanceRefresh(
                    observed_cost_usd=costs.known_total_usd,
                    authorized_budget_usd=budget,
                    effective_threshold_usd=refreshed_threshold,
                ),
            )
            try:
                resume_record = create_task_resume_intent(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    task_agent_session_id=subject.routing.session.id,
                    task_block_id=subject.block.id,
                    workflow_role=subject.routing.session.role,
                    external_session_id=subject.routing.session.external_session_id or "",
                    requested_by_profile_id=profile_id,
                    routing=subject.routing.routing_context(
                        authorized_state_token=restored.state_token
                    ),
                )
            except UniqueViolation as error:
                # The session's one-shot non-terminal authorization already
                # stands (the database partial unique index): a duplicate
                # Continue grants nothing and dispatches nothing, and the
                # whole composition rolls back to the blocked state.
                raise ConflictError(
                    "this session already carries an outstanding one-shot "
                    "session-resume authorization; the continue action is refused"
                ) from error
        return AuthorizedTaskCostContinue(
            task=restored,
            resolved_owner_gate=resolved_gate,
            resolved_task_block=resolved_block,
            effective_cost_threshold_usd=refreshed_threshold,
            resume=resume_record,
        )
