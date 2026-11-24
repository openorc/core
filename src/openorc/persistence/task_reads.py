"""The composed Task observation read behind the authorized Task read surface
(issue #376).

The composed read the Owner-authorized Task detail service loads: the Task
aggregate's current workflow facts — state token, the exact current OwnerGate,
the exact unresolved TaskBlocks, the current PlanRevision, the session
bindings, the Executions, the review loops and their iterations, the runtime
requests, the intent evidence, the detached-turn receipts, the runtime send
claims, the planning-bootstrap evidence, the canonical TaskPullRequest, the
linked GitHub issue projection, and the retained session-cost aggregate.

The loader is database-only and strictly side-effect free: it never locks
rows for update, never advances any stamp, and never touches a provider. It
composes through the established transaction-composition seam: the service
opens ONE ``composed_transaction`` and passes the transaction-scoped pool
view, so every read here joins that one transaction and the current state
token, gate, block, subject, and the backend-generated action facts are one
coherent durable observation — the same coherence boundary every command
service composes its subject reads under. The durable evidence it returns is
complete: presentation bounds belong to the service layer's rendered
projection, never to the evidence the action guards evaluate. Cross-module
imports reuse the established row mappers and column projections (the
deletion/gates precedent); no mapper is re-implemented here.

Repository-scoped batched lookups for the authorized list composition live
beside their owning repositories:
``persistence.pull_requests.get_task_pull_requests_for_tasks``,
``persistence.github_issues.find_github_issue_projections_by_identity``,
``persistence.gates.list_owner_gates_by_ids``, and
``persistence.sessions.sum_task_retained_session_costs_for_tasks``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from openorc.domain.blocks import TaskBlock
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.executions import Execution
from openorc.domain.gates import OwnerGate
from openorc.domain.github_issues import GitHubIssueProjection
from openorc.domain.intents import (
    TaskInterruptIntent,
    TaskPullRequestCloseIntent,
    TaskResumeIntent,
)
from openorc.domain.planning import PlanRevision
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.reviews import ReviewIteration, ReviewLoop
from openorc.domain.runtime_requests import RuntimeRequest
from openorc.domain.send_claims import TaskRuntimeSendClaim
from openorc.domain.sessions import TaskAgentSession
from openorc.domain.tasks import (
    Task,
    TaskDomainError,
    TaskPlanningBootstrapEvidence,
    TaskPlanningBootstrapStage,
)
from openorc.persistence.blocks import _TASK_BLOCK_COLUMNS, _task_block_from_row
from openorc.persistence.detached_turns import (
    _RECEIPT_COLUMNS,
    _receipt_from_row,
)
from openorc.persistence.executions import _EXECUTION_COLUMNS, _execution_from_row
from openorc.persistence.gates import _OWNER_GATE_COLUMNS, _owner_gate_from_row
from openorc.persistence.github_issues import (
    _GITHUB_ISSUE_COLUMNS,
    _github_issue_from_row,
)
from openorc.persistence.intents import (
    _TASK_INTERRUPT_INTENT_COLUMNS,
    _TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS,
    _TASK_RESUME_INTENT_COLUMNS,
    _close_intent_from_row,
    _interrupt_intent_from_row,
    _resume_intent_from_row,
)
from openorc.persistence.planning import (
    _PLAN_REVISION_COLUMNS,
    _plan_revision_from_row,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import (
    _TASK_PULL_REQUEST_COLUMNS,
    _task_pull_request_from_row,
)
from openorc.persistence.reviews import (
    _REVIEW_ITERATION_COLUMNS,
    _REVIEW_LOOP_COLUMNS,
    _review_iteration_from_row,
    _review_loop_from_row,
)
from openorc.persistence.runtime_requests import (
    _RUNTIME_REQUEST_COLUMNS,
    _runtime_request_from_row,
)
from openorc.persistence.send_claims import _CLAIM_COLUMNS, _claim_from_row
from openorc.persistence.sessions import _SESSION_COLUMNS, _session_from_row
from openorc.persistence.tasks import _TASK_COLUMNS, _task_from_row, normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "TaskObservationRecord",
    "load_task_observation",
]


@dataclass(frozen=True, slots=True)
class TaskObservationRecord:
    """The coherent durable observation of one Task (issue #376).

    Every field is read from the same ``REPEATABLE READ READ ONLY``
    transaction snapshot, so the state token, the exact current gate, the
    exact unresolved block, and every fact the backend-generated action
    projection evaluates are one durable observation. Collections carry the
    complete durable evidence — presentation bounding is the service layer's
    rendered-projection concern, never a restriction on the evaluated
    evidence.
    """

    task: Task
    current_gate: OwnerGate | None
    current_blocks: tuple[TaskBlock, ...]
    current_plan: PlanRevision | None
    sessions: tuple[TaskAgentSession, ...]
    executions: tuple[Execution, ...]
    review_loops: tuple[ReviewLoop, ...]
    review_iterations: tuple[ReviewIteration, ...]
    runtime_requests: tuple[RuntimeRequest, ...]
    interrupt_intents: tuple[TaskInterruptIntent, ...]
    resume_intents: tuple[TaskResumeIntent, ...]
    pull_request_close_intent: TaskPullRequestCloseIntent | None
    receipts: tuple[TaskDetachedTurnReceipt, ...]
    receipts_total: int
    send_claims: tuple[TaskRuntimeSendClaim, ...]
    planning_bootstrap: TaskPlanningBootstrapEvidence | None
    pull_request: TaskPullRequest | None
    issue_projection: GitHubIssueProjection | None
    session_costs_known_total_usd: Decimal | None
    session_costs_has_unobserved: bool

def _optional(row: Any, mapper: Callable[[Any], Any]) -> Any:
    """Map one optional row, or ``None``."""
    return None if row is None else mapper(row)


def load_task_observation(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipts_limit: int = 50,
) -> TaskObservationRecord | None:
    """Load one Task's coherent durable observation (issue #376).

    Composed through the caller's transaction-scoped pool view (the service's
    ONE ``composed_transaction``): the current state token, the exact current
    gate, the exact unresolved blocks, and every fact the backend-generated
    action projection evaluates come from one transaction — the same
    coherence boundary every command service composes its subject reads
    under. The read is strictly database-only and side-effect free: no row
    locks, no stamps, no provider I/O. The loaded evidence is complete;
    presentation bounds are the service layer's rendered-projection concern.
    A foreign-Workspace or absent Task is the uniform ``None`` — never a
    probing oracle.
    """
    if not isinstance(workspace_id, UUID):
        raise TaskDomainError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")
    if (
        isinstance(receipts_limit, bool)
        or not isinstance(receipts_limit, int)
        or receipts_limit <= 0
    ):
        raise TaskDomainError("receipts_limit must be a positive integer")
    with transaction(pool) as conn:
        task_row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where id = %s and workspace_id = %s",
            (task_id, workspace_id),
        ).fetchone()
        if task_row is None:
            return None
        task = _task_from_row(task_row)
        current_gate = (
            None
            if task.current_owner_gate_id is None
            else _optional(
                conn.execute(
                    f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates "
                    "where id = %s",
                    (task.current_owner_gate_id,),
                ).fetchone(),
                _owner_gate_from_row,
            )
        )
        current_blocks = [
            _task_block_from_row(row)
            for row in conn.execute(
                f"select {_TASK_BLOCK_COLUMNS} from openorc.task_blocks "
                "where task_id = %s and resolved_at is null "
                "order by created_at desc, id desc",
                (task_id,),
            ).fetchall()
        ]
        current_plan = (
            None
            if task.current_plan_revision_id is None
            else _optional(
                conn.execute(
                    f"select {_PLAN_REVISION_COLUMNS} from openorc.plan_revisions "
                    "where id = %s",
                    (task.current_plan_revision_id,),
                ).fetchone(),
                _plan_revision_from_row,
            )
        )
        sessions = [
            _session_from_row(row)
            for row in conn.execute(
                f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
                "where task_id = %s order by created_at, id",
                (task_id,),
            ).fetchall()
        ]
        executions = [
            _execution_from_row(row)
            for row in conn.execute(
                f"select {_EXECUTION_COLUMNS} from openorc.executions "
                "where task_id = %s order by execution_number, id",
                (task_id,),
            ).fetchall()
        ]
        review_loops = [
            _review_loop_from_row(row)
            for row in conn.execute(
                f"select {_REVIEW_LOOP_COLUMNS} from openorc.review_loops "
                "where task_id = %s order by created_at, id",
                (task_id,),
            ).fetchall()
        ]
        review_iterations = [
            _review_iteration_from_row(row)
            for row in conn.execute(
                f"select {_REVIEW_ITERATION_COLUMNS} from openorc.review_iterations "
                "where task_id = %s order by review_loop_id, iteration_number, id",
                (task_id,),
            ).fetchall()
        ]
        runtime_requests = [
            _runtime_request_from_row(row)
            for row in conn.execute(
                f"select {_RUNTIME_REQUEST_COLUMNS} from openorc.runtime_requests "
                "where task_id = %s order by created_at, id",
                (task_id,),
            ).fetchall()
        ]
        interrupt_intents = [
            _interrupt_intent_from_row(row)
            for row in conn.execute(
                f"select {_TASK_INTERRUPT_INTENT_COLUMNS} "
                "from openorc.task_interrupt_intents "
                "where task_id = %s order by created_at, id",
                (task_id,),
            ).fetchall()
        ]
        resume_intents = [
            _resume_intent_from_row(row)
            for row in conn.execute(
                f"select {_TASK_RESUME_INTENT_COLUMNS} from openorc.task_resume_intents "
                "where task_id = %s order by created_at, id",
                (task_id,),
            ).fetchall()
        ]
        pull_request_close_intent = _optional(
            conn.execute(
                f"select {_TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS} "
                "from openorc.task_pull_request_close_intents where task_id = %s "
                "order by created_at desc, id desc limit 1",
                (task_id,),
            ).fetchone(),
            _close_intent_from_row,
        )
        receipts_rows = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            "where workspace_id = %s and task_id = %s "
            "order by claimed_at desc, id desc limit %s",
            (workspace_id, task_id, receipts_limit),
        ).fetchall()
        receipts_total_row = conn.execute(
            "select count(*) from openorc.task_hub_run_receipts "
            "where workspace_id = %s and task_id = %s",
            (workspace_id, task_id),
        ).fetchone()
        send_claims = [
            _claim_from_row(row)
            for row in conn.execute(
                f"select {_CLAIM_COLUMNS} from openorc.task_runtime_send_claims "
                "where task_id = %s order by claimed_at, id",
                (task_id,),
            ).fetchall()
        ]
        bootstrap_row = conn.execute(
            "select planning_bootstrap_claim_token, planning_bootstrap_claimed_at, "
            "planning_bootstrap_stage, planning_bootstrap_stage_settled_at "
            "from openorc.tasks where id = %s",
            (task_id,),
        ).fetchone()
        planning_bootstrap = (
            None
            if bootstrap_row is None or bootstrap_row[0] is None
            else TaskPlanningBootstrapEvidence(
                claim_token=bootstrap_row[0],
                claimed_at=(
                    None if bootstrap_row[1] is None else normalize_utc(bootstrap_row[1])
                ),
                stage=(
                    None
                    if bootstrap_row[2] is None
                    else TaskPlanningBootstrapStage(bootstrap_row[2])
                ),
                stage_settled_at=(
                    None
                    if bootstrap_row[3] is None
                    else normalize_utc(bootstrap_row[3])
                ),
            )
        )
        pull_request = _optional(
            conn.execute(
                f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
                "where task_id = %s",
                (task_id,),
            ).fetchone(),
            _task_pull_request_from_row,
        )
        issue_projection = _optional(
            conn.execute(
                f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
                "where repository_id = %s and github_issue_id = %s",
                (task.repository_id, task.github_issue_id),
            ).fetchone(),
            _github_issue_from_row,
        )
        cost_row = conn.execute(
            "select sum(retained_cost_usd), "
            "coalesce(bool_or(retained_cost_usd is null), false) "
            "from openorc.task_agent_sessions where task_id = %s",
            (task_id,),
        ).fetchone()
    assert cost_row is not None and receipts_total_row is not None
    return TaskObservationRecord(
        task=task,
        current_gate=current_gate,
        current_blocks=tuple(current_blocks),
        current_plan=current_plan,
        sessions=tuple(sessions),
        executions=tuple(executions),
        review_loops=tuple(review_loops),
        review_iterations=tuple(review_iterations),
        runtime_requests=tuple(runtime_requests),
        interrupt_intents=tuple(interrupt_intents),
        resume_intents=tuple(resume_intents),
        pull_request_close_intent=pull_request_close_intent,
        receipts=tuple(_receipt_from_row(row) for row in receipts_rows),
        receipts_total=int(receipts_total_row[0]),
        send_claims=tuple(send_claims),
        planning_bootstrap=planning_bootstrap,
        pull_request=pull_request,
        issue_projection=issue_projection,
        session_costs_known_total_usd=cost_row[0],
        session_costs_has_unobserved=bool(cost_row[1]),
    )
