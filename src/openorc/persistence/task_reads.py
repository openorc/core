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
opens ONE ``composed_read_transaction`` and passes the transaction-scoped pool
view, so every read here joins that one repeatable-read snapshot and the
current state token, gate, block, subject, and the backend-generated action
facts are one coherent durable observation. Potentially large collections
select their relevant population (the unsettled/open work and the outstanding
operational evidence the detail presents) explicitly bounded with a matching
durable total of exactly that population — never a silent truncation of an
unrelated window. Presentation bounding never restricts the durable evidence
the action guards evaluate: the guards evaluate their authoritative
predicates through their own complete repository reads. Cross-module imports
reuse the established row mappers and column projections (the
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
from openorc.domain.detached_turns import (
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.executions import Execution, ExecutionStatus
from openorc.domain.gates import OwnerGate
from openorc.domain.github_issues import GitHubIssueProjection
from openorc.domain.intents import (
    TaskInterruptIntent,
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntent,
    TaskResumeIntent,
    TaskResumeIntentStatus,
)
from openorc.domain.planning import PlanRevision
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.reviews import ReviewIteration, ReviewLoop
from openorc.domain.runtime_requests import RuntimeRequest, RuntimeRequestStatus
from openorc.domain.send_claims import TaskRuntimeSendClaim, TaskRuntimeSendClaimStatus
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
    transaction snapshot (the service's ``composed_read_transaction``), so
    the state token, the exact current gate, the exact unresolved block, and
    every fact the backend-generated action projection evaluates are one
    durable observation. Potentially large collections select their
    relevant population explicitly — the unsettled/open work and the
    outstanding operational evidence the detail presents — newest-first
    bounded at the caller's presentation bound with a matching durable
    total of exactly that population carried beside the collection, so a
    client distinguishes a complete population from a transparently bounded
    one and the projected collection never disagrees with its companion
    total. The action guards evaluate their authoritative predicates through
    their own complete repository reads, never this bounded projection.
    """

    task: Task
    current_gate: OwnerGate | None
    current_blocks: tuple[TaskBlock, ...]
    current_plan: PlanRevision | None
    sessions: tuple[TaskAgentSession, ...]
    executions: tuple[Execution, ...]
    executions_total: int
    review_loops: tuple[ReviewLoop, ...]
    review_iterations: tuple[ReviewIteration, ...]
    runtime_requests: tuple[RuntimeRequest, ...]
    interrupt_intents: tuple[TaskInterruptIntent, ...]
    resume_intents: tuple[TaskResumeIntent, ...]
    pull_request_close_intent: TaskPullRequestCloseIntent | None
    receipts: tuple[TaskDetachedTurnReceipt, ...]
    outstanding_receipts_total: int
    send_claims: tuple[TaskRuntimeSendClaim, ...]
    planning_bootstrap: TaskPlanningBootstrapEvidence | None
    pull_request: TaskPullRequest | None
    issue_projection: GitHubIssueProjection | None
    session_costs_known_total_usd: Decimal | None
    session_costs_has_unobserved: bool


def _optional(row: Any, mapper: Callable[[Any], Any]) -> Any:
    """Map one optional row, or ``None``."""
    return None if row is None else mapper(row)


# The relevant-unsettled execution population of the Task detail (issue
# #376): the open attempts whose runtime work is outstanding — the same
# durable open-work vocabulary the action guards evaluate — plus, when the
# Task's latest attempt is a durably finalized successful completion, that
# exact attempt: the validated canonical implementation result the current
# workflow carries (the completed execution the PR_AUTHORIZATION decision
# presents, resolved from durable history the same way the command boundary
# resolves it). An earlier success detached from a later attempt, a failed
# or terminal attempt, and every older attempt are history, available on
# the separate paginated history endpoint.
_RELEVANT_OPEN_EXECUTION_STATUSES: frozenset[ExecutionStatus] = frozenset(
    {
        ExecutionStatus.QUEUED,
        ExecutionStatus.RUNNING,
        ExecutionStatus.PAUSED,
        ExecutionStatus.PAUSED_FOR_APPROVAL,
    }
)
_RELEVANT_OPEN_EXECUTION_STATUS_VALUES: list[str] = sorted(
    status.value for status in _RELEVANT_OPEN_EXECUTION_STATUSES
)

# The outstanding detached-turn receipt population: the pre-settlement
# evidence a claimed or admitted turn carries. Settled and refused receipts
# are terminal history on the paginated history endpoint; they never
# displace the unresolved operational evidence the detail presents.
_OUTSTANDING_RECEIPT_STATUS_VALUES: list[str] = sorted(
    status.value
    for status in (
        TaskDetachedTurnReceiptStatus.CLAIMED,
        TaskDetachedTurnReceiptStatus.ADMITTED,
    )
)

# The outstanding intent evidence populations (#376): an interrupt intent
# not positively settled (the #342 accounted-runtime vocabulary) and a
# resume authorization that is committed-but-never-dispatched or in-flight.
_OUTSTANDING_INTERRUPT_STATUS_VALUES: list[str] = sorted(
    status.value
    for status in TaskInterruptIntentStatus
    if status is not TaskInterruptIntentStatus.SETTLED
)
_OUTSTANDING_RESUME_STATUS_VALUES: list[str] = sorted(
    status.value
    for status in (
        TaskResumeIntentStatus.AUTHORIZED,
        TaskResumeIntentStatus.DISPATCHED,
    )
)

# The outstanding dispatch-claim and pending-runtime-request populations:
# the conservatively claimed sends and the pending requests are the
# outstanding operational evidence; their terminal outcomes are history.
_OUTSTANDING_SEND_CLAIM_STATUS_VALUES: list[str] = sorted(
    status.value for status in (TaskRuntimeSendClaimStatus.CLAIMED,)
)
_PENDING_RUNTIME_REQUEST_STATUS_VALUES: list[str] = sorted(
    status.value for status in (RuntimeRequestStatus.PENDING,)
)

# The shared SQL predicate of the relevant-unsettled execution population:
# every attempt whose runtime work is still open, plus the Task's latest
# attempt when that attempt is a durably finalized successful completion
# carrying its validated implementation result. One predicate for the
# bounded selection and its matching durable total, so the projected
# collection and its companion count can never disagree.
_RELEVANT_EXECUTION_WHERE = (
    "attempt.task_id = %s "
    "and (attempt.status = any(%s) or ("
    "attempt.status = 'succeeded' and attempt.implementation_result is not null "
    "and not exists (select 1 from openorc.executions later_attempt "
    "where later_attempt.task_id = attempt.task_id "
    "and later_attempt.execution_number > attempt.execution_number)))"
)


def load_task_observation(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    presentation_bound: int = 50,
) -> TaskObservationRecord | None:
    """Load one Task's coherent durable observation (issue #376).

    Composed through the caller's transaction-scoped pool view (the service's
    ONE ``composed_read_transaction``): the current state token, the exact
    current gate, the exact unresolved blocks, and every fact the
    backend-generated action projection evaluates come from one
    repeatable-read snapshot — the coherence boundary every command service
    composes its subject reads under. The read is strictly database-only and
    side-effect free: no row locks, no stamps, no provider I/O. Potentially
    large collections select their relevant population (the unsettled work
    and the outstanding evidence the detail presents) explicitly bounded at
    ``presentation_bound`` with the matching durable total of that exact
    population; the complete history remains reachable through the separate
    paginated history endpoint, and the action guards evaluate their own
    complete repository reads. A foreign-Workspace or absent Task is the
    uniform ``None`` — never a probing oracle.
    """
    if not isinstance(workspace_id, UUID):
        raise TaskDomainError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")
    if (
        isinstance(presentation_bound, bool)
        or not isinstance(presentation_bound, int)
        or presentation_bound <= 0
    ):
        raise TaskDomainError("presentation_bound must be a positive integer")
    with transaction(pool) as conn:
        task_row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s and workspace_id = %s",
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
                    f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates where id = %s",
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
                    f"select {_PLAN_REVISION_COLUMNS} from openorc.plan_revisions where id = %s",
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
            for row in reversed(
                conn.execute(
                    f"select {_EXECUTION_COLUMNS} from openorc.executions attempt "
                    f"where {_RELEVANT_EXECUTION_WHERE} "
                    "order by attempt.execution_number desc, attempt.id desc limit %s",
                    (task_id, _RELEVANT_OPEN_EXECUTION_STATUS_VALUES, presentation_bound),
                ).fetchall()
            )
        ]
        executions_total_row = conn.execute(
            f"select count(*) from openorc.executions attempt where {_RELEVANT_EXECUTION_WHERE}",
            (task_id, _RELEVANT_OPEN_EXECUTION_STATUS_VALUES),
        ).fetchone()
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
            for row in reversed(
                conn.execute(
                    f"select {_RUNTIME_REQUEST_COLUMNS} from openorc.runtime_requests "
                    "where task_id = %s and status = any(%s) "
                    "order by created_at desc, id desc limit %s",
                    (task_id, _PENDING_RUNTIME_REQUEST_STATUS_VALUES, presentation_bound),
                ).fetchall()
            )
        ]
        interrupt_intents = [
            _interrupt_intent_from_row(row)
            for row in reversed(
                conn.execute(
                    f"select {_TASK_INTERRUPT_INTENT_COLUMNS} "
                    "from openorc.task_interrupt_intents "
                    "where task_id = %s and status = any(%s) "
                    "order by created_at desc, id desc limit %s",
                    (task_id, _OUTSTANDING_INTERRUPT_STATUS_VALUES, presentation_bound),
                ).fetchall()
            )
        ]
        resume_intents = [
            _resume_intent_from_row(row)
            for row in reversed(
                conn.execute(
                    f"select {_TASK_RESUME_INTENT_COLUMNS} from openorc.task_resume_intents "
                    "where task_id = %s and status = any(%s) "
                    "order by created_at desc, id desc limit %s",
                    (task_id, _OUTSTANDING_RESUME_STATUS_VALUES, presentation_bound),
                ).fetchall()
            )
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
            "where workspace_id = %s and task_id = %s and status = any(%s) "
            "order by claimed_at desc, id desc limit %s",
            (workspace_id, task_id, _OUTSTANDING_RECEIPT_STATUS_VALUES, presentation_bound),
        ).fetchall()
        outstanding_receipts_total_row = conn.execute(
            "select count(*) from openorc.task_hub_run_receipts "
            "where workspace_id = %s and task_id = %s and status = any(%s)",
            (workspace_id, task_id, _OUTSTANDING_RECEIPT_STATUS_VALUES),
        ).fetchone()
        send_claims = [
            _claim_from_row(row)
            for row in reversed(
                conn.execute(
                    f"select {_CLAIM_COLUMNS} from openorc.task_runtime_send_claims "
                    "where task_id = %s and status = any(%s) "
                    "order by claimed_at desc, id desc limit %s",
                    (task_id, _OUTSTANDING_SEND_CLAIM_STATUS_VALUES, presentation_bound),
                ).fetchall()
            )
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
                claimed_at=(None if bootstrap_row[1] is None else normalize_utc(bootstrap_row[1])),
                stage=(
                    None
                    if bootstrap_row[2] is None
                    else TaskPlanningBootstrapStage(bootstrap_row[2])
                ),
                stage_settled_at=(
                    None if bootstrap_row[3] is None else normalize_utc(bootstrap_row[3])
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
    assert cost_row is not None and executions_total_row is not None
    assert outstanding_receipts_total_row is not None
    return TaskObservationRecord(
        task=task,
        current_gate=current_gate,
        current_blocks=tuple(current_blocks),
        current_plan=current_plan,
        sessions=tuple(sessions),
        executions=tuple(executions),
        executions_total=int(executions_total_row[0]),
        review_loops=tuple(review_loops),
        review_iterations=tuple(review_iterations),
        runtime_requests=tuple(runtime_requests),
        interrupt_intents=tuple(interrupt_intents),
        resume_intents=tuple(resume_intents),
        pull_request_close_intent=pull_request_close_intent,
        receipts=tuple(_receipt_from_row(row) for row in receipts_rows),
        outstanding_receipts_total=int(outstanding_receipts_total_row[0]),
        send_claims=tuple(send_claims),
        planning_bootstrap=planning_bootstrap,
        pull_request=pull_request,
        issue_projection=issue_projection,
        session_costs_known_total_usd=cost_row[0],
        session_costs_has_unobserved=bool(cost_row[1]),
    )
