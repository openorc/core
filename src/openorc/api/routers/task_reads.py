"""Thin authorized Task read transport (issue #376).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction, path
and query parsing, delegation to the Owner-authorized read services, and the
composed read models' wire mapping. It owns no authorization policy, no
ownership-split rule, and no action-eligibility semantics — those belong to
the service layer. The reads are database-only and side-effect free: no GET
ever calls GitHub or a runtime provider, mutates workflow, or initiates a
refresh. Blocking database work runs off the event loop (sync route
handlers; the v1 persistence surface is synchronous). The addressed errors
are the typed application vocabulary, mapped to HTTP by the
application-level handler.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_INVALID_COMMAND_422,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.tasks import (
    GitHubTaskFactsRead,
    GitHubTaskIssueSummaryRead,
    GitHubTaskPullRequestHeadRefRead,
    GitHubTaskPullRequestRead,
    GitHubTaskUserRefRead,
    OpenOrcTaskBlockDetailRead,
    OpenOrcTaskBlockSummaryRead,
    OpenOrcTaskCostRead,
    OpenOrcTaskDetailBlockRead,
    OpenOrcTaskDetailRead,
    OpenOrcTaskExecutionRead,
    OpenOrcTaskExecutionResultRead,
    OpenOrcTaskGateDetailRead,
    OpenOrcTaskGateSummaryRead,
    OpenOrcTaskOutstandingIntentRead,
    OpenOrcTaskOutstandingReceiptRead,
    OpenOrcTaskPendingInteractionRead,
    OpenOrcTaskPlanRead,
    OpenOrcTaskReviewIterationRead,
    OpenOrcTaskReviewLoopRead,
    OpenOrcTaskRuntimeRequestRead,
    OpenOrcTaskSessionRead,
    OpenOrcTaskSummaryRead,
    OpenOrcTaskTerminalizationDetailRead,
    OpenOrcTaskTerminalizationRead,
    TaskHistoryCollectionRead,
    TaskHistoryExecutionRead,
    TaskHistoryInterruptIntentRead,
    TaskHistoryOwnerGateRead,
    TaskHistoryPlanRevisionRead,
    TaskHistoryPrCloseIntentRead,
    TaskHistoryRead,
    TaskHistoryReceiptRead,
    TaskHistoryResumeIntentRead,
    TaskHistoryReviewIterationRead,
    TaskHistoryReviewLoopRead,
    TaskHistoryRuntimeRequestRead,
    TaskHistorySendClaimRead,
    TaskHistoryTaskBlockRead,
    TaskListItemRead,
    TaskListRead,
)
from openorc.config import Settings
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
from openorc.domain.ownership import Profile
from openorc.domain.planning import PlanRevision
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.reviews import ReviewIteration, ReviewLoop
from openorc.domain.runtime_requests import RuntimeRequest
from openorc.domain.send_claims import TaskRuntimeSendClaim
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import get_database_pool
from openorc.services import task_history as task_history_service
from openorc.services import task_reads
from openorc.services.task_history import HistoryCollection
from openorc.services.task_reads import (
    TaskCostFacts,
    TaskDetail,
    TaskSummaryItem,
)

router = APIRouter(tags=["tasks"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_TASKS_PATH = "/workspaces/{workspace_id}/tasks"
_TASK_PATH = "/workspaces/{workspace_id}/tasks/{task_id}"
_TASK_HISTORY_PATH = "/workspaces/{workspace_id}/tasks/{task_id}/history"
_REPOSITORY_TASKS_PATH = "/workspaces/{workspace_id}/repositories/{repository_id}/tasks"
_ISSUE_TASKS_PATH = (
    "/workspaces/{workspace_id}/repositories/{repository_id}/issues/{issue_id}/tasks"
)

_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}

_STATUS_VALUES = tuple(status.value for status in TaskStatus)


def _dec_str(value: Decimal | None) -> str | None:
    """Serialize an exact decimal USD value as its exact decimal string."""
    return None if value is None else format(value, "f")


def _issue_summary_to_wire(projection: GitHubIssueProjection) -> GitHubTaskIssueSummaryRead:
    return GitHubTaskIssueSummaryRead(
        id=projection.identity.github_issue_id,
        number=projection.issue_number,
        title=projection.title,
        state=projection.state.value,
        html_url=projection.html_url,
    )


def _pr_to_wire(pull_request: TaskPullRequest) -> GitHubTaskPullRequestRead:
    return GitHubTaskPullRequestRead(
        id=pull_request.github_pr_id,
        number=pull_request.github_pr_number,
        title=pull_request.title,
        body=pull_request.body,
        html_url=pull_request.html_url,
        user=(
            None
            if pull_request.user_id is None or pull_request.user_login is None
            else GitHubTaskUserRefRead(id=pull_request.user_id, login=pull_request.user_login)
        ),
        state=pull_request.state.value,
        draft=pull_request.draft,
        created_at=pull_request.provider_created_at,
        updated_at=pull_request.provider_updated_at,
        closed_at=pull_request.provider_closed_at,
        merged_at=pull_request.merged_at,
        head=GitHubTaskPullRequestHeadRefRead(ref=pull_request.head_ref, sha=pull_request.head_sha),
        base=GitHubTaskPullRequestHeadRefRead(ref=pull_request.base_ref, sha=pull_request.base_sha),
        mergeable=pull_request.mergeable,
        merge_state_status=pull_request.merge_state_status,
        mergeable_observed_at=pull_request.pr_mergeability_observed_at,
        mergeable_head_sha=pull_request.pr_mergeability_head_sha,
    )


def _github_facts_to_wire(item: TaskSummaryItem) -> GitHubTaskFactsRead:
    return GitHubTaskFactsRead(
        issue=None if item.github_issue is None else _issue_summary_to_wire(item.github_issue),
        pull_request=(None if item.pull_request is None else _pr_to_wire(item.pull_request)),
    )


def _cost_to_wire(cost: TaskCostFacts) -> OpenOrcTaskCostRead:
    return OpenOrcTaskCostRead(
        retained_cost_usd=_dec_str(cost.retained_cost_usd),
        authorized_task_budget_usd=_dec_str(cost.authorized_task_budget_usd),
        effective_cost_threshold_usd=_dec_str(cost.effective_cost_threshold_usd),
        is_partial=cost.is_partial,
    )


def _gate_summary_to_wire(
    summary: task_reads.TaskGateSummary | None,
) -> OpenOrcTaskGateSummaryRead | None:
    if summary is None:
        return None
    gate = summary.gate
    return OpenOrcTaskGateSummaryRead(
        id=gate.id,
        gate_type=gate.gate_type.value,
        status=gate.status.value,
        actions=list(summary.actions),
    )


def _block_summary_to_wire(
    summary: task_reads.TaskBlockSummary | None,
) -> OpenOrcTaskBlockSummaryRead | None:
    if summary is None:
        return None
    return OpenOrcTaskBlockSummaryRead(
        id=summary.block.id,
        reason=summary.block.reason.value,
        created_at=summary.block.created_at,
    )


def _terminalization_to_wire(
    facts: task_reads.TaskTerminalizationFacts | None,
) -> OpenOrcTaskTerminalizationRead | None:
    if facts is None:
        return None
    return OpenOrcTaskTerminalizationRead(
        outcome=facts.outcome.value,
        origin=facts.origin,
        established_at=facts.established_at,
    )


def _summary_to_wire(item: TaskSummaryItem) -> TaskListItemRead:
    task = item.task
    return TaskListItemRead(
        github=_github_facts_to_wire(item),
        openorc=OpenOrcTaskSummaryRead(
            id=task.id,
            workspace_id=task.workspace_id,
            repository_id=task.repository_id,
            status=task.status.value,
            state_token=task.state_token,
            task_pull_request_id=(None if item.pull_request is None else item.pull_request.id),
            cost=_cost_to_wire(item.cost),
            current_owner_gate=_gate_summary_to_wire(item.current_gate),
            current_block=_block_summary_to_wire(item.current_block),
            actions=list(item.actions),
            queued_at=task.queued_at,
            queue_position=item.queue_position,
            created_at=task.created_at,
            updated_at=task.updated_at,
            archived_at=task.archived_at,
            terminal_cause=(None if task.terminal_cause is None else task.terminal_cause.value),
            terminalization=_terminalization_to_wire(task_reads.terminalization_facts(task)),
        ),
    )


def _parse_statuses(statuses: str | None) -> tuple[TaskStatus, ...] | None:
    """Parse the canonical status filter (comma-separated TaskStatus values)."""
    if statuses is None:
        return None
    values = [part.strip() for part in statuses.split(",") if part.strip()]
    if not values:
        return None
    parsed: list[TaskStatus] = []
    for value in values:
        try:
            parsed.append(TaskStatus(value))
        except ValueError as error:
            raise task_reads.InvalidCommandError(
                "the status filter must use canonical TaskStatus values"
            ) from error
    return tuple(parsed)


def _page_tasks(
    *,
    list_fn: Any,
    workspace_id: UUID,
    statuses: tuple[TaskStatus, ...] | None,
    archival: str,
    cursor: str | None,
) -> TaskListRead:
    statuses_tuple = statuses if statuses is not None else tuple(TaskStatus)
    items, next_cursor = list_fn(
        statuses=statuses_tuple,
        archival=archival,
        cursor=cursor,
    )
    return TaskListRead(
        items=[_summary_to_wire(item) for item in items],
        next_cursor=next_cursor,
    )


@router.get(
    _TASKS_PATH,
    response_model=TaskListRead,
    operation_id="list_workspace_tasks",
    responses=_READ_RESPONSES,
)
def list_workspace_tasks(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    status: Annotated[
        str | None, Query(description="Comma-separated canonical TaskStatus values")
    ] = None,
    archival: Annotated[str, Query(pattern="^(current|archived|all)$")] = "current",
    cursor: Annotated[
        str | None, Query(description="The previous page's opaque next_cursor")
    ] = None,
) -> TaskListRead:
    """List one Workspace's authorized Tasks.

    Bounded, deterministically ordered (``(created_at, id)`` keyset), and
    advanced by the opaque ``next_cursor`` when more items exist. The
    archival filter defaults to current (non-archived) attempts; the status
    filter defaults to every canonical status. List items carry the
    ownership-separated summary and the backend-generated actions.
    """
    settings: Settings = request.app.state.settings
    parsed = _parse_statuses(status)
    return _page_tasks(
        list_fn=lambda **kwargs: task_reads.list_workspace_tasks_for_owner(
            get_database_pool(settings),
            profile_id=profile.id,
            workspace_id=workspace_id,
            **kwargs,
        ),
        workspace_id=workspace_id,
        statuses=parsed,
        archival=archival,
        cursor=cursor,
    )


@router.get(
    _REPOSITORY_TASKS_PATH,
    response_model=TaskListRead,
    operation_id="list_repository_tasks",
    responses=_READ_RESPONSES,
)
def list_repository_tasks(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    repository_id: UUID,
    status: Annotated[
        str | None, Query(description="Comma-separated canonical TaskStatus values")
    ] = None,
    archival: Annotated[str, Query(pattern="^(current|archived|all)$")] = "current",
    cursor: Annotated[
        str | None, Query(description="The previous page's opaque next_cursor")
    ] = None,
) -> TaskListRead:
    """List one Workspace Repository's authorized Tasks.

    Bounded, deterministically ordered, and advanced by the opaque
    ``next_cursor``. The archival filter defaults to current attempts.
    """
    settings: Settings = request.app.state.settings
    parsed = _parse_statuses(status)
    return _page_tasks(
        list_fn=lambda **kwargs: task_reads.list_repository_tasks_for_owner(
            get_database_pool(settings),
            profile_id=profile.id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            **kwargs,
        ),
        workspace_id=workspace_id,
        statuses=parsed,
        archival=archival,
        cursor=cursor,
    )


@router.get(
    _ISSUE_TASKS_PATH,
    response_model=TaskListRead,
    operation_id="list_repository_issue_tasks",
    responses=_READ_RESPONSES,
)
def list_repository_issue_tasks(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    repository_id: UUID,
    issue_id: UUID,
    status: Annotated[
        str | None, Query(description="Comma-separated canonical TaskStatus values")
    ] = None,
    archival: Annotated[str, Query(pattern="^(current|archived|all)$")] = "all",
    cursor: Annotated[
        str | None, Query(description="The previous page's opaque next_cursor")
    ] = None,
) -> TaskListRead:
    """List every attempt for one G2a issue projection.

    Each Task is one attempt — there is no separate Attempt entity — and the
    list includes archived attempts by default. Bounded, deterministically
    ordered, and advanced by the opaque ``next_cursor``.
    """
    settings: Settings = request.app.state.settings
    parsed = _parse_statuses(status)
    return _page_tasks(
        list_fn=lambda **kwargs: task_reads.list_issue_tasks_for_owner(
            get_database_pool(settings),
            profile_id=profile.id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_id=issue_id,
            **kwargs,
        ),
        workspace_id=workspace_id,
        statuses=parsed,
        archival=archival,
        cursor=cursor,
    )


def _gate_detail_to_wire(summary: task_reads.TaskGateSummary) -> OpenOrcTaskGateDetailRead:
    gate = summary.gate
    return OpenOrcTaskGateDetailRead(
        id=gate.id,
        gate_type=gate.gate_type.value,
        status=gate.status.value,
        actions=list(summary.actions),
        plan_revision_id=gate.plan_revision_id,
        subject_head_sha=gate.subject_head_sha,
        task_pull_request_id=gate.task_pull_request_id,
        task_block_id=gate.task_block_id,
        created_at=gate.created_at,
        decided_at=gate.decided_at,
    )


def _block_detail_to_wire(
    summary: task_reads.TaskBlockSummary | None,
) -> OpenOrcTaskBlockDetailRead | None:
    if summary is None:
        return None
    block = summary.block
    return OpenOrcTaskBlockDetailRead(
        id=block.id,
        reason=block.reason.value,
        created_at=block.created_at,
        context=dict(block.context),
        resolved_at=block.resolved_at,
    )


def _plan_to_wire(plan: PlanRevision | None) -> OpenOrcTaskPlanRead | None:
    if plan is None:
        return None
    return OpenOrcTaskPlanRead(
        id=plan.id,
        revision_number=plan.revision_number,
        content=plan.content,
        repository_base_sha=plan.repository_base_sha,
        created_at=plan.created_at,
    )


def _execution_result_to_wire(
    result: dict[str, object] | None,
) -> OpenOrcTaskExecutionResultRead | None:
    if result is None:
        return None
    status_value = result.get("status")
    branch_value = result.get("branch")
    summary_value = result.get("summary")
    changes_value = result.get("changes")
    validation_value = result.get("validation")
    notes_value = result.get("notes")
    return OpenOrcTaskExecutionResultRead(
        status=status_value if isinstance(status_value, str) else None,
        branch=branch_value if isinstance(branch_value, str) else None,
        summary=summary_value if isinstance(summary_value, str) else None,
        changes=changes_value if isinstance(changes_value, list) else None,
        validation=validation_value if isinstance(validation_value, list) else None,
        notes=notes_value if isinstance(notes_value, str) else None,
    )


def _execution_to_wire(facts: task_reads.TaskExecutionFacts) -> OpenOrcTaskExecutionRead:
    execution = facts.execution
    return OpenOrcTaskExecutionRead(
        id=execution.id,
        execution_number=execution.execution_number,
        status=execution.status.value,
        producer_session_id=execution.producer_session_id,
        review_iteration_id=execution.review_iteration_id,
        created_at=execution.created_at,
        updated_at=execution.updated_at,
        last_qualifying_activity_at=execution.last_qualifying_activity_at,
        result=_execution_result_to_wire(facts.result),
    )


def _session_to_wire(facts: task_reads.TaskSessionFacts) -> OpenOrcTaskSessionRead:
    session = facts.session
    return OpenOrcTaskSessionRead(
        id=session.id,
        role=session.role.value,
        connection_id=session.connection_id,
        lifecycle_status=session.lifecycle_status.value,
        configured_provider=facts.configured_provider,
        configured_model=facts.configured_model,
        reported_provider=session.reported_provider,
        reported_model=session.reported_model,
        reported_runtime_version=session.reported_runtime_version,
        retained_cost_usd=_dec_str(session.retained_cost_usd),
        cost_observed_at=session.cost_observed_at,
        initialized_at=session.initialized_at,
        ended_at=session.ended_at,
        created_at=session.created_at,
        updated_at=session.updated_at,
    )


def _review_iteration_to_wire(
    facts: task_reads.TaskReviewIterationFacts,
) -> OpenOrcTaskReviewIterationRead:
    iteration = facts.iteration
    return OpenOrcTaskReviewIterationRead(
        id=iteration.id,
        review_loop_id=iteration.review_loop_id,
        iteration_number=iteration.iteration_number,
        plan_revision_id=iteration.plan_revision_id,
        task_pull_request_id=iteration.task_pull_request_id,
        reviewed_head_sha=iteration.reviewed_head_sha,
        outcome=None if iteration.outcome is None else iteration.outcome.value,
        summary=iteration.summary,
        findings=None if iteration.findings is None else list(iteration.findings),
        decided_at=iteration.decided_at,
        created_at=iteration.created_at,
    )


def _review_loop_to_wire(facts: task_reads.TaskReviewLoopFacts) -> OpenOrcTaskReviewLoopRead:
    loop = facts.loop
    return OpenOrcTaskReviewLoopRead(
        id=loop.id,
        purpose=loop.purpose.value,
        iteration_limit=loop.iteration_limit,
        status=loop.status.value,
        created_at=loop.created_at,
        closed_at=loop.closed_at,
        iterations=[
            _review_iteration_to_wire(iteration) for iteration in facts.presented_iterations
        ],
        iteration_total=facts.iteration_total,
        iterations_bounded=facts.iterations_bounded,
    )


def _terminalization_detail_to_wire(
    facts: task_reads.TaskTerminalizationFacts | None,
) -> OpenOrcTaskTerminalizationDetailRead | None:
    if facts is None:
        return None
    return OpenOrcTaskTerminalizationDetailRead(
        outcome=facts.outcome.value,
        origin=facts.origin,
        established_at=facts.established_at,
        cleanup_send_claimed_at=facts.cleanup_send_claimed_at,
        cleanup_send_settled_at=facts.cleanup_send_settled_at,
    )


def _detail_to_wire(detail: TaskDetail) -> OpenOrcTaskDetailRead:
    summary = detail.summary
    task = summary.task
    return OpenOrcTaskDetailRead(
        github=_github_facts_to_wire(summary),
        openorc=OpenOrcTaskDetailBlockRead(
            id=task.id,
            workspace_id=task.workspace_id,
            repository_id=task.repository_id,
            status=task.status.value,
            state_token=task.state_token,
            task_pull_request_id=(
                None if summary.pull_request is None else summary.pull_request.id
            ),
            canonical_feature_branch=task.canonical_feature_branch,
            cost=_cost_to_wire(summary.cost),
            current_owner_gate=(
                None if summary.current_gate is None else _gate_detail_to_wire(summary.current_gate)
            ),
            current_block=_block_detail_to_wire(summary.current_block),
            current_plan=_plan_to_wire(detail.current_plan),
            sessions=[_session_to_wire(facts) for facts in detail.sessions],
            executions=[_execution_to_wire(facts) for facts in detail.executions],
            executions_total=detail.executions_total,
            planning_bootstrap_stage=detail.planning_bootstrap_stage,
            review_loops=[_review_loop_to_wire(facts) for facts in detail.review_loops],
            pending_runtime_interactions=[
                OpenOrcTaskPendingInteractionRead(
                    operation=interaction.operation,
                    role=interaction.role,
                    task_agent_session_id=interaction.task_agent_session_id,
                    subject=interaction.subject,
                    started_at=interaction.started_at,
                )
                for interaction in detail.pending_runtime_interactions
            ],
            pending_runtime_requests=[
                OpenOrcTaskRuntimeRequestRead(
                    id=request.id,
                    kind=request.kind.value,
                    external_approval_id=request.external_approval_id,
                    status=request.status.value,
                    created_at=request.created_at,
                    producer_session_id=request.producer_session_id,
                )
                for request in detail.pending_runtime_requests
            ],
            outstanding_receipts=[
                OpenOrcTaskOutstandingReceiptRead(
                    id=receipt.id,
                    operation=receipt.operation.value,
                    original_attempt_kind=receipt.original_attempt_kind.value,
                    original_attempt_id=receipt.original_attempt_id,
                    status=receipt.status.value,
                    outcome=dict(receipt.outcome),
                    claimed_at=receipt.claimed_at,
                    admission_attempted_at=receipt.admission_attempted_at,
                    admitted_at=receipt.admitted_at,
                    settled_at=receipt.settled_at,
                    contradicted_at=receipt.contradicted_at,
                )
                for receipt in detail.outstanding_receipts
            ],
            outstanding_receipts_total=detail.outstanding_receipts_total,
            outstanding_intents=[
                OpenOrcTaskOutstandingIntentRead(
                    id=intent.id,
                    kind=intent.kind,
                    status=intent.status,
                    created_at=intent.created_at,
                    updated_at=intent.updated_at,
                )
                for intent in detail.outstanding_intents
            ],
            actions=list(summary.actions),
            queued_at=task.queued_at,
            queue_position=summary.queue_position,
            created_at=task.created_at,
            updated_at=task.updated_at,
            archived_at=task.archived_at,
            terminal_cause=(None if task.terminal_cause is None else task.terminal_cause.value),
            terminalization=_terminalization_detail_to_wire(task_reads.terminalization_facts(task)),
        ),
    )


@router.get(
    _TASK_PATH,
    response_model=OpenOrcTaskDetailRead,
    operation_id="get_task",
    responses=_READ_RESPONSES,
)
def get_task(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    task_id: UUID,
) -> OpenOrcTaskDetailRead:
    """Return one authorized Task's composed detail.

    Addressed by the internal Task UUID and scoped to the addressed
    Workspace; the detail carries the ownership-separated fact set with the
    current typed records, the backend-generated actions, and the coherent
    durable observation behind them.
    """
    settings: Settings = request.app.state.settings
    detail = task_reads.get_task_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
    )
    return _detail_to_wire(detail)


def _history_collection_to_wire(collection: HistoryCollection) -> TaskHistoryCollectionRead:
    items: list[object] = []
    for item in collection.items:
        if isinstance(item, PlanRevision):
            items.append(
                TaskHistoryPlanRevisionRead(
                    id=item.id,
                    revision_number=item.revision_number,
                    content=item.content,
                    repository_base_sha=item.repository_base_sha,
                    created_at=item.created_at,
                )
            )
        elif isinstance(item, ReviewLoop):
            items.append(
                TaskHistoryReviewLoopRead(
                    id=item.id,
                    purpose=item.purpose.value,
                    iteration_limit=item.iteration_limit,
                    status=item.status.value,
                    created_at=item.created_at,
                    closed_at=item.closed_at,
                )
            )
        elif isinstance(item, ReviewIteration):
            items.append(
                TaskHistoryReviewIterationRead(
                    id=item.id,
                    review_loop_id=item.review_loop_id,
                    iteration_number=item.iteration_number,
                    plan_revision_id=item.plan_revision_id,
                    task_pull_request_id=item.task_pull_request_id,
                    reviewed_head_sha=item.reviewed_head_sha,
                    outcome=None if item.outcome is None else item.outcome.value,
                    summary=item.summary,
                    findings=None if item.findings is None else list(item.findings),
                    decided_at=item.decided_at,
                    created_at=item.created_at,
                )
            )
        elif isinstance(item, OwnerGate):
            # Historical gates carry their decision record only — never an
            # actionable menu.
            items.append(
                TaskHistoryOwnerGateRead(
                    id=item.id,
                    gate_type=item.gate_type.value,
                    status=item.status.value,
                    plan_revision_id=item.plan_revision_id,
                    subject_head_sha=item.subject_head_sha,
                    task_pull_request_id=item.task_pull_request_id,
                    task_block_id=item.task_block_id,
                    decided_at=item.decided_at,
                    created_at=item.created_at,
                )
            )
        elif isinstance(item, Execution):
            items.append(
                TaskHistoryExecutionRead(
                    id=item.id,
                    execution_number=item.execution_number,
                    status=item.status.value,
                    producer_session_id=item.producer_session_id,
                    review_iteration_id=item.review_iteration_id,
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                    last_qualifying_activity_at=item.last_qualifying_activity_at,
                    result=(
                        None
                        if not isinstance(item.implementation_result, dict)
                        else _execution_result_to_wire(item.implementation_result)
                    ),
                )
            )
        elif isinstance(item, TaskBlock):
            items.append(
                TaskHistoryTaskBlockRead(
                    id=item.id,
                    reason=item.reason.value,
                    context=dict(item.context),
                    resolved_at=item.resolved_at,
                    created_at=item.created_at,
                )
            )
        elif isinstance(item, RuntimeRequest):
            items.append(
                TaskHistoryRuntimeRequestRead(
                    id=item.id,
                    kind=item.kind.value,
                    external_approval_id=item.external_approval_id,
                    status=item.status.value,
                    resolution=(None if item.resolution is None else item.resolution.value),
                    closed_at=item.closed_at,
                    created_at=item.created_at,
                    producer_session_id=item.producer_session_id,
                )
            )
        elif isinstance(item, TaskInterruptIntent):
            items.append(
                TaskHistoryInterruptIntentRead(
                    id=item.id,
                    status=item.status.value,
                    outcome=dict(item.outcome),
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                )
            )
        elif isinstance(item, TaskResumeIntent):
            items.append(
                TaskHistoryResumeIntentRead(
                    id=item.id,
                    status=item.status.value,
                    outcome=dict(item.outcome),
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                )
            )
        elif isinstance(item, TaskPullRequestCloseIntent):
            items.append(
                TaskHistoryPrCloseIntentRead(
                    id=item.id,
                    status=item.status.value,
                    outcome=dict(item.outcome),
                    created_at=item.created_at,
                    updated_at=item.updated_at,
                )
            )
        elif isinstance(item, TaskDetachedTurnReceipt):
            items.append(
                TaskHistoryReceiptRead(
                    id=item.id,
                    operation=item.operation.value,
                    original_attempt_kind=item.original_attempt_kind.value,
                    original_attempt_id=item.original_attempt_id,
                    status=item.status.value,
                    outcome=dict(item.outcome),
                    claimed_at=item.claimed_at,
                    admission_attempted_at=item.admission_attempted_at,
                    admitted_at=item.admitted_at,
                    settled_at=item.settled_at,
                    contradicted_at=item.contradicted_at,
                )
            )
        elif isinstance(item, TaskRuntimeSendClaim):
            items.append(
                TaskHistorySendClaimRead(
                    id=item.id,
                    kind=item.kind.value,
                    status=item.status.value,
                    outcome=dict(item.outcome),
                    claimed_at=item.claimed_at,
                    settled_at=item.settled_at,
                )
            )
        else:  # pragma: no cover - the vocabulary is closed
            raise TypeError("unmapped history collection item")
    return TaskHistoryCollectionRead(
        name=collection.name,
        items=items,
        next_cursor=collection.next_cursor,
        total=collection.total,
    )


@router.get(
    _TASK_HISTORY_PATH,
    response_model=TaskHistoryRead,
    operation_id="get_task_history",
    responses=_READ_RESPONSES,
)
def get_task_history(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    task_id: UUID,
    collections: Annotated[
        str | None,
        Query(description="Comma-separated documented collection selection (default: all)"),
    ] = None,
    cursor: Annotated[
        str | None,
        Query(description="A collection's opaque next_cursor (pages that collection only)"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = task_history_service.HISTORY_COLLECTION_PAGE_LIMIT,
) -> TaskHistoryRead:
    """Return the Task's separate bounded workflow history.

    Historical domain records, not reconstructed audit events: each selected
    collection is bounded independently and advanced by its own opaque
    per-collection ``next_cursor``; a cursor pages exactly its collection.
    Historical gates carry no actionable menus. G2c (issue #377) owns the
    WorkflowEvent audit/history surface.
    """
    settings: Settings = request.app.state.settings
    selected = (
        None
        if collections is None
        else [part.strip() for part in collections.split(",") if part.strip()]
    )
    history = task_history_service.list_task_history_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        collections=selected,
        cursor=cursor,
        limit=limit,
    )
    return TaskHistoryRead(
        task_id=history.task_id,
        collections=[_history_collection_to_wire(collection) for collection in history.collections],
    )
