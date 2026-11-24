"""Wire DTOs for the Task discussion and conversation surface (issue #339).

Thin transport models only: the Owner-authorized application services own
every semantic rule (discussion admission authority, gate currentness,
receipt lifecycle, conversation identity) and these models never duplicate
service/domain validation — FastAPI parses the declared shapes and the
router delegates. No model ever carries conversation text durably: the
request message travels to the operation invocation, and the conversation
read projects the runtime-owned messages transiently.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, StrictInt, StrictStr

from openorc.domain.detached_turns import TaskDetachedTurnReceiptStatus

__all__ = [
    "ArchivedTaskPurge",
    "GitHubTaskFactsRead",
    "GitHubTaskIssueSummaryRead",
    "GitHubTaskPullRequestHeadRefRead",
    "GitHubTaskPullRequestRead",
    "GitHubTaskUserRefRead",
    "OpenOrcTaskBlockDetailRead",
    "OpenOrcTaskBlockSummaryRead",
    "OpenOrcTaskCostRead",
    "OpenOrcTaskDetailBlockRead",
    "OpenOrcTaskDetailRead",
    "OpenOrcTaskExecutionRead",
    "OpenOrcTaskExecutionResultRead",
    "OpenOrcTaskGateDetailRead",
    "OpenOrcTaskGateSummaryRead",
    "OpenOrcTaskOutstandingIntentRead",
    "OpenOrcTaskOutstandingReceiptRead",
    "OpenOrcTaskPendingInteractionRead",
    "OpenOrcTaskPlanRead",
    "OpenOrcTaskReviewIterationRead",
    "OpenOrcTaskReviewLoopRead",
    "OpenOrcTaskRuntimeRequestRead",
    "OpenOrcTaskSessionRead",
    "OpenOrcTaskSummaryRead",
    "OpenOrcTaskTerminalizationDetailRead",
    "OpenOrcTaskTerminalizationRead",
    "TaskListItemRead",
    "TaskListRead",
    "DiscussionAdmissionRead",
    "DiscussionMessageCreate",
    "DiscussionStatusRead",
    "ReviewerConversationMessageRead",
    "ReviewerConversationRead",
]


class DiscussionMessageCreate(BaseModel):
    """One verbatim Owner message for the addressed current OwnerGate.

    Bounded to the service's own discussion message bound — the wire bound
    mirrors it so an oversized body is rejected at the boundary before any
    service effect. The message is Owner-authored conversational command
    input: transient on the wire and in the operation invocation, never
    durably stored by OpenOrc.
    """

    message: str = Field(min_length=1, max_length=20_000)


class DiscussionAdmissionRead(BaseModel):
    """The 202 acknowledgement of one authorized-and-queued discussion message.

    This is explicitly NOT delivery and carries no model reply: the receipt
    is committed and claimed, the admission operation was accepted for
    delivery, and the receipt-status projection reports the actual
    admission, refusal, or recorded exchange lifecycle.
    """

    receipt_id: UUID
    owner_gate_id: UUID
    reviewer_session_id: UUID
    task_id: UUID
    status: TaskDetachedTurnReceiptStatus
    claimed_at: datetime


class DiscussionStatusRead(BaseModel):
    """The Owner-visible lifecycle of one gate-bound discussion exchange.

    ``admission_attempted_at`` lets a ``claimed`` status distinguish an
    authorized message whose admission has not yet been attempted from one
    whose attempt is unresolved. ``outcome_label`` is the safe settlement or
    refusal label exactly as recorded (``None`` while claimed or admitted).
    """

    receipt_id: UUID
    status: TaskDetachedTurnReceiptStatus
    claimed_at: datetime
    admission_attempted_at: datetime | None
    admitted_at: datetime | None
    settled_at: datetime | None
    outcome_label: str | None


class ReviewerConversationMessageRead(BaseModel):
    """One projected conversation message (the runtime's own record shape).

    ``index`` is the zero-based conversation position; ``kind`` is the
    runtime message's own category token when present; ``text`` is its
    textual content when present. A message shaped beyond the projection
    carries ``None`` fields — honestly unknown, never guessed.
    """

    index: int
    kind: str | None
    text: str | None


class ReviewerConversationRead(BaseModel):
    """The transient projection of the Task's Reviewer session conversation.

    Read through the runtime-native transcript surface at request time; the
    runtime owns the conversation and OpenOrc records no transcript.
    """

    task_id: UUID
    reviewer_session_id: UUID
    messages: list[ReviewerConversationMessageRead]


class ArchivedTaskPurge(BaseModel):
    """The settled result of one archived-Task administrative purge (issue #374).

    Identifies the purged archived attempt within its Workspace. Only
    archived (terminal) attempts are purgeable — a current Task is a typed
    conflict, and normal workflow cancellation is the supported route. The
    purge is internal-state deletion only: the backing GitHub issue,
    branch, and PR are never touched.
    """

    workspace_id: UUID
    task_id: UUID


# --- The authorized Task read surface (issue #376) ---------------------------


class GitHubTaskIssueSummaryRead(BaseModel):
    """The linked GitHub issue's selected G2a native-field summary (#376).

    GitHub-owned facts only, native names/types/nullability preserved: the
    stable identity/number plus the presentation fields the G2a projection
    has observed. An unprojected presentation field is ``null`` — never
    fabricated. The full GitHub issue data remains available through G2a.
    """

    id: StrictInt
    number: StrictInt
    title: StrictStr | None
    state: StrictStr | None
    html_url: StrictStr | None


class GitHubTaskUserRefRead(BaseModel):
    """The documented bounded PR author presentation subset (#376)."""

    id: StrictInt
    login: StrictStr


class GitHubTaskPullRequestHeadRefRead(BaseModel):
    """The documented PR head/base ref facts (#376)."""

    ref: StrictStr
    sha: StrictStr | None


class GitHubTaskPullRequestRead(BaseModel):
    """The canonical TaskPullRequest's observed native provider facts (#376).

    The richer documented PR object the #63/#376 reconciliation retains:
    stable id/number, presentation facts, lifecycle instants, exact head
    association, and the head-bound mergeability observation context. A
    retained observation reports its own ``mergeable_observed_at`` and
    ``mergeable_head_sha`` — a stale result is never presented as the new
    head's observation, and unknown/not-determined readings stay ``null``.
    Conflict mergeability and the policy/check-dependent merge state stay
    distinct facts; neither is ever a can-merge boolean, and GitHub remains
    the final merge authority.
    """

    id: StrictInt
    number: StrictInt
    title: StrictStr | None
    body: StrictStr | None
    html_url: StrictStr | None
    user: GitHubTaskUserRefRead | None
    state: StrictStr
    draft: bool | None
    created_at: datetime | None
    updated_at: datetime | None
    closed_at: datetime | None
    merged_at: datetime | None
    head: GitHubTaskPullRequestHeadRefRead
    base: GitHubTaskPullRequestHeadRefRead
    mergeable: bool | None
    merge_state_status: StrictStr | None
    mergeable_observed_at: datetime | None
    mergeable_head_sha: StrictStr | None


class GitHubTaskFactsRead(BaseModel):
    """The ``github`` block of the ownership-separated Task read (#376)."""

    issue: GitHubTaskIssueSummaryRead | None
    pull_request: GitHubTaskPullRequestRead | None


class OpenOrcTaskCostRead(BaseModel):
    """The Task's derived expenditure block (issues #240/#291, #376).

    Decimal USD values serialize as exact decimal strings. ``null`` retained
    cost is unknown (never zero); ``is_partial`` marks a known subtotal. A
    ``null`` effective threshold means spending policy is disabled, never
    zero. No live cost polling ever happens behind this projection.
    """

    retained_cost_usd: StrictStr | None
    authorized_task_budget_usd: StrictStr | None
    effective_cost_threshold_usd: StrictStr | None
    is_partial: bool


class OpenOrcTaskGateSummaryRead(BaseModel):
    """The exact current pending OwnerGate and its offered menu (#376)."""

    id: UUID
    gate_type: StrictStr
    status: StrictStr
    actions: list[StrictStr]


class OpenOrcTaskBlockSummaryRead(BaseModel):
    """The exact unresolved TaskBlock summary (#376)."""

    id: UUID
    reason: StrictStr
    created_at: datetime


class OpenOrcTaskTerminalizationRead(BaseModel):
    """The Task's terminalization commitment (summary shape, #376)."""

    outcome: StrictStr
    origin: StrictStr
    established_at: datetime


class OpenOrcTaskSummaryRead(BaseModel):
    """The ``openorc`` block of the ownership-separated Task read (#376).

    ``status`` is the canonical TaskStatus, lowercase, never a synthetic UI
    state. ``queue_position`` is the derived Workspace FIFO order context
    when queued (no new position storage, no guaranteed execution rank);
    ``terminal_cause`` is the canonical externally-established cancellation
    cause of an archived attempt, deliberately distinct from an active
    block. ``actions`` are the backend-generated Task-level command tokens;
    ``task_pull_request_id`` is the internal canonical PR identity.
    """

    id: UUID
    workspace_id: UUID
    repository_id: UUID
    status: StrictStr
    state_token: UUID
    task_pull_request_id: UUID | None
    cost: OpenOrcTaskCostRead
    current_owner_gate: OpenOrcTaskGateSummaryRead | None
    current_block: OpenOrcTaskBlockSummaryRead | None
    actions: list[StrictStr]
    queued_at: datetime | None
    queue_position: int | None
    created_at: datetime
    updated_at: datetime
    archived_at: datetime | None
    terminal_cause: StrictStr | None
    terminalization: OpenOrcTaskTerminalizationRead | None


class TaskListItemRead(BaseModel):
    """The ownership-separated Task list item (issue #376)."""

    github: GitHubTaskFactsRead
    openorc: OpenOrcTaskSummaryRead


class TaskListRead(BaseModel):
    """The bounded Task list envelope with its opaque deterministic cursor."""

    items: list[TaskListItemRead]
    next_cursor: StrictStr | None


class OpenOrcTaskGateDetailRead(OpenOrcTaskGateSummaryRead):
    """The exact current gate's full subject binding (detail shape, #376).

    The exact subject IDs/SHA the gate binds — the PlanRevision, the exact
    head SHA, the canonical TaskPullRequest, or the unresolved TaskBlock —
    per the settled v1 subject rules, plus the decision stamps.
    """

    plan_revision_id: UUID | None
    subject_head_sha: StrictStr | None
    task_pull_request_id: UUID | None
    task_block_id: UUID | None
    created_at: datetime
    decided_at: datetime | None


class OpenOrcTaskBlockDetailRead(OpenOrcTaskBlockSummaryRead):
    """The unresolved TaskBlock's typed reason-specific context (#376).

    ``context`` is the block's own reason-specific recovery/continuation
    context — the durable canonical context object, never an arbitrary JSON
    serialization — plus the semantic resolution stamp.
    """

    context: dict[str, object]
    resolved_at: datetime | None


class OpenOrcTaskTerminalizationDetailRead(OpenOrcTaskTerminalizationRead):
    """The terminalization commitment with its cleanup evidence (#376).

    Preserves the committed-but-unarchived distinction: the set-once
    commitment is visible in list and detail alike, and the detail adds the
    optional cleanup send's claim/settlement instants.
    """

    cleanup_send_claimed_at: datetime | None
    cleanup_send_settled_at: datetime | None


class OpenOrcTaskPlanRead(BaseModel):
    """The current immutable PlanRevision pointer target (#376).

    Existence never implies Reviewer acceptance; the content is the exact
    immutable revision the pointer targets.
    """

    id: UUID
    revision_number: StrictInt
    content: StrictStr
    repository_base_sha: StrictStr
    created_at: datetime


class OpenOrcTaskSessionRead(BaseModel):
    """One Task/role session binding's authorized presentation facts (#376).

    The role-array sessions carry the existing ``WorkflowRole`` values — no
    fixed producer/reviewer keys, no fictitious sessions, and the existing
    one-session-per-Task/role v1 invariant. Configured provider/model are
    the effective configuration captured for that Task session at
    initialization; reported fields are runtime observations. No raw
    configuration snapshot, prompt, secret, auth reference, native run ID,
    or internal control endpoint is ever exposed.
    """

    id: UUID
    role: StrictStr
    connection_id: UUID
    lifecycle_status: StrictStr
    configured_provider: StrictStr | None
    configured_model: StrictStr | None
    reported_provider: StrictStr | None
    reported_model: StrictStr | None
    reported_runtime_version: StrictStr | None
    retained_cost_usd: StrictStr | None
    cost_observed_at: datetime | None
    initialized_at: datetime | None
    ended_at: datetime | None
    created_at: datetime
    updated_at: datetime


class OpenOrcTaskExecutionResultRead(BaseModel):
    """The safe typed projection of one E6 validated execution result (#376)."""

    status: StrictStr | None
    branch: StrictStr | None
    summary: StrictStr | None
    changes: list[object] | None
    validation: list[object] | None
    notes: StrictStr | None


class OpenOrcTaskExecutionRead(BaseModel):
    """One relevant current/unsettled Execution's projection (#376).

    ``review_iteration_id`` is the durable one-shot E9 remediation claim;
    there is deliberately no singular current-Execution pointer — this is a
    collection of relevant current/unsettled attempts.
    """

    id: UUID
    execution_number: StrictInt
    status: StrictStr
    producer_session_id: UUID
    review_iteration_id: UUID | None
    created_at: datetime
    updated_at: datetime
    last_qualifying_activity_at: datetime | None
    result: OpenOrcTaskExecutionResultRead | None


class OpenOrcTaskReviewIterationRead(BaseModel):
    """One ReviewIteration's presentation facts (#376).

    The exact subject reviewed (the PlanRevision, or the TaskPullRequest
    plus the exact reviewed head SHA) and, once finalized, the complete
    Reviewer result — outcome, summary, findings, decided_at. An
    unfinalized iteration carries all four ``None``.
    """

    id: UUID
    review_loop_id: UUID
    iteration_number: StrictInt
    plan_revision_id: UUID | None
    task_pull_request_id: UUID | None
    reviewed_head_sha: StrictStr | None
    outcome: StrictStr | None
    summary: StrictStr | None
    findings: list[object] | None
    decided_at: datetime | None
    created_at: datetime


class OpenOrcTaskReviewLoopRead(BaseModel):
    """One open/current ReviewLoop with its presented iterations (#376).

    ``iteration_limit`` is the loop's durable autonomous-revision-budget
    snapshot — never a cap on total ReviewIterations: Owner-mediated
    evaluations can continue indefinitely within one open loop, so the
    always-present relevant current exact-subject iteration and the latest
    finalized outcome/findings are presented first, additional iterations up
    to the explicit bound, and ``iteration_total`` distinguishes a complete
    presentation from a bounded one. The full history is on the paginated
    history endpoint.
    """

    id: UUID
    purpose: StrictStr
    iteration_limit: StrictInt
    status: StrictStr
    created_at: datetime
    closed_at: datetime | None
    iterations: list[OpenOrcTaskReviewIterationRead]
    iteration_total: StrictInt
    iterations_bounded: bool


class OpenOrcTaskPendingInteractionRead(BaseModel):
    """One current-operation fact from the pending-interaction projection (#376).

    Operation, role, session, safe typed subject, and the committed start
    instant. Start means QUEUED; PLANNING can still await session
    establishment/PREPARE/PLAN. Pending durable work is never proof of live
    activity or result.
    """

    operation: StrictStr
    role: StrictStr
    task_agent_session_id: UUID | None
    subject: dict[str, object]
    started_at: datetime | None


class OpenOrcTaskOutstandingReceiptRead(BaseModel):
    """One detached-turn receipt's safe outstanding evidence (#376).

    The claim/admission-attempt/admission/settlement/contradiction instants
    and the safe outcome label exactly as recorded — never raw staged result
    candidates, provider payloads, prompts, or transcripts.
    """

    id: UUID
    operation: StrictStr
    original_attempt_kind: StrictStr
    original_attempt_id: UUID
    status: StrictStr
    outcome: dict[str, object]
    claimed_at: datetime
    admission_attempted_at: datetime | None
    admitted_at: datetime | None
    settled_at: datetime | None
    contradicted_at: datetime | None


class OpenOrcTaskOutstandingIntentRead(BaseModel):
    """One outstanding intent record's safe evidence (#376)."""

    id: UUID
    kind: StrictStr
    status: StrictStr
    created_at: datetime
    updated_at: datetime


class OpenOrcTaskRuntimeRequestRead(BaseModel):
    """One pending RuntimeRequest's safe facts (#376).

    No invented native approval action exists (#93): this is the safe
    pending-request fact set only.
    """

    id: UUID
    kind: StrictStr
    external_approval_id: StrictStr
    status: StrictStr
    created_at: datetime
    producer_session_id: UUID


class OpenOrcTaskDetailRead(BaseModel):
    """The ownership-separated Task detail (issue #376).

    Extends the shared summary with the current typed records. History is
    separate; the detail includes current/relevant records only, bounded
    explicitly with companion totals, never silently truncated. No G5 live
    runtime telemetry or transcript persistence exists behind this surface.
    """

    github: GitHubTaskFactsRead
    openorc: OpenOrcTaskDetailBlockRead


class OpenOrcTaskDetailBlockRead(OpenOrcTaskSummaryRead):
    """The detail extension of the ``openorc`` block (#376)."""

    canonical_feature_branch: StrictStr | None
    current_owner_gate: OpenOrcTaskGateDetailRead | None
    current_block: OpenOrcTaskBlockDetailRead | None
    current_plan: OpenOrcTaskPlanRead | None
    sessions: list[OpenOrcTaskSessionRead]
    executions: list[OpenOrcTaskExecutionRead]
    executions_total: StrictInt
    planning_bootstrap_stage: StrictStr | None
    review_loops: list[OpenOrcTaskReviewLoopRead]
    pending_runtime_interactions: list[OpenOrcTaskPendingInteractionRead]
    pending_runtime_requests: list[OpenOrcTaskRuntimeRequestRead]
    pending_runtime_requests_total: StrictInt
    outstanding_receipts: list[OpenOrcTaskOutstandingReceiptRead]
    outstanding_receipts_total: StrictInt
    outstanding_intents: list[OpenOrcTaskOutstandingIntentRead]
    outstanding_intents_total: StrictInt


class TaskHistoryPlanRevisionRead(BaseModel):
    """One immutable plan revision's historical record (#376)."""

    id: UUID
    revision_number: StrictInt
    content: StrictStr
    repository_base_sha: StrictStr
    created_at: datetime


class TaskHistoryReviewLoopRead(BaseModel):
    """One review loop's historical record (#376)."""

    id: UUID
    purpose: StrictStr
    iteration_limit: StrictInt
    status: StrictStr
    created_at: datetime
    closed_at: datetime | None


class TaskHistoryReviewIterationRead(BaseModel):
    """One review iteration's historical record (#376)."""

    id: UUID
    review_loop_id: UUID
    iteration_number: StrictInt
    plan_revision_id: UUID | None
    task_pull_request_id: UUID | None
    reviewed_head_sha: StrictStr | None
    outcome: StrictStr | None
    summary: StrictStr | None
    findings: list[object] | None
    decided_at: datetime | None
    created_at: datetime


class TaskHistoryOwnerGateRead(BaseModel):
    """One historical OwnerGate decision record (#376).

    History carries the decision record only — never an actionable menu on a
    historical gate.
    """

    id: UUID
    gate_type: StrictStr
    status: StrictStr
    plan_revision_id: UUID | None
    subject_head_sha: StrictStr | None
    task_pull_request_id: UUID | None
    task_block_id: UUID | None
    decided_at: datetime | None
    created_at: datetime


class TaskHistoryExecutionRead(BaseModel):
    """One execution attempt's historical record (#376)."""

    id: UUID
    execution_number: StrictInt
    status: StrictStr
    producer_session_id: UUID
    review_iteration_id: UUID | None
    created_at: datetime
    updated_at: datetime
    last_qualifying_activity_at: datetime | None
    result: OpenOrcTaskExecutionResultRead | None


class TaskHistoryTaskBlockRead(BaseModel):
    """One TaskBlock's historical record with its safe context (#376)."""

    id: UUID
    reason: StrictStr
    context: dict[str, object]
    resolved_at: datetime | None
    created_at: datetime


class TaskHistoryRuntimeRequestRead(BaseModel):
    """One runtime request's historical record (#376)."""

    id: UUID
    kind: StrictStr
    external_approval_id: StrictStr
    status: StrictStr
    resolution: StrictStr | None
    closed_at: datetime | None
    created_at: datetime
    producer_session_id: UUID


class TaskHistoryInterruptIntentRead(BaseModel):
    """One interrupt intent's historical record with its safe outcome (#376)."""

    id: UUID
    status: StrictStr
    outcome: dict[str, object]
    created_at: datetime
    updated_at: datetime


class TaskHistoryResumeIntentRead(BaseModel):
    """One resume intent's historical record with its safe outcome (#376)."""

    id: UUID
    status: StrictStr
    outcome: dict[str, object]
    created_at: datetime
    updated_at: datetime


class TaskHistoryPrCloseIntentRead(BaseModel):
    """The Task's PR-close obligation record (#376)."""

    id: UUID
    status: StrictStr
    outcome: dict[str, object]
    created_at: datetime
    updated_at: datetime


class TaskHistoryReceiptRead(BaseModel):
    """One detached-turn receipt's historical record (#376)."""

    id: UUID
    operation: StrictStr
    original_attempt_kind: StrictStr
    original_attempt_id: UUID
    status: StrictStr
    outcome: dict[str, object]
    claimed_at: datetime
    admission_attempted_at: datetime | None
    admitted_at: datetime | None
    settled_at: datetime | None
    contradicted_at: datetime | None


class TaskHistorySendClaimRead(BaseModel):
    """One runtime send claim's attempt-ledger record (#376)."""

    id: UUID
    kind: StrictStr
    status: StrictStr
    outcome: dict[str, object]
    claimed_at: datetime
    settled_at: datetime | None


class TaskHistoryCollectionRead(BaseModel):
    """One bounded history collection with its opaque continuation (#376).

    ``total`` is the collection's full durable count where the composition
    counts it, so a client distinguishes a complete page from a bounded one;
    ``next_cursor`` is the per-collection opaque continuation (``None`` when
    this page exhausted the collection). Never silently truncated.
    """

    name: StrictStr
    items: list[object]
    next_cursor: StrictStr | None
    total: StrictInt | None


class TaskHistoryRead(BaseModel):
    """The addressed Task's selected workflow history (issue #376).

    Historical domain records, not reconstructed audit events: separate
    typed collections with per-collection opaque continuation and documented
    collection selection. G2c (issue #377) owns the WorkflowEvent
    audit/history surface; no event sourcing, no new history store, no
    transcript history, and no raw secrets, runtime payloads, prompts, or
    transcripts live behind this read.
    """

    task_id: UUID
    collections: list[TaskHistoryCollectionRead]
