"""Wire DTOs for the Task discussion and conversation surface (issue #339).

Thin transport models only: the Owner-authorized application services own
every semantic rule (discussion admission authority, gate currentness,
receipt lifecycle, conversation identity) and these models never duplicate
service/domain validation — FastAPI parses the declared shapes and the
router delegates. No model ever carries conversation text durably: the
request message travels to the operation invocation, and the conversation
read projects the runtime-owned messages transiently.

The module also carries the Owner Task-command wire models (issue #378):
the Start command body and the command-outcome envelopes, which project the
application services' authoritative post-write snapshots — never a
re-derived current-state projection and never a command-status ledger —
the ordinary OwnerGate decision command models (issue #379): the
approve command body for the three ordinary decision gates and their
classified outcome envelopes, and the Task-level Owner Abort and shared
Cancel Task command models (issue #380): the state_token-only command
bodies and their classified outcome envelopes with the committed
post-write snapshots and the separately-typed per-target dispatch facts.
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
    "RepositoryIssueTaskCreatedRead",
    "TaskCommandSnapshotRead",
    "TaskListItemRead",
    "TaskListRead",
    "TaskStartCommand",
    "TaskStartSnapshotRead",
    "TaskStartedRead",
    "DiscussionAdmissionRead",
    "DiscussionMessageCreate",
    "DiscussionStatusRead",
    "ImplementationAuthorizationApprovalRead",
    "MergeDecisionOutcomeRead",
    "MergeRetryAttemptRead",
    "OwnerGateDecisionCommand",
    "OwnerGateDecisionSnapshotRead",
    "OwnerGateResolutionRead",
    "PrAuthorizationApprovalRead",
    "ReviewerConversationMessageRead",
    "ReviewerConversationRead",
    "TaskAbortCommand",
    "TaskAbortInterruptOutcomeRead",
    "TaskAbortOutcomeRead",
    "TaskCancellationOutcomeRead",
    "TaskCancelCommand",
    "TaskCloseIntentRead",
    "TaskClosedReviewLoopRead",
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


class TaskStartCommand(BaseModel):
    """One Owner Start command addressing the exact current Task state token.

    The opaque ``state_token`` is the optimistic-concurrency token the
    command's guarded ``queued`` write is conditional on: a token that no
    longer matches the current Task is the typed stale outcome, never a
    second logical start. No other command input exists — the addressed
    Task and the caller's authenticated Profile carry everything else.
    """

    state_token: UUID


class TaskCommandSnapshotRead(BaseModel):
    """The authoritative post-write Task facts of one command outcome (#378).

    These are the committed facts exactly as the command's application
    service returned them — the snapshot the guarded write produced, not a
    current-state projection. Subsequent FIFO admission, workflow
    progression, or concurrent work can move the Task immediately after the
    command commits; the current-state retrieval is the existing Task-read
    resource's job, never a re-derivation behind this envelope and never a
    command-status ledger.
    """

    task_id: UUID
    workspace_id: UUID
    repository_id: UUID
    github_issue_id: StrictInt
    github_issue_number: StrictInt
    status: StrictStr
    state_token: UUID
    created_at: datetime
    updated_at: datetime


class TaskStartSnapshotRead(TaskCommandSnapshotRead):
    """The Start command's committed ``queued`` snapshot facts (#378).

    ``queued_at`` is the original durable queue-entry timestamp; the frozen
    ``source_requirements_fingerprint`` and ``authorized_task_budget_usd``
    (an exact decimal string, ``null`` when spending policy is disabled)
    were captured atomically with the same guarded ``queued`` write.
    """

    queued_at: datetime | None
    source_requirements_fingerprint: StrictStr | None
    authorized_task_budget_usd: StrictStr | None


class RepositoryIssueTaskCreatedRead(BaseModel):
    """The intake command outcome for one addressed issue projection (#378).

    ``task`` is the returned Task snapshot exactly as the intake service
    produced it — its ``status`` is the Task's actual returned status, never
    an assumed ``ready_to_plan``. ``created`` is the intake uniqueness
    evidence: ``true`` exactly for the invocation whose insert won the
    current-Task invariant, ``false`` when a concurrent duplicate converged
    onto the existing current Task. A fresh Task carries no requirements
    baseline and no budget (issue #233) — Owner Start, not intake, freezes
    them — and successful intake never enqueues, never begins planning.
    """

    task: TaskCommandSnapshotRead
    created: bool


class TaskStartedRead(BaseModel):
    """The Start command outcome: the committed queue entry plus wake evidence (#378).

    ``task`` is the post-write snapshot of the guarded ``queued`` write,
    even when FIFO admission advances the Task before this response is
    written — successful Start means enqueue, never FIFO admission, READY
    sessions, or completed planning. ``queue_wake_outcome`` is the actual
    post-commit queue-wake delivery evidence the owning service captured
    from the shared seam — ``accepted_for_delivery``, ``known_refused``, or
    ``uncertain`` — with ``null`` for an unwired seam. Delivery evidence is
    transport information only, kept deliberately separate from the
    committed queue entry: a refused or uncertain wake never fails or
    reverses the committed entry, and the durable ``queued`` facts own
    recovery.
    """

    task: TaskStartSnapshotRead
    queue_wake_outcome: StrictStr | None


# --- The ordinary OwnerGate decision commands (issue #379) -------------------


class OwnerGateDecisionCommand(BaseModel):
    """The ordinary gate decision command body (#379).

    The same single-input contract as the #378 Start command: the opaque
    ``state_token`` is the optimistic-concurrency token the guarded gate
    resolution is conditional on — a token that no longer matches the
    current Task is the typed stale outcome, never a second logical
    decision. The addressed Task, its exact current gate, and the caller's
    authenticated Profile carry everything else; a projected ``approve``
    action never grants authority by itself — every command independently
    revalidates the Profile, the current Task token, the exact current
    gate, and the applicable subject at the service boundary.
    """

    state_token: UUID


class OwnerGateDecisionSnapshotRead(TaskCommandSnapshotRead):
    """The gate decision commands' authoritative post-write Task facts (#379).

    The #378 command snapshot contract plus ``archived_at``: the merge
    decision's ``completed`` outcome kind confirms the GitHub-confirmed
    merge and commits the Task's terminalization, while the mandatory
    archival belongs to the F10 terminal finalizer strictly after that
    commitment — so a completed merge can truthfully coexist with a still
    ``null`` ``archived_at``. The snapshot is the service's returned facts
    verbatim and never claims archival the Task does not yet carry.
    """

    archived_at: datetime | None


class OwnerGateResolutionRead(BaseModel):
    """One OwnerGate's resolved or pending subject facts (#379).

    The gate identity, type, status, ``decided_at``, and the exact bound
    subject form — ``plan_revision_id`` for the plan subject,
    ``subject_head_sha`` for pre-PR head subjects, and
    ``task_pull_request_id`` plus the exact ``subject_head_sha`` for PR
    subjects — verbatim from the service's returned OwnerGate. Deliberately
    no action menu: menus are the G2b read surface's current-context
    projection, never a command re-derivation; subsequent gate state comes
    from the G2b reads.
    """

    id: UUID
    gate_type: StrictStr
    status: StrictStr
    decided_at: datetime | None
    plan_revision_id: UUID | None
    subject_head_sha: StrictStr | None
    task_pull_request_id: UUID | None


class OwnerGateApprovalRead(BaseModel):
    """The committed ordinary approval outcome of both pure-decision gates (#379).

    ``task`` is the authoritative post-write snapshot (``implementing``
    after the implementation authorization — the one irreversible planning
    boundary; still ``waiting_for_owner`` after the PR authorization — that
    approval is neither a transition nor a PR creation), ``owner_gate`` is
    the gate resolved APPROVED with truthful OWNER provenance, and
    ``continuation_outcome`` is the separately-typed post-commit F1
    delivery evidence for the workflow continuation the approval arranged —
    ``accepted_for_delivery``, ``known_refused``, or ``uncertain``.
    Delivery evidence is transport information only, kept deliberately
    separate from the committed decision: a refused or uncertain
    continuation never fails or reverses the committed approval, and the
    committed facts own recovery. Approval alone never certifies
    implementation completion or PR publication; subsequent state comes
    from the G2b reads.
    """

    task: OwnerGateDecisionSnapshotRead
    owner_gate: OwnerGateResolutionRead
    continuation_outcome: StrictStr | None


class ImplementationAuthorizationApprovalRead(OwnerGateApprovalRead):
    """The implementation-authorization approval outcome (E5 → E6, #379).

    The committed APPROVED resolution of the exact current
    IMPLEMENTATION_AUTHORIZATION gate bound to the review-cleared
    PlanRevision, the ``implementing`` Task with its rotated token, and the
    separately-typed E5→E6 continuation delivery evidence.
    """


class PrAuthorizationApprovalRead(OwnerGateApprovalRead):
    """The PR-authorization approval outcome (E7 → E8, #379).

    The committed APPROVED resolution of the exact current
    PR_AUTHORIZATION gate bound to the authorized committed head (before a
    canonical PR exists), the still-``waiting_for_owner`` Task under its
    rotated token, and the separately-typed E7→E8 PR-composition
    continuation delivery evidence.
    """


class MergeRetryAttemptRead(BaseModel):
    """The durably authorized automatic merge retry attempt (#379).

    The classified ``retry_authorized`` outcome's committed facts: the
    exact authorized attempt identity, its 1..3 ``ordinal`` within the
    episode's bounded budget, the persisted exponential-backoff ``due``
    instant, and the typed ``error_code`` classification. The due-time
    wakeup scheduling belongs to the F12 recovery machinery, never to this
    envelope: the Task stays ``waiting_for_owner`` with no current gate and
    the committed authorization row owns recovery.
    """

    id: UUID
    ordinal: StrictInt
    due: datetime
    error_code: StrictStr


class MergeDecisionOutcomeRead(BaseModel):
    """The classified outcome of one Owner merge decision command (#379).

    ``kind`` states which external effect actually occurred:
    ``completed`` — a GitHub-confirmed merge committed the Task's
    terminalization (the ``terminalization`` facts projected from the
    returned Task; the mandatory archival is the F10 terminal finalizer's
    follow-up, so a confirmed merge can be visible while
    ``task.archived_at`` is still ``null``); ``retry_gate_installed`` — a
    definitive no-merge outcome installed the NEW pending exact-head
    ``retry_gate`` (fresh Owner authorization required);
    ``retry_authorized`` — the merge send's own classified transient
    transport failure received ONE durably authorized bounded retry
    (``retry_attempt``); and ``blocked_stale_operation`` /
    ``blocked_uncertain`` — the expected-head mismatch or unconfirmable
    outcome applied the ``task_block`` blocking condition. Every classified
    kind is a committed durable disposition of a validly addressed command
    — not an HTTP error. ``pull_request`` is the authoritative reconciled
    canonical PR the classification is grounded in;
    ``merge_commit_sha`` carries GitHub's confirmed merge commit exactly
    where the merge send itself confirmed it — ``null`` when the merge was
    confirmed through authoritative reconciliation without that SHA, never
    synthesized. Hard precondition failures are typed HTTP errors, never a
    classified outcome; approval alone does not certify the merge (an
    uncertain outcome blocks) and subsequent state comes from the G2b
    reads.
    """

    kind: StrictStr
    task: OwnerGateDecisionSnapshotRead
    pull_request: GitHubTaskPullRequestRead
    terminalization: OpenOrcTaskTerminalizationRead | None
    retry_gate: OwnerGateResolutionRead | None
    task_block: OpenOrcTaskBlockSummaryRead | None
    merge_commit_sha: StrictStr | None
    retry_attempt: MergeRetryAttemptRead | None


# --- The Task-level Owner Abort and shared Cancel Task commands (issue #380) --


class TaskAbortCommand(BaseModel):
    """The Task-level Owner Abort command body (#380).

    The same single-input contract as the #378 Start command: the opaque
    ``state_token`` is the optimistic-concurrency token the guarded blocking
    composition is conditional on — a token that no longer matches the
    current Task is the typed stale outcome, never a second logical abort.
    No client-selected interruption targets exist: the application service
    derives the exact targets from the durable admitted-operation evidence
    inside its establish transaction, and no settlement evidence is an HTTP
    input — the addressed Task and the caller's authenticated Profile carry
    everything else.
    """

    state_token: UUID


class TaskAbortInterruptOutcomeRead(BaseModel):
    """One exact-target Interrupt's durable post-dispatch facts (#380).

    The safe classified outcome the shared E10b dispatch machinery recorded
    on the durable one-shot intent, verbatim: the intent identity, the
    targeted workflow role, the durable intent status (an acknowledgement
    stays ``unsettled`` — an SDK acknowledgement proves request acceptance,
    never quiescence), and the safe classified detail. These are separately
    typed follow-on-submission facts, kept deliberately distinct from the
    committed blocking composition.
    """

    task_interrupt_intent_id: UUID
    workflow_role: StrictStr
    status: StrictStr
    detail: StrictStr


class TaskAbortOutcomeRead(BaseModel):
    """The classified outcome of one Task-level Owner Abort command (#380).

    ``kind`` maps the service's classified outcome verbatim (the HTTP
    command's dispatch is ``interrupted``; the settlement kinds require
    positive D6b evidence that is never an HTTP input, so an
    already-``blocked`` Task aborts to the typed unresolved-interruption
    conflict instead). ``task`` is the authoritative post-write snapshot of
    the committed blocking composition — the Task blocked under its rotated
    token with the pending ``OWNER_ABORT`` gate installed — and
    ``task_block`` is the applied blocking condition's summary.
    ``interrupt_outcomes`` carries each exact target's separately-typed
    dispatch facts: the committed Abort and the acknowledged or uncertain
    follow-on submissions are distinct facts, and an acknowledged Interrupt
    is never settlement. Subsequent state comes from the G2b Task reads.
    """

    kind: StrictStr
    task: OwnerGateDecisionSnapshotRead
    task_block: OpenOrcTaskBlockSummaryRead | None
    interrupt_outcomes: list[TaskAbortInterruptOutcomeRead]


class TaskCancelCommand(BaseModel):
    """The shared Owner Cancel Task command body (#380).

    The same single-input contract as the Start and decision commands: the
    opaque ``state_token`` is the optimistic-concurrency token the guarded
    terminalization is conditional on. One shared command serves both the
    Task-level ``cancel_task`` offering and the current gate's menu — no
    gate identifier exists and no gate-scoped cancel route does: the service
    re-proves the exact current gate under the Task lock itself, and a
    projected ``cancel_task`` action never grants authority by itself.
    """

    state_token: UUID


class TaskClosedReviewLoopRead(BaseModel):
    """One ReviewLoop the Cancel command closed, as the service returned it (#380)."""

    id: UUID
    purpose: StrictStr
    status: StrictStr
    closed_at: datetime | None


class TaskCloseIntentRead(BaseModel):
    """The cancellation's one-shot PR-close obligation facts (#380).

    The durable one-shot close intent recorded atomically with the
    terminalization exactly when the canonical open/unmerged pull request
    may require closure, verbatim: its identity, the bound stable GitHub PR
    identity, and the forward-only status (``pending`` is the unresolved
    obligation; the terminal values are outcome records). The post-commit
    closure attempt's classified outcome lives on the durable intent, never
    synthesized here.
    """

    id: UUID
    github_pr_id: StrictInt
    github_pr_number: StrictInt
    status: StrictStr


class TaskCancellationOutcomeRead(BaseModel):
    """The classified outcome of one shared Owner Cancel Task command (#380).

    ``kind`` states which committed disposition actually occurred, verbatim:
    ``cancelled`` — the exact-Task terminalization committed (the
    ``terminalization`` facts projected from the returned Task; the mandatory
    archival is the F10 terminal finalizer's follow-up, so a committed
    terminalization can truthfully coexist with a still ``null``
    ``task.archived_at``); ``completed`` — the canonical PR was already
    authoritatively confirmed merged at the point of decision, so the command
    routed to the shared completion path instead of an Owner ``CANCELLED``
    claim. ``owner_gate`` is the exact current gate resolved with truthful
    OWNER provenance (``null`` when none existed), ``closed_loops`` the
    ReviewLoops closed as actually present, ``pull_request`` the
    authoritative canonical PR the classification is grounded in, and
    ``close_intent`` the pending one-shot close obligation when applicable.
    Cancel does not mean Interrupt, and committed terminalization never
    implies archived or capacity-released status before the final archival
    transaction; subsequent state comes from the G2b Task reads.
    """

    kind: StrictStr
    task: OwnerGateDecisionSnapshotRead
    terminalization: OpenOrcTaskTerminalizationRead | None
    owner_gate: OwnerGateResolutionRead | None
    closed_loops: list[TaskClosedReviewLoopRead]
    pull_request: GitHubTaskPullRequestRead | None
    close_intent: TaskCloseIntentRead | None
