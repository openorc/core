-- OpenOrc human-authority, execution, runtime-request, and block
-- persistence primitives (Phase 1, issue #24).
--
-- Establishes the durable control records that later workflow services
-- build on:
--
--   Workspace -> Project -> Repository -> Task -> OwnerGate
--                                            Execution
--                                            RuntimeRequest
--                                            TaskBlock
--
-- Durable properties carried by this migration:
--
-- - OwnerGates are historical human-authority decision records: the exact
--   decision (approved, rejected, cancelled) is bound to the exact subject
--   it governs. Resolved gates are immutable and never recycled: a later
--   gate of the same type is a new row, and no rewrite path exists for a
--   stamped ``decided_at``. A pending gate that loses currency (superseded
--   or never installed) stays pending as history; resolution applies only
--   to the Task's current gate.
-- - v1 OwnerGate types are exactly IMPLEMENTATION_AUTHORIZATION,
--   PR_AUTHORIZATION, MERGE_DECISION, and REVIEW_RESOLUTION. Statuses are
--   exactly PENDING, APPROVED, REJECTED, and CANCELLED.
-- - Each gate binds to the exact authority subject it governs through the
--   per-type subject coherence CHECK: IMPLEMENTATION_AUTHORIZATION carries
--   the exact review-cleared PlanRevision and no head SHA;
--   PR_AUTHORIZATION and MERGE_DECISION carry the exact head SHA and no
--   PlanRevision; REVIEW_RESOLUTION carries exactly one subject — the
--   exhausted planning PlanRevision or the PR/head review subject. The
--   PR/TaskPullRequest binding for PR/merge subjects arrives with #25 on
--   this same table.
-- - REVIEW_RESOLUTION approval is an explicit Owner override of an
--   unresolved Reviewer objection: a separate durable fact on the gate. It
--   never rewrites the historical Reviewer result; plan-subject approval
--   leads to a new IMPLEMENTATION_AUTHORIZATION and PR/head-subject
--   approval to a new MERGE_DECISION (orchestration concerns).
-- - The Task's ``current_owner_gate_id`` pointer is validated against the
--   same Task/Workspace by the composite foreign key below. Normal
--   lifecycle: at most one pending gate is current; resolving it clears
--   the pointer and rotates the Task ``state_token`` atomically; only then
--   may another pending gate be installed. No gate data is duplicated on
--   the Task (one fact, one home), and there is deliberately no singular
--   current-Execution pointer.
-- - Executions are historical attempt records inside the Task's Producer
--   session. There is no single-active-execution constraint and no
--   Execution-owned branch: the canonical feature branch remains a
--   Task-level exclusive ownership fact, and Executions observe repository
--   state without becoming a competing branch owner. There is deliberately
--   no Git SHA field of any kind: runtime sandboxes are runtime-private
--   execution state and local Git HEAD is not OpenOrc's canonical
--   engineering truth; GitHub owns durable committed repository truth once
--   the Producer pushes, reconciled later through GitHub (webhook payloads
--   are notifications, not a second truth source).
-- - The Execution lifecycle vocabulary is exactly queued, running, paused,
--   paused_for_approval, succeeded, failed_transient, failed_final, and
--   cancelled.
-- - RuntimeRequests represent only scoped Producer/runtime-originated
--   consequential-action approvals in v1: the semantic kind is exactly
--   action_approval — no generic Owner-decision, question, chat, or
--   free-form request kind. A request is tied to the exact Task, Producer
--   TaskAgentSession, and external approval identifier that created it;
--   Reviewer sessions do not participate. The Owner response is a typed
--   correlated control (approved or rejected) to that exact pending
--   request, never a general Owner-to-Producer communication channel. The
--   correlation identity ``(producer_session_id, external_approval_id)``
--   is unique across all history: one exact external request is one row
--   forever, and resolution never frees the external identity for a second
--   historical row in the same Producer session.
-- - RuntimeRequest lifecycle is exactly pending, resolved, expired, and
--   cancelled; terminal states are immutable historical records.
-- - TaskBlocks persist the blocking reason plus reason/recovery context.
--   The reason vocabulary is exactly the twelve settled values; ReviewLoop
--   iteration-limit exhaustion is deliberately absent — it is represented
--   through the Task's ``waiting_for_owner`` status plus a
--   REVIEW_RESOLUTION OwnerGate, never as a TaskBlock reason. Recovery is
--   reason/context-specific: no universal blocked-to-next-state transition
--   is encoded here. A resolved block remains historical and retains its
--   reason and context.
--
-- No privileges are granted here: the ``openorc`` schema lockout from the
-- create_openorc_schema migration carries isolation, and runtime access is
-- backend-only.

-- Hook for Workspace-owned children of a TaskAgentSession (the Execution
-- and RuntimeRequest attempt/request records): lets their direct Workspace
-- scope be constrained to agree with the binding's Task/Workspace scope,
-- exactly like the tasks hook added by the session migration.
alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_id_task_id_workspace_id_uniq
    unique (id, task_id, workspace_id);

-- OwnerGates: one durable human-authority decision record. Deliberately no
-- ``updated_at``: the row's only mutation is the resolution, and
-- ``decided_at`` is the semantic stamp for exactly that mutation.
create table openorc.owner_gates (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- v1 gate types are exactly the four settled human-authority decisions.
    gate_type text not null check (gate_type in (
        'implementation_authorization', 'pr_authorization', 'merge_decision',
        'review_resolution'
    )),
    -- Lifecycle: pending until an authoritative resolution stamps the
    -- outcome; approved/rejected/cancelled are terminal and immutable once
    -- stamped. A superseded or never-installed pending gate remains
    -- pending history: resolution applies only to the Task's current gate.
    status text not null check (status in (
        'pending', 'approved', 'rejected', 'cancelled'
    )),
    -- Exact subject binding. Exactly one subject form per gate type (see
    -- the coherence CHECK below): the exact review-cleared PlanRevision
    -- for implementation authorization; the exact latest committed
    -- Producer head SHA presented to the Owner for PR authorization and
    -- for the reviewed/Owner-overridden merge subject; and for
    -- REVIEW_RESOLUTION either the exhausted planning PlanRevision or the
    -- PR/head review subject.
    plan_revision_id uuid,
    subject_head_sha text check (subject_head_sha is null or subject_head_sha ~ '\S'),
    -- Semantic resolution timestamp, stamped atomically with the terminal
    -- status (non-NULL exactly when the status is not pending).
    decided_at timestamptz,
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- The plan-revision subject belongs to the same Task and Workspace as
    -- the gate: the exact-subject rule is durable.
    foreign key (plan_revision_id, task_id, workspace_id)
        references openorc.plan_revisions (id, task_id, workspace_id),

    -- Hook for the Task's same-Task/Workspace current-gate pointer
    -- (mirrors the plan_revisions, review_loops, and task_agent_sessions
    -- hooks).
    unique (id, task_id, workspace_id),

    -- Per-type exact-subject coherence: the gate binds to the exact
    -- authority subject it governs, and only that subject.
    check (
        (
            gate_type = 'implementation_authorization'
            and plan_revision_id is not null
            and subject_head_sha is null
        ) or (
            gate_type = 'pr_authorization'
            and plan_revision_id is null
            and subject_head_sha is not null
        ) or (
            gate_type = 'merge_decision'
            and plan_revision_id is null
            and subject_head_sha is not null
        ) or (
            gate_type = 'review_resolution'
            and (
                (plan_revision_id is not null and subject_head_sha is null)
                or (plan_revision_id is null and subject_head_sha is not null)
            )
        )
    ),

    -- ``decided_at`` is non-NULL exactly when the status is terminal.
    check (
        (status = 'pending' and decided_at is null)
        or (status in ('approved', 'rejected', 'cancelled') and decided_at is not null)
    )
);

-- Pending-gate lookup for a Task (the one gate a Task may currently be
-- waiting on; serves the install/resolve pointer flows).
create index owner_gates_task_pending_idx
    on openorc.owner_gates (task_id)
    where status = 'pending';

-- Executions: one historical attempt record inside the Task's Producer
-- session. Deliberately no Git SHA field of any kind (see the header) and
-- no canonical-branch ownership: the canonical feature branch is a
-- Task-level fact, and an Execution never becomes a competing branch owner.
create table openorc.executions (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- The attempt record lives inside the Task's Producer session. The
    -- composite foreign key keeps the session's Task/Workspace scope in
    -- agreement; the producer-role requirement is enforced by the creation
    -- repository path (a Reviewer session can never anchor an Execution).
    producer_session_id uuid not null,
    -- Per-Task attempt ordering. Identity and history lookup only: it
    -- deliberately enforces no single-active-execution exclusivity, and
    -- parallel active Executions remain possible.
    execution_number integer not null check (execution_number > 0),
    status text not null check (status in (
        'queued', 'running', 'paused', 'paused_for_approval',
        'succeeded', 'failed_transient', 'failed_final', 'cancelled'
    )),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- The Producer session belongs to the same Task and Workspace as the
    -- Execution.
    foreign key (producer_session_id, task_id, workspace_id)
        references openorc.task_agent_sessions (id, task_id, workspace_id),

    -- One attempt number per Task: a retry/recovery/continuation is a
    -- fresh row with the next number, never a rewrite of a finalized
    -- attempt. Also serves the attempt-history lookup.
    unique (task_id, execution_number)
);

-- Active-execution lookup for a Task. Deliberately non-exclusive: several
-- rows may match, and no constraint assumes only one active Execution can
-- exist.
create index executions_task_active_idx
    on openorc.executions (task_id)
    where status in ('queued', 'running', 'paused', 'paused_for_approval');

-- RuntimeRequests: one exact external approval request from the Producer
-- runtime. Deliberately no ``updated_at``: terminal transitions are stamped
-- by the semantic ``closed_at``.
create table openorc.runtime_requests (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- The exact Producer session that created the request. Reviewer
    -- sessions do not participate in runtime approvals; the creation
    -- repository path enforces the producer role.
    producer_session_id uuid not null,
    -- v1 semantic kind is exactly action_approval: a scoped
    -- consequential-action approval. No generic Owner-decision, question,
    -- chat, or free-form request kind exists.
    kind text not null check (kind = 'action_approval'),
    -- The exact external approval/action identifier that created this
    -- request. Unique per Producer session across all history: one exact
    -- external request is one RuntimeRequest row forever.
    external_approval_id text not null check (external_approval_id ~ '\S'),
    -- Lifecycle: pending until a terminal transition; resolved, expired,
    -- and cancelled are terminal and immutable once stamped.
    status text not null check (status in (
        'pending', 'resolved', 'expired', 'cancelled'
    )),
    -- The typed correlated Owner control, set exactly when resolved.
    resolution text check (resolution in ('approved', 'rejected')),
    -- Semantic terminal stamp, non-NULL exactly when the status is
    -- terminal (resolved, expired, or cancelled). Generic on purpose: an
    -- expiry or cancellation closes the request without an Owner decision.
    closed_at timestamptz,
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- The Producer session belongs to the same Task and Workspace as the
    -- request.
    foreign key (producer_session_id, task_id, workspace_id)
        references openorc.task_agent_sessions (id, task_id, workspace_id),

    -- The correlation identity is unique across all history, not merely
    -- while pending: one exact external approval request is one row
    -- forever, and resolution never makes the external identity reusable
    -- for a second historical row in the same Producer session.
    unique (producer_session_id, external_approval_id),

    -- Terminal coherence: a resolved request carries its typed resolution
    -- and its stamp; expired/cancelled carry the terminal stamp without a
    -- typed control; pending carries neither. A terminal request is a
    -- historical record with no rewrite path.
    check (
        (
            status = 'pending'
            and resolution is null
            and closed_at is null
        )
        or (
            status = 'resolved'
            and resolution is not null
            and closed_at is not null
        )
        or (
            status in ('expired', 'cancelled')
            and resolution is null
            and closed_at is not null
        )
    )
);

-- Pending-request lookup for a Task (the Producer is waiting on these).
create index runtime_requests_task_pending_idx
    on openorc.runtime_requests (task_id)
    where status = 'pending';

-- TaskBlocks: one blocking condition with its reason and recovery context.
-- Deliberately no ``updated_at``: the row's only mutation is resolution,
-- and ``resolved_at`` is the semantic stamp for exactly that.
create table openorc.task_blocks (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- The settled v1 reason vocabulary. ReviewLoop iteration-limit
    -- exhaustion is deliberately absent: it is represented through the
    -- Task's ``waiting_for_owner`` status plus a REVIEW_RESOLUTION
    -- OwnerGate, never as a TaskBlock reason.
    reason text not null check (reason in (
        'review_failure', 'runtime_failure', 'agent_session_lost',
        'connection_unavailable', 'invalid_credentials',
        'external_operation_uncertain', 'stale_operation',
        'github_source_changed', 'runtime_request_rejected',
        'pr_closed_unmerged', 'owner_action_required', 'unknown'
    )),
    -- Reason/recovery context (canonical JSON object): the
    -- recovery/continuation facts specific to this block, persisted for
    -- the block's lifetime — historical evidence that survives resolution.
    -- NOT NULL by design: a block always carries its recovery context, and
    -- an empty JSON object is the valid empty context.
    context jsonb not null check (jsonb_typeof(context) = 'object'),
    -- Semantic resolution stamp: NULL while the block is current; stamped
    -- exactly once when resolved. A resolved block remains historical and
    -- retains its reason and context.
    resolved_at timestamptz,
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id)
);

-- Current/active block lookup for a Task.
create index task_blocks_task_current_idx
    on openorc.task_blocks (task_id)
    where resolved_at is null;

-- Same-Task/Workspace current-gate pointer consistency: a non-NULL
-- tasks.current_owner_gate_id must reference an OwnerGate whose
-- (id, task_id, workspace_id) matches the pointing Task's identity and
-- Workspace. NULL passes; cross-Task or cross-Workspace corruption is
-- rejected durably. The pointer identifies the gate the Task is waiting on
-- without duplicating its type, subject, or outcome (one fact, one home).
alter table openorc.tasks
    add constraint tasks_current_owner_gate_fk
    foreign key (current_owner_gate_id, id, workspace_id)
    references openorc.owner_gates (id, task_id, workspace_id);
