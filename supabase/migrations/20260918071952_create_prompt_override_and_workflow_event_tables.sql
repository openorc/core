-- OpenOrc prompt override and workflow event persistence (Phase 1, issue #26).
--
-- Establishes the Workspace-owned prompt override configuration and the
-- append-oriented durable WorkflowEvent audit stream that later workflow
-- services build on:
--
--   Workspace -> PromptTemplateOverride            (override slot, mutable config)
--   Workspace -> WorkflowEvent (optional Task)     (append-only audit history)
--
-- Durable properties carried by this migration:
--
-- - PromptTemplateOverride stores Workspace overrides of built-in prompt
--   slots' configurable instruction text only. Built-in prompt defaults
--   remain application code/versioned defaults and are never materialized
--   into this table: an override row exists exactly while a Workspace has
--   overridden the slot, and row absence means the currently shipped
--   built-in default applies. A reset is actual row deletion — never a
--   tombstone, never a stored default copy.
-- - An override may change configurable instruction text but can never
--   redefine protocol, authority, session, or workflow semantics: the row
--   carries an instruction body keyed by a template slot, never a complete
--   prompt/protocol template, so the instruction-only boundary holds by
--   construction. ``base_template_version`` preserves the built-in version
--   the override was authored against — enough template/version identity
--   for later audit reconstruction without copying built-in prompts into
--   the database.
-- - ``(workspace_id, template_key)`` is the override slot identity: one
--   current override per Workspace per template key. A change updates the
--   slot in place (identity and creation instant preserved, updated_at
--   advanced); the constraint doubles as the slot lookup.
-- - WorkflowEvent is append-oriented durable audit/history for
--   consequential OpenOrc facts. It is not event sourcing and not a second
--   source of workflow state: canonical current Task state keeps its own
--   durable homes (tasks, plan revisions, reviews, gates, executions,
--   runtime requests, blocks, pull requests) and events must never become
--   the mechanism used to reconstruct it. Event rows are immutable after
--   creation: the table carries no updated_at and no database trigger is
--   used — the append-only contract is owned by the persistence module's
--   INSERT-only public surface.
-- - WorkflowEvent is not runtime telemetry storage: high-frequency
--   model/tool/activity telemetry belongs to the later runtime-telemetry/
--   SSE boundary. Webhook raw payloads are never stored here either.
-- - The event actor vocabulary is exactly owner, openorc, producer,
--   reviewer, runtime, github — the logical actor type plus optional
--   opaque actor identity where meaningful (its kind varies by actor).
--   OWNER is the human-authority actor terminology; HUMAN is not an
--   OpenOrc actor.
-- - Every event carries direct Workspace scope. Task scope is optional: a
--   Workspace-level event legitimately has no Task, and a Task-related
--   event's scope must agree with the Task's Workspace through the
--   existing tasks composite hook — a Task-related event can never carry
--   disagreeing scope.
-- - One primary generic subject reference: subject_type (deliberately open
--   validated text, not a closed enum) plus subject_id, present as a pair
--   or absent as a pair, with no foreign key — a new auditable subject
--   kind must not require a schema migration. The constrained JSONB event
--   context is subordinate metadata only and must not become the canonical
--   home for modeled domain state.
-- - Query-driven indexes only, matching the demonstrated v1 read paths:
--   Workspace activity, Task history, event type/time, actor, and subject
--   lookup. No speculative indexes.
-- - No privileges are granted to browser-facing roles: the schema-level
--   lockout established by the create_openorc_schema migration carries
--   isolation.

create table openorc.prompt_template_overrides (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    -- Identity of the built-in application-code prompt slot being
    -- overridden. Stable across built-in content/version changes.
    template_key text not null check (template_key ~ '\S'),
    -- The built-in template version this override was authored against:
    -- template/version identity for later audit reconstruction, without
    -- copying built-in prompt content into the database.
    base_template_version text not null check (base_template_version ~ '\S'),
    -- The override body: configurable instruction text only — never a
    -- complete prompt/protocol template and never protocol/authority/
    -- session/workflow semantics.
    instruction_text text not null check (instruction_text ~ '\S'),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- The override slot: one current override per Workspace per template
    -- key (doubles as the slot lookup). Row absence means the currently
    -- shipped built-in default applies.
    unique (workspace_id, template_key)
);

create table openorc.workflow_events (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    -- Direct Task scope for Task-related events; NULL for Workspace-level
    -- events (a Workspace-level event legitimately has no Task). The
    -- composite foreign key below keeps the scopes in agreement.
    task_id uuid,
    -- The locked v1 workflow-event vocabulary: meaningful workflow/audit
    -- facts, not CRUD symmetry over persistence mutations. Extended
    -- deliberately by migration when a new durable audit semantic exists.
    event_type text not null check (event_type in (
        'task_created',
        'agent_session_created',
        'agent_session_bound',
        'agent_session_lost',
        'planning_started',
        'plan_revision_created',
        'plan_reviewed',
        'review_limit_reached',
        'plan_revision_revised',
        'plan_ready',
        'implementation_authorized',
        'execution_started',
        'execution_completed',
        'execution_failed',
        'runtime_request_created',
        'runtime_request_resolved',
        'runtime_request_cancelled',
        'retry_started',
        'task_blocked',
        'owner_gate_created',
        'owner_gate_resolved',
        'owner_reviewer_discussion_message',
        'prompt_override_changed',
        'pr_created',
        'pr_reviewed',
        'task_relationship_synced',
        'task_dependency_synced',
        'pr_head_changed',
        'merge_requested',
        'merge_rejected_by_github',
        'pr_merged',
        'task_cancelled',
        'task_completed'
    )),
    -- Logical actor type. OWNER is the human-authority actor; HUMAN is
    -- not an OpenOrc actor.
    actor_type text not null check (actor_type in (
        'owner', 'openorc', 'producer', 'reviewer', 'runtime', 'github'
    )),
    -- Optional opaque logical actor identity where meaningful. Its kind
    -- varies by actor type, and logical actor identity is never moved
    -- into the context payload.
    actor_id text check (actor_id is null or actor_id ~ '\S'),
    -- One primary generic subject reference: open validated text plus a
    -- UUID, not a closed enum and not foreign-key bound, so a new
    -- auditable subject kind needs no schema migration. Present as a pair
    -- or absent as a pair (the CHECK below).
    subject_type text check (subject_type is null or subject_type ~ '\S'),
    subject_id uuid,
    -- Subordinate event-specific context: a canonical JSON object. It is
    -- never a shadow copy of canonical domain records and never the
    -- canonical home for modeled domain state.
    context jsonb not null default '{}'::jsonb
        check (jsonb_typeof(context) = 'object'),
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace: a
    -- Task-related event can never carry disagreeing scope.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- The generic subject reference is a pair: both present or both absent.
    check (
        (subject_type is null and subject_id is null)
        or (subject_type is not null and subject_id is not null)
    )
);

-- Recent Workspace activity (the Workspace audit feed).
create index workflow_events_workspace_activity_idx
    on openorc.workflow_events (workspace_id, created_at);

-- Task history: all events of one Task (newest first on read). Workspace-
-- level events have no Task and are outside this index.
create index workflow_events_task_history_idx
    on openorc.workflow_events (task_id, created_at)
    where task_id is not null;

-- Event type over time (filtered activity streams).
create index workflow_events_type_time_idx
    on openorc.workflow_events (event_type, created_at);

-- Actor-filtered Workspace activity.
create index workflow_events_workspace_actor_idx
    on openorc.workflow_events (workspace_id, actor_type, created_at);

-- Generic subject lookup (what happened around one auditable subject).
create index workflow_events_subject_idx
    on openorc.workflow_events (subject_type, subject_id)
    where subject_type is not null;

