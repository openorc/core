-- Level 2 operational-failure disposition durable state (issue #251,
-- Phase 2F F4). Additive shared foundation for the typed failure/disposition
-- triage service; the Level 1 handoff (F5, issue #253), the direct-block
-- conversion (F6, issue #252), and the due-at retry wakeups (F11, issue
-- #257) consume it later.
--
-- Durable properties carried by this migration:
--
-- - openorc.operational_failure_episodes: ONE failure episode per original
--   durable logical operation/attempt identity. The unique
--   (original_attempt_kind, original_attempt_id) pair roots the episode at
--   the exact existing durable identity (an F2 runtime-send claim, the E1b
--   planning-bootstrap claim token, the E3a reserved ReviewIteration, or the
--   E6/E9 Execution row) — never a random new episode minted per error
--   report, and never a Task state token or mutable phase.
-- - openorc.operational_failure_attempts: the at-most-three automatic
--   additional attempts authorized within one episode, each with its own
--   UUID, ordinal 1..3, source failed-attempt reference, stable error code,
--   and persisted UTC ``not_before`` instant. UNIQUE (episode_id, ordinal)
--   and UNIQUE (source_kind, source_attempt_id) are the durable backstops:
--   one ordinal per episode and at most one successor per failed attempt,
--   independent of any Task state token, process restart, or execution
--   mode. Authorized rows are preserved, never rewritten, for replay
--   prevention and later F5/F11 consumption.
-- - ``resulting_execution_id``: the narrow, durable association between a
--   consumed retry authorization and the exact Execution its execution
--   actually created (set once by the executing seam; a subsequent failure
--   report about that authorization must address exactly this Execution).
--   The composite foreign key to the executions identity key proves
--   same-Task/Workspace scope; the within-Task-aggregate linkage edge is
--   NO ACTION DEFERRABLE INITIALLY DEFERRED, never a cascade.
-- - ``escalated_task_block_id``: the episode's terminal disposition — the
--   exact #258 F3 TaskBlock installed by the escalation, set exactly once
--   and never cleared. The composite foreign key to the F3 identity key
--   ``task_blocks (id, task_id, workspace_id)`` proves same-Task/Workspace
--   scope; the within-Task-aggregate linkage edge is NO ACTION DEFERRABLE
--   INITIALLY DEFERRED like the gate's TaskBlock subject, never a cascade.
-- - Rows carry only safe identity/classification facts: closed CHECK
--   vocabularies and a canonical JSONB subject of stable identifiers. No
--   prompts, exception text or stacks, provider messages, credentials,
--   transcripts, or raw diagnostic payloads.
-- - No new privilege grants: the openorc schema lockout established by the
--   create_openorc_schema migration carries isolation, and runtime access
--   remains backend-only.
--
-- Deletion ownership follows the established classification: the episodes
-- are Task-owned operational rows that cascade from their Task and
-- Workspace; the attempts are owned by their episode and cascade through
-- it; the escalation TaskBlock and resulting-Execution references are
-- within-aggregate linkage that must never cascade.

create table openorc.operational_failure_episodes (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    operation_family text not null check (operation_family in (
        'planning_bootstrap_prepare', 'planning_bootstrap_transition',
        'initial_plan_dispatch', 'planning_revise_dispatch',
        'reviewer_review', 'implementation_execution',
        'pr_remediation_execution', 'pr_compose_dispatch'
    )),
    original_attempt_kind text not null check (original_attempt_kind in (
        'runtime_send_claim', 'planning_bootstrap_claim', 'review_iteration',
        'execution', 'owner_control_intent'
    )),
    original_attempt_id uuid not null,
    subject jsonb not null,
    escalated_task_block_id uuid,
    created_at timestamptz not null default now(),
    -- One episode per original durable logical attempt identity: the same
    -- failure report can never mint a second episode, and a report whose
    -- original identity differs is a distinct logical operation.
    constraint operational_failure_episodes_original_attempt_uniq
        unique (original_attempt_kind, original_attempt_id),
    -- The identity hook the attempts' scope-coherent composite foreign key
    -- references (the same pattern as task_blocks (id, task_id,
    -- workspace_id)).
    constraint operational_failure_episodes_id_task_id_workspace_id_uniq
        unique (id, task_id, workspace_id)
);

create table openorc.operational_failure_attempts (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    episode_id uuid not null,
    ordinal smallint not null check (ordinal between 1 and 3),
    source_kind text not null check (source_kind in ('original', 'prior_authorized')),
    source_attempt_id uuid not null,
    error_code text not null check (error_code in (
        'runtime_unavailable', 'runtime_timeout', 'runtime_delivery_uncertain',
        'runtime_transport', 'runtime_protocol', 'agent_session_lost',
        'runtime_configuration_rejected', 'external_operation_failed',
        'external_operation_uncertain', 'external_operation_rejected', 'unknown'
    )),
    not_before timestamptz not null,
    -- The exact Execution the executing seam created from this consumed
    -- authorization: NULL while the authorization has not been executed,
    -- and bound exactly once afterwards. A later failure report about this
    -- authorization must address exactly this Execution.
    resulting_execution_id uuid,
    created_at timestamptz not null default now(),
    -- The episode budget: exactly one authorized attempt per ordinal.
    constraint operational_failure_attempts_episode_ordinal_uniq
        unique (episode_id, ordinal),
    -- At most one successor per failed attempt, across the whole history:
    -- the durable replay-prevention backstop that makes a repeated failure
    -- report a rejected no-op instead of a second authorization.
    constraint operational_failure_attempts_source_attempt_uniq
        unique (source_kind, source_attempt_id)
);

-- The identity hook the attempts' resulting-Execution linkage references
-- (the same additive identity-key pattern the F3 migration used for
-- task_blocks).
alter table openorc.executions
    add constraint executions_id_task_id_workspace_id_uniq
    unique (id, task_id, workspace_id);

-- The episodes are Task-owned operational rows that cascade from their
-- Task and Workspace.
alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id)
    references openorc.tasks (id, workspace_id)
    on delete cascade;

-- The attempts are owned by their episode and cascade with it; the
-- composite key keeps the attempt in the episode's Task/Workspace scope.
alter table openorc.operational_failure_attempts
    add constraint operational_failure_attempts_episode_id_task_id_workspace_id_fkey
    foreign key (episode_id, task_id, workspace_id)
    references openorc.operational_failure_episodes (id, task_id, workspace_id)
    on delete cascade;

-- The escalation TaskBlock reference is within-Task-aggregate linkage:
-- identity and scope only, never an ownership edge, so the Workspace
-- deletion sequence is never order-sensitive.
alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_escalated_task_block_fk
    foreign key (escalated_task_block_id, task_id, workspace_id)
    references openorc.task_blocks (id, task_id, workspace_id)
    deferrable initially deferred;

-- A consumed authorization's resulting Execution is the same-Task/Workspace
-- effect evidence the authorization was executed as: linkage, never
-- ownership, and never a dangling association.
alter table openorc.operational_failure_attempts
    add constraint operational_failure_attempts_resulting_execution_fk
    foreign key (resulting_execution_id, task_id, workspace_id)
    references openorc.executions (id, task_id, workspace_id)
    deferrable initially deferred;

create index operational_failure_episodes_task_id_idx
    on openorc.operational_failure_episodes (task_id);

-- Each TaskBlock terminal-disposes at most one failure episode: the exact
-- F3 block is the episode's one terminal disposition, never shared.
create unique index operational_failure_episodes_escalated_task_block_uniq
    on openorc.operational_failure_episodes (escalated_task_block_id)
    where escalated_task_block_id is not null;

