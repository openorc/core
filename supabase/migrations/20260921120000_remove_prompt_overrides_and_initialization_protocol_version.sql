-- Corrective schema migration for the Phase 2 prerequisite (issue #100).
--
-- Phase 1 persistence carried two concepts superseded by the settled OpenOrc
-- interaction model:
--
-- - ``openorc.prompt_template_overrides`` — the Workspace prompt-template
--   override abstraction (``template_key`` / ``base_template_version`` /
--   ``instruction_text`` instruction slots) and its
--   ``prompt_override_changed`` workflow-event vocabulary value. OpenOrc no
--   longer has Workspace-overridable command-prompt templates or
--   prompt/template version audit reconstruction. No replacement persistence
--   is introduced here; Phase 2 owns the new first-class Workspace
--   configuration field.
-- - ``task_agent_sessions.initialization_protocol_version`` — a separately
--   persisted initialization-protocol version column. Canonical OpenOrc-owned
--   initialization contracts/content belong to the runtime-neutral
--   protocol/interaction layer, not to persistence.
--
-- Merged migrations are append-only, so this is an additive corrective
-- migration that deliberately:
--
-- 1. Drops the ``openorc.prompt_template_overrides`` table. Its Workspace
--    foreign key (re-declared as ON DELETE CASCADE by the deletion-ownership
--    migration) and every other table-owned constraint/index disappear with
--    the table.
--
-- 2. Handles existing ``prompt_override_changed`` workflow events explicitly.
--    OpenOrc is pre-v1 and the prompt-override abstraction is intentionally
--    removed rather than preserved (no migration compatibility for persisted
--    overrides), so this migration deletes any obsolete audit rows of the
--    removed event type before installing the narrowed vocabulary CHECK.
--    This is deterministic on any environment state — including a zero-row
--    delete — and is a schema-management action, not an application rewrite
--    path; the persistence surface itself stays INSERT-only.
--
-- 3. Replaces the workflow_events ``event_type`` CHECK with exactly the
--    surviving vocabulary. The committed #26 migration defined the CHECK
--    inline on the column, so Postgres assigned the deterministic
--    auto-generated name ``workflow_events_event_type_check``; dropping by
--    that exact name is deterministic. No dynamic catalog discovery and no
--    substring constraint matching are used anywhere in this migration.
--
-- 4. Drops ``task_agent_sessions.initialization_protocol_version``.
--    PostgreSQL automatically drops every table constraint involving a
--    dropped column, which deterministically removes both the column-level
--    nonblank CHECK and the table-level four-fact initialization-coherence
--    CHECK. The explicitly named replacement CHECK below then enforces the
--    settled three-fact initialization coherence:
--
--        external_session_id, initialized_at, effective_config_snapshot
--
--    all NULL before initialization succeeds and all non-NULL once it does.
--    Lifecycle vocabulary (CONNECTING/READY/LOST/ENDED), the no-replacement
--    rule, Connection-scoped capacity accounting, and exact-session
--    continuity are unchanged. ``effective_config_snapshot`` remains the
--    non-secret caller-assembled effective runtime/session configuration
--    snapshot only — it must not become a prompt/schema/guidance snapshot.

-- 1) The obsolete Workspace prompt-template override table. Its foreign
--    key to openorc.workspaces and its unique slot constraint drop with it.
drop table openorc.prompt_template_overrides;

-- 2) The obsolete prompt-override event vocabulary. Obsolete audit rows of
--    the removed event type are deliberately removed first (pre-v1; the
--    abstraction is removed, not preserved), then the locked CHECK is
--    narrowed to exactly the surviving Python WorkflowEventType vocabulary.
delete from openorc.workflow_events where event_type = 'prompt_override_changed';

alter table openorc.workflow_events
    drop constraint workflow_events_event_type_check;
alter table openorc.workflow_events
    add constraint workflow_events_event_type_check check (event_type in (
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
    ));

-- 3) The initialization-protocol version column. Dropping the column
--    automatically removes both dependent CHECKs; the explicitly named
--    replacement enforces the settled three-fact initialization coherence.
alter table openorc.task_agent_sessions
    drop column initialization_protocol_version;

alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_initialization_coherence check (
        (
            lifecycle_status = 'connecting'
            and external_session_id is null
            and initialized_at is null
            and effective_config_snapshot is null
        )
        or (
            lifecycle_status in ('ready', 'lost')
            and external_session_id is not null
            and initialized_at is not null
            and effective_config_snapshot is not null
        )
        or (
            lifecycle_status = 'ended'
            and (
                (
                    external_session_id is null
                    and initialized_at is null
                    and effective_config_snapshot is null
                )
                or (
                    external_session_id is not null
                    and initialized_at is not null
                    and effective_config_snapshot is not null
                )
            )
        )
    );

