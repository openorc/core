-- WorkflowEvent coordination vocabulary extension (Phase 2A, issue #56).
--
-- Services now coordinate consequential state mutations with their durable
-- WorkflowEvent facts (one composed transaction: canonical mutation + event
-- insert). The first coordinated Workspace-level fact is the deliberate
-- Workspace configuration change, for which the locked v1 event vocabulary
-- had no semantic event: issue #53's review_iteration_limit and guidance
-- settings previously had no event mapping.
--
-- Extends the workflow_events ``event_type`` CHECK with exactly one
-- demonstrated new value, ``workspace_configuration_changed``, mirroring the
-- new ``WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED`` member in
-- ``openorc.domain.events``. No event rows exist for the new value yet, so
-- the extension is purely additive: no data migration, no deletes, no
-- backfill.
--
-- The CHECK was replaced (not created) by the issue #100 corrective
-- migration, which relied on Postgres assigning the deterministic
-- auto-generated name ``workflow_events_event_type_check`` to the inline
-- column constraint; dropping by that exact name is deterministic. No
-- dynamic catalog discovery and no substring constraint matching are used.
--
-- No privileges are changed here: the openorc schema lockout established by
-- create_openorc_schema carries isolation, and runtime access is
-- backend-only.

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
        'task_completed',
        'workspace_configuration_changed'
    ));

