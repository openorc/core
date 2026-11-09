-- The one-shot session-resume authorization (F8g, issue #292).
--
-- ``openorc.task_resume_intents`` is the durable pre-dispatch guard of one
-- Owner Resume decision for the resolved ``OWNER_ABORT`` blocking episode.
-- The Owner action commits it atomically with the gate's APPROVED
-- resolution, the TaskBlock's resolution, the guarded blocked→restored
-- status transition, and the truthful OWNER audit: the record is the
-- exactly-once authorization of the qualified same-session continuation
-- (issue #300) across crashes, concurrent commands, and worker delays.
-- One row per exact admitted Task/role session carries the binding's
-- stable internal identity, its opaque external session identity, the
-- resolved episode's TaskBlock (the lineage the cancellation accounting
-- reads), the accountable Owner Profile, an immutable ``routing`` mapping
-- of safe machine facts (the original workflow operation the resume
-- continues, its expected formal response family, the exact durable
-- subject reference, the restored Task status, and the authorization
-- token), and a forward-only status: ``authorized`` is the committed-but-
-- never-dispatched authorization, ``dispatched`` is the claimed in-flight
-- resumed turn, and ``settled``/``non_delivered``/``interrupted``/
-- ``superseded`` are the terminal outcome records. The partial unique
-- index admits at most one non-terminal record per session binding, so a
-- duplicate authorization can never blind-duplicate a pending or in-flight
-- dispatch. Outcomes are small canonical JSON objects of safe machine
-- facts — never tokens beyond the internal authorization token identity,
-- transcript content, prompts, or raw provider errors.
--
-- Deletion ownership follows the established classification (issue #57):
-- the table is a Task-owned operational row that cascades from its Task
-- and Workspace; the interrupted session binding, the resolved episode's
-- TaskBlock, and the accountable Profile are the subjects the record is
-- meaningless without, so those edges cascade too — as COMPOSITE subject
-- foreign keys binding (id, task_id, workspace_id) against the existing
-- unique hooks, so a record can never reference another Task's or another
-- Workspace's session or block. No new privilege is granted here: the
-- openorc schema lockout established by the create_openorc_schema
-- migration carries isolation, and runtime access is backend-only.

create table openorc.task_resume_intents (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    task_agent_session_id uuid not null,
    task_block_id uuid not null,
    workflow_role text not null check (workflow_role in ('producer', 'reviewer')),
    external_session_id text not null check (external_session_id <> ''),
    requested_by_profile_id uuid not null,
    status text not null check (
        status in ('authorized', 'dispatched', 'settled', 'non_delivered', 'interrupted', 'superseded')
    ),
    routing jsonb not null,
    outcome jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (workspace_id)
        references openorc.workspaces (id)
        on delete cascade,
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id)
        on delete cascade,
    foreign key (task_agent_session_id, task_id, workspace_id)
        references openorc.task_agent_sessions (id, task_id, workspace_id)
        on delete cascade,
    foreign key (task_block_id, task_id, workspace_id)
        references openorc.task_blocks (id, task_id, workspace_id)
        on delete cascade,
    foreign key (requested_by_profile_id)
        references openorc.profiles (id)
        on delete cascade
);

-- At most one non-terminal Resume record per session binding: a duplicate
-- authorization can never replace a pending authorization or an in-flight
-- resumed turn.
create unique index task_resume_intents_open_session_uniq
    on openorc.task_resume_intents (task_agent_session_id)
    where status in ('authorized', 'dispatched');

create index task_resume_intents_task_id_idx
    on openorc.task_resume_intents (task_id);

create index task_resume_intents_task_block_id_idx
    on openorc.task_resume_intents (task_block_id);
