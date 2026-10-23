-- Owner-control one-shot external-effect intents (E10b, issue #228).
--
-- The Owner Abort and unified Task Cancel commands each perform exactly one
-- externally-visible effect outside their authoritative database
-- transaction: the ClineBox activity Interrupt (Abort) and the canonical
-- pull-request closure request (Cancel). Both effects are one-shot, may be
-- interrupted by a crash between the durable decision and the external
-- call, and must never be blindly re-issued after an uncertain outcome. The
-- workflow-relevant decision facts (Task status, TaskBlock, OwnerGate,
-- ReviewLoop, archival) already live on their canonical records; what was
-- missing is the narrow durable identity of each admitted external effect
-- and its safe, inspectable outcome — audit events alone are not a work
-- queue.
--
-- ``openorc.task_interrupt_intents`` is the one-shot, exact-session
-- Interrupt intent registered atomically with the Owner Abort's
-- ``owner_action_required`` block and the Task's ``blocked`` transition. One
-- row per exact admitted Task/role session carries the binding's stable
-- internal identity and its opaque external session identity, the
-- accountable Owner Profile, and a forward-only status: ``unsettled`` is
-- the registered-not-yet-accounted state (a crash, a second Abort command,
-- or a new adapter instance must find it rather than re-interrupt), and
-- ``settled``/``failed``/``unknown`` are the recorded dispatch or
-- settlement outcomes. The partial unique index admits at most one
-- unsettled intent per session binding, so a duplicate dispatch can never
-- replace a pending uncertainty. Outcomes are small canonical JSON objects
-- of safe machine facts — never tokens, transcript content, prompts, or
-- raw provider errors.
--
-- ``openorc.task_pull_request_close_intents`` is the one-shot PR-close
-- obligation of a Task cancellation, committed atomically with the
-- terminal archival whenever the canonical open/unmerged pull request may
-- require closure: a crash immediately after archival must leave the
-- closure obligation observable. One row per Task (the canonical
-- ``TaskPullRequest`` is ``unique (task_id)`` and a Task cancels at most
-- once) carries the stable GitHub PR identity, the accountable Owner
-- Profile whose user authorization must perform the write, and the
-- forward-only status: ``pending`` is the unresolved obligation,
-- ``close_requested`` a delivered closure whose observed outcome is
-- recorded in ``outcome``, ``no_action`` an authoritative observation that
-- made no write necessary (already closed-unmerged, or merged after
-- archival — archived history is never rewritten), and
-- ``failed``/``unknown`` the safe terminal records of a known failure or
-- an unconfirmable outcome that must never be blindly replayed.
--
-- Deletion ownership follows the established classification (issue #57):
-- both tables are Task-owned operational rows that cascade from their Task
-- and Workspace; the interrupted session binding and the canonical PR
-- record are the subjects the intents are meaningless without, so those
-- edges cascade too — as COMPOSITE subject foreign keys, so an intent can
-- never reference another Task's (or another Workspace's) session or
-- canonical PR: the session edge binds (id, task_id, workspace_id) against
-- the composite uniqueness hook added to ``task_agent_sessions`` here, and
-- the PR edge binds (id, task_id, workspace_id) against the canonical
-- record's existing ``unique (id, task_id, workspace_id)`` hook; the
-- accountable Profile edge cascades with the account root. No new privilege
-- is granted here: the openorc schema lockout established by the
-- create_openorc_schema migration carries isolation, and runtime access is
-- backend-only.

-- The composite uniqueness hook the Interrupt intent's subject foreign key
-- binds against (the sessions table deliberately carries no (id,
-- workspace_id) hook of its own; this additive constraint follows the
-- ``tasks_id_workspace_id_uniq`` precedent).
alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_id_task_id_workspace_id_uniq
    unique (id, task_id, workspace_id);

create table openorc.task_interrupt_intents (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    task_agent_session_id uuid not null,
    workflow_role text not null check (workflow_role in ('producer', 'reviewer')),
    external_session_id text not null check (external_session_id <> ''),
    requested_by_profile_id uuid not null,
    status text not null check (status in ('unsettled', 'settled', 'failed', 'unknown')),
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
    foreign key (requested_by_profile_id)
        references openorc.profiles (id)
        on delete cascade
);

-- At most one outstanding (unsettled) Interrupt intent per session binding:
-- a second Abort command can never blind-duplicate a pending uncertainty.
create unique index task_interrupt_intents_unsettled_session_uniq
    on openorc.task_interrupt_intents (task_agent_session_id)
    where status = 'unsettled';

create index task_interrupt_intents_task_id_idx
    on openorc.task_interrupt_intents (task_id);

create table openorc.task_pull_request_close_intents (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    task_pull_request_id uuid not null,
    github_pr_id bigint not null check (github_pr_id > 0),
    github_pr_number integer not null check (github_pr_number > 0),
    requested_by_profile_id uuid not null,
    status text not null check (status in ('pending', 'close_requested', 'no_action', 'failed', 'unknown')),
    outcome jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (workspace_id)
        references openorc.workspaces (id)
        on delete cascade,
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id)
        on delete cascade,
    foreign key (task_pull_request_id, task_id, workspace_id)
        references openorc.task_pull_requests (id, task_id, workspace_id)
        on delete cascade,
    foreign key (requested_by_profile_id)
        references openorc.profiles (id)
        on delete cascade,
    unique (task_id)
);
