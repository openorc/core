-- OpenOrc destructive-deletion semantics (Phase 1, issue #27).
--
-- Defines and enforces the deletion ownership graph of the complete Phase 1
-- persistence model so OpenOrc can remove OpenOrc-owned state at account,
-- Workspace, Project, Repository-mapping, archived-Task, and Connection
-- scope without orphaned OpenOrc rows and without ever touching external
-- systems.
--
-- Deletion follows OpenOrc ownership boundaries. Owned OpenOrc descendants
-- are removed together through deliberate aggregate-level persistence
-- operations (openorc.persistence.deletion) and the database actions below.
-- Deleting OpenOrc state NEVER deletes or mutates external engineering
-- artifacts (GitHub repositories, issues, branches, commits, pull requests,
-- checks, statuses, comments) or runtime-owned state (Cline provider/MCP
-- credentials, runtime configuration, filesystem state). An OpenOrc
-- Repository record is OpenOrc's own persisted mapping for an external
-- GitHub repository: deleting it removes only that mapping. No action below
-- and no persistence primitive involves GitHub or any runtime.
--
-- THE SANCTIONED SUPABASE AUTH BOUNDARY: Profile identity is 1:1 by value
-- with the Supabase Auth user UUID. openorc.profiles.id references
-- auth.users (id) ON DELETE CASCADE. This is the single, deliberate,
-- reviewed exception to the rule that OpenOrc migrations never hold foreign
-- keys into Supabase-managed schemas, and it exists so the OpenOrc-owned
-- application graph can never outlive its account identity — regardless of
-- who initiates deletion:
--
--     auth.users
--         | on delete cascade
--     openorc.profiles
--         | on delete cascade
--     workspaces -> projects -> repositories -> tasks -> (task-owned graph)
--         |              |             |
--   connections   prompt_template_overrides   workflow_events
--   workflow_role_bindings
--
-- A direct Supabase-administrative deletion of an auth user must therefore
-- remove the complete OpenOrc-owned graph (no orphaned Profile/Workspace
-- rows from administrative deletion outside the OpenOrc application), and a
-- future OpenOrc Delete Account service (Phase 2) converges on the same
-- database behavior after performing its external cleanup. Phase 1
-- implements only this database-level behavior; no OpenOrc application code
-- invokes Supabase Auth admin APIs.
--
-- FOREIGN-KEY CLASSIFICATION — every relationship below is re-declared
-- explicitly so the deletion graph is reviewable, not implicit. Do not add
-- ON DELETE CASCADE mechanically:
--
-- 1. TRUE OWNERSHIP EDGES -> ON DELETE CASCADE. The child exists solely as
--    part of the referenced parent's owned aggregate and the parent is the
--    deletion authority: the ownership spine (Workspace->Profile,
--    Project->Workspace, Repository->Project, Task->Repository mapping),
--    every Task-owned aggregate's composite Task edge, the task-scoped
--    WorkflowEvent edge, and the Workspace-owned configuration edges
--    (Connections, role bindings, prompt overrides, Workspace-level events).
-- 2. RESTRICTIVE EDGES -> NO ACTION, DEFERRABLE INITIALLY DEFERRED.
--    Direct Workspace scope-consistency edges of Task-owned rows (their
--    ownership is through the Task, never through this second path), the
--    Connection historical/config cross-references (a Connection referenced
--    by historical TaskAgentSessions or role bindings can never be cascaded
--    through), the within-Task-aggregate linkage edges (gates/iterations to
--    their PlanRevision/Session/TaskPullRequest subjects — historical
--    references, not ownership, so raw deletion of a referenced row stays
--    durably blocked while references exist), and the Task's upward
--    current-object pointers. Deferral exists because one deliberate root
--    deletion (above all the auth.users account root) reaches rows through
--    several ownership paths while Postgres fires the sibling referential
--    triggers of the deleted row in an undefined order; checking at commit
--    makes the single-statement root cascade deterministic without ever
--    cascading through history. OpenOrc-initiated aggregate deletions also
--    clear Task current-object pointers explicitly inside their transaction
--    and delete Task-owned rows in deliberate dependency order, so these
--    deferred checks settle cleanly.
-- 3. AUTH ROOT EDGE -> the profiles/auth.users cascade above.
--
-- Migration history is append-only; the FK actions are corrected here, not
-- by editing earlier migrations.

-- ---------------------------------------------------------------------------
-- Sanctioned Supabase Auth boundary (the account root).
-- ---------------------------------------------------------------------------

alter table openorc.profiles
    add constraint profiles_id_auth_users_fk
    foreign key (id)
    references auth.users (id)
    on delete cascade;

-- ---------------------------------------------------------------------------
-- True ownership edges: ON DELETE CASCADE.
-- ---------------------------------------------------------------------------

-- Ownership spine: Profile -> Workspace -> Project -> Repository -> Task.
alter table openorc.workspaces
    drop constraint workspaces_owner_profile_id_fkey;
alter table openorc.workspaces
    add constraint workspaces_owner_profile_id_fkey
    foreign key (owner_profile_id) references openorc.profiles (id)
    on delete cascade;

alter table openorc.projects
    drop constraint projects_workspace_id_fkey;
alter table openorc.projects
    add constraint projects_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    on delete cascade;

alter table openorc.repositories
    drop constraint repositories_project_id_workspace_id_fkey;
alter table openorc.repositories
    add constraint repositories_project_id_workspace_id_fkey
    foreign key (project_id, workspace_id) references openorc.projects (id, workspace_id)
    on delete cascade;

alter table openorc.tasks
    drop constraint tasks_repository_id_workspace_id_fkey;
alter table openorc.tasks
    add constraint tasks_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id) references openorc.repositories (id, workspace_id)
    on delete cascade;

-- Task-owned aggregates cascade with their Task.
alter table openorc.task_agent_sessions
    drop constraint task_agent_sessions_task_id_workspace_id_fkey;
alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.plan_revisions
    drop constraint plan_revisions_task_id_workspace_id_fkey;
alter table openorc.plan_revisions
    add constraint plan_revisions_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.review_loops
    drop constraint review_loops_task_id_workspace_id_fkey;
alter table openorc.review_loops
    add constraint review_loops_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.review_iterations
    drop constraint review_iterations_task_id_workspace_id_fkey;
alter table openorc.review_iterations
    add constraint review_iterations_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.owner_gates
    drop constraint owner_gates_task_id_workspace_id_fkey;
alter table openorc.owner_gates
    add constraint owner_gates_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.executions
    drop constraint executions_task_id_workspace_id_fkey;
alter table openorc.executions
    add constraint executions_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.runtime_requests
    drop constraint runtime_requests_task_id_workspace_id_fkey;
alter table openorc.runtime_requests
    add constraint runtime_requests_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.task_blocks
    drop constraint task_blocks_task_id_workspace_id_fkey;
alter table openorc.task_blocks
    add constraint task_blocks_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.task_pull_requests
    drop constraint task_pull_requests_task_id_workspace_id_fkey;
alter table openorc.task_pull_requests
    add constraint task_pull_requests_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

alter table openorc.task_pull_requests
    drop constraint task_pull_requests_task_id_repository_id_workspace_id_fkey;
alter table openorc.task_pull_requests
    add constraint task_pull_requests_task_id_repository_id_workspace_id_fkey
    foreign key (task_id, repository_id, workspace_id)
    references openorc.tasks (id, repository_id, workspace_id)
    on delete cascade;

-- Task-scoped workflow events are Task-owned history; workspace-level
-- events (task_id NULL) are untouched by this edge.
alter table openorc.workflow_events
    drop constraint workflow_events_task_id_workspace_id_fkey;
alter table openorc.workflow_events
    add constraint workflow_events_task_id_workspace_id_fkey
    foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)
    on delete cascade;

-- Workspace-owned configuration cascades with its Workspace.
alter table openorc.connections
    drop constraint connections_workspace_id_fkey;
alter table openorc.connections
    add constraint connections_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    on delete cascade;

alter table openorc.workflow_role_bindings
    drop constraint workflow_role_bindings_workspace_id_fkey;
alter table openorc.workflow_role_bindings
    add constraint workflow_role_bindings_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    on delete cascade;

alter table openorc.prompt_template_overrides
    drop constraint prompt_template_overrides_workspace_id_fkey;
alter table openorc.prompt_template_overrides
    add constraint prompt_template_overrides_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    on delete cascade;

alter table openorc.workflow_events
    drop constraint workflow_events_workspace_id_fkey;
alter table openorc.workflow_events
    add constraint workflow_events_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    on delete cascade;

-- ---------------------------------------------------------------------------
-- Restrictive edges: NO ACTION, DEFERRABLE INITIALLY DEFERRED.
--
-- Scope-consistency facts, historical/config cross-references, and
-- within-aggregate linkage are never ownership. These edges keep deletion of
-- a referenced row durably blocked while references exist, checked at commit
-- (or immediately where a repository operation forces the constraint) so the
-- deliberate single-statement root cascade settles deterministically.
-- ---------------------------------------------------------------------------

-- Direct Workspace scope consistency of Task-owned rows: ownership is
-- through the Task (and its Repository mapping), never through this edge.
alter table openorc.tasks
    drop constraint tasks_workspace_id_fkey;
alter table openorc.tasks
    add constraint tasks_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.task_agent_sessions
    drop constraint task_agent_sessions_workspace_id_fkey;
alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.plan_revisions
    drop constraint plan_revisions_workspace_id_fkey;
alter table openorc.plan_revisions
    add constraint plan_revisions_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.review_loops
    drop constraint review_loops_workspace_id_fkey;
alter table openorc.review_loops
    add constraint review_loops_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.review_iterations
    drop constraint review_iterations_workspace_id_fkey;
alter table openorc.review_iterations
    add constraint review_iterations_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.owner_gates
    drop constraint owner_gates_workspace_id_fkey;
alter table openorc.owner_gates
    add constraint owner_gates_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.executions
    drop constraint executions_workspace_id_fkey;
alter table openorc.executions
    add constraint executions_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.runtime_requests
    drop constraint runtime_requests_workspace_id_fkey;
alter table openorc.runtime_requests
    add constraint runtime_requests_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.task_blocks
    drop constraint task_blocks_workspace_id_fkey;
alter table openorc.task_blocks
    add constraint task_blocks_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

alter table openorc.task_pull_requests
    drop constraint task_pull_requests_workspace_id_fkey;
alter table openorc.task_pull_requests
    add constraint task_pull_requests_workspace_id_fkey
    foreign key (workspace_id) references openorc.workspaces (id)
    deferrable initially deferred;

-- Connection historical/config cross-references: deleting a Connection can
-- never cascade through Task/session history or Workspace configuration.
alter table openorc.task_agent_sessions
    drop constraint task_agent_sessions_connection_id_workspace_id_fkey;
alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_connection_id_workspace_id_fkey
    foreign key (connection_id, workspace_id) references openorc.connections (id, workspace_id)
    deferrable initially deferred;

alter table openorc.workflow_role_bindings
    drop constraint workflow_role_bindings_connection_id_workspace_id_fkey;
alter table openorc.workflow_role_bindings
    add constraint workflow_role_bindings_connection_id_workspace_id_fkey
    foreign key (connection_id, workspace_id) references openorc.connections (id, workspace_id)
    deferrable initially deferred;

-- Within-Task-aggregate linkage: historical references to subjects inside
-- the same aggregate — restrictive, never an ownership path.
alter table openorc.review_iterations
    drop constraint review_iterations_review_loop_id_task_id_workspace_id_fkey;
alter table openorc.review_iterations
    add constraint review_iterations_review_loop_id_task_id_workspace_id_fkey
    foreign key (review_loop_id, task_id, workspace_id)
    references openorc.review_loops (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.review_iterations
    drop constraint review_iterations_plan_revision_id_task_id_workspace_id_fkey;
alter table openorc.review_iterations
    add constraint review_iterations_plan_revision_id_task_id_workspace_id_fkey
    foreign key (plan_revision_id, task_id, workspace_id)
    references openorc.plan_revisions (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.owner_gates
    drop constraint owner_gates_plan_revision_id_task_id_workspace_id_fkey;
alter table openorc.owner_gates
    add constraint owner_gates_plan_revision_id_task_id_workspace_id_fkey
    foreign key (plan_revision_id, task_id, workspace_id)
    references openorc.plan_revisions (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.review_iterations
    drop constraint review_iterations_task_pull_request_fk;
alter table openorc.review_iterations
    add constraint review_iterations_task_pull_request_fk
    foreign key (task_pull_request_id, task_id, workspace_id)
    references openorc.task_pull_requests (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.owner_gates
    drop constraint owner_gates_task_pull_request_fk;
alter table openorc.owner_gates
    add constraint owner_gates_task_pull_request_fk
    foreign key (task_pull_request_id, task_id, workspace_id)
    references openorc.task_pull_requests (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.executions
    drop constraint executions_producer_session_id_task_id_workspace_id_fkey;
alter table openorc.executions
    add constraint executions_producer_session_id_task_id_workspace_id_fkey
    foreign key (producer_session_id, task_id, workspace_id)
    references openorc.task_agent_sessions (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.runtime_requests
    drop constraint runtime_requests_producer_session_id_task_id_workspace_id_fkey;
alter table openorc.runtime_requests
    add constraint runtime_requests_producer_session_id_task_id_workspace_id_fkey
    foreign key (producer_session_id, task_id, workspace_id)
    references openorc.task_agent_sessions (id, task_id, workspace_id)
    deferrable initially deferred;

-- The Task's upward current-object pointers: restrictive deferred edges, not
-- ownership. OpenOrc-initiated aggregate deletions clear them explicitly
-- before deleting Tasks; the deferral backstops the Auth-root cascade, where
-- no OpenOrc code runs.
alter table openorc.tasks
    drop constraint tasks_current_plan_revision_fk;
alter table openorc.tasks
    add constraint tasks_current_plan_revision_fk
    foreign key (current_plan_revision_id, id, workspace_id)
    references openorc.plan_revisions (id, task_id, workspace_id)
    deferrable initially deferred;

alter table openorc.tasks
    drop constraint tasks_current_owner_gate_fk;
alter table openorc.tasks
    add constraint tasks_current_owner_gate_fk
    foreign key (current_owner_gate_id, id, workspace_id)
    references openorc.owner_gates (id, task_id, workspace_id)
    deferrable initially deferred;