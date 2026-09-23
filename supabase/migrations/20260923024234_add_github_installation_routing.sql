-- GitHub App installation persistence and deterministic Repository routing
-- (Phase 2B, issue #57).
--
-- Establishes the durable Workspace-scoped GitHub App installation model and
-- the explicit Repository -> installation route required by every later
-- GitHub operation:
--
--   Workspace -> GitHubInstallation
--   Repository .github_installation_id -> GitHubInstallation (same Workspace)
--
-- Durable properties carried by this migration:
--
-- - A GitHubInstallation is one GitHub App installation available to one
--   Workspace. It carries only durable routing and observation facts: the
--   OpenOrc UUID identity, the direct Workspace scope, the stable external
--   installation ID and account ID, the mutable observed account login/type,
--   and the mutable observed ``suspended_at`` fact. Raw GitHub App
--   private-key material, installation access tokens, human OAuth tokens,
--   PATs, and any other credential material never appear in this table:
--   OpenOrc-owned runtime credentials live in Supabase Vault (issue #55),
--   and GitHub App credentials are not OpenOrc application-table state at
--   all.
-- - A Workspace may hold several installations (its repositories may span
--   multiple GitHub accounts); the same external installation may be
--   represented independently in several Workspaces. Within one Workspace
--   there is exactly one canonical record per external installation ID.
-- - Repository routing is one explicit nullable foreign-key column. A single
--   column makes multiple competing installation routes for one Repository
--   unrepresentable; the composite foreign key
--   ``(github_installation_id, workspace_id) ->
--   github_installations (id, workspace_id)`` (hooked by the installations'
--   ``unique (id, workspace_id)``) makes a route into another Workspace
--   unrepresentable. Mutable owner/login/name/URL presentation is never
--   routing authority.
-- - Existing Phase 1 Repository rows keep a NULL route: valid historical /
--   configuration state, but not configured for GitHub operations until
--   explicitly routed. Later services must fail closed on unconfigured
--   repositories rather than guessing an installation.
-- - Stable identity vs observation: reconciling an installation updates the
--   observed facts (account ID as reported, login/type, suspended_at) and
--   never replaces the record identity (OpenOrc UUID, Workspace, external
--   installation ID). ``suspended_at`` is an observation carried verbatim;
--   no column or view derives a usability/authorization claim from it —
--   current repository access is established by later reconciliation work.
--
-- FOREIGN-KEY CLASSIFICATION (the deletion-ownership vocabulary, issue #27):
-- - github_installations.workspace_id -> workspaces: TRUE OWNERSHIP edge,
--   ON DELETE CASCADE — the record exists solely within its Workspace's
--   aggregate, and deleting OpenOrc configuration never uninstalls the
--   GitHub App or touches external GitHub artifacts.
-- - repositories.(github_installation_id, workspace_id) ->
--   github_installations.(id, workspace_id): RESTRICTIVE,
--   NO ACTION DEFERRABLE INITIALLY DEFERRED — a referenced installation
--   record can never be cascaded away through the route; it is removed only
--   when no Repository routes to it (the route is explicitly unbound
--   first), and the deferral backstops the Auth-root cascade, which deletes
--   Repositories and installation records together in one statement.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

create table openorc.github_installations (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    github_installation_id bigint not null check (github_installation_id > 0),
    github_account_id bigint not null check (github_account_id > 0),
    account_login text not null check (account_login ~ '\S'),
    account_type text not null check (account_type ~ '\S'),
    suspended_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Hooks the composite foreign key used by Repository routing so a
    -- Repository route can never cross into another Workspace.
    unique (id, workspace_id),

    -- One canonical installation record per Workspace per external
    -- installation ID; the same external installation may exist
    -- independently in other Workspaces.
    unique (workspace_id, github_installation_id)
);

create index github_installations_workspace_id_idx
    on openorc.github_installations (workspace_id);

-- The Workspace ownership edge, re-declared with its explicit deletion
-- action (the deletion-ownership vocabulary, issue #27): true ownership —
-- the record exists solely within its Workspace's aggregate.
alter table openorc.github_installations
    add constraint github_installations_workspace_id_fkey
    foreign key (workspace_id)
    references openorc.workspaces (id)
    on delete cascade;

alter table openorc.repositories
    add column github_installation_id uuid,
    add constraint repositories_github_installation_id_workspace_id_fkey
    foreign key (github_installation_id, workspace_id)
    references openorc.github_installations (id, workspace_id)
    deferrable initially deferred;

