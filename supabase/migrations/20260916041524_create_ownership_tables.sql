-- OpenOrc ownership and repository identity foundation (Phase 1, issue #19).
--
-- Establishes the ownership hierarchy that scopes later Workspace-owned
-- OpenOrc state:
--
--   Profile -> Workspace -> Project -> Repository
--
-- Durable properties carried by this migration:
--
-- - Profile is the canonical OpenOrc application identity. Profile.id is
--   deliberately the corresponding Supabase Auth user UUID, supplied by the
--   caller at insert time (1:1 by value). OpenOrc tables never hold foreign
--   keys into Supabase-managed schemas; domain references point at profiles.
-- - A Workspace is owned by exactly one Profile in v1. Membership, invitation,
--   and RBAC concepts do not exist in this model.
-- - A Project belongs to one Workspace. A Repository belongs to one Project
--   and carries its Workspace directly; the composite foreign key below makes
--   direct-scope disagreement with the parent Project impossible.
-- - A Repository record's identity is its OpenOrc UUID. The GitHub repository
--   ID is the stable external identity used for reconciliation and
--   canonicalization within a Workspace — never the mutable owner/name
--   presentation fields. The same external GitHub repository may exist
--   independently in multiple Workspaces; within one Workspace there is
--   exactly one canonical Repository record per GitHub repository identity.
--   Observed presentation metadata (owner login, name, URL, visibility,
--   default branch) is mutable and never identity.
-- - No privileges are given to browser-facing roles: the schema-level lockout
--   established by the create_openorc_schema migration carries isolation, and
--   runtime access is backend-only.

create table openorc.profiles (
    id uuid primary key,
    created_at timestamptz not null default now()
);

create table openorc.workspaces (
    id uuid primary key default gen_random_uuid(),
    owner_profile_id uuid not null references openorc.profiles (id),
    name text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index workspaces_owner_profile_id_idx
    on openorc.workspaces (owner_profile_id);

create table openorc.projects (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    name text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Hooks the composite foreign key used by Workspace-owned children so a
    -- child's direct workspace scope cannot disagree with parent ownership.
    unique (id, workspace_id)
);

create index projects_workspace_id_idx
    on openorc.projects (workspace_id);

create table openorc.repositories (
    id uuid primary key default gen_random_uuid(),
    project_id uuid not null,
    workspace_id uuid not null,
    github_repository_id bigint not null check (github_repository_id > 0),
    owner_login text not null,
    name text not null,
    html_url text not null,
    is_private boolean not null,
    default_branch text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the owning Project's Workspace.
    foreign key (project_id, workspace_id)
        references openorc.projects (id, workspace_id),

    -- One canonical Repository record per Workspace per GitHub repository
    -- identity; the same external repository may exist in other Workspaces.
    unique (workspace_id, github_repository_id)
);

create index repositories_project_id_idx
    on openorc.repositories (project_id);
