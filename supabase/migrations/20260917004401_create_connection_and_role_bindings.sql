-- OpenOrc Connection and workflow role binding persistence (Phase 1, issue #20).
--
-- Establishes Workspace-scoped runtime routing state:
--
--   Workspace -> Connection
--   Workspace -> WorkflowRoleBinding -> Connection
--
-- Durable properties carried by this migration:
--
-- - A Connection is one configured agent-runtime route within one Workspace.
--   Raw OpenOrc-owned credentials never live in ordinary application tables:
--   OpenOrc-owned authentication is represented only through the nullable,
--   opaque ``auth_reference`` (NULL means no OpenOrc-owned auth reference is
--   currently configured; non-NULL means a reference has been supplied).
--   Validation, expiry, revocation, health, and authentication lifecycle
--   state belong to later functionality that can actually determine those
--   facts, so no speculative auth status vocabulary is persisted here.
-- - ``enabled`` is the Owner-controlled eligibility switch: OpenOrc is
--   permitted to use the Connection. It says nothing about whether the
--   runtime is reachable, healthy, initialized, READY, or WORKING.
-- - ``session_capacity`` is Owner-configured OpenOrc admission control,
--   scoped to the Connection and never discovered from the runtime. The
--   default of 1 is deliberate: concurrency must be explicitly enabled by
--   the Owner rather than assumed.
-- - Durable checks mirror the domain's validation so a transaction cannot
--   commit state the domain would reject afterwards: Connection names are
--   nonblank, ``auth_reference`` is NULL or nonblank, and ``safe_config`` is
--   a JSON object.
-- - ``reported_provider``/``reported_model`` are nullable opaque
--   runtime-reported observation strings, never OpenOrc configuration
--   authority and never an enum; Cline provider/model selection remains
--   unset in v1.
-- - A WorkflowRoleBinding is mutable per-role runtime configuration: exactly
--   one binding per (workspace, role) in v1 (no runtime pools or failover).
--   Producer and Reviewer bindings are independent and may reference the
--   same Connection or separate Connections. The binding carries only
--   workspace/role/connection identity and timestamps — no per-role session
--   configuration exists in v1 (provider/model selection is intentionally
--   unavailable; PLAN/ACT mode is workflow-derived; initialization prompts
--   are OpenOrc-controlled protocol behavior).
-- - The composite foreign keys keep the binding's direct Workspace scope in
--   agreement with the Connection's Workspace, exactly like the ownership
--   tables.
-- - No privileges are given to browser-facing roles: the schema-level lockout
--   established by the create_openorc_schema migration carries isolation, and
--   runtime access is backend-only.

create table openorc.connections (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    adapter_type text not null check (adapter_type = 'cline'),
    -- Nonblank name: mirrors the domain's "strip() must be non-empty" rule
    -- ('\S' matches at least one non-whitespace character).
    name text not null check (name ~ '\S'),
    -- Non-secret Owner configuration container: canonical JSON object only.
    safe_config jsonb not null default '{}'::jsonb
        check (jsonb_typeof(safe_config) = 'object'),
    session_capacity integer not null default 1 check (session_capacity > 0),
    enabled boolean not null default true,
    -- Opaque OpenOrc-owned authentication boundary. NULL means no reference
    -- is configured; a non-NULL value must be nonblank.
    auth_reference text check (auth_reference is null or auth_reference ~ '\S'),
    reported_provider text,
    reported_model text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Hooks the composite foreign key used by Workspace-owned children so a
    -- child's direct workspace scope cannot disagree with parent ownership.
    unique (id, workspace_id)
);

create index connections_workspace_id_idx
    on openorc.connections (workspace_id);

create table openorc.workflow_role_bindings (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    role text not null check (role in ('producer', 'reviewer')),
    connection_id uuid not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the bound Connection's Workspace.
    foreign key (connection_id, workspace_id)
        references openorc.connections (id, workspace_id),

    -- One configured runtime binding per workflow role in v1; no pools and
    -- no failover. Producer and Reviewer bindings are independent rows.
    unique (workspace_id, role)
);

create index workflow_role_bindings_connection_id_idx
    on openorc.workflow_role_bindings (connection_id);

