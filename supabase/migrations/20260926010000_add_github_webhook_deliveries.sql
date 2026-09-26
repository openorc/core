-- Durable GitHub webhook delivery intake (Phase 2B, issue #61).
--
-- Establishes the secure, durable notification-intake boundary for the thin
-- FastAPI webhook route: authenticated deliveries are recorded exactly once,
-- keyed by GitHub's delivery GUID, with only safe routing metadata:
--
--   GitHub delivery (GUID-keyed) -> resolved Workspace routing linkages
--
-- Durable properties carried by this migration:
--
-- - A delivery record exists only for a VERIFIED delivery (the SHA-256
--   signature is verified over the exact raw request bytes before intake);
--   missing/malformed/invalid signatures are rejected before any record and
--   are represented by no row.
-- - The GitHub delivery GUID is the provider deduplication identity and is
--   durably unique: a re-delivered GUID is acknowledged idempotently and can
--   never create a second accepted record (the unique constraint
--   arbitrates concurrent duplicate inserts deterministically).
-- - Only safe metadata is stored: event name, optional action, the bounded
--   intake classification, the bounded routing resolution, a normalized
--   semantic routing target for relevant deliveries, nullable stable GitHub
--   installation/repository/issue/PR identifiers, and the received instant.
--   Raw webhook request bodies, signatures, webhook secrets, and customer
--   content (issue/PR titles and bodies, repository content) have no column
--   and no path into this table.
-- - A `relevant` classification (a settled v1 event family with sufficient
--   stable routing identity) requires the normalized semantic routing
--   target, the bounded routing resolution, and both stable
--   installation/repository identities; ignored and unusable deliveries
--   carry none of them. Issue and pull-request numbers are mutually
--   exclusive (they address distinct provider object families).
-- - The delivery table carries no Workspace foreign key: the delivery GUID
--   is provider-owned identity and survives Workspace lifecycle.
-- - The routing-linkage table retains the resolved Workspace routing facts
--   dispatch (#120) dispatches from: one row per exact route match of the
--   delivery's installation and the affected stable repository identity
--   within a Workspace. Legitimate multi-Workspace mappings are
--   representable (fan-out); nothing is ever inferred from mutable
--   owner/login presentation data.
-- - Delivery and routing records are notification/recovery metadata, not
--   workflow authority and not an event-sourced copy of GitHub.
--
-- FOREIGN-KEY CLASSIFICATION (the deletion-ownership vocabulary, issue #27):
-- - github_webhook_delivery_routes.delivery_id -> github_webhook_deliveries:
--   TRUE OWNERSHIP edge, ON DELETE CASCADE — the linkage exists solely
--   within its delivery's record.
-- - github_webhook_delivery_routes.workspace_id -> workspaces: TRUE OWNERSHIP
--   edge, ON DELETE CASCADE — the linkage is a Workspace-scoped operational
--   row; deleting a Workspace removes its linkages while the provider-owned
--   dedup record survives.
-- - github_webhook_delivery_routes.(repository_id, workspace_id) ->
--   repositories (id, workspace_id): TRUE OWNERSHIP composite edge,
--   ON DELETE CASCADE (the #59 pattern), keeping the linkage's Workspace
--   scope consistent with the owning Repository.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

create table openorc.github_webhook_deliveries (
    id uuid primary key default gen_random_uuid(),
    delivery_guid text not null check (delivery_guid ~ '\S'),
    event_name text not null check (event_name ~ '\S'),
    action text check (action is null or action ~ '\S'),
    classification text not null
        check (classification in ('relevant', 'ignored', 'unusable')),
    routing_target text check (routing_target is null or routing_target in (
        'repository_metadata', 'issue_state', 'issue_relations',
        'task_branch_or_pull_request', 'pull_request_state', 'checks')),
    routing_resolution text check (routing_resolution is null or
        routing_resolution in ('resolved', 'unmapped_installation',
        'unconfigured_repository', 'route_mismatch')),
    github_installation_id bigint
        check (github_installation_id is null or github_installation_id > 0),
    github_repository_id bigint
        check (github_repository_id is null or github_repository_id > 0),
    github_issue_number bigint
        check (github_issue_number is null or github_issue_number > 0),
    github_pull_request_number bigint
        check (github_pull_request_number is null or github_pull_request_number > 0),
    received_at timestamptz not null default now(),

    -- The provider deduplication identity: exactly one accepted record per
    -- delivery GUID, durably.
    unique (delivery_guid),

    -- A relevant delivery always carries the normalized semantic routing
    -- target and the bounded routing resolution; ignored/unusable
    -- deliveries carry neither.
    check ((classification = 'relevant') = (routing_target is not null)),
    check ((classification = 'relevant') = (routing_resolution is not null)),

    -- A relevant delivery requires both stable installation and repository
    -- identity (sufficient stable routing identity).
    check (classification <> 'relevant' or github_installation_id is not null),
    check (classification <> 'relevant' or github_repository_id is not null),

    -- Issue and pull-request numbers address distinct provider object
    -- families and are never both present.
    check (github_issue_number is null or github_pull_request_number is null)
);

create table openorc.github_webhook_delivery_routes (
    id uuid primary key default gen_random_uuid(),
    delivery_id uuid not null,
    workspace_id uuid not null,
    repository_id uuid not null,
    created_at timestamptz not null default now(),

    -- One resolved routing linkage per delivery per Workspace Repository;
    -- fan-out across Workspaces (the same external repository connected
    -- independently in several Workspaces) is representable.
    unique (delivery_id, workspace_id, repository_id)
);

-- The delivery ownership edge, declared with its explicit deletion action:
-- true ownership — the linkage exists solely within its delivery's record.
alter table openorc.github_webhook_delivery_routes
    add constraint github_webhook_delivery_routes_delivery_id_fkey
    foreign key (delivery_id)
    references openorc.github_webhook_deliveries (id)
    on delete cascade;

-- The Workspace scope edge: true ownership — a Workspace-scoped operational
-- row; the provider-owned dedup record survives Workspace deletion.
alter table openorc.github_webhook_delivery_routes
    add constraint github_webhook_delivery_routes_workspace_id_fkey
    foreign key (workspace_id)
    references openorc.workspaces (id)
    on delete cascade;

-- The Repository ownership edge (the #59 composite-cascade pattern): keeps
-- the linkage's Workspace scope consistent with the owning Repository.
alter table openorc.github_webhook_delivery_routes
    add constraint github_webhook_delivery_routes_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id)
    references openorc.repositories (id, workspace_id)
    on delete cascade;

-- Query-driven index for the webhook-direction routing resolver: the exact
-- stable-repository-identity lookup the delivery resolution performs.
create index repositories_github_repository_id_idx
    on openorc.repositories (github_repository_id);