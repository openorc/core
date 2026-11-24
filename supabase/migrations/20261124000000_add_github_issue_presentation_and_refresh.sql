-- GitHub issue presentation projection extension and scoped on-demand
-- refresh tracking (Phase 2G, issue #375).
--
-- Part 1 extends the durable GitHub Issue projection (#59) with the agreed
-- presentation metadata. GitHub remains the engineering source of truth; the
-- projection deliberately retains selected typed facts, never the raw
-- provider response:
--
-- - `state_reason`, `html_url`, `provider_created_at`, `provider_closed_at`
--   plus the bounded nested user/labels/assignees/milestone facts. Every new
--   column is nullable: an existing row has not yet observed the fact, and
--   the projection stays honest until a successful reconciliation supplies
--   it. No default fabricates a provider fact, and migrations preserve
--   existing issue identity, Task links, and workflow state untouched.
-- - Nested presentation subsets (JSONB): user/assignee carry the documented
--   `id`/`login` pair; labels carry `id`/`name`/`color`/`description`;
--   milestone carries `id`/`number`/`title`/`description`/`state`/`html_url`/
--   `due_on`. Collections are arrays only when observed — `null` means the
--   fact has not been observed, `[]` means GitHub reported an empty listing.
-- - `hierarchy_observed_at` / `dependencies_observed_at` are the minimal
--   observation-evidence instants stamped only by the successful relations
--   synchronizations (#60): they let the read surface distinguish an
--   authoritative empty relation observation from "never observed" so a
--   failed refresh can never masquerade as confirmed emptiness. They are
--   presentation evidence, never workflow facts, and never gate Task Start
--   (Start keeps its existing fresh authoritative checks).
-- - The requirements fingerprint remains exactly title + body under the
--   #59 canonicalization: no fingerprint column changes, and presentation
--   changes can never invalidate a Task or invent source-drift evidence.
--
-- Part 2 creates `openorc.github_refreshes`, the minimum durable
-- refresh-specific execution tracking for the scoped on-demand GitHub
-- refresh (#375). It is not a generic job framework, not a second
-- workflow/event ledger, and never workflow authority: it records only the
-- truthfully observable lifecycle of one authorized refresh execution.
--
-- - One row per authorized refresh admission. `status` follows the closed
--   vocabulary `accepted → running → {completed, partially_completed, failed}`
--   plus the distinct `unknown` outcome for lost completion evidence.
-- - `lease_expires_at` bounds admission and execution claims: an expired
--   accepted or running record is recoverable to `unknown` on demand (at the
--   next overlapping-scope admission), never silently claimable by a stale
--   job.
-- - Active-uniqueness backstops (partial unique indexes over the active
--   statuses) make the admission containment durable: at most one active
--   workspace-scope refresh per Workspace and at most one active
--   repository-scope refresh per (Workspace, Repository). Concurrent
--   overlapping admissions are contained by the database itself.
-- - Foreign-key classification follows the deletion-ownership vocabulary
--   (issue #27): the Workspace edge is true ownership (`ON DELETE
--   CASCADE`), integrating retention with Workspace deletion; the nullable
--   composite `(repository_id, workspace_id)` edge is likewise true
--   ownership of the addressed Repository aggregate (composite foreign keys
--   do not match NULL, so workspace-scope rows are unaffected).
-- - Bounded retention: terminal rows older than a documented constant are
--   trimmed by the admission operation; nothing here grows unboundedly.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.github_issues
    add column state_reason text,
    add column html_url text,
    add column provider_created_at timestamptz,
    add column provider_closed_at timestamptz,
    add column user_json jsonb,
    add column labels_json jsonb,
    add column assignees_json jsonb,
    add column milestone_json jsonb,
    add column hierarchy_observed_at timestamptz,
    add column dependencies_observed_at timestamptz;

create table openorc.github_refreshes (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    repository_id uuid,
    status text not null
        check (status in (
            'accepted', 'running', 'completed', 'partially_completed', 'failed', 'unknown'
        )),
    requested_at timestamptz not null default now(),
    started_at timestamptz,
    lease_expires_at timestamptz,
    completed_at timestamptz,
    summary jsonb,
    errors jsonb,

    -- Hooks the composite foreign key used by Workspace-owned children: the
    -- projection pattern (#59/#60) for the addressed-repository edge below.
    unique (id, workspace_id)
);

-- The Workspace ownership edge, declared with its explicit deletion action
-- (the deletion-ownership vocabulary, issue #27): true ownership — the
-- refresh record exists solely within its Workspace's aggregate, so
-- Workspace deletion removes its bounded refresh history.
alter table openorc.github_refreshes
    add constraint github_refreshes_workspace_id_fkey
    foreign key (workspace_id)
    references openorc.workspaces (id)
    on delete cascade;

-- The nullable composite edge to the addressed Repository aggregate: true
-- ownership when set (the refresh tracks one Workspace Repository), inert
-- for workspace-scope refreshes because composite foreign keys never match
-- NULL.
alter table openorc.github_refreshes
    add constraint github_refreshes_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id)
    references openorc.repositories (id, workspace_id)
    on delete cascade;

-- Durable active-uniqueness backstops for the #375 admission containment:
-- at most one active workspace-scope refresh per Workspace, and at most one
-- active refresh per addressed (Workspace, Repository). Terminal statuses
-- are exempt: history never blocks a fresh admission.
create unique index github_refreshes_one_active_workspace_scope
    on openorc.github_refreshes (workspace_id)
    where status in ('accepted', 'running') and repository_id is null;

create unique index github_refreshes_one_active_repository_scope
    on openorc.github_refreshes (workspace_id, repository_id)
    where status in ('accepted', 'running') and repository_id is not null;

-- The minimal per-route completion evidence for the scoped webhook-delivery
-- recovery (#375): a route's routed reconciliation completing or typedly
-- declining stamps this instant. A scope-limited recovery may then prove
-- that EVERY route of a shared fan-out delivery is resolved — across
-- invocations, without ever dispatching an out-of-scope route — before the
-- whole delivery is marked processed. Existing routing rows migrate honestly
-- with no fabricated completion evidence; the delivery-level `processed_at`
-- rule keeps its established meaning.
alter table openorc.github_webhook_delivery_routes
    add column resolved_at timestamptz;