-- Durable GitHub Issue projection and Repository reconciliation support
-- (Phase 2B, issue #59).
--
-- Establishes the focused persistence for authoritative GitHub Issue
-- observations, independent from the Task aggregate:
--
--   Workspace -> Repository -> GitHubIssue (projection)
--
-- Durable properties carried by this migration:
--
-- - The projection is keyed by the stable external identity: one canonical
--   row per OpenOrc Workspace Repository record plus the stable GitHub issue
--   ID. The repository-local issue number is durable address metadata with
--   its own uniqueness so issue-number reuse can never substitute for, or
--   race past, stable issue identity: two reconciliations can never insert
--   the same number under different stable IDs, and the canonical identity
--   constraint keeps one projection per (Repository, stable issue ID).
-- - The projection carries only the issue facts the Phase 2 workflow /
--   reconciliation needs: stable issue ID, issue number (address metadata),
--   title, verbatim nullable body (GitHub reports issues with no body as
--   NULL), open/closed state, the deterministic title+body requirements
--   fingerprint, the ordinary observed provider `updated_at` metadata, and
--   OpenOrc observation instants. Presentation/relationship fields (labels,
--   assignees, reactions, comments, timeline events) have no columns:
--   comments — including OpenOrc's future published plan comment — can never
--   move the requirements fingerprint because they are never stored here.
-- - Issue facts are never copied onto `tasks`; Task identity/workflow state
--   stays separate (Phase 1), and later workflow services decide any
--   source-change consequence outside this table.
-- - Foreign-key classification follows the deletion-ownership vocabulary
--   (issue #27): `(repository_id, workspace_id) -> repositories` is a
--   true-ownership ON DELETE CASCADE edge — the projection exists solely
--   within its Workspace Repository's aggregate, exactly like Tasks. The
--   composite foreign key makes a projection row whose direct Workspace
--   scope disagrees with the owning Repository unrepresentable. `repositories`
--   gains the deliberate `unique (id, workspace_id)` hook that Workspace-owned
--   children compose against (the #53/#57 pattern).
-- - Reconciliation updates only mutable observations and never rewrites
--   stable identity: the OpenOrc UUID, Workspace, Repository, and stable
--   GitHub issue ID are immutable on every update path, and `updated_at`
--   advances only when canonical observed state actually changes.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

create table openorc.github_issues (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    repository_id uuid not null,
    github_issue_id bigint not null check (github_issue_id > 0),
    issue_number bigint not null check (issue_number > 0),
    title text not null check (title ~ '\S'),
    body text,
    state text not null check (state in ('open', 'closed')),
    requirements_fingerprint text not null
        check (requirements_fingerprint ~ '^[0-9a-f]{64}$'),
    provider_updated_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Hooks the composite foreign key used by Workspace-owned children so a
    -- projection row's direct Workspace scope cannot disagree with the
    -- owning Repository's Workspace.
    unique (id, workspace_id),

    -- One canonical projection per Workspace Repository per stable GitHub
    -- issue identity. The stable ID — never the number — is the identity.
    unique (repository_id, github_issue_id),

    -- Durable race backstop for issue-number reuse: two concurrent
    -- reconciliations can never insert the same number under different
    -- stable identities. A conflicting number is classified from re-read
    -- durable state and never silently rebinds identity.
    unique (repository_id, issue_number)
);

-- The Workspace ownership edge, declared with its explicit deletion action
-- (the deletion-ownership vocabulary, issue #27): true ownership — the
-- projection exists solely within its Workspace Repository's aggregate.
alter table openorc.github_issues
    add constraint github_issues_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id)
    references openorc.repositories (id, workspace_id)
    on delete cascade;

-- Composite-foreign-key hook for Workspace-owned children (the #53/#57
-- pattern): the projection's (repository_id, workspace_id) edge above
-- requires the parent hook to make cross-Workspace scope disagreement
-- unrepresentable.
alter table openorc.repositories
    add constraint repositories_id_workspace_id_key unique (id, workspace_id);
