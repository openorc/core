-- GitHub issue hierarchy/dependency mirrors and the Task source baseline
-- (Phase 2B, issue #60).
--
-- Establishes the presentation-only relationship mirrors synchronized from
-- fresh authoritative GitHub observations, and the immutable Task source
-- baseline recorded at authoritative Task intake:
--
--   Workspace -> Repository -> GitHubIssue (projection)          (#59)
--   Workspace -> Repository -> {Hierarchy, SubIssues, Dependencies}
--
-- Durable properties carried by this migration:
--
-- - The mirrors are keyed by the subject's stable GitHub issue identity
--   within its Workspace Repository, exactly like the #59 issue projection.
--   Related endpoints are stored as plain stable numeric GitHub facts
--   (`*_github_repository_id`, `*_github_issue_id`): a parent, blocker, or
--   sub-issue repository need not be configured as an OpenOrc Repository in
--   this Workspace, so there are deliberately NO foreign keys on the related
--   endpoints and valid cross-repository relationships stay representable.
--   The local `(repository_id, workspace_id)` edge to repositories is the
--   aggregate-ownership edge only.
-- - `openorc.github_issue_hierarchy` is the parent/sub-issue hierarchy
--   mirror: at most one parent edge per subject issue. Hierarchy is
--   descriptive only and carries no Task-eligibility effect.
-- - `openorc.github_issue_sub_issues` is the observed sub-issue listing
--   mirror of one subject issue, replaced wholesale per observation.
-- - `openorc.github_issue_dependencies` is the blocked-by dependency mirror:
--   one row per authoritative dependency edge GitHub currently reports.
--   The issue's current blocked state is derived from this edge set (an
--   empty set is not blocked); there is deliberately no duplicated boolean.
-- - The mirrors carry no freshness/observation-instant columns: they are,
--   by definition, the last successfully observed projection; a failed or
--   unobservable GitHub read leaves them byte-identical (errors are never
--   reinterpreted as authoritative relationship state, and an authoritative
--   empty observation removes rows rather than stamping anything).
-- - Foreign-key classification follows the deletion-ownership vocabulary
--   (issue #27): each mirror is a true-ownership edge of its Workspace
--   Repository's aggregate (`(repository_id, workspace_id) -> repositories`
--   ON DELETE CASCADE, the #59 pattern; the repositories `(id, workspace_id)`
--   hook already exists).
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

create table openorc.github_issue_hierarchy (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    repository_id uuid not null,
    github_issue_id bigint not null check (github_issue_id > 0),
    parent_github_repository_id bigint not null
        check (parent_github_repository_id > 0),
    parent_github_issue_id bigint not null check (parent_github_issue_id > 0),

    -- One canonical parent edge per Workspace Repository per stable GitHub
    -- issue identity; also the subject lookup path.
    unique (repository_id, github_issue_id)
);

alter table openorc.github_issue_hierarchy
    add constraint github_issue_hierarchy_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id)
    references openorc.repositories (id, workspace_id)
    on delete cascade;

create table openorc.github_issue_sub_issues (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    repository_id uuid not null,
    github_issue_id bigint not null check (github_issue_id > 0),
    child_github_repository_id bigint not null
        check (child_github_repository_id > 0),
    child_github_issue_id bigint not null check (child_github_issue_id > 0),

    -- One observed child edge per related endpoint per subject issue.
    unique (repository_id, github_issue_id, child_github_repository_id, child_github_issue_id)
);

alter table openorc.github_issue_sub_issues
    add constraint github_issue_sub_issues_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id)
    references openorc.repositories (id, workspace_id)
    on delete cascade;

create table openorc.github_issue_dependencies (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    repository_id uuid not null,
    github_issue_id bigint not null check (github_issue_id > 0),
    blocker_github_repository_id bigint not null
        check (blocker_github_repository_id > 0),
    blocker_github_issue_id bigint not null check (blocker_github_issue_id > 0),

    -- One observed blocked-by edge per blocker endpoint per subject issue.
    unique (repository_id, github_issue_id, blocker_github_repository_id, blocker_github_issue_id)
);

alter table openorc.github_issue_dependencies
    add constraint github_issue_dependencies_repository_id_workspace_id_fkey
    foreign key (repository_id, workspace_id)
    references openorc.repositories (id, workspace_id)
    on delete cascade;

-- The immutable Task source baseline: the exact requirements fingerprint of
-- the issue as freshly reconciled at intake, against which later B3/B6
-- source-drift detection compares. Lowercase hex SHA-256, the same canonical
-- digest vocabulary as the #59 issue projection. It is never rewritten when
-- GitHub requirements change, and the entire issue body/title is
-- deliberately NOT duplicated onto the Task.
alter table openorc.tasks
    add column source_requirements_fingerprint text not null
        check (source_requirements_fingerprint ~ '^[0-9a-f]{64}$');