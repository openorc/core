-- OpenOrc Task aggregate persistence (Phase 1, issue #21).
--
-- Establishes the durable Task aggregate root that later workflow services
-- build on:
--
--   Workspace -> Project -> Repository -> Task
--
-- Durable properties carried by this migration:
--
-- - A Task belongs to one Workspace-scoped Repository and is backed by one
--   stable GitHub issue identity. ``github_issue_id`` is the stable external
--   identity used for reconciliation — it survives issue title/state changes
--   and repository-local presentation. ``github_issue_number`` is
--   repository-local address/observed metadata only and is never the sole
--   identity. Issue title/state are GitHub-owned presentation facts with no
--   Phase 1 consumer; they are deliberately not stored.
-- - At most one current (non-archived) Task may exist per
--   (repository_id, github_issue_id). The partial unique index below makes
--   two concurrent open attempts for the same issue impossible while leaving
--   archived attempts as unrestricted history, so a fresh Task can be
--   created for an open issue after a CANCELLED attempt and for a previously
--   completed issue if GitHub later reopens it. The same external issue may
--   be tracked independently in different Workspaces (repositories are
--   already Workspace-canonical).
-- - Archival is represented independently from terminal outcome:
--   ``archived_at`` records that an attempt is no longer current, while
--   ``status`` preserves WHICH terminal outcome the attempt reached. The
--   settled lifecycle invariant below makes both directions hold:
--   every CANCELLED or COMPLETED attempt is archived history, and every
--   nonterminal attempt is current. CANCELLED and COMPLETED remain
--   distinguishable archived history.
-- - ``status`` is the canonical coarse primary workflow state. It never
--   absorbs subordinate facts that have their own canonical homes: Owner
--   gate type/subject belong on OwnerGate (v1 gate types:
--   IMPLEMENTATION_AUTHORIZATION, PR_AUTHORIZATION, MERGE_DECISION,
--   REVIEW_RESOLUTION), review state/outcomes belong on ReviewLoop and
--   ReviewIteration, and blocking reasons/context belong on TaskBlock.
--   ``ready_to_plan`` is an eligible leaf Task before autonomous work
--   starts; ``queued`` is normal runtime-capacity backpressure, not failure;
--   ``planning``/``implementing``/``reviewing`` are the active coarse
--   workflow phases; ``waiting_for_owner`` is the primary state shared by
--   every Owner-facing gate wait; ``blocked`` is the primary state for
--   blocking conditions. Only ``cancelled`` and ``completed`` are terminal.
-- - ``canonical_feature_branch`` is the Task's nullable canonical
--   feature-branch identity, persisted only once later workflow/runtime
--   logic has verified and bound the Producer-created branch. Branch
--   ownership is a Task-level fact, never an Execution-level one: the
--   partial unique index makes two current Tasks in one Repository owning
--   the same non-null canonical branch impossible. Retries, later
--   Executions, and PR remediation for a Task continue on that Task's
--   branch. Binding is a one-time operation: the branch is created NULL and
--   bound exactly once, and a current Task never releases or switches its
--   canonical branch — archival releases the ownership, so a fresh Task
--   after reopen may bind it again.
-- - ``state_token`` is the opaque optimistic-concurrency state token. Every
--   authoritative Task-state mutation must replace it, and mutations are
--   conditional on the caller's expected token: stale operations fail
--   instead of mutating newer subjects. It is not history/revision
--   numbering.
-- - ``current_plan_revision_id``/``current_owner_gate_id`` are nullable
--   current-object pointer fields only. They identify related records and
--   never duplicate those records' content or review outcomes (one fact,
--   one home). PlanRevision and OwnerGate tables do not exist yet (issues
--   #23/#24), so no foreign keys are declared here; those issues add the
--   FK and same-Task/Workspace consistency constraints. There is
--   deliberately no singular current Execution pointer.
-- - ``workspace_id`` is carried directly on the Workspace-owned row, and the
--   composite foreign key keeps the direct scope in agreement with the
--   Repository's Workspace. ``openorc.repositories`` gains the
--   unique (id, workspace_id) hook here because only projects carried it
--   until now.
-- - Ordinary lifecycle archives. Explicit Owner purge of archived internal
--   Task data is a distinct, separately authorized operation and must never
--   imply GitHub mutation; nothing in this row is GitHub-authoritative.
-- - No privileges are given to browser-facing roles: the schema-level lockout
--   established by the create_openorc_schema migration carries isolation, and
--   runtime access is backend-only.

-- Hook for Workspace-owned children of a Repository: lets a child's direct
-- Workspace scope be constrained to agree with parent ownership, exactly
-- like the projects and connections hooks.
alter table openorc.repositories
    add constraint repositories_id_workspace_id_uniq unique (id, workspace_id);

create table openorc.tasks (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    repository_id uuid not null,
    -- Stable GitHub issue identity. Survives issue title/state changes;
    -- the repository-local number below is observed address metadata.
    github_issue_id bigint not null check (github_issue_id > 0),
    github_issue_number integer not null check (github_issue_number > 0),
    -- Canonical coarse primary workflow state. Subordinate facts (gate
    -- type/subject, review state/outcomes, blocking context) live on their
    -- own aggregates, never here.
    status text not null check (status in (
        'ready_to_plan', 'queued', 'planning', 'waiting_for_owner',
        'implementing', 'reviewing', 'blocked', 'cancelled', 'completed'
    )),
    -- NULL while the attempt is current; set once the attempt becomes
    -- archived history. Terminal outcome stays distinguishable through
    -- status, never through archival alone.
    archived_at timestamptz,
    -- Task-level canonical feature-branch ownership. NULL until later
    -- workflow logic verifies and binds the Producer-created branch; binding
    -- is one-time (a current Task never releases or switches its branch) and
    -- archival releases the ownership for future Tasks.
    canonical_feature_branch text check (
        canonical_feature_branch is null or canonical_feature_branch ~ '\S'
    ),
    -- Opaque optimistic-concurrency state token: every authoritative
    -- Task-state mutation replaces it. Not history/revision numbering.
    state_token uuid not null default gen_random_uuid(),
    -- Nullable current-object pointers only (one fact, one home). The
    -- PlanRevision (#23) and OwnerGate (#24) migrations add the foreign keys
    -- and same-Task/Workspace consistency constraints once those tables
    -- exist. Deliberately no current-Execution pointer.
    current_plan_revision_id uuid,
    current_owner_gate_id uuid,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Repository's Workspace.
    foreign key (repository_id, workspace_id)
        references openorc.repositories (id, workspace_id),

    -- Settled lifecycle invariant: archived_at IS NULL iff the status is
    -- nonterminal. Both CANCELLED and COMPLETED attempts become archived
    -- history and remain distinguishable through status.
    check (
        (archived_at is null and status not in ('cancelled', 'completed'))
        or
        (archived_at is not null and status in ('cancelled', 'completed'))
    )
);

create index tasks_workspace_active_idx
    on openorc.tasks (workspace_id)
    where archived_at is null;

create index tasks_workspace_status_idx
    on openorc.tasks (workspace_id, status);

-- Full attempt history for one stable GitHub issue within one Repository
-- (archived and current alike).
create index tasks_repository_issue_idx
    on openorc.tasks (repository_id, github_issue_id);

-- One current (non-archived) Task per stable GitHub issue identity within a
-- Repository. Archived attempts are outside the index, so a fresh Task can
-- follow a cancelled or completed attempt for the same issue. Also serves
-- current-Task resolution by (repository_id, github_issue_id).
create unique index tasks_repository_issue_current_uniq
    on openorc.tasks (repository_id, github_issue_id)
    where archived_at is null;

-- Exclusive Task-level ownership of a canonical feature branch among the
-- current (non-archived) Tasks of one Repository. Archived attempts release
-- the branch. Also serves current canonical-branch ownership lookup.
create unique index tasks_repository_canonical_branch_current_uniq
    on openorc.tasks (repository_id, canonical_feature_branch)
    where archived_at is null and canonical_feature_branch is not null;

