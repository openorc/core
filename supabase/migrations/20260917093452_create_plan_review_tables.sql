-- OpenOrc planning and review persistence primitives (Phase 1, issue #23).
--
-- Establishes the durable planning/review history that later workflow
-- services build on:
--
--   Workspace -> Project -> Repository -> Task -> PlanRevision
--                                            ReviewLoop -> ReviewIteration
--
-- Durable properties carried by this migration:
--
-- - PlanRevisions are versioned, immutable Producer artifacts: the exact
--   plan content authored for one Task attempt together with the
--   repository-base context it was created and reviewed against. Versioning
--   is a per-Task ``revision_number`` sequence: a changed plan is a fresh
--   revision, never a mutation of an existing one, and the Task's planning
--   history remains complete and intact. No update path exists for a
--   committed revision; revisions are not rewritten to manufacture a
--   different past.
-- - ``repository_base_sha`` is audit/context metadata for the plan's
--   creation/review context. Later movement of the repository base does not
--   by itself invalidate a previously accepted/authorized plan revision and
--   never authorizes rewriting one.
-- - ReviewLoops own the loop's purpose, effective iteration limit, and
--   current lifecycle. v1 purposes are exactly PLANNING and PR_REVIEW: no
--   implementation-review loop and no speculative additional review-loop
--   categories exist.
-- - ``iteration_limit`` has no database default. The v1 default (5) and any
--   later Workspace-level configurability live at the configured setting
--   boundary; the effective limit a loop was created with is stored on the
--   loop and preserved for historical reconstruction, never hard-coded into
--   individual iterations.
-- - Loop lifecycle is minimal: ``open`` (accepting iterations) or ``closed``
--   (no more iterations). ``closed_at`` is the semantic closure timestamp,
--   non-NULL exactly when the status is closed. Closure policy and
--   max-iteration transitions belong to later orchestration services; this
--   is the durable lifecycle fact only.
-- - ReviewIterations are immutable once a result is recorded. The complete
--   finalized Reviewer result — ``outcome``, ``summary``, ``findings``,
--   ``decided_at`` — finalizes atomically: an unfinalized iteration carries
--   all four facts NULL, a finalized iteration carries all four non-NULL,
--   and no rewrite path exists afterwards. Revisions and results are not
--   rewritten to manufacture a different past.
-- - Reviewer outcomes are exactly ACCEPTED and CHANGES_REQUESTED.
--   Provider/runtime/protocol failures are not reviewer judgments and are
--   structurally excluded from the outcome vocabulary.
-- - The settled Reviewer-result coherence is durable: ACCEPTED clears with
--   zero findings (nothing unresolved); CHANGES_REQUESTED retains at least
--   one finding of unresolved work. The coherence CHECK is CASE-guarded on
--   jsonb_typeof so a non-array findings document is rejected cleanly as a
--   plain CheckViolation, never by an evaluation error. A later
--   REVIEW_RESOLUTION consumes the retained findings and iteration history.
-- - ``findings`` is the protocol-settled Reviewer-result findings document:
--   a JSON array persisted verbatim as historical evidence. Per-item
--   protocol schema validation (finding objects with non-empty summary and
--   details) belongs to Phase 2 protocol validation in ``openorc.protocol``;
--   persistence never parses or reshapes the document. The ``review_result``
--   wire envelope (``type``, ``schema_version``) remains a protocol concern
--   and is deliberately not stored on the iteration: the semantic result is
--   persisted as separate fields, not the envelope.
-- - Each ReviewIteration references the exact subject reviewed. v1 planning
--   iterations bind a specific PlanRevision of the same Task and Workspace
--   (composite foreign keys enforce the same-Task agreement with the loop
--   and the revision). PR review support later binds one TaskPullRequest
--   plus exact head SHA on this same table — no second review-history
--   model is created.
-- - Iteration numbering is unique per ReviewLoop.
-- - ``tasks.current_plan_revision_id`` gains its foreign key here: a
--   non-NULL pointer must reference a PlanRevision whose
--   (id, task_id, workspace_id) matches the pointing Task's identity and
--   Workspace. The pointer identifies the authoritative current revision;
--   it never duplicates plan content or review outcomes on the Task row
--   (one fact, one home). Moving the pointer to a newer same-Task revision
--   leaves every older revision intact.
-- - ``workspace_id`` is carried directly on every Workspace-owned row, and
--   the composite foreign keys keep the direct scope in agreement with Task
--   ownership (the tasks ``unique (id, workspace_id)`` hook from the
--   session migration).
-- - Ordinary lifecycle archives Tasks; these tables are Task-scoped
--   internal history. Explicit Owner purge of archived internal Task data
--   is a distinct, separately authorized operation that never implies
--   GitHub mutation.
-- - No privileges are given to browser-facing roles: the schema-level
--   lockout established by the create_openorc_schema migration carries
--   isolation, and runtime access is backend-only.

-- PlanRevisions: versioned, immutable Producer artifacts. Deliberately no
-- ``updated_at``: an immutable artifact has no update path and therefore no
-- last-change timestamp.
create table openorc.plan_revisions (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- Per-Task version sequence: a changed plan is a fresh revision. The
    -- caller supplies the next number; the unique constraint makes a
    -- duplicate version impossible.
    revision_number integer not null check (revision_number > 0),
    -- The exact Producer plan content, preserved verbatim and immutable.
    content text not null check (content ~ '\S'),
    -- Audit/context metadata for the plan's creation/review context. Later
    -- repository-base movement neither invalidates an accepted/authorized
    -- revision nor authorizes rewriting one.
    repository_base_sha text not null check (repository_base_sha ~ '\S'),
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- One revision per Task version. Also serves Task planning-history
    -- lookup (revisions ordered by version), so no separate index is
    -- needed.
    unique (task_id, revision_number),

    -- Hook for the Task's same-Task/Workspace current-plan pointer (mirrors
    -- the projects, repositories, connections, and tasks hooks).
    unique (id, task_id, workspace_id)
);

-- ReviewLoops: one governed review loop per purpose on a Task. Deliberately
-- no ``updated_at``: the loop's only mutation is closure, and ``closed_at``
-- is the semantic stamp for exactly that mutation.
create table openorc.review_loops (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- v1 purposes are exactly PLANNING and PR_REVIEW.
    purpose text not null check (purpose in ('planning', 'pr_review')),
    -- The effective iteration limit used by THIS loop, preserved for
    -- historical reconstruction. No database default: the v1 default (5)
    -- lives at the configured setting boundary.
    iteration_limit integer not null check (iteration_limit > 0),
    -- Minimal lifecycle: open (accepting iterations) or closed (no more
    -- iterations). Closure policy belongs to later orchestration.
    status text not null check (status in ('open', 'closed')),
    -- Semantic closure timestamp: non-NULL exactly when the status is
    -- closed.
    closed_at timestamptz,
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- ``closed_at`` is non-NULL exactly when the status is closed.
    check (
        (status = 'closed' and closed_at is not null)
        or (status = 'open' and closed_at is null)
    ),

    -- Hook for the iteration's composite foreign keys.
    unique (id, task_id, workspace_id)
);

-- ReviewLoop status lookup for a Task (for example, the open planning
-- loop).
create index review_loops_task_status_idx
    on openorc.review_loops (task_id, status);

-- ReviewIterations: one immutable-once-finalized review of one exact
-- subject. Deliberately no ``updated_at``: the row's only mutation is the
-- finalize, and ``decided_at`` is the semantic stamp for exactly that.
create table openorc.review_iterations (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    review_loop_id uuid not null,
    -- Iteration numbering is unique per ReviewLoop. Also serves iteration
    -- lookup within the loop, so no separate index is needed.
    iteration_number integer not null check (iteration_number > 0),
    -- The exact subject reviewed: v1 planning iterations bind a specific
    -- PlanRevision of the same Task. PR review support (#25) extends this
    -- same table with one TaskPullRequest plus exact head SHA binding — no
    -- second review-history model.
    plan_revision_id uuid,
    -- The complete finalized Reviewer result. The four facts finalize
    -- atomically (see the coherence CHECK below) and are immutable once
    -- set. Reviewer outcomes are exactly ACCEPTED and CHANGES_REQUESTED;
    -- provider/runtime/protocol failures are not outcomes.
    outcome text check (outcome in ('accepted', 'changes_requested')),
    -- The Reviewer's non-empty summary, finalized with the outcome.
    summary text check (summary is null or summary ~ '\S'),
    -- The protocol-settled Reviewer-result findings document: a JSON array
    -- persisted verbatim as historical evidence. Per-item protocol schema
    -- validation belongs to Phase 2 protocol validation in
    -- ``openorc.protocol``; persistence never parses or reshapes the
    -- document. The ``review_result`` wire envelope (``type``,
    -- ``schema_version``) remains a protocol concern and is deliberately
    -- not stored here.
    findings jsonb check (jsonb_typeof(findings) = 'array'),
    -- Semantic finalize timestamp, stamped atomically with the result.
    decided_at timestamptz,
    created_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- Workspace scope and Task must agree with the loop's.
    foreign key (review_loop_id, task_id, workspace_id)
        references openorc.review_loops (id, task_id, workspace_id),

    -- The reviewed PlanRevision belongs to the same Task and Workspace as
    -- the iteration: the exact-subject rule is durable.
    foreign key (plan_revision_id, task_id, workspace_id)
        references openorc.plan_revisions (id, task_id, workspace_id),

    -- Iteration numbering is unique per ReviewLoop.
    unique (review_loop_id, iteration_number),

    -- v1 subjects are planning revisions: the exact PlanRevision under
    -- review. PR-review subject binding arrives with TaskPullRequest
    -- persistence (#25) on this same table.
    check (plan_revision_id is not null),

    -- Finalize coherence: the four result facts move atomically. An
    -- unfinalized iteration carries all four NULL; a finalized iteration
    -- carries all four non-NULL. No partially recorded result can exist.
    check (
        (
            outcome is null and summary is null
            and findings is null and decided_at is null
        )
        or (
            outcome is not null and summary is not null
            and findings is not null and decided_at is not null
        )
    ),

    -- Settled Reviewer-result coherence: ACCEPTED clears with zero
    -- findings; CHANGES_REQUESTED retains at least one finding of
    -- unresolved work. The CASE guard keeps this CHECK well-formed for
    -- arbitrary JSON: a non-array findings document fails this CHECK
    -- cleanly (together with the jsonb_typeof column CHECK above) as a
    -- plain CheckViolation, never as an evaluation error from
    -- jsonb_array_length.
    check (
        outcome is null
        or case
            when jsonb_typeof(findings) = 'array' then
                (
                    (outcome = 'accepted' and jsonb_array_length(findings) = 0)
                    or
                    (outcome = 'changes_requested' and jsonb_array_length(findings) >= 1)
                )
            else false
        end
    )
);

-- Same-Task/Workspace current-plan pointer consistency: a non-NULL
-- tasks.current_plan_revision_id must reference a PlanRevision whose
-- (id, task_id, workspace_id) matches the pointing Task's identity and
-- Workspace. NULL passes; cross-Task or cross-Workspace corruption is
-- rejected durably. The pointer identifies the authoritative current
-- revision without duplicating its content or review outcomes (one fact,
-- one home).
alter table openorc.tasks
    add constraint tasks_current_plan_revision_fk
    foreign key (current_plan_revision_id, id, workspace_id)
    references openorc.plan_revisions (id, task_id, workspace_id);





