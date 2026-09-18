-- OpenOrc Task pull request persistence and exact-head review identity
-- (Phase 1, issue #25).
--
-- Establishes the durable TaskPullRequest record that later workflow
-- services build on, and binds PR-subject review/gate records to the exact
-- PR identity:
--
--   Workspace -> Project -> Repository -> Task -> TaskPullRequest
--                                       \-> ReviewIteration (PR subject)
--                                       \-> OwnerGate (PR-subject forms)
--
-- Durable properties carried by this migration:
--
-- - One Task has exactly one TaskPullRequest for its whole v1 lifetime:
--   ``unique (task_id)`` is full-history with no partial predicate. There
--   are no replacement-PR rows and no speculative PR-replacement/adoption
--   history: a closed-unmerged PR remains the Task's canonical PR record,
--   and later workflow services block against it rather than silently
--   replacing it. The constraint doubles as the per-Task PR lookup.
-- - Stable GitHub PR identity is persisted separately from
--   repository-local PR address metadata: ``github_pr_id`` is the stable
--   external identity used for reconciliation and per-Workspace
--   canonicalization (it survives PR renumbering and identity transfer);
--   ``github_pr_number`` is the repository-local address captured at
--   creation and never identity. ``unique (workspace_id, github_pr_id)``
--   keeps one canonical PR record per GitHub PR identity per Workspace and
--   doubles as the identity lookup.
-- - Mutable observed reconciliation state — ``head_ref``, ``base_ref``,
--   ``head_sha``, ``state``, ``merged_at`` — is updated in place as
--   reconciliation observes GitHub. ``head_sha`` is the PR's current
--   observed head and changes across remediation rounds while the PR
--   identity stays stable; the exact reviewed head SHAs are historical
--   facts on the review records, never on the PR.
-- - ``state`` is the observed GitHub PR lifecycle (open/closed);
--   ``merged_at`` is the observed merge timestamp and is non-NULL only for
--   a closed PR. No richer lifecycle is modeled: GitHub remains the owner
--   of PR truth and OpenOrc persists observed reconciliation state.
-- - Exact-head review identity: a PR ReviewIteration binds the same
--   TaskPullRequest plus the exact reviewed head SHA
--   (``reviewed_head_sha``). Reviewer acceptance identity is the
--   TaskPullRequest plus the exact reviewed head SHA: a changed head
--   invalidates prior acceptance; movement of the PR target/base branch
--   alone does not invalidate acceptance and is not part of the immutable
--   review-subject identity. Review history references the same
--   TaskPullRequest plus per-iteration head SHAs rather than creating a
--   new PR record per iteration.
-- - OwnerGate subject forms are tightened to the settled authority
--   subjects: IMPLEMENTATION_AUTHORIZATION binds the exact review-cleared
--   PlanRevision; PR_AUTHORIZATION binds only the exact committed Producer
--   head SHA (it happens before the canonical PR exists); MERGE_DECISION
--   binds the canonical TaskPullRequest plus the exact reviewed/authorized
--   head SHA; REVIEW_RESOLUTION binds exactly one subject — the exhausted
--   planning PlanRevision, or the PR-review subject as TaskPullRequest
--   plus exact head SHA. A PR-subject gate is never representable as a
--   bare head SHA: once the TaskPullRequest exists, the canonical PR
--   identity is required.
-- - The intended v1 flow around these records: the Producer implements and
--   pushes; OpenOrc reconciles the exact branch head; the Owner authorizes
--   that exact head (PR_AUTHORIZATION); the Producer authors the PR
--   content and OpenOrc performs the GitHub PR side effect; the returned
--   PR identity/head is recorded/reconciled here; the Reviewer reviews
--   that exact PR/head. GitHub PR creation/reconciliation API calls are
--   later service/adapter work and are deliberately not in persistence.

-- Same-Repository Task identity hook: lets TaskPullRequest rows and the
-- gate/iteration subject foreign keys below require Task/Repository/
-- Workspace agreement through one composite foreign key (mirrors the
-- unique (id, workspace_id) hook pattern).
alter table openorc.tasks
    add constraint tasks_id_repository_id_workspace_id_uniq
    unique (id, repository_id, workspace_id);

create table openorc.task_pull_requests (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    -- The PR's owning Repository must agree with the Task's Repository:
    -- a PR cannot attach across incompatible Task/Repository/Workspace
    -- ownership (the composite foreign key below is the durable backstop).
    repository_id uuid not null,
    -- Stable GitHub PR identity: the durable external identity used for
    -- reconciliation, distinct from the repository-local address below.
    github_pr_id bigint not null check (github_pr_id > 0),
    -- Repository-local PR address (address metadata, never identity):
    -- captured at creation; mutable observed metadata must never be sole
    -- identity, so updates to reconciliation state never rewrite this.
    github_pr_number integer not null check (github_pr_number > 0),
    -- Mutable observed reconciliation state: the PR's current head/base
    -- refs and current observed head SHA. ``head_sha`` changes across
    -- remediation rounds while the PR identity stays stable.
    head_ref text not null check (head_ref ~ '\S'),
    base_ref text not null check (base_ref ~ '\S'),
    head_sha text not null check (head_sha ~ '\S'),
    -- Observed GitHub PR lifecycle. No lifecycle default: reconciliation
    -- establishes the observed state explicitly.
    state text not null check (state in ('open', 'closed')),
    -- Observed merge timestamp: non-NULL exactly for a closed (merged) PR.
    merged_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- Task/Repository/Workspace triple agreement: the PR belongs to the
    -- Task's own Repository within the same Workspace.
    foreign key (task_id, repository_id, workspace_id)
        references openorc.tasks (id, repository_id, workspace_id),

    -- One canonical PR record per GitHub PR identity per Workspace (the
    -- per-Workspace canonicalization; doubles as the identity lookup).
    unique (workspace_id, github_pr_id),

    -- Exactly one PR per Task for the whole v1 lifetime (full history, no
    -- partial predicate): no replacement-PR rows exist; a closed-unmerged
    -- PR remains the canonical record. Doubles as the per-Task lookup.
    unique (task_id),

    -- Hook for the review/gate subject foreign keys below (mirrors the
    -- sibling hooks).
    unique (id, task_id, workspace_id),

    -- A merged PR is a closed PR.
    check (merged_at is null or state = 'closed')
);

-- PR-review subject binding on the review-iteration history table
-- (issue #25). The exact reviewed head SHA is immutable history: each
-- finalized PR-review iteration carries the head SHA it reviewed, so PR
-- identity stays stable while ``head_sha`` moves across remediation
-- rounds.
alter table openorc.review_iterations
    add column task_pull_request_id uuid,
    add column reviewed_head_sha text
        check (reviewed_head_sha is null or reviewed_head_sha ~ '\S');

-- v1 planning subjects were previously the only subject form (a bare
-- single-column not-NULL check, auto-named by Postgres as
-- ``review_iterations_plan_revision_id_check``).
-- Replace it with the settled exact-subject coherence: a planning subject
-- is the exact PlanRevision with no PR binding; a PR subject is the exact
-- TaskPullRequest plus the exact reviewed head SHA. Exactly one form per
-- iteration.
alter table openorc.review_iterations
    drop constraint review_iterations_plan_revision_id_check;

alter table openorc.review_iterations
    add constraint review_iterations_subject_form_check
    check (
        (
            plan_revision_id is not null
            and task_pull_request_id is null
            and reviewed_head_sha is null
        )
        or (
            plan_revision_id is null
            and task_pull_request_id is not null
            and reviewed_head_sha is not null
        )
    );

-- The PR subject belongs to the same Task and Workspace as the iteration:
-- the exact-subject rule is durable (mirrors the plan-revision subject FK).
alter table openorc.review_iterations
    add constraint review_iterations_task_pull_request_fk
    foreign key (task_pull_request_id, task_id, workspace_id)
    references openorc.task_pull_requests (id, task_id, workspace_id);

-- OwnerGate PR-subject binding (issue #25). ``task_pull_request_id`` is
-- NULL for plan-subject and pre-PR head-subject gates and set exactly for
-- the PR-subject forms (merge decision, PR-review resolution).
alter table openorc.owner_gates
    add column task_pull_request_id uuid;

-- v1 PR-subject gates were previously representable as a bare head SHA
-- (auto-named as ``owner_gates_check``). Replace with the settled per-type
-- subject coherence: PR_AUTHORIZATION remains the pre-PR exact-head gate
-- (no PR binding); MERGE_DECISION requires the canonical TaskPullRequest
-- plus the exact head SHA; REVIEW_RESOLUTION binds exactly one subject —
-- the exhausted planning PlanRevision, or the PR-review subject as
-- TaskPullRequest plus exact head SHA.
alter table openorc.owner_gates
    drop constraint owner_gates_check;

alter table openorc.owner_gates
    add constraint owner_gates_subject_coherence_check
    check (
        (
            gate_type = 'implementation_authorization'
            and plan_revision_id is not null
            and subject_head_sha is null
            and task_pull_request_id is null
        ) or (
            gate_type = 'pr_authorization'
            and plan_revision_id is null
            and subject_head_sha is not null
            and task_pull_request_id is null
        ) or (
            gate_type = 'merge_decision'
            and plan_revision_id is null
            and subject_head_sha is not null
            and task_pull_request_id is not null
        ) or (
            gate_type = 'review_resolution'
            and (
                (
                    plan_revision_id is not null
                    and subject_head_sha is null
                    and task_pull_request_id is null
                )
                or (
                    plan_revision_id is null
                    and subject_head_sha is not null
                    and task_pull_request_id is not null
                )
            )
        )
    );

-- The PR-subject gate binds a TaskPullRequest of the same Task and
-- Workspace: the exact-subject rule is durable.
alter table openorc.owner_gates
    add constraint owner_gates_task_pull_request_fk
    foreign key (task_pull_request_id, task_id, workspace_id)
    references openorc.task_pull_requests (id, task_id, workspace_id);

