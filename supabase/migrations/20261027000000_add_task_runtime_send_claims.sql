-- F2: durable runtime-send claims for the consequential automated
-- Task→agent-runtime dispatches (issue #87).
--
-- The automated workflow's formal consequential interactions must be
-- at-most-once per logical workflow subject across concurrent invocation,
-- replay, worker death, and regular/queue cutover. Some subjects already
-- carry durable claim identities (the E1b planning-bootstrap claim token,
-- the E3a reserved unfinalized ReviewIteration, the E6/E9 Execution claim),
-- but three sends had no committed pre-send identity:
--
-- - the initial PLAN dispatch (E2): sent before the first PlanRevision
--   existed, so a replay or worker death between the send and its
--   finalization could duplicate the conversation turn;
-- - the planning REVISE dispatch (E2): routed to the exact finalized
--   CHANGES_REQUESTED planning ReviewIteration before the next PlanRevision
--   existed;
-- - the PR_COMPOSE dispatch (E8): sent before the canonical PR existed.
--
-- ``openorc.task_runtime_send_claims`` is the purpose-limited durable
-- identity for exactly those three kinds. It is not a generic ledger: the
-- closed ``kind`` CHECK, the per-kind subject-form coherence CHECK, and the
-- partial unique indexes make each row a one-shot claim of one exact
-- subject, and the E3/E6/E9 claim state is deliberately NOT replicated here.
-- ``PlanRevision`` is immutable produced output and is never a pre-send
-- claim.
--
-- Rows carry only safe identity/outcome facts: the Task/Workspace scope,
-- the Producer session binding, the closed kind, the exact source subject
-- (the finalized planning ReviewIteration for planning REVISE; the approved
-- PR_AUTHORIZATION OwnerGate plus its exact approved head SHA for
-- PR_COMPOSE), a database-stamped claim instant, and a forward-only status
-- with a small canonical JSONB outcome. Prompts, plan text, review content,
-- reasoning, ``pr_result`` bodies, credentials, and SDK data never enter
-- claims.
--
-- The forward-only status vocabulary: ``claimed`` is the committed pre-send
-- claim whose formal result is still outstanding (the conservatively
-- potentially-dispatched record for transport timeouts, connection loss,
-- invalid or missing formal replies, worker death, and failed finalization
-- alike); ``settled`` records a validated formal result that completed the
-- operation; and ``known_nondelivery`` records an error positively proven to
-- precede any effect. An unresolved claim is never replayed; recovery policy
-- is not implemented here (F4b owns it later).
--
-- Deletion ownership follows the established classification: the claims are
-- Task-owned operational rows that cascade from their Task and Workspace;
-- the Producer session binding, the planning ReviewIteration, and the
-- PR_AUTHORIZATION OwnerGate are the subjects each claim is meaningless
-- without, so those edges cascade too — as COMPOSITE subject foreign keys,
-- so a claim can never reference another Task's or another Workspace's
-- subject. The hooks are the existing ``unique (id, task_id, workspace_id)``
-- identities on task_agent_sessions, review_iterations, and owner_gates. No
-- new privilege is granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

create table openorc.task_runtime_send_claims (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    kind text not null check (kind in (
        'initial_plan_dispatch', 'planning_revise_dispatch', 'pr_compose_dispatch'
    )),
    task_agent_session_id uuid not null,
    review_iteration_id uuid,
    owner_gate_id uuid,
    subject_head_sha text check (subject_head_sha is null or subject_head_sha ~ '\S'),
    status text not null default 'claimed' check (status in (
        'claimed', 'settled', 'known_nondelivery'
    )),
    outcome jsonb not null default '{}'::jsonb,
    claimed_at timestamptz not null default now(),
    settled_at timestamptz,
    updated_at timestamptz not null default now(),
    foreign key (workspace_id)
        references openorc.workspaces (id)
        on delete cascade,
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id)
        on delete cascade,
    foreign key (task_agent_session_id, task_id, workspace_id)
        references openorc.task_agent_sessions (id, task_id, workspace_id)
        on delete cascade,
    foreign key (review_iteration_id, task_id, workspace_id)
        references openorc.review_iterations (id, task_id, workspace_id)
        on delete cascade,
    foreign key (owner_gate_id, task_id, workspace_id)
        references openorc.owner_gates (id, task_id, workspace_id)
        on delete cascade,
    -- Exactly one source-subject form per kind.
    check (
        (
            kind = 'initial_plan_dispatch'
            and review_iteration_id is null
            and owner_gate_id is null
            and subject_head_sha is null
        ) or (
            kind = 'planning_revise_dispatch'
            and review_iteration_id is not null
            and owner_gate_id is null
            and subject_head_sha is null
        ) or (
            kind = 'pr_compose_dispatch'
            and review_iteration_id is null
            and owner_gate_id is not null
            and subject_head_sha is not null
        )
    ),
    -- A terminal status and its settlement stamp are written together.
    check (
        (status in ('settled', 'known_nondelivery')) = (settled_at is not null)
    )
);

-- The F2 bootstrap stage evidence (issue #87): forward-only observational
-- facts about the E1b planning-bootstrap claim's progress, added here
-- because the F2 persistence layer reads them together with the claim
-- token that issue #203 added. ``planning_bootstrap_claimed_at`` is the
-- database-stamped claim instant written atomically with the claim token
-- going forward; it stays nullable because Task rows claimed before this
-- migration legitimately carry a token with no stamped instant (residue
-- whose start instant is honestly unknown). The settled stage and its
-- settlement instant are written together by the forward-only stage
-- advance and never regress, so they are coherent-or-both-null by
-- construction and by CHECK.
alter table openorc.tasks
    add column planning_bootstrap_claimed_at timestamptz,
    add column planning_bootstrap_stage text,
    add column planning_bootstrap_stage_settled_at timestamptz,
    add constraint tasks_planning_bootstrap_stage_vocab check (
        planning_bootstrap_stage is null
        or planning_bootstrap_stage in ('prepared', 'ready_for_planning')
    ),
    add constraint tasks_planning_bootstrap_stage_coherence check (
        (planning_bootstrap_stage is null) = (planning_bootstrap_stage_settled_at is null)
    );

-- The durable one-shot backstops: at most one claim per logical workflow
-- subject per kind, independent of any Task state token. A replayed or raced
-- second claim for the same subject can never commit a second row.
create unique index task_runtime_send_claims_initial_plan_uniq
    on openorc.task_runtime_send_claims (task_id)
    where kind = 'initial_plan_dispatch';

create unique index task_runtime_send_claims_planning_revise_uniq
    on openorc.task_runtime_send_claims (review_iteration_id)
    where kind = 'planning_revise_dispatch';

create unique index task_runtime_send_claims_pr_compose_uniq
    on openorc.task_runtime_send_claims (owner_gate_id)
    where kind = 'pr_compose_dispatch';

create index task_runtime_send_claims_task_id_idx
    on openorc.task_runtime_send_claims (task_id);