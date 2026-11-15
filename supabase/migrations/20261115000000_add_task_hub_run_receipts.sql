-- The detached Hub-run correlation/settlement seam (issue #322).
--
-- ``openorc.task_hub_run_receipts`` is the durable claim-to-run receipt that
-- binds an already-authorized OpenOrc one-shot claim identity (the F2
-- runtime-send claim, the E6/E9 Execution reservation, or the F8g
-- session-resume record) to the exact authoritative TaskAgentSession
-- binding and, after admission, to the acknowledged native Hub ``runId``.
-- The runId is execution correlation evidence, never OpenOrc workflow
-- authority. Rows carry only safe identity/outcome facts — never prompts,
-- plan/review content, transcripts, credentials, or SDK payloads.
--
-- Durable properties carried by this migration:
--
-- - The forward-only status vocabulary with exact per-status coherence:
--   ``claimed`` (pre-enqueue, no run identity), ``admitted`` (acknowledged
--   runId attached), ``settled`` (one consumed terminal observation routed
--   through the owning services), and ``refused`` (a positively proven
--   pre-effect admission refusal, so a never-dispatched run cannot hold the
--   session's single-outstanding slot).
-- - The single-outstanding-run invariant as a durable backstop: at most one
--   unsettled (``claimed``/``admitted``) receipt per exact authoritative
--   ``task_agent_sessions`` row. The scope is the OpenOrc session binding,
--   never the opaque external identifier: different Connections own
--   independent external-session namespaces, and identifiers that coincide
--   across Connections must never be treated as one session.
-- - The acknowledged ``runId`` binds at most once per session binding
--   (same scoping principle).
-- - The explicitly observed contradictory-run-identity marker
--   (``contradicted_at``) is set once on an outstanding admitted receipt
--   when a ``run.started`` observation carries a concretely different run
--   identity; a contradicted receipt can never settle, so the correlation
--   stays honestly unresolved instead of inventing a match.
-- - Stale-replay rejection reads the durable consumed-sequence markers the
--   settled receipts already record in their outcomes; no separate ledger
--   exists.
-- - ``openorc.task_hub_run_early_completions`` is the smallest safe handoff
--   for the admission race: a terminal observation that resolves to a
--   receipt still ``claimed`` (acknowledgement not yet persisted) is staged
--   on that exact receipt and consumed exactly once by the acknowledgement
--   attach, which then routes it through the ordinary settlement. At most
--   one staged completion exists per receipt — a second is a duplicate
--   delivery, rejected as a no-op. The staged formal candidate is deleted
--   at consumption, so unvalidated agent-produced content never becomes
--   permanent durable history.
--
-- Deletion ownership follows the established classification: the receipts
-- are Task-owned operational rows that cascade from their Task and
-- Producer session binding (composite subject foreign keys so a receipt can
-- never reference another Task's or another Workspace's session), and the
-- staged completions cascade from their receipt. No new privilege is
-- granted here: the openorc schema lockout established by the
-- create_openorc_schema migration carries isolation, and runtime access is
-- backend-only.

create table openorc.task_hub_run_receipts (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    task_agent_session_id uuid not null,
    operation text not null check (operation in (
        'planning_bootstrap_prepare',
        'planning_bootstrap_transition',
        'initial_plan_dispatch',
        'planning_revise_dispatch',
        'reviewer_review',
        'implementation_execution',
        'pr_remediation_execution',
        'pr_compose_dispatch',
        'session_resume'
    )),
    original_attempt_kind text not null check (original_attempt_kind in (
        'runtime_send_claim',
        'planning_bootstrap_claim',
        'review_iteration',
        'execution',
        'owner_control_intent',
        'merge_decision_gate'
    )),
    original_attempt_id uuid not null,
    expected_formal_family text not null,
    run_id text,
    status text not null check (status in (
        'claimed', 'admitted', 'settled', 'refused'
    )),
    outcome jsonb not null default '{}'::jsonb check (jsonb_typeof(outcome) = 'object'),
    contradicted_at timestamptz,
    claimed_at timestamptz not null default now(),
    admitted_at timestamptz,
    settled_at timestamptz,
    updated_at timestamptz not null default now(),
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id)
        on delete cascade,
    foreign key (task_agent_session_id, task_id, workspace_id)
        references openorc.task_agent_sessions (id, task_id, workspace_id)
        on delete cascade,
    -- The exact forward-only lifecycle coherence per status.
    check (
        (status = 'claimed'
            and run_id is null and admitted_at is null and settled_at is null)
        or (status = 'admitted'
            and run_id is not null and admitted_at is not null and settled_at is null)
        or (status = 'settled'
            and run_id is not null and admitted_at is not null and settled_at is not null)
        or (status = 'refused'
            and run_id is null and admitted_at is null and settled_at is not null)
    ),
    -- A settled receipt carries its settlement label; a refused receipt
    -- carries its refusal kind. Terminal rows are immutable outcome records.
    check (
        (status = 'settled') = (outcome ? 'settlement')
        and (status = 'refused') = (outcome ? 'refusal_kind')
    ),
    -- The explicitly observed contradictory-run-identity marker (issue #322)
    -- is set once on an outstanding admitted receipt and permanently blocks
    -- its settlement: a contradicted correlation is never a match.
    check (contradicted_at is null or status = 'admitted')
);

-- The single-outstanding-run invariant: at most one unsettled
-- OpenOrc-dispatched Hub run per exact authoritative session binding.
create unique index task_hub_run_receipts_session_outstanding_uniq
    on openorc.task_hub_run_receipts (task_agent_session_id)
    where status in ('claimed', 'admitted');

-- The acknowledged runId binds at most once per session binding.
create unique index task_hub_run_receipts_session_run_uniq
    on openorc.task_hub_run_receipts (task_agent_session_id, run_id)
    where run_id is not null;

create index task_hub_run_receipts_task_id_idx
    on openorc.task_hub_run_receipts (task_id);

create table openorc.task_hub_run_early_completions (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null,
    task_id uuid not null,
    task_hub_run_receipt_id uuid not null,
    kind text not null check (kind in ('run.completed', 'run.failed')),
    sequence bigint,
    result_candidate text,
    staged_at timestamptz not null default now(),
    foreign key (task_hub_run_receipt_id, task_id, workspace_id)
        references openorc.task_hub_run_receipts (id, task_id, workspace_id)
        on delete cascade
);

-- At most one staged completion per receipt: a second terminal delivery for
-- the same single outstanding run is a duplicate, rejected as a no-op.
create unique index task_hub_run_early_completions_receipt_uniq
    on openorc.task_hub_run_early_completions (task_hub_run_receipt_id);
