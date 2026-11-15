-- The detached session establishment (issue #324): the mandatory
-- Producer/Reviewer initialization becomes an enqueued, independently
-- completed Hub run. ``openorc.task_hub_run_receipts`` gains the
-- ``session_initialization`` operation — its single-outstanding invariant
-- per binding carries over unchanged (exactly one initialization run per
-- Task/role binding) — and the minimum separate provisional establishment
-- evidence a CONNECTING binding deliberately does not carry:
--
-- - ``created_external_session_id`` is the provisional external identity
--   the allocation returned. The authoritative binding keeps its NULL
--   external identity until a validated ``session_ready`` finalizes it;
--   this column is the separate durable fact the observation registration
--   proves against and the settlement resolves within the binding's own
--   Connection scope.
-- - ``effective_config_snapshot`` is the dispatch-composed non-secret
--   configuration selection the later readiness finalization binds into
--   the durable READY snapshot, so the fresh construction and the bound
--   snapshot agree by construction across the process boundary.
-- - ``created_session_provenance`` optionally carries the runtime-reported
--   provenance facts (absence stays valid).
--
-- All three are nullable at table level: existing and future
-- non-initialization receipts carry none of them, and the original
-- operation CHECK is replaced by the widened named vocabulary constraint.
-- The conditional coherence CHECKs enforce the per-operation shape: a
-- ``session_initialization`` receipt must carry a nonempty provisional
-- identity and a canonical JSON-object snapshot (provenance optional), and
-- every other operation must carry none. Rows still carry only safe
-- identity/configuration facts — never prompts, initialization Markdown,
-- transcripts, credentials, or SDK payloads.
alter table openorc.task_hub_run_receipts
    drop constraint task_hub_run_receipts_operation_check,
    add column created_external_session_id text,
    add column effective_config_snapshot jsonb,
    add column created_session_provenance jsonb,
    add constraint task_hub_run_receipts_operation_vocab check (
        operation in (
            'planning_bootstrap_prepare',
            'planning_bootstrap_transition',
            'initial_plan_dispatch',
            'planning_revise_dispatch',
            'reviewer_review',
            'implementation_execution',
            'pr_remediation_execution',
            'pr_compose_dispatch',
            'session_resume',
            'session_initialization'
        )
    ),
    add constraint task_hub_run_receipts_provisional_identity_coherence check (
        (
            operation = 'session_initialization'
            and created_external_session_id is not null
            and btrim(created_external_session_id) <> ''
        ) or (
            operation <> 'session_initialization'
            and created_external_session_id is null
        )
    ),
    add constraint task_hub_run_receipts_provisional_snapshot_coherence check (
        (
            operation = 'session_initialization'
            and effective_config_snapshot is not null
            and jsonb_typeof(effective_config_snapshot) = 'object'
        ) or (
            operation <> 'session_initialization'
            and effective_config_snapshot is null
        )
    ),
    add constraint task_hub_run_receipts_provisional_provenance_coherence check (
        created_session_provenance is null
        or (
            operation = 'session_initialization'
            and jsonb_typeof(created_session_provenance) = 'object'
        )
    );

-- The provisional-identity lookup the establishment settlement resolves:
-- the Connection/Workspace scope is enforced through the authoritative
-- binding join (receipts deliberately carry no connection_id; external
-- identifiers never match across Connections).
create index task_hub_run_receipts_created_session_idx
    on openorc.task_hub_run_receipts (created_external_session_id)
    where created_external_session_id is not null;

-- The detached bootstrap stage vocabulary (issue #324): the settled-stage
-- chain gains the durable PREPARE-dispatch marker. ``prepare_dispatched``
-- records that the PREPARE control was durably claimed for dispatch
-- strictly before its runtime send, so the post-readiness continuation's
-- replay can never re-dispatch a possibly-delivered control and a raced
-- continuation pair can never both dispatch. It sits between the claim
-- (no stage) and ``prepared`` in the forward-only chain.
alter table openorc.tasks
    drop constraint tasks_planning_bootstrap_stage_vocab,
    add constraint tasks_planning_bootstrap_stage_vocab check (
        planning_bootstrap_stage is null
        or planning_bootstrap_stage in (
            'prepare_dispatched', 'prepared', 'ready_for_planning'
        )
    );