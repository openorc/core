-- The detached plan-result production dispatch context (issue #335).
--
-- The detached initial-PLAN and planning-REVISE dispatches (issues #325/#335)
-- admit the canonical Producer instruction as one independently completed
-- agent turn; the attributable ``plan_result`` is settled later, in a
-- different process, through the shared detached-turn settlement seam. That
-- settlement atomically creates the next immutable ``PlanRevision`` — whose
-- ``repository_base_sha`` is the installation-authenticated default-branch
-- head the dispatch observed before claiming — together with the current
-- revision/token rotation, the send-claim settlement, and the audit event.
-- The settlement is database-only inside the seam's single locked
-- transaction, so the dispatch-time observation must be durable from the
-- claim commit onward.
--
-- ``dispatch_repository_base_sha`` is that fact: the exact repository-base
-- context the canonical instruction was dispatched against, recorded on the
-- plan-kind claim rows in the claim transaction, before any runtime effect.
-- It is the same class of safe, stable identity fact as the PR_COMPOSE
-- dispatch's ``subject_head_sha`` (the approved head identity): a Git SHA
-- only — never a prompt, plan text, review content, transcript, credential,
-- or SDK payload. Pre-#335 residue rows legitimately carry no dispatch
-- context (the blocking path consumed its observation in-process), so the
-- column stays nullable for the plan kinds; going forward the claim services
-- require it, and a receipt can only ever exist for a claim that recorded
-- it. The PR_COMPOSE kind keeps no dispatch base: its subject head is the
-- gate's approved ``subject_head_sha``.
--
-- No new table, no new privilege, no backfill: the forward-only column joins
-- the claim row's existing scope, and the openorc schema lockout established
-- by the create_openorc_schema migration carries isolation.

alter table openorc.task_runtime_send_claims
    add column dispatch_repository_base_sha text,
    add constraint task_runtime_send_claims_dispatch_base_coherence check (
        (
            kind = 'pr_compose_dispatch'
            and dispatch_repository_base_sha is null
        )
        or (
            kind in ('initial_plan_dispatch', 'planning_revise_dispatch')
            and (
                dispatch_repository_base_sha is null
                or dispatch_repository_base_sha ~ '\S'
            )
        )
    );
