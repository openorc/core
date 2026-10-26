-- COST_LIMIT TaskBlock reason (issue #240, Pre-F cost accounting).
--
-- Extends the settled v1 TaskBlock reason vocabulary with the additive
-- Owner-authorized spending-policy reason: ``cost_limit`` marks the durable
-- blocking condition entered when a Task's retained known session cost sum
-- reaches its non-NULL authorized budget. The block is the same E10b
-- Owner-attributed blocking composition as the interactive Owner Abort
-- (issue #228) — OWNER provenance, ``owner_abort`` control context, exact
-- one-shot Interrupt intents, and the guarded ``blocked`` transition —
-- established by the persisted standing budget decision rather than by a
-- live Owner command.
--
-- This is the ONLY vocabulary change: no new primary Task status, no new
-- OwnerGate type, no new RuntimeRequest kind, and no new WorkflowEvent type.
-- Reason-specific resolution (budget increase, continuation, safe
-- re-dispatch) belongs entirely to F4b (issue #230), which must require
-- explicit additional spending authorization; COST_LIMIT never grants an
-- automatic retry, a replacement session, or implicit approval to spend
-- more.
--
-- The original inline CHECK on ``openorc.task_blocks.reason`` is replaced
-- by an explicitly named constraint so later vocabulary evolution stays
-- reviewable. Resolved blocks retain their reason and context intact, and
-- no backfill is needed: existing rows already satisfy the extended
-- vocabulary. No privileges are granted here: the openorc schema lockout
-- established by the create_openorc_schema migration carries isolation, and
-- runtime access is backend-only.

alter table openorc.task_blocks drop constraint task_blocks_reason_check;

alter table openorc.task_blocks
    add constraint task_blocks_reason_check check (reason in (
        'review_failure', 'runtime_failure', 'agent_session_lost',
        'connection_unavailable', 'invalid_credentials',
        'external_operation_uncertain', 'stale_operation',
        'github_source_changed', 'runtime_request_rejected',
        'pr_closed_unmerged', 'owner_action_required', 'cost_limit',
        'unknown'
    ));