-- Owner spending authorization: the Task's authorized budget (issue #240,
-- Pre-F cost accounting).
--
-- ``openorc.tasks.authorized_task_budget_usd numeric`` is the per-Task
-- spending authorization a successful Owner Start established: the
-- then-current Workspace ``default_task_budget_usd`` copied atomically with
-- the guarded ``ready_to_plan -> planning`` or ``ready_to_plan -> queued``
-- transition (the same statement that freezes the source requirements
-- baseline, issue #233). It is the durable standing Owner authorization for
-- OpenOrc to interrupt autonomous work when observed spend reaches it — no
-- second approval to stop is ever needed; a fresh Owner authorization is
-- required only for additional spend/continuation (F4b, issue #230).
--
-- Durable properties carried by this migration:
--
-- - Nullable with a strictly-positive CHECK: NULL means no spending policy
--   is authorized for this Task — disabled, never zero and never unlimited.
--   Absence is exactly the pre-migration state, so Tasks that started before
--   this migration retain a disabled budget and receive no retroactive
--   enforcement (no backfill).
-- - The value is immutable once captured: the only writer is the guarded
--   start-transition capture (``coalesce`` conditioned on the still-NULL
--   value), so a queued Task keeps the authorization it queued with when
--   capacity later admits it, an unsuccessful or stale Start captures
--   nothing, and later Workspace default edits never rewrite a started
--   Task's authorization. The CHECK below also rejects a present value on a
--   ``ready_to_plan`` row — a never-started Task cannot carry an
--   authorization, because the capture commits only together with the start
--   transition. A later cancelled attempt keeps the value it started with as
--   immutable historical configuration, exactly like a frozen baseline.
-- - The Task's OBSERVED expenditure is deliberately not stored here or
--   anywhere on the Task row: it is derived by summing the retained known
--   per-session cost checkpoints of every TaskAgentSession matching the
--   Task, so the aggregate can never silently disagree with its
--   constituents (one fact, one home).
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.tasks
    add column authorized_task_budget_usd numeric
    check (
        authorized_task_budget_usd is null
        or (
            authorized_task_budget_usd > 0
            and status <> 'ready_to_plan'
        )
    );