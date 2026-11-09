-- Owner spending authorization: the Task's refreshed effective cumulative
-- threshold (issue #291, F8f).
--
-- ``openorc.tasks.effective_cost_threshold_usd numeric`` is the current
-- effective cumulative spending threshold a successful Owner Continue
-- established: the latest retained known cumulative Task spend plus the
-- Task's frozen ``authorized_task_budget_usd`` (one fresh full Task-budget
-- allowance measured from the currently observed spend, including any prior
-- overshoot). It is committed atomically with the Continue's guarded
-- resolution of the exact ``COST_LIMIT`` gate/block and restored Task
-- authority, and it is the threshold every subsequent live cost observation
-- enforces.
--
-- Durable properties carried by this migration:
--
-- - Nullable with a strictly-positive CHECK: NULL means no Continue has
--   ever refreshed this Task's allowance, and enforcement then compares
--   against the frozen ``authorized_task_budget_usd`` exactly as issue #240
--   delivered. Absence is the ordinary pre-F8f state; no backfill exists.
-- - The column is only ever written by the guarded Continue restore
--   statement (conditioned on the exact expected ``state_token``, the
--   ``blocked`` status, and the cleared current-gate pointer), so an unsafe,
--   stale, or ambiguous Continue attempts nothing and a repeated Continue
--   overwrites it only through the next fully guarded episode. The CHECK
--   below also rejects a present value on a ``ready_to_plan`` row.
-- - The Task's OBSERVED expenditure is deliberately not stored here or
--   anywhere on the Task row: it is derived by summing the retained known
--   per-session cost checkpoints of every TaskAgentSession matching the
--   Task, so the aggregate can never silently disagree with its
--   constituents (one fact, one home). The original budget stays immutable.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.tasks
    add column effective_cost_threshold_usd numeric
    check (
        effective_cost_threshold_usd is null
        or (
            effective_cost_threshold_usd > 0
            and status <> 'ready_to_plan'
        )
    );
