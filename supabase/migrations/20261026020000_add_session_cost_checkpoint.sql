-- Session cost checkpoint (issue #240, Pre-F cost accounting).
--
-- Adds the minimal durable per-session cost checkpoint to
-- ``openorc.task_agent_sessions``: the last RETAINED nonnegative aggregate
-- Cline-reported USD cost for the exact bound Task/role/Connection session,
-- plus the observation instant. This is the session-scoped fact the Task
-- expenditure aggregate sums; it is not a billing ledger.
--
-- Durable properties carried by this migration:
--
-- - ``retained_cost_usd numeric`` is the high-water reported total for the
--   binding: the narrow conditional checkpoint update accepts only a value
--   strictly greater than the retained one, so repeated, out-of-order, and
--   Cline Hub-reset (counter-reset) snapshots can never decrease the
--   retained amount or double count. Accepting the resulting undercount
--   after a Hub restart is deliberate — no Hub epoch/incarnation identity
--   and no retrospective reconstruction of uncorrelated readings exist.
-- - ``cost_observed_at timestamptz`` is the observation instant of the
--   retained value, stamped by the database clock in the same statement as
--   the write. It is never substituted by ``updated_at``.
-- - Coherence CHECK in both directions: the two columns are NULL together
--   (the binding has never reported an aggregate cost — unknown, NEVER
--   zero) or non-NULL together with a nonnegative finite amount. A zero
--   retained value is a legitimate reported reading, not a sentinel.
-- - The aggregate USD cost source is Cline's session-wide
--   ``aggregate_usage.total_cost`` scope (which already includes supported
--   subagents/teammates exactly once). The root/lead-agent ``usage`` scope
--   is never added to it and never substituted for it; that scope
--   discipline lives in the adapter projection and the checkpointing
--   service, and this column stores only the already-aggregate value.
-- - Exact scope is enforced by the checkpointing update's WHERE clause
--   (id + task_id + workspace_id + connection_id), backed by the table's
--   existing composite foreign keys: an obsolete or foreign observation
--   matches no row and is a no-op, never another Task's accounting.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.task_agent_sessions
    add column retained_cost_usd numeric,
    add column cost_observed_at timestamptz;

alter table openorc.task_agent_sessions
    add constraint task_agent_sessions_cost_checkpoint_check
    check (
        (retained_cost_usd is null and cost_observed_at is null)
        or (
            retained_cost_usd is not null
            and cost_observed_at is not null
            and retained_cost_usd >= 0
        )
    );