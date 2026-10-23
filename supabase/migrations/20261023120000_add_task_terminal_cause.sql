-- The canonical terminal cause of an externally established Task
-- cancellation (issue #229, Phase 2E E10c).
--
-- When existing authoritative GitHub reconciliation durably confirms that a
-- Task's one canonical pull request is CLOSED without merge, the still-
-- current Task is terminally cancelled through the shared E10b cancellation
-- composition with truthful GITHUB provenance. The cause is a Task-owned
-- terminal fact, deliberately separate from an active TaskBlock: an active
-- TaskBlock describes a recoverable blocking condition on a current Task,
-- while a terminal cause describes immutable archived history. The value is
-- stamped exactly once, atomically with the guarded terminal archival, and
-- is never rewritten or cleared afterwards.
--
-- Durable properties carried by this migration:
--
-- - `openorc.tasks.terminal_cause` is a nullable text column carrying the
--   canonical terminal-cause vocabulary. NULL is the ordinary case: an
--   Owner-attributed cancellation carries no terminal cause (its provenance
--   lives on the terminal event), and a COMPLETED attempt never carries
--   one.
-- - The explicitly named CHECK admits a present value only on an archived
--   row whose terminal status is `cancelled`, and only for the single
--   settled value `pr_closed_unmerged`. The table's existing settled-
--   lifecycle CHECK (archived_at IS NULL iff the status is nonterminal) is
--   unchanged and composes with this one.
-- - No backfill: historical archived rows keep their recorded facts exactly
--   as they were; no past row gains a terminal cause.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.tasks
    add column terminal_cause text;

alter table openorc.tasks
    add constraint tasks_terminal_cause_check
    check (
        terminal_cause is null
        or (
            archived_at is not null
            and status = 'cancelled'
            and terminal_cause = 'pr_closed_unmerged'
        )
    );