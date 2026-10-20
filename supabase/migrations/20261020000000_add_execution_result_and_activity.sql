-- E6 durable implementation-result and qualifying-activity facts on
-- openorc.executions (issue #81).
--
-- Two deliberately minimal additive columns on the historical attempt
-- record:
--
-- - implementation_result: the one-shot, validated canonical v1
--   implementation_result payload attached to exactly the Execution it
--   completed. It is written only by the guarded RUNNING -> succeeded
--   finalize of the authorized-execution seam (E6) and is never rewritten
--   afterwards: finalized history stays absorbing. It is a workflow-layer
--   record: the payload's ``branch`` value is the Producer's discovery/
--   routing claim, never authoritative GitHub branch/head state, and no
--   Git SHA field of any kind is introduced.
-- - last_qualifying_activity_at: the F5 activity marker — the last
--   database-stamped instant the bound Producer session showed genuine
--   implementation activity while this Execution was running (thinking,
--   iteration/work polling, tool lifecycle, or non-formal commentary).
--   It is initialized at the dispatch claim, advanced monotonically with
--   database time only while the row is running, and never written for a
--   finalized row. It stores no activity content of any kind: no
--   commentary text, reasoning, tool inputs/outputs, or raw payloads.
--
-- No other column, table, constraint, index, or privilege changes; the
-- openorc schema remains locked to non-OpenOrc roles (no new grants).
alter table openorc.executions
    add column implementation_result jsonb
        check (
            implementation_result is null
            or jsonb_typeof(implementation_result) = 'object'
        ),
    add column last_qualifying_activity_at timestamptz;