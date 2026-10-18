-- Task FIFO queue-entry timestamp (issue #76).
--
-- ``openorc.tasks`` already carries the ``queued`` coarse status (Phase 1:
-- normal runtime-capacity backpressure, not failure), but it had no durable
-- fact establishing WHEN the current queue wait began. FIFO admission (E1a,
-- issue #76) needs that fact: the queue order is the durable
-- ``(queued_at, id)`` key, preserved under concurrency, overlap, and process
-- restart. It is deliberately not Task creation time, an event timestamp,
-- process memory, job time, or a requeue update — and ``QUEUED`` is never an
-- RQ/Valkey job or a second queue table: Task.status plus this one column
-- are the canonical queue state.
--
-- The column is a small append-only addition: nullable ``queued_at
-- timestamptz``, populated by the database clock exactly once when a Task
-- first moves from ``ready_to_plan`` into ``queued``, kept unchanged for the
-- lifetime of that queue wait (repeated failed capacity checks never
-- rewrite it), and cleared on the successful ``queued -> planning`` claim
-- and on any later authorized path out of ``queued`` (terminal archival
-- included). It is non-null iff the current status is ``queued`` — the same
-- bidirectional invariant shape as the archival lifecycle CHECK, enforced
-- here in both directions by one table constraint. Existing historical
-- ``queued`` rows (if any) are backfilled once with the migration's DB
-- clock: the only honest available fact for a wait whose entry instant was
-- never recorded; their relative order then falls back to the Task id,
-- deterministically.
--
-- The partial index serves the one canonical queue read — the bounded,
-- Workspace-scoped FIFO scan in ``(queued_at, id)`` order — and nothing
-- else. No privilege is granted here: the openorc schema lockout
-- established by the create_openorc_schema migration carries isolation, and
-- runtime access is backend-only.

alter table openorc.tasks
    add column queued_at timestamptz;

update openorc.tasks
    set queued_at = now()
    where status = 'queued' and queued_at is null;

alter table openorc.tasks
    add constraint tasks_queued_iff_queued_at_chk
    check ((status = 'queued') = (queued_at is not null));

create index tasks_workspace_queue_fifo_idx
    on openorc.tasks (workspace_id, queued_at, id)
    where status = 'queued';