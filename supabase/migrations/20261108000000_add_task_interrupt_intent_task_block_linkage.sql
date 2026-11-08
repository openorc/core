-- Owner-control episode identity: the exact TaskBlock an Interrupt intent
-- belongs to (F8 prerequisite, issue #299).
--
-- ``openorc.task_interrupt_intents`` already carries the exact-session
-- identity of one admitted interruption, registered atomically with the
-- Owner Abort's (or standing cost-limit's) blocking composition. What it
-- has not carried is the durable linkage to the exact ``owner_action_required``
-- TaskBlock that composition installed: session bindings persist across
-- subsequent workflow activity (later resume/continuation decisions own
-- that), so a session binding alone cannot distinguish the CURRENT
-- interruption episode from a historical settled intent for the same
-- reused session. Without this linkage, a settled intent from an earlier
-- episode could be mistaken for cessation evidence authorizing the
-- cancellation of later, independently unaccounted work in the same
-- session.
--
-- The column is nullable because historical rows predate it; no backfill
-- is invented — legacy settled intents simply carry NULL and can never
-- satisfy the cancellation guard's episode identity check (the fail-closed
-- default). New compositions always populate it: the shared blocking
-- composition creates the TaskBlock in the same transaction it registers
-- the intents, so the linkage is an in-transaction fact pass-through.
--
-- The foreign key binds (task_block_id, task_id, workspace_id) against the
-- EXISTING ``task_blocks_id_task_id_workspace_id_uniq`` hook (migration
-- 20261029044410), following the same composite subject-FK convention as
-- the session edge: an intent can never reference another Task's or
-- another Workspace's block. The block row is the subject the intent is
-- meaningless without, so the edge cascades with the Task/Workspace root.
-- A NULL ``task_block_id`` passes composite-FK validation by SQL semantics
-- and fails the service guard instead — historical rows never authorize.
--
-- No privileges are changed here: the openorc schema lockout established
-- by create_openorc_schema carries isolation, and runtime access is
-- backend-only.

alter table openorc.task_interrupt_intents
    add column task_block_id uuid;

alter table openorc.task_interrupt_intents
    add constraint task_interrupt_intents_task_block_id_task_id_workspace_id_fkey
    foreign key (task_block_id, task_id, workspace_id)
    references openorc.task_blocks (id, task_id, workspace_id)
    on delete cascade;

create index task_interrupt_intents_task_block_id_idx
    on openorc.task_interrupt_intents (task_block_id);
