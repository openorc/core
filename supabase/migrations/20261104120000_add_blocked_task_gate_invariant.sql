-- Universal BLOCKED Task invariant cutover (issue #282, Phase 2F F7c).
--
-- Enforces, in durable PostgreSQL, the settled current-BLOCKED contract on
-- top of the F3 exceptional-blocking foundation (#258) and the fully
-- converted producer set (F7a #280 / F7b #281): every committed current
-- BLOCKED Task carries exactly one unresolved TaskBlock and one exact
-- current pending exceptional OwnerGate bound to that same block. The F3
-- migration deliberately left this global invariant to the later
-- integration; this file now adds it. Append-only: no merged migration is
-- edited.
--
-- Owner-approved scope (recorded on issue #282): OpenOrc is in development
-- against one completely empty application database, so there is no
-- historical BLOCKED history to classify, backfill, or preserve — this
-- migration performs no data conversion at all. It only creates the
-- enforcement machinery; a deployed database therefore transitions to the
-- enforced invariant without touching any row.
--
-- Durable properties carried by this migration:
--
-- 1. The partial unique index ``task_blocks_task_unresolved_uniq`` makes
--    two unresolved TaskBlocks for one Task unrepresentable for every
--    Task, current or archived — a durable backstop, never a requirement
--    that a Task have an unresolved block. Zero unresolved blocks on a
--    non-BLOCKED Task is legitimate history.
--
-- 2. The shared assertion function validates, for one Task: at most one
--    unresolved TaskBlock; and — only when the Task is CURRENT and BLOCKED
--    — a non-NULL current gate that is a pending exceptional gate
--    (``block_resolution``/``github_issue_change``/``github_state_mismatch``)
--    bound to exactly that sole unresolved block. Identity and scope of the
--    gate↔block↔Task relationship are already durable through the composite
--    foreign keys; the assertion composes them into the one contract. The
--    sole block identity is retrieved with a version-compatible aggregate
--    (``array_agg`` over UUIDs — ``min``/``max`` on UUID is unavailable on
--    the released PostgreSQL versions Supabase runs).
--
-- 3. Three DEFERRABLE INITIALLY DEFERRED constraint triggers run the
--    assertion over committed state on Task, gate, and block mutations
--    alike — a Task-only trigger would miss gate status/type/subject
--    changes and block resolution/deletion. Deferral lets the existing
--    atomic multitable compositions (the installer's block→gate→task
--    sequence, the archival pointer-clear, and deletion cascades) commit
--    and validates their final state; a violating intermediate state is
--    refused at commit, never at an intermediate statement.
--
-- 4. The converse invariant is deliberately NOT imposed: a Task that is
--    archived or not blocked asserts nothing beyond the unresolved-block
--    backstop. A pending exceptional gate retained as truthful terminal
--    history after ``archive_task`` cleared the pointer (confirmed GitHub
--    merge completion, trusted closed-unmerged cancellation), and a pending
--    superseded ordinary gate, remain legal. A missing Task row (a deletion
--    cascade in progress) asserts nothing.
--
-- 5. Deliberate absences: no new tables or columns, no data conversion, no
--    Owner decisions or terminal stamps, no forced historical gate cleanup,
--    no Retry/Ignore/Discuss actions (#254 F8), and no privilege grants —
--    the openorc schema lockout carries isolation, the assertion and its
--    wrapper are revoked from PUBLIC (browser-facing roles cannot reach
--    this schema in any case), and runtime access remains backend-only.

-- ---------------------------------------------------------------------------
-- The shared committed-state assertion.
-- ---------------------------------------------------------------------------

create function openorc.assert_blocked_task_gate_invariant (p_task_id uuid)
returns void
language plpgsql
as $assert$
declare
    v_status text;
    v_archived_at timestamptz;
    v_current_gate uuid;
    v_gate_type text;
    v_gate_status text;
    v_gate_block uuid;
    v_unresolved_count integer;
    v_sole_block uuid;
begin
    select status, archived_at, current_owner_gate_id
      into v_status, v_archived_at, v_current_gate
      from openorc.tasks
     where id = p_task_id;

    if not found then
        return;
    end if;

    -- ``array_agg`` carries the sole unresolved block identity: min/max on
    -- UUID values is not available on the released PostgreSQL versions
    -- Supabase runs, and the deterministic (created_at, id) order keeps the
    -- retrieval exact when exactly one row exists.
    select count(*), (array_agg(id order by created_at, id))[1]
      into v_unresolved_count, v_sole_block
      from openorc.task_blocks
     where task_id = p_task_id
       and resolved_at is null;

    if v_unresolved_count > 1 then
        raise exception
            'blocked-task gate invariant: task % has % unresolved task blocks; '
            'at most one is permitted',
            p_task_id, v_unresolved_count;
    end if;

    -- Only a CURRENT BLOCKED Task is constrained further; archived and
    -- non-blocked Tasks leave their gate history untouched.
    if v_archived_at is not null or v_status <> 'blocked' then
        return;
    end if;

    if v_current_gate is null then
        raise exception
            'blocked-task gate invariant: current blocked task % has no current OwnerGate pointer',
            p_task_id;
    end if;

    select gate_type, status, task_block_id
      into v_gate_type, v_gate_status, v_gate_block
      from openorc.owner_gates
     where id = v_current_gate;

    if not found then
        raise exception
            'blocked-task gate invariant: current blocked task % points at missing gate %',
            p_task_id, v_current_gate;
    end if;

    if v_gate_status is distinct from 'pending'
       or v_gate_type not in
           ('block_resolution', 'github_issue_change', 'github_state_mismatch')
    then
        raise exception
            'blocked-task gate invariant: current blocked task % points at gate % of type % '
            'with status %, which is not a pending exceptional gate',
            p_task_id, v_current_gate, v_gate_type, v_gate_status;
    end if;

    if v_gate_block is null
       or v_unresolved_count is distinct from 1
       or v_gate_block is distinct from v_sole_block
    then
        raise exception
            'blocked-task gate invariant: current blocked task % points at exceptional gate % '
            'not bound to its sole unresolved task block',
            p_task_id, v_current_gate;
    end if;
end;
$assert$;

-- The trigger entry point. PostgreSQL trigger functions take no declared
-- arguments and return ``trigger``, so this wrapper derives the affected
-- Task identity from the row context (``tasks`` carries the Task id as
-- ``id``; ``owner_gates`` and ``task_blocks`` carry ``task_id``), runs the
-- shared assertion, and hands the row back unchanged.
create function openorc.assert_blocked_task_gate_invariant_trigger ()
returns trigger
language plpgsql
as $trigger$
declare
    v_task_id uuid;
begin
    if TG_TABLE_NAME = 'tasks' then
        if TG_OP = 'DELETE' then
            v_task_id := old.id;
        else
            v_task_id := new.id;
        end if;
    else
        if TG_OP = 'DELETE' then
            v_task_id := old.task_id;
        else
            v_task_id := new.task_id;
        end if;
    end if;

    perform openorc.assert_blocked_task_gate_invariant (v_task_id);

    if TG_OP = 'DELETE' then
        return old;
    end if;
    return new;
end;
$trigger$;

-- The assertion and its trigger wrapper are internal durability
-- mechanisms, not an API: the openorc schema lockout already isolates
-- browser-facing roles, and these revocations keep the default PUBLIC
-- function grants from widening it.
revoke execute on function openorc.assert_blocked_task_gate_invariant (uuid) from public;
revoke execute on function openorc.assert_blocked_task_gate_invariant_trigger () from public;

-- ---------------------------------------------------------------------------
-- Durable enforcement.
-- ---------------------------------------------------------------------------

-- At most one unresolved TaskBlock per Task, for every Task — current or
-- archived. This backstop forbids only multiples; it never requires a Task
-- to have an unresolved block.
create unique index task_blocks_task_unresolved_uniq
    on openorc.task_blocks (task_id)
    where resolved_at is null;

-- The Task's own mutations: the guarded blocking transition, the pointer
-- installation, the status movement, and the archival pointer-clear all
-- validate their committed result.
create constraint trigger tasks_blocked_gate_invariant
    after insert or update of status, current_owner_gate_id, archived_at
    on openorc.tasks
    deferrable initially deferred
    for each row
    execute function openorc.assert_blocked_task_gate_invariant_trigger ();

-- Gate mutations that would silently corrupt a blocked Task's authority:
-- a resolution stamp, a type change, or a subject rebind while the gate is
-- the current pointer of a current BLOCKED Task.
create constraint trigger owner_gates_blocked_gate_invariant
    after insert or update of status, gate_type, task_block_id or delete
    on openorc.owner_gates
    deferrable initially deferred
    for each row
    execute function openorc.assert_blocked_task_gate_invariant_trigger ();

-- Block mutations: resolving (or deleting) the bound block of a current
-- BLOCKED Task's current gate must not leave the Task blocked without its
-- exact exceptional authority. Deletion is additionally FK-restricted by
-- the gate-subject linkage; the trigger is the committed-state backstop.
create constraint trigger task_blocks_blocked_gate_invariant
    after insert or update of resolved_at or delete
    on openorc.task_blocks
    deferrable initially deferred
    for each row
    execute function openorc.assert_blocked_task_gate_invariant_trigger ();