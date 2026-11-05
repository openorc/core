-- Level 3 blocking-case gate types (issue #286, Phase 2F F8a).
-- Append-only: no merged migration is edited.
--
-- The OwnerGate type vocabulary grows from seven settled types to nine:
-- COST_LIMIT ('cost_limit') and OWNER_ABORT ('owner_abort') join the
-- three F3 exceptional blocked-Task forms (issue #258) as the Level 3
-- blocking-case vocabulary the F8 classification catalog (issue #254)
-- selects. Both are exceptional blocked-Task forms: each binds exactly
-- one unresolved TaskBlock as its only subject, exactly like the three
-- prior exceptional types. The F8c decision menus (issue #288) complete
-- the Owner action vocabularies for the two new types; nothing here
-- executes or authorizes any action.
--
-- Durable properties carried by this migration:
--
-- 1. The single-column type CHECK accepts the two new values alongside
--    the seven settled types.
--
-- 2. The per-type subject coherence CHECK gains the two exceptional
--    branches: exactly the TaskBlock subject (plan_revision_id,
--    subject_head_sha, and task_pull_request_id NULL; task_block_id NOT
--    NULL). The ordinary types keep their exact prior subject forms,
--    including the review_resolution-gated 'settled' status.
--
-- 3. The F7c blocked-task gate invariant (issue #282) is re-declared with
--    the widened exceptional-type list, so a committed current BLOCKED
--    Task may carry a 'cost_limit' or 'owner_abort' current pending
--    exceptional gate bound to its sole unresolved block. The assertion
--    body is otherwise unchanged; the trigger functions invoke the
--    assertion by name and need no re-creation, and the existing
--    revocations and constraint triggers persist untouched across the
--    function replacement.
--
-- 4. No data conversion, no backfill, no new tables, columns, or
--    privilege grants: the openorc schema lockout carries isolation, and
--    the Owner-approved scope on issue #282 applies equally — the
--    development application database is empty, so no historical rows
--    exist to convert.

alter table openorc.owner_gates
    drop constraint owner_gates_gate_type_check;

alter table openorc.owner_gates
    add constraint owner_gates_gate_type_check
    check (gate_type in (
        'implementation_authorization', 'pr_authorization', 'merge_decision',
        'review_resolution', 'block_resolution', 'github_issue_change',
        'github_state_mismatch', 'cost_limit', 'owner_abort'
    ));

-- The per-type subject coherence: the four ordinary branches keep their
-- exact prior forms (task_block_id NULL), the three F3 exceptional
-- branches and the two new F8a branches require exactly the TaskBlock
-- subject with every other subject form NULL.
alter table openorc.owner_gates
    drop constraint owner_gates_subject_coherence_check;

alter table openorc.owner_gates
    add constraint owner_gates_subject_coherence_check
    check (
        (
            gate_type = 'implementation_authorization'
            and plan_revision_id is not null
            and subject_head_sha is null
            and task_pull_request_id is null
            and task_block_id is null
        ) or (
            gate_type = 'pr_authorization'
            and plan_revision_id is null
            and subject_head_sha is not null
            and task_pull_request_id is null
            and task_block_id is null
        ) or (
            gate_type = 'merge_decision'
            and plan_revision_id is null
            and subject_head_sha is not null
            and task_pull_request_id is not null
            and task_block_id is null
        ) or (
            gate_type = 'review_resolution'
            and task_block_id is null
            and (
                (
                    plan_revision_id is not null
                    and subject_head_sha is null
                    and task_pull_request_id is null
                )
                or (
                    plan_revision_id is null
                    and subject_head_sha is not null
                    and task_pull_request_id is not null
                )
            )
        ) or (
            gate_type = 'block_resolution'
            and plan_revision_id is null
            and subject_head_sha is null
            and task_pull_request_id is null
            and task_block_id is not null
        ) or (
            gate_type = 'github_issue_change'
            and plan_revision_id is null
            and subject_head_sha is null
            and task_pull_request_id is null
            and task_block_id is not null
        ) or (
            gate_type = 'github_state_mismatch'
            and plan_revision_id is null
            and subject_head_sha is null
            and task_pull_request_id is null
            and task_block_id is not null
        ) or (
            gate_type = 'cost_limit'
            and plan_revision_id is null
            and subject_head_sha is null
            and task_pull_request_id is null
            and task_block_id is not null
        ) or (
            gate_type = 'owner_abort'
            and plan_revision_id is null
            and subject_head_sha is null
            and task_pull_request_id is null
            and task_block_id is not null
        )
    );

-- The F7c shared committed-state assertion (issue #282), re-declared with
-- the widened exceptional-type list. The body is otherwise byte-identical
-- to the F7c declaration: at most one unresolved TaskBlock per Task, and
-- — only for a CURRENT BLOCKED Task — a non-NULL current gate that is a
-- pending exceptional gate (now the five types) bound to exactly that
-- sole unresolved block, retrieved version-compatibly. ``create or
-- replace`` preserves the existing revocations and the constraint
-- triggers, which invoke this function by name.
create or replace function openorc.assert_blocked_task_gate_invariant (p_task_id uuid)
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
           ('block_resolution', 'github_issue_change', 'github_state_mismatch',
            'cost_limit', 'owner_abort')
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