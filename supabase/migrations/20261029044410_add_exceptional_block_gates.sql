-- Exceptional OwnerGate types and TaskBlock-bound subjects (issue #258,
-- Phase 2F F3). Additive shared foundation for the blocked-Task authority
-- seam consumed by the later Level 2 triage (#251), direct-blocking
-- integration (#250), and Owner action services (#254).
--
-- Durable properties carried by this migration:
--
-- - The OwnerGate type vocabulary grows from four ordinary types to the
--   seven settled types: the four ordinary human-authority decisions are
--   unchanged, and BLOCK_RESOLUTION, GITHUB_ISSUE_CHANGE, and
--   GITHUB_STATE_MISMATCH join as the exceptional blocked-Task forms.
-- - The exceptional types bind exactly one TaskBlock as their only
--   subject: ``task_block_id`` is required and ``plan_revision_id``,
--   ``subject_head_sha``, and ``task_pull_request_id`` are NULL. The
--   ordinary types never carry a TaskBlock subject and keep their exact
--   prior subject forms (including the Reviewer-only ``settled`` status,
--   which stays review_resolution-gated and untouched here).
-- - The gate's TaskBlock subject is enforced as the same Task and
--   Workspace through the composite foreign key
--   ``(task_block_id, task_id, workspace_id)`` on a new unique key of
--   ``task_blocks (id, task_id, workspace_id)``. The FK establishes
--   identity and scope ONLY — not that the block is unresolved
--   (``resolved_at IS NULL``); the composed installation service verifies
--   unresolved-ness of the block it just created. Nullable ordinary-gate
--   subjects continue to work unchanged.
-- - Deliberately NOT here (the #250 cutover owns them): no global
--   ``BLOCKED ↔ unresolved TaskBlock ↔ current pending gate`` database
--   invariant, no new uniqueness over all unresolved blocks or pending
--   historical gates, and no historical backfill for already-blocked
--   Tasks. Existing direct ``blocked`` producers keep working unchanged
--   until their assigned integration.
-- - No new tables, columns outside ``owner_gates.task_block_id``, or
--   privilege grants: the openorc schema lockout carries isolation, and
--   runtime access remains backend-only.

-- The TaskBlock subject column. NULL for every ordinary gate; set exactly
-- for the three exceptional types.
alter table openorc.owner_gates
    add column task_block_id uuid;

-- The type vocabulary: the four ordinary decisions plus the three
-- exceptional blocked-Task forms. The single-column CHECK was auto-named
-- ``owner_gates_gate_type_check`` at table creation.
alter table openorc.owner_gates
    drop constraint owner_gates_gate_type_check;

alter table openorc.owner_gates
    add constraint owner_gates_gate_type_check
    check (gate_type in (
        'implementation_authorization', 'pr_authorization', 'merge_decision',
        'review_resolution', 'block_resolution', 'github_issue_change',
        'github_state_mismatch'
    ));



-- The per-type subject coherence, extended for the TaskBlock subject: the
-- four ordinary branches additionally require ``task_block_id is null``
-- (their exact prior subject forms are unchanged), and the three
-- exceptional branches require exactly the TaskBlock subject with every
-- other subject form NULL.
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
        )
    );

-- The composite-FK hook on the referenced side: the same identity key the
-- Task-side pointers and the runtime send claims already use.
alter table openorc.task_blocks
    add constraint task_blocks_id_task_id_workspace_id_uniq
    unique (id, task_id, workspace_id);

-- The gate's TaskBlock subject belongs to the same Task and Workspace:
-- the exact-subject rule is durable. Within-Task-aggregate linkage, never
-- an ownership edge — NO ACTION DEFERRABLE INITIALLY DEFERRED like the
-- other gate-subject linkage foreign keys, so the Workspace deletion
-- sequence is never order-sensitive.
alter table openorc.owner_gates
    add constraint owner_gates_task_block_fk
    foreign key (task_block_id, task_id, workspace_id)
    references openorc.task_blocks (id, task_id, workspace_id)
    deferrable initially deferred;
