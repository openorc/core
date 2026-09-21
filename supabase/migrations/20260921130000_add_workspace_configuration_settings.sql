-- Workspace configuration settings (Phase 2A, issue #53).
--
-- Adds the first-class, typed Workspace settings established by issue #53.
-- Two additive columns on openorc.workspaces — no replacement table, no
-- generic JSON settings bag, no prompt/template/hash/version history:
--
-- - review_iteration_limit — the Workspace-configured review-loop iteration
--   limit. Non-null with a durable positive-integer CHECK constraint; the
--   column default IS the backfill value, so every Workspace existing before
--   this migration (and every future insert that omits the column) receives
--   the v1 configured boundary — DEFAULT_REVIEW_LOOP_ITERATION_LIMIT = 5 in
--   openorc.domain.reviews (the literal here is that same value, asserted by
--   tests). A change affects future ReviewLoops only: each ReviewLoop keeps
--   the effective iteration_limit stored on it at creation as immutable
--   historical configuration, which is never rewritten when this setting
--   changes.
-- - guidance — one current, Owner-authored prose value. Default and valid
--   value is the empty string, meaning no Workspace-specific guidance. It is
--   current mutable Workspace configuration, not historical evidence: no
--   template key, built-in/default prompt counterpart, base-template version,
--   hash, snapshot, or per-interaction use record exists anywhere in this
--   schema. Guidance can never redefine protocol, authority, or state
--   semantics.
--
-- No membership, invitation, role, or team concepts are introduced: a
-- Workspace is owned by exactly one owner_profile_id in v1.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.workspaces
    add column review_iteration_limit integer not null default 5
    check (review_iteration_limit > 0);

alter table openorc.workspaces
    add column guidance text not null default '';