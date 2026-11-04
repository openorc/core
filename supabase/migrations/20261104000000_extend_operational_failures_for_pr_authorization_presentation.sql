-- F5f (issue #269): extend the Level 2 operational-failure disposition for
-- the exact-head PR-authorization presentation family. Additive evolution of
-- the F4 (#251) shared foundation in the exact form the F5d (issue #267)
-- migration established for the merge family.
--
-- Durable properties carried by this migration:
--
-- - ``operational_failure_episodes.operation_family`` gains the ONE explicit
--   exact-head PR-authorization presentation family
--   ('exact_head_pr_authorization_presentation'). The presentation is an
--   OpenOrc continuation over GitHub reads and guarded durable writes — it
--   is never mislabeled as a pending agent-runtime interaction and never
--   conflated with the merge family; the existing runtime
--   pending-interaction values and 'exact_head_merge' are unchanged.
-- - No other vocabulary, column, table, or constraint changes. The episode
--   still roots at an existing durable identity kind: F5f reuses
--   ``original_attempt_kind = 'execution'`` (already admitted) — the
--   episode's root is the durably finalized SUCCESSFUL E6 Execution UUID,
--   which the episode never labels as failed. The execution-status
--   disjointness (E6-family episodes root only at FAILED executions, the
--   presentation family only at SUCCEEDED ones) keeps the existing
--   (original_attempt_kind, original_attempt_id) uniqueness collision-free.
--   The attempt error-code vocabulary already admits
--   'external_transport_unavailable' (F5d), the one retry-eligible code of
--   the presentation family's narrowly typed transient-transport
--   classification.
-- - Rows still carry only safe identity/classification facts: closed CHECK
--   vocabularies and canonical JSONB subjects. No prompts, exception text,
--   provider messages, credentials, transcripts, or raw diagnostic payloads.
-- - No new privilege grants: the openorc schema lockout established by the
--   create_openorc_schema migration carries isolation, and runtime access
--   remains backend-only.

-- The episode's operation-family vocabulary: the eight runtime
-- pending-interaction values, the one exact-head merge family (F5d), and the
-- one exact-head PR-authorization presentation family (F5f). The
-- single-column CHECK was auto-named
-- ``operational_failure_episodes_operation_family_check`` at table creation
-- and replaced once by the F5d migration.
alter table openorc.operational_failure_episodes
    drop constraint operational_failure_episodes_operation_family_check;

alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_operation_family_check
    check (operation_family in (
        'planning_bootstrap_prepare', 'planning_bootstrap_transition',
        'initial_plan_dispatch', 'planning_revise_dispatch',
        'reviewer_review', 'implementation_execution',
        'pr_remediation_execution', 'pr_compose_dispatch',
        'exact_head_merge',
        'exact_head_pr_authorization_presentation'
    ));