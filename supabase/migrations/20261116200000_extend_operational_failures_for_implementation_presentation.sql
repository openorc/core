-- Issue #336: extend the Level 2 operational-failure disposition for the
-- exact-plan implementation-authorization presentation family. Additive
-- evolution of the F4 (#251) shared foundation consumed by the Level 1
-- handoff (#262), following the F5d (#267) and F5f (#269) family-extension
-- pattern.
--
-- Durable properties carried by this migration:
--
-- - ``operational_failure_episodes.operation_family`` gains the ONE explicit
--   exact-plan implementation-authorization presentation family
--   ('exact_plan_implementation_authorization_presentation'). A gate
--   presentation is never mislabeled as a pending agent-runtime interaction;
--   the existing values are unchanged.
-- - ``operational_failure_episodes.original_attempt_kind`` is unchanged: the
--   presentation family roots its episodes at the existing durable
--   'review_iteration' identity — the finalized ACCEPTED planning
--   ReviewIteration the exact-plan gate presents from — so the existing
--   episode uniqueness (original_attempt_kind, original_attempt_id) yields
--   exactly one episode per accepted review attempt.
-- - Rows still carry only safe identity/classification facts: closed CHECK
--   vocabularies and canonical JSONB subjects. No prompts, exception text,
--   provider messages, credentials, transcripts, or raw diagnostic payloads.
-- - No new privilege grants: the openorc schema lockout established by the
--   create_openorc_schema migration carries isolation, and runtime access
--   remains backend-only.

-- The episode's operation-family vocabulary: the nine runtime
-- pending-interaction values, the one exact-head merge family, the one
-- exact-head PR-authorization presentation family, and the one exact-plan
-- implementation-authorization presentation family. The single-column CHECK
-- was auto-named
-- ``operational_failure_episodes_operation_family_check`` at table creation
-- and re-named identically by the F5d extension.
alter table openorc.operational_failure_episodes
    drop constraint operational_failure_episodes_operation_family_check;

alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_operation_family_check
    check (operation_family in (
        'planning_bootstrap_prepare', 'planning_bootstrap_transition',
        'initial_plan_dispatch', 'planning_revise_dispatch',
        'reviewer_review', 'implementation_execution',
        'pr_remediation_execution', 'pr_compose_dispatch',
        'exact_head_merge', 'exact_head_pr_authorization_presentation',
        'exact_plan_implementation_authorization_presentation'
    ));
