-- F12 (issue #316): the narrow startup-recovery lookup support for the
-- bounded post-restart pass that resubmits still-outstanding durably
-- authorized Level 2 retries (2/4/8-second worker-local backoff).
--
-- Durable properties carried by this migration:
--
-- - ONE additive partial index over ``operational_failure_episodes`` covering
--   exactly the recovery enumeration's filter: non-escalated episodes
--   (``escalated_task_block_id IS NULL``) in the three supported retry
--   families ('implementation_execution', 'exact_head_merge',
--   'exact_head_pr_authorization_presentation'). Keyset paging orders by
--   episode id, which the indexed key directly serves; the per-episode
--   latest-attempt selection rides the existing
--   ``operational_failure_attempts (episode_id, ordinal)`` unique index.
--   No second retry queue/table, no ledger rewrite, and no resettable
--   state: the recovery pass reads and never mutates.
-- - Rows still carry only safe identity/classification facts. No prompts,
--   exception text, provider messages, credentials, transcripts, or raw
--   diagnostic payloads belong in this ledger.
-- - No new privilege grants: the openorc schema lockout established by the
--   create_openorc_schema migration carries isolation, and runtime access
--   remains backend-only.

create index operational_failure_episodes_outstanding_retry_idx
    on openorc.operational_failure_episodes (id)
    where escalated_task_block_id is null
      and operation_family in (
          'implementation_execution',
          'exact_head_merge',
          'exact_head_pr_authorization_presentation'
      );
