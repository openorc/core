-- F5d (issue #267): extend the Level 2 operational-failure disposition for
-- the exact-head GitHub merge family. Additive evolution of the F4 (#251)
-- shared foundation consumed by the Level 1 handoff (#262), the authorized
-- implementation-retry execution (#263), and the due-time wakeups (#257).
--
-- Durable properties carried by this migration:
--
-- - ``operational_failure_episodes.operation_family`` gains the ONE explicit
--   exact-head GitHub merge family ('exact_head_merge'). A merge is never
--   mislabeled as a pending agent-runtime interaction; the existing eight
--   runtime pending-interaction values are unchanged.
-- - ``operational_failure_episodes.original_attempt_kind`` gains
--   'merge_decision_gate': the existing approved exact-head MERGE_DECISION
--   OwnerGate, whose UUID is the original durable merge-operation identity
--   one merge episode roots at. The existing episode uniqueness
--   (original_attempt_kind, original_attempt_id) therefore yields exactly
--   one episode per approved merge decision — no re-approval, no gate
--   mutation, and no invented MERGE_REQUESTED Owner decision can mint a
--   second root. A Task state token, a bare caller UUID, or a PR number is
--   never an episode root.
-- - ``operational_failure_attempts.error_code`` gains
--   'external_transport_unavailable': the classified transient
--   network/transport failure of an external mutation, derived type-only
--   from the GitHub adapter's recognized transient transport subtype. It is
--   the one retry-eligible code of the merge family; every other code
--   escalates. No caller-supplied retryable flag can produce it.
-- - ``operational_failure_attempts.consumed_at``: the set-once consumption
--   stamp an execution seam writes atomically BEFORE the outbound merge it
--   runs — the minimum durable claim/consumption and subsequent-failure
--   evidence for a merge attempt, WITHOUT a fake resulting Execution (the
--   merge attempt creates no Execution row) and WITHOUT a separate merge
--   retry ledger. NULL while the authorization has not been executed; a
--   later failure report about this authorization must address exactly this
--   consumed attempt (source_attempt_id = the attempt's own UUID, with
--   consumed_at proven). The stamp is never cleared or rewritten. There is
--   deliberately no database-level uniqueness over consumed_at: one-shot
--   consumption is enforced by the conditional set-once update in the
--   repository (``consumed_at IS NULL`` guard) inside the consuming Task-lock
--   transaction, matching the repository-enforced escalation discipline.
-- - Rows still carry only safe identity/classification facts: closed CHECK
--   vocabularies and canonical JSONB subjects. No prompts, exception text,
--   provider messages, credentials, transcripts, or raw diagnostic payloads.
-- - No new privilege grants: the openorc schema lockout established by the
--   create_openorc_schema migration carries isolation, and runtime access
--   remains backend-only.

-- The episode's operation-family vocabulary: the eight runtime
-- pending-interaction values plus the one exact-head merge family. The
-- single-column CHECK was auto-named
-- ``operational_failure_episodes_operation_family_check`` at table creation.
alter table openorc.operational_failure_episodes
    drop constraint operational_failure_episodes_operation_family_check;

alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_operation_family_check
    check (operation_family in (
        'planning_bootstrap_prepare', 'planning_bootstrap_transition',
        'initial_plan_dispatch', 'planning_revise_dispatch',
        'reviewer_review', 'implementation_execution',
        'pr_remediation_execution', 'pr_compose_dispatch',
        'exact_head_merge'
    ));

-- The episode's original identity-kind vocabulary: the existing durable
-- claim/attempt identities plus the approved merge-decision gate. The CHECK
-- was auto-named
-- ``operational_failure_episodes_original_attempt_kind_check`` at creation.
alter table openorc.operational_failure_episodes
    drop constraint operational_failure_episodes_original_attempt_kind_check;

alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_original_attempt_kind_check
    check (original_attempt_kind in (
        'runtime_send_claim', 'planning_bootstrap_claim', 'review_iteration',
        'execution', 'owner_control_intent', 'merge_decision_gate'
    ));

-- The attempt's error-code vocabulary: the existing classified codes plus
-- the F5d transient external-transport code. The CHECK was auto-named
-- ``operational_failure_attempts_error_code_check`` at creation.
alter table openorc.operational_failure_attempts
    drop constraint operational_failure_attempts_error_code_check;

alter table openorc.operational_failure_attempts
    add constraint operational_failure_attempts_error_code_check
    check (error_code in (
        'runtime_unavailable', 'runtime_timeout', 'runtime_delivery_uncertain',
        'runtime_transport', 'runtime_protocol', 'agent_session_lost',
        'runtime_configuration_rejected', 'external_operation_failed',
        'external_operation_uncertain', 'external_operation_rejected',
        'external_transport_unavailable', 'unknown'
    ));

-- The set-once consumption stamp for a merge attempt (F5d): the exact
-- instant the executing seam claimed this authorization for its one outbound
-- merge, written atomically before that merge and never cleared afterwards.
alter table openorc.operational_failure_attempts
    add column consumed_at timestamptz;
