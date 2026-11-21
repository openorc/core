-- Detached terminal cleanup (issue #344): the one-shot optional
-- CLEANUP_WORKSPACE run for a committed, not-yet-archived terminalization,
-- migrated onto the shared detached Hub-run receipt seam beside the existing
-- blocking send claim (issue #317).
--
-- ``openorc.task_hub_run_receipts`` gains the ``terminal_cleanup`` operation
-- and its dedicated Task-scoped ``terminal_cleanup_claim`` original attempt
-- kind: the receipt's ``original_attempt_id`` is the exact terminalized Task
-- UUID, with the claim's committed timestamp and the Task's terminal
-- commitment re-proven at correlation time. Cleanup invents no Execution,
-- no bootstrap claim, and no control intent, and no parallel effect ledger
-- exists beside the Task row's ``cleanup_send_claimed_at``/
-- ``cleanup_send_settled_at`` evidence pair (issue #317). The receipt's
-- ``expected_formal_family`` carries the canonical ``workspace_result``
-- family: the turn's independently observed completion is a validated
-- ``workspace_result`` (``operation=CLEANUP``, ``COMPLETED|FAILED``), never
-- a result whose absence authorizes a second send. Rows still carry only
-- safe identity/outcome facts — never prompts, transcripts, credentials,
-- or SDK payloads. The operation is deliberately NOT a Level 2
-- operational-failure family and is the terminalization exception: no
-- ordinary currentness gate that rejects terminal commitment applies to it.
alter table openorc.task_hub_run_receipts
    drop constraint task_hub_run_receipts_operation_vocab,
    drop constraint task_hub_run_receipts_original_attempt_kind_vocab,
    add constraint task_hub_run_receipts_operation_vocab check (
        operation in (
            'planning_bootstrap_prepare',
            'planning_bootstrap_transition',
            'initial_plan_dispatch',
            'planning_revise_dispatch',
            'reviewer_review',
            'implementation_execution',
            'pr_remediation_execution',
            'pr_compose_dispatch',
            'session_resume',
            'session_initialization',
            'owner_reviewer_discussion',
            'terminal_cleanup'
        )
    ),
    add constraint task_hub_run_receipts_original_attempt_kind_vocab check (
        original_attempt_kind in (
            'runtime_send_claim',
            'planning_bootstrap_claim',
            'review_iteration',
            'execution',
            'owner_control_intent',
            'merge_decision_gate',
            'owner_gate_discussion',
            'terminal_cleanup_claim'
        )
    ),
    add constraint task_hub_run_receipts_terminal_cleanup_identity_coherence check (
        (
            operation = 'terminal_cleanup'
            and original_attempt_kind = 'terminal_cleanup_claim'
            and expected_formal_family = 'workspace_result'
        )
        or (
            operation <> 'terminal_cleanup'
            and original_attempt_kind <> 'terminal_cleanup_claim'
        )
    );

-- The Level 2 episode identity-kind vocabulary gains the Task-scoped
-- terminal-cleanup attempt kind (issue #344): the closed
-- ``OriginalAttemptKind`` vocabulary is shared, and the episode CHECK must
-- admit every member even though the cleanup family itself is deliberately
-- NOT a Level 2 operational-failure family (no cleanup episode is ever
-- created; cleanup is outside the retry/block machinery per issue #344).
alter table openorc.operational_failure_episodes
    drop constraint operational_failure_episodes_original_attempt_kind_check;

alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_original_attempt_kind_check check (
        original_attempt_kind in (
            'runtime_send_claim', 'planning_bootstrap_claim', 'review_iteration',
            'execution', 'owner_control_intent', 'merge_decision_gate',
            'owner_gate_discussion', 'terminal_cleanup_claim'
        ));
