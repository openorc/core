-- Detached Owner ↔ Reviewer discussion (issue #339): the single enqueued
-- conversational turn on the exact existing Reviewer session at one pending
-- current OwnerGate, migrated onto the shared detached Hub-run receipt seam.
--
-- ``openorc.task_hub_run_receipts`` gains the ``owner_reviewer_discussion``
-- operation and its dedicated gate-bound ``owner_gate_discussion`` original
-- attempt kind — the receipt's ``original_attempt_id`` is the exact pending
-- current OwnerGate UUID the conversation is admitted at (the gate is the
-- durable discussion subject, never an approved decision and never a claim
-- the discussion invents). The receipt's ``expected_formal_family`` carries
-- the dedicated NON-formal discussion token: the turn's independently
-- observed completion is ordinary attributed text, never a formal response,
-- so no canonical v1 response family applies. Rows still carry only safe
-- identity/outcome facts — never message or reply text, transcripts,
-- credentials, or SDK payloads.
--
-- ``admission_attempted_at`` is the set-once one-shot admission-attempt
-- stamp (the same pattern as ``contradicted_at``): the discussion's claim
-- (API prepare) and admission (worker admit) live in different invocations,
-- so the admission stamps the claimed receipt exactly once, strictly before
-- its enqueue — a duplicate or re-delivered admission execution can never
-- reach a second enqueue of the same prepared message. A positively refused
-- operation submission settles the receipt ``refused`` without a stamp (no
-- admission attempt was made and the Reviewer session slot is freed); an
-- attempted admission — acknowledged, refused at the runtime, or uncertain —
-- carries the stamp truthfully. The stamp is application-CAS set-once; no
-- database CHECK constrains it beyond nullability.
alter table openorc.task_hub_run_receipts
    drop constraint task_hub_run_receipts_operation_vocab,
    drop constraint task_hub_run_receipts_original_attempt_kind_check,
    add column admission_attempted_at timestamptz,
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
            'owner_reviewer_discussion'
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
            'owner_gate_discussion'
        )
    ),
    add constraint task_hub_run_receipts_discussion_identity_coherence check (
        (
            operation = 'owner_reviewer_discussion'
            and original_attempt_kind = 'owner_gate_discussion'
            and expected_formal_family = 'owner_reviewer_discussion'
        )
        or (
            operation <> 'owner_reviewer_discussion'
            and original_attempt_kind <> 'owner_gate_discussion'
            and expected_formal_family <> 'owner_reviewer_discussion'
        )
    );

comment on column openorc.task_hub_run_receipts.admission_attempted_at is
    'Set-once one-shot admission-attempt stamp (issue #339): the split '
    'prepare/admit discussion dispatch stamps the claimed receipt exactly '
    'once before its enqueue, so a duplicate admission execution can never '
    're-enqueue the same prepared message.';

-- The Level 2 episode identity-kind vocabulary gains the gate-bound
-- discussion attempt kind (issue #339): the closed ``OriginalAttemptKind``
-- vocabulary is shared, and the episode CHECK must admit every member even
-- though the discussion family itself is deliberately NOT a Level 2
-- operational-failure family (no discussion episode is ever created).
alter table openorc.operational_failure_episodes
    drop constraint operational_failure_episodes_original_attempt_kind_check;

alter table openorc.operational_failure_episodes
    add constraint operational_failure_episodes_original_attempt_kind_check check (
        original_attempt_kind in (
            'runtime_send_claim', 'planning_bootstrap_claim', 'review_iteration',
            'execution', 'owner_control_intent', 'merge_decision_gate',
            'owner_gate_discussion'
        ));
