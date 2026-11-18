-- The truthful post-effect terminal Resume outcome (issue #343).
--
-- The detached Owner-authorized same-session Resume (issue #343) admits the
-- fixed authorized resumption instruction as one independent detached Hub
-- run on the original still-live Task role session. Its independently
-- observed terminal can positively establish that the run definitively
-- ended WITHOUT a result its original workflow owner could process: an
-- observed run failure, or a malformed/unusable formal candidate. That is
-- post-effect evidence — truthfully distinct from ``non_delivered`` (which
-- remains a positively proven pre-effect refusal, so nothing can ever be
-- retroactively reclassified) — and the record must carry a terminal
-- outcome of its own: an outstanding ``dispatched`` record would otherwise
-- permanently veto the Owner Cancel action behind the Level 2 escalation's
-- Type B blocking gate (the cancellation guard refuses while any record is
-- ``authorized``/``dispatched``), stranding the Task.
--
-- ``failed`` is that terminal: the positively observed post-effect terminal
-- failure of the resumed run, recorded atomically with the detached-turn
-- receipt's ``observed_failure``/``result_not_applied`` settlement and the
-- existing Level 2 disposition, carrying only safe classified provenance in
-- its outcome mapping (never raw provider errors, prompts, or transcripts).
-- Genuinely uncertain admissions stay conservatively ``dispatched`` — their
-- veto is intentional and the existing uncertainty recovery owns them.
--
-- The original vocabulary constraint was created unnamed by
-- ``20261109000000_add_task_resume_intents`` (the driver's auto-generated
-- ``task_resume_intents_status_check`` name); it is replaced here with an
-- explicitly named constraint following the established vocabulary-extension
-- pattern, and remains otherwise unchanged. No new privilege is granted:
-- the openorc schema lockout carries isolation, and runtime access is
-- backend-only.

alter table openorc.task_resume_intents
    drop constraint task_resume_intents_status_check;

alter table openorc.task_resume_intents
    add constraint task_resume_intents_status_vocab check (
        status in (
            'authorized',
            'dispatched',
            'settled',
            'non_delivered',
            'interrupted',
            'superseded',
            'failed'
        )
    );
