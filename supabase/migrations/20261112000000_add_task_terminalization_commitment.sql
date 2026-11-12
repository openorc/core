-- The exact-Task terminalization commitment (issue #317, F10 remediation).
--
-- ``openorc.tasks`` gains the minimum durable, set-once facts the shared
-- terminal finalization needs before the bounded optional Cline checkout
-- cleanup runs and mandatory archival commits:
--
-- - ``terminalization_outcome`` ('cancelled' | 'completed'),
--   ``terminalization_origin`` ('owner_cancelled' | 'github_closed_unmerged' |
--   'github_confirmed_merged'), and ``terminalization_established_at`` are
--   the pre-archival commitment every terminal authority (Owner Cancel, the
--   trusted E10c closed-unmerged cancellation, and the E10a confirmed-merge
--   completion) commits atomically with its existing decision composition.
--   The commitment is the explicit authority/currentness barrier across
--   ordinary Task interactions (the exact-subject guard refuses operations
--   against a committed, not-yet-archived Task) and preserves the authorized
--   terminal outcome and its origin across the bounded external cleanup
--   interaction, so the finalizer archives exactly the committed outcome
--   with the truthful terminal event actor provenance and terminal cause.
--   The origin echo is the smallest fact the finalizer cannot safely
--   re-derive from the decision records alone; the decision records remain
--   the accountable audit of actor, gate, loop, and PR-close provenance.
-- - ``cleanup_send_claimed_at`` / ``cleanup_send_settled_at`` are the
--   minimal one-shot evidence of the optional Cline CLEANUP_WORKSPACE send:
--   the claim is a compare-and-set taken before the external call (a
--   concurrent or duplicate finalizer can never double-send), and the settle
--   stamps the bounded response-or-timeout window's conclusion. An unsettled
--   claim protects the in-flight attempt: no competing finalizer may archive,
--   end sessions, or release Connection capacity while it is outstanding. If
--   the executing worker dies silently, the claim stays unsettled and
--   terminal archival stays unfinished — the accepted system-wide v1
--   limitation recorded in the #317 reconciliation comment; no deadline,
--   recovery, or discovery mechanism exists for it.
--
-- Durable properties carried by this migration:
--
-- - Nullable set-once facts with no backfill: absence is the ordinary
--   pre-#317 state. The commitment columns are written exactly once by the
--   guarded commitment statement (conditioned on the exact expected
--   ``state_token``, a non-archived row, and a NULL commitment) and are
--   never cleared afterwards — they remain truthful history after archival.
-- - The CHECK constraints enforce the value vocabularies, the three-way
--   commitment coherence (outcome, origin, and establishment timestamp are
--   present or absent together), and the cleanup-evidence coherence
--   (settled implies claimed; claimed implies a committed terminalization).
-- - No privileges are granted here: the openorc schema lockout established
--   by the create_openorc_schema migration carries isolation, and runtime
--   access is backend-only.

alter table openorc.tasks
    add column terminalization_outcome text
    check (
        terminalization_outcome is null
        or terminalization_outcome in ('cancelled', 'completed')
    ),
    add column terminalization_origin text
    check (
        terminalization_origin is null
        or terminalization_origin in
            ('owner_cancelled', 'github_closed_unmerged', 'github_confirmed_merged')
    ),
    add column terminalization_established_at timestamptz,
    add column cleanup_send_claimed_at timestamptz,
    add column cleanup_send_settled_at timestamptz,
    add constraint tasks_terminalization_coherence_check check (
        (terminalization_outcome is null) = (terminalization_established_at is null)
        and (terminalization_outcome is null) = (terminalization_origin is null)
    ),
    add constraint tasks_cleanup_send_evidence_check check (
        (cleanup_send_settled_at is null or cleanup_send_claimed_at is not null)
        and (cleanup_send_claimed_at is null or terminalization_outcome is not null)
    );
