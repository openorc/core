-- GitHub webhook delivery recovery-query index (Phase 2B, issue #62).
--
-- The #62 repeatable reconciliation sweep's delivery-recovery batch reads
-- the accepted relevant deliveries whose processing is still unresolved
-- (`classification = 'relevant' and processed_at is null`), oldest first,
-- under an explicit batch limit. Without a supporting index that bounded
-- batch would scan the whole delivery history on every sweep. The partial
-- index covers exactly the recovery-eligible rows; processed and
-- ignored/unusable deliveries are outside it.
--
-- Operational recovery metadata only: the index never makes inbox state
-- GitHub truth, never affects the durable delivery-GUID uniqueness, and
-- changes no grants, foreign keys, or deletion-ownership edges (the openorc
-- schema lockout established by create_openorc_schema carries isolation with
-- backend-only runtime access).

create index github_webhook_deliveries_recovery_pending_idx
    on openorc.github_webhook_deliveries (received_at, id)
    where classification = 'relevant' and processed_at is null;