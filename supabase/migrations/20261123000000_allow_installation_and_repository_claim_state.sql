-- GitHub installation and Repository mapping claim state (issue #372).
--
-- The Owner-facing installation/Repository mapping APIs (G1c) record
-- Owner-submitted stable external identities as UNVERIFIED configuration
-- claims. An authenticated Workspace Owner is authorized to configure which
-- installation routes the Workspace's GitHub operations and which stable
-- repository identities belong to its Projects, but that authorization never
-- establishes that the submitted identities are genuine GitHub observations.
-- Claim-state rows therefore carry the stable identity columns only; every
-- GitHub observation column is NULL until the trusted GitHub reconciliation
-- paths (the #57 trusted-facts record path and later trusted observation
-- flows) populate them:
--
--   openorc.github_installations: github_account_id, account_login,
--     account_type (suspended_at was already nullable);
--   openorc.repositories: owner_login, name, html_url, is_private
--     (default_branch was already nullable).
--
-- Provenance is structural: observation columns are present if and only if
-- trusted reconciliation wrote them, and a claim never updates an existing
-- row's observation columns or stable identity (the claim path is
-- insert-if-absent). Trusted reconciliation establishes a claim's stable
-- GitHub account ID atomically with its first observation write
-- (coalesce-preserved for already-established rows). The existing CHECK
-- constraints stay and are NULL-safe; no new column or table is introduced,
-- and no durable identity or routing semantics change.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.github_installations
    alter column github_account_id drop not null,
    alter column account_login drop not null,
    alter column account_type drop not null;

alter table openorc.repositories
    alter column owner_login drop not null,
    alter column name drop not null,
    alter column html_url drop not null,
    alter column is_private drop not null;
