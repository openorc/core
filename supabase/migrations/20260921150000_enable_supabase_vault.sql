-- Supabase Vault for OpenOrc-owned Agent Runtime control credentials (Phase 2A, issue #55).
--
-- Enables the supported Supabase Vault extension for the project and begins
-- locking the vault schema down to the direct backend Postgres path. This is
-- the FIRST of the two Vault migrations:
--
-- - OpenOrc's backend reaches Vault over the existing process-local Postgres
--   pool as the connecting role: vault.create_secret, vault.update_secret,
--   the decrypt-on-read vault.decrypted_secrets view, and targeted
--   vault.secrets existence/lookup/delete by exact UUID. That path needs no
--   grants: the extension objects are administered by the migration role.
-- - This migration revokes PUBLIC and the Supabase browser-facing roles
--   (anon, authenticated) from the vault schema, the secret table/view, and
--   the secret-management functions. Those revokes hold on deployed
--   branches.
-- - It deliberately contains NO service_role handling: Supabase's platform
--   post-create handling re-establishes the platform's service_role Vault
--   grants after the migration that enables the extension reports
--   successful (observed on hosted previews, where service_role ended up
--   with direct grants on the schema, the secret table/view, and the
--   secret-management functions despite same-migration revokes). The
--   follow-up migration `*_lock_vault_from_service_role.sql` enforces the
--   service_role lockout after that platform handling has settled; keep the
--   two migrations in this order.
-- - The revokes that are present run unconditionally so the posture is
--   re-asserted on every application of the committed migration. The
--   effective privilege posture is proven against a real Supabase branch by
--   the integration-marked suite
--   (tests/integration/test_connection_credential_services.py), not
--   inferred from this text.
-- - This is a Vault-specific least-privilege restriction only: OpenOrc still
--   requires the Supabase privileged service-role/secret credential for
--   other Supabase-owned capabilities (Auth, administrative lifecycle), and
--   nothing in either Vault migration disables, removes, or broadly reduces
--   service_role outside the vault schema.
--
-- Vault stores the credential values encrypted at rest; ordinary openorc.*
-- tables carry only the opaque v1 auth_reference boundary (issue #55).
-- Runtime-owned provider/MCP/tool credentials remain outside this boundary
-- entirely.
--
-- `cascade` matches upstream's documented install so older platform images
-- that still model pgsodium as a dependency can enable the extension in one
-- step; on current images supabase_vault has no dependencies, and
-- `if not exists` no-ops when the extension is already present. The
-- extension's control file pins its objects to the `vault` schema, so no
-- WITH SCHEMA clause is given.

create extension if not exists supabase_vault cascade;

-- Schema lockout: USAGE (and CREATE) on the vault schema are denied to every
-- non-backend role, mirroring the openorc schema lockout posture.

revoke all on schema vault from public;
revoke all on schema vault from anon;
revoke all on schema vault from authenticated;

-- Encrypted secret storage and the decrypt-on-read view: denied to every
-- browser-facing role. The view exposes decrypted secrets, so its SELECT is
-- as sensitive as the plaintext itself. The service_role lockout is enforced
-- by the follow-up migration, after the platform's post-enablement grant
-- handling has settled.

revoke all on vault.secrets from public;
revoke all on vault.secrets from anon;
revoke all on vault.secrets from authenticated;

revoke all on vault.decrypted_secrets from public;
revoke all on vault.decrypted_secrets from anon;
revoke all on vault.decrypted_secrets from authenticated;

-- Secret-management functions: denied to every browser-facing role. The
-- extension itself already revokes EXECUTE from PUBLIC; these per-role
-- revokes make the posture deterministic regardless of image version or
-- dashboard-era grants. The function references are name-only (exactly one
-- function of each name exists in the extension), which stays valid across
-- supported signature revisions.

revoke execute on function vault.create_secret from public;
revoke execute on function vault.create_secret from anon;
revoke execute on function vault.create_secret from authenticated;

revoke execute on function vault.update_secret from public;
revoke execute on function vault.update_secret from anon;
revoke execute on function vault.update_secret from authenticated;


