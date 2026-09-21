-- Supabase Vault for OpenOrc-owned Agent Runtime control credentials (Phase 2A, issue #55).
--
-- Enables the supported Supabase Vault extension for the project and locks the
-- vault schema down to the direct backend Postgres path:
--
-- - OpenOrc's backend reaches Vault over the existing process-local Postgres
--   pool as the connecting (owning) role: vault.create_secret,
--   vault.update_secret, the decrypt-on-read vault.decrypted_secrets view, and
--   targeted vault.secrets existence/lookup/delete by exact UUID. That path
--   needs no grants: the extension objects are owned by the role that runs
--   the migrations.
-- - Every other access path is explicitly revoked. PUBLIC, and the Supabase
--   roles anon, authenticated, and service_role, get no privileges on the
--   vault schema, the secret table/view, or the secret-management functions.
--   This is a Vault-specific least-privilege restriction only: OpenOrc still
--   requires the Supabase privileged service-role/secret credential for other
--   Supabase-owned capabilities (Auth, administrative lifecycle), and nothing
--   here disables, removes, or broadly reduces service_role outside the vault
--   schema.
-- - The revokes run unconditionally so the posture is re-asserted on every
--   application of the committed migration, whether or not the extension was
--   already enabled on the target. The effective privilege posture is proven
--   against a real Supabase branch by the integration-marked suite
--   (tests/integration/test_connection_credential_services.py), not inferred
--   from this text.
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
revoke all on schema vault from service_role;

-- Encrypted secret storage and the decrypt-on-read view: denied to every
-- non-backend role. The view exposes decrypted secrets, so its SELECT is as
-- sensitive as the plaintext itself.

revoke all on vault.secrets from public;
revoke all on vault.secrets from anon;
revoke all on vault.secrets from authenticated;
revoke all on vault.secrets from service_role;

revoke all on vault.decrypted_secrets from public;
revoke all on vault.decrypted_secrets from anon;
revoke all on vault.decrypted_secrets from authenticated;
revoke all on vault.decrypted_secrets from service_role;

-- Secret-management functions: denied to every non-backend role. The
-- extension itself already revokes EXECUTE from PUBLIC; these per-role
-- revokes make the posture deterministic regardless of image version or
-- dashboard-era grants. The function references are name-only (exactly one
-- function of each name exists in the extension), which stays valid across
-- supported signature revisions.

revoke execute on function vault.create_secret from public;
revoke execute on function vault.create_secret from anon;
revoke execute on function vault.create_secret from authenticated;
revoke execute on function vault.create_secret from service_role;

revoke execute on function vault.update_secret from public;
revoke execute on function vault.update_secret from anon;
revoke execute on function vault.update_secret from authenticated;
revoke execute on function vault.update_secret from service_role;

revoke execute on function vault._crypto_aead_det_encrypt from public;
revoke execute on function vault._crypto_aead_det_encrypt from anon;
revoke execute on function vault._crypto_aead_det_encrypt from authenticated;
revoke execute on function vault._crypto_aead_det_encrypt from service_role;

revoke execute on function vault._crypto_aead_det_decrypt from public;
revoke execute on function vault._crypto_aead_det_decrypt from anon;
revoke execute on function vault._crypto_aead_det_decrypt from authenticated;
revoke execute on function vault._crypto_aead_det_decrypt from service_role;

revoke execute on function vault._crypto_aead_det_noncegen from public;
revoke execute on function vault._crypto_aead_det_noncegen from anon;
revoke execute on function vault._crypto_aead_det_noncegen from authenticated;
revoke execute on function vault._crypto_aead_det_noncegen from service_role;

