-- Supabase Vault for OpenOrc-owned Agent Runtime control credentials (Phase 2A, issue #55).
--
-- Enables the supported Supabase Vault extension for the project and locks
-- the vault schema down for the browser-facing roles:
--
-- - OpenOrc's backend reaches Vault over the existing process-local Postgres
--   pool as the connecting migration/application role: vault.create_secret,
--   vault.update_secret, the decrypt-on-read vault.decrypted_secrets view,
--   and targeted vault.secrets existence/lookup/delete by exact UUID. That
--   path needs no grants: the extension objects are administered by the
--   migration role. The direct Postgres connection is the only OpenOrc
--   application path to runtime-control credentials.
-- - This migration revokes PUBLIC and the Supabase browser-facing roles
--   (anon, authenticated) from the vault schema, the secret table/view, and
--   the secret-management functions.
-- - Supabase's platform manages privileged Postgres `service_role` access to
--   Vault (direct platform grants, re-established by platform post-create
--   handling on hosted previews). OpenOrc deliberately accepts that trusted
--   Supabase administrative/platform boundary: Postgres `service_role` is
--   the database role of that tier, this migration neither fights nor
--   overrides its platform-managed Vault privileges, and no OpenOrc
--   application path uses Postgres `service_role` — or the Supabase secret
--   API key — to read or write runtime-control credentials.
-- - `vault` must never become an exposed Data API schema: browser clients
--   reach OpenOrc workflow data only through the OpenOrc application API,
--   never through Supabase Data APIs (see supabase/AGENTS.md).
-- - The revokes run unconditionally so the posture is re-asserted on every
--   application of the committed migration. The effective privilege posture
--   (anon/authenticated denied; the backend Postgres path allowed) is
--   proven against a real Supabase branch by the integration-marked suite
--   (tests/integration/test_connection_credential_services.py).
-- - The Supabase secret API key (the current `sb_secret_...` administrative
--   credential) remains the credential for the privileged Supabase
--   administrative operations that need it; nothing here changes the
--   Postgres `service_role` role outside the vault schema.
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

-- Schema lockout: USAGE (and CREATE) on the vault schema are denied to the
-- browser-facing roles, mirroring the openorc schema lockout posture.

revoke all on schema vault from public;
revoke all on schema vault from anon;
revoke all on schema vault from authenticated;

-- Encrypted secret storage and the decrypt-on-read view: denied to every
-- browser-facing role. The view exposes decrypted secrets, so its SELECT is
-- as sensitive as the plaintext itself.

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


