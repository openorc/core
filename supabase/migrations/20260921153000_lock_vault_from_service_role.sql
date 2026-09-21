-- Vault service_role lockout (Phase 2A, issue #55).
--
-- The second of the two Vault migrations, and the one that completes the
-- lockout to the direct backend Postgres path. Supabase's platform
-- post-create handling re-establishes the platform's service_role Vault
-- grants after the migration that enables supabase_vault reports successful
-- (observed on hosted previews: direct grants to service_role on the vault
-- schema, the secret table/view, and the secret-management functions, even
-- though the enablement migration itself revoked them). This follow-up
-- migration runs after that platform handling has settled and revokes the
-- supported service_role Vault surface:
--
-- - USAGE on the vault schema, all privileges on vault.secrets and
--   vault.decrypted_secrets, and EXECUTE on vault.create_secret and
--   vault.update_secret. These revokes are strict: a failure here is a real
--   lockout failure and must fail the migration.
-- - EXECUTE on the internal decryption helper
--   (vault._crypto_aead_det_decrypt), per role and best-effort: current
--   platform images may grant it to non-backend roles, and an
--   unadministrable platform-managed grant must not fail the migration —
--   the integration privilege assertions
--   (tests/integration/test_connection_credential_services.py) prove the
--   effective posture instead. The encrypting/nonce-generation internals
--   are deliberately not revoked anywhere: upstream already denies them to
--   PUBLIC, OpenOrc neither uses nor administers them, and hosted Supabase
--   refuses revokes on them (SQLSTATE 42501).
--
-- This is a Vault-specific least-privilege restriction only: service_role is
-- unchanged outside the vault schema, and OpenOrc still requires the
-- privileged Supabase service-role/secret credential for other
-- Supabase-owned capabilities (Auth, administrative lifecycle). Keep this
-- migration after 20260921150000_enable_supabase_vault.sql.

revoke all on schema vault from service_role;

revoke all on vault.secrets from service_role;

revoke all on vault.decrypted_secrets from service_role;

revoke execute on function vault.create_secret from service_role;

revoke execute on function vault.update_secret from service_role;

-- Per-role guarded decryption-helper revokes: each role's grant is revoked
-- in its own exception-guarded block, so an unadministrable grant on one
-- role can never prevent the others from being enforced.

do $$
begin
    if to_regprocedure(
        'vault._crypto_aead_det_decrypt(bytea,bytea,bigint,bytea,bytea)'
    ) is not null then
        begin
            revoke execute on function vault._crypto_aead_det_decrypt from anon;
        exception
            when insufficient_privilege then
                null;
        end;
        begin
            revoke execute on function vault._crypto_aead_det_decrypt from authenticated;
        exception
            when insufficient_privilege then
                null;
        end;
        begin
            revoke execute on function vault._crypto_aead_det_decrypt from service_role;
        exception
            when insufficient_privilege then
                null;
        end;
    end if;
end $$;
