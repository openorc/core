"""Convention tests for the Supabase Vault migrations (issue #55).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the two committed additive migrations:

- the enablement migration enables the extension idempotently with cascade
  and locks the vault schema, its secret table/view, and its
  secret-management functions down for PUBLIC, anon, and authenticated. It
  deliberately contains NO service_role handling: Supabase's platform
  post-create handling re-establishes the platform's service_role Vault
  grants after the extension-enabling migration reports successful, so
  same-migration service_role revokes would be silently re-granted away;
- the follow-up lockout migration runs after that platform handling has
  settled and enforces the service_role lockout, including a guarded
  per-role best-effort revoke of the platform-granted internal decryption
  helper; the platform-administered encrypt/nonce internals are deliberately
  not revoked by OpenOrc (upstream already denies them to PUBLIC and hosted
  Supabase refuses revokes on them);
- no grants are made to anyone (the backend path is the migration role,
  which needs none), and no product schema or environment/project-specific
  identifier is touched.

The effective privilege posture — including that service_role has no Vault
access on a deployed branch — is proven against a real Supabase branch by
the integration-marked suite.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

_ENABLEMENT_GLOB = "*_enable_supabase_vault.sql"
_LOCKOUT_GLOB = "*_lock_vault_from_service_role.sql"

# Every role that must have no Vault access; the backend reaches Vault as the
# migration role, which deliberately appears in no revoke.
_NON_BACKEND_ROLES = ("public", "anon", "authenticated", "service_role")
# Roles the enablement migration can lock out durably: the platform
# re-grants service_role after the extension-enabling migration reports
# successful, so service_role is handled by the follow-up lockout migration.
_ENABLEMENT_LOCKED_ROLES = ("public", "anon", "authenticated")
# Secret-management functions the migration role can administer on hosted
# Supabase: revoked unconditionally.
_SECRET_MANAGEMENT_FUNCTIONS = ("vault.create_secret", "vault.update_secret")
# The internal decryption helper the platform may grant to non-backend roles:
# revoked from those roles by the lockout migration, best-effort per role.
_DECRYPTION_HELPER = "vault._crypto_aead_det_decrypt"
_DECRYPTION_HELPER_ROLES = ("anon", "authenticated", "service_role")
# Platform-administered internals: upstream already denies them to PUBLIC,
# OpenOrc neither uses nor administers them, and a redundant revoke is
# refused by hosted Supabase (SQLSTATE 42501). No Vault migration may touch
# them.
_PLATFORM_INTERNAL_CRYPTO = (
    "vault._crypto_aead_det_encrypt",
    "vault._crypto_aead_det_noncegen",
)


def _migration_sql(glob_pattern: str) -> tuple[str, str]:
    """Return (file name, normalized lowercase SQL) for exactly one migration."""
    matches = sorted(MIGRATIONS_DIR.glob(glob_pattern))
    assert len(matches) == 1, (
        f"expected exactly one {glob_pattern} migration, found {[match.name for match in matches]}"
    )
    raw = (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")
    statements = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--"))
    return matches[0].name, " ".join(statements.lower().split())


def test_enablement_migration_is_followed_by_the_service_role_lockout() -> None:
    enablement_name, _ = _migration_sql(_ENABLEMENT_GLOB)
    lockout_name, _ = _migration_sql(_LOCKOUT_GLOB)

    # The lockout must run after the enablement migration (and after the
    # platform's post-enablement grant handling has settled), so the lockout
    # migration sorts strictly after the enablement migration.
    assert enablement_name < lockout_name


def test_vault_extension_is_enabled_additively() -> None:
    _, sql = _migration_sql(_ENABLEMENT_GLOB)

    # Idempotent enable, matching upstream's documented install; `cascade`
    # covers older images that still model pgsodium as a dependency.
    assert "create extension if not exists supabase_vault cascade;" in sql
    # The extension's control file pins its objects to the `vault` schema; the
    # migration must not relocate it, touch the product schema, or replace
    # platform state.
    assert "with schema" not in sql
    assert "openorc." not in sql
    for stray in ("create table", "drop ", "insert into", "alter system", "create schema"):
        assert stray not in sql


def test_enablement_migration_locks_out_public_and_browser_roles() -> None:
    _, sql = _migration_sql(_ENABLEMENT_GLOB)

    for role in _ENABLEMENT_LOCKED_ROLES:
        assert f"revoke all on schema vault from {role};" in sql
        assert f"revoke all on vault.secrets from {role};" in sql
        assert f"revoke all on vault.decrypted_secrets from {role};" in sql
        for function in _SECRET_MANAGEMENT_FUNCTIONS:
            assert f"revoke execute on function {function} from {role};" in sql

    # The platform re-establishes its service_role Vault grants after this
    # migration reports successful, so same-migration service_role revokes
    # are deliberately absent — the lockout migration enforces them after the
    # platform handling has settled.
    assert "from service_role" not in sql


def test_lockout_migration_revokes_the_service_role_vault_surface() -> None:
    _, sql = _migration_sql(_LOCKOUT_GLOB)

    assert "revoke all on schema vault from service_role;" in sql
    assert "revoke all on vault.secrets from service_role;" in sql
    assert "revoke all on vault.decrypted_secrets from service_role;" in sql
    for function in _SECRET_MANAGEMENT_FUNCTIONS:
        assert f"revoke execute on function {function} from service_role;" in sql

    # The lockout migration is a pure hardening migration: it enables
    # nothing and touches no product schema.
    for stray in (
        "create extension",
        "create table",
        "drop ",
        "insert into",
        "alter system",
        "create schema",
        "openorc.",
    ):
        assert stray not in sql


def test_lockout_migration_revokes_the_decryption_helper_per_role() -> None:
    _, sql = _migration_sql(_LOCKOUT_GLOB)

    # Current platform images may grant EXECUTE on the internal decryption
    # helper to non-backend roles; the lockout migration revokes each role's
    # grant independently, guarded so an unadministrable platform-managed
    # grant cannot fail the migration (the integration privilege assertions
    # still prove the resulting posture).
    assert "to_regprocedure" in sql
    assert "vault._crypto_aead_det_decrypt(bytea,bytea,bigint,bytea,bytea)" in sql
    for role in _DECRYPTION_HELPER_ROLES:
        assert f"revoke execute on function {_DECRYPTION_HELPER} from {role};" in sql
    # One independent exception guard per role: a failed revoke on one role
    # can never prevent the others from being enforced.
    assert sql.count("insufficient_privilege") == 3
    # PUBLIC never holds the helper on supported images (upstream denies it),
    # so no public revoke is attempted.
    assert f"revoke execute on function {_DECRYPTION_HELPER} from public;" not in sql


def test_platform_administered_crypto_internals_are_not_revoked() -> None:
    combined = _migration_sql(_ENABLEMENT_GLOB)[1] + " " + _migration_sql(_LOCKOUT_GLOB)[1]

    # The encrypting/nonce-generation internals are platform-administered:
    # upstream already denies them to PUBLIC, OpenOrc neither uses nor
    # administers them, and a redundant revoke is refused by hosted Supabase
    # (SQLSTATE 42501) — neither migration may contain one.
    for function in _PLATFORM_INTERNAL_CRYPTO:
        for role in _NON_BACKEND_ROLES:
            assert f"revoke execute on function {function} from {role};" not in combined


def test_migrations_grant_no_privileges_to_anyone() -> None:
    # Backend access needs no grants (the migration role administers the
    # extension objects); any grant statement would widen the posture instead
    # of narrowing it.
    combined = _migration_sql(_ENABLEMENT_GLOB)[1] + " " + _migration_sql(_LOCKOUT_GLOB)[1]

    assert re.search(r"\bgrant\b", combined) is None


def test_migrations_hardcode_no_environment_or_project_identifiers() -> None:
    combined = _migration_sql(_ENABLEMENT_GLOB)[1] + " " + _migration_sql(_LOCKOUT_GLOB)[1]

    # No project refs, host names, URLs, or literal hex identifiers: project
    # identity comes from actual environment/repository configuration.
    assert re.search(r"\b[0-9a-f]{16,}\b", combined) is None
    assert "supabase.co" not in combined
    assert "http" not in combined
