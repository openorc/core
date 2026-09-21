"""Convention tests for the Supabase Vault migration (issue #55).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed additive migration: the
extension is enabled idempotently with cascade; the vault schema, its secret
table/view, and its secret-management functions are revoked from every
non-backend role (PUBLIC, anon, authenticated, service_role); the platform
may grant EXECUTE on the internal decryption helper to non-backend roles and
the migration revokes that grant best-effort; the platform-administered
encrypt/nonce internals are deliberately not revoked by OpenOrc (upstream
already denies them to PUBLIC and hosted Supabase refuses revokes on them);
no grants are made to anyone (the backend path is the migration role, which
needs none); and no product schema or environment/project-specific
identifier is touched. The effective privilege posture is proven against a
real Supabase branch by the integration-marked suite.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

# Every role that must have no Vault access; the backend reaches Vault as the
# migration role, which deliberately appears in no revoke.
_NON_BACKEND_ROLES = ("public", "anon", "authenticated", "service_role")
# Roles the platform may grant the internal decryption helper to; PUBLIC
# never holds it on supported images (upstream denies it), so no public
# revoke is attempted.
_PLATFORM_NON_BACKEND_ROLES = ("anon", "authenticated", "service_role")
# Secret-management functions the migration role can administer on hosted
# Supabase: revoked unconditionally from every non-backend role.
_SECRET_MANAGEMENT_FUNCTIONS = ("vault.create_secret", "vault.update_secret")
# The internal decryption helper the platform may grant to non-backend roles:
# revoked from those roles, best-effort (guarded against an
# unadministrable platform-managed grant so the migration always applies).
_DECRYPTION_HELPER = "vault._crypto_aead_det_decrypt"
# Platform-administered internals: upstream already denies them to PUBLIC,
# OpenOrc neither uses nor administers them, and a redundant revoke is
# refused by hosted Supabase (SQLSTATE 42501). The migration must never
# touch them.
_PLATFORM_INTERNAL_CRYPTO = (
    "vault._crypto_aead_det_encrypt",
    "vault._crypto_aead_det_noncegen",
)


def _vault_sql() -> str:
    """Return the migration's SQL (comments stripped, normalized) lowercased."""
    matches = sorted(MIGRATIONS_DIR.glob("*_enable_supabase_vault.sql"))
    assert len(matches) == 1, (
        f"expected exactly one enable_supabase_vault migration, found "
        f"{[match.name for match in matches]}"
    )
    raw = (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")
    statements = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--"))
    return " ".join(statements.lower().split())


def test_vault_extension_is_enabled_additively() -> None:
    sql = _vault_sql()

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


def test_vault_schema_and_secrets_are_revoked_from_every_non_backend_role() -> None:
    sql = _vault_sql()

    for role in _NON_BACKEND_ROLES:
        assert f"revoke all on schema vault from {role};" in sql
        assert f"revoke all on vault.secrets from {role};" in sql
        assert f"revoke all on vault.decrypted_secrets from {role};" in sql
        for function in _SECRET_MANAGEMENT_FUNCTIONS:
            assert f"revoke execute on function {function} from {role};" in sql


def test_platform_granted_decryption_helper_is_revoked_from_non_backend_roles() -> None:
    sql = _vault_sql()

    # Current Supabase images may grant EXECUTE on the internal decryption
    # helper to non-backend roles; the migration revokes those grants for
    # every role that could otherwise reach the backend surface, guarded so
    # an unadministrable platform-managed grant cannot fail the migration
    # (the integration privilege assertions still prove the resulting
    # posture).
    assert "to_regprocedure" in sql
    assert "vault._crypto_aead_det_decrypt(bytea,bytea,bigint,bytea,bytea)" in sql
    for role in _PLATFORM_NON_BACKEND_ROLES:
        assert f"revoke execute on function {_DECRYPTION_HELPER} from {role};" in sql
    # PUBLIC never holds the helper on supported images (upstream denies it),
    # so no public revoke is attempted.
    assert f"revoke execute on function {_DECRYPTION_HELPER} from public;" not in sql
    assert "exception" in sql
    assert "insufficient_privilege" in sql


def test_platform_administered_crypto_internals_are_not_revoked() -> None:
    sql = _vault_sql()

    # The encrypting/nonce-generation internals are platform-administered:
    # upstream already denies them to PUBLIC, OpenOrc neither uses nor
    # administers them, and a redundant revoke is refused by hosted Supabase
    # (SQLSTATE 42501) — it must never appear in the migration.
    for function in _PLATFORM_INTERNAL_CRYPTO:
        for role in _NON_BACKEND_ROLES:
            assert f"revoke execute on function {function} from {role};" not in sql


def test_migration_grants_no_privileges_to_anyone() -> None:
    # Backend access needs no grants (the migration runner owns the extension
    # objects); any grant statement would widen the posture instead of
    # narrowing it.
    assert re.search(r"\bgrant\b", _vault_sql()) is None


def test_migration_hardcodes_no_environment_or_project_identifiers() -> None:
    sql = _vault_sql()

    # No project refs, host names, URLs, or literal hex identifiers: project
    # identity comes from actual environment/repository configuration.
    assert re.search(r"\b[0-9a-f]{16,}\b", sql) is None
    assert "supabase.co" not in sql
    assert "http" not in sql
