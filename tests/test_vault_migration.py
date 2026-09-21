"""Convention tests for the Supabase Vault migration (issue #55).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed additive migration:

- the extension is enabled idempotently with cascade;
- the vault schema, its secret table/view, and its secret-management
  functions are revoked from the browser-facing roles (PUBLIC, anon,
  authenticated);
- Supabase's platform manages privileged Postgres `service_role` access to
  Vault, and OpenOrc deliberately accepts that trusted
  administrative/platform boundary: the migration contains no `service_role`
  revokes and no platform-internal function manipulation;
- no grants are made to anyone (the backend path is the migration role,
  which needs none), and no product schema or environment/project-specific
  identifier is touched.

The effective privilege posture (anon/authenticated denied; the backend
Postgres path allowed) is proven against a real Supabase branch by the
integration-marked suite.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

_MIGRATION_GLOB = "*_enable_supabase_vault.sql"

# The browser-facing roles the migration durably locks out of the vault
# schema, the secret table/view, and the secret-management functions.
_BROWSER_FACING_ROLES = ("public", "anon", "authenticated")
# Supabase's platform manages privileged Postgres `service_role` access to
# Vault (direct platform grants, re-established by platform post-create
# handling). OpenOrc deliberately accepts that trusted administrative/
# platform boundary and never revokes or overrides it.
_PLATFORM_MANAGED_ROLE = "service_role"
# Secret-management functions the migration role can administer on hosted
# Supabase: revoked from the browser-facing roles.
_SECRET_MANAGEMENT_FUNCTIONS = ("vault.create_secret", "vault.update_secret")
# Platform-administered internals: upstream already denies them to PUBLIC,
# OpenOrc neither uses nor administers them, and hosted Supabase refuses
# revokes on them (SQLSTATE 42501). The migration must not touch them —
# including for the platform-managed service_role.
_PLATFORM_ADMINISTERED_FUNCTIONS = (
    "vault._crypto_aead_det_encrypt",
    "vault._crypto_aead_det_decrypt",
    "vault._crypto_aead_det_noncegen",
)


def _vault_sql() -> str:
    """Return the migration's SQL (comments stripped, normalized) lowercased."""
    matches = sorted(MIGRATIONS_DIR.glob(_MIGRATION_GLOB))
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


def test_vault_schema_and_secrets_are_revoked_from_browser_facing_roles() -> None:
    sql = _vault_sql()

    for role in _BROWSER_FACING_ROLES:
        assert f"revoke all on schema vault from {role};" in sql
        assert f"revoke all on vault.secrets from {role};" in sql
        assert f"revoke all on vault.decrypted_secrets from {role};" in sql
        for function in _SECRET_MANAGEMENT_FUNCTIONS:
            assert f"revoke execute on function {function} from {role};" in sql


def test_migration_accepts_the_platform_managed_service_role_boundary() -> None:
    sql = _vault_sql()

    # Supabase's platform manages privileged Postgres `service_role` access
    # to Vault (direct platform grants, re-established by platform
    # post-create handling). OpenOrc deliberately accepts that trusted
    # administrative/platform boundary: no OpenOrc-managed `service_role`
    # revokes exist, and none may be added — the direct backend Postgres
    # connection remains the only OpenOrc application path to Vault, and
    # `vault` must stay outside the exposed Data API schemas.
    assert "from service_role" not in sql


def test_platform_administered_functions_are_not_revoked() -> None:
    sql = _vault_sql()

    # The platform-administered crypto internals (encrypt/decrypt/noncegen)
    # are not OpenOrc-managed: upstream already denies them to PUBLIC,
    # OpenOrc neither uses nor administers them, and hosted Supabase refuses
    # revokes on them (SQLSTATE 42501) — for any role, including the
    # platform-managed service_role.
    for function in _PLATFORM_ADMINISTERED_FUNCTIONS:
        for role in (*_BROWSER_FACING_ROLES, _PLATFORM_MANAGED_ROLE):
            assert f"revoke execute on function {function} from {role};" not in sql


def test_migration_grants_no_privileges_to_anyone() -> None:
    # Backend access needs no grants (the migration role administers the
    # extension objects); any grant statement would widen the posture instead
    # of narrowing it.
    assert re.search(r"\bgrant\b", _vault_sql()) is None


def test_migration_hardcodes_no_environment_or_project_identifiers() -> None:
    sql = _vault_sql()

    # No project refs, host names, URLs, or literal hex identifiers: project
    # identity comes from actual environment/repository configuration.
    assert re.search(r"\b[0-9a-f]{16,}\b", sql) is None
    assert "supabase.co" not in sql
    assert "http" not in sql
