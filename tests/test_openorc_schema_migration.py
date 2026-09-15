"""Convention tests for the OpenOrc application schema migration.

The ordinary suite cannot execute Postgres; these tests assert the durable
conventions of the committed migration file (dedicated `openorc` schema,
locked down against PUBLIC and browser-facing roles). Real schema/privilege
behavior is validated through the documented non-production migration tooling.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _schema_migration_name() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_openorc_schema.sql"))
    assert len(matches) == 1, (
        f"expected exactly one create_openorc_schema migration, found {matches}"
    )
    return matches[0]


def test_openorc_schema_migration_establishes_and_locks_the_schema() -> None:
    text = (MIGRATIONS_DIR / _schema_migration_name()).read_text(encoding="utf-8").lower()

    assert "create schema if not exists openorc" in text
    assert "revoke all on schema openorc from public" in text
    assert "revoke all on schema openorc from anon" in text
    assert "revoke all on schema openorc from authenticated" in text


def test_openorc_schema_migration_grants_nothing_to_browser_facing_roles() -> None:
    text = (MIGRATIONS_DIR / _schema_migration_name()).read_text(encoding="utf-8").lower()

    assert "grant usage on schema openorc to anon" not in text
    assert "grant usage on schema openorc to authenticated" not in text
    assert "grant create on schema openorc" not in text


def test_openorc_schema_migration_sorts_after_the_bootstrap_baseline() -> None:
    baseline = sorted(p.name for p in MIGRATIONS_DIR.glob("*.sql"))[0]

    assert baseline == "20260914035628_bootstrap_baseline.sql"
    assert _schema_migration_name() > baseline
