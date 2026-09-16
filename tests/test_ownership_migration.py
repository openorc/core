"""Convention tests for the ownership persistence migration (issue #19).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish. Behavioral invariants (ownership
foreign keys, direct-scope consistency, per-Workspace repository uniqueness)
are proven against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

OWNERSHIP_TABLES = ("profiles", "workspaces", "projects", "repositories")


def _ownership_migration_text() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_ownership_tables.sql"))
    assert len(matches) == 1, (
        f"expected exactly one create_ownership_tables migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8").lower()


def test_ownership_migration_creates_the_four_ownership_tables() -> None:
    text = _ownership_migration_text()

    for table in OWNERSHIP_TABLES:
        assert f"create table openorc.{table}" in text


def test_ownership_migration_never_references_supabase_managed_schemas() -> None:
    text = _ownership_migration_text()

    # Profile identity is the Supabase Auth user UUID by value; OpenOrc
    # migrations must never create dependencies on Supabase-managed schemas.
    assert "auth." not in text
    assert "storage." not in text


def test_ownership_migration_introduces_no_role_privileges() -> None:
    text = _ownership_migration_text()

    # Browser-facing roles keep no access: the openorc schema lockout carries
    # isolation, and any future runtime role permission is a separate reviewed
    # decision in its own migration.
    assert "grant" not in text
