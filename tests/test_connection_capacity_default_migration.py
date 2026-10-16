"""Convention tests for the Connection default session-capacity bump (#200).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed forward-only migration that
runtime behavior does not naturally establish: exactly the
``session_capacity`` column default changes from 1 to 2, with no backfill, no
row rewrites, and no other column/table/validation/privilege touched — while
the historical initial migration keeps its shipped ``default 1`` baseline.
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
CONNECTIONS_TABLE = "openorc.connections"


def _migration_text() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_set_connection_session_capacity_default_two.sql")
    )
    assert len(matches) == 1, (
        "expected exactly one set_connection_session_capacity_default_two "
        f"migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def test_migration_sets_only_the_session_capacity_default_to_two() -> None:
    text = _migration_text()
    assert f"alter table {CONNECTIONS_TABLE}" in text
    assert "alter column session_capacity set default 2" in text
    # The change is exactly one default statement: no other alteration,
    # column, table, or validation is touched.
    assert text.count("alter table") == 1
    assert text.count("alter column") == 1
    assert "drop column" not in text
    assert "drop constraint" not in text
    assert "add column" not in text
    assert "set not null" not in text


def test_migration_performs_no_data_backfill() -> None:
    # Existing rows keep their stored capacity — including an explicitly
    # configured 1, a valid dedicated single-role Connection: the Owner's
    # configuration and live reservations are never rewritten by a default
    # change.
    text = _migration_text()
    assert "insert into" not in text
    assert "update openorc.connections" not in text
    assert "delete from" not in text


def test_migration_leaves_the_positive_capacity_validation_intact() -> None:
    # Nonpositive capacities remain rejected by the initial migration's
    # durable rule; this migration changes only the default.
    text = _migration_text()
    assert "check (" not in text.lower()
    assert "constraint" not in text.lower()


def test_migration_never_touches_other_tables() -> None:
    text = _migration_text()
    assert "task_agent_sessions" not in text
    assert "workflow_role_bindings" not in text


def test_migration_never_references_supabase_managed_schemas() -> None:
    text = _migration_text()
    for forbidden in ("auth.", "vault.", "storage.", "realtime."):
        assert forbidden not in text


def test_migration_grants_no_privileges() -> None:
    text = _migration_text()
    assert "grant" not in text.lower()
    assert "revoke" not in text.lower()
