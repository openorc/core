"""Convention tests for the Connection reported-observation retirement (#188).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed forward-only migration that
runtime behavior does not naturally establish: exactly the two retired
Connection-level observation columns are dropped, with no backfill, no
invented endpoint values, no other column/table/constraint/privilege touched,
and TaskAgentSession runtime-reported provenance left untouched. Behavioral
invariants are proven against a real database by the integration-marked suite
in ``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
CONNECTIONS_TABLE = "openorc.connections"


def _migration_text() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_remove_connection_reported_observations.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one remove_connection_reported_observations migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def test_migration_drops_exactly_the_two_retired_observation_columns() -> None:
    text = _migration_text()
    assert f"alter table {CONNECTIONS_TABLE}" in text
    assert "drop column reported_provider" in text
    assert "drop column reported_model" in text
    # The retirement is exactly the two placeholder observations: no other
    # column, table, or constraint is dropped and no type/default is altered.
    assert text.count("drop column") == 2
    assert "drop table" not in text
    assert "alter column" not in text
    assert "set default" not in text


def test_migration_never_touches_session_provenance_or_bindings() -> None:
    # TaskAgentSession runtime-reported provenance is optional session
    # history, deliberately outside this retirement; role bindings and their
    # #162 configuration are never touched either.
    text = _migration_text()
    assert "task_agent_sessions" not in text
    assert "workflow_role_bindings" not in text


def test_migration_performs_no_data_backfill_and_invents_no_endpoints() -> None:
    # Legacy rows do not gain invented endpoint values and no data migration
    # runs: the control endpoint is Owner configuration inside the existing
    # safe_config JSON container, never a persisted column or default.
    text = _migration_text()
    assert "insert into" not in text
    assert "update openorc.connections" not in text
    assert "add column" not in text


def test_migration_never_references_supabase_managed_schemas() -> None:
    text = _migration_text()
    for forbidden in ("auth.", "vault.", "storage.", "realtime."):
        assert forbidden not in text


def test_migration_grants_no_privileges() -> None:
    text = _migration_text()
    assert "grant" not in text.lower()
    assert "revoke" not in text.lower()
