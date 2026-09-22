"""Convention tests for the account-deletion attempt-state migration (issue #97).

The ordinary suite cannot execute Postgres; these tests assert the durable
conventions of the committed migration file: the three nullable attempt-state
columns on ``openorc.profiles`` and the composite CHECK that makes impossible
state tuples unrepresentable at the database boundary (either every column is
NULL — normal operation — or the state is 'active'/'uncertain' with both the
attempt UUID and the database-clock establishment time non-NULL). Real
constraint behavior is validated by the integration-marked suite.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_name() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_account_deletion_state.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_account_deletion_state migration, found {matches}"
    )
    return matches[0]


def test_migration_adds_the_three_attempt_state_columns() -> None:
    text = (MIGRATIONS_DIR / _migration_name()).read_text(encoding="utf-8")

    assert "add column account_deletion_state text" in text
    assert "add column account_deletion_attempt_id uuid" in text
    assert "add column account_deletion_started_at timestamptz" in text


def test_migration_enforces_the_composite_state_tuple_check() -> None:
    text = (MIGRATIONS_DIR / _migration_name()).read_text(encoding="utf-8")

    assert "profiles_account_deletion_state_tuple_check" in text
    # Normal operation: the whole tuple is NULL.
    assert "account_deletion_state is null" in text
    assert "account_deletion_attempt_id is null" in text
    assert "account_deletion_started_at is null" in text
    # A present state is exactly 'active' or 'uncertain' with both the
    # attempt UUID and the establishment time non-NULL.
    assert "account_deletion_state in ('active', 'uncertain')" in text
    assert "account_deletion_attempt_id is not null" in text
    assert "account_deletion_started_at is not null" in text


def test_migration_changes_no_privileges() -> None:
    text = (MIGRATIONS_DIR / _migration_name()).read_text(encoding="utf-8").lower()

    assert "grant " not in text
    assert "revoke " not in text


def test_migration_sorts_after_the_vault_enablement_migration() -> None:
    vault = sorted(p.name for p in MIGRATIONS_DIR.glob("*enable_supabase_vault.sql"))
    assert len(vault) == 1
    assert _migration_name() > vault[0]
