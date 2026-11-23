"""Convention tests for the claim-state nullability migration (issue #372).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the GitHub observation columns of the
installation and Repository records become nullable (the issue #372 claim
state), no durable identity or routing column changes, no column or table is
added or dropped, and no privilege is granted. The claim/observation
provenance is structural (observation columns present iff trusted
reconciliation wrote them), so the migration introduces no provenance column.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_allow_installation_and_repository_claim_state.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one claim-state nullability migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _alter_statements(text: str, table: str) -> list[str]:
    statements = [
        statement.strip()
        for statement in text.split(";")
        if re.search(rf"alter table {re.escape(table)}\b", statement)
    ]
    assert statements, f"expected at least one alter table statement for {table}"
    return statements


def test_the_installation_observation_columns_become_nullable() -> None:
    statements = "\n".join(_alter_statements(_migration_text(), "openorc.github_installations"))
    for column in ("github_account_id", "account_login", "account_type"):
        assert re.search(rf"alter column {column} drop not null", statements), column


def test_the_repository_observation_columns_become_nullable() -> None:
    statements = "\n".join(_alter_statements(_migration_text(), "openorc.repositories"))
    for column in ("owner_login", "name", "html_url", "is_private"):
        assert re.search(rf"alter column {column} drop not null", statements), column


def test_no_identity_or_routing_column_changes() -> None:
    text = _migration_text().lower()
    for column in (
        "github_installation_id",
        "github_repository_id",
        "suspended_at",
        "default_branch",
        "id",
        "workspace_id",
        "project_id",
        "created_at",
        "updated_at",
    ):
        assert f"alter column {column}" not in text, column


def test_no_column_table_or_index_is_added_or_dropped() -> None:
    text = _migration_text().lower()
    assert "add column" not in text
    assert "drop column" not in text
    assert "create table" not in text
    assert "create index" not in text
    assert "drop table" not in text


def test_no_privileges_are_granted() -> None:
    assert "grant " not in _migration_text().lower()
