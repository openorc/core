"""Convention tests for the GitHub installation routing migration (issue #57).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the Workspace ownership cascade, the
per-Workspace external installation uniqueness, the composite hook that makes
cross-Workspace Repository routing unrepresentable, the nullable legacy route,
the restrictive-deferred route foreign-key classification, and the absence of
credential-bearing columns. Behavioral invariants are proven against a real
database by ``tests/integration/test_github_installation_persistence.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

INSTALLATIONS_TABLE = "openorc.github_installations"
ROUTE_FK = "repositories_github_installation_id_workspace_id_fkey"


def _migration_text() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_github_installation_routing.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_github_installation_routing migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _table_block(text: str, table: str) -> str:
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def _add_constraint_statement(text: str, constraint: str) -> str:
    statements = [s for s in text.split(";") if re.search(rf"\badd constraint {constraint}\b", s)]
    assert len(statements) == 1, f"expected exactly one add constraint for {constraint}"
    return statements[0]


def test_migration_creates_the_installation_table_and_route_column() -> None:
    text = _migration_text()
    assert f"create table {INSTALLATIONS_TABLE}" in text
    assert "alter table openorc.repositories" in text
    assert "add column github_installation_id uuid," in text


def test_the_workspace_ownership_edge_cascades() -> None:
    # True ownership edge: the installation record exists solely within its
    # Workspace's aggregate. Deleting OpenOrc configuration never uninstalls
    # the GitHub App — the cascade removes only OpenOrc's own rows. The edge
    # is re-declared with its explicit action (deletion-ownership vocabulary).
    statement = _add_constraint_statement(
        _migration_text().lower(), "github_installations_workspace_id_fkey"
    )
    assert "foreign key (workspace_id)" in statement
    assert "references openorc.workspaces (id)" in statement
    assert "on delete cascade" in statement
    assert "deferrable" not in statement


def test_stable_external_ids_are_explicit_and_positive() -> None:
    block = _table_block(_migration_text(), INSTALLATIONS_TABLE)
    assert "github_installation_id bigint not null check (github_installation_id > 0)" in block
    assert "github_account_id bigint not null check (github_account_id > 0)" in block


def test_observed_metadata_is_nonblank_text_without_an_enum() -> None:
    block = _table_block(_migration_text(), INSTALLATIONS_TABLE)
    assert "check (account_login ~ '\\S')" in block
    assert "check (account_type ~ '\\S')" in block
    # Observed metadata is never configuration authority: no enum/CHECK list.
    assert "account_type in (" not in block


def test_suspended_at_is_a_nullable_observation() -> None:
    text = _migration_text()
    block = _table_block(text, INSTALLATIONS_TABLE)
    assert "suspended_at timestamptz," in block
    assert "suspended_at timestamptz not null" not in text


def test_per_workspace_external_installation_uniqueness() -> None:
    # One canonical record per Workspace per external installation ID; the
    # same external installation may exist independently in other Workspaces.
    block = _table_block(_migration_text(), INSTALLATIONS_TABLE)
    assert "unique (workspace_id, github_installation_id)" in block


def test_unique_hook_for_the_composite_route_foreign_key() -> None:
    block = _table_block(_migration_text(), INSTALLATIONS_TABLE)
    assert "unique (id, workspace_id)" in block


def test_the_route_column_is_nullable_and_single() -> None:
    text = _migration_text()
    assert "add column github_installation_id uuid," in text
    assert "github_installation_id uuid not null" not in text


def test_the_route_fk_is_composite_restrictive_and_deferred() -> None:
    statement = _add_constraint_statement(_migration_text().lower(), ROUTE_FK)
    assert "foreign key (github_installation_id, workspace_id)" in statement
    assert "references openorc.github_installations (id, workspace_id)" in statement
    assert "deferrable initially deferred" in statement
    assert "on delete cascade" not in statement


def test_no_credential_bearing_column_exists() -> None:
    block = _table_block(_migration_text().lower(), INSTALLATIONS_TABLE)
    for word in ("token", "secret", "private_key", "pat", "oauth", "credential"):
        assert word not in block, word


def test_no_privileges_are_granted() -> None:
    assert "grant " not in _migration_text().lower()
