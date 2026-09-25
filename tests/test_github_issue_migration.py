"""Convention tests for the GitHub issue projection migration (issue #59).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the canonical stable-identity
uniqueness, the durable issue-number race backstop, the composite
true-ownership cascade edge (with the parent hook that makes cross-Workspace
scope disagreement unrepresentable), the state and fingerprint CHECK
vocabulary, the nullable verbatim body, and the absence of grants and
credential-bearing columns. Behavioral invariants are proven against a real
database by ``tests/integration/test_github_issue_persistence.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

ISSUES_TABLE = "openorc.github_issues"
IDENTITY_FK = "github_issues_repository_id_workspace_id_fkey"


def _migration_text() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_github_issue_reconciliation.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_github_issue_reconciliation migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _table_block(text: str, table: str) -> str:
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def test_the_migration_creates_the_projection_table_and_the_parent_hook() -> None:
    text = _migration_text()
    assert f"create table {ISSUES_TABLE}" in text
    # The deliberate parent hook for the Workspace-owned-children composite
    # foreign key (the #53/#57 pattern).
    assert "alter table openorc.repositories" in text
    assert "unique (id, workspace_id)" in text


def test_the_stable_identity_is_the_canonical_uniqueness_key() -> None:
    # One canonical projection per Workspace Repository per stable GitHub
    # issue identity; the repository-local number is never identity.
    block = _table_block(_migration_text(), ISSUES_TABLE)
    assert "unique (repository_id, github_issue_id)" in block


def test_the_issue_number_is_a_durable_address_with_its_own_race_backstop() -> None:
    # Two concurrent reconciliations can never insert the same number under
    # different stable identities.
    block = _table_block(_migration_text(), ISSUES_TABLE)
    assert "unique (repository_id, issue_number)" in block
    assert "github_issue_id bigint not null check (github_issue_id > 0)" in block
    assert "issue_number bigint not null check (issue_number > 0)" in block


def test_the_workspace_scope_edge_is_a_true_ownership_cascade() -> None:
    # The projection exists solely within its Workspace Repository's
    # aggregate (the same classification as Tasks): the composite foreign
    # key makes direct-scope disagreement with the parent unrepresentable
    # and the cascade follows the sanctioned account-root deletion path.
    statement = _add_constraint_statement(_migration_text().lower(), IDENTITY_FK)
    assert "foreign key (repository_id, workspace_id)" in statement
    assert "references openorc.repositories (id, workspace_id)" in statement
    assert "on delete cascade" in statement
    assert "deferrable" not in statement


def test_the_state_and_fingerprint_columns_carry_durable_vocabularies() -> None:
    block = _table_block(_migration_text(), ISSUES_TABLE)
    assert "state text not null check (state in ('open', 'closed'))" in block
    assert "check (requirements_fingerprint ~ '^[0-9a-f]{64}$')" in block
    # GitHub's nullable body is stored verbatim: NULL means GitHub reported
    # a null body.
    assert "body text," in block
    assert "provider_updated_at timestamptz," in block


def test_the_projection_carries_no_credential_bearing_column() -> None:
    block = _table_block(_migration_text(), ISSUES_TABLE)
    # "primary key" is the only legitimate "key" occurrence; token/secret/
    # credential material has no column here.
    forbidden = re.compile(r"\b(token|secret|credential|password|private_key|api_key)")
    assert not forbidden.search(block)


def test_the_migration_grants_no_privileges() -> None:
    # The openorc schema lockout established by the create_openorc_schema
    # migration carries isolation; runtime access is backend-only.
    text = _migration_text().lower()
    assert "grant " not in text
    assert "to anon" not in text
    assert "to authenticated" not in text


def _add_constraint_statement(text: str, constraint: str) -> str:
    statements = [s for s in text.split(";") if re.search(rf"\badd constraint {constraint}\b", s)]
    assert len(statements) == 1, f"expected exactly one add constraint for {constraint}"
    return statements[0]
