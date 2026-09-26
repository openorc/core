"""Convention tests for the GitHub relationship-mirror migration (issue #60).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the stable subject-identity
uniqueness keys, the plain numeric related-endpoint facts with NO foreign
key to openorc.repositories (a related repository need not be configured as
an OpenOrc Repository), the true-ownership composite cascade edges, the
fingerprint CHECK vocabulary on the Task source baseline, and the absence
of grants, freshness columns, and credential-bearing columns. Behavioral
invariants are proven against a real database by the integration-marked
suite.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_dir() -> Path:
    return REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_github_issue_relations_and_task_source.sql")
    )
    assert len(matches) == 1, (
        "expected exactly one add_github_issue_relations_and_task_source migration, "
        f"found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _table_block(text: str, table: str) -> str:
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def test_the_migration_creates_the_three_mirrors() -> None:
    text = _migration_text()
    for table in (
        "openorc.github_issue_hierarchy",
        "openorc.github_issue_sub_issues",
        "openorc.github_issue_dependencies",
    ):
        assert f"create table {table}" in text


def test_the_stable_subject_identity_is_the_canonical_uniqueness_key() -> None:
    text = _migration_text()
    assert "unique (repository_id, github_issue_id)" in _table_block(
        text, "openorc.github_issue_hierarchy"
    )
    assert (
        "unique (repository_id, github_issue_id, child_github_repository_id, child_github_issue_id)"
        in _table_block(text, "openorc.github_issue_sub_issues")
    )
    assert (
        "unique (repository_id, github_issue_id, blocker_github_repository_id, "
        "blocker_github_issue_id)" in _table_block(text, "openorc.github_issue_dependencies")
    )


def test_related_endpoints_are_plain_numeric_facts_without_local_foreign_keys() -> None:
    # A parent, blocker, or sub-issue repository need not be configured as
    # an OpenOrc Repository in this Workspace: the numeric GitHub identities
    # are plain CHECK-constrained facts, never foreign keys.
    text = _migration_text()
    for column in (
        "parent_github_repository_id",
        "child_github_repository_id",
        "blocker_github_repository_id",
        "parent_github_issue_id",
        "child_github_issue_id",
        "blocker_github_issue_id",
    ):
        assert f"{column} bigint not null" in text
        assert f"check ({column} > 0)" in text
    for table in ("github_issue_hierarchy", "github_issue_sub_issues", "github_issue_dependencies"):
        block = _table_block(text, f"openorc.{table}")
        assert "references" not in block


def test_the_workspace_scope_edges_are_true_ownership_cascades() -> None:
    text = _migration_text().lower()
    for table in ("github_issue_hierarchy", "github_issue_sub_issues", "github_issue_dependencies"):
        statement = [
            s
            for s in text.split(";")
            if f"alter table openorc.{table}" in s
            and "foreign key (repository_id, workspace_id)" in s
        ]
        assert len(statement) == 1, f"expected exactly one cascade edge for {table}"
        assert "references openorc.repositories (id, workspace_id)" in statement[0]
        assert "on delete cascade" in statement[0]
        assert "deferrable" not in statement[0]


def test_the_task_source_baseline_carries_the_fingerprint_vocabulary() -> None:
    text = _migration_text()
    assert "alter table openorc.tasks" in text
    assert "add column source_requirements_fingerprint text not null" in text
    assert "check (source_requirements_fingerprint ~ '^[0-9a-f]{64}$')" in text


def test_the_mirrors_carry_no_freshness_or_credential_columns() -> None:
    text = _migration_text()
    block_text = " ".join(
        _table_block(text, f"openorc.{table}")
        for table in (
            "github_issue_hierarchy",
            "github_issue_sub_issues",
            "github_issue_dependencies",
        )
    ).lower()
    for forbidden in ("observed_at", "freshness", "reconciled_at", "token", "secret", "credential"):
        assert forbidden not in block_text


def test_the_migration_grants_no_privileges() -> None:
    text = _migration_text().lower()
    assert "grant " not in text
    assert "to anon" not in text
