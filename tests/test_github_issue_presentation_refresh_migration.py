"""Convention tests for the #375 presentation/refresh migration.

The ordinary suite cannot execute Postgres. These tests assert the durable,
architectural properties of the committed migration that runtime behavior
does not naturally establish: every presentation column is a nullable
addition (no fabricated provider facts, no defaults), the refresh record's
closed status vocabulary and active-uniqueness backstops, the
deletion-ownership cascade classification (Workspace deletion removes the
bounded refresh history), and the nullable per-route completion evidence.
Constraint behavior is proven against a real database by the
integration-marked suites.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_github_issue_presentation_and_refresh.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one add_github_issue_presentation_and_refresh migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


_PRESENTATION_COLUMNS = (
    "state_reason",
    "html_url",
    "provider_created_at",
    "provider_closed_at",
    "user_json",
    "labels_json",
    "assignees_json",
    "milestone_json",
    "hierarchy_observed_at",
    "dependencies_observed_at",
)


def test_the_presentation_columns_are_nullable_additions_without_defaults() -> None:
    text = _migration_text()
    alter_block = re.search(r"alter table openorc\.github_issues(.*?);", text, re.DOTALL)
    assert alter_block is not None, "expected the presentation-column alter block"
    block = alter_block.group(1)
    for column in _PRESENTATION_COLUMNS:
        match = re.search(rf"add column {column} ([^,\n]+)", block)
        assert match is not None, f"expected the {column} presentation column"
        declaration = match.group(1)
        # Nullable (no NOT NULL) and no fabricated default: an existing row
        # has not observed the fact, and the projection stays honest.
        assert "not null" not in declaration
        assert "default" not in declaration


def test_the_observation_evidence_instants_are_presentation_only() -> None:
    text = _migration_text()
    for column in ("hierarchy_observed_at", "dependencies_observed_at"):
        assert f"add column {column} timestamptz" in text
    # The evidence is documentation-backed presentation observation context,
    # never a workflow fact and never a fingerprint input.
    # The observation-evidence columns are presentation-only: no fingerprint
    # column is added or altered by this migration.
    assert "alter table openorc.github_issues" in text
    assert text.count("requirements_fingerprint") == 0


def test_the_refresh_table_carries_the_closed_status_vocabulary() -> None:
    text = _migration_text()
    match = re.search(r"create table openorc\.github_refreshes\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, "expected the refresh table"
    block = match.group(1)
    assert "check (status in (" in block
    for status in (
        "'accepted'",
        "'running'",
        "'completed'",
        "'partially_completed'",
        "'failed'",
        "'unknown'",
    ):
        assert status in block
    # The hooks for the composite foreign key and the workspace edge.
    assert "unique (id, workspace_id)" in block


def test_the_refresh_foreign_keys_follow_the_deletion_ownership_vocabulary() -> None:
    text = _migration_text()
    # True ownership: Workspace deletion removes the bounded refresh history.
    assert re.search(
        r"github_refreshes_workspace_id_fkey\s*"
        r"foreign key \(workspace_id\)\s*"
        r"references openorc\.workspaces \(id\)\s*"
        r"on delete cascade",
        text,
    )
    # The nullable composite edge to the addressed Repository aggregate is
    # likewise true ownership; composite foreign keys never match NULL, so
    # workspace-scope rows are unaffected.
    assert re.search(
        r"github_refreshes_repository_id_workspace_id_fkey\s*"
        r"foreign key \(repository_id, workspace_id\)\s*"
        r"references openorc\.repositories \(id, workspace_id\)\s*"
        r"on delete cascade",
        text,
    )


def test_the_active_uniqueness_backstops_cover_both_scopes() -> None:
    text = _migration_text()
    assert re.search(
        r"create unique index github_refreshes_one_active_workspace_scope\s*"
        r"on openorc\.github_refreshes \(workspace_id\)\s*"
        r"where status in \('accepted', 'running'\) and repository_id is null",
        text,
    )
    assert re.search(
        r"create unique index github_refreshes_one_active_repository_scope\s*"
        r"on openorc\.github_refreshes \(workspace_id, repository_id\)\s*"
        r"where status in \('accepted', 'running'\) and repository_id is not null",
        text,
    )


def test_the_routing_completion_evidence_is_a_nullable_addition() -> None:
    text = _migration_text()
    match = re.search(
        r"alter table openorc\.github_webhook_delivery_routes\s*"
        r"add column resolved_at ([^;\n]+)",
        text,
    )
    assert match is not None, "expected the per-route completion evidence column"
    declaration = match.group(1)
    assert "not null" not in declaration
    assert "default" not in declaration


def test_the_migration_grants_no_privileges() -> None:
    text = _migration_text()
    assert not re.search(r"\bgrant\b", text, re.IGNORECASE)
    assert not re.search(r"\brevoke\b", text, re.IGNORECASE)
