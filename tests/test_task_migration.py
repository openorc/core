"""Convention tests for the Task migration (issue #21).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the nine-state status vocabulary, the
archival/terminal lifecycle invariant, both exclusivity partial unique
indexes, the stable-issue identity checks, the state_token column, the
Workspace-scope composite foreign key (and the repositories hook it needs),
and the deliberate absences (no current-Execution pointer, no duplicated
subordinate facts, no GitHub presentation columns, no native enums).
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

TASKS_TABLE = "openorc.tasks"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_tasks.sql"))
    assert len(matches) == 1, f"expected exactly one create_tasks migration, found {matches}"
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _migration_text() -> str:
    return _migration_text_raw().lower()


def _table_block(text: str, table: str) -> str:
    """Return the create-table statement body for one openorc table."""
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def test_migration_creates_the_tasks_table() -> None:
    assert f"create table {TASKS_TABLE}" in _migration_text()


def test_status_check_enumerates_exactly_the_nine_settled_states() -> None:
    block = _table_block(_migration_text(), TASKS_TABLE)
    for value in (
        "ready_to_plan",
        "queued",
        "planning",
        "waiting_for_owner",
        "implementing",
        "reviewing",
        "blocked",
        "cancelled",
        "completed",
    ):
        assert f"'{value}'" in block, f"status check is missing '{value}'"


def test_archival_lifecycle_invariant_is_enforced_in_both_directions() -> None:
    # archived_at IS NULL iff status is nonterminal: every CANCELLED or
    # COMPLETED attempt is archived history, and every nonterminal attempt is
    # current. Terminal outcome stays distinguishable through status.
    block = _table_block(_migration_text(), TASKS_TABLE)
    assert "archived_at is null and status not in ('cancelled', 'completed')" in block
    assert "archived_at is not null and status in ('cancelled', 'completed')" in block


def test_github_issue_identity_is_the_stable_column() -> None:
    block = _table_block(_migration_text_raw(), TASKS_TABLE)
    assert "github_issue_id bigint not null check (github_issue_id > 0)" in block
    # Repository-local issue number is observed address metadata only.
    assert "github_issue_number integer not null check (github_issue_number > 0)" in block
    # GitHub-owned presentation facts are deliberately not stored.
    assert "github_issue_title" not in _migration_text()
    assert "github_issue_state" not in _migration_text()


def test_state_token_is_the_opaque_optimistic_concurrency_column() -> None:
    block = _table_block(_migration_text(), TASKS_TABLE)
    assert "state_token uuid not null default gen_random_uuid()" in block


def test_current_object_pointers_are_nullable_and_content_free() -> None:
    block = _table_block(_migration_text(), TASKS_TABLE)
    assert "current_plan_revision_id uuid," in block
    assert "current_owner_gate_id uuid," in block
    # No FKs yet: PlanRevision (#23) and OwnerGate (#24) tables do not exist,
    # so the pointer accommodation stays dependency-order safe.
    assert "references openorc.plan" not in _migration_text()
    assert "references openorc.owner" not in _migration_text()


def test_no_singular_current_execution_pointer() -> None:
    assert "current_execution_id" not in _migration_text()


def test_no_duplicated_subordinate_facts_on_tasks() -> None:
    # One fact, one home: gate type/subject, review outcomes, and blocking
    # context belong on OwnerGate/ReviewLoop/ReviewIteration/TaskBlock.
    text = _migration_text()
    for forbidden in (
        "gate_type",
        "gate_subject",
        "review_outcome",
        "block_reason",
        "plan_content",
    ):
        assert forbidden not in text


def test_canonical_branch_ownership_is_a_task_level_column() -> None:
    block = _table_block(_migration_text_raw(), TASKS_TABLE)
    assert "canonical_feature_branch text check (" in block
    # Nonblank when set ('\S' matches at least one non-whitespace character).
    assert "canonical_feature_branch ~ '\\S'" in _migration_text_raw()
    # One-time binding is documented intent: created NULL, bound once, never
    # released or switched by a current Task — archival releases ownership.
    assert "binding is a one-time operation" in _migration_text()


def test_exclusivity_partial_unique_indexes() -> None:
    text = _migration_text()
    # One current Task per stable issue within a Repository.
    assert (
        "create unique index tasks_repository_issue_current_uniq" in text
        and "on openorc.tasks (repository_id, github_issue_id)" in text
    )
    # Exclusive current-Task ownership of a canonical branch.
    assert (
        "create unique index tasks_repository_canonical_branch_current_uniq" in text
        and "on openorc.tasks (repository_id, canonical_feature_branch)" in text
    )
    # Both partial indexes only constrain current (non-archived) rows, and the
    # branch index only constrains non-null branches.
    assert text.count("where archived_at is null") >= 2
    assert "where archived_at is null and canonical_feature_branch is not null" in text


def test_query_driven_indexes() -> None:
    text = _migration_text()
    assert "create index tasks_workspace_active_idx" in text
    assert "create index tasks_workspace_status_idx" in text
    assert "on openorc.tasks (workspace_id, status)" in text
    assert "create index tasks_repository_issue_idx" in text
    assert "on openorc.tasks (repository_id, github_issue_id)" in text


def test_workspace_scope_composite_foreign_key_with_repositories_hook() -> None:
    text = _migration_text()
    # Direct Workspace scope must agree with the Repository's Workspace.
    assert "foreign key (repository_id, workspace_id)" in text
    assert "references openorc.repositories (id, workspace_id)" in text
    # repositories gains the (id, workspace_id) hook this composite FK needs.
    assert "alter table openorc.repositories" in text
    assert "unique (id, workspace_id)" in text


def test_no_native_postgres_enums() -> None:
    assert "as enum" not in _migration_text()


def test_instants_are_timestamptz() -> None:
    block = _table_block(_migration_text(), TASKS_TABLE)
    assert "archived_at timestamptz" in block
    assert "created_at timestamptz not null default now()" in block
    assert "updated_at timestamptz not null default now()" in block
