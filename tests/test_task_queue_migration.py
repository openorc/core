"""Convention tests for the Task queued_at migration (issue #76).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: exactly one forward-only migration
adds the nullable ``queued_at`` column, the iff-QUEUED table CHECK is
enforced in both directions, the backfill stamps existing queued rows with
the DB clock once, the partial index serves exactly the bounded
Workspace-scoped ``(queued_at, id)`` FIFO read, and no historical migration
is rewritten. Behavioral invariants are proven against a real database by
the integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _queued_at_migrations() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*_add_task_queued_at.sql"))


def _migration_text() -> str:
    matches = _queued_at_migrations()
    assert len(matches) == 1, f"expected exactly one add_task_queued_at migration, found {matches}"
    text = matches[0].read_text(encoding="utf-8").lower()
    return " ".join(text.split())


def test_exactly_one_forward_only_migration_adds_the_column() -> None:
    text = _migration_text()
    assert "alter table openorc.tasks add column queued_at timestamptz" in text
    # The create_tasks migration predates the queue fact and must stay
    # unchanged: queued_at is added forward-only, never backported.
    create_matches = sorted(MIGRATIONS_DIR.glob("*_create_tasks.sql"))
    assert len(create_matches) == 1
    assert "queued_at" not in create_matches[0].read_text(encoding="utf-8").lower()


def test_the_iff_queued_invariant_is_enforced_in_both_directions() -> None:
    text = _migration_text()
    assert "check ((status = 'queued') = (queued_at is not null))" in text


def test_the_backfill_stamps_existing_queued_rows_with_the_db_clock() -> None:
    text = _migration_text()
    assert "update openorc.tasks set queued_at = now()" in text
    assert "where status = 'queued' and queued_at is null" in text


def test_the_partial_index_serves_the_fifo_scan() -> None:
    text = _migration_text()
    assert "create index tasks_workspace_queue_fifo_idx" in text
    assert "on openorc.tasks (workspace_id, queued_at, id)" in text
    assert "where status = 'queued'" in text


def test_no_second_queue_surface_is_invented() -> None:
    text = _migration_text()
    for forbidden in ("task_queue", "create table", "grant ", "policy"):
        assert forbidden not in text, f"the queued_at migration must not add {forbidden!r}"
