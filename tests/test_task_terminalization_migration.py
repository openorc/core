"""Convention tests for the Task terminalization commitment migration (issue #317).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: exactly one forward-only migration adds
the nullable set-once commitment and one-shot cleanup-evidence columns, the
value vocabularies and the commitment/evidence coherence CHECKs are enforced,
and no historical migration is rewritten, no backfill or grant exists, and no
new table or queue surface is invented. Behavioral invariants are proven
against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _terminalization_migrations() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*_add_task_terminalization_commitment.sql"))


def _migration_text() -> str:
    matches = _terminalization_migrations()
    assert len(matches) == 1, (
        f"expected exactly one add_task_terminalization_commitment migration, found {matches}"
    )
    text = matches[0].read_text(encoding="utf-8").lower()
    return " ".join(text.split())


def test_exactly_one_forward_only_migration_adds_the_columns() -> None:
    text = _migration_text()
    assert "alter table openorc.tasks" in text
    for column in (
        "add column terminalization_outcome text",
        "add column terminalization_origin text",
        "add column terminalization_established_at timestamptz",
        "add column cleanup_send_claimed_at timestamptz",
        "add column cleanup_send_settled_at timestamptz",
    ):
        assert column in text
    # The create_tasks migration predates the commitment facts and must stay
    # unchanged: they are added forward-only, never backported.
    create_matches = sorted(MIGRATIONS_DIR.glob("*_create_tasks.sql"))
    assert len(create_matches) == 1
    create_text = create_matches[0].read_text(encoding="utf-8").lower()
    assert "terminalization_outcome" not in create_text
    assert "cleanup_send_claimed_at" not in create_text


def test_the_value_vocabularies_are_enforced() -> None:
    text = _migration_text()
    assert "terminalization_outcome in ('cancelled', 'completed')" in text
    assert (
        "terminalization_origin in "
        "('owner_cancelled', 'github_closed_unmerged', 'github_confirmed_merged')"
    ) in text


def test_the_commitment_coherence_is_enforced() -> None:
    text = _migration_text()
    assert "tasks_terminalization_coherence_check" in text
    assert "(terminalization_outcome is null) = (terminalization_established_at is null)" in text
    assert "(terminalization_outcome is null) = (terminalization_origin is null)" in text


def test_the_cleanup_evidence_coherence_is_enforced() -> None:
    text = _migration_text()
    assert "tasks_cleanup_send_evidence_check" in text
    assert "(cleanup_send_settled_at is null or cleanup_send_claimed_at is not null)" in text
    assert "(cleanup_send_claimed_at is null or terminalization_outcome is not null)" in text


def test_no_backfill_grant_or_new_surface_is_invented() -> None:
    text = _migration_text()
    for forbidden in ("update openorc.tasks", "create table", "grant ", "policy", "trigger"):
        assert forbidden not in text, f"the terminalization migration must not add {forbidden!r}"
