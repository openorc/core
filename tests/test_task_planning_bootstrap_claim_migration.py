"""Convention tests for the planning-bootstrap claim migration (issue #203).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: exactly one forward-only migration
adds the nullable ``planning_bootstrap_claim_token`` column, no backfill
every claims an un-bootstrapped Task, no second queue/claim surface is
invented, and no historical migration is rewritten. Behavioral invariants are
proven against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(MIGRATIONS_DIR.glob("*_add_task_planning_bootstrap_claim.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_task_planning_bootstrap_claim migration, found {matches}"
    )
    text = matches[0].read_text(encoding="utf-8").lower()
    return " ".join(text.split())


def test_exactly_one_forward_only_migration_adds_the_column() -> None:
    text = _migration_text()
    assert "alter table openorc.tasks add column planning_bootstrap_claim_token uuid" in text
    # The create_tasks migration predates the claim fact and must stay
    # unchanged: the claim is added forward-only, never backported.
    create_matches = sorted(MIGRATIONS_DIR.glob("*_create_tasks.sql"))
    assert len(create_matches) == 1
    assert (
        "planning_bootstrap_claim_token"
        not in create_matches[0].read_text(encoding="utf-8").lower()
    )


def test_no_backfill_no_grants_and_no_second_surface_is_invented() -> None:
    text = _migration_text()
    for forbidden in (
        "update openorc.tasks",
        "create table",
        "create index",
        "grant ",
        "policy",
    ):
        assert forbidden not in text, f"the claim migration must not add {forbidden!r}"
