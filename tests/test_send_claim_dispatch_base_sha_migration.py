"""Convention tests for the send-claim dispatch-base migration (issue #335).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: exactly one forward-only migration
adds the nullable ``dispatch_repository_base_sha`` column to the existing
F2 claims table, the PR_COMPOSE kind never carries a dispatch base, no
backfill and no second claim/result surface is invented, and no historical
migration is rewritten. Behavioral invariants are proven against a real
database by the integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(MIGRATIONS_DIR.glob("*_add_send_claim_dispatch_base_sha.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_send_claim_dispatch_base_sha migration, found {matches}"
    )
    text = matches[0].read_text(encoding="utf-8").lower()
    return " ".join(text.split())


def test_exactly_one_forward_only_migration_adds_the_column() -> None:
    text = _migration_text()
    assert "add column dispatch_repository_base_sha text" in text
    # The F2 claims migration predates the dispatch context and must stay
    # unchanged: the column is added forward-only, never backported.
    claim_matches = sorted(MIGRATIONS_DIR.glob("*_add_task_runtime_send_claims.sql"))
    assert len(claim_matches) == 1
    assert (
        "dispatch_repository_base_sha" not in claim_matches[0].read_text(encoding="utf-8").lower()
    )


def test_the_pr_compose_kind_never_carries_a_dispatch_base() -> None:
    text = _migration_text()
    # The PR_COMPOSE dispatch's subject head is the approved gate's
    # subject_head_sha; a dispatch repository-base context belongs to the
    # plan dispatches only.
    assert "kind = 'pr_compose_dispatch'" in text
    assert "dispatch_repository_base_sha is null" in text


def test_no_backfill_no_grants_and_no_second_surface_is_invented() -> None:
    text = _migration_text()
    for forbidden in (
        "update openorc",
        "create table",
        "insert into",
        "grant ",
        "policy",
    ):
        assert forbidden not in text, f"the dispatch-base migration must not add {forbidden!r}"
