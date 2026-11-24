"""Convention tests for the #376 presentation/mergeability migration.

The ordinary suite cannot execute Postgres. These tests assert the durable,
architectural properties of the committed migration that runtime behavior
does not naturally establish: every presentation/mergeability column is a
nullable addition (no fabricated provider facts, no defaults), the bounded
user subset and the head-bound mergeability observation pair carry their
together-or-absent coherence checks, and no workflow column or existing
constraint is touched. Constraint behavior is proven against a real database
by the integration-marked suites.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(
        p.name
        for p in MIGRATIONS_DIR.glob("*_add_task_pull_request_presentation_and_mergeability.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one add_task_pull_request_presentation_and_mergeability "
        f"migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


_PRESENTATION_COLUMNS = (
    "title",
    "body",
    "html_url",
    "user_id",
    "user_login",
    "draft",
    "provider_created_at",
    "provider_updated_at",
    "provider_closed_at",
    "base_sha",
    "mergeable",
    "merge_state_status",
    "pr_mergeability_observed_at",
    "pr_mergeability_head_sha",
)


def test_every_presentation_column_is_a_nullable_addition_without_a_default() -> None:
    text = _migration_text()
    alter_block = re.search(r"alter table openorc\.task_pull_requests(.*?);", text, re.DOTALL)
    assert alter_block is not None, "expected the presentation-column alter block"
    block = alter_block.group(1)
    for column in _PRESENTATION_COLUMNS:
        assert re.search(rf"add column {column} ", block), f"missing column {column}"
    # No default fabricates a provider fact, and no NOT NULL forces a value
    # an existing row has not yet observed.
    assert "not null" not in block
    assert "default" not in block


def test_the_user_pair_and_mergeability_pair_carry_their_coherence_checks() -> None:
    text = _migration_text()
    assert "task_pull_requests_user_pair_coherence" in text
    # A partially observed user subset is never a readable fact.
    assert re.search(
        r"task_pull_requests_user_pair_coherence\s*\n?\s*check \(\(user_id is null\) = "
        r"\(user_login is null\)\)",
        text,
    )
    # The head-bound mergeability observation evidence moves together: an
    # observation instant without its observed head is not a readable
    # last-observed fact.
    assert "task_pull_requests_mergeability_observation_coherence" in text
    assert re.search(
        r"task_pull_requests_mergeability_observation_coherence\s*\n?\s*check "
        r"\(\(pr_mergeability_observed_at is null\) = "
        r"\(pr_mergeability_head_sha is null\)\)",
        text,
    )


def test_no_grants_and_no_workflow_columns_are_touched() -> None:
    text = _migration_text()
    # The openorc schema lockout carries isolation: runtime access is
    # backend-only and no privilege statements are granted here.
    assert not re.search(r"^\s*grant\b", text, re.MULTILINE)
    # The migration touches only the presentation/mergeability surface: no
    # workflow column (state token, currentness pointers, lifecycle stamps)
    # is added or altered.
    for forbidden in ("state_token", "current_owner_gate_id", "archived_at"):
        assert forbidden not in text
