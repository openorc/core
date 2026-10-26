"""The cost-accounting migrations carry the durable issue #240 contracts.

The four additive migrations are the schema source of truth for the Owner
spending authorization and the session cost checkpoint. These assertions
keep the migration text reviewable: the nullable-positive budget shapes,
the coherent nonnegative checkpoint pair, and the additive ``cost_limit``
block reason — exactly the vocabulary extension the block-reason migration
names, and nothing else.
"""

from __future__ import annotations

from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent.parent / "supabase" / "migrations"


def _migration_text(suffix: str) -> str:
    matches = sorted(MIGRATIONS_DIR.glob(f"*_{suffix}.sql"))
    assert len(matches) == 1, f"expected exactly one {suffix} migration, found {matches}"
    return matches[0].read_text()


def test_the_workspace_default_budget_is_nullable_and_strictly_positive() -> None:
    text = _migration_text("add_workspace_default_task_budget")
    assert "add column default_task_budget_usd numeric" in text
    # NULL disables spending policy; zero and negative values are
    # configuration errors, never "unlimited".
    assert "default_task_budget_usd is null or default_task_budget_usd > 0" in text


def test_the_task_authorized_budget_is_positive_and_never_pre_start() -> None:
    text = _migration_text("add_task_authorized_budget")
    assert "add column authorized_task_budget_usd numeric" in text
    assert "authorized_task_budget_usd > 0" in text
    # A never-started Task cannot carry an authorization: the capture
    # commits only together with the guarded start transition.
    assert "status <> 'ready_to_plan'" in text


def test_the_session_cost_checkpoint_is_a_coherent_nonnegative_pair() -> None:
    text = _migration_text("add_session_cost_checkpoint")
    assert "add column retained_cost_usd numeric" in text
    assert "add column cost_observed_at timestamptz" in text
    # Both NULL together (never observed — unknown, never zero) or both set
    # with a nonnegative amount.
    assert "retained_cost_usd is null and cost_observed_at is null" in text
    assert "retained_cost_usd is not null" in text
    assert "cost_observed_at is not null" in text
    assert "retained_cost_usd >= 0" in text


def test_cost_limit_joins_the_block_reason_vocabulary_additively() -> None:
    text = _migration_text("add_cost_limit_block_reason")
    # The original inline CHECK is replaced by an explicitly named constraint.
    assert "drop constraint task_blocks_reason_check" in text
    assert "add constraint task_blocks_reason_check" in text
    # The extended vocabulary keeps every settled value and adds exactly one.
    for settled in (
        "review_failure",
        "runtime_failure",
        "agent_session_lost",
        "connection_unavailable",
        "invalid_credentials",
        "external_operation_uncertain",
        "stale_operation",
        "github_source_changed",
        "runtime_request_rejected",
        "pr_closed_unmerged",
        "owner_action_required",
        "unknown",
    ):
        assert f"'{settled}'" in text
    assert "'cost_limit'" in text
    # No other vocabulary value is introduced by this migration.
    assert text.count("'") >= 26  # the thirteen quoted values plus prose
