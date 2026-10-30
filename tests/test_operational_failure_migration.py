"""Convention tests for the operational-failure episodes migration (issue #251).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: one failure episode per original
durable attempt identity, the at-most-three ordinal budget with at most one
successor per failed attempt, the closed CHECK vocabularies, the terminal
TaskBlock linkage with its set-once partial uniqueness, and the deliberate
absences (no grants, no broad new privileges, additive tables only).
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

_FAMILIES = (
    "planning_bootstrap_prepare",
    "planning_bootstrap_transition",
    "initial_plan_dispatch",
    "planning_revise_dispatch",
    "reviewer_review",
    "implementation_execution",
    "pr_remediation_execution",
    "pr_compose_dispatch",
)
_IDENTITY_KINDS = (
    "runtime_send_claim",
    "planning_bootstrap_claim",
    "review_iteration",
    "execution",
    "owner_control_intent",
)
_ERROR_CODES = (
    "runtime_unavailable",
    "runtime_timeout",
    "runtime_delivery_uncertain",
    "runtime_transport",
    "runtime_protocol",
    "agent_session_lost",
    "runtime_configuration_rejected",
    "external_operation_failed",
    "external_operation_uncertain",
    "external_operation_rejected",
    "unknown",
)


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_operational_failure_episodes.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_operational_failure_episodes migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_exactly_two_additive_tables_are_created() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "create table openorc.operational_failure_episodes (" in raw
    assert "create table openorc.operational_failure_attempts (" in raw
    assert raw.count("create table") == 2


def test_the_episode_is_unique_per_original_attempt_identity() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "constraint operational_failure_episodes_original_attempt_uniq "
        "unique (original_attempt_kind, original_attempt_id)"
    ) in raw


def test_the_attempt_budget_is_three_ordinals_with_one_successor_per_attempt() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "constraint operational_failure_attempts_episode_ordinal_uniq unique (episode_id, ordinal)"
    ) in raw
    assert (
        "constraint operational_failure_attempts_source_attempt_uniq "
        "unique (source_kind, source_attempt_id)"
    ) in raw
    assert "ordinal smallint not null check (ordinal between 1 and 3)" in raw


def test_the_check_vocabularies_are_the_closed_sets() -> None:
    raw = _collapsed(_migration_text_raw())
    for family in _FAMILIES:
        assert f"'{family}'" in raw, family
    for kind in _IDENTITY_KINDS:
        assert f"'{kind}'" in raw, kind
    for code in _ERROR_CODES:
        assert f"'{code}'" in raw, code
    assert "source_kind in ('original', 'prior_authorized')" in raw


def test_the_escalation_linkage_is_deferred_restriction_and_set_once() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "constraint operational_failure_episodes_escalated_task_block_fk "
        "foreign key (escalated_task_block_id, task_id, workspace_id) "
        "references openorc.task_blocks (id, task_id, workspace_id) "
        "deferrable initially deferred"
    ) in raw
    assert (
        "create unique index operational_failure_episodes_escalated_task_block_uniq "
        "on openorc.operational_failure_episodes (escalated_task_block_id) "
        "where escalated_task_block_id is not null"
    ) in raw


def test_the_ownership_edges_cascade_through_the_episode() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "constraint operational_failure_episodes_task_id_workspace_id_fkey "
        "foreign key (task_id, workspace_id) references openorc.tasks "
        "(id, workspace_id) on delete cascade"
    ) in raw
    assert (
        "constraint operational_failure_attempts_episode_id_task_id_workspace_id_fkey "
        "foreign key (episode_id, task_id, workspace_id) references "
        "openorc.operational_failure_episodes (id, task_id, workspace_id) "
        "on delete cascade"
    ) in raw


def test_the_migration_grants_no_new_privileges() -> None:
    text = _migration_text_raw().lower()
    assert "grant " not in text
    assert "create policy" not in text
    assert "alter table openorc.tasks" not in text
