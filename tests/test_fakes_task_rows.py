"""Deterministic tests for the shared full Task repository-row builder (issue #312).

These tests prove the builder's own contract: canonical positional order of
the named fields, coherent pre-Start defaults, and strict rejection of
unknown override names. They deliberately do not re-prove the Task
repository mapping — column order, nullable handling, and UTC normalization
of ``_task_from_row`` stay independently verified by
``tests/test_persistence_tasks.py`` with hand-written rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from tests.fakes.task_rows import _TASK_ROW_FIELDS, task_row


def test_default_row_is_a_coherent_pre_start_task() -> None:
    row = task_row()

    assert len(row) == len(_TASK_ROW_FIELDS)
    # Identity fields are fresh UUIDs per call, like the replaced local helpers.
    assert isinstance(row[0], UUID)
    assert isinstance(row[1], UUID)
    assert isinstance(row[2], UUID)
    assert isinstance(row[8], UUID)
    assert row[3] == 9001
    assert row[4] == 42
    # Coherent pre-Start defaults (issue #233): ready_to_plan status with NO
    # frozen source_requirements_fingerprint.
    assert row[5] == "ready_to_plan"
    assert row[11] is None
    assert row[6] is None  # archived_at
    assert row[7] is None  # canonical_feature_branch
    assert row[9] is None  # current_plan_revision_id
    assert row[10] is None  # current_owner_gate_id
    assert row[12] is None  # queued_at
    assert row[15] is None  # terminal_cause
    assert row[16] is None  # authorized_task_budget_usd
    assert row[17] is None  # effective_cost_threshold_usd
    assert row[13] == datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert row[14] == datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_named_overrides_land_at_canonical_positions() -> None:
    task_id = UUID(int=1)
    workspace_id = UUID(int=2)
    repository_id = UUID(int=3)
    state_token = UUID(int=4)
    gate_id = UUID(int=5)
    revision_id = UUID(int=6)
    observed = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)

    row = task_row(
        id=task_id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=1234,
        github_issue_number=7,
        status="planning",
        archived_at=observed,
        canonical_feature_branch="openorc/task-7",
        state_token=state_token,
        current_plan_revision_id=revision_id,
        current_owner_gate_id=gate_id,
        source_requirements_fingerprint="f" * 64,
        queued_at=observed,
        created_at=observed,
        updated_at=observed,
        terminal_cause="pr_closed_unmerged",
        authorized_task_budget_usd=Decimal("5.00"),
        effective_cost_threshold_usd=Decimal("1.50"),
    )

    assert row[0] == task_id
    assert row[1] == workspace_id
    assert row[2] == repository_id
    assert row[3] == 1234
    assert row[4] == 7
    assert row[5] == "planning"
    assert row[6] == observed
    assert row[7] == "openorc/task-7"
    assert row[8] == state_token
    assert row[9] == revision_id
    assert row[10] == gate_id
    assert row[11] == "f" * 64
    assert row[12] == observed
    assert row[13] == observed
    assert row[14] == observed
    assert row[15] == "pr_closed_unmerged"
    assert row[16] == Decimal("5.00")
    assert row[17] == Decimal("1.50")


def test_started_state_override_pairs_status_with_fingerprint() -> None:
    # A started Task carries the frozen fingerprint Owner Start captured;
    # overrides keep the status/fingerprint combination explicit.
    row = task_row(
        status="waiting_for_owner",
        source_requirements_fingerprint="a" * 64,
        current_owner_gate_id=UUID(int=9),
    )

    assert row[5] == "waiting_for_owner"
    assert row[10] == UUID(int=9)
    assert row[11] == "a" * 64


def test_unknown_override_name_fails_clearly() -> None:
    with pytest.raises(TypeError) as excinfo:
        task_row(canonical_branch="openorc/task-7")  # stale convenience name

    message = str(excinfo.value)
    assert "canonical_branch" in message
    assert "canonical_feature_branch" in message
