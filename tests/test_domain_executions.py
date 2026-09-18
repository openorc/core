"""Domain tests for Execution (issue #24).

Ordinary deterministic tests: no database, no network. These prove the
attempt-history invariants later persistence and workflow-service behavior
inherit: the settled eight-value lifecycle vocabulary, the final-status
absorbing set, per-Task attempt ordering as identity, and the exact field
set (no Git SHA field, no branch ownership, no current-Execution pointer
concept).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.executions import (
    FINAL_EXECUTION_STATUSES,
    Execution,
    ExecutionDomainError,
    ExecutionStatus,
    execution_field_names,
)

_NOW = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)


def _execution(**overrides: Any) -> Execution:
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "producer_session_id": uuid4(),
        "execution_number": 1,
        "status": ExecutionStatus.QUEUED,
        "created_at": _NOW,
        "updated_at": _NOW,
    }
    values.update(overrides)
    return Execution(**values)


def test_status_vocabulary_is_exactly_the_settled_lifecycle() -> None:
    assert {member.value for member in ExecutionStatus} == {
        "queued",
        "running",
        "paused",
        "paused_for_approval",
        "succeeded",
        "failed_transient",
        "failed_final",
        "cancelled",
    }


def test_final_statuses_are_exactly_the_four_absorbing_outcomes() -> None:
    assert {status.value for status in FINAL_EXECUTION_STATUSES} == {
        "succeeded",
        "failed_transient",
        "failed_final",
        "cancelled",
    }
    for status in ExecutionStatus:
        assert _execution(status=status).is_final == (status in FINAL_EXECUTION_STATUSES)


def test_execution_rejects_non_vocabulary_status() -> None:
    with pytest.raises(ExecutionDomainError):
        _execution(status="dispatching")  # type: ignore[arg-type]


def test_attempt_ordering_requires_a_positive_integer() -> None:
    for bad in (0, -1, 1.5, "1", True):
        with pytest.raises(ExecutionDomainError):
            _execution(execution_number=bad)


def test_execution_requires_uuid_identity_fields() -> None:
    for name in ("id", "workspace_id", "task_id", "producer_session_id"):
        with pytest.raises(ExecutionDomainError):
            _execution(**{name: "not-a-uuid"})


def test_finalized_attempts_are_history_not_rewritable_records() -> None:
    # A finalized attempt and a later retry coexist as separate rows in the
    # same Producer session: the retry never mutates the finalized one.
    first = _execution(execution_number=1, status=ExecutionStatus.FAILED_TRANSIENT)
    retry = _execution(
        task_id=first.task_id,
        workspace_id=first.workspace_id,
        producer_session_id=first.producer_session_id,
        execution_number=2,
        status=ExecutionStatus.QUEUED,
    )
    assert first.producer_session_id == retry.producer_session_id
    assert first.is_final and not retry.is_final
    assert first.status == ExecutionStatus.FAILED_TRANSIENT


def test_execution_is_frozen() -> None:
    execution = _execution()
    with pytest.raises(AttributeError):
        execution.status = ExecutionStatus.RUNNING  # type: ignore[misc]


def test_field_set_carries_no_git_sha_or_branch_ownership() -> None:
    # The exact field set is asserted so future drift cannot appear silently:
    # runtime sandboxes are runtime-private execution state, GitHub owns
    # committed repository truth, and the canonical branch is a Task-level
    # fact — no Git SHA field and no branch-ownership field exists here.
    assert execution_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "producer_session_id",
            "execution_number",
            "status",
            "created_at",
            "updated_at",
        }
    )
