"""Domain tests for the Task aggregate (issue #21).

These tests prove the transport-independent Task semantics: the settled
nine-state status vocabulary with only two terminal outcomes, the archival
lifecycle invariant (archived iff terminal), stable GitHub issue identity
with observed address metadata, Task-level canonical-branch ownership, the
opaque ``state_token``, and that the aggregate carries no duplicated
PlanRevision/OwnerGate/ReviewLoop/TaskBlock content and no singular
current-Execution pointer.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from openorc.domain.tasks import (
    TERMINAL_STATUSES,
    Task,
    TaskDomainError,
    TaskStatus,
    task_field_names,
)


def _task(**overrides: object) -> Task:
    values: dict[str, object] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "repository_id": uuid4(),
        "github_issue_id": 1234,
        "github_issue_number": 42,
        "status": TaskStatus.READY_TO_PLAN,
        "archived_at": None,
        "canonical_feature_branch": None,
        "state_token": uuid4(),
        "current_plan_revision_id": None,
        "current_owner_gate_id": None,
        "source_requirements_fingerprint": "a" * 64,
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }
    values.update(overrides)
    return Task(**values)  # type: ignore[arg-type]


def test_status_vocabulary_is_exactly_the_nine_settled_states() -> None:
    assert {status.value for status in TaskStatus} == {
        "ready_to_plan",
        "queued",
        "planning",
        "waiting_for_owner",
        "implementing",
        "reviewing",
        "blocked",
        "cancelled",
        "completed",
    }


def test_only_cancelled_and_completed_are_terminal() -> None:
    assert {TaskStatus.CANCELLED, TaskStatus.COMPLETED} == TERMINAL_STATUSES
    assert TaskStatus.CANCELLED.is_terminal
    assert TaskStatus.COMPLETED.is_terminal
    for status in TaskStatus:
        if status not in TERMINAL_STATUSES:
            assert not status.is_terminal, f"{status} must not be terminal"


def test_task_accepts_settled_shape() -> None:
    task = _task(
        status=TaskStatus.IMPLEMENTING,
        canonical_feature_branch="openorc/task-42/producer",
        current_plan_revision_id=uuid4(),
        current_owner_gate_id=uuid4(),
    )
    assert isinstance(task.state_token, UUID)
    assert task.archived_at is None


def test_task_is_frozen() -> None:
    task = _task()
    with pytest.raises(Exception):  # noqa: B017 - frozen dataclass raises FrozenInstanceError
        task.status = TaskStatus.IMPLEMENTING  # type: ignore[misc]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"github_issue_id": 0}, "github_issue_id"),
        ({"github_issue_id": -5}, "github_issue_id"),
        ({"github_issue_id": True}, "github_issue_id"),
        ({"github_issue_number": 0}, "github_issue_number"),
        ({"status": "planning"}, "status"),
        ({"canonical_feature_branch": ""}, "canonical_feature_branch"),
        ({"canonical_feature_branch": "   "}, "canonical_feature_branch"),
        ({"state_token": "not-a-uuid"}, "state_token"),
        ({"current_plan_revision_id": 123}, "current_plan_revision_id"),
        ({"current_owner_gate_id": "abc"}, "current_owner_gate_id"),
        ({"source_requirements_fingerprint": "not-a-digest"}, "source_requirements_fingerprint"),
        ({"source_requirements_fingerprint": "A" * 64}, "source_requirements_fingerprint"),
    ],
)
def test_task_rejects_invalid_fields(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(TaskDomainError, match=message):
        _task(**overrides)


def test_terminal_status_without_archival_is_rejected() -> None:
    with pytest.raises(TaskDomainError, match="must be archived"):
        _task(status=TaskStatus.CANCELLED)


def test_archived_nonterminal_status_is_rejected() -> None:
    with pytest.raises(TaskDomainError, match="terminal outcome"):
        _task(archived_at=datetime.now(UTC), status=TaskStatus.PLANNING)


def test_archived_terminal_attempts_remain_distinguishable() -> None:
    cancelled = _task(status=TaskStatus.CANCELLED, archived_at=datetime.now(UTC))
    completed = _task(status=TaskStatus.COMPLETED, archived_at=datetime.now(UTC))
    assert cancelled.status != completed.status
    assert cancelled.archived_at is not None and completed.archived_at is not None


def test_task_field_set_is_exactly_the_aggregate_boundary() -> None:
    # One fact, one home: current-object pointers only — no plan content,
    # no gate type/subject, no review outcomes, no blocking context, and no
    # singular current-Execution pointer ever duplicated onto the Task.
    assert task_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "repository_id",
            "github_issue_id",
            "github_issue_number",
            "status",
            "archived_at",
            "canonical_feature_branch",
            "state_token",
            "current_plan_revision_id",
            "current_owner_gate_id",
            "source_requirements_fingerprint",
            "created_at",
            "updated_at",
        }
    )
    for forbidden in (
        "current_execution_id",
        "plan_content",
        "plan_review_outcome",
        "gate_type",
        "gate_subject",
        "review_outcome",
        "block_reason",
        "github_issue_title",
        "github_issue_state",
    ):
        assert forbidden not in task_field_names(), (
            f"Task must not carry duplicated subordinate fact: {forbidden}"
        )
