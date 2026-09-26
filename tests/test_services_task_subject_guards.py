"""Deterministic tests for the Task-subject guards (issue #54).

The ordinary suite cannot execute Postgres: these tests use canned rows and
a scripted fake connection seam to prove the three guard layers — scope-only
subject membership (missing, cross-Task, and cross-Workspace subjects are
uniformly ``NotFoundError``), currentness binding (``StaleOperationError``
for stale tokens, archived Tasks, superseded revisions, superseded or
resolved gates), and exact PR-head binding (a changed head is stale;
base-branch movement alone is irrelevant). Real conditional-SQL and
constraint behavior is proven by the integration-marked suite and the
Phase 1 persistence suites.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.persistence.pool import DatabasePool
from openorc.services import task_subject_guards
from openorc.services.errors import InvalidCommandError, NotFoundError, StaleOperationError

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)

_WORKSPACE_ID = uuid.uuid4()
_OTHER_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_OTHER_TASK_ID = uuid.uuid4()
_TOKEN = uuid.uuid4()
_OTHER_TOKEN = uuid.uuid4()
_REVISION_ID = uuid.uuid4()
_OTHER_REVISION_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_OTHER_GATE_ID = uuid.uuid4()


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("guard tests never close pools")


def _pool(results: list[tuple[Any, ...] | None]) -> DatabasePool:
    return cast(DatabasePool, FakePool(ScriptedConnection(results)))


def _task_row(
    *,
    task_id: uuid.UUID = _TASK_ID,
    workspace_id: uuid.UUID = _WORKSPACE_ID,
    status: str = "ready_to_plan",
    archived_at: Any = None,
    canonical_feature_branch: str | None = None,
    state_token: uuid.UUID = _TOKEN,
    current_plan_revision_id: uuid.UUID | None = None,
    current_owner_gate_id: uuid.UUID | None = None,
) -> tuple[Any, ...]:
    return (
        task_id,
        workspace_id,
        _REPOSITORY_ID,
        9001,
        42,
        status,
        archived_at,
        canonical_feature_branch,
        state_token,
        current_plan_revision_id,
        current_owner_gate_id,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _revision_row(
    *,
    revision_id: uuid.UUID = _REVISION_ID,
    workspace_id: uuid.UUID = _WORKSPACE_ID,
    task_id: uuid.UUID = _TASK_ID,
) -> tuple[Any, ...]:
    return (revision_id, workspace_id, task_id, 1, "plan text", "b" * 40, _OBSERVED)


def _gate_row(
    *,
    gate_id: uuid.UUID = _GATE_ID,
    workspace_id: uuid.UUID = _WORKSPACE_ID,
    task_id: uuid.UUID = _TASK_ID,
    status: str = "pending",
    decided_at: Any = None,
) -> tuple[Any, ...]:
    return (
        gate_id,
        workspace_id,
        task_id,
        "pr_authorization",
        status,
        None,
        "a" * 40,
        None,
        decided_at,
        _OBSERVED,
    )


def _pull_request_row(
    *,
    workspace_id: uuid.UUID = _WORKSPACE_ID,
    task_id: uuid.UUID = _TASK_ID,
    head_sha: str = "c" * 40,
    base_ref: str = "main",
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        task_id,
        _REPOSITORY_ID,
        555001,
        7,
        "feat/producer-branch",
        base_ref,
        head_sha,
        "open",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def test_require_current_task_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    assert task.id == _TASK_ID
    assert task.workspace_id == _WORKSPACE_ID
    assert task.state_token == _TOKEN


def test_require_current_task_missing_is_not_found() -> None:
    with pytest.raises(NotFoundError):
        task_subject_guards.require_current_task(
            _pool([None]),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
        )


def test_require_current_task_cross_workspace_is_not_found() -> None:
    with pytest.raises(NotFoundError):
        task_subject_guards.require_current_task(
            _pool([_task_row(workspace_id=_OTHER_WORKSPACE_ID)]),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
        )


def test_require_current_task_archived_is_stale() -> None:
    with pytest.raises(StaleOperationError):
        task_subject_guards.require_current_task(
            _pool([_task_row(status="completed", archived_at=_OBSERVED)]),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
        )


def test_require_current_task_stale_token_is_stale() -> None:
    with pytest.raises(StaleOperationError):
        task_subject_guards.require_current_task(
            _pool([_task_row(state_token=_OTHER_TOKEN)]),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
        )


def test_require_current_task_malformed_token_is_invalid_command() -> None:
    with pytest.raises(InvalidCommandError):
        task_subject_guards.require_current_task(
            _pool([]),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token="not-a-uuid",  # type: ignore[arg-type]
        )


def test_require_plan_revision_in_task_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    revision = task_subject_guards.require_plan_revision_in_task(
        _pool([_revision_row()]), task=task, plan_revision_id=_REVISION_ID
    )
    assert revision.id == _REVISION_ID
    assert revision.task_id == _TASK_ID


def test_require_plan_revision_in_task_missing_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_plan_revision_in_task(
            _pool([None]), task=task, plan_revision_id=_REVISION_ID
        )


def test_require_plan_revision_in_task_cross_task_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_plan_revision_in_task(
            _pool([_revision_row(task_id=_OTHER_TASK_ID)]),
            task=task,
            plan_revision_id=_REVISION_ID,
        )


def test_require_plan_revision_in_task_cross_workspace_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_plan_revision_in_task(
            _pool([_revision_row(workspace_id=_OTHER_WORKSPACE_ID)]),
            task=task,
            plan_revision_id=_REVISION_ID,
        )


def test_require_pending_owner_gate_in_task_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    gate = task_subject_guards.require_pending_owner_gate_in_task(
        _pool([_gate_row()]), task=task, owner_gate_id=_GATE_ID
    )
    assert gate.id == _GATE_ID
    assert gate.task_id == _TASK_ID


def test_require_pending_owner_gate_in_task_missing_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_pending_owner_gate_in_task(
            _pool([None]), task=task, owner_gate_id=_GATE_ID
        )


def test_require_pending_owner_gate_in_task_cross_task_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_pending_owner_gate_in_task(
            _pool([_gate_row(task_id=_OTHER_TASK_ID)]), task=task, owner_gate_id=_GATE_ID
        )


def test_require_pending_owner_gate_in_task_cross_workspace_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_pending_owner_gate_in_task(
            _pool([_gate_row(workspace_id=_OTHER_WORKSPACE_ID)]), task=task, owner_gate_id=_GATE_ID
        )


def test_require_pending_owner_gate_in_task_resolved_is_stale() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(StaleOperationError):
        task_subject_guards.require_pending_owner_gate_in_task(
            _pool([_gate_row(status="approved", decided_at=_OBSERVED)]),
            task=task,
            owner_gate_id=_GATE_ID,
        )


def test_require_current_plan_revision_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row(current_plan_revision_id=_REVISION_ID)]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    revision = task_subject_guards.require_current_plan_revision(
        _pool([_revision_row()]), task=task, plan_revision_id=_REVISION_ID
    )
    assert revision.id == _REVISION_ID


def test_require_current_plan_revision_superseded_is_stale() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row(current_plan_revision_id=_OTHER_REVISION_ID)]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(StaleOperationError):
        task_subject_guards.require_current_plan_revision(
            _pool([_revision_row()]), task=task, plan_revision_id=_REVISION_ID
        )


def test_require_current_owner_gate_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row(current_owner_gate_id=_GATE_ID)]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    gate = task_subject_guards.require_current_owner_gate(
        _pool([_gate_row()]), task=task, owner_gate_id=_GATE_ID
    )
    assert gate.id == _GATE_ID
    assert gate.is_pending


def test_require_current_owner_gate_not_current_is_stale() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row(current_owner_gate_id=_OTHER_GATE_ID)]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(StaleOperationError):
        task_subject_guards.require_current_owner_gate(
            _pool([_gate_row()]), task=task, owner_gate_id=_GATE_ID
        )


def test_require_canonical_task_pull_request_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    pull_request = task_subject_guards.require_canonical_task_pull_request(
        _pool([_pull_request_row()]), task=task
    )
    assert pull_request.task_id == _TASK_ID
    assert pull_request.workspace_id == _WORKSPACE_ID


def test_require_canonical_task_pull_request_missing_is_not_found() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(NotFoundError):
        task_subject_guards.require_canonical_task_pull_request(_pool([None]), task=task)


def test_require_task_pull_request_head_success() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    pull_request = task_subject_guards.require_task_pull_request_head(
        _pool([_pull_request_row(head_sha="c" * 40)]), task=task, expected_head_sha="c" * 40
    )
    assert pull_request.head_sha == "c" * 40


def test_require_task_pull_request_head_changed_head_is_stale() -> None:
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    with pytest.raises(StaleOperationError):
        task_subject_guards.require_task_pull_request_head(
            _pool([_pull_request_row(head_sha="d" * 40)]), task=task, expected_head_sha="c" * 40
        )


def test_require_task_pull_request_head_base_movement_alone_remains_current() -> None:
    """Base-branch movement is never consulted: an unchanged head stays current."""
    task = task_subject_guards.require_current_task(
        _pool([_task_row()]),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
    )
    pull_request = task_subject_guards.require_task_pull_request_head(
        _pool([_pull_request_row(head_sha="c" * 40, base_ref="develop")]),
        task=task,
        expected_head_sha="c" * 40,
    )
    assert pull_request.base_ref == "develop"
