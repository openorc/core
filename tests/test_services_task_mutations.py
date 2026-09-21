"""Deterministic tests for the authoritative Task mutation services (issue #54).

The ordinary suite cannot execute Postgres: these tests use canned rows and
a scripted fake connection seam to prove each mutation family's
orchestration — the currentness guard, the scope-only subject resolution
where installation is involved, the conditional token-rotating write, and
the classification of a rejected write into stable typed outcomes
(``StaleOperationError`` for stale tokens/archived Tasks/raced subjects,
``ConflictError`` for a bound branch or an installed gate). They also prove
the token-continuation contract: a successful mutation returns the
post-write Task, and a caller continuing with the returned token succeeds
while the pre-mutation token is stale. Real conditional-SQL classification
is proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.gates import OwnerGateStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services import task_mutations
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)

_WORKSPACE_ID = uuid.uuid4()
_OTHER_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_OTHER_TASK_ID = uuid.uuid4()
_TOKEN = uuid.uuid4()
_NEW_TOKEN = uuid.uuid4()
_THIRD_TOKEN = uuid.uuid4()
_REVISION_ID = uuid.uuid4()
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

    def __init__(self, results: list[tuple[Any, ...] | None | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

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
        raise AssertionError("mutation tests never close pools")


def _pool(
    results: list[tuple[Any, ...] | None | Exception],
) -> tuple[DatabasePool, ScriptedConnection]:
    conn = ScriptedConnection(results)
    return cast(DatabasePool, FakePool(conn)), conn


def _task_row(
    *,
    status: str = "ready_to_plan",
    archived_at: Any = None,
    canonical_feature_branch: str | None = None,
    state_token: uuid.UUID = _TOKEN,
    current_plan_revision_id: uuid.UUID | None = None,
    current_owner_gate_id: uuid.UUID | None = None,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        9001,
        42,
        status,
        archived_at,
        canonical_feature_branch,
        state_token,
        current_plan_revision_id,
        current_owner_gate_id,
        _OBSERVED,
        _OBSERVED,
    )


def _revision_row(*, revision_id: uuid.UUID = _REVISION_ID) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, 1, "plan text", "b" * 40, _OBSERVED)


def _gate_row(
    *,
    gate_id: uuid.UUID = _GATE_ID,
    task_id: uuid.UUID = _TASK_ID,
    status: str = "pending",
    decided_at: Any = None,
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        task_id,
        "pr_authorization",
        status,
        None,
        "a" * 40,
        None,
        decided_at,
        _OBSERVED,
    )


def test_update_task_status_success_rotates_token() -> None:
    pool, conn = _pool([_task_row(), _task_row(status="planning", state_token=_NEW_TOKEN)])
    task = task_mutations.update_task_status(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        status=TaskStatus.PLANNING,
    )
    assert task.status is TaskStatus.PLANNING
    assert task.state_token == _NEW_TOKEN != _TOKEN
    assert len(conn.executed) == 2
    assert "for update" not in conn.executed[0][0]
    assert "update openorc.tasks" in conn.executed[1][0]


def test_update_task_status_stale_token_rejects_before_any_write() -> None:
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.update_task_status(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.PLANNING,
        )
    # The currentness guard ran; no conditional write was attempted.
    assert len(conn.executed) == 1
    assert "update openorc.tasks" not in conn.executed[0][0]


def test_update_task_status_archived_task_is_rejected() -> None:
    pool, conn = _pool([_task_row(status="completed", archived_at=_OBSERVED)])
    with pytest.raises(StaleOperationError):
        task_mutations.update_task_status(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.PLANNING,
        )
    assert len(conn.executed) == 1


def test_update_task_status_terminal_status_is_invalid_command() -> None:
    pool, conn = _pool([])
    with pytest.raises(InvalidCommandError):
        task_mutations.update_task_status(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.CANCELLED,
        )
    assert conn.executed == []


def test_archive_task_success_rotates_token_and_archives() -> None:
    pool, conn = _pool(
        [_task_row(), _task_row(status="cancelled", archived_at=_OBSERVED, state_token=_NEW_TOKEN)]
    )
    task = task_mutations.archive_task(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert task.status is TaskStatus.CANCELLED
    assert task.archived_at is not None
    assert task.state_token == _NEW_TOKEN != _TOKEN
    assert len(conn.executed) == 2


def test_archive_task_stale_token_rejects_before_any_write() -> None:
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.CANCELLED,
        )
    assert len(conn.executed) == 1


def test_archive_task_nonterminal_status_is_invalid_command() -> None:
    pool, conn = _pool([])
    with pytest.raises(InvalidCommandError):
        task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.READY_TO_PLAN,
        )
    assert conn.executed == []


def test_bind_canonical_branch_success() -> None:
    pool, conn = _pool(
        [
            _task_row(),
            _task_row(canonical_feature_branch="feat/producer-branch", state_token=_NEW_TOKEN),
        ]
    )
    task = task_mutations.bind_canonical_branch(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        canonical_feature_branch="feat/producer-branch",
    )
    assert task.canonical_feature_branch == "feat/producer-branch"
    assert task.state_token == _NEW_TOKEN != _TOKEN
    assert len(conn.executed) == 2


def test_bind_canonical_branch_rebinding_is_a_conflict() -> None:
    """A rebinding attempt against the current token conflicts with durable state."""
    bound = _task_row(canonical_feature_branch="feat/producer-branch")
    pool, conn = _pool([bound, None, bound])
    with pytest.raises(ConflictError):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            canonical_feature_branch="feat/other-branch",
        )
    assert len(conn.executed) == 3
    assert "for update" in conn.executed[2][0]


def test_bind_canonical_branch_cross_task_collision_is_a_conflict() -> None:
    """The repository-wide branch-ownership unique index rejects a bind of a
    branch another current Task of the Repository already owns; the driver
    exception is translated into the stable conflict without exposing the
    other Task."""
    pool, conn = _pool([_task_row(), UniqueViolation()])
    with pytest.raises(ConflictError) as excinfo:
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            canonical_feature_branch="feat/producer-branch",
        )
    assert "another current task" in str(excinfo.value)
    assert str(_TASK_ID) not in str(excinfo.value)
    assert len(conn.executed) == 2
    assert "update openorc.tasks" in conn.executed[1][0]


def test_bind_canonical_branch_stale_token_rejects_before_any_write() -> None:
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            canonical_feature_branch="feat/producer-branch",
        )
    assert len(conn.executed) == 1


def test_bind_canonical_branch_blank_branch_is_invalid_command() -> None:
    pool, conn = _pool([])
    with pytest.raises(InvalidCommandError):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            canonical_feature_branch="   ",
        )
    assert conn.executed == []


def test_bind_canonical_branch_unexplained_rejection_fails_closed() -> None:
    """A rejected write with no classifiable cause is a stale operation, never success."""
    unexplained = _task_row()
    pool, conn = _pool([unexplained, None, unexplained])
    with pytest.raises(StaleOperationError):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            canonical_feature_branch="feat/producer-branch",
        )
    assert len(conn.executed) == 3


def test_set_current_plan_revision_success_installs_pointer() -> None:
    pool, conn = _pool(
        [
            _task_row(),
            _revision_row(),
            _task_row(current_plan_revision_id=_REVISION_ID, state_token=_NEW_TOKEN),
        ]
    )
    task = task_mutations.set_current_plan_revision(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        plan_revision_id=_REVISION_ID,
    )
    assert task.current_plan_revision_id == _REVISION_ID
    assert task.state_token == _NEW_TOKEN != _TOKEN
    assert len(conn.executed) == 3
    assert "update openorc.tasks" in conn.executed[2][0]


def test_set_current_plan_revision_cross_task_revision_is_not_found() -> None:
    """A same-shaped revision of another Task is not an installable subject."""
    pool, conn = _pool(
        [_task_row(), (_REVISION_ID, _WORKSPACE_ID, _OTHER_TASK_ID, 1, "plan", "b" * 40, _OBSERVED)]
    )
    with pytest.raises(NotFoundError):
        task_mutations.set_current_plan_revision(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            plan_revision_id=_REVISION_ID,
        )
    assert len(conn.executed) == 2
    assert conn.executed[-1][0].startswith("select")


def test_set_current_plan_revision_stale_token_rejects_before_any_write() -> None:
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.set_current_plan_revision(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            plan_revision_id=_REVISION_ID,
        )
    assert len(conn.executed) == 1


def test_set_current_owner_gate_success_installs_pending_gate() -> None:
    pool, conn = _pool(
        [
            _task_row(),
            _gate_row(),
            _task_row(current_owner_gate_id=_GATE_ID, state_token=_NEW_TOKEN),
        ]
    )
    task = task_mutations.set_current_owner_gate(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        owner_gate_id=_GATE_ID,
    )
    assert task.current_owner_gate_id == _GATE_ID
    assert task.state_token == _NEW_TOKEN != _TOKEN
    assert len(conn.executed) == 3


def test_set_current_owner_gate_second_install_is_a_conflict() -> None:
    """Installing a gate while one is already current conflicts with durable state."""
    pool, conn = _pool(
        [
            _task_row(current_owner_gate_id=_GATE_ID),
            _gate_row(gate_id=_OTHER_GATE_ID),
            None,
            _task_row(current_owner_gate_id=_GATE_ID),
        ]
    )
    with pytest.raises(ConflictError):
        task_mutations.set_current_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_OTHER_GATE_ID,
        )
    assert len(conn.executed) == 4
    assert "for update" in conn.executed[3][0]


def test_set_current_owner_gate_raced_resolution_is_stale() -> None:
    """A gate that lost pending status between the read and the write is a stale operation."""
    pool, conn = _pool(
        [
            _task_row(),
            _gate_row(gate_id=_OTHER_GATE_ID),
            None,
            _task_row(),
            _gate_row(gate_id=_OTHER_GATE_ID, status="approved", decided_at=_OBSERVED),
        ]
    )
    with pytest.raises(StaleOperationError):
        task_mutations.set_current_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_OTHER_GATE_ID,
        )
    assert len(conn.executed) == 5


def test_set_current_owner_gate_cross_task_gate_is_not_found() -> None:
    pool, conn = _pool([_task_row(), _gate_row(task_id=_OTHER_TASK_ID)])
    with pytest.raises(NotFoundError):
        task_mutations.set_current_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
        )
    assert len(conn.executed) == 2
    assert conn.executed[-1][0].startswith("select")


def test_set_current_owner_gate_resolved_gate_is_stale() -> None:
    pool, conn = _pool([_task_row(), _gate_row(status="rejected", decided_at=_OBSERVED)])
    with pytest.raises(StaleOperationError):
        task_mutations.set_current_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
        )
    assert len(conn.executed) == 2


def test_set_current_owner_gate_stale_token_rejects_before_any_write() -> None:
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.set_current_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
        )
    assert len(conn.executed) == 1


def test_resolve_owner_gate_success_returns_resolved_gate_and_rotated_task() -> None:
    pool, conn = _pool(
        [
            _task_row(current_owner_gate_id=_GATE_ID),
            _gate_row(),
            # persistence resolve_owner_gate: gate select, task lock, then both writes.
            _gate_row(),
            _task_row(current_owner_gate_id=_GATE_ID),
            _gate_row(status="approved", decided_at=_OBSERVED),
            _task_row(current_owner_gate_id=None, state_token=_NEW_TOKEN),
        ]
    )
    resolution = task_mutations.resolve_owner_gate(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        owner_gate_id=_GATE_ID,
        outcome=OwnerGateStatus.APPROVED,
    )
    assert resolution.gate.status is OwnerGateStatus.APPROVED
    assert resolution.task.current_owner_gate_id is None
    assert resolution.task.state_token == _NEW_TOKEN != _TOKEN
    assert len(conn.executed) == 6


def test_resolve_owner_gate_stale_token_outcome_is_stale() -> None:
    """The persistence STALE outcome translates to a typed stale operation."""
    pool, conn = _pool(
        [
            _task_row(current_owner_gate_id=_GATE_ID),
            _gate_row(),
            _gate_row(),
            _task_row(current_owner_gate_id=_GATE_ID, state_token=_NEW_TOKEN),
        ]
    )
    with pytest.raises(StaleOperationError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
            outcome=OwnerGateStatus.APPROVED,
        )
    # The gate was read and the Task was locked, but neither write applied.
    assert len(conn.executed) == 4
    assert all("update openorc" not in sql for sql, _ in conn.executed)


def test_resolve_owner_gate_non_current_gate_outcome_is_stale() -> None:
    """A pending-but-never-installed gate cannot be resolved against the Task."""
    pool, conn = _pool(
        [
            _task_row(),
            _gate_row(),
            _gate_row(),
            _task_row(),
        ]
    )
    with pytest.raises(StaleOperationError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
            outcome=OwnerGateStatus.REJECTED,
        )
    assert len(conn.executed) == 4


def test_resolve_owner_gate_pending_outcome_is_invalid_command() -> None:
    pool, conn = _pool([])
    with pytest.raises(InvalidCommandError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
            outcome=OwnerGateStatus.PENDING,
        )
    assert conn.executed == []


def test_resolve_owner_gate_cross_task_gate_is_not_found() -> None:
    pool, conn = _pool([_task_row(), _gate_row(task_id=_OTHER_TASK_ID)])
    with pytest.raises(NotFoundError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
            outcome=OwnerGateStatus.APPROVED,
        )
    assert len(conn.executed) == 2


def test_resolve_owner_gate_already_resolved_gate_is_stale() -> None:
    pool, conn = _pool([_task_row(), _gate_row(status="approved", decided_at=_OBSERVED)])
    with pytest.raises(StaleOperationError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            owner_gate_id=_GATE_ID,
            outcome=OwnerGateStatus.APPROVED,
        )
    assert len(conn.executed) == 2


def test_caller_proceeds_with_returned_token_not_the_old_one() -> None:
    """The returned token is the only current one: the old token is stale immediately."""
    # First mutation succeeds and rotates the token.
    pool, _ = _pool([_task_row(), _task_row(status="planning", state_token=_NEW_TOKEN)])
    updated = task_mutations.update_task_status(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        status=TaskStatus.PLANNING,
    )
    assert updated.state_token == _NEW_TOKEN

    # Continuing with the pre-mutation token is rejected before any write.
    stale_pool, stale_conn = _pool([_task_row(status="planning", state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.update_task_status(
            stale_pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.IMPLEMENTING,
        )
    assert len(stale_conn.executed) == 1

    # Continuing with the returned token succeeds.
    next_pool, next_conn = _pool(
        [
            _task_row(status="planning", state_token=_NEW_TOKEN),
            _task_row(status="implementing", state_token=_THIRD_TOKEN),
        ]
    )
    continued = task_mutations.update_task_status(
        next_pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_NEW_TOKEN,
        status=TaskStatus.IMPLEMENTING,
    )
    assert continued.state_token == _THIRD_TOKEN
    assert len(next_conn.executed) == 2
