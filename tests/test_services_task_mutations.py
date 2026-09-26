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

Since #56, the terminal archival and exact current-gate resolution also
coordinate their locked event types through
``openorc.services.event_coordination`` inside the same composition: these
tests prove a successful mutation executes its event insert with the exact
safe actor/subject/context mapping, while every stale/failed path and the
deliberately non-evented mutations (coarse-status movement, canonical-branch
binding, plan/gate installation) execute no event insert at all.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry import trace as trace_api
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from psycopg.errors import UniqueViolation

from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.tasks import TaskStatus
from openorc.observability import (
    OPERATION,
    TASK_ID,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination, task_mutations
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
_OWNER_PROFILE_ID = uuid.uuid4()
_OWNER_ACTOR = event_coordination.owner_actor(_OWNER_PROFILE_ID)


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
        "a" * 64,
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


def _event_row(
    *,
    event_type: str = "task_cancelled",
    task_id: uuid.UUID | None = _TASK_ID,
    actor_id: str | None = str(_OWNER_PROFILE_ID),
    subject: tuple[str, uuid.UUID] | None = None,
) -> tuple[Any, ...]:
    """A canned `record_workflow_event` INSERT ... RETURNING row."""
    subject_type, subject_id = subject if subject is not None else (None, None)
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        task_id,
        event_type,
        "owner",
        actor_id,
        subject_type,
        subject_id,
        {},
        _OBSERVED,
    )


def test_update_task_status_success_rotates_token() -> None:
    """A deliberately non-evented mutation executes no event insert."""
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
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


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


def test_archive_task_cancel_success_rotates_token_and_records_event() -> None:
    pool, conn = _pool(
        [
            _task_row(),
            _task_row(status="cancelled", archived_at=_OBSERVED, state_token=_NEW_TOKEN),
            _event_row(event_type="task_cancelled"),
        ]
    )
    task = task_mutations.archive_task(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        terminal_status=TaskStatus.CANCELLED,
        actor=_OWNER_ACTOR,
    )
    assert task.status is TaskStatus.CANCELLED
    assert task.archived_at is not None
    assert task.state_token == _NEW_TOKEN != _TOKEN
    # Currentness guard, conditional archival write, then the coordinated
    # event insert — all one composed transaction.
    assert len(conn.executed) == 3
    event_sql, event_params = conn.executed[2]
    assert "insert into openorc.workflow_events" in event_sql
    assert event_params is not None
    assert event_params[:7] == (
        _WORKSPACE_ID,
        _TASK_ID,
        "task_cancelled",
        "owner",
        str(_OWNER_PROFILE_ID),
        None,
        None,
    )
    # The terminal archival event carries no context payload.
    assert event_params[7].obj == {}


def test_archive_task_complete_success_records_task_completed_event() -> None:
    pool, conn = _pool(
        [
            _task_row(),
            _task_row(status="completed", archived_at=_OBSERVED, state_token=_NEW_TOKEN),
            _event_row(event_type="task_completed"),
        ]
    )
    task = task_mutations.archive_task(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        terminal_status=TaskStatus.COMPLETED,
        actor=_OWNER_ACTOR,
    )
    assert task.status is TaskStatus.COMPLETED
    assert task.archived_at is not None
    event_params = conn.executed[2][1]
    assert event_params is not None
    assert event_params[2] == "task_completed"


def test_archive_task_stale_token_rejects_before_any_write_and_event() -> None:
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])
    with pytest.raises(StaleOperationError):
        task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.CANCELLED,
            actor=_OWNER_ACTOR,
        )
    assert len(conn.executed) == 1
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


def test_archive_task_event_insert_failure_rolls_back_the_mutation() -> None:
    """A failed event insertion propagates — the mutation never stands alone.

    The deterministic seam proves the event insert is attempted inside the
    same composition after the canonical write; the real all-or-nothing
    rollback is proven by the integration-marked suite.
    """
    pool, conn = _pool(
        [
            _task_row(),
            _task_row(status="cancelled", archived_at=_OBSERVED, state_token=_NEW_TOKEN),
            RuntimeError("event insertion failed"),
        ]
    )
    with pytest.raises(RuntimeError, match="event insertion failed"):
        task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.CANCELLED,
            actor=_OWNER_ACTOR,
        )
    assert len(conn.executed) == 3
    assert "update openorc.tasks" in conn.executed[1][0]
    assert "insert into openorc.workflow_events" in conn.executed[2][0]


def test_archive_task_requires_the_actor_context() -> None:
    """The audited mutation has no unaudited path: actor context is required."""
    kwargs: dict[str, Any] = {
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "expected_state_token": _TOKEN,
        "terminal_status": TaskStatus.CANCELLED,
    }
    pool, conn = _pool([])
    with pytest.raises(TypeError):
        task_mutations.archive_task(pool, **kwargs)
    assert conn.executed == []


def test_archive_task_rejects_unsafe_actor_context_before_any_io() -> None:
    """OWNER actor identity is the canonical Profile UUID, never a GitHub login."""
    pool, conn = _pool([])
    with pytest.raises(InvalidCommandError):
        task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.CANCELLED,
            actor=event_coordination.WorkflowActorContext(
                WorkflowEventActor.OWNER, "octocat-github-login"
            ),
        )
    assert conn.executed == []


def test_archive_task_nonterminal_status_is_invalid_command() -> None:
    pool, conn = _pool([])
    with pytest.raises(InvalidCommandError):
        task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.READY_TO_PLAN,
            actor=_OWNER_ACTOR,
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
            _event_row(event_type="owner_gate_resolved", subject=("owner_gate", _GATE_ID)),
        ]
    )
    resolution = task_mutations.resolve_owner_gate(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        owner_gate_id=_GATE_ID,
        outcome=OwnerGateStatus.APPROVED,
        actor=_OWNER_ACTOR,
    )
    assert resolution.gate.status is OwnerGateStatus.APPROVED
    assert resolution.task.current_owner_gate_id is None
    assert resolution.task.state_token == _NEW_TOKEN != _TOKEN
    # The resolved gate is the coordinated event's primary subject over the
    # Task scope; context is the single safe outcome value.
    assert len(conn.executed) == 7
    event_sql, event_params = conn.executed[6]
    assert "insert into openorc.workflow_events" in event_sql
    assert event_params is not None
    assert event_params[:5] == (
        _WORKSPACE_ID,
        _TASK_ID,
        "owner_gate_resolved",
        "owner",
        str(_OWNER_PROFILE_ID),
    )
    assert event_params[5:7] == ("owner_gate", _GATE_ID)
    assert event_params[7].obj == {"outcome": "approved"}


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
            actor=_OWNER_ACTOR,
        )
    # The gate was read and the Task was locked, but neither write applied
    # and no event was written.
    assert len(conn.executed) == 4
    assert all("update openorc" not in sql for sql, _ in conn.executed)
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


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
            actor=_OWNER_ACTOR,
        )
    assert len(conn.executed) == 4
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


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
            actor=_OWNER_ACTOR,
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
            actor=_OWNER_ACTOR,
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
            actor=_OWNER_ACTOR,
        )
    assert len(conn.executed) == 2
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


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


def _local_provider_with_exporter() -> tuple[SdkTracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = SdkTracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_update_task_status_opens_its_service_span_with_safe_subject_attributes() -> None:
    provider, exporter = _local_provider_with_exporter()
    pool, conn = _pool([_task_row(), _task_row(status="planning", state_token=_NEW_TOKEN)])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        task_mutations.update_task_status(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.PLANNING,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "task_mutations.update_task_status"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID, TASK_ID}
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)
    assert exported.status.status_code is StatusCode.UNSET
    # The workflow-authority state token never becomes telemetry.
    assert str(_TOKEN) not in str(attributes)
    # The mutation itself is unchanged under the surrounding span.
    assert len(conn.executed) == 2


def test_stale_rejection_preserves_the_typed_error_and_exports_only_the_classification() -> None:
    provider, exporter = _local_provider_with_exporter()
    pool, conn = _pool([_task_row(state_token=_NEW_TOKEN)])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(StaleOperationError),
    ):
        task_mutations.update_task_status(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.PLANNING,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.status.status_code is StatusCode.ERROR
    # Safe classification only — the exception type name, never its text.
    assert exported.status.description == "StaleOperationError"
    # The typed outcome is unchanged: no write was attempted.
    assert len(conn.executed) == 1


def test_invalid_commands_open_the_operation_span_with_the_safe_classification() -> None:
    provider, exporter = _local_provider_with_exporter()
    pool, conn = _pool([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        task_mutations.update_task_status(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            status=TaskStatus.CANCELLED,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "task_mutations.update_task_status"
    assert exported.status.description == "InvalidCommandError"
    assert conn.executed == []


def test_branch_collision_keeps_the_typed_conflict_with_telemetry() -> None:
    provider, exporter = _local_provider_with_exporter()
    pool, conn = _pool([_task_row(), UniqueViolation("tasks_canonical_branch_repo_unique")])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(ConflictError),
    ):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            canonical_feature_branch="feat/issue-109-otel-retrofit",
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "task_mutations.bind_canonical_branch"
    assert exported.status.description == "ConflictError"
    assert len(conn.executed) == 2


def test_evented_archival_records_the_workflow_event_with_telemetry_active() -> None:
    """State + event atomicity is untouched by the surrounding span."""
    provider, exporter = _local_provider_with_exporter()
    pool, conn = _pool(
        [
            _task_row(),
            _task_row(status="cancelled", archived_at=_OBSERVED, state_token=_NEW_TOKEN),
            _event_row(event_type="task_cancelled"),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        task = task_mutations.archive_task(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            terminal_status=TaskStatus.CANCELLED,
            actor=_OWNER_ACTOR,
        )

    # The coordinated event insert still runs inside the same composed
    # transaction, independent of the telemetry backend.
    assert task.state_token == _NEW_TOKEN
    assert len(conn.executed) == 3
    assert "insert into openorc.workflow_events" in conn.executed[2][0]
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "task_mutations.archive_task"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)


def test_unconfigured_telemetry_does_not_change_mutation_behavior() -> None:
    # No global provider is installed in the deterministic suite: the tracer
    # seam resolves the no-op proxy and the mutation behaves identically with
    # no OpenTelemetry runtime installed (issues #108/#109).
    pool, conn = _pool([_task_row(), _task_row(status="planning", state_token=_NEW_TOKEN)])

    updated = task_mutations.update_task_status(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        status=TaskStatus.PLANNING,
    )

    assert updated.state_token == _NEW_TOKEN
    assert len(conn.executed) == 2
    assert not isinstance(trace_api.get_tracer_provider(), SdkTracerProvider)


def test_malformed_identifiers_never_enter_exported_telemetry() -> None:
    """Malformed command values are classified without being exported."""
    provider, exporter = _local_provider_with_exporter()
    pool, conn = _pool([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        task_mutations.update_task_status(
            pool,
            workspace_id="not-a-workspace-uuid",  # type: ignore[arg-type]
            task_id="not-a-task-uuid",  # type: ignore[arg-type]
            expected_state_token="not-a-token-uuid",  # type: ignore[arg-type]
            status=TaskStatus.PLANNING,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "task_mutations.update_task_status"
    assert exported.status.status_code is StatusCode.ERROR
    assert exported.status.description == "InvalidCommandError"
    # The caller-supplied identifiers are validated before attachment: the
    # malformed values never become safe-vocabulary span attributes.
    attributes = exported.attributes or {}
    assert WORKSPACE_ID not in attributes
    assert TASK_ID not in attributes
    assert "not-a-workspace-uuid" not in str(attributes)
    assert "not-a-task-uuid" not in str(attributes)
    assert conn.executed == []
