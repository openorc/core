"""Deterministic mapping tests for WorkflowEvent repositories (issue #26).

The ordinary suite cannot execute Postgres; these tests use canned rows
and a fake pool/connection seam (mirroring the pull-request mapping
fakes) to prove row-to-domain-object mapping, UTC normalization at the
persistence boundary, parameterization, the INSERT-only write surface
(append-only is a surface contract — no UPDATE, DELETE, or upsert
statement exists anywhere in the module), the Jsonb context adapter, and
the index-matching read shapes. Database constraint behavior is proven
against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from types import MappingProxyType
from typing import Any, cast

import pytest
from psycopg.types.json import Jsonb

from openorc.domain.events import (
    WorkflowEventActor,
    WorkflowEventDomainError,
    WorkflowEventType,
)
from openorc.persistence import events as events_module
from openorc.persistence.events import (
    find_events_by_subject,
    get_workflow_event,
    list_recent_events_by_actor,
    list_recent_events_by_type,
    list_recent_workspace_events,
    list_task_events,
    record_workflow_event,
)
from openorc.persistence.pool import DatabasePool


class FakeCursor:
    """Returns one canned row (and optional canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    """Records executed SQL and returns canned rows, optionally per statement."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        responses: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.responses = list(responses) if responses is not None else None
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if self.responses is not None:
            response = self.responses.pop(0)
            return FakeCursor(response, None if response is None else [response])
        return FakeCursor(self.row, self.rows)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[FakeConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("mapping tests never close pools")


def _recorded_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 18, 12, 0, 0, tzinfo=timezone(timedelta(hours=3)))


def _utc_recorded_at() -> datetime:
    return _recorded_at().astimezone(UTC)


def _event_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": None,
        "event_type": "task_created",
        "actor_type": "openorc",
        "actor_id": None,
        "subject_type": None,
        "subject_id": None,
        "context": {"summary": "task 200 opened"},
        "created_at": _recorded_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["event_type"],
        values["actor_type"],
        values["actor_id"],
        values["subject_type"],
        values["subject_id"],
        values["context"],
        values["created_at"],
    )


def test_record_inserts_a_new_row_with_the_full_payload() -> None:
    row = _event_row(task_id=uuid.uuid4(), context={"summary": "task 200", "tags": ["a", "b"]})
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))
    context = {"summary": "task 200", "tags": ("a", "b")}

    event = record_workflow_event(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
        actor_id=None,
        subject_type="task",
        subject_id=row[2],
        context=context,
    )

    assert event.id == row[0]
    assert event.event_type is WorkflowEventType.TASK_CREATED
    assert event.actor_type is WorkflowEventActor.OPENORC
    assert event.context == MappingProxyType({"summary": "task 200", "tags": ["a", "b"]})
    assert event.created_at == _utc_recorded_at()

    sql, params = fake_conn.executed[0]
    # Append-only at the surface: the only write statement is an INSERT.
    assert "insert into openorc.workflow_events" in sql
    assert "on conflict" not in sql
    assert len(fake_conn.executed) == 1
    assert params is not None
    assert params[:3] == (row[1], row[2], "task_created")
    assert params[3] == "openorc"
    adapted = params[7]
    assert isinstance(adapted, Jsonb)
    assert adapted.obj == {"summary": "task 200", "tags": ["a", "b"]}


def test_record_omitted_fields_persist_as_null_and_empty_context() -> None:
    row = _event_row(context={})
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    event = record_workflow_event(
        pool,
        workspace_id=row[1],
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OWNER,
    )

    assert event.task_id is None
    assert event.actor_id is None
    assert event.subject_type is None
    assert event.subject_id is None
    assert event.context == MappingProxyType({})

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.workflow_events" in sql
    assert params is not None
    assert params[1] is None  # no Task scope: a Workspace-level event
    assert params[4] is None and params[5] is None and params[6] is None
    adapted = params[7]
    assert isinstance(adapted, Jsonb)
    assert adapted.obj == {}


def test_record_validates_payload_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    workspace_id = uuid.uuid4()
    with pytest.raises(WorkflowEventDomainError):
        record_workflow_event(
            pool,
            workspace_id=workspace_id,
            event_type="task_archived",  # type: ignore[arg-type]
            actor_type=WorkflowEventActor.OPENORC,
        )
    with pytest.raises(WorkflowEventDomainError):
        record_workflow_event(
            pool,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type="human",  # type: ignore[arg-type]
        )
    with pytest.raises(WorkflowEventDomainError):
        record_workflow_event(
            pool,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=WorkflowEventActor.OPENORC,
            subject_type="task",
        )
    with pytest.raises(WorkflowEventDomainError):
        record_workflow_event(
            pool,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=WorkflowEventActor.OPENORC,
            actor_id="   ",
        )
    with pytest.raises(WorkflowEventDomainError):
        record_workflow_event(
            pool,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=WorkflowEventActor.OPENORC,
            context=["not", "a", "mapping"],
        )
    assert fake_conn.executed == []


def test_get_maps_a_row_and_normalizes_to_utc() -> None:
    row = _event_row(
        task_id=uuid.uuid4(),
        actor_id="github-node-1",
        subject_type="owner_gate",
        subject_id=uuid.uuid4(),
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    event = get_workflow_event(pool, workflow_event_id=row[0])

    assert event is not None
    assert event.task_id == row[2]
    assert event.actor_id == "github-node-1"
    assert event.subject_type == "owner_gate"
    assert event.subject_id == row[7]
    assert event.created_at == _utc_recorded_at()
    assert event.created_at.utcoffset() == timedelta(0)
    assert isinstance(event.context, MappingProxyType)
    sql, params = fake_conn.executed[0]
    assert "select" in sql and "openorc.workflow_events" in sql
    assert params == (row[0],)


def test_the_read_paths_are_workspace_scoped_newest_first_and_limited() -> None:
    row = _event_row(task_id=uuid.uuid4())
    workspace_id = row[1]
    fake_conn = FakeConnection(None, rows=[row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    events = list_recent_workspace_events(pool, workspace_id=workspace_id, limit=25)

    assert [e.id for e in events] == [row[0]]
    sql, params = fake_conn.executed[0]
    assert "where workspace_id = %s order by created_at desc, id desc limit %s" in sql
    assert params == (workspace_id, 25)

    fake_conn = FakeConnection(None, rows=[row])
    pool = cast(DatabasePool, FakePool(fake_conn))
    list_task_events(pool, task_id=row[2], limit=10)
    sql, params = fake_conn.executed[0]
    assert "where task_id = %s order by created_at desc, id desc limit %s" in sql
    assert params == (row[2], 10)

    fake_conn = FakeConnection(None, rows=[row])
    pool = cast(DatabasePool, FakePool(fake_conn))
    list_recent_events_by_type(
        pool, workspace_id=workspace_id, event_type=WorkflowEventType.TASK_CREATED, limit=5
    )
    sql, params = fake_conn.executed[0]
    assert "where workspace_id = %s and event_type = %s" in sql
    assert params == (workspace_id, "task_created", 5)

    fake_conn = FakeConnection(None, rows=[row])
    pool = cast(DatabasePool, FakePool(fake_conn))
    list_recent_events_by_actor(
        pool, workspace_id=workspace_id, actor_type=WorkflowEventActor.OPENORC, limit=5
    )
    sql, params = fake_conn.executed[0]
    assert "where workspace_id = %s and actor_type = %s" in sql
    assert params == (workspace_id, "openorc", 5)

    fake_conn = FakeConnection(None, rows=[row])
    pool = cast(DatabasePool, FakePool(fake_conn))
    find_events_by_subject(
        pool, workspace_id=workspace_id, subject_type="task", subject_id=row[2], limit=5
    )
    sql, params = fake_conn.executed[0]
    assert "where workspace_id = %s and subject_type = %s and subject_id = %s" in sql
    assert params == (workspace_id, "task", row[2], 5)


def test_read_paths_reject_invalid_limits_vocabularies_and_subjects() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    workspace_id = uuid.uuid4()
    for call in (
        lambda: list_recent_workspace_events(pool, workspace_id=workspace_id, limit=0),
        lambda: list_recent_workspace_events(pool, workspace_id=workspace_id, limit=True),
        lambda: list_recent_events_by_type(
            pool,
            workspace_id=workspace_id,
            event_type="task_archived",  # type: ignore[arg-type]
            limit=5,
        ),
        lambda: list_recent_events_by_actor(
            pool,
            workspace_id=workspace_id,
            actor_type="human",  # type: ignore[arg-type]
            limit=5,
        ),
        lambda: find_events_by_subject(
            pool, workspace_id=workspace_id, subject_type="   ", subject_id=uuid.uuid4(), limit=5
        ),
    ):
        with pytest.raises(WorkflowEventDomainError):
            call()
    assert fake_conn.executed == []


def test_the_event_persistence_surface_is_insert_only() -> None:
    # Append-only is a surface contract, not a database trigger: the
    # module's public API is INSERT + reads, and no UPDATE/DELETE/upsert
    # statement exists anywhere in the module — no ordinary OpenOrc
    # persistence path can rewrite an existing event.
    assert set(events_module.__all__) == {
        "record_workflow_event",
        "get_workflow_event",
        "list_recent_workspace_events",
        "list_task_events",
        "list_recent_events_by_type",
        "list_recent_events_by_actor",
        "find_events_by_subject",
    }
    source = inspect.getsource(events_module)
    assert "update openorc.workflow_events" not in source
    assert "delete from openorc.workflow_events" not in source
    assert "on conflict" not in source
