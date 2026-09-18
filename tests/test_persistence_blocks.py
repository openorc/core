"""Deterministic mapping tests for TaskBlock repositories (issue #24).

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the planning/review mapping fakes)
to prove row-to-domain-object mapping, UTC normalization, parameterization,
the canonical-JSON context write through the explicit ``Jsonb`` adapter,
and the one-shot resolution SQL (a resolved block is historical and retains
its reason and recovery context). Database constraint behavior is proven
against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from psycopg.types.json import Jsonb

from openorc.domain.blocks import TaskBlockDomainError, TaskBlockReason
from openorc.persistence import blocks as blocks_module
from openorc.persistence.blocks import (
    create_task_block,
    get_task_block,
    list_current_task_blocks,
    list_task_blocks,
    resolve_task_block,
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
    """Records executed SQL and returns canned rows."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
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


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _block_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "reason": "stale_operation",
        "context": {"expected_subject": "abc", "observed_subject": "def"},
        "resolved_at": None,
        "created_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["reason"],
        values["context"],
        values["resolved_at"],
        values["created_at"],
    )


def test_create_task_block_inserts_current_and_maps_the_row() -> None:
    row = _block_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    block = create_task_block(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        reason=TaskBlockReason.STALE_OPERATION,
        context={"expected_subject": "abc", "observed_subject": "def"},
    )

    assert block.id == row[0]
    assert block.reason is TaskBlockReason.STALE_OPERATION
    assert block.context == {"expected_subject": "abc", "observed_subject": "def"}
    assert block.resolved_at is None
    assert block.created_at == _utc_observed_at()
    assert block.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.task_blocks" in sql
    assert params is not None
    reason, context = params[2], params[3]
    assert reason == "stale_operation"
    assert isinstance(context, Jsonb)
    assert context.obj == {"expected_subject": "abc", "observed_subject": "def"}
    assert len(fake_conn.executed) == 1


def test_create_task_block_requires_a_vocabulary_reason_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    with pytest.raises(TaskBlockDomainError):
        create_task_block(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            reason="review_loop_exhausted",  # type: ignore[arg-type]
            context={},
        )
    with pytest.raises(TaskBlockDomainError):
        create_task_block(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            reason="chat",  # type: ignore[arg-type]
            context={},
        )
    assert fake_conn.executed == []


def test_create_task_block_canonicalizes_the_context_write() -> None:
    row = _block_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    create_task_block(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        reason=TaskBlockReason.RUNTIME_FAILURE,
        context={"steps": ("retry", "reconcile"), "nested": {"deep": (1, 2)}},
    )

    _, params = fake_conn.executed[0]
    assert params is not None
    context = params[3]
    assert isinstance(context, Jsonb)
    assert context.obj == {
        "steps": ["retry", "reconcile"],
        "nested": {"deep": [1, 2]},
    }


def test_get_task_block_maps_or_returns_none() -> None:
    row = _block_row()
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    block = get_task_block(pool, task_block_id=row[0])
    assert block is not None
    assert block.created_at == _utc_observed_at()

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_task_block(empty_pool, task_block_id=row[0]) is None


def test_list_task_blocks_retains_full_history_in_creation_order() -> None:
    first = _block_row(resolved_at=_observed_at())
    second = _block_row(task_id=first[2], reason="agent_session_lost")
    fake_conn = FakeConnection(rows=[first, second])
    pool = cast(DatabasePool, FakePool(fake_conn))

    blocks = list_task_blocks(pool, task_id=first[2])

    assert [block.id for block in blocks] == [first[0], second[0]]
    assert blocks[0].resolved_at == _utc_observed_at()  # resolved blocks stay historical
    sql, params = fake_conn.executed[0]
    assert "openorc.task_blocks" in sql
    assert "order by created_at, id" in sql
    assert params == (first[2],)


def test_list_current_task_blocks_filters_unresolved() -> None:
    current = _block_row()
    fake_conn = FakeConnection(rows=[current])
    pool = cast(DatabasePool, FakePool(fake_conn))

    blocks = list_current_task_blocks(pool, task_id=current[2])

    assert [block.id for block in blocks] == [current[0]]
    sql, params = fake_conn.executed[0]
    assert "resolved_at is null" in sql
    assert params == (current[2],)


def test_resolve_task_block_is_a_one_shot_semantic_stamp() -> None:
    row = _block_row()
    resolved = _block_row(id=row[0], resolved_at=_observed_at())
    fake_conn = FakeConnection(row=resolved)
    pool = cast(DatabasePool, FakePool(fake_conn))

    block = resolve_task_block(pool, task_block_id=row[0])

    assert block is not None
    assert block.resolved_at == _utc_observed_at()
    assert block.reason is TaskBlockReason.STALE_OPERATION
    # The reason and recovery context survive resolution intact.
    assert block.context == {"expected_subject": "abc", "observed_subject": "def"}
    sql, params = fake_conn.executed[0]
    assert "update openorc.task_blocks" in sql
    assert "set resolved_at = now()" in sql
    assert "where id = %s and resolved_at is null" in sql
    assert params == (row[0],)


def test_resolve_task_block_on_missing_or_resolved_blocks_is_a_no_op() -> None:
    pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert resolve_task_block(pool, task_block_id=uuid.uuid4()) is None


def test_the_module_surface_is_history_shaped() -> None:
    assert set(blocks_module.__all__) == {
        "create_task_block",
        "get_task_block",
        "list_current_task_blocks",
        "list_task_blocks",
        "resolve_task_block",
    }
