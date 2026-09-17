"""Deterministic mapping tests for PlanRevision repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the session/ownership mapping fakes)
to prove row-to-domain-object mapping, UTC normalization at the persistence
boundary, parameterization, the insert-only SQL shape, and empty-result
handling. Database constraint behavior is proven against a real database by
the integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.planning import PlanRevisionDomainError
from openorc.persistence import planning as planning_module
from openorc.persistence.planning import (
    create_plan_revision,
    get_plan_revision,
    list_task_plan_revisions,
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


def _revision_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "revision_number": 2,
        "content": "# Plan\n\n1. Implement the thing.",
        "repository_base_sha": "0123456789abcdef0123456789abcdef01234567",
        "created_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["revision_number"],
        values["content"],
        values["repository_base_sha"],
        values["created_at"],
    )


def test_create_plan_revision_inserts_and_maps_the_row() -> None:
    row = _revision_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    revision = create_plan_revision(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        revision_number=row[3],
        content=row[4],
        repository_base_sha=row[5],
    )

    assert revision.id == row[0]
    assert revision.workspace_id == row[1]
    assert revision.task_id == row[2]
    assert revision.revision_number == 2
    assert revision.content == row[4]
    assert revision.repository_base_sha == row[5]
    assert revision.created_at == _utc_observed_at()
    assert revision.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.plan_revisions" in sql
    assert params == (row[1], row[2], row[3], row[4], row[5])
    assert len(fake_conn.executed) == 1


def test_create_plan_revision_validates_inputs_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    base: dict[str, Any] = {
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
    }
    with pytest.raises(PlanRevisionDomainError):
        create_plan_revision(pool, revision_number=0, content="x", repository_base_sha="b", **base)
    with pytest.raises(PlanRevisionDomainError):
        create_plan_revision(
            pool, revision_number=1, content="   ", repository_base_sha="b", **base
        )
    with pytest.raises(PlanRevisionDomainError):
        create_plan_revision(pool, revision_number=1, content="x", repository_base_sha="", **base)
    assert fake_conn.executed == []


def test_get_plan_revision_maps_or_returns_none() -> None:
    row = _revision_row()
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    revision = get_plan_revision(pool, plan_revision_id=row[0])
    assert revision is not None
    assert revision.id == row[0]
    assert revision.created_at == _utc_observed_at()

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_plan_revision(empty_pool, plan_revision_id=row[0]) is None


def test_list_task_plan_revisions_orders_by_revision_number() -> None:
    first = _revision_row(revision_number=1)
    second = _revision_row(revision_number=2)
    fake_conn = FakeConnection(rows=[first, second])
    pool = cast(DatabasePool, FakePool(fake_conn))

    revisions = list_task_plan_revisions(pool, task_id=first[2])

    assert [revision.revision_number for revision in revisions] == [1, 2]
    assert [revision.id for revision in revisions] == [first[0], second[0]]
    sql, params = fake_conn.executed[0]
    assert "openorc.plan_revisions" in sql
    assert "order by revision_number" in sql
    assert params == (first[2],)


def test_the_module_surface_is_insert_only() -> None:
    # Immutability is structural: the module's public surface has no update
    # function, only insert/get/list. A changed plan is a fresh revision.
    assert set(planning_module.__all__) == {
        "create_plan_revision",
        "get_plan_revision",
        "list_task_plan_revisions",
    }
