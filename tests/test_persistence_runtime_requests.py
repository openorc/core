"""Deterministic mapping tests for RuntimeRequest repositories (issue #24).

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the planning/review mapping fakes)
to prove row-to-domain-object mapping, UTC normalization, parameterization,
the producer-role enforcement in the creation transaction, the fixed
``action_approval`` kind, and the one-shot terminal-transition SQL (a
terminal request is an immutable historical record). Database constraint
behavior (the full-history correlation uniqueness) is proven against a real
database by the integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.runtime_requests import (
    RuntimeRequestDomainError,
    RuntimeRequestResolution,
    RuntimeRequestStatus,
)
from openorc.persistence import runtime_requests as requests_module
from openorc.persistence.pool import DatabasePool
from openorc.persistence.runtime_requests import (
    cancel_runtime_request,
    create_runtime_request,
    expire_runtime_request,
    get_runtime_request,
    list_task_runtime_requests,
    resolve_runtime_request,
)


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


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _request_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "producer_session_id": uuid.uuid4(),
        "kind": "action_approval",
        "external_approval_id": "approval-1",
        "status": "pending",
        "resolution": None,
        "closed_at": None,
        "created_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["producer_session_id"],
        values["kind"],
        values["external_approval_id"],
        values["status"],
        values["resolution"],
        values["closed_at"],
        values["created_at"],
    )


def test_create_runtime_request_inserts_pending_action_approval() -> None:
    row = _request_row()
    fake_conn = FakeConnection(responses=[("producer",), row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    request = create_runtime_request(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        producer_session_id=row[3],
        external_approval_id=row[5],
    )

    assert request.id == row[0]
    assert request.kind.value == "action_approval"
    assert request.status is RuntimeRequestStatus.PENDING
    assert request.resolution is None and request.closed_at is None
    assert request.created_at == _utc_observed_at()
    assert request.created_at.utcoffset() == timedelta(0)

    assert len(fake_conn.executed) == 2
    role_sql, role_params = fake_conn.executed[0]
    assert "select role from openorc.task_agent_sessions" in role_sql
    assert role_params == (row[3],)
    insert_sql, insert_params = fake_conn.executed[1]
    assert "insert into openorc.runtime_requests" in insert_sql
    # The lifecycle column carries no database default: the initial pending
    # status is explicit in the creation statement.
    assert "producer_session_id, kind, status, external_approval_id" in insert_sql
    assert insert_params == (
        row[1],
        row[2],
        row[3],
        "action_approval",
        "pending",
        row[5],
    )
    assert len(fake_conn.executed) == 2


def test_create_runtime_request_rejects_reviewer_or_missing_session() -> None:
    base: dict[str, Any] = {
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "producer_session_id": uuid.uuid4(),
        "external_approval_id": "approval-1",
    }
    reviewer_conn = FakeConnection(responses=[("reviewer",)])
    with pytest.raises(RuntimeRequestDomainError):
        create_runtime_request(cast(DatabasePool, FakePool(reviewer_conn)), **base)
    missing_conn = FakeConnection(responses=[None])
    with pytest.raises(RuntimeRequestDomainError):
        create_runtime_request(cast(DatabasePool, FakePool(missing_conn)), **base)
    assert len(reviewer_conn.executed) == 1
    assert len(missing_conn.executed) == 1


def test_create_runtime_request_requires_a_nonempty_external_identifier() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    for bad in ("", "   ", None, 42):
        with pytest.raises(RuntimeRequestDomainError):
            create_runtime_request(
                pool,
                workspace_id=uuid.uuid4(),
                task_id=uuid.uuid4(),
                producer_session_id=uuid.uuid4(),
                external_approval_id=bad,  # type: ignore[arg-type]
            )
    assert fake_conn.executed == []


def test_get_runtime_request_maps_or_returns_none() -> None:
    row = _request_row()
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    request = get_runtime_request(pool, runtime_request_id=row[0])
    assert request is not None
    assert request.created_at == _utc_observed_at()

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_runtime_request(empty_pool, runtime_request_id=row[0]) is None


def test_list_task_runtime_requests_orders_by_creation() -> None:
    first = _request_row(status="resolved", resolution="approved", closed_at=_observed_at())
    second = _request_row(task_id=first[2], external_approval_id="approval-2")
    fake_conn = FakeConnection(rows=[first, second])
    pool = cast(DatabasePool, FakePool(fake_conn))

    requests = list_task_runtime_requests(pool, task_id=first[2])

    assert [request.id for request in requests] == [first[0], second[0]]
    sql, params = fake_conn.executed[0]
    assert "openorc.runtime_requests" in sql
    assert "order by created_at, id" in sql
    assert params == (first[2],)


def test_resolve_runtime_request_applies_the_typed_one_shot_control() -> None:
    row = _request_row()
    resolved = _request_row(
        id=row[0], status="resolved", resolution="approved", closed_at=_observed_at()
    )
    fake_conn = FakeConnection(row=resolved)
    pool = cast(DatabasePool, FakePool(fake_conn))

    request = resolve_runtime_request(
        pool, runtime_request_id=row[0], resolution=RuntimeRequestResolution.APPROVED
    )

    assert request is not None
    assert request.status is RuntimeRequestStatus.RESOLVED
    assert request.resolution is RuntimeRequestResolution.APPROVED
    assert request.closed_at == _utc_observed_at()
    sql, params = fake_conn.executed[0]
    assert "update openorc.runtime_requests" in sql
    assert "set status = %s, resolution = %s, closed_at = now()" in sql
    assert "where id = %s and status = 'pending'" in sql
    assert params == ("resolved", "approved", row[0])


def test_resolve_runtime_request_rejects_non_typed_controls() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    with pytest.raises(RuntimeRequestDomainError):
        resolve_runtime_request(
            pool,
            runtime_request_id=uuid.uuid4(),
            resolution="sure, go ahead",  # type: ignore[arg-type]
        )
    assert fake_conn.executed == []


def test_expire_and_cancel_are_one_shot_terminal_transitions() -> None:
    expired = _request_row(status="expired", closed_at=_observed_at())
    expire_conn = FakeConnection(row=expired)
    request = expire_runtime_request(
        cast(DatabasePool, FakePool(expire_conn)), runtime_request_id=expired[0]
    )
    assert request is not None
    assert request.status is RuntimeRequestStatus.EXPIRED
    assert request.resolution is None and request.closed_at == _utc_observed_at()
    sql, params = expire_conn.executed[0]
    assert "set status = %s, resolution = %s, closed_at = now()" in sql
    assert "where id = %s and status = 'pending'" in sql
    assert params == ("expired", None, expired[0])

    cancelled = _request_row(status="cancelled", closed_at=_observed_at())
    cancel_conn = FakeConnection(row=cancelled)
    request = cancel_runtime_request(
        cast(DatabasePool, FakePool(cancel_conn)), runtime_request_id=cancelled[0]
    )
    assert request is not None
    assert request.status is RuntimeRequestStatus.CANCELLED
    assert request.resolution is None
    assert cancel_conn.executed[0][1] == ("cancelled", None, cancelled[0])


def test_terminal_transitions_on_missing_or_terminal_rows_are_no_ops() -> None:
    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    request_id = uuid.uuid4()
    assert (
        resolve_runtime_request(
            empty_pool, runtime_request_id=request_id, resolution=RuntimeRequestResolution.APPROVED
        )
        is None
    )
    assert expire_runtime_request(empty_pool, runtime_request_id=request_id) is None
    assert cancel_runtime_request(empty_pool, runtime_request_id=request_id) is None


def test_the_module_surface_carries_the_one_shot_terminal_contract() -> None:
    assert set(requests_module.__all__) == {
        "cancel_runtime_request",
        "create_runtime_request",
        "expire_runtime_request",
        "get_runtime_request",
        "list_task_runtime_requests",
        "resolve_runtime_request",
    }
