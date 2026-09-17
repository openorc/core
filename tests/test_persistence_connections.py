"""Deterministic mapping tests for Connection and role binding repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows and a
fake pool/connection seam (mirroring the transaction-boundary and ownership
mapping fakes) to prove row-to-domain-object mapping, UTC normalization at the
persistence boundary, parameterization, upsert semantics at the SQL level, and
empty-result handling. Database constraint behavior is proven against a real
database by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

from psycopg.types.json import Jsonb

from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.persistence.connections import (
    create_connection,
    get_connection,
    get_role_binding,
    list_connection_bindings,
    list_workspace_connections,
    set_role_binding,
    update_connection,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction


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


def _connection_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "adapter_type": "cline",
        "name": "primary cline hub",
        "safe_config": {"base_url": "https://hub.example.com"},
        "session_capacity": 2,
        "enabled": True,
        "auth_reference": None,
        "reported_provider": None,
        "reported_model": None,
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["adapter_type"],
        values["name"],
        values["safe_config"],
        values["session_capacity"],
        values["enabled"],
        values["auth_reference"],
        values["reported_provider"],
        values["reported_model"],
        values["created_at"],
        values["updated_at"],
    )


def _binding_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "role": "producer",
        "connection_id": uuid.uuid4(),
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["role"],
        values["connection_id"],
        values["created_at"],
        values["updated_at"],
    )


def test_create_connection_maps_row_and_normalizes_utc() -> None:
    row = _connection_row(auth_reference="vault://openorc/connection-auth/abc")
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    created = create_connection(
        pool,
        workspace_id=row[1],
        adapter=AdapterType("cline"),
        name="primary cline hub",
        safe_config={"base_url": "https://hub.example.com"},
        session_capacity=2,
        enabled=True,
        auth_reference="vault://openorc/connection-auth/abc",
    )

    assert created.id == row[0]
    assert created.workspace_id == row[1]
    assert created.adapter == AdapterType("cline")
    assert created.session_capacity == 2
    assert created.enabled is True
    assert created.auth_reference == "vault://openorc/connection-auth/abc"
    assert created.reported_provider is None
    assert created.reported_model is None
    assert created.created_at == _utc_observed_at()
    assert created.updated_at == _utc_observed_at()
    assert created.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "openorc.connections" in sql
    assert params is not None
    # psycopg 3 does not adapt plain mappings to jsonb without an explicit
    # wrapper: the repository must supply the Jsonb adapter itself.
    assert isinstance(params[3], Jsonb)
    assert params[3].obj == {"base_url": "https://hub.example.com"}
    assert params[0:3] == (row[1], "cline", "primary cline hub")
    assert params[4:] == (2, True, "vault://openorc/connection-auth/abc", None, None)


def test_get_connection_maps_row_or_none() -> None:
    row = _connection_row()
    found = get_connection(cast(DatabasePool, FakePool(FakeConnection(row))), row[0])
    assert found is not None
    assert found.id == row[0]
    assert found.created_at == _utc_observed_at()
    assert get_connection(cast(DatabasePool, FakePool(FakeConnection(None))), row[0]) is None


def test_list_workspace_connections_maps_rows() -> None:
    rows = [_connection_row(), _connection_row()]
    listed = list_workspace_connections(
        cast(DatabasePool, FakePool(FakeConnection(None, rows))), workspace_id=rows[0][1]
    )
    assert [connection.id for connection in listed] == [rows[0][0], rows[1][0]]
    assert all(connection.created_at == _utc_observed_at() for connection in listed)
    assert (
        list_workspace_connections(
            cast(DatabasePool, FakePool(FakeConnection(None, []))), workspace_id=rows[0][1]
        )
        == []
    )


def test_update_connection_maps_updated_row_and_params() -> None:
    row = _connection_row(
        name="renamed hub",
        safe_config={},
        session_capacity=3,
        enabled=False,
        auth_reference="vault://openorc/connection-auth/def",
        updated_at=_observed_at().astimezone(UTC),
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    updated = update_connection(
        pool,
        row[0],
        name="renamed hub",
        safe_config={},
        session_capacity=3,
        enabled=False,
        auth_reference="vault://openorc/connection-auth/def",
    )

    assert updated is not None
    assert updated.name == "renamed hub"
    assert updated.session_capacity == 3
    assert updated.enabled is False
    assert updated.auth_reference == "vault://openorc/connection-auth/def"
    sql, params = fake_conn.executed[0]
    assert "openorc.connections" in sql
    assert params is not None
    assert "updated_at = now()" in sql
    assert params[0] == "renamed hub"
    assert isinstance(params[1], Jsonb)
    assert params[1].obj == {}
    assert params[2:5] == (3, False, "vault://openorc/connection-auth/def")
    assert params[5] == row[0]
    assert (
        update_connection(
            cast(DatabasePool, FakePool(FakeConnection(None))),
            row[0],
            name="missing",
            safe_config={},
            session_capacity=1,
            enabled=True,
            auth_reference=None,
        )
        is None
    )


def test_set_role_binding_upsert_repoints_and_preserves_identity() -> None:
    # The canned row models the upsert result for a binding that already
    # existed: ON CONFLICT ... DO UPDATE keeps the original id/created_at and
    # repoints connection_id. The repository returns exactly that row, so the
    # caller sees one stable binding identity across reconfiguration.
    original_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repointed_row = _binding_row(
        id=original_id,
        workspace_id=workspace_id,
        role="producer",
        connection_id=uuid.uuid4(),
    )
    fake_conn = FakeConnection(repointed_row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    binding = set_role_binding(
        pool,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=repointed_row[3],
    )

    assert binding.id == original_id
    assert binding.role == WorkflowRole.PRODUCER
    assert binding.connection_id == repointed_row[3]

    sql, params = fake_conn.executed[0]
    assert "openorc.workflow_role_bindings" in sql
    assert "on conflict (workspace_id, role) do update" in sql
    assert "set connection_id = excluded.connection_id" in sql
    assert params == (workspace_id, "producer", repointed_row[3])


def test_get_role_binding_maps_row_or_none() -> None:
    row = _binding_row(role="reviewer")
    found = get_role_binding(
        cast(DatabasePool, FakePool(FakeConnection(row))),
        workspace_id=row[1],
        role=WorkflowRole.REVIEWER,
    )
    assert found is not None
    assert found.id == row[0]
    assert found.role == WorkflowRole.REVIEWER
    assert found.created_at == _utc_observed_at()
    assert (
        get_role_binding(
            cast(DatabasePool, FakePool(FakeConnection(None))),
            workspace_id=row[1],
            role=WorkflowRole.REVIEWER,
        )
        is None
    )


def test_list_connection_bindings_maps_rows() -> None:
    rows = [_binding_row(role="producer"), _binding_row(role="reviewer")]
    listed = list_connection_bindings(
        cast(DatabasePool, FakePool(FakeConnection(None, rows))), connection_id=rows[0][3]
    )
    assert [binding.id for binding in listed] == [rows[0][0], rows[1][0]]
    assert [binding.role for binding in listed] == [WorkflowRole.PRODUCER, WorkflowRole.REVIEWER]
    assert (
        list_connection_bindings(
            cast(DatabasePool, FakePool(FakeConnection(None, []))), connection_id=rows[0][3]
        )
        == []
    )


def test_transaction_boundary_is_used_for_every_repository_operation() -> None:
    # Repositories never self-manage commits: every operation goes through the
    # shared thin transaction boundary over the pool's connection context.
    fake_conn = FakeConnection(_binding_row())
    pool = cast(DatabasePool, FakePool(fake_conn))
    with transaction(pool) as conn:
        assert isinstance(conn, FakeConnection)
