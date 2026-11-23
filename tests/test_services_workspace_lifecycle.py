"""Deterministic tests for the Workspace lifecycle service (issue #370).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the narrowly-composed Workspace discovery and creation
flows — command-shape validation before any database work, the issue #97
account-deletion Owner-mutation barrier as the first lock acquisition of
creation, canonical Profile-derived ownership (no caller-supplied owner), and
the owner-scoped deterministic listing. Database defaults/constraints and the
real all-or-nothing rollback are proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.domain.ownership import Workspace
from openorc.observability import OPERATION, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import workspace_lifecycle
from openorc.services.errors import ConflictError, InvalidCommandError

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of creation; the scripted result row for an operational
# account carries the all-NULL attempt-state tuple, and an in-flight attempt
# carries its durable state.
_GUARD_OPERATIONAL_ROW = (None, None, None)
_GUARD_DELETION_ACTIVE_ROW = ("active", uuid.uuid4(), _OBSERVED)


class FakeCursor:
    def __init__(self, result: Any) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result if isinstance(self._result, tuple) else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._result) if isinstance(self._result, list) else []


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL.

    A scripted result is either a tuple (a ``fetchone`` row) or a list (a
    ``fetchall`` row set).
    """

    def __init__(self, results: list[Any]) -> None:
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
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("workspace lifecycle tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(
    owner_profile_id: Any,
    *,
    name: str = "platform",
    review_iteration_limit: int = 5,
    guidance: str = "",
    default_task_budget_usd: Any = None,
) -> tuple[Any, ...]:
    # The full openorc.workspaces column projection in persistence order.
    return (
        uuid.uuid4(),
        owner_profile_id,
        name,
        _OBSERVED,
        _OBSERVED,
        review_iteration_limit,
        guidance,
        default_task_budget_usd,
    )


def test_create_workspace_derives_ownership_from_the_authenticated_profile() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(profile_id)])

    workspace = workspace_lifecycle.create_workspace_for_profile(
        _pool(conn), profile_id=profile_id, name="platform"
    )

    assert workspace.owner_profile_id == profile_id
    assert isinstance(workspace, Workspace)
    # The barrier read is the FIRST statement; the owner comes only from the
    # authenticated Profile, and the name is the only honored command value.
    guard_sql, _ = conn.executed[0]
    assert "openorc.profiles" in guard_sql
    insert_sql, insert_params = conn.executed[1]
    assert "insert into openorc.workspaces" in insert_sql
    assert insert_params == (profile_id, "platform")


def test_create_workspace_rejects_invalid_names_before_any_database_work() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])

    for name in ("", "   ", "x" * 201, None, 42):
        with pytest.raises(InvalidCommandError):
            workspace_lifecycle.create_workspace_for_profile(
                _pool(conn),
                profile_id=profile_id,
                name=name,  # type: ignore[arg-type]
            )

    # Nothing was authorized, nothing was touched.
    assert conn.executed == []


def test_create_workspace_is_bounded_at_two_hundred_characters() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(profile_id, name="w" * 200)])

    workspace = workspace_lifecycle.create_workspace_for_profile(
        _pool(conn), profile_id=profile_id, name="w" * 200
    )
    assert workspace.name == "w" * 200

    over = ScriptedConnection([])
    with pytest.raises(InvalidCommandError):
        workspace_lifecycle.create_workspace_for_profile(
            _pool(over), profile_id=profile_id, name="w" * 201
        )
    assert over.executed == []


def test_create_workspace_fails_closed_during_an_account_deletion_attempt() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_DELETION_ACTIVE_ROW])

    with pytest.raises(ConflictError):
        workspace_lifecycle.create_workspace_for_profile(
            _pool(conn), profile_id=profile_id, name="platform"
        )

    # The barrier read ran and the guarded mutation was rejected before any
    # Workspace insert — and it would roll back with the composed transaction
    # even if one had raced in.
    assert len(conn.executed) == 1
    assert "openorc.profiles" in conn.executed[0][0]


def test_list_owned_workspaces_scopes_to_the_owner_deterministically() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            [
                _ws_row(profile_id),
                _ws_row(profile_id, review_iteration_limit=9, guidance="prose"),
            ]
        ]
    )

    workspaces = workspace_lifecycle.list_owned_workspaces(_pool(conn), profile_id=profile_id)

    assert len(workspaces) == 2
    assert all(workspace.owner_profile_id == profile_id for workspace in workspaces)
    assert workspaces[1].review_iteration_limit == 9
    assert workspaces[1].guidance == "prose"
    sql, params = conn.executed[0]
    assert "from openorc.workspaces" in sql
    assert "where owner_profile_id = %s" in sql
    assert "order by created_at, id" in sql
    assert params == (profile_id,)


def test_list_owned_workspaces_is_empty_without_owned_workspaces() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([[]])

    assert workspace_lifecycle.list_owned_workspaces(_pool(conn), profile_id=profile_id) == []
    assert conn.executed[0][1] == (profile_id,)


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_create_workspace_opens_its_service_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(profile_id)])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        workspace = workspace_lifecycle.create_workspace_for_profile(
            _pool(conn), profile_id=profile_id, name="platform"
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "workspace_lifecycle.create_workspace_for_profile"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID}
    assert attributes[WORKSPACE_ID] == str(workspace.id)


def test_list_owned_workspaces_opens_its_service_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([[_ws_row(profile_id)]])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        workspace_lifecycle.list_owned_workspaces(_pool(conn), profile_id=profile_id)

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "workspace_lifecycle.list_owned_workspaces"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION}
