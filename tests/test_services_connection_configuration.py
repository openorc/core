"""Deterministic tests for the Workspace Connection administration service (issue #371).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the Owner-authorized administration flows — command
shape validated before any database work (the representative invalid
capacity/endpoint rejections), the issue #97 account-deletion barrier as the
first lock acquisition of every mutation, the uniform fail-closed subject
outcome for missing/cross-Workspace Connections, the full-record replacement
with a same-value no-op (no row churn, no fabricated audit) and a
``WORKSPACE_CONFIGURATION_CHANGED`` event carrying setting+connection context
only, the non-interference contract (no statement touches
``task_agent_sessions`` or ``workflow_role_bindings``), and the read-boundary
rule: persisted safe config that violates the established #188/canonical-path
contracts fails the read closed with a fixed safe message that never echoes
the stored value. Credential isolation stays with the issue #55 service
tests; durable constraints and the real all-or-nothing rollback are proven by
the integration-marked suites.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.domain.connections import AdapterType, Connection
from openorc.observability import CONNECTION_ID, OPERATION, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import connection_configuration, connection_credentials
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of every mutation flow; the scripted result row for an
# operational account carries the all-NULL attempt-state tuple, and an
# in-flight attempt carries its durable state. The read flows compose no
# barrier.
_GUARD_OPERATIONAL_ROW = (None, None, None)
_GUARD_DELETION_ACTIVE_ROW = ("active", uuid.uuid4(), _OBSERVED)

# Safe-config values used across the flows; they must never leak into error
# messages, audit events, or telemetry attributes.
_REPOSITORY_BASE = "/srv/openorc"
_CONTROL_ENDPOINT = "ws://127.0.0.1:25463/hub"
_VALID_SAFE_CONFIG = {"repository_base": _REPOSITORY_BASE, "control_endpoint": _CONTROL_ENDPOINT}


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
        raise AssertionError("connection configuration tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(owner_profile_id: Any) -> tuple[Any, ...]:
    # The full openorc.workspaces column projection in persistence order.
    return (
        uuid.uuid4(),
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        "",
        None,
    )


def _connection_row(
    workspace_id: Any,
    *,
    connection_id: Any = None,
    name: str = "primary cline hub",
    safe_config: Any = None,
    session_capacity: int = 2,
    enabled: bool = True,
    auth_reference: str | None = None,
) -> tuple[Any, ...]:
    # The full openorc.connections column projection in persistence order.
    return (
        uuid.uuid4() if connection_id is None else connection_id,
        workspace_id,
        "cline",
        name,
        {} if safe_config is None else safe_config,
        session_capacity,
        enabled,
        auth_reference,
        _OBSERVED,
        _OBSERVED,
    )


def _event_row(*, actor_id: str) -> tuple[Any, ...]:
    """A canned ``record_workflow_event`` INSERT ... RETURNING row."""
    return (
        uuid.uuid4(),
        uuid.uuid4(),
        None,
        "workspace_configuration_changed",
        "owner",
        actor_id,
        "workspace",
        uuid.uuid4(),
        {},
        _OBSERVED,
    )


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_create_rejects_invalid_commands_before_any_database_work() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()

    invalid_commands: list[dict[str, Any]] = [
        {"name": ""},  # blank name
        {"name": "   "},  # whitespace-only name
        {"name": "x" * 201},  # overlong name
        {"adapter_type": "cline"},  # a string is never the adapter type
        {"session_capacity": 0},  # non-positive capacity
        {"session_capacity": -2},
        {"session_capacity": True},  # a boolean is never an integer
        {"session_capacity": 2.5},
        {"enabled": 1},
        {"repository_base": "relative/path"},  # not absolute
        {"repository_base": "/srv/../openorc"},  # non-canonical traversal
        {"repository_base": "/srv//openorc"},  # empty path segment
        {"repository_base": ""},
        {"control_endpoint": "http://hub.example/hub"},  # unsupported scheme
        {"control_endpoint": "ws://127.0.0.1"},  # no endpoint path
        {"control_endpoint": "wss://user:pass@hub.example/hub"},  # userinfo
        {"control_endpoint": "ws://hub.example/hub?token=x"},  # query component
        {"control_endpoint": 123},
    ]
    for overrides in invalid_commands:
        command: dict[str, Any] = {
            "adapter_type": AdapterType.CLINE,
            "name": "primary cline hub",
            "session_capacity": 2,
            "enabled": True,
            **overrides,
        }
        conn = ScriptedConnection([])
        with pytest.raises(InvalidCommandError):
            connection_configuration.create_workspace_connection(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, **command
            )
        # Validation precedes every mutation: no barrier read, no ownership
        # read, and no insert happened at all.
        assert conn.executed == []


def test_create_composes_the_account_barrier_ownership_and_insert() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    created = _connection_row(workspace_id, safe_config=_VALID_SAFE_CONFIG, session_capacity=3)
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, ws, created])

    connection = connection_configuration.create_workspace_connection(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        adapter_type=AdapterType.CLINE,
        name="primary cline hub",
        repository_base=_REPOSITORY_BASE,
        control_endpoint=_CONTROL_ENDPOINT,
        session_capacity=3,
        enabled=True,
    )

    assert connection.id == created[0]
    assert connection.name == "primary cline hub"
    assert connection.session_capacity == 3
    assert connection.safe_config == _VALID_SAFE_CONFIG
    assert connection.auth_reference is None  # creation never installs a reference

    # The guarded flow: the account barrier read and the ownership read are
    # the only statements before the insert (three executed statements).
    assert len(conn.executed) == 3
    insert_sql, insert_params = conn.executed[2]
    assert "insert into openorc.connections" in insert_sql
    assert insert_params is not None
    assert insert_params[0] == workspace_id
    assert insert_params[1] == "cline"
    assert insert_params[2] == "primary cline hub"
    assert insert_params[3].obj == _VALID_SAFE_CONFIG
    assert insert_params[4] == 3
    assert insert_params[5] is True
    assert insert_params[6] is None

    # No statement ever touches sessions or role bindings: configuration
    # edits never migrate existing Task/session bindings.
    assert not any(
        "task_agent_sessions" in sql or "workflow_role_bindings" in sql for sql, _ in conn.executed
    )


def test_create_takes_the_durable_capacity_default_and_leaves_safe_config_unset() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    created = _connection_row(ws[0])
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, ws, created])

    connection = connection_configuration.create_workspace_connection(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=ws[0],
        adapter_type=AdapterType.CLINE,
        name="primary cline hub",
    )

    assert connection.session_capacity == 2
    assert connection.safe_config == {}
    _, insert_params = conn.executed[2]
    assert insert_params is not None
    assert insert_params[3].obj == {}  # stored-but-not-configurable/attachable state
    assert insert_params[4] == 2  # the issue #200 durable default


def test_create_fails_closed_for_a_missing_or_foreign_workspace() -> None:
    profile_id = uuid.uuid4()
    foreign_workspace = _ws_row(uuid.uuid4())  # owned by another Profile

    for ws in (None, foreign_workspace):
        conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, ws])
        with pytest.raises(NotFoundError):
            connection_configuration.create_workspace_connection(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=uuid.uuid4(),
                adapter_type=AdapterType.CLINE,
                name="primary cline hub",
            )
        # The uniform subject outcome: the insert never happened.
        assert len(conn.executed) == 2


def test_create_fails_closed_while_a_deletion_attempt_is_active() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_DELETION_ACTIVE_ROW])

    with pytest.raises(ConflictError):
        connection_configuration.create_workspace_connection(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=uuid.uuid4(),
            adapter_type=AdapterType.CLINE,
            name="primary cline hub",
        )

    # The barrier is the first lock acquisition: nothing else ran.
    assert len(conn.executed) == 1


def test_replace_writes_the_full_record_and_audits_the_actual_change() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    connection_id = uuid.uuid4()
    current = _connection_row(
        workspace_id,
        connection_id=connection_id,
        safe_config={},
        session_capacity=2,
        enabled=True,
        auth_reference="vault-ref-1",
    )
    updated = _connection_row(
        workspace_id,
        connection_id=connection_id,
        name="renamed hub",
        safe_config={"repository_base": _REPOSITORY_BASE},
        session_capacity=4,
        enabled=False,
        auth_reference="vault-ref-1",
    )
    conn = ScriptedConnection(
        [_GUARD_OPERATIONAL_ROW, ws, current, updated, _event_row(actor_id=str(profile_id))]
    )

    connection = connection_configuration.replace_workspace_connection(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="renamed hub",
        repository_base=_REPOSITORY_BASE,
        control_endpoint=None,
        session_capacity=4,
        enabled=False,
    )

    assert connection.name == "renamed hub"
    assert connection.enabled is False
    # The opaque auth_reference is not a command input and is preserved
    # verbatim from the row-locked read.
    assert connection.auth_reference == "vault-ref-1"

    update_sql, update_params = conn.executed[3]
    assert "update openorc.connections" in update_sql
    assert update_params is not None
    assert update_params == (
        "renamed hub",
        update_params[1],
        4,
        False,
        "vault-ref-1",
        connection_id,
    )
    assert update_params[1].obj == {"repository_base": _REPOSITORY_BASE}

    # The coordinated event rides the same composed transaction:
    # Workspace-scoped, subject the Workspace, OWNER actor carrying the
    # authenticated Profile UUID, context only the setting and connection.
    event_sql, event_params = conn.executed[4]
    assert "insert into openorc.workflow_events" in event_sql
    assert event_params is not None
    assert event_params[0] == workspace_id
    assert event_params[1] is None
    assert event_params[2] == "workspace_configuration_changed"
    assert event_params[3] == "owner"
    assert event_params[4] == str(profile_id)
    assert event_params[5] == "workspace"
    assert event_params[6] == workspace_id
    assert event_params[7].obj == {"setting": "connection", "connection_id": str(connection_id)}

    # The Owner's safe-config values are never copied into the event.
    assert _REPOSITORY_BASE not in event_sql
    assert _REPOSITORY_BASE not in repr(event_params)

    # No statement ever touches sessions or role bindings.
    assert not any(
        "task_agent_sessions" in sql or "workflow_role_bindings" in sql for sql, _ in conn.executed
    )


def test_replace_same_value_is_a_noop_without_row_churn_or_audit() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    connection_id = uuid.uuid4()
    current = _connection_row(
        workspace_id,
        connection_id=connection_id,
        name="primary cline hub",
        safe_config=_VALID_SAFE_CONFIG,
        session_capacity=3,
        enabled=True,
        auth_reference="vault-ref-1",
    )
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, ws, current])

    connection = connection_configuration.replace_workspace_connection(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="primary cline hub",
        repository_base=_REPOSITORY_BASE,
        control_endpoint=_CONTROL_ENDPOINT,
        session_capacity=3,
        enabled=True,
    )

    assert connection.id == connection_id
    # Identical stored configuration: no update statement and no event
    # insert — the write is a no-op and fabricates no audit change.
    assert len(conn.executed) == 3
    assert "update openorc.connections" not in conn.executed[2][0]


def test_replace_fails_closed_for_missing_and_cross_workspace_connections() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    other_workspace_id = uuid.uuid4()
    foreign_connection = _connection_row(other_workspace_id)

    for row in (None, foreign_connection):
        conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, ws, row])
        with pytest.raises(NotFoundError):
            connection_configuration.replace_workspace_connection(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=workspace_id,
                connection_id=uuid.uuid4(),
                name="primary cline hub",
                repository_base=None,
                control_endpoint=None,
                session_capacity=2,
                enabled=True,
            )
        # The uniform subject outcome under the row lock: no update, no event.
        assert len(conn.executed) == 3


def test_replace_rejects_invalid_commands_before_any_database_work() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])

    for overrides in ({"name": ""}, {"session_capacity": True}, {"repository_base": "relative"}):
        command: dict[str, Any] = {
            "name": "primary cline hub",
            "repository_base": None,
            "control_endpoint": None,
            "session_capacity": 2,
            "enabled": True,
            **overrides,
        }
        with pytest.raises(InvalidCommandError):
            connection_configuration.replace_workspace_connection(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=uuid.uuid4(),
                connection_id=uuid.uuid4(),
                **command,
            )
    assert conn.executed == []


def test_reads_are_ownership_gated_and_compose_no_account_barrier() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    rows = [_connection_row(workspace_id), _connection_row(workspace_id)]

    list_conn = ScriptedConnection([ws, rows])
    listed = connection_configuration.list_workspace_connections(
        _pool(list_conn), profile_id=profile_id, workspace_id=workspace_id
    )
    assert [connection.id for connection in listed] == [rows[0][0], rows[1][0]]
    # Two statements only: ownership, then the owner-scoped listing. No
    # account-mutation barrier is composed for reads.
    assert len(list_conn.executed) == 2

    foreign_workspace = _ws_row(uuid.uuid4())
    foreign_list_conn = ScriptedConnection([foreign_workspace])
    with pytest.raises(NotFoundError):
        connection_configuration.list_workspace_connections(
            _pool(foreign_list_conn), profile_id=profile_id, workspace_id=foreign_workspace[0]
        )
    assert len(foreign_list_conn.executed) == 1


def test_get_is_ownership_gated_and_uniformly_fails_closed() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    connection_id = uuid.uuid4()
    row = _connection_row(workspace_id, connection_id=connection_id)

    conn = ScriptedConnection([ws, row])
    connection = connection_configuration.get_workspace_connection(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, connection_id=connection_id
    )
    assert connection.id == connection_id
    assert connection.safe_config == {}
    assert len(conn.executed) == 2

    # A cross-Workspace Connection is uniformly the same not-found outcome
    # as a missing one: never a probing oracle.
    foreign = _connection_row(uuid.uuid4())
    foreign_conn = ScriptedConnection([ws, foreign])
    with pytest.raises(NotFoundError):
        connection_configuration.get_workspace_connection(
            _pool(foreign_conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=foreign[0],
        )
    missing_conn = ScriptedConnection([ws, None])
    with pytest.raises(NotFoundError):
        connection_configuration.get_workspace_connection(
            _pool(missing_conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=uuid.uuid4(),
        )


def test_get_projects_a_valid_persisted_configuration_verbatim() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    connection_id = uuid.uuid4()
    row = _connection_row(workspace_id, connection_id=connection_id, safe_config=_VALID_SAFE_CONFIG)

    conn = ScriptedConnection([ws, row])
    connection = connection_configuration.get_workspace_connection(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, connection_id=connection_id
    )
    # The validated values are exactly what the domain carries: no
    # normalization, no rewriting, no invented defaults.
    assert connection.safe_config == _VALID_SAFE_CONFIG
    assert len(conn.executed) == 2


def test_get_fails_closed_on_a_malformed_persisted_endpoint_without_echo(
    caplog: pytest.LogCaptureFixture,
) -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    connection_id = uuid.uuid4()
    stored = "wss://user:pass@hub.example/hub"
    row = _connection_row(
        workspace_id, connection_id=connection_id, safe_config={"control_endpoint": stored}
    )
    conn = ScriptedConnection([ws, row])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_configuration"),
        pytest.raises(ConflictError) as exc_info,
    ):
        connection_configuration.get_workspace_connection(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=connection_id,
        )

    # Fixed safe message only: never the stored endpoint or any of its
    # prohibited components.
    message = str(exc_info.value)
    assert message == (
        "the connection's stored configuration does not satisfy the established "
        "safe-configuration contract"
    )
    assert stored not in message
    assert "user:pass" not in message
    warnings = [
        record
        for record in caplog.records
        if record.name == "openorc.services.connection_configuration"
    ]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert stored not in warnings[0].getMessage()


def test_get_fails_closed_on_a_malformed_persisted_repository_base(
    caplog: pytest.LogCaptureFixture,
) -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    stored = "/srv/../openorc"
    row = _connection_row(workspace_id, safe_config={"repository_base": stored})
    conn = ScriptedConnection([ws, row])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_configuration"),
        pytest.raises(ConflictError) as exc_info,
    ):
        connection_configuration.get_workspace_connection(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id, connection_id=row[0]
        )

    assert str(exc_info.value).startswith("the connection's stored configuration does not satisfy")
    assert stored not in str(exc_info.value)
    assert any(
        record.name == "openorc.services.connection_configuration"
        and record.levelno == logging.WARNING
        for record in caplog.records
    )


def test_list_fails_closed_when_any_persisted_row_is_malformed() -> None:
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    workspace_id = ws[0]
    good = _connection_row(workspace_id)
    malformed = _connection_row(
        workspace_id, safe_config={"control_endpoint": "ws://hub.example/hub?token=x"}
    )
    conn = ScriptedConnection([ws, [good, malformed]])

    # Fail closed: one malformed persisted row fails the whole listing
    # rather than surfacing prohibited material as approved configuration.
    with pytest.raises(ConflictError):
        connection_configuration.list_workspace_connections(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id
        )


def test_create_opens_its_service_span_with_safe_attributes_only() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    created = _connection_row(ws[0], safe_config=_VALID_SAFE_CONFIG)
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, ws, created])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        connection_configuration.create_workspace_connection(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=ws[0],
            adapter_type=AdapterType.CLINE,
            name="primary cline hub",
            repository_base=_REPOSITORY_BASE,
            control_endpoint=_CONTROL_ENDPOINT,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "connection_configuration.create_workspace_connection"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID, CONNECTION_ID}
    assert attributes[WORKSPACE_ID] == str(ws[0])
    assert attributes[CONNECTION_ID] == str(created[0])
    # Safe-config values have no supported path into telemetry.
    assert _REPOSITORY_BASE not in repr(attributes)
    assert _CONTROL_ENDPOINT not in repr(attributes)


def test_get_opens_its_service_span_with_safe_attributes_only() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    ws = _ws_row(profile_id)
    connection_id = uuid.uuid4()
    row = _connection_row(ws[0], connection_id=connection_id)
    conn = ScriptedConnection([ws, row])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        connection_configuration.get_workspace_connection(
            _pool(conn), profile_id=profile_id, workspace_id=ws[0], connection_id=connection_id
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "connection_configuration.get_workspace_connection"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID, CONNECTION_ID}


def _domain_connection(
    workspace_id: Any,
    *,
    connection_id: Any = None,
    safe_config: Any = None,
    auth_reference: str | None = None,
) -> Connection:
    """A domain Connection as the #55 credential service would return it."""
    return Connection(
        id=connection_id or uuid.uuid4(),
        workspace_id=workspace_id,
        adapter=AdapterType.CLINE,
        name="primary cline hub",
        safe_config={} if safe_config is None else safe_config,
        session_capacity=2,
        enabled=True,
        auth_reference=auth_reference,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def test_credential_setup_delegates_to_the_55_service_and_revalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    configured = _domain_connection(
        workspace_id,
        connection_id=connection_id,
        safe_config=_VALID_SAFE_CONFIG,
        auth_reference="vault-ref-1",
    )
    commands: list[dict[str, Any]] = []

    def _fake_configure(pool: Any, **command: Any) -> Any:
        commands.append(command)
        return configured

    monkeypatch.setattr(connection_credentials, "configure_connection_credential", _fake_configure)
    conn = ScriptedConnection([])

    connection = connection_configuration.setup_connection_credential(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        credential="s3cr3t",
    )

    # The #55 composition delegates the exact command and is the only
    # database consumer: the wrapper itself executes no SQL.
    assert connection is configured
    assert commands == [
        {
            "profile_id": profile_id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
            "credential": "s3cr3t",
        }
    ]
    assert conn.executed == []


def test_credential_setup_fails_closed_on_a_malformed_persisted_endpoint(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stored = "wss://user:pass@hub.example/hub"
    leaked = _domain_connection(
        uuid.uuid4(), safe_config={"control_endpoint": stored}, auth_reference="vault-ref-1"
    )
    monkeypatch.setattr(
        connection_credentials,
        "configure_connection_credential",
        lambda pool, **command: leaked,
    )
    conn = ScriptedConnection([])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_configuration"),
        pytest.raises(ConflictError) as exc_info,
    ):
        connection_configuration.setup_connection_credential(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            connection_id=uuid.uuid4(),
            credential="s3cr3t",
        )

    # The credential composition must not project the unvalidated stored
    # endpoint: the fixed safe message never carries any stored value.
    assert str(exc_info.value) == (
        "the connection's stored configuration does not satisfy the established "
        "safe-configuration contract"
    )
    assert stored not in str(exc_info.value)
    assert "user:pass" not in str(exc_info.value)
    assert any(
        record.name == "openorc.services.connection_configuration"
        and record.levelno == logging.WARNING
        for record in caplog.records
    )


def test_credential_rotation_fails_closed_on_a_malformed_persisted_endpoint(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stored = "ws://hub.example/hub?token=x"
    leaked = _domain_connection(
        uuid.uuid4(), safe_config={"control_endpoint": stored}, auth_reference="vault-ref-1"
    )
    monkeypatch.setattr(
        connection_credentials,
        "rotate_connection_credential",
        lambda pool, **command: leaked,
    )
    conn = ScriptedConnection([])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_configuration"),
        pytest.raises(ConflictError) as exc_info,
    ):
        connection_configuration.rotate_connection_credential(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            connection_id=uuid.uuid4(),
            credential="s3cr3t",
        )

    assert str(exc_info.value).startswith("the connection's stored configuration does not satisfy")
    assert stored not in str(exc_info.value)
    assert any(
        record.name == "openorc.services.connection_configuration"
        and record.levelno == logging.WARNING
        for record in caplog.records
    )
