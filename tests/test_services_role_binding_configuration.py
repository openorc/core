"""Deterministic tests for the role-binding configuration service (issue #162).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the ownership-gated configuration flows — ownership
established by the shared Workspace resolvers for reads and mutations alike
(uniform fail-closed not-found for missing/foreign Workspaces), the
statement-level change fact, the audit coordination (a
``WORKSPACE_CONFIGURATION_CHANGED`` event carrying setting+role context only,
recorded inside the same composed transaction for actual changes including
initial creation, and never for a no-op), the provider/model pair command
validation before persistence, the verbatim role-prompt override (any
non-NULL string, including empty, stored verbatim; explicit reset to NULL),
and the non-interference contract: no statement touches
``task_agent_sessions``, so a configuration edit can never rewrite an
initialized ``TaskAgentSession`` or its historical effective snapshot.
Database constraints and the real all-or-nothing rollback are proven by the
integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import errors as psycopg_errors

from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.services import role_binding_configuration
from openorc.services.errors import InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of the mutation flow; the scripted result row for an
# operational account carries the all-NULL attempt-state tuple. The read
# flow composes no barrier.
_GUARD_OPERATIONAL_ROW = (None, None, None)


class FakeCursor:
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
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("configuration tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(owner_profile_id: Any) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        "",
    )


def _binding_row(
    *,
    workspace_id: Any,
    role: str = "producer",
    connection_id: Any = None,
    configured_provider: str | None = None,
    configured_model: str | None = None,
    role_prompt_override: str | None = None,
    binding_id: Any = None,
) -> tuple[Any, ...]:
    return (
        binding_id or uuid.uuid4(),
        workspace_id,
        role,
        connection_id or uuid.uuid4(),
        configured_provider,
        configured_model,
        role_prompt_override,
        _OBSERVED,
        _OBSERVED,
    )


def _event_row(*, actor_id: str, context: dict[str, object] | None = None) -> tuple[Any, ...]:
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
        context or {},
        _OBSERVED,
    )


def test_get_role_binding_configuration_is_ownership_gated() -> None:
    profile_id = uuid.uuid4()
    workspace = _ws_row(profile_id)
    workspace_id = workspace[0]
    binding = _binding_row(workspace_id=workspace_id, role="producer")

    loaded = role_binding_configuration.get_role_binding_configuration(
        _pool(ScriptedConnection([workspace, binding])),
        profile_id=profile_id,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
    )
    assert loaded.id == binding[0]
    assert loaded.configured_provider is None
    assert loaded.configured_model is None
    assert loaded.role_prompt_override is None

    # Authentication does not establish Workspace access: another Profile's
    # Workspace (foreign-owned and missing alike) is uniformly not found.
    with pytest.raises(NotFoundError):
        role_binding_configuration.get_role_binding_configuration(
            _pool(ScriptedConnection([_ws_row(uuid.uuid4())])),
            profile_id=profile_id,
            workspace_id=workspace_id,
            role=WorkflowRole.PRODUCER,
        )

    # A missing binding is uniformly not found (never invented).
    with pytest.raises(NotFoundError):
        role_binding_configuration.get_role_binding_configuration(
            _pool(ScriptedConnection([_ws_row(profile_id), None])),
            profile_id=profile_id,
            workspace_id=uuid.uuid4(),
            role=WorkflowRole.REVIEWER,
        )


def test_set_role_binding_configuration_creates_and_records_the_change() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    created = _binding_row(
        workspace_id=workspace_id,
        role="producer",
        connection_id=connection_id,
        configured_provider="provider-id-1",
        configured_model="model-id-x",
        role_prompt_override="/# Producer override",
    )
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            created,
            _event_row(actor_id=str(profile_id)),
        ]
    )

    result = role_binding_configuration.set_role_binding_configuration(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
        configured_provider="provider-id-1",
        configured_model="model-id-x",
        role_prompt_override="/# Producer override",
    )

    assert result.changed is True
    assert result.binding.id == created[0]
    assert result.binding.configured_provider == "provider-id-1"
    assert result.binding.configured_model == "model-id-x"
    assert result.binding.role_prompt_override == "/# Producer override"

    upsert_sql, upsert_params = conn.executed[2]
    assert "insert into openorc.workflow_role_bindings" in upsert_sql
    assert upsert_params is not None
    assert upsert_params == (
        workspace_id,
        "producer",
        connection_id,
        "provider-id-1",
        "model-id-x",
        "/# Producer override",
    )

    # Initial creation is an actual change: the coordinated event rides the
    # same composed transaction. Workspace-scoped, subject the Workspace,
    # OWNER actor carrying the authenticated Profile UUID, context only the
    # setting and role.
    event_sql, event_params = conn.executed[3]
    assert "insert into openorc.workflow_events" in event_sql
    assert event_params is not None
    assert event_params[0] == workspace_id
    assert event_params[1] is None
    assert event_params[2] == "workspace_configuration_changed"
    assert event_params[3] == "owner"
    assert event_params[4] == str(profile_id)
    assert event_params[5] == "workspace"
    assert event_params[6] == workspace_id
    assert event_params[7].obj == {
        "setting": "workflow_role_binding",
        "role": "producer",
    }

    # The Owner's provider/model identifiers and role-prompt prose are never
    # copied into the event: no value appears in the event statement or its
    # parameters.
    event_sql, event_params = conn.executed[3]
    assert "provider-id-1" not in event_sql
    assert "/# Producer override" not in event_sql
    assert "provider-id-1" not in repr(event_params)
    assert "/# Producer override" not in repr(event_params)


def test_set_role_binding_configuration_updates_in_place_and_can_clear() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    updated = _binding_row(
        binding_id=uuid.uuid4(),
        workspace_id=workspace_id,
        role="reviewer",
        connection_id=connection_id,
        configured_provider="provider-id-2",
        configured_model="model-id-y",
        role_prompt_override=None,
    )
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            updated,
            _event_row(actor_id=str(profile_id)),
        ]
    )

    result = role_binding_configuration.set_role_binding_configuration(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        role=WorkflowRole.REVIEWER,
        connection_id=connection_id,
        configured_provider="provider-id-2",
        configured_model="model-id-y",
        role_prompt_override=None,
    )

    assert result.changed is True
    # The full record is written; a None override is the explicit reset (the
    # shipped default resumes on the next runtime construction).
    assert result.binding.configured_provider == "provider-id-2"
    assert result.binding.configured_model == "model-id-y"
    assert result.binding.role_prompt_override is None
    upsert_sql, upsert_params = conn.executed[2]
    assert upsert_params is not None
    assert upsert_params[-1] is None
    _sql, event_params = conn.executed[3]
    assert event_params is not None
    assert event_params[7].obj == {
        "setting": "workflow_role_binding",
        "role": "reviewer",
    }


def test_set_role_binding_configuration_identical_write_is_a_no_op() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    current = _binding_row(
        workspace_id=workspace_id,
        role="producer",
        connection_id=connection_id,
        configured_provider="provider-id-1",
        configured_model="model-id-x",
        role_prompt_override="   ",
    )
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            None,  # suppressed upsert: the stored configuration is identical
            current,  # the re-selected current row
        ]
    )

    result = role_binding_configuration.set_role_binding_configuration(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
        configured_provider="provider-id-1",
        configured_model="model-id-x",
        role_prompt_override="   ",
    )

    assert result.changed is False
    assert result.binding.id == current[0]
    assert result.binding.updated_at == current[8]
    # A no-op write records no event and attempts no event insert.
    assert len(conn.executed) == 4
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)
    # The reported state comes from the honest re-select, not from the
    # suppressed upsert.
    assert "select" in conn.executed[3][0]


def test_set_role_binding_configuration_rejects_invalid_commands() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection([])

    def update(**overrides: Any) -> None:
        command: dict[str, Any] = {
            "profile_id": profile_id,
            "workspace_id": workspace_id,
            "role": WorkflowRole.PRODUCER,
            "connection_id": uuid.uuid4(),
            "configured_provider": None,
            "configured_model": None,
            "role_prompt_override": None,
        }
        command.update(overrides)
        role_binding_configuration.set_role_binding_configuration(_pool(conn), **command)

    # A partial one-value pair is not a valid command.
    with pytest.raises(InvalidCommandError):
        update(configured_provider="provider-only")
    with pytest.raises(InvalidCommandError):
        update(configured_model="model-only")
    # A present pair member must be a nonblank opaque string.
    with pytest.raises(InvalidCommandError):
        update(configured_provider="   ", configured_model="model-id-x")
    with pytest.raises(InvalidCommandError):
        update(configured_provider="provider-id-1", configured_model="   ")
    # The override is a string or null; any other type is not a command.
    with pytest.raises(InvalidCommandError):
        update(role_prompt_override=123)
    # Malformed identities and roles are invalid commands, never a
    # best-effort interpretation.
    with pytest.raises(InvalidCommandError):
        update(connection_id="not-a-uuid")  # type: ignore[arg-type]
    with pytest.raises(InvalidCommandError):
        update(role="navigator")  # type: ignore[arg-type]

    # Invalid commands never reach authorization or persistence.
    assert conn.executed == []


def test_set_role_binding_configuration_fails_closed_for_foreign_connections() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            psycopg_errors.ForeignKeyViolation("foreign key violation"),
        ]
    )

    with pytest.raises(NotFoundError):
        role_binding_configuration.set_role_binding_configuration(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            role=WorkflowRole.PRODUCER,
            connection_id=uuid.uuid4(),
            configured_provider="provider-id-1",
            configured_model="model-id-x",
            role_prompt_override=None,
        )

    # The barriers ran and the composite-foreign-key rejection was classified
    # into the uniform not-found outcome: no probing oracle, no event.
    assert len(conn.executed) == 3
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


def test_set_role_binding_configuration_fails_closed_for_non_owners() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(uuid.uuid4())])

    with pytest.raises(NotFoundError):
        role_binding_configuration.set_role_binding_configuration(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
            connection_id=uuid.uuid4(),
            configured_provider="provider-id-1",
            configured_model="model-id-x",
            role_prompt_override=None,
        )

    # The barrier and ownership-gate reads ran: neither the write nor any
    # event insert.
    assert len(conn.executed) == 2
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


def test_set_role_binding_configuration_event_failure_rolls_back() -> None:
    """A failed event insertion propagates — the change never stands alone."""
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    updated = _binding_row(workspace_id=workspace_id, role="producer")
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            updated,
            RuntimeError("event insertion failed"),
        ]
    )

    with pytest.raises(RuntimeError, match="event insertion failed"):
        role_binding_configuration.set_role_binding_configuration(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            role=WorkflowRole.PRODUCER,
            connection_id=uuid.uuid4(),
            configured_provider="provider-id-1",
            configured_model="model-id-x",
            role_prompt_override=None,
        )

    assert "insert into openorc.workflow_events" in conn.executed[3][0]


def test_configuration_edits_never_touch_task_agent_sessions() -> None:
    # The non-interference contract (issue #162): a role-binding
    # configuration edit is live Workspace configuration. It can never
    # rewrite an initialized TaskAgentSession, its admitted Connection
    # boundary, or its historical effective configuration snapshot — the
    # whole composed flow contains no task_agent_sessions statement.
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    updated = _binding_row(workspace_id=workspace_id, role="producer")
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            updated,
            _event_row(actor_id=str(profile_id)),
        ]
    )

    role_binding_configuration.set_role_binding_configuration(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=uuid.uuid4(),
        configured_provider="provider-id-2",
        configured_model="model-id-y",
        role_prompt_override=None,
    )

    for sql, _params in conn.executed:
        assert "task_agent_sessions" not in sql
