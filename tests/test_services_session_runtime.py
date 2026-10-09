"""Deterministic tests for the TaskAgentSession runtime establishment,
readiness, and lifecycle services (issue #130).

Scripted ``AgentRuntimeAdapter`` doubles stand in for the runtime exactly as
the issue requires (fake-runtime integration coverage belongs to #69), and a
scripted fake connection seam proves the phase composition against canned
rows: the exact admitted-binding validation (idempotent READY, continuity and
terminal conflicts, disabled Connection, uniform not-found), the ONE logical
readiness handshake with the composed canonical #66 initialization, known
failure versus uncertain outcome, the recovery-required condition after known
external readiness, the conditional-write rejection classifications, the two
lifecycle records (genuine loss and explicit end), snapshot hygiene, and that
no database transaction is held across the runtime call. Durable concurrency
semantics of the underlying persistence primitives are already proven by the
existing TaskAgentSession integration suite and are not duplicated here.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from tests.fakes.agent_runtime import FakeAgentRuntimeAdapter, session_ready_candidate

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.observability import (
    TASK_ID,
    WORKFLOW_ROLE,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.protocol.initialization_assets import compose_initialization
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.session_runtime import (
    ConnectionNotEstablishableError,
    SessionContinuityConflictError,
    SessionEstablishmentRecoveryRequiredError,
    SessionTerminalConflictError,
    end_task_agent_session,
    establish_task_agent_session,
    record_task_agent_session_lost,
)

_OBSERVED = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
_BARRIER_ROW = (None, None, None)  # account-operational barrier: no attempt state

# One canned statement outcome: a row tuple, a row list, None (empty result),
# or an exception instance the seam raises instead of returning.
_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception


class FakeCursor:
    """Returns one canned result, like a psycopg cursor (row or row list)."""

    def __init__(self, result: tuple[Any, ...] | list[tuple[Any, ...]] | None) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._result is None:
            return None
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._result is None:
            return []
        if isinstance(self._result, list):
            return list(self._result)
        return [self._result]


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL
    and the connection/transaction lifecycle."""

    def __init__(self, results: Sequence[_CannedResult]) -> None:
        self.results: list[_CannedResult] = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.lifecycle: list[str] = []

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
        self.lifecycle.append("tx_enter")
        try:
            yield
        finally:
            self.lifecycle.append("tx_exit")


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        conn = self._conn

        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            conn.lifecycle.append("pool_enter")
            try:
                yield conn
            finally:
                conn.lifecycle.append("pool_exit")

        return managed()

    def close(self) -> None:
        raise AssertionError("session runtime service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


class ScriptedAgentRuntimeAdapter(AgentRuntimeAdapter):
    """A scripted ``AgentRuntimeAdapter`` double: it records every
    ``create_session`` invocation, replays queued results or normalized
    exceptions exactly once per call, and never performs any other runtime
    operation — establishment must not need one."""

    def __init__(self, outcomes: list[Any], *, lifecycle: list[str] | None = None) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[AgentSessionCreationRequest] = []
        self.lifecycle = lifecycle if lifecycle is not None else []

    def create_session(self, request: AgentSessionCreationRequest) -> AgentSessionCreated:
        self.calls.append(request)
        self.lifecycle.append("create_session")
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, AgentSessionCreated)
        return outcome

    def send(self, session_id: str, message: str, *, expected_family: str | None = None) -> Any:
        raise AssertionError("establishment never performs a second ordinary send")


def _ws_row(workspace_id: Any, owner_profile_id: Any, *, guidance: str = "") -> tuple[Any, ...]:
    return (
        workspace_id,
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        guidance,
    )


def _task_row(workspace_id: Any) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        uuid.uuid4(),
        42,
        42,
        "ready_to_plan",
        None,
        None,
        uuid.uuid4(),
        None,
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _binding_row(workspace_id: Any, role: str, connection_id: Any) -> tuple[Any, ...]:
    # Canned row matching the #162 binding columns: the scripted flows use
    # the unconfigured configuration state.
    return (
        uuid.uuid4(),
        workspace_id,
        role,
        connection_id,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _connection_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    enabled: bool = True,
    safe_config: Any = None,
) -> tuple[Any, ...]:
    return (
        connection_id,
        workspace_id,
        "cline",
        "primary cline hub",
        {} if safe_config is None else safe_config,
        1,
        enabled,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _session_row(
    workspace_id: Any,
    task_id: Any,
    role: str,
    connection_id: Any,
    *,
    row_id: Any = None,
    lifecycle_status: str = "connecting",
    external_session_id: str | None = None,
    initialized_at: Any = None,
    snapshot: Any = None,
    ended_at: Any = None,
    reported_provider: str | None = None,
    reported_model: str | None = None,
    reported_runtime_version: str | None = None,
) -> tuple[Any, ...]:
    return (
        row_id if row_id is not None else uuid.uuid4(),
        workspace_id,
        task_id,
        role,
        connection_id,
        external_session_id,
        lifecycle_status,
        snapshot if snapshot is not None else ({} if external_session_id is not None else None),
        reported_provider,
        reported_model,
        reported_runtime_version,
        initialized_at,
        ended_at if ended_at is not None else (_OBSERVED if lifecycle_status == "ended" else None),
        _OBSERVED,
        _OBSERVED,
    )


_PHASE1_LIFECYCLE = ["pool_enter", "pool_exit"] * 6
_FINALIZE_LIFECYCLE = [
    "pool_enter",
    "tx_enter",
    "tx_enter",
    "tx_exit",
    "tx_enter",
    "tx_exit",
    "tx_enter",
    "tx_exit",
    "tx_exit",
    "pool_exit",
]


def _establish_results(
    workspace_id: Any,
    owner_profile_id: Any,
    task_id: Any,
    connection_id: Any,
    *,
    binding_row: tuple[Any, ...],
    route_row: tuple[Any, ...],
    connection_row: tuple[Any, ...],
    guidance: str = "",
    finalize_results: list[_CannedResult] | None = None,
) -> ScriptedConnection:
    """Canned establish reads: the Phase-1 validation sequence, then the
    optional finalize-phase sequence (workspace, barrier, initialize write)."""
    results: list[_CannedResult] = [
        _ws_row(workspace_id, owner_profile_id, guidance=guidance),
        _BARRIER_ROW,
        _task_row(workspace_id),
        binding_row,
        route_row,
        connection_row,
    ]
    if finalize_results is not None:
        results.extend(finalize_results)
    return ScriptedConnection(results)


def _establish_adapter(
    conn_lifecycle: list[str],
    *,
    external_session_id: str = "ext-1",
) -> ScriptedAgentRuntimeAdapter:
    return ScriptedAgentRuntimeAdapter(
        [
            AgentSessionCreated(
                external_session_id=external_session_id,
                reported_provider="fake-provider",
                reported_model="fake-model",
                reported_runtime_version="fake-1.2.3",
            )
        ],
        lifecycle=conn_lifecycle,
    )


def _establish_kwargs(workspace_id: Any, task_id: Any) -> dict[str, Any]:
    return {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "role": WorkflowRole.PRODUCER,
    }


def _initialize_updates(conn: ScriptedConnection) -> list[tuple[str, tuple[Any, ...] | None]]:
    return [
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.task_agent_sessions set external_session_id" in sql
    ]


def test_successful_establishment_finalizes_ready_exactly_once() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    guidance = "Keep changes minimal and documented."
    assembled_snapshot = {
        "adapter": "cline",
        "configuration": {"hub_url_label": "primary", "max_turns": 40},
    }
    ready_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
        snapshot=assembled_snapshot,
        reported_provider="fake-provider",
        reported_model="fake-model",
        reported_runtime_version="fake-1.2.3",
    )
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(
            workspace_id, connection_id, safe_config=assembled_snapshot["configuration"]
        ),
        guidance=guidance,
        finalize_results=[_ws_row(workspace_id, owner_profile_id), _BARRIER_ROW, ready_row],
    )
    adapter = _establish_adapter(conn.lifecycle)

    session = establish_task_agent_session(
        _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
    )

    # The exact binding finalized READY with the exact external identity and
    # the provenance reported by the ready-session result.
    assert session.id == ready_row[0]
    assert session.connection_id == connection_id
    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id == "ext-1"
    assert session.initialized_at == _OBSERVED
    assert session.effective_config_snapshot == assembled_snapshot
    assert session.reported_provider == "fake-provider"
    assert session.reported_model == "fake-model"
    assert session.reported_runtime_version == "fake-1.2.3"

    # Exactly ONE runtime invocation: the #67 readiness handshake, carrying
    # the exact Task/role identity and the canonical #66 initialization
    # composed with the Workspace guidance. No second ordinary send.
    assert len(adapter.calls) == 1
    request = adapter.calls[0]
    assert request.workspace_id == workspace_id
    assert request.task_id == task_id
    assert request.role is WorkflowRole.PRODUCER
    assert request.initialization == compose_initialization("producer", guidance)

    # The durable finalize wrote exactly the created identity, the deliberate
    # non-secret snapshot, and the reported provenance.
    updates = _initialize_updates(conn)
    assert len(updates) == 1
    update_params = updates[0][1]
    assert update_params is not None
    assert update_params[0] == "ext-1"
    assert update_params[1].obj == assembled_snapshot
    assert update_params[2] == "fake-provider"
    assert update_params[3] == "fake-model"
    assert update_params[4] == "fake-1.2.3"
    assert update_params[5] == task_id
    assert update_params[6] == "producer"

    # No database transaction was held across the runtime call: the
    # create_session invocation sits strictly between the closed Phase-1
    # reads and the finalize transaction's connection checkout.
    assert conn.lifecycle == _PHASE1_LIFECYCLE + ["create_session"] + _FINALIZE_LIFECYCLE


def test_establishment_through_the_reusable_fake_agent_runtime() -> None:
    """The #69 fake runtime drives the genuine establish seam: the real #67
    create-session readiness contract finalizes a seeded CONNECTING binding
    READY with the external identity the fake itself returned."""
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    guidance = "Owner-authored guidance composed upstream into the initialization."
    ready_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ready",
        external_session_id="fake-session-1",
        initialized_at=_OBSERVED,
        snapshot={"adapter": "cline", "configuration": {}},
        reported_provider="fake-provider",
        reported_model="fake-model",
        reported_runtime_version="fake-1.2.3",
    )
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id, safe_config={}),
        guidance=guidance,
        finalize_results=[_ws_row(workspace_id, owner_profile_id), _BARRIER_ROW, ready_row],
    )
    adapter = FakeAgentRuntimeAdapter(
        reported_provider="fake-provider",
        reported_model="fake-model",
        reported_runtime_version="fake-1.2.3",
        lifecycle=conn.lifecycle,
    )
    adapter.queue_creation(session_ready_candidate())

    session = establish_task_agent_session(
        _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
    )

    # The genuine CONNECTING binding finalized READY with the exact external
    # identity the fake runtime established and returned; the identity was
    # never installed manually.
    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id == "fake-session-1"
    assert adapter.created_session_ids() == ("fake-session-1",)
    update_params = _initialize_updates(conn)[0][1]
    assert update_params is not None
    assert update_params[0] == "fake-session-1"
    assert update_params[2] == "fake-provider"
    assert update_params[3] == "fake-model"
    assert update_params[4] == "fake-1.2.3"

    # The fake received the exact #67 creation request: the same Task/role
    # identity and the canonical #66 initialization composed with the
    # Workspace guidance, delivered verbatim and never re-authored.
    assert len(adapter.creation_requests()) == 1
    request = adapter.creation_requests()[0]
    assert request.workspace_id == workspace_id
    assert request.task_id == task_id
    assert request.role is WorkflowRole.PRODUCER
    assert request.initialization == compose_initialization("producer", guidance)

    # Same seam shape as the ordinary establishment: no database transaction
    # is held across the runtime call.
    assert conn.lifecycle == _PHASE1_LIFECYCLE + ["create_session"] + _FINALIZE_LIFECYCLE


def test_effective_config_snapshot_excludes_secret_prompt_guidance_material() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    guidance = "Owner-authored guidance prose that must never be persisted."
    safe_config = {"hub_url_label": "primary", "max_turns": 40}
    ready_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
        snapshot={"adapter": "cline", "configuration": safe_config},
    )
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id, safe_config=safe_config),
        guidance=guidance,
        finalize_results=[_ws_row(workspace_id, owner_profile_id), _BARRIER_ROW, ready_row],
    )
    adapter = _establish_adapter(conn.lifecycle)

    establish_task_agent_session(_pool(conn), adapter, **_establish_kwargs(workspace_id, task_id))

    update_params = _initialize_updates(conn)[0][1]
    assert update_params is not None
    persisted = json.dumps(update_params[1].obj)
    # The snapshot is exactly the deliberate assembly: the adapter type and
    # the demonstrated non-secret Owner safe_config. Nothing else enters it —
    # no initialization Markdown, rendered schemas, Workspace guidance,
    # control/prompt bodies, transcripts, or credential material.
    assert update_params[1].obj == {"adapter": "cline", "configuration": safe_config}
    assert guidance not in persisted
    assert compose_initialization("producer", guidance) not in persisted
    assert "auth_reference" not in persisted
    assert "token" not in persisted


def test_already_ready_binding_is_idempotent_without_runtime_call() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-existing",
        initialized_at=_OBSERVED,
    )
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=ready_row,
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
    )
    adapter = _establish_adapter(conn.lifecycle)

    session = establish_task_agent_session(
        _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
    )

    assert session.id == ready_row[0]
    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id == "ext-existing"
    assert adapter.calls == []
    # No Connection read and no durable write happened at all.
    assert len(conn.executed) == 5
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)
    assert conn.lifecycle == ["pool_enter", "pool_exit"] * 5


def test_different_route_binding_is_a_continuity_conflict_before_any_runtime_call() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    bound_connection, repointed_connection = sorted((uuid.uuid4(), uuid.uuid4()))
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        bound_connection,
        binding_row=_session_row(workspace_id, task_id, "producer", bound_connection),
        route_row=_binding_row(workspace_id, "producer", repointed_connection),
        connection_row=_connection_row(workspace_id, bound_connection),
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(SessionContinuityConflictError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert adapter.calls == []
    # The conflict was decided from the route coherence check: the Connection
    # was never read and nothing was written.
    assert len(conn.executed) == 5


@pytest.mark.parametrize("terminal_status", ["lost", "ended"])
def test_terminal_binding_is_a_conflict_and_never_replaced(terminal_status: str) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(
            workspace_id,
            task_id,
            "producer",
            connection_id,
            lifecycle_status=terminal_status,
            external_session_id="ext-historical",
            initialized_at=_OBSERVED,
        ),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(SessionTerminalConflictError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert adapter.calls == []
    assert len(conn.executed) == 5


def test_disabled_connection_fails_closed_before_any_runtime_call() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id, enabled=False),
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(ConnectionNotEstablishableError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert adapter.calls == []
    assert len(conn.executed) == 6


@pytest.mark.parametrize(
    "failure",
    [
        AgentRuntimeUnavailableError("runtime unreachable"),
        AgentRuntimeConfigurationRejectedError("configuration rejected"),
        AgentRuntimeTransportError("response not extractable"),
        AgentRuntimeProtocolFailureError("readiness response failed the protocol contract"),
    ],
)
def test_known_creation_failure_leaves_binding_connecting(failure: Exception) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
    )
    adapter = ScriptedAgentRuntimeAdapter([failure], lifecycle=conn.lifecycle)

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    # A known failure is deliberately distinct from uncertainty.
    assert not isinstance(excinfo.value, ExternalOperationUncertainError)
    assert len(adapter.calls) == 1
    # The binding was never finalized: no READY write and no lifecycle change.
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)
    assert conn.lifecycle == _PHASE1_LIFECYCLE + ["create_session"]


@pytest.mark.parametrize(
    "uncertain",
    [
        AgentRuntimeTimeoutError("creation exceeded its deadline"),
        AgentRuntimeDeliveryUncertainError("delivery became unclassifiable"),
    ],
)
def test_uncertain_creation_outcome_remains_connecting_and_is_not_replayed(
    uncertain: Exception,
) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
    )
    adapter = ScriptedAgentRuntimeAdapter([uncertain], lifecycle=conn.lifecycle)

    with pytest.raises(ExternalOperationUncertainError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    # No blind replay: exactly one creation attempt, no finalize write, and
    # the binding stays CONNECTING.
    assert len(adapter.calls) == 1
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)
    assert conn.lifecycle == _PHASE1_LIFECYCLE + ["create_session"]


def test_persistence_failure_after_known_readiness_is_recovery_required() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        finalize_results=[
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            RuntimeError("driver failure during the READY write"),
        ],
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(SessionEstablishmentRecoveryRequiredError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    # The known external readiness is never denied and never replayed: the
    # create_session handshake happened exactly once, no second create.
    assert len(adapter.calls) == 1


def _reloaded_row(
    workspace_id: Any,
    task_id: Any,
    connection_id: Any,
    *,
    row_id: Any = None,
    lifecycle_status: str,
    external_session_id: str | None = None,
) -> tuple[Any, ...]:
    return _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        row_id=row_id,
        lifecycle_status=lifecycle_status,
        external_session_id=external_session_id,
        initialized_at=_OBSERVED if external_session_id is not None else None,
    )


def test_conditional_write_rejection_same_ready_identity_is_idempotent() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reloaded_row = _reloaded_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
    )
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        finalize_results=[
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            None,
            reloaded_row,
        ],
    )
    adapter = _establish_adapter(conn.lifecycle)

    session = establish_task_agent_session(
        _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
    )

    # A concurrent finalization of the SAME exact identity won the conditional
    # write: idempotent success, no rewrite, no second runtime call.
    assert session.id == reloaded_row[0]
    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id == "ext-1"
    assert len(adapter.calls) == 1


@pytest.mark.parametrize(
    "reloaded_status, reloaded_external_id, expected_error",
    [
        pytest.param(
            "ready", "ext-other", SessionContinuityConflictError, id="different-ready-identity"
        ),
        pytest.param("lost", "ext-1", SessionTerminalConflictError, id="lost-is-terminal"),
        pytest.param("ended", None, SessionTerminalConflictError, id="ended-is-terminal"),
        pytest.param(
            "connecting", None, ConflictError, id="unexpected-connecting-state-fails-closed"
        ),
    ],
)
def test_conditional_write_rejection_is_classified_from_the_reloaded_binding(
    reloaded_status: str,
    reloaded_external_id: str | None,
    expected_error: type[Exception],
) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reloaded_row = _reloaded_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status=reloaded_status,
        external_session_id=reloaded_external_id,
    )
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        finalize_results=[
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            None,
            reloaded_row,
        ],
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(expected_error):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    # The rejection classification never re-invokes the runtime.
    assert len(adapter.calls) == 1


def test_conditional_write_rejection_missing_or_mis_scoped_fails_closed() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    # The reload reports a binding of a different Workspace: the uniform
    # not-found outcome, never a stale pre-image returned as fact.
    mis_scoped_row = _session_row(uuid.uuid4(), uuid.uuid4(), "producer", uuid.uuid4())
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        finalize_results=[
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            None,
            mis_scoped_row,
        ],
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(NotFoundError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert len(adapter.calls) == 1


def test_conditional_write_rejection_with_missing_reload_fails_closed() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _establish_results(
        workspace_id,
        owner_profile_id,
        task_id,
        connection_id,
        binding_row=_session_row(workspace_id, task_id, "producer", connection_id),
        route_row=_binding_row(workspace_id, "producer", connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        finalize_results=[_ws_row(workspace_id, owner_profile_id), _BARRIER_ROW, None, None],
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(NotFoundError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert len(adapter.calls) == 1


@pytest.mark.parametrize(
    "case",
    ["missing-workspace", "missing-task", "missing-binding", "missing-route", "missing-connection"],
)
def test_establish_scope_gaps_fail_closed_with_uniform_not_found(case: str) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    binding_row = _session_row(workspace_id, task_id, "producer", connection_id)
    route_row = _binding_row(workspace_id, "producer", connection_id)
    by_case: dict[str, list[_CannedResult]] = {
        "missing-workspace": [None],
        "missing-task": [_ws_row(workspace_id, owner_profile_id), _BARRIER_ROW, None],
        "missing-binding": [
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            _task_row(workspace_id),
            None,
        ],
        "missing-route": [
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            _task_row(workspace_id),
            binding_row,
            None,
        ],
        "missing-connection": [
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            _task_row(workspace_id),
            binding_row,
            route_row,
            None,
        ],
    }
    conn = ScriptedConnection(by_case[case])
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(NotFoundError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert adapter.calls == []


def test_establish_cross_workspace_task_or_binding_fails_closed() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    # The binding's durable scope disagrees with the addressed Workspace:
    # uniform not-found, never a cross-Workspace oracle.
    foreign_binding = _session_row(uuid.uuid4(), uuid.uuid4(), "producer", connection_id)
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            _task_row(workspace_id),
            foreign_binding,
        ]
    )
    adapter = _establish_adapter(conn.lifecycle)

    with pytest.raises(NotFoundError):
        establish_task_agent_session(
            _pool(conn), adapter, **_establish_kwargs(workspace_id, task_id)
        )

    assert adapter.calls == []


_LOSS_LIFECYCLE = [
    "pool_enter",
    "tx_enter",
    "tx_enter",
    "tx_exit",
    "tx_enter",
    "tx_exit",
    "tx_enter",
    "tx_exit",
    "tx_enter",
    "tx_exit",
    "tx_exit",
    "pool_exit",
]


def _loss_results(
    workspace_id: Any,
    owner_profile_id: Any,
    task_id: Any,
    *,
    binding_row: tuple[Any, ...],
    write_results: list[_CannedResult] | None = None,
) -> ScriptedConnection:
    results: list[_CannedResult] = [
        _ws_row(workspace_id, owner_profile_id),
        _task_row(workspace_id),
        binding_row,
    ]
    if write_results is not None:
        results.extend(write_results)
    return ScriptedConnection(results)


def _loss_kwargs(workspace_id: Any, task_id: Any) -> dict[str, Any]:
    return {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "role": WorkflowRole.PRODUCER,
        "evidence": AgentSessionNotFoundError("the exact addressed session is gone"),
    }


def _lost_updates(conn: ScriptedConnection) -> list[tuple[str, tuple[Any, ...] | None]]:
    return [
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.task_agent_sessions set lifecycle_status = 'lost'" in sql
    ]


def test_genuine_loss_records_lost_on_the_same_binding() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    binding_id = uuid.uuid4()
    ready_row = _reloaded_row(
        workspace_id,
        task_id,
        connection_id,
        row_id=binding_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
    )
    lost_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        row_id=binding_id,
        lifecycle_status="lost",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
    )
    conn = _loss_results(
        workspace_id,
        owner_profile_id,
        task_id,
        binding_row=ready_row,
        write_results=[lost_row],
    )

    session = record_task_agent_session_lost(_pool(conn), **_loss_kwargs(workspace_id, task_id))

    # LOST is lifecycle history on the SAME binding: same identity, same
    # Connection, initialization facts preserved, no successor session.
    assert session.id == ready_row[0]
    assert session.lifecycle_status is TaskSessionLifecycleStatus.LOST
    assert session.external_session_id == "ext-1"
    assert session.connection_id == connection_id
    assert len(_lost_updates(conn)) == 1
    assert conn.lifecycle == _LOSS_LIFECYCLE


def test_loss_on_already_lost_binding_is_idempotent() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    lost_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="lost",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
    )
    conn = _loss_results(workspace_id, owner_profile_id, task_id, binding_row=lost_row)

    session = record_task_agent_session_lost(_pool(conn), **_loss_kwargs(workspace_id, task_id))

    assert session.id == lost_row[0]
    assert session.lifecycle_status is TaskSessionLifecycleStatus.LOST
    assert _lost_updates(conn) == []


@pytest.mark.parametrize("unready_status", ["connecting", "ended"])
def test_loss_on_connecting_or_ended_binding_is_a_conflict(unready_status: str) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _loss_results(
        workspace_id,
        owner_profile_id,
        task_id,
        binding_row=_session_row(
            workspace_id, task_id, "producer", connection_id, lifecycle_status=unready_status
        ),
    )

    with pytest.raises(ConflictError):
        record_task_agent_session_lost(_pool(conn), **_loss_kwargs(workspace_id, task_id))

    assert _lost_updates(conn) == []


def test_loss_concurrent_transition_reloads_and_classifies() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _reloaded_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
    )
    # A concurrent transition committed between the classification read and
    # the conditional write; the reload reports the binding already LOST.
    lost_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="lost",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
    )
    conn = _loss_results(
        workspace_id,
        owner_profile_id,
        task_id,
        binding_row=ready_row,
        write_results=[None, lost_row],
    )

    session = record_task_agent_session_lost(_pool(conn), **_loss_kwargs(workspace_id, task_id))

    assert session.lifecycle_status is TaskSessionLifecycleStatus.LOST
    assert len(_lost_updates(conn)) == 1


def test_loss_concurrent_non_loss_transition_fails_closed() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _reloaded_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
    )
    # The concurrent transition ended the binding: the stale loss record is
    # rejected, never applied to newer state.
    ended_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ended",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
    )
    conn = _loss_results(
        workspace_id,
        owner_profile_id,
        task_id,
        binding_row=ready_row,
        write_results=[None, ended_row],
    )

    with pytest.raises(ConflictError):
        record_task_agent_session_lost(_pool(conn), **_loss_kwargs(workspace_id, task_id))

    assert len(_lost_updates(conn)) == 1


@pytest.mark.parametrize(
    "non_loss_evidence",
    [
        AgentRuntimeUnavailableError("runtime unreachable"),
        AgentRuntimeTimeoutError("operation timed out"),
        AgentRuntimeConfigurationRejectedError("configuration rejected"),
        "not-an-exception",
        None,
    ],
)
def test_non_loss_evidence_is_an_invalid_command_without_database_access(
    non_loss_evidence: Any,
) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        record_task_agent_session_lost(
            _pool(conn),
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            evidence=non_loss_evidence,
        )

    # Unavailability never became loss and no database access happened.
    assert conn.executed == []


@pytest.mark.parametrize("case", ["missing-workspace", "missing-task", "missing-binding"])
def test_loss_scope_gaps_fail_closed_with_uniform_not_found(case: str) -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    by_case: dict[str, list[_CannedResult]] = {
        "missing-workspace": [None],
        "missing-task": [_ws_row(workspace_id, owner_profile_id), None],
        "missing-binding": [
            _ws_row(workspace_id, owner_profile_id),
            _task_row(workspace_id),
            None,
        ],
    }
    conn = ScriptedConnection(by_case[case])

    with pytest.raises(NotFoundError):
        record_task_agent_session_lost(_pool(conn), **_loss_kwargs(workspace_id, task_id))


def test_end_connecting_binding_stamps_ended_without_bound_identity() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connecting_row = _session_row(workspace_id, task_id, "producer", connection_id)
    ended_row = _session_row(
        workspace_id, task_id, "producer", connection_id, lifecycle_status="ended"
    )
    conn = _loss_results(
        workspace_id,
        owner_profile_id,
        task_id,
        binding_row=connecting_row,
        write_results=[ended_row],
    )

    session = end_task_agent_session(
        _pool(conn),
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
    )

    # The establishment attempt ended before ever binding an external
    # session: the binding stays valid with a NULL identity.
    assert session.id == ended_row[0]
    assert session.lifecycle_status is TaskSessionLifecycleStatus.ENDED
    assert session.external_session_id is None
    assert session.ended_at == _OBSERVED
    assert any(
        "update openorc.task_agent_sessions set lifecycle_status = 'ended'" in sql
        for sql, _ in conn.executed
    )


def test_end_ready_binding_preserves_initialization_history() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _reloaded_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
    )
    ended_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ended",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
    )
    conn = _loss_results(
        workspace_id,
        owner_profile_id,
        task_id,
        binding_row=ready_row,
        write_results=[ended_row],
    )

    session = end_task_agent_session(
        _pool(conn),
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
    )

    # History is preserved: the immutable external session identity and the
    # initialization facts survive the normal end.
    assert session.lifecycle_status is TaskSessionLifecycleStatus.ENDED
    assert session.external_session_id == "ext-1"
    assert session.initialized_at == _OBSERVED
    assert session.ended_at == _OBSERVED


def test_end_already_ended_binding_is_idempotent() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ended_row = _session_row(
        workspace_id, task_id, "producer", connection_id, lifecycle_status="ended"
    )
    conn = _loss_results(workspace_id, owner_profile_id, task_id, binding_row=ended_row)

    session = end_task_agent_session(
        _pool(conn),
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
    )

    assert session.id == ended_row[0]
    assert session.lifecycle_status is TaskSessionLifecycleStatus.ENDED
    assert not any(
        "update openorc.task_agent_sessions set lifecycle_status = 'ended'" in sql
        for sql, _ in conn.executed
    )


def test_end_lost_binding_is_a_conflict() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    lost_row = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="lost",
        external_session_id="ext-1",
        initialized_at=_OBSERVED,
    )
    conn = _loss_results(workspace_id, owner_profile_id, task_id, binding_row=lost_row)

    with pytest.raises(ConflictError):
        end_task_agent_session(
            _pool(conn),
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
        )

    assert not any(
        "update openorc.task_agent_sessions set lifecycle_status = 'ended'" in sql
        for sql, _ in conn.executed
    )


def test_end_missing_binding_fails_closed_with_uniform_not_found() -> None:
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection(
        [_ws_row(workspace_id, owner_profile_id), _task_row(workspace_id), None]
    )

    with pytest.raises(NotFoundError):
        end_task_agent_session(
            _pool(conn),
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
        )


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


@pytest.mark.parametrize("field_name", ["workspace_id", "task_id"])
@pytest.mark.parametrize("malformed", ["not-a-uuid", 123, None])
def test_establish_malformed_identifiers_fail_inside_the_span_without_database_access(
    field_name: str, malformed: Any
) -> None:
    provider, exporter = _local_provider_with_exporter()
    kwargs = _establish_kwargs(uuid.uuid4(), uuid.uuid4())
    kwargs[field_name] = malformed
    conn = ScriptedConnection([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        establish_task_agent_session(_pool(conn), ScriptedAgentRuntimeAdapter([]), **kwargs)

    assert conn.executed == []
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "session_runtime.establish_task_agent_session"
    assert exported.status is not None
    assert exported.status.status_code is StatusCode.ERROR
    assert exported.status.description == "InvalidCommandError"
    # Validation ran before annotation: no identifier attributes were attached.
    attributes = exported.attributes or {}
    assert WORKSPACE_ID not in attributes
    assert TASK_ID not in attributes
    if malformed is not None:
        assert str(malformed) not in str(attributes)


@pytest.mark.parametrize("malformed_role", ["producer", 7, None])
def test_establish_malformed_role_fails_inside_the_span_without_database_access(
    malformed_role: Any,
) -> None:
    provider, exporter = _local_provider_with_exporter()
    conn = ScriptedConnection([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        establish_task_agent_session(
            _pool(conn),
            ScriptedAgentRuntimeAdapter([]),
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            role=malformed_role,
        )

    assert conn.executed == []
    (exported,) = exporter.get_finished_spans()
    assert exported.status is not None
    assert exported.status.status_code is StatusCode.ERROR
    attributes = exported.attributes or {}
    assert WORKFLOW_ROLE not in attributes


def test_establish_non_adapter_runtime_is_an_invalid_command() -> None:
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        establish_task_agent_session(
            _pool(conn),
            "not-an-adapter",  # type: ignore[arg-type]
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
        )

    assert conn.executed == []


@pytest.mark.parametrize(
    "malformed_evidence", [AgentRuntimeUnavailableError("runtime unreachable"), "text", None]
)
def test_loss_malformed_evidence_fails_inside_the_span_without_database_access(
    malformed_evidence: Any,
) -> None:
    provider, exporter = _local_provider_with_exporter()
    conn = ScriptedConnection([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        record_task_agent_session_lost(
            _pool(conn),
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
            evidence=malformed_evidence,
        )

    assert conn.executed == []
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "session_runtime.record_task_agent_session_lost"
    assert exported.status is not None
    assert exported.status.status_code is StatusCode.ERROR
    attributes = exported.attributes or {}
    assert WORKFLOW_ROLE not in attributes
    assert WORKSPACE_ID not in attributes


@pytest.mark.parametrize("malformed_role", ["reviewer", 7, None])
def test_end_malformed_command_fails_inside_the_span_without_database_access(
    malformed_role: Any,
) -> None:
    provider, exporter = _local_provider_with_exporter()
    conn = ScriptedConnection([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        end_task_agent_session(
            _pool(conn),
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            role=malformed_role,
        )

    assert conn.executed == []
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "session_runtime.end_task_agent_session"
    assert exported.status is not None
    assert exported.status.status_code is StatusCode.ERROR
