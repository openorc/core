"""Deterministic tests for the Connection-backed Cline SDK attachment
composition (issue #190, D8b).

A scripted fake connection seam proves the composition against canned durable
rows and the #55 credential/Vault reads; an injectable backend factory
supplies the ``FakeClineSdkBackend`` so every backend operation is recorded
and scripted. The suite proves: fresh and bound routing on the exact admitted
binding (with the one-selection handoff to ``establish_task_agent_session``),
the pre-dial refusal matrix with zero backend connects, the real composition
seam (typed ``ClineRemoteConfig``, wired durable resolver, successful adapter
read, rotation refresh, bounded dispose), the preserved failure taxonomy
(rejected configuration / unavailable attachment / uncertain outcome), and
that secrets, endpoint material, and native error text never reach
exceptions, causes, logs, or spans. The adapter's own contract and recovery
matrices, the resolver's, the credential service's, and the bridge process's
are proven by their existing suites and are not duplicated here.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.cline import (
    ClineAdapter,
    ClineBridgeProtocolError,
)
from openorc.adapters.cline.activity import ClineActivitySnapshot
from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
)
from openorc.adapters.cline.values import ClineRemoteConfig, ClineStartResult
from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.observability import (
    CONNECTION_ID,
    OPERATION,
    TASK_ID,
    WORKFLOW_ROLE,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence import runtime_control_secrets
from openorc.persistence.pool import DatabasePool
from openorc.services.cline_attachment import (
    OPENORC_CLIENT_IDENTITY,
    ClineAttachmentPhase,
    _require_attachable_connection,
    attach_cline_runtime,
)
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.session_runtime import establish_task_agent_session
from tests.fakes.cline_sdk_backend import FakeClineSdkBackend

_OBSERVED = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
_BARRIER_ROW = (None, None, None)  # account-operational barrier: no attempt state

_SECRET = "s3cr3t-credential-value"
_ROTATED_SECRET = "r0t4ted-credential-value"
_ENDPOINT = "ws://hub.example.test:25463/hub"
_SELECTED_BASE = "/srv/selected/repositories"
_ORIGINAL_BASE = "/srv/original/repositories"
_NEUTRAL_DIRECTORY = "/srv/neutral/review"

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
        raise AssertionError("attachment composition tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


class _ScriptedBackendFactory:
    """Counts constructions and hands back the scripted backends in order."""

    def __init__(self, *backends: FakeClineSdkBackend) -> None:
        self._backends = list(backends)
        self.constructed = 0

    def __call__(self) -> FakeClineSdkBackend:
        self.constructed += 1
        return self._backends.pop(0)


# --- canned durable rows ---------------------------------------------------


def _binding_row(
    workspace_id: Any,
    task_id: Any,
    connection_id: Any,
    *,
    role: str = "producer",
    lifecycle_status: str = "connecting",
    external_session_id: str | None = None,
    snapshot: Any = None,
) -> tuple[Any, ...]:
    """One canned TaskAgentSession row (domain-valid for the given lifecycle)."""
    return (
        uuid.uuid4(),
        workspace_id,
        task_id,
        role,
        connection_id,
        external_session_id,
        lifecycle_status,
        snapshot if snapshot is not None else ({} if external_session_id is not None else None),
        None,
        None,
        None,
        _OBSERVED if external_session_id is not None else None,
        _OBSERVED if lifecycle_status == "ended" else None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _connection_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    enabled: bool = True,
    safe_config: Any = None,
    auth_reference: Any = None,
    adapter_type: str = "cline",
) -> tuple[Any, ...]:
    return (
        connection_id,
        workspace_id,
        adapter_type,
        "primary cline hub",
        {} if safe_config is None else safe_config,
        1,
        enabled,
        auth_reference,
        _OBSERVED,
        _OBSERVED,
    )


def _route_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    role: str = "producer",
    provider: str | None = None,
    model: str | None = None,
    override: str | None = None,
) -> tuple[Any, ...]:
    """One canned WorkflowRoleBinding row (unconfigured by default)."""
    return (
        uuid.uuid4(),
        workspace_id,
        role,
        connection_id,
        provider,
        model,
        override,
        _OBSERVED,
        _OBSERVED,
    )


def _ws_row(workspace_id: Any, owner_profile_id: Any, *, guidance: str = "") -> tuple[Any, ...]:
    return (
        workspace_id,
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        guidance,
        None,
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
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _selected_config(endpoint: Any = _ENDPOINT, base: Any = _SELECTED_BASE) -> dict[str, Any]:
    return {"repository_base": base, "control_endpoint": endpoint}


def _auth_reference(secret_id: Any) -> str:
    return runtime_control_secrets.encode_connection_auth_reference(secret_id)


def _attach_kwargs(
    workspace_id: Any,
    task_id: Any,
    *,
    role: Any = WorkflowRole.PRODUCER,
    phase: Any = ClineAttachmentPhase.FRESH,
    external_session_id: Any = None,
    working_directory: Any = None,
    backend_factory: Any = None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "role": role,
        "phase": phase,
    }
    if external_session_id is not None:
        kwargs["external_session_id"] = external_session_id
    if working_directory is not None:
        kwargs["working_directory"] = working_directory
    if backend_factory is not None:
        kwargs["backend_factory"] = backend_factory
    return kwargs


# --- fresh and bound routing -----------------------------------------------


def test_fresh_producer_attachment_selects_one_configuration_and_agrees_with_establishment() -> (
    None
):
    owner_profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    # The composition's one selection is the admitted Connection's current
    # non-secret safe_config at composition time; the durable rows a later
    # establishment re-reads carry a still-newer configuration that must
    # never substitute for the selection.
    selected = _selected_config()
    later = {
        "repository_base": "/srv/later/base",
        "control_endpoint": "ws://later.example.test:9/hub",
    }
    ready_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="cline-session-1",
        snapshot={"adapter": "cline", "configuration": selected},
    )
    conn = ScriptedConnection(
        [
            # Composition: binding, connection validation, credential + Vault.
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            (_SECRET,),
            # Fresh establishment Phase 1 over the LATER durable rows:
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            _task_row(workspace_id),
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id, connection_id, safe_config=later, auth_reference=reference
            ),
            # The resolver's fresh reads inside the readiness handshake:
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id, connection_id, safe_config=later, auth_reference=reference
            ),
            _route_row(
                workspace_id,
                connection_id,
                provider="configured-provider",
                model="configured-model",
            ),
            # READY finalization:
            _ws_row(workspace_id, owner_profile_id),
            _BARRIER_ROW,
            ready_row,
        ]
    )
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send(
        "cline-session-1",
        {
            "finishReason": "completed",
            "text": json.dumps({"type": "session_ready", "schema_version": 1, "status": "READY"}),
        },
    )
    factory = _ScriptedBackendFactory(backend)

    with attach_cline_runtime(
        _pool(conn),
        **_attach_kwargs(
            workspace_id,
            task_id,
            phase=ClineAttachmentPhase.FRESH,
            backend_factory=factory,
        ),
    ) as attached:
        # The composition held no database transaction across its reads.
        assert "tx_enter" not in conn.lifecycle
        assert attached.selected_configuration == selected
        assert attached.selected_configuration is not None
        assert isinstance(attached.adapter, ClineAdapter)

        session = establish_task_agent_session(
            _pool(conn),
            attached.adapter,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            effective_configuration=attached.selected_configuration,
        )

    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id == "cline-session-1"

    (connect_call,) = [call for call in backend.calls() if call.operation == "connect"]
    assert connect_call.remote is not None
    assert connect_call.remote.endpoint == _ENDPOINT
    assert connect_call.remote.client_identity == OPENORC_CLIENT_IDENTITY
    assert connect_call.remote.auth_token == _SECRET
    assert _SECRET not in repr(connect_call.remote)

    (start_call,) = [call for call in backend.calls() if call.operation == "start"]
    assert start_call.request is not None
    # The fresh construction ran at the SELECTED base, never the later one.
    assert start_call.request.cwd == _SELECTED_BASE
    assert start_call.request.workspace_root == _SELECTED_BASE

    # The durable READY snapshot records exactly the selection.
    updates = [
        params
        for sql, params in conn.executed
        if "update openorc.task_agent_sessions set external_session_id" in sql
    ]
    (update_params,) = updates
    assert update_params is not None
    assert update_params[1].obj == {"adapter": "cline", "configuration": selected}


def test_bound_producer_attachment_keeps_the_snapshot_base_and_exact_identity() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    original_snapshot = {
        "adapter": "cline",
        "configuration": _selected_config(base=_ORIGINAL_BASE),
    }
    edited = {
        "repository_base": "/srv/edited/base",
        "control_endpoint": "ws://edited.example.test:7/hub",
    }
    binding = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        snapshot=original_snapshot,
    )
    connection_row = _connection_row(
        workspace_id, connection_id, safe_config=edited, auth_reference=reference
    )
    route_row = _route_row(
        workspace_id, connection_id, provider="configured-provider", model="configured-model"
    )
    conn = ScriptedConnection(
        [
            # Composition: binding, connection validation, credential + Vault.
            binding,
            connection_row,
            connection_row,
            (_SECRET,),
            # The wired resolver's bound resolution:
            [binding],
            connection_row,
            route_row,
            # A successful adapter observation through the attached backend:
            [binding],
            connection_row,
            route_row,
        ]
    )
    backend = FakeClineSdkBackend()
    backend.queue_get_accumulated_usage("ext-1", {"usage": None, "aggregateUsage": None})
    factory = _ScriptedBackendFactory(backend)

    with attach_cline_runtime(
        _pool(conn),
        **_attach_kwargs(
            workspace_id,
            task_id,
            phase=ClineAttachmentPhase.BOUND,
            external_session_id="ext-1",
            working_directory=_ORIGINAL_BASE,
            backend_factory=factory,
        ),
    ) as attached:
        # The bound path returns no selection: the durable READY snapshot is
        # already authoritative.
        assert attached.selected_configuration is None
        # The wired durable resolver recovers the ORIGINAL snapshot base —
        # never the since-edited Connection configuration.
        context = attached.adapter._construction_contexts.resolve_bound_session_context("ext-1")
        assert context is not None
        assert context.cwd == _ORIGINAL_BASE
        assert context.workspace_root == _ORIGINAL_BASE
        # A successful adapter observation through the attached backend:
        snapshot = attached.adapter.read_activity_snapshot("ext-1")
        assert isinstance(snapshot, ClineActivitySnapshot)
        assert snapshot.usage is None
        assert snapshot.aggregate_usage is None

    (connect_call,) = [call for call in backend.calls() if call.operation == "connect"]
    assert connect_call.remote is not None
    # The attachment dialed the admitted Connection's CURRENT endpoint.
    assert connect_call.remote.endpoint == "ws://edited.example.test:7/hub"
    assert connect_call.remote.auth_token == _SECRET


def test_fresh_reviewer_attachment_uses_the_supplied_neutral_directory() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    # A Reviewer Connection needs no repository base at all.
    selected = {"control_endpoint": _ENDPOINT}
    conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id, role="reviewer"),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            (_SECRET,),
            # The resolver's fresh reads:
            _binding_row(workspace_id, task_id, connection_id, role="reviewer"),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            _route_row(
                workspace_id,
                connection_id,
                role="reviewer",
                provider="configured-provider",
                model="configured-model",
            ),
        ]
    )
    backend = FakeClineSdkBackend()
    factory = _ScriptedBackendFactory(backend)

    with attach_cline_runtime(
        _pool(conn),
        **_attach_kwargs(
            workspace_id,
            task_id,
            role=WorkflowRole.REVIEWER,
            phase=ClineAttachmentPhase.FRESH,
            working_directory=_NEUTRAL_DIRECTORY,
            backend_factory=factory,
        ),
    ) as attached:
        assert attached.selected_configuration == selected
        context = attached.adapter._construction_contexts.resolve_construction_context(
            workspace_id, task_id, WorkflowRole.REVIEWER
        )
        assert context.cwd == _NEUTRAL_DIRECTORY
        assert context.workspace_root == _NEUTRAL_DIRECTORY

    (connect_call,) = [call for call in backend.calls() if call.operation == "connect"]
    assert connect_call.remote is not None
    assert connect_call.remote.endpoint == _ENDPOINT


def test_a_changed_role_route_never_redirects_the_attachment() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            (_SECRET,),
        ]
    )
    backend = FakeClineSdkBackend()
    factory = _ScriptedBackendFactory(backend)

    with attach_cline_runtime(
        _pool(conn),
        **_attach_kwargs(workspace_id, task_id, backend_factory=factory),
    ):
        pass

    # Only the four composition reads ran, and the mutable role-binding
    # route was never consulted for routing.
    assert len(conn.executed) == 4
    assert all("workflow_role_bindings" not in sql for sql, _ in conn.executed)
    (connect_call,) = [call for call in backend.calls() if call.operation == "connect"]
    assert connect_call.remote is not None
    assert connect_call.remote.endpoint == _ENDPOINT


# --- pre-dial refusals ------------------------------------------------------


def _refusal_script(
    mode: str,
    *,
    workspace_id: Any,
    task_id: Any,
    connection_id: Any,
) -> tuple[list[_CannedResult], dict[str, Any], type[Exception], int]:
    """Build one pre-dial refusal: script, invocation overrides, error, reads.

    The read count proves where the composition stopped: no refused
    invocation ever reaches the backend, and the credential/Vault reads
    never run for subjects refused before them.
    """
    reference = _auth_reference(uuid.uuid4())
    good_connection = _connection_row(
        workspace_id, connection_id, safe_config=_selected_config(), auth_reference=reference
    )
    binding = _binding_row(workspace_id, task_id, connection_id)
    ready = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        snapshot={"adapter": "cline", "configuration": _selected_config()},
    )
    if mode == "missing-binding":
        return [None], {}, NotFoundError, 1
    if mode == "cross-workspace-binding":
        return [_binding_row(uuid.uuid4(), task_id, connection_id)], {}, NotFoundError, 1
    if mode == "terminal-binding":
        lost = _binding_row(
            workspace_id,
            task_id,
            connection_id,
            lifecycle_status="lost",
            external_session_id="ext-1",
            snapshot={"adapter": "cline", "configuration": _selected_config()},
        )
        return [lost], {}, ConflictError, 1
    if mode == "already-established":
        return [ready], {}, ConflictError, 1
    if mode == "bound-on-connecting":
        return (
            [binding],
            {"phase": ClineAttachmentPhase.BOUND, "external_session_id": "ext-1"},
            ConflictError,
            1,
        )
    if mode == "mismatched-external-identity":
        return (
            [ready],
            {"phase": ClineAttachmentPhase.BOUND, "external_session_id": "ext-2"},
            ConflictError,
            1,
        )
    if mode == "missing-connection":
        return [binding, None], {}, NotFoundError, 2
    if mode == "disabled-connection":
        return (
            [binding, _connection_row(workspace_id, connection_id, enabled=False)],
            {},
            ConflictError,
            2,
        )
    if mode == "missing-endpoint":
        return (
            [
                binding,
                good_connection,
                _connection_row(
                    workspace_id,
                    connection_id,
                    safe_config={"repository_base": _SELECTED_BASE},
                    auth_reference=reference,
                ),
                (_SECRET,),
            ],
            {},
            ConflictError,
            4,
        )
    if mode == "malformed-endpoint":
        malformed = {
            "repository_base": _SELECTED_BASE,
            "control_endpoint": "https://not-a-websocket.example.test/hub",
        }
        return (
            [
                binding,
                good_connection,
                _connection_row(
                    workspace_id, connection_id, safe_config=malformed, auth_reference=reference
                ),
                (_SECRET,),
            ],
            {},
            ConflictError,
            4,
        )
    if mode == "missing-credential":
        return (
            [
                binding,
                good_connection,
                _connection_row(workspace_id, connection_id, safe_config=_selected_config()),
            ],
            {},
            ConflictError,
            3,
        )
    if mode == "malformed-credential-reference":
        return (
            [
                binding,
                good_connection,
                _connection_row(
                    workspace_id,
                    connection_id,
                    safe_config=_selected_config(),
                    auth_reference="garbage-reference",
                ),
            ],
            {},
            ConflictError,
            3,
        )
    if mode == "dangling-credential":
        return [binding, good_connection, good_connection, None], {}, ConflictError, 4
    raise AssertionError(mode)


@pytest.mark.parametrize(
    "mode",
    [
        "missing-binding",
        "cross-workspace-binding",
        "terminal-binding",
        "already-established",
        "bound-on-connecting",
        "mismatched-external-identity",
        "missing-connection",
        "disabled-connection",
        "missing-endpoint",
        "malformed-endpoint",
        "missing-credential",
        "malformed-credential-reference",
        "dangling-credential",
    ],
)
def test_known_pre_dial_refusals_fail_closed_before_any_backend_connect(mode: str) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    results, overrides, expected_error, expected_reads = _refusal_script(
        mode, workspace_id=workspace_id, task_id=task_id, connection_id=connection_id
    )
    conn = ScriptedConnection(results)
    factory = _ScriptedBackendFactory(FakeClineSdkBackend())

    with (
        pytest.raises(expected_error),
        attach_cline_runtime(
            _pool(conn),
            **_attach_kwargs(workspace_id, task_id, backend_factory=factory, **overrides),
        ),
    ):
        raise AssertionError("the lease body must never run for a refusal")

    assert factory.constructed == 0
    assert len(conn.executed) == expected_reads
    if expected_reads < 4:
        assert all("vault.decrypted_secrets" not in sql for sql, _ in conn.executed)


def test_the_attachable_connection_guard_rejects_non_cline_connections() -> None:
    """The v1 schema admits only Cline Connections; the guard stays defensive.

    A non-Cline adapter type cannot currently exist in durable state, so the
    refusal is exercised directly against the guard with a shape the schema
    cannot admit.
    """
    connection = SimpleNamespace(workspace_id=uuid.uuid4(), adapter="not-a-runtime", enabled=True)

    with pytest.raises(ConflictError):
        _require_attachable_connection(
            connection,  # type: ignore[arg-type]
            workspace_id=connection.workspace_id,
        )


# --- malformed commands -----------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"workspace_id": "not-a-uuid"},
        {"task_id": None},
        {"role": "producer"},
        {"phase": "fresh"},
        {"phase": ClineAttachmentPhase.FRESH, "external_session_id": "ext-1"},
        {"phase": ClineAttachmentPhase.BOUND},
        {"phase": ClineAttachmentPhase.BOUND, "external_session_id": "   "},
        {"working_directory": 42},
        {"working_directory": "   "},
    ],
)
def test_malformed_commands_are_rejected_before_any_read(overrides: dict[str, Any]) -> None:
    conn = ScriptedConnection([])
    factory = _ScriptedBackendFactory(FakeClineSdkBackend())
    kwargs: dict[str, Any] = {
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "role": WorkflowRole.PRODUCER,
        "phase": ClineAttachmentPhase.FRESH,
        "backend_factory": factory,
        **overrides,
    }

    with pytest.raises(InvalidCommandError), attach_cline_runtime(_pool(conn), **kwargs):
        raise AssertionError("the lease body must never run for a malformed command")

    assert conn.executed == []
    assert factory.constructed == 0


# --- attachment failure taxonomy --------------------------------------------


@pytest.mark.parametrize(
    ("scripted", "expected_error", "expected_cause"),
    [
        (
            ClineRemoteAttachmentRejectedError(),
            ExternalOperationFailedError,
            AgentRuntimeConfigurationRejectedError,
        ),
        (
            ClineRemoteAttachmentUnavailableError(),
            ExternalOperationFailedError,
            AgentRuntimeUnavailableError,
        ),
        (
            ClineBridgeProtocolError("not_attached"),
            ExternalOperationFailedError,
            AgentRuntimeUnavailableError,
        ),
        (
            ClineBackendUncertainOutcomeError(),
            ExternalOperationUncertainError,
            AgentRuntimeDeliveryUncertainError,
        ),
    ],
)
def test_attachment_failures_preserve_the_normalized_taxonomy(
    scripted: Any,
    expected_error: type[Exception],
    expected_cause: type[Exception],
) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            (_SECRET,),
        ]
    )
    backend = FakeClineSdkBackend()
    backend.queue_connect(scripted)
    factory = _ScriptedBackendFactory(backend)
    body_ran = False

    with (
        pytest.raises(expected_error) as excinfo,
        attach_cline_runtime(
            _pool(conn),
            **_attach_kwargs(workspace_id, task_id, backend_factory=factory),
        ),
    ):
        body_ran = True

    assert body_ran is False
    # An uncertain attachment outcome is never a reported known failure.
    if expected_error is ExternalOperationUncertainError:
        assert not isinstance(excinfo.value, ExternalOperationFailedError)
    assert isinstance(excinfo.value.__cause__, expected_cause)
    assert factory.constructed == 1
    # The freshly created backend was disposed exactly once; no adapter
    # escaped the failed acquisition.
    assert [call.operation for call in backend.calls()] == ["connect", "dispose"]


# --- unexpected connect-time failures and interruption ----------------------


class _UnexpectedConnectFailureBackend(FakeClineSdkBackend):
    """A fake backend whose ``connect`` fails outside the sanitized taxonomy."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__()
        self._failure = failure

    def connect(self, remote: ClineRemoteConfig) -> None:
        raise self._failure


@pytest.mark.parametrize(
    "failure",
    [RuntimeError("unexpected bridge failure"), KeyboardInterrupt()],
    ids=["unexpected-exception", "interruption"],
)
def test_an_unexpected_or_interrupted_connect_still_disposes_the_backend_exactly_once(
    failure: BaseException,
) -> None:
    """Regression (PR review): disposal is owned from backend construction.

    A connect-time failure outside the sanitized backend taxonomy — or an
    interruption of the acquisition — must neither be masked nor leak the
    locally acquired bridge resources: the failure propagates unchanged and
    the backend is disposed exactly once.
    """
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            (_SECRET,),
        ]
    )
    backend = _UnexpectedConnectFailureBackend(failure)
    factory = _ScriptedBackendFactory(backend)

    with (
        pytest.raises(type(failure)) as excinfo,
        attach_cline_runtime(
            _pool(conn),
            **_attach_kwargs(workspace_id, task_id, backend_factory=factory),
        ),
    ):
        pass  # pragma: no cover - the lease body never runs

    # The unexpected failure is never masked by the taxonomy translation.
    assert excinfo.value is failure
    assert factory.constructed == 1
    # connect never completed and was never recorded; disposal ran exactly once.
    assert [call.operation for call in backend.calls()] == ["dispose"]


# --- refresh, bounded cleanup, and no replay --------------------------------


def test_a_later_explicit_attachment_resolves_the_current_rotated_secret() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    ready = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        snapshot={"adapter": "cline", "configuration": _selected_config()},
    )
    connection_row = _connection_row(
        workspace_id, connection_id, safe_config=_selected_config(), auth_reference=reference
    )
    # Rotation updates the Vault secret in place: the same auth_reference
    # resolves the NEW value on the explicitly new attachment.
    first_conn = ScriptedConnection([ready, connection_row, connection_row, (_SECRET,)])
    second_conn = ScriptedConnection([ready, connection_row, connection_row, (_ROTATED_SECRET,)])
    first_backend = FakeClineSdkBackend()
    second_backend = FakeClineSdkBackend()
    factory = _ScriptedBackendFactory(first_backend, second_backend)
    kwargs = _attach_kwargs(
        workspace_id,
        task_id,
        phase=ClineAttachmentPhase.BOUND,
        external_session_id="ext-1",
        backend_factory=factory,
    )

    with attach_cline_runtime(_pool(first_conn), **kwargs):
        pass
    with attach_cline_runtime(_pool(second_conn), **kwargs):
        pass

    (first_connect,) = [call for call in first_backend.calls() if call.operation == "connect"]
    (second_connect,) = [call for call in second_backend.calls() if call.operation == "connect"]
    assert first_connect.remote is not None
    assert second_connect.remote is not None
    assert first_connect.remote.auth_token == _SECRET
    assert second_connect.remote.auth_token == _ROTATED_SECRET
    assert first_connect.remote is not second_connect.remote


def test_the_lease_disposes_on_success_and_a_new_attachment_never_replays() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    reference = _auth_reference(uuid.uuid4())
    conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            (_SECRET,),
        ]
    )
    backend = FakeClineSdkBackend()
    factory = _ScriptedBackendFactory(backend)

    with attach_cline_runtime(
        _pool(conn),
        **_attach_kwargs(workspace_id, task_id, backend_factory=factory),
    ) as attached:
        # Attachment itself is the only backend operation: nothing is sent,
        # started, reconstructed, or replayed automatically.
        assert [call.operation for call in backend.calls()] == ["connect"]
        assert isinstance(attached.adapter, ClineAdapter)

    assert [call.operation for call in backend.calls()] == ["connect", "dispose"]


# --- secret/content safety ---------------------------------------------------


def _exception_chain(error: BaseException) -> list[BaseException]:
    """Collect the full visible cause/context chain of one exception."""
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def test_attachment_failures_never_leak_secret_or_endpoint_material(
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    secret_sentinel = "SENSITIVE-SECRET-SENTINEL"
    endpoint_sentinel = "ws://SENSITIVE-ENDPOINT-SENTINEL.example:25463/hub"
    reference = _auth_reference(secret_id)
    selected = {"repository_base": _SELECTED_BASE, "control_endpoint": endpoint_sentinel}
    conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            _connection_row(
                workspace_id, connection_id, safe_config=selected, auth_reference=reference
            ),
            (secret_sentinel,),
        ]
    )
    backend = FakeClineSdkBackend()
    backend.queue_connect(ClineRemoteAttachmentUnavailableError())
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with (
        caplog.at_level(logging.DEBUG),
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(ExternalOperationFailedError) as excinfo,
        attach_cline_runtime(
            _pool(conn),
            **_attach_kwargs(
                workspace_id, task_id, backend_factory=_ScriptedBackendFactory(backend)
            ),
        ),
    ):
        pass

    # No secret, endpoint, or auth-reference material appears anywhere in the
    # visible exception chain (the sanitized backend error carries no native
    # text by construction, and the composition adds only safe messages).
    forbidden = (secret_sentinel, "SENSITIVE-ENDPOINT-SENTINEL", endpoint_sentinel, reference)
    for item in _exception_chain(excinfo.value):
        text = f"{item} {item!r}"
        for value in forbidden:
            assert value not in text

    # The exported spans carry only the safe identifier vocabulary.
    exported = exporter.get_finished_spans()
    assert exported
    for span in exported:
        attributes = span.attributes
        assert attributes is not None
        for key, value in attributes.items():
            assert key in {OPERATION, WORKSPACE_ID, TASK_ID, WORKFLOW_ROLE, CONNECTION_ID}
            for value_sentinel in forbidden:
                assert value_sentinel not in str(value)

    # No log record carries the material either.
    for value in forbidden:
        assert value not in caplog.text

    # The #188 parser's typed endpoint rejection never echoes the raw value.
    # The credential resolution's own read carries the malformed endpoint, so
    # the refusal surfaces at the endpoint resolution of the selection.
    malformed = "https://SENSITIVE-MALFORMED-ENDPOINT.example/hub"
    refusal_conn = ScriptedConnection(
        [
            _binding_row(workspace_id, task_id, connection_id),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config=_selected_config(),
                auth_reference=reference,
            ),
            _connection_row(
                workspace_id,
                connection_id,
                safe_config={"repository_base": _SELECTED_BASE, "control_endpoint": malformed},
                auth_reference=reference,
            ),
            (secret_sentinel,),
        ]
    )
    with (
        pytest.raises(ConflictError) as refusal,
        attach_cline_runtime(
            _pool(refusal_conn),
            **_attach_kwargs(
                workspace_id,
                task_id,
                backend_factory=_ScriptedBackendFactory(FakeClineSdkBackend()),
            ),
        ),
    ):
        pass
    assert malformed not in str(refusal.value)
    assert malformed not in repr(refusal.value)
