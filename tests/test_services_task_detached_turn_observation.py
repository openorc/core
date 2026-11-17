"""Deterministic service tests for the process-owned Hub-run observation host
(issue #323).

The observer's own behavior and its critical cross-boundary seams: the
positive pre-enqueue registration gate and its typed refusals, verified
terminal ingestion into the shared settlement seam with successor-guarded
subscription cleanup, the admission-race staging handoff, reconnection with
the bounded status-only consultation, scoped session isolation, and the
v1 no-recovery restart limitation. The established settlement, persistence,
and protocol matrices are not re-proven here.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from openorc.adapters.cline import ClineHubEvent, Subscription, resolve_connection_control_endpoint
from openorc.adapters.cline import hub_observation as source_module
from openorc.adapters.cline.errors import ClineBackendUncertainOutcomeError
from openorc.adapters.cline.hub_observation import ClineHubTurnObservationSource
from openorc.domain.connections import AdapterType
from openorc.domain.detached_turns import (
    ObservedTurnEvidence,
    TaskDetachedTurnReceiptStatus,
    TurnEvidenceFamily,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.pool import DatabasePool
from openorc.services import task_detached_turn_observation as observation_module
from openorc.services.connection_credentials import ControlEndpointSecret
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    ObservationRegistrationRefusedError,
)
from openorc.services.task_detached_turn_correlation import (
    TurnSettlementKind,
    TurnSettlementOutcome,
    TurnStartCorroboration,
    TurnStatusClassification,
    TurnStatusClassificationKind,
)
from openorc.services.task_detached_turn_observation import (
    DetachedTurnObservationHost,
    ObservationRegistrationRefusalReason,
)
from tests.fakes.cline_sdk_backend import FakeClineSdkBackend

_OBSERVED = datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_CONNECTION_ID = uuid4()
_SESSION_ID = uuid4()
_EXTERNAL_SESSION = "ext-session-1"
_RECEIPT_ID = uuid4()
_EXECUTION_ID = uuid4()
_RUN_ID = "hub-run-1"
_ENDPOINT = "ws://hub.example.test:9/hub"
_RESULT_TEXT = '{"kind": "implementation_result"}'


def _delivered_event(evidence: ObservedTurnEvidence) -> ClineHubEvent:
    """Unwrap this integration's own opaque carrier inside a test."""
    carrier = cast(source_module._ClineHubEventEvidence, evidence.native_evidence)
    return carrier.event


def _carried_records(status_evidence: Any) -> list[dict[str, Any]]:
    """Unwrap the integration's status-only evidence carrier inside a test."""
    carrier = cast(source_module._ClineStatusRecordsEvidence, status_evidence)
    return carrier.records


def _terminal_event(
    *,
    session_id: str = _EXTERNAL_SESSION,
    kind: str = "run.completed",
    sequence: int | None = 41,
    text: str | None = _RESULT_TEXT,
) -> ClineHubEvent:
    result = {} if text is None else {"text": text}
    return ClineHubEvent(
        session_id=session_id,
        kind=kind,
        sequence=sequence,
        payload={"reason": "finished", "result": result},
    )


# --- the scripted-connection harness ----------------------------------------------


class _Predicate:
    def __init__(self, *fragments: str, fn: Callable[[str], bool] | None = None) -> None:
        self._fragments = fragments
        self._fn = fn

    def __call__(self, sql: str) -> bool:
        if self._fn is not None:
            return self._fn(sql)
        lowered = " ".join(sql.split()).lower()
        return all(fragment in lowered for fragment in self._fragments)


def _sql_has(*fragments: str) -> _Predicate:
    return _Predicate(*fragments)


class FakeCursor:
    def __init__(self, row: Any) -> None:
        self._row = row

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        if self._row is None:
            return []
        if isinstance(self._row, list):
            return list(self._row)
        return [self._row]


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler; handlers
    are consumed in registration order unless registered as repeating."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, Any, bool]] = []

    def on(self, predicate: _Predicate, result: Any) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: Any) -> None:
        self._handlers.append((predicate, result, False))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        for index, (predicate, result, once) in enumerate(self._handlers):
            if predicate(sql):
                if once:
                    self._handlers.pop(index)
                if isinstance(result, Exception):
                    raise result
                if callable(result):
                    result = result(self)
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {' '.join(sql.split())[:90]}")

    def transaction(self):  # noqa: ANN201 - test fake
        class _Noop:
            def __enter__(self) -> None:
                return None

            def __exit__(self, *_args: object) -> bool:
                return False

        return _Noop()


class ScriptedPool:
    def __init__(self, connection: ScriptedConnection) -> None:
        self._connection = connection

    def connection(self):  # noqa: ANN201 - test fake
        @contextmanager
        def managed():  # noqa: ANN202 - test fake
            yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the pool lifetime")


def _pool(connection: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, ScriptedPool(connection))


# --- canned durable rows ------------------------------------------------------------


@dataclass
class _BindingRow:
    """The scripted bindings keyed by their exact external session identity."""

    rows: dict[str, tuple[Any, ...]] = field(default_factory=dict)

    def handler(self, conn: ScriptedConnection) -> tuple[Any, ...] | None:
        _sql, params = conn.executed[-1]
        assert params is not None
        return self.rows.get(cast(str, params[2]))


@dataclass
class _ReceiptRow:
    """The scripted outstanding receipts keyed by their session binding id."""

    rows: dict[UUID, tuple[Any, ...]] = field(default_factory=dict)

    def handler(self, conn: ScriptedConnection) -> tuple[Any, ...] | None:
        _sql, params = conn.executed[-1]
        assert params is not None
        return self.rows.get(cast(UUID, params[0]))


def _binding_row() -> tuple[Any, ...]:
    return (
        _SESSION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "producer",
        _CONNECTION_ID,
        _EXTERNAL_SESSION,
        "ready",
        {"adapter": "cline"},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _admitted_receipt_row() -> tuple[Any, ...]:
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "implementation_execution",
        "execution",
        _EXECUTION_ID,
        "implementation_result",
        _RUN_ID,
        TaskDetachedTurnReceiptStatus.ADMITTED.value,
        {},
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        _OBSERVED,
        None,
        None,
        None,
        None,
    )


# --- focused seam stand-ins ---------------------------------------------------------


@dataclass
class _FakeConnection:
    adapter: AdapterType
    safe_config: dict[str, Any]


@dataclass
class _SettleRecorder:
    """Records the settlement-seam deliveries and answers a fixed outcome."""

    outcome_kind: TurnSettlementKind
    receipt: Any = None
    calls: list[tuple[UUID, UUID, ObservedTurnEvidence]] = field(default_factory=list)

    def __call__(
        self,
        pool: DatabasePool,
        *,
        classifier: Any,
        workspace_id: UUID,
        connection_id: UUID,
        evidence: ObservedTurnEvidence,
    ) -> TurnSettlementOutcome:
        self.calls.append((workspace_id, connection_id, evidence))
        return TurnSettlementOutcome(kind=self.outcome_kind, receipt=self.receipt)


@dataclass
class _ClassifyRecorder:
    calls: list[tuple[UUID, UUID, UUID, Any]] = field(default_factory=list)

    def __call__(
        self,
        pool: DatabasePool,
        *,
        classifier: Any,
        workspace_id: UUID,
        task_id: UUID,
        receipt_id: UUID,
        status_evidence: Any,
    ) -> TurnStatusClassification:
        self.calls.append((workspace_id, task_id, receipt_id, status_evidence))
        return TurnStatusClassification(
            kind=TurnStatusClassificationKind.CLASSIFIED_FAILURE, receipt=None
        )


@dataclass
class _StartRecorder:
    calls: list[tuple[UUID, UUID, ObservedTurnEvidence]] = field(default_factory=list)

    def __call__(
        self,
        pool: DatabasePool,
        *,
        classifier: Any,
        workspace_id: UUID,
        connection_id: UUID,
        evidence: ObservedTurnEvidence,
    ) -> TurnStartCorroboration:
        self.calls.append((workspace_id, connection_id, evidence))
        return TurnStartCorroboration.CORROBORATED


def _fake_credential_resolver(
    pool: DatabasePool, *, workspace_id: UUID, connection_id: UUID
) -> Any:
    return (
        _FakeConnection(
            adapter=AdapterType.CLINE,
            safe_config={"control_endpoint": _ENDPOINT},
        ),
        ControlEndpointSecret("observation-attach-token"),
    )


def _subscribe_calls(backend: FakeClineSdkBackend) -> list[Any]:
    return [call for call in backend.calls() if call.operation == "subscribe_hub"]


def _make_observer(
    conn: ScriptedConnection,
    *,
    backends: list[FakeClineSdkBackend],
) -> DetachedTurnObservationHost:
    pool = _pool(conn)
    pending = list(backends)
    source = ClineHubTurnObservationSource(backend_factory=lambda: pending.pop(0))
    host = DetachedTurnObservationHost(
        lambda: pool,
        observation_source=source,
        runtime_credential_resolver=_fake_credential_resolver,
        control_endpoint_resolver=resolve_connection_control_endpoint,
    )
    host.start()
    return host


def _register(
    observer: DetachedTurnObservationHost,
    *,
    task_agent_session_id: UUID = _SESSION_ID,
    external_session_id: str = _EXTERNAL_SESSION,
) -> None:
    observer.register_session(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=task_agent_session_id,
        connection_id=_CONNECTION_ID,
        external_session_id=external_session_id,
    )


def _binding_row_for(session_id: UUID, external_session_id: str) -> tuple[Any, ...]:
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        "producer",
        _CONNECTION_ID,
        external_session_id,
        "ready",
        {"adapter": "cline"},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _script_bindings(conn: ScriptedConnection, bindings: _BindingRow) -> None:
    conn.on_repeat(
        _sql_has("from openorc.task_agent_sessions", "connection_id = %s"),
        bindings.handler,
    )


def _script_outstanding_receipts(conn: ScriptedConnection, receipts: _ReceiptRow) -> None:
    conn.on_repeat(
        _sql_has("from openorc.task_hub_run_receipts", "status in ('claimed', 'admitted')"),
        receipts.handler,
    )


def _test_observer(
    monkeypatch: pytest.MonkeyPatch,
    conn: ScriptedConnection,
    *,
    backends: list[FakeClineSdkBackend],
    settle: _SettleRecorder | None = None,
) -> DetachedTurnObservationHost:
    """A started observer whose terminal routing is recorded, not settled."""
    observer = _make_observer(conn, backends=backends)
    if settle is not None:
        monkeypatch.setattr(
            observation_module, "settle_observed_turn_completion", settle, raising=True
        )
    return observer


def test_registration_subscribes_the_exact_session_through_the_current_trusted_attachment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)

    _register(observer)

    subscribe_calls = _subscribe_calls(backend)
    assert [(call.session_id) for call in subscribe_calls] == [_EXTERNAL_SESSION]
    assert settle.calls == []  # nothing observed yet, nothing settled


def test_a_verified_completion_settles_through_the_seam_and_releases_the_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    _register(observer)

    backend.emit_hub(_terminal_event())

    assert len(settle.calls) == 1
    workspace_id, connection_id, evidence = settle.calls[0]
    assert workspace_id == _WORKSPACE_ID
    assert connection_id == _CONNECTION_ID
    assert evidence.family is TurnEvidenceFamily.TURN_TERMINAL
    assert _delivered_event(evidence) == _terminal_event()
    # The terminal classification releases the session's observation: a
    # further delivery reaches no listener and no re-subscription happened.
    backend.emit_hub(_terminal_event(sequence=42))
    assert len(settle.calls) == 1
    assert len(_subscribe_calls(backend)) == 1


def test_an_observed_completion_after_the_dispatch_job_has_exited_still_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    # The dispatch job's registration returned long before the completion:
    # the observation is independent of that job's lifetime.
    _register(observer)

    backend.emit_hub(_terminal_event(sequence=7))

    assert len(settle.calls) == 1
    assert _delivered_event(settle.calls[0][2]) == _terminal_event(sequence=7)


def test_a_duplicate_delivery_is_rejected_and_the_observation_is_kept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.REJECTED_STALE)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    _register(observer)

    backend.emit_hub(_terminal_event())
    backend.emit_hub(_terminal_event())

    assert len(settle.calls) == 2  # both deliveries routed; the seam rejected the stale one
    assert len(_subscribe_calls(backend)) == 1


def test_a_terminal_event_before_the_acknowledgement_is_staged_and_observation_is_kept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    receipts = _ReceiptRow(rows={_SESSION_ID: _admitted_receipt_row()})
    receipts.rows[_SESSION_ID] = (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "implementation_execution",
        "execution",
        _EXECUTION_ID,
        "implementation_result",
        None,
        TaskDetachedTurnReceiptStatus.CLAIMED.value,
        {},
        None,
        _OBSERVED,
        None,
        None,
        _OBSERVED,
        None,
        None,
        None,
        None,
    )
    _script_outstanding_receipts(conn, receipts)
    conn.on_repeat(
        _sql_has("select max((outcome ->> 'consumed_sequence')::bigint)"),
        None,
    )
    staged_id = uuid4()
    conn.on(
        _sql_has("insert into openorc.task_hub_run_early_completions"),
        (
            staged_id,
            _WORKSPACE_ID,
            _TASK_ID,
            _RECEIPT_ID,
            "run.completed",
            41,
            _RESULT_TEXT,
            _OBSERVED,
        ),
    )
    backend = FakeClineSdkBackend()
    observer = _make_observer(conn, backends=[backend])
    _register(observer)

    backend.emit_hub(_terminal_event())

    # The real seam staged the early completion on the claimed receipt: the
    # acknowledgement attach will consume it exactly once. The observation
    # stays subscribed until the run's terminal classification.
    insert_calls = [
        call
        for call in conn.executed
        if "insert into openorc.task_hub_run_early_completions" in call[0]
    ]
    assert len(insert_calls) == 1
    assert len(_subscribe_calls(backend)) == 1
    assert observer.disposed is False


def test_a_run_started_identity_is_corroborated_through_the_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    _register(observer)
    start = _StartRecorder()
    monkeypatch.setattr(observation_module, "check_turn_start_identity", start, raising=True)

    backend.emit_hub(
        ClineHubEvent(
            session_id=_EXTERNAL_SESSION,
            kind="run.started",
            sequence=3,
            payload={"requestId": _RUN_ID},
        )
    )

    assert len(start.calls) == 1
    start_workspace, start_connection, start_evidence = start.calls[0]
    assert (start_workspace, start_connection) == (_WORKSPACE_ID, _CONNECTION_ID)
    assert start_evidence.family is TurnEvidenceFamily.TURN_START
    assert _delivered_event(start_evidence).payload == {"requestId": _RUN_ID}
    assert settle.calls == []  # a start identity is never a terminal observation


def test_scoped_session_isolation_routes_each_session_within_its_own_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    other_session_id = uuid4()
    other_external = "ext-session-2"
    bindings = _BindingRow(
        rows={
            _EXTERNAL_SESSION: _binding_row(),
            other_external: _binding_row_for(other_session_id, other_external),
        }
    )
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    _register(observer)
    _register(observer, task_agent_session_id=other_session_id, external_session_id=other_external)

    backend.emit_hub(_terminal_event(session_id=other_external, sequence=5))

    assert len(settle.calls) == 1
    _workspace_id, _connection_id, evidence = settle.calls[0]
    assert evidence.external_session_id == other_external
    assert settle.calls[0][:2] == (_WORKSPACE_ID, _CONNECTION_ID)
    # The foreign session's event never reached the other session's scope.
    backend.emit_hub(_terminal_event(sequence=6))
    assert len(settle.calls) == 2
    assert settle.calls[1][2].external_session_id == _EXTERNAL_SESSION


def test_reconnect_re_establishes_observation_and_consults_the_status_only_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    other_session_id = uuid4()
    other_external = "ext-session-2"
    bindings = _BindingRow(
        rows={
            _EXTERNAL_SESSION: _binding_row(),
            other_external: _binding_row_for(other_session_id, other_external),
        }
    )
    _script_bindings(conn, bindings)
    receipts = _ReceiptRow(rows={_SESSION_ID: _admitted_receipt_row()})
    _script_outstanding_receipts(conn, receipts)
    dead_backend = FakeClineSdkBackend()
    fresh_backend = FakeClineSdkBackend()
    fresh_backend.queue_list_runs(
        _EXTERNAL_SESSION, [{"runId": _RUN_ID, "state": "failed", "error": "session not found"}]
    )
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(
        monkeypatch, conn, backends=[dead_backend, fresh_backend], settle=settle
    )
    _register(observer)

    # The attachment's subscription path proves unusable for the next
    # registration: the typed refusal blocks the enqueue and the dead
    # attachment is dropped.
    original_subscribe = dead_backend.subscribe_hub

    def failing_subscribe(session_id: str, listener: Any) -> Subscription:
        if session_id == other_external:
            raise ClineBackendUncertainOutcomeError(
                "the hub attachment is no longer available for subscriptions"
            )
        return original_subscribe(session_id, listener)

    monkeypatch.setattr(dead_backend, "subscribe_hub", failing_subscribe, raising=True)
    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        _register(
            observer, task_agent_session_id=other_session_id, external_session_id=other_external
        )
    assert (
        refused.value.reason == ObservationRegistrationRefusalReason.OBSERVATION_SUBSCRIPTION_FAILED
    )
    assert any(call.operation == "dispose" for call in dead_backend.calls())

    # The next registration re-attaches through a fresh credential read and
    # the bounded reconnect reconciliation re-subscribes the outstanding
    # session and consults its status-only run.list read.
    classify = _ClassifyRecorder()
    monkeypatch.setattr(
        observation_module, "classify_acknowledged_turn_status", classify, raising=True
    )
    _register(observer, task_agent_session_id=other_session_id, external_session_id=other_external)

    assert len(classify.calls) == 1
    workspace_id, task_id, receipt_id, status_evidence = classify.calls[0]
    assert (workspace_id, task_id, receipt_id) == (_WORKSPACE_ID, _TASK_ID, _RECEIPT_ID)
    assert _carried_records(status_evidence) == [
        {"runId": _RUN_ID, "state": "failed", "error": "session not found"}
    ]
    fresh_subscribes = _subscribe_calls(fresh_backend)
    assert [call.session_id for call in fresh_subscribes] == [
        _EXTERNAL_SESSION,
        other_external,
    ]
    assert settle.calls == []  # a status-only failed record is classified, not settled


def test_registration_refusal_blocks_the_enqueue_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    other_session_id = uuid4()
    other_external = "ext-session-2"
    bindings = _BindingRow(
        rows={
            _EXTERNAL_SESSION: _binding_row(),
            other_external: _binding_row_for(other_session_id, other_external),
        }
    )
    _script_bindings(conn, bindings)

    # Not started: the observer refuses before any attachment or dial.
    unstarted = DetachedTurnObservationHost(
        lambda: _pool(conn),
        observation_source=ClineHubTurnObservationSource(backend_factory=FakeClineSdkBackend),
        runtime_credential_resolver=_fake_credential_resolver,
        control_endpoint_resolver=resolve_connection_control_endpoint,
    )
    with pytest.raises(ObservationRegistrationRefusedError) as unavailable:
        _register(unstarted)
    assert unavailable.value.reason == ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE

    backend = FakeClineSdkBackend()
    observer = _make_observer(conn, backends=[backend])

    # Capacity exhaustion: the bounded host refuses rather than degrading
    # observation, so the dispatch blocks its enqueue.
    monkeypatch.setattr(observation_module, "_MAX_CONCURRENT_SESSION_OBSERVATIONS", 1)
    _register(observer)
    with pytest.raises(ObservationRegistrationRefusedError) as at_capacity:
        _register(
            observer,
            task_agent_session_id=other_session_id,
            external_session_id=other_external,
        )
    assert at_capacity.value.reason == ObservationRegistrationRefusalReason.OBSERVER_AT_CAPACITY
    monkeypatch.undo()

    # An unresolvable session binding fails closed before any secret access.
    with pytest.raises(ObservationRegistrationRefusedError) as unresolved:
        _register(observer, external_session_id="ext-session-unknown")
    assert (
        unresolved.value.reason == ObservationRegistrationRefusalReason.SESSION_BINDING_UNRESOLVED
    )

    # An attachment failure refuses positively: the enqueue must not proceed.
    failing_backend = FakeClineSdkBackend()
    failing_backend.queue_connect(
        ClineBackendUncertainOutcomeError("the hub attachment outcome is unknown")
    )
    replacing = _make_observer(conn, backends=[failing_backend])
    with pytest.raises(ObservationRegistrationRefusedError) as attachment:
        _register(replacing)
    assert (
        attachment.value.reason
        == ObservationRegistrationRefusalReason.OBSERVATION_ATTACHMENT_FAILED
    )
    assert any(call.operation == "dispose" for call in failing_backend.calls())

    # A subscription failure refuses positively and drops the dead attachment.
    subscription_backend = FakeClineSdkBackend()

    def failing_subscribe(session_id: str, listener: Any) -> Subscription:
        raise ClineBackendUncertainOutcomeError(
            "the hub attachment is no longer available for subscriptions"
        )

    monkeypatch.setattr(subscription_backend, "subscribe_hub", failing_subscribe, raising=True)
    replacing_subscription = _make_observer(conn, backends=[subscription_backend])
    with pytest.raises(ObservationRegistrationRefusedError) as subscription:
        _register(replacing_subscription)
    assert (
        subscription.value.reason
        == ObservationRegistrationRefusalReason.OBSERVATION_SUBSCRIPTION_FAILED
    )
    assert any(call.operation == "dispose" for call in subscription_backend.calls())


def test_malformed_registration_commands_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    backend = FakeClineSdkBackend()
    observer = _test_observer(
        monkeypatch,
        conn,
        backends=[backend],
        settle=_SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT),
    )

    with pytest.raises(InvalidCommandError):
        _register(observer, external_session_id="   ")
    with pytest.raises(InvalidCommandError):
        observer.register_session(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=cast(UUID, "not-a-uuid"),
            connection_id=_CONNECTION_ID,
            external_session_id=_EXTERNAL_SESSION,
        )


def test_a_successor_registration_between_settlement_and_cleanup_keeps_the_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    observer = _make_observer(conn, backends=[backend])
    _register(observer)

    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    registered_successor = False

    def interleaving_settle(
        pool: DatabasePool,
        *,
        classifier: Any,
        workspace_id: UUID,
        connection_id: UUID,
        evidence: ObservedTurnEvidence,
    ) -> TurnSettlementOutcome:
        # Run A's atomic settlement released the outstanding-run constraint;
        # Run B legitimately registers during the settlement, before Run A's
        # subscription cleanup has executed.
        nonlocal registered_successor
        if not registered_successor:
            registered_successor = True
            _register(observer)
        settle.calls.append((workspace_id, connection_id, evidence))
        return TurnSettlementOutcome(kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT, receipt=None)

    monkeypatch.setattr(observation_module, "settle_observed_turn_completion", interleaving_settle)

    backend.emit_hub(_terminal_event())

    assert registered_successor is True
    assert len(settle.calls) == 1
    # Run A's cleanup must not disconnect the observation Run B renewed: the
    # successor reused the live subscription instead of forcing a new one,
    # and Run B's own completion still routes.
    assert len(_subscribe_calls(backend)) == 1
    backend.emit_hub(_terminal_event(sequence=99))
    assert len(settle.calls) == 2


def test_an_unknown_result_after_an_observer_restart_is_never_invented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    _register(observer)
    observer.dispose()

    # The restarted process owns no registry: a delivered completion for the
    # previously observed session reaches nothing and is never invented.
    backend.emit_hub(_terminal_event())
    restarted = _make_observer(conn, backends=[FakeClineSdkBackend()])
    restarted_backend = FakeClineSdkBackend()
    restarted_backend.emit_hub(_terminal_event())

    assert observer.disposed is True
    assert settle.calls == []
    assert restarted.disposed is False


def test_disposal_stops_observation_idempotently(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    observer = _test_observer(monkeypatch, conn, backends=[backend], settle=settle)
    _register(observer)

    observer.dispose()
    observer.dispose()  # idempotent

    backend.emit_hub(_terminal_event())
    assert settle.calls == []
    assert any(call.operation == "dispose" for call in backend.calls())
    with pytest.raises(ObservationRegistrationRefusedError) as after_disposal:
        _register(observer)
    assert after_disposal.value.reason == ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE
    with pytest.raises(ConflictError):
        observer.start()


def test_known_attachment_configuration_failures_are_typed_registration_refusals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    bindings = _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()})
    _script_bindings(conn, bindings)
    backend = FakeClineSdkBackend()
    pool = _pool(conn)
    resolver_outcomes: list[Exception] = [
        NotFoundError("the requested connection is not available in this workspace"),
        ConflictError("the connection is disabled; its credential cannot be used"),
    ]

    def refusing_resolver(pool: DatabasePool, *, workspace_id: UUID, connection_id: UUID) -> Any:
        outcome = resolver_outcomes.pop(0)
        assert isinstance(outcome, Exception)
        raise outcome

    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    observer = DetachedTurnObservationHost(
        lambda: pool,
        observation_source=source,
        runtime_credential_resolver=refusing_resolver,
        control_endpoint_resolver=resolve_connection_control_endpoint,
    )
    observer.start()

    # A known, positively established pre-subscription configuration failure
    # is the typed registration refusal the dispatch gates the enqueue on —
    # never a workflow conflict escaping the observation host.
    for _ in range(2):
        with pytest.raises(ObservationRegistrationRefusedError) as refused:
            _register(observer)
        assert (
            refused.value.reason
            == ObservationRegistrationRefusalReason.OBSERVATION_ATTACHMENT_FAILED
        )

    # The same contract covers the host's own endpoint validation: a stored
    # but unattachable connection refuses positively before any dial.
    def unattachable_resolver(
        pool: DatabasePool, *, workspace_id: UUID, connection_id: UUID
    ) -> Any:
        return (
            _FakeConnection(adapter=AdapterType.CLINE, safe_config={}),
            ControlEndpointSecret("observation-attach-token"),
        )

    monkeypatch.setattr(observer, "_credential_resolver", unattachable_resolver, raising=True)
    with pytest.raises(ObservationRegistrationRefusedError) as unattachable:
        _register(observer)
    assert (
        unattachable.value.reason
        == ObservationRegistrationRefusalReason.OBSERVATION_ATTACHMENT_FAILED
    )

    # Every refusal was pre-subscription: no backend was ever constructed or dialed.
    assert backend.calls() == ()


# --- establishment registration and the completion handoff (issues #324/#333) -----


class _EstablishmentReceiptRows:
    """The scripted establishment receipts keyed by provisional external id."""

    def __init__(self, rows: dict[str, tuple[Any, ...]]) -> None:
        self.rows = rows

    def handler(self, conn: ScriptedConnection) -> tuple[Any, ...] | None:
        _sql, params = conn.executed[-1]
        assert params is not None
        return self.rows.get(cast(str, params[2]))


def _establishment_receipt_row(
    task_agent_session_id: UUID, external_session_id: str
) -> tuple[Any, ...]:
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        task_agent_session_id,
        "session_initialization",
        "planning_bootstrap_claim",
        uuid4(),
        "session_ready",
        None,
        TaskDetachedTurnReceiptStatus.CLAIMED.value,
        {},
        None,
        _OBSERVED,
        None,
        None,
        _OBSERVED,
        external_session_id,
        {"adapter": "cline", "configuration": {}},
        None,
        None,
    )


def _script_establishment_receipts(
    conn: ScriptedConnection, rows: _EstablishmentReceiptRows
) -> None:
    conn.on_repeat(
        _sql_has("from openorc.task_hub_run_receipts r", "created_external_session_id = %s"),
        rows.handler,
    )


class _RecordingHandoff:
    """Records the post-commit completion handoff invocations (issue #333)."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def handoff_settled_completion(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        operation: PendingTaskRuntimeInteractionOperation,
        validated_result: Any = None,
    ) -> None:
        self.calls.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "operation": operation,
                "validated_result": validated_result,
            }
        )


def _make_handoff_observer(
    conn: ScriptedConnection,
    *,
    backends: list[FakeClineSdkBackend],
    handoff: _RecordingHandoff,
) -> DetachedTurnObservationHost:
    pool = _pool(conn)
    pending = list(backends)
    source = ClineHubTurnObservationSource(backend_factory=lambda: pending.pop(0))
    observer = DetachedTurnObservationHost(
        lambda: pool,
        observation_source=source,
        runtime_credential_resolver=_fake_credential_resolver,
        control_endpoint_resolver=resolve_connection_control_endpoint,
        completion_handoff=handoff,
    )
    observer.start()
    return observer


def test_establishment_registration_proves_the_provisional_identity_and_subscribes() -> None:
    backend = FakeClineSdkBackend()
    conn = ScriptedConnection()
    _script_bindings(conn, _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()}))
    _script_establishment_receipts(
        conn,
        _EstablishmentReceiptRows(
            rows={"ext-init-9": _establishment_receipt_row(_SESSION_ID, "ext-init-9")}
        ),
    )
    observer = _make_observer(conn, backends=[backend])

    observer.register_establishment_session(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        connection_id=_CONNECTION_ID,
        external_session_id="ext-init-9",
    )

    assert len(_subscribe_calls(backend)) == 1


def test_establishment_registration_refuses_without_a_matching_receipt() -> None:
    backend = FakeClineSdkBackend()
    conn = ScriptedConnection()
    _script_bindings(conn, _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()}))
    _script_establishment_receipts(
        conn,
        _EstablishmentReceiptRows(
            rows={"ext-init-9": _establishment_receipt_row(_SESSION_ID, "ext-init-9")}
        ),
    )
    observer = _make_observer(conn, backends=[backend])

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        observer.register_establishment_session(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            connection_id=_CONNECTION_ID,
            external_session_id="ext-init-other",
        )

    assert refused.value.reason == ObservationRegistrationRefusalReason.SESSION_BINDING_UNRESOLVED
    assert _subscribe_calls(backend) == []


def test_validated_establishment_settlement_fires_the_completion_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _RecordingHandoff()
    backend = FakeClineSdkBackend()
    conn = ScriptedConnection()
    _script_bindings(conn, _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()}))
    _script_outstanding_receipts(conn, _ReceiptRow(rows={_SESSION_ID: _admitted_receipt_row()}))
    settle = _SettleRecorder(
        outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=cast(
            Any,
            SimpleNamespace(
                operation=PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION
            ),
        ),
    )
    observer = _make_handoff_observer(conn, backends=[backend], handoff=handoff)
    _register(observer)
    monkeypatch.setattr(observation_module, "settle_observed_turn_completion", settle, raising=True)

    backend.emit_hub(_terminal_event())

    assert len(settle.calls) == 1
    assert handoff.calls == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "operation": PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
            "validated_result": None,
        }
    ]


def test_validated_execution_settlement_fires_the_completion_handoff_with_its_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _RecordingHandoff()
    backend = FakeClineSdkBackend()
    conn = ScriptedConnection()
    _script_bindings(conn, _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()}))
    _script_outstanding_receipts(conn, _ReceiptRow(rows={_SESSION_ID: _admitted_receipt_row()}))
    settle = _SettleRecorder(
        outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=cast(
            Any,
            SimpleNamespace(
                operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
            ),
        ),
    )
    observer = _make_handoff_observer(conn, backends=[backend], handoff=handoff)
    _register(observer)
    monkeypatch.setattr(observation_module, "settle_observed_turn_completion", settle, raising=True)

    backend.emit_hub(_terminal_event())

    assert len(settle.calls) == 1
    assert handoff.calls == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "operation": PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            "validated_result": None,
        }
    ]
