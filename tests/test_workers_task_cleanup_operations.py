"""Deterministic tests for the F10 terminal finalization operation composition (issues #256/#317).

The registered terminal finalization F1 handler invokes the shared
authoritative finalization service with process-local dependencies and only the decoded
scoped Workspace/Task identifiers; the same production handler table serves
both execution modes — regular mode through the bounded local executor (no
queue backend is ever contacted) and queue mode through the F1 queue
submission seam, whose recorded wire form is strict-decoded back through the
shared dispatcher. The submission bridge maps the F1 submission outcomes 1:1
onto the cleanup service's bounded outcome vocabulary without inventing
completion or replaying. No live Postgres, Valkey, GitHub, or Cline is
touched.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from openorc.config import ExecutionMode, Settings
from openorc.services.task_terminal_finalization import (
    TerminalCleanupDispatchSelection,
    TerminalFinalizationSubmissionOutcome,
)
from openorc.workers import invocation, task_cleanup_operations
from openorc.workers.invocation import (
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
    TerminalFinalizationPayload,
    decode_operation_invocation,
    invoke_operation,
)
from openorc.workers.local_executor import BoundedLocalExecutor
from openorc.workers.queues import queue_name
from openorc.workers.task_cleanup_operations import (
    F1TerminalFinalizationSubmission,
    build_terminal_cleanup_operation_handlers,
)
from openorc.workers.task_queue_operations import F1TaskQueueWakeSubmission

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")


def _settings(*, execution_mode: ExecutionMode = ExecutionMode.REGULAR) -> Settings:
    return Settings(
        environment="test",
        api_host="127.0.0.1",
        api_port=3999,
        api_reload=False,
        execution_mode=execution_mode,
        valkey_url="redis://127.0.0.1:6379/0",
        database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        db_pool_min=1,
        db_pool_max=5,
        db_pool_timeout=5.0,
        github_app_id=1234,
        github_app_private_key="not-a-real-key",
    )


@dataclass
class _CleanupServiceHarness:
    """Recording fake for the shared cleanup application service."""

    calls: list[dict[str, Any]] = field(default_factory=list)
    started: threading.Event = field(default_factory=threading.Event)
    error: Exception | None = None
    outcome: Any = None

    def __call__(
        self,
        pool: Any,
        runtime: Any,
        *,
        workspace_id: UUID,
        task_id: UUID,
        dispatch: Any = None,
    ) -> Any:
        self.calls.append(
            {
                "pool": pool,
                "runtime": runtime,
                "workspace_id": workspace_id,
                "task_id": task_id,
                "dispatch": dispatch,
            }
        )
        self.started.set()
        if self.error is not None:
            raise self.error
        if self.outcome is not None:
            return self.outcome
        return SimpleNamespace(kind="archived", task=None, released_session_bindings=())


@pytest.fixture
def cleanup_service_harness(
    monkeypatch: pytest.MonkeyPatch,
) -> _CleanupServiceHarness:
    """Patch the shared cleanup service at the composition-module boundary."""
    harness = _CleanupServiceHarness()
    monkeypatch.setattr(task_cleanup_operations, "finalize_task_terminalization", harness)
    return harness


class _RecordingRuntime:
    """Any-typed cleanup runtime double resolved through the provider seam."""


@dataclass
class _RecordingSubmitter:
    """Recording F1 submitter double with one scripted outcome."""

    outcome: SubmissionResult = field(default_factory=SubmissionResult.accepted)
    submissions: list[OperationInvocation] = field(default_factory=list)

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        self.submissions.append(invocation)
        return self.outcome


def _dependencies(settings: Settings, runtime: Any, detached_registrar: Any = None) -> Any:
    pool: Any = object()
    github: Any = object()
    providers: dict[str, Any] = {
        "settings": settings,
        "database_pool_provider": lambda _settings: pool,
        "github_client_provider": lambda _settings: github,
        "continuation_submitter_provider": lambda _settings: _RecordingSubmitter(),
        "terminal_cleanup_runtime_provider": lambda _settings: runtime,
    }
    if detached_registrar is not None:
        providers["detached_turn_observer_provider"] = lambda _settings: detached_registrar
    return OperationDependencies(**providers)


class _FakeQueue:
    """The patched rq.Queue recording the queue name and enqueue arguments."""

    def __init__(self, name: str, connection: Any = None) -> None:
        self.name = name
        self.connection = connection
        self.enqueued: list[tuple[Any, tuple[Any, ...]]] = []

    def enqueue(self, func: Any, *args: Any) -> object:
        self.enqueued.append((func, args))
        return object()


@pytest.fixture
def queue_wire(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Patch the F1 queue seam's backend: recorded enqueues, no live Valkey."""
    queue = _FakeQueue(queue_name("default"))
    holder: dict[str, Any] = {"queue": queue, "client_settings": []}

    def _fake_client(settings: Settings) -> object:
        holder["client_settings"].append(settings)
        return object()

    monkeypatch.setattr(invocation, "create_redis_client", _fake_client)
    monkeypatch.setattr(invocation.rq, "Queue", lambda name, connection=None: queue)
    return holder


def _cleanup_invocation() -> OperationInvocation:
    return OperationInvocation(
        kind=OperationKind.TASK_TERMINAL_FINALIZATION,
        payload=TerminalFinalizationPayload(workspace_id=str(_WORKSPACE_ID), task_id=str(_TASK_ID)),
    )


# --- the handler table and the direct handler invocation ------------------------


def test_the_finalization_handler_table_registers_exactly_the_finalization_kind() -> None:
    handlers = build_terminal_cleanup_operation_handlers()

    assert set(handlers) == {OperationKind.TASK_TERMINAL_FINALIZATION}


def test_the_finalization_handler_invokes_the_existing_service_with_the_scoped_identifiers(
    cleanup_service_harness: _CleanupServiceHarness,
) -> None:
    harness = cleanup_service_harness
    settings = _settings()
    runtime = _RecordingRuntime()
    dependencies = _dependencies(settings, runtime)
    resolved_pool = dependencies.database_pool()
    handler = build_terminal_cleanup_operation_handlers()[OperationKind.TASK_TERMINAL_FINALIZATION]

    handler(_cleanup_invocation().payload, dependencies=dependencies)

    assert harness.calls == [
        {
            "pool": resolved_pool,
            "runtime": runtime,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "dispatch": harness.calls[0]["dispatch"],
        }
    ]
    # The explicit cleanup dispatch selection (issue #344): a regular-mode
    # process with no observation host bound is the observation-unavailable
    # selection — never an inferred blocking fallback.
    assert harness.calls[0]["dispatch"] is not None
    assert (
        harness.calls[0]["dispatch"].selection
        is TerminalCleanupDispatchSelection.OBSERVATION_UNAVAILABLE
    )


# --- the F11 post-release queue wake (issue #257) ------------------------------


def test_the_finalization_handler_arranges_one_wake_only_for_a_real_release(
    cleanup_service_harness: _CleanupServiceHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = cleanup_service_harness
    arrangements: list[dict[str, Any]] = []

    def recording_arrange(queue_wake: Any, *, workspace_id: Any, origin: str) -> Any:
        arrangements.append({"seam": queue_wake, "workspace_id": workspace_id, "origin": origin})
        return None

    monkeypatch.setattr(task_cleanup_operations, "arrange_task_queue_wake", recording_arrange)
    settings = _settings()
    runtime = _RecordingRuntime()
    dependencies = _dependencies(settings, runtime)
    handler = build_terminal_cleanup_operation_handlers()[OperationKind.TASK_TERMINAL_FINALIZATION]

    # A finalization that released no occupancy invents no wake.
    harness.outcome = SimpleNamespace(kind="archived", task=None, released_session_bindings=())
    handler(_cleanup_invocation().payload, dependencies=dependencies)
    assert arrangements == []

    # A committed archival release arranges exactly one post-commit wake
    # through the executing process's F1 wake bridge.
    harness.outcome = SimpleNamespace(
        kind="archived", task=None, released_session_bindings=(object(), object())
    )
    handler(_cleanup_invocation().payload, dependencies=dependencies)

    assert len(arrangements) == 1
    assert arrangements[0]["workspace_id"] == _WORKSPACE_ID
    assert arrangements[0]["origin"] == "terminal_finalization_handler"
    assert isinstance(arrangements[0]["seam"], F1TaskQueueWakeSubmission)


# --- the submission bridge: F1 outcome mapping, truthfully -----------------------


@pytest.mark.parametrize(
    ("submission_result", "expected_outcome"),
    [
        pytest.param(
            SubmissionResult.accepted(),
            TerminalFinalizationSubmissionOutcome.ACCEPTED_FOR_DELIVERY,
            id="accepted",
        ),
        pytest.param(
            SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE),
            TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED,
            id="refused",
        ),
        pytest.param(
            SubmissionResult.uncertain(),
            TerminalFinalizationSubmissionOutcome.UNCERTAIN,
            id="uncertain",
        ),
    ],
)
def test_the_f1_finalization_submission_bridge_maps_the_outcome_1_to_1(
    submission_result: SubmissionResult,
    expected_outcome: TerminalFinalizationSubmissionOutcome,
) -> None:
    submitter = _RecordingSubmitter(outcome=submission_result)
    bridge = F1TerminalFinalizationSubmission(submitter)

    outcome = bridge.submit(workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)

    assert outcome is expected_outcome
    assert submitter.submissions == [_cleanup_invocation()]


# --- both execution modes over the same production handler table -----------------


def _drained_executor(settings: Settings) -> BoundedLocalExecutor:
    return BoundedLocalExecutor(
        workers=1,
        pending=2,
        handlers=invocation.build_production_handlers(),
        dependencies=_dependencies(settings, _RecordingRuntime()),
    )


def test_the_finalization_flows_through_the_regular_mode_executor_to_the_service(
    cleanup_service_harness: _CleanupServiceHarness,
    queue_wire: dict[str, Any],
) -> None:
    harness = cleanup_service_harness
    settings = _settings()
    executor = _drained_executor(settings)
    try:
        result = executor.submit(_cleanup_invocation())
        assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        assert harness.started.wait(timeout=5.0)
    finally:
        drain = executor.shutdown()

    assert drain.drained
    assert [call["workspace_id"] for call in harness.calls] == [_WORKSPACE_ID]
    assert [call["task_id"] for call in harness.calls] == [_TASK_ID]
    # The regular-mode executor resolves the explicit selection: no bound
    # observation host means the observation-unavailable skip, never an
    # inferred blocking fallback (issue #344).
    assert (
        harness.calls[0]["dispatch"].selection
        is TerminalCleanupDispatchSelection.OBSERVATION_UNAVAILABLE
    )
    # Regular mode never contacts the queue backend (issue #86 isolation):
    # the executor alone delivers the registered cleanup handler.
    assert queue_wire["client_settings"] == []
    assert queue_wire["queue"].enqueued == []


def test_the_finalization_wire_form_is_strictly_decoded_and_dispatched_in_queue_mode(
    cleanup_service_harness: _CleanupServiceHarness,
    queue_wire: dict[str, Any],
) -> None:
    harness = cleanup_service_harness
    settings = _settings(execution_mode=ExecutionMode.QUEUE)
    submitter = invocation.RQQueueOperationSubmission(settings)
    registrar = object()
    dependencies = _dependencies(settings, _RecordingRuntime(), detached_registrar=registrar)

    result = submitter.submit(_cleanup_invocation())

    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    assert len(queue_wire["queue"].enqueued) == 1
    func, args = queue_wire["queue"].enqueued[0]
    assert func == invocation._ENTRYPOINT_PATH
    (encoded,) = args
    wire_invocation = decode_operation_invocation(encoded)
    assert wire_invocation.kind is OperationKind.TASK_TERMINAL_FINALIZATION
    assert wire_invocation.payload == _cleanup_invocation().payload
    invoke_operation(
        wire_invocation,
        handlers=invocation.build_production_handlers(),
        dependencies=dependencies,
    )
    assert [call["workspace_id"] for call in harness.calls] == [_WORKSPACE_ID]
    # The queue-mode worker dispatches the cleanup detached through the #346
    # remote registrar proxy: the detached selection carries the resolved
    # registrar seam and the process's ONE post-commit completion handoff
    # (issues #344/#345/#347).
    dispatch = harness.calls[0]["dispatch"]
    assert dispatch is not None
    assert dispatch.selection is TerminalCleanupDispatchSelection.DETACHED_REGULAR
    assert dispatch.observation_registrar is registrar
    assert callable(dispatch.completion_handoff.handoff_settled_completion)
    assert queue_wire["client_settings"] == [settings]


# --- strict payload validation ----------------------------------------------------


def test_the_wire_payload_decodes_strictly_from_the_scoped_identifiers() -> None:
    wire_invocation = decode_operation_invocation(
        {
            "kind": "task_terminal_finalization",
            "payload": {"workspace_id": str(_WORKSPACE_ID), "task_id": str(_TASK_ID)},
        }
    )

    assert wire_invocation.payload == TerminalFinalizationPayload(
        workspace_id=str(_WORKSPACE_ID), task_id=str(_TASK_ID)
    )


def test_the_wire_payload_rejects_unexpected_fields_and_malformed_uuids() -> None:
    with pytest.raises(InvalidInvocationError):
        decode_operation_invocation(
            {
                "kind": "task_terminal_finalization",
                "payload": {
                    "workspace_id": str(_WORKSPACE_ID),
                    "task_id": str(_TASK_ID),
                    # An agent- or caller-supplied path is never a payload
                    # member: the executing service re-derives the exact
                    # checkout from durable state.
                    "checkout_directory": "/agent/invented",
                },
            }
        )
    with pytest.raises(InvalidInvocationError):
        decode_operation_invocation(
            {
                "kind": "task_terminal_finalization",
                "payload": {"workspace_id": "not-a-uuid", "task_id": str(_TASK_ID)},
            }
        )
    with pytest.raises(InvalidInvocationError):
        TerminalFinalizationPayload(workspace_id=str(_WORKSPACE_ID), task_id="")


def test_the_regular_mode_dispatch_carries_the_completion_handoff() -> None:
    """The detached-regular dispatch (issue #345) carries the process's ONE
    post-commit completion handoff beside its observation registrar, so a
    staged early completion consumed at the acknowledgement attach routes the
    owning finalization's archival; the non-detached selections carry
    neither."""
    settings = _settings()
    pool: Any = object()
    runtime: Any = _RecordingRuntime()
    observer: Any = object()
    dependencies = OperationDependencies(
        settings=settings,
        database_pool_provider=lambda _settings: pool,
        terminal_cleanup_runtime_provider=lambda _settings: runtime,
        detached_turn_observer_provider=lambda _settings: observer,
    )

    dispatch = task_cleanup_operations.resolve_terminal_cleanup_dispatch(dependencies)

    assert dispatch.selection is TerminalCleanupDispatchSelection.DETACHED_REGULAR
    assert dispatch.observation_registrar is not None
    assert dispatch.completion_handoff is not None


def test_the_observation_unavailable_dispatch_carries_no_completion_handoff() -> None:
    settings = _settings()
    pool: Any = object()
    runtime: Any = _RecordingRuntime()
    dependencies = OperationDependencies(
        settings=settings,
        database_pool_provider=lambda _settings: pool,
        terminal_cleanup_runtime_provider=lambda _settings: runtime,
    )

    dispatch = task_cleanup_operations.resolve_terminal_cleanup_dispatch(dependencies)

    assert dispatch.selection is TerminalCleanupDispatchSelection.OBSERVATION_UNAVAILABLE
    assert dispatch.observation_registrar is None
    assert dispatch.completion_handoff is None
