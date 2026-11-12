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
from typing import Any
from uuid import UUID

import pytest

from openorc.config import ExecutionMode, Settings
from openorc.services.task_terminal_finalization import TerminalFinalizationSubmissionOutcome
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

    def __call__(
        self,
        pool: Any,
        runtime: Any,
        *,
        workspace_id: UUID,
        task_id: UUID,
    ) -> Any:
        self.calls.append(
            {
                "pool": pool,
                "runtime": runtime,
                "workspace_id": workspace_id,
                "task_id": task_id,
            }
        )
        self.started.set()
        if self.error is not None:
            raise self.error


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


def _dependencies(settings: Settings, runtime: Any) -> OperationDependencies:
    pool: Any = object()
    github: Any = object()
    return OperationDependencies(
        settings=settings,
        database_pool_provider=lambda _settings: pool,
        github_client_provider=lambda _settings: github,
        continuation_submitter_provider=lambda _settings: _RecordingSubmitter(),
        terminal_cleanup_runtime_provider=lambda _settings: runtime,
    )


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
        }
    ]


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
    dependencies = _dependencies(settings, _RecordingRuntime())

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
