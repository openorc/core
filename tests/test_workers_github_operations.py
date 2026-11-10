"""Deterministic tests for the F9 GitHub operation composition (issue #255).

The registered GitHub F1 handlers invoke the existing shared GitHub
application services with process-local dependencies and carry no workflow
decisions of their own. Both execution modes are proven over the same
production handler table: regular mode through the bounded local executor
(no queue backend is ever contacted) and queue mode through the F1 queue
submission seam, whose recorded wire form is strict-decoded back through
the shared dispatcher. The dispatch submission bridge maps F1 submission
outcomes onto the dispatch service's bounded outcome vocabulary without
inventing completion or replaying, and the explicit sweep submission seam
returns the actual F1 result with the tick passed through unchanged. No
live Postgres, Valkey, GitHub, or Cline is touched.
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from openorc.config import ExecutionMode, Settings
from openorc.services.github_webhook_dispatch import EnqueueOutcome
from openorc.workers import github_operations, invocation
from openorc.workers.github_operations import (
    F1WebhookDispatchSubmission,
    build_github_operation_handlers,
    submit_github_reconciliation_sweep,
)
from openorc.workers.invocation import (
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    ReconciliationSweepPayload,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
    WebhookDeliveryDispatchPayload,
    decode_operation_invocation,
    invoke_operation,
)
from openorc.workers.local_executor import BoundedLocalExecutor
from openorc.workers.queues import queue_name


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


class _GitHubServiceHarness:
    """Recording fakes for the two shared GitHub application services."""

    def __init__(self) -> None:
        # Recording fakes: Any-typed stand-ins for the DatabasePool and
        # GitHubAppClient protocols the dependency providers resolve.
        self.pool: Any = object()
        self.github: Any = object()
        self.dispatch_calls: list[dict[str, Any]] = []
        self.sweep_calls: list[dict[str, Any]] = []
        self.service_error: Exception | None = None
        self.dispatch_started = threading.Event()
        self.sweep_started = threading.Event()

    def record_dispatch(self, pool: Any, github: Any, *, delivery_guid: str) -> None:
        self.dispatch_calls.append({"pool": pool, "github": github, "delivery_guid": delivery_guid})
        self.dispatch_started.set()
        if self.service_error is not None:
            raise self.service_error

    def record_sweep(
        self, pool: Any, github: Any, settings: Settings, *, sweep_tick: int | None
    ) -> None:
        self.sweep_calls.append(
            {"pool": pool, "github": github, "settings": settings, "sweep_tick": sweep_tick}
        )
        self.sweep_started.set()
        if self.service_error is not None:
            raise self.service_error


@pytest.fixture
def github_service_harness(monkeypatch: pytest.MonkeyPatch) -> _GitHubServiceHarness:
    """Patch the two shared GitHub services at the composition-module boundary."""
    harness = _GitHubServiceHarness()
    monkeypatch.setattr(
        github_operations, "dispatch_github_webhook_delivery", harness.record_dispatch
    )
    monkeypatch.setattr(github_operations, "run_github_reconciliation_sweep", harness.record_sweep)
    return harness


def _dependencies(settings: Settings, harness: _GitHubServiceHarness) -> OperationDependencies:
    """Process-local dependencies resolving the recording pool/GitHub client."""
    return OperationDependencies(
        settings=settings,
        database_pool_provider=lambda _settings: harness.pool,
        github_client_provider=lambda _settings: harness.github,
    )


class _RecordingSubmitter:
    """The scripted F1 submitter: records invocations, returns one outcome."""

    def __init__(self, result: SubmissionResult | None = None) -> None:
        self.submissions: list[OperationInvocation] = []
        self.result = result if result is not None else SubmissionResult.accepted()

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        self.submissions.append(invocation)
        return self.result


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


def _drained_executor(settings: Settings, harness: _GitHubServiceHarness) -> BoundedLocalExecutor:
    """The regular-mode executor over the full production handler table."""
    return BoundedLocalExecutor(
        workers=1,
        pending=2,
        handlers=invocation.build_production_handlers(),
        dependencies=_dependencies(settings, harness),
    )


def _recorded_queue_invocation(holder: dict[str, Any]) -> OperationInvocation:
    """Decode the single recorded F1 queue wire form."""
    assert len(holder["queue"].enqueued) == 1
    func, args = holder["queue"].enqueued[0]
    assert func == invocation._ENTRYPOINT_PATH
    (encoded,) = args
    return decode_operation_invocation(encoded)


# --- registered handlers invoke the existing shared services --------------------


def test_the_github_handler_table_registers_exactly_the_two_task_less_kinds() -> None:
    handlers = build_github_operation_handlers()

    assert set(handlers) == {
        OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
        OperationKind.GITHUB_RECONCILIATION_SWEEP,
    }


def test_the_dispatch_handler_invokes_the_existing_service_with_only_the_guid(
    github_service_harness: _GitHubServiceHarness,
) -> None:
    settings = _settings()
    dependencies = _dependencies(settings, github_service_harness)
    handler = build_github_operation_handlers()[OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH]

    handler(WebhookDeliveryDispatchPayload(delivery_guid="guid-1"), dependencies=dependencies)

    assert github_service_harness.dispatch_calls == [
        {
            "pool": github_service_harness.pool,
            "github": github_service_harness.github,
            "delivery_guid": "guid-1",
        }
    ]
    assert github_service_harness.sweep_calls == []


def test_the_sweep_handler_invokes_the_existing_service_with_the_settings_and_tick(
    github_service_harness: _GitHubServiceHarness,
) -> None:
    settings = _settings()
    dependencies = _dependencies(settings, github_service_harness)
    handler = build_github_operation_handlers()[OperationKind.GITHUB_RECONCILIATION_SWEEP]

    handler(ReconciliationSweepPayload(sweep_tick=7), dependencies=dependencies)
    handler(ReconciliationSweepPayload(sweep_tick=None), dependencies=dependencies)

    assert github_service_harness.sweep_calls == [
        {
            "pool": github_service_harness.pool,
            "github": github_service_harness.github,
            "settings": settings,
            "sweep_tick": 7,
        },
        {
            "pool": github_service_harness.pool,
            "github": github_service_harness.github,
            "settings": settings,
            "sweep_tick": None,
        },
    ]
    assert github_service_harness.dispatch_calls == []


# --- regular mode: the bounded local executor, no queue backend -----------------


def test_webhook_dispatch_flows_through_the_regular_mode_executor_to_the_service(
    github_service_harness: _GitHubServiceHarness, queue_wire: dict[str, Any]
) -> None:
    settings = _settings()
    executor = _drained_executor(settings, github_service_harness)
    try:
        result = executor.submit(
            OperationInvocation(
                kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
                payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-1"),
            )
        )
        assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        assert github_service_harness.dispatch_started.wait(timeout=5.0)
    finally:
        drain = executor.shutdown()

    assert drain.drained
    assert github_service_harness.dispatch_calls == [
        {
            "pool": github_service_harness.pool,
            "github": github_service_harness.github,
            "delivery_guid": "guid-1",
        }
    ]
    # Regular mode never contacts the queue backend (issue #86 isolation):
    # the executor alone delivers the registered GitHub handler.
    assert queue_wire["client_settings"] == []
    assert queue_wire["queue"].enqueued == []


def test_the_explicit_sweep_seam_flows_through_the_regular_mode_executor(
    github_service_harness: _GitHubServiceHarness, queue_wire: dict[str, Any]
) -> None:
    settings = _settings()
    executor = _drained_executor(settings, github_service_harness)
    try:
        full = submit_github_reconciliation_sweep(executor, sweep_tick=None)
        assert full.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        assert github_service_harness.sweep_started.wait(timeout=5.0)
        partitioned = submit_github_reconciliation_sweep(executor, sweep_tick=3)
        assert partitioned.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    finally:
        drain = executor.shutdown()

    assert drain.drained
    # The explicit full sweep and the partition tick pass through unchanged;
    # the tick is a scheduling position, never authority.
    assert [call["sweep_tick"] for call in github_service_harness.sweep_calls] == [None, 3]
    assert github_service_harness.dispatch_calls == []
    assert queue_wire["client_settings"] == []


def test_a_handler_failure_is_contained_and_never_reported_as_success(
    github_service_harness: _GitHubServiceHarness, caplog: pytest.LogCaptureFixture
) -> None:
    github_service_harness.service_error = RuntimeError("payload fragments and SECRET content")
    executor = _drained_executor(_settings(), github_service_harness)
    try:
        result = executor.submit(
            OperationInvocation(
                kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
                payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-1"),
            )
        )
        assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        assert github_service_harness.dispatch_started.wait(timeout=5.0)
    finally:
        drain = executor.shutdown()

    # The failure never escaped the executor and never became reported
    # success; only the safe exception type name reached the log record.
    assert drain.drained
    assert github_service_harness.dispatch_calls != []
    matching = [
        record for record in caplog.records if record.name == "openorc.workers.local_executor"
    ]
    assert len(matching) == 1
    assert "RuntimeError" in matching[0].getMessage()
    assert "SECRET content" not in matching[0].getMessage()
    assert "payload fragments" not in matching[0].getMessage()


# --- queue mode: the F1 queue submission wire form ------------------------------


def test_webhook_dispatch_flows_through_the_queue_wire_form_to_the_service(
    github_service_harness: _GitHubServiceHarness, queue_wire: dict[str, Any]
) -> None:
    settings = _settings(execution_mode=ExecutionMode.QUEUE)
    submitter = invocation.RQQueueOperationSubmission(settings)
    dependencies = _dependencies(settings, github_service_harness)

    result = submitter.submit(
        OperationInvocation(
            kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
            payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-1"),
        )
    )

    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    # The recorded wire form strict-decodes back through the shared
    # dispatcher and reaches the same registered handler and service.
    wire_invocation = _recorded_queue_invocation(queue_wire)
    assert wire_invocation.kind is OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH
    # Only the delivery GUID travels in the invocation.
    assert wire_invocation.payload == WebhookDeliveryDispatchPayload(delivery_guid="guid-1")
    invoke_operation(
        wire_invocation,
        handlers=invocation.build_production_handlers(),
        dependencies=dependencies,
    )
    assert github_service_harness.dispatch_calls == [
        {
            "pool": github_service_harness.pool,
            "github": github_service_harness.github,
            "delivery_guid": "guid-1",
        }
    ]
    assert queue_wire["client_settings"] == [settings]


def test_the_explicit_sweep_seam_flows_through_the_queue_wire_form(
    github_service_harness: _GitHubServiceHarness, queue_wire: dict[str, Any]
) -> None:
    settings = _settings(execution_mode=ExecutionMode.QUEUE)
    submitter = invocation.RQQueueOperationSubmission(settings)
    dependencies = _dependencies(settings, github_service_harness)

    result = submit_github_reconciliation_sweep(submitter, sweep_tick=None)

    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    wire_invocation = _recorded_queue_invocation(queue_wire)
    assert wire_invocation.kind is OperationKind.GITHUB_RECONCILIATION_SWEEP
    assert wire_invocation.payload == ReconciliationSweepPayload(sweep_tick=None)
    invoke_operation(
        wire_invocation,
        handlers=invocation.build_production_handlers(),
        dependencies=dependencies,
    )
    assert github_service_harness.sweep_calls == [
        {
            "pool": github_service_harness.pool,
            "github": github_service_harness.github,
            "settings": settings,
            "sweep_tick": None,
        }
    ]


# --- the dispatch submission bridge: F1 outcome mapping, truthfully ------------


@pytest.mark.parametrize(
    ("submission_result", "expected_outcome"),
    [
        (SubmissionResult.accepted(), EnqueueOutcome.ENQUEUED),
        (
            SubmissionResult.refused(SubmissionRefusalReason.EXECUTOR_SATURATED),
            EnqueueOutcome.KNOWN_FAILED,
        ),
        (
            SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE),
            EnqueueOutcome.KNOWN_FAILED,
        ),
        (SubmissionResult.uncertain(), EnqueueOutcome.UNCERTAIN),
    ],
)
def test_the_bridge_maps_f1_outcomes_onto_the_dispatch_submission_outcomes(
    submission_result: SubmissionResult, expected_outcome: EnqueueOutcome
) -> None:
    submitter = _RecordingSubmitter(submission_result)
    bridge = F1WebhookDispatchSubmission(submitter)

    outcome = bridge.enqueue("guid-1")

    assert outcome is expected_outcome
    # Exactly one invocation traveled, carrying only the delivery GUID: the
    # bridge never retried a refused or uncertain submission, and acceptance
    # is not completion.
    assert submitter.submissions == [
        OperationInvocation(
            kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
            payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-1"),
        )
    ]


# --- the explicit sweep submission seam: truthful results ----------------------


def test_the_sweep_seam_preserves_the_actual_f1_submission_result() -> None:
    refused = SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE)
    submitter = _RecordingSubmitter(refused)

    result = submit_github_reconciliation_sweep(submitter, sweep_tick=None)

    # The actual F1 result — a known refusal is never fabricated completion,
    # and the durable recovery path stays the only recovery path.
    assert result is refused
    assert submitter.submissions == [
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=None),
        )
    ]


@pytest.mark.parametrize("bad_tick", [-1, True, "0", 1.5])
def test_a_malformed_tick_is_refused_before_any_submission(bad_tick: Any) -> None:
    submitter = _RecordingSubmitter()

    with pytest.raises(InvalidInvocationError):
        submit_github_reconciliation_sweep(submitter, sweep_tick=bad_tick)  # type: ignore[arg-type]

    # The malformed scheduling position never became a submission.
    assert submitter.submissions == []
