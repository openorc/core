"""Tests for the shared operation invocation and submission boundary (issue #86).

Deterministic and fake-backed: handlers and dependencies are injected
recording doubles, the queue transport is exercised through monkeypatched
redis/rq seams, and no live Postgres, Valkey, or GitHub is touched.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from openorc.config import ExecutionMode, Settings
from openorc.services.errors import InvalidCommandError
from openorc.workers import invocation
from openorc.workers.invocation import (
    ExecutionModeDependencyError,
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    ReconciliationSweepPayload,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
    TaskScope,
    WebhookDeliveryDispatchPayload,
    WorkflowRoleRef,
    build_operation_submitter,
    decode_operation_invocation,
    execute_operation_job,
    invoke_operation,
)

UUID_A = "0f1c2d3e-4a5b-6c7d-8e9f-000102030405"
UUID_B = "1f2c3d4e-5a6b-7c8d-9eaf-010203040506"


@dataclass
class RecordingHandler:
    """Injected handler double recording every dispatch."""

    calls: list[tuple[object, OperationDependencies]] = field(default_factory=list)
    error: Exception | None = None

    def __call__(self, payload: Any, *, dependencies: OperationDependencies) -> None:
        self.calls.append((payload, dependencies))
        if self.error is not None:
            raise self.error


@dataclass
class RecordingProvider:
    """Injected provider double recording resolution and returning a sentinel."""

    sentinel: Any = None
    calls: list[Settings] = field(default_factory=list)

    def __call__(self, settings: Settings) -> Any:
        self.calls.append(settings)
        return self.sentinel


def make_dependencies(
    settings: Settings,
) -> tuple[OperationDependencies, RecordingProvider, RecordingProvider]:
    pool_provider = RecordingProvider(sentinel=object())
    github_provider = RecordingProvider(sentinel=object())
    dependencies = OperationDependencies(
        settings=settings,
        database_pool_provider=pool_provider,
        github_client_provider=github_provider,
    )
    return dependencies, pool_provider, github_provider


# --- envelope round trips ---


def test_webhook_delivery_dispatch_payload_round_trips_exactly() -> None:
    original = OperationInvocation(
        kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
        payload=WebhookDeliveryDispatchPayload(
            delivery_guid=" 123e4567-e89b-12d3-a456-426614174000 "
        ),
    )

    decoded = decode_operation_invocation(original.encode())

    assert decoded == original
    assert isinstance(decoded.payload, WebhookDeliveryDispatchPayload)
    assert decoded.payload.delivery_guid == " 123e4567-e89b-12d3-a456-426614174000 "


def test_reconciliation_sweep_payload_round_trips_with_a_tick() -> None:
    original = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=5),
    )

    decoded = decode_operation_invocation(original.encode())

    assert decoded == original


def test_reconciliation_sweep_payload_round_trips_without_a_tick() -> None:
    original = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=None),
    )

    decoded = decode_operation_invocation(original.encode())

    assert decoded == original
    assert isinstance(decoded.payload, ReconciliationSweepPayload)
    assert decoded.payload.sweep_tick is None


def test_encoded_wire_form_is_stable_json() -> None:
    invocation_to_encode = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=7),
    )

    encoded = json.loads(invocation_to_encode.encode())

    assert encoded == {"kind": "github_reconciliation_sweep", "payload": {"sweep_tick": 7}}


# --- task-scoped component machinery (envelope only, no workflow wiring) ---


def test_task_scope_component_round_trips_and_canonicalizes() -> None:
    scope = TaskScope(workspace_id=UUID_A.upper(), task_id=UUID_B, role=WorkflowRoleRef.PRODUCER)

    decoded = TaskScope.decode(scope.encode())

    assert decoded.workspace_id == UUID_A
    assert decoded.task_id == UUID_B
    assert decoded.role.value == "producer"
    assert decoded.encode() == {
        "workspace_id": UUID_A,
        "task_id": UUID_B,
        "role": "producer",
    }


@pytest.mark.parametrize(
    "raw",
    [
        {},
        {"workspace_id": UUID_A, "task_id": UUID_B},
        {"workspace_id": UUID_A, "task_id": UUID_B, "role": "producer", "extra": 1},
        {"workspace_id": "not-a-uuid", "task_id": UUID_B, "role": "producer"},
        {"workspace_id": UUID_A, "task_id": UUID_B, "role": "owner"},
        {"workspace_id": UUID_A, "task_id": UUID_B, "role": 3},
        [UUID_A, UUID_B, "producer"],
    ],
)
def test_task_scope_component_strictly_rejects_malformed_input(raw: object) -> None:
    with pytest.raises(InvalidCommandError):
        TaskScope.decode(raw)


@pytest.mark.parametrize(
    "raw",
    [
        '{"kind": "definitely-not-a-kind", "payload": {}}',
        '{"kind": "github_reconciliation_sweep", "payload": {}, "extra": true}',
        '{"payload": {}}',
        '{"kind": "github_reconciliation_sweep"}',
        '{"kind": "github_reconciliation_sweep", "payload": {"sweep_tick": -1}}',
        '{"kind": "github_reconciliation_sweep", "payload": {"sweep_tick": true}}',
        '{"kind": "github_reconciliation_sweep", "payload": {"sweep_tick": "3"}}',
        '{"kind": "github_reconciliation_sweep", "payload": {"tick": 3}}',
        '{"kind": "github_webhook_delivery_dispatch", "payload": {}}',
        '{"kind": "github_webhook_delivery_dispatch", "payload": {"delivery_guid": ""}}',
        '{"kind": "github_webhook_delivery_dispatch", "payload": {"delivery_guid": 7}}',
        '{"kind": "github_webhook_delivery_dispatch", "payload": {"delivery_guid": "x",'
        ' "role": "producer"}}',
        '{"kind": "github_webhook_delivery_dispatch", "payload": []}',
        '["not", "an", "object"]',
        "not json at all",
    ],
)
def test_strict_decoding_rejects_malformed_invocations(raw: str) -> None:
    with pytest.raises(InvalidInvocationError):
        decode_operation_invocation(raw)


def test_decoding_rejects_a_non_string_kind_before_any_effect() -> None:
    with pytest.raises(InvalidInvocationError):
        decode_operation_invocation({"kind": 3, "payload": {}})


def test_decoding_accepts_an_already_parsed_mapping() -> None:
    decoded = decode_operation_invocation(
        {"kind": "github_webhook_delivery_dispatch", "payload": {"delivery_guid": "abc"}}
    )

    assert isinstance(decoded.payload, WebhookDeliveryDispatchPayload)
    assert decoded.payload.delivery_guid == "abc"


# --- dispatcher ---


def test_invoke_operation_reaches_the_injected_handler(
    settings_factory: Any,
) -> None:
    settings = settings_factory()
    dependencies, pool_provider, github_provider = make_dependencies(settings)
    handler = RecordingHandler()
    decoded = decode_operation_invocation(
        OperationInvocation(
            kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
            payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-1"),
        ).encode()
    )

    invoke_operation(decoded, handlers={decoded.kind: handler}, dependencies=dependencies)

    assert len(handler.calls) == 1
    payload, resolved = handler.calls[0]
    assert isinstance(payload, WebhookDeliveryDispatchPayload)
    assert payload == WebhookDeliveryDispatchPayload(delivery_guid="guid-1")
    assert resolved is dependencies
    # Providers resolve only when the handler requests them.
    assert pool_provider.calls == []
    assert github_provider.calls == []


def test_invoke_operation_resolves_dependencies_only_on_request(
    settings_factory: Any,
) -> None:
    settings = settings_factory()
    dependencies, pool_provider, github_provider = make_dependencies(settings)

    def handler(payload: Any, *, dependencies: OperationDependencies) -> None:
        assert dependencies.database_pool() is pool_provider.sentinel
        assert dependencies.github_client() is github_provider.sentinel

    decoded = decode_operation_invocation(
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=None),
        ).encode()
    )

    invoke_operation(decoded, handlers={decoded.kind: handler}, dependencies=dependencies)

    assert pool_provider.calls == [settings]
    assert github_provider.calls == [settings]


def test_invoke_operation_fails_closed_for_an_unregistered_kind(
    settings_factory: Any,
) -> None:
    settings = settings_factory()
    dependencies, _pool, _github = make_dependencies(settings)
    handler = RecordingHandler()
    decoded = decode_operation_invocation(
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=None),
        ).encode()
    )

    with pytest.raises(InvalidInvocationError, match="no handler is registered"):
        invoke_operation(decoded, handlers={}, dependencies=dependencies)

    assert handler.calls == []


def test_production_handler_table_stays_fail_closed_until_f3a_f3b() -> None:
    assert invocation.build_production_handlers() == {}


# --- submission seams ---


@dataclass
class FakeRQQueue:
    """Recording rq.Queue double."""

    calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    error: Exception | None = None

    def enqueue(self, entrypoint: str, *args: object) -> None:
        self.calls.append((entrypoint, args))
        if self.error is not None:
            raise self.error


@pytest.fixture
def queue_mode_settings(settings_factory: Any) -> Settings:
    return settings_factory(
        execution_mode=ExecutionMode.QUEUE, valkey_url="redis://127.0.0.1:6379/9"
    )


def test_queue_submission_enqueues_the_encoded_invocation(
    queue_mode_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = FakeRQQueue()
    clients: list[Settings] = []

    def fake_client_factory(settings: Settings) -> Any:
        clients.append(settings)
        return object()

    monkeypatch.setattr(invocation, "create_redis_client", fake_client_factory)
    monkeypatch.setattr(invocation.rq, "Queue", lambda name, connection: queue)
    submitter = invocation.RQQueueOperationSubmission(queue_mode_settings)
    wire = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=3),
    )

    result = submitter.submit(wire)

    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    assert result.refusal_reason is None
    assert clients == [queue_mode_settings]
    assert queue.calls == [
        (
            "openorc.workers.invocation.execute_operation_job",
            (wire.encode(),),
        )
    ]


def test_queue_submission_classifies_a_queue_timeout_as_uncertain(
    queue_mode_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = FakeRQQueue(error=RedisTimeoutError("timeout after send"))
    monkeypatch.setattr(invocation, "create_redis_client", lambda _settings: object())
    monkeypatch.setattr(invocation.rq, "Queue", lambda name, connection: queue)
    submitter = invocation.RQQueueOperationSubmission(queue_mode_settings)

    result = submitter.submit(
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=None),
        )
    )

    assert result.outcome is SubmissionOutcome.UNCERTAIN
    assert result.refusal_reason is None


def test_queue_submission_classifies_redis_errors_as_known_refusal(
    queue_mode_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = FakeRQQueue(error=RedisError("server said no"))
    monkeypatch.setattr(invocation, "create_redis_client", lambda _settings: object())
    monkeypatch.setattr(invocation.rq, "Queue", lambda name, connection: queue)
    submitter = invocation.RQQueueOperationSubmission(queue_mode_settings)

    result = submitter.submit(
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=None),
        )
    )

    assert result.outcome is SubmissionOutcome.KNOWN_REFUSED
    assert result.refusal_reason is SubmissionRefusalReason.QUEUE_UNAVAILABLE


def test_queue_submission_refuses_without_a_configured_url(
    settings_factory: Any,
) -> None:
    # No implicit localhost fallback: a missing explicitly configured
    # VALKEY_URL is a definitive refusal, never a crash or a guess. The real
    # client factory fails fast before any backend contact.
    settings = settings_factory(execution_mode=ExecutionMode.QUEUE, valkey_url=None)
    submitter = invocation.RQQueueOperationSubmission(settings)

    result = submitter.submit(
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=None),
        )
    )

    assert result.outcome is SubmissionOutcome.KNOWN_REFUSED
    assert result.refusal_reason is SubmissionRefusalReason.QUEUE_UNAVAILABLE


@dataclass
class FakeSubmitter:
    """A minimal OperationSubmitter double for the factory selection test."""

    submitted: list[OperationInvocation] = field(default_factory=list)

    def submit(self, invocation: OperationInvocation) -> Any:
        self.submitted.append(invocation)
        return SubmissionResult.accepted()


def test_submitter_factory_uses_the_executor_in_regular_mode(
    settings_factory: Any,
) -> None:
    settings = settings_factory()
    executor = FakeSubmitter()

    submitter = build_operation_submitter(settings, local_executor=executor)

    assert submitter is executor


def test_submitter_factory_requires_an_executor_in_regular_mode(
    settings_factory: Any,
) -> None:
    with pytest.raises(ExecutionModeDependencyError, match="bounded local executor"):
        build_operation_submitter(settings_factory())


def test_submitter_factory_builds_the_queue_seam_in_queue_mode(
    queue_mode_settings: Settings,
) -> None:
    submitter = build_operation_submitter(queue_mode_settings)

    assert isinstance(submitter, invocation.RQQueueOperationSubmission)


# --- RQ entrypoint ---


def test_entrypoint_reaches_the_injected_handler_through_the_same_dispatcher(
    settings_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory(
        execution_mode=ExecutionMode.QUEUE, valkey_url="redis://127.0.0.1:6379/9"
    )
    handler = RecordingHandler()
    monkeypatch.setattr(invocation.Settings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(
        invocation,
        "build_production_handlers",
        lambda: {OperationKind.GITHUB_RECONCILIATION_SWEEP: handler},
    )
    dependencies, pool_provider, github_provider = make_dependencies(settings)
    monkeypatch.setattr(
        invocation, "build_default_operation_dependencies", lambda _settings: dependencies
    )
    wire = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=11),
    )

    execute_operation_job(wire.encode())

    assert len(handler.calls) == 1
    payload, resolved = handler.calls[0]
    assert payload == ReconciliationSweepPayload(sweep_tick=11)
    assert resolved is dependencies
    assert pool_provider.calls == []
    assert github_provider.calls == []


def test_entrypoint_rejects_a_malformed_payload_before_any_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_from_env(cls: type) -> Any:
        raise AssertionError("settings must not be read for a malformed payload")

    monkeypatch.setattr(invocation.Settings, "from_env", classmethod(unexpected_from_env))

    with pytest.raises(InvalidInvocationError):
        execute_operation_job('{"kind": "no-such-kind", "payload": {}}')


def test_entrypoint_wraps_handler_failures_with_safe_classification(
    settings_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory(
        execution_mode=ExecutionMode.QUEUE, valkey_url="redis://127.0.0.1:6379/9"
    )
    handler = RecordingHandler(error=RuntimeError("secret-bearing internal detail"))
    monkeypatch.setattr(invocation.Settings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(
        invocation,
        "build_production_handlers",
        lambda: {OperationKind.GITHUB_RECONCILIATION_SWEEP: handler},
    )
    dependencies, _pool, _github = make_dependencies(settings)
    monkeypatch.setattr(
        invocation, "build_default_operation_dependencies", lambda _settings: dependencies
    )
    wire = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=None),
    )

    with pytest.raises(invocation.OperationExecutionError) as raised:
        execute_operation_job(wire.encode())

    message = str(raised.value)
    assert "github_reconciliation_sweep" in message
    assert "RuntimeError" in message
    assert "secret-bearing internal detail" not in message


def test_entrypoint_fails_closed_for_an_unregistered_kind(
    settings_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory(
        execution_mode=ExecutionMode.QUEUE, valkey_url="redis://127.0.0.1:6379/9"
    )
    monkeypatch.setattr(invocation.Settings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(invocation, "build_production_handlers", lambda: {})
    wire = OperationInvocation(
        kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
        payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-2"),
    )

    with pytest.raises(InvalidInvocationError, match="no handler is registered"):
        execute_operation_job(wire.encode())
