"""Tests for the OpenOrc API application factory and health transport."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from openorc import __version__
from openorc.api import app as app_module
from openorc.api.app import app as module_app
from openorc.api.app import create_app
from openorc.config import ExecutionMode, Settings
from openorc.observability import shutdown_observability
from openorc.services.task_detached_turn_observation import DetachedTurnObservationHost
from openorc.workers.invocation import (
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    RQQueueOperationSubmission,
    SubmissionOutcome,
    SubmissionRefusalReason,
    WebhookDeliveryDispatchPayload,
)
from openorc.workers.local_executor import BoundedLocalExecutor
from tests.test_services_task_detached_turn_observation import (
    ScriptedConnection,
    _pool,
    _sql_has,
)


def test_module_level_app_is_compatible_with_uvicorn_import_string() -> None:
    assert isinstance(module_app, FastAPI)


def test_create_app_uses_injected_settings(settings_factory: Callable[..., Settings]) -> None:
    settings = settings_factory(api_port=3998)

    app = create_app(settings)

    assert isinstance(app, FastAPI)
    assert app.state.settings is settings


def test_create_app_instances_are_independent(settings_factory: Callable[..., Settings]) -> None:
    first = create_app(settings_factory())
    second = create_app(settings_factory())

    assert first is not second


def test_create_app_is_executor_free(settings_factory: Callable[..., Settings]) -> None:
    # No executor is created by module import, create_app(), or a request;
    # ownership starts exactly once per serving process in the lifespan.
    app = create_app(settings_factory())

    assert not hasattr(app.state, "operation_executor")
    assert not hasattr(app.state, "operation_submitter")
    assert not hasattr(app.state, "detached_turn_observer")


def test_healthz_returns_deterministic_payload(settings_factory: Callable[..., Settings]) -> None:
    client = TestClient(create_app(settings_factory()))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "openorc-api",
        "version": __version__,
    }


def test_lifespan_owns_exactly_one_executor_in_regular_mode(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    app = create_app(settings)

    with TestClient(app) as client:
        executor = app.state.operation_executor
        assert isinstance(executor, BoundedLocalExecutor)
        assert app.state.operation_submitter is executor
        # The process-owned hub-run observation host (issue #323) is started
        # beside the executor and disposed deterministically at teardown.
        observer = app.state.detached_turn_observer
        assert isinstance(observer, DetachedTurnObservationHost)
        assert observer.disposed is False
        assert client.get("/healthz").status_code == 200

    # Teardown stopped admission: the executor refuses further submissions.
    refused = executor.submit(
        OperationInvocation(
            kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
            payload=WebhookDeliveryDispatchPayload(delivery_guid="after-shutdown"),
        )
    )
    assert refused.outcome is SubmissionOutcome.KNOWN_REFUSED
    assert refused.refusal_reason is SubmissionRefusalReason.EXECUTOR_SHUTTING_DOWN
    assert observer.disposed is True
    shutdown_observability()


def test_lifespan_starts_no_executor_in_queue_mode(
    settings_factory: Callable[..., Settings], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The queue-mode lifespan hosts exactly one long-lived detached-turn
    # observation host (issue #346); its start-up restart reconciliation
    # enumerates the durable outstanding receipts, so the scripted pool
    # answers the bounded candidate read.
    conn = ScriptedConnection()
    conn.on_repeat(
        _sql_has("from openorc.task_hub_run_receipts r", "limit %s"),
        [],
    )
    settings = settings_factory(
        execution_mode=ExecutionMode.QUEUE,
        valkey_url="redis://127.0.0.1:6379/9",
        queue_observer_registration_url="http://127.0.0.1:3999",
        queue_observer_registration_secret="deployment-registration-secret",
    )
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: OperationDependencies(
            settings=settings,
            database_pool_provider=lambda _settings: _pool(conn),
        ),
        raising=True,
    )
    app = create_app(settings)

    with TestClient(app) as client:
        assert app.state.operation_executor is None
        assert isinstance(app.state.operation_submitter, RQQueueOperationSubmission)
        # The one long-lived observation host is hosted by the serving
        # API/control-plane process, never inside a workhorse (issue #346).
        observer = app.state.detached_turn_observer
        assert isinstance(observer, DetachedTurnObservationHost)
        assert observer.disposed is False
        assert client.get("/healthz").status_code == 200

    assert observer.disposed is True
    shutdown_observability()
