"""Lifespan tests for the process-local notification listener (issue #397).

The serving lifespan owns exactly one process-local notification listener
per serving process in either execution mode, bound on ``app.state``, and
never at import or factory construction. A module-level factory seam injects
a deterministic fake listener; no database and no real connection is touched.
The listener's entire started lifetime is enclosed by a cleanup boundary, so
a startup failure after ``start()`` still reaches the clean stop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from fastapi.testclient import TestClient

from openorc.api import app as app_module
from openorc.api.app import create_app
from openorc.config import ExecutionMode, Settings
from openorc.observability import shutdown_observability
from openorc.workers.invocation import OperationDependencies
from tests.test_services_task_detached_turn_observation import (
    ScriptedConnection,
    _pool,
    _sql_has,
)


class FakeNotificationListener:
    """Deterministic lifespan seam: records start/stop, never touches the network."""

    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0
        self.built_with: list[Settings] = []

    def start(self) -> None:
        self.start_calls += 1

    def stop(self) -> None:
        self.stop_calls += 1


def _install_listener_factory(
    monkeypatch: pytest.MonkeyPatch, listener: FakeNotificationListener
) -> None:
    def factory(settings: Settings) -> FakeNotificationListener:
        listener.built_with.append(settings)
        return listener

    monkeypatch.setattr(app_module, "_build_notification_listener", factory, raising=True)


def test_create_app_starts_no_notification_listener(
    settings_factory: Callable[..., Settings],
) -> None:
    # No listener is created by module import, create_app(), or a request;
    # ownership starts exactly once per serving process in the lifespan.
    app = create_app(settings_factory())

    assert not hasattr(app.state, "notification_listener")


def test_lifespan_owns_exactly_one_listener_in_regular_mode(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory()
    listener = FakeNotificationListener()
    _install_listener_factory(monkeypatch, listener)
    app = create_app(settings)

    with TestClient(app) as client:
        assert app.state.notification_listener is listener
        assert listener.start_calls == 1
        assert listener.built_with == [settings]
        assert client.get("/healthz").status_code == 200

    assert listener.stop_calls == 1
    shutdown_observability()


def test_lifespan_owns_exactly_one_listener_in_queue_mode(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The queue-mode lifespan hosts the one long-lived observation host and
    # runs its restart reconciliation (issue #346); the scripted pool answers
    # the bounded outstanding-receipt read.
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
    listener = FakeNotificationListener()
    _install_listener_factory(monkeypatch, listener)
    app = create_app(settings)

    with TestClient(app) as client:
        assert app.state.notification_listener is listener
        assert listener.start_calls == 1
        assert listener.built_with == [settings]
        assert client.get("/healthz").status_code == 200

    assert listener.stop_calls == 1
    shutdown_observability()


def test_lifespan_initialization_failure_still_stops_the_listener(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory()
    listener = FakeNotificationListener()
    _install_listener_factory(monkeypatch, listener)

    def raising_dependencies(settings: Settings) -> OperationDependencies:
        raise RuntimeError("dependencies unavailable")

    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        raising_dependencies,
        raising=True,
    )

    app = create_app(settings)

    async def drive() -> None:
        async with app_module._serving_lifespan(settings)(app):
            pass  # never reached: initialization raises

    with pytest.raises(RuntimeError, match="dependencies unavailable"):
        asyncio.run(drive())

    # The listener's entire started lifetime is enclosed by the cleanup
    # boundary: the startup failure after start() still reaches the stop.
    assert listener.start_calls == 1
    assert listener.stop_calls == 1
    shutdown_observability()
