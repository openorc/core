"""Tests for the worker observability lifecycle wiring (issue #108)."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest
import redis
import rq
from redis.exceptions import ConnectionError as RedisConnectionError

from openorc.config import Settings
from openorc.observability import observability_status
from openorc.workers import bootstrap


class _RecordingLifecycle:
    def __init__(self) -> None:
        self.events: list[str] = []

    def initialize(self, settings: Settings) -> None:
        self.events.append("initialize")

    def shutdown(self) -> None:
        self.events.append("shutdown")


class _FakeRedisClient:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def ping(self) -> bool:
        self._events.append("ping")
        return True


class _FakeWorker:
    def __init__(self, events: list[str], error: Exception | None = None) -> None:
        self._events = events
        self._error = error

    def work(self) -> None:
        self._events.append("work")
        if self._error is not None:
            raise self._error


def test_worker_initializes_then_shuts_down_terminally_in_finally(
    settings_factory: Callable[..., Settings],
) -> None:
    lifecycle = _RecordingLifecycle()
    events = lifecycle.events

    bootstrap.run_worker(
        settings_factory(),
        client_factory=lambda _settings: cast("redis.Redis", _FakeRedisClient(events)),
        worker_builder=lambda _client: cast("rq.Worker", _FakeWorker(events)),
        observability=lifecycle,
    )

    assert events == ["initialize", "ping", "work", "shutdown"]


def test_worker_shuts_down_when_backend_connect_fails(
    settings_factory: Callable[..., Settings],
) -> None:
    lifecycle = _RecordingLifecycle()

    class _FailingClient:
        def ping(self) -> bool:
            raise RedisConnectionError("connection refused")

    with pytest.raises(bootstrap.WorkerBootstrapError):
        bootstrap.run_worker(
            settings_factory(),
            client_factory=lambda _settings: cast("redis.Redis", _FailingClient()),
            observability=lifecycle,
        )

    assert lifecycle.events == ["initialize", "shutdown"]


def test_worker_shuts_down_when_the_loop_errors(
    settings_factory: Callable[..., Settings],
) -> None:
    lifecycle = _RecordingLifecycle()
    events = lifecycle.events

    with pytest.raises(RuntimeError, match="loop failure"):
        bootstrap.run_worker(
            settings_factory(),
            client_factory=lambda _settings: cast("redis.Redis", _FakeRedisClient(events)),
            worker_builder=lambda _client: cast(
                "rq.Worker", _FakeWorker(events, RuntimeError("loop failure"))
            ),
            observability=lifecycle,
        )

    assert events == ["initialize", "ping", "work", "shutdown"]


def test_default_lifecycle_leaves_telemetry_unconfigured(
    settings_factory: Callable[..., Settings],
) -> None:
    bootstrap.run_worker(
        settings_factory(),
        client_factory=lambda _settings: cast("redis.Redis", _FakeRedisClient([])),
        worker_builder=lambda _client: cast("rq.Worker", _FakeWorker([])),
    )

    status = observability_status()
    assert status.configured is False
    assert status.terminal is False
