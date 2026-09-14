"""Tests for the RQ worker process bootstrap."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from unittest.mock import Mock

import pytest
import redis
import rq
from redis.exceptions import ConnectionError as RedisConnectionError

from openorc.config import (
    DEFAULT_API_HOST,
    DEFAULT_API_PORT,
    DEFAULT_ENVIRONMENT,
    Settings,
)
from openorc.workers import bootstrap


def test_build_worker_uses_stock_default_queue() -> None:
    redis_client = redis.Redis()

    worker = bootstrap.build_worker(redis_client)

    assert isinstance(worker, rq.Worker)
    assert [queue.name for queue in worker.queues] == ["default"]
    assert worker.connection is redis_client


def test_run_worker_fails_fast_when_backend_ping_fails(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    failing_client = cast(
        "redis.Redis",
        _FakeRedisClient(ping_error=RedisConnectionError("connection refused")),
    )

    with pytest.raises(bootstrap.WorkerBootstrapError):
        bootstrap.run_worker(settings, client_factory=lambda _settings: failing_client)


def test_run_worker_wraps_client_construction_errors(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()

    def malformed(_settings: Settings) -> redis.Redis:
        raise ValueError("unsupported scheme")

    with pytest.raises(bootstrap.WorkerBootstrapError):
        bootstrap.run_worker(settings, client_factory=malformed)


def test_run_worker_wraps_malformed_valkey_url_from_redis_parser() -> None:
    # redis-py's URL parser raises ValueError before any socket is opened, so
    # this exercises the real normalization layer without live infrastructure.
    settings = Settings(
        environment=DEFAULT_ENVIRONMENT,
        api_host=DEFAULT_API_HOST,
        api_port=DEFAULT_API_PORT,
        api_reload=False,
        valkey_url="http://127.0.0.1:1/0",
    )

    with pytest.raises(bootstrap.WorkerBootstrapError):
        bootstrap.run_worker(settings)


def test_run_worker_starts_worker_loop_once_connected(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory()
    fake_client = cast("redis.Redis", _FakeRedisClient())
    fake_worker = Mock(spec=rq.Worker)

    def fake_builder(_client: redis.Redis) -> Any:
        return fake_worker

    monkeypatch.setattr(bootstrap, "build_worker", fake_builder)

    bootstrap.run_worker(settings, client_factory=lambda _settings: fake_client)

    assert cast("_FakeRedisClient", fake_client).ping_calls == 1
    fake_worker.work.assert_called_once_with()


class _FakeRedisClient:
    """Minimal Redis-compatible double for worker bootstrap tests."""

    def __init__(self, ping_error: Exception | None = None) -> None:
        self.ping_calls = 0
        self._ping_error = ping_error

    def ping(self) -> bool:
        self.ping_calls += 1
        if self._ping_error is not None:
            raise self._ping_error
        return True
