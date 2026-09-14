"""Tests for the RQ worker process bootstrap."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from unittest.mock import Mock

import pytest
import redis
import rq

from openorc.config import Settings
from openorc.workers import bootstrap


def test_build_worker_uses_stock_default_queue() -> None:
    redis_client = redis.Redis()

    worker = bootstrap.build_worker(redis_client)

    assert isinstance(worker, rq.Worker)
    assert [queue.name for queue in worker.queues] == ["default"]
    assert worker.connection is redis_client


def test_run_worker_fails_fast_when_backend_is_unreachable(
    settings_factory: Callable[..., Settings],
) -> None:
    # Loopback port 1 has no listener, so the connection is refused offline.
    settings = settings_factory(valkey_url="redis://127.0.0.1:1/0")

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

    def __init__(self) -> None:
        self.ping_calls = 0

    def ping(self) -> bool:
        self.ping_calls += 1
        return True
