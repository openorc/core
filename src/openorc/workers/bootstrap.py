"""Bootstrap for the OpenOrc RQ worker process surface.

The worker is a thin queue-transport process: it connects to the configured
Redis-compatible backend and runs an RQ worker loop over the canonical OpenOrc
queues defined in :mod:`openorc.workers.queues`. No OpenOrc workflow jobs
exist yet; this module owns queue transport mechanics only.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Protocol

import redis
import rq
from redis.exceptions import RedisError

from openorc.config import Settings
from openorc.observability import (
    ObservabilitySurface,
    initialize_observability,
    shutdown_observability,
)
from openorc.workers.queues import DEFAULT_QUEUE_NAMES

logger = logging.getLogger(__name__)


class WorkerBootstrapError(Exception):
    """Raised when the worker cannot start against its configured backend."""


class ObservabilityLifecycle(Protocol):
    """Worker-observability lifecycle seam (tests inject recordings)."""

    def initialize(self, settings: Settings) -> None:
        """Initialize observability for the worker process surface."""
        ...

    def shutdown(self) -> None:
        """Perform the terminal observability shutdown for this process."""
        ...


class ProcessObservabilityLifecycle:
    """Default lifecycle adapter over the process-scoped observability boundary.

    Initialization is the process-scoped exactly-once bootstrap (issue #108);
    shutdown is terminal because it runs when the worker process is ending.
    """

    def initialize(self, settings: Settings) -> None:
        initialize_observability(settings, ObservabilitySurface.WORKER)

    def shutdown(self) -> None:
        shutdown_observability()


def create_redis_client(settings: Settings) -> redis.Redis:
    """Create a Redis-compatible client from configured settings."""
    return redis.Redis.from_url(settings.valkey_url)


def build_worker(
    redis_client: redis.Redis,
    *,
    queue_names: Sequence[str] = DEFAULT_QUEUE_NAMES,
) -> rq.Worker:
    """Construct the RQ worker over the configured queues."""
    queues = [rq.Queue(name, connection=redis_client) for name in queue_names]
    return rq.Worker(queues, connection=redis_client)


def run_worker(
    settings: Settings,
    *,
    client_factory: Callable[[Settings], redis.Redis] | None = None,
    worker_builder: Callable[[redis.Redis], rq.Worker] | None = None,
    observability: ObservabilityLifecycle | None = None,
) -> None:
    """Connect to the configured backend and run the worker loop until stopped.

    Connection failure or client-construction failure (including malformed
    backend URLs that redis-py's parser rejects) fails fast with
    :class:`WorkerBootstrapError` so the process never idles against an
    unreachable or misconfigured backend.

    The worker process owns observability terminally (issue #108):
    initialization happens at start (once per process, guarded by the
    observability boundary) and the terminal shutdown/flush runs in
    ``finally`` after the loop exits — normally or on error — because the
    worker process is ending at that point.
    """
    make_client = client_factory if client_factory is not None else create_redis_client
    make_worker = worker_builder if worker_builder is not None else build_worker
    lifecycle = observability if observability is not None else ProcessObservabilityLifecycle()

    try:
        lifecycle.initialize(settings)
        try:
            redis_client = make_client(settings)
            redis_client.ping()
        except (RedisError, ValueError) as exc:
            # ValueError covers redis-py's URL parser (unsupported schemes and
            # invalid URL options) so malformed backend configuration fails with
            # the same concise bootstrap error instead of a raw traceback.
            raise WorkerBootstrapError(
                "Cannot connect to the configured Redis-compatible queue backend."
            ) from exc

        worker = make_worker(redis_client)
        logger.info("OpenOrc worker starting")
        worker.work()
    finally:
        lifecycle.shutdown()
