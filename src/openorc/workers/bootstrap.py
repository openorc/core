"""Bootstrap for the OpenOrc RQ worker process surface.

The worker is a thin queue-transport process: it connects to the configured
Redis-compatible backend and runs a minimal RQ worker loop. Queue naming and
prefix conventions and fuller Valkey/RQ wiring are owned by dedicated queue
foundation work; this bootstrap deliberately uses RQ's stock default queue
and defines no OpenOrc workflow jobs.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence

import redis
import rq
from redis.exceptions import RedisError

from openorc.config import Settings

logger = logging.getLogger(__name__)

# RQ's stock queue name. This is deliberately not an OpenOrc naming
# convention: queue naming/prefix conventions are established by the queue
# foundation work.
DEFAULT_QUEUE_NAMES: tuple[str, ...] = ("default",)


class WorkerBootstrapError(Exception):
    """Raised when the worker cannot start against its configured backend."""


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
) -> None:
    """Connect to the configured backend and run the worker loop until stopped.

    Connection failure fails fast with :class:`WorkerBootstrapError` so the
    process never idles against an unreachable backend.
    """
    make_client = client_factory if client_factory is not None else create_redis_client
    make_worker = worker_builder if worker_builder is not None else build_worker

    redis_client = make_client(settings)
    try:
        redis_client.ping()
    except RedisError as exc:
        raise WorkerBootstrapError(
            "Cannot connect to the configured Redis-compatible queue backend."
        ) from exc

    worker = make_worker(redis_client)
    logger.info("OpenOrc worker starting")
    worker.work()
