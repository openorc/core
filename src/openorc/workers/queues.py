"""OpenOrc RQ queue naming conventions.

``openorc:`` is the stable application-level RQ queue prefix. Deployment
isolation is provided by the Redis/Valkey namespace selected by ``VALKEY_URL``.
In the current reference/local setup this may be a Redis DB index; another
deployment may use a dedicated instance/service.

RQ has no native queue-prefix mechanism, so OpenOrc queue names carry the
prefix themselves. RQ's own internal keys (worker registrations,
``rq:queue:*`` bookkeeping) share the selected namespace and are covered by
that namespace selection. Queue names are deployment-neutral: no product- or
deployment-specific names are baked in here.
"""

from __future__ import annotations

QUEUE_PREFIX = "openorc"


def queue_name(suffix: str) -> str:
    """Return the canonical OpenOrc queue name for a logical queue suffix."""
    return f"{QUEUE_PREFIX}:{suffix}"


DEFAULT_QUEUE_NAMES: tuple[str, ...] = (queue_name("default"),)
