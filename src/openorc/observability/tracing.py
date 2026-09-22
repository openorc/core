"""Application tracer acquisition seam (issue #108).

OpenOrc boundary spans (API request entry, service operations, external
adapter operations, worker job entrypoints) acquire their tracer through
this seam instead of reaching into the OpenTelemetry global runtime
directly.

The default source resolves the process-global provider once the
observability bootstrap has installed it; before that the OpenTelemetry
proxy tracer is returned, which stays a no-op.

Tests may inject a tracer source built from a local, non-global
``TracerProvider`` — an explicitly supported OpenTelemetry primitive — so
configured span behavior is exercised with zero process-global mutation.
Processor/exporter factory injection alone is not the test-isolation
primitive: the tracer seam is. The injected source is test-owned state:
:func:`injected_tracer_source` restores the previous source in ``finally``
so a failing test cannot leak its source into other tests.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from opentelemetry import trace

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

TracerSource = Callable[[str], "Tracer"]

_tracer_source: TracerSource | None = None


@contextmanager
def injected_tracer_source(source: TracerSource) -> Iterator[None]:
    """Temporarily replace the application tracer source (test seam)."""
    global _tracer_source
    previous = _tracer_source
    _tracer_source = source
    try:
        yield
    finally:
        _tracer_source = previous


def application_tracer(name: str) -> Tracer:
    """Return the tracer for an OpenOrc instrumentation scope name."""
    source = _tracer_source
    if source is not None:
        return source(name)
    return trace.get_tracer(name)
