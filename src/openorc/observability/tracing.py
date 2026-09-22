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

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.util.types import AttributeValue

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer

TracerSource = Callable[[str], "Tracer"] | None

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


def application_tracer(tracer_scope: str) -> Tracer:
    """Return the tracer for an OpenOrc instrumentation scope name."""
    source = _tracer_source
    if source is not None:
        return source(tracer_scope)
    return trace.get_tracer(tracer_scope)


@contextmanager
def application_span(
    tracer_scope: str,
    span_name: str,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: Mapping[str, AttributeValue] | None = None,
) -> Iterator[Span]:
    """Open an OpenOrc application span with safe failure classification.

    Automatic OpenTelemetry exception recording is disabled at this
    boundary: ``record_exception`` exports ``exception.message`` and
    ``exception.stacktrace``, and ``set_status_on_exception`` places the
    full exception message into the status description — arbitrary
    exception text may carry secret-bearing content and must never be
    exported. On failure the span receives an ERROR status whose
    description is only the exception type name; diagnostics beyond that
    classification belong to framework-owned server-side logging, not
    telemetry export.
    """
    tracer = application_tracer(tracer_scope)
    with tracer.start_as_current_span(
        span_name,
        kind=kind,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            yield span
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            raise
