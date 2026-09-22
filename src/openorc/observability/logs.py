"""OpenOrc log pipeline (issue #108).

Ordinary Python ``logging`` remains the application logging API; there is
no bespoke OpenOrc logger wrapper. This module owns the logging artifacts
the observability bootstrap installs on the root logger, at most once per
process:

- the operational stderr baseline handler (installed on every surface, so
  worker lifecycle and operational logs are visible without a collector);
- the OpenTelemetry SDK ``LoggingHandler`` (configured OTLP export only),
  which exports records over OTLP and correlates them with the active
  trace/span context.

The request-ID enrichment is a stateless ``logging.Filter`` reading the
request-scoped ``ContextVar`` that the API request middleware sets and
resets in ``finally``. The pinned OpenTelemetry SDK maps non-reserved
``LogRecord`` attributes into exported OTel log attributes, so the enriched
record exports ``openorc.request_id`` alongside its trace/span identifiers.

OTLP export is scoped to the ``openorc`` logger hierarchy: dependency,
framework, and connected-runtime loggers reach only the local stderr
baseline, never OpenOrc operational telemetry.
"""

from __future__ import annotations

import logging
import sys
import warnings
from collections.abc import Sequence
from contextvars import ContextVar

from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

from openorc.observability.attributes import REQUEST_ID

REQUEST_ID_CONTEXT: ContextVar[str | None] = ContextVar("openorc_request_id", default=None)
"""Request-scoped safe opaque request ID (set/reset by the API middleware)."""

APPLICATION_LOGGER_NAME = "openorc"
"""Root of the OpenOrc application logger hierarchy used for OTLP export."""

STDERR_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


class RequestLogEnrichmentFilter(logging.Filter):
    """Attach the safe request ID from request context to emitted records."""

    def filter(self, record: logging.LogRecord) -> bool:
        request_id = REQUEST_ID_CONTEXT.get()
        if request_id is not None:
            vars(record)[REQUEST_ID] = request_id
        return True


_ENRICHMENT_FILTER = RequestLogEnrichmentFilter()


def request_log_enrichment_filter() -> logging.Filter:
    """Return the shared stateless request-ID enrichment filter."""
    return _ENRICHMENT_FILTER


def stderr_baseline_handler(*, enrich: bool) -> logging.Handler:
    """Build the operational stderr baseline handler."""
    handler: logging.Handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(STDERR_LOG_FORMAT))
    if enrich:
        handler.addFilter(request_log_enrichment_filter())
    return handler


def otel_logging_handler(logger_provider: LoggerProvider) -> logging.Handler:
    """Build the OTel ``LoggingHandler`` with request-ID enrichment.

    The pinned SDK constructor emits a DeprecationWarning pointing at a
    contrib instrumentation package OpenOrc deliberately does not adopt in
    v1 (dependency policy); the SDK handler is the supported core surface.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        handler: logging.Handler = LoggingHandler(logger_provider=logger_provider)
    handler.addFilter(request_log_enrichment_filter())
    return handler


_installed_application_handlers: list[logging.Handler] = []
_installed_process_handlers: list[logging.Handler] = []
_previous_root_level: int | None = None


def install(
    application_handlers: Sequence[logging.Handler],
    process_handlers: Sequence[logging.Handler],
) -> None:
    """Attach the bootstrap-owned logging handlers.

    OpenOrc application handlers attach to the ``openorc`` logger hierarchy
    so OTLP export is scoped to OpenOrc's own selective, secret-excluded
    logging — dependency, framework, and connected-runtime loggers never
    become OpenOrc operational telemetry. Process handlers (the stderr
    baseline) attach to the root logger and stay process-wide. The root
    level becomes INFO so ordinary application lifecycle records are
    visible; the previous level is restored at uninstall.
    """
    global _previous_root_level
    root = logging.getLogger()
    if _previous_root_level is None:
        _previous_root_level = root.level
        root.setLevel(logging.INFO)
    for handler in process_handlers:
        root.addHandler(handler)
        _installed_process_handlers.append(handler)
    application_logger = logging.getLogger(APPLICATION_LOGGER_NAME)
    for handler in application_handlers:
        application_logger.addHandler(handler)
        _installed_application_handlers.append(handler)


def uninstall() -> None:
    """Remove bootstrap-owned handlers and restore the previous root level."""
    global _previous_root_level
    root = logging.getLogger()
    for handler in _installed_process_handlers:
        root.removeHandler(handler)
    _installed_process_handlers.clear()
    application_logger = logging.getLogger(APPLICATION_LOGGER_NAME)
    for handler in _installed_application_handlers:
        application_logger.removeHandler(handler)
    _installed_application_handlers.clear()
    if _previous_root_level is not None:
        root.setLevel(_previous_root_level)
        _previous_root_level = None
