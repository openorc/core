"""Process-scoped OpenTelemetry bootstrap boundary (issue #108).

One explicit application observability boundary: OpenTelemetry tracing,
logging, and metrics with OTLP as the vendor-neutral export boundary. There
is no custom telemetry framework on top of the OpenTelemetry SDK.

Lifecycle contract (verified against the pinned SDK 1.44.0):

- Global OpenTelemetry providers are set-once and shutdown is terminal, so
  initialization happens **at most once per process** and is **terminal
  after shutdown**. There is no same-process provider reset or reinstall,
  and no SDK private state is used.
- Repeated initialization with the same identity (surface, OTLP endpoint,
  environment) is a no-op. A conflicting identity raises
  :class:`ObservabilityConfigurationError` instead of silently reinstalling
  providers or retaining wrong resource identity. Initialization after
  terminal shutdown raises :class:`ObservabilityTerminalError`.
- :func:`shutdown_observability` flushes and shuts the telemetry runtime
  down once and idempotently, and marks the process runtime terminal. When
  telemetry is unconfigured there is no runtime to terminate: shutdown
  removes only the process logging baseline and leaves the boundary
  re-initializable.
- Constructing the API application or importing modules installs nothing:
  the factory stays telemetry-free. The API lifespan triggers
  initialization inside the process that actually serves the application
  (uvicorn runs the lifespan once per serving process, including every
  reload subprocess, and never in a launcher parent). Lifespan teardown
  deliberately performs no process-global shutdown; the terminal flush
  belongs to the process-exit hook registered by initialization. The worker
  surface performs terminal shutdown in ``run_worker``'s ``finally``
  because that process is ending at that point.
- Unconfigured telemetry (no OTLP endpoint) installs no OpenTelemetry
  runtime at all — only the process logging baseline. The application
  continues to run correctly. Malformed configuration fails fast at startup
  like other configuration errors; runtime export failures never crash the
  application.
"""

from __future__ import annotations

import atexit
import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs._internal import LogRecordProcessor
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import set_tracer_provider

from openorc import __version__
from openorc.config import Settings
from openorc.observability import logs as log_pipeline

TRACES_PATH = "/v1/traces"
LOGS_PATH = "/v1/logs"
METRICS_PATH = "/v1/metrics"


class ObservabilitySurface(Enum):
    """OpenOrc process surfaces distinguished by service/resource identity."""

    API = "api"
    WORKER = "worker"


_SERVICE_NAME_BY_SURFACE: dict[ObservabilitySurface, str] = {
    ObservabilitySurface.API: "openorc-api",
    ObservabilitySurface.WORKER: "openorc-worker",
}


class ObservabilityError(Exception):
    """Base for observability lifecycle errors."""


class ObservabilityConfigurationError(ObservabilityError):
    """A conflicting second initialization was attempted before shutdown."""


class ObservabilityTerminalError(ObservabilityError):
    """Initialization was attempted after the process runtime became terminal."""


@dataclass(frozen=True)
class ObservabilityFactories:
    """Injectable telemetry construction seams.

    The defaults build the production OTLP/HTTP pipeline (batch processors
    and a periodic metric reader). Tests inject in-memory exporters/readers
    through this supported seam — never SDK private globals.
    """

    span_exporter_factory: Callable[[str], SpanExporter]
    span_processor_factory: Callable[[SpanExporter], SpanProcessor]
    log_exporter_factory: Callable[[str], LogRecordExporter]
    log_processor_factory: Callable[[LogRecordExporter], LogRecordProcessor]
    metric_reader_factory: Callable[[str], MetricReader]


def default_factories() -> ObservabilityFactories:
    """Return the production OTLP/HTTP telemetry construction seams."""
    return ObservabilityFactories(
        span_exporter_factory=lambda endpoint: OTLPSpanExporter(endpoint=endpoint),
        span_processor_factory=lambda exporter: BatchSpanProcessor(exporter),
        log_exporter_factory=lambda endpoint: OTLPLogExporter(endpoint=endpoint),
        log_processor_factory=lambda exporter: BatchLogRecordProcessor(exporter),
        metric_reader_factory=lambda endpoint: PeriodicExportingMetricReader(
            exporter=OTLPMetricExporter(endpoint=endpoint)
        ),
    )


@dataclass(frozen=True)
class ObservabilityStatus:
    """Diagnostics snapshot of the process observability state."""

    initialized: bool
    configured: bool
    terminal: bool
    surface: ObservabilitySurface | None


@dataclass
class _State:
    identity: tuple[str, str | None, str]
    configured: bool
    terminal: bool = False


_state: _State | None = None
_tracer_provider: TracerProvider | None = None
_logger_provider: LoggerProvider | None = None
_meter_provider: MeterProvider | None = None
_at_exit_registered = False


def observability_status() -> ObservabilityStatus:
    """Return the process observability state for diagnostics and tests."""
    if _state is None:
        return ObservabilityStatus(False, False, False, None)
    return ObservabilityStatus(
        True,
        _state.configured,
        _state.terminal,
        ObservabilitySurface(_state.identity[0]),
    )


def signal_endpoint(base: str, path: str) -> str:
    """Derive the per-signal OTLP/HTTP endpoint from the configured base URL.

    The OTLP HTTP exporters use an explicitly supplied endpoint as-is, so
    the standard signal paths are derived here rather than relying on
    exporter defaults.
    """
    return base.rstrip("/") + path


def initialize_observability(
    settings: Settings,
    surface: ObservabilitySurface,
    *,
    factories: ObservabilityFactories | None = None,
) -> None:
    """Install the process observability runtime exactly once.

    See the module docstring for the full lifecycle contract. Unconfigured
    telemetry (no OTLP endpoint) installs only the process logging baseline;
    configured telemetry installs the tracing/logging/metrics runtime with
    OTLP export and registers the single idempotent process-exit flush hook.
    """
    global _state, _at_exit_registered
    identity = (surface.value, settings.otlp_endpoint, settings.environment)
    if _state is not None:
        if _state.terminal:
            raise ObservabilityTerminalError(
                "OpenOrc observability has already terminated for this process"
            )
        if _state.identity == identity:
            return
        if _state.configured:
            raise ObservabilityConfigurationError(
                "OpenOrc observability is already initialized with a different process identity"
            )
    resolved_factories = factories if factories is not None else default_factories()
    endpoint = settings.otlp_endpoint
    if endpoint is not None:
        _install_telemetry_runtime(settings, surface, endpoint, resolved_factories)
        installed_logger_provider = _logger_provider
        assert installed_logger_provider is not None
        # Upgrade the logging pipeline from the plain baseline to the
        # enriched configured pipeline (unconfigured -> configured path).
        log_pipeline.uninstall_root_logging()
        log_pipeline.install_root_logging(
            [
                log_pipeline.otel_logging_handler(installed_logger_provider),
                log_pipeline.stderr_baseline_handler(enrich=True),
            ]
        )
        _state = _State(identity=identity, configured=True)
        _register_process_exit_flush()
    elif _state is None:
        log_pipeline.install_root_logging([log_pipeline.stderr_baseline_handler(enrich=False)])
        _state = _State(identity=identity, configured=False)
    # Remaining case: staying unconfigured with a different identity. No
    # global runtime exists, so there is nothing to conflict with or change.


def _install_telemetry_runtime(
    settings: Settings,
    surface: ObservabilitySurface,
    endpoint: str,
    factories: ObservabilityFactories,
) -> None:
    """Build and install the tracing/logging/metrics runtime atomically."""
    global _tracer_provider, _logger_provider, _meter_provider
    resource = _build_resource(settings, surface)
    tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=False)
    logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[factories.metric_reader_factory(signal_endpoint(endpoint, METRICS_PATH))],
        shutdown_on_exit=False,
    )
    tracer_provider.add_span_processor(
        factories.span_processor_factory(
            factories.span_exporter_factory(signal_endpoint(endpoint, TRACES_PATH))
        )
    )
    logger_provider.add_log_record_processor(
        factories.log_processor_factory(
            factories.log_exporter_factory(signal_endpoint(endpoint, LOGS_PATH))
        )
    )
    try:
        set_tracer_provider(tracer_provider)
        set_logger_provider(logger_provider)
        set_meter_provider(meter_provider)
    except BaseException:
        # Leave no half-installed runtime behind and keep the boundary
        # re-initializable; the process never runs with a broken telemetry
        # pipeline.
        tracer_provider.shutdown()
        logger_provider.shutdown()
        meter_provider.shutdown()
        raise
    _tracer_provider = tracer_provider
    _logger_provider = logger_provider
    _meter_provider = meter_provider


def _build_resource(settings: Settings, surface: ObservabilitySurface) -> Resource:
    """Build the stable OpenOrc resource identity for the process surface."""
    return Resource.create(
        {
            "service.name": _SERVICE_NAME_BY_SURFACE[surface],
            "service.version": __version__,
            "deployment.environment.name": settings.environment,
        }
    )


def shutdown_observability() -> None:
    """Flush and terminally shut down the process telemetry runtime.

    Idempotent and safe to call at any time. When a configured runtime
    exists, its processors/readers are flushed and shut down once and the
    process state becomes terminal (later initialization raises
    :class:`ObservabilityTerminalError`). Without a configured runtime there
    is nothing to terminate: the logging baseline is removed and the
    boundary stays re-initializable.
    """
    global _state, _tracer_provider, _logger_provider, _meter_provider
    if _state is None:
        return
    if not _state.configured or _state.terminal:
        return
    _state.terminal = True
    log_pipeline.uninstall_root_logging()
    for provider in (_meter_provider, _logger_provider, _tracer_provider):
        if provider is None:
            continue
        try:
            provider.shutdown()
        except Exception:  # noqa: BLE001 - exit-path flush must never crash the process
            logging.getLogger(__name__).exception("OpenOrc observability shutdown error")
    _tracer_provider = None
    _logger_provider = None
    _meter_provider = None


def _register_process_exit_flush() -> None:
    global _at_exit_registered
    if _at_exit_registered:
        return
    _at_exit_registered = True
    atexit.register(shutdown_observability)
