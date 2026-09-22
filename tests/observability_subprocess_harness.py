"""Subprocess harness for real process-global observability behaviors.

The OpenTelemetry global providers are set-once and shutdown is terminal,
so behaviors that install the real global runtime or exercise the final
process-exit flush run in small deterministic subprocesses driven by this
script (see ``test_observability_subprocess.py``). In-memory
exporters/readers are injected through the supported
``ObservabilityFactories`` seam — no network and no SDK private globals.

Output protocol: each mode prints one ``pre_exit`` JSON object at the end of
its main flow and (when a report hook is registered) one ``post_exit`` JSON
object at interpreter exit. The post-exit report hook is registered BEFORE
``initialize_observability`` so atexit's LIFO order runs the flush first and
the report second.
"""

from __future__ import annotations

import atexit
import json
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import cast

import redis
import rq
from opentelemetry import metrics, trace
from opentelemetry._logs import get_logger_provider
from opentelemetry.sdk._logs import LoggerProvider as SdkLoggerProvider
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    InMemoryLogRecordExporter,
)
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.config import Settings
from openorc.observability import (
    ObservabilityConfigurationError,
    ObservabilityFactories,
    ObservabilitySurface,
    ObservabilityTerminalError,
    initialize_observability,
    observability_status,
    shutdown_observability,
)
from openorc.workers.bootstrap import run_worker


@dataclass
class _Harness:
    """In-memory exporters/readers injected through the factories seam."""

    span_exporter: InMemorySpanExporter = field(default_factory=InMemorySpanExporter)
    log_exporter: InMemoryLogRecordExporter = field(default_factory=InMemoryLogRecordExporter)
    metric_reader: InMemoryMetricReader = field(default_factory=InMemoryMetricReader)

    def factories(self) -> ObservabilityFactories:
        return ObservabilityFactories(
            span_exporter_factory=lambda _endpoint: self.span_exporter,
            span_processor_factory=lambda exporter: BatchSpanProcessor(exporter),
            log_exporter_factory=lambda _endpoint: self.log_exporter,
            log_processor_factory=lambda exporter: BatchLogRecordProcessor(exporter),
            metric_reader_factory=lambda _endpoint: self.metric_reader,
        )


def _settings(endpoint: str | None) -> Settings:
    return Settings(
        environment="production",
        api_host="127.0.0.1",
        api_port=3000,
        api_reload=False,
        valkey_url="redis://127.0.0.1:6379/0",
        otlp_endpoint=endpoint,
    )


def _emit_one_span_and_one_log() -> None:
    tracer = trace.get_tracer("harness.scope")
    with tracer.start_as_current_span("harness.operation") as span:
        span.set_attribute("openorc.operation", "harness.operation")
    logging.getLogger("harness.logger").info("harness message")


def _register_post_exit_report(harness: _Harness) -> None:
    """Register the report hook FIRST so it runs AFTER the flush (LIFO)."""
    atexit.register(_report_post_exit, harness)


def _report_post_exit(harness: _Harness) -> None:
    print(
        json.dumps(
            {
                "kind": "post_exit",
                "spans": len(harness.span_exporter.get_finished_spans()),
                "logs": len(harness.log_exporter.get_finished_logs()),
            }
        ),
        flush=True,
    )


def _expect(error_type: type[Exception], action: Callable[[], object]) -> bool:
    try:
        action()
    except error_type:
        return True
    return False


def run_identity() -> None:
    harness = _Harness()
    _register_post_exit_report(harness)
    initialize_observability(
        _settings("https://collector.example"),
        ObservabilitySurface.API,
        factories=harness.factories(),
    )
    status = observability_status()
    tracer_provider = trace.get_tracer_provider()
    assert isinstance(tracer_provider, SdkTracerProvider)
    assert isinstance(metrics.get_meter_provider(), SdkMeterProvider)
    assert isinstance(get_logger_provider(), SdkLoggerProvider)
    resource = tracer_provider.resource

    before = trace.get_tracer_provider()
    initialize_observability(
        _settings("https://collector.example"),
        ObservabilitySurface.API,
        factories=harness.factories(),
    )
    duplicate_noop = trace.get_tracer_provider() is before
    surface_conflict = _expect(
        ObservabilityConfigurationError,
        lambda: initialize_observability(
            _settings("https://collector.example"),
            ObservabilitySurface.WORKER,
            factories=harness.factories(),
        ),
    )
    endpoint_conflict = _expect(
        ObservabilityConfigurationError,
        lambda: initialize_observability(
            _settings("https://other.example"),
            ObservabilitySurface.API,
            factories=harness.factories(),
        ),
    )

    assert not harness.span_exporter.get_finished_spans()
    _emit_one_span_and_one_log()

    shutdown_observability()
    terminal = observability_status().terminal
    reinit_terminal_error = _expect(
        ObservabilityTerminalError,
        lambda: initialize_observability(
            _settings("https://collector.example"),
            ObservabilitySurface.API,
            factories=harness.factories(),
        ),
    )
    print(
        json.dumps(
            {
                "kind": "pre_exit",
                "configured": status.configured,
                "terminal": terminal,
                "service_name": resource.attributes["service.name"],
                "service_version": resource.attributes["service.version"],
                "environment": resource.attributes["deployment.environment.name"],
                "duplicate_noop": duplicate_noop,
                "surface_conflict": surface_conflict,
                "endpoint_conflict": endpoint_conflict,
                "reinit_terminal_error": reinit_terminal_error,
            }
        ),
        flush=True,
    )


def run_unconfigured() -> None:
    initialize_observability(_settings(None), ObservabilitySurface.API)
    status = observability_status()
    sdk_provider_installed = isinstance(trace.get_tracer_provider(), SdkTracerProvider)
    root_level = logging.getLogger().level
    shutdown_observability()
    terminal = observability_status().terminal
    initialize_observability(_settings(None), ObservabilitySurface.API)
    reinitialized = observability_status().initialized
    print(
        json.dumps(
            {
                "kind": "pre_exit",
                "configured": status.configured,
                "terminal": terminal,
                "sdk_provider_installed": sdk_provider_installed,
                "root_level_info": root_level == logging.INFO,
                "reinitialized": reinitialized,
            }
        ),
        flush=True,
    )


def run_worker_mode() -> None:
    harness = _Harness()
    _register_post_exit_report(harness)

    class _FakeRedisClient:
        def ping(self) -> bool:
            return True

    class _FakeWorker:
        def work(self) -> None:
            return None

    run_worker(
        _settings("https://collector.example"),
        client_factory=lambda _settings: cast(redis.Redis, _FakeRedisClient()),
        worker_builder=lambda _client: cast(rq.Worker, _FakeWorker()),
    )
    status = observability_status()
    tracer_provider = trace.get_tracer_provider()
    assert isinstance(tracer_provider, SdkTracerProvider)
    service_name = tracer_provider.resource.attributes["service.name"]
    reinit_terminal_error = _expect(
        ObservabilityTerminalError,
        lambda: initialize_observability(
            _settings("https://collector.example"),
            ObservabilitySurface.WORKER,
            factories=harness.factories(),
        ),
    )
    print(
        json.dumps(
            {
                "kind": "pre_exit",
                "configured": status.configured,
                "terminal": status.terminal,
                "service_name": service_name,
                "reinit_terminal_error": reinit_terminal_error,
            }
        ),
        flush=True,
    )


def run_atexit_flush() -> None:
    harness = _Harness()
    _register_post_exit_report(harness)
    initialize_observability(
        _settings("https://collector.example"),
        ObservabilitySurface.API,
        factories=harness.factories(),
    )
    _emit_one_span_and_one_log()
    print(
        json.dumps(
            {
                "kind": "pre_exit",
                "configured": observability_status().configured,
                "terminal": observability_status().terminal,
            }
        ),
        flush=True,
    )


def main() -> None:
    mode = sys.argv[1]
    if mode == "identity":
        run_identity()
    elif mode == "unconfigured":
        run_unconfigured()
    elif mode == "worker":
        run_worker_mode()
    elif mode == "atexit_flush":
        run_atexit_flush()
    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
