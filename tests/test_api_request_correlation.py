"""In-process tests for API request correlation (issue #108).

Configured span/log behavior is exercised against local, non-global
providers through the observability tracer seam — never the process-global
runtime, whose real install/terminal behaviors run in isolated subprocesses
(``test_observability_subprocess.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from fastapi.testclient import TestClient
from opentelemetry import trace as trace_api
from opentelemetry.sdk._logs import LoggerProvider as SdkLoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from openorc.api.app import create_app
from openorc.api.request_correlation import REQUEST_ID_HEADER
from openorc.config import Settings
from openorc.observability import (
    REQUEST_ID as REQUEST_ID_ATTRIBUTE,
)
from openorc.observability import (
    injected_tracer_source,
    request_log_enrichment_filter,
)
from openorc.observability.logs import otel_logging_handler


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_request_id_header_is_returned_and_fresh_per_request(
    settings_factory: Callable[..., Settings],
) -> None:
    app = create_app(settings_factory())
    with TestClient(app) as client:
        first = client.get("/healthz")
        second = client.get("/healthz")

    assert first.status_code == 200
    assert second.status_code == 200
    first_id = first.headers[REQUEST_ID_HEADER]
    second_id = second.headers[REQUEST_ID_HEADER]
    assert first_id and second_id
    assert first_id != second_id


def test_request_produces_exactly_one_entry_span_carrying_the_request_id(
    settings_factory: Callable[..., Settings],
) -> None:
    span_exporter = InMemorySpanExporter()
    provider = SdkTracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    app = create_app(settings_factory())
    with injected_tracer_source(lambda name: provider.get_tracer(name)), TestClient(app) as client:
        response = client.get("/healthz")

    request_id = response.headers[REQUEST_ID_HEADER]
    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    (span,) = spans
    assert span.name == "openorc.api.request"
    assert span.kind == SpanKind.SERVER
    assert span.attributes is not None
    assert span.attributes[REQUEST_ID_ATTRIBUTE] == request_id
    # The tracer seam kept the process-global runtime untouched.
    assert not isinstance(trace_api.get_tracer_provider(), SdkTracerProvider)


def test_request_id_is_enriched_onto_correlated_log_records(
    settings_factory: Callable[..., Settings],
) -> None:
    span_exporter = InMemorySpanExporter()
    provider = SdkTracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    log_exporter = InMemoryLogRecordExporter()
    local_logger_provider = SdkLoggerProvider()
    local_logger_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    capture = _CaptureHandler()
    capture.addFilter(request_log_enrichment_filter())
    otel_handler = otel_logging_handler(local_logger_provider)

    root = logging.getLogger()
    previous_level = root.level
    root.addHandler(capture)
    root.addHandler(otel_handler)
    root.setLevel(logging.DEBUG)
    try:
        app = create_app(settings_factory())

        @app.get("/_observability-test/log")
        def _log_once() -> dict[str, str]:
            logging.getLogger(__name__).info("inside request")
            return {"logged": "true"}

        with (
            injected_tracer_source(lambda name: provider.get_tracer(name)),
            TestClient(app) as client,
        ):
            response = client.get("/_observability-test/log")
        request_id = response.headers[REQUEST_ID_HEADER]

        matching = [record for record in capture.records if record.getMessage() == "inside request"]
        assert matching
        assert vars(matching[0])[REQUEST_ID_ATTRIBUTE] == request_id

        exported = [
            data.log_record
            for data in log_exporter.get_finished_logs()
            if data.log_record.body == "inside request"
        ]
        assert exported
        otel_record = exported[0]
        attributes = otel_record.attributes or {}
        assert attributes[REQUEST_ID_ATTRIBUTE] == request_id
        (span,) = span_exporter.get_finished_spans()
        assert otel_record.context is not None
        assert span.context is not None
        span_context = trace_api.get_current_span(otel_record.context).get_span_context()
        assert span_context.trace_id == span.context.trace_id
        assert span_context.span_id == span.context.span_id
    finally:
        root.removeHandler(capture)
        root.removeHandler(otel_handler)
        root.setLevel(previous_level)


def test_request_id_enrichment_resets_outside_the_request(
    settings_factory: Callable[..., Settings],
) -> None:
    capture = _CaptureHandler()
    capture.addFilter(request_log_enrichment_filter())
    root = logging.getLogger()
    root.addHandler(capture)
    try:
        app = create_app(settings_factory())
        with TestClient(app) as client:
            response = client.get("/healthz")
        assert REQUEST_ID_HEADER in response.headers
        logging.getLogger(__name__).info("after request")
    finally:
        root.removeHandler(capture)

    matching = [record for record in capture.records if record.getMessage() == "after request"]
    assert matching
    assert REQUEST_ID_ATTRIBUTE not in vars(matching[0])


def test_unconfigured_request_returns_header_without_telemetry(
    settings_factory: Callable[..., Settings],
) -> None:
    # No TestClient context manager: the lifespan (and observability
    # initialization) does not run; nothing is installed and no real spans
    # exist, but the response header is still present.
    app = create_app(settings_factory())
    client = TestClient(app)
    response = client.get("/healthz")

    assert REQUEST_ID_HEADER in response.headers
    assert not isinstance(trace_api.get_tracer_provider(), SdkTracerProvider)
