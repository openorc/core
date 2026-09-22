"""In-process tests for the observability bootstrap boundary (issue #108).

Behavior that installs the real process-global OpenTelemetry runtime, or
exercises the final process-exit flush, runs in isolated subprocesses (see
``test_observability_subprocess.py``). This module only exercises behavior
that keeps the global runtime untouched.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest
from opentelemetry import trace as trace_api
from opentelemetry._logs import get_logger_provider
from opentelemetry.metrics import get_meter_provider
from opentelemetry.sdk._logs import LoggerProvider as SdkLoggerProvider
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

from openorc.config import Settings
from openorc.observability import (
    ObservabilitySurface,
    application_tracer,
    initialize_observability,
    injected_tracer_source,
    observability_status,
    shutdown_observability,
)


def test_initialize_without_endpoint_installs_no_telemetry_runtime(
    settings_factory: Callable[..., Settings],
) -> None:
    initialize_observability(settings_factory(), ObservabilitySurface.WORKER)

    status = observability_status()
    assert status.initialized is True
    assert status.configured is False
    assert status.terminal is False
    assert not isinstance(trace_api.get_tracer_provider(), SdkTracerProvider)
    assert not isinstance(get_logger_provider(), SdkLoggerProvider)
    assert not isinstance(get_meter_provider(), SdkMeterProvider)


def test_unconfigured_shutdown_removes_the_logging_baseline_and_clears_state(
    settings_factory: Callable[..., Settings],
) -> None:
    # Clear any pre-existing unconfigured state deterministically first.
    shutdown_observability()
    root = logging.getLogger()
    previous_level = root.level
    previous_handlers = set(root.handlers)

    initialize_observability(settings_factory(), ObservabilitySurface.WORKER)

    assert root.level == logging.INFO
    assert set(root.handlers) != previous_handlers

    shutdown_observability()

    status = observability_status()
    assert status.initialized is False
    assert status.configured is False
    assert status.terminal is False
    assert root.level == previous_level
    assert set(root.handlers) == previous_handlers

    # The boundary is cleanly re-initializable after the cleanup.
    initialize_observability(settings_factory(), ObservabilitySurface.WORKER)
    assert observability_status().initialized is True
    assert observability_status().configured is False
    assert root.level == logging.INFO
    shutdown_observability()
    assert observability_status().initialized is False


def test_repeated_shutdown_is_safe(settings_factory: Callable[..., Settings]) -> None:
    initialize_observability(settings_factory(), ObservabilitySurface.WORKER)
    before = observability_status()

    shutdown_observability()
    shutdown_observability()

    after = observability_status()
    assert after.terminal == before.terminal


def test_application_tracer_defaults_to_the_global_runtime() -> None:
    # Without an injected source the seam falls through to the OpenTelemetry
    # global runtime: a fresh proxy tracer before initialization, the real
    # provider's tracer after guarded initialization in a serving process.
    tracer = application_tracer("openorc.test.scope")
    global_tracer = trace_api.get_tracer("openorc.test.scope")
    assert type(tracer) is type(global_tracer)


def test_injected_tracer_source_replaces_and_restores() -> None:
    provider = SdkTracerProvider()
    local_tracer = provider.get_tracer("openorc.test.injected")

    with injected_tracer_source(lambda name: local_tracer):
        assert application_tracer("openorc.test.injected") is local_tracer

    restored = application_tracer("openorc.test.injected")
    # The default source is back: the tracer comes from the global runtime
    # again, not from the injected local provider.
    assert restored is not local_tracer
    assert type(restored) is type(trace_api.get_tracer("openorc.test.injected"))


def test_injected_tracer_source_restores_after_errors() -> None:
    provider = SdkTracerProvider()
    local_tracer = provider.get_tracer("openorc.test.injected")

    with (
        pytest.raises(RuntimeError, match="boom"),
        injected_tracer_source(lambda name: local_tracer),
    ):
        raise RuntimeError("boom")

    restored = application_tracer("openorc.test.injected")
    assert restored is not local_tracer
    assert type(restored) is type(trace_api.get_tracer("openorc.test.injected"))
