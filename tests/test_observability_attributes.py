"""Tests for the safe telemetry attribute vocabulary (issue #108)."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.observability import (
    CONNECTION_ID,
    EXECUTION_ID,
    GITHUB_HEAD_SHA,
    GITHUB_ISSUE_NUMBER,
    GITHUB_PULL_REQUEST_NUMBER,
    GITHUB_REPOSITORY,
    OPERATION,
    RQ_JOB_ID,
    TASK_ID,
    WORKFLOW_ROLE,
    WORKSPACE_ID,
    annotate_span,
    application_tracer,
    injected_tracer_source,
)

_TRACER_SCOPE = "tests.attributes"


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_annotate_span_sets_only_the_provided_safe_attributes() -> None:
    provider, exporter = _local_provider_with_exporter()
    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        application_tracer(_TRACER_SCOPE).start_as_current_span("op") as span,
    ):
        annotate_span(
            span,
            operation="test.operation",
            workspace_id="w-1",
            task_id="t-1",
            execution_id="e-1",
            connection_id="c-1",
            workflow_role="owner",
            github_repository="openorc/core",
            github_issue_number=108,
            github_pull_request_number=111,
            github_head_sha="a" * 40,
            rq_job_id="job-1",
        )
    (exported,) = exporter.get_finished_spans()
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[OPERATION] == "test.operation"
    assert attributes[WORKSPACE_ID] == "w-1"
    assert attributes[TASK_ID] == "t-1"
    assert attributes[EXECUTION_ID] == "e-1"
    assert attributes[CONNECTION_ID] == "c-1"
    assert attributes[WORKFLOW_ROLE] == "owner"
    assert attributes[GITHUB_REPOSITORY] == "openorc/core"
    assert attributes[GITHUB_ISSUE_NUMBER] == 108
    assert attributes[GITHUB_PULL_REQUEST_NUMBER] == 111
    assert attributes[GITHUB_HEAD_SHA] == "a" * 40
    assert attributes[RQ_JOB_ID] == "job-1"


def test_annotate_span_skips_unset_values() -> None:
    provider, exporter = _local_provider_with_exporter()
    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        application_tracer(_TRACER_SCOPE).start_as_current_span("op") as span,
    ):
        annotate_span(span, operation="only.operation", task_id="t-2")
    (exported,) = exporter.get_finished_spans()
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[OPERATION] == "only.operation"
    assert attributes[TASK_ID] == "t-2"
    assert WORKSPACE_ID not in attributes
    assert GITHUB_HEAD_SHA not in attributes


def test_annotate_span_has_no_free_form_context_path() -> None:
    provider, exporter = _local_provider_with_exporter()
    annotator = cast("Callable[..., None]", annotate_span)
    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        application_tracer(_TRACER_SCOPE).start_as_current_span("op") as span,
        pytest.raises(TypeError),
    ):
        annotator(span, bearer_token="secret-value")
    (exported,) = exporter.get_finished_spans()
    attributes = exported.attributes
    assert attributes is not None
    assert "bearer_token" not in attributes
