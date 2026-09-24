"""OpenOrc application observability boundary (issue #108).

One focused, process-scoped OpenTelemetry bootstrap with OTLP as the
vendor-neutral export boundary, an ordinary-logging contract, a stable safe
attribute vocabulary, and API request correlation. See
:mod:`openorc.observability.bootstrap` for the process lifecycle contract.
"""

from __future__ import annotations

from openorc.observability.attributes import (
    CONNECTION_ID,
    EXECUTION_ID,
    GITHUB_HEAD_SHA,
    GITHUB_INSTALLATION_ID,
    GITHUB_ISSUE_NUMBER,
    GITHUB_PULL_REQUEST_NUMBER,
    GITHUB_REPOSITORY,
    OPERATION,
    REQUEST_ID,
    RQ_JOB_ID,
    TASK_ID,
    WORKFLOW_ROLE,
    WORKSPACE_ID,
    annotate_span,
)
from openorc.observability.bootstrap import (
    ObservabilityConfigurationError,
    ObservabilityError,
    ObservabilityFactories,
    ObservabilityStatus,
    ObservabilitySurface,
    ObservabilityTerminalError,
    initialize_observability,
    observability_status,
    shutdown_observability,
)
from openorc.observability.logs import (
    REQUEST_ID_CONTEXT,
    request_log_enrichment_filter,
)
from openorc.observability.tracing import (
    application_span,
    application_tracer,
    injected_tracer_source,
)

__all__ = [
    "CONNECTION_ID",
    "EXECUTION_ID",
    "GITHUB_HEAD_SHA",
    "GITHUB_INSTALLATION_ID",
    "GITHUB_ISSUE_NUMBER",
    "GITHUB_PULL_REQUEST_NUMBER",
    "GITHUB_REPOSITORY",
    "OPERATION",
    "REQUEST_ID",
    "RQ_JOB_ID",
    "TASK_ID",
    "WORKFLOW_ROLE",
    "WORKSPACE_ID",
    "ObservabilityConfigurationError",
    "ObservabilityError",
    "ObservabilityFactories",
    "ObservabilityStatus",
    "ObservabilitySurface",
    "ObservabilityTerminalError",
    "REQUEST_ID_CONTEXT",
    "annotate_span",
    "application_span",
    "application_tracer",
    "initialize_observability",
    "injected_tracer_source",
    "observability_status",
    "request_log_enrichment_filter",
    "shutdown_observability",
]
