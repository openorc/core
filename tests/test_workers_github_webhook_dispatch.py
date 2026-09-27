"""Deterministic tests for the RQ dispatch job transport (issue #120).

The job is a thin queue entrypoint: the payload is exactly the provider
delivery GUID, dependencies are constructed per process, the shared dispatch
service is invoked, and unexpected errors are contained with a safe type
classification (arbitrary exception content never reaches operational logs;
the bounded ``processed_at`` recovery metadata remains the authority for
outstanding work). The enqueue seam classifies the queue outcome:
timeout/connection loss is uncertain, other Redis errors are definitive
known failures, and a known success enqueues exactly the delivery GUID on
the canonical default queue. No live Valkey, GitHub, or Postgres.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
import redis.exceptions
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.config import Settings
from openorc.observability import injected_tracer_source
from openorc.services.errors import InvalidCommandError
from openorc.workers.jobs import github_webhook_dispatch as job_module

_SETTINGS = Settings(
    environment="test",
    api_host="127.0.0.1",
    api_port=3999,
    api_reload=False,
    valkey_url="redis://127.0.0.1:6379/0",
    database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
    db_pool_min=1,
    db_pool_max=5,
    db_pool_timeout=5.0,
    github_app_id=1234,
    github_app_private_key="not-a-real-key",
)


@pytest.fixture
def job_harness(monkeypatch: pytest.MonkeyPatch):
    class _Harness:
        def __init__(self) -> None:
            self.pool_settings: list[Settings] = []
            self.client_settings: list[Settings] = []
            self.service_calls: list[dict[str, Any]] = []
            self.pool: object = object()
            self.github: object = object()
            self.service_error: Exception | None = None

        def run(self, delivery_guid: str) -> None:
            job_module.dispatch_github_webhook_delivery_job(delivery_guid)

    harness = _Harness()

    def _fake_get_database_pool(settings: Settings) -> object:
        harness.pool_settings.append(settings)
        return harness.pool

    def _fake_client_from_settings(cls: Any, settings: Settings) -> object:
        harness.client_settings.append(settings)
        return harness.github

    def _fake_dispatch(pool: Any, github: Any, *, delivery_guid: str) -> None:
        harness.service_calls.append(
            {"pool": pool, "github": github, "delivery_guid": delivery_guid}
        )
        if harness.service_error is not None:
            raise harness.service_error

    monkeypatch.setattr(Settings, "from_env", classmethod(lambda cls: _SETTINGS))
    monkeypatch.setattr(job_module, "get_database_pool", _fake_get_database_pool)
    monkeypatch.setattr(
        job_module.HttpGitHubAppClient, "from_settings", classmethod(_fake_client_from_settings)
    )
    monkeypatch.setattr(job_module, "dispatch_github_webhook_delivery", _fake_dispatch)
    return harness


def test_the_job_invokes_the_shared_service_with_the_delivery_guid(job_harness) -> None:
    job_harness.run("guid-1")

    assert job_harness.service_calls == [
        {"pool": job_harness.pool, "github": job_harness.github, "delivery_guid": "guid-1"}
    ]
    assert job_harness.pool_settings == [_SETTINGS]
    assert job_harness.client_settings == [_SETTINGS]


def test_the_job_rejects_a_malformed_payload_identity(job_harness) -> None:
    with pytest.raises(InvalidCommandError):
        job_harness.run("   ")


def test_the_job_contains_unexpected_errors_with_safe_classification(
    job_harness, caplog: pytest.LogCaptureFixture
) -> None:
    job_harness.service_error = RuntimeError("payload fragments and SECRET content")

    with caplog.at_level(logging.WARNING, logger="openorc.workers.jobs.github_webhook_dispatch"):
        job_harness.run("guid-1")

    # The job did not crash into arbitrary traceback logging, and only the
    # safe exception type name reached the record: no exception text.
    assert job_harness.service_calls != []
    assert len(caplog.records) == 1
    assert "RuntimeError" in caplog.records[0].getMessage()
    assert "SECRET content" not in caplog.records[0].getMessage()
    assert "payload fragments" not in caplog.records[0].getMessage()


def test_the_job_span_carries_only_the_safe_vocabulary(job_harness) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with injected_tracer_source(lambda _scope: provider.get_tracer("test")):
        job_harness.run("guid-1")

    provider.shutdown()
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attributes = dict(spans[0].attributes or {})
    assert attributes["openorc.operation"] == (
        "github_webhook_dispatch_job.dispatch_github_webhook_delivery_job"
    )
    # Outside a real worker there is no current RQ job; the safe vocabulary
    # still admits the job identity when one exists.
    assert set(attributes) <= {
        "openorc.request_id",
        "openorc.workspace_id",
        "openorc.task_id",
        "openorc.execution_id",
        "openorc.connection_id",
        "openorc.workflow_role",
        "openorc.operation",
        "openorc.github_installation_id",
        "openorc.github_repository",
        "openorc.github_issue_number",
        "openorc.github_pull_request_number",
        "openorc.github_head_sha",
        "openorc.rq_job_id",
    }


# --- the enqueue seam -----------------------------------------------------------


class _FakeQueue:
    """The patched rq.Queue recording enqueue arguments or raising."""

    def __init__(self, name: str, connection: Any = None, error: Exception | None = None) -> None:
        self.name = name
        self.connection = connection
        self.enqueued: list[tuple[Any, tuple[Any, ...]]] = []
        self._error = error

    def enqueue(self, func: Any, *args: Any) -> object:
        if self._error is not None:
            raise self._error
        self.enqueued.append((func, args))
        return object()


@pytest.fixture
def queue_harness(monkeypatch: pytest.MonkeyPatch):
    class _QueueHarness:
        def __init__(self) -> None:
            self.queue: _FakeQueue | None = None
            self.client_settings: list[Settings] = []

    holder = _QueueHarness()

    def _run(error: Exception | None = None) -> Any:
        queue = _FakeQueue("openorc:default", error=error)

        def _fake_client(settings: Settings) -> object:
            holder.client_settings.append(settings)
            return object()

        monkeypatch.setattr(job_module, "create_redis_client", _fake_client)
        monkeypatch.setattr(job_module.rq, "Queue", lambda name, connection=None: queue)
        submission = job_module.build_github_webhook_dispatch_submission(_SETTINGS)
        holder.queue = queue
        return submission.enqueue("guid-1")

    return holder, _run


def test_the_seam_enqueues_the_job_function_with_only_the_delivery_guid(queue_harness) -> None:
    holder, run = queue_harness

    outcome = run()

    assert outcome is job_module.EnqueueOutcome.ENQUEUED
    assert holder.queue is not None
    assert holder.queue.name == "openorc:default"
    assert holder.queue.enqueued == [
        (
            "openorc.workers.jobs.github_webhook_dispatch.dispatch_github_webhook_delivery_job",
            ("guid-1",),
        )
    ]
    assert holder.client_settings == [_SETTINGS]


def test_a_timeout_classifies_the_enqueue_outcome_as_uncertain(queue_harness) -> None:
    holder, run = queue_harness

    outcome = run(error=redis.exceptions.TimeoutError())

    assert outcome is job_module.EnqueueOutcome.UNCERTAIN
    assert holder.queue is not None
    assert holder.queue.enqueued == []


def test_a_connection_loss_classifies_the_enqueue_outcome_as_uncertain(queue_harness) -> None:
    holder, run = queue_harness

    outcome = run(error=redis.exceptions.ConnectionError())

    assert outcome is job_module.EnqueueOutcome.UNCERTAIN


def test_a_definitive_redis_error_classifies_as_known_failure(queue_harness) -> None:
    holder, run = queue_harness

    outcome = run(error=redis.exceptions.ResponseError("rejected"))

    assert outcome is job_module.EnqueueOutcome.KNOWN_FAILED
    assert holder.queue is not None
    assert holder.queue.enqueued == []
