"""Deterministic tests for the RQ reconciliation-sweep job transport (issue #62).

The job is a thin queue entrypoint: the payload is at most the scheduling
tick (a validated non-negative integer, or omitted for the operator's full
sweep), dependencies are constructed per process, the shared sweep service
is invoked, and unexpected errors are contained with a safe type
classification (arbitrary exception content never reaches operational logs).
A malformed payload fails the job loudly before any work. The enqueue seam
validates the scheduling position and enqueues exactly the canonical job
path plus the tick on the canonical default queue. No live Valkey, GitHub,
or Postgres.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.config import Settings
from openorc.observability import injected_tracer_source
from openorc.services.errors import InvalidCommandError
from openorc.workers.jobs import github_reconciliation_sweep as job_module

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
    github_reconciliation_sweep_partitions=4,
)

_SANCTIONED_JOB_ATTRIBUTE_NAMES = frozenset(
    {
        "openorc.operation",
        "openorc.rq_job_id",
    }
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

        def run(self, *args: Any) -> None:
            job_module.run_github_reconciliation_sweep_job(*args)

    harness = _Harness()

    def _fake_get_database_pool(settings: Settings) -> object:
        harness.pool_settings.append(settings)
        return harness.pool

    def _fake_client_from_settings(cls: Any, settings: Settings) -> object:
        harness.client_settings.append(settings)
        return harness.github

    def _fake_sweep(pool: Any, github: Any, settings: Settings, *, sweep_tick: int | None) -> None:
        harness.service_calls.append(
            {
                "pool": pool,
                "github": github,
                "settings": settings,
                "sweep_tick": sweep_tick,
            }
        )
        if harness.service_error is not None:
            raise harness.service_error

    monkeypatch.setattr(Settings, "from_env", classmethod(lambda cls: _SETTINGS))
    monkeypatch.setattr(job_module, "get_database_pool", _fake_get_database_pool)
    monkeypatch.setattr(
        job_module.HttpGitHubAppClient, "from_settings", classmethod(_fake_client_from_settings)
    )
    monkeypatch.setattr(job_module, "run_github_reconciliation_sweep", _fake_sweep)
    return harness


def test_the_job_invokes_the_shared_service_with_its_scheduling_tick(job_harness) -> None:
    job_harness.run(7)

    assert job_harness.service_calls == [
        {
            "pool": job_harness.pool,
            "github": job_harness.github,
            "settings": _SETTINGS,
            "sweep_tick": 7,
        }
    ]
    assert job_harness.pool_settings == [_SETTINGS]
    assert job_harness.client_settings == [_SETTINGS]


def test_the_job_default_payload_is_the_operator_full_sweep(job_harness) -> None:
    job_harness.run()

    assert job_harness.service_calls[-1]["sweep_tick"] is None


def test_a_malformed_payload_tick_fails_the_job_loudly(job_harness) -> None:
    for bad_tick in (-1, True, "0", 1.5):
        with pytest.raises(InvalidCommandError):
            job_harness.run(bad_tick)

    # No dependencies were constructed and no service work ran.
    assert job_harness.service_calls == []
    assert job_harness.pool_settings == []


def test_the_job_contains_unexpected_errors_with_safe_classification(
    job_harness, caplog: pytest.LogCaptureFixture
) -> None:
    job_harness.service_error = RuntimeError("payload fragments and SECRET content")

    with caplog.at_level(
        logging.WARNING, logger="openorc.workers.jobs.github_reconciliation_sweep"
    ):
        job_harness.run(0)

    # The job did not crash into arbitrary traceback logging, and only the
    # safe exception type name reached the record: no exception text. The
    # sweep is re-runnable; nothing was destructively lost.
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
        job_harness.run(3)

    provider.shutdown()
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attributes = dict(spans[0].attributes or {})
    assert attributes["openorc.operation"] == (
        "github_reconciliation_sweep_job.run_github_reconciliation_sweep_job"
    )
    # Outside a real worker there is no current RQ job; the safe vocabulary
    # still admits the job identity when one exists.
    assert set(attributes) <= {
        "openorc.operation",
        "openorc.rq_job_id",
    }


# --- the enqueue seam ------------------------------------------------------------


class _FakeQueue:
    """The patched rq.Queue recording the queue name and enqueue arguments."""

    def __init__(self, name: str, connection: Any = None) -> None:
        self.name = name
        self.connection = connection
        self.enqueued: list[tuple[Any, tuple[Any, ...]]] = []

    def enqueue(self, func: Any, *args: Any) -> object:
        self.enqueued.append((func, args))
        return object()


def test_the_seam_enqueues_the_job_function_with_the_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueue("openorc:default")

    def _fake_client(settings: Settings) -> object:
        return object()

    monkeypatch.setattr(Settings, "from_env", classmethod(lambda cls: _SETTINGS))
    monkeypatch.setattr(job_module, "create_redis_client", _fake_client)
    monkeypatch.setattr(job_module.rq, "Queue", lambda name, connection=None: queue)

    job_module.enqueue_github_reconciliation_sweep_job(5)

    assert queue.enqueued == [
        (
            "openorc.workers.jobs.github_reconciliation_sweep.run_github_reconciliation_sweep_job",
            (5,),
        )
    ]


def test_the_seam_rejects_a_malformed_tick_without_touching_the_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueue("openorc:default")
    monkeypatch.setattr(Settings, "from_env", classmethod(lambda cls: _SETTINGS))
    monkeypatch.setattr(job_module.rq, "Queue", lambda name, connection=None: queue)

    for bad_tick in (-1, True, "0", 1.5):
        with pytest.raises(InvalidCommandError):
            job_module.enqueue_github_reconciliation_sweep_job(bad_tick)  # type: ignore[arg-type]

    # The queue backend was never touched by a malformed submission.
    assert queue.enqueued == []
