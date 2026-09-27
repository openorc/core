"""RQ queue transport for the GitHub reconciliation/recovery sweep (issue #62).

The thin job entrypoint for the repeatable reconciliation/recovery sweep.
The job owns no workflow logic: it decodes the payload — at most the
scheduling position (``sweep_tick``, a validated non-negative integer, or
omitted for the operator's full sweep) — constructs the process
dependencies, and invokes the shared sweep service. The tick is a scheduling
position only, never workflow truth: every sweep unit re-derives its subject
from durable Postgres and re-reads fresh authoritative GitHub state, so a
replayed, delayed, or reordered job is harmless. The sweep rides the
canonical default queue.

Periodic scheduling is a deployment/operational concern: an external
scheduler enqueues this job with successive ticks modulo the configured
partition count (``OPENORC_GITHUB_RECONCILIATION_SWEEP_PARTITIONS``, read
identically by the scheduler and this process through Settings). Example
enqueue for one tick::

    rq.Queue("openorc:default").enqueue(
        "openorc.workers.jobs.github_reconciliation_sweep"
        ".run_github_reconciliation_sweep_job",
        tick,
    )

Operator/manual recovery enqueues the same job without a tick (or invokes
the shared service directly).
"""

from __future__ import annotations

import logging

import rq

from openorc.adapters.github import HttpGitHubAppClient
from openorc.config import Settings
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import get_database_pool
from openorc.services.errors import InvalidCommandError
from openorc.services.github_reconciliation_recovery import run_github_reconciliation_sweep
from openorc.workers.bootstrap import create_redis_client
from openorc.workers.queues import queue_name

__all__ = [
    "enqueue_github_reconciliation_sweep_job",
    "run_github_reconciliation_sweep_job",
]

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.workers.jobs.github_reconciliation_sweep"
_JOB_SPAN_NAME = "github_reconciliation_sweep_job.run_github_reconciliation_sweep_job"

# The sweep rides the canonical default queue: the worker built over
# DEFAULT_QUEUE_NAMES consumes it, and a dedicated queue can be split later
# without changing this module's contract.
_SWEEP_QUEUE_NAME = queue_name("default")


def run_github_reconciliation_sweep_job(sweep_tick: int | None = None) -> None:
    """Run one reconciliation/recovery sweep (RQ job entrypoint, issue #62).

    The queue payload is at most the scheduling tick — validated here,
    revalidated by the shared service, and never trusted as canonical input.
    Unexpected errors are contained with safe type classification: arbitrary
    exception content never reaches operational logs, and every unserviced
    or failed unit remains recoverable by the next sweep (tick rotation or
    re-run).
    """
    if sweep_tick is not None and (
        isinstance(sweep_tick, bool) or not isinstance(sweep_tick, int) or sweep_tick < 0
    ):
        raise InvalidCommandError("the queue payload sweep tick is missing, malformed, or negative")
    settings = Settings.from_env()
    with application_span(_TRACER_SCOPE, _JOB_SPAN_NAME) as span:
        job = rq.get_current_job()
        annotate_span(
            span,
            operation=_JOB_SPAN_NAME,
            rq_job_id=job.id if job is not None else None,
        )
        pool = get_database_pool(settings)
        github = HttpGitHubAppClient.from_settings(settings)
        try:
            run_github_reconciliation_sweep(pool, github, settings, sweep_tick=sweep_tick)
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            logger.warning(
                "github reconciliation sweep job did not complete (error type %s)",
                type(error).__name__,
            )


def enqueue_github_reconciliation_sweep_job(sweep_tick: int | None) -> None:
    """Enqueue one sweep invocation on the canonical default queue.

    The deployment scheduler's thin seam: validates the scheduling position
    and enqueues exactly the job function path plus the tick. Queue mechanics
    only — correctness never depends on the tick value. A malformed tick is
    a typed command error, never silently coerced.
    """
    if sweep_tick is not None and (
        isinstance(sweep_tick, bool) or not isinstance(sweep_tick, int) or sweep_tick < 0
    ):
        raise InvalidCommandError("sweep_tick must be a non-negative integer or None")
    client = create_redis_client(Settings.from_env())
    queue = rq.Queue(_SWEEP_QUEUE_NAME, connection=client)
    queue.enqueue(
        "openorc.workers.jobs.github_reconciliation_sweep.run_github_reconciliation_sweep_job",
        sweep_tick,
    )
