"""RQ queue transport for GitHub webhook dispatch (issue #120).

The thin job entrypoint and the enqueue-seam implementation. The job owns no
workflow logic: it decodes the payload — exactly one safe stable identity,
the provider delivery GUID — constructs the process dependencies, and invokes
the shared dispatch service. The enqueue seam owns the RQ mechanics and
classifies the queue outcome; the service owns the decision and its durable
marking. Webhook dispatch rides the canonical default queue.
"""

from __future__ import annotations

import logging

import rq
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from openorc.adapters.github import HttpGitHubAppClient
from openorc.config import Settings
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import get_database_pool
from openorc.services.errors import InvalidCommandError
from openorc.services.github_webhook_dispatch import (
    EnqueueOutcome,
    WebhookDispatchSubmission,
    dispatch_github_webhook_delivery,
)
from openorc.workers.bootstrap import create_redis_client
from openorc.workers.queues import queue_name

__all__ = [
    "build_github_webhook_dispatch_submission",
    "dispatch_github_webhook_delivery_job",
]

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.workers.jobs.github_webhook_dispatch"
_JOB_SPAN_NAME = "github_webhook_dispatch_job.dispatch_github_webhook_delivery_job"

# Webhook dispatch rides the canonical default queue: the worker built over
# DEFAULT_QUEUE_NAMES consumes it, and a dedicated queue can be split later
# without changing this module's contract.
_DISPATCH_QUEUE_NAME = queue_name("default")


def dispatch_github_webhook_delivery_job(delivery_guid: str) -> None:
    """Dispatch one accepted webhook delivery (RQ job entrypoint, issue #120).

    The queue payload is exactly the provider delivery GUID: the worker
    reloads durable state through the shared service and never trusts queue
    or webhook content as canonical input. Unexpected errors are contained
    with safe type classification — arbitrary exception content never
    reaches operational logs, the bounded ``processed_at`` recovery metadata
    remains the authority for outstanding work, and unresolved processing
    stays recoverable by #62's independent reconciliation path.
    """
    if not isinstance(delivery_guid, str) or not delivery_guid.strip():
        raise InvalidCommandError("the queue payload delivery identity is missing or malformed")
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
            dispatch_github_webhook_delivery(pool, github, delivery_guid=delivery_guid)
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            logger.warning(
                "github webhook dispatch job did not complete (error type %s)",
                type(error).__name__,
            )


class _RQWebhookDispatchSubmission:
    """The RQ-backed submission seam (transport-owned queue mechanics).

    The connection is created per submission: an ignored or terminal
    delivery never touches the queue backend at all. Timeout and connection
    errors are classified as UNCERTAIN — a timeout after sending is
    indistinguishable from a connect failure, so whether the enqueue landed
    is unknown — while other Redis errors are definitive rejections.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def enqueue(self, delivery_guid: str) -> EnqueueOutcome:
        try:
            client = create_redis_client(self._settings)
            queue = rq.Queue(_DISPATCH_QUEUE_NAME, connection=client)
            queue.enqueue(
                "openorc.workers.jobs.github_webhook_dispatch.dispatch_github_webhook_delivery_job",
                delivery_guid,
            )
        except (RedisTimeoutError, RedisConnectionError):
            return EnqueueOutcome.UNCERTAIN
        except RedisError:
            return EnqueueOutcome.KNOWN_FAILED
        return EnqueueOutcome.ENQUEUED


def build_github_webhook_dispatch_submission(settings: Settings) -> WebhookDispatchSubmission:
    """Build the RQ-backed dispatch submission seam for this process."""
    return _RQWebhookDispatchSubmission(settings)
