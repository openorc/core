"""The post-commit Task-queue wake invitation seam (issue #257).

One shared F11 vocabulary for the wake every capacity-relevant commit
arranges strictly post-commit: a successful Owner Start ``QUEUED`` entry and
every committed capacity-releasing TaskAgentSession transition (the terminal
archival's CONNECTING/READY → ENDED releases and the genuine LOST/ENDED
lifecycle records). The wake is an invitation to rederive queue eligibility
from Postgres — never admission authority, never durable workflow state, and
never a second queue — so a duplicate, refused, uncertain, or lost wake is
always harmless: the durable ``queued`` facts own recovery, and the bounded
restart/recovery triggers revisit a wake the delivery channel lost.

Services own the seam Protocol and its containment helper only; the worker
layer implements the seam over the executing process's F1
:class:`~openorc.workers.invocation.OperationSubmitter` (the bounded local
executor in regular execution mode, the canonical queue submission in queue
mode). The seam owns no durable state, makes no Task-state, currentness, or
authorization decision, and never delays, blocks, or reverses the committing
caller: an unwired seam arranges nothing and the committed state stands.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import Protocol
from uuid import UUID

logger = logging.getLogger(__name__)

__all__ = [
    "TaskQueueWakeSubmission",
    "TaskQueueWakeSubmissionOutcome",
    "arrange_task_queue_wake",
]


class TaskQueueWakeSubmissionOutcome(StrEnum):
    """The bounded submission outcome of one wake arrangement.

    Mirrors the F1 submission vocabulary: acceptance only means the
    invitation was accepted for delivery — never that any queue decision was
    made. A known refusal is an observed delivery failure, and an uncertain
    submission is handled conservatively: nothing is resubmitted, nothing is
    polled, and the durable state owns recovery.
    """

    ACCEPTED_FOR_DELIVERY = "accepted_for_delivery"
    KNOWN_REFUSED = "known_refused"
    UNCERTAIN = "uncertain"


class TaskQueueWakeSubmission(Protocol):
    """The transport-facing Task-queue wake submission seam.

    Implemented by the worker layer over a supplied process-local F1
    :class:`~openorc.workers.invocation.OperationSubmitter`. The seam owns no
    durable state and makes no Task-state or authorization decision.
    """

    def submit(
        self,
        *,
        workspace_id: UUID,
    ) -> TaskQueueWakeSubmissionOutcome: ...


def arrange_task_queue_wake(
    queue_wake: TaskQueueWakeSubmission | None,
    *,
    workspace_id: UUID,
    origin: str,
) -> TaskQueueWakeSubmissionOutcome | None:
    """Arrange one post-commit queue-processor wake invitation.

    The shared post-commit hook every capacity-relevant commit calls —
    never inside a database transaction, never for a duplicate/no-op
    transition that released no occupancy, and never as a condition of the
    committed state. An unwired seam (``None``) arranges nothing; a refused
    or failed submission is contained with safe classification and never
    fails, retries, or reverses the already-committed work. The returned
    outcome is transport information only, never Task truth.
    """
    if queue_wake is None:
        return None
    try:
        outcome = queue_wake.submit(workspace_id=workspace_id)
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        logger.warning(
            "task queue wake submission failed (origin=%s, error type %s)",
            origin,
            type(error).__name__,
        )
        return TaskQueueWakeSubmissionOutcome.UNCERTAIN
    if outcome is not TaskQueueWakeSubmissionOutcome.ACCEPTED_FOR_DELIVERY:
        logger.warning(
            "task queue wake was not accepted for delivery (origin=%s, outcome=%s)",
            origin,
            outcome.value,
        )
    return outcome
