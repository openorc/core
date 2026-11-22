"""The post-commit authorized-retry F1 submission seam (F12, issue #316).

One shared vocabulary for the typed F1 retry invocation every committed
``RETRY_AUTHORIZED`` Level 2 disposition arranges strictly post-commit: the
three durably authorized, not-yet-executed additional attempts F4 (#251)
alone can authorize, in exactly its three supported families — the
implementation Execution (F5c, #263), the approved exact-head merge (F5d,
#267), and the exact-head PR-authorization presentation (F5f, #269).

The wake is an invitation to revalidate durable facts, never authority: the
executing service re-proves the exact attempt identity, its persisted
``not_before``, Task currentness, and effect safety before any effect, and
one-shot consumption admits the exact attempt once. A duplicate, refused,
uncertain, or lost arrangement is always harmless — the committed
authorization row stays outstanding for the bounded startup recovery, and
nothing is replayed, consumed, undone, or blindly resubmitted here.

Services own the seam Protocol and its containment helper only; the worker
layer implements the seam over the executing process's F1
:class:`~openorc.workers.invocation.OperationSubmitter` (the bounded local
executor in regular execution mode, the canonical queue submission in queue
mode). The seam owns no durable state, makes no Task-state, currentness, or
authorization decision, and never delays, blocks, or reverses the committing
caller: an unwired seam arranges nothing and the committed state stands.
The pre-service due wait belongs to the executing F1 worker/job, never to
this arrangement or to the observer/settlement paths that arrange it.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from openorc.domain.operational_failures import (
    GitHubMergeOperation,
    OperationalFailureFamily,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus

logger = logging.getLogger(__name__)

__all__ = [
    "AUTHORIZED_RETRY_FAMILY_STATUSES",
    "AuthorizedRetrySubmission",
    "AuthorizedRetrySubmissionOutcome",
    "arrange_authorized_retry",
]


class AuthorizedRetrySubmissionOutcome(StrEnum):
    """The bounded submission outcome of one retry arrangement.

    Mirrors the F1 submission vocabulary: acceptance only means the
    invocation was accepted for delivery — never that any attempt ran, was
    admitted, or completed. A known refusal is an observed delivery failure,
    and an uncertain submission is handled conservatively: nothing is
    resubmitted, nothing is polled, and the committed authorization row owns
    recovery.
    """

    ACCEPTED_FOR_DELIVERY = "accepted_for_delivery"
    KNOWN_REFUSED = "known_refused"
    UNCERTAIN = "uncertain"


# The coarse Task status each supported retry family's executing service
# re-proves at admission: the implementation retry and the exact-head
# PR-authorization presentation address the still-``implementing`` Task; the
# exact-head merge retry addresses the still-``waiting_for_owner`` Task with
# no current gate. Startup recovery uses the same expectations to skip
# candidates whose durable facts moved on.
AUTHORIZED_RETRY_FAMILY_STATUSES: Mapping[OperationalFailureFamily, TaskStatus] = {
    PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION: TaskStatus.IMPLEMENTING,
    GitHubMergeOperation.EXACT_HEAD_MERGE: TaskStatus.WAITING_FOR_OWNER,
    PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION: (
        TaskStatus.IMPLEMENTING
    ),
}

# Exactly the families F4's eligibility matrix can authorize a retry in. Any
# other family arranges nothing — an unexpected RETRY_AUTHORIZED outside this
# set is a durable-state invariant failure the log owns, never a replay.
_AUTHORIZED_RETRY_FAMILIES: frozenset[OperationalFailureFamily] = frozenset(
    AUTHORIZED_RETRY_FAMILY_STATUSES
)


class AuthorizedRetrySubmission(Protocol):
    """The transport-facing authorized-retry submission seam.

    Implemented by the worker layer over a supplied process-local F1
    :class:`~openorc.workers.invocation.OperationSubmitter`. The seam owns no
    durable state and makes no Task-state or authorization decision.
    """

    def submit(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        profile_id: UUID,
        retry_attempt_id: UUID,
        expected_state_token: UUID,
        family: OperationalFailureFamily,
    ) -> AuthorizedRetrySubmissionOutcome: ...


def arrange_authorized_retry(
    submission: AuthorizedRetrySubmission | None,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    retry_attempt_id: UUID,
    expected_state_token: UUID,
    family: OperationalFailureFamily,
    origin: str,
) -> AuthorizedRetrySubmissionOutcome | None:
    """Arrange one post-commit authorized-retry F1 wake invitation.

    The shared post-commit hook every committed ``RETRY_AUTHORIZED``
    disposition calls — never inside a database transaction, never with any
    sleep or network I/O standing between the committed authorization and
    this call, and never as a condition of the committed state. An unwired
    seam (``None``) arranges nothing; a family without a supported retry
    operation arranges nothing; a refused, failed, or uncertain submission is
    contained with safe classification and never fails, retries, or reverses
    the already-committed work. The returned outcome is transport information
    only, never Task truth.
    """
    if submission is None:
        return None
    if family not in _AUTHORIZED_RETRY_FAMILIES:
        logger.warning(
            "an authorized retry disposition named a family with no supported "
            "retry operation (origin=%s, family=%s); the committed authorization "
            "row owns recovery",
            origin,
            str(family),
        )
        return None
    try:
        outcome = submission.submit(
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=profile_id,
            retry_attempt_id=retry_attempt_id,
            expected_state_token=expected_state_token,
            family=family,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        logger.warning(
            "authorized retry submission failed (origin=%s, error type %s); "
            "the committed authorization row owns recovery",
            origin,
            type(error).__name__,
        )
        return AuthorizedRetrySubmissionOutcome.UNCERTAIN
    if outcome is not AuthorizedRetrySubmissionOutcome.ACCEPTED_FOR_DELIVERY:
        logger.warning(
            "the authorized retry was not accepted for delivery (origin=%s, "
            "outcome=%s); the committed authorization row owns recovery",
            origin,
            outcome.value,
        )
    return outcome
