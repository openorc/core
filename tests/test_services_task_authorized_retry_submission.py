"""Tests for the F12 authorized-retry F1 submission seam (issue #316).

Deterministic and fake-backed: the shared post-commit arrangement hook is
exercised with recording seams and scripted outcomes. No live Postgres,
Valkey, GitHub, or Cline is touched; the F1 outcome mapping and the worker
adapter are covered by the dedicated worker-layer tests.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.operational_failures import (
    GitHubMergeOperation,
    ImplementationAuthorizationPresentationOperation,
)
from openorc.services.task_authorized_retry_submission import (
    AuthorizedRetrySubmissionOutcome,
    arrange_authorized_retry,
)

_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_PROFILE_ID = uuid4()
_ATTEMPT_ID = uuid4()
_TOKEN = uuid4()


class _RecordingSubmission:
    """Recording AuthorizedRetrySubmission double with scripted behavior."""

    def __init__(
        self,
        *,
        outcome: AuthorizedRetrySubmissionOutcome = (
            AuthorizedRetrySubmissionOutcome.ACCEPTED_FOR_DELIVERY
        ),
        error: Exception | None = None,
    ) -> None:
        self.calls: list[dict[str, Any]] = []
        self._outcome = outcome
        self._error = error

    def submit(self, **kwargs: Any) -> AuthorizedRetrySubmissionOutcome:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return self._outcome


def _submit(**overrides: Any) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "profile_id": _PROFILE_ID,
        "retry_attempt_id": _ATTEMPT_ID,
        "expected_state_token": _TOKEN,
        "family": GitHubMergeOperation.EXACT_HEAD_MERGE,
    }
    facts.update(overrides)
    return facts


def test_an_unwired_seam_arranges_nothing() -> None:
    """An unwired seam (``None``) arranges nothing and the committed state stands."""
    assert arrange_authorized_retry(None, origin="test", **_submit()) is None


def test_an_accepted_arrangement_carries_the_exact_facts() -> None:
    """The exact authorized attempt facts cross the seam verbatim."""
    submission = _RecordingSubmission()
    outcome = arrange_authorized_retry(submission, origin="test", **_submit())

    assert outcome is AuthorizedRetrySubmissionOutcome.ACCEPTED_FOR_DELIVERY
    assert submission.calls == [_submit()]


def test_a_refused_arrangement_is_reported_not_replayed() -> None:
    """A known refusal is returned as transport information only."""
    submission = _RecordingSubmission(
        outcome=AuthorizedRetrySubmissionOutcome.KNOWN_REFUSED
    )
    outcome = arrange_authorized_retry(submission, origin="test", **_submit())

    assert outcome is AuthorizedRetrySubmissionOutcome.KNOWN_REFUSED
    assert len(submission.calls) == 1  # attempted exactly once, never replayed


def test_a_failing_submission_is_contained_as_uncertain() -> None:
    """A seam failure is an unknown outcome — never a known non-delivery and
    never a failure of the already-committed work."""
    submission = _RecordingSubmission(error=RuntimeError("the transport exploded"))
    outcome = arrange_authorized_retry(submission, origin="test", **_submit())

    assert outcome is AuthorizedRetrySubmissionOutcome.UNCERTAIN
    assert len(submission.calls) == 1


def test_a_family_without_a_supported_retry_operation_arranges_nothing() -> None:
    """Only F4's three supported families arrange the handoff; anything else
    is a durable-state invariant failure the log owns, never a submission."""
    submission = _RecordingSubmission()
    outcome = arrange_authorized_retry(
        submission,
        origin="test",
        family=(
            ImplementationAuthorizationPresentationOperation.EXACT_PLAN_IMPLEMENTATION_AUTHORIZATION_PRESENTATION
        ),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        profile_id=_PROFILE_ID,
        retry_attempt_id=_ATTEMPT_ID,
        expected_state_token=_TOKEN,
    )

    assert outcome is None
    assert submission.calls == []


def test_the_supported_family_roster_is_exactly_f4s_three_families() -> None:
    """The status expectations name exactly the three supported families."""
    from openorc.domain.operational_failures import PRAuthorizationPresentationOperation
    from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
    from openorc.domain.tasks import TaskStatus
    from openorc.services.task_authorized_retry_submission import (
        AUTHORIZED_RETRY_FAMILY_STATUSES,
    )

    assert set(AUTHORIZED_RETRY_FAMILY_STATUSES) == {
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        GitHubMergeOperation.EXACT_HEAD_MERGE,
        PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION,
    }
    assert (
        AUTHORIZED_RETRY_FAMILY_STATUSES[
            PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
        ]
        is TaskStatus.IMPLEMENTING
    )
    assert (
        AUTHORIZED_RETRY_FAMILY_STATUSES[GitHubMergeOperation.EXACT_HEAD_MERGE]
        is TaskStatus.WAITING_FOR_OWNER
    )
    assert (
        AUTHORIZED_RETRY_FAMILY_STATUSES[
            PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
        ]
        is TaskStatus.IMPLEMENTING
    )


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (
            AuthorizedRetrySubmissionOutcome.KNOWN_REFUSED,
            AuthorizedRetrySubmissionOutcome.KNOWN_REFUSED,
        ),
        (
            AuthorizedRetrySubmissionOutcome.UNCERTAIN,
            AuthorizedRetrySubmissionOutcome.UNCERTAIN,
        ),
    ],
)
def test_non_accepted_outcomes_pass_through_verbatim(
    outcome: AuthorizedRetrySubmissionOutcome, expected: AuthorizedRetrySubmissionOutcome
) -> None:
    """Refused and uncertain outcomes map 1:1 and never become success."""
    submission = _RecordingSubmission(outcome=outcome)
    assert arrange_authorized_retry(submission, origin="test", **_submit()) is expected
