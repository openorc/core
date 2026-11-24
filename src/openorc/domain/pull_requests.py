"""TaskPullRequest domain models.

A TaskPullRequest is the one canonical pull request record for one Task
(Phase 1, issue #25). It is the durable bridge between OpenOrc workflow
state and GitHub PR truth: OpenOrc owns workflow/control semantics; GitHub
owns actual PR truth, reconciled later. Creation/reconciliation API calls
are later service/adapter work — this module carries the durable facts and
their transport-independent validation only.

- One Task has exactly one TaskPullRequest for its whole v1 lifetime. There
  are no replacement-PR rows and no speculative PR-replacement/adoption
  history: a closed-unmerged PR remains the Task's canonical PR record, and
  later workflow services block against it rather than silently replacing
  it.
- Stable GitHub PR identity is separate from repository-local PR address
  metadata: ``github_pr_id`` is the stable external identity used for
  reconciliation and per-Workspace canonicalization; ``github_pr_number``
  is the repository-local address captured at creation and never identity.
  They are distinct persistence concerns (issue #25 acceptance criterion).
- Mutable observed reconciliation state — ``head_ref``, ``base_ref``,
  ``head_sha``, ``state``, ``merged_at`` — is updated in place as
  reconciliation observes GitHub. ``head_sha`` is the PR's current observed
  head and changes across remediation rounds while the PR identity stays
  stable; the exact reviewed head SHAs live as immutable history on the
  review records, never on the PR.
- ``state`` is the observed GitHub PR lifecycle (OPEN/CLOSED) and
  ``merged_at`` is the observed merge timestamp. A merged PR is a closed
  PR: ``merged_at`` requires the CLOSED state (mirroring the database
  CHECK). No richer lifecycle exists and observed state never becomes
  workflow authority by itself.
- Reviewer acceptance identity is the TaskPullRequest plus the exact
  reviewed head SHA. A changed head invalidates prior acceptance;
  movement of the PR target/base branch alone does not invalidate
  acceptance and is not part of the immutable review-subject identity.
- The #376 presentation and mergeability observations are nullable
  provider facts retained from successful reconciliations: ``title``,
  the nullable ``body`` verbatim, the verbatim ``html_url`` navigation
  fact, the bounded ``user_id``/``user_login`` subset (a deleted account
  reports no user), ``draft``, the provider lifecycle instants
  (``provider_created_at``/``provider_updated_at``/``provider_closed_at``
  — deliberately distinct from the record's own bookkeeping stamps),
  ``base_sha`` where authoritatively obtained, and the head-bound
  mergeability observation pair. ``mergeable`` is GitHub's documented
  conflict-mergeability answer (NULL while GitHub has not yet determined
  it — unknown, never zero, never false); ``merge_state_status`` is the
  documented ``mergeable_state`` vocabulary verbatim, where ``unknown``
  is preserved as the provider's own not-safely-observable reading.
  Conflict mergeability and the policy/check-dependent merge state stay
  distinct facts, and neither ever becomes workflow permission: GitHub
  remains the final merge authority. ``pr_mergeability_observed_at`` and
  ``pr_mergeability_head_sha`` move together and bind the last
  mergeability observation to the exact head it was observed at — a stale
  result is never presented as the new head's observation. Existing rows
  carry NULL for every newly supported fact until a reconciliation
  observes it; absence is honest unknown, never a fabricated fact.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "TaskPullRequest",
    "TaskPullRequestDomainError",
    "TaskPullRequestState",
    "task_pull_request_field_names",
]


class TaskPullRequestDomainError(Exception):
    """Raised when a TaskPullRequest domain invariant is violated."""


class TaskPullRequestState(StrEnum):
    """The observed GitHub PR lifecycle vocabulary (open/closed)."""

    OPEN = "open"
    CLOSED = "closed"


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a positive integer")


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a non-empty string")


def _require_nonblank_str_or_none(value: object, name: str) -> None:
    if value is not None:
        _require_nonblank_str(value, name)


def _require_timezone_aware_or_none(value: object, name: str) -> None:
    if value is not None and not (isinstance(value, datetime) and value.tzinfo is not None):
        raise TaskPullRequestDomainError(
            f"TaskPullRequest.{name} must be None or a timezone-aware datetime"
        )


@dataclass(frozen=True, slots=True)
class TaskPullRequest:
    """The one canonical pull request record for one Task.

    ``github_pr_id`` is the stable external GitHub PR identity;
    ``github_pr_number`` is repository-local address metadata; ``state``
    and ``merged_at`` are observed GitHub lifecycle facts; and
    ``head_ref``/``base_ref``/``head_sha`` are the mutable observed
    reconciliation state that changes across remediation rounds while the
    PR identity stays stable.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    repository_id: UUID
    github_pr_id: int
    github_pr_number: int
    head_ref: str
    base_ref: str
    head_sha: str
    state: TaskPullRequestState
    merged_at: datetime | None
    created_at: datetime
    updated_at: datetime
    # The #376 presentation facts: all nullable — an existing row has not yet
    # observed the fact until a successful reconciliation supplies it.
    title: str | None = None
    body: str | None = None
    html_url: str | None = None
    # The bounded user presentation subset: both present or both absent.
    user_id: int | None = None
    user_login: str | None = None
    draft: bool | None = None
    # Provider-reported lifecycle instants (distinct from the record's own
    # bookkeeping stamps above).
    provider_created_at: datetime | None = None
    provider_updated_at: datetime | None = None
    provider_closed_at: datetime | None = None
    # The base branch head SHA where GitHub authoritatively reported one.
    base_sha: str | None = None
    # The head-bound mergeability observation (issue #376): GitHub's
    # documented nullable conflict-mergeability answer and the verbatim
    # documented mergeable_state vocabulary. The observation pair moves
    # together and binds to the exact head it was observed at.
    mergeable: bool | None = None
    merge_state_status: str | None = None
    pr_mergeability_observed_at: datetime | None = None
    pr_mergeability_head_sha: str | None = None

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "repository_id"):
            _require_uuid(getattr(self, name), name)
        _require_positive_int(self.github_pr_id, "github_pr_id")
        _require_positive_int(self.github_pr_number, "github_pr_number")
        _require_nonblank_str(self.head_ref, "head_ref")
        _require_nonblank_str(self.base_ref, "base_ref")
        _require_nonblank_str(self.head_sha, "head_sha")
        if not isinstance(self.state, TaskPullRequestState):
            raise TaskPullRequestDomainError(
                "TaskPullRequest.state must be a TaskPullRequestState (open or closed)"
            )
        # Merged coherence, mirrored by the database CHECK: a merged PR is
        # a closed PR.
        if self.merged_at is not None and self.state is not TaskPullRequestState.CLOSED:
            raise TaskPullRequestDomainError(
                "a merged TaskPullRequest is closed: merged_at requires the closed state"
            )
        # The #376 presentation facts: every new fact is nullable — an existing
        # row has not yet observed it until a successful reconciliation
        # supplies it. Absence is honest unknown, never a fabricated fact.
        _require_nonblank_str_or_none(self.title, "title")
        _require_nonblank_str_or_none(self.html_url, "html_url")
        _require_nonblank_str_or_none(self.user_login, "user_login")
        _require_nonblank_str_or_none(self.base_sha, "base_sha")
        _require_nonblank_str_or_none(self.merge_state_status, "merge_state_status")
        _require_nonblank_str_or_none(self.pr_mergeability_head_sha, "pr_mergeability_head_sha")
        # The bounded user presentation subset: both present or both absent —
        # a partially observed user is never a readable fact.
        if (self.user_id is None) != (self.user_login is None):
            raise TaskPullRequestDomainError(
                "TaskPullRequest.user_id and user_login are present or absent together"
            )
        if self.user_id is not None:
            _require_positive_int(self.user_id, "user_id")
        if isinstance(self.draft, bool) is False and self.draft is not None:
            raise TaskPullRequestDomainError("TaskPullRequest.draft must be None or a boolean")
        for name in (
            "provider_created_at",
            "provider_updated_at",
            "provider_closed_at",
            "pr_mergeability_observed_at",
        ):
            _require_timezone_aware_or_none(getattr(self, name), name)
        # The mergeability observation evidence moves together: an observation
        # instant without its observed head (or the reverse) is not a readable
        # last-observed fact.
        if (self.pr_mergeability_observed_at is None) != (self.pr_mergeability_head_sha is None):
            raise TaskPullRequestDomainError(
                "TaskPullRequest.pr_mergeability_observed_at and "
                "pr_mergeability_head_sha are present or absent together"
            )


def task_pull_request_field_names() -> frozenset[str]:
    """Return the exact field set a TaskPullRequest exposes.

    Lets tests prove the record carries only its own facts: stable GitHub
    identity and local address metadata, the mutable observed
    reconciliation state, and no review outcomes (they live on the review
    records as per-iteration exact-head history).
    """
    return frozenset(field.name for field in fields(TaskPullRequest))
