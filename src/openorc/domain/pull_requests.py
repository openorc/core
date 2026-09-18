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


def task_pull_request_field_names() -> frozenset[str]:
    """Return the exact field set a TaskPullRequest exposes.

    Lets tests prove the record carries only its own facts: stable GitHub
    identity and local address metadata, the mutable observed
    reconciliation state, and no review outcomes (they live on the review
    records as per-iteration exact-head history).
    """
    return frozenset(field.name for field in fields(TaskPullRequest))
