"""GitHub issue relationship-mirror domain models (issue #60).

The presentation-only mirrors of authoritative GitHub parent/sub-issue
hierarchy and blocked-by dependency observations, independent from the Task
aggregate and deliberately without any Task-eligibility semantics:

- Hierarchy (parent/sub-issue state) is descriptive only. It exists for
  UI/context/progress and never gates Task intake: an issue may create a
  Task whether or not it has a parent or sub-issues, and hierarchy changes
  alone never make a Task ineligible or blocked.
- Blocked-by dependencies are the authoritative GitHub blocking facts. The
  current blocked state of an issue is derived from its dependency-edge set
  (an empty set is not blocked); OpenOrc never invents a blocker-resolution
  rule of its own.
- Both identity spaces are explicit: the SUBJECT side of every edge is the
  local OpenOrc `(workspace_id, repository_id, github_issue_id)` triple where
  ``repository_id`` is the OpenOrc Repository UUID; the RELATED side is the
  plain stable numeric GitHub pair ``(github_repository_id, github_issue_id)``
  — a related repository need not be configured as an OpenOrc Repository in
  this Workspace, so the related endpoint is never a local foreign key and
  mutable owner/login/name data is never identity.
- The mirrors carry no freshness metadata: they are, by definition, the last
  successfully observed projection. Failed or unobservable GitHub reads
  leave the durable mirror byte-identical; errors are never reinterpreted as
  authoritative relationship state.

This module carries transport-independent validation only. It performs no
observation, no synchronization, and no intake decisions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from uuid import UUID

__all__ = [
    "GitHubIssueDependencyEdge",
    "GitHubIssueParentEdge",
    "GitHubIssueRelationsDomainError",
    "GitHubIssueSubIssueEdge",
    "IssueBlockingState",
    "RelatedIssueEndpoint",
    "dependency_edge_field_names",
    "parent_edge_field_names",
    "sub_issue_edge_field_names",
]

_ENDPOINT_FINGERPRINTLESS_IDS = re.compile(r"[0-9]+")
_FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class GitHubIssueRelationsDomainError(Exception):
    """Raised when a GitHub issue-relationship-mirror invariant is violated."""


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GitHubIssueRelationsDomainError(f"{name} must be a positive integer")


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise GitHubIssueRelationsDomainError(f"{name} must be a UUID")


@dataclass(frozen=True, slots=True)
class RelatedIssueEndpoint:
    """The stable numeric GitHub identity of one related issue.

    The RELATED side of a relationship edge: the stable numeric GitHub
    repository ID plus the stable numeric GitHub issue ID. Deliberately
    never a local OpenOrc foreign key — the related repository may not be
    configured as an OpenOrc Repository in this Workspace — and never
    resolved from mutable owner/login/name data.
    """

    github_repository_id: int
    github_issue_id: int

    def __post_init__(self) -> None:
        _require_positive_int(self.github_repository_id, "github_repository_id")
        _require_positive_int(self.github_issue_id, "github_issue_id")


@dataclass(frozen=True, slots=True)
class _SubjectScope:
    """Shared subject-side validation for the three edge value objects."""

    workspace_id: UUID
    repository_id: UUID
    github_issue_id: int

    def __post_init__(self) -> None:
        _require_uuid(self.workspace_id, "workspace_id")
        _require_uuid(self.repository_id, "repository_id")
        _require_positive_int(self.github_issue_id, "github_issue_id")


@dataclass(frozen=True, slots=True)
class GitHubIssueParentEdge(_SubjectScope):
    """One authoritative parent edge of a subject issue (at most one exists)."""

    parent: RelatedIssueEndpoint

    def __post_init__(self) -> None:
        _SubjectScope.__post_init__(self)
        if not isinstance(self.parent, RelatedIssueEndpoint):
            raise GitHubIssueRelationsDomainError(
                "GitHubIssueParentEdge.parent must be a RelatedIssueEndpoint"
            )


@dataclass(frozen=True, slots=True)
class GitHubIssueSubIssueEdge(_SubjectScope):
    """One observed child edge from a subject issue's sub-issue listing."""

    child: RelatedIssueEndpoint

    def __post_init__(self) -> None:
        _SubjectScope.__post_init__(self)
        if not isinstance(self.child, RelatedIssueEndpoint):
            raise GitHubIssueRelationsDomainError(
                "GitHubIssueSubIssueEdge.child must be a RelatedIssueEndpoint"
            )


@dataclass(frozen=True, slots=True)
class GitHubIssueDependencyEdge(_SubjectScope):
    """One authoritative blocked-by edge GitHub currently reports."""

    blocker: RelatedIssueEndpoint

    def __post_init__(self) -> None:
        _SubjectScope.__post_init__(self)
        if not isinstance(self.blocker, RelatedIssueEndpoint):
            raise GitHubIssueRelationsDomainError(
                "GitHubIssueDependencyEdge.blocker must be a RelatedIssueEndpoint"
            )


class IssueBlockingState:
    """The intake-blocking classification of one fresh dependency observation.

    ``NOT_BLOCKED`` — GitHub currently reports no blocked-by dependency for
    the issue; hierarchy alone must never prevent intake. ``BLOCKED`` —
    GitHub currently reports at least one blocked-by dependency. The
    unobservable case is not a member: an unobservable authoritative blocking
    state is a typed external-outcome error (fail closed), never a state.
    """

    NOT_BLOCKED = "not_blocked"
    BLOCKED = "blocked"


def parent_edge_field_names() -> frozenset[str]:
    """Return the exact field set a GitHubIssueParentEdge exposes."""
    return frozenset(field.name for field in fields(GitHubIssueParentEdge))


def sub_issue_edge_field_names() -> frozenset[str]:
    """Return the exact field set a GitHubIssueSubIssueEdge exposes."""
    return frozenset(field.name for field in fields(GitHubIssueSubIssueEdge))


def dependency_edge_field_names() -> frozenset[str]:
    """Return the exact field set a GitHubIssueDependencyEdge exposes."""
    return frozenset(field.name for field in fields(GitHubIssueDependencyEdge))


# The relationship mirrors carry no eligibility semantics: their field sets
# contain no Task/eligibility/blocking-outcome field at all. The digest
# pattern above is shared with the Task source baseline vocabulary (validated
# there through the Task aggregate), so it is checked, not exported.
del _FINGERPRINT_PATTERN, _ENDPOINT_FINGERPRINTLESS_IDS
