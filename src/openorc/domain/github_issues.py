"""GitHub issue-source projection domain models (issue #59).

The durable projection of authoritative GitHub Issue observations,
independent from the Task aggregate. Phase 1 deliberately keeps Task
identity/workflow state separate: nothing here copies issue fields onto a
Task, and later workflow services decide any source-change consequence.

- The projection is keyed by the stable GitHub issue ID within one Workspace
  Repository. The repository-local issue number is durable address metadata
  and never identity: issue-number reuse cannot substitute for stable issue
  identity.
- The projection carries only the issue facts Phase 2 reconciliation and
  workflow need: stable identity, number, title, verbatim nullable body
  (GitHub reports issue bodies as ``string or null``), open/closed state,
  the deterministic requirements fingerprint, and ordinary observed
  provider ``updated_at`` metadata. Comments (including OpenOrc's future
  published review-cleared plan comment), labels, assignees, reactions, and
  unrelated timeline events have no representation here and can therefore
  never move the requirements fingerprint.
- ``requirements_fingerprint`` is one deterministic digest over the canonical
  title + body representation — the only inputs OpenOrc treats as
  engineering intent for v1. It is a persisted-equality digest (an ordinary
  cryptographic digest suitable for durable comparison), deliberately not a
  security/authentication value, and it is computed with ``hashlib``, never
  with Python's process-randomized ``hash()``.
- Canonicalization is explicit and stable: the digest input is
  ``title UTF-8 + NUL + body-or-empty UTF-8``. A NULL body and an empty body
  carry identical requirements content and therefore produce the same
  fingerprint; the NUL separator makes the two-field concatenation
  unambiguous ("ab" + "c" and "a" + "bc" can never collide).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "GitHubIssueDomainError",
    "GitHubIssueIdentity",
    "GitHubIssueProjection",
    "GitHubIssueState",
    "github_issue_field_names",
    "github_issue_requirements_fingerprint",
]

_FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class GitHubIssueDomainError(Exception):
    """Raised when a GitHub issue-projection invariant is violated."""


class GitHubIssueState(StrEnum):
    """The observed open/closed state vocabulary of a GitHub issue.

    Values are exactly GitHub's documented issue ``state`` strings, so the
    provider observation converts without semantic invention. Closed issue
    *reasons* and other presentation facts are deliberately absent: they are
    not requirements state.
    """

    OPEN = "open"
    CLOSED = "closed"


def github_issue_requirements_fingerprint(title: str, body: str | None) -> str:
    """Return the deterministic requirements fingerprint of one issue.

    The canonical representation is ``title`` encoded UTF-8, one NUL byte,
    then the body encoded UTF-8 where a ``None`` body canonicalizes to the
    empty string (GitHub's nullable body and an empty body carry the same
    requirements content). The result is the lowercase hex SHA-256 digest:
    deterministic across processes and platforms, suitable for persisted
    equality comparison, and never used for security authentication.
    """

    if not isinstance(title, str) or not title.strip():
        raise GitHubIssueDomainError(f"the issue title must be a non-empty string, got {title!r}")
    if body is not None and not isinstance(body, str):
        raise GitHubIssueDomainError(
            f"the issue body must be None or a string, got {type(body).__name__}"
        )
    canonical = title.encode("utf-8") + b"\x00" + ("" if body is None else body).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True, slots=True)
class GitHubIssueIdentity:
    """Stable external identity of a GitHub issue.

    GitHub issue IDs are stable and globally unique; they survive edits,
    state changes, commenting, and labeling. The repository-local issue
    number is an address within one repository and is deliberately absent
    from this value object: number reuse (delete/recreate, transfer) must
    never substitute for identity.
    """

    github_issue_id: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.github_issue_id, bool)
            or not isinstance(self.github_issue_id, int)
            or self.github_issue_id <= 0
        ):
            raise GitHubIssueDomainError(
                f"github_issue_id must be a positive integer, got {self.github_issue_id!r}"
            )


@dataclass(frozen=True, slots=True)
class GitHubIssueProjection:
    """One durable GitHub Issue projection within one Workspace Repository.

    ``id`` is the OpenOrc projection-record identity. ``identity`` is the
    stable external GitHub issue identity used for reconciliation;
    ``issue_number`` is durable address metadata. ``body`` stores GitHub's
    observed value verbatim — ``None`` means GitHub reported a null body.
    ``requirements_fingerprint`` is the deterministic title+body digest;
    ``provider_updated_at`` is ordinary observed provider metadata carried
    verbatim (never a write-ordering or version authority).
    """

    id: UUID
    workspace_id: UUID
    repository_id: UUID
    identity: GitHubIssueIdentity
    issue_number: int
    title: str
    body: str | None
    state: GitHubIssueState
    requirements_fingerprint: str
    provider_updated_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "repository_id"):
            if not isinstance(getattr(self, name), UUID):
                raise GitHubIssueDomainError(f"GitHubIssueProjection.{name} must be a UUID")
        if (
            isinstance(self.issue_number, bool)
            or not isinstance(self.issue_number, int)
            or self.issue_number <= 0
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.issue_number must be a positive integer, "
                f"got {self.issue_number!r}"
            )
        if not isinstance(self.title, str) or not self.title.strip():
            raise GitHubIssueDomainError("GitHubIssueProjection.title must be a non-empty string")
        if self.body is not None and not isinstance(self.body, str):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.body must be None or a string, "
                f"got {type(self.body).__name__}"
            )
        if not isinstance(self.state, GitHubIssueState):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.state must be a GitHubIssueState value"
            )
        if not isinstance(self.requirements_fingerprint, str) or not _FINGERPRINT_PATTERN.match(
            self.requirements_fingerprint
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.requirements_fingerprint must be a "
                "lowercase hex SHA-256 digest"
            )


def github_issue_field_names() -> frozenset[str]:
    """Return the exact field set a GitHubIssueProjection exposes.

    Lets tests prove the projection carries only the issue facts Phase 2
    reconciliation needs and no credential-bearing or presentation-surplus
    field: comments, labels, assignees, reactions, timeline events, and raw
    provider payloads have no supported column here.
    """
    return frozenset(field.name for field in fields(GitHubIssueProjection))
