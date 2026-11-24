"""GitHub issue-source projection domain models (issue #59, extended #375).

The durable projection of authoritative GitHub Issue observations,
independent from the Task aggregate. Phase 1 deliberately keeps Task
identity/workflow state separate: nothing here copies issue fields onto a
Task, and later workflow services decide any source-change consequence.

- The projection is keyed by the stable GitHub issue ID within one Workspace
  Repository. The repository-local issue number is durable address metadata
  and never identity: issue-number reuse cannot substitute for stable issue
  identity.
- The projection carries the issue facts Phase 2 reconciliation, workflow,
  and the #375 authorized read surface need: stable identity, number, title,
  verbatim nullable body (GitHub reports issue bodies as ``string or null``),
  open/closed state, the deterministic requirements fingerprint, ordinary
  observed provider ``updated_at`` metadata, and — since issue #375 — the
  agreed presentation facts: ``state_reason``, the provider-supplied
  ``html_url`` (verbatim, never reconstructed), ``provider_created_at``,
  ``provider_closed_at``, and the bounded nested user/label/assignee/
  milestone subsets. Comments, reactions, and unrelated timeline events have
  no representation here and can therefore never move the requirements
  fingerprint.
- Every presentation field is nullable: ``None`` means the fact has not been
  observed for this row (a row projected before the field existed, or a
  provider fact that is itself absent). Collection facts
  (``labels``/``assignees``) distinguish unobserved (``None``) from observed
  empty (``()``); nested nullable fields preserve GitHub nulls verbatim.
  Presentation facts are never fabricated from defaults — an unobserved fact
  stays honest until a successful reconciliation supplies it.
- ``requirements_fingerprint`` is one deterministic digest over the canonical
  title + body representation — the only inputs OpenOrc treats as
  engineering intent for v1. It is a persisted-equality digest (an ordinary
  cryptographic digest suitable for durable comparison), deliberately not a
  security/authentication value, and it is computed with ``hashlib``, never
  with Python's process-randomized ``hash()``. Presentation changes are
  persisted through the reconciliation path but can never move the
  fingerprint, invalidate a Task, or invent source-drift evidence.
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
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "GitHubIssueDomainError",
    "GitHubIssueIdentity",
    "GitHubIssueLabel",
    "GitHubIssueMilestone",
    "GitHubIssueObservedFacts",
    "GitHubIssueProjection",
    "GitHubIssueState",
    "GitHubIssueUserRef",
    "github_issue_assignees_from_json",
    "github_issue_assignees_json",
    "github_issue_field_names",
    "github_issue_labels_from_json",
    "github_issue_labels_json",
    "github_issue_milestone_from_json",
    "github_issue_milestone_json",
    "github_issue_requirements_fingerprint",
    "github_issue_user_from_json",
    "github_issue_user_json",
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


@dataclass(frozen=True, slots=True)
class GitHubIssueUserRef:
    """The documented bounded user/assignee presentation subset (issue #375).

    ``id`` is the stable numeric GitHub user identity and ``login`` the
    display handle; exactly the documented native-field subset the read
    surface projects. No avatar URLs, profile navigation, or e-mail data.
    """

    id: int
    login: str

    def __post_init__(self) -> None:
        if isinstance(self.id, bool) or not isinstance(self.id, int) or self.id <= 0:
            raise GitHubIssueDomainError(
                f"GitHubIssueUserRef.id must be a positive integer, got {self.id!r}"
            )
        if not isinstance(self.login, str) or not self.login.strip():
            raise GitHubIssueDomainError("GitHubIssueUserRef.login must be a non-empty string")


@dataclass(frozen=True, slots=True)
class GitHubIssueLabel:
    """The documented bounded label presentation subset (issue #375).

    ``id``/``name`` identify the label; ``color``/``description`` carry the
    documented nullable presentation fields verbatim.
    """

    id: int
    name: str
    color: str | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.id, bool) or not isinstance(self.id, int) or self.id <= 0:
            raise GitHubIssueDomainError(
                f"GitHubIssueLabel.id must be a positive integer, got {self.id!r}"
            )
        if not isinstance(self.name, str) or not self.name.strip():
            raise GitHubIssueDomainError("GitHubIssueLabel.name must be a non-empty string")
        if self.color is not None and not isinstance(self.color, str):
            raise GitHubIssueDomainError("GitHubIssueLabel.color must be None or a string")
        if self.description is not None and not isinstance(self.description, str):
            raise GitHubIssueDomainError("GitHubIssueLabel.description must be None or a string")


@dataclass(frozen=True, slots=True)
class GitHubIssueMilestone:
    """The documented bounded milestone presentation subset (issue #375).

    ``due_on`` preserves GitHub's nullable due instant verbatim; ``state``
    carries GitHub's milestone state string. All fields are the documented
    native subset, never raw provider payload.
    """

    id: int
    number: int
    title: str
    state: str
    html_url: str
    description: str | None = None
    due_on: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("id", "number"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise GitHubIssueDomainError(
                    f"GitHubIssueMilestone.{name} must be a positive integer, got {value!r}"
                )
        if not isinstance(self.title, str) or not self.title.strip():
            raise GitHubIssueDomainError("GitHubIssueMilestone.title must be a non-empty string")
        if not isinstance(self.state, str) or not self.state.strip():
            raise GitHubIssueDomainError("GitHubIssueMilestone.state must be a non-empty string")
        if not isinstance(self.html_url, str) or not self.html_url.strip():
            raise GitHubIssueDomainError("GitHubIssueMilestone.html_url must be a non-empty string")
        if self.description is not None and not isinstance(self.description, str):
            raise GitHubIssueDomainError(
                "GitHubIssueMilestone.description must be None or a string"
            )
        if self.due_on is not None and not isinstance(self.due_on, datetime):
            raise GitHubIssueDomainError("GitHubIssueMilestone.due_on must be None or a datetime")


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

    The issue #375 presentation facts follow, all defaulting to ``None``
    (unobserved): ``state_reason``, the provider-supplied ``html_url``,
    ``provider_created_at``/``provider_closed_at``, the bounded nested
    ``user``/``labels``/``assignees``/``milestone`` subsets, and the
    ``hierarchy_observed_at``/``dependencies_observed_at`` observation
    evidence instants. Collection facts distinguish unobserved (``None``)
    from observed empty (an empty tuple); nested nullable fields preserve
    GitHub nulls verbatim. These facts never feed the requirements
    fingerprint and are never fabricated from defaults.
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
    state_reason: str | None = None
    html_url: str | None = None
    provider_created_at: datetime | None = None
    provider_closed_at: datetime | None = None
    user: GitHubIssueUserRef | None = None
    labels: tuple[GitHubIssueLabel, ...] | None = None
    assignees: tuple[GitHubIssueUserRef, ...] | None = None
    milestone: GitHubIssueMilestone | None = None
    hierarchy_observed_at: datetime | None = None
    dependencies_observed_at: datetime | None = None

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
        if self.state_reason is not None and not isinstance(self.state_reason, str):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.state_reason must be None or a string"
            )
        if self.html_url is not None and (
            not isinstance(self.html_url, str) or not self.html_url.strip()
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.html_url must be None or a non-empty string"
            )
        for name in ("provider_created_at", "provider_closed_at", "provider_updated_at"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, datetime):
                raise GitHubIssueDomainError(
                    f"GitHubIssueProjection.{name} must be None or a datetime"
                )
        if self.user is not None and not isinstance(self.user, GitHubIssueUserRef):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.user must be None or a GitHubIssueUserRef"
            )
        if self.labels is not None and (
            not isinstance(self.labels, tuple)
            or not all(isinstance(value, GitHubIssueLabel) for value in self.labels)
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.labels must be None or a tuple of GitHubIssueLabel"
            )
        if self.assignees is not None and (
            not isinstance(self.assignees, tuple)
            or not all(isinstance(value, GitHubIssueUserRef) for value in self.assignees)
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.assignees must be None or a tuple of GitHubIssueUserRef"
            )
        if self.milestone is not None and not isinstance(self.milestone, GitHubIssueMilestone):
            raise GitHubIssueDomainError(
                "GitHubIssueProjection.milestone must be None or a GitHubIssueMilestone"
            )
        for name in ("hierarchy_observed_at", "dependencies_observed_at"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, datetime):
                raise GitHubIssueDomainError(
                    f"GitHubIssueProjection.{name} must be None or a datetime"
                )


def github_issue_field_names() -> frozenset[str]:
    """Return the exact field set a GitHubIssueProjection exposes.

    Lets tests prove the projection carries only the documented issue facts
    and no credential-bearing or raw-payload-surplus field: comments,
    reactions, timeline events, and arbitrary provider navigation data have
    no supported column here. The #375 presentation facts are the agreed
    bounded native-field subsets, not raw provider payload.
    """
    return frozenset(field.name for field in fields(GitHubIssueProjection))


def _canonical_user_json(value: GitHubIssueUserRef) -> dict[str, object]:
    return {"id": value.id, "login": value.login}


def github_issue_user_json(value: GitHubIssueUserRef | None) -> dict[str, object] | None:
    """Return the canonical JSON shape of one nullable user/assignee fact."""
    return None if value is None else _canonical_user_json(value)


def github_issue_user_from_json(value: object) -> GitHubIssueUserRef | None:
    """Restore one nullable user/assignee fact from its canonical JSON shape.

    A SQL ``NULL`` and JSON ``null`` both mean the fact is unobserved (or
    GitHub-reported absent); a malformed stored shape is a durable invariant
    failure, never a silent reinterpretation.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise GitHubIssueDomainError("stored user presentation JSON is malformed")
    return GitHubIssueUserRef(
        id=value.get("id"),  # type: ignore[arg-type]
        login=value.get("login"),  # type: ignore[arg-type]
    )


def github_issue_labels_json(
    values: tuple[GitHubIssueLabel, ...] | None,
) -> list[dict[str, object]] | None:
    """Return the canonical JSON shape of the nullable label collection."""
    if values is None:
        return None
    return [
        {
            "id": label.id,
            "name": label.name,
            "color": label.color,
            "description": label.description,
        }
        for label in values
    ]


def github_issue_labels_from_json(
    value: object,
) -> tuple[GitHubIssueLabel, ...] | None:
    """Restore the nullable label collection from its canonical JSON shape."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise GitHubIssueDomainError("stored labels presentation JSON is malformed")
    labels: list[GitHubIssueLabel] = []
    for entry in value:
        if not isinstance(entry, dict):
            raise GitHubIssueDomainError("stored label presentation JSON is malformed")
        labels.append(
            GitHubIssueLabel(
                id=entry.get("id"),  # type: ignore[arg-type]
                name=entry.get("name"),  # type: ignore[arg-type]
                color=entry.get("color"),
                description=entry.get("description"),
            )
        )
    return tuple(labels)


def github_issue_assignees_json(
    values: tuple[GitHubIssueUserRef, ...] | None,
) -> list[dict[str, object]] | None:
    """Return the canonical JSON shape of the nullable assignee collection."""
    if values is None:
        return None
    return [_canonical_user_json(value) for value in values]


def github_issue_assignees_from_json(
    value: object,
) -> tuple[GitHubIssueUserRef, ...] | None:
    """Restore the nullable assignee collection from its canonical JSON shape."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise GitHubIssueDomainError("stored assignees presentation JSON is malformed")
    assignees: list[GitHubIssueUserRef] = []
    for entry in value:
        if not isinstance(entry, dict):
            raise GitHubIssueDomainError("stored assignee presentation JSON is malformed")
        assignees.append(
            GitHubIssueUserRef(
                id=entry.get("id"),  # type: ignore[arg-type]
                login=entry.get("login"),  # type: ignore[arg-type]
            )
        )
    return tuple(assignees)


def github_issue_milestone_json(value: GitHubIssueMilestone | None) -> dict[str, object] | None:
    """Return the canonical JSON shape of the nullable milestone fact."""
    if value is None:
        return None
    return {
        "id": value.id,
        "number": value.number,
        "title": value.title,
        "description": value.description,
        "state": value.state,
        "html_url": value.html_url,
        "due_on": None if value.due_on is None else value.due_on.astimezone(UTC).isoformat(),
    }


def github_issue_milestone_from_json(value: object) -> GitHubIssueMilestone | None:
    """Restore the nullable milestone fact from its canonical JSON shape."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise GitHubIssueDomainError("stored milestone presentation JSON is malformed")
    due_on_raw = value.get("due_on")
    due_on = None
    if due_on_raw is not None:
        if not isinstance(due_on_raw, str):
            raise GitHubIssueDomainError("stored milestone due instant is malformed")
        due_on = datetime.fromisoformat(due_on_raw)
    return GitHubIssueMilestone(
        id=value.get("id"),  # type: ignore[arg-type]
        number=value.get("number"),  # type: ignore[arg-type]
        title=value.get("title"),  # type: ignore[arg-type]
        state=value.get("state"),  # type: ignore[arg-type]
        html_url=value.get("html_url"),  # type: ignore[arg-type]
        description=value.get("description"),
        due_on=due_on,
    )


@dataclass(frozen=True, slots=True)
class GitHubIssueObservedFacts:
    """The reconciliation-mutable observed fact set of one issue observation.

    One coherent snapshot grouping every fact a reconciliation may durably
    advance — the #59 core facts plus the #375 presentation facts — so the
    serialized conditional update compares and writes one whole observation.
    Identity, Workspace/Repository scope, and the local record instants are
    deliberately absent: they are immutable or persistence-owned. The
    requirements fingerprint stays exactly the title+body digest; the
    presentation facts can never move it.
    """

    issue_number: int
    title: str
    body: str | None
    state: GitHubIssueState
    requirements_fingerprint: str
    provider_updated_at: datetime | None
    state_reason: str | None = None
    html_url: str | None = None
    provider_created_at: datetime | None = None
    provider_closed_at: datetime | None = None
    user: GitHubIssueUserRef | None = None
    labels: tuple[GitHubIssueLabel, ...] | None = None
    assignees: tuple[GitHubIssueUserRef, ...] | None = None
    milestone: GitHubIssueMilestone | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.issue_number, bool)
            or not isinstance(self.issue_number, int)
            or self.issue_number <= 0
        ):
            raise GitHubIssueDomainError(
                f"GitHubIssueObservedFacts.issue_number must be a positive integer, "
                f"got {self.issue_number!r}"
            )
        if not isinstance(self.title, str) or not self.title.strip():
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.title must be a non-empty string"
            )
        if self.body is not None and not isinstance(self.body, str):
            raise GitHubIssueDomainError("GitHubIssueObservedFacts.body must be None or a string")
        if not isinstance(self.state, GitHubIssueState):
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.state must be a GitHubIssueState value"
            )
        if not isinstance(self.requirements_fingerprint, str) or not _FINGERPRINT_PATTERN.match(
            self.requirements_fingerprint
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.requirements_fingerprint must be a "
                "lowercase hex SHA-256 digest"
            )
        if self.user is not None and not isinstance(self.user, GitHubIssueUserRef):
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.user must be None or a GitHubIssueUserRef"
            )
        if self.labels is not None and (
            not isinstance(self.labels, tuple)
            or not all(isinstance(value, GitHubIssueLabel) for value in self.labels)
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.labels must be None or a tuple of GitHubIssueLabel"
            )
        if self.assignees is not None and (
            not isinstance(self.assignees, tuple)
            or not all(isinstance(value, GitHubIssueUserRef) for value in self.assignees)
        ):
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.assignees must be None or a tuple of GitHubIssueUserRef"
            )
        if self.milestone is not None and not isinstance(self.milestone, GitHubIssueMilestone):
            raise GitHubIssueDomainError(
                "GitHubIssueObservedFacts.milestone must be None or a GitHubIssueMilestone"
            )
