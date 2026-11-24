"""Wire DTOs for the authorized GitHub issue read surface (issue #375).

Thin transport models only: the Owner-authorized read services own every
semantic rule (Workspace ownership, same-Workspace relation resolution,
DB-only action eligibility) and these models never duplicate it.

The ownership separation is structural: the ``github`` block carries the
GitHub-owned facts with native names/types/nullability (including the
bounded nested presentation subsets and the composed parent/sub-issue/
blocked-by relations), while the ``openorc`` block carries OpenOrc identity
and workflow facts (the internal projection UUID, current-Task identity,
and the backend-generated actions). Relation collections present at most
the documented bound with companion totals, so a client can distinguish a
complete collection from a transparently bounded one; ``null`` collections
are unobserved observations, ``[]`` are observed-empty ones. List items
deliberately carry no issue body; the detail resource carries the full
nullable GitHub body.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, StrictInt, StrictStr

__all__ = [
    "GitHubIssueBlockedByRefRead",
    "GitHubIssueDetailBlockRead",
    "GitHubIssueDetailRead",
    "GitHubIssueLabelRead",
    "GitHubIssueListItemBlockRead",
    "GitHubIssueListItemRead",
    "GitHubIssueListRead",
    "GitHubIssueMilestoneRead",
    "GitHubIssueParentRefRead",
    "GitHubIssueSubIssueRefRead",
    "GitHubIssueUserRefRead",
    "OpenOrcCurrentTaskRead",
    "OpenOrcIssueBlockRead",
]


class GitHubIssueUserRefRead(BaseModel):
    """The documented bounded user/assignee presentation subset."""

    id: StrictInt
    login: StrictStr


class GitHubIssueLabelRead(BaseModel):
    """The documented bounded label presentation subset."""

    id: StrictInt
    name: StrictStr
    color: StrictStr | None
    description: StrictStr | None


class GitHubIssueMilestoneRead(BaseModel):
    """The documented bounded milestone presentation subset."""

    id: StrictInt
    number: StrictInt
    title: StrictStr
    description: StrictStr | None
    state: StrictStr
    html_url: StrictStr
    due_on: datetime | None


class GitHubIssueParentRefRead(BaseModel):
    """One composed relation reference with its native presentation facts.

    The stable numeric endpoints always identify the relation; the native
    ``number``/``title``/``state``/``html_url`` facts resolve only through
    the same authorized Workspace's own projections and stay ``null``
    otherwise (never resolved through another Workspace).
    """

    github_repository_id: StrictInt
    github_issue_id: StrictInt
    number: StrictInt | None
    title: StrictStr | None
    state: StrictStr | None
    html_url: StrictStr | None


class GitHubIssueSubIssueRefRead(GitHubIssueParentRefRead):
    """One composed sub-issue reference."""


class GitHubIssueBlockedByRefRead(GitHubIssueParentRefRead):
    """One composed blocked-by dependency reference."""


class GitHubIssueDetailBlockRead(BaseModel):
    """The ``github`` block of the detail resource (full nullable body).

    Native names/types/meaning preserved, including nested objects and
    GitHub nulls. ``parent``/``sub_issues``/``blocked_by`` are composed from
    separate GitHub relationship operations: ``null`` means the relation was
    never observed, ``[]`` an observed empty one. ``sub_issues_total`` and
    ``blocked_by_total`` are the durable observed counts; when a collection
    presents fewer entries than its total, the presentation is transparently
    bounded — never silently truncated, and never an eligibility input.
    ``html_url`` is the provider-supplied link carried verbatim for
    "View on GitHub"; it is navigation, never authority.
    """

    id: StrictInt
    number: StrictInt
    title: StrictStr
    body: StrictStr | None
    state: StrictStr
    state_reason: StrictStr | None
    html_url: StrictStr | None
    created_at: datetime | None
    updated_at: datetime | None
    closed_at: datetime | None
    user: GitHubIssueUserRefRead | None
    labels: list[GitHubIssueLabelRead] | None
    assignees: list[GitHubIssueUserRefRead] | None
    milestone: GitHubIssueMilestoneRead | None
    parent: GitHubIssueParentRefRead | None
    sub_issues: list[GitHubIssueSubIssueRefRead]
    sub_issues_total: int
    blocked_by: list[GitHubIssueBlockedByRefRead]
    blocked_by_total: int


class GitHubIssueListItemBlockRead(BaseModel):
    """The ``github`` block of one list item (no full issue body).

    Identical native fact set to the detail block except that the full body
    is deliberately absent: the bounded list never carries issue bodies.
    """

    id: StrictInt
    number: StrictInt
    title: StrictStr
    state: StrictStr
    state_reason: StrictStr | None
    html_url: StrictStr | None
    created_at: datetime | None
    updated_at: datetime | None
    closed_at: datetime | None
    user: GitHubIssueUserRefRead | None
    labels: list[GitHubIssueLabelRead] | None
    assignees: list[GitHubIssueUserRefRead] | None
    milestone: GitHubIssueMilestoneRead | None
    parent: GitHubIssueParentRefRead | None
    sub_issues: list[GitHubIssueSubIssueRefRead]
    sub_issues_total: int
    blocked_by: list[GitHubIssueBlockedByRefRead]
    blocked_by_total: int


class OpenOrcCurrentTaskRead(BaseModel):
    """The current (non-archived) Task bound to the issue, when one exists."""

    id: UUID
    status: StrictStr


class OpenOrcIssueBlockRead(BaseModel):
    """The ``openorc`` block: OpenOrc identity and workflow facts.

    ``id`` is the internal projection UUID (the detail address);
    ``current_task`` is the current non-archived Task or ``null``;
    ``actions`` is the backend-generated action vocabulary — the single
    ``create_task`` token when the durable intake prerequisites positively
    support creation (observed open state, positively observed-empty
    dependencies, no current Task, usable route, operational account), and
    empty otherwise. The action is a hint revalidated freshly at the intake
    command boundary; it is never persisted and never fresh eligibility.
    """

    id: UUID
    workspace_id: UUID
    repository_id: UUID
    current_task: OpenOrcCurrentTaskRead | None
    actions: list[StrictStr]


class GitHubIssueDetailRead(BaseModel):
    """The ownership-separated detail resource (full nullable GitHub body)."""

    github: GitHubIssueDetailBlockRead
    openorc: OpenOrcIssueBlockRead


class GitHubIssueListItemRead(BaseModel):
    """The ownership-separated list item (no full GitHub body)."""

    github: GitHubIssueListItemBlockRead
    openorc: OpenOrcIssueBlockRead


class GitHubIssueListRead(BaseModel):
    """The bounded list envelope with its opaque deterministic cursor."""

    items: list[GitHubIssueListItemRead]
    next_cursor: StrictStr | None
