"""Wire DTOs for the scoped on-demand GitHub refresh surface (issue #375).

Thin transport models only: the Owner-authorized admission/status services
own every semantic rule and these models never duplicate it. The admission
resource is the record accepted for delivery — never a completion claim;
the status resource presents stored truth (statuses: accepted, running,
completed, partially_completed, failed, unknown) with null timestamps and
summary until established, and the bounded sanitized per-unit error
classifications (family, safe resource identities, error code — never
provider payloads, secrets, or other Workspace information).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, StrictInt, StrictStr

__all__ = [
    "GitHubRefreshAdmissionRead",
    "GitHubRefreshStatusRead",
    "GitHubRefreshSummaryRead",
    "GitHubRefreshUnitErrorRead",
]


class GitHubRefreshAdmissionRead(BaseModel):
    """The admitted refresh record: authorization and submission acceptance.

    A 202 response with this body acknowledges exactly that — never
    delivery, never execution, never completion.
    """

    refresh_id: UUID
    workspace_id: UUID
    repository_id: UUID | None
    status: StrictStr


class GitHubRefreshSummaryRead(BaseModel):
    """The completion counts: actual completed observations/changes.

    ``null`` until established; counts never reflect queued work.
    """

    repositories_checked: StrictInt | None
    issues_discovered: StrictInt | None
    issues_updated: StrictInt | None
    issues_closed: StrictInt | None
    pull_requests_checked: StrictInt | None


class GitHubRefreshUnitErrorRead(BaseModel):
    """One bounded sanitized per-unit failure classification."""

    family: StrictStr
    code: StrictStr
    workspace_id: UUID | None
    repository_id: UUID | None
    github_issue_id: StrictInt | None
    delivery_guid: StrictStr | None


class GitHubRefreshStatusRead(BaseModel):
    """One refresh record's truthfully presented status projection.

    ``status`` follows the closed vocabulary; an expired
    ``accepted``/``running`` record presents as ``unknown`` (lost completion
    evidence is never success). Timestamps and summary stay ``null`` until
    established.
    """

    refresh_id: UUID
    workspace_id: UUID
    repository_id: UUID | None
    status: StrictStr
    started_at: datetime | None
    completed_at: datetime | None
    summary: GitHubRefreshSummaryRead | None
    errors: list[GitHubRefreshUnitErrorRead]
