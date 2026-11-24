"""Scoped GitHub refresh tracking domain models (issue #375).

The minimum durable refresh-specific execution vocabulary for the scoped
on-demand GitHub refresh. This is not a generic job framework and not a
second workflow/event ledger: it records only the truthfully observable
lifecycle of one authorized refresh execution so its status can be reported
truthfully in both execution modes and across restarts.

- ``GitHubRefreshStatus`` is the closed outcome vocabulary: ``accepted →
  running → {completed, partially_completed, failed}`` plus the distinct
  ``unknown`` outcome for lost completion evidence. Unknown/uncertain
  admission and lost completion evidence are never success.
- ``GitHubRefreshSummary`` counts actual completed observations/durable
  changes, never queued work. ``GitHubRefreshUnitError`` is a bounded,
  sanitized typed per-unit classification: a family token, a safe error
  code, and only safe stable resource identifiers — never provider payloads,
  secrets, or other Workspace information.
- ``GitHubRefreshRecord`` is one admitted refresh's durable row projection.
  ``lease_expires_at`` bounds admission and execution claims; an expired
  accepted or running record is recoverable to ``unknown`` on demand, never
  silently claimable by a stale job.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

__all__ = [
    "GitHubRefreshRecord",
    "GitHubRefreshStatus",
    "GitHubRefreshSummary",
    "GitHubRefreshUnitError",
    "github_refresh_summary_from_json",
    "github_refresh_summary_json",
    "github_refresh_unit_errors_from_json",
    "github_refresh_unit_errors_json",
]


class GitHubRefreshStatus(StrEnum):
    """The closed lifecycle vocabulary of one tracked refresh execution."""

    ACCEPTED = "accepted"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIALLY_COMPLETED = "partially_completed"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class GitHubRefreshSummary:
    """The contract's completion counts, reflecting actual durable changes.

    Every count is the actual completed observation/durable change a refresh
    execution established — never queued or admitted work. ``None`` means
    the count is not yet established.
    """

    repositories_checked: int | None = None
    issues_discovered: int | None = None
    issues_updated: int | None = None
    issues_closed: int | None = None
    pull_requests_checked: int | None = None


def github_refresh_summary_json(summary: GitHubRefreshSummary) -> dict[str, object]:
    """Return the canonical JSON shape of one summary."""
    return {
        "repositories_checked": summary.repositories_checked,
        "issues_discovered": summary.issues_discovered,
        "issues_updated": summary.issues_updated,
        "issues_closed": summary.issues_closed,
        "pull_requests_checked": summary.pull_requests_checked,
    }


def github_refresh_summary_from_json(value: object) -> GitHubRefreshSummary | None:
    """Restore one summary from its canonical JSON shape."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("stored refresh summary JSON is malformed")
    return GitHubRefreshSummary(
        repositories_checked=value.get("repositories_checked"),
        issues_discovered=value.get("issues_discovered"),
        issues_updated=value.get("issues_updated"),
        issues_closed=value.get("issues_closed"),
        pull_requests_checked=value.get("pull_requests_checked"),
    )


@dataclass(frozen=True, slots=True)
class GitHubRefreshUnitError:
    """One bounded, sanitized per-unit refresh failure classification.

    ``family`` and ``code`` are closed safe vocabulary tokens;
    ``workspace_id``/``repository_id``/``github_issue_id``/``delivery_guid``
    are the unit's own safe stable identifiers. Provider payloads, secrets,
    and other Workspace information have no representation here.
    """

    family: str
    code: str
    workspace_id: UUID | None = None
    repository_id: UUID | None = None
    github_issue_id: int | None = None
    delivery_guid: str | None = None


def github_refresh_unit_errors_json(
    errors: tuple[GitHubRefreshUnitError, ...],
) -> list[dict[str, object]]:
    """Return the canonical JSON shape of the bounded error list."""
    return [
        {
            "family": error.family,
            "code": error.code,
            "workspace_id": None if error.workspace_id is None else str(error.workspace_id),
            "repository_id": None if error.repository_id is None else str(error.repository_id),
            "github_issue_id": error.github_issue_id,
            "delivery_guid": error.delivery_guid,
        }
        for error in errors
    ]


def github_refresh_unit_errors_from_json(
    value: object,
) -> tuple[GitHubRefreshUnitError, ...]:
    """Restore the bounded error list from its canonical JSON shape."""
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("stored refresh errors JSON is malformed")
    errors: list[GitHubRefreshUnitError] = []
    for entry in value:
        if not isinstance(entry, dict):
            raise ValueError("stored refresh error JSON is malformed")
        errors.append(
            GitHubRefreshUnitError(
                family=entry.get("family"),  # type: ignore[arg-type]
                code=entry.get("code"),  # type: ignore[arg-type]
                workspace_id=_optional_uuid(entry.get("workspace_id")),
                repository_id=_optional_uuid(entry.get("repository_id")),
                github_issue_id=entry.get("github_issue_id"),
                delivery_guid=entry.get("delivery_guid"),
            )
        )
    return tuple(errors)


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    return UUID(str(value))


@dataclass(frozen=True, slots=True)
class GitHubRefreshRecord:
    """One admitted refresh's durable row projection."""

    id: UUID
    workspace_id: UUID
    repository_id: UUID | None
    status: GitHubRefreshStatus
    requested_at: datetime
    started_at: datetime | None
    lease_expires_at: datetime | None
    completed_at: datetime | None
    summary: GitHubRefreshSummary | None
    errors: tuple[GitHubRefreshUnitError, ...]
