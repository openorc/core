"""Authorized GitHub issue read services (issue #375).

The read-side composition behind the authorized issue list/detail surface:
Owner-authorized, database-only, and strictly side-effect free. It composes
the durable projection facts with the presentation-only relationship mirrors
(#60), the same-Workspace related-projection resolution, the current-Task
identity, and the backend-generated ``create_task`` action eligibility —
reusing the existing intake prerequisites; never a second policy engine and
never persisted.

Relationship presentation is bounded transparently: each collection presents
at most :data:`RELATION_PRESENTATION_BOUND` entries in deterministic
stable-identity order, and the companion total (the durable observation's
full count) lets clients distinguish a complete collection from a bounded
one. The unobserved vs observed-empty distinction follows the relations
observation-evidence instants (#375), never the presentation bound.
Eligibility is computed from the underlying durable dependency facts, so
presentation truncation never invalidates an otherwise complete durable
observation.

Cross-Workspace isolation is absolute: a related issue resolves only through
the same authorized Workspace's own projections — a related Repository need
not be onboarded, but its projection is never resolved through another
Workspace, and unresolvable references stay honestly null.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from uuid import UUID

from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.domain.github_issues import GitHubIssueProjection, GitHubIssueState
from openorc.domain.ownership import GitHubRepositoryIdentity
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_issue_relations import (
    find_github_issue_parent_edge,
    find_github_issue_sub_issue_edges,
    list_github_issue_blocked_by_edges,
)
from openorc.persistence.github_issues import (
    GitHubIssuePageState,
    find_github_issue,
    find_github_issue_by_id,
    list_repository_github_issues_page,
)
from openorc.persistence.ownership import find_repository_by_github_identity
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import find_current_task_for_issue
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.task_intake import durably_eligible_for_create_task_action
from openorc.services.workspace_authorization import require_workspace_repository

__all__ = [
    "ISSUE_PAGE_LIMIT",
    "RELATION_PRESENTATION_BOUND",
    "RepositoryIssueDetail",
    "RepositoryIssueListItem",
    "ResolvedRelatedIssue",
    "decode_issue_page_cursor",
    "encode_issue_page_cursor",
    "get_repository_issue_for_owner",
    "list_repository_issues_for_owner",
]

# The bounded list page: one authorized list request returns at most this
# many items, advanced by the opaque keyset cursor.
ISSUE_PAGE_LIMIT = 50
# The transparent relation-collection presentation bound: a collection with
# more observed entries presents the first bound in deterministic order with
# its durable total as the companion count.
RELATION_PRESENTATION_BOUND = 50

_READS_TRACER_SCOPE = "openorc.services.github_issue_reads"
_LIST_SPAN_NAME = "github_issue_reads.list_repository_issues_for_owner"
_DETAIL_SPAN_NAME = "github_issue_reads.get_repository_issue_for_owner"


@dataclass(frozen=True, slots=True)
class ResolvedRelatedIssue:
    """The native presentation facts of a same-Workspace related projection."""

    number: int
    title: str
    state: GitHubIssueState
    html_url: str | None


@dataclass(frozen=True, slots=True)
class RepositoryIssueListItem:
    """One authorized issue's composed read model (list shape, no body use)."""

    projection: GitHubIssueProjection
    parent: RelatedIssueEndpoint | None
    sub_issues: tuple[RelatedIssueEndpoint, ...]
    blocked_by: tuple[RelatedIssueEndpoint, ...]
    resolved_related: dict[tuple[int, int], ResolvedRelatedIssue]
    current_task: Task | None
    create_task_action: bool


@dataclass(frozen=True, slots=True)
class RepositoryIssueDetail:
    """One authorized issue's composed read model (detail, full body)."""

    item: RepositoryIssueListItem


def encode_issue_page_cursor(last_issue_number: int) -> str:
    """Return the opaque keyset cursor for one page's last issue number."""
    raw = f"n:{last_issue_number}".encode("ascii")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_issue_page_cursor(cursor: str) -> int:
    """Restore one opaque keyset cursor's issue number, failing closed."""
    if not isinstance(cursor, str) or not cursor.strip():
        raise InvalidCommandError("the page cursor is malformed")
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        text = raw.decode("ascii")
        if not text.startswith("n:"):
            raise ValueError("unknown cursor form")
        number = int(text[2:])
    except (ValueError, UnicodeDecodeError) as error:
        raise InvalidCommandError("the page cursor is malformed") from error
    if number <= 0:
        raise InvalidCommandError("the page cursor is malformed")
    return number


def _resolve_related(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    endpoints: tuple[RelatedIssueEndpoint, ...],
) -> dict[tuple[int, int], ResolvedRelatedIssue]:
    """Resolve related endpoints through the same Workspace's own projections.

    A related Repository need not be onboarded as an OpenOrc Repository in
    this Workspace; when its projection exists here, the native number/
    title/state/html_url presentation facts resolve. References are never
    resolved through another Workspace, and unresolvable references stay
    absent from the map (the read model presents them unresolved).
    """
    resolved: dict[tuple[int, int], ResolvedRelatedIssue] = {}
    for endpoint in endpoints:
        key = (endpoint.github_repository_id, endpoint.github_issue_id)
        if key in resolved:
            continue
        repository = find_repository_by_github_identity(
            pool,
            workspace_id=workspace_id,
            identity=GitHubRepositoryIdentity(github_repository_id=endpoint.github_repository_id),
        )
        if repository is None:
            continue
        projection = find_github_issue(
            pool,
            repository_id=repository.id,
            github_issue_id=endpoint.github_issue_id,
        )
        if projection is None:
            continue
        resolved[key] = ResolvedRelatedIssue(
            number=projection.issue_number,
            title=projection.title,
            state=projection.state,
            html_url=projection.html_url,
        )
    return resolved


def _compose_item(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    projection: GitHubIssueProjection,
) -> RepositoryIssueListItem:
    """Compose one projection's relations, current Task, and action eligibility."""
    parent = find_github_issue_parent_edge(
        pool, repository_id=repository_id, github_issue_id=projection.identity.github_issue_id
    )
    sub_issues = tuple(
        find_github_issue_sub_issue_edges(
            pool, repository_id=repository_id, github_issue_id=projection.identity.github_issue_id
        )
    )
    blocked_by = tuple(
        list_github_issue_blocked_by_edges(
            pool, repository_id=repository_id, github_issue_id=projection.identity.github_issue_id
        )
    )
    endpoints = ((parent,) if parent is not None else ()) + sub_issues + blocked_by
    resolved_related = _resolve_related(pool, workspace_id=workspace_id, endpoints=endpoints)
    current_task = find_current_task_for_issue(
        pool, repository_id=repository_id, github_issue_id=projection.identity.github_issue_id
    )
    create_task_action = durably_eligible_for_create_task_action(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        projection=projection,
    )
    return RepositoryIssueListItem(
        projection=projection,
        parent=parent,
        sub_issues=sub_issues,
        blocked_by=blocked_by,
        resolved_related=resolved_related,
        current_task=current_task,
        create_task_action=create_task_action,
    )


def list_repository_issues_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    state: str,
    cursor: str | None,
) -> tuple[list[RepositoryIssueListItem], str | None]:
    """Return one authorized, deterministically paged list of issues.

    The authenticated Owner authorization boundary resolves first (uniform
    anti-probing not-found); the bounded keyset page follows with its opaque
    next cursor when more items exist. Database-only and side-effect free.
    """
    if not isinstance(profile_id, UUID):
        raise InvalidCommandError("profile_id must be a UUID")
    if not isinstance(workspace_id, UUID):
        raise InvalidCommandError("workspace_id must be a UUID")
    if not isinstance(repository_id, UUID):
        raise InvalidCommandError("repository_id must be a UUID")
    if state not in ("open", "closed", "all"):
        raise InvalidCommandError("state must be one of open, closed, all")
    with application_span(_READS_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_LIST_SPAN_NAME)
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        page_state = GitHubIssuePageState(state)
        after_issue_number = None if cursor is None else decode_issue_page_cursor(cursor)
        projections = list_repository_github_issues_page(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            state=page_state,
            after_issue_number=after_issue_number,
            limit=ISSUE_PAGE_LIMIT + 1,
        )
        has_more = len(projections) > ISSUE_PAGE_LIMIT
        projections = projections[:ISSUE_PAGE_LIMIT]
        items = [
            _compose_item(
                pool,
                workspace_id=workspace_id,
                repository_id=repository_id,
                projection=projection,
            )
            for projection in projections
        ]
        next_cursor = (
            encode_issue_page_cursor(projections[-1].issue_number)
            if has_more and projections
            else None
        )
        return items, next_cursor


def get_repository_issue_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    issue_id: UUID,
) -> RepositoryIssueDetail:
    """Return one authorized issue's composed detail (full nullable body).

    The internal projection UUID is the only address; a foreign-Workspace or
    absent projection is the uniform not-found outcome, never a probing
    oracle. Database-only and side-effect free.
    """
    if not isinstance(profile_id, UUID):
        raise InvalidCommandError("profile_id must be a UUID")
    if not isinstance(workspace_id, UUID):
        raise InvalidCommandError("workspace_id must be a UUID")
    if not isinstance(repository_id, UUID):
        raise InvalidCommandError("repository_id must be a UUID")
    if not isinstance(issue_id, UUID):
        raise InvalidCommandError("issue_id must be a UUID")
    with application_span(_READS_TRACER_SCOPE, _DETAIL_SPAN_NAME) as span:
        annotate_span(span, operation=_DETAIL_SPAN_NAME)
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        projection = find_github_issue_by_id(
            pool, workspace_id=workspace_id, repository_id=repository_id, issue_id=issue_id
        )
        if projection is None:
            raise NotFoundError("the requested github issue is not available in this repository")
        return RepositoryIssueDetail(
            item=_compose_item(
                pool,
                workspace_id=workspace_id,
                repository_id=repository_id,
                projection=projection,
            )
        )
