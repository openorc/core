"""Thin authorized GitHub issue read transport (issue #375).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction, path
and query parsing, delegation to the Owner-authorized read services, and the
composed read model's wire mapping. It owns no authorization policy, no
relation-resolution rule, and no action-eligibility semantics — those belong
to the service layer. The reads are database-only and side-effect free.
Blocking database work runs off the event loop (sync route handlers; the v1
persistence surface is synchronous). The addressed errors are the typed
application vocabulary, mapped to HTTP by the application-level handler.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_INVALID_COMMAND_422,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.github_issues import (
    GitHubIssueBlockedByRefRead,
    GitHubIssueDetailBlockRead,
    GitHubIssueDetailRead,
    GitHubIssueLabelRead,
    GitHubIssueListItemBlockRead,
    GitHubIssueListItemRead,
    GitHubIssueListRead,
    GitHubIssueMilestoneRead,
    GitHubIssueParentRefRead,
    GitHubIssueSubIssueRefRead,
    GitHubIssueUserRefRead,
    OpenOrcCurrentTaskRead,
    OpenOrcIssueBlockRead,
)
from openorc.config import Settings
from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.domain.github_issues import GitHubIssueLabel, GitHubIssueMilestone, GitHubIssueUserRef
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import github_issue_reads
from openorc.services.github_issue_reads import (
    RELATION_PRESENTATION_BOUND,
    RepositoryIssueDetail,
    RepositoryIssueListItem,
)

router = APIRouter(tags=["github-issues"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_ISSUES_PATH = "/workspaces/{workspace_id}/repositories/{repository_id}/issues"
_ISSUE_PATH = "/workspaces/{workspace_id}/repositories/{repository_id}/issues/{issue_id}"

_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}

# The wire type per composed relation family: the schema keeps the families
# distinct on the wire while the shared reference shape stays one model.
_SUB_ISSUE_WIRE = GitHubIssueSubIssueRefRead
_BLOCKED_BY_WIRE = GitHubIssueBlockedByRefRead


def _user_to_wire(user: GitHubIssueUserRef | None) -> GitHubIssueUserRefRead | None:
    return None if user is None else GitHubIssueUserRefRead(id=user.id, login=user.login)


def _collection_user_to_wire(user: GitHubIssueUserRef) -> GitHubIssueUserRefRead:
    """Map one collection entry (never null inside an observed collection)."""
    return GitHubIssueUserRefRead(id=user.id, login=user.login)


def _label_to_wire(label: GitHubIssueLabel) -> GitHubIssueLabelRead:
    return GitHubIssueLabelRead(
        id=label.id, name=label.name, color=label.color, description=label.description
    )


def _milestone_to_wire(milestone: GitHubIssueMilestone | None) -> GitHubIssueMilestoneRead | None:
    if milestone is None:
        return None
    return GitHubIssueMilestoneRead(
        id=milestone.id,
        number=milestone.number,
        title=milestone.title,
        description=milestone.description,
        state=milestone.state,
        html_url=milestone.html_url,
        due_on=milestone.due_on,
    )


def _relation_to_wire[RelationWire: GitHubIssueParentRefRead](
    endpoint: RelatedIssueEndpoint,
    resolved: dict[tuple[int, int], github_issue_reads.ResolvedRelatedIssue],
    wire_type: type[RelationWire],
) -> RelationWire:
    """Map one relation endpoint with its same-Workspace resolution facts."""
    facts = resolved.get((endpoint.github_repository_id, endpoint.github_issue_id))
    return wire_type(
        github_repository_id=endpoint.github_repository_id,
        github_issue_id=endpoint.github_issue_id,
        number=None if facts is None else facts.number,
        title=None if facts is None else facts.title,
        state=None if facts is None else facts.state.value,
        html_url=None if facts is None else facts.html_url,
    )


def _presented(
    entries: tuple[RelatedIssueEndpoint, ...],
) -> tuple[tuple[RelatedIssueEndpoint, ...], int]:
    """Return the transparently bounded presentation and the durable total."""
    return entries[:RELATION_PRESENTATION_BOUND], len(entries)


def _openorc_to_wire(
    item: RepositoryIssueListItem, *, workspace_id: UUID, repository_id: UUID
) -> OpenOrcIssueBlockRead:
    projection = item.projection
    return OpenOrcIssueBlockRead(
        id=projection.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        current_task=(
            None
            if item.current_task is None
            else OpenOrcCurrentTaskRead(
                id=item.current_task.id, status=item.current_task.status.value
            )
        ),
        actions=(["create_task"] if item.create_task_action else []),
    )


def _relations_to_wire(
    item: RepositoryIssueListItem,
) -> tuple[
    GitHubIssueParentRefRead | None,
    list[GitHubIssueSubIssueRefRead] | None,
    int | None,
    list[GitHubIssueBlockedByRefRead] | None,
    int | None,
]:
    """Map one item's composed parent/sub-issue/blocked-by presentation.

    Each relationship family projects independently through its durable
    observation evidence (issue #375): the hierarchy family (parent and
    sub-issues) through ``hierarchy_observed_at`` and the dependencies
    family (blocked-by) through ``dependencies_observed_at``. An unobserved
    family presents null collections and null totals — never a fabricated
    empty observation — while an observed family presents its bounded
    collection with the durable total.
    """
    projection = item.projection
    hierarchy_observed = projection.hierarchy_observed_at is not None
    dependencies_observed = projection.dependencies_observed_at is not None
    sub_presented, sub_total = _presented(item.sub_issues)
    blocked_presented, blocked_total = _presented(item.blocked_by)
    parent = (
        None
        if not hierarchy_observed or item.parent is None
        else _relation_to_wire(item.parent, item.resolved_related, GitHubIssueParentRefRead)
    )
    sub_issues = (
        None
        if not hierarchy_observed
        else [
            _relation_to_wire(endpoint, item.resolved_related, _SUB_ISSUE_WIRE)
            for endpoint in sub_presented
        ]
    )
    blocked_by = (
        None
        if not dependencies_observed
        else [
            _relation_to_wire(endpoint, item.resolved_related, _BLOCKED_BY_WIRE)
            for endpoint in blocked_presented
        ]
    )
    return (
        parent,
        sub_issues,
        None if not hierarchy_observed else sub_total,
        blocked_by,
        None if not dependencies_observed else blocked_total,
    )


def _github_list_block_to_wire(item: RepositoryIssueListItem) -> GitHubIssueListItemBlockRead:
    projection = item.projection
    parent, sub_issues, sub_total, blocked_by, blocked_total = _relations_to_wire(item)
    return GitHubIssueListItemBlockRead(
        id=projection.identity.github_issue_id,
        number=projection.issue_number,
        title=projection.title,
        state=projection.state.value,
        state_reason=projection.state_reason,
        html_url=projection.html_url,
        created_at=projection.provider_created_at,
        updated_at=projection.provider_updated_at,
        closed_at=projection.provider_closed_at,
        user=_user_to_wire(projection.user),
        labels=(
            None
            if projection.labels is None
            else [_label_to_wire(label) for label in projection.labels]
        ),
        assignees=(
            None
            if projection.assignees is None
            else [_collection_user_to_wire(assignee) for assignee in projection.assignees]
        ),
        milestone=_milestone_to_wire(projection.milestone),
        parent=parent,
        sub_issues=sub_issues,
        sub_issues_total=sub_total,
        blocked_by=blocked_by,
        blocked_by_total=blocked_total,
    )


@router.get(
    _ISSUES_PATH,
    response_model=GitHubIssueListRead,
    operation_id="list_repository_github_issues",
    responses=_READ_RESPONSES,
)
def list_repository_issues(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    repository_id: UUID,
    state: str = Query(default="open", pattern="^(open|closed|all)$"),
    cursor: str | None = Query(default=None),
) -> GitHubIssueListRead:
    """List one Workspace Repository's authorized GitHub issue projections.

    Bounded, deterministically ordered (issue number ascending), and advanced
    by the opaque ``next_cursor`` when more items exist. The ``state`` filter
    defaults to ``open``. List items carry the ownership-separated projection
    without issue bodies.
    """
    settings: Settings = request.app.state.settings
    items, next_cursor = github_issue_reads.list_repository_issues_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        state=state,
        cursor=cursor,
    )
    return GitHubIssueListRead(
        items=[
            GitHubIssueListItemRead(
                github=_github_list_block_to_wire(item),
                openorc=_openorc_to_wire(
                    item, workspace_id=workspace_id, repository_id=repository_id
                ),
            )
            for item in items
        ],
        next_cursor=next_cursor,
    )


@router.get(
    _ISSUE_PATH,
    response_model=GitHubIssueDetailRead,
    operation_id="get_repository_github_issue",
    responses=_READ_RESPONSES,
)
def get_repository_issue(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    repository_id: UUID,
    issue_id: UUID,
) -> GitHubIssueDetailRead:
    """Return one authorized GitHub issue projection's full detail.

    Addressed by the internal projection UUID and scoped to the addressed
    Workspace Repository; the detail carries the full nullable GitHub body
    plus the same ownership-separated fact set as the list items.
    """
    settings: Settings = request.app.state.settings
    detail: RepositoryIssueDetail = github_issue_reads.get_repository_issue_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        issue_id=issue_id,
    )
    item = detail.item
    projection = item.projection
    parent, sub_issues, sub_total, blocked_by, blocked_total = _relations_to_wire(item)
    github_block = GitHubIssueDetailBlockRead(
        id=projection.identity.github_issue_id,
        number=projection.issue_number,
        title=projection.title,
        body=projection.body,
        state=projection.state.value,
        state_reason=projection.state_reason,
        html_url=projection.html_url,
        created_at=projection.provider_created_at,
        updated_at=projection.provider_updated_at,
        closed_at=projection.provider_closed_at,
        user=_user_to_wire(projection.user),
        labels=(
            None
            if projection.labels is None
            else [_label_to_wire(label) for label in projection.labels]
        ),
        assignees=(
            None
            if projection.assignees is None
            else [_collection_user_to_wire(assignee) for assignee in projection.assignees]
        ),
        milestone=_milestone_to_wire(projection.milestone),
        parent=parent,
        sub_issues=sub_issues,
        sub_issues_total=sub_total,
        blocked_by=blocked_by,
        blocked_by_total=blocked_total,
    )
    return GitHubIssueDetailRead(
        github=github_block,
        openorc=_openorc_to_wire(item, workspace_id=workspace_id, repository_id=repository_id),
    )
