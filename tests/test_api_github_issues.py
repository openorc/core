"""Transport tests for the authorized GitHub issue read surface (issue #375).

Route-level coverage of the HTTP mapping: the bounded list envelope with its
opaque deterministic cursor, the default-open state filter, the list/detail
body difference (no issue body in list items; the full nullable body on the
detail), the ownership-separated structure with native GitHub names and the
composed relation totals, the backend-generated ``create_task`` action, and
the uniform 401/404 outcomes including cross-Workspace isolation. The reads
are database-only and side-effect free — the service seam proves delegation
and the transport proves the wire.
"""

from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import github_issues as github_issues_router
from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.domain.github_issues import (
    GitHubIssueIdentity,
    GitHubIssueLabel,
    GitHubIssueMilestone,
    GitHubIssueProjection,
    GitHubIssueState,
    GitHubIssueUserRef,
    github_issue_requirements_fingerprint,
)
from openorc.domain.ownership import Profile
from openorc.services import github_issue_reads
from openorc.services.errors import NotFoundError
from openorc.services.github_issue_reads import RepositoryIssueDetail, RepositoryIssueListItem

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Steps")


def _projection(workspace_id: Any, repository_id: Any, *, body: str | None = "Steps") -> Any:
    return GitHubIssueProjection(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        repository_id=repository_id,
        identity=GitHubIssueIdentity(github_issue_id=6550419907),
        issue_number=375,
        title="Example issue",
        body=body,
        state=GitHubIssueState.OPEN,
        requirements_fingerprint=_FINGERPRINT,
        provider_updated_at=_OBSERVED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        state_reason=None,
        html_url="https://github.com/example/repo/issues/375",
        provider_created_at=_OBSERVED,
        provider_closed_at=None,
        user=GitHubIssueUserRef(id=501, login="octocat"),
        labels=(GitHubIssueLabel(id=11, name="bug", color="d73a4a", description=None),),
        assignees=(),
        milestone=GitHubIssueMilestone(
            id=13,
            number=7,
            title="v1",
            state="open",
            html_url="https://github.com/example/repo/milestone/7",
            description=None,
            due_on=None,
        ),
        hierarchy_observed_at=_OBSERVED,
        dependencies_observed_at=_OBSERVED,
    )


def _item(
    workspace_id: Any, repository_id: Any, *, body: str | None = "Steps"
) -> RepositoryIssueListItem:
    return RepositoryIssueListItem(
        projection=_projection(workspace_id, repository_id, body=body),
        parent=None,
        sub_issues=(RelatedIssueEndpoint(github_repository_id=42, github_issue_id=100),),
        blocked_by=(),
        resolved_related={},
        current_task=None,
        create_task_action=True,
    )


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        list_items: list[RepositoryIssueListItem] | None = None,
        list_cursor: str | None = None,
        detail: RepositoryIssueDetail | None = None,
        list_error: Exception | None = None,
        detail_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"lists": [], "details": []}

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _list(pool: Any, **command: Any) -> Any:
            seam["lists"].append(command)
            if list_error is not None:
                raise list_error
            return (list_items or [], list_cursor)

        def _detail(pool: Any, **command: Any) -> Any:
            seam["details"].append(command)
            if detail_error is not None:
                raise detail_error
            assert detail is not None
            return detail

        monkeypatch.setattr(github_issues_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(github_issue_reads, "list_repository_issues_for_owner", _list)
        monkeypatch.setattr(github_issue_reads, "get_repository_issue_for_owner", _detail)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_the_list_envelope_is_bounded_state_filtered_and_cursor_paged(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    items = [_item(workspace_id, repository_id)]
    cursor = base64.urlsafe_b64encode(b"n:375").decode("ascii")
    seam = service_seam(list_items=items, list_cursor=cursor)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues")

    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] == cursor
    assert len(body["items"]) == 1
    assert seam["lists"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
            "state": "open",
            "cursor": None,
        }
    ]


def test_the_state_filter_and_cursor_delegate_verbatim(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)
    cursor = base64.urlsafe_b64encode(b"n:41").decode("ascii")

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues",
        params={"state": "all", "cursor": cursor},
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}
    assert seam["lists"][0]["state"] == "all"
    assert seam["lists"][0]["cursor"] == cursor


def test_list_items_carry_the_github_block_without_a_body_and_the_openorc_block(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    service_seam(list_items=[_item(workspace_id, repository_id)])
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues")

    item = response.json()["items"][0]
    github_block = item["github"]
    # Native names/types preserved verbatim; no full issue body in list items.
    assert github_block["id"] == 6550419907
    assert github_block["number"] == 375
    assert github_block["title"] == "Example issue"
    assert "body" not in github_block
    assert github_block["state"] == "open"
    assert github_block["html_url"] == "https://github.com/example/repo/issues/375"
    assert github_block["created_at"] is not None
    assert github_block["user"] == {"id": 501, "login": "octocat"}
    assert github_block["labels"] == [
        {"id": 11, "name": "bug", "color": "d73a4a", "description": None}
    ]
    assert github_block["milestone"]["number"] == 7
    assert github_block["sub_issues_total"] == 1
    assert github_block["sub_issues"] == [
        {
            "github_repository_id": 42,
            "github_issue_id": 100,
            "number": None,
            "title": None,
            "state": None,
            "html_url": None,
        }
    ]
    openorc_block = item["openorc"]
    assert openorc_block["workspace_id"] == str(workspace_id)
    assert openorc_block["repository_id"] == str(repository_id)
    assert openorc_block["current_task"] is None
    assert openorc_block["actions"] == ["create_task"]


def test_the_detail_carries_the_full_nullable_body_and_the_same_fact_set(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    item = _item(workspace_id, repository_id, body=None)
    detail = RepositoryIssueDetail(item=item)
    seam = service_seam(detail=detail)
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues/{item.projection.id}"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["github"]["body"] is None
    assert body["github"]["number"] == 375
    assert body["openorc"]["id"] == str(item.projection.id)
    assert seam["details"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
            "issue_id": item.projection.id,
        }
    ]


def test_a_malformed_state_filter_is_rejected_before_the_service(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues",
        params={"state": "everything"},
    )

    assert response.status_code == 422
    assert seam["lists"] == []


def test_a_foreign_workspace_issue_is_the_uniform_not_found(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    seam = service_seam(detail_error=NotFoundError("not available"))
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues/{uuid.uuid4()}"
    )

    assert response.status_code == 404
    assert seam["details"][0]["workspace_id"] == workspace_id
