"""Transport tests for the scoped on-demand GitHub refresh surface (#375).

Route-level coverage: the 202 admission shape (authorization and established
submission acceptance only), the delegation of both scopes, the typed
conflict/refusal/uncertainty mappings (409/503), and the truthful status
projection with its null-until-established timestamps/summary and the bounded
sanitized per-unit error classifications.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import workspace_refresh as workspace_refresh_router
from openorc.domain.github_refreshes import (
    GitHubRefreshRecord,
    GitHubRefreshStatus,
    GitHubRefreshSummary,
    GitHubRefreshUnitError,
)
from openorc.domain.ownership import Profile
from openorc.services import github_refresh
from openorc.services.errors import ConflictError, ExternalOperationUncertainError, NotFoundError

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _record(*, repository_id: uuid.UUID | None = None) -> GitHubRefreshRecord:
    return GitHubRefreshRecord(
        id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        repository_id=repository_id,
        status=GitHubRefreshStatus.ACCEPTED,
        requested_at=_OBSERVED,
        started_at=None,
        lease_expires_at=_OBSERVED,
        completed_at=None,
        summary=None,
        errors=(),
    )


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        record: GitHubRefreshRecord | None = None,
        admit_error: Exception | None = None,
        status_projection: Any = None,
        status_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {
            "workspace_admissions": [],
            "repository_admissions": [],
            "reads": [],
        }

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _fake_submitter_factory(submitter: Any) -> object:
            return object()

        def _workspace(pool: Any, submission: Any, **command: Any) -> Any:
            seam["workspace_admissions"].append(command)
            if admit_error is not None:
                raise admit_error
            assert record is not None
            return record

        def _repository(pool: Any, submission: Any, **command: Any) -> Any:
            seam["repository_admissions"].append(command)
            if admit_error is not None:
                raise admit_error
            assert record is not None
            return record

        def _status(pool: Any, **command: Any) -> Any:
            seam["reads"].append(command)
            if status_error is not None:
                raise status_error
            assert status_projection is not None
            return status_projection

        monkeypatch.setattr(workspace_refresh_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(
            workspace_refresh_router, "F1RefreshExecutionSubmission", _fake_submitter_factory
        )
        monkeypatch.setattr(
            github_refresh, "request_workspace_github_refresh_for_owner", _workspace
        )
        monkeypatch.setattr(
            github_refresh, "request_repository_github_refresh_for_owner", _repository
        )
        monkeypatch.setattr(github_refresh, "get_workspace_github_refresh_for_owner", _status)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    # The submission bridge is resolved from app state; the service seam owns
    # the outcome, so any inert stand-in proves the delegation.
    app.state.operation_submitter = object()
    return TestClient(app)


class _StatusProjection:
    """The service's status projection stand-in (class bodies cannot close)."""

    def __init__(self, *, workspace_id: uuid.UUID, refresh_id: uuid.UUID) -> None:
        self.id = refresh_id
        self.workspace_id = workspace_id
        self.repository_id = None
        self.status = GitHubRefreshStatus.PARTIALLY_COMPLETED
        self.started_at = _OBSERVED
        self.completed_at = _OBSERVED
        self.summary = GitHubRefreshSummary(repositories_checked=1, issues_discovered=2)
        self.errors = (
            GitHubRefreshUnitError(
                family="issue",
                code="failed_known",
                workspace_id=workspace_id,
                github_issue_id=9,
            ),
        )


def test_the_workspace_refresh_returns_202_with_the_accepted_record(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    record = _record()
    seam = service_seam(record=record)
    client = _client(settings_factory(), owner_profile)

    response = client.post(f"/api/v1/workspaces/{workspace_id}/refresh")

    assert response.status_code == 202
    body = response.json()
    assert body["refresh_id"] == str(record.id)
    assert body["workspace_id"] == str(record.workspace_id)
    assert body["repository_id"] is None
    assert body["status"] == "accepted"
    assert seam["workspace_admissions"] == [
        {"profile_id": owner_profile.id, "workspace_id": workspace_id}
    ]


def test_the_repository_refresh_is_scoped_to_the_addressed_repository(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    record = _record(repository_id=repository_id)
    seam = service_seam(record=record)
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/refresh"
    )

    assert response.status_code == 202
    assert response.json()["repository_id"] == str(repository_id)
    assert seam["repository_admissions"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
        }
    ]


def test_an_active_overlapping_scope_maps_to_the_documented_conflict(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    _seam = service_seam(admit_error=ConflictError("an active refresh covers this scope"))
    client = _client(settings_factory(), owner_profile)

    response = client.post(f"/api/v1/workspaces/{workspace_id}/refresh")

    assert response.status_code == 409
    assert response.json()["detail"]


def test_an_uncertain_submission_maps_to_the_documented_unavailable(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    _seam = service_seam(admit_error=ExternalOperationUncertainError("outcome unknown"))
    client = _client(settings_factory(), owner_profile)

    response = client.post(f"/api/v1/workspaces/{workspace_id}/refresh")

    assert response.status_code == 503
    assert response.json()["detail"]


def test_the_status_resource_presents_the_stored_truth(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    refresh_id = uuid.uuid4()

    projection = _StatusProjection(workspace_id=workspace_id, refresh_id=refresh_id)

    seam = service_seam(status_projection=projection)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/refresh/{refresh_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["refresh_id"] == str(refresh_id)
    assert body["status"] == "partially_completed"
    assert body["started_at"] is not None
    assert body["summary"]["issues_discovered"] == 2
    assert body["errors"] == [
        {
            "family": "issue",
            "code": "failed_known",
            "workspace_id": str(workspace_id),
            "repository_id": None,
            "github_issue_id": 9,
            "delivery_guid": None,
        }
    ]
    assert seam["reads"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "refresh_id": refresh_id,
        }
    ]


def test_a_foreign_refresh_is_the_uniform_not_found(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    _seam = service_seam(status_error=NotFoundError("not available"))
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/refresh/{uuid.uuid4()}")

    assert response.status_code == 404
