"""Tests for the Workspace Project transport (issue #372).

Route-level coverage of the HTTP mapping: transport thinness (exact command
delegation to the patched service boundaries), the safe read model, the
uniform 401 isolation outcome, the typed-error mapping, and wire-shape
rejections that never reach the services. The onboarding service behavior
matrices stay in the issue #372 service tests.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import projects as projects_router
from openorc.domain.ownership import Profile, Project
from openorc.services import repository_onboarding
from openorc.services.errors import AuthenticationError, InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _project(workspace_id: Any) -> Project:
    return Project(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="delivery",
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        create_result: Any = None,
        create_error: Exception | None = None,
        list_result: Any = None,
        list_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"creates": [], "lists": []}

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _create(pool: Any, **command: Any) -> Any:
            seam["creates"].append(command)
            if create_error is not None:
                raise create_error
            assert create_result is not None
            return create_result

        def _list(pool: Any, *, profile_id: Any, workspace_id: Any) -> Any:
            seam["lists"].append({"profile_id": profile_id, "workspace_id": workspace_id})
            if list_error is not None:
                raise list_error
            return [] if list_result is None else list_result

        monkeypatch.setattr(projects_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(repository_onboarding, "create_workspace_project", _create)
        monkeypatch.setattr(repository_onboarding, "list_workspace_projects", _list)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_list_and_create_delegate_the_exact_commands(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    project = _project(workspace_id)
    seam = service_seam(create_result=project, list_result=[project])
    client = _client(settings_factory(), owner_profile)

    create_response = client.post(
        f"/api/v1/workspaces/{workspace_id}/projects", json={"name": "delivery"}
    )
    list_response = client.get(f"/api/v1/workspaces/{workspace_id}/projects")

    assert create_response.status_code == 201
    assert create_response.json() == {
        "id": str(project.id),
        "workspace_id": str(workspace_id),
        "name": "delivery",
        "created_at": create_response.json()["created_at"],
        "updated_at": create_response.json()["updated_at"],
    }
    assert list_response.status_code == 200
    assert [item["id"] for item in list_response.json()] == [str(project.id)]
    assert seam["creates"] == [
        {"profile_id": owner_profile.id, "workspace_id": workspace_id, "name": "delivery"}
    ]
    assert seam["lists"] == [{"profile_id": owner_profile.id, "workspace_id": workspace_id}]


def test_typed_errors_map_and_malformed_bodies_never_reach_the_services(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        create_error=InvalidCommandError("Project name must be a non-blank string"),
        list_error=NotFoundError("the requested workspace is not available to this Profile"),
    )
    client = _client(settings_factory(), owner_profile)

    invalid_name = client.post(f"/api/v1/workspaces/{workspace_id}/projects", json={"name": "   "})
    missing_name = client.post(f"/api/v1/workspaces/{workspace_id}/projects", json={})
    foreign_workspace = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/projects")

    assert invalid_name.status_code == 422
    assert missing_name.status_code == 422
    assert foreign_workspace.status_code == 404
    # The blank-name rule is the service's: the wire-valid blank command
    # reaches it and is classified there.
    assert seam["creates"] == [
        {"profile_id": owner_profile.id, "workspace_id": workspace_id, "name": "   "}
    ]


def test_missing_credentials_are_a_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/projects")

    assert response.status_code == 401
    assert "projects" not in response.json()["detail"]
