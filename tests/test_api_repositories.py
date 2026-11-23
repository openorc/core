"""Tests for the Workspace Repository mapping transport (issue #372).

Route-level coverage of the HTTP mapping: transport thinness — the claim
command delegates the stable identity ALONE, the read models structurally
separate the unverified claim from the observed block and the route
reference, and the route bind/unbind delegates to the issue #57 services and
projects the composed status through the shared read service — plus the
uniform 401/404 outcomes, the typed-error mapping, and wire-shape rejections
that never reach the services.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import repositories as repositories_router
from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationAccount,
    GitHubInstallationIdentity,
)
from openorc.domain.ownership import GitHubRepositoryIdentity as RepoIdentity
from openorc.domain.ownership import Profile, Repository, RepositoryMetadata
from openorc.services import github_installations, repository_onboarding
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.repository_onboarding import WorkspaceRepositoryStatus

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _repository(workspace_id: Any, *, route: Any = None, claimed: bool = True) -> Repository:
    return Repository(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        workspace_id=workspace_id,
        identity=RepoIdentity(github_repository_id=987654321),
        metadata=(
            None
            if claimed
            else RepositoryMetadata(
                owner_login="octocat",
                name="hello-world",
                html_url="https://github.com/octocat/hello-world",
                is_private=False,
                default_branch="main",
            )
        ),
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        github_installation_id=route,
    )


def _installation(workspace_id: Any) -> GitHubInstallation:
    return GitHubInstallation(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        identity=GitHubInstallationIdentity(github_installation_id=12345678),
        account=GitHubInstallationAccount(
            github_account_id=501, login="octocat", type="Organization"
        ),
        suspended_at=None,
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
        claim_result: Any = None,
        claim_error: Exception | None = None,
        list_result: Any = None,
        read_result: Any = None,
        read_error: Exception | None = None,
        bind_error: Exception | None = None,
        unbind_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {
            "claims": [],
            "lists": [],
            "reads": [],
            "binds": [],
            "unbinds": [],
        }

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _claim(pool: Any, **command: Any) -> Any:
            seam["claims"].append(command)
            if claim_error is not None:
                raise claim_error
            assert claim_result is not None
            return claim_result

        def _list(pool: Any, *, profile_id: Any, workspace_id: Any) -> Any:
            seam["lists"].append({"profile_id": profile_id, "workspace_id": workspace_id})
            return [] if list_result is None else list_result

        def _read(pool: Any, **command: Any) -> Any:
            seam["reads"].append(command)
            if read_error is not None:
                raise read_error
            assert read_result is not None
            return read_result

        def _bind(pool: Any, **command: Any) -> Any:
            seam["binds"].append(command)
            if bind_error is not None:
                raise bind_error
            assert read_result is not None
            return read_result.repository

        def _unbind(pool: Any, **command: Any) -> Any:
            seam["unbinds"].append(command)
            if unbind_error is not None:
                raise unbind_error
            assert read_result is not None
            return read_result.repository

        monkeypatch.setattr(repositories_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(repository_onboarding, "claim_workspace_repository", _claim)
        monkeypatch.setattr(repository_onboarding, "list_workspace_repositories", _list)
        monkeypatch.setattr(repository_onboarding, "read_workspace_repository", _read)
        monkeypatch.setattr(github_installations, "bind_repository_installation", _bind)
        monkeypatch.setattr(github_installations, "unbind_repository_installation", _unbind)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_claim_returns_201_and_delegates_the_identity_alone(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    repository = _repository(workspace_id)
    seam = service_seam(claim_result=repository)
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/repositories",
        json={"project_id": str(project_id), "github_repository_id": 987654321},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["github_repository_id"] == 987654321
    # The claim-state read model: no observed block, no route, and no
    # observation or credential field anywhere on the wire.
    assert body["observed"] is None
    assert body["installation_route"] is None
    for forbidden in ("owner_login", "html_url", "is_private", "default_branch"):
        assert forbidden not in body
    assert seam["claims"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "project_id": project_id,
            "github_repository_id": 987654321,
        }
    ]


def test_list_and_read_map_the_claim_observed_and_route_states(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    installation = _installation(workspace_id)
    claimed = _repository(workspace_id)
    routed = _repository(workspace_id, route=installation.id, claimed=False)
    seam = service_seam(
        list_result=[
            WorkspaceRepositoryStatus(repository=claimed, installation=None),
            WorkspaceRepositoryStatus(repository=routed, installation=installation),
        ],
        read_result=WorkspaceRepositoryStatus(repository=routed, installation=installation),
    )
    client = _client(settings_factory(), owner_profile)

    list_response = client.get(f"/api/v1/workspaces/{workspace_id}/repositories")
    read_response = client.get(f"/api/v1/workspaces/{workspace_id}/repositories/{routed.id}")

    assert list_response.status_code == 200
    items = list_response.json()
    assert items[0]["observed"] is None and items[0]["installation_route"] is None
    assert items[1]["observed"]["owner_login"] == "octocat"
    assert items[1]["installation_route"]["installation_id"] == str(installation.id)
    assert items[1]["installation_route"]["github_installation_id"] == 12345678
    assert read_response.status_code == 200
    assert read_response.json()["installation_route"] is not None
    assert seam["lists"] == [{"profile_id": owner_profile.id, "workspace_id": workspace_id}]
    assert seam["reads"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": routed.id,
        }
    ]


def test_bind_and_unbind_delegate_to_the_route_services_and_project_the_status(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    installation = _installation(workspace_id)
    routed = _repository(workspace_id, route=installation.id)
    unrouted = _repository(workspace_id)
    seam = service_seam(
        read_result=WorkspaceRepositoryStatus(repository=routed, installation=installation),
    )
    client = _client(settings_factory(), owner_profile)
    repository_id = unrouted.id

    bind_response = client.put(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/installation-route",
        json={"installation_id": str(installation.id)},
    )
    unbind_response = client.delete(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/installation-route"
    )

    assert bind_response.status_code == 200
    assert bind_response.json()["installation_route"]["installation_id"] == str(installation.id)
    assert unbind_response.status_code == 200
    assert seam["binds"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
            "github_installation_id": installation.id,
        }
    ]
    assert seam["unbinds"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
        }
    ]


def test_typed_errors_map_to_the_documented_statuses(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        claim_error=ConflictError(
            "the GitHub repository identity is already claimed in this workspace"
        ),
        read_error=NotFoundError("the requested repository is not available in this workspace"),
        bind_error=InvalidCommandError("github_installation_id must be a UUID"),
        unbind_error=NotFoundError("the requested installation is not available in this workspace"),
    )
    client = _client(settings_factory(), owner_profile)
    repository_id = uuid.uuid4()

    claim_response = client.post(
        f"/api/v1/workspaces/{workspace_id}/repositories",
        json={"project_id": str(uuid.uuid4()), "github_repository_id": 987654321},
    )
    read_response = client.get(f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}")
    bind_response = client.put(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/installation-route",
        json={"installation_id": "not-a-uuid"},
    )
    unbind_response = client.delete(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/installation-route"
    )

    assert claim_response.status_code == 409
    assert read_response.status_code == 404
    # The malformed UUID is a wire-validation error (422) that never reaches
    # the typed-error handler; the unbind 404 proves the service mapping.
    assert bind_response.status_code == 422
    assert unbind_response.status_code == 404
    # The malformed route UUID never reached the bind service.
    assert seam["binds"] == []


def test_missing_credentials_are_a_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/repositories")

    assert response.status_code == 401
    assert "repositories" not in response.json()["detail"]


def test_malformed_claim_bodies_never_reach_the_services(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    seam = service_seam(claim_result=_repository(workspace_id))
    client = _client(settings_factory(), owner_profile)

    missing = client.post(f"/api/v1/workspaces/{workspace_id}/repositories", json={})
    boolean_id = client.post(
        f"/api/v1/workspaces/{workspace_id}/repositories",
        json={"project_id": str(uuid.uuid4()), "github_repository_id": True},
    )
    # An observation fact has no field to occupy: the claim model declares
    # none, so an observation-shaped extra key can never be bound.
    smuggled = client.post(
        f"/api/v1/workspaces/{workspace_id}/repositories",
        json={
            "project_id": str(project_id),
            "github_repository_id": 987654321,
            "owner_login": "someone-else",
        },
    )

    for response in (missing, boolean_id):
        assert response.status_code == 422
    assert smuggled.status_code == 201
    assert seam["claims"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "project_id": project_id,
            "github_repository_id": 987654321,
        }
    ]
