"""Tests for the Workspace GitHub installation mapping transport (issue #372).

Route-level coverage of the HTTP mapping: transport thinness — the routes
delegate every decision to the patched application-service boundaries with
the exact command values — the trust-boundary read model that structurally
separates the unverified claim from the observed block (no observation fact
is accepted or echoed by any claim operation), the uniform 401/404 isolation
outcomes, the application-level application-error mapping, and wire-shape
rejections that never reach the services. The established claim-service
behavior matrices stay in the issue #372 service tests; a representative
uniform 401 is re-proven here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import github_installations as installations_router
from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationAccount,
    GitHubInstallationIdentity,
)
from openorc.domain.ownership import Profile
from openorc.services import github_installation_claims
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    InvalidCommandError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _installation(
    workspace_id: Any = None,
    *,
    claimed: bool = True,
) -> GitHubInstallation:
    return GitHubInstallation(
        id=uuid.uuid4(),
        workspace_id=workspace_id or uuid.uuid4(),
        identity=GitHubInstallationIdentity(github_installation_id=12345678),
        account=(
            None
            if claimed
            else GitHubInstallationAccount(
                github_account_id=501, login="octocat", type="Organization"
            )
        ),
        suspended_at=None if claimed else _OBSERVED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


class _ServiceSeam:
    def __init__(self) -> None:
        self.claims: list[dict[str, Any]] = []
        self.lists: list[dict[str, Any]] = []
        self.reads: list[dict[str, Any]] = []
        self.pool_acquisitions = 0


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        claim_result: Any = None,
        claim_error: Exception | None = None,
        list_result: Any = None,
        list_error: Exception | None = None,
        read_result: Any = None,
        read_error: Exception | None = None,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _claim(pool: Any, **command: Any) -> Any:
            seam.claims.append(command)
            if claim_error is not None:
                raise claim_error
            assert claim_result is not None
            return claim_result

        def _list(pool: Any, *, profile_id: Any, workspace_id: Any) -> Any:
            seam.lists.append({"profile_id": profile_id, "workspace_id": workspace_id})
            if list_error is not None:
                raise list_error
            return [] if list_result is None else list_result

        def _read(pool: Any, **command: Any) -> Any:
            seam.reads.append(command)
            if read_error is not None:
                raise read_error
            assert read_result is not None
            return read_result

        monkeypatch.setattr(installations_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(github_installation_claims, "record_installation_claim", _claim)
        monkeypatch.setattr(github_installation_claims, "list_workspace_installation_claims", _list)
        monkeypatch.setattr(github_installation_claims, "read_installation_claim", _read)
        return seam

    return _make


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


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
    claimed = _installation(workspace_id)
    seam = service_seam(claim_result=claimed)
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/github/installations",
        json={"github_installation_id": 12345678},
    )

    assert response.status_code == 201
    body = response.json()
    # The claim-state read model: the claimed stable identity with no
    # observed block, and no observation or credential field anywhere.
    assert body["github_installation_id"] == 12345678
    assert body["observed"] is None
    assert body["id"] == str(claimed.id)
    for forbidden in ("account_login", "account_type", "github_account_id", "suspended_at"):
        assert forbidden not in body
    assert seam.claims == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "github_installation_id": 12345678,
        }
    ]


def test_read_and_list_map_the_claim_and_observed_states(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    established = _installation(workspace_id, claimed=False)
    seam = service_seam(read_result=established, list_result=[established])
    client = _client(settings_factory(), owner_profile)
    installation_id = established.id

    read_response = client.get(
        f"/api/v1/workspaces/{workspace_id}/github/installations/{installation_id}"
    )
    list_response = client.get(f"/api/v1/workspaces/{workspace_id}/github/installations")

    assert read_response.status_code == 200
    observed = read_response.json()["observed"]
    assert observed["github_account_id"] == 501
    assert observed["account_login"] == "octocat"
    assert observed["account_type"] == "Organization"
    assert observed["suspended_at"] is not None
    assert list_response.status_code == 200
    assert list_response.json()[0]["observed"]["account_login"] == "octocat"
    assert seam.reads == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "installation_id": installation_id,
        }
    ]
    assert seam.lists == [{"profile_id": owner_profile.id, "workspace_id": workspace_id}]


def test_typed_application_errors_map_to_the_documented_statuses(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        claim_error=ConflictError(
            "the trusted installation facts report a different GitHub account"
        ),
        list_error=NotFoundError("the requested workspace is not available to this Profile"),
        read_error=InvalidCommandError("installation_id must be a UUID"),
    )
    client = _client(settings_factory(), owner_profile)

    claim_response = client.post(
        f"/api/v1/workspaces/{workspace_id}/github/installations",
        json={"github_installation_id": 12345678},
    )
    list_response = client.get(f"/api/v1/workspaces/{workspace_id}/github/installations")
    read_response = client.get(
        f"/api/v1/workspaces/{workspace_id}/github/installations/{uuid.uuid4()}"
    )

    assert claim_response.status_code == 409
    assert list_response.status_code == 404
    assert read_response.status_code == 422
    # The claim command reached the service boundary before the conflict was
    # classified there.
    assert seam.claims == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "github_installation_id": 12345678,
        }
    ]


def test_missing_or_malformed_credentials_are_a_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/github/installations")

    assert response.status_code == 401
    assert "installations" not in response.json()["detail"]


def test_malformed_claim_bodies_never_reach_the_services(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(claim_result=_installation(workspace_id))
    client = _client(settings_factory(), owner_profile)
    missing = client.post(f"/api/v1/workspaces/{workspace_id}/github/installations", json={})
    boolean_id = client.post(
        f"/api/v1/workspaces/{workspace_id}/github/installations",
        json={"github_installation_id": True},
    )
    string_id = client.post(
        f"/api/v1/workspaces/{workspace_id}/github/installations",
        json={"github_installation_id": "12345678"},
    )
    # An observation fact has no field to occupy: the claim model declares
    # none, so an observation-shaped extra key can never be bound.
    smuggled = client.post(
        f"/api/v1/workspaces/{workspace_id}/github/installations",
        json={"github_installation_id": 12345678, "suspended_at": "2026-11-23T00:00:00Z"},
    )

    for response in (missing, boolean_id, string_id):
        assert response.status_code == 422
    assert smuggled.status_code == 201
    assert seam.claims == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "github_installation_id": 12345678,
        }
    ]
    assert seam.lists == []
    assert seam.reads == []
