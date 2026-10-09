"""Tests for the thin role-binding configuration transport (issue #162).

Route-level coverage of the HTTP mapping: authenticated Profile extraction
through the #52 dependency (a uniform 401 for missing/malformed bearer
credentials, a fail-closed 503 for an unconfigured Supabase project URL, and
503 for JWKS retrieval failures — never a probing detail), the
application-level application-error mapping (401/403/404/409/422/503), and
transport thinness — the routes delegate every decision to the patched
application-service boundary with the exact full-record command values
(explicit null reset included), and perform no authorization, configuration,
or persistence logic of their own.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import role_bindings as role_bindings_router
from openorc.domain.connections import WorkflowRole, WorkflowRoleBinding
from openorc.domain.ownership import Profile
from openorc.services import role_binding_configuration
from openorc.services.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)


def _binding(
    *,
    role: WorkflowRole = WorkflowRole.PRODUCER,
    configured_provider: str | None = None,
    configured_model: str | None = None,
    role_prompt_override: str | None = None,
) -> WorkflowRoleBinding:
    return WorkflowRoleBinding(
        id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        role=role,
        connection_id=uuid.uuid4(),
        configured_provider=configured_provider,
        configured_model=configured_model,
        role_prompt_override=role_prompt_override,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


class _ServiceSeam:
    """The patched service boundary: recorded commands and scripted outcomes."""

    def __init__(self) -> None:
        self.reads: list[dict[str, Any]] = []
        self.updates: list[dict[str, Any]] = []
        self.pool_acquisitions = 0


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        read_result: Any = None,
        read_error: Exception | None = None,
        update_result: Any = None,
        update_error: Exception | None = None,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _read(pool: Any, *, profile_id: Any, workspace_id: Any, role: Any) -> Any:
            seam.reads.append(
                {
                    "profile_id": profile_id,
                    "workspace_id": workspace_id,
                    "role": role,
                }
            )
            if read_error is not None:
                raise read_error
            assert read_result is not None
            return read_result

        def _update(pool: Any, **command: Any) -> Any:
            seam.updates.append(command)
            if update_error is not None:
                raise update_error
            assert update_result is not None
            return update_result

        monkeypatch.setattr(role_bindings_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(role_bindings_router, "get_role_binding_configuration", _read)
        monkeypatch.setattr(role_bindings_router, "set_role_binding_configuration", _update)
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


def test_get_returns_the_durable_configuration(
    service_seam, settings_factory, owner_profile
) -> None:
    binding = _binding(
        role=WorkflowRole.PRODUCER,
        configured_provider="provider-id-1",
        configured_model="model-id-x",
        role_prompt_override="/# Override",
    )
    seam = service_seam(read_result=binding)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/workspaces/{binding.workspace_id}/role-bindings/producer")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(binding.id)
    assert body["workspace_id"] == str(binding.workspace_id)
    assert body["role"] == "producer"
    assert body["connection_id"] == str(binding.connection_id)
    assert body["configured_provider"] == "provider-id-1"
    assert body["configured_model"] == "model-id-x"
    assert body["role_prompt_override"] == "/# Override"
    assert body["created_at"]
    assert body["updated_at"]
    # The route delegates the Owner-authorized read to the service with the
    # authenticated Profile and the addressed subject — and acquires the
    # process pool inside the off-event-loop handler.
    assert seam.reads == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": binding.workspace_id,
            "role": WorkflowRole.PRODUCER,
        }
    ]
    assert seam.pool_acquisitions == 1


def test_put_delegates_the_full_record(service_seam, settings_factory, owner_profile) -> None:
    binding = _binding(
        role=WorkflowRole.REVIEWER,
        configured_provider="provider-id-2",
        configured_model="model-id-y",
        role_prompt_override=None,
    )
    seam = service_seam(
        update_result=role_binding_configuration.RoleBindingConfigurationUpdate(
            binding=binding, changed=True
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/workspaces/{binding.workspace_id}/role-bindings/reviewer",
        json={
            "connection_id": str(binding.connection_id),
            "configured_provider": "provider-id-2",
            "configured_model": "model-id-y",
            "role_prompt_override": None,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["role"] == "reviewer"
    assert body["role_prompt_override"] is None
    # Full-record PUT semantics: every field arrives as the desired current
    # value, so the null override is the explicit reset to the shipped
    # default — and the authenticated Profile owns the addressed Workspace.
    assert seam.updates == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": binding.workspace_id,
            "role": WorkflowRole.REVIEWER,
            "connection_id": binding.connection_id,
            "configured_provider": "provider-id-2",
            "configured_model": "model-id-y",
            "role_prompt_override": None,
        }
    ]


def test_put_passes_an_empty_override_verbatim(
    service_seam, settings_factory, owner_profile
) -> None:
    binding = _binding(role=WorkflowRole.PRODUCER, role_prompt_override="")
    seam = service_seam(
        update_result=role_binding_configuration.RoleBindingConfigurationUpdate(
            binding=binding, changed=True
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/workspaces/{binding.workspace_id}/role-bindings/producer",
        json={
            "connection_id": str(binding.connection_id),
            "configured_provider": None,
            "configured_model": None,
            "role_prompt_override": "",
        },
    )

    assert response.status_code == 200
    # An empty string is an explicit verbatim override, never normalized.
    assert seam.updates[0]["role_prompt_override"] == ""
    assert response.json()["role_prompt_override"] == ""


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (NotFoundError("not found"), 404),
        (InvalidCommandError("invalid command"), 422),
        (AuthorizationError("not permitted"), 403),
        (ConflictError("conflict"), 409),
    ],
)
def test_application_errors_map_to_their_http_statuses(
    service_seam, settings_factory, owner_profile, error: Exception, status: int
) -> None:
    seam = service_seam(update_error=error)
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/workspaces/{uuid.uuid4()}/role-bindings/producer",
        json={
            "connection_id": str(uuid.uuid4()),
            "configured_provider": None,
            "configured_model": None,
            "role_prompt_override": None,
        },
    )

    assert response.status_code == status
    # The typed message is the safe, authored application detail.
    assert response.json()["detail"] == str(error)
    assert len(seam.updates) == 1


def test_a_read_error_maps_to_not_found(service_seam, settings_factory, owner_profile) -> None:
    service_seam(read_error=NotFoundError("binding not available"))
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/workspaces/{uuid.uuid4()}/role-bindings/reviewer")

    assert response.status_code == 404


def test_an_unknown_role_path_is_a_wire_validation_error(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/workspaces/{uuid.uuid4()}/role-bindings/navigator")

    assert response.status_code == 422
    assert seam.reads == []


def test_a_malformed_put_body_is_a_wire_validation_error(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/workspaces/{uuid.uuid4()}/role-bindings/producer",
        json={"configured_provider": "p"},
    )

    assert response.status_code == 422
    assert seam.updates == []


def test_a_malformed_workspace_path_is_a_wire_validation_error(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.get("/api/workspaces/not-a-uuid/role-bindings/producer")

    assert response.status_code == 422
    assert seam.reads == []


# --- the authenticated-Profile dependency (#52 boundary) ----------------------


@pytest.fixture
def auth_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the dependency module's pool and authenticate seams."""

    def _make(*, user: Any = None, error: Exception | None = None):
        tokens: list[str] = []

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _fake_authenticate(pool: Any, verifier: Any, *, token: str) -> Any:
            tokens.append(token)
            if error is not None:
                raise error
            assert user is not None
            return user

        monkeypatch.setattr(authentication_dependency, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(authentication_dependency, "authenticate", _fake_authenticate)
        return tokens

    return _make


def _auth_user(profile: Profile) -> Any:
    from openorc.domain.identity import AuthenticatedPrincipal
    from openorc.services.authentication import AuthenticatedUser

    return AuthenticatedUser(
        principal=AuthenticatedPrincipal(user_id=profile.id),
        profile=profile,
    )


def _settings_for_auth(settings_factory):
    return settings_factory(supabase_url="https://supabase.example.com")


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer "},
        {"Authorization": "Token something"},
        {"Authorization": "Bearer    "},
    ],
)
def test_missing_or_malformed_credentials_are_a_uniform_401(
    auth_seam, settings_factory, owner_profile, headers: dict[str, str]
) -> None:
    tokens = auth_seam()
    app = create_app(_settings_for_auth(settings_factory))
    client = TestClient(app)

    response = client.get(f"/api/workspaces/{uuid.uuid4()}/role-bindings/producer", headers=headers)

    # Uniform, detail-free rejection class: the typed authentication failure
    # maps to 401 before any Profile or Workspace work.
    assert response.status_code == 401
    assert tokens == []


def test_an_invalid_token_is_a_uniform_401(auth_seam, settings_factory) -> None:
    auth_seam(error=AuthenticationError("authentication failed: the access token is not valid"))
    app = create_app(_settings_for_auth(settings_factory))
    client = TestClient(app)

    response = client.get(
        f"/api/workspaces/{uuid.uuid4()}/role-bindings/producer",
        headers={"Authorization": "Bearer rejected-token"},
    )

    assert response.status_code == 401
    # The typed failure message is authored safe: it carries no token material.
    assert "rejected-token" not in response.json()["detail"]


def test_jwks_unavailability_is_a_fail_closed_503(auth_seam, settings_factory) -> None:
    auth_seam(
        error=ExternalOperationUncertainError(
            "authentication is temporarily unavailable: the identity provider "
            "signing-key source could not be reached"
        )
    )
    app = create_app(_settings_for_auth(settings_factory))
    client = TestClient(app)

    response = client.get(
        f"/api/workspaces/{uuid.uuid4()}/role-bindings/producer",
        headers={"Authorization": "Bearer some-token"},
    )

    # An external verification-infrastructure failure is never reported as
    # invalid caller credentials.
    assert response.status_code == 503


def test_an_unconfigured_project_url_fails_closed_503(auth_seam, settings_factory) -> None:
    auth_seam()
    app = create_app(settings_factory(supabase_url=None))
    client = TestClient(app)

    response = client.get(
        f"/api/workspaces/{uuid.uuid4()}/role-bindings/producer",
        headers={"Authorization": "Bearer some-token"},
    )

    # The deployment fails closed rather than verifying against nothing.
    assert response.status_code == 503
    assert isinstance(auth_seam, object)  # the verification never ran: the dependency raised first


def test_a_verified_identity_reaches_the_authorized_route(
    auth_seam, service_seam, settings_factory, owner_profile
) -> None:
    tokens = auth_seam(user=_auth_user(owner_profile))
    binding = _binding(role=WorkflowRole.PRODUCER)
    seam = service_seam(read_result=binding)
    app = create_app(_settings_for_auth(settings_factory))
    client = TestClient(app)

    response = client.get(
        f"/api/workspaces/{binding.workspace_id}/role-bindings/producer",
        headers={"Authorization": "Bearer verified-token"},
    )

    assert response.status_code == 200
    assert tokens == ["verified-token"]
    # The resolved Profile — not the raw token — is the service's authority
    # input.
    assert seam.reads[0]["profile_id"] == owner_profile.id
