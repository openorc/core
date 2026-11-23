"""Tests for the Workspace Connection administration transport (issue #371).

Route-level coverage of the HTTP mapping: transport thinness — the routes
delegate every decision to the patched application-service boundaries with
the exact command values (full-record replacement with explicit null resets
included) — the safe read model that structurally cannot carry credential
material (no ``auth_reference``, no credential value, only the presence
boolean), the uniform 401/404 isolation outcomes, the application-level
application-error mapping, and wire-shape rejections that never reach the
services. The established credential-failure matrices stay in the issue #55
service tests and the authenticated-Profile failure matrices in the
role-binding transport tests; a representative uniform 401 is re-proven
here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import connections as connections_router
from openorc.domain.connections import AdapterType, Connection
from openorc.domain.ownership import Profile
from openorc.services import connection_configuration, connection_credentials
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    InvalidCommandError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

_CREDENTIAL = "s3cr3t-credential-value"


def _connection(
    workspace_id: Any = None,
    *,
    name: str = "primary cline hub",
    repository_base: str | None = "/srv/openorc",
    control_endpoint: str | None = "ws://127.0.0.1:25463/hub",
    session_capacity: int = 2,
    enabled: bool = True,
    auth_reference: str | None = None,
) -> Connection:
    safe_config: dict[str, object] = {}
    if repository_base is not None:
        safe_config["repository_base"] = repository_base
    if control_endpoint is not None:
        safe_config["control_endpoint"] = control_endpoint
    return Connection(
        id=uuid.uuid4(),
        workspace_id=workspace_id or uuid.uuid4(),
        adapter=AdapterType.CLINE,
        name=name,
        safe_config=safe_config,
        session_capacity=session_capacity,
        enabled=enabled,
        auth_reference=auth_reference,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


class _ServiceSeam:
    """The patched service boundaries: recorded commands and scripted outcomes."""

    def __init__(self) -> None:
        self.lists: list[dict[str, Any]] = []
        self.creates: list[dict[str, Any]] = []
        self.reads: list[dict[str, Any]] = []
        self.replaces: list[dict[str, Any]] = []
        self.configures: list[dict[str, Any]] = []
        self.rotates: list[dict[str, Any]] = []
        self.pool_acquisitions = 0


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        list_result: Any = None,
        list_error: Exception | None = None,
        create_result: Any = None,
        create_error: Exception | None = None,
        read_result: Any = None,
        read_error: Exception | None = None,
        replace_result: Any = None,
        replace_error: Exception | None = None,
        credential_result: Any = None,
        credential_error: Exception | None = None,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _list(pool: Any, *, profile_id: Any, workspace_id: Any) -> Any:
            seam.lists.append({"profile_id": profile_id, "workspace_id": workspace_id})
            if list_error is not None:
                raise list_error
            return [] if list_result is None else list_result

        def _create(pool: Any, **command: Any) -> Any:
            seam.creates.append(command)
            if create_error is not None:
                raise create_error
            assert create_result is not None
            return create_result

        def _read(pool: Any, **command: Any) -> Any:
            seam.reads.append(command)
            if read_error is not None:
                raise read_error
            assert read_result is not None
            return read_result

        def _replace(pool: Any, **command: Any) -> Any:
            seam.replaces.append(command)
            if replace_error is not None:
                raise replace_error
            assert replace_result is not None
            return replace_result

        def _configure(pool: Any, **command: Any) -> Any:
            seam.configures.append(command)
            if credential_error is not None:
                raise credential_error
            assert credential_result is not None
            return credential_result

        def _rotate(pool: Any, **command: Any) -> Any:
            seam.rotates.append(command)
            if credential_error is not None:
                raise credential_error
            assert credential_result is not None
            return credential_result

        monkeypatch.setattr(connections_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(connection_configuration, "list_workspace_connections", _list)
        monkeypatch.setattr(connection_configuration, "create_workspace_connection", _create)
        monkeypatch.setattr(connection_configuration, "get_workspace_connection", _read)
        monkeypatch.setattr(connection_configuration, "replace_workspace_connection", _replace)
        monkeypatch.setattr(connection_credentials, "configure_connection_credential", _configure)
        monkeypatch.setattr(connection_credentials, "rotate_connection_credential", _rotate)
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


def test_list_delegates_and_maps_the_safe_read_models(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    configured = _connection(workspace_id, auth_reference="vault-ref-1")
    bare = _connection(workspace_id, repository_base=None, control_endpoint=None)
    seam = service_seam(list_result=[configured, bare])
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/connections")

    assert response.status_code == 200
    items = response.json()
    assert len(items) == 2
    assert items[0]["id"] == str(configured.id)
    assert items[0]["workspace_id"] == str(workspace_id)
    assert items[0]["adapter_type"] == "cline"
    assert items[0]["name"] == "primary cline hub"
    assert items[0]["repository_base"] == "/srv/openorc"
    assert items[0]["control_endpoint"] == "ws://127.0.0.1:25463/hub"
    assert items[0]["session_capacity"] == 2
    assert items[0]["enabled"] is True
    assert items[0]["credential_configured"] is True
    assert items[1]["repository_base"] is None
    assert items[1]["control_endpoint"] is None
    assert items[1]["credential_configured"] is False
    # The opaque reference never appears on the wire in any form.
    assert "auth_reference" not in items[0]
    assert "vault-ref-1" not in response.text
    assert seam.lists == [{"profile_id": owner_profile.id, "workspace_id": workspace_id}]


def test_create_returns_201_and_delegates_the_exact_command(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    created = _connection(workspace_id, session_capacity=3, enabled=False)
    seam = service_seam(create_result=created)
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections",
        json={
            "name": "primary cline hub",
            "adapter_type": "cline",
            "repository_base": "/srv/openorc",
            "control_endpoint": "ws://127.0.0.1:25463/hub",
            "session_capacity": 3,
            "enabled": False,
        },
    )

    assert response.status_code == 201
    assert response.json()["id"] == str(created.id)
    assert response.json()["credential_configured"] is False
    assert seam.creates == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "adapter_type": AdapterType.CLINE,
            "name": "primary cline hub",
            "repository_base": "/srv/openorc",
            "control_endpoint": "ws://127.0.0.1:25463/hub",
            "session_capacity": 3,
            "enabled": False,
        }
    ]


def test_create_delegates_the_declared_defaults(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    created = _connection(workspace_id)
    seam = service_seam(create_result=created)
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections",
        json={"name": "primary cline hub"},
    )

    assert response.status_code == 201
    # The unprovided optional fields delegate as their declared defaults:
    # the durable capacity default and enabled state are the service's.
    assert seam.creates == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "adapter_type": AdapterType.CLINE,
            "name": "primary cline hub",
            "repository_base": None,
            "control_endpoint": None,
            "session_capacity": None,
            "enabled": True,
        }
    ]


def test_read_maps_the_safe_read_model_and_never_discloses_secrets(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connection = _connection(workspace_id, auth_reference="vault-ref-1")
    seam = service_seam(read_result=connection)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/connections/{connection_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(connection.id)
    assert body["credential_configured"] is True
    assert body["control_endpoint"] == "ws://127.0.0.1:25463/hub"
    # The opaque reference and any credential material never appear on the
    # wire in any form; the endpoint is part of the approved safe config.
    assert "auth_reference" not in body
    assert "vault-ref-1" not in response.text
    assert seam.reads == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
        }
    ]


def test_replace_delegates_the_full_record_with_explicit_null_resets(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    replaced = _connection(workspace_id)
    seam = service_seam(replace_result=replaced)
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/v1/workspaces/{workspace_id}/connections/{connection_id}",
        json={
            "name": "renamed hub",
            "repository_base": None,
            "control_endpoint": None,
            "session_capacity": 1,
            "enabled": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["id"] == str(replaced.id)
    # Full-record replacement: every field delegates, and explicit null is
    # the reset value — omitted fields are wire errors, never silent clears.
    assert seam.replaces == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
            "name": "renamed hub",
            "repository_base": None,
            "control_endpoint": None,
            "session_capacity": 1,
            "enabled": True,
        }
    ]


def test_credential_setup_returns_201_and_never_echoes_the_value(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connection = _connection(workspace_id, auth_reference="vault-ref-1")
    seam = service_seam(credential_result=connection)
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections/{connection_id}/credentials",
        json={"credential": _CREDENTIAL},
    )

    assert response.status_code == 201
    assert response.json()["credential_configured"] is True
    # The #55 service delegates unchanged, and the credential value is never
    # echoed back by any response.
    assert seam.configures == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
            "credential": _CREDENTIAL,
        }
    ]
    assert _CREDENTIAL not in response.text
    assert "vault-ref-1" not in response.text


def test_credential_rotation_returns_200_and_delegates(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connection = _connection(workspace_id, auth_reference="vault-ref-1")
    seam = service_seam(credential_result=connection)
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/v1/workspaces/{workspace_id}/connections/{connection_id}/credentials",
        json={"credential": _CREDENTIAL},
    )

    assert response.status_code == 200
    assert response.json()["credential_configured"] is True
    assert seam.rotates == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
            "credential": _CREDENTIAL,
        }
    ]
    assert _CREDENTIAL not in response.text


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (NotFoundError("not found"), 404),
        (InvalidCommandError("invalid command"), 422),
        (ConflictError("conflict"), 409),
    ],
)
def test_connection_errors_map_to_their_http_statuses(
    service_seam, settings_factory, owner_profile, error: Exception, status: int
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(read_error=error)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/connections/{uuid.uuid4()}")

    assert response.status_code == status
    assert response.json()["detail"] == str(error)
    assert len(seam.reads) == 1


def test_credential_setup_conflict_maps_to_409(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    service_seam(
        credential_error=ConflictError(
            "the connection already has a configured credential; rotate it instead"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections/{uuid.uuid4()}/credentials",
        json={"credential": _CREDENTIAL},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "the connection already has a configured credential; rotate it instead"
    )
    # The conflict message is authored safe: no credential material.
    assert _CREDENTIAL not in response.text


def test_missing_or_malformed_credentials_are_a_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/connections")

    assert response.status_code == 401
    assert "connections" not in response.json()["detail"]


def test_malformed_bodies_never_reach_the_services(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)
    workspace_id = uuid.uuid4()

    missing_name = client.post(f"/api/v1/workspaces/{workspace_id}/connections", json={})
    boolean_capacity = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections",
        json={"name": "hub", "session_capacity": True},
    )
    unknown_adapter = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections",
        json={"name": "hub", "adapter_type": "gherkin"},
    )
    incomplete_replace = client.put(
        f"/api/v1/workspaces/{workspace_id}/connections/{uuid.uuid4()}",
        json={"name": "renamed hub", "session_capacity": 1, "enabled": True},
    )
    missing_credential = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections/{uuid.uuid4()}/credentials",
        json={},
    )

    for response in (
        missing_name,
        boolean_capacity,
        unknown_adapter,
        incomplete_replace,
        missing_credential,
    ):
        assert response.status_code == 422
    assert seam.lists == []
    assert seam.creates == []
    assert seam.reads == []
    assert seam.replaces == []
    assert seam.configures == []
    assert seam.rotates == []
