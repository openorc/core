"""Tests for the Workspace discovery, creation and configuration transport (issue #370).

Route-level coverage of the HTTP mapping: transport thinness — the routes
delegate every decision to the patched application-service boundaries with
the exact command values (explicit null budget reset included) — the read
models with exact-decimal settings, the uniform 404 isolation outcome for
foreign Workspaces, and the application-level application-error mapping. The
established authenticated-Profile failure matrices stay in the role-binding
transport tests; a representative uniform 401 is re-proven here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import workspaces as workspaces_router
from openorc.domain.ownership import Profile, Workspace
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.workspace_configuration import (
    DefaultTaskBudgetUpdate,
    ReviewIterationLimitUpdate,
    WorkspaceGuidanceUpdate,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _workspace(
    owner_profile_id: Any = None,
    *,
    review_iteration_limit: int = 5,
    guidance: str = "",
    default_task_budget_usd: Decimal | None = None,
) -> Workspace:
    return Workspace(
        id=uuid.uuid4(),
        owner_profile_id=owner_profile_id or uuid.uuid4(),
        name="platform",
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        review_iteration_limit=review_iteration_limit,
        guidance=guidance,
        default_task_budget_usd=default_task_budget_usd,
    )


class _ServiceSeam:
    """The patched service boundaries: recorded commands and scripted outcomes."""

    def __init__(self) -> None:
        self.creates: list[dict[str, Any]] = []
        self.lists: list[dict[str, Any]] = []
        self.detail_reads: list[dict[str, Any]] = []
        self.setting_updates: list[dict[str, Any]] = []
        self.pool_acquisitions = 0


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        create_result: Any = None,
        create_error: Exception | None = None,
        list_result: Any = None,
        detail_result: Any = None,
        detail_error: Exception | None = None,
        setting_result: Any = None,
        setting_error: Exception | None = None,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _create(pool: Any, *, profile_id: Any, name: Any) -> Any:
            seam.creates.append({"profile_id": profile_id, "name": name})
            if create_error is not None:
                raise create_error
            assert create_result is not None
            return create_result

        def _list(pool: Any, *, profile_id: Any) -> Any:
            seam.lists.append({"profile_id": profile_id})
            return [] if list_result is None else list_result

        def _detail(pool: Any, *, profile_id: Any, workspace_id: Any) -> Any:
            seam.detail_reads.append({"profile_id": profile_id, "workspace_id": workspace_id})
            if detail_error is not None:
                raise detail_error
            assert detail_result is not None
            return detail_result

        def _setting(pool: Any, **command: Any) -> Any:
            seam.setting_updates.append(command)
            if setting_error is not None:
                raise setting_error
            assert setting_result is not None
            return setting_result

        monkeypatch.setattr(workspaces_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(workspaces_router, "create_workspace_for_profile", _create)
        monkeypatch.setattr(workspaces_router, "list_owned_workspaces", _list)
        monkeypatch.setattr(
            workspaces_router.workspace_configuration,
            "get_workspace_configuration",
            _detail,
        )
        monkeypatch.setattr(
            workspaces_router.workspace_configuration,
            "set_review_iteration_limit",
            _setting,
        )
        monkeypatch.setattr(workspaces_router.workspace_configuration, "set_guidance", _setting)
        monkeypatch.setattr(
            workspaces_router.workspace_configuration, "set_default_task_budget", _setting
        )
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


def test_post_creates_the_owned_workspace(service_seam, settings_factory, owner_profile) -> None:
    workspace = _workspace(owner_profile.id)
    seam = service_seam(create_result=workspace)
    client = _client(settings_factory(), owner_profile)

    response = client.post("/api/v1/profile/workspaces", json={"name": "platform"})

    assert response.status_code == 201
    body = response.json()
    assert body["id"] == str(workspace.id)
    assert body["owner_profile_id"] == str(owner_profile.id)
    assert body["name"] == "platform"
    assert body["created_at"]
    assert body["updated_at"]
    assert body["review_iteration_limit"] == 5
    assert body["guidance"] == ""
    assert body["default_task_budget_usd"] is None
    # The authenticated Profile — never a client-supplied owner — is the only
    # ownership input, and the name is the only other command value.
    assert seam.creates == [{"profile_id": owner_profile.id, "name": "platform"}]
    assert seam.pool_acquisitions == 1


def test_get_lists_the_owned_workspaces(service_seam, settings_factory, owner_profile) -> None:
    owned = [
        _workspace(owner_profile.id, default_task_budget_usd=Decimal("12.5")),
        _workspace(owner_profile.id, review_iteration_limit=9, guidance="prose"),
    ]
    seam = service_seam(list_result=owned)
    client = _client(settings_factory(), owner_profile)

    response = client.get("/api/v1/profile/workspaces")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    # The budget travels as exact canonical decimal text — never a binary float.
    assert body[0]["default_task_budget_usd"] == "12.5"
    assert body[1]["review_iteration_limit"] == 9
    assert body[1]["guidance"] == "prose"
    assert seam.lists == [{"profile_id": owner_profile.id}]
    assert seam.pool_acquisitions == 1


def test_get_detail_maps_the_workspace_with_settings(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace = _workspace(
        owner_profile.id, guidance="prose", default_task_budget_usd=Decimal("24.5")
    )
    seam = service_seam(detail_result=workspace)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(workspace.id)
    assert body["owner_profile_id"] == str(owner_profile.id)
    assert body["guidance"] == "prose"
    assert body["default_task_budget_usd"] == "24.5"
    assert seam.detail_reads == [{"profile_id": owner_profile.id, "workspace_id": workspace.id}]


def test_a_foreign_workspace_is_a_uniform_404(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam(
        detail_error=NotFoundError("the requested workspace is not available to this Profile")
    )
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}")

    # Foreign-Workspace, missing, and non-owned subjects are uniformly this
    # outcome: probing exact internal UUIDs is indistinguishable from absence.
    assert response.status_code == 404
    assert len(seam.detail_reads) == 1


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (NotFoundError("not found"), 404),
        (InvalidCommandError("invalid command"), 422),
        (ConflictError("conflict"), 409),
        (ExternalOperationUncertainError("uncertain"), 503),
    ],
)
def test_creation_errors_map_to_their_http_statuses(
    service_seam, settings_factory, owner_profile, error: Exception, status: int
) -> None:
    seam = service_seam(create_error=error)
    client = _client(settings_factory(), owner_profile)

    response = client.post("/api/v1/profile/workspaces", json={"name": "platform"})

    assert response.status_code == status
    # The typed message is the safe, authored application detail.
    assert response.json()["detail"] == str(error)
    assert len(seam.creates) == 1


def test_put_review_iteration_limit_delegates_the_exact_command(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        setting_result=ReviewIterationLimitUpdate(
            workspace_id=workspace_id,
            changed=True,
            previous_review_iteration_limit=5,
            new_review_iteration_limit=7,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/v1/workspaces/{workspace_id}/configuration/review-iteration-limit",
        json={"review_iteration_limit": 7},
    )

    assert response.status_code == 200
    assert response.json() == {
        "workspace_id": str(workspace_id),
        "changed": True,
        "previous_review_iteration_limit": 5,
        "new_review_iteration_limit": 7,
    }
    assert seam.setting_updates == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "review_iteration_limit": 7,
        }
    ]


def test_put_guidance_delegates_blank_prose_verbatim(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        setting_result=WorkspaceGuidanceUpdate(workspace_id=workspace_id, changed=True)
    )
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/v1/workspaces/{workspace_id}/configuration/guidance",
        json={"guidance": ""},
    )

    assert response.status_code == 200
    # The change result identifies the setting change only; Owner-authored
    # prose is never echoed back through it.
    assert response.json() == {"workspace_id": str(workspace_id), "changed": True}
    assert seam.setting_updates == [
        {"profile_id": owner_profile.id, "workspace_id": workspace_id, "guidance": ""}
    ]


def test_put_default_task_budget_null_is_the_explicit_disable(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        setting_result=DefaultTaskBudgetUpdate(
            workspace_id=workspace_id,
            changed=True,
            previous_default_task_budget_usd=Decimal("24.5"),
            new_default_task_budget_usd=None,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/v1/workspaces/{workspace_id}/configuration/default-task-budget",
        json={"default_task_budget_usd": None},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["changed"] is True
    assert body["previous_default_task_budget_usd"] == "24.5"
    assert body["new_default_task_budget_usd"] is None
    # Explicit null is the only reset value: the submitted None is delegated
    # verbatim as the disable-spending-policy command.
    assert seam.setting_updates == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "default_task_budget_usd": None,
        }
    ]


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (NotFoundError("not found"), 404),
        (InvalidCommandError("invalid command"), 422),
        (ConflictError("conflict"), 409),
    ],
)
def test_setting_update_errors_map_to_their_http_statuses(
    service_seam, settings_factory, owner_profile, error: Exception, status: int
) -> None:
    seam = service_seam(setting_error=error)
    client = _client(settings_factory(), owner_profile)

    response = client.put(
        f"/api/v1/workspaces/{uuid.uuid4()}/configuration/review-iteration-limit",
        json={"review_iteration_limit": 7},
    )

    assert response.status_code == status
    assert response.json()["detail"] == str(error)
    assert len(seam.setting_updates) == 1


def test_malformed_bodies_never_reach_the_services(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    missing_name = client.post("/api/v1/profile/workspaces", json={})
    missing_limit = client.put(
        f"/api/v1/workspaces/{uuid.uuid4()}/configuration/review-iteration-limit",
        json={},
    )
    boolean_limit = client.put(
        f"/api/v1/workspaces/{uuid.uuid4()}/configuration/review-iteration-limit",
        json={"review_iteration_limit": True},
    )

    for response in (missing_name, missing_limit, boolean_limit):
        assert response.status_code == 422
    assert seam.creates == []
    assert seam.setting_updates == []


def test_missing_or_malformed_credentials_are_a_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.post("/api/v1/profile/workspaces", json={"name": "platform"})

    assert response.status_code == 401
    # The typed failure message is authored safe: no command content is
    # reflected back.
    assert "platform" not in response.json()["detail"]
