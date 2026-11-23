"""Tests for the safe administrative disconnect and deletion transport (issue #374).

Route-level coverage of the HTTP mapping: transport thinness — the routes
delegate every decision to the patched issue #97 application-service
boundaries with the exact authenticated Profile and path-target values —
the truthful deleted-aggregate response bodies (the deleted Connection's
final credential-free inventory state included), the guarded-refusal
conflicts (referenced Connections, non-archived Tasks, the account-deletion
mutation barrier), the uniform 404 isolation mapping, the honest 503 on
unknown external outcomes, a representative uniform 401, and wire-shape
rejections that never reach the services. The established deletion-safety
matrices stay in the issue #97 service tests
(``test_services_administrative_deletion.py``) and are not re-proven here.
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
from openorc.api.routers import connections as connections_router
from openorc.api.routers import projects as projects_router
from openorc.api.routers import repositories as repositories_router
from openorc.api.routers import tasks as tasks_router
from openorc.api.routers import workspaces as workspaces_router
from openorc.domain.connections import AdapterType, Connection
from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    Profile,
    Project,
    Repository,
    RepositoryMetadata,
    Workspace,
)
from openorc.domain.tasks import Task, TaskStatus
from openorc.services import administrative_deletion
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    ExternalOperationUncertainError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


class _ServiceSeam:
    """The patched issue #97 boundaries: recorded commands and scripted outcomes."""

    def __init__(self) -> None:
        self.disconnects: list[dict[str, Any]] = []
        self.connection_deletes: list[dict[str, Any]] = []
        self.task_purges: list[dict[str, Any]] = []
        self.repository_deletes: list[dict[str, Any]] = []
        self.project_deletes: list[dict[str, Any]] = []
        self.workspace_deletes: list[dict[str, Any]] = []
        self.pool_acquisitions = 0


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        disconnect_result: Any = None,
        disconnect_error: Exception | None = None,
        connection_delete_result: Any = None,
        connection_delete_error: Exception | None = None,
        task_purge_result: Any = None,
        task_purge_error: Exception | None = None,
        repository_delete_result: Any = None,
        repository_delete_error: Exception | None = None,
        project_delete_result: Any = None,
        project_delete_error: Exception | None = None,
        workspace_delete_result: Any = None,
        workspace_delete_error: Exception | None = None,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _disconnect(
            pool: Any, *, profile_id: Any, workspace_id: Any, connection_id: Any
        ) -> Any:
            seam.disconnects.append(
                {
                    "profile_id": profile_id,
                    "workspace_id": workspace_id,
                    "connection_id": connection_id,
                }
            )
            if disconnect_error is not None:
                raise disconnect_error
            assert disconnect_result is not None
            return disconnect_result

        def _delete_connection(
            pool: Any, *, profile_id: Any, workspace_id: Any, connection_id: Any
        ) -> Any:
            seam.connection_deletes.append(
                {
                    "profile_id": profile_id,
                    "workspace_id": workspace_id,
                    "connection_id": connection_id,
                }
            )
            if connection_delete_error is not None:
                raise connection_delete_error
            assert connection_delete_result is not None
            return connection_delete_result

        def _purge_task(pool: Any, *, profile_id: Any, workspace_id: Any, task_id: Any) -> Any:
            seam.task_purges.append(
                {
                    "profile_id": profile_id,
                    "workspace_id": workspace_id,
                    "task_id": task_id,
                }
            )
            if task_purge_error is not None:
                raise task_purge_error
            assert task_purge_result is not None
            return task_purge_result

        def _delete_repository(
            pool: Any, *, profile_id: Any, workspace_id: Any, repository_id: Any
        ) -> Any:
            seam.repository_deletes.append(
                {
                    "profile_id": profile_id,
                    "workspace_id": workspace_id,
                    "repository_id": repository_id,
                }
            )
            if repository_delete_error is not None:
                raise repository_delete_error
            assert repository_delete_result is not None
            return repository_delete_result

        def _delete_project(
            pool: Any, *, profile_id: Any, workspace_id: Any, project_id: Any
        ) -> Any:
            seam.project_deletes.append(
                {
                    "profile_id": profile_id,
                    "workspace_id": workspace_id,
                    "project_id": project_id,
                }
            )
            if project_delete_error is not None:
                raise project_delete_error
            assert project_delete_result is not None
            return project_delete_result

        def _delete_workspace(pool: Any, *, profile_id: Any, workspace_id: Any) -> Any:
            seam.workspace_deletes.append({"profile_id": profile_id, "workspace_id": workspace_id})
            if workspace_delete_error is not None:
                raise workspace_delete_error
            assert workspace_delete_result is not None
            return workspace_delete_result

        for router_module in (
            connections_router,
            tasks_router,
            repositories_router,
            projects_router,
            workspaces_router,
        ):
            monkeypatch.setattr(router_module, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(
            administrative_deletion, "disconnect_connection_credential", _disconnect
        )
        monkeypatch.setattr(administrative_deletion, "delete_connection_record", _delete_connection)
        monkeypatch.setattr(administrative_deletion, "purge_archived_task_record", _purge_task)
        monkeypatch.setattr(administrative_deletion, "delete_repository_record", _delete_repository)
        monkeypatch.setattr(administrative_deletion, "delete_project_record", _delete_project)
        monkeypatch.setattr(administrative_deletion, "delete_workspace_record", _delete_workspace)
        return seam

    return _make


def _connection(
    workspace_id: Any, *, enabled: bool = True, auth_reference: str | None = None
) -> Connection:
    return Connection(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        adapter=AdapterType.CLINE,
        name="primary cline hub",
        safe_config={
            "repository_base": "/srv/openorc",
            "control_endpoint": "ws://127.0.0.1:25463/hub",
        },
        session_capacity=2,
        enabled=enabled,
        auth_reference=auth_reference,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _task(workspace_id: Any) -> Task:
    return Task(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        repository_id=uuid.uuid4(),
        github_issue_id=4242,
        github_issue_number=7,
        status=TaskStatus.CANCELLED,
        archived_at=_OBSERVED,
        queued_at=None,
        canonical_feature_branch="openorc/issue-7",
        state_token=uuid.uuid4(),
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _project(workspace_id: Any) -> Project:
    return Project(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="platform",
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _repository(workspace_id: Any, project_id: Any, *, observed: bool) -> Repository:
    return Repository(
        id=uuid.uuid4(),
        project_id=project_id,
        workspace_id=workspace_id,
        identity=GitHubRepositoryIdentity(github_repository_id=501),
        metadata=(
            None
            if not observed
            else RepositoryMetadata(
                owner_login="octo-org",
                name="widgets",
                html_url="https://github.com/octo-org/widgets",
                is_private=False,
                default_branch="main",
            )
        ),
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _workspace(owner_profile: Profile) -> Workspace:
    return Workspace(
        id=uuid.uuid4(),
        owner_profile_id=owner_profile.id,
        name="primary workspace",
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        review_iteration_limit=3,
        guidance="",
        default_task_budget_usd=Decimal("25.00"),
    )


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_disconnect_credential_delegates_and_reports_the_final_inventory_state(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    disconnected = _connection(workspace_id, enabled=False)
    seam = service_seam(disconnect_result=disconnected)
    client = _client(settings_factory(), owner_profile)

    response = client.delete(
        f"/api/v1/workspaces/{workspace_id}/connections/{connection_id}/credentials"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(disconnected.id)
    assert body["enabled"] is False
    assert body["credential_configured"] is False
    # The opaque reference never appears on the wire in any form.
    assert "auth_reference" not in body
    assert seam.disconnects == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
        }
    ]


def test_delete_connection_delegates_and_reports_the_deleted_record(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    deleted = _connection(workspace_id)
    seam = service_seam(connection_delete_result=deleted)
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/connections/{connection_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(deleted.id)
    assert body["credential_configured"] is False
    assert "auth_reference" not in body
    assert seam.connection_deletes == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "connection_id": connection_id,
        }
    ]


def test_purge_archived_task_delegates_and_identifies_the_purged_attempt(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    task = _task(workspace_id)
    seam = service_seam(task_purge_result=task)
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/tasks/{task.id}")

    assert response.status_code == 200
    assert response.json() == {
        "workspace_id": str(workspace_id),
        "task_id": str(task.id),
    }
    assert seam.task_purges == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "task_id": task.id,
        }
    ]


def test_delete_repository_reports_the_deleted_mapping_without_github_effects(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    deleted = _repository(workspace_id, project_id, observed=True)
    seam = service_seam(repository_delete_result=deleted)
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(deleted.id)
    assert body["github_repository_id"] == 501
    assert body["observed"]["owner_login"] == "octo-org"
    # The mapping record is gone: no installation route reference survives it.
    assert body["installation_route"] is None
    assert seam.repository_deletes == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "repository_id": repository_id,
        }
    ]


def test_delete_project_reports_the_deleted_project(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    project = _project(workspace_id)
    seam = service_seam(project_delete_result=project)
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/projects/{project.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(project.id)
    assert body["workspace_id"] == str(workspace_id)
    assert body["name"] == "platform"
    assert seam.project_deletes == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "project_id": project.id,
        }
    ]


def test_delete_workspace_reports_the_deleted_workspace(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace = _workspace(owner_profile)
    seam = service_seam(workspace_delete_result=workspace)
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(workspace.id)
    assert body["owner_profile_id"] == str(owner_profile.id)
    assert body["name"] == "primary workspace"
    # The exact-decimal transport: canonical decimal text, never a float.
    assert body["default_task_budget_usd"] == "25.00"
    assert seam.workspace_deletes == [
        {"profile_id": owner_profile.id, "workspace_id": workspace.id}
    ]


def test_guarded_connection_deletion_is_a_conflict_never_a_success(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        connection_delete_error=ConflictError(
            "the connection is still referenced by historical records"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/connections/{uuid.uuid4()}")

    assert response.status_code == 409
    assert response.json()["detail"] == ("the connection is still referenced by historical records")
    # The guarded refusal reached the service and was reported truthfully —
    # never laundered into a success outcome.
    assert len(seam.connection_deletes) == 1


def test_purging_a_current_task_is_a_conflict_cancellation_is_the_route(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        task_purge_error=ConflictError(
            "the task is not archived; cancel the task before purging it"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/tasks/{uuid.uuid4()}")

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "the task is not archived; cancel the task before purging it"
    )
    assert len(seam.task_purges) == 1


def test_the_account_deletion_barrier_refusal_is_a_conflict(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        workspace_delete_error=ConflictError(
            "the operation conflicts with an unresolved account deletion attempt"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}")

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "the operation conflicts with an unresolved account deletion attempt"
    )
    assert len(seam.workspace_deletes) == 1


def test_missing_or_foreign_subjects_are_a_uniform_404(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        connection_delete_error=NotFoundError(
            "the requested connection is not available in this workspace"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}/connections/{uuid.uuid4()}")

    assert response.status_code == 404
    # The uniform isolation vocabulary is the service's; the transport maps
    # the typed outcome verbatim so probing stays indistinguishable from
    # addressing something absent.
    assert response.json()["detail"] == (
        "the requested connection is not available in this workspace"
    )
    assert len(seam.connection_deletes) == 1


def test_unknown_external_outcome_is_an_honest_503(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam(
        workspace_delete_error=ExternalOperationUncertainError(
            "the outcome of the external operation is unknown"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete(f"/api/v1/workspaces/{workspace_id}")

    assert response.status_code == 503
    assert response.json()["detail"] == "the outcome of the external operation is unknown"
    assert len(seam.workspace_deletes) == 1


def test_representative_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.delete(f"/api/v1/workspaces/{uuid.uuid4()}")

    assert response.status_code == 401
    assert "workspaces" not in response.json()["detail"]


def test_malformed_path_uuids_never_reach_the_services(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    workspace_response = client.delete("/api/v1/workspaces/not-a-uuid")
    connection_response = client.delete("/api/v1/workspaces/not-a-uuid/connections/also-not")

    assert workspace_response.status_code == 422
    assert connection_response.status_code == 422
    assert seam.disconnects == []
    assert seam.connection_deletes == []
    assert seam.task_purges == []
    assert seam.repository_deletes == []
    assert seam.project_deletes == []
    assert seam.workspace_deletes == []
    assert seam.pool_acquisitions == 0
