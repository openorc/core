"""Transport tests for the authorized WorkflowEvent audit read surface (issue #377).

Route-level coverage of the HTTP mapping: the bounded list envelope with its
opaque deterministic cursor on both scopes, the canonical filter parsing and
delegation (event_type/actor_type enums, the from/to instants through the
aliased ``from`` query name, the bounded limit, the cursor), the exact public
record wire shape (the audit fact's own fields plus the service-projected
safe context — no wrapper, no prose, no presentation metadata), and the
documented error mapping. The reads are database-only and side-effect free —
the service seam proves delegation and the transport proves the wire.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import event_reads as event_reads_router
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.ownership import Profile
from openorc.services import event_reads
from openorc.services.errors import (
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.event_reads import PublicWorkflowEvent

_OBSERVED = datetime(2026, 11, 24, 8, 30, 0, tzinfo=UTC)


def _profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


def _event(**overrides: Any) -> PublicWorkflowEvent:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "event_type": "task_started",
        "actor_type": "owner",
        "actor_id": str(uuid.uuid4()),
        "subject_type": None,
        "subject_id": None,
        "context": {"outcome": "approved"},
        "created_at": _OBSERVED,
    }
    values.update(overrides)
    return PublicWorkflowEvent(**values)


@pytest.fixture()
def owner_profile() -> Profile:
    return _profile()


@pytest.fixture()
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        items: list[PublicWorkflowEvent] | None = None,
        next_cursor: str | None = None,
        list_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"lists": []}

        def _fake_pool(settings: Any) -> object:
            return object()

        def _workspace_list(pool: Any, **command: Any) -> Any:
            seam["lists"].append({"scope": "workspace", **command})
            if list_error is not None:
                raise list_error
            return (items or [], next_cursor)

        def _task_list(pool: Any, **command: Any) -> Any:
            seam["lists"].append({"scope": "task", **command})
            if list_error is not None:
                raise list_error
            return (items or [], next_cursor)

        monkeypatch.setattr(event_reads_router, "get_database_pool", _fake_pool)
        monkeypatch.setattr(event_reads, "list_workspace_events_for_owner", _workspace_list)
        monkeypatch.setattr(event_reads, "list_task_events_for_owner", _task_list)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_the_workspace_listing_delegates_parsed_filters_and_maps_the_envelope(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    events = [
        _event(),
        _event(
            event_type="owner_gate_resolved",
            subject_type="owner_gate",
            subject_id=uuid.uuid4(),
            context={},
        ),
    ]
    seam = service_seam(items=events, next_cursor="opaque-1")
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/events",
        params={
            "event_type": "owner_gate_resolved",
            "actor_type": "owner",
            "from": "2026-11-01T00:00:00Z",
            "to": "2026-12-01T00:00:00Z",
            "limit": 7,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items", "next_cursor"}
    assert body["next_cursor"] == "opaque-1"
    assert len(body["items"]) == 2
    first = body["items"][0]
    assert set(first) == {
        "id",
        "workspace_id",
        "task_id",
        "event_type",
        "actor_type",
        "actor_id",
        "subject_type",
        "subject_id",
        "context",
        "created_at",
    }
    assert first["event_type"] == "task_started"
    assert first["actor_type"] == "owner"
    assert first["context"] == {"outcome": "approved"}
    assert first["created_at"] == "2026-11-24T08:30:00Z"
    assert body["items"][1]["subject_type"] == "owner_gate"
    # The command reached the service seam with the parsed canonical values,
    # the aliased ``from`` bound, and the bounded limit.
    assert seam["lists"][0]["scope"] == "workspace"
    assert seam["lists"][0]["workspace_id"] == workspace_id
    assert seam["lists"][0]["event_type"] is WorkflowEventType.OWNER_GATE_RESOLVED
    assert seam["lists"][0]["actor_type"] is WorkflowEventActor.OWNER
    assert seam["lists"][0]["from_created_at"] == datetime(2026, 11, 1, tzinfo=UTC)
    assert seam["lists"][0]["to_created_at"] == datetime(2026, 12, 1, tzinfo=UTC)
    assert seam["lists"][0]["limit"] == 7
    assert seam["lists"][0]["cursor"] is None


def test_the_task_listing_delegates_the_exact_task_scope(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    seam = service_seam(items=[_event(context={})])
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/tasks/{task_id}/events",
        params={"event_type": "task_started", "cursor": "opaque-2"},
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["event_type"] == "task_started"
    assert seam["lists"][0]["scope"] == "task"
    assert seam["lists"][0]["workspace_id"] == workspace_id
    assert seam["lists"][0]["task_id"] == task_id
    assert seam["lists"][0]["event_type"] is WorkflowEventType.TASK_STARTED
    assert seam["lists"][0]["cursor"] == "opaque-2"


def test_an_unknown_filter_value_is_the_documented_422_before_any_read(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{uuid.uuid4()}/events",
        params={"event_type": "not-a-type"},
    )

    assert response.status_code == 422
    assert seam["lists"] == []


@pytest.mark.parametrize(
    ("list_error", "status"),
    [
        (NotFoundError("the requested workspace is not available"), 404),
        (InvalidCommandError("the cursor is not a valid event page cursor"), 422),
        (ExternalOperationUncertainError("the database is unavailable"), 503),
    ],
)
def test_service_failures_map_to_their_documented_statuses(
    service_seam, settings_factory, owner_profile, list_error: Exception, status: int
) -> None:
    seam = service_seam(list_error=list_error)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/tasks/{uuid.uuid4()}/events")

    assert response.status_code == status
    assert seam["lists"]


def test_an_empty_page_maps_to_an_empty_envelope_with_a_null_cursor(
    service_seam, settings_factory, owner_profile
) -> None:
    seam = service_seam(items=[], next_cursor=None)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/events")

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}
    assert seam["lists"][0]["scope"] == "workspace"
