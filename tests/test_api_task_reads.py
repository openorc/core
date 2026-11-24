"""Transport tests for the authorized Task read surface (issue #376).

Route-level coverage of the HTTP mapping: the bounded list envelope with its
opaque deterministic cursor, the archival defaults (current for the
Workspace/Repository lists; all attempts for the issue list), the canonical
status filter delegation, the ownership-separated detail structure with the
backend-generated action tokens and the full current typed records, and the
history surface's documented collection selection with per-collection
continuation. The reads are database-only and side-effect free — the service
seam proves delegation and the transport proves the wire.
"""

from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import task_reads as task_reads_router
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.github_issues import (
    GitHubIssueIdentity,
    GitHubIssueProjection,
    GitHubIssueState,
)
from openorc.domain.ownership import Profile
from openorc.domain.planning import PlanRevision
from openorc.domain.tasks import TaskStatus
from openorc.persistence.tasks import _task_from_row
from openorc.services import task_history as task_history_service
from openorc.services import task_reads
from openorc.services.errors import NotFoundError
from openorc.services.task_history import HistoryCollection, TaskHistory
from openorc.services.task_reads import (
    TaskCostFacts,
    TaskDetail,
    TaskGateSummary,
    TaskSummaryItem,
)

_OBSERVED = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)


def _profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


def _task(task_id: Any, workspace_id: Any, repository_id: Any, **overrides: Any) -> Any:
    from tests.fakes.task_rows import task_row

    fields: dict[str, Any] = {
        "id": task_id,
        "workspace_id": workspace_id,
        "repository_id": repository_id,
        "github_issue_id": 6550419907,
        "github_issue_number": 375,
        "status": "waiting_for_owner",
        "source_requirements_fingerprint": "a" * 64,
        "created_at": _OBSERVED,
        "updated_at": _OBSERVED,
    }
    fields.update(overrides)
    return _task_from_row(task_row(**fields))


def _issue_projection(workspace_id: Any, repository_id: Any) -> GitHubIssueProjection:
    return GitHubIssueProjection(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        repository_id=repository_id,
        identity=GitHubIssueIdentity(github_issue_id=6550419907),
        issue_number=375,
        title="Example issue",
        body="Steps",
        state=GitHubIssueState.OPEN,
        requirements_fingerprint="f" * 64,
        provider_updated_at=_OBSERVED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        state_reason=None,
        html_url="https://github.com/example/repo/issues/375",
    )


def _summary_item(task: Any, issue: Any, pull_request: Any = None) -> TaskSummaryItem:
    return TaskSummaryItem(
        task=task,
        github_issue=issue,
        pull_request=pull_request,
        cost=TaskCostFacts(
            retained_cost_usd=Decimal("2.47"),
            authorized_task_budget_usd=Decimal("10.00"),
            effective_cost_threshold_usd=None,
            is_partial=False,
        ),
        current_gate=None,
        current_block=None,
        actions=("cancel_task",),
        queue_position=None,
    )


@pytest.fixture()
def owner_profile() -> Profile:
    return _profile()


@pytest.fixture()
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        list_items: list[TaskSummaryItem] | None = None,
        list_cursor: str | None = None,
        detail: TaskDetail | None = None,
        history: TaskHistory | None = None,
        list_error: Exception | None = None,
        detail_error: Exception | None = None,
        history_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"lists": [], "details": [], "history": []}

        def _fake_pool(settings: Any) -> object:
            return object()

        def _list(kind: str):
            def _call(pool: Any, **command: Any) -> Any:
                seam["lists"].append({"kind": kind, **command})
                if list_error is not None:
                    raise list_error
                return (list_items or [], list_cursor)

            return _call

        def _detail(pool: Any, **command: Any) -> Any:
            seam["details"].append(command)
            if detail_error is not None:
                raise detail_error
            assert detail is not None
            return detail

        def _history(pool: Any, **command: Any) -> Any:
            seam["history"].append(command)
            if history_error is not None:
                raise history_error
            assert history is not None
            return history

        monkeypatch.setattr(task_reads_router, "get_database_pool", _fake_pool)
        monkeypatch.setattr(task_reads, "list_workspace_tasks_for_owner", _list("workspace"))
        monkeypatch.setattr(task_reads, "list_repository_tasks_for_owner", _list("repository"))
        monkeypatch.setattr(task_reads, "list_issue_tasks_for_owner", _list("issue"))
        monkeypatch.setattr(task_reads, "get_task_for_owner", _detail)
        monkeypatch.setattr(task_history_service, "list_task_history_for_owner", _history)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_the_workspace_list_is_bounded_cursor_paged_and_defaults_to_current(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    task_id = uuid.uuid4()
    items = [_summary_item(_task(task_id, workspace_id, repository_id), None)]
    cursor = base64.urlsafe_b64encode(b"t:1:2").decode("ascii")
    seam = service_seam(list_items=items, list_cursor=cursor)
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/tasks")

    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] == cursor
    assert len(body["items"]) == 1
    item = body["items"][0]
    # The ownership separation: a null github block (no observed projection
    # facts) beside the OpenOrc workflow facts with exact decimal strings.
    assert item["github"] == {"issue": None, "pull_request": None}
    assert item["openorc"]["id"] == str(task_id)
    assert item["openorc"]["status"] == "waiting_for_owner"
    assert item["openorc"]["cost"] == {
        "retained_cost_usd": "2.47",
        "authorized_task_budget_usd": "10.00",
        "effective_cost_threshold_usd": None,
        "is_partial": False,
    }
    assert item["openorc"]["actions"] == ["cancel_task"]
    assert seam["lists"] == [
        {
            "kind": "workspace",
            "profile_id": owner_profile.id,
            "workspace_id": workspace_id,
            "statuses": tuple(TaskStatus),
            "archival": "current",
            "cursor": None,
        }
    ]


def test_the_status_and_archival_filters_delegate_verbatim(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)
    cursor = base64.urlsafe_b64encode(b"t:1:2").decode("ascii")

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/tasks",
        params={"status": "queued,blocked", "archival": "archived", "cursor": cursor},
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}
    assert seam["lists"][0]["statuses"] == (TaskStatus.QUEUED, TaskStatus.BLOCKED)
    assert seam["lists"][0]["archival"] == "archived"
    assert seam["lists"][0]["cursor"] == cursor

    # A non-canonical status is the typed invalid command.
    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/tasks", params={"status": "fabricated"}
    )
    assert response.status_code == 422


def test_the_issue_attempts_list_defaults_to_all_archival(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    issue_id = uuid.uuid4()
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues/{issue_id}/tasks"
    )

    assert response.status_code == 200
    assert seam["lists"][0]["kind"] == "issue"
    assert seam["lists"][0]["archival"] == "all"
    assert seam["lists"][0]["issue_id"] == issue_id


def test_the_repository_list_is_scoped_to_the_addressed_repository(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    seam = service_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/tasks")

    assert response.status_code == 200
    assert seam["lists"][0]["kind"] == "repository"
    assert seam["lists"][0]["repository_id"] == repository_id

    # A foreign-scope probe is the uniform not-found, never an oracle.
    seam = service_seam(list_error=NotFoundError("the requested workspace is not available"))
    client = _client(settings_factory(), owner_profile)
    response = client.get(f"/api/v1/workspaces/{uuid.uuid4()}/repositories/{uuid.uuid4()}/tasks")
    assert response.status_code == 404


def _detail(task: Any, issue: Any, gate: Any) -> TaskDetail:
    plan = PlanRevision(
        id=uuid.uuid4(),
        workspace_id=task.workspace_id,
        task_id=task.id,
        revision_number=3,
        content="Full current implementation plan",
        repository_base_sha="abc123",
        created_at=_OBSERVED,
    )
    summary = TaskSummaryItem(
        task=task,
        github_issue=issue,
        pull_request=None,
        cost=TaskCostFacts(None, None, None, False),
        current_gate=TaskGateSummary(gate=gate, actions=("approve", "discuss", "cancel_task")),
        current_block=None,
        actions=("abort",),
        queue_position=None,
    )
    return TaskDetail(
        summary=summary,
        current_plan=plan,
        sessions=(),
        executions=(),
        executions_total=0,
        planning_bootstrap_stage=None,
        review_loops=(),
        pending_runtime_interactions=(),
        pending_runtime_requests=(),
        outstanding_receipts=(),
        outstanding_receipts_total=0,
        outstanding_intents=(),
    )


def test_the_detail_carries_the_current_gate_subject_menu_and_actions(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    task_id = uuid.uuid4()
    task = _task(
        task_id,
        workspace_id,
        repository_id,
        canonical_feature_branch="openorc/issue-375",
    )
    issue = _issue_projection(workspace_id, repository_id)
    gate = OwnerGate(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        status=OwnerGateStatus.PENDING,
        plan_revision_id=uuid.uuid4(),
        subject_head_sha=None,
        task_pull_request_id=None,
        task_block_id=None,
        decided_at=None,
        created_at=_OBSERVED,
    )
    service_seam(detail=_detail(task, issue, gate))
    client = _client(settings_factory(), owner_profile)

    response = client.get(f"/api/v1/workspaces/{workspace_id}/tasks/{task_id}")

    assert response.status_code == 200
    body = response.json()
    openorc = body["openorc"]
    assert body["github"]["issue"]["number"] == 375
    assert body["github"]["pull_request"] is None
    assert openorc["canonical_feature_branch"] == "openorc/issue-375"
    assert openorc["current_owner_gate"]["gate_type"] == "implementation_authorization"
    assert openorc["current_owner_gate"]["actions"] == [
        "approve",
        "discuss",
        "cancel_task",
    ]
    assert openorc["current_owner_gate"]["plan_revision_id"] == str(gate.plan_revision_id)
    assert openorc["current_plan"]["content"] == "Full current implementation plan"
    assert openorc["actions"] == ["abort"]
    # A history is separate: the detail carries no historical collections.
    assert "collections" not in openorc

    service_seam(detail_error=NotFoundError("the requested task is not available"))
    client = _client(settings_factory(), owner_profile)
    response = client.get(f"/api/v1/workspaces/{workspace_id}/tasks/{uuid.uuid4()}")
    assert response.status_code == 404


def test_the_history_delegates_the_collection_selection_and_per_collection_cursor(
    service_seam, settings_factory, owner_profile
) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    cursor = base64.urlsafe_b64encode(b"h:owner_gates:x|y").decode("ascii")
    history = TaskHistory(
        task_id=task_id,
        collections=(
            HistoryCollection(
                name="owner_gates",
                items=(),
                next_cursor=cursor,
                total=None,
            ),
        ),
    )
    seam = service_seam(history=history)
    client = _client(settings_factory(), owner_profile)

    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/tasks/{task_id}/history",
        params={"collections": "owner_gates", "cursor": cursor, "limit": 25},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["task_id"] == str(task_id)
    assert body["collections"][0]["name"] == "owner_gates"
    assert body["collections"][0]["next_cursor"] == cursor
    assert body["collections"][0]["items"] == []
    assert seam["history"][0] == {
        "profile_id": owner_profile.id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "collections": ["owner_gates"],
        "cursor": cursor,
        "limit": 25,
    }

    # An undocumented collection is the typed invalid command.
    service_seam(history_error=task_history_service.InvalidCommandError("unknown collection"))
    client = _client(settings_factory(), owner_profile)
    response = client.get(
        f"/api/v1/workspaces/{workspace_id}/tasks/{task_id}/history",
        params={"collections": "audit_events"},
    )
    assert response.status_code == 422
