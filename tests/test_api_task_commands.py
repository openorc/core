"""Transport tests for the Owner Task-command surface (issue #378).

Route-level coverage of the HTTP mapping: the intake command's created and
converged-duplicate outcomes projected from the service's authoritative
post-write snapshot (a converged duplicate preserves the existing Task's
actual returned status — never an assumed ``ready_to_plan``), the Start
command's committed ``queued`` snapshot with the separately-typed wake
delivery evidence mapped verbatim, the no-reload regression (immediate FIFO
admission before the HTTP response never re-derives the body through the
Task-read surface), and the uniform typed-error outcomes. The command
services' own eligibility, authorization, queue, and audit matrices are
owned by the #60/#257/#377 service suites — never repeated here.
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
from openorc.api.dependencies.github_app_client import resolve_github_app_client
from openorc.api.routers import task_commands as task_commands_router
from openorc.domain.ownership import Profile
from openorc.domain.tasks import Task, TaskStatus
from openorc.services import task_intake, task_reads, task_start
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_intake import IssueBlockedEligibilityError, RepositoryTaskIntake
from openorc.services.task_queue_wake import TaskQueueWakeSubmissionOutcome
from openorc.services.task_start import TaskStartOutcome
from openorc.workers.task_queue_operations import F1TaskQueueWakeSubmission

_OBSERVED = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
_QUEUED_AT = datetime(2026, 11, 25, 12, 30, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_ISSUE_PROJECTION_ID = uuid.uuid4()
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_STATE_TOKEN = uuid.uuid4()
_ROTATED_STATE_TOKEN = uuid.uuid4()
_FINGERPRINT = "a" * 64
_BUDGET = Decimal("15.00")


def _task(
    *,
    status: TaskStatus,
    queued_at: datetime | None = None,
    fingerprint: str | None = None,
    budget: Decimal | None = None,
) -> Task:
    """One Task snapshot as the command services authoritatively return it."""
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        github_issue_number=_ISSUE_NUMBER,
        status=status,
        archived_at=None,
        queued_at=queued_at,
        canonical_feature_branch=None,
        state_token=_ROTATED_STATE_TOKEN,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint=fingerprint,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        authorized_task_budget_usd=budget,
    )


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


def _client(settings: Any, owner_profile: Profile, operation_submitter: Any = None) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    if operation_submitter is not None:
        app.state.operation_submitter = operation_submitter
    return TestClient(app)


@pytest.fixture
def command_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the router module's service boundaries with recorded doubles."""

    def _make(
        *,
        intake_result: RepositoryTaskIntake | None = None,
        intake_error: Exception | None = None,
        start_result: TaskStartOutcome | None = None,
        start_error: Exception | None = None,
        resolve_error: Exception | None = None,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"intakes": [], "starts": [], "github": []}
        github_sentinel = object()

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _fake_resolve_github_app_client(settings: Any) -> object:
            seam["github"].append(settings)
            if resolve_error is not None:
                raise resolve_error
            return github_sentinel

        seam["github_sentinel"] = github_sentinel

        def _intake(pool: Any, github: Any, **command: Any) -> RepositoryTaskIntake:
            seam["intakes"].append({"github": github, **command})
            if intake_error is not None:
                raise intake_error
            assert intake_result is not None
            return intake_result

        def _start(pool: Any, github: Any, **command: Any) -> TaskStartOutcome:
            seam["starts"].append({"github": github, **command})
            if start_error is not None:
                raise start_error
            assert start_result is not None
            return start_result

        monkeypatch.setattr(task_commands_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(
            task_commands_router, "resolve_github_app_client", _fake_resolve_github_app_client
        )
        monkeypatch.setattr(
            task_intake, "intake_repository_task_for_issue_projection_for_owner", _intake
        )
        monkeypatch.setattr(task_start, "start_task_for_owner", _start)
        return seam

    return _make


# --- The issue-level create_task command ------------------------------------


def test_the_create_task_command_returns_the_created_snapshot(
    command_seam, settings_factory, owner_profile
) -> None:
    seam = command_seam(
        intake_result=RepositoryTaskIntake(
            task=_task(status=TaskStatus.READY_TO_PLAN), created=True
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/repositories/{_REPOSITORY_ID}"
        f"/issues/{_ISSUE_PROJECTION_ID}/tasks"
    )

    assert response.status_code == 200
    body = response.json()
    # The authoritative post-write snapshot, verbatim — a created intake is
    # a ready_to_plan Task with no baseline and no budget, never enqueued.
    assert body["created"] is True
    assert body["task"]["task_id"] == str(_TASK_ID)
    assert body["task"]["workspace_id"] == str(_WORKSPACE_ID)
    assert body["task"]["repository_id"] == str(_REPOSITORY_ID)
    assert body["task"]["github_issue_id"] == _GITHUB_ISSUE_ID
    assert body["task"]["github_issue_number"] == _ISSUE_NUMBER
    assert body["task"]["status"] == "ready_to_plan"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert datetime.fromisoformat(body["task"]["created_at"]) == _OBSERVED
    assert seam["intakes"] == [
        {
            "github": seam["intakes"][0]["github"],
            "profile_id": owner_profile.id,
            "workspace_id": _WORKSPACE_ID,
            "repository_id": _REPOSITORY_ID,
            "issue_id": _ISSUE_PROJECTION_ID,
        }
    ]
    assert seam["intakes"][0]["github"] is seam["github_sentinel"]
    assert len(seam["github"]) == 1


def test_a_converged_duplicate_intake_preserves_the_existing_task_status(
    command_seam, settings_factory, owner_profile
) -> None:
    # A concurrent duplicate converges onto the existing current Task: the
    # returned snapshot is that Task's ACTUAL status (it may already have
    # been started by its creator) — never an assumed ready_to_plan.
    command_seam(
        intake_result=RepositoryTaskIntake(
            task=_task(
                status=TaskStatus.QUEUED,
                queued_at=_QUEUED_AT,
                fingerprint=_FINGERPRINT,
                budget=_BUDGET,
            ),
            created=False,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/repositories/{_REPOSITORY_ID}"
        f"/issues/{_ISSUE_PROJECTION_ID}/tasks"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["created"] is False
    assert body["task"]["status"] == "queued"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)


# --- The Task-level start command -------------------------------------------


def _start_outcome(wake_outcome: TaskQueueWakeSubmissionOutcome | None) -> TaskStartOutcome:
    return TaskStartOutcome(
        task=_task(
            status=TaskStatus.QUEUED,
            queued_at=_QUEUED_AT,
            fingerprint=_FINGERPRINT,
            budget=_BUDGET,
        ),
        queue_wake_outcome=wake_outcome,
    )


def test_the_start_command_returns_the_committed_queued_snapshot_and_wake_evidence(
    command_seam, settings_factory, owner_profile
) -> None:
    seam = command_seam(
        start_result=_start_outcome(TaskQueueWakeSubmissionOutcome.ACCEPTED_FOR_DELIVERY)
    )
    client = _client(settings_factory(), owner_profile, operation_submitter=object())

    response = client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/start",
        json={"state_token": str(_STATE_TOKEN)},
    )

    assert response.status_code == 200
    body = response.json()
    # The committed post-write snapshot: the original queue-entry timestamp,
    # the rotated token, the frozen fingerprint and budget — exactly one
    # guarded write, regardless of any later FIFO admission.
    assert body["task"]["task_id"] == str(_TASK_ID)
    assert body["task"]["status"] == "queued"
    assert datetime.fromisoformat(body["task"]["queued_at"]) == _QUEUED_AT
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["task"]["source_requirements_fingerprint"] == _FINGERPRINT
    assert body["task"]["authorized_task_budget_usd"] == "15.00"
    assert body["queue_wake_outcome"] == "accepted_for_delivery"
    # The command delegated with the exact body state token, the
    # authenticated Profile, and the F1-backed wake seam.
    recorded = seam["starts"][0]
    assert recorded["profile_id"] == owner_profile.id
    assert recorded["workspace_id"] == _WORKSPACE_ID
    assert recorded["task_id"] == _TASK_ID
    assert recorded["expected_state_token"] == _STATE_TOKEN
    assert recorded["github"] is seam["github_sentinel"]
    assert isinstance(recorded["queue_wake"], F1TaskQueueWakeSubmission)


@pytest.mark.parametrize(
    ("wake_outcome", "wire_value"),
    [
        (TaskQueueWakeSubmissionOutcome.KNOWN_REFUSED, "known_refused"),
        (TaskQueueWakeSubmissionOutcome.UNCERTAIN, "uncertain"),
        (None, None),
    ],
)
def test_the_wake_delivery_evidence_is_documented_separately_from_the_queue_entry(
    command_seam, settings_factory, owner_profile, wake_outcome, wire_value
) -> None:
    command_seam(start_result=_start_outcome(wake_outcome))
    client = _client(settings_factory(), owner_profile, operation_submitter=object())

    response = client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/start",
        json={"state_token": str(_STATE_TOKEN)},
    )

    # A refused, uncertain, or unwired wake never fails or reverses the
    # committed queue entry: still a 200 carrying the committed queued
    # snapshot, with the delivery evidence as its own separate field.
    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "queued"
    assert datetime.fromisoformat(body["task"]["queued_at"]) == _QUEUED_AT
    assert body["queue_wake_outcome"] == wire_value


def test_immediate_fifo_admission_before_the_response_never_rederives_the_body(
    command_seam, settings_factory, owner_profile, monkeypatch: pytest.MonkeyPatch
) -> None:
    # FIFO admission can advance the Task (to planning, clearing queued_at
    # and rotating the token) before the HTTP response is written. The
    # command response is the service's authoritative post-write snapshot —
    # the Task-read surface is never consulted to build it.
    def _no_read_service(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the start command must never re-read the Task projection")

    monkeypatch.setattr(task_reads, "get_task_for_owner", _no_read_service)
    seam = command_seam(
        start_result=_start_outcome(TaskQueueWakeSubmissionOutcome.ACCEPTED_FOR_DELIVERY)
    )
    client = _client(settings_factory(), owner_profile, operation_submitter=object())

    response = client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/start",
        json={"state_token": str(_STATE_TOKEN)},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "queued"
    assert datetime.fromisoformat(body["task"]["queued_at"]) == _QUEUED_AT
    assert body["task"]["source_requirements_fingerprint"] == _FINGERPRINT
    assert seam["starts"] != []


# --- The shared typed-error transport mapping -------------------------------


@pytest.mark.parametrize(
    ("service_error", "expected_status"),
    [
        (NotFoundError("uniform not-found"), 404),
        (AuthorizationError("not authorized"), 403),
        (IssueBlockedEligibilityError("the issue is blocked"), 409),
        (ConflictError("unserializable intake write"), 409),
        (StaleOperationError("the task state has moved on"), 409),
        (InvalidCommandError("malformed command"), 422),
        (ExternalOperationFailedError("github unavailable"), 503),
        (ExternalOperationUncertainError("github outcome unknown"), 503),
    ],
)
def test_the_command_error_vocabulary_maps_onto_the_uniform_http_outcomes(
    command_seam, settings_factory, owner_profile, service_error, expected_status
) -> None:
    command_seam(intake_error=service_error)
    intake_client = _client(settings_factory(), owner_profile)

    intake_response = intake_client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/repositories/{_REPOSITORY_ID}"
        f"/issues/{_ISSUE_PROJECTION_ID}/tasks"
    )
    assert intake_response.status_code == expected_status

    command_seam(start_error=service_error)
    start_client = _client(settings_factory(), owner_profile, operation_submitter=object())

    start_response = start_client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/start",
        json={"state_token": str(_STATE_TOKEN)},
    )
    assert start_response.status_code == expected_status


def test_an_unconfigured_github_identity_fails_closed(
    command_seam, settings_factory, owner_profile
) -> None:
    # The command fails closed at the boundary composition — the unconfigured
    # GitHub App identity refuses the command before the service ever runs.
    seam = command_seam(resolve_error=IntegrationNotConfiguredError("unconfigured identity"))
    client = _client(settings_factory(), owner_profile)

    response = client.post(
        f"/api/v1/workspaces/{_WORKSPACE_ID}/repositories/{_REPOSITORY_ID}"
        f"/issues/{_ISSUE_PROJECTION_ID}/tasks"
    )

    assert response.status_code == 503
    assert seam["intakes"] == []


def test_the_github_app_boundary_fails_closed_when_unconfigured(settings_factory) -> None:
    # The test Settings carry no GitHub App identity: the adapter's own
    # authoritative ConfigurationError converts to the typed
    # integration-unconfigured vocabulary at the transport boundary.
    with pytest.raises(IntegrationNotConfiguredError):
        resolve_github_app_client(settings_factory())
