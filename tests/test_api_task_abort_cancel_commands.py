"""Transport tests for the Owner Abort and shared Cancel Task commands (issue #380).

Route-level coverage of the HTTP mapping: the Abort command projects the
committed blocking composition plus each exact target's separately-typed
post-commit Interrupt dispatch facts (an acknowledged Interrupt is never
settlement, and an already-``blocked`` Task aborts to the typed
unresolved-interruption conflict), and the shared Cancel command maps the
service's classified outcome kinds verbatim — including the
terminalization-versus-archival distinction (a committed terminalization
with a still ``null`` ``archived_at``) and the confirmed-merge completion
routing with its F10 terminal-finalization seam composed. The uniform
typed-error outcomes (stale token, uniform not-found isolation, unconfigured
user-flow credentials, malformed body) round out the surface. The command
services' own authorization, targeting derivation, eligibility, and
cancellation matrices are owned by the #228/#376/#317 service suites — never
repeated here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.dependencies.github_user_flow import resolve_profile_user_token_resolver
from openorc.api.routers import tasks as tasks_router
from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntent,
    TaskPullRequestCloseIntentStatus,
)
from openorc.domain.ownership import Profile
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.reviews import ReviewLoop, ReviewLoopPurpose, ReviewLoopStatus
from openorc.domain.tasks import Task, TaskStatus, TaskTerminalizationOrigin
from openorc.services import task_abort, task_cancellation
from openorc.services.errors import (
    ConflictError,
    IntegrationNotConfiguredError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_user_authorization import GitHubUserAccessTokenResolver
from openorc.services.task_abort import (
    TaskAbortOutcome,
    TaskAbortOutcomeKind,
    TaskInterruptOutcome,
)
from openorc.services.task_cancellation import (
    TaskCancellationOutcome,
    TaskCancellationOutcomeKind,
)
from openorc.workers.invocation import SubmissionResult
from openorc.workers.task_cleanup_operations import F1TerminalFinalizationSubmission

_OBSERVED = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_REPOSITORY_ID = uuid4()
_TASK_ID = uuid4()
_GATE_ID = uuid4()
_PR_RECORD_ID = uuid4()
_BLOCK_ID = uuid4()
_LOOP_ID = uuid4()
_CLOSE_INTENT_ID = uuid4()
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_GITHUB_PR_ID = 901
_PR_NUMBER = 7
_STATE_TOKEN = uuid4()
_ROTATED_STATE_TOKEN = uuid4()
_HEAD_SHA = "b" * 40
_FINGERPRINT = "a" * 64

_TASK_ABORT_PATH = f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/abort"
_TASK_CANCEL_PATH = f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/cancel"
_COMMAND_BODY = {"state_token": str(_STATE_TOKEN)}


def _task(
    *,
    status: TaskStatus,
    terminalization_outcome: TaskStatus | None = None,
) -> Task:
    """One Task snapshot exactly as the command services authoritatively return it."""
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        github_issue_number=_ISSUE_NUMBER,
        status=status,
        archived_at=None,
        queued_at=None,
        canonical_feature_branch=None,
        state_token=_ROTATED_STATE_TOKEN,
        current_plan_revision_id=None,
        current_owner_gate_id=(_GATE_ID if status is TaskStatus.BLOCKED else None),
        source_requirements_fingerprint=_FINGERPRINT,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        authorized_task_budget_usd=None,
        terminalization_outcome=terminalization_outcome,
        terminalization_origin=(
            TaskTerminalizationOrigin.OWNER_CANCELLED if terminalization_outcome else None
        ),
        terminalization_established_at=(_OBSERVED if terminalization_outcome else None),
    )


def _gate() -> OwnerGate:
    """One resolved gate as the cancellation authoritatively returns it."""
    return OwnerGate(
        id=_GATE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        status=OwnerGateStatus.CANCELLED,
        plan_revision_id=None,
        subject_head_sha=_HEAD_SHA,
        task_pull_request_id=None,
        task_block_id=None,
        decided_at=_OBSERVED,
        created_at=_OBSERVED,
    )


def _pull_request() -> TaskPullRequest:
    """One authoritative canonical TaskPullRequest the classification grounds in."""
    return TaskPullRequest(
        id=_PR_RECORD_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        repository_id=_REPOSITORY_ID,
        github_pr_id=_GITHUB_PR_ID,
        github_pr_number=_PR_NUMBER,
        head_ref="feature/owned-branch",
        base_ref="main",
        head_sha=_HEAD_SHA,
        state=TaskPullRequestState.OPEN,
        merged_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _block() -> TaskBlock:
    """One applied OWNER_ABORT blocking condition as the Abort service returns it."""
    return TaskBlock(
        id=_BLOCK_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
        context={"control": "owner_abort", "target_roles": ["producer"]},
        resolved_at=None,
        created_at=_OBSERVED,
    )


def _loop() -> ReviewLoop:
    """One ReviewLoop the cancellation closed as actually present."""
    return ReviewLoop(
        id=_LOOP_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
        status=ReviewLoopStatus.CLOSED,
        closed_at=_OBSERVED,
        created_at=_OBSERVED,
    )


def _close_intent() -> TaskPullRequestCloseIntent:
    """The pending one-shot PR-close obligation recorded with the terminalization."""
    return TaskPullRequestCloseIntent(
        id=_CLOSE_INTENT_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_pull_request_id=_PR_RECORD_ID,
        github_pr_id=_GITHUB_PR_ID,
        github_pr_number=_PR_NUMBER,
        requested_by_profile_id=uuid4(),
        status=TaskPullRequestCloseIntentStatus.PENDING,
        outcome={},
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _abort_outcome(*, roles: tuple[str, ...] = ("producer",)) -> TaskAbortOutcome:
    return TaskAbortOutcome(
        kind=TaskAbortOutcomeKind.INTERRUPTED,
        task=_task(status=TaskStatus.BLOCKED),
        task_block=_block(),
        interrupt_outcomes=tuple(
            TaskInterruptOutcome(
                task_interrupt_intent_id=uuid4(),
                workflow_role=WorkflowRole(role),
                status=TaskInterruptIntentStatus.UNSETTLED,
                detail="acknowledged",
            )
            for role in roles
        ),
    )


def _cancel_outcome(
    *, kind: TaskCancellationOutcomeKind = TaskCancellationOutcomeKind.CANCELLED
) -> TaskCancellationOutcome:
    # The terminalization commitment keeps the Task's primary status and
    # ``archived_at`` truthfully untouched: the F10 terminal finalizer's
    # final archival transaction follows the command's commitment.
    if kind is TaskCancellationOutcomeKind.COMPLETED:
        return TaskCancellationOutcome(
            kind=kind,
            task=_task(status=TaskStatus.REVIEWING, terminalization_outcome=TaskStatus.COMPLETED),
            owner_gate=None,
            closed_loops=(),
            pull_request=_pull_request(),
            close_intent=None,
        )
    return TaskCancellationOutcome(
        kind=kind,
        task=_task(
            status=TaskStatus.WAITING_FOR_OWNER, terminalization_outcome=TaskStatus.CANCELLED
        ),
        owner_gate=_gate(),
        closed_loops=(_loop(),),
        pull_request=_pull_request(),
        close_intent=_close_intent(),
    )


class _RecordingSubmitter:
    """One process F1 submitter double."""

    def __init__(self) -> None:
        self.invocations: list[Any] = []

    def submit(self, invocation: Any) -> SubmissionResult:
        self.invocations.append(invocation)
        return SubmissionResult.accepted()


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid4(), created_at=_OBSERVED)


def _client(settings: Any, owner_profile: Profile, submitter: _RecordingSubmitter) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    app.state.operation_submitter = submitter
    return TestClient(app)


@pytest.fixture
def abort_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the router's Owner Abort service boundary, recording its calls."""
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(tasks_router, "get_database_pool", lambda _settings: object())

    def _abort(
        pool: Any,
        dispatch: Any,
        *,
        profile_id: UUID,
        workspace_id: UUID,
        task_id: UUID,
        expected_state_token: UUID,
    ) -> TaskAbortOutcome:
        calls.append(
            {
                "dispatch": dispatch,
                "profile_id": profile_id,
                "workspace_id": workspace_id,
                "task_id": task_id,
                "expected_state_token": expected_state_token,
            }
        )
        return _abort_outcome()

    monkeypatch.setattr(task_abort, "abort_task_activity_for_owner", _abort)
    return calls


@pytest.fixture
def cancel_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the router's shared Cancel service boundary, recording its calls."""

    def _make(
        *,
        cancel_result: TaskCancellationOutcome | None = None,
        cancel_error: Exception | None = None,
        real_user_token_resolver: bool = False,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"cancels": []}
        github_sentinel = object()
        user_token_sentinel = object()

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _fake_github(settings: Any) -> object:
            return github_sentinel

        def _fake_user_token_resolver(settings: Any) -> object:
            return user_token_sentinel

        def _cancel(
            pool: Any, github: Any, user_token_resolver: Any, **command: Any
        ) -> TaskCancellationOutcome:
            seam["cancels"].append(
                {"github": github, "user_token_resolver": user_token_resolver, **command}
            )
            if cancel_error is not None:
                raise cancel_error
            assert cancel_result is not None
            return cancel_result

        monkeypatch.setattr(tasks_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(tasks_router, "resolve_github_app_client", _fake_github)
        if not real_user_token_resolver:
            monkeypatch.setattr(
                tasks_router, "resolve_profile_user_token_resolver", _fake_user_token_resolver
            )
        monkeypatch.setattr(task_cancellation, "cancel_task", _cancel)
        seam["github_sentinel"] = github_sentinel
        return seam

    return _make


# --- The Task-level Owner Abort command ----------------------------------------


def test_the_owner_abort_projects_the_committed_block_and_dispatch_facts(
    abort_seam, settings_factory, owner_profile
) -> None:
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_ABORT_PATH, json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "interrupted"
    assert body["task"]["status"] == "blocked"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["task_block"]["id"] == str(_BLOCK_ID)
    assert body["task_block"]["reason"] == "owner_action_required"
    assert body["interrupt_outcomes"] == [
        {
            "task_interrupt_intent_id": body["interrupt_outcomes"][0]["task_interrupt_intent_id"],
            "workflow_role": "producer",
            "status": "unsettled",
            "detail": "acknowledged",
        }
    ]
    # The exact addressed evidence reaches the service; the dispatch seam is
    # the composition-rooted per-target dispatcher the router composed.
    (call,) = abort_seam
    assert call["expected_state_token"] == _STATE_TOKEN
    assert call["profile_id"] == owner_profile.id
    assert isinstance(call["dispatch"], tasks_router.OwnerAbortInterruptDispatcher)


@pytest.mark.parametrize(
    ("seam_error", "expected_status"),
    [
        (ConflictError("the owner activity interruption is unresolved"), 409),
        (StaleOperationError("the abort is stale"), 409),
        (NotFoundError("uniform not-found"), 404),
        (IntegrationNotConfiguredError("unconfigured runtime boundary"), 503),
    ],
)
def test_the_abort_command_maps_the_typed_error_vocabulary(
    monkeypatch: pytest.MonkeyPatch, settings_factory, owner_profile, seam_error, expected_status
) -> None:
    # An already-blocked Task's unresolved interruption, a stale token, a
    # foreign/missing subject, and an unconfigured runtime boundary are the
    # typed HTTP outcomes — the service owns every classification.
    def _failing_abort(*_args: Any, **_kwargs: Any) -> TaskAbortOutcome:
        raise seam_error

    monkeypatch.setattr(task_abort, "abort_task_activity_for_owner", _failing_abort)
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_ABORT_PATH, json=_COMMAND_BODY)

    assert response.status_code == expected_status


def test_a_malformed_abort_body_is_the_framework_validation_error(
    abort_seam, settings_factory, owner_profile
) -> None:
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_ABORT_PATH, json={})

    assert response.status_code == 422


# --- The shared Cancel Task command ---------------------------------------------


def test_the_shared_cancel_projects_the_committed_terminalization_truthfully(
    cancel_seam, settings_factory, owner_profile
) -> None:
    seam = cancel_seam(cancel_result=_cancel_outcome())
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_CANCEL_PATH, json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "cancelled"
    # A committed terminalization never implies archived or capacity-released
    # status: the Task's primary status and ``archived_at`` stay truthfully
    # untouched — the mandatory final archival is the F10 terminal
    # finalizer's follow-up transaction.
    assert body["task"]["status"] == "waiting_for_owner"
    assert body["task"]["archived_at"] is None
    assert body["terminalization"]["outcome"] == "cancelled"
    assert body["terminalization"]["origin"] == "owner_cancelled"
    assert body["owner_gate"]["id"] == str(_GATE_ID)
    assert body["owner_gate"]["status"] == "cancelled"
    assert body["closed_loops"] == [
        {
            "id": str(_LOOP_ID),
            "purpose": "planning",
            "status": "closed",
            "closed_at": body["closed_loops"][0]["closed_at"],
        }
    ]
    assert body["pull_request"]["id"] == _GITHUB_PR_ID
    assert body["pull_request"]["state"] == "open"
    assert body["close_intent"]["id"] == str(_CLOSE_INTENT_ID)
    assert body["close_intent"]["github_pr_id"] == _GITHUB_PR_ID
    assert body["close_intent"]["github_pr_number"] == _PR_NUMBER
    assert body["close_intent"]["status"] == "pending"
    # The exact addressed evidence reaches the unified service with the
    # GitHub boundaries and the F10 terminal-finalization seam composed.
    (call,) = seam["cancels"]
    assert call["expected_state_token"] == _STATE_TOKEN
    assert call["profile_id"] == owner_profile.id
    assert call["github"] is seam["github_sentinel"]
    assert isinstance(call["terminal_finalization_submission"], F1TerminalFinalizationSubmission)


def test_the_shared_cancel_maps_the_confirmed_merge_completion_kind(
    cancel_seam, settings_factory, owner_profile
) -> None:
    # A canonical PR already authoritatively confirmed merged at the point of
    # decision routes to the shared completion path instead of an Owner
    # ``CANCELLED`` claim — a committed durable disposition, never an error.
    cancel_seam(cancel_result=_cancel_outcome(kind=TaskCancellationOutcomeKind.COMPLETED))
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_CANCEL_PATH, json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "completed"
    assert body["task"]["status"] == "reviewing"
    assert body["task"]["archived_at"] is None
    assert body["terminalization"]["outcome"] == "completed"
    assert body["owner_gate"] is None
    assert body["close_intent"] is None
    assert body["closed_loops"] == []
    assert body["pull_request"]["state"] == "open"


@pytest.mark.parametrize(
    ("seam_error", "expected_status"),
    [
        (ConflictError("an outstanding runtime work fact vetoes cancel"), 409),
        (StaleOperationError("the cancellation is stale"), 409),
        (NotFoundError("uniform not-found"), 404),
        (IntegrationNotConfiguredError("unconfigured GitHub boundary"), 503),
    ],
)
def test_the_cancel_command_maps_the_typed_error_vocabulary(
    cancel_seam, settings_factory, owner_profile, seam_error, expected_status
) -> None:
    # The outstanding-work veto, a stale gate/token, a foreign/missing
    # subject, and an unconfigured boundary are the typed HTTP outcomes —
    # the unified service owns every classification.
    cancel_seam(cancel_error=seam_error)
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_CANCEL_PATH, json=_COMMAND_BODY)

    assert response.status_code == expected_status


def test_unconfigured_user_flow_credentials_fail_closed(
    cancel_seam, settings_factory, owner_profile
) -> None:
    # The shared Cancel command composes the Profile user-token resolver
    # itself: an unconfigured deployment refuses the command at the boundary
    # before the service ever runs — and never fails on the unrelated #373
    # Admin credentials.
    seam = cancel_seam(real_user_token_resolver=True)
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_CANCEL_PATH, json=_COMMAND_BODY)

    assert response.status_code == 503
    assert seam["cancels"] == []


def test_a_malformed_cancel_body_is_the_framework_validation_error(
    cancel_seam, settings_factory, owner_profile
) -> None:
    cancel_seam()
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_TASK_CANCEL_PATH, json={})

    assert response.status_code == 422


# --- The Profile user-token resolver composition --------------------------------


def test_the_user_token_resolver_composition_fails_closed_when_unconfigured(
    settings_factory,
) -> None:
    with pytest.raises(IntegrationNotConfiguredError):
        resolve_profile_user_token_resolver(settings_factory())


def test_the_user_token_resolver_composition_builds_from_the_user_flow_credentials(
    settings_factory,
) -> None:
    resolver = resolve_profile_user_token_resolver(
        settings_factory(
            github_app_user_flow_client_id="test-client-id",
            github_app_user_flow_client_secret="test-client-secret",
        )
    )
    assert isinstance(resolver, GitHubUserAccessTokenResolver)
