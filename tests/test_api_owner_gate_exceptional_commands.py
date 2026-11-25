"""Transport tests for the exceptional gate action commands (G3d, issue #381).

Route-level coverage of the HTTP mapping: the gate-scoped Ignore command
(the G2b ``ignore`` action for a pending ``GITHUB_ISSUE_CHANGE`` gate)
projects the committed Ignore composition verbatim — the resolved gate and
block, the acknowledged fingerprint, and the interrupted boundary's
re-presented approval gate with its exact subject facts for both the
implementation and PR boundary variants — with no continuation submission
at all; the gate-scoped Continue and Resume commands (the G2b ``continue``
and ``resume`` actions for pending ``COST_LIMIT`` and ``OWNER_ABORT`` gates)
project the committed authorization facts plus the separately-typed one-shot
session-resume submission evidence the real shared continuation helper
captured through the process submitter (a refused or uncertain submission
never fails the committed authorization). The uniform typed-error outcomes
(unsupported menu or unsettled abort episode, stale token/gate, uniform
not-found isolation, unconfigured GitHub boundary, malformed body) round out
the surface. The command services' own authorization, exactness, episode,
and continuation matrices are owned by the #289/#291/#292/#258 service
suites — never repeated here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import tasks as tasks_router
from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import TaskResumeIntent, TaskResumeIntentStatus
from openorc.domain.ownership import Profile
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.tasks import Task, TaskStatus
from openorc.services import (
    task_owner_cost_continue,
    task_owner_resume,
    task_source_change_ignore,
)
from openorc.services.errors import (
    ConflictError,
    IntegrationNotConfiguredError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_implementation_authorization import ImplementationGateOpened
from openorc.services.task_owner_cost_continue import AuthorizedTaskCostContinue
from openorc.services.task_owner_resume import AuthorizedTaskResume
from openorc.services.task_pr_authorization import PrAuthorizationGateOpened
from openorc.services.task_source_change_ignore import (
    IgnoredSourceChange,
    IgnoredSourceChangeBoundary,
)
from openorc.workers.invocation import (
    OperationKind,
    SubmissionRefusalReason,
    SubmissionResult,
)

_OBSERVED = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_REPOSITORY_ID = uuid4()
_TASK_ID = uuid4()
_GATE_ID = uuid4()
_PLAN_REVISION_ID = uuid4()
_BLOCK_ID = uuid4()
_LOOP_ID = uuid4()
_ITERATION_ID = uuid4()
_SESSION_ID = uuid4()
_RESUME_ID = uuid4()
_OWNER_PROFILE_ID = uuid4()
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_STATE_TOKEN = uuid4()
_ROTATED_STATE_TOKEN = uuid4()
_HEAD_SHA = "b" * 40
_FINGERPRINT = "a" * 64
_REFRESHED_THRESHOLD = Decimal("22.00")

_COMMAND_BODY = {"state_token": str(_STATE_TOKEN)}


def _gate_path(action: str) -> str:
    return f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/owner-gates/{_GATE_ID}/{action}"


def _task(*, status: TaskStatus) -> Task:
    """One Task snapshot exactly as the exceptional services authoritatively return it."""
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        github_issue_number=_ISSUE_NUMBER,
        status=status,
        archived_at=None,
        queued_at=None,
        canonical_feature_branch="feature/owned-branch",
        state_token=_ROTATED_STATE_TOKEN,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint=_FINGERPRINT,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        authorized_task_budget_usd=None,
        terminalization_outcome=None,
        terminalization_origin=None,
        terminalization_established_at=None,
    )


def _gate(*, gate_type: OwnerGateType, status: OwnerGateStatus) -> OwnerGate:
    """One service-returned OwnerGate with its exact bound subject form.

    The subject binding is the domain's own per-type invariant: ordinary
    implementation gates bind exactly the PlanRevision, ordinary PR gates
    bind exactly the verified head SHA, and the exceptional types bind
    exactly their one blocking condition.
    """
    plan_revision_id: UUID | None
    subject_head_sha: str | None
    task_block_id: UUID | None
    if gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        plan_revision_id, subject_head_sha, task_block_id = _PLAN_REVISION_ID, None, None
    elif gate_type is OwnerGateType.PR_AUTHORIZATION:
        plan_revision_id, subject_head_sha, task_block_id = None, _HEAD_SHA, None
    else:
        plan_revision_id, subject_head_sha, task_block_id = None, None, _BLOCK_ID
    return OwnerGate(
        id=_GATE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=gate_type,
        status=status,
        plan_revision_id=plan_revision_id,
        subject_head_sha=subject_head_sha,
        task_pull_request_id=None,
        task_block_id=task_block_id,
        decided_at=(_OBSERVED if status is OwnerGateStatus.APPROVED else None),
        created_at=_OBSERVED,
    )


def _block(reason: TaskBlockReason, context: dict[str, Any]) -> TaskBlock:
    """One resolved blocking condition with its evidence context retained."""
    return TaskBlock(
        id=_BLOCK_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        reason=reason,
        context=context,
        resolved_at=_OBSERVED,
        created_at=_OBSERVED,
    )


def _resume_record() -> TaskResumeIntent:
    """The committed one-shot session-resume record the continuation addresses."""
    return TaskResumeIntent(
        id=_RESUME_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        task_block_id=_BLOCK_ID,
        workflow_role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-1",
        requested_by_profile_id=_OWNER_PROFILE_ID,
        status=TaskResumeIntentStatus.AUTHORIZED,
        routing={"control": "owner_resume", "operation": "implementation_execution"},
        outcome={},
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _plan_revision() -> PlanRevision:
    """The bound plan subject of the re-presented implementation gate."""
    return PlanRevision(
        id=_PLAN_REVISION_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        revision_number=1,
        content="the accepted plan",
        repository_base_sha="0" * 40,
        created_at=_OBSERVED,
    )


def _review_loop() -> ReviewLoop:
    """The still-OPEN planning loop the presented implementation gate binds."""
    return ReviewLoop(
        id=_LOOP_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
        status=ReviewLoopStatus.OPEN,
        closed_at=None,
        created_at=_OBSERVED,
    )


def _review_iteration() -> ReviewIteration:
    """The finalized ACCEPTED planning iteration that made the plan eligible."""
    return ReviewIteration(
        id=_ITERATION_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        review_loop_id=_LOOP_ID,
        iteration_number=1,
        plan_revision_id=_PLAN_REVISION_ID,
        task_pull_request_id=None,
        reviewed_head_sha=None,
        outcome=ReviewOutcome.ACCEPTED,
        summary="accepted",
        findings=[],
        decided_at=_OBSERVED,
        created_at=_OBSERVED,
    )


def _ignore_outcome(*, boundary: IgnoredSourceChangeBoundary) -> IgnoredSourceChange:
    """The committed Ignore effect for one interrupted approval boundary."""
    presented = (
        PrAuthorizationGateOpened(
            task=_task(status=TaskStatus.WAITING_FOR_OWNER),
            owner_gate=_gate(
                gate_type=OwnerGateType.PR_AUTHORIZATION, status=OwnerGateStatus.PENDING
            ),
            canonical_branch="feature/owned-branch",
            verified_head_sha=_HEAD_SHA,
        )
        if boundary is IgnoredSourceChangeBoundary.PR_AUTHORIZATION
        else ImplementationGateOpened(
            task=_task(status=TaskStatus.WAITING_FOR_OWNER),
            owner_gate=_gate(
                gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
                status=OwnerGateStatus.PENDING,
            ),
            plan_revision=_plan_revision(),
            review_iteration=_review_iteration(),
            review_loop=_review_loop(),
        )
    )
    return IgnoredSourceChange(
        boundary=boundary,
        task=_task(status=TaskStatus.WAITING_FOR_OWNER),
        resolved_owner_gate=_gate(
            gate_type=OwnerGateType.GITHUB_ISSUE_CHANGE, status=OwnerGateStatus.APPROVED
        ),
        resolved_task_block=_block(
            TaskBlockReason.GITHUB_SOURCE_CHANGED,
            {"expected_fingerprint": _FINGERPRINT, "observed_fingerprint": _FINGERPRINT},
        ),
        acknowledged_fingerprint=_FINGERPRINT,
        presented=presented,
    )


def _continue_outcome() -> AuthorizedTaskCostContinue:
    """The committed Continue effect with its refreshed allowance and one-shot record."""
    return AuthorizedTaskCostContinue(
        task=_task(status=TaskStatus.IMPLEMENTING),
        resolved_owner_gate=_gate(
            gate_type=OwnerGateType.COST_LIMIT, status=OwnerGateStatus.APPROVED
        ),
        resolved_task_block=_block(TaskBlockReason.COST_LIMIT, {"control": "cost_limit"}),
        effective_cost_threshold_usd=_REFRESHED_THRESHOLD,
        resume=_resume_record(),
    )


def _resume_outcome() -> AuthorizedTaskResume:
    """The committed Resume effect with its restored status and one-shot record."""
    return AuthorizedTaskResume(
        task=_task(status=TaskStatus.IMPLEMENTING),
        resolved_owner_gate=_gate(
            gate_type=OwnerGateType.OWNER_ABORT, status=OwnerGateStatus.APPROVED
        ),
        resolved_task_block=_block(
            TaskBlockReason.OWNER_ACTION_REQUIRED, {"control": "owner_abort"}
        ),
        resume=_resume_record(),
    )


class _RecordingSubmitter:
    """One process F1 submitter double recording submitted invocations."""

    def __init__(self, result: SubmissionResult | None = None) -> None:
        self._result = result if result is not None else SubmissionResult.accepted()
        self.invocations: list[Any] = []

    def submit(self, invocation: Any) -> SubmissionResult:
        self.invocations.append(invocation)
        return self._result


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=_OWNER_PROFILE_ID, created_at=_OBSERVED)


def _client(settings: Any, owner_profile: Profile, submitter: _RecordingSubmitter) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    app.state.operation_submitter = submitter
    return TestClient(app)


@pytest.fixture
def exceptional_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the router's three exceptional-gate service boundaries.

    The shared session-resume continuation helper is deliberately left real:
    the Continue and Resume commands must compose the exact typed
    TASK_SESSION_RESUME invocation through the process submitter, which the
    tests inject as a recording double.
    """

    def _make(
        *,
        ignore_result: Any = None,
        ignore_error: Exception | None = None,
        continue_result: Any = None,
        continue_error: Exception | None = None,
        resume_result: Any = None,
        resume_error: Exception | None = None,
        real_github_composition: bool = False,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {"ignores": [], "continues": [], "resumes": []}
        github_sentinel = object()

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _fake_github(settings: Any) -> object:
            return github_sentinel

        def _ignore(pool: Any, github: Any, **command: Any) -> Any:
            seam["ignores"].append({"github": github, **command})
            if ignore_error is not None:
                raise ignore_error
            return ignore_result

        def _continue(pool: Any, **command: Any) -> Any:
            seam["continues"].append(command)
            if continue_error is not None:
                raise continue_error
            return continue_result

        def _resume(pool: Any, **command: Any) -> Any:
            seam["resumes"].append(command)
            if resume_error is not None:
                raise resume_error
            return resume_result

        monkeypatch.setattr(tasks_router, "get_database_pool", _fake_get_database_pool)
        if not real_github_composition:
            monkeypatch.setattr(tasks_router, "resolve_github_app_client", _fake_github)
        monkeypatch.setattr(task_source_change_ignore, "ignore_github_source_change", _ignore)
        monkeypatch.setattr(task_owner_cost_continue, "continue_task_after_cost_limit", _continue)
        monkeypatch.setattr(task_owner_resume, "resume_task_after_abort", _resume)
        seam["github_sentinel"] = github_sentinel
        return seam

    return _make


# --- The gate-scoped Ignore command ----------------------------------------------


def test_the_ignore_command_projects_the_committed_ignore_and_presented_gate(
    exceptional_seam, settings_factory, owner_profile
) -> None:
    # The interrupted E5 implementation-authorization boundary: the committed
    # Ignore resolves the exceptional gate and block, overwrites the Task's
    # fingerprint with the exact observed value, and re-presents the existing
    # approval gate with its exact bound plan subject — all in one commit.
    # No continuation is submitted here at all.
    seam = exceptional_seam(
        ignore_result=_ignore_outcome(
            boundary=IgnoredSourceChangeBoundary.IMPLEMENTATION_AUTHORIZATION
        )
    )
    submitter = _RecordingSubmitter()
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("ignore-source-change"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["task_id"] == str(_TASK_ID)
    assert body["task"]["status"] == "waiting_for_owner"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["owner_gate"]["id"] == str(_GATE_ID)
    assert body["owner_gate"]["gate_type"] == "github_issue_change"
    assert body["owner_gate"]["status"] == "approved"
    assert body["owner_gate"]["decided_at"] is not None
    assert body["task_block"]["id"] == str(_BLOCK_ID)
    assert body["task_block"]["reason"] == "github_source_changed"
    assert body["boundary"] == "implementation_authorization"
    assert body["acknowledged_fingerprint"] == _FINGERPRINT
    assert body["presented"]["owner_gate"]["id"] == str(_GATE_ID)
    assert body["presented"]["owner_gate"]["gate_type"] == "implementation_authorization"
    assert body["presented"]["owner_gate"]["status"] == "pending"
    assert body["presented"]["plan_revision_id"] == str(_PLAN_REVISION_ID)
    assert body["presented"]["canonical_branch"] is None
    assert body["presented"]["verified_head_sha"] is None
    # The re-presented gate is the committed handoff — no continuation
    # submission exists for the Ignore command.
    assert submitter.invocations == []
    # The exact addressed evidence reaches the service with the GitHub App
    # boundary composed per request.
    (call,) = seam["ignores"]
    assert call["github"] is seam["github_sentinel"]
    assert call["profile_id"] == _OWNER_PROFILE_ID
    assert call["workspace_id"] == _WORKSPACE_ID
    assert call["task_id"] == _TASK_ID
    assert call["owner_gate_id"] == _GATE_ID
    assert call["expected_state_token"] == _STATE_TOKEN


def test_the_ignore_command_projects_the_pr_boundary_presentation(
    exceptional_seam, settings_factory, owner_profile
) -> None:
    # The interrupted E7 PR-authorization boundary: the presented gate's
    # exact subject is the verified canonical branch and the GitHub-observed
    # head SHA the interrupted presentation binds — no plan subject.
    exceptional_seam(
        ignore_result=_ignore_outcome(boundary=IgnoredSourceChangeBoundary.PR_AUTHORIZATION)
    )
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_gate_path("ignore-source-change"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["boundary"] == "pr_authorization"
    assert body["presented"]["owner_gate"]["gate_type"] == "pr_authorization"
    assert body["presented"]["plan_revision_id"] is None
    assert body["presented"]["canonical_branch"] == "feature/owned-branch"
    assert body["presented"]["verified_head_sha"] == _HEAD_SHA


# --- The gate-scoped Continue command --------------------------------------------


def test_the_continue_command_projects_the_committed_allowance_and_delivery_evidence(
    exceptional_seam, settings_factory, owner_profile
) -> None:
    # The committed authorization restores the pre-block status under the
    # rotated token with the refreshed allowance, then arranges the one-shot
    # session-resume continuation through the real shared helper: the exact
    # typed invocation carries the record identity, the restored status, the
    # rotated token, and the requesting Owner Profile — never the routing.
    submitter = _RecordingSubmitter()
    seam = exceptional_seam(continue_result=_continue_outcome())
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("continue-cost-limit"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "implementing"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["owner_gate"]["gate_type"] == "cost_limit"
    assert body["owner_gate"]["status"] == "approved"
    assert body["task_block"]["reason"] == "cost_limit"
    assert body["effective_cost_threshold_usd"] == "22.00"
    assert body["continuation_outcome"] == "accepted_for_delivery"
    # The real shared continuation helper composed the exact typed
    # invocation through the process submitter.
    assert len(submitter.invocations) == 1
    invocation = submitter.invocations[0]
    assert invocation.kind is OperationKind.TASK_SESSION_RESUME
    assert invocation.payload.profile_id == str(_OWNER_PROFILE_ID)
    assert invocation.payload.task_resume_intent_id == str(_RESUME_ID)
    assert invocation.payload.restored_status == "implementing"
    assert invocation.payload.expected_state_token == str(_ROTATED_STATE_TOKEN)
    assert invocation.payload.task.workspace_id == str(_WORKSPACE_ID)
    assert invocation.payload.task.task_id == str(_TASK_ID)
    assert invocation.payload.task.role == "producer"
    # The exact addressed evidence reaches the service.
    (call,) = seam["continues"]
    assert call["profile_id"] == _OWNER_PROFILE_ID
    assert call["owner_gate_id"] == _GATE_ID
    assert call["expected_state_token"] == _STATE_TOKEN


@pytest.mark.parametrize(
    ("submission_result", "expected_wire"),
    [
        (
            SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE),
            "known_refused",
        ),
        (SubmissionResult.uncertain(), "uncertain"),
    ],
)
def test_a_refused_or_uncertain_continue_submission_never_fails_the_committed_authorization(
    exceptional_seam, settings_factory, owner_profile, submission_result, expected_wire
) -> None:
    # The Continue authorization is already committed when the continuation
    # is arranged: a refused or uncertain submission is separately-typed
    # delivery evidence, never an HTTP failure and never a reversal — the
    # later operation settlement belongs to the G2b reads.
    submitter = _RecordingSubmitter(submission_result)
    exceptional_seam(continue_result=_continue_outcome())
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("continue-cost-limit"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "implementing"
    assert body["continuation_outcome"] == expected_wire


# --- The gate-scoped Resume command ----------------------------------------------


def test_the_resume_command_projects_the_committed_authorization_and_delivery_evidence(
    exceptional_seam, settings_factory, owner_profile
) -> None:
    # The committed authorization restores the pre-abort status under the
    # rotated token, then arranges the one-shot original-session continuation
    # through the real shared helper. The settled gate and block stay
    # resolved history: HTTP success certifies the committed facts and the
    # acknowledged delivery only — never settlement and never a model result.
    submitter = _RecordingSubmitter()
    seam = exceptional_seam(resume_result=_resume_outcome())
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("resume-abort"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "implementing"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["owner_gate"]["gate_type"] == "owner_abort"
    assert body["owner_gate"]["status"] == "approved"
    assert body["task_block"]["reason"] == "owner_action_required"
    assert body["continuation_outcome"] == "accepted_for_delivery"
    assert len(submitter.invocations) == 1
    invocation = submitter.invocations[0]
    assert invocation.kind is OperationKind.TASK_SESSION_RESUME
    assert invocation.payload.profile_id == str(_OWNER_PROFILE_ID)
    assert invocation.payload.task_resume_intent_id == str(_RESUME_ID)
    assert invocation.payload.restored_status == "implementing"
    assert invocation.payload.expected_state_token == str(_ROTATED_STATE_TOKEN)
    assert invocation.payload.task.task_id == str(_TASK_ID)
    assert invocation.payload.task.role == "producer"
    (call,) = seam["resumes"]
    assert call["profile_id"] == _OWNER_PROFILE_ID
    assert call["owner_gate_id"] == _GATE_ID
    assert call["expected_state_token"] == _STATE_TOKEN


@pytest.mark.parametrize(
    ("submission_result", "expected_wire"),
    [
        (
            SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE),
            "known_refused",
        ),
        (SubmissionResult.uncertain(), "uncertain"),
    ],
)
def test_a_refused_or_uncertain_resume_submission_never_fails_the_committed_authorization(
    exceptional_seam, settings_factory, owner_profile, submission_result, expected_wire
) -> None:
    # The Resume authorization is already committed when the continuation is
    # arranged: the original gate and block remain resolved history whatever
    # later happens to the runtime continuation.
    submitter = _RecordingSubmitter(submission_result)
    exceptional_seam(resume_result=_resume_outcome())
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("resume-abort"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["owner_gate"]["status"] == "approved"
    assert body["continuation_outcome"] == expected_wire


# --- The uniform typed-error transport mapping ------------------------------------


@pytest.mark.parametrize(
    ("seam_error", "expected_status"),
    [
        # An action addressed at a gate whose fixed menu does not offer it
        # (e.g. resume against a COST_LIMIT gate) and an unsettled-abort
        # episode are typed conflicts — never invented actions and never
        # menu re-derivations.
        (ConflictError("the addressed owner gate does not offer this action"), 409),
        # A stale token or a moved-on gate refuses the whole action.
        (StaleOperationError("the exceptional action is stale"), 409),
        # A foreign or missing Workspace/Task/gate subject is uniform
        # not-found isolation: another Workspace or Profile learns nothing.
        (NotFoundError("uniform not-found"), 404),
        # An unconfigured boundary refuses the command before any effect.
        (IntegrationNotConfiguredError("unconfigured boundary"), 503),
    ],
)
def test_the_exceptional_commands_map_the_typed_error_vocabulary(
    exceptional_seam, settings_factory, owner_profile, seam_error, expected_status
) -> None:
    # Every command delegates its gate-menu, currentness, episode, and
    # authority checks to its service; nothing is dispatched and nothing is
    # applied when the service refuses.
    exceptional_seam(ignore_error=seam_error, continue_error=seam_error, resume_error=seam_error)
    submitter = _RecordingSubmitter()
    client = _client(settings_factory(), owner_profile, submitter)

    for action in ("ignore-source-change", "continue-cost-limit", "resume-abort"):
        response = client.post(_gate_path(action), json=_COMMAND_BODY)
        assert response.status_code == expected_status

    assert submitter.invocations == []


def test_an_unconfigured_github_identity_fails_closed(
    exceptional_seam, settings_factory, owner_profile
) -> None:
    # The Ignore command composes the GitHub App boundary itself: an
    # unconfigured deployment refuses the command at the boundary
    # composition — before the service ever runs.
    exceptional_seam(real_github_composition=True)
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_gate_path("ignore-source-change"), json=_COMMAND_BODY)

    assert response.status_code == 503


def test_a_malformed_command_body_is_the_framework_validation_error(
    exceptional_seam, settings_factory, owner_profile
) -> None:
    exceptional_seam()
    client = _client(settings_factory(), owner_profile, _RecordingSubmitter())

    response = client.post(_gate_path("resume-abort"), json={})

    assert response.status_code == 422
