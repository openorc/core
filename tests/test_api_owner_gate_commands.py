"""Transport tests for the ordinary OwnerGate decision commands (issue #379).

Route-level coverage of the HTTP mapping: the implementation-authorization
and PR-authorization approvals project the service's authoritative post-write
snapshot plus the separately-typed E5→E6 / E7→E8 continuation delivery
evidence (a refused or uncertain continuation never fails the committed
approval), and the merge decision maps the service's classified outcome
kinds verbatim — including the merge-confirmation-versus-archival
distinction (a confirmed merge with a committed terminalization and a still
``null`` ``archived_at``, and a reconciliation-confirmed completion without
a merge commit SHA) and the committed blocked/retry dispositions. The
uniform typed-error outcomes (disallowed gate, stale token, uniform
not-found isolation, unconfigured user-flow credentials, malformed body)
round out the surface. The command services' own authorization, subject,
and outcome matrices are owned by the #80/#82/#63/#339/F service suites —
never repeated here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.dependencies.github_user_flow import resolve_profile_user_token_resolver
from openorc.api.routers import tasks as tasks_router
from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import (
    OperationalFailureAttempt,
    OperationalFailureCode,
    OperationalFailureSourceKind,
)
from openorc.domain.ownership import Profile
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.tasks import Task, TaskStatus, TaskTerminalizationOrigin
from openorc.services import (
    task_implementation_authorization,
    task_merge_lifecycle,
    task_pr_authorization,
)
from openorc.services.errors import (
    ConflictError,
    IntegrationNotConfiguredError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_user_authorization import GitHubUserAccessTokenResolver
from openorc.services.task_merge_lifecycle import TaskMergeOutcome, TaskMergeOutcomeKind
from openorc.services.task_pr_authorization import ApprovedPrAuthorization
from openorc.workers.invocation import (
    OperationKind,
    SubmissionRefusalReason,
    SubmissionResult,
)
from openorc.workers.task_cleanup_operations import F1TerminalFinalizationSubmission
from openorc.workers.task_workflow import F1AuthorizedRetrySubmission

_OBSERVED = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
_DUE_AT = datetime(2026, 11, 25, 12, 4, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_REPOSITORY_ID = uuid4()
_TASK_ID = uuid4()
_GATE_ID = uuid4()
_PLAN_REVISION_ID = uuid4()
_PR_RECORD_ID = uuid4()
_BLOCK_ID = uuid4()
_ATTEMPT_ID = uuid4()
_EPISODE_ID = uuid4()
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_GITHUB_PR_ID = 901
_PR_NUMBER = 7
_STATE_TOKEN = uuid4()
_ROTATED_STATE_TOKEN = uuid4()
_HEAD_SHA = "b" * 40
_MERGE_COMMIT_SHA = "c" * 40
_FINGERPRINT = "a" * 64


def _task(
    *,
    status: TaskStatus,
    archived_at: datetime | None = None,
    terminalization: TaskTerminalizationOrigin | None = None,
) -> Task:
    """One Task snapshot as the decision services authoritatively return it."""
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        github_issue_number=_ISSUE_NUMBER,
        status=status,
        archived_at=archived_at,
        queued_at=None,
        canonical_feature_branch=None,
        state_token=_ROTATED_STATE_TOKEN,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint=_FINGERPRINT,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        authorized_task_budget_usd=None,
        terminalization_outcome=(TaskStatus.COMPLETED if terminalization is not None else None),
        terminalization_origin=terminalization,
        terminalization_established_at=(_OBSERVED if terminalization is not None else None),
    )


def _gate(
    *,
    gate_type: OwnerGateType,
    status: OwnerGateStatus,
    decided_at: datetime | None = None,
    plan_revision_id: UUID | None = None,
    subject_head_sha: str | None = None,
    task_pull_request_id: UUID | None = None,
) -> OwnerGate:
    """One service-returned OwnerGate with its exact bound subject form."""
    return OwnerGate(
        id=_GATE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=gate_type,
        status=status,
        plan_revision_id=plan_revision_id,
        subject_head_sha=subject_head_sha,
        task_pull_request_id=task_pull_request_id,
        task_block_id=None,
        decided_at=decided_at,
        created_at=_OBSERVED,
    )


def _pull_request(*, merged_at: datetime | None = None) -> TaskPullRequest:
    """One authoritative reconciled canonical TaskPullRequest."""
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
        state=TaskPullRequestState.CLOSED if merged_at is not None else TaskPullRequestState.OPEN,
        merged_at=merged_at,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _block(reason: TaskBlockReason) -> TaskBlock:
    """One applied blocking condition as the merge service returns it."""
    return TaskBlock(
        id=_BLOCK_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        reason=reason,
        context={"subject": "merge-request"},
        resolved_at=None,
        created_at=_OBSERVED,
    )


def _retry_attempt() -> OperationalFailureAttempt:
    """One durably authorized bounded merge retry attempt (F4/F5d facts)."""
    return OperationalFailureAttempt(
        id=_ATTEMPT_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        episode_id=_EPISODE_ID,
        ordinal=1,
        source_kind=OperationalFailureSourceKind.ORIGINAL,
        source_attempt_id=_GATE_ID,
        error_code=OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE,
        not_before=_DUE_AT,
        resulting_execution_id=None,
        consumed_at=None,
        created_at=_OBSERVED,
    )


class _RecordingSubmitter:
    """One process F1 submitter double recording submitted invocations."""

    def __init__(self, result: SubmissionResult) -> None:
        self._result = result
        self.invocations: list[Any] = []

    def submit(self, invocation: Any) -> SubmissionResult:
        self.invocations.append(invocation)
        return self._result


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid4(), created_at=_OBSERVED)


def _client(
    settings: Any,
    owner_profile: Profile,
    submitter: _RecordingSubmitter | None = None,
) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    app.state.operation_submitter = (
        submitter if submitter is not None else _RecordingSubmitter(SubmissionResult.accepted())
    )
    return TestClient(app)


@pytest.fixture
def decision_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the router module's service boundaries with recorded doubles.

    The E5→E6 / E7→E8 continuation helpers are deliberately left real: the
    router must compose the exact typed continuations through the process
    submitter, which the tests inject as a recording double.
    """

    def _make(
        *,
        implementation_result: Any = None,
        implementation_error: Exception | None = None,
        pr_result: Any = None,
        pr_error: Exception | None = None,
        merge_result: Any = None,
        merge_error: Exception | None = None,
        real_user_token_resolver: bool = False,
    ) -> dict[str, Any]:
        seam: dict[str, Any] = {
            "approvals": [],
            "pr_approvals": [],
            "merges": [],
        }
        github_sentinel = object()
        user_token_sentinel = object()

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _fake_github(settings: Any) -> object:
            return github_sentinel

        def _fake_user_token_resolver(settings: Any) -> object:
            return user_token_sentinel

        def _approve(pool: Any, **command: Any) -> Any:
            seam["approvals"].append(command)
            if implementation_error is not None:
                raise implementation_error
            return implementation_result

        def _approve_pr(pool: Any, **command: Any) -> Any:
            seam["pr_approvals"].append(command)
            if pr_error is not None:
                raise pr_error
            return pr_result

        def _merge(pool: Any, github: Any, user_token_resolver: Any, **command: Any) -> Any:
            seam["merges"].append(
                {"github": github, "user_token_resolver": user_token_resolver, **command}
            )
            if merge_error is not None:
                raise merge_error
            return merge_result

        monkeypatch.setattr(tasks_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(tasks_router, "resolve_github_app_client", _fake_github)
        if not real_user_token_resolver:
            monkeypatch.setattr(
                tasks_router, "resolve_profile_user_token_resolver", _fake_user_token_resolver
            )
        monkeypatch.setattr(
            task_implementation_authorization, "approve_implementation_authorization", _approve
        )
        monkeypatch.setattr(task_pr_authorization, "approve_pr_authorization", _approve_pr)
        monkeypatch.setattr(task_merge_lifecycle, "request_task_merge", _merge)
        seam["github_sentinel"] = github_sentinel
        seam["user_token_sentinel"] = user_token_sentinel
        return seam

    return _make


def _gate_path(action: str) -> str:
    return f"/api/v1/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/owner-gates/{_GATE_ID}/{action}"


_COMMAND_BODY = {"state_token": str(_STATE_TOKEN)}


# --- The implementation-authorization approval (E5 → E6) ----------------------


def test_the_implementation_authorization_approve_maps_the_committed_decision_and_continuation(
    decision_seam, settings_factory, owner_profile
) -> None:
    submitter = _RecordingSubmitter(SubmissionResult.accepted())
    seam = decision_seam(
        implementation_result=SimpleNamespace(
            task=_task(status=TaskStatus.IMPLEMENTING),
            owner_gate=_gate(
                gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
                status=OwnerGateStatus.APPROVED,
                decided_at=_OBSERVED,
                plan_revision_id=_PLAN_REVISION_ID,
            ),
        )
    )
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("approve-implementation-authorization"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    # The authoritative post-write snapshot, verbatim: implementing under the
    # rotated token, nothing archived.
    assert body["task"]["task_id"] == str(_TASK_ID)
    assert body["task"]["status"] == "implementing"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["task"]["archived_at"] is None
    assert body["owner_gate"]["id"] == str(_GATE_ID)
    assert body["owner_gate"]["gate_type"] == "implementation_authorization"
    assert body["owner_gate"]["status"] == "approved"
    assert body["owner_gate"]["decided_at"] is not None
    assert body["owner_gate"]["plan_revision_id"] == str(_PLAN_REVISION_ID)
    assert body["owner_gate"]["subject_head_sha"] is None
    assert body["continuation_outcome"] == "accepted_for_delivery"
    # The service received the exact command inputs.
    assert seam["approvals"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "owner_gate_id": _GATE_ID,
            "expected_state_token": _STATE_TOKEN,
        }
    ]
    # The real E5→E6 continuation helper composed the exact typed invocation
    # through the process submitter: the post-write Task's rotated token and
    # the approving Profile are carried; the gate is re-derived, never sent.
    assert len(submitter.invocations) == 1
    invocation = submitter.invocations[0]
    assert invocation.kind is OperationKind.TASK_AUTHORIZED_IMPLEMENTATION
    assert invocation.payload.profile_id == str(owner_profile.id)
    assert invocation.payload.expected_state_token == str(_ROTATED_STATE_TOKEN)
    assert invocation.payload.task.workspace_id == str(_WORKSPACE_ID)
    assert invocation.payload.task.task_id == str(_TASK_ID)
    assert invocation.payload.task.role == "producer"


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
def test_a_refused_or_uncertain_continuation_never_fails_the_committed_approval(
    decision_seam, settings_factory, owner_profile, submission_result, expected_wire
) -> None:
    # The approval is already committed when the continuation is arranged: a
    # refused or uncertain submission is separately-typed delivery evidence,
    # never an HTTP failure and never a reversal.
    submitter = _RecordingSubmitter(submission_result)
    decision_seam(
        implementation_result=SimpleNamespace(
            task=_task(status=TaskStatus.IMPLEMENTING),
            owner_gate=_gate(
                gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
                status=OwnerGateStatus.APPROVED,
                decided_at=_OBSERVED,
                plan_revision_id=_PLAN_REVISION_ID,
            ),
        )
    )
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("approve-implementation-authorization"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["task"]["status"] == "implementing"
    assert body["continuation_outcome"] == expected_wire
    assert len(submitter.invocations) == 1


# --- The PR-authorization approval (E7 → E8) ----------------------------------


def test_the_pr_authorization_approve_keeps_the_task_waiting_for_owner(
    decision_seam, settings_factory, owner_profile
) -> None:
    submitter = _RecordingSubmitter(SubmissionResult.accepted())
    seam = decision_seam(
        pr_result=ApprovedPrAuthorization(
            task=_task(status=TaskStatus.WAITING_FOR_OWNER),
            owner_gate=_gate(
                gate_type=OwnerGateType.PR_AUTHORIZATION,
                status=OwnerGateStatus.APPROVED,
                decided_at=_OBSERVED,
                subject_head_sha=_HEAD_SHA,
            ),
        )
    )
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("approve-pr-authorization"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    # The approval is neither a transition nor a PR creation: the Task stays
    # waiting_for_owner under its rotated token.
    assert body["task"]["status"] == "waiting_for_owner"
    assert body["task"]["state_token"] == str(_ROTATED_STATE_TOKEN)
    assert body["task"]["archived_at"] is None
    assert body["owner_gate"]["gate_type"] == "pr_authorization"
    assert body["owner_gate"]["status"] == "approved"
    assert body["owner_gate"]["subject_head_sha"] == _HEAD_SHA
    assert body["owner_gate"]["plan_revision_id"] is None
    assert body["owner_gate"]["task_pull_request_id"] is None
    assert body["continuation_outcome"] == "accepted_for_delivery"
    assert seam["pr_approvals"] == [
        {
            "profile_id": owner_profile.id,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "owner_gate_id": _GATE_ID,
            "expected_state_token": _STATE_TOKEN,
        }
    ]
    # The real E7→E8 continuation helper composed the exact typed invocation
    # addressed to the approved PR_AUTHORIZATION gate.
    assert len(submitter.invocations) == 1
    invocation = submitter.invocations[0]
    assert invocation.kind is OperationKind.TASK_PR_COMPOSITION
    assert invocation.payload.profile_id == str(owner_profile.id)
    assert invocation.payload.owner_gate_id == str(_GATE_ID)
    assert invocation.payload.expected_state_token == str(_ROTATED_STATE_TOKEN)
    assert invocation.payload.task.task_id == str(_TASK_ID)
    assert invocation.payload.task.role == "producer"


# --- The merge decision command (E10) -----------------------------------------


def test_the_merge_decision_approve_maps_the_direct_send_completion(
    decision_seam, settings_factory, owner_profile
) -> None:
    submitter = _RecordingSubmitter(SubmissionResult.accepted())
    seam = decision_seam(
        merge_result=TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=_task(
                status=TaskStatus.COMPLETED,
                archived_at=_OBSERVED,
                terminalization=TaskTerminalizationOrigin.GITHUB_CONFIRMED_MERGED,
            ),
            pull_request=_pull_request(merged_at=_OBSERVED),
            merge_commit_sha=_MERGE_COMMIT_SHA,
        )
    )
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_gate_path("approve-merge-decision"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "completed"
    assert body["task"]["status"] == "completed"
    assert body["task"]["archived_at"] is not None
    assert body["terminalization"]["outcome"] == "completed"
    assert body["terminalization"]["origin"] == "github_confirmed_merged"
    assert body["terminalization"]["established_at"] is not None
    # The reconciled canonical PR the classification is grounded in.
    assert body["pull_request"]["id"] == _GITHUB_PR_ID
    assert body["pull_request"]["number"] == _PR_NUMBER
    assert body["pull_request"]["state"] == "closed"
    assert body["pull_request"]["head"]["sha"] == _HEAD_SHA
    assert body["pull_request"]["merged_at"] is not None
    # The merge send confirmed the commit itself.
    assert body["merge_commit_sha"] == _MERGE_COMMIT_SHA
    assert body["retry_gate"] is None
    assert body["task_block"] is None
    assert body["retry_attempt"] is None
    # The ONE Owner merge command received both external boundaries, the
    # exact command inputs, and the composed F10/F12 submission seams.
    recorded = seam["merges"][0]
    assert recorded["github"] is seam["github_sentinel"]
    assert recorded["user_token_resolver"] is seam["user_token_sentinel"]
    assert recorded["profile_id"] == owner_profile.id
    assert recorded["workspace_id"] == _WORKSPACE_ID
    assert recorded["task_id"] == _TASK_ID
    assert recorded["owner_gate_id"] == _GATE_ID
    assert recorded["expected_state_token"] == _STATE_TOKEN
    assert isinstance(
        recorded["terminal_finalization_submission"], F1TerminalFinalizationSubmission
    )
    assert isinstance(recorded["retry_submission"], F1AuthorizedRetrySubmission)
    # No continuation is arranged behind a merge command: the merge is the
    # synchronous external effect, the F10/F12 seams belong to the service.
    assert submitter.invocations == []


def test_a_reconciliation_confirmed_completion_never_claims_sha_or_archival(
    decision_seam, settings_factory, owner_profile
) -> None:
    # A confirmed merge established through authoritative reconciliation
    # (e.g. the PR was merged externally before the command ran) commits the
    # terminalization while the mandatory archival stays pending with the F10
    # finalizer: the response preserves the Task's actual pre-archival
    # status, the null archived_at, the committed terminalization facts, and
    # the absent merge commit SHA — never a synthesized SHA, never a claimed
    # archival.
    decision_seam(
        merge_result=TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=_task(
                status=TaskStatus.WAITING_FOR_OWNER,
                archived_at=None,
                terminalization=TaskTerminalizationOrigin.GITHUB_CONFIRMED_MERGED,
            ),
            pull_request=_pull_request(merged_at=_OBSERVED),
            merge_commit_sha=None,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(_gate_path("approve-merge-decision"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "completed"
    assert body["task"]["status"] == "waiting_for_owner"
    assert body["task"]["archived_at"] is None
    assert body["terminalization"]["outcome"] == "completed"
    assert body["terminalization"]["origin"] == "github_confirmed_merged"
    assert body["merge_commit_sha"] is None


def test_a_definitive_no_merge_outcome_maps_the_new_retry_gate(
    decision_seam, settings_factory, owner_profile
) -> None:
    # A definitive no-merge classification installed the NEW pending
    # exact-head MERGE_DECISION gate: fresh Owner authorization is required,
    # and the response projects that gate's pending subject facts verbatim.
    decision_seam(
        merge_result=TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.RETRY_GATE_INSTALLED,
            task=_task(status=TaskStatus.WAITING_FOR_OWNER),
            pull_request=_pull_request(),
            retry_gate=_gate(
                gate_type=OwnerGateType.MERGE_DECISION,
                status=OwnerGateStatus.PENDING,
                subject_head_sha=_HEAD_SHA,
                task_pull_request_id=_PR_RECORD_ID,
            ),
            merge_commit_sha=None,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(_gate_path("approve-merge-decision"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "retry_gate_installed"
    assert body["task"]["status"] == "waiting_for_owner"
    assert body["retry_gate"]["gate_type"] == "merge_decision"
    assert body["retry_gate"]["status"] == "pending"
    assert body["retry_gate"]["subject_head_sha"] == _HEAD_SHA
    assert body["retry_gate"]["task_pull_request_id"] == str(_PR_RECORD_ID)
    assert body["merge_commit_sha"] is None


def test_an_authorized_merge_retry_maps_the_committed_attempt(
    decision_seam, settings_factory, owner_profile
) -> None:
    # The truthful async acceptance: the merge send's classified transient
    # transport failure received ONE durably authorized bounded retry. The
    # Task stays waiting_for_owner with no current gate and the due-time
    # wakeup belongs to the F12 machinery, never to this envelope.
    decision_seam(
        merge_result=TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.RETRY_AUTHORIZED,
            task=_task(status=TaskStatus.WAITING_FOR_OWNER),
            pull_request=_pull_request(),
            retry_attempt=_retry_attempt(),
            merge_commit_sha=None,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(_gate_path("approve-merge-decision"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "retry_authorized"
    assert body["task"]["status"] == "waiting_for_owner"
    assert body["retry_attempt"]["id"] == str(_ATTEMPT_ID)
    assert body["retry_attempt"]["ordinal"] == 1
    assert datetime.fromisoformat(body["retry_attempt"]["due"]) == _DUE_AT
    assert body["retry_attempt"]["error_code"] == "external_transport_unavailable"


def test_a_blocked_merge_outcome_maps_the_applied_block(
    decision_seam, settings_factory, owner_profile
) -> None:
    # The expected-head mismatch blocked the Task: a committed durable
    # disposition of a validly addressed command — a classified outcome,
    # never an HTTP error.
    decision_seam(
        merge_result=TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION,
            task=_task(status=TaskStatus.BLOCKED),
            pull_request=_pull_request(),
            task_block=_block(TaskBlockReason.STALE_OPERATION),
            merge_commit_sha=None,
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.post(_gate_path("approve-merge-decision"), json=_COMMAND_BODY)

    assert response.status_code == 200
    body = response.json()
    assert body["kind"] == "blocked_stale_operation"
    assert body["task"]["status"] == "blocked"
    assert body["task_block"]["id"] == str(_BLOCK_ID)
    assert body["task_block"]["reason"] == "stale_operation"


# --- The uniform typed-error transport mapping --------------------------------


@pytest.mark.parametrize(
    ("seam_error", "expected_status"),
    [
        (ConflictError("the addressed owner gate is not this command's gate"), 409),
        (StaleOperationError("the approval is stale"), 409),
        (NotFoundError("uniform not-found"), 404),
        (IntegrationNotConfiguredError("unconfigured boundary"), 503),
    ],
)
def test_the_decision_commands_map_the_typed_error_vocabulary(
    decision_seam, settings_factory, owner_profile, seam_error, expected_status
) -> None:
    # Every command delegates its gate-type, currentness, and authority
    # checks to its service: a disallowed gate (e.g. an approve addressed at
    # a REVIEW_RESOLUTION gate), a stale token, a foreign/missing subject,
    # and an unconfigured boundary are the typed HTTP outcomes — with the
    # merge's hard precondition failures mapped identically.
    decision_seam(implementation_error=seam_error, pr_error=seam_error, merge_error=seam_error)
    client = _client(settings_factory(), owner_profile)

    for action in (
        "approve-implementation-authorization",
        "approve-pr-authorization",
        "approve-merge-decision",
    ):
        response = client.post(_gate_path(action), json=_COMMAND_BODY)
        assert response.status_code == expected_status


def test_unconfigured_user_flow_credentials_fail_closed(
    decision_seam, settings_factory, owner_profile
) -> None:
    # The merge command composes the Profile user-token resolver itself: an
    # unconfigured deployment refuses the command at the boundary before the
    # service ever runs — and never fails on the unrelated #373 Admin
    # credentials.
    seam = decision_seam(real_user_token_resolver=True)
    client = _client(settings_factory(), owner_profile)

    response = client.post(_gate_path("approve-merge-decision"), json=_COMMAND_BODY)

    assert response.status_code == 503
    assert seam["merges"] == []


def test_a_malformed_command_body_is_the_framework_validation_error(
    decision_seam, settings_factory, owner_profile
) -> None:
    decision_seam()
    client = _client(settings_factory(), owner_profile)

    response = client.post(_gate_path("approve-implementation-authorization"), json={})

    assert response.status_code == 422


# --- The Profile user-token resolver composition ------------------------------


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
