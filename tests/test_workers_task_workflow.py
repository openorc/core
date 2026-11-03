"""Tests for the F5a Task-workflow operation composition (issue #253).

Deterministic and fake-backed: the per-kind compositions are exercised
through the single seam with recording service doubles, fake runtime
attachments, a recording continuation submitter, and scripted-pool reloads.
No live Postgres, Valkey, GitHub, or Cline is touched; the existing F1
decoder, local executor, and queue-submission matrices are reused rather
than re-run.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from openorc.config import ConfigurationError, ExecutionMode, Settings
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import StaleOperationError
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_planning_review_decision import PlanningReviewDecision
from openorc.services.task_pr_composition import TaskPrPublished
from openorc.services.task_pr_review_decision import PrReviewDecision
from openorc.services.task_start import TaskStartStatus
from openorc.workers import task_workflow
from openorc.workers.invocation import (
    ExecutionModeDependencyError,
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    OperationPayload,
    OperationSubmitter,
    RQQueueOperationSubmission,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
    TaskAuthorizedImplementationPayload,
    TaskAuthorizedImplementationRetryPayload,
    TaskAuthorizedMergeRetryPayload,
    TaskPlanningBootstrapAndInitialPlanPayload,
    TaskPlanningReviewPayload,
    TaskPrCompositionPayload,
    TaskPrReviewPayload,
    TaskScope,
    WorkflowRoleRef,
    build_default_operation_dependencies,
    decode_operation_invocation,
    execute_operation_job,
)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")
_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_STATE_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000004")
_NEXT_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000005")
_THIRD_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000006")
_OWNER_GATE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000007")
_PR_RECORD_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000008")
_EXECUTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000009")

_NEUTRAL_DIRECTORY = "/srv/neutral/review"
_CHECKOUT_DIRECTORY = "/srv/base/repo/checkout"
_PRODUCER_EXTERNAL_SESSION_ID = "producer-external-session"
_REVIEWER_EXTERNAL_SESSION_ID = "reviewer-external-session"

_TASK_SCOPE_PRODUCER = TaskScope(
    workspace_id=str(_WORKSPACE_ID), task_id=str(_TASK_ID), role=WorkflowRoleRef.PRODUCER
)
_TASK_SCOPE_REVIEWER = TaskScope(
    workspace_id=str(_WORKSPACE_ID), task_id=str(_TASK_ID), role=WorkflowRoleRef.REVIEWER
)


@dataclass
class RecordingService:
    """Recording service double standing in for one application service.

    ``result`` is returned on every call; ``results`` (when supplied) are
    returned in call order, repeating the last one once exhausted.
    """

    calls: list[dict[str, Any]] = field(default_factory=list)
    result: Any = None
    results: list[Any] | None = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # The F5b coordinator's attempt-evidence collector is coordinator
        # plumbing the real services accept optionally (issue #262); it is
        # accepted here and excluded from recorded comparisons — its
        # threading is covered by the dedicated coordinator tests.
        recorded_kwargs = {key: value for key, value in kwargs.items() if key != "attempt_evidence"}
        self.calls.append({"args": args, "kwargs": recorded_kwargs})
        if self.results is not None:
            index = min(len(self.calls) - 1, len(self.results) - 1)
            outcome = self.results[index]
        else:
            outcome = self.result
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    @property
    def call_count(self) -> int:
        return len(self.calls)


@dataclass
class FakeSubmitter:
    """Recording OperationSubmitter double with scripted outcomes."""

    outcomes: list[SubmissionResult] = field(default_factory=lambda: [SubmissionResult.accepted()])
    submissions: list[OperationInvocation] = field(default_factory=list)

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        self.submissions.append(invocation)
        index = min(len(self.submissions) - 1, len(self.outcomes) - 1)
        return self.outcomes[index]


@dataclass
class FakeAttachedRuntime:
    """Recorded attachment acquisitions and the yielded adapter double."""

    acquisitions: list[dict[str, Any]] = field(default_factory=list)
    adapter: Any = field(default_factory=object)


def make_dependencies(
    settings: Settings | None = None, **providers: Any
) -> tuple[OperationDependencies, FakeSubmitter]:
    """Build test dependencies with recording fakes and one recording submitter."""
    submitter = FakeSubmitter()
    resolved: dict[str, Any] = {
        "settings": settings
        or Settings(environment="test", api_host="127.0.0.1", api_port=3999, api_reload=False),
        "database_pool_provider": lambda _settings: object(),
        "github_client_provider": lambda _settings: object(),
        "bootstrap_runtime_composition_provider": lambda _settings: object(),
        "user_token_resolver_provider": lambda _settings: object(),
        "reviewer_neutral_directory_provider": lambda _settings: _NEUTRAL_DIRECTORY,
        "continuation_submitter_provider": lambda _settings: submitter,
    }
    resolved.update(providers)
    return OperationDependencies(**resolved), submitter


@contextmanager
def patched_workflow_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    prepare: RecordingService | None = None,
    produce: RecordingService | None = None,
    dispatch_planning: RecordingService | None = None,
    finalize_planning: RecordingService | None = None,
    open_implementation: RecordingService | None = None,
    execute_implementation: RecordingService | None = None,
    execute_implementation_retry: RecordingService | None = None,
    execute_merge_retry: RecordingService | None = None,
    open_pr: RecordingService | None = None,
    compose_pr: RecordingService | None = None,
    dispatch_pr: RecordingService | None = None,
    finalize_pr: RecordingService | None = None,
    reloaded_plan_production: Any = None,
    reloaded_published: Any = None,
    producer_checkout: str | None = None,
    bound_session_id: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Patch the composition module's seams with recording doubles.

    The attachment composition records every acquisition's exact identity
    arguments; the durable reloads return canned outcomes unless overridden;
    the session-identity helpers return canned durable facts.
    """
    attachments = FakeAttachedRuntime()

    @contextmanager
    def fake_attach_cline_runtime(
        _pool: Any,
        *,
        workspace_id: UUID,
        task_id: UUID,
        role: WorkflowRole,
        phase: Any,
        external_session_id: str | None = None,
        working_directory: str | None = None,
        **_kwargs: Any,
    ) -> Iterator[Any]:
        attachments.acquisitions.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "role": role,
                "phase": phase,
                "external_session_id": external_session_id,
                "working_directory": working_directory,
            }
        )
        yield attachments

    patches: dict[str, Any] = {
        "attach_cline_runtime": fake_attach_cline_runtime,
        "_bound_session_external_id": (
            lambda _pool, *, workspace_id, task_id, role: (
                bound_session_id or _REVIEWER_EXTERNAL_SESSION_ID
            )
        ),
        "_producer_checkout_directory": (
            lambda _pool, *, workspace_id, task_id: producer_checkout or _CHECKOUT_DIRECTORY
        ),
    }
    service_patches: dict[str, Any] = {
        "prepare_task_for_planning": prepare,
        "produce_initial_task_plan": produce,
        "dispatch_planning_review": dispatch_planning,
        "finalize_planning_review_decision": finalize_planning,
        "open_implementation_authorization": open_implementation,
        "execute_authorized_implementation": execute_implementation,
        "execute_authorized_implementation_retry": execute_implementation_retry,
        "execute_authorized_merge_retry": execute_merge_retry,
        "open_pr_authorization": open_pr,
        "compose_and_publish_task_pr": compose_pr,
        "dispatch_pr_review": dispatch_pr,
        "finalize_pr_review_decision": finalize_pr,
    }
    for name, service in service_patches.items():
        if service is not None:
            patches[name] = service
    if reloaded_plan_production is not None:
        patches["_reload_plan_production_outcome"] = (
            lambda _pool, *, workspace_id, task_id, expected_state_token: reloaded_plan_production
        )
    if reloaded_published is not None:
        patches["_reload_published_subjects"] = lambda _pool, **_kwargs: reloaded_published
    for name, replacement in patches.items():
        monkeypatch.setattr(task_workflow, name, replacement)
    yield {"attachments": attachments}


# --- canned outcome builders (composition-level fakes) -----------------------


def fake_preparation() -> Any:
    return SimpleNamespace(
        producer_session=SimpleNamespace(external_session_id=_PRODUCER_EXTERNAL_SESSION_ID),
        producer_checkout_directory=_CHECKOUT_DIRECTORY,
    )


def fake_production(state_token: UUID = _NEXT_TOKEN) -> Any:
    return SimpleNamespace(
        plan_revision=SimpleNamespace(id=_PR_RECORD_ID),
        task=SimpleNamespace(state_token=state_token),
    )


def fake_dispatch_outcome(state_token: UUID = _NEXT_TOKEN) -> Any:
    return SimpleNamespace(
        review_loop=SimpleNamespace(id=uuid4()),
        review_iteration=SimpleNamespace(id=uuid4()),
        review_result=SimpleNamespace(outcome="accepted"),
        reviewer_session=SimpleNamespace(external_session_id=_REVIEWER_EXTERNAL_SESSION_ID),
        task=SimpleNamespace(state_token=state_token),
        pull_request=SimpleNamespace(id=_PR_RECORD_ID),
        reconciled_head_sha="a" * 40,
    )


def fake_planning_decision(
    decision: Any, *, state_token: UUID = _NEXT_TOKEN, revision_production: Any = None
) -> Any:
    return SimpleNamespace(
        decision=decision,
        task=SimpleNamespace(state_token=state_token),
        revision_production=revision_production,
    )


def fake_published(state_token: UUID = _NEXT_TOKEN) -> Any:
    return SimpleNamespace(
        task=SimpleNamespace(state_token=state_token),
        pull_request=SimpleNamespace(id=_PR_RECORD_ID),
    )


def fake_execution(state_token: UUID = _NEXT_TOKEN) -> Any:
    return SimpleNamespace(
        task=SimpleNamespace(state_token=state_token),
        execution=SimpleNamespace(id=_EXECUTION_ID),
    )


def fake_remediation(dispatch_outcome: Any) -> Any:
    return SimpleNamespace(execution=SimpleNamespace(id=_EXECUTION_ID), dispatch=dispatch_outcome)


def fake_pr_decision(remediation: Any) -> Any:
    return SimpleNamespace(decision=PrReviewDecision.REMEDIATION_ROUTED, remediation=remediation)


# --- wire vocabulary: closed kind/payload round-trips and strict decoding ----


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (
            OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
            TaskPlanningBootstrapAndInitialPlanPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
        (
            OperationKind.TASK_PLANNING_REVIEW,
            TaskPlanningReviewPayload(
                task=_TASK_SCOPE_REVIEWER,
                profile_id=str(_PROFILE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
        (
            OperationKind.TASK_AUTHORIZED_IMPLEMENTATION,
            TaskAuthorizedImplementationPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
        (
            OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY,
            TaskAuthorizedImplementationRetryPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                retry_attempt_id=str(_OWNER_GATE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
        (
            OperationKind.TASK_AUTHORIZED_MERGE_RETRY,
            TaskAuthorizedMergeRetryPayload(
                task=_TASK_SCOPE_REVIEWER,
                profile_id=str(_PROFILE_ID),
                retry_attempt_id=str(_OWNER_GATE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
        (
            OperationKind.TASK_PR_COMPOSITION,
            TaskPrCompositionPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                owner_gate_id=str(_OWNER_GATE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
        (
            OperationKind.TASK_PR_REVIEW,
            TaskPrReviewPayload(
                task=_TASK_SCOPE_REVIEWER,
                profile_id=str(_PROFILE_ID),
                task_pull_request_id=str(_PR_RECORD_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        ),
    ],
)
def test_task_payload_round_trips_exactly(kind: OperationKind, payload: OperationPayload) -> None:
    wire = OperationInvocation(kind=kind, payload=payload)
    encoded = wire.encode()

    decoded = decode_operation_invocation(encoded)

    assert decoded.kind is kind
    assert decoded.payload == payload


def test_task_payload_rejects_a_role_that_does_not_match_its_kind() -> None:
    raw = {
        "task": {
            "workspace_id": str(_WORKSPACE_ID),
            "task_id": str(_TASK_ID),
            "role": WorkflowRoleRef.REVIEWER.value,
        },
        "profile_id": str(_PROFILE_ID),
        "expected_state_token": str(_STATE_TOKEN),
    }

    with pytest.raises(InvalidInvocationError, match="role"):
        decode_operation_invocation({"kind": "task_authorized_implementation", "payload": raw})


def test_task_payload_rejects_unexpected_and_missing_fields() -> None:
    base = {
        "task": {
            "workspace_id": str(_WORKSPACE_ID),
            "task_id": str(_TASK_ID),
            "role": WorkflowRoleRef.PRODUCER.value,
        },
        "profile_id": str(_PROFILE_ID),
        "expected_state_token": str(_STATE_TOKEN),
    }
    with_extra = {**base, "prompt": "carry no prompts"}
    with pytest.raises(InvalidInvocationError, match="unexpected fields"):
        decode_operation_invocation(
            {"kind": "task_planning_bootstrap_and_initial_plan", "payload": with_extra}
        )
    missing_token = {key: value for key, value in base.items() if key != "expected_state_token"}
    with pytest.raises(InvalidInvocationError, match="missing required fields"):
        decode_operation_invocation(
            {"kind": "task_planning_bootstrap_and_initial_plan", "payload": missing_token}
        )


def test_task_payload_rejects_malformed_uuid_fields_before_any_effect() -> None:
    raw = {
        "task": {
            "workspace_id": str(_WORKSPACE_ID),
            "task_id": str(_TASK_ID),
            "role": WorkflowRoleRef.PRODUCER.value,
        },
        "profile_id": "not-a-uuid",
        "expected_state_token": str(_STATE_TOKEN),
    }

    with pytest.raises(InvalidInvocationError, match="profile_id"):
        decode_operation_invocation({"kind": "task_authorized_implementation", "payload": raw})


def test_authorized_implementation_retry_payload_decodes_strictly() -> None:
    base = {
        "task": {
            "workspace_id": str(_WORKSPACE_ID),
            "task_id": str(_TASK_ID),
            "role": WorkflowRoleRef.PRODUCER.value,
        },
        "profile_id": str(_PROFILE_ID),
        "retry_attempt_id": str(_OWNER_GATE_ID),
        "expected_state_token": str(_STATE_TOKEN),
    }
    # The strict decoder accepts exactly the retry payload's field set.
    decoded = decode_operation_invocation(
        {"kind": "task_authorized_implementation_retry", "payload": base}
    )
    assert decoded.kind is OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY
    retry_payload = cast(TaskAuthorizedImplementationRetryPayload, decoded.payload)
    assert retry_payload.retry_attempt_id == str(_OWNER_GATE_ID)
    # A `retryable`-style free authority flag is never carried: unexpected
    # fields are refused before any effect.
    with_extra = {**base, "retryable": True}
    with pytest.raises(InvalidInvocationError, match="unexpected fields"):
        decode_operation_invocation(
            {"kind": "task_authorized_implementation_retry", "payload": with_extra}
        )
    # A malformed retry-attempt UUID is rejected before any effect.
    malformed = {**base, "retry_attempt_id": "not-a-uuid"}
    with pytest.raises(InvalidInvocationError, match="retry_attempt_id"):
        decode_operation_invocation(
            {"kind": "task_authorized_implementation_retry", "payload": malformed}
        )
    # A Reviewer-role scope does not match the retry kind's consuming role.
    reviewer_role = {**base, "task": {**base["task"], "role": WorkflowRoleRef.REVIEWER.value}}
    with pytest.raises(InvalidInvocationError, match="role"):
        decode_operation_invocation(
            {"kind": "task_authorized_implementation_retry", "payload": reviewer_role}
        )


def test_task_invocation_rejects_a_payload_of_another_kind() -> None:
    with pytest.raises(InvalidInvocationError, match="does not match the operation kind"):
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_REVIEW,
            payload=TaskPrCompositionPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                owner_gate_id=str(_OWNER_GATE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        )


def test_decode_rejects_an_unknown_kind() -> None:
    with pytest.raises(InvalidInvocationError, match="kind"):
        decode_operation_invocation(
            json.dumps({"kind": "task_speculative_session_operation", "payload": {}})
        )


# --- per-kind composition through the single seam ----------------------------


def test_seam_dispatches_the_registered_composition(monkeypatch: pytest.MonkeyPatch) -> None:
    dependencies, submitter = make_dependencies()
    recorded: list[OperationInvocation] = []

    def composition(
        payload: OperationPayload,
        *,
        dependencies: OperationDependencies,
        attempt_evidence: InvocationAttemptEvidence | None = None,
    ) -> None:
        recorded.append(OperationInvocation(kind=OperationKind.TASK_PR_REVIEW, payload=payload))

    monkeypatch.setitem(
        task_workflow._TASK_WORKFLOW_COMPOSITIONS, OperationKind.TASK_PR_REVIEW, composition
    )
    payload = TaskPrReviewPayload(
        task=_TASK_SCOPE_REVIEWER,
        profile_id=str(_PROFILE_ID),
        task_pull_request_id=str(_PR_RECORD_ID),
        expected_state_token=str(_STATE_TOKEN),
    )

    task_workflow.execute_task_workflow_operation(
        OperationInvocation(kind=OperationKind.TASK_PR_REVIEW, payload=payload),
        dependencies=dependencies,
    )

    assert len(recorded) == 1
    assert recorded[0].payload == payload
    assert submitter.submissions == []


def test_planning_bootstrap_composition_chains_prepare_and_produce_and_submits_the_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    preparation = fake_preparation()
    production = fake_production()
    prepare = RecordingService(result=preparation)
    produce = RecordingService(result=production)
    with patched_workflow_environment(monkeypatch, prepare=prepare, produce=produce) as environment:
        payload = TaskPlanningBootstrapAndInitialPlanPayload(
            task=_TASK_SCOPE_PRODUCER,
            profile_id=str(_PROFILE_ID),
            expected_state_token=str(_STATE_TOKEN),
        )

        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN, payload=payload
            ),
            dependencies=dependencies,
        )

        # The exact bootstrap subjects and the deployment's reviewer-neutral
        # directory reached the E1b service.
        assert prepare.call_count == 1
        assert prepare.calls[0]["kwargs"] == {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "profile_id": _PROFILE_ID,
            "expected_state_token": _STATE_TOKEN,
            "reviewer_neutral_directory": _NEUTRAL_DIRECTORY,
        }
        # The initial PLAN was produced with the same logical invocation's
        # preparation outcome (never re-derived, never serialized).
        assert produce.call_count == 1
        assert len(produce.calls[0]["args"]) == 3
        assert produce.calls[0]["kwargs"]["preparation"] is preparation
        # The BOUND Producer attachment used the preparation's exact session
        # identity and checkout directory.
        producer_acquisitions = [
            acquisition
            for acquisition in environment["attachments"].acquisitions
            if acquisition["role"] is WorkflowRole.PRODUCER
        ]
        assert len(producer_acquisitions) == 1
        assert producer_acquisitions[0]["phase"] is task_workflow.ClineAttachmentPhase.BOUND
        assert producer_acquisitions[0]["external_session_id"] == _PRODUCER_EXTERNAL_SESSION_ID
        assert producer_acquisitions[0]["working_directory"] == _CHECKOUT_DIRECTORY
        # The exact post-E2 token authorizes the first review round.
        assert len(submitter.submissions) == 1
        submitted = submitter.submissions[0]
        assert submitted.kind is OperationKind.TASK_PLANNING_REVIEW
        assert isinstance(submitted.payload, TaskPlanningReviewPayload)
        assert submitted.payload.task == _TASK_SCOPE_REVIEWER
        assert submitted.payload.profile_id == str(_PROFILE_ID)
        assert submitted.payload.expected_state_token == str(production.task.state_token)


def test_planning_review_composition_runs_dispatch_and_decision_in_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    production = fake_production(_STATE_TOKEN)
    routed_production = fake_production(_NEXT_TOKEN)
    dispatch = RecordingService(result=fake_dispatch_outcome())
    finalize = RecordingService(
        result=fake_planning_decision(
            PlanningReviewDecision.REVISION_ROUTED, revision_production=routed_production
        )
    )
    with patched_workflow_environment(
        monkeypatch,
        dispatch_planning=dispatch,
        finalize_planning=finalize,
        reloaded_plan_production=production,
        bound_session_id=_REVIEWER_EXTERNAL_SESSION_ID,
    ) as environment:
        payload = TaskPlanningReviewPayload(
            task=_TASK_SCOPE_REVIEWER,
            profile_id=str(_PROFILE_ID),
            expected_state_token=str(_STATE_TOKEN),
        )

        task_workflow.execute_task_workflow_operation(
            OperationInvocation(kind=OperationKind.TASK_PLANNING_REVIEW, payload=payload),
            dependencies=dependencies,
        )

        # The reloaded production subject reached the dispatch service.
        assert dispatch.call_count == 1
        assert dispatch.calls[0]["kwargs"]["plan_production"] is production
        assert dispatch.calls[0]["kwargs"]["profile_id"] == _PROFILE_ID
        # The dispatch attachment was the BOUND Reviewer at the neutral
        # directory; the decision attachment was the BOUND Producer at the
        # prepared checkout (the merged E2 revise continues there).
        assert environment["attachments"].acquisitions[0]["role"] is WorkflowRole.REVIEWER
        assert environment["attachments"].acquisitions[0]["working_directory"] == _NEUTRAL_DIRECTORY
        assert environment["attachments"].acquisitions[1]["role"] is WorkflowRole.PRODUCER
        assert (
            environment["attachments"].acquisitions[1]["working_directory"] == _CHECKOUT_DIRECTORY
        )
        assert finalize.calls[0]["kwargs"]["expected_state_token"] == production.task.state_token
        # The next review round is authorized by the fresh post-revise token.
        assert len(submitter.submissions) == 1
        submitted = submitter.submissions[0]
        assert submitted.kind is OperationKind.TASK_PLANNING_REVIEW
        assert isinstance(submitted.payload, TaskPlanningReviewPayload)
        assert submitted.payload.expected_state_token == str(routed_production.task.state_token)


def test_planning_review_acceptance_presents_the_implementation_gate_without_approving_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    decision_token = _NEXT_TOKEN
    open_implementation = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_THIRD_TOKEN))
    )
    finalize = RecordingService(
        result=fake_planning_decision(
            PlanningReviewDecision.ACCEPTED_CLEAR, state_token=decision_token
        )
    )
    with patched_workflow_environment(
        monkeypatch,
        dispatch_planning=RecordingService(result=fake_dispatch_outcome()),
        finalize_planning=finalize,
        open_implementation=open_implementation,
        reloaded_plan_production=fake_production(_STATE_TOKEN),
    ):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_PLANNING_REVIEW,
                payload=TaskPlanningReviewPayload(
                    task=_TASK_SCOPE_REVIEWER,
                    profile_id=str(_PROFILE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        # The exact-plan IMPLEMENTATION_AUTHORIZATION gate was presented with
        # the exact post-decision token — presenting, never approving.
        assert open_implementation.call_count == 1
        assert open_implementation.calls[0]["kwargs"] == {
            "profile_id": _PROFILE_ID,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "expected_state_token": decision_token,
        }
        # Acceptance launches no implementation operation and no Owner
        # approval is ever inferred.
        assert submitter.submissions == []


def test_planning_review_exhaustion_ends_at_the_resolution_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    finalize = RecordingService(
        result=fake_planning_decision(PlanningReviewDecision.EXHAUSTED, state_token=_NEXT_TOKEN)
    )
    with patched_workflow_environment(
        monkeypatch,
        dispatch_planning=RecordingService(result=fake_dispatch_outcome()),
        finalize_planning=finalize,
        reloaded_plan_production=fake_production(_STATE_TOKEN),
    ):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_PLANNING_REVIEW,
                payload=TaskPlanningReviewPayload(
                    task=_TASK_SCOPE_REVIEWER,
                    profile_id=str(_PROFILE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        assert finalize.call_count == 1
        assert submitter.submissions == []


def test_authorized_implementation_composition_presents_the_pr_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    execution = fake_execution()
    execute = RecordingService(result=execution)
    open_pr = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_THIRD_TOKEN))
    )
    with patched_workflow_environment(monkeypatch, execute_implementation=execute, open_pr=open_pr):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_AUTHORIZED_IMPLEMENTATION,
                payload=TaskAuthorizedImplementationPayload(
                    task=_TASK_SCOPE_PRODUCER,
                    profile_id=str(_PROFILE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        # The E6 service received the exact subjects and the zero-arg
        # Producer attachment factory.
        assert execute.call_count == 1
        execute_kwargs = execute.calls[0]["kwargs"]
        assert execute_kwargs["profile_id"] == _PROFILE_ID
        assert execute_kwargs["workspace_id"] == _WORKSPACE_ID
        assert execute_kwargs["task_id"] == _TASK_ID
        assert execute_kwargs["expected_state_token"] == _STATE_TOKEN
        assert callable(execute_kwargs["producer_runtime_factory"])
        # The exact-head PR_AUTHORIZATION gate was presented with the exact
        # post-execution token and finalized Execution identity.
        assert open_pr.call_count == 1
        assert len(open_pr.calls[0]["args"]) == 2
        assert open_pr.calls[0]["kwargs"] == {
            "profile_id": _PROFILE_ID,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "expected_state_token": execution.task.state_token,
            "execution_id": _EXECUTION_ID,
        }
        # Presenting the gate launches no publication; E7 approval is an
        # explicit Owner command.
        assert submitter.submissions == []


def test_authorized_implementation_retry_composition_consumes_and_presents_the_pr_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    execution = fake_execution()
    execute_retry = RecordingService(result=execution)
    open_pr = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_THIRD_TOKEN))
    )
    with patched_workflow_environment(
        monkeypatch, execute_implementation_retry=execute_retry, open_pr=open_pr
    ):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY,
                payload=TaskAuthorizedImplementationRetryPayload(
                    task=_TASK_SCOPE_PRODUCER,
                    profile_id=str(_PROFILE_ID),
                    retry_attempt_id=str(_OWNER_GATE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        # The F5c service received the exact subjects, the exact F4 retry
        # attempt identity, and the zero-arg Producer attachment factory.
        assert execute_retry.call_count == 1
        retry_kwargs = execute_retry.calls[0]["kwargs"]
        assert retry_kwargs["profile_id"] == _PROFILE_ID
        assert retry_kwargs["workspace_id"] == _WORKSPACE_ID
        assert retry_kwargs["task_id"] == _TASK_ID
        assert retry_kwargs["expected_state_token"] == _STATE_TOKEN
        assert retry_kwargs["retry_attempt_id"] == _OWNER_GATE_ID
        assert callable(retry_kwargs["producer_runtime_factory"])
        # The exact-head PR_AUTHORIZATION gate was presented with the exact
        # post-execution token and the NEW Execution identity.
        assert open_pr.call_count == 1
        assert open_pr.calls[0]["kwargs"] == {
            "profile_id": _PROFILE_ID,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "expected_state_token": execution.task.state_token,
            "execution_id": _EXECUTION_ID,
        }
        assert submitter.submissions == []


def test_pr_composition_composition_submits_the_pr_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    published = fake_published()
    compose = RecordingService(result=published)
    with patched_workflow_environment(monkeypatch, compose_pr=compose):
        payload = TaskPrCompositionPayload(
            task=_TASK_SCOPE_PRODUCER,
            profile_id=str(_PROFILE_ID),
            owner_gate_id=str(_OWNER_GATE_ID),
            expected_state_token=str(_STATE_TOKEN),
        )

        task_workflow.execute_task_workflow_operation(
            OperationInvocation(kind=OperationKind.TASK_PR_COMPOSITION, payload=payload),
            dependencies=dependencies,
        )

        # The E8 service received the exact gate/token subjects and the
        # accountable Profile-driven resolver seam.
        assert compose.call_count == 1
        assert len(compose.calls[0]["args"]) == 4
        assert compose.calls[0]["kwargs"] == {
            "profile_id": _PROFILE_ID,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "owner_gate_id": _OWNER_GATE_ID,
            "expected_state_token": _STATE_TOKEN,
        }
        # The exact post-publication token and canonical PR identity
        # authorize the PR review dispatch.
        assert len(submitter.submissions) == 1
        submitted = submitter.submissions[0]
        assert submitted.kind is OperationKind.TASK_PR_REVIEW
        assert isinstance(submitted.payload, TaskPrReviewPayload)
        assert submitted.payload.task_pull_request_id == str(published.pull_request.id)
        assert submitted.payload.expected_state_token == str(published.task.state_token)


def test_pr_review_composition_loops_finalize_over_remediation_in_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    published = fake_published(_STATE_TOKEN)
    first_dispatch = fake_dispatch_outcome(_NEXT_TOKEN)
    remediated_dispatch = fake_dispatch_outcome(_THIRD_TOKEN)
    dispatch = RecordingService(result=first_dispatch)
    finalize = RecordingService(
        results=[
            fake_pr_decision(fake_remediation(remediated_dispatch)),
            fake_pr_decision(None),
        ]
    )
    with patched_workflow_environment(
        monkeypatch,
        dispatch_pr=dispatch,
        finalize_pr=finalize,
        reloaded_published=published,
    ) as environment:
        payload = TaskPrReviewPayload(
            task=_TASK_SCOPE_REVIEWER,
            profile_id=str(_PROFILE_ID),
            task_pull_request_id=str(_PR_RECORD_ID),
            expected_state_token=str(_STATE_TOKEN),
        )

        task_workflow.execute_task_workflow_operation(
            OperationInvocation(kind=OperationKind.TASK_PR_REVIEW, payload=payload),
            dependencies=dependencies,
        )

        # The reloaded published subjects reached the dispatch service.
        assert dispatch.call_count == 1
        assert dispatch.calls[0]["kwargs"]["published"] is published
        assert dispatch.calls[0]["kwargs"]["expected_state_token"] == published.task.state_token
        # The remediation chain finalized in the same process: the re-entered
        # dispatch outcome was finalized immediately, never serialized.
        assert finalize.call_count == 2
        assert finalize.calls[0]["kwargs"]["dispatch"] is first_dispatch
        assert finalize.calls[1]["kwargs"]["dispatch"] is remediated_dispatch
        # One BOUND Producer attachment per decision round.
        producer_acquisitions = [
            acquisition
            for acquisition in environment["attachments"].acquisitions
            if acquisition["role"] is WorkflowRole.PRODUCER
        ]
        assert len(producer_acquisitions) == 2
        assert submitter.submissions == []


def test_stale_reload_refuses_before_any_service_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    dispatch = RecordingService()
    finalize = RecordingService()
    reload = RecordingService(result=StaleOperationError("the task state has moved on"))
    with patched_workflow_environment(
        monkeypatch,
        dispatch_planning=dispatch,
        finalize_planning=finalize,
    ) as environment:
        monkeypatch.setattr(task_workflow, "_reload_plan_production_outcome", reload)

        with pytest.raises(StaleOperationError):
            task_workflow.execute_task_workflow_operation(
                OperationInvocation(
                    kind=OperationKind.TASK_PLANNING_REVIEW,
                    payload=TaskPlanningReviewPayload(
                        task=_TASK_SCOPE_REVIEWER,
                        profile_id=str(_PROFILE_ID),
                        expected_state_token=str(_STATE_TOKEN),
                    ),
                ),
                dependencies=dependencies,
            )

        # The stale payload never reaches a runtime attachment or service.
        assert dispatch.call_count == 0
        assert finalize.call_count == 0
        assert environment["attachments"].acquisitions == []
        assert submitter.submissions == []


def test_stale_published_subject_refuses_before_the_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    dispatch = RecordingService()
    finalize = RecordingService()
    with patched_workflow_environment(
        monkeypatch, dispatch_pr=dispatch, finalize_pr=finalize
    ) as environment:
        monkeypatch.setattr(
            task_workflow,
            "_reload_published_subjects",
            RecordingService(result=StaleOperationError("not the canonical pull request")),
        )

        with pytest.raises(StaleOperationError):
            task_workflow.execute_task_workflow_operation(
                OperationInvocation(
                    kind=OperationKind.TASK_PR_REVIEW,
                    payload=TaskPrReviewPayload(
                        task=_TASK_SCOPE_REVIEWER,
                        profile_id=str(_PROFILE_ID),
                        task_pull_request_id=str(_PR_RECORD_ID),
                        expected_state_token=str(_STATE_TOKEN),
                    ),
                ),
                dependencies=dependencies,
            )

        assert dispatch.call_count == 0
        assert finalize.call_count == 0
        assert environment["attachments"].acquisitions == []
        assert submitter.submissions == []


@pytest.mark.parametrize(
    "outcome",
    [
        SubmissionResult.refused(SubmissionRefusalReason.EXECUTOR_SATURATED),
        SubmissionResult.uncertain(),
    ],
)
def test_continuation_submission_refusal_or_uncertainty_does_not_fail_the_operation(
    monkeypatch: pytest.MonkeyPatch, outcome: SubmissionResult
) -> None:
    dependencies, submitter = make_dependencies()
    submitter.outcomes = [outcome]
    prepare = RecordingService(result=fake_preparation())
    produce = RecordingService(result=fake_production())
    with patched_workflow_environment(monkeypatch, prepare=prepare, produce=produce):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
                payload=TaskPlanningBootstrapAndInitialPlanPayload(
                    task=_TASK_SCOPE_PRODUCER,
                    profile_id=str(_PROFILE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        # The completed operation's effects stand; the non-accepted
        # continuation submission is never retried and never fails it.
        assert prepare.call_count == 1
        assert produce.call_count == 1
        assert len(submitter.submissions) == 1


# --- minimal durable reloads (the former in-memory DTO handoffs) -------------


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self._handlers: list[tuple[Any, tuple[Any, ...] | None, bool]] = []

    def on(self, predicate: Any, row: tuple[Any, ...] | None) -> None:
        self._handlers.append((predicate, row, True))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        lowered = " ".join(sql.split()).lower()
        self.executed.append(lowered)
        for index, (predicate, row, once) in enumerate(self._handlers):
            if predicate(sql, params):
                if once:
                    del self._handlers[index]
                return FakeCursor(row)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class ScriptedPool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()


_OBSERVED = datetime(2026, 10, 31, 9, 0, 0, tzinfo=UTC)
_REPOSITORY_ID = uuid4()


def _durable_task_row(*, state_token: UUID, revision_id: UUID | None) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        503,
        42,
        "planning",
        None,
        None,
        state_token,
        revision_id,
        None,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _durable_revision_row(revision_id: UUID) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, 2, "revised plan", "b" * 40, _OBSERVED)


def _task_read_matches(sql: str, _params: Any) -> bool:
    lowered = " ".join(sql.split()).lower()
    return "from openorc.tasks where id" in lowered and "for update" not in lowered


def _revision_read_matches(sql: str, _params: Any) -> bool:
    return "from openorc.plan_revisions where id" in " ".join(sql.split()).lower()


def test_plan_production_subjects_reload_from_durable_state_without_a_wire_dto() -> None:
    conn = ScriptedConnection()
    conn.on(
        _task_read_matches, _durable_task_row(state_token=_NEXT_TOKEN, revision_id=_PR_RECORD_ID)
    )
    conn.on(_revision_read_matches, _durable_revision_row(_PR_RECORD_ID))

    production = task_workflow._reload_plan_production_outcome(
        cast(DatabasePool, ScriptedPool(conn)),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_NEXT_TOKEN,
    )

    # The exact durable facts only — the persisted current PlanRevision and
    # the current Task — re-derived by the composed subject guards. Nothing
    # is serialized onto the wire; the payload carries identities alone.
    assert isinstance(production, TaskPlanProductionOutcome)
    assert production.task.state_token == _NEXT_TOKEN
    assert production.plan_revision.id == _PR_RECORD_ID
    assert production.plan_revision.task_id == _TASK_ID


def test_published_subjects_reload_from_durable_state_without_a_wire_dto() -> None:
    conn = ScriptedConnection()
    conn.on(
        _task_read_matches, _durable_task_row(state_token=_NEXT_TOKEN, revision_id=_PR_RECORD_ID)
    )
    conn.on(
        lambda sql, _params: "from openorc.task_pull_requests" in " ".join(sql.split()).lower(),
        (
            _PR_RECORD_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            _REPOSITORY_ID,
            73301,
            9,
            "feature/task-2",
            "main",
            "c" * 40,
            "open",
            None,
            _OBSERVED,
            _OBSERVED,
        ),
    )

    published = task_workflow._reload_published_subjects(
        cast(DatabasePool, ScriptedPool(conn)),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_NEXT_TOKEN,
        task_pull_request_id=_PR_RECORD_ID,
    )

    assert isinstance(published, TaskPrPublished)
    assert published.task.state_token == _NEXT_TOKEN
    assert published.pull_request.id == _PR_RECORD_ID


def test_stale_token_reloads_refuse_before_any_dispatch() -> None:
    conn = ScriptedConnection()
    conn.on(
        _task_read_matches, _durable_task_row(state_token=_THIRD_TOKEN, revision_id=_PR_RECORD_ID)
    )

    with pytest.raises(StaleOperationError):
        task_workflow._reload_plan_production_outcome(
            cast(DatabasePool, ScriptedPool(conn)),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_NEXT_TOKEN,
        )


# --- process-local dependency wiring and mode behavior -----------------------


def test_queue_mode_dependencies_resolve_the_queue_continuation_submitter(
    settings_factory: Any,
) -> None:
    settings = settings_factory(
        execution_mode=ExecutionMode.QUEUE, valkey_url="redis://127.0.0.1:6379/9"
    )

    dependencies = build_default_operation_dependencies(settings)

    submitter = dependencies.continuation_submitter()
    assert isinstance(submitter, RQQueueOperationSubmission)


def test_regular_mode_continuation_submitter_is_the_late_bound_process_executor(
    settings_factory: Any,
) -> None:
    settings = settings_factory(execution_mode=ExecutionMode.REGULAR, valkey_url=None)

    dependencies = build_default_operation_dependencies(settings)

    # Resolution fails closed until the serving lifespan binds the executor.
    with pytest.raises(ExecutionModeDependencyError):
        dependencies.continuation_submitter()
    bound = object()
    dependencies.bind_regular_process_submitter(cast(OperationSubmitter, bound))
    assert dependencies.continuation_submitter() is bound


def test_regular_mode_dependencies_never_touch_the_queue_backend(
    settings_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory(execution_mode=ExecutionMode.REGULAR, valkey_url=None)

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("regular-mode operation dependencies never touch a queue backend")

    monkeypatch.setattr("openorc.workers.bootstrap.create_redis_client", forbidden)

    dependencies = build_default_operation_dependencies(settings)

    # Constructing and resolving the process-local providers performs no
    # queue I/O in regular mode.
    assert dependencies.reviewer_neutral_directory_provider is not None
    assert dependencies.task_bootstrap_runtime_composition() is not None


def test_default_providers_fail_fast_on_missing_configuration(settings_factory: Any) -> None:
    settings = settings_factory(reviewer_neutral_directory=None)

    with pytest.raises(ConfigurationError, match="reviewer-neutral directory"):
        task_workflow.default_reviewer_neutral_directory_provider(settings)
    with pytest.raises(ConfigurationError, match="user-flow credentials"):
        task_workflow.default_user_token_resolver_provider(settings)


def test_default_reviewer_neutral_directory_provider_rejects_a_blank_value(
    settings_factory: Any,
) -> None:
    with pytest.raises(ConfigurationError, match="reviewer-neutral directory"):
        task_workflow.default_reviewer_neutral_directory_provider(
            settings_factory(reviewer_neutral_directory="   ")
        )


def test_entrypoint_delivers_task_operations_through_the_production_table(
    settings_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory(
        execution_mode=ExecutionMode.QUEUE, valkey_url="redis://127.0.0.1:6379/9"
    )
    delivered: list[OperationInvocation] = []

    def fake_seam(
        invocation: OperationInvocation,
        *,
        dependencies: OperationDependencies,
        attempt_evidence: InvocationAttemptEvidence | None = None,
    ) -> None:
        delivered.append(invocation)

    monkeypatch.setattr(Settings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(task_workflow, "execute_task_workflow_operation", fake_seam)

    payload = TaskAuthorizedImplementationPayload(
        task=_TASK_SCOPE_PRODUCER,
        profile_id=str(_PROFILE_ID),
        expected_state_token=str(_STATE_TOKEN),
    )
    wire = OperationInvocation(kind=OperationKind.TASK_AUTHORIZED_IMPLEMENTATION, payload=payload)

    execute_operation_job(wire.encode())

    # The production handler table delivered the decoded Task operation to
    # the single composition seam; no resource left the wire.
    assert len(delivered) == 1
    assert delivered[0].kind is OperationKind.TASK_AUTHORIZED_IMPLEMENTATION
    assert delivered[0].payload == payload


# --- explicit workflow entry continuations (review remediations) --------------


def _fake_start_outcome(status: Any, state_token: UUID) -> Any:
    return SimpleNamespace(
        status=status,
        task=SimpleNamespace(id=_TASK_ID, workspace_id=_WORKSPACE_ID, state_token=state_token),
    )


def test_start_continuation_submits_the_exact_bootstrap_invocation() -> None:
    dependencies, submitter = make_dependencies()

    result = task_workflow.submit_planning_bootstrap_after_start(
        cast(Any, _fake_start_outcome(TaskStartStatus.PLANNING, _STATE_TOKEN)),
        profile_id=_PROFILE_ID,
        dependencies=dependencies,
    )

    assert result is not None
    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    assert isinstance(submitted.payload, TaskPlanningBootstrapAndInitialPlanPayload)
    # The exact post-start token and the starting Owner's accountable Profile.
    assert submitted.payload.task == _TASK_SCOPE_PRODUCER
    assert submitted.payload.profile_id == str(_PROFILE_ID)
    assert submitted.payload.expected_state_token == str(_STATE_TOKEN)


def test_queued_start_has_no_bootstrap_continuation() -> None:
    dependencies, submitter = make_dependencies()

    result = task_workflow.submit_planning_bootstrap_after_start(
        cast(Any, _fake_start_outcome(TaskStartStatus.QUEUED, _STATE_TOKEN)),
        profile_id=_PROFILE_ID,
        dependencies=dependencies,
    )

    assert result is None
    assert submitter.submissions == []


def test_implementation_approval_continuation_submits_the_exact_invocation() -> None:
    dependencies, submitter = make_dependencies()
    authorization = cast(
        Any,
        SimpleNamespace(
            task=SimpleNamespace(id=_TASK_ID, workspace_id=_WORKSPACE_ID, state_token=_NEXT_TOKEN),
            owner_gate=SimpleNamespace(id=_OWNER_GATE_ID),
        ),
    )

    result = task_workflow.submit_authorized_implementation_after_approval(
        authorization, profile_id=_PROFILE_ID, dependencies=dependencies
    )

    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is OperationKind.TASK_AUTHORIZED_IMPLEMENTATION
    assert isinstance(submitted.payload, TaskAuthorizedImplementationPayload)
    assert submitted.payload.task == _TASK_SCOPE_PRODUCER
    assert submitted.payload.profile_id == str(_PROFILE_ID)
    assert submitted.payload.expected_state_token == str(_NEXT_TOKEN)
    # The approved gate is re-derived by the executing service and is never
    # carried on the implementation wire.
    assert not hasattr(submitted.payload, "owner_gate_id")


def test_pr_authorization_approval_continuation_submits_the_exact_invocation() -> None:
    dependencies, submitter = make_dependencies()
    authorization = cast(
        Any,
        SimpleNamespace(
            task=SimpleNamespace(id=_TASK_ID, workspace_id=_WORKSPACE_ID, state_token=_NEXT_TOKEN),
            owner_gate=SimpleNamespace(id=_OWNER_GATE_ID),
        ),
    )

    result = task_workflow.submit_pr_composition_after_approval(
        authorization, profile_id=_PROFILE_ID, dependencies=dependencies
    )

    assert result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is OperationKind.TASK_PR_COMPOSITION
    assert isinstance(submitted.payload, TaskPrCompositionPayload)
    assert submitted.payload.task == _TASK_SCOPE_PRODUCER
    # The exact approved gate identity and post-approval token.
    assert submitted.payload.owner_gate_id == str(_OWNER_GATE_ID)
    assert submitted.payload.expected_state_token == str(_NEXT_TOKEN)


def test_directly_constructed_invocation_rejects_a_mismatched_role() -> None:
    # Regular mode executes directly constructed invocations without the wire
    # decoder; the construction-time role correspondence keeps the same
    # fail-closed validation in every mode (issue #253).
    with pytest.raises(InvalidInvocationError, match="role"):
        OperationInvocation(
            kind=OperationKind.TASK_AUTHORIZED_IMPLEMENTATION,
            payload=TaskAuthorizedImplementationPayload(
                task=TaskScope(
                    workspace_id=str(_WORKSPACE_ID),
                    task_id=str(_TASK_ID),
                    role=WorkflowRoleRef.REVIEWER,
                ),
                profile_id=str(_PROFILE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
        )


def test_authorized_merge_retry_payload_decodes_strictly() -> None:
    """The F5d merge-retry payload (issue #267) decodes with the strict field set."""
    base = {
        "task": {
            "workspace_id": str(_WORKSPACE_ID),
            "task_id": str(_TASK_ID),
            "role": WorkflowRoleRef.REVIEWER.value,
        },
        "profile_id": str(_PROFILE_ID),
        "retry_attempt_id": str(_OWNER_GATE_ID),
        "expected_state_token": str(_STATE_TOKEN),
    }
    decoded = decode_operation_invocation({"kind": "task_authorized_merge_retry", "payload": base})
    assert decoded.kind is OperationKind.TASK_AUTHORIZED_MERGE_RETRY
    merge_payload = cast(TaskAuthorizedMergeRetryPayload, decoded.payload)
    assert merge_payload.retry_attempt_id == str(_OWNER_GATE_ID)
    # Unexpected fields are refused before any effect: no `retryable`-style
    # free authority flag is ever carried.
    with_extra = {**base, "retryable": True}
    with pytest.raises(InvalidInvocationError, match="unexpected fields"):
        decode_operation_invocation({"kind": "task_authorized_merge_retry", "payload": with_extra})
    malformed = {**base, "retry_attempt_id": "not-a-uuid"}
    with pytest.raises(InvalidInvocationError, match="retry_attempt_id"):
        decode_operation_invocation({"kind": "task_authorized_merge_retry", "payload": malformed})
    # A Producer-role scope does not match the merge kind's review-context role.
    producer_role = {**base, "task": {**base["task"], "role": WorkflowRoleRef.PRODUCER.value}}
    with pytest.raises(InvalidInvocationError, match="role"):
        decode_operation_invocation(
            {"kind": "task_authorized_merge_retry", "payload": producer_role}
        )


def test_authorized_merge_retry_composition_consumes_and_sends_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The F5d composition delegates to the merge-retry execution service."""
    dependencies, submitter = make_dependencies()
    merge_retry = RecordingService(result=None)
    with patched_workflow_environment(monkeypatch, execute_merge_retry=merge_retry):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_AUTHORIZED_MERGE_RETRY,
                payload=TaskAuthorizedMergeRetryPayload(
                    task=_TASK_SCOPE_REVIEWER,
                    profile_id=str(_PROFILE_ID),
                    retry_attempt_id=str(_OWNER_GATE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        # The F5d service received the exact subjects, the exact F4 retry
        # attempt identity, the accountable Profile, and the invocation's
        # attempt-evidence collector; the GitHub client and the Profile
        # user-token resolver resolve process-locally.
        assert merge_retry.call_count == 1
        retry_kwargs = merge_retry.calls[0]["kwargs"]
        assert retry_kwargs["profile_id"] == _PROFILE_ID
        assert retry_kwargs["workspace_id"] == _WORKSPACE_ID
        assert retry_kwargs["task_id"] == _TASK_ID
        assert retry_kwargs["expected_state_token"] == _STATE_TOKEN
        assert retry_kwargs["retry_attempt_id"] == _OWNER_GATE_ID
        # (The attempt-evidence collector is coordinator plumbing, excluded
        # from recorded comparisons and threaded by the coordinator tests.)
        # No continuation submission exists for the merge retry: the outcome
        # stays at its committed durable state and F11 owns any wakeup.
        assert submitter.submissions == []
