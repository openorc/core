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
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.send_claims import (
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
)
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import PR_RESULT_FAMILY
from openorc.protocol.parsing import parse_formal_response
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_planning_bootstrap import TaskPlanningPreparationOutcome
from openorc.services.task_pr_composition import SettledTaskPrPublication, TaskPrPublished
from openorc.services.task_pr_review_decision import PrReviewDecision
from openorc.workers import task_workflow
from openorc.workers.invocation import (
    ExecutionModeDependencyError,
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    OperationPayload,
    OperationSubmitter,
    QueueWakePayload,
    RQQueueOperationSubmission,
    SubmissionOutcome,
    SubmissionResult,
    TaskAuthorizedImplementationPayload,
    TaskAuthorizedImplementationRetryPayload,
    TaskAuthorizedMergeRetryPayload,
    TaskPlanningBootstrapAndInitialPlanPayload,
    TaskPlanningReviewPayload,
    TaskPrAuthorizationRetryPayload,
    TaskPrCompositionPayload,
    TaskPrReviewPayload,
    TaskScope,
    WorkflowRoleRef,
    build_default_operation_dependencies,
    decode_operation_invocation,
    execute_operation_job,
)
from tests.fakes.agent_runtime import pr_result_candidate
from tests.fakes.task_rows import task_row

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
        # threading is covered by the dedicated coordinator tests. The F10
        # cleanup submission bridge (issue #256) is the same shape of
        # optional process-local plumbing: accepted, excluded from recorded
        # comparisons, and covered by the dedicated cleanup threading tests.
        recorded_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key not in ("attempt_evidence", "terminal_finalization_submission")
        }
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
        # The regular-mode hub-run observation host (issue #323): a inert
        # stand-in; the recorded service seams swallow the registrar and the
        # continuation trigger without touching it.
        "detached_turn_observer_provider": lambda _settings: object(),
    }
    resolved.update(providers)
    return OperationDependencies(**resolved), submitter


def make_queue_dependencies(**providers: Any) -> tuple[OperationDependencies, FakeSubmitter]:
    """Queue-mode dependencies: the same detached composition seams.

    Since the #347 transport parity, queue mode runs the identical detached
    compositions; the resolved observation registrar stands in for the #346
    remote proxy and the continuation submitter is the recording queue seam.
    """
    return make_dependencies(
        settings=Settings(
            environment="test",
            api_host="127.0.0.1",
            api_port=3999,
            api_reload=False,
            execution_mode=ExecutionMode.QUEUE,
            valkey_url="redis://127.0.0.1:6379/9",
        ),
        **providers,
    )


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
    execute_pr_authorization_retry: RecordingService | None = None,
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
    # The #347 seam mapping: every model-turn family the compositions invoke
    # is the owning service's detached dispatch; the retired blocking
    # interim seams no longer exist on the worker module. The recording
    # parameter names keep their historical roles (produce/dispatch/finalize)
    # so the surviving tests stay readable.
    service_patches: dict[str, Any] = {
        "prepare_task_for_planning": prepare,
        "dispatch_initial_plan_detached": produce,
        "dispatch_planning_review_detached": dispatch_planning,
        "open_implementation_authorization": open_implementation,
        "dispatch_authorized_implementation_detached": execute_implementation,
        "dispatch_authorized_implementation_retry_detached": execute_implementation_retry,
        "execute_authorized_merge_retry": execute_merge_retry,
        "execute_authorized_pr_authorization_retry": execute_pr_authorization_retry,
        "open_pr_authorization": open_pr,
        "dispatch_pr_compose_detached": compose_pr,
        "dispatch_pr_review_detached": dispatch_pr,
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
    # The real prepared-outcome type: the composition distinguishes the
    # detached dispatch suspension from the prepared handoff by type, and the
    # duck-typed members the E2 production consumes stay SimpleNamespaces.
    return cast(
        Any,
        TaskPlanningPreparationOutcome(
            task=cast(Any, SimpleNamespace(state_token=_STATE_TOKEN)),  # type: ignore[arg-type]
            producer_session=cast(
                Any, SimpleNamespace(external_session_id=_PRODUCER_EXTERNAL_SESSION_ID)
            ),
            reviewer_session=cast(Any, SimpleNamespace(external_session_id=None)),
            producer_checkout_directory=_CHECKOUT_DIRECTORY,
        ),
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
        reviewer_session=SimpleNamespace(
            id=uuid4(), external_session_id=_REVIEWER_EXTERNAL_SESSION_ID
        ),
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
    return task_row(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        status="planning",
        state_token=state_token,
        current_plan_revision_id=revision_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
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


def test_pr_authorization_retry_payload_decodes_strictly() -> None:
    """The F5f presentation-retry payload (issue #269) decodes with the
    strict field set."""
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
    decoded = decode_operation_invocation({"kind": "task_pr_authorization_retry", "payload": base})
    assert decoded.kind is OperationKind.TASK_PR_AUTHORIZATION_RETRY
    retry_payload = cast(TaskPrAuthorizationRetryPayload, decoded.payload)
    assert retry_payload.retry_attempt_id == str(_OWNER_GATE_ID)
    # Unexpected fields are refused before any effect.
    with_extra = {**base, "retryable": True}
    with pytest.raises(InvalidInvocationError, match="unexpected fields"):
        decode_operation_invocation({"kind": "task_pr_authorization_retry", "payload": with_extra})
    malformed = {**base, "retry_attempt_id": "not-a-uuid"}
    with pytest.raises(InvalidInvocationError, match="retry_attempt_id"):
        decode_operation_invocation({"kind": "task_pr_authorization_retry", "payload": malformed})
    # A Reviewer-role scope does not match the presentation-retry kind's
    # implementation-phase role.
    reviewer_role = {**base, "task": {**base["task"], "role": WorkflowRoleRef.REVIEWER.value}}
    with pytest.raises(InvalidInvocationError, match="role"):
        decode_operation_invocation(
            {"kind": "task_pr_authorization_retry", "payload": reviewer_role}
        )


def test_pr_authorization_retry_composition_consumes_and_represents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The F5f composition delegates to the presentation-retry execution
    seam — the E7-only guarded re-presentation, never the implementation."""
    dependencies, submitter = make_dependencies()
    presentation_retry = RecordingService(result=None)
    with patched_workflow_environment(
        monkeypatch, execute_pr_authorization_retry=presentation_retry
    ):
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(
                kind=OperationKind.TASK_PR_AUTHORIZATION_RETRY,
                payload=TaskPrAuthorizationRetryPayload(
                    task=_TASK_SCOPE_PRODUCER,
                    profile_id=str(_PROFILE_ID),
                    retry_attempt_id=str(_OWNER_GATE_ID),
                    expected_state_token=str(_STATE_TOKEN),
                ),
            ),
            dependencies=dependencies,
        )

        assert presentation_retry.call_count == 1
        retry_kwargs = presentation_retry.calls[0]["kwargs"]
        assert retry_kwargs["profile_id"] == _PROFILE_ID
        assert retry_kwargs["workspace_id"] == _WORKSPACE_ID
        assert retry_kwargs["task_id"] == _TASK_ID
        assert retry_kwargs["expected_state_token"] == _STATE_TOKEN
        assert retry_kwargs["retry_attempt_id"] == _OWNER_GATE_ID
        # (The attempt-evidence collector is coordinator plumbing, excluded
        # from recorded comparisons and threaded by the dedicated test
        # below.) No continuation submission exists for the presentation
        # retry: the outcome stays at its committed durable state and F11
        # owns any wakeup.
        assert submitter.submissions == []


def test_the_implementation_compositions_thread_the_attempt_evidence_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both implementation compositions (initial and F5c-authorized retry)
    thread the invocation's attempt-evidence collector into their detached
    dispatch — the same application path, so an operational failure
    attributes the invocation's own Level 1 attempt identity in either
    composition (the E7 presentation is the completion handoff's seam)."""
    dependencies, submitter = make_queue_dependencies()
    dispatched = SimpleNamespace(task=None, execution=None, producer_session=None, receipt=None)
    evidence = InvocationAttemptEvidence()

    cases = [
        (
            OperationKind.TASK_AUTHORIZED_IMPLEMENTATION,
            TaskAuthorizedImplementationPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
            "dispatch_authorized_implementation_detached",
        ),
        (
            OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY,
            TaskAuthorizedImplementationRetryPayload(
                task=_TASK_SCOPE_PRODUCER,
                profile_id=str(_PROFILE_ID),
                retry_attempt_id=str(_OWNER_GATE_ID),
                expected_state_token=str(_STATE_TOKEN),
            ),
            "dispatch_authorized_implementation_retry_detached",
        ),
    ]
    for kind, payload, seam_name in cases:
        # The raw recording stub (not the comparison-filtering
        # RecordingService) proves the collector threading itself.
        dispatch_calls: list[dict[str, Any]] = []

        def recording_dispatch(
            *args: Any,
            _calls: list[dict[str, Any]] = dispatch_calls,
            **kwargs: Any,
        ) -> Any:
            _calls.append(
                {key: value for key, value in kwargs.items() if key != "attempt_evidence"}
                | {"_has_attempt_evidence": "attempt_evidence" in kwargs}
            )
            return dispatched

        monkeypatch.setattr(task_workflow, seam_name, recording_dispatch, raising=True)
        task_workflow.execute_task_workflow_operation(
            OperationInvocation(kind=kind, payload=payload),
            dependencies=dependencies,
            attempt_evidence=evidence,
        )
        assert len(dispatch_calls) == 1
        assert dispatch_calls[0]["_has_attempt_evidence"] is True
    assert submitter.submissions == []


# --- the #347 queue-mode transport parity -------------------------------------


def test_queue_mode_composition_dispatches_detached_and_returns_before_the_model_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #347 queue parity: the queue dispatch worker runs the same detached
    composition — the owning service's detached dispatch receives the
    queue-resolved registrar seam (the #346 remote proxy stand-in) and the
    ONE completion handoff, and the worker returns after the durable
    admission without any runtime attachment or model turn (the
    independently observed completion settles later)."""
    registrar = object()
    dependencies, submitter = make_queue_dependencies(
        detached_turn_observer_provider=lambda _settings: registrar
    )
    review_dispatch = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_NEXT_TOKEN))
    )
    monkeypatch.setattr(
        task_workflow, "dispatch_planning_review_detached", review_dispatch, raising=True
    )
    production = fake_production(_STATE_TOKEN)
    with patched_workflow_environment(
        monkeypatch, reloaded_plan_production=production
    ) as environment:
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

        assert review_dispatch.call_count == 1
        dispatch_kwargs = review_dispatch.calls[0]["kwargs"]
        assert dispatch_kwargs["observation_registrar"] is registrar
        assert callable(dispatch_kwargs["completion_handoff"].handoff_settled_completion)
        # The worker exits before any model response: no runtime attachment
        # is acquired and no continuation is submitted by the dispatching
        # job — the durable state owns recovery.
        assert environment["attachments"].acquisitions == []
        assert submitter.submissions == []


def test_queue_mode_observation_refusal_is_a_proven_pre_effect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The queue dispatch worker cannot attest a live observer registration:
    the resolved registrar's typed refusal propagates as a proven pre-effect
    failure before any admission, with no runtime attachment acquired and no
    continuation submitted (issues #346/#347)."""
    from openorc.services.errors import ObservationRegistrationRefusedError

    def refusing_registrar(_settings: Any) -> Any:
        raise ObservationRegistrationRefusedError(
            "the queue registration transport refused",
            reason="observer_unavailable",
        )

    dependencies, submitter = make_queue_dependencies(
        detached_turn_observer_provider=refusing_registrar
    )
    production = fake_production(_STATE_TOKEN)
    with patched_workflow_environment(
        monkeypatch, reloaded_plan_production=production
    ) as environment:
        with pytest.raises(ObservationRegistrationRefusedError):
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

        # The refusal is pre-effect: no runtime attachment was acquired and
        # nothing was submitted; the durable claim/receipt state owns the
        # settled refusal.
        assert environment["attachments"].acquisitions == []
        assert submitter.submissions == []


def test_queue_mode_duplicate_job_refuses_before_any_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duplicate or stale RQ job never re-dispatches: the composition's
    durable subject reload refuses the stale second delivery before any
    detached dispatch, runtime attachment, or continuation submission."""
    dependencies, submitter = make_queue_dependencies()
    review_dispatch = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_NEXT_TOKEN))
    )
    monkeypatch.setattr(
        task_workflow, "dispatch_planning_review_detached", review_dispatch, raising=True
    )
    production = fake_production(_STATE_TOKEN)
    payload = TaskPlanningReviewPayload(
        task=_TASK_SCOPE_REVIEWER,
        profile_id=str(_PROFILE_ID),
        expected_state_token=str(_STATE_TOKEN),
    )
    invocation = OperationInvocation(kind=OperationKind.TASK_PLANNING_REVIEW, payload=payload)
    with patched_workflow_environment(monkeypatch, reloaded_plan_production=production):
        task_workflow.execute_task_workflow_operation(invocation, dependencies=dependencies)
        assert review_dispatch.call_count == 1

        # The replayed job's durable reload classifies the moved-on state as
        # stale: the duplicate delivery is refused before any service effect.
        monkeypatch.setattr(
            task_workflow,
            "_reload_plan_production_outcome",
            RecordingService(result=StaleOperationError("the task state has moved on")),
        )
        with pytest.raises(StaleOperationError):
            task_workflow.execute_task_workflow_operation(invocation, dependencies=dependencies)
        assert review_dispatch.call_count == 1
        assert submitter.submissions == []


# --- the ONE post-commit completion handoff (issues #324/#333) -------------------


def test_the_completion_handoff_submits_the_resumed_bootstrap_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    resumable = SimpleNamespace(
        task=SimpleNamespace(workspace_id=_WORKSPACE_ID, id=_TASK_ID, state_token=_NEXT_TOKEN),
        owner_profile_id=_PROFILE_ID,
    )
    monkeypatch.setattr(
        task_workflow,
        "find_resumable_bootstrap_continuation",
        lambda _pool, *, workspace_id, task_id: resumable,
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
    )

    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is task_workflow.OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    payload = submitted.payload
    assert isinstance(payload, task_workflow.TaskPlanningBootstrapAndInitialPlanPayload)
    assert payload.task.workspace_id == str(_WORKSPACE_ID)
    assert payload.task.task_id == str(_TASK_ID)
    assert payload.profile_id == str(_PROFILE_ID)
    assert payload.expected_state_token == str(_NEXT_TOKEN)


def test_the_completion_handoff_submits_nothing_when_the_state_is_not_resumable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    monkeypatch.setattr(
        task_workflow,
        "find_resumable_bootstrap_continuation",
        lambda _pool, *, workspace_id, task_id: None,
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
    )

    assert submitter.submissions == []


def test_the_completion_handoff_refuses_a_family_without_a_wired_route() -> None:
    dependencies, submitter = make_dependencies()

    handoff = task_workflow.build_detached_completion_handoff(dependencies)

    with pytest.raises(ConflictError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.SESSION_RESUME,
        )
    assert submitter.submissions == []


def test_the_completion_handoff_presents_the_exact_head_pr_gate_for_the_implementation_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.IMPLEMENTING,
        current_owner_gate_id=None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    coordinated: list[dict[str, Any]] = []
    presentations: list[dict[str, Any]] = []

    def recording_coordinate(pool: Any, **kwargs: Any) -> None:
        coordinated.append(dict(kwargs))
        kwargs["invoke_level_one"](InvocationAttemptEvidence())

    def recording_open_pr(pool: Any, github: Any, **kwargs: Any) -> Any:
        presentations.append(dict(kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", recording_coordinate)
    monkeypatch.setattr(task_workflow, "open_pr_authorization", recording_open_pr)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
    )

    # The presentation ran under the F5b coordinator with the fresh
    # committed-state token seed and the durable Workspace Owner identity.
    assert len(coordinated) == 1
    assert coordinated[0]["expected_status"] is TaskStatus.IMPLEMENTING
    assert coordinated[0]["profile_id"] == _PROFILE_ID
    assert coordinated[0]["expected_state_token"] == _NEXT_TOKEN
    # The existing F5f service was invoked exactly once with the re-derived
    # authority; execution discovery is the service's own committed-state
    # re-derivation.
    assert len(presentations) == 1
    assert presentations[0]["profile_id"] == _PROFILE_ID
    assert presentations[0]["workspace_id"] == _WORKSPACE_ID
    assert presentations[0]["task_id"] == _TASK_ID
    assert presentations[0]["expected_state_token"] == _NEXT_TOKEN
    assert presentations[0]["execution_id"] is None
    assert presentations[0]["attempt_evidence"] is not None
    assert submitter.submissions == []


def test_the_completion_handoff_transitions_and_arranges_the_initial_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The workspace-PREPARE route (issue #334): the settled ``prepared``
    stage is the only authority — the transition continuation runs from
    committed state and the next typed initial-plan invocation is submitted
    with the fresh post-tail token through the shared continuation seam.
    """
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=None,
    )
    claim_token = UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000a")
    evidence = SimpleNamespace(
        claim_token=claim_token,
        stage=task_workflow.TaskPlanningBootstrapStage.PREPARED,
        stage_settled_at=None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task_planning_bootstrap_evidence",
        lambda _pool, *, task_id: evidence if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    transitions: list[dict[str, Any]] = []
    coordinations: list[dict[str, Any]] = []

    def recording_transition(
        _pool: Any,
        _composition: Any,
        *,
        workspace_id: Any,
        profile_id: Any,
        task_id: Any,
        expected_state_token: Any,
        attempt_evidence: Any,
    ) -> Any:
        transitions.append(
            {
                "workspace_id": workspace_id,
                "profile_id": profile_id,
                "task_id": task_id,
                "expected_state_token": expected_state_token,
                "attempt_evidence": attempt_evidence,
            }
        )
        return SimpleNamespace(task=SimpleNamespace(state_token=_NEXT_TOKEN))

    monkeypatch.setattr(
        task_workflow,
        "complete_settled_bootstrap_prepare_transition",
        recording_transition,
    )

    def recording_coordinate(pool: Any, **kwargs: Any) -> None:
        evidence = InvocationAttemptEvidence()
        kwargs["invoke_level_one"](evidence)
        coordinations.append(
            {
                "expected_status": kwargs["expected_status"],
                "workspace_id": kwargs["workspace_id"],
                "task_id": kwargs["task_id"],
                "profile_id": kwargs["profile_id"],
                "expected_state_token": kwargs["expected_state_token"],
                "evidence": evidence,
            }
        )

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", recording_coordinate)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
    )

    # The transition continuation runs under the F5b coordinator's typed
    # failure handoff with the committed-state currentness seed.
    assert [{k: v for k, v in call.items() if k != "evidence"} for call in coordinations] == [
        {
            "expected_status": TaskStatus.PLANNING,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "profile_id": _PROFILE_ID,
            "expected_state_token": _NEXT_TOKEN,
        }
    ]
    assert transitions == [
        {
            "workspace_id": _WORKSPACE_ID,
            "profile_id": _PROFILE_ID,
            "task_id": _TASK_ID,
            "expected_state_token": _NEXT_TOKEN,
            "attempt_evidence": coordinations[0]["evidence"],
        }
    ]
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is task_workflow.OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    payload = submitted.payload
    assert isinstance(payload, task_workflow.TaskPlanningBootstrapAndInitialPlanPayload)
    assert payload.task.workspace_id == str(_WORKSPACE_ID)
    assert payload.task.task_id == str(_TASK_ID)
    assert payload.profile_id == str(_PROFILE_ID)
    assert payload.expected_state_token == str(_NEXT_TOKEN)


def test_the_completion_handoff_reports_a_failing_coordinated_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positively detected reconstruction failure in the coordinated

    transition propagates from the coordinator unchanged after its typed
    failure handoff: the observation host contains it, and the durable
    state — the prepared stage with the Level 2/3 blocking consequence —
    owns recovery. The BLOCKED disposition itself is proven at the service
    level against the real coordinator and disposition machinery.
    """
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=None,
    )
    evidence = SimpleNamespace(
        claim_token=UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000a"),
        stage=task_workflow.TaskPlanningBootstrapStage.PREPARED,
        stage_settled_at=None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task_planning_bootstrap_evidence",
        lambda _pool, *, task_id: evidence if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    monkeypatch.setattr(
        task_workflow,
        "complete_settled_bootstrap_prepare_transition",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ExternalOperationFailedError("the planning-context transition failed")
        ),
    )

    def failing_coordinate(pool: Any, **kwargs: Any) -> None:
        kwargs["invoke_level_one"](InvocationAttemptEvidence())

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", failing_coordinate)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    with pytest.raises(ExternalOperationFailedError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
        )

    # A failed transition is never a submission authorization.
    assert submitter.submissions == []


def test_the_completion_handoff_ignores_a_bootstrap_stage_that_is_not_prepared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durably dispatched (not yet settled) or already-transitioned stage

    is not the settled handoff state: the route continues nothing and
    submits nothing — the durable state owns every other path.
    """
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=None,
    )
    evidence = SimpleNamespace(
        claim_token=UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000a"),
        stage=task_workflow.TaskPlanningBootstrapStage.PREPARE_DISPATCHED,
        stage_settled_at=None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task_planning_bootstrap_evidence",
        lambda _pool, *, task_id: evidence if task_id == _TASK_ID else None,
    )
    transitions: list[Any] = []
    monkeypatch.setattr(
        task_workflow,
        "complete_settled_bootstrap_prepare_transition",
        lambda *args, **kwargs: transitions.append(kwargs),
    )

    def refusing_coordinate(pool: Any, **kwargs: Any) -> None:
        raise AssertionError("the coordinator must not run for a non-prepared stage")

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", refusing_coordinate)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
    )

    assert transitions == []
    assert submitter.submissions == []


def test_the_completion_handoff_ignores_a_missing_implementation_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies()
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: None,
    )

    def recording_coordinate(pool: Any, **kwargs: Any) -> None:
        raise AssertionError("the coordinator must not run for a missing task")

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", recording_coordinate)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
    )

    assert submitter.submissions == []


def test_authorized_implementation_composition_dispatches_detached_in_regular_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regular-mode E6 composition admits the detached turn and returns.

    No blocking E6 execution and no inline PR-gate presentation happen in
    the dispatching worker: the observed completion settles the attempt and
    the ONE post-commit completion handoff owns the presentation.
    """
    dependencies, submitter = make_dependencies()
    dispatched = SimpleNamespace(task=None, execution=None, producer_session=None, receipt=None)
    dispatch = RecordingService(result=dispatched)
    monkeypatch.setattr(
        task_workflow, "dispatch_authorized_implementation_detached", dispatch, raising=True
    )

    presentations: list[dict[str, Any]] = []

    def recording_open_pr(pool: Any, github: Any, **kwargs: Any) -> Any:
        presentations.append(dict(kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(task_workflow, "open_pr_authorization", recording_open_pr, raising=True)

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

    assert dispatch.call_count == 1
    dispatch_kwargs = dispatch.calls[0]["kwargs"]
    assert dispatch_kwargs["profile_id"] == _PROFILE_ID
    assert dispatch_kwargs["workspace_id"] == _WORKSPACE_ID
    assert dispatch_kwargs["task_id"] == _TASK_ID
    assert dispatch_kwargs["expected_state_token"] == _STATE_TOKEN
    assert callable(dispatch_kwargs["producer_runtime_factory"])
    assert dispatch_kwargs["observation_registrar"] is not None
    assert callable(dispatch_kwargs["completion_handoff"].handoff_settled_completion)
    # The blocking E6 service and the inline presentation never ran.
    assert presentations == []
    assert submitter.submissions == []


def test_authorized_implementation_retry_composition_dispatches_detached_in_regular_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regular-mode F5c composition admits the detached retry and returns.

    The registered F1 retry operation is directly invocable for an
    already-due F4 authorization (issues #263/#340), so the ordinary
    model-backed retry turn must not hold a regular-mode worker: no blocking
    E6 execution and no inline PR-gate presentation happen in the dispatching
    worker — the observed completion settles the NEW retry Execution and the
    ONE post-commit completion handoff owns the presentation. F12 (#316)
    owns submitting due authorized retries in either mode.
    """
    dependencies, submitter = make_dependencies()
    dispatched = SimpleNamespace(task=None, execution=None, producer_session=None, receipt=None)
    dispatch = RecordingService(result=dispatched)
    monkeypatch.setattr(
        task_workflow,
        "dispatch_authorized_implementation_retry_detached",
        dispatch,
        raising=True,
    )

    presentations: list[dict[str, Any]] = []

    def recording_open_pr(pool: Any, github: Any, **kwargs: Any) -> Any:
        presentations.append(dict(kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(task_workflow, "open_pr_authorization", recording_open_pr, raising=True)

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

    assert dispatch.call_count == 1
    dispatch_kwargs = dispatch.calls[0]["kwargs"]
    assert dispatch_kwargs["profile_id"] == _PROFILE_ID
    assert dispatch_kwargs["workspace_id"] == _WORKSPACE_ID
    assert dispatch_kwargs["task_id"] == _TASK_ID
    assert dispatch_kwargs["expected_state_token"] == _STATE_TOKEN
    assert dispatch_kwargs["retry_attempt_id"] == _OWNER_GATE_ID
    assert callable(dispatch_kwargs["producer_runtime_factory"])
    assert dispatch_kwargs["observation_registrar"] is not None
    assert callable(dispatch_kwargs["completion_handoff"].handoff_settled_completion)
    # The blocking F5c service and the inline presentation never ran.
    assert presentations == []
    assert submitter.submissions == []


def test_the_completion_handoff_submits_the_planning_review_after_a_plan_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The plan-result routes (issue #335): a settled initial-PLAN or REVISE
    result's committed revision is the only authority — the existing typed
    planning REVIEW round is submitted from committed state with the freshly
    reloaded post-revision token, for either plan family."""
    dependencies, submitter = make_queue_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=uuid4(),
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    for operation in (
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
    ):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=operation,
        )

    assert len(submitter.submissions) == 2
    for submitted in submitter.submissions:
        assert submitted.kind is OperationKind.TASK_PLANNING_REVIEW
        assert isinstance(submitted.payload, TaskPlanningReviewPayload)
        assert submitted.payload.task == _TASK_SCOPE_REVIEWER
        assert submitted.payload.profile_id == str(_PROFILE_ID)
        assert submitted.payload.expected_state_token == str(_NEXT_TOKEN)


def test_the_completion_handoff_plan_routes_are_noops_without_a_published_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A moved-on or revision-less durable state is never a continuation
    authority: the plan-result routes submit nothing."""
    dependencies, submitter = make_queue_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_STATE_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=None,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
    )

    assert submitter.submissions == []


def test_planning_bootstrap_composition_dispatches_the_initial_plan_detached_in_regular_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regular-mode bootstrap composition admits the initial PLAN as one
    detached turn and returns: no blocking production and no inline review
    submission happen in the dispatching worker (issue #335)."""
    dependencies, submitter = make_dependencies()
    dispatched = SimpleNamespace(task=None, send_claim=None, receipt=None, producer_session=None)
    dispatch = RecordingService(result=dispatched)
    monkeypatch.setattr(task_workflow, "dispatch_initial_plan_detached", dispatch, raising=True)
    prepare = RecordingService(result=fake_preparation())
    with patched_workflow_environment(monkeypatch, prepare=prepare):
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

    assert dispatch.call_count == 1
    dispatch_kwargs = dispatch.calls[0]["kwargs"]
    assert dispatch_kwargs["profile_id"] == _PROFILE_ID
    assert dispatch_kwargs["workspace_id"] == _WORKSPACE_ID
    assert dispatch_kwargs["task_id"] == _TASK_ID
    assert callable(dispatch_kwargs["producer_runtime_factory"])
    assert dispatch_kwargs["observation_registrar"] is not None
    assert callable(dispatch_kwargs["completion_handoff"].handoff_settled_completion)
    # No inline planning-review submission: the observed completion's
    # handoff owns it from committed state.
    assert submitter.submissions == []


def test_planning_review_composition_dispatches_the_review_detached_in_regular_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regular-mode planning-review composition (issue #336): the REVIEW
    dispatch is itself one detached agent turn — the reserved iteration, its
    detached-turn receipt companion, and the verbatim admission commit in the
    dispatch service, and the composition returns after the durable admission
    without any blocking send, in-process decision, or inline submission."""
    dependencies, submitter = make_dependencies()
    review_dispatch = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_NEXT_TOKEN))
    )
    monkeypatch.setattr(
        task_workflow, "dispatch_planning_review_detached", review_dispatch, raising=True
    )
    finalize = RecordingService(result=None)
    production = fake_production(_STATE_TOKEN)
    with patched_workflow_environment(monkeypatch, reloaded_plan_production=production):
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

        # The E3a detached dispatch ran against the reloaded production
        # subject with the composed deferred Reviewer attachment factory
        # (F5e), the process's observation registrar, and the ONE completion
        # handoff.
        assert review_dispatch.call_count == 1
        assert review_dispatch.calls[0]["kwargs"]["plan_production"] is production
        assert review_dispatch.calls[0]["kwargs"]["profile_id"] == _PROFILE_ID
        assert len(review_dispatch.calls[0]["args"]) == 2
        assert review_dispatch.calls[0]["kwargs"]["observation_registrar"] is not None
        assert callable(
            review_dispatch.calls[0]["kwargs"]["completion_handoff"].handoff_settled_completion
        )
        # No blocking decision runs in the composition and no next round is
        # submitted inline: the observed completion's settlement and the ONE
        # handoff own the continuation from committed state.
        assert finalize.call_count == 0
        assert submitter.submissions == []


def test_the_completion_handoff_review_route_presents_the_implementation_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The planning-REVIEW route (issue #336): the settled accepted review's
    committed state — a still-``planning`` Task with its accepted current
    plan revision and no current gate — is the only presentation authority;
    the exact-plan IMPLEMENTATION_AUTHORIZATION gate is presented (never
    approved) and no Producer prompt is sent."""
    from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome

    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=_PR_RECORD_ID,
        current_owner_gate_id=None,
    )
    loop = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        status=ReviewLoopStatus.OPEN,
    )
    accepted = SimpleNamespace(
        id=uuid4(), outcome=ReviewOutcome.ACCEPTED, plan_revision_id=_PR_RECORD_ID
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_task_review_loops",
        lambda _pool, *, task_id: [loop] if task_id == _TASK_ID else [],
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_review_loop_iterations",
        lambda _pool, *, review_loop_id: [accepted] if review_loop_id == loop.id else [],
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    presentations: list[dict[str, Any]] = []
    coordinated: list[dict[str, Any]] = []

    def recording_coordinate(pool: Any, **kwargs: Any) -> None:
        coordinated.append(dict(kwargs))
        kwargs["invoke_level_one"](InvocationAttemptEvidence())

    def recording_open_implementation(pool: Any, github: Any, **kwargs: Any) -> Any:
        presentations.append(dict(kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", recording_coordinate)
    monkeypatch.setattr(
        task_workflow, "open_implementation_authorization", recording_open_implementation
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )

    # The presentation ran under the F5b coordinator with the fresh
    # committed-state token seed and the durable Workspace Owner identity.
    assert coordinated[0]["expected_status"] is TaskStatus.PLANNING
    assert coordinated[0]["profile_id"] == _PROFILE_ID
    assert coordinated[0]["expected_state_token"] == _NEXT_TOKEN
    # The exact-plan gate was presented with the fresh committed-state token
    # and the durable Workspace Owner identity — presenting, never approving,
    # with the invocation's own attempt-evidence collector threaded through.
    assert presentations == [
        {
            "profile_id": _PROFILE_ID,
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "expected_state_token": _NEXT_TOKEN,
            "attempt_evidence": presentations[0]["attempt_evidence"],
        }
    ]
    assert presentations[0]["attempt_evidence"] is not None
    assert submitter.submissions == []


def test_the_completion_handoff_review_route_dispatches_the_revise_detached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The planning-REVIEW route (issue #336): an in-budget change request's
    persisted iteration authorizes one new detached Producer REVISE through
    the existing #335 service under the F5b coordinator — after which the
    plan-result handoff route submits the next typed REVIEW round."""
    from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome

    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=_PR_RECORD_ID,
        current_owner_gate_id=None,
    )
    loop = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        status=ReviewLoopStatus.OPEN,
    )
    changes = SimpleNamespace(
        id=uuid4(), outcome=ReviewOutcome.CHANGES_REQUESTED, plan_revision_id=_PR_RECORD_ID
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_task_review_loops",
        lambda _pool, *, task_id: [loop] if task_id == _TASK_ID else [],
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_review_loop_iterations",
        lambda _pool, *, review_loop_id: [changes] if review_loop_id == loop.id else [],
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    coordinated: list[dict[str, Any]] = []
    dispatches: list[dict[str, Any]] = []

    def recording_coordinate(pool: Any, **kwargs: Any) -> None:
        coordinated.append(dict(kwargs))
        kwargs["invoke_level_one"](InvocationAttemptEvidence())

    def recording_dispatch(pool: Any, github: Any, **kwargs: Any) -> Any:
        dispatches.append(dict(kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", recording_coordinate)
    monkeypatch.setattr(task_workflow, "dispatch_planning_revise_detached", recording_dispatch)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )

    # The detached REVISE dispatch ran under the F5b coordinator with the
    # persisted iteration as its authorization subject.
    assert coordinated[0]["expected_status"] is TaskStatus.PLANNING
    assert coordinated[0]["profile_id"] == _PROFILE_ID
    assert coordinated[0]["expected_state_token"] == _NEXT_TOKEN
    assert dispatches[0]["prior_plan_revision_id"] == _PR_RECORD_ID
    assert dispatches[0]["review_iteration_id"] == changes.id
    assert dispatches[0]["expected_state_token"] == _NEXT_TOKEN
    assert dispatches[0]["attempt_evidence"] is not None
    assert callable(dispatches[0]["producer_runtime_factory"])
    # The revise dispatch is durably admitted; the next REVIEW round is the


def test_the_completion_handoff_review_route_reports_a_failed_presentation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The accepted route's gate presentation runs under the F5b coordinator
    with the invocation's own attempt-evidence collector threaded through
    (issue #336): a detected presentation failure reaches the Level 2 triage
    under the presentation's own family with the accepted iteration as its
    durable attribution identity — the recorded report is verified here, and
    the coordinator re-raises the original error (its escalation disposition
    is the triage's own tested contract) instead of silently stranding the
    accepted review."""
    from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
    from openorc.services import task_workflow_operation_coordinator as coordinator_module

    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.PLANNING,
        current_plan_revision_id=_PR_RECORD_ID,
        current_owner_gate_id=None,
    )
    loop = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        status=ReviewLoopStatus.OPEN,
    )
    accepted = SimpleNamespace(
        id=uuid4(), outcome=ReviewOutcome.ACCEPTED, plan_revision_id=_PR_RECORD_ID
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_task_review_loops",
        lambda _pool, *, task_id: [loop] if task_id == _TASK_ID else [],
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_review_loop_iterations",
        lambda _pool, *, review_loop_id: [accepted] if review_loop_id == loop.id else [],
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    from openorc.domain.operational_failures import (
        ImplementationAuthorizationPresentationOperation,
        OriginalAttemptKind,
    )

    presentations: list[dict[str, Any]] = []

    def failing_open_implementation(pool: Any, github: Any, **kwargs: Any) -> Any:
        presentations.append(dict(kwargs))
        # The real service's Level 1 shape: the accepted iteration's identity
        # is recorded into the invocation's evidence before the external
        # source-requirements reconciliation can fail.
        evidence = kwargs["attempt_evidence"]
        assert evidence is not None
        evidence.record_task_token(_NEXT_TOKEN)
        evidence.record_attempt(
            family=ImplementationAuthorizationPresentationOperation.EXACT_PLAN_IMPLEMENTATION_AUTHORIZATION_PRESENTATION,
            original_attempt_kind=OriginalAttemptKind.REVIEW_ITERATION,
            original_attempt_id=accepted.id,
            phase="implementation_authorization_presentation",
        )
        raise ExternalOperationFailedError("the presentation reconciliation failed")

    monkeypatch.setattr(
        task_workflow, "open_implementation_authorization", failing_open_implementation
    )
    reports: list[Any] = []

    def recording_triage(_pool: Any, **kwargs: Any) -> Any:
        reports.append(kwargs)
        return SimpleNamespace(kind=SimpleNamespace(value="blocked"))

    monkeypatch.setattr(coordinator_module, "triage_operational_failure", recording_triage)
    # The coordinator's own identity verification reads the durable rows the
    # service's recorded identity names; the accepted iteration and its
    # current revision are the committed evidence.
    monkeypatch.setattr(
        coordinator_module.review_records,
        "get_review_iteration",
        lambda _pool, *, review_iteration_id: SimpleNamespace(
            id=review_iteration_id,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            outcome=ReviewOutcome.ACCEPTED,
            plan_revision_id=_PR_RECORD_ID,
        ),
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    with pytest.raises(ExternalOperationFailedError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
        )

    # The failing presentation was attempted exactly once with the collected
    # evidence, and the typed report reached the Level 2 triage under the
    # presentation's own family with the accepted iteration as the episode
    # root — the durable escalation disposition, never a silent stranding.
    assert len(presentations) == 1
    evidence = presentations[0]["attempt_evidence"]
    assert evidence is not None
    assert len(reports) == 1
    operation = reports[0]["operation"]
    assert operation.family.value == "exact_plan_implementation_authorization_presentation"
    assert operation.original_attempt_kind.value == "review_iteration"
    assert reports[0]["expected_status"] is TaskStatus.PLANNING
    assert submitter.submissions == []

    # plan-result handoff route's own submission, never an inline one here.
    assert submitter.submissions == []


def test_the_completion_handoff_review_route_ends_at_the_resolution_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exhausted or Owner-mediated change request's REVIEW_RESOLUTION gate
    stands as the Task's current gate: the handoff route sends no Producer
    prompt and the Owner-mediated resolution is not a workflow operation."""
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.WAITING_FOR_OWNER,
        current_plan_revision_id=_PR_RECORD_ID,
        current_owner_gate_id=_OWNER_GATE_ID,
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    presentations: list[Any] = []
    dispatches: list[Any] = []
    monkeypatch.setattr(
        task_workflow,
        "open_implementation_authorization",
        lambda *args, **kwargs: presentations.append(kwargs),
    )
    monkeypatch.setattr(
        task_workflow,
        "dispatch_planning_revise_detached",
        lambda *args, **kwargs: dispatches.append(kwargs),
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )

    assert presentations == []
    assert dispatches == []
    assert submitter.submissions == []


# --- the detached PR_COMPOSE dispatch and the settlement handoff route (#337) ---------


def test_pr_composition_composition_dispatches_detached_in_regular_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regular-mode composition admits the PR_COMPOSE turn detached and
    returns: no blocking composition, no publication, and no inline PR-review
    submission happen in the dispatching worker (issue #337)."""
    dependencies, submitter = make_dependencies()
    dispatched = SimpleNamespace(published=None)
    dispatch = RecordingService(result=dispatched)
    monkeypatch.setattr(task_workflow, "dispatch_pr_compose_detached", dispatch, raising=True)

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

    assert dispatch.call_count == 1
    recorded = dispatch.calls[0]["kwargs"]
    assert recorded["profile_id"] == _PROFILE_ID
    assert recorded["workspace_id"] == _WORKSPACE_ID
    assert recorded["task_id"] == _TASK_ID
    assert recorded["owner_gate_id"] == _OWNER_GATE_ID
    assert recorded["expected_state_token"] == _STATE_TOKEN
    assert recorded["observation_registrar"] is not None
    assert callable(recorded["completion_handoff"].handoff_settled_completion)
    assert submitter.submissions == []


def test_pr_composition_composition_submits_the_pr_review_on_the_continuation_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The narrow interrupted-publication continuation owns no detached turn:
    the dispatch finalizes it inline and the composition submits the exact
    typed PR review for the finalized publication (issue #337)."""
    dependencies, submitter = make_dependencies()
    published_token = _NEXT_TOKEN
    pull_request_id = uuid4()
    dispatched = SimpleNamespace(
        published=SimpleNamespace(
            task=SimpleNamespace(state_token=published_token),
            pull_request=SimpleNamespace(id=pull_request_id),
        )
    )
    dispatch = RecordingService(result=dispatched)
    monkeypatch.setattr(task_workflow, "dispatch_pr_compose_detached", dispatch, raising=True)

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

    assert dispatch.call_count == 1
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is OperationKind.TASK_PR_REVIEW
    assert isinstance(submitted.payload, TaskPrReviewPayload)
    assert submitted.payload.profile_id == str(_PROFILE_ID)
    assert submitted.payload.task_pull_request_id == str(pull_request_id)
    assert submitted.payload.expected_state_token == str(published_token)


def _settled_pr_claim() -> Any:
    return SimpleNamespace(
        kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
        status=TaskRuntimeSendClaimStatus.SETTLED,
        owner_gate_id=_OWNER_GATE_ID,
    )


def _waiting_task(*, status: TaskStatus = TaskStatus.WAITING_FOR_OWNER) -> Any:
    return SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_STATE_TOKEN,
        status=status,
        current_owner_gate_id=None,
        canonical_feature_branch="openorc/task-42-implementation",
    )


def test_the_completion_handoff_publishes_the_pr_compose_family_and_submits_the_pr_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settled PR_COMPOSE completion publishes through the owning service
    with the in-memory validated result and submits exactly one typed
    TASK_PR_REVIEW from committed state (issue #337)."""
    dependencies, submitter = make_queue_dependencies()
    task = _waiting_task()
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.send_claim_records,
        "list_task_runtime_send_claims",
        lambda _pool, *, task_id: [_settled_pr_claim()] if task_id == _TASK_ID else [],
    )
    pull_request_id = uuid4()
    published = SettledTaskPrPublication(
        task=cast(Any, SimpleNamespace(state_token=_NEXT_TOKEN)),
        pull_request=cast(Any, SimpleNamespace(id=pull_request_id)),
        approving_profile_id=_PROFILE_ID,
    )
    publication = RecordingService(result=published)
    monkeypatch.setattr(
        task_workflow, "publish_settled_task_pr_composition", publication, raising=True
    )
    validated = parse_formal_response(
        pr_result_candidate(title="t", body="b"), expected_family=PR_RESULT_FAMILY
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
        validated_result=validated,
    )

    assert publication.call_count == 1
    assert publication.calls[0]["kwargs"] == {
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "owner_gate_id": _OWNER_GATE_ID,
        "expected_state_token": _STATE_TOKEN,
        "pr_result": validated,
    }
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is OperationKind.TASK_PR_REVIEW
    assert isinstance(submitted.payload, TaskPrReviewPayload)
    assert submitted.payload.profile_id == str(_PROFILE_ID)
    assert submitted.payload.task_pull_request_id == str(pull_request_id)
    assert submitted.payload.expected_state_token == str(_NEXT_TOKEN)


def test_the_completion_handoff_pr_route_requires_the_validated_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publication cannot re-derive the v1-unstored title/body: a missing
    in-memory validated result fails closed before any read (issue #337)."""
    dependencies, submitter = make_queue_dependencies()

    def _fail_read(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no durable read may run without the validated result")

    monkeypatch.setattr(task_workflow.task_records, "get_task", _fail_read)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    with pytest.raises(ConflictError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
        )

    assert submitter.submissions == []


def test_the_completion_handoff_rejects_a_result_on_other_families(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the PR_COMPOSE family consumes an in-memory validated result;
    every other family's durable-state continuation rejects one fail-closed
    (issue #337)."""
    dependencies, submitter = make_queue_dependencies()
    validated = parse_formal_response(
        pr_result_candidate(title="t", body="b"), expected_family=PR_RESULT_FAMILY
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    with pytest.raises(ConflictError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            validated_result=validated,
        )

    assert submitter.submissions == []


def test_the_completion_handoff_pr_route_is_a_noop_without_a_settled_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A moved-on Task or a missing settled claim submits nothing: the durable
    state owns every other outcome (issue #337)."""
    dependencies, submitter = make_queue_dependencies()
    task = _waiting_task(status=TaskStatus.REVIEWING)
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.send_claim_records,
        "list_task_runtime_send_claims",
        lambda _pool, *, task_id: [],
    )
    validated = parse_formal_response(
        pr_result_candidate(title="t", body="b"), expected_family=PR_RESULT_FAMILY
    )

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
        validated_result=validated,
    )

    assert submitter.submissions == []


# --- the detached PR-review cycle (issue #338) -----------------------------------------


def test_pr_review_composition_dispatches_detached_in_regular_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regular-mode PR-review composition (issue #338): the REVIEW
    dispatch is itself one detached agent turn — the reserved PR/head
    iteration, its detached-turn receipt companion, and the verbatim admission
    commit in the dispatch service, and the composition returns after the
    durable admission without any blocking send, in-process decision, or
    inline submission."""
    dependencies, submitter = make_dependencies()
    review_dispatch = RecordingService(
        result=SimpleNamespace(task=SimpleNamespace(state_token=_NEXT_TOKEN))
    )
    monkeypatch.setattr(task_workflow, "dispatch_pr_review_detached", review_dispatch, raising=True)
    finalize = RecordingService(result=None)
    published = fake_published(_STATE_TOKEN)
    with patched_workflow_environment(monkeypatch, reloaded_published=published):
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

    # The E9 detached dispatch ran against the reloaded published subject with
    # the composed deferred Reviewer attachment factory, the process's
    # observation registrar, and the ONE completion handoff.
    assert review_dispatch.call_count == 1
    assert review_dispatch.calls[0]["kwargs"]["profile_id"] == _PROFILE_ID
    assert review_dispatch.calls[0]["kwargs"]["expected_state_token"] == (
        published.task.state_token
    )
    assert len(review_dispatch.calls[0]["args"]) == 3
    assert review_dispatch.calls[0]["kwargs"]["observation_registrar"] is not None
    assert callable(
        review_dispatch.calls[0]["kwargs"]["completion_handoff"].handoff_settled_completion
    )
    # No blocking decision runs in the composition and nothing is submitted
    # inline: the observed completion's settlement and the ONE handoff own the
    # continuation from committed state.
    assert finalize.call_count == 0
    assert submitter.submissions == []


def _pr_review_handoff_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    task: Any,
    loops: list[Any],
    iterations: list[Any],
    executions: list[Any] | None = None,
) -> Any:

    dependencies, submitter = make_dependencies()
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_task_review_loops",
        lambda _pool, *, task_id: loops if task_id == _TASK_ID else [],
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_review_loop_iterations",
        lambda _pool, *, review_loop_id: iterations if loops else [],
    )
    monkeypatch.setattr(
        task_workflow.execution_records,
        "list_task_executions",
        lambda _pool, *, task_id: executions or [],
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )
    return dependencies, submitter


def test_the_pr_review_handoff_dispatches_the_pr_change_request_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The PR-review handoff route (issue #338): an in-budget CHANGES_REQUESTED
    PR/head decision's committed state — the REVIEWING Task with no current
    gate, the OPEN PR loop, its latest finalized PR/head iteration, and no
    claimed remediation Execution — is the only remediation authority; the
    detached E9 enqueue runs on the exact Task checkout under the F5b
    coordinator and nothing is submitted inline."""
    from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome

    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.REVIEWING,
        current_owner_gate_id=None,
        canonical_feature_branch="feat/branch",
    )
    loop = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        status=ReviewLoopStatus.OPEN,
    )
    changes_requested = SimpleNamespace(
        id=_PR_RECORD_ID,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        task_pull_request_id=_PR_RECORD_ID,
        reviewed_head_sha="a" * 40,
    )
    dependencies, submitter = _pr_review_handoff_environment(
        monkeypatch,
        task=task,
        loops=[loop],
        iterations=[changes_requested],
        executions=[],
    )
    dispatch = RecordingService(result=SimpleNamespace(task=task))
    monkeypatch.setattr(task_workflow, "dispatch_pr_remediation_detached", dispatch, raising=True)

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )

    assert dispatch.call_count == 1
    dispatch_kwargs = dispatch.calls[0]["kwargs"]
    assert dispatch_kwargs["profile_id"] == _PROFILE_ID
    assert dispatch_kwargs["workspace_id"] == _WORKSPACE_ID
    assert dispatch_kwargs["task_id"] == _TASK_ID
    assert dispatch_kwargs["expected_state_token"] == _NEXT_TOKEN
    assert dispatch_kwargs["addressed_iteration"] is changes_requested
    assert callable(dispatch_kwargs["producer_runtime_factory"])
    assert dispatch_kwargs["observation_registrar"] is not None
    assert callable(dispatch_kwargs["completion_handoff"].handoff_settled_completion)
    assert submitter.submissions == []


def test_the_pr_review_handoff_is_a_noop_for_an_already_claimed_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one-shot E9 claim association: an iteration whose remediation
    Execution is already claimed is owned by its own durable attempt — the
    handoff dispatches nothing and permits no blind re-enqueue."""
    from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome

    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.REVIEWING,
        current_owner_gate_id=None,
    )
    loop = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        status=ReviewLoopStatus.OPEN,
    )
    changes_requested = SimpleNamespace(
        id=_PR_RECORD_ID,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        task_pull_request_id=_PR_RECORD_ID,
        reviewed_head_sha="a" * 40,
    )
    claimed = SimpleNamespace(review_iteration_id=_PR_RECORD_ID)
    dependencies, submitter = _pr_review_handoff_environment(
        monkeypatch,
        task=task,
        loops=[loop],
        iterations=[changes_requested],
        executions=[claimed],
    )
    dispatch = RecordingService(result=None)
    monkeypatch.setattr(task_workflow, "dispatch_pr_remediation_detached", dispatch, raising=True)

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )

    assert dispatch.call_count == 0
    assert submitter.submissions == []


def test_the_pr_review_handoff_is_a_noop_for_gate_owned_review_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ACCEPTED MERGE_DECISION and the exhausted/sticky REVIEW_RESOLUTION
    gates own their Owner seams: a Task carrying a current owner gate receives
    no Producer prompt and no remediation dispatch."""
    from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome

    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.REVIEWING,
        current_owner_gate_id=uuid4(),
    )
    loop = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        status=ReviewLoopStatus.OPEN,
    )
    changes_requested = SimpleNamespace(
        id=_PR_RECORD_ID,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        task_pull_request_id=_PR_RECORD_ID,
        reviewed_head_sha="a" * 40,
    )
    dependencies, submitter = _pr_review_handoff_environment(
        monkeypatch,
        task=task,
        loops=[loop],
        iterations=[changes_requested],
    )
    dispatch = RecordingService(result=None)
    monkeypatch.setattr(task_workflow, "dispatch_pr_remediation_detached", dispatch, raising=True)

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )

    assert dispatch.call_count == 0
    assert submitter.submissions == []


def test_the_pr_remediation_handoff_submits_the_pr_review_for_the_confirmed_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The E9-remediation handoff route (issue #338): the settled round's
    owning verification under the F5b coordinator proves the recorded branch
    claim and the GitHub-confirmed new head, and only that confirmed head
    submits the existing typed TASK_PR_REVIEW through F1 with the re-derived
    canonical PR identity and post-reconciliation token."""
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.REVIEWING,
        current_owner_gate_id=None,
    )
    continuation = SimpleNamespace(
        task=task,
        pull_request=SimpleNamespace(id=_PR_RECORD_ID),
        addressed_iteration=SimpleNamespace(id=uuid4()),
        finalized_execution=SimpleNamespace(id=uuid4()),
        new_head_sha="d" * 40,
    )
    verify = RecordingService(result=continuation)
    monkeypatch.setattr(task_workflow, "verify_settled_pr_remediation_push", verify, raising=True)
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
    )

    assert verify.call_count == 1
    assert verify.calls[0]["kwargs"]["expected_state_token"] == _NEXT_TOKEN
    assert len(submitter.submissions) == 1
    submitted = submitter.submissions[0]
    assert submitted.kind is OperationKind.TASK_PR_REVIEW
    assert isinstance(submitted.payload, TaskPrReviewPayload)
    assert submitted.payload.profile_id == str(_PROFILE_ID)
    assert submitted.payload.task_pull_request_id == str(_PR_RECORD_ID)
    assert submitted.payload.expected_state_token == str(_NEXT_TOKEN)


def test_the_pr_remediation_handoff_supplies_the_terminal_finalization_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The E9-remediation handoff route forwards the executing process's
    F1-backed terminal-finalization submission into the owning verification,
    so a confirmed closed-unmerged cancellation detected there arranges the
    normal bounded checkout cleanup instead of the archive-only fallback
    (issue #338 review)."""
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.REVIEWING,
        current_owner_gate_id=None,
    )
    verify_calls: list[dict[str, Any]] = []

    def _verify_stub(*args: Any, **kwargs: Any) -> Any:
        # A dedicated recorder: the shared RecordingService deliberately
        # excludes the optional process-local submission plumbing from its
        # recorded comparisons, and this test's subject is exactly that
        # threading.
        verify_calls.append(kwargs)
        return None

    monkeypatch.setattr(
        task_workflow, "verify_settled_pr_remediation_push", _verify_stub, raising=True
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
    )

    assert len(verify_calls) == 1
    forwarded = verify_calls[0]["terminal_finalization_submission"]
    assert isinstance(forwarded, task_workflow.F1TerminalFinalizationSubmission)
    # The forwarded seam is live: it submits the bounded terminal-finalization
    # operation through the executing process's continuation submitter.
    forwarded.submit(workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)
    assert submitter.submissions[-1].kind is OperationKind.TASK_TERMINAL_FINALIZATION


def test_the_pr_remediation_handoff_submits_nothing_without_a_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A verification that owns no continuation (nothing settled, a moved-on
    subject, or an already-continued head) submits nothing and never permits a
    blind re-enqueue — the durable state owns recovery."""
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.REVIEWING,
        current_owner_gate_id=None,
    )
    verify = RecordingService(result=None)
    monkeypatch.setattr(task_workflow, "verify_settled_pr_remediation_push", verify, raising=True)
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        task_workflow,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=_PROFILE_ID),
    )

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
    )

    assert verify.call_count == 1
    assert submitter.submissions == []


def test_the_pr_remediation_handoff_is_a_noop_for_moved_on_subjects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settled remediation's durable continuations are the only authority:
    a moved-on Task (a gate, a decision, or the Owner Resume routing) receives
    no verification call and no submission."""
    dependencies, submitter = make_dependencies()
    task = SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        state_token=_NEXT_TOKEN,
        status=TaskStatus.WAITING_FOR_OWNER,
        current_owner_gate_id=uuid4(),
    )
    verify = RecordingService(result=None)
    monkeypatch.setattr(task_workflow, "verify_settled_pr_remediation_push", verify, raising=True)
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: task if task_id == _TASK_ID else None,
    )

    task_workflow.build_detached_completion_handoff(dependencies).handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
    )

    assert verify.call_count == 0
    assert submitter.submissions == []


def test_the_completion_handoff_terminal_cleanup_route_invokes_the_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The TERMINAL_CLEANUP route (issue #345) re-invokes the ONE idempotent
    terminal finalization with the executing process's explicit dispatch
    selection: the one-shot claim already settled atomically with the settled
    receipt, so the finalization re-derives archive-only — never a
    redispatch, never a second claim."""
    dependencies, submitter = make_dependencies(
        terminal_cleanup_runtime_provider=lambda _settings: object()
    )
    calls: list[dict[str, Any]] = []

    def recording_finalize(pool: Any, runtime: Any, **kwargs: Any) -> Any:
        calls.append({"pool": pool, "runtime": runtime, **kwargs})
        return SimpleNamespace(kind="archived", task=None, released_session_bindings=())

    monkeypatch.setattr(task_workflow, "finalize_task_terminalization", recording_finalize)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP,
    )

    assert len(calls) == 1
    assert calls[0]["workspace_id"] == _WORKSPACE_ID
    assert calls[0]["task_id"] == _TASK_ID
    # The explicit selection is resolved from the executing process's own
    # execution mode — never inferred, never a blocking fallback.
    dispatch = calls[0]["dispatch"]
    assert dispatch is not None
    assert dispatch.selection.value in {
        "detached_regular",
        "observation_unavailable",
    }
    assert submitter.submissions == []


def test_the_completion_handoff_terminal_cleanup_route_wakes_a_real_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, submitter = make_dependencies(
        terminal_cleanup_runtime_provider=lambda _settings: object()
    )
    calls: list[dict[str, Any]] = []

    def recording_finalize(pool: Any, runtime: Any, **kwargs: Any) -> Any:
        calls.append({"pool": pool, "runtime": runtime, **kwargs})
        return SimpleNamespace(
            kind="archived", task=None, released_session_bindings=(object(), object())
        )

    monkeypatch.setattr(task_workflow, "finalize_task_terminalization", recording_finalize)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP,
    )

    # The settlement-driven re-finalization released occupancy, so the route
    # arranges exactly one post-commit Task-queue wake (issue #257).
    assert [entry.kind for entry in submitter.submissions] == [OperationKind.TASK_QUEUE_WAKE]
    wake_payload = submitter.submissions[0].payload
    assert isinstance(wake_payload, QueueWakePayload)
    assert wake_payload.workspace_id == str(_WORKSPACE_ID)


def test_the_completion_handoff_terminal_cleanup_route_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The route propagates like the completion routes it sits beside; the
    containment is the settlement-observing callers' contract (the observation
    host and the dispatching acknowledgement attach), never the route's own."""
    dependencies, submitter = make_dependencies(
        terminal_cleanup_runtime_provider=lambda _settings: object()
    )

    def failing_finalize(_pool: Any, _runtime: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("the archival failed")

    monkeypatch.setattr(task_workflow, "finalize_task_terminalization", failing_finalize)

    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    with pytest.raises(RuntimeError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP,
        )
    assert submitter.submissions == []
