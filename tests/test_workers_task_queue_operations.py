"""Deterministic tests for the F11 Task-queue wake operation composition (issue #257).

The registered Task-queue wake F1 handler invokes the ONE bounded FIFO
admission processor with process-local dependencies and, strictly after the
processor's committed admissions, submits exactly one current-token
planning-bootstrap continuation per admitted candidate plus one bounded
continuation wake when the sweep reached its candidate budget. The
submission bridge maps the F1 submission outcomes 1:1 onto the shared wake
outcome vocabulary, and the bounded post-restart recovery derives its
candidates from the persisted queued/unclaimed-bootstrap facts alone,
re-proving every candidate before any submission. No live Postgres, Valkey,
GitHub, or Cline is touched.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from openorc.config import ExecutionMode, Settings
from openorc.domain.tasks import Task, TaskStatus
from openorc.services.task_queue_wake import TaskQueueWakeSubmissionOutcome
from openorc.services.task_start import (
    QueuedCandidateDisposition,
    QueuedCandidateOutcome,
    QueuedSweepOutcome,
)
from openorc.workers import invocation, task_queue_operations
from openorc.workers.invocation import (
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
    TaskPlanningBootstrapAndInitialPlanPayload,
    decode_operation_invocation,
)
from openorc.workers.task_queue_operations import (
    F1TaskQueueWakeSubmission,
    build_task_queue_operation_handlers,
)

_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_CANDIDATE_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_POST_CLAIM_TOKEN = uuid.uuid4()
_QUEUED_AT = datetime(2026, 11, 21, 23, 0, 0, tzinfo=UTC)
_NOW = datetime(2026, 11, 21, 23, 30, 0, tzinfo=UTC)


def _settings(*, execution_mode: ExecutionMode = ExecutionMode.REGULAR) -> Settings:
    return Settings(
        environment="test",
        api_host="127.0.0.1",
        api_port=3999,
        api_reload=False,
        execution_mode=execution_mode,
        valkey_url="redis://127.0.0.1:6379/0",
        database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        db_pool_min=1,
        db_pool_max=5,
        db_pool_timeout=5.0,
        github_app_id=1234,
        github_app_private_key="not-a-real-key",
    )


@dataclass
class _RecordingSubmitter:
    """Recording F1 submitter double with one scripted outcome."""

    outcome: SubmissionResult = field(default_factory=SubmissionResult.accepted)
    submissions: list[OperationInvocation] = field(default_factory=list)

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        self.submissions.append(invocation)
        return self.outcome


def _dependencies(submitter: _RecordingSubmitter) -> Any:
    pool: Any = object()
    github: Any = object()
    return OperationDependencies(
        settings=_settings(),
        database_pool_provider=lambda _settings: pool,
        github_client_provider=lambda _settings: github,
        continuation_submitter_provider=lambda _settings: submitter,
    )


def _task(*, task_id: Any = _CANDIDATE_ID, status: TaskStatus = TaskStatus.PLANNING) -> Task:
    return Task(
        id=task_id,
        workspace_id=_WORKSPACE_ID,
        repository_id=uuid.uuid4(),
        github_issue_id=503,
        github_issue_number=42,
        status=status,
        archived_at=None,
        queued_at=None if status is not TaskStatus.QUEUED else _QUEUED_AT,
        canonical_feature_branch=None,
        state_token=_POST_CLAIM_TOKEN if status is TaskStatus.PLANNING else _STATE_TOKEN,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint="a" * 64,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _admitted_outcome(*, task_id: Any = _CANDIDATE_ID) -> QueuedCandidateOutcome:
    return QueuedCandidateOutcome(
        task_id=task_id,
        github_issue_number=42,
        disposition=QueuedCandidateDisposition.ADMITTED,
        admitted_task=_task(task_id=task_id),
        owner_profile_id=_OWNER_PROFILE_ID,
    )


def _sweep(
    *outcomes: QueuedCandidateOutcome,
    exhausted: bool = True,
    continuation_cursor: tuple[datetime, uuid.UUID] | None = None,
) -> QueuedSweepOutcome:
    return QueuedSweepOutcome(
        outcomes=outcomes,
        exhausted=exhausted,
        continuation_cursor=continuation_cursor,
    )


class _SweepHarness:
    """Scripted FIFO-processor double recording every wake's sweep call."""

    def __init__(self) -> None:
        self.scripted: list[QueuedSweepOutcome] = []
        self.calls: list[dict[str, Any]] = []

    def __call__(self, pool: Any, *, workspace_id: Any, **kwargs: Any) -> QueuedSweepOutcome:
        self.calls.append({"pool": pool, "workspace_id": workspace_id, **kwargs})
        return self.scripted.pop(0)


@pytest.fixture
def sweep_harness(monkeypatch: pytest.MonkeyPatch) -> _SweepHarness:
    """Patch the FIFO processor at the composition-module boundary."""
    harness = _SweepHarness()
    monkeypatch.setattr(task_queue_operations, "reevaluate_queued_tasks", harness)
    return harness


def _bootstrap_invocation(
    *, task_id: Any = _CANDIDATE_ID, token: Any = _POST_CLAIM_TOKEN
) -> OperationInvocation:
    return OperationInvocation(
        kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
        payload=TaskPlanningBootstrapAndInitialPlanPayload(
            task=invocation.TaskScope(
                workspace_id=str(_WORKSPACE_ID),
                task_id=str(task_id),
                role=invocation.WorkflowRoleRef.PRODUCER,
            ),
            profile_id=str(_OWNER_PROFILE_ID),
            expected_state_token=str(token),
        ),
    )


def _wake_invocation(
    *, after_queued_at: str | None = None, after_task_id: str | None = None
) -> OperationInvocation:
    return OperationInvocation(
        kind=OperationKind.TASK_QUEUE_WAKE,
        payload=invocation.QueueWakePayload(
            workspace_id=str(_WORKSPACE_ID),
            after_queued_at=after_queued_at,
            after_task_id=after_task_id,
        ),
    )


# --- the wake handler ---


def test_the_wake_handler_submits_exactly_one_current_token_bootstrap(
    sweep_harness: _SweepHarness,
) -> None:
    submitter = _RecordingSubmitter()
    sweep_harness.scripted.append(_sweep(_admitted_outcome()))
    dependencies = _dependencies(submitter)

    task_queue_operations._handle_task_queue_wake(
        _wake_invocation().payload, dependencies=dependencies
    )

    # The ONE sweep ran with the decoded workspace scope and no cursor.
    assert sweep_harness.calls == [
        {
            "pool": dependencies.database_pool(),
            "workspace_id": _WORKSPACE_ID,
            "after_queued_at": None,
            "after_task_id": None,
        }
    ]
    # Exactly one post-commit planning-bootstrap continuation, composed from
    # the admitted outcome's exact post-claim facts (issue #257).
    assert submitter.submissions == [_bootstrap_invocation()]
    decoded = decode_operation_invocation(submitter.submissions[0].encode())
    assert decoded == _bootstrap_invocation()


def test_an_exhausted_sweep_arranges_no_continuation_wake(
    sweep_harness: _SweepHarness,
) -> None:
    submitter = _RecordingSubmitter()
    sweep_harness.scripted.append(_sweep(_admitted_outcome()))
    dependencies = _dependencies(submitter)

    task_queue_operations._handle_task_queue_wake(
        _wake_invocation().payload, dependencies=dependencies
    )

    assert [entry.kind for entry in submitter.submissions] == [
        OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    ]


def test_a_budget_bounded_sweep_arranges_one_bounded_continuation_wake(
    sweep_harness: _SweepHarness,
) -> None:
    submitter = _RecordingSubmitter()
    sweep_harness.scripted.append(
        _sweep(
            _admitted_outcome(),
            exhausted=False,
            continuation_cursor=(_QUEUED_AT, _CANDIDATE_ID),
        )
    )
    dependencies = _dependencies(submitter)

    task_queue_operations._handle_task_queue_wake(
        _wake_invocation().payload, dependencies=dependencies
    )

    # The bootstrap continuation plus exactly one bounded continuation wake
    # past the examined tail (issue #257).
    assert submitter.submissions == [
        _bootstrap_invocation(),
        _wake_invocation(after_queued_at=_QUEUED_AT.isoformat(), after_task_id=str(_CANDIDATE_ID)),
    ]


def test_a_duplicate_wake_never_duplicates_the_bootstrap(
    sweep_harness: _SweepHarness,
) -> None:
    submitter = _RecordingSubmitter()
    sweep_harness.scripted.append(_sweep(_admitted_outcome()))
    dependencies = _dependencies(submitter)
    task_queue_operations._handle_task_queue_wake(
        _wake_invocation().payload, dependencies=dependencies
    )
    assert len(submitter.submissions) == 1

    # The replayed wake's sweep observes the candidate already admitted.
    sweep_harness.scripted.append(
        _sweep(
            QueuedCandidateOutcome(
                task_id=_CANDIDATE_ID,
                github_issue_number=42,
                disposition=QueuedCandidateDisposition.NO_LONGER_QUEUED,
            )
        )
    )
    task_queue_operations._handle_task_queue_wake(
        _wake_invocation().payload, dependencies=dependencies
    )

    # No second admission, no duplicated bootstrap work.
    assert submitter.submissions == [_bootstrap_invocation()]


def test_a_refused_bootstrap_continuation_is_contained(
    sweep_harness: _SweepHarness,
) -> None:
    submitter = _RecordingSubmitter(
        outcome=SubmissionResult(
            outcome=SubmissionOutcome.KNOWN_REFUSED,
            refusal_reason=SubmissionRefusalReason.EXECUTOR_SATURATED,
        )
    )
    sweep_harness.scripted.append(_sweep(_admitted_outcome()))
    dependencies = _dependencies(submitter)

    # The refused submission never fails the already-committed admission and
    # is never retried here: the durable state owns recovery.
    task_queue_operations._handle_task_queue_wake(
        _wake_invocation().payload, dependencies=dependencies
    )

    assert len(submitter.submissions) == 1


def test_an_admitted_outcome_without_post_claim_facts_fails_closed(
    sweep_harness: _SweepHarness,
) -> None:
    submitter = _RecordingSubmitter()
    sweep_harness.scripted.append(
        _sweep(
            QueuedCandidateOutcome(
                task_id=_CANDIDATE_ID,
                github_issue_number=42,
                disposition=QueuedCandidateDisposition.ADMITTED,
            )
        )
    )
    dependencies = _dependencies(submitter)

    with pytest.raises(InvalidInvocationError, match="post-claim facts"):
        task_queue_operations._handle_task_queue_wake(
            _wake_invocation().payload, dependencies=dependencies
        )

    assert submitter.submissions == []


def test_the_wake_handler_table_registers_the_wake_kind() -> None:
    handlers = build_task_queue_operation_handlers()

    assert set(handlers) == {OperationKind.TASK_QUEUE_WAKE}


# --- the wake submission bridge ---


@pytest.mark.parametrize(
    ("f1_outcome", "expected"),
    [
        (
            SubmissionOutcome.ACCEPTED_FOR_DELIVERY,
            TaskQueueWakeSubmissionOutcome.ACCEPTED_FOR_DELIVERY,
        ),
        (SubmissionOutcome.KNOWN_REFUSED, TaskQueueWakeSubmissionOutcome.KNOWN_REFUSED),
        (SubmissionOutcome.UNCERTAIN, TaskQueueWakeSubmissionOutcome.UNCERTAIN),
    ],
)
def test_the_wake_bridge_maps_f1_outcomes_one_to_one(
    f1_outcome: SubmissionOutcome, expected: TaskQueueWakeSubmissionOutcome
) -> None:
    submitter = _RecordingSubmitter(outcome=SubmissionResult(outcome=f1_outcome))
    bridge = F1TaskQueueWakeSubmission(submitter)

    outcome = bridge.submit(workspace_id=_WORKSPACE_ID)

    assert outcome is expected
    assert submitter.submissions == [_wake_invocation()]


def test_a_submission_error_is_contained_as_an_uncertain_arrangement() -> None:
    class _ExplodingSubmitter:
        def submit(self, operation_invocation: OperationInvocation) -> SubmissionResult:
            raise RuntimeError("transport down")

    bridge = F1TaskQueueWakeSubmission(_ExplodingSubmitter())  # type: ignore[arg-type]

    # The containment lives at the arranging seam: a submission error is an
    # unknown outcome — never a known non-delivery, never a replay — and the
    # committed capacity-relevant state is authoritative either way.
    outcome = task_queue_operations.arrange_task_queue_wake(
        bridge, workspace_id=_WORKSPACE_ID, origin="test"
    )

    assert outcome is TaskQueueWakeSubmissionOutcome.UNCERTAIN


def test_the_bridge_wire_form_strictly_round_trips() -> None:
    submitter = _RecordingSubmitter()
    bridge = F1TaskQueueWakeSubmission(submitter)

    bridge.submit(workspace_id=_WORKSPACE_ID)

    decoded = decode_operation_invocation(submitter.submissions[0].encode())
    assert decoded == _wake_invocation()


# --- the bounded post-restart recovery ---


class SimpleWorkspace:
    def __init__(self, *, owner_profile_id: Any) -> None:
        self.owner_profile_id = owner_profile_id


class _RecoveryHarness:
    """Scripted recovery enumeration, classification, and workspace reads."""

    def __init__(self) -> None:
        self.workspaces: list[uuid.UUID] = []
        self.unclaimed: list[Task] = []
        self.classified: list[uuid.UUID] = []
        self.classification_result: Task | None = None
        self.workspace: Any = SimpleWorkspace(owner_profile_id=_OWNER_PROFILE_ID)

    def classifier(self, pool: Any, *, workspace_id: Any, task_id: Any) -> Task | None:
        self.classified.append(task_id)
        return self.classification_result


@pytest.fixture
def recovery_harness(monkeypatch: pytest.MonkeyPatch) -> _RecoveryHarness:
    """Patch the recovery enumerations and classification seams."""
    harness = _RecoveryHarness()
    monkeypatch.setattr(
        task_queue_operations,
        "list_task_queue_recovery_workspaces",
        lambda pool, *, limit: harness.workspaces[:limit],
    )
    monkeypatch.setattr(
        task_queue_operations,
        "list_unclaimed_planning_bootstrap_tasks",
        lambda pool, *, limit: harness.unclaimed[:limit],
    )
    monkeypatch.setattr(
        task_queue_operations, "find_unclaimed_bootstrap_handoff", harness.classifier
    )
    monkeypatch.setattr(
        task_queue_operations, "get_workspace", lambda pool, *, workspace_id: harness.workspace
    )
    return harness


def test_recovery_arranges_wakes_and_the_unclaimed_bootstrap_handoff(
    recovery_harness: _RecoveryHarness,
) -> None:
    submitter = _RecordingSubmitter()
    recovery_harness.workspaces = [_WORKSPACE_ID]
    recovery_harness.unclaimed = [_task()]
    recovery_harness.classification_result = _task()
    dependencies = _dependencies(submitter)

    task_queue_operations.arrange_task_queue_recovery(
        dependencies.database_pool(),
        submitter,  # type: ignore[arg-type]
    )

    # One wake invitation per queued-workspace subject and exactly one
    # re-proven bootstrap handoff for the unclaimed planning subject.
    assert submitter.submissions == [_wake_invocation(), _bootstrap_invocation()]


def test_recovery_skips_a_candidate_whose_durable_facts_moved_on(
    recovery_harness: _RecoveryHarness,
) -> None:
    submitter = _RecordingSubmitter()
    recovery_harness.unclaimed = [_task()]
    recovery_harness.classification_result = None
    dependencies = _dependencies(submitter)

    task_queue_operations.arrange_task_queue_recovery(
        dependencies.database_pool(),
        submitter,  # type: ignore[arg-type]
    )

    # The enumerated subject was re-proven and no longer proves the unclaimed
    # handoff: the recovery submits nothing for it.
    assert submitter.submissions == []


def test_a_refused_recovery_arrangement_is_contained(
    recovery_harness: _RecoveryHarness,
) -> None:
    submitter = _RecordingSubmitter(
        outcome=SubmissionResult(
            outcome=SubmissionOutcome.KNOWN_REFUSED,
            refusal_reason=SubmissionRefusalReason.QUEUE_UNAVAILABLE,
        )
    )
    recovery_harness.workspaces = [_WORKSPACE_ID]
    recovery_harness.unclaimed = [_task()]
    recovery_harness.classification_result = _task()
    dependencies = _dependencies(submitter)

    # A refused or uncertain recovery arrangement never fails the pass and
    # never replays; the next supported trigger revisits the rest.
    task_queue_operations.arrange_task_queue_recovery(
        dependencies.database_pool(),
        submitter,  # type: ignore[arg-type]
    )

    assert [entry.kind for entry in submitter.submissions] == [
        OperationKind.TASK_QUEUE_WAKE,
        OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
    ]
