"""Deterministic tests for the F10 post-terminal checkout cleanup (issue #256).

The execution service rechecks every current terminal, identity, and safety
fact from durable state before any runtime effect, targets ONLY the exact
checkout derived from the original bound Producer snapshot and the persisted
Task identity, and contains every runtime/control failure as a bounded
in-memory disposition — never persistence, retry, escalation, or a change to
the archived Task. The submission helper contains refused, uncertain, and
failing arrangements exactly as accepted ones. No live Postgres, GitHub,
Valkey, or Cline is touched; the runtime seam is a recording fake, and the
shared snapshot-recovery derivation is imported the same way the production
service imports it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.intents import TaskInterruptIntentStatus
from openorc.domain.runtime_requests import RuntimeRequestStatus
from openorc.domain.send_claims import TaskRuntimeSendClaimStatus
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import TaskStatus
from openorc.protocol.interaction import CleanupWorkspaceInteraction
from openorc.protocol.models import WorkspaceResult
from openorc.services import task_terminal_cleanup
from openorc.services.errors import ApplicationError
from openorc.services.task_terminal_cleanup import (
    TerminalCheckoutCleanupDisposition,
    TerminalCheckoutCleanupRuntime,
    TerminalCheckoutCleanupSubmissionOutcome,
    execute_terminal_checkout_cleanup,
    submit_terminal_checkout_cleanup,
)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")
_REPOSITORY_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_ORIGINAL_BASE = "/srv/runtime/repositories"
_EXTERNAL_SESSION_ID = "producer-external-session"
_OWNER_LOGIN = "octocat"
_REPOSITORY_NAME = "admitted-repo"


def _archived_task(*, status: TaskStatus = TaskStatus.COMPLETED) -> SimpleNamespace:
    return SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_number=4311,
        status=status,
        archived_at=SimpleNamespace(year=2026),
    )


def _ready_producer_binding() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=WorkflowRole.PRODUCER,
        external_session_id=_EXTERNAL_SESSION_ID,
        lifecycle_status=TaskSessionLifecycleStatus.READY,
        effective_config_snapshot={
            "adapter": "cline",
            "configuration": {"repository_base": _ORIGINAL_BASE},
        },
    )


def _repository() -> SimpleNamespace:
    return SimpleNamespace(
        id=_REPOSITORY_ID,
        workspace_id=_WORKSPACE_ID,
        metadata=SimpleNamespace(owner_login=_OWNER_LOGIN, name=_REPOSITORY_NAME),
    )


@dataclass
class _RecordingRuntime(TerminalCheckoutCleanupRuntime):
    """The scripted cleanup runtime seam: records and returns one outcome."""

    results: list[Any] = field(default_factory=list)
    calls: list[dict[str, Any]] = field(default_factory=list)

    def cleanup_checkout(
        self,
        pool: Any,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        working_directory: str,
        interaction: CleanupWorkspaceInteraction,
    ) -> WorkspaceResult:
        self.calls.append(
            {
                "pool": pool,
                "workspace_id": workspace_id,
                "task_id": task_id,
                "external_session_id": external_session_id,
                "working_directory": working_directory,
                "interaction": interaction,
            }
        )
        outcome = self.results[min(len(self.calls) - 1, len(self.results) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@dataclass
class _RecordFacts:
    """Scripted durable reads behind the execution service's rechecks."""

    task: Any = None
    binding: Any = None
    original_base: str | None = _ORIGINAL_BASE
    repository: Any = None
    interrupt_intents: list[Any] = field(default_factory=list)
    open_resume_intents: list[Any] = field(default_factory=list)
    executions: list[Any] = field(default_factory=list)
    runtime_requests: list[Any] = field(default_factory=list)
    send_claims: list[Any] = field(default_factory=list)


def _install_facts(monkeypatch: pytest.MonkeyPatch, facts: _RecordFacts) -> None:
    """Patch the service's durable reads at the persistence-module boundary."""

    def _get_task(_pool: Any, task_id: UUID) -> Any:
        assert task_id == _TASK_ID
        return facts.task

    def _get_binding(_pool: Any, *, task_id: UUID, role: WorkflowRole) -> Any:
        assert task_id == _TASK_ID
        assert role is WorkflowRole.PRODUCER
        return facts.binding

    def _snapshot_base(_pool: Any, binding: Any, *, workspace_id: UUID) -> str:
        assert workspace_id == _WORKSPACE_ID
        if facts.original_base is None:
            raise ApplicationError("no applicable producer snapshot")
        return facts.original_base

    def _get_repository(_pool: Any, repository_id: UUID) -> Any:
        assert repository_id == _REPOSITORY_ID
        return facts.repository

    monkeypatch.setattr(task_terminal_cleanup.task_records, "get_task", _get_task)
    monkeypatch.setattr(
        task_terminal_cleanup.session_records, "get_task_agent_session", _get_binding
    )
    monkeypatch.setattr(task_terminal_cleanup, "_producer_snapshot_repository_base", _snapshot_base)
    monkeypatch.setattr(task_terminal_cleanup, "get_repository", _get_repository)
    monkeypatch.setattr(
        task_terminal_cleanup.intent_records,
        "list_task_interrupt_intents",
        lambda _pool, *, task_id: facts.interrupt_intents,
    )
    monkeypatch.setattr(
        task_terminal_cleanup.intent_records,
        "list_open_task_resume_intents",
        lambda _pool, *, task_id: facts.open_resume_intents,
    )
    monkeypatch.setattr(
        task_terminal_cleanup.execution_records,
        "list_task_executions",
        lambda _pool, *, task_id: facts.executions,
    )
    monkeypatch.setattr(
        task_terminal_cleanup.runtime_request_records,
        "list_task_runtime_requests",
        lambda _pool, *, task_id: facts.runtime_requests,
    )
    monkeypatch.setattr(
        task_terminal_cleanup.send_claim_records,
        "list_task_runtime_send_claims",
        lambda _pool, *, task_id: facts.send_claims,
    )


def _workspace_result(*, operation: str = "CLEANUP", status: str = "COMPLETED") -> WorkspaceResult:
    return WorkspaceResult(operation=operation, status=status, summary="reported by the producer")


def _pool() -> Any:
    return object()


class _RuntimeUncertainError(Exception):
    """A typed-uncertainty-shaped realization failure for the containment test."""


# --- the happy paths: one attempt for the exact checkout -------------------------


def test_a_newly_completed_task_attempts_cleanup_for_the_exact_snapshot_derived_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = _RecordFacts(
        task=_archived_task(), binding=_ready_producer_binding(), repository=_repository()
    )
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime(results=[_workspace_result()])

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert execution.disposition is TerminalCheckoutCleanupDisposition.CLEANUP_SENT
    assert len(runtime.calls) == 1
    call = runtime.calls[0]
    # The reconstruction runs at the ORIGINAL repository base — outside the
    # checkout being removed — on the exact original Producer session.
    assert call["working_directory"] == _ORIGINAL_BASE
    assert call["external_session_id"] == _EXTERNAL_SESSION_ID
    interaction = call["interaction"]
    assert isinstance(interaction, CleanupWorkspaceInteraction)
    # The exact destructive target is the durable snapshot base plus the
    # persisted Task UUID; the lifecycle context is the Task's own facts.
    assert interaction.target_directory == f"{_ORIGINAL_BASE}/{_TASK_ID}"
    assert interaction.task_uuid == _TASK_ID
    assert interaction.repository_full_name == f"{_OWNER_LOGIN}/{_REPOSITORY_NAME}"
    assert interaction.issue_number == 4311


def test_a_newly_cancelled_task_attempts_the_same_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = _RecordFacts(
        task=_archived_task(status=TaskStatus.CANCELLED),
        binding=_ready_producer_binding(),
        repository=_repository(),
    )
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime(results=[_workspace_result(status="FAILED")])

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    # A producer-reported FAILED workspace_result is a valid typed outcome,
    # not a control failure: the attempt ran and the stale checkout stands.
    assert execution.disposition is TerminalCheckoutCleanupDisposition.CLEANUP_SENT
    assert len(runtime.calls) == 1


# --- bounded skips: legitimate no-checkout and unsafe cases ----------------------


@pytest.mark.parametrize(
    ("facts_overrides", "expected_disposition"),
    [
        pytest.param(
            {"task": None},
            TerminalCheckoutCleanupDisposition.SKIPPED_NO_TERMINAL_SUBJECT,
            id="missing-task",
        ),
        pytest.param(
            {
                "task": SimpleNamespace(
                    id=_TASK_ID,
                    workspace_id=uuid4(),
                    status=TaskStatus.COMPLETED,
                    archived_at=SimpleNamespace(year=2026),
                )
            },
            TerminalCheckoutCleanupDisposition.SKIPPED_NO_TERMINAL_SUBJECT,
            id="workspace-mismatch",
        ),
        pytest.param(
            {
                "task": SimpleNamespace(
                    id=_TASK_ID,
                    workspace_id=_WORKSPACE_ID,
                    status=TaskStatus.REVIEWING,
                    archived_at=None,
                )
            },
            TerminalCheckoutCleanupDisposition.SKIPPED_NO_TERMINAL_SUBJECT,
            id="not-archived-nonterminal",
        ),
        pytest.param(
            {"binding": None},
            TerminalCheckoutCleanupDisposition.SKIPPED_NO_APPLICABLE_CHECKOUT,
            id="no-producer-binding",
        ),
        pytest.param(
            {"original_base": None},
            TerminalCheckoutCleanupDisposition.SKIPPED_NO_APPLICABLE_CHECKOUT,
            id="no-applicable-snapshot",
        ),
        pytest.param(
            {
                "binding": SimpleNamespace(
                    workspace_id=_WORKSPACE_ID,
                    external_session_id=_EXTERNAL_SESSION_ID,
                    lifecycle_status=TaskSessionLifecycleStatus.LOST,
                )
            },
            TerminalCheckoutCleanupDisposition.SKIPPED_SESSION_UNAVAILABLE,
            id="lost-session",
        ),
        pytest.param(
            {
                "binding": SimpleNamespace(
                    workspace_id=_WORKSPACE_ID,
                    external_session_id=None,
                    lifecycle_status=TaskSessionLifecycleStatus.READY,
                )
            },
            TerminalCheckoutCleanupDisposition.SKIPPED_SESSION_UNAVAILABLE,
            id="ready-without-external-identity",
        ),
    ],
)
def test_non_applicable_or_unsafe_subjects_skip_without_any_runtime_effect(
    monkeypatch: pytest.MonkeyPatch,
    facts_overrides: dict[str, Any],
    expected_disposition: TerminalCheckoutCleanupDisposition,
) -> None:
    overrides: dict[str, Any] = {
        "task": _archived_task(),
        "binding": _ready_producer_binding(),
        "repository": _repository(),
    }
    overrides.update(facts_overrides)
    facts = _RecordFacts(**overrides)
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime()

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert execution.disposition is expected_disposition
    assert runtime.calls == []


@pytest.mark.parametrize(
    ("outstanding", "family"),
    [
        pytest.param(
            {"interrupt_intents": [SimpleNamespace(status=TaskInterruptIntentStatus.UNSETTLED)]},
            "unsettled-interrupt",
        ),
        pytest.param(
            {"open_resume_intents": [SimpleNamespace(status=TaskInterruptIntentStatus.UNSETTLED)]},
            "open-resume",
        ),
        pytest.param(
            {"executions": [SimpleNamespace(status=ExecutionStatus.RUNNING)]},
            "open-execution",
        ),
        pytest.param(
            {"runtime_requests": [SimpleNamespace(status=RuntimeRequestStatus.PENDING)]},
            "pending-runtime-request",
        ),
        pytest.param(
            {"send_claims": [SimpleNamespace(status=TaskRuntimeSendClaimStatus.CLAIMED)]},
            "claimed-send-claim",
        ),
    ],
)
def test_outstanding_producer_work_evidence_skips_the_destructive_attempt(
    monkeypatch: pytest.MonkeyPatch,
    outstanding: dict[str, Any],
    family: str,
) -> None:
    facts = _RecordFacts(
        task=_archived_task(),
        binding=_ready_producer_binding(),
        repository=_repository(),
        **outstanding,
    )
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime()

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    # Lack of positive safety evidence is a skip — never a poll, retry, or
    # recovery workflow; a terminalized Task may keep an outstanding
    # Producer turn (external merge/PR closure) and still archive cleanly.
    assert (
        execution.disposition is TerminalCheckoutCleanupDisposition.SKIPPED_UNSAFE_OUTSTANDING_WORK
    ), family
    assert runtime.calls == []


# --- contained outcomes: no raise, no retry, no second send ----------------------


def test_a_failing_runtime_realization_is_contained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = _RecordFacts(
        task=_archived_task(), binding=_ready_producer_binding(), repository=_repository()
    )
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime(results=[_RuntimeUncertainError("the outcome is unknown")])

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert execution.disposition is TerminalCheckoutCleanupDisposition.CONTAINED_FAILURE


def test_an_unusable_workspace_result_is_contained_and_never_re_sent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = _RecordFacts(
        task=_archived_task(), binding=_ready_producer_binding(), repository=_repository()
    )
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime(results=[_workspace_result(operation="PREPARE")])

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert execution.disposition is TerminalCheckoutCleanupDisposition.CONTAINED_FAILURE
    assert len(runtime.calls) == 1


def test_a_missing_repository_record_is_contained_as_a_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = _RecordFacts(task=_archived_task(), binding=_ready_producer_binding(), repository=None)
    _install_facts(monkeypatch, facts)
    runtime = _RecordingRuntime()

    execution = execute_terminal_checkout_cleanup(
        _pool(), runtime, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert (
        execution.disposition is TerminalCheckoutCleanupDisposition.SKIPPED_NO_APPLICABLE_CHECKOUT
    )
    assert runtime.calls == []


# --- the submission helper: bounded, contained, never recorded --------------------


class _RecordingSubmission:
    def __init__(self, outcome: Any = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.outcome = outcome
        self.error: Exception | None = None

    def submit(self, *, workspace_id: UUID, task_id: UUID) -> Any:
        self.calls.append({"workspace_id": workspace_id, "task_id": task_id})
        if self.error is not None:
            raise self.error
        return self.outcome


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        pytest.param(
            TerminalCheckoutCleanupSubmissionOutcome.ACCEPTED_FOR_DELIVERY,
            TerminalCheckoutCleanupSubmissionOutcome.ACCEPTED_FOR_DELIVERY,
            id="accepted",
        ),
        pytest.param(
            TerminalCheckoutCleanupSubmissionOutcome.KNOWN_REFUSED,
            TerminalCheckoutCleanupSubmissionOutcome.KNOWN_REFUSED,
            id="refused",
        ),
        pytest.param(
            TerminalCheckoutCleanupSubmissionOutcome.UNCERTAIN,
            TerminalCheckoutCleanupSubmissionOutcome.UNCERTAIN,
            id="uncertain",
        ),
    ],
)
def test_the_submission_helper_passes_the_actual_outcome_through(
    outcome: TerminalCheckoutCleanupSubmissionOutcome,
    expected: TerminalCheckoutCleanupSubmissionOutcome,
) -> None:
    submission = _RecordingSubmission(outcome=outcome)

    result = submit_terminal_checkout_cleanup(
        submission, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result is expected
    assert submission.calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_a_failing_submission_is_an_uncertain_outcome_and_never_raises() -> None:
    submission = _RecordingSubmission()
    submission.error = RuntimeError("transport exploded")

    result = submit_terminal_checkout_cleanup(
        submission, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result is TerminalCheckoutCleanupSubmissionOutcome.UNCERTAIN


def test_an_unwired_submission_is_a_silent_skip() -> None:
    assert (
        submit_terminal_checkout_cleanup(None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID) is None
    )
