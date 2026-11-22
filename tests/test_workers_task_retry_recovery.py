"""Tests for the F12 bounded startup authorized-retry recovery (issue #316).

Deterministic and fake-backed: the bounded pass is exercised with scripted
enumerations, Task/Workspace reads, and a recording F1 submitter. No live
Postgres, Valkey, GitHub, or Cline is touched; the outstanding-attempt SQL
lookup, the due wait, and the F5 one-shot admission are covered by their
owning layers' tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

from openorc.domain.operational_failures import (
    GitHubMergeOperation,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus
from openorc.workers import task_retry_recovery
from openorc.workers.invocation import (
    OperationInvocation,
    OperationKind,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
    TaskAuthorizedImplementationRetryPayload,
    TaskAuthorizedMergeRetryPayload,
    TaskPrAuthorizationRetryPayload,
)

_RETRY_PAYLOAD_TYPES = (
    TaskAuthorizedImplementationRetryPayload,
    TaskAuthorizedMergeRetryPayload,
    TaskPrAuthorizationRetryPayload,
)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_OWNER_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_ATTEMPT_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000004")
_EPISODE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000005")
_STATE_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000006")
_NOT_BEFORE = datetime(2026, 11, 22, 12, 0, 0, tzinfo=UTC)


class _RecordingSubmitter:
    """Recording OperationSubmitter double with scripted outcomes."""

    def __init__(self, outcome: SubmissionResult | None = None) -> None:
        self.submissions: list[OperationInvocation] = []
        self._outcome = outcome if outcome is not None else SubmissionResult.accepted()

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        self.submissions.append(invocation)
        return self._outcome


class _RecoveryHarness:
    """Scripted recovery enumeration, Task, and Workspace reads."""

    def __init__(self) -> None:
        self.candidates: list[Any] = []
        self.tasks: dict[UUID, Any] = {}
        self.workspaces: dict[UUID, Any] = {}
        self.enumeration_limits: list[int] = []

    def enumerate(self, pool: Any, *, limit: int) -> list[Any]:
        self.enumeration_limits.append(limit)
        return self.candidates[:limit]

    def task(self, pool: Any, *, task_id: UUID) -> Any:
        return self.tasks.get(task_id)

    def workspace(self, pool: Any, *, workspace_id: UUID) -> Any:
        return self.workspaces.get(workspace_id)


def _candidate(**overrides: Any) -> Any:
    facts: dict[str, Any] = {
        "attempt_id": _ATTEMPT_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": uuid4(),
        "episode_id": _EPISODE_ID,
        "ordinal": 1,
        "operation_family": PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        "not_before": _NOT_BEFORE,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _task(**overrides: Any) -> Any:
    facts: dict[str, Any] = {
        "workspace_id": _WORKSPACE_ID,
        "status": TaskStatus.IMPLEMENTING,
        "state_token": _STATE_TOKEN,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _workspace() -> Any:
    return SimpleNamespace(owner_profile_id=_OWNER_PROFILE_ID)


@pytest.fixture
def recovery_harness(monkeypatch: pytest.MonkeyPatch) -> _RecoveryHarness:
    """Patch the recovery enumeration and re-derivation seams."""
    harness = _RecoveryHarness()
    monkeypatch.setattr(
        task_retry_recovery, "list_outstanding_authorized_retry_attempts", harness.enumerate
    )
    monkeypatch.setattr(task_retry_recovery, "get_task", harness.task)
    monkeypatch.setattr(task_retry_recovery, "get_workspace", harness.workspace)
    return harness


def test_recovery_submits_the_same_typed_retry_invocation_for_each_family(
    recovery_harness: _RecoveryHarness,
) -> None:
    """Each still-outstanding authorization carries its original attempt
    identity through the family's existing F1 kind with the re-proved token
    and the accountable Owner identity."""
    implementation = _candidate()
    merge = _candidate(operation_family=GitHubMergeOperation.EXACT_HEAD_MERGE)
    presentation = _candidate(
        operation_family=PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
    )
    recovery_harness.candidates = [implementation, merge, presentation]
    recovery_harness.tasks[implementation.task_id] = _task()
    recovery_harness.tasks[merge.task_id] = _task(status=TaskStatus.WAITING_FOR_OWNER)
    recovery_harness.tasks[presentation.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter  # type: ignore[arg-type]
    )

    assert [entry.kind for entry in submitter.submissions] == [
        OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY,
        OperationKind.TASK_AUTHORIZED_MERGE_RETRY,
        OperationKind.TASK_PR_AUTHORIZATION_RETRY,
    ]
    task_ids: list[str] = []
    for entry in submitter.submissions:
        assert isinstance(entry.payload, _RETRY_PAYLOAD_TYPES)
        task_ids.append(entry.payload.task.task_id)
    assert task_ids == [
        str(implementation.task_id),
        str(merge.task_id),
        str(presentation.task_id),
    ]
    for entry in submitter.submissions:
        assert isinstance(entry.payload, _RETRY_PAYLOAD_TYPES)
        payload = entry.payload
        assert payload.task.workspace_id == str(_WORKSPACE_ID)
        assert payload.profile_id == str(_OWNER_PROFILE_ID)
        assert payload.expected_state_token == str(_STATE_TOKEN)
    retry_attempt_ids: list[str] = []
    for entry in submitter.submissions:
        assert isinstance(entry.payload, _RETRY_PAYLOAD_TYPES)
        retry_attempt_ids.append(entry.payload.retry_attempt_id)
    assert retry_attempt_ids == [
        str(implementation.attempt_id),
        str(merge.attempt_id),
        str(presentation.attempt_id),
    ]


def test_recovery_skips_a_candidate_whose_durable_facts_moved_on(
    recovery_harness: _RecoveryHarness,
) -> None:
    """A candidate whose Task moved past the family's expected status submits
    nothing — the committed authorization row stays for the next supported
    trigger."""
    still_current = _candidate()
    moved_on = _candidate(operation_family=GitHubMergeOperation.EXACT_HEAD_MERGE)
    recovery_harness.candidates = [still_current, moved_on]
    recovery_harness.tasks[still_current.task_id] = _task()
    # The merge family expects waiting_for_owner; the Task is still
    # implementing, so the recovery submits nothing for it.
    recovery_harness.tasks[moved_on.task_id] = _task(status=TaskStatus.IMPLEMENTING)
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter  # type: ignore[arg-type]
    )

    assert [entry.kind for entry in submitter.submissions] == [
        OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY
    ]


def test_a_missing_task_or_workspace_submits_nothing(
    recovery_harness: _RecoveryHarness,
) -> None:
    """A gone or foreign Task, or a missing Workspace, submits nothing and
    never fails the pass."""
    candidate = _candidate()
    recovery_harness.candidates = [candidate]
    recovery_harness.tasks[candidate.task_id] = _task(workspace_id=uuid4())  # foreign
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter  # type: ignore[arg-type]
    )
    assert submitter.submissions == []

    recovery_harness.tasks[candidate.task_id] = _task()
    recovery_harness.workspaces.clear()
    submitter = _RecordingSubmitter()
    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter  # type: ignore[arg-type]
    )
    assert submitter.submissions == []


def test_a_refused_recovery_arrangement_is_contained(
    recovery_harness: _RecoveryHarness,
) -> None:
    """A refused or uncertain recovery arrangement never fails the pass and
    never replays; the next supported trigger revisits the rest."""
    candidate = _candidate()
    recovery_harness.candidates = [candidate]
    recovery_harness.tasks[candidate.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter(
        outcome=SubmissionResult(
            outcome=SubmissionOutcome.KNOWN_REFUSED,
            refusal_reason=SubmissionRefusalReason.QUEUE_UNAVAILABLE,
        )
    )

    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter  # type: ignore[arg-type]
    )

    assert len(submitter.submissions) == 1  # attempted exactly once


def test_the_recovery_pass_is_bounded_by_its_subject_budget(
    recovery_harness: _RecoveryHarness,
) -> None:
    """The enumeration is a bounded paging read; the next supported trigger
    revisits the rest."""
    candidate = _candidate()
    recovery_harness.candidates = [candidate]
    recovery_harness.tasks[candidate.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter  # type: ignore[arg-type]
    )

    assert recovery_harness.enumeration_limits == [task_retry_recovery._RECOVERY_SUBJECT_BUDGET]

    task_retry_recovery.arrange_task_retry_recovery(
        object(), submitter, subject_budget=7  # type: ignore[arg-type]
    )

    assert recovery_harness.enumeration_limits[-1] == 7
