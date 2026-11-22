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
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from openorc.domain.operational_failures import (
    GitHubMergeOperation,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
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

# The recovery pass's pool is never touched under these fakes — every
# persistence seam is patched — so one inert stand-in satisfies the signature.
_UNUSED_POOL = cast(DatabasePool, object())


class _RecordingSubmitter:
    """Recording OperationSubmitter double with scripted outcomes."""

    def __init__(self, outcome: SubmissionResult | None = None) -> None:
        self.submissions: list[OperationInvocation] = []
        self._outcome = outcome if outcome is not None else SubmissionResult.accepted()

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        self.submissions.append(invocation)
        return self._outcome


class _RecoveryHarness:
    """Scripted recovery enumeration pages, Task, and Workspace reads."""

    def __init__(self) -> None:
        self.pages: list[list[Any]] = []
        self.tasks: dict[UUID, Any] = {}
        self.workspaces: dict[UUID, Any] = {}
        self.calls: list[tuple[int, UUID | None]] = []

    def enumerate(
        self, pool: Any, *, limit: int, after_episode_id: UUID | None = None
    ) -> list[Any]:
        self.calls.append((limit, after_episode_id))
        if not self.pages:
            return []
        return self.pages.pop(0)[:limit]

    def task(self, pool: Any, *, task_id: UUID) -> Any:
        return self.tasks.get(task_id)

    def workspace(self, pool: Any, *, workspace_id: UUID) -> Any:
        return self.workspaces.get(workspace_id)

    def stale_page(self) -> list[Any]:
        """One full page of candidates whose Tasks are no longer executable.

        None of these candidates' Tasks are scripted, so every arrangement
        re-proves them as moved-on and submits nothing — exactly the
        historical batch shape that must never permanently starve a later
        eligible attempt.
        """
        return [
            _candidate(episode_id=uuid4())
            for _ in range(task_retry_recovery._RECOVERY_SUBJECT_BUDGET)
        ]


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
    recovery_harness.pages = [[implementation, merge, presentation]]
    recovery_harness.tasks[implementation.task_id] = _task()
    recovery_harness.tasks[merge.task_id] = _task(status=TaskStatus.WAITING_FOR_OWNER)
    recovery_harness.tasks[presentation.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
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
    recovery_harness.pages = [[still_current, moved_on]]
    recovery_harness.tasks[still_current.task_id] = _task()
    # The merge family expects waiting_for_owner; the Task is still
    # implementing, so the recovery submits nothing for it.
    recovery_harness.tasks[moved_on.task_id] = _task(status=TaskStatus.IMPLEMENTING)
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
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
    recovery_harness.pages = [[candidate]]
    recovery_harness.tasks[candidate.task_id] = _task(workspace_id=uuid4())  # foreign
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
    )
    assert submitter.submissions == []

    recovery_harness.tasks[candidate.task_id] = _task()
    recovery_harness.workspaces.clear()
    submitter = _RecordingSubmitter()
    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
    )
    assert submitter.submissions == []


def test_a_refused_recovery_arrangement_is_contained(
    recovery_harness: _RecoveryHarness,
) -> None:
    """A refused or uncertain recovery arrangement never fails the pass and
    never replays; the next supported trigger revisits the rest."""
    candidate = _candidate()
    recovery_harness.pages = [[candidate]]
    recovery_harness.tasks[candidate.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter(
        outcome=SubmissionResult(
            outcome=SubmissionOutcome.KNOWN_REFUSED,
            refusal_reason=SubmissionRefusalReason.QUEUE_UNAVAILABLE,
        )
    )

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
    )

    assert len(submitter.submissions) == 1  # attempted exactly once


def test_the_recovery_pass_is_bounded_by_its_subject_budget(
    recovery_harness: _RecoveryHarness,
) -> None:
    """The enumeration is a bounded paging read; the next supported trigger
    revisits the rest."""
    candidate = _candidate()
    recovery_harness.pages = [[candidate]]
    recovery_harness.tasks[candidate.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
    )

    assert recovery_harness.calls == [(task_retry_recovery._RECOVERY_SUBJECT_BUDGET, None)]

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
        subject_budget=7,
    )

    assert recovery_harness.calls[-1] == (7, None)


def test_recovery_pages_past_a_stale_batch_to_reach_a_later_attempt(
    recovery_harness: _RecoveryHarness,
) -> None:
    """The regression guard against permanent starvation: a full initial page
    of historical, no-longer-executable candidates advances the keyset cursor
    within the same bounded pass, so the valid outstanding attempt behind it
    is reached and resubmitted — at this startup, not only after the ledger
    happens to shrink."""
    stale = recovery_harness.stale_page()
    valid = _candidate()
    recovery_harness.pages = [stale, [valid]]
    recovery_harness.tasks[valid.task_id] = _task()
    recovery_harness.workspaces[_WORKSPACE_ID] = _workspace()
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
    )

    # The valid attempt behind the stale batch was arranged.
    assert [entry.kind for entry in submitter.submissions] == [
        OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY
    ]
    assert isinstance(submitter.submissions[0].payload, _RETRY_PAYLOAD_TYPES)
    assert submitter.submissions[0].payload.retry_attempt_id == str(valid.attempt_id)
    # The pass advanced its keyset cursor past the stale batch instead of
    # repeatedly examining the same prefix on every subsequent startup.
    assert recovery_harness.calls == [
        (task_retry_recovery._RECOVERY_SUBJECT_BUDGET, None),
        (task_retry_recovery._RECOVERY_SUBJECT_BUDGET, stale[-1].episode_id),
    ]


def test_the_recovery_pass_is_bounded_by_its_page_budget(
    recovery_harness: _RecoveryHarness,
) -> None:
    """The pass examines at most its bounded page count — a paging bound,
    never a scheduler; the next supported trigger revisits the rest."""
    stale_pages = [
        recovery_harness.stale_page() for _ in range(task_retry_recovery._RECOVERY_PAGE_BUDGET + 2)
    ]
    recovery_harness.pages = list(stale_pages)
    submitter = _RecordingSubmitter()

    task_retry_recovery.arrange_task_retry_recovery(
        _UNUSED_POOL,
        submitter,
    )

    assert len(recovery_harness.calls) == task_retry_recovery._RECOVERY_PAGE_BUDGET
    assert submitter.submissions == []
    # Every page advanced the cursor past the examined prefix.
    cursors = [call[1] for call in recovery_harness.calls]
    assert cursors[0] is None
    assert cursors[1:] == [
        stale_pages[0][-1].episode_id,
        stale_pages[1][-1].episode_id,
        stale_pages[2][-1].episode_id,
    ]
