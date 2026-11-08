"""Deterministic tests for the E10b unified Task-cancellation command (issue #228).

Scripted SQL-connection seams, a stubbed B7 reconciliation, and a fake
GitHub close client prove the unified Owner Cancel command on its critical
paths:

- the lifecycle ordering: the available authoritative canonical-PR facts are
  established BEFORE any irreversible mutation — a PR already authoritatively
  confirmed merged at the point of decision routes to E10a's shared
  completion path (never an Owner CANCELLED claim), while a read failure
  still allows cancellation with a durable unresolved PR-close obligation;
- the one shared decision transaction applies the gate ``CANCELLED``
  resolution (exact current pending gate of any type, no fabricated gate),
  the OPEN ReviewLoop closures, the terminal ``CANCELLED`` archival
  (clearing ``queued_at`` and the current-gate pointer and releasing the
  issue mapping), the single OWNER ``task_cancelled`` audit, and the
  one-shot PR-close intent atomically;
- the one-shot post-archival closure attempt: already closed-unmerged
  records no-action with no write; a PR observed merged after terminal
  cancellation preserves the archived outcome; an open PR is closed exactly
  once under the recorded Profile's credential; definitive rejections
  record ``failed`` and uncertain outcomes record ``unknown`` — never
  blindly replayed; pre-delivery conditions keep the obligation pending;
- the accounted-runtime veto (unsettled interrupts, open executions,
  pending runtime requests) and the inapplicable/stale classifications
  mutate nothing.

No live GitHub or Postgres access; the reconciliation, the completion seam,
the credential resolution, and the route resolution are stubbed at their
module seams (their own modules own those matrices).
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.adapters.github import (
    GitHubOutcomeUncertainError,
    GitHubPullRequestCloseOperation,
    GitHubPullRequestCloseResult,
    GitHubRepositoryObservation,
)
from openorc.domain.executions import ExecutionStatus
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntentStatus,
)
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.reviews import ReviewLoopPurpose
from openorc.domain.runtime_requests import RuntimeRequestStatus
from openorc.domain.send_claims import TaskRuntimeSendClaimKind, TaskRuntimeSendClaimStatus
from openorc.domain.tasks import TaskStatus, TaskTerminalCause
from openorc.persistence import blocks as block_records
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import intents as intent_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence import reviews as review_records
from openorc.persistence import runtime_requests as runtime_request_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import task_cancellation, task_subject_guards
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_cancellation import (
    TaskCancellationOutcomeKind,
    TaskPrClosureOutcomeKind,
    cancel_task,
    cancel_task_composition,
    cancel_task_for_confirmed_pr_closure,
)
from tests.test_services_task_pr_review_decision import (
    _EVENT_INSERT,
    _event_returned_row,
    _event_types,
    _gate_row,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _GITHUB_REPOSITORY_ID,
    _HEAD_SHA,
    _OWNER_PROFILE_ID,
    _PR_ID,
    _PR_NUMBER,
    _REPOSITORY_ID,
    _STATE_TOKEN,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    _WORKSPACE_READ,
    ScriptedConnection,
    _count,
    _pool,
    _sql_has,
    _ws_row,
)

_OBSERVED = datetime(2026, 10, 23, 12, 0, 0, tzinfo=UTC)
_CANCELLED_TOKEN = uuid4()
_GATE_ID = uuid4()
_OTHER_GATE_ID = uuid4()
_LOOP_ID = uuid4()
_GITHUB_PR_ID = 555
_REVISION_ID = uuid4()
_LATER_REVISION_ID = uuid4()
_ITERATION_ID = uuid4()
_LATER_ITERATION_ID = uuid4()
_BLOCK_ID = uuid4()
_OTHER_BLOCK_ID = uuid4()
_EPISODE_ID = uuid4()

_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")
_GATE_POINTER_CLEAR = _sql_has("update openorc.tasks", "current_owner_gate_id")
_ARCHIVE_WRITE = _sql_has("update openorc.tasks", "archived_at = now()")
_CLOSE_INTENT_INSERT = _sql_has("insert into openorc.task_pull_request_close_intents")
_CLOSE_INTENT_UPDATE = _sql_has("update openorc.task_pull_request_close_intents")


def _task_values(
    *,
    status: str = "waiting_for_owner",
    state_token: Any = _STATE_TOKEN,
    current_gate_id: Any = None,
    archived: bool = False,
    queued: bool = False,
    fingerprint: Any = "a" * 64,
    cause: Any = None,
    current_plan_revision: Any = None,
) -> tuple[Any, ...]:
    """One 17-column ``openorc.tasks`` row shaped like the durable model."""
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        503,
        42,
        status,
        _OBSERVED if archived else None,
        "openorc/task-42-implementation",
        state_token,
        current_plan_revision,
        current_gate_id,
        fingerprint,
        _OBSERVED if queued else None,
        _OBSERVED,
        _OBSERVED,
        cause,
        None,
    )


def _archived_cancelled_row(fingerprint: Any = "a" * 64, cause: Any = None) -> tuple[Any, ...]:
    return _task_values(
        status="cancelled",
        state_token=_CANCELLED_TOKEN,
        current_gate_id=None,
        archived=True,
        queued=False,
        fingerprint=fingerprint,
        cause=cause,
    )


def _pull_request(
    *,
    state: TaskPullRequestState = TaskPullRequestState.OPEN,
    merged_at: datetime | None = None,
    workspace_id: Any = _WORKSPACE_ID,
) -> TaskPullRequest:
    return TaskPullRequest(
        id=_PR_ID,
        workspace_id=workspace_id,
        task_id=_TASK_ID,
        repository_id=_REPOSITORY_ID,
        github_pr_id=_GITHUB_PR_ID,
        github_pr_number=_PR_NUMBER,
        head_ref="openorc/task-42-implementation",
        base_ref="main",
        head_sha=_HEAD_SHA,
        state=state,
        merged_at=merged_at,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _reconciled(*, pull_request: TaskPullRequest) -> Any:
    from openorc.services.task_pull_request_reconciliation import (
        TaskPullRequestReconciliation,
    )

    return TaskPullRequestReconciliation(
        pull_request=pull_request,
        updated=False,
        head_changed=False,
        base_changed=False,
        state_changed=False,
        previous_head_sha=None,
        previous_base_ref=None,
        previous_state=None,
    )


def _close_intent_row(
    *,
    status: str = "pending",
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _PR_ID,
        _GITHUB_PR_ID,
        _PR_NUMBER,
        _OWNER_PROFILE_ID,
        status,
        outcome if outcome is not None else {},
        _OBSERVED,
        _OBSERVED,
    )


class _ReconcileStub:
    """The stubbed B7 reconciliation seam recording each invocation."""

    def __init__(self, *results: Any) -> None:
        self._results = list(results)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, pool: Any, github: Any, *, workspace_id: Any, task_id: Any) -> Any:
        self.calls.append({"workspace_id": workspace_id, "task_id": task_id})
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class _CloseGithub:
    """The fake GitHub client recording the exact close routing arguments."""

    def __init__(self, *, close_results: tuple[Any, ...] = ()) -> None:
        self.repository_calls: list[dict[str, int]] = []
        self.close_calls: list[dict[str, Any]] = []
        self._close_results = list(close_results)

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> Any:
        self.repository_calls.append(
            {
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        return GitHubRepositoryObservation(
            github_repository_id=github_repository_id,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def close_pull_request(self, **kwargs: Any) -> Any:
        self.close_calls.append(dict(kwargs))
        result = self._close_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    *,
    reconcile: _ReconcileStub | None = None,
    completion: Any = None,
    pull_request: TaskPullRequest | None = None,
    interrupt_intents: list[Any] | None = None,
    executions: list[Any] | None = None,
    runtime_requests: list[Any] | None = None,
    loops: list[Any] | None = None,
    send_claims: list[Any] | None = None,
    bootstrap_evidence: Any = None,
    loop_iterations: list[Any] | None = None,
    review_iteration: Any = None,
    episode: Any = None,
    escalated_block: Any = None,
    block: Any = None,
    veto_gate: Any = None,
) -> None:
    if reconcile is not None:
        monkeypatch.setattr(task_cancellation, "reconcile_task_pull_request", reconcile)
    if completion is not None:
        monkeypatch.setattr(task_cancellation, "complete_confirmed_task_merge", completion)
    monkeypatch.setattr(
        task_cancellation,
        "get_task_pull_request_for_task",
        lambda pool, *, task_id: pull_request,
    )
    monkeypatch.setattr(
        intent_records,
        "list_task_interrupt_intents",
        lambda pool, *, task_id: list(interrupt_intents or []),
    )
    monkeypatch.setattr(
        execution_records,
        "list_task_executions",
        lambda pool, *, task_id: list(executions or []),
    )
    monkeypatch.setattr(
        runtime_request_records,
        "list_task_runtime_requests",
        lambda pool, *, task_id: list(runtime_requests or []),
    )
    monkeypatch.setattr(
        review_records,
        "list_task_review_loops",
        lambda pool, *, task_id, status: list(loops or []),
    )
    monkeypatch.setattr(
        send_claim_records,
        "list_task_runtime_send_claims",
        lambda pool, *, task_id: list(send_claims or []),
    )
    monkeypatch.setattr(
        task_records,
        "get_task_planning_bootstrap_evidence",
        lambda pool, *, task_id: bootstrap_evidence,
    )
    monkeypatch.setattr(
        review_records,
        "get_review_iteration",
        lambda pool, *, review_iteration_id: review_iteration,
    )
    monkeypatch.setattr(
        review_records,
        "list_review_loop_iterations",
        lambda pool, *, review_loop_id: list(loop_iterations or []),
    )
    monkeypatch.setattr(
        failure_records,
        "get_operational_failure_episode_by_original_attempt",
        lambda pool, *, original_attempt_kind, original_attempt_id: episode,
    )
    monkeypatch.setattr(
        block_records,
        "get_task_block",
        lambda pool, *, task_block_id: escalated_block,
    )
    monkeypatch.setattr(
        task_subject_guards,
        "get_task_block",
        lambda pool, *, task_block_id: block,
    )
    if veto_gate is not None:
        # Only the veto's own current-gate read goes through the module
        # attribute; the guard and the resolution keep their scripted reads.
        monkeypatch.setattr(
            gate_records, "get_owner_gate", lambda pool, *, owner_gate_id: veto_gate
        )

    def _close(pool: Any, *, review_loop_id: Any) -> Any:
        for loop in loops or []:
            if loop.id == review_loop_id:
                return SimpleNamespace(
                    id=loop.id,
                    workspace_id=_WORKSPACE_ID,
                    task_id=_TASK_ID,
                    purpose=loop.purpose,
                    iteration_limit=loop.iteration_limit,
                    status="closed",
                    closed_at=_OBSERVED,
                    created_at=_OBSERVED,
                )
        return None

    monkeypatch.setattr(review_records, "close_review_loop", _close)


def _credential_stub(monkeypatch: pytest.MonkeyPatch, *, error: Exception | None = None) -> None:
    def _resolve(pool: Any, resolver: Any, github: Any, *, profile_id: Any, route: Any) -> Any:
        if error is not None:
            raise error
        return object()

    monkeypatch.setattr(task_cancellation, "resolve_owner_write_credential", _resolve)
    monkeypatch.setattr(
        task_cancellation, "resolve_owner_write_credential_after_rejection", _resolve
    )
    monkeypatch.setattr(
        task_cancellation,
        "resolve_system_repository_installation_route",
        lambda pool, *, workspace_id, repository_id: SimpleNamespace(
            github_installation_id=501,
            repository=SimpleNamespace(
                identity=SimpleNamespace(github_repository_id=_GITHUB_REPOSITORY_ID)
            ),
        ),
    )


def _cancel_call(
    pool: DatabasePool, github: Any, *, expected_state_token: Any = _STATE_TOKEN
) -> Any:
    return cancel_task(
        pool,
        cast(Any, github),
        cast(Any, object()),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
    )


# --- script helpers (in service call order) --------------------------------------


def _script_authorization(
    conn: ScriptedConnection,
    *,
    row: tuple[Any, ...],
) -> None:
    """The account/Owner/currentness head shared by every cancel phase."""
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, row)  # the Workspace guard task read
    conn.on(_TASK_READ_LOCKED, row)  # get_task_for_update
    conn.on(_TASK_READ, row)  # require_current_task


def _script_gate_resolution(
    conn: ScriptedConnection,
    *,
    row: tuple[Any, ...],
    gate_id: Any = _GATE_ID,
) -> None:
    conn.on(  # require_current_owner_gate
        _GATE_READ, _gate_row(gate_id=gate_id, gate_type="merge_decision")
    )
    conn.on(  # the resolution gate read
        _GATE_READ, _gate_row(gate_id=gate_id, gate_type="merge_decision")
    )
    conn.on(_TASK_READ_LOCKED, row)  # the resolution Task row lock
    conn.on(
        _GATE_UPDATE,
        _gate_row(gate_id=gate_id, gate_type="merge_decision", status="cancelled"),
    )
    conn.on(
        _GATE_POINTER_CLEAR,
        _task_values(status="waiting_for_owner", state_token=uuid4(), current_gate_id=None),
    )


# --- no-PR cancel paths -----------------------------------------------------------


def test_cancel_archives_a_ready_to_plan_task_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    # A never-started ready_to_plan Task (issue #233) carries NO baseline:
    # the cancellation must accept the null fingerprint, archive CANCELLED
    # without inventing one, and require no fresh issue reconciliation, no
    # runtime sessions, and no canonical PR (zero GitHub calls).
    _script_authorization(conn, row=_task_values(status="ready_to_plan", fingerprint=None))
    _script_authorization(conn, row=_task_values(status="ready_to_plan", fingerprint=None))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row(fingerprint=None))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch)

    github = _CloseGithub()
    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED
    assert outcome.task.archived_at is not None
    assert outcome.task.source_requirements_fingerprint is None
    assert outcome.owner_gate is None
    assert outcome.close_intent is None
    assert _event_types(conn) == ["task_cancelled"]
    assert _count(conn, _GATE_READ) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert github.repository_calls == []
    assert github.close_calls == []


def test_cancel_archives_a_queued_task_and_clears_its_admission_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="queued", queued=True))
    _script_authorization(conn, row=_task_values(status="queued", queued=True))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch)

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.task.status is TaskStatus.CANCELLED
    # A queued Task has started (issue #233): its frozen baseline is
    # immutable through the cancellation — never erased, never invented.
    assert outcome.task.source_requirements_fingerprint == "a" * 64
    archive_params = [params for sql, params in conn.executed if "archived_at = now()" in sql]
    assert len(archive_params) == 1
    assert _event_types(conn) == ["task_cancelled"]


def test_cancel_resolves_the_exact_current_pending_gate_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    waiting = _task_values(status="waiting_for_owner", current_gate_id=_GATE_ID)
    _script_authorization(conn, row=waiting)
    _script_authorization(conn, row=waiting)
    _script_gate_resolution(conn, row=waiting)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_resolved"))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch)

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.owner_gate is not None
    gate_updates = [params for sql, params in conn.executed if "update openorc.owner_gates" in sql]
    assert gate_updates[0] is not None
    assert gate_updates[0][0] == "cancelled"
    assert _event_types(conn) == ["owner_gate_resolved", "task_cancelled"]


def test_cancel_closes_every_open_review_loop_as_actually_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="ready_to_plan"))
    _script_authorization(conn, row=_task_values(status="ready_to_plan"))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    loops = [
        SimpleNamespace(
            id=_LOOP_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            purpose="planning",
            iteration_limit=2,
            status="open",
            closed_at=None,
            created_at=_OBSERVED,
        )
    ]
    _wire(monkeypatch, loops=loops)

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert len(outcome.closed_loops) == 1
    assert outcome.closed_loops[0].status == "closed"


def test_cancel_from_a_settled_blocked_task_applies_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked"))
    _script_authorization(conn, row=_task_values(status="blocked"))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.SETTLED)],
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.task.status is TaskStatus.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


# --- the canonical-PR lifecycle ordering (issue #228 §4) -------------------------


def _script_blocked_cancel_with_close_intent(
    conn: ScriptedConnection,
) -> None:
    """A settled BLOCKED Task with an open canonical PR: Phases A+B script."""
    blocked = _task_values(status="blocked")
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    conn.on(_CLOSE_INTENT_INSERT, _close_intent_row())
    # Phase C's archived-Task read for the close-attempt routing.
    conn.on(_TASK_READ, _archived_cancelled_row())


def _wire_open_pr(monkeypatch: pytest.MonkeyPatch, reconcile: _ReconcileStub) -> None:
    _wire(
        monkeypatch,
        reconcile=reconcile,
        pull_request=_pull_request(),
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.SETTLED)],
    )
    _credential_stub(monkeypatch)


def test_a_confirmed_merge_at_the_point_of_decision_takes_the_completion_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked")
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _task_values(status="completed", archived=True, state_token=uuid4()))
    conn.on(_EVENT_INSERT, _event_returned_row("task_completed"))
    reconcile = _ReconcileStub(
        _reconciled(
            pull_request=_pull_request(state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED)
        )
    )

    def _completion(pool: Any, *, workspace_id: Any, task_id: Any) -> Any:
        return SimpleNamespace(
            task=SimpleNamespace(status=TaskStatus.COMPLETED, archived_at=_OBSERVED),
            pull_request=_pull_request(state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED),
        )

    _wire(
        monkeypatch,
        reconcile=reconcile,
        completion=_completion,
        pull_request=_pull_request(),
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.SETTLED)],
    )
    github = _CloseGithub()

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.COMPLETED
    # No Owner CANCELLED claim over an already-confirmed merge: no close
    # request, and the completion seam consumed the decision.
    assert github.close_calls == []
    # One pre-decision observation established the confirmed merge.
    assert len(reconcile.calls) == 1
    assert _event_types(conn) == []
    assert _count(conn, _CLOSE_INTENT_INSERT) == 0


def test_an_open_pr_is_closed_exactly_once_and_the_outcome_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.CLOSED, merged_at=None)),
    )
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub(
        close_results=(
            GitHubPullRequestCloseResult(
                outcome=GitHubPullRequestCloseOperation.CLOSED,
                pull_request=None,
            ),
        )
    )
    conn.on(
        _CLOSE_INTENT_UPDATE,
        _close_intent_row(status="close_requested", outcome={"confirmed": True}),
    )

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED
    # Exactly one close request under the recorded Profile, addressed to the
    # stable PR identity; the observed outcome confirmed closed-unmerged.
    assert len(github.close_calls) == 1
    assert github.close_calls[0]["pull_number"] == _PR_NUMBER
    assert github.close_calls[0]["owner_login"] == "octocat"
    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.CLOSE_REQUESTED
    assert len(reconcile.calls) == 3
    assert _count(conn, _CLOSE_INTENT_INSERT) == 1
    assert _count(conn, _CLOSE_INTENT_UPDATE) == 1


def test_a_pr_observed_merged_after_cancellation_preserves_the_archived_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(
            pull_request=_pull_request(state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED)
        ),
    )
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub()
    conn.on(
        _CLOSE_INTENT_UPDATE,
        _close_intent_row(status="no_action", outcome={"observed_state": "merged"}),
    )

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    # The post-cancellation merge observation never requests closure and
    # never rewrites the archived terminal outcome.
    assert github.close_calls == []
    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.NO_ACTION
    assert _count(conn, _ARCHIVE_WRITE) == 1


def test_an_already_closed_unmerged_pr_records_no_action_with_no_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked")
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.CLOSED, merged_at=None))
    )
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub()

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    # No close obligation for an already closed-unmerged canonical PR.
    assert github.close_calls == []
    assert outcome.close_intent is None
    assert _count(conn, _CLOSE_INTENT_INSERT) == 0
    # One pre-decision observation established the closed-unmerged state.
    assert len(reconcile.calls) == 1


def test_a_read_failure_still_cancels_with_a_durable_unresolved_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(AuthorizationError("access lost"), AuthorizationError("access lost"))
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub()

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED
    # The unresolved obligation is durably observable and stays pending: no
    # outcome is fabricated and no blind replay happens here.
    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.PENDING
    assert github.close_calls == []
    assert _count(conn, _CLOSE_INTENT_INSERT) == 1
    assert _count(conn, _CLOSE_INTENT_UPDATE) == 0


# --- the one-shot closure attempt outcome classes ----------------------------------


def test_a_definitive_close_rejection_records_failed_without_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
    )
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub(
        close_results=(
            GitHubPullRequestCloseResult(
                outcome=GitHubPullRequestCloseOperation.REJECTED, pull_request=None
            ),
        )
    )
    conn.on(
        _CLOSE_INTENT_UPDATE,
        _close_intent_row(status="failed", outcome={"close": "rejected"}),
    )

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.FAILED
    assert len(github.close_calls) == 1
    assert _count(conn, _CLOSE_INTENT_UPDATE) == 1


def test_an_uncertain_close_outcome_records_unknown_without_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
    )
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub(close_results=(GitHubOutcomeUncertainError("lost"),))
    conn.on(
        _CLOSE_INTENT_UPDATE,
        _close_intent_row(status="unknown", outcome={"close": "uncertain"}),
    )

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.UNKNOWN
    assert len(github.close_calls) == 1


def test_a_known_success_without_durable_confirmation_records_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        AuthorizationError("reconciliation failed"),
    )
    _wire_open_pr(monkeypatch, reconcile)
    github = _CloseGithub(
        close_results=(
            GitHubPullRequestCloseResult(
                outcome=GitHubPullRequestCloseOperation.CLOSED, pull_request=None
            ),
        )
    )
    conn.on(
        _CLOSE_INTENT_UPDATE,
        _close_intent_row(status="unknown", outcome={"close": "unconfirmed"}),
    )

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.UNKNOWN
    # The close request is never repeated after the unconfirmed success.
    assert len(github.close_calls) == 1


def test_a_pre_delivery_credential_failure_keeps_the_obligation_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_blocked_cancel_with_close_intent(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN)),
    )
    _wire(
        monkeypatch,
        reconcile=reconcile,
        pull_request=_pull_request(),
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.SETTLED)],
    )
    _credential_stub(monkeypatch, error=AuthorizationError("user authorization unavailable"))
    github = _CloseGithub()

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.task.status is TaskStatus.CANCELLED
    # The admitted close request never went out: the pending obligation
    # stays observable for the later reconciliation machinery.
    assert outcome.close_intent is not None
    assert outcome.close_intent.status is TaskPullRequestCloseIntentStatus.PENDING
    assert github.close_calls == []
    assert github.repository_calls == []
    assert _count(conn, _CLOSE_INTENT_UPDATE) == 0


# --- the accounted-runtime veto and inapplicable classifications -------------------


def test_an_unsettled_interruption_vetoes_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked"))
    _wire(
        monkeypatch,
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.UNSETTLED)],
    )

    with pytest.raises(ConflictError, match="not settled"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_failed_or_unknown_interruption_vetoes_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    for status in (TaskInterruptIntentStatus.FAILED, TaskInterruptIntentStatus.UNKNOWN):
        conn = ScriptedConnection()
        _script_authorization(conn, row=_task_values(status="blocked"))
        _wire(monkeypatch, interrupt_intents=[SimpleNamespace(status=status)])

        with pytest.raises(ConflictError, match="not settled"):
            _cancel_call(_pool(conn), _CloseGithub())

        assert _count(conn, _ARCHIVE_WRITE) == 0


def test_an_open_execution_vetoes_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(
        conn, row=_task_values(status="waiting_for_owner", current_gate_id=_GATE_ID)
    )
    _wire(
        monkeypatch,
        executions=[SimpleNamespace(status=ExecutionStatus.RUNNING)],
    )

    with pytest.raises(ConflictError, match="open execution"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_pending_runtime_request_vetoes_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(
        conn, row=_task_values(status="waiting_for_owner", current_gate_id=_GATE_ID)
    )
    _wire(
        monkeypatch,
        runtime_requests=[SimpleNamespace(status=RuntimeRequestStatus.PENDING)],
    )

    with pytest.raises(ConflictError, match="pending runtime request"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


# --- F7-prep (issue #278): exceptional BLOCKED gates and the completed veto -------


def _exceptional_blocked_gate_row(
    *,
    gate_id: Any = _GATE_ID,
    gate_type: str = "block_resolution",
    status: str = "pending",
    task_block_id: Any = _BLOCK_ID,
) -> tuple[Any, ...]:
    row = list(
        _gate_row(gate_id=gate_id, gate_type=gate_type, head_sha=None, pr_id=None, status=status)
    )
    row[8] = task_block_id  # the gate's TaskBlock subject
    return tuple(row)


def _script_exceptional_gate_resolution(
    conn: ScriptedConnection,
    *,
    row: tuple[Any, ...],
    gate_type: str = "block_resolution",
) -> None:
    """The blocked-gate resolution sequence inside the decision transaction."""
    conn.on(  # require_current_owner_gate
        _GATE_READ,
        _exceptional_blocked_gate_row(gate_type=gate_type),
    )
    conn.on(  # the resolution gate read
        _GATE_READ,
        _exceptional_blocked_gate_row(gate_type=gate_type),
    )
    conn.on(_TASK_READ_LOCKED, row)  # the resolution Task row lock
    conn.on(
        _GATE_UPDATE,
        _exceptional_blocked_gate_row(gate_type=gate_type, status="cancelled"),
    )
    conn.on(
        _GATE_POINTER_CLEAR,
        _task_values(status="blocked", state_token=uuid4(), current_gate_id=None),
    )


def _ordinary_subject_gate_row(gate_type: str) -> tuple[Any, ...]:
    """One valid ordinary-subject gate row (no TaskBlock binding)."""
    row = list(_gate_row(gate_id=_GATE_ID, gate_type=gate_type, head_sha=None, pr_id=None))
    row[5] = _REVISION_ID  # the gate's PlanRevision subject
    return tuple(row)


def _unresolved_block(*, block_id: Any = _BLOCK_ID, resolved: bool = False) -> Any:
    return SimpleNamespace(
        id=block_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        reason="owner_action_required",
        resolved_at=_OBSERVED if resolved else None,
        context={},
    )


def _send_claim(
    kind: TaskRuntimeSendClaimKind,
    *,
    status: TaskRuntimeSendClaimStatus = TaskRuntimeSendClaimStatus.CLAIMED,
    review_iteration_id: Any | None = None,
) -> Any:
    return SimpleNamespace(
        id=uuid4(),
        kind=kind,
        status=status,
        review_iteration_id=review_iteration_id,
    )


def _review_iteration(
    *,
    iteration_id: Any = _ITERATION_ID,
    iteration_number: int = 1,
    outcome: Any = None,
    plan_revision_id: Any | None = _REVISION_ID,
    task_pull_request_id: Any | None = None,
    reviewed_head_sha: str | None = None,
) -> Any:
    return SimpleNamespace(
        id=iteration_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        review_loop_id=_LOOP_ID,
        iteration_number=iteration_number,
        plan_revision_id=plan_revision_id,
        task_pull_request_id=task_pull_request_id,
        reviewed_head_sha=reviewed_head_sha,
        outcome=outcome,
    )


def _escalation_episode(*, escalated_block_id: Any | None = _BLOCK_ID) -> Any:
    return SimpleNamespace(
        id=_EPISODE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation_family=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
        original_attempt_kind=OriginalAttemptKind.REVIEW_ITERATION,
        original_attempt_id=_ITERATION_ID,
        escalated_task_block_id=escalated_block_id,
    )


def _escalated_block(
    *,
    block_id: Any = _BLOCK_ID,
    effect_evidence: str = "pre_effect_not_applied",
) -> Any:
    return SimpleNamespace(
        id=block_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        resolved_at=None,
        context={
            "failure_episode_id": str(_EPISODE_ID),
            "operation_family": "reviewer_review",
            "original_attempt_kind": "review_iteration",
            "original_attempt_id": str(_ITERATION_ID),
            "effect_evidence": effect_evidence,
        },
    )


def _planning_loop() -> Any:
    return SimpleNamespace(
        id=_LOOP_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
        status="open",
        closed_at=None,
        created_at=_OBSERVED,
    )


def _pr_review_loop() -> Any:
    return SimpleNamespace(
        id=_LOOP_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=5,
        status="open",
        closed_at=None,
        created_at=_OBSERVED,
    )


def test_cancel_resolves_each_current_exceptional_blocked_gate_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for gate_type in ("block_resolution", "github_issue_change", "github_state_mismatch"):
        conn = ScriptedConnection()
        blocked = _task_values(status="blocked", current_gate_id=_GATE_ID)
        _script_authorization(conn, row=blocked)
        _script_authorization(conn, row=blocked)
        _script_exceptional_gate_resolution(conn, row=blocked, gate_type=gate_type)
        conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
        conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_resolved"))
        conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
        _wire(monkeypatch, block=_unresolved_block())

        outcome = _cancel_call(_pool(conn), _CloseGithub())

        assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
        assert outcome.task.status is TaskStatus.CANCELLED
        assert outcome.owner_gate is not None
        gate_updates = [
            params for sql, params in conn.executed if "update openorc.owner_gates" in sql
        ]
        assert len(gate_updates) == 1
        assert gate_updates[0] is not None
        assert gate_updates[0][0] == "cancelled"
        assert _event_types(conn) == ["owner_gate_resolved", "task_cancelled"]


def test_a_wrong_kind_current_gate_on_a_blocked_task_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_gate_id=_GATE_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(
        _GATE_READ,
        _ordinary_subject_gate_row("implementation_authorization"),
    )
    _wire(monkeypatch)

    with pytest.raises(ConflictError, match="exceptional blocked-task gate"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_blocked_gate_bound_to_a_resolved_block_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_gate_id=_GATE_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_GATE_READ, _exceptional_blocked_gate_row())
    _wire(monkeypatch, block=_unresolved_block(resolved=True))

    with pytest.raises(StaleOperationError, match="block subject has already resolved"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_blocked_gate_bound_to_a_missing_block_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_gate_id=_GATE_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_GATE_READ, _exceptional_blocked_gate_row())
    _wire(monkeypatch)

    with pytest.raises(NotFoundError, match="block subject is not available"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_superseded_blocked_gate_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_gate_id=_GATE_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    # The currentness guard reads a pending gate that is no longer the
    # Task's current one: a superseded gate is never resolved as if the
    # Owner cancelled through it.
    conn.on(_GATE_READ, _exceptional_blocked_gate_row(gate_id=_OTHER_GATE_ID))
    _wire(monkeypatch, block=_unresolved_block())

    with pytest.raises(StaleOperationError, match="no longer the task's current gate"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_the_delegating_gate_composition_refuses_a_blocked_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The E5/E7 delegation seam stays WAITING_FOR_OWNER-only (issue #278).

    A blocked Task with a current exceptional gate is refused at the
    delegating entrypoint — rechecked under the Task lock — because that
    seam records no canonical-PR close obligation and performs no
    confirmed-merge preflight; the unified ``cancel_task`` command owns
    those safeguards and is the only blocked-cancellation admission path.
    """
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked", current_gate_id=_GATE_ID))
    _wire(monkeypatch, block=_unresolved_block())

    with pytest.raises(ConflictError, match="delegating gate-cancellation entrypoint"):
        cancel_task_composition(
            _pool(conn),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            owner_gate_id=_GATE_ID,
        )

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_claimed_send_dispatch_vetoes_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked"))
    _wire(
        monkeypatch,
        send_claims=[_send_claim(TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH)],
    )

    with pytest.raises(ConflictError, match="unsettled runtime send claim"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_claimed_planning_revise_dispatch_on_its_current_subject_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration()
    conn = ScriptedConnection()
    _script_authorization(
        conn, row=_task_values(status="blocked", current_plan_revision=_REVISION_ID)
    )
    _wire(
        monkeypatch,
        send_claims=[
            _send_claim(
                TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
                review_iteration_id=_ITERATION_ID,
            )
        ],
        review_iteration=iteration,
        loop_iterations=[iteration],
    )

    with pytest.raises(ConflictError, match="unsettled runtime send claim"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_superseded_planning_revise_claim_permits_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration(iteration_id=_ITERATION_ID, iteration_number=1)
    later = _review_iteration(iteration_id=_LATER_ITERATION_ID, iteration_number=2)
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_plan_revision=_REVISION_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        send_claims=[
            _send_claim(
                TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
                review_iteration_id=_ITERATION_ID,
            )
        ],
        review_iteration=iteration,
        loop_iterations=[iteration, later],
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


def test_a_claimed_pr_compose_dispatch_without_a_canonical_pr_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked"))
    _wire(
        monkeypatch,
        send_claims=[_send_claim(TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH)],
    )

    with pytest.raises(ConflictError, match="unsettled runtime send claim"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_claimed_pr_compose_dispatch_with_a_canonical_pr_permits_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked")
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        send_claims=[_send_claim(TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH)],
        pull_request=_pull_request(),
        reconcile=_ReconcileStub(
            _reconciled(pull_request=_pull_request(state=TaskPullRequestState.CLOSED))
        ),
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.close_intent is None
    assert _event_types(conn) == ["task_cancelled"]


def test_terminal_send_claims_permits_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked")
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        send_claims=[
            _send_claim(
                TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
                status=TaskRuntimeSendClaimStatus.SETTLED,
            ),
            _send_claim(
                TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
                status=TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY,
            ),
        ],
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


def test_an_unsettled_planning_bootstrap_vetoes_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked"))
    _wire(monkeypatch, bootstrap_evidence=SimpleNamespace(stage=None, claim_token=uuid4()))

    with pytest.raises(ConflictError, match="planning bootstrap is unsettled"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_published_plan_revision_resolves_the_bootstrap_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_plan_revision=_REVISION_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch, bootstrap_evidence=SimpleNamespace(stage=None, claim_token=uuid4()))

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


def test_an_unfinalized_review_iteration_on_the_current_subject_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration()
    conn = ScriptedConnection()
    _script_authorization(
        conn, row=_task_values(status="blocked", current_plan_revision=_REVISION_ID)
    )
    _wire(monkeypatch, loops=[_planning_loop()], loop_iterations=[iteration])

    with pytest.raises(ConflictError, match="outstanding reviewer review dispatch"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_an_escalated_post_send_uncertain_review_failure_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration()
    conn = ScriptedConnection()
    _script_authorization(
        conn, row=_task_values(status="blocked", current_plan_revision=_REVISION_ID)
    )
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[iteration],
        episode=_escalation_episode(),
        escalated_block=_escalated_block(effect_evidence="effect_unclassified"),
    )

    with pytest.raises(ConflictError, match="outstanding reviewer review dispatch"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_an_escalated_known_failed_review_failure_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration()
    conn = ScriptedConnection()
    _script_authorization(
        conn, row=_task_values(status="blocked", current_plan_revision=_REVISION_ID)
    )
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[iteration],
        episode=_escalation_episode(),
        escalated_block=_escalated_block(effect_evidence="known_failed"),
    )

    with pytest.raises(ConflictError, match="outstanding reviewer review dispatch"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_an_escalated_review_failure_on_a_different_block_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration()
    conn = ScriptedConnection()
    _script_authorization(
        conn,
        row=_task_values(
            status="blocked", current_gate_id=_GATE_ID, current_plan_revision=_REVISION_ID
        ),
    )
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[iteration],
        episode=_escalation_episode(escalated_block_id=_OTHER_BLOCK_ID),
        escalated_block=_escalated_block(
            block_id=_OTHER_BLOCK_ID, effect_evidence="pre_effect_not_applied"
        ),
        veto_gate=SimpleNamespace(
            id=_GATE_ID, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID, task_block_id=_BLOCK_ID
        ),
    )

    with pytest.raises(ConflictError, match="outstanding reviewer review dispatch"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_pr_review_iteration_on_the_current_head_vetoes_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration(
        plan_revision_id=None, task_pull_request_id=_PR_ID, reviewed_head_sha=_HEAD_SHA
    )
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="blocked"))
    _wire(
        monkeypatch,
        loops=[_pr_review_loop()],
        loop_iterations=[iteration],
        pull_request=_pull_request(),
    )

    with pytest.raises(ConflictError, match="outstanding reviewer review dispatch"):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_positively_proven_pre_send_review_failure_permits_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iteration = _review_iteration()
    blocked = _task_values(
        status="blocked", current_gate_id=_GATE_ID, current_plan_revision=_REVISION_ID
    )
    conn = ScriptedConnection()
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    _script_exceptional_gate_resolution(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_resolved"))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[iteration],
        episode=_escalation_episode(),
        escalated_block=_escalated_block(effect_evidence="pre_effect_not_applied"),
        veto_gate=SimpleNamespace(
            id=_GATE_ID, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID, task_block_id=_BLOCK_ID
        ),
        block=_unresolved_block(),
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.owner_gate is not None
    assert _event_types(conn) == ["owner_gate_resolved", "task_cancelled"]


def test_a_finalized_newer_iteration_supersedes_the_unfinalized_attempt_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unfinalized = _review_iteration(iteration_id=_ITERATION_ID, iteration_number=1)
    finalized_newer = _review_iteration(
        iteration_id=_LATER_ITERATION_ID, iteration_number=2, outcome="accepted"
    )
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_plan_revision=_REVISION_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[unfinalized, finalized_newer],
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


def test_a_finalized_review_iteration_permits_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_plan_revision=_REVISION_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[_review_iteration(outcome="accepted")],
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


def test_a_review_iteration_on_a_moved_on_subject_permits_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked", current_plan_revision=_REVISION_ID)
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        loops=[_planning_loop()],
        loop_iterations=[_review_iteration(plan_revision_id=_LATER_REVISION_ID)],
    )

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert _event_types(conn) == ["task_cancelled"]


def test_cancel_is_inapplicable_during_autonomous_progression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for status in ("planning", "implementing", "reviewing"):
        conn = ScriptedConnection()
        _script_authorization(conn, row=_task_values(status=status))
        _wire(monkeypatch)

        with pytest.raises(ConflictError, match="autonomous progression"):
            _cancel_call(_pool(conn), _CloseGithub())

        assert _count(conn, _ARCHIVE_WRITE) == 0
        assert _event_types(conn) == []


def test_cancel_of_an_archived_task_is_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="cancelled", archived=True))
    _wire(monkeypatch)

    with pytest.raises(StaleOperationError):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0


def test_a_stale_token_cancels_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn, row=_task_values(status="ready_to_plan", state_token=uuid4()))
    _wire(monkeypatch)

    with pytest.raises(StaleOperationError):
        _cancel_call(_pool(conn), _CloseGithub())

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_merge_confirmed_under_the_final_lock_routes_to_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B7 reconciliation persists a confirmed merge between the pre-decision
    observation and the final Task lock (no Task token rotation): the
    composition's locked PR re-read observes it, the read-only decision
    transaction rolls back, and E10a's completion path applies."""
    conn = ScriptedConnection()
    blocked = _task_values(status="blocked")
    _script_authorization(conn, row=blocked)
    _script_authorization(conn, row=blocked)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_pull_request(state=TaskPullRequestState.OPEN))
    )
    pr_sequence = [
        _pull_request(state=TaskPullRequestState.OPEN),
        _pull_request(state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED),
    ]
    pr_reads: list[int] = []

    def _pr_read(pool: Any, *, task_id: Any) -> Any:
        pr_reads.append(task_id)
        return pr_sequence[min(len(pr_reads), len(pr_sequence)) - 1]

    completion_calls: list[tuple[Any, Any]] = []

    def _completion(pool: Any, *, workspace_id: Any, task_id: Any) -> Any:
        completion_calls.append((workspace_id, task_id))
        return SimpleNamespace(
            task=SimpleNamespace(status=TaskStatus.COMPLETED, archived_at=_OBSERVED),
            pull_request=_pull_request(state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED),
        )

    _wire(monkeypatch, reconcile=reconcile)
    monkeypatch.setattr(task_cancellation, "get_task_pull_request_for_task", _pr_read)
    monkeypatch.setattr(task_cancellation, "complete_confirmed_task_merge", _completion)
    github = _CloseGithub()

    outcome = _cancel_call(_pool(conn), github)

    assert outcome.kind is TaskCancellationOutcomeKind.COMPLETED
    assert len(completion_calls) == 1
    # No Owner CANCELLED claim, no close obligation, no archival.
    assert github.close_calls == []
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _count(conn, _CLOSE_INTENT_INSERT) == 0
    assert _event_types(conn) == []


def test_the_complete_implementing_abort_settlement_path_keeps_the_execution_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full Owner path after the settlement correction (issue #298):
    IMPLEMENTING → Abort dispatch → positive settlement records the settled
    Interrupt intent only → the interrupted Execution stays truthfully open,
    and the existing accounted-runtime veto still refuses Cancel while it
    remains open (the Cancel-after-Abort remediation, issue #299, owns
    reopening that path)."""
    from openorc.services.task_abort import (
        TaskAbortOutcomeKind as _AbortOutcomeKind,
    )
    from openorc.services.task_abort import (
        TaskInterruptSettlement as _TaskInterruptSettlement,
    )
    from tests.test_services_task_abort import (
        _INTENT_INSERT as _ABORT_INTENT_INSERT,
    )
    from tests.test_services_task_abort import (
        _INTENT_UPDATE as _ABORT_INTENT_UPDATE,
    )
    from tests.test_services_task_abort import (
        _INTENTS_LIST as _ABORT_INTENTS_LIST,
    )
    from tests.test_services_task_abort import (
        _SESSION_ID as _ABORT_SESSION_ID,
    )
    from tests.test_services_task_abort import (
        _dispatch_call as _abort_dispatch_call,
    )
    from tests.test_services_task_abort import (
        _FakeClineAdapter,
    )
    from tests.test_services_task_abort import (
        _intent_row as _abort_intent_row,
    )
    from tests.test_services_task_abort import (
        _ready_session_row as _abort_ready_session_row,
    )
    from tests.test_services_task_abort import (
        _script_block_composition as _abort_script_block_composition,
    )
    from tests.test_services_task_abort import (
        _script_dispatch_head as _abort_script_dispatch_head,
    )
    from tests.test_services_task_abort import (
        _script_settlement_head as _abort_script_settlement_head,
    )
    from tests.test_services_task_abort import (
        _settlement_call as _abort_settlement_call,
    )

    execution = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        producer_session_id=_ABORT_SESSION_ID,
        execution_number=1,
        status=ExecutionStatus.RUNNING,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        implementation_result=None,
        last_qualifying_activity_at=None,
        review_iteration_id=None,
    )
    transitions: list[tuple[Any, Any, Any]] = []

    def _list_executions(pool: Any, *, task_id: Any) -> list[Any]:
        return [execution]

    def _update_execution(
        pool: Any, *, execution_id: Any, expected_status: Any, next_status: Any
    ) -> Any:
        transitions.append((execution_id, expected_status, next_status))
        if execution.id == execution_id and execution.status is expected_status:
            execution.status = next_status
            return execution
        return None

    monkeypatch.setattr(execution_records, "list_task_executions", _list_executions)
    monkeypatch.setattr(execution_records, "update_execution_status", _update_execution)

    conn = ScriptedConnection()
    # --- Phase 1: the Abort dispatch over the IMPLEMENTING Task ---
    _abort_script_dispatch_head(conn, status="implementing", session=_abort_ready_session_row())
    conn.on(_ABORT_INTENTS_LIST, [])
    _abort_script_block_composition(conn, status="implementing")
    conn.on(_ABORT_INTENT_INSERT, _abort_intent_row())
    conn.on(_ABORT_INTENT_UPDATE, _abort_intent_row(outcome={"dispatch": "acknowledged"}))
    adapter = _FakeClineAdapter()

    abort_outcome = _abort_dispatch_call(_pool(conn), adapter)

    assert abort_outcome.kind is _AbortOutcomeKind.INTERRUPTED
    assert adapter.abort_calls != []

    # --- Phase 2: the Owner re-issues Abort with positive D6b evidence ---
    _abort_script_settlement_head(conn, intents=[_abort_intent_row()])
    conn.on(
        _ABORT_INTENT_UPDATE,
        _abort_intent_row(status="settled", outcome={"settlement": "confirmed"}),
    )

    settlement_outcome = _abort_settlement_call(
        _pool(conn),
        adapter,
        settlement=_TaskInterruptSettlement(
            runtime_work_settled=True, operation_accounted_for=True
        ),
    )

    assert settlement_outcome.kind is _AbortOutcomeKind.SETTLED
    # Settlement records the Interrupt intent alone (issue #298): no
    # Execution finalization, the attempt stays truthfully open.
    assert transitions == []
    assert execution.status is ExecutionStatus.RUNNING

    # --- Phase 3: the accounted-runtime veto still refuses Cancel ---
    _wire(
        monkeypatch,
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.SETTLED)],
        executions=[execution],
    )
    conn2 = ScriptedConnection()
    _script_authorization(conn2, row=_task_values(status="blocked", state_token=_CANCELLED_TOKEN))
    _script_authorization(conn2, row=_task_values(status="blocked", state_token=_CANCELLED_TOKEN))

    with pytest.raises(ConflictError, match="open execution"):
        cancel_task(
            _pool(conn2),
            cast(Any, _CloseGithub()),
            cast(Any, object()),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_CANCELLED_TOKEN,
        )

    # The veto is a safety boundary, not a gap: nothing archived, nothing
    # resolved — the settled-but-open Execution awaits issue #299.
    assert _count(conn2, _ARCHIVE_WRITE) == 0
    assert _event_types(conn2) == []


# --- E10c: the trusted GitHub-observation cancellation (issue #229) ---------------


def _closure_call(pool: DatabasePool) -> Any:
    return cancel_task_for_confirmed_pr_closure(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
    )


def _script_closure_authorization(
    conn: ScriptedConnection,
    *,
    row: tuple[Any, ...],
) -> None:
    """The trusted path's head: the workspace-derived barrier and locked Task."""
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ_LOCKED, row)  # get_task_for_update


def _lock_evidence(monkeypatch: pytest.MonkeyPatch, pull_request: TaskPullRequest | None) -> None:
    """Script the row-locked canonical PR evidence read (issue #229)."""
    monkeypatch.setattr(
        task_subject_guards,
        "get_task_pull_request_for_task_for_update",
        lambda pool, *, workspace_id, task_id: pull_request,
    )


def _event_params_of_type(conn: Any, event_type: str) -> list[tuple[Any, ...]]:
    matched: list[tuple[Any, ...]] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None and params[2] == event_type:
            matched.append(params)
    return matched


def test_a_confirmed_closed_unmerged_observation_cancels_the_current_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The one trusted admission path (issue #229): the durable canonical PR
    # evidence is re-proved under the PR row lock, the shared archival core
    # applies, and the terminal event carries truthful GITHUB provenance
    # with the canonical pr_closed_unmerged terminal cause. A pending
    # OwnerGate is never resolved as if its Owner chose Cancel, and no
    # PR-close intent exists — the external closure already happened.
    conn = ScriptedConnection()
    _script_closure_authorization(
        conn,
        row=_task_values(status="waiting_for_owner", current_gate_id=_GATE_ID),
    )
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row(cause="pr_closed_unmerged"))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch)
    _lock_evidence(monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, merged_at=None))

    outcome = _closure_call(_pool(conn))

    assert outcome.kind is TaskPrClosureOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED
    assert outcome.task.terminal_cause is TaskTerminalCause.PR_CLOSED_UNMERGED
    assert outcome.pull_request is not None
    assert outcome.pull_request.state is TaskPullRequestState.CLOSED
    assert outcome.closed_loops == ()
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _CLOSE_INTENT_INSERT) == 0
    assert _event_types(conn) == ["task_cancelled"]
    (terminal_event,) = _event_params_of_type(conn, "task_cancelled")
    assert terminal_event[3] == "github"
    assert terminal_event[4] is None


@pytest.mark.parametrize(
    "status",
    [
        "ready_to_plan",
        "queued",
        "planning",
        "waiting_for_owner",
        "implementing",
        "reviewing",
        "blocked",
    ],
)
def test_the_trusted_closure_admits_every_non_archived_status(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    # Unlike the Owner command, the externally confirmed terminal closure is
    # not gated on runtime idleness or safe inactivity: every non-archived
    # status is cancellable when the evidence holds.
    conn = ScriptedConnection()
    _script_closure_authorization(
        conn, row=_task_values(status=status, queued=(status == "queued"))
    )
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row(cause="pr_closed_unmerged"))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch)
    _lock_evidence(monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, merged_at=None))

    outcome = _closure_call(_pool(conn))

    assert outcome.kind is TaskPrClosureOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED


def test_the_trusted_closure_closes_open_review_loops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loops = [SimpleNamespace(id=_LOOP_ID, purpose="pr_review", iteration_limit=5)]
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values(status="reviewing"))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row(cause="pr_closed_unmerged"))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch, loops=loops)
    _lock_evidence(monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, merged_at=None))

    outcome = _closure_call(_pool(conn))

    assert outcome.kind is TaskPrClosureOutcomeKind.CANCELLED
    assert len(outcome.closed_loops) == 1
    assert outcome.closed_loops[0].status == "closed"


def test_outstanding_runtime_work_does_not_veto_the_external_closure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The external closure is not gated on runtime idleness: an unsettled
    # interrupt intent, an open Execution, and a pending RuntimeRequest do
    # not veto it. Late results cannot advance the archived Task, and the
    # F4c cleanup waits for safe accounting independently.
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values(status="implementing"))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row(cause="pr_closed_unmerged"))
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(
        monkeypatch,
        interrupt_intents=[SimpleNamespace(status=TaskInterruptIntentStatus.UNSETTLED)],
        executions=[SimpleNamespace(status=ExecutionStatus.RUNNING)],
        runtime_requests=[SimpleNamespace(status=RuntimeRequestStatus.PENDING)],
    )
    _lock_evidence(monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, merged_at=None))

    outcome = _closure_call(_pool(conn))

    assert outcome.kind is TaskPrClosureOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED


def test_an_open_canonical_pull_request_causes_no_terminal_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values())
    _wire(monkeypatch)
    _lock_evidence(monkeypatch, _pull_request(state=TaskPullRequestState.OPEN))

    with pytest.raises(StaleOperationError):
        _closure_call(_pool(conn))

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_confirmed_merged_canonical_pull_request_refuses_the_closure_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Confirmed merged facts are E10a's territory: a merge confirmed under
    # the evidence lock (the raced-reconciliation case) refuses fail-closed
    # and applies nothing; the retryable read/reconciliation paths
    # re-classify and route it to the completion seam instead.
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values(status="reviewing"))
    _wire(monkeypatch)
    _lock_evidence(
        monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED)
    )

    with pytest.raises(StaleOperationError):
        _closure_call(_pool(conn))

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


@pytest.mark.parametrize("status", ["cancelled", "completed"])
def test_an_already_archived_task_is_the_idempotent_no_op(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    # Archived history is immutable whatever its terminal outcome: duplicate
    # webhook/sweep deliveries find the Task already archived and rewrite
    # nothing — no second archival, cause stamp, loop closure, or event.
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values(status=status, archived=True))
    _wire(monkeypatch)
    _lock_evidence(monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, merged_at=None))

    outcome = _closure_call(_pool(conn))

    assert outcome.kind is TaskPrClosureOutcomeKind.ALREADY_TERMINAL
    assert outcome.task.status is TaskStatus(status)
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_missing_canonical_pull_request_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values())
    _wire(monkeypatch)
    _lock_evidence(monkeypatch, None)

    with pytest.raises(NotFoundError):
        _closure_call(_pool(conn))

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_a_cross_workspace_canonical_pull_request_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_closure_authorization(conn, row=_task_values())
    _wire(monkeypatch)
    _lock_evidence(
        monkeypatch, _pull_request(state=TaskPullRequestState.CLOSED, workspace_id=uuid4())
    )

    with pytest.raises(NotFoundError):
        _closure_call(_pool(conn))

    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []
