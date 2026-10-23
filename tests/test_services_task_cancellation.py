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
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.runtime_requests import RuntimeRequestStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence import executions as execution_records
from openorc.persistence import intents as intent_records
from openorc.persistence import reviews as review_records
from openorc.persistence import runtime_requests as runtime_request_records
from openorc.persistence.pool import DatabasePool
from openorc.services import task_cancellation
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    StaleOperationError,
)
from openorc.services.task_cancellation import (
    TaskCancellationOutcomeKind,
    cancel_task,
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
_LOOP_ID = uuid4()
_GITHUB_PR_ID = 555

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
) -> tuple[Any, ...]:
    """One 15-column ``openorc.tasks`` row shaped like the durable model."""
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
        None,
        current_gate_id,
        "a" * 64,
        _OBSERVED if queued else None,
        _OBSERVED,
        _OBSERVED,
    )


def _archived_cancelled_row() -> tuple[Any, ...]:
    return _task_values(
        status="cancelled",
        state_token=_CANCELLED_TOKEN,
        current_gate_id=None,
        archived=True,
        queued=False,
    )


def _pull_request(
    *,
    state: TaskPullRequestState = TaskPullRequestState.OPEN,
    merged_at: datetime | None = None,
) -> TaskPullRequest:
    return TaskPullRequest(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
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
    _script_authorization(conn, row=_task_values(status="ready_to_plan"))
    _script_authorization(conn, row=_task_values(status="ready_to_plan"))
    conn.on(_ARCHIVE_WRITE, _archived_cancelled_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_cancelled"))
    _wire(monkeypatch)

    outcome = _cancel_call(_pool(conn), _CloseGithub())

    assert outcome.kind is TaskCancellationOutcomeKind.CANCELLED
    assert outcome.task.status is TaskStatus.CANCELLED
    assert outcome.task.archived_at is not None
    assert outcome.owner_gate is None
    assert outcome.close_intent is None
    assert _event_types(conn) == ["task_cancelled"]
    assert _count(conn, _GATE_READ) == 0
    assert _count(conn, _GATE_UPDATE) == 0


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
