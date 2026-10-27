"""Deterministic service tests for the read-only pending-interaction
projection (F2, issue #87).

The projection is the F2a input: one Task's current outstanding consequential
automated runtime interactions, composed from the durable claim facts with no
runtime-status probe. These tests prove the conservative eligibility rules —
current-work-only listing, honest-unknown ordering, and the empty result for
an idle READY session, an archived Task, or a terminal Task.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.domain.connections import WorkflowRole
from openorc.domain.pending_interactions import (
    PendingTaskRuntimeInteractionOperation,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.task_pending_runtime_interactions import (
    list_pending_task_runtime_interactions,
)

_OBSERVED = datetime(2026, 10, 27, 9, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_REPOSITORY_ID = uuid4()
_STATE_TOKEN = uuid4()
_PRODUCER_SESSION_ID = uuid4()
_REVIEWER_SESSION_ID = uuid4()
_CONNECTION_ID = uuid4()
_CLAIM_ID = uuid4()
_LOOP_ID = uuid4()
_ITERATION_ID = uuid4()
_EXECUTION_ID = uuid4()
_PR_ID = uuid4()
_HEAD_SHA = "1" * 40


class _Predicate:
    def __init__(self, *fragments: str, fn: Callable[[str], bool] | None = None) -> None:
        self._fragments = fragments
        self._fn = fn

    def __call__(self, sql: str) -> bool:
        if self._fn is not None:
            return self._fn(sql)
        lowered = " ".join(sql.split()).lower()
        return all(fragment in lowered for fragment in self._fragments)


def _sql_has(*fragments: str) -> _Predicate:
    return _Predicate(*fragments)


def _role_is(predicate: _Predicate) -> _Predicate:
    """Narrow a session-read predicate to the role-parameterized SELECT."""
    return _Predicate(
        fn=lambda sql: predicate(sql) and "%s and role = %s" in " ".join(sql.split()).lower()
    )


class FakeCursor:
    def __init__(self, row: Any) -> None:
        self._row = row

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        if self._row is None:
            return []
        if isinstance(self._row, list):
            return list(self._row)
        return [self._row]


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler; handlers
    are consumed in registration order unless registered as repeating."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, Any, bool]] = []

    def on(self, predicate: _Predicate, result: Any) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: Any) -> None:
        self._handlers.append((predicate, result, False))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        for index, (predicate, result, once) in enumerate(self._handlers):
            if predicate(sql):
                if once:
                    self._handlers.pop(index)
                if isinstance(result, Exception):
                    raise result
                if callable(result):
                    result = result(self)
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {' '.join(sql.split())[:90]}")

    def transaction(self):  # noqa: ANN201 - test fake
        class _Noop:
            def __enter__(self) -> None:
                return None

            def __exit__(self, *_args: object) -> bool:
                return False

        return _Noop()


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        conn = self._conn

        @contextmanager
        def managed() -> Any:
            yield conn

        return managed()

    def close(self) -> None:
        raise AssertionError("the projection tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _task_row(
    *,
    status: str = "planning",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = None,
    gate_id: Any = None,
    archived: bool = False,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        503,
        42,
        status,
        _OBSERVED if archived else None,
        None,
        state_token,
        current_plan_revision_id,
        gate_id,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _session_row(role: str, session_id: Any) -> tuple[Any, ...]:
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        f"ext-{str(session_id)[:8]}",
        "ready",
        {"adapter": "cline"},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _claim_row(
    *,
    kind: str,
    status: str = "claimed",
    review_iteration_id: Any = None,
    owner_gate_id: Any = None,
    subject_head_sha: Any = None,
) -> tuple[Any, ...]:
    settled_at = _OBSERVED if status != "claimed" else None
    return (
        _CLAIM_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        kind,
        _PRODUCER_SESSION_ID,
        review_iteration_id,
        owner_gate_id,
        subject_head_sha,
        status,
        {},
        _OBSERVED,
        settled_at,
        _OBSERVED,
    )


def _loop_row(*, status: str = "open", purpose: str = "planning") -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, purpose, 3, status, None, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any = _ITERATION_ID,
    number: int = 1,
    plan_revision_id: Any = None,
    task_pull_request_id: Any = None,
    reviewed_head_sha: Any = None,
    outcome: Any = None,
) -> tuple[Any, ...]:
    finalized = outcome is not None
    return (
        iteration_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _LOOP_ID,
        number,
        plan_revision_id,
        task_pull_request_id,
        reviewed_head_sha,
        outcome,
        "Needs work" if finalized else None,
        (("clarify",),) if finalized else None,
        _OBSERVED if finalized else None,
        _OBSERVED,
    )


def _pr_row(*, pr_id: Any = None, head_sha: Any = None, state: str = "open") -> tuple[Any, ...]:
    return (
        pr_id if pr_id is not None else _PR_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        7001,
        5,
        "openorc/task-branch",
        "main",
        head_sha if head_sha is not None else _HEAD_SHA,
        state,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _execution_row(*, status: str = "running", review_iteration_id: Any = None) -> tuple[Any, ...]:
    return (
        _EXECUTION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _PRODUCER_SESSION_ID,
        1,
        status,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
        review_iteration_id,
    )


_TASK_READ = _sql_has("from openorc.tasks where id = %s")
_EVIDENCE_READ = _sql_has("planning_bootstrap_claim_token")
_CLAIMS_LIST = _sql_has("from openorc.task_runtime_send_claims where task_id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions where task_id")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_ITERATION_READ = _sql_has("from openorc.review_iterations where id = %s")
_ITERATIONS_LIST = _sql_has("from openorc.review_iterations where review_loop_id")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")
_PR_READ = _sql_has("from openorc.task_pull_requests where task_id")


def _script_projection(
    conn: ScriptedConnection,
    *,
    task: tuple[Any, ...],
    evidence: Any = None,
    claims: list[tuple[Any, ...]] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    iteration: Any = None,
    iterations: list[tuple[Any, ...]] | None = None,
    executions: list[tuple[Any, ...]] | None = None,
    pr_row: Any = None,
    producer_session: Any = None,
    reviewer_session: Any = None,
) -> None:
    """Script one read-only projection composition, in call order."""
    conn.on(_TASK_READ, task)
    conn.on_repeat(_EVIDENCE_READ, evidence)
    conn.on_repeat(_CLAIMS_LIST, claims or [])
    if iteration is not None:
        conn.on_repeat(_ITERATION_READ, iteration)
    if producer_session is not None:
        conn.on_repeat(_role_is(_SESSION_READ), producer_session)
    conn.on_repeat(
        _role_is(_SESSION_READ),
        reviewer_session
        if reviewer_session is not None
        else _session_row("reviewer", _REVIEWER_SESSION_ID),
    )
    conn.on_repeat(_LOOPS_LIST, loops or [])
    if iterations is not None:
        conn.on_repeat(_ITERATIONS_LIST, iterations)
    conn.on_repeat(_EXECUTIONS_LIST, executions or [])
    conn.on_repeat(_PR_READ, pr_row)


def test_idle_ready_producer_session_lists_empty() -> None:
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="planning"),
        # The bootstrap claimed and settled into its stage evidence: the
        # preparation resolved into its later continuations and the bootstrap
        # itself is never listed again.
        evidence=(_STATE_TOKEN, _OBSERVED, "ready_for_planning", _OBSERVED),
    )

    assert (
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_claimed_initial_plan_dispatch_is_the_outstanding_send() -> None:
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="planning", current_plan_revision_id=None),
        claims=[_claim_row(kind="initial_plan_dispatch")],
    )

    (interaction,) = list_pending_task_runtime_interactions(
        _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert interaction.operation is PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH
    assert interaction.role is WorkflowRole.PRODUCER
    assert interaction.task_agent_session_id == _PRODUCER_SESSION_ID
    assert interaction.subject == {"send_claim_id": str(_CLAIM_ID)}
    assert interaction.started_at == _OBSERVED


def test_moved_on_subjects_are_never_listed() -> None:
    conn = ScriptedConnection()
    stale_revision = uuid4()
    _script_projection(
        conn,
        # The plan revision published: the initial-plan claim is a resolved
        # attempt; the revise claim's iteration addresses an older revision.
        task=_task_row(status="planning", current_plan_revision_id=uuid4()),
        claims=[
            _claim_row(kind="initial_plan_dispatch"),
            _claim_row(kind="planning_revise_dispatch", review_iteration_id=_ITERATION_ID),
            _claim_row(kind="initial_plan_dispatch", status="settled"),
        ],
        iteration=_iteration_row(plan_revision_id=stale_revision),
    )

    assert (
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_pr_compose_claim_lists_only_without_a_canonical_pr() -> None:
    gate_id = uuid4()
    head_sha = "0" * 40
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="waiting_for_owner", current_plan_revision_id=uuid4()),
        claims=[
            _claim_row(
                kind="pr_compose_dispatch",
                owner_gate_id=gate_id,
                subject_head_sha=head_sha,
            )
        ],
        pr_row=None,
    )
    (interaction,) = list_pending_task_runtime_interactions(
        _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert interaction.operation is PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH
    assert interaction.subject == {
        "send_claim_id": str(_CLAIM_ID),
        "owner_gate_id": str(gate_id),
        "subject_head_sha": head_sha,
    }

    # The canonical PR already exists: the compose attempt is resolved.
    conn2 = ScriptedConnection()
    _script_projection(
        conn2,
        task=_task_row(status="waiting_for_owner", current_plan_revision_id=uuid4()),
        claims=[
            _claim_row(
                kind="pr_compose_dispatch",
                owner_gate_id=gate_id,
                subject_head_sha=head_sha,
            )
        ],
        pr_row=(
            uuid4(),
            _WORKSPACE_ID,
            _TASK_ID,
            _REPOSITORY_ID,
            7001,
            5,
            "openorc/task-branch",
            "main",
            "1" * 40,
            "open",
            None,
            _OBSERVED,
            _OBSERVED,
        ),
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn2), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_unstaged_bootstrap_is_outstanding_and_resolved_is_not() -> None:
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="planning", current_plan_revision_id=None),
        evidence=(_STATE_TOKEN, _OBSERVED, None, None),
        producer_session=_session_row("producer", _PRODUCER_SESSION_ID),
    )
    (interaction,) = list_pending_task_runtime_interactions(
        _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert (
        interaction.operation is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE
    )
    assert interaction.started_at == _OBSERVED
    assert interaction.subject == {
        "planning_bootstrap_claim_token": str(_STATE_TOKEN),
        "formal_result_outstanding": True,
    }

    # Residue claimed before the instant evidence existed carries None.
    conn2 = ScriptedConnection()
    _script_projection(
        conn2,
        task=_task_row(status="planning", current_plan_revision_id=None),
        evidence=(_STATE_TOKEN, None, None, None),
        producer_session=_session_row("producer", _PRODUCER_SESSION_ID),
    )
    (residue,) = list_pending_task_runtime_interactions(
        _pool(conn2), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert residue.started_at is None

    # A published plan revision resolves the bootstrap at any stage.
    conn3 = ScriptedConnection()
    _script_projection(
        conn3,
        task=_task_row(status="planning", current_plan_revision_id=uuid4()),
        evidence=(_STATE_TOKEN, _OBSERVED, "prepared", _OBSERVED),
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn3), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_prepared_stage_reports_the_outstanding_context_transition() -> None:
    """Issue #87 review fix: a settled ``prepared`` stage is NOT completion
    of the bootstrap — the effectful planning-context transition it
    authorized is still potentially unfinished and must be projected, or
    F2a could never observe a stall between the stage settlement and the
    transition."""
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="planning", current_plan_revision_id=None),
        evidence=(_STATE_TOKEN, _OBSERVED, "prepared", _OBSERVED),
        producer_session=_session_row("producer", _PRODUCER_SESSION_ID),
    )
    (interaction,) = list_pending_task_runtime_interactions(
        _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert (
        interaction.operation
        is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION
    )
    assert interaction.role is WorkflowRole.PRODUCER
    # The exact Producer binding and the existing bootstrap claim identity.
    assert interaction.task_agent_session_id == _PRODUCER_SESSION_ID
    assert interaction.subject == {
        "planning_bootstrap_claim_token": str(_STATE_TOKEN),
        # No formal result is outstanding: the PREPARE result already
        # validated; the transition itself is the unsettled effect.
        "formal_result_outstanding": False,
    }
    # The committed start evidence: the stage settlement instant is the
    # instant the validated PREPARE result started the transition.
    assert interaction.started_at == _OBSERVED


def test_ready_for_planning_stage_is_never_an_outstanding_bootstrap() -> None:
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="planning", current_plan_revision_id=None),
        evidence=(_STATE_TOKEN, _OBSERVED, "ready_for_planning", _OBSERVED),
        producer_session=_session_row("producer", _PRODUCER_SESSION_ID),
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_reviewer_review_and_running_execution_are_outstanding() -> None:
    current_revision = uuid4()
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="planning", current_plan_revision_id=current_revision),
        loops=[_loop_row()],
        iterations=[
            _iteration_row(
                iteration_id=uuid4(),
                number=1,
                plan_revision_id=current_revision,
                outcome="changes_requested",
            ),
            _iteration_row(iteration_id=_ITERATION_ID, number=2, plan_revision_id=current_revision),
        ],
        executions=[_execution_row(status="running")],
        reviewer_session=_session_row("reviewer", _REVIEWER_SESSION_ID),
    )
    interactions = list_pending_task_runtime_interactions(
        _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    # Deterministic ordering: committed start instant, then operation, then
    # subject identity.
    assert [interaction.operation for interaction in interactions] == [
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    ]
    review = interactions[1]
    assert review.role is WorkflowRole.REVIEWER
    assert review.task_agent_session_id == _REVIEWER_SESSION_ID
    assert review.subject == {
        "review_loop_id": str(_LOOP_ID),
        "review_iteration_id": str(_ITERATION_ID),
        "iteration_number": 2,
        "purpose": "planning",
        "plan_revision_id": str(current_revision),
    }


def test_a_superseded_unfinalized_attempt_is_never_listed() -> None:
    """Issue #87 review fix: a newer iteration — finalized or not —
    supersedes an older unfinalized attempt; the projection lists the latest
    iteration of the loop overall, never a superseded one."""
    conn = ScriptedConnection()
    current_revision = uuid4()
    _script_projection(
        conn,
        task=_task_row(status="planning", current_plan_revision_id=current_revision),
        loops=[_loop_row()],
        iterations=[
            _iteration_row(
                iteration_id=_ITERATION_ID,
                number=1,
                plan_revision_id=current_revision,
            ),
            _iteration_row(
                iteration_id=uuid4(),
                number=2,
                plan_revision_id=current_revision,
                outcome="changes_requested",
            ),
        ],
        reviewer_session=_session_row("reviewer", _REVIEWER_SESSION_ID),
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_a_review_off_the_current_phase_or_subject_is_never_listed() -> None:
    """Issue #87 review fix: the outstanding REVIEW projection mirrors the
    dispatch services' currentness proofs — the eligible active phase and
    the exact current workflow subject."""
    current_revision = uuid4()
    stale_revision = uuid4()
    reviewer = _session_row("reviewer", _REVIEWER_SESSION_ID)

    # An implementing Task's planning loop is not a live review.
    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="implementing", current_plan_revision_id=current_revision),
        loops=[_loop_row()],
        iterations=[
            _iteration_row(iteration_id=_ITERATION_ID, number=1, plan_revision_id=current_revision)
        ],
        reviewer_session=reviewer,
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )

    # An Owner-gated waiting Task has no live review either.
    conn2 = ScriptedConnection()
    _script_projection(
        conn2,
        task=_task_row(
            status="waiting_for_owner",
            current_plan_revision_id=current_revision,
            gate_id=uuid4(),
        ),
        loops=[_loop_row()],
        iterations=[
            _iteration_row(iteration_id=_ITERATION_ID, number=1, plan_revision_id=current_revision)
        ],
        reviewer_session=reviewer,
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn2), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )

    # A planning attempt addressing a revision that is no longer current.
    conn3 = ScriptedConnection()
    _script_projection(
        conn3,
        task=_task_row(status="planning", current_plan_revision_id=current_revision),
        loops=[_loop_row()],
        iterations=[
            _iteration_row(iteration_id=_ITERATION_ID, number=1, plan_revision_id=stale_revision)
        ],
        reviewer_session=reviewer,
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn3), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_a_current_pr_review_attempt_is_listed_and_a_moved_on_one_is_not() -> None:
    """Issue #87 review fix: a PR-review attempt is current work only while
    the Task is REVIEWING with no current Owner gate, the canonical PR is
    exactly the addressed one, and the reviewed head is still the PR's
    head."""
    reviewer = _session_row("reviewer", _REVIEWER_SESSION_ID)
    pr_iteration = _iteration_row(
        iteration_id=_ITERATION_ID,
        number=1,
        task_pull_request_id=_PR_ID,
        reviewed_head_sha=_HEAD_SHA,
    )

    conn = ScriptedConnection()
    _script_projection(
        conn,
        task=_task_row(status="reviewing"),
        loops=[_loop_row(purpose="pr_review")],
        iterations=[pr_iteration],
        pr_row=_pr_row(),
        reviewer_session=reviewer,
    )
    (interaction,) = list_pending_task_runtime_interactions(
        _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert interaction.operation is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW
    assert interaction.subject == {
        "review_loop_id": str(_LOOP_ID),
        "review_iteration_id": str(_ITERATION_ID),
        "iteration_number": 1,
        "purpose": "pr_review",
        "task_pull_request_id": str(_PR_ID),
        "reviewed_head_sha": _HEAD_SHA,
    }

    # The reviewed head is no longer the PR's head: a stale attempt.
    conn2 = ScriptedConnection()
    _script_projection(
        conn2,
        task=_task_row(status="reviewing"),
        loops=[_loop_row(purpose="pr_review")],
        iterations=[pr_iteration],
        pr_row=_pr_row(head_sha="2" * 40),
        reviewer_session=reviewer,
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn2), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )

    # A current Owner gate suspends the review dispatch.
    conn3 = ScriptedConnection()
    _script_projection(
        conn3,
        task=_task_row(status="reviewing", gate_id=uuid4()),
        loops=[_loop_row(purpose="pr_review")],
        iterations=[pr_iteration],
        pr_row=_pr_row(),
        reviewer_session=reviewer,
    )
    assert (
        list_pending_task_runtime_interactions(
            _pool(conn3), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
        == []
    )


def test_archived_and_terminal_tasks_never_list_history() -> None:
    conn = ScriptedConnection()
    for task_row in (
        _task_row(status="cancelled", archived=True),
        _task_row(status="completed", archived=True),
        _task_row(status="cancelled", archived=True),
    ):
        conn = ScriptedConnection()
        conn.on(_TASK_READ, task_row)
        assert (
            list_pending_task_runtime_interactions(
                _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
            )
            == []
        )


def test_command_validation() -> None:
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        list_pending_task_runtime_interactions(
            _pool(conn),
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK_ID,
        )
    with pytest.raises(NotFoundError):
        conn.on(_TASK_READ, None)
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
    with pytest.raises(NotFoundError):
        other_task = _task_row()
        foreign_row = (other_task[0], uuid4()) + other_task[2:]
        conn.on(_TASK_READ, foreign_row)
        list_pending_task_runtime_interactions(
            _pool(conn), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )
