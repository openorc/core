"""Deterministic tests for the E9 §2/§4 PR review decision seams (issue #84).

A scripted SQL-connection seam proves the decision, settlement, and
merge-gate seams on their critical paths:

- the ordinary decision finalizes once with the REVIEWER ``PR_REVIEWED``
  audit and commits the budget/stickiness-derived next decision: a
  within-budget ``CHANGES_REQUESTED`` keeps ``REVIEWING`` and routes the
  remediation outside the transaction; an exhausted ``CHANGES_REQUESTED``
  installs the exact PR/head ``REVIEW_RESOLUTION`` (limit event on the first
  exhaustion only); a sticky history skips the limit event; an ``ACCEPTED``
  installs the exact-head ``MERGE_DECISION`` and leaves the loop OPEN;
- the PR/head resolution settlement appends a NEW same-head finalized
  iteration, settles the gate through the ``SETTLED`` primitive with truthful
  Reviewer provenance, and installs a fresh MERGE_DECISION on acceptance or
  returns to ``REVIEWING`` with remediation routed on a change request;
- the actionable merge-gate change request retires G1 through the guarded
  ``reviewing`` destination — the gate row stays pending history, never
  Owner-stamped, never SETTLED, with no false gate-resolution event — while
  advisory acceptance is refused with nothing applied.

The Producer remediation routing is stubbed at the module seam (its own
module owns the claim/send/record/redispatch proofs).
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest

from openorc.services import task_pr_review_decision
from openorc.services.errors import InvalidCommandError
from openorc.services.task_pr_review_decision import (
    PrMergeGateChangeRequestDecision,
    PrReviewDecision,
    PrReviewGateResolutionDecision,
    finalize_pr_review_decision,
    retire_pr_merge_decision_with_changes,
    settle_pr_review_resolution,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _EXECUTIONS_LIST,
    _HEAD_SHA,
    _ITERATION_INSERT,
    _ITERATIONS_LIST,
    _LOOP_GUARD_LOCKED,
    _LOOP_ID,
    _LOOP_READ_LOCKED,
    _LOOPS_LIST,
    _OBSERVED,
    _OWNER_PROFILE_ID,
    _PR_ID,
    _PR_READ,
    _REPOSITORY_ID,
    _REVIEWER_EXTERNAL_ID,
    _REVIEWER_SESSION_ID,
    _SESSION_READ,
    _STATE_TOKEN,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    _WORKSPACE_READ,
    ScriptedConnection,
    _count,
    _iteration_row,
    _loop_row,
    _pool,
    _pr_row,
    _session_row,
    _sql_has,
    _task_row,
    _ws_row,
)

# SQL predicates for the decision seams' writes.
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")
_TASK_GATE_INSTALL = _sql_has("update openorc.tasks", "current_owner_gate_id = %s")
_TASK_GATE_CLEAR = _sql_has("update openorc.tasks", "current_owner_gate_id = null")
_STATUS_UPDATE = _sql_has("update openorc.tasks", "queued_at = case")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_RESULT_UPDATE = _sql_has("update openorc.review_iterations")
_PR_REVIEWED_EVENT = "pr_reviewed"
_GATE_RESOLVED_EVENT = "owner_gate_resolved"
_GATE_CREATED_EVENT = "owner_gate_created"
_LIMIT_EVENT = "review_limit_reached"
_DISCUSSION_EVENT_TYPE = "owner_reviewer_discussion_message"


def _event_returned_row(event_type: str) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type,
        "reviewer",
        "session-actor",
        None,
        None,
        {},
        _OBSERVED,
    )


def _event_types(conn: ScriptedConnection) -> list[str]:
    types: list[str] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None:
            types.append(str(params[2]))
    return types


def _gate_row(
    *,
    gate_id: Any,
    gate_type: str,
    head_sha: str | None = _HEAD_SHA,
    pr_id: Any | None = _PR_ID,
    status: str = "pending",
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        None,
        head_sha,
        pr_id,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _post_settlement_task_row(
    *, state_token: Any, status: str = "waiting_for_owner"
) -> tuple[Any, ...]:
    return _task_row(status=status, state_token=state_token)


def _stub_remediation(monkeypatch: pytest.MonkeyPatch, *, sentinel: str = "remediated"):
    calls: list[dict[str, Any]] = []

    def _stub(
        pool: Any,
        producer_runtime: Any,
        github: Any,
        reviewer_runtime_factory: Any,
        *,
        profile_id: Any,
        workspace_id: Any,
        task_id: Any,
        expected_state_token: Any,
        review_result: Any,
        addressed_iteration: Any,
    ):
        calls.append(
            {
                "expected_state_token": expected_state_token,
                "review_result": review_result,
                "addressed_iteration": addressed_iteration,
            }
        )
        return sentinel

    monkeypatch.setattr(task_pr_review_decision, "route_pr_remediation", _stub)
    return calls


def _stub_dispatch(monkeypatch: pytest.MonkeyPatch, *, sentinel: str = "redispatched"):
    calls: list[dict[str, Any]] = []

    def _stub(pool: Any, *args: Any, **kwargs: Any):
        calls.append(kwargs)
        return sentinel

    return calls


def _script_finalization(
    conn: ScriptedConnection,
    *,
    iterations: list[tuple[Any, ...]],
    gates: list[tuple[Any, ...]],
    reserved_id: Any,
    finalized_row: tuple[Any, ...],
) -> None:
    """The one short ordinary decision transaction up to the decision branch."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, iterations)
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))
    conn.on(_RESULT_UPDATE, finalized_row)
    conn.on(_EVENT_INSERT, _event_returned_row("pr_reviewed"))
    conn.on_repeat(_ITERATIONS_LIST, iterations)
    conn.on(_EXECUTIONS_LIST, [])
    conn.on(_GATES_LIST, gates)


def test_within_budget_changes_requested_keeps_reviewing_and_routes_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    reserved = _iteration_row(iteration_id=_RESERVED_ID, number=3)
    finalized_row = _iteration_row(
        iteration_id=_RESERVED_ID,
        number=3,
        outcome="changes_requested",
        summary="Needs work",
        findings=[{"summary": "a", "details": "b"}],
        decided_at=_OBSERVED,
    )
    _script_finalization(
        conn,
        iterations=[_finalized_row(1), _finalized_row(2), reserved],
        gates=[],
        reserved_id=_RESERVED_ID,
        finalized_row=finalized_row,
    )
    calls = _stub_remediation(monkeypatch)
    adapter = FakeAgentRuntimeAdapter()

    outcome = finalize_pr_review_decision(
        pool,
        adapter,
        _GitHubClient().as_client(),
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=_dispatch_outcome(reserved),
    )

    assert outcome.decision is PrReviewDecision.REMEDIATION_ROUTED
    assert outcome.task.status.value == "reviewing"
    assert outcome.gate is None
    assert outcome.remediation == "remediated"
    assert len(calls) == 1
    assert calls[0]["expected_state_token"] == _STATE_TOKEN
    assert calls[0]["addressed_iteration"].id == _RESERVED_ID
    # Exactly one PR_REVIEWED event; no gate rows, no gate events, no Task
    # status mutation, and no limit event inside the budget.
    assert _event_types(conn) == [_PR_REVIEWED_EVENT]
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _TASK_GATE_INSTALL) == 0


def test_exhausted_changes_requested_installs_exact_head_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    reserved = _iteration_row(iteration_id=_RESERVED_ID, number=4)
    finalized_row = _iteration_row(
        iteration_id=_RESERVED_ID,
        number=4,
        outcome="changes_requested",
        summary="Still broken",
        findings=[{"summary": "a", "details": "b"}],
        decided_at=_OBSERVED,
    )
    _script_finalization(
        conn,
        iterations=[
            _finalized_row(1),
            _finalized_row(2),
            _finalized_row(3),
            reserved,
        ],
        gates=[],
        reserved_id=_RESERVED_ID,
        finalized_row=finalized_row,
    )
    conn.on(_GATE_INSERT, _gate_row(gate_id=_GATE_ID, gate_type="review_resolution"))
    conn.on(
        _TASK_GATE_INSTALL,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN, status="waiting_for_owner"),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))
    conn.on(_EVENT_INSERT, _event_returned_row("review_limit_reached"))
    conn.on(
        _STATUS_UPDATE,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN_2, status="waiting_for_owner"),
    )
    calls = _stub_remediation(monkeypatch)
    adapter = FakeAgentRuntimeAdapter()

    outcome = finalize_pr_review_decision(
        pool,
        adapter,
        _GitHubClient().as_client(),
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=_dispatch_outcome(reserved),
    )

    assert outcome.decision is PrReviewDecision.RESOLUTION_INSTALLED
    assert outcome.gate is not None
    assert outcome.gate.gate_type.value == "review_resolution"
    assert outcome.gate.subject_head_sha == _HEAD_SHA
    assert outcome.gate.task_pull_request_id == _PR_ID
    assert outcome.task.status.value == "waiting_for_owner"
    assert calls == []
    # First exhaustion records the limit event exactly once; the gate install
    # and the waiting transition rotate tokens in order.
    assert _event_types(conn) == [
        _PR_REVIEWED_EVENT,
        _GATE_CREATED_EVENT,
        _LIMIT_EVENT,
    ]
    assert _count(conn, _GATE_INSERT) == 1


def test_sticky_history_skips_the_limit_event_and_goes_straight_to_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    reserved = _iteration_row(iteration_id=_RESERVED_ID, number=4)
    finalized_row = _iteration_row(
        iteration_id=_RESERVED_ID,
        number=4,
        outcome="changes_requested",
        summary="Still broken",
        findings=[{"summary": "a", "details": "b"}],
        decided_at=_OBSERVED,
    )
    _script_finalization(
        conn,
        iterations=[
            _finalized_row(1),
            _finalized_row(2),
            _finalized_row(3),
            reserved,
        ],
        gates=[_gate_row(gate_id=uuid.uuid4(), gate_type="merge_decision")],
        reserved_id=_RESERVED_ID,
        finalized_row=finalized_row,
    )
    conn.on(_GATE_INSERT, _gate_row(gate_id=_GATE_ID, gate_type="review_resolution"))
    conn.on(
        _TASK_GATE_INSTALL,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN, status="waiting_for_owner"),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))
    conn.on(
        _STATUS_UPDATE,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN_2, status="waiting_for_owner"),
    )
    calls = _stub_remediation(monkeypatch)
    adapter = FakeAgentRuntimeAdapter()

    outcome = finalize_pr_review_decision(
        pool,
        adapter,
        _GitHubClient().as_client(),
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=_dispatch_outcome(reserved),
    )

    assert outcome.decision is PrReviewDecision.RESOLUTION_INSTALLED
    assert calls == []
    # The sticky PR gate history means the already-exhausted budget is never
    # re-represented as newly reached.
    assert _event_types(conn) == [_PR_REVIEWED_EVENT, _GATE_CREATED_EVENT]


def test_accepted_review_installs_exact_head_merge_decision_and_leaves_loop_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    reserved = _iteration_row(iteration_id=_RESERVED_ID, number=1)
    finalized_row = _iteration_row(
        iteration_id=_RESERVED_ID,
        number=1,
        outcome="accepted",
        summary="Everything checks out.",
        findings=[],
        decided_at=_OBSERVED,
    )
    _script_finalization(
        conn,
        iterations=[reserved],
        gates=[],
        reserved_id=_RESERVED_ID,
        finalized_row=finalized_row,
    )
    conn.on(_GATE_INSERT, _gate_row(gate_id=_GATE_ID, gate_type="merge_decision"))
    conn.on(
        _TASK_GATE_INSTALL,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN, status="waiting_for_owner"),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))
    conn.on(
        _STATUS_UPDATE,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN_2, status="waiting_for_owner"),
    )
    calls = _stub_remediation(monkeypatch)
    adapter = FakeAgentRuntimeAdapter()

    outcome = finalize_pr_review_decision(
        pool,
        adapter,
        _GitHubClient().as_client(),
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=_dispatch_outcome(reserved, accepted=True),
    )

    assert outcome.decision is PrReviewDecision.ACCEPTED_MERGE_DECISION
    assert outcome.gate is not None
    assert outcome.gate.gate_type.value == "merge_decision"
    assert outcome.gate.subject_head_sha == _HEAD_SHA
    assert outcome.task.status.value == "waiting_for_owner"
    assert calls == []
    assert _event_types(conn) == [_PR_REVIEWED_EVENT, _GATE_CREATED_EVENT]
    # Acceptance never closes the loop.
    assert not any("update openorc.review_loops" in sql for sql, _params in conn.executed)


def test_settle_pr_review_resolution_appends_same_head_iteration_and_installs_merge_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolution_gate = _gate_row(gate_id=_GATE_ID, gate_type="review_resolution")
    _script_settlement(
        conn,
        gate_row=resolution_gate,
        settled_task_row=_post_settlement_task_row(state_token=_ROTATED_TOKEN),
    )
    conn.on(_GATE_INSERT, _gate_row(gate_id=_MERGE_GATE_ID, gate_type="merge_decision"))
    conn.on(
        _TASK_GATE_INSTALL,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN_2, status="waiting_for_owner"),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))
    adapter = FakeAgentRuntimeAdapter()

    outcome = settle_pr_review_resolution(
        pool,
        None,
        None,
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        review_result=_accepted_result(),
    )

    assert outcome.decision is PrReviewGateResolutionDecision.SETTLED_ACCEPTED
    assert outcome.review_iteration.task_pull_request_id == _PR_ID
    assert outcome.review_iteration.reviewed_head_sha == _HEAD_SHA
    assert outcome.settled_gate.id == _GATE_ID
    assert outcome.merge_decision_gate is not None
    assert outcome.merge_decision_gate.gate_type.value == "merge_decision"
    assert outcome.task.status.value == "waiting_for_owner"
    # The settled stamp carries truthful Reviewer provenance with the
    # settling iteration references.
    events = _event_types(conn)
    assert events == [
        _PR_REVIEWED_EVENT,
        _GATE_RESOLVED_EVENT,
        _GATE_CREATED_EVENT,
    ]


def test_settle_pr_review_resolution_routes_changes_requested_to_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolution_gate = _gate_row(gate_id=_GATE_ID, gate_type="review_resolution")
    _script_settlement(
        conn,
        gate_row=resolution_gate,
        settled_task_row=_post_settlement_task_row(state_token=_ROTATED_TOKEN),
    )
    conn.on(
        _STATUS_UPDATE,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN_2, status="reviewing"),
    )
    calls = _stub_remediation(monkeypatch)
    adapter = FakeAgentRuntimeAdapter()

    outcome = settle_pr_review_resolution(
        pool,
        adapter,
        _GitHubClient().as_client(),
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        review_result=_changes_requested_result(),
    )

    assert outcome.decision is PrReviewGateResolutionDecision.CONTINUATION_ROUTED
    assert outcome.merge_decision_gate is None
    assert outcome.task.status.value == "reviewing"
    assert len(calls) == 1
    # The remediation continues under the post-settlement rotated token.
    assert calls[0]["expected_state_token"] == _ROTATED_TOKEN_2
    assert _event_types(conn) == [_PR_REVIEWED_EVENT, _GATE_RESOLVED_EVENT]


def test_actionable_merge_gate_change_request_retires_g1_and_routes_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    merge_gate = _gate_row(gate_id=_GATE_ID, gate_type="merge_decision")
    _script_merge_retirement(conn, gate_row=merge_gate)
    conn.on(
        _TASK_GATE_CLEAR,
        _post_settlement_task_row(state_token=_ROTATED_TOKEN, status="reviewing"),
    )
    calls = _stub_remediation(monkeypatch)
    adapter = FakeAgentRuntimeAdapter()

    outcome = retire_pr_merge_decision_with_changes(
        pool,
        adapter,
        _GitHubClient().as_client(),
        lambda binding: _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        review_result=_changes_requested_result(),
        record_discussion_message_events=True,
    )

    assert outcome.decision is PrMergeGateChangeRequestDecision.MERGE_GATE_RETIRED
    assert outcome.retired_gate.id == _GATE_ID
    assert outcome.task.status.value == "reviewing"
    assert len(calls) == 1
    assert calls[0]["expected_state_token"] == _ROTATED_TOKEN
    # The retired gate is pending history: never Owner-stamped, never
    # SETTLED, and no false gate-resolution event exists.
    assert _count(conn, _GATE_UPDATE) == 0
    events = _event_types(conn)
    assert events == [
        _PR_REVIEWED_EVENT,
        _DISCUSSION_EVENT_TYPE,
        _DISCUSSION_EVENT_TYPE,
    ]


def test_advisory_acceptance_at_the_merge_gate_is_refused_with_nothing_applied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    calls = _stub_remediation(monkeypatch)

    with pytest.raises(InvalidCommandError):
        retire_pr_merge_decision_with_changes(
            pool,
            adapter,
            cast(Any, object()),
            lambda binding: _noop_attachment(adapter),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            review_result=_accepted_result(),
        )

    assert conn.executed == []
    assert calls == []


# --- shared constants and helpers ----------------------------------------------------

_RESERVED_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_MERGE_GATE_ID = uuid.uuid4()
_ROTATED_TOKEN = uuid.uuid4()
_ROTATED_TOKEN_2 = uuid.uuid4()


def _finalized_row(number: int) -> tuple[Any, ...]:
    return _iteration_row(
        iteration_id=uuid.uuid4(),
        number=number,
        outcome="changes_requested",
        summary=f"Round {number} needs work",
        findings=[{"summary": "a", "details": "b"}],
        decided_at=_OBSERVED,
    )


def _accepted_result() -> Any:
    from openorc.protocol.models import ReviewResult

    return ReviewResult(outcome="ACCEPTED", summary="Everything checks out.", findings=())


def _changes_requested_result() -> Any:
    from openorc.protocol.models import ReviewFinding, ReviewResult

    return ReviewResult(
        outcome="CHANGES_REQUESTED",
        summary="Needs work",
        findings=(ReviewFinding(summary="a", details="b"),),
    )


class _GitHubClient:
    """The minimal structural GitHub surface the decision seams validate."""

    def as_client(self) -> Any:
        return cast(Any, self)

    def get_installation_repository(self, **_kwargs: Any) -> Any:
        raise AssertionError("no GitHub call is expected in these tests")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("no GitHub call is expected in these tests")


def _noop_attachment(adapter: FakeAgentRuntimeAdapter) -> Any:
    from contextlib import contextmanager

    @contextmanager
    def _ctx(binding: Any):
        yield adapter

    return _ctx(None)


def _dispatch_outcome(reserved: tuple[Any, ...], *, accepted: bool = False) -> Any:
    """The internal dispatch continuation input, rebuilt from the scripted rows."""
    from openorc.domain.connections import WorkflowRole as _WorkflowRole
    from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
    from openorc.domain.reviews import (
        ReviewIteration,
        ReviewLoop,
        ReviewLoopPurpose,
        ReviewLoopStatus,
    )
    from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
    from openorc.domain.tasks import Task, TaskStatus
    from openorc.protocol.models import ReviewFinding, ReviewResult
    from openorc.services.task_pr_review_dispatch import TaskPrReviewDispatchOutcome

    findings = () if accepted else (ReviewFinding(summary="a", details="b"),)
    return TaskPrReviewDispatchOutcome(
        review_loop=ReviewLoop(
            id=_LOOP_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            purpose=ReviewLoopPurpose.PR_REVIEW,
            iteration_limit=2,
            status=ReviewLoopStatus.OPEN,
            closed_at=None,
            created_at=_OBSERVED,
        ),
        review_iteration=ReviewIteration(
            id=reserved[0],
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            review_loop_id=_LOOP_ID,
            iteration_number=reserved[4],
            plan_revision_id=None,
            task_pull_request_id=_PR_ID,
            reviewed_head_sha=_HEAD_SHA,
            outcome=None,
            summary=None,
            findings=None,
            decided_at=None,
            created_at=_OBSERVED,
        ),
        review_result=ReviewResult(
            outcome="ACCEPTED" if accepted else "CHANGES_REQUESTED",
            summary="Everything checks out." if accepted else "Needs work",
            findings=findings,
        ),
        reviewer_session=TaskAgentSession(
            id=_REVIEWER_SESSION_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=_WorkflowRole.REVIEWER,
            connection_id=uuid.uuid4(),
            external_session_id=_REVIEWER_EXTERNAL_ID,
            lifecycle_status=TaskSessionLifecycleStatus.READY,
            effective_config_snapshot={},
            reported_provider=None,
            reported_model=None,
            reported_runtime_version=None,
            initialized_at=_OBSERVED,
            ended_at=None,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
        ),
        task=Task(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            github_issue_id=503,
            github_issue_number=42,
            status=TaskStatus.REVIEWING,
            archived_at=None,
            canonical_feature_branch="feature/issue-42",
            state_token=_STATE_TOKEN,
            current_plan_revision_id=None,
            current_owner_gate_id=None,
            source_requirements_fingerprint="a" * 64,
            queued_at=None,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
        ),
        pull_request=TaskPullRequest(
            id=_PR_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            repository_id=_REPOSITORY_ID,
            github_pr_id=555001,
            github_pr_number=7,
            head_ref="feature/issue-42",
            base_ref="main",
            head_sha=_HEAD_SHA,
            state=TaskPullRequestState.OPEN,
            merged_at=None,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
        ),
        reconciled_head_sha=_HEAD_SHA,
    )


def _script_settlement(
    conn: ScriptedConnection,
    *,
    gate_row: tuple[Any, ...],
    settled_task_row: tuple[Any, ...],
) -> None:
    """The one short settlement transaction up to the decision branch."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_TASK_READ_LOCKED, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_GATE_READ, gate_row)
    conn.on(_PR_READ, _pr_row())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))
    conn.on(_ITERATIONS_LIST, [])
    conn.on(_LOOP_GUARD_LOCKED, ("open", "pr_review"))
    conn.on(_ITERATION_INSERT, _iteration_row(iteration_id=_RESERVED_ID, number=2))
    conn.on(
        _RESULT_UPDATE,
        _iteration_row(
            iteration_id=_RESERVED_ID,
            number=2,
            outcome="accepted",
            summary="Everything checks out.",
            findings=[],
            decided_at=_OBSERVED,
        ),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("pr_reviewed"))
    conn.on(_GATE_READ, gate_row)
    conn.on(_TASK_READ_LOCKED, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_GATE_UPDATE, gate_row)
    conn.on(_TASK_GATE_CLEAR, settled_task_row)
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_resolved"))


def _script_merge_retirement(
    conn: ScriptedConnection,
    *,
    gate_row: tuple[Any, ...],
) -> None:
    """The one short merge-gate retirement transaction up to the supersession."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_TASK_READ_LOCKED, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(_GATE_READ, gate_row)
    conn.on(_PR_READ, _pr_row())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))
    conn.on(_ITERATIONS_LIST, [])
    conn.on(_LOOP_GUARD_LOCKED, ("open", "pr_review"))
    conn.on(_ITERATION_INSERT, _iteration_row(iteration_id=_RESERVED_ID, number=2))
    conn.on(
        _RESULT_UPDATE,
        _iteration_row(
            iteration_id=_RESERVED_ID,
            number=2,
            outcome="changes_requested",
            summary="Needs work",
            findings=[{"summary": "a", "details": "b"}],
            decided_at=_OBSERVED,
        ),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("pr_reviewed"))
    conn.on(_GATE_READ, gate_row)
    conn.on(_TASK_READ_LOCKED, _task_row(status="waiting_for_owner", current_gate_id=_GATE_ID))
    conn.on(
        _TASK_GATE_CLEAR, _post_settlement_task_row(state_token=_ROTATED_TOKEN, status="reviewing")
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_reviewer_discussion_message"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_reviewer_discussion_message"))
