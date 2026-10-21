"""Deterministic tests for the E9 §3 durable one-shot PR remediation (issue #84).

A scripted SQL-connection seam and the deterministic fake Agent Runtime prove
the remediation seam on its critical paths:

- the one-shot claim: exactly one next-numbered Execution is allocated with
  the ``review_iteration_id`` claim binding, claimed ``running``, audited,
  and never rolled back; the ONE ``ReviseInteraction`` carries only the
  canonical ``review_result`` JSON on the original Producer ACT session;
- replaying the claim — after a successful completion, after a known-failed
  send, and with the Task's freshly rotated current token — allocates no
  second attempt and sends no second REVISE: the partial unique index makes
  the durable claim token-independent;
- the validated completion is recorded exactly once; a wrong branch claim
  and an unchanged reconciled head fail closed with no invented success and
  no re-dispatch; an uncertain send leaves the attempt conservatively
  running (the potentially-dispatched record) and is never resent.

The re-entered ordinary dispatch is stubbed at the module seam (the dispatch
module owns its own proofs).
"""

from __future__ import annotations

import types
import uuid
from typing import Any, cast

import pytest
from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.errors import AgentRuntimeUncertainOutcomeError
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import GitHubPullRequestObservation
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.services import task_pr_remediation
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    StaleOperationError,
)
from openorc.services.task_pr_remediation import route_pr_remediation
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    implementation_result_candidate,
    session_ready_candidate,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _BRANCH,
    _EXECUTIONS_LIST,
    _HEAD_SHA,
    _ITERATIONS_LIST,
    _LOOP_ID,
    _LOOP_READ_LOCKED,
    _LOOPS_LIST,
    _NEW_HEAD_SHA,
    _OBSERVED,
    _OWNER_PROFILE_ID,
    _PR_ID,
    _PR_READ,
    _PR_READ_LOCKED,
    _PR_UPDATE,
    _REPOSITORY_ID,
    _SESSION_READ,
    _STATE_TOKEN,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    _WORKSPACE_READ,
    FakePool,
    ScriptedConnection,
    _count,
    _execution_row,
    _installation_row,
    _iteration_row,
    _loop_row,
    _pool,
    _pr_row,
    _repository_row,
    _session_row,
    _sql_has,
    _task_row,
    _ws_row,
)

_PRODUCER_SESSION_ID = uuid.uuid4()
_PRODUCER_EXTERNAL_ID = "fake-session-1"
_EXECUTION_ID = uuid.uuid4()
_REVIEW_ITERATION_ID = uuid.uuid4()
_ROTATED_TOKEN = uuid.uuid4()


def _task_domain() -> Any:
    from openorc.domain.tasks import Task, TaskStatus

    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        github_issue_number=42,
        status=TaskStatus.REVIEWING,
        archived_at=None,
        canonical_feature_branch=_BRANCH,
        state_token=_STATE_TOKEN,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint="a" * 64,
        queued_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


# SQL predicates for the remediation seam's attempt writes.
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_EXECUTION_INSERT = _sql_has("insert into openorc.executions")
_CLAIM_UPDATE = _sql_has("update openorc.executions", "status = 'running'")
_COMPLETE_UPDATE = _sql_has("update openorc.executions", "status = 'succeeded'")


def _event_returned_row(event_type: str) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type,
        "producer",
        str(_PRODUCER_SESSION_ID),
        None,
        None,
        {},
        _OBSERVED,
    )


def _producer_session_row() -> tuple[Any, ...]:
    return _session_row("producer", _PRODUCER_SESSION_ID, _PRODUCER_EXTERNAL_ID)


def _addressed_iteration_row() -> tuple[Any, ...]:
    return _iteration_row(
        iteration_id=_REVIEW_ITERATION_ID,
        number=1,
        outcome="changes_requested",
        summary="Needs work",
        findings=[{"summary": "a", "details": "b"}],
        decided_at=_OBSERVED,
    )


def _claimed_execution_row(*, status: str = "running") -> tuple[Any, ...]:
    return _execution_row(
        execution_number=2,
        status=status,
        result_payload=None,
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )


def _review_result() -> Any:
    from openorc.protocol.models import ReviewFinding, ReviewResult

    return ReviewResult(
        outcome="CHANGES_REQUESTED",
        summary="Needs work",
        findings=(ReviewFinding(summary="a", details="b"),),
    )


def _addressed_iteration_domain() -> Any:
    from openorc.domain.reviews import ReviewIteration, ReviewOutcome

    return ReviewIteration(
        id=_REVIEW_ITERATION_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        review_loop_id=_LOOP_ID,
        iteration_number=1,
        plan_revision_id=None,
        task_pull_request_id=_PR_ID,
        reviewed_head_sha=_HEAD_SHA,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="Needs work",
        findings=[{"summary": "a", "details": "b"}],
        decided_at=_OBSERVED,
        created_at=_OBSERVED,
    )


def _prepared_producer_adapter(
    pool: FakePool,
    *,
    producer_outcomes: list[Any] | None = None,
) -> FakeAgentRuntimeAdapter:
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    producer = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert producer.external_session_id == _PRODUCER_EXTERNAL_ID
    for outcome in producer_outcomes or []:
        adapter.queue_send(_PRODUCER_EXTERNAL_ID, outcome)
    return adapter


def _stub_dispatch(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, Any]] = []

    def _stub(pool: Any, *args: Any, **kwargs: Any):
        calls.append(kwargs)
        return types.SimpleNamespace(task=_task_domain())

    monkeypatch.setattr(task_pr_remediation, "dispatch_pr_review", _stub)
    return calls


def _noop_attachment(adapter: FakeAgentRuntimeAdapter) -> Any:
    from contextlib import contextmanager

    @contextmanager
    def _ctx(binding: Any):
        yield adapter

    return _ctx(None)


class _GitHubClient:
    def __init__(self, pool: FakePool, *, observed_head: str = _NEW_HEAD_SHA) -> None:
        self._pool = pool
        self._observed_head = observed_head
        self.repository_calls: list[tuple[int, int]] = []
        self.pull_request_calls: list[int] = []

    def as_client(self) -> Any:
        return cast(Any, self)

    def get_installation_repository(self, **kwargs: Any) -> Any:
        assert self._pool.active_connections == 0
        self.repository_calls.append(
            (kwargs["github_installation_id"], kwargs["github_repository_id"])
        )
        from openorc.adapters.github import GitHubRepositoryObservation

        return GitHubRepositoryObservation(
            github_repository_id=987654321,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def get_repository_pull_request(self, **kwargs: Any) -> GitHubPullRequestObservation:
        assert self._pool.active_connections == 0
        self.pull_request_calls.append(kwargs["pull_number"])
        return GitHubPullRequestObservation(
            github_pr_id=900_719_925_474_099,
            pull_number=7,
            head_ref=_BRANCH,
            head_sha=self._observed_head,
            base_ref="main",
            state="open",
            merged=False,
            merged_at=None,
        )


def _remediation_call(
    pool: DatabasePool,
    adapter: FakeAgentRuntimeAdapter,
    github: Any,
    *,
    expected_state_token: Any = _STATE_TOKEN,
) -> Any:
    return route_pr_remediation(
        pool,
        adapter,
        github,
        _noop_attachment(adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        review_result=_review_result(),
        addressed_iteration=_addressed_iteration_domain(),
    )


def _script_claim(
    conn: ScriptedConnection,
    *,
    execution_insert_result: Any = None,
    task_state_token: Any = _STATE_TOKEN,
    claim_row: Any = None,
) -> None:
    """The one-shot claim transaction: subjects, guards, allocation, claim."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(state_token=task_state_token))
    conn.on(_TASK_READ_LOCKED, _task_row(state_token=task_state_token))
    conn.on(_TASK_READ, _task_row(state_token=task_state_token))
    conn.on(_SESSION_READ, _producer_session_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, [_addressed_iteration_row()])
    conn.on(_EXECUTIONS_LIST, [_execution_row()])
    # create_execution re-reads the referenced session's role inside its own
    # transaction (a Reviewer session never anchors an Execution).
    conn.on(_SESSION_READ, ("producer",))
    if execution_insert_result is not None:
        conn.on(_EXECUTION_INSERT, execution_insert_result)
    else:
        conn.on(_EXECUTION_INSERT, _claimed_execution_row())
    conn.on(_CLAIM_UPDATE, claim_row if claim_row is not None else _claimed_execution_row())
    conn.on(_EVENT_INSERT, _event_returned_row("execution_started"))


def _script_send_phase(
    conn: ScriptedConnection,
    *,
    observed_head: str = _NEW_HEAD_SHA,
) -> None:
    """The post-claim reads, R1 revalidation, the completion recording, and
    the fresh exact-head reconciliation up to the re-dispatch."""
    conn.on(_TASK_READ, _task_row())
    conn.on(_REPOSITORY_ROW_READ, _repository_row())
    # R1 revalidation after the attachment acquisition.
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_SESSION_READ, _producer_session_row())
    conn.on(_PR_READ, _pr_row())
    # The one-shot completion recording.
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_COMPLETE_UPDATE, _claimed_execution_row(status="succeeded"))
    conn.on(_EVENT_INSERT, _event_returned_row("execution_completed"))
    # Phase 3: the branch claim verification read and B7's reconciliation.
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_REPOSITORY_ROW_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_REPOSITORY_ROW_LOCKED, _repository_row())
    conn.on(_PR_READ_LOCKED, _pr_row())
    conn.on(_PR_UPDATE, _pr_row(head_sha=observed_head))


_REPOSITORY_ROW_READ = _sql_has("from openorc.repositories where id")
_REPOSITORY_ROW_LOCKED = _sql_has("from openorc.repositories where id", "for update")
_INSTALLATION_READ = _sql_has("from openorc.github_installations where id")


def test_the_one_shot_claim_allocates_one_attempt_and_sends_one_revise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_claim(conn)
    _script_send_phase(conn)
    adapter = _prepared_producer_adapter(
        fake_pool,
        producer_outcomes=[
            implementation_result_candidate(
                branch=_BRANCH,
                summary="Implemented the requested changes.",
                changes=["fix: applied review findings"],
                validation=["pytest green"],
            )
        ],
    )
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)
    dispatch_calls = _stub_dispatch(monkeypatch)

    outcome = _remediation_call(pool, adapter, github.as_client())

    # The durable claim: the next attempt row binds the addressed iteration.
    insert_params = next(params for sql, params in conn.executed if _EXECUTION_INSERT(sql, None))
    assert insert_params is not None
    assert insert_params[5] == _REVIEW_ITERATION_ID
    # Exactly one REVISE on the original Producer session, the entire message
    # being the canonical review_result JSON with no remediation prose.
    (control,) = adapter.realized_controls(_PRODUCER_EXTERNAL_ID)
    assert control.message is not None
    assert control.expected_family == "implementation_result"
    assert control.message.startswith('{"type": "review_result"')
    assert _HEAD_SHA not in control.message
    # The completion is recorded exactly once with the completed audit.
    assert _count(conn, _COMPLETE_UPDATE) == 1
    # The fresh head is GitHub-confirmed and the ordinary dispatch is
    # re-entered for it with the new evidence facts.
    assert outcome.new_head_sha == _NEW_HEAD_SHA
    assert len(dispatch_calls) == 1
    assert dispatch_calls[0]["expected_state_token"] == _STATE_TOKEN
    assert dispatch_calls[0]["published"].pull_request.head_sha == _NEW_HEAD_SHA


def test_replaying_the_claim_after_completion_allocates_no_second_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    # The partial unique index refuses the second claim for the same
    # iteration — the durable one-shot invariant, token-independent.
    _script_claim(conn, execution_insert_result=UniqueViolation("duplicate claim"))
    adapter = _prepared_producer_adapter(fake_pool)
    github = _GitHubClient(fake_pool)
    dispatch_calls = _stub_dispatch(monkeypatch)

    with pytest.raises(StaleOperationError):
        _remediation_call(pool, adapter, github.as_client())

    assert _count(conn, _CLAIM_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls()) == 0
    assert dispatch_calls == []


def test_replaying_the_claim_with_the_rotated_current_token_still_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_claim(
        conn,
        execution_insert_result=UniqueViolation("duplicate claim"),
        task_state_token=_ROTATED_TOKEN,
    )
    adapter = _prepared_producer_adapter(fake_pool)
    github = _GitHubClient(fake_pool)
    dispatch_calls = _stub_dispatch(monkeypatch)

    with pytest.raises(StaleOperationError):
        _remediation_call(pool, adapter, github.as_client(), expected_state_token=_ROTATED_TOKEN)

    assert _count(conn, _EXECUTION_INSERT) == 1
    assert len(adapter.realized_controls()) == 0
    assert dispatch_calls == []


def test_an_unchanged_reconciled_head_fails_closed_after_the_recorded_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_claim(conn)
    # The Producer reported completion but never pushed: the reconciliation
    # observes the SAME head and the re-dispatch never happens.
    _script_send_phase(conn, observed_head=_HEAD_SHA)
    conn.on(_PR_UPDATE, _pr_row(head_sha=_HEAD_SHA))
    adapter = _prepared_producer_adapter(
        fake_pool,
        producer_outcomes=[
            implementation_result_candidate(
                branch=_BRANCH,
                summary="Implemented the requested changes.",
                changes=["fix: applied review findings"],
                validation=["pytest green"],
            )
        ],
    )
    github = _GitHubClient(fake_pool, observed_head=_HEAD_SHA)
    dispatch_calls = _stub_dispatch(monkeypatch)

    with pytest.raises(ExternalOperationFailedError):
        _remediation_call(pool, adapter, github.as_client())

    # The completion stays truthfully recorded; no success is invented and
    # no new review is launched.
    assert _count(conn, _COMPLETE_UPDATE) == 1
    assert dispatch_calls == []


def test_a_wrong_branch_claim_fails_closed_before_any_github_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_claim(conn)
    _script_send_phase(conn)
    adapter = _prepared_producer_adapter(
        fake_pool,
        producer_outcomes=[
            implementation_result_candidate(
                branch="some/other-branch",
                summary="Implemented the requested changes.",
                changes=["fix: applied review findings"],
                validation=["pytest green"],
            )
        ],
    )
    github = _GitHubClient(fake_pool)
    dispatch_calls = _stub_dispatch(monkeypatch)

    with pytest.raises(ConflictError):
        _remediation_call(pool, adapter, github.as_client())

    assert github.repository_calls == []
    assert github.pull_request_calls == []
    assert dispatch_calls == []


def test_an_uncertain_send_leaves_the_claimed_attempt_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_claim(conn)
    # R1 revalidation runs; the realization ends uncertain.
    conn.on(_TASK_READ, _task_row())
    conn.on(_REPOSITORY_ROW_READ, _repository_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_SESSION_READ, _producer_session_row())
    conn.on(_PR_READ, _pr_row())
    adapter = _prepared_producer_adapter(
        fake_pool,
        producer_outcomes=[AgentRuntimeUncertainOutcomeError("the outcome is unknown")],
    )
    github = _GitHubClient(fake_pool)
    dispatch_calls = _stub_dispatch(monkeypatch)

    with pytest.raises(ExternalOperationUncertainError):
        _remediation_call(pool, adapter, github.as_client())

    # The potentially-dispatched record: the attempt is never finalized and
    # the single possibly-delivered send is never resent.
    assert _count(conn, _COMPLETE_UPDATE) == 0
    assert len(adapter.realized_controls()) == 1
    assert dispatch_calls == []
