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

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import GitHubPullRequestObservation
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.services import task_pr_remediation
from openorc.services.errors import (
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    StaleOperationError,
)
from openorc.services.task_pr_remediation import (
    SettledPrRemediationVerification,
    verify_settled_pr_remediation_push,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    session_ready_candidate,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _BRANCH,
    _EXECUTIONS_LIST,
    _HEAD_SHA,
    _IMPLEMENTATION_PAYLOAD,
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
_CLAIM_UPDATE = _sql_has("update openorc.executions", "set status = 'running'")
_COMPLETE_UPDATE = _sql_has("update openorc.executions", "set status = 'succeeded'")


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


def _noop_attachment(adapter: FakeAgentRuntimeAdapter) -> Any:
    from contextlib import contextmanager

    @contextmanager
    def _ctx(binding: Any):
        yield adapter

    return _ctx(None)


class _GitHubClient:
    def __init__(
        self,
        pool: FakePool,
        *,
        observed_head: str = _NEW_HEAD_SHA,
        observed_state: str = "open",
        observed_merged: bool = False,
        observed_merged_at: Any = None,
    ) -> None:
        self._pool = pool
        self._observed_head = observed_head
        self._observed_state = observed_state
        self._observed_merged = observed_merged
        self._observed_merged_at = observed_merged_at
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
            state=self._observed_state,
            merged=self._observed_merged,
            merged_at=self._observed_merged_at,
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
    # The one-shot completion recording: Task, Producer binding identity,
    # and canonical PR identity — deliberately without requiring the old
    # head to remain the persisted reconciled head.
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_SESSION_READ, _producer_session_row())
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


# --- the detached Producer remediation dispatch and the settled continuation
# --- verification (issue #338) ---------------------------------------------------------

_REMEDIATION_RECEIPT_ID = uuid.uuid4()
_REMEDIATION_TURN_REFERENCE = "remediation-turn-1"
_RECEIPT_INSERT = _sql_has("insert into openorc.task_hub_run_receipts")
_RECEIPT_FOR_UPDATE = _sql_has("from openorc.task_hub_run_receipts", "where id = %s for update")
_RECEIPT_ATTACH = _sql_has("update openorc.task_hub_run_receipts", "set status = 'admitted'")
_RECEIPT_REFUSE = _sql_has("update openorc.task_hub_run_receipts", "set status = 'refused'")
_EARLY_COMPLETION_READ = _sql_has("from openorc.task_hub_run_early_completions", "for update")
_EXECUTION_STATUS_UPDATE = _sql_has("update openorc.executions set status = %s")


def _remediation_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    if status == "settled":
        outcome: dict[str, Any] = {"settlement": "validated_result"}
    elif status == "refused":
        outcome = {"refusal_kind": "admission_refused"}
    else:
        outcome = {}
    return (
        _REMEDIATION_RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _PRODUCER_SESSION_ID,
        "pr_remediation_execution",
        "execution",
        _EXECUTION_ID,
        "implementation_result",
        admitted_turn_reference,
        status,
        outcome,
        None,
        _OBSERVED,
        admitted_at,
        settled_at,
        _OBSERVED,
        None,
        None,
        None,
        None,
    )


class _RecordingRegistrar:
    """Records the pre-admission observation registrations (issue #323 seam)."""

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.registrations: list[dict[str, Any]] = []

    def register_session(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        task_agent_session_id: Any,
        connection_id: Any,
        external_session_id: Any,
    ) -> None:
        self.registrations.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "task_agent_session_id": task_agent_session_id,
                "connection_id": connection_id,
                "external_session_id": external_session_id,
            }
        )
        if self._error is not None:
            raise self._error


class _RecordingHandoff:
    """Records the ONE post-commit completion handoff invocations (issue #338)."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.interrupt_calls: list[dict[str, Any]] = []

    def handoff_settled_completion(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        operation: Any,
        validated_result: Any = None,
    ) -> None:
        self.calls.append(
            {"workspace_id": workspace_id, "task_id": task_id, "operation": operation}
        )

    def handoff_settled_interrupt(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        task_agent_session_id: Any,
        external_session_id: str,
    ) -> None:
        self.interrupt_calls.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "task_agent_session_id": task_agent_session_id,
                "external_session_id": external_session_id,
            }
        )


def _producer_factory(adapter: FakeAgentRuntimeAdapter) -> Any:
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        yield adapter

    return _ctx


def _script_r1(conn: ScriptedConnection) -> None:
    """R1: the post-attachment authority revalidation before the receipt."""
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_SESSION_READ, _producer_session_row())
    conn.on(_PR_READ, _pr_row())


def _script_attach(conn: ScriptedConnection) -> None:
    """The acknowledgement attach (claimed → admitted, no staged completion)."""
    conn.on(_RECEIPT_FOR_UPDATE, _remediation_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _remediation_receipt_row(
            status="admitted", admitted_turn_reference=_REMEDIATION_TURN_REFERENCE
        ),
    )
    conn.on(_EARLY_COMPLETION_READ, None)


def _execution_insert_id(conn: ScriptedConnection) -> Any:
    params = next(params for sql, params in conn.executed if _EXECUTION_INSERT(sql, None))
    assert params is not None
    return params[0]


# The Level 3 block installation's SQL predicates and canned rows (the shared
# F3 exceptional-blocking foundation, mirroring the E8 block shape).
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_GATES_INSERT = _sql_has("insert into openorc.owner_gates")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks set current_owner_gate_id")


def _remediation_block_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "stale_operation",
        {},
        None,
        _OBSERVED,
    )


def _remediation_block_gate_row() -> tuple[Any, ...]:
    """The pending Type B ``BLOCK_RESOLUTION`` gate an unverifiable round
    installs, bound to its fresh TaskBlock subject."""
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "block_resolution",
        "pending",
        None,
        None,
        None,
        uuid.uuid4(),
        None,
        _OBSERVED,
    )


def _event_types(conn: ScriptedConnection) -> list[str]:
    types: list[str] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None:
            types.append(str(params[2]))
    return types


def _verification_pool_setup(
    conn: ScriptedConnection,
    *,
    execution_row: Any,
    iteration_rows: list[Any],
    observed_head: str | None,
    update_row: Any = None,
) -> FakePool:
    """Phase 1 subject set + (optionally) B7 reconciliation + post reads."""
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on_repeat(_ITERATIONS_LIST, iteration_rows)
    conn.on(_EXECUTIONS_LIST, [execution_row])
    if observed_head is not None:
        # B7's phase-1 reads, the phase-3 locked write phase, and the post
        # reads.
        conn.on(_TASK_READ, _task_row())
        conn.on(_PR_READ, _pr_row())
        conn.on(_REPOSITORY_ROW_READ, _repository_row())
        conn.on(_INSTALLATION_READ, _installation_row())
        conn.on(_WORKSPACE_READ, _ws_row())
        conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
        conn.on(_REPOSITORY_ROW_LOCKED, _repository_row())
        conn.on(_PR_READ_LOCKED, _pr_row())
        conn.on(_PR_UPDATE, update_row or _pr_row(head_sha=observed_head))
        conn.on(_TASK_READ, _task_row())
    return FakePool(conn)


def _script_verification_block(conn: ScriptedConnection) -> None:
    """The atomic Level 3 block installation for an unverifiable round: the
    account barrier, the locked currentness re-proof, and the F3 foundation's
    block/gate/transition/audit composition."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    # The F3 foundation's own guarded currentness re-proof.
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_BLOCK_INSERT, _remediation_block_row())
    conn.on(_GATES_INSERT, _remediation_block_gate_row())
    conn.on(_TASK_GATE_WRITE, _task_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))


def _verification_call(
    pool: DatabasePool,
    github: Any,
    fake_pool: FakePool,
) -> Any:
    return verify_settled_pr_remediation_push(
        pool,
        github,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
    )


def test_the_settled_remediation_verification_confirms_the_new_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durably SUCCEEDED remediation Execution whose recorded branch claim
    names the canonical branch and whose B7 reconciliation confirms a NEW head
    yields the exact continuation facts for the next typed review dispatch."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=_NEW_HEAD_SHA,
    )
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert isinstance(continuation, SettledPrRemediationVerification)
    assert continuation.new_head_sha == _NEW_HEAD_SHA
    assert continuation.addressed_iteration.id == _REVIEW_ITERATION_ID
    assert continuation.finalized_execution.status.value == "succeeded"
    assert continuation.task.state_token == _STATE_TOKEN
    assert _count(conn, _BLOCK_INSERT) == 0


def test_a_wrong_branch_recorded_completion_installs_the_level_3_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding-2 semantics for the detached continuation: a recorded completion
    claiming a branch other than the Task's canonical branch is a positively
    unverifiable round — the atomic Level 3 BLOCK_RESOLUTION block installs
    through the shared exceptional-blocking foundation (before any GitHub
    call), the recorded completion is never rewritten, and no review
    dispatches."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload={**_IMPLEMENTATION_PAYLOAD, "branch": "other/branch"},
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=None,
    )
    _script_verification_block(conn)
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert continuation is None
    assert github.repository_calls == []
    assert github.pull_request_calls == []
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _GATES_INSERT) == 1
    assert _count(conn, _TASK_GATE_WRITE) == 1
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    # The unverifiable-round fact is deliberately not a supported Type A
    # condition: the Level 3 catalog selects the Type B BLOCK_RESOLUTION
    # diagnostic gate, and the block preserves the exact proven detection
    # facts for later guarded recovery (issue #280).
    (gate_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert gate_params is not None and gate_params[2] == "block_resolution"
    block_params_list = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    (block_params,) = block_params_list
    assert block_params is not None
    assert block_params[2] == "stale_operation"
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context["detection"] == "wrong_branch_claim"
    assert block_context["claimed_branch"] == "other/branch"
    assert block_context["review_iteration_id"] == str(_REVIEW_ITERATION_ID)


def test_an_unconfirmed_push_installs_the_level_3_block_after_the_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recorded completion whose canonical pull request head is unchanged
    after the authoritative reconciliation is a positively unverifiable round:
    the atomic Level 3 block installs instead of stranding the reviewing Task,
    and the recorded result is never rewritten into invented success."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=_HEAD_SHA,
    )
    _script_verification_block(conn)
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert continuation is None
    assert len(github.pull_request_calls) == 1
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _GATES_INSERT) == 1
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    block_params_list = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    (block_params,) = block_params_list
    assert block_params is not None
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context["detection"] == "no_confirmed_push"
    assert block_context["addressed_head_sha"] == _HEAD_SHA


def test_an_already_continued_head_applies_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A review iteration already reserved for the reconciled head means the
    continuation is already owned: nothing is dispatched twice."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[
            _addressed_iteration_row(),
            _iteration_row(
                iteration_id=uuid.uuid4(),
                number=2,
                pr_id=_PR_ID,
                head_sha=_NEW_HEAD_SHA,
            ),
        ],
        observed_head=_NEW_HEAD_SHA,
    )
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert continuation is None
    assert _count(conn, _BLOCK_INSERT) == 0


def test_a_superseded_continuation_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """A later routed iteration means the addressed remediation already
    resolved (the E2 supersession rule): the continuation is a no-op — no
    verification I/O, no block, no dispatch."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[
            _addressed_iteration_row(),
            _iteration_row(
                iteration_id=uuid.uuid4(),
                number=2,
                pr_id=_PR_ID,
                head_sha=_HEAD_SHA,
                outcome="accepted",
                summary="Resolved.",
                findings=[],
                decided_at=_OBSERVED,
            ),
        ],
        observed_head=None,
    )
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert continuation is None
    assert github.repository_calls == []
    assert _count(conn, _BLOCK_INSERT) == 0


def test_a_moved_on_subject_mid_verification_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """A subject that moved on between the handoff's pre-check and the
    verification's own transaction is a no-op — never an error, never a block
    against another seam's subject."""
    conn = ScriptedConnection()
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, StaleOperationError("the task state has moved on"))
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert continuation is None
    assert github.repository_calls == []


def test_an_unsettled_remediation_owns_no_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claimed remediation Execution that has not durably succeeded owns no
    continuation: nothing is verified, nothing is dispatched."""
    conn = ScriptedConnection()
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=_claimed_execution_row(status="running"),
        iteration_rows=[_addressed_iteration_row()],
        observed_head=None,
    )
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    continuation = _verification_call(pool, github.as_client(), fake_pool)

    assert continuation is None
    assert github.repository_calls == []


def _closed_pr_row(*, merged: bool) -> tuple[Any, ...]:
    """The reconciled canonical record in its GitHub-confirmed terminal state."""
    row = list(_pr_row(head_sha=_HEAD_SHA))
    row[9] = "closed"
    row[10] = _OBSERVED if merged else None
    return tuple(row)


def test_a_confirmed_closed_unmerged_record_routes_through_the_trusted_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A GitHub-confirmed closed-unmerged canonical record is the externally
    established terminal condition (issue #229): the ONE trusted
    GitHub-observation cancellation entrypoint applies the terminal
    consequence with the forwarded finalization submission — never a
    competing recoverable block, never a dispatch, never a rewrite of the
    recorded round."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=_HEAD_SHA,
        update_row=_closed_pr_row(merged=False),
    )
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_HEAD_SHA, observed_state="closed")
    cancellation_calls: list[dict[str, Any]] = []
    submission = object()

    def _stub_cancellation(*args: Any, **kwargs: Any) -> Any:
        cancellation_calls.append(kwargs)
        return types.SimpleNamespace(task=None)

    monkeypatch.setattr(
        task_pr_remediation, "cancel_task_for_confirmed_pr_closure", _stub_cancellation
    )

    continuation = verify_settled_pr_remediation_push(
        pool,
        github.as_client(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        terminal_finalization_submission=cast(Any, submission),
    )

    assert continuation is None
    assert len(cancellation_calls) == 1
    assert cancellation_calls[0]["workspace_id"] == _WORKSPACE_ID
    assert cancellation_calls[0]["task_id"] == _TASK_ID
    assert cancellation_calls[0]["terminal_finalization_submission"] is submission
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _GATES_INSERT) == 0


def test_a_confirmed_merge_applies_zero_durable_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A GitHub-confirmed merged canonical record is the externally
    established terminal fact whose completion belongs to the later recovery
    layer: zero durable mutation here — never a competing recoverable block,
    never a cancellation, never a dispatch."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=_HEAD_SHA,
        update_row=_closed_pr_row(merged=True),
    )
    pool = _pool(conn)
    github = _GitHubClient(
        fake_pool,
        observed_head=_HEAD_SHA,
        observed_state="closed",
        observed_merged=True,
        observed_merged_at=_OBSERVED,
    )

    def _refusing_cancellation(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a confirmed merge never routes to the cancellation entrypoint")

    monkeypatch.setattr(
        task_pr_remediation, "cancel_task_for_confirmed_pr_closure", _refusing_cancellation
    )

    continuation = verify_settled_pr_remediation_push(
        pool,
        github.as_client(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert continuation is None
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0


def test_an_uncertain_reconciliation_read_installs_the_level_3_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed verification observation is a guarded direct Level 3
    disposition: the atomic BLOCK_RESOLUTION block installs under the
    escalation reason vocabulary, so the settled round can never silently
    strand the reviewing Task without an actionable Owner gate."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=None,
    )
    _script_verification_block(conn)
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    def _uncertain_reconcile(*args: Any, **kwargs: Any) -> Any:
        raise ExternalOperationUncertainError("the authoritative read is unknown")

    monkeypatch.setattr(task_pr_remediation, "reconcile_task_pull_request", _uncertain_reconcile)

    continuation = verify_settled_pr_remediation_push(
        pool,
        github.as_client(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert continuation is None
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _GATES_INSERT) == 1
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    (gate_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert gate_params is not None and gate_params[2] == "block_resolution"
    block_params_list = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    (block_params,) = block_params_list
    assert block_params is not None
    assert block_params[2] == "external_operation_uncertain"
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context["detection"] == "github_reconciliation_uncertain"
    assert block_context["review_iteration_id"] == str(_REVIEW_ITERATION_ID)


def test_a_known_failed_reconciliation_read_installs_the_level_3_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A known-failed verification observation receives the same guarded
    direct Level 3 disposition under the escalation vocabulary's coarse
    ``unknown`` reason — never a silent stranding of the reviewing Task."""
    conn = ScriptedConnection()
    execution_row = _execution_row(
        execution_number=2,
        status="succeeded",
        result_payload=dict(_IMPLEMENTATION_PAYLOAD),
        claimed_iteration_id=_REVIEW_ITERATION_ID,
    )
    fake_pool = _verification_pool_setup(
        conn,
        execution_row=execution_row,
        iteration_rows=[_addressed_iteration_row()],
        observed_head=None,
    )
    _script_verification_block(conn)
    pool = _pool(conn)
    github = _GitHubClient(fake_pool, observed_head=_NEW_HEAD_SHA)

    def _failed_reconcile(*args: Any, **kwargs: Any) -> Any:
        raise ExternalOperationFailedError("the authoritative read failed")

    monkeypatch.setattr(task_pr_remediation, "reconcile_task_pull_request", _failed_reconcile)

    continuation = verify_settled_pr_remediation_push(
        pool,
        github.as_client(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert continuation is None
    assert _count(conn, _BLOCK_INSERT) == 1
    block_params_list = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    (block_params,) = block_params_list
    assert block_params is not None
    assert block_params[2] == "unknown"
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context["detection"] == "github_reconciliation_read_failed"
