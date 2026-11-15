"""Deterministic service tests for the detached Hub-run correlation/settlement
seam (issue #322).

The seam's own behavior and its critical cross-boundary seams: the
claim-before-effect receipt companion, the exactly-once acknowledgement
attach with the staged early-completion handoff (terminal-before-ack), the
serial single-outstanding-run attribution on the authoritative session
binding within the authenticated Connection/Workspace scope, validated
result settlement and observed-failure classification through the existing
owning services and Level 2 report, duplicate/stale/wrong-session rejection,
and the read-only run-start corroboration. The established persistence,
triage-disposition, and protocol-validation matrices are not re-proven here.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.hub_runs import (
    SETTLEMENT_OBSERVED_FAILURE,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
    SETTLEMENT_RESULT_NOT_APPLIED,
    SETTLEMENT_VALIDATED_RESULT,
    HubRunObservationKind,
    ObservedHubRunCompletion,
    ObservedHubRunStatus,
    TaskHubRunReceiptStatus,
)
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import ImplementationResult
from openorc.services import task_hub_run_correlation as correlation_module
from openorc.services.errors import ConflictError, InvalidCommandError, StaleOperationError
from openorc.services.task_hub_run_correlation import (
    HubRunSettlementKind,
    HubRunStartCorroboration,
    HubRunStatusClassificationKind,
    attach_acknowledged_hub_run,
    check_hub_run_start_identity,
    classify_acknowledged_hub_run_status,
    record_task_hub_run_receipt,
    settle_observed_hub_run_completion,
)
from tests.fakes.agent_runtime import implementation_result_candidate
from tests.fakes.task_rows import task_row

_OBSERVED = datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_CONNECTION_ID = uuid4()
_SESSION_ID = uuid4()
_EXTERNAL_SESSION = "ext-session-1"
_RECEIPT_ID = uuid4()
_EXECUTION_ID = uuid4()
_STATE_TOKEN = uuid4()
_REVISION_ID = uuid4()
_ITERATION_ID = uuid4()
_LOOP_ID = uuid4()
_PR_ID = uuid4()
_HEAD_SHA = "b" * 40
_RUN_ID = "hub-run-1"
_STAGED_ID = uuid4()
_RESULT_CANDIDATE = json.dumps(
    implementation_result_candidate(
        branch="feat/issue-322",
        summary="implemented",
        changes=["one change"],
        validation=["checked"],
    )
)
_COMPLETED_PAYLOAD = json.loads(_RESULT_CANDIDATE)

# --- the scripted-connection harness ----------------------------------------------


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


def _sql_has_not(*fragments: str, absent: str) -> _Predicate:
    return _Predicate(fn=lambda sql: _sql_has(*fragments)(sql) and absent not in sql.lower())


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


class ScriptedPool:
    def __init__(self, connection: ScriptedConnection) -> None:
        self._connection = connection

    def connection(self):  # noqa: ANN201 - test fake
        @contextmanager
        def managed():  # noqa: ANN202 - test fake
            yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the pool lifetime")


def _pool(connection: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, ScriptedPool(connection))


# --- durable row shapes ------------------------------------------------------------


def _session_row() -> tuple[Any, ...]:
    return (
        _SESSION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        WorkflowRole.PRODUCER.value,
        _CONNECTION_ID,
        _EXTERNAL_SESSION,
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


def _receipt_row(
    *,
    status: str = "claimed",
    run_id: str | None = None,
    outcome: dict[str, Any] | None = None,
    operation: str = "implementation_execution",
    contradicted: bool = False,
    created_external_session_id: str | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    initialization = operation == "session_initialization"
    snapshot: dict[str, Any] | None = (
        {"adapter": "clin", "configuration": {}} if initialization else None
    )
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        operation,
        "execution",
        _EXECUTION_ID,
        "implementation_result",
        run_id,
        status,
        outcome if outcome is not None else {},
        _OBSERVED if contradicted else None,
        _OBSERVED,
        admitted_at,
        settled_at,
        _OBSERVED,
        created_external_session_id if initialization else None,
        snapshot,
        None,
    )


def _staged_row(
    *,
    kind: str = "run.completed",
    candidate: str | None = None,
) -> tuple[Any, ...]:
    return (
        _STAGED_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _RECEIPT_ID,
        kind,
        41,
        candidate,
        _OBSERVED,
    )


def _execution_row(
    *,
    status: str = "running",
    implementation_result: Any = None,
    producer_session_id: UUID | None = None,
    review_iteration_id: UUID | None = None,
) -> tuple[Any, ...]:
    return (
        _EXECUTION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        producer_session_id if producer_session_id is not None else _SESSION_ID,
        1,
        status,
        _OBSERVED,
        _OBSERVED,
        implementation_result,
        _OBSERVED,
        review_iteration_id,
    )


def _event_row(
    *, event_type: WorkflowEventType, failure_kind: str | None = None
) -> tuple[Any, ...]:
    context: dict[str, Any] = {} if failure_kind is None else {"failure_kind": failure_kind}
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type.value,
        WorkflowEventActor.PRODUCER.value,
        str(_SESSION_ID),
        "execution",
        _EXECUTION_ID,
        context,
        _OBSERVED,
    )


def _admitted_row() -> tuple[Any, ...]:
    return _receipt_row(status="admitted", run_id=_RUN_ID)


def _remediation_admitted_row() -> tuple[Any, ...]:
    return _receipt_row(status="admitted", run_id=_RUN_ID, operation="pr_remediation_execution")


def _settled_row(label: str, *, sequence: int | None = 41) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if sequence is not None:
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = sequence
    return _receipt_row(status="settled", run_id=_RUN_ID, outcome=outcome)


# --- scripted-flow helpers ---------------------------------------------------------


def _script_binding_lookup(
    conn: ScriptedConnection,
    *,
    found: bool = True,
    repeat: bool = False,
) -> None:
    register = conn.on_repeat if repeat else conn.on
    register(
        _sql_has("from openorc.task_agent_sessions", "connection_id = %s"),
        _session_row() if found else None,
    )


def _script_initialization_receipt_lookup(
    conn: ScriptedConnection,
    result: Any,
) -> None:
    """Script the establishment fallback's provisional-identity lookup."""
    conn.on(
        _sql_has(
            "from openorc.task_hub_run_receipts r",
            "created_external_session_id = %s",
        ),
        result,
    )


def _script_outstanding_receipt(
    conn: ScriptedConnection, row: Any, *, repeat: bool = False
) -> None:
    register = conn.on_repeat if repeat else conn.on
    register(
        _sql_has("from openorc.task_hub_run_receipts", "status in ('claimed', 'admitted')"),
        row,
    )


def _script_receipt_for_update(conn: ScriptedConnection, row: Any) -> None:
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s for update"),
        row,
    )


def _script_attach(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("update openorc.task_hub_run_receipts", "set status = 'admitted'"), row)


def _script_settle(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("update openorc.task_hub_run_receipts", "set status = 'settled'"), row)


def _script_staged(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("from openorc.task_hub_run_early_completions", "for update"), row)


def _script_stage_insert(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("insert into openorc.task_hub_run_early_completions"), row)


def _script_consume(conn: ScriptedConnection) -> None:
    conn.on(
        _sql_has("delete from openorc.task_hub_run_early_completions"),
        [_STAGED_ID],
    )


def _script_execution_read(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has_not("from openorc.executions where id = %s", absent="for update"), row)


def _script_execution_lock(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("from openorc.executions", "for update"), row)


def _script_task_lock(conn: ScriptedConnection, *, status: str = "implementing") -> None:
    conn.on(
        _sql_has("from openorc.tasks where id = %s for update"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status=status,
            state_token=_STATE_TOKEN,
            source_requirements_fingerprint="a" * 64,
            current_plan_revision_id=_REVISION_ID,
        ),
    )


def _script_last_consumed_sequence(conn: ScriptedConnection, value: int | None) -> None:
    conn.on(
        _sql_has("select max((outcome ->> 'consumed_sequence')::bigint)"),
        None if value is None else [value],
    )


def _script_implementation_subject(conn: ScriptedConnection, *, gate_count: int = 1) -> None:
    """Script the E5 authorization-subject reproof reads: the current plan
    revision and the Task's gate history."""
    conn.on(
        _sql_has("from openorc.plan_revisions where id = %s"),
        (
            _REVISION_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            1,
            "# plan",
            "c" * 40,
            _OBSERVED,
        ),
    )
    conn.on(
        _sql_has("from openorc.owner_gates"),
        [
            (
                uuid4(),
                _WORKSPACE_ID,
                _TASK_ID,
                OwnerGateType.IMPLEMENTATION_AUTHORIZATION.value,
                OwnerGateStatus.APPROVED.value,
                _REVISION_ID,
                None,
                None,
                None,
                _OBSERVED,
                _OBSERVED,
            )
        ]
        * gate_count,
    )


def _script_remediation_subject(conn: ScriptedConnection, *, head_sha: str) -> None:
    """``head_sha`` is the canonical PR's CURRENT reconciled head; the claimed
    iteration always reviews ``_HEAD_SHA``, so a moved head is scriptable."""
    """Script the E9 remediation-subject reproof reads: the claimed
    iteration, its open PR-review loop, the loop's iterations, and the
    canonical TaskPullRequest with its reconciled head."""
    conn.on(
        _sql_has("from openorc.review_iterations where id = %s"),
        (
            _ITERATION_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            _LOOP_ID,
            1,
            None,
            _PR_ID,
            _HEAD_SHA,
            ReviewOutcome.CHANGES_REQUESTED.value,
            "summary",
            [{"summary": "finding", "details": "details"}],
            _OBSERVED,
            _OBSERVED,
        ),
    )
    conn.on(
        _sql_has("from openorc.review_loops where id = %s"),
        (
            _LOOP_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            ReviewLoopPurpose.PR_REVIEW.value,
            3,
            ReviewLoopStatus.OPEN.value,
            None,
            _OBSERVED,
        ),
    )
    conn.on(
        _sql_has("from openorc.review_iterations", "review_loop_id = %s"),
        [
            (
                _ITERATION_ID,
                _WORKSPACE_ID,
                _TASK_ID,
                _LOOP_ID,
                1,
                None,
                _PR_ID,
                _HEAD_SHA,
                ReviewOutcome.CHANGES_REQUESTED.value,
                "summary",
                [{"summary": "finding", "details": "details"}],
                _OBSERVED,
                _OBSERVED,
            )
        ],
    )
    conn.on(
        _sql_has("from openorc.task_pull_requests where task_id = %s"),
        (
            _PR_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            uuid4(),
            5001,
            7,
            "feat/branch",
            "main",
            head_sha,
            TaskPullRequestState.OPEN.value,
            None,
            _OBSERVED,
            _OBSERVED,
        ),
    )


def _script_complete_execution(conn: ScriptedConnection) -> None:
    conn.on(
        _sql_has("update openorc.executions", "set status = 'succeeded'"),
        _execution_row(status="succeeded", implementation_result=_COMPLETED_PAYLOAD),
    )


def _script_fail_execution(conn: ScriptedConnection) -> None:
    conn.on(
        _sql_has("update openorc.executions", "set status = %s"),
        _execution_row(status="failed_final"),
    )


def _script_event_insert(
    conn: ScriptedConnection,
    *,
    event_type: WorkflowEventType,
    failure_kind: str | None = None,
) -> None:
    conn.on(
        _sql_has("insert into openorc.workflow_events"),
        _event_row(event_type=event_type, failure_kind=failure_kind),
    )


def _script_current_task(conn: ScriptedConnection, *, status: str = "implementing") -> None:
    conn.on(
        _sql_has_not("from openorc.tasks where id = %s", absent="for update"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status=status,
            state_token=_STATE_TOKEN,
            source_requirements_fingerprint="a" * 64,
            current_plan_revision_id=uuid4(),
        ),
    )


@dataclass
class _RecordingTriage:
    calls: list[dict[str, Any]] = field(default_factory=list)
    disposition: Any = object()

    def __call__(self, _pool: Any, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return self.disposition


@pytest.fixture
def triage(monkeypatch: pytest.MonkeyPatch) -> _RecordingTriage:
    recording = _RecordingTriage()
    monkeypatch.setattr(correlation_module, "triage_operational_failure", recording)
    return recording


# --- the receipt companion (claim-before-effect) ------------------------------------


def test_record_receipt_commits_the_claim_companion_with_pinned_family() -> None:
    conn = ScriptedConnection()
    conn.on(_sql_has("insert into openorc.task_hub_run_receipts"), _receipt_row())
    receipt = record_task_hub_run_receipt(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=_EXECUTION_ID,
    )
    assert receipt.id == _RECEIPT_ID
    assert receipt.status is TaskHubRunReceiptStatus.CLAIMED
    assert receipt.expected_formal_family == "implementation_result"
    insert_sql, insert_params = conn.executed[0]
    assert "insert into openorc.task_hub_run_receipts" in " ".join(insert_sql.split())
    assert insert_params is not None and insert_params[6] == "implementation_result"


def test_record_receipt_refuses_a_second_outstanding_run_for_the_session() -> None:
    conn = ScriptedConnection()
    conn.on(_sql_has("insert into openorc.task_hub_run_receipts"), UniqueViolation())
    with pytest.raises(ConflictError, match="outstanding hub-run receipt"):
        record_task_hub_run_receipt(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
        )


def test_record_receipt_validates_the_family_pairing() -> None:
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError, match="implementation_result family"):
        record_task_hub_run_receipt(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            expected_formal_family="pr_result",
        )
    with pytest.raises(InvalidCommandError, match="expected formal family"):
        record_task_hub_run_receipt(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_attempt_id=uuid4(),
        )


# --- the acknowledgement attach -----------------------------------------------------

_COMPLETION_OBSERVATION = ObservedHubRunCompletion(
    external_session_id=_EXTERNAL_SESSION,
    kind=HubRunObservationKind.RUN_COMPLETED,
    sequence=41,
    result_candidate=_RESULT_CANDIDATE,
)


def test_attach_acknowledges_the_claimed_receipt_exactly_once() -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, None)
    outcome = attach_acknowledged_hub_run(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        run_id=_RUN_ID,
    )
    assert outcome.kind is HubRunSettlementKind.ADMITTED
    attach_params = next(
        params
        for sql, params in conn.executed
        if "set status = 'admitted'" in " ".join(sql.split())
    )
    assert attach_params is not None and attach_params[0] == _RUN_ID
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)


def test_attach_refuses_a_receipt_that_is_no_longer_claimed() -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _admitted_row())
    with pytest.raises(StaleOperationError, match="no longer claimed"):
        attach_acknowledged_hub_run(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            receipt_id=_RECEIPT_ID,
            run_id=_RUN_ID,
        )


def test_attach_consumes_the_staged_completion_and_settles_the_validated_result(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_complete_execution(conn)
    _script_event_insert(conn, event_type=WorkflowEventType.EXECUTION_COMPLETED)
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT))
    _script_consume(conn)
    outcome = attach_acknowledged_hub_run(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        run_id=_RUN_ID,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskHubRunReceiptStatus.SETTLED
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] == 41
    assert isinstance(outcome.formal_result, ImplementationResult)
    assert outcome.formal_result.branch == "feat/issue-322"
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    # The early result is preserved: the staged observation is consumed only
    # after the settlement routing has committed its facts.
    delete_at = next(
        index for index, sql in enumerate(executed) if sql.startswith("delete from openorc")
    )
    settle_at = next(index for index, sql in enumerate(executed) if "set status = 'settled'" in sql)
    assert settle_at < delete_at


def test_attach_consumes_the_staged_failure_and_reports_level_2(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, _staged_row(kind="run.failed"))
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_fail_execution(conn)
    _script_event_insert(
        conn, event_type=WorkflowEventType.EXECUTION_FAILED, failure_kind="observed_run_failure"
    )
    _script_settle(conn, _settled_row("observed_failure"))
    _script_current_task(conn)
    _script_consume(conn)
    outcome = attach_acknowledged_hub_run(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        run_id=_RUN_ID,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == "observed_failure"
    assert len(triage.calls) == 1
    call = triage.calls[0]
    assert call["workspace_id"] == _WORKSPACE_ID
    assert call["task_id"] == _TASK_ID
    assert call["expected_state_token"] == _STATE_TOKEN
    assert call["expected_status"].value == "implementing"
    assert call["expected_current_owner_gate_id"] is None
    assert call["operation"].family.value == "implementation_execution"
    assert call["operation"].original_attempt_kind.value == "execution"
    assert call["operation"].original_attempt_id == _EXECUTION_ID
    assert call["report"].error_code is OperationalFailureCode.EXTERNAL_OPERATION_FAILED
    assert call["report"].effect_evidence is FailureEffectEvidence.KNOWN_FAILED
    assert call["report"].reporting_attempt_kind is OperationalFailureSourceKind.ORIGINAL
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    settle_at = next(index for index, sql in enumerate(executed) if "set status = 'settled'" in sql)
    delete_at = next(
        index for index, sql in enumerate(executed) if sql.startswith("delete from openorc")
    )
    assert settle_at < delete_at


def test_attach_routes_a_malformed_staged_result_through_the_failure_path(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, _staged_row(candidate="not a formal response"))
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_fail_execution(conn)
    _script_event_insert(
        conn, event_type=WorkflowEventType.EXECUTION_FAILED, failure_kind="malformed_run_result"
    )
    _script_settle(conn, _settled_row("observed_failure"))
    _script_current_task(conn)
    _script_consume(conn)
    outcome = attach_acknowledged_hub_run(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        run_id=_RUN_ID,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskHubRunReceiptStatus.SETTLED
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code is OperationalFailureCode.RUNTIME_PROTOCOL
    assert triage.calls[0]["report"].effect_evidence is FailureEffectEvidence.KNOWN_FAILED
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert any("set status = 'settled'" in sql for sql in executed)
    assert any(
        sql.startswith("delete from openorc.task_hub_run_early_completions") for sql in executed
    )


def test_settlement_routes_a_resultless_completion_through_the_failure_path(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_fail_execution(conn)
    _script_event_insert(
        conn, event_type=WorkflowEventType.EXECUTION_FAILED, failure_kind="malformed_run_result"
    )
    _script_settle(conn, _settled_row("observed_failure", sequence=None))
    _script_current_task(conn)
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=ObservedHubRunCompletion(
            external_session_id=_EXTERNAL_SESSION,
            kind=HubRunObservationKind.RUN_COMPLETED,
            sequence=None,
            result_candidate=None,
        ),
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code is OperationalFailureCode.RUNTIME_PROTOCOL


# --- the settlement entry ------------------------------------------------------------


def test_settlement_resolves_within_the_authenticated_connection_scope() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    # The establishment fallback's provisional-identity lookup (issue #324)
    # is equally Connection-scoped: no outstanding initialization receipt
    # resolves a cross-Connection identifier either.
    _script_initialization_receipt_lookup(conn, None)
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.NO_OUTSTANDING_RUN
    (sql, _params) = conn.executed[0]
    normalized = " ".join(sql.split())
    # The external identifier never matches across Connections: the lookup is
    # scoped to the authenticated Workspace and Connection.
    assert "workspace_id = %s and connection_id = %s and external_session_id = %s" in normalized
    fallback_sql = " ".join(conn.executed[1][0].split())
    assert "s.connection_id = %s" in fallback_sql
    assert "r.created_external_session_id = %s" in fallback_sql
    assert len(conn.executed) == 2


def test_settlement_is_unowned_without_an_outstanding_receipt() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, None)
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.NO_OUTSTANDING_RUN
    assert outcome.receipt is None


def test_settlement_stages_an_early_completion_for_a_claimed_receipt() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_stage_insert(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.STAGED_PENDING_ACKNOWLEDGEMENT
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskHubRunReceiptStatus.CLAIMED
    insert_sql, insert_params = next(
        (sql, params)
        for sql, params in conn.executed
        if "insert into openorc.task_hub_run_early_completions" in " ".join(sql.split())
    )
    assert insert_params is not None
    assert insert_params[3] == "run.completed"
    assert insert_params[4] == 41
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)


def test_settlement_rejects_a_duplicate_early_completion() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_stage_insert(conn, UniqueViolation())
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.REJECTED_DUPLICATE


def test_settlement_settles_the_single_outstanding_run_without_run_started(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_complete_execution(conn)
    _script_event_insert(conn, event_type=WorkflowEventType.EXECUTION_COMPLETED)
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT))
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
    assert isinstance(outcome.formal_result, ImplementationResult)
    assert outcome.formal_result.status == "COMPLETED"
    assert triage.calls == []


def test_settlement_classifies_an_observed_failure_through_the_typed_path(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_fail_execution(conn)
    _script_event_insert(
        conn, event_type=WorkflowEventType.EXECUTION_FAILED, failure_kind="observed_run_failure"
    )
    _script_settle(conn, _settled_row("observed_failure"))
    _script_current_task(conn)
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=ObservedHubRunCompletion(
            external_session_id=_EXTERNAL_SESSION,
            kind=HubRunObservationKind.RUN_FAILED,
            sequence=None,
            result_candidate=None,
        ),
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].effect_evidence is FailureEffectEvidence.KNOWN_FAILED


def test_settlement_leaves_a_concrete_attribution_conflict_unresolved(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row(producer_session_id=uuid4()))
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.LEFT_UNRESOLVED
    assert triage.calls == []
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)
    assert not any("update openorc.executions" in sql for sql, _ in conn.executed)


# --- status-only reconciliation ------------------------------------------------------


def test_status_classification_routes_the_exact_failed_run(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _admitted_row(),
    )
    _script_receipt_for_update(conn, _admitted_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_fail_execution(conn)
    _script_event_insert(
        conn, event_type=WorkflowEventType.EXECUTION_FAILED, failure_kind="observed_run_failure"
    )
    _script_settle(conn, _settled_row("observed_failure", sequence=None))
    _script_current_task(conn)
    classification = classify_acknowledged_hub_run_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        observed_statuses=[
            ObservedHubRunStatus(run_id="other-run", status="completed"),
            ObservedHubRunStatus(run_id=_RUN_ID, status="failed"),
        ],
    )
    assert classification.kind is HubRunStatusClassificationKind.CLASSIFIED_FAILURE
    assert classification.receipt is not None
    assert classification.receipt.status is TaskHubRunReceiptStatus.SETTLED
    assert len(triage.calls) == 1


def test_status_classification_never_treats_completed_as_success_evidence(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _admitted_row(),
    )
    classification = classify_acknowledged_hub_run_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        observed_statuses=[ObservedHubRunStatus(run_id=_RUN_ID, status="completed")],
    )
    assert classification.kind is HubRunStatusClassificationKind.NEVER_SUCCESS_EVIDENCE
    assert triage.calls == []
    assert not any("update openorc" in sql for sql, _ in conn.executed)


def test_status_classification_is_not_classifiable_unless_admitted() -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _receipt_row(),
    )
    classification = classify_acknowledged_hub_run_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        observed_statuses=[ObservedHubRunStatus(run_id=_RUN_ID, status="failed")],
    )
    assert classification.kind is HubRunStatusClassificationKind.NOT_CLASSIFIABLE


def test_status_classification_leaves_a_missing_record_unresolved() -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _admitted_row(),
    )
    classification = classify_acknowledged_hub_run_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        observed_statuses=[ObservedHubRunStatus(run_id="other-run", status="completed")],
    )
    assert classification.kind is HubRunStatusClassificationKind.UNRESOLVED


# --- the read-only run-start corroboration -------------------------------------------


def test_start_identity_corroboration_is_read_only_and_never_required() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, repeat=True)
    _script_outstanding_receipt(conn, _admitted_row(), repeat=True)
    assert (
        check_hub_run_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            external_session_id=_EXTERNAL_SESSION,
            request_id=_RUN_ID,
        )
        is HubRunStartCorroboration.CORROBORATED
    )
    assert not any(
        " ".join(sql.split()).startswith(("update ", "insert ", "delete "))
        for sql, _ in conn.executed
    )


def test_start_identity_is_unattributable_before_admission_and_unowned_without_a_run() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _receipt_row())
    assert (
        check_hub_run_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            external_session_id=_EXTERNAL_SESSION,
            request_id=_RUN_ID,
        )
        is HubRunStartCorroboration.UNATTRIBUTABLE
    )
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    assert (
        check_hub_run_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            external_session_id=_EXTERNAL_SESSION,
            request_id=_RUN_ID,
        )
        is HubRunStartCorroboration.UNOWNED
    )


# --- the stale-replay safeguard ------------------------------------------------------


def test_settlement_rejects_a_stale_replay_after_a_consumed_settlement(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, 41)
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.REJECTED_STALE
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskHubRunReceiptStatus.ADMITTED
    assert triage.calls == []
    # The stale duplicate never reaches the attempt: no execution reads, no
    # settlement, no staging.
    assert not any("from openorc.executions" in sql for sql, _ in conn.executed)
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)
    assert not any(
        "insert into openorc.task_hub_run_early_completions" in sql for sql, _ in conn.executed
    )


def test_settlement_accepts_a_fresher_completion_after_a_consumed_settlement(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, 41)
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_complete_execution(conn)
    _script_event_insert(conn, event_type=WorkflowEventType.EXECUTION_COMPLETED)
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT, sequence=43))
    fresher = ObservedHubRunCompletion(
        external_session_id=_EXTERNAL_SESSION,
        kind=HubRunObservationKind.RUN_COMPLETED,
        sequence=43,
        result_candidate=_RESULT_CANDIDATE,
    )
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=fresher,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] == 43


# --- the explicit contradiction marker -----------------------------------------------


def test_settlement_leaves_a_contradicted_receipt_unresolved(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(
        conn, _receipt_row(contradicted=True, run_id=_RUN_ID, status="admitted")
    )
    _script_last_consumed_sequence(conn, None)
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.LEFT_UNRESOLVED
    assert outcome.receipt is not None
    assert outcome.receipt.contradicted_at is not None
    assert triage.calls == []
    assert not any("from openorc.executions" in sql for sql, _ in conn.executed)
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)


def test_start_identity_records_the_contradiction_marker() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    conn.on(
        _sql_has("set contradicted_at = now()"),
        _receipt_row(status="admitted", run_id=_RUN_ID, contradicted=True),
    )
    assert (
        check_hub_run_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            external_session_id=_EXTERNAL_SESSION,
            request_id="hub-run-other",
        )
        is HubRunStartCorroboration.CONTRADICTED
    )
    marker_sql, marker_params = next(
        (sql, params)
        for sql, params in conn.executed
        if "set contradicted_at = now()" in " ".join(sql.split())
    )
    assert marker_params is not None and marker_params[0] == _RECEIPT_ID


# --- the family authorization-subject reproof ----------------------------------------


def test_settlement_leaves_a_moved_remediation_head_a_truthful_no_op(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _remediation_admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row(review_iteration_id=_ITERATION_ID))
    _script_task_lock(conn, status="reviewing")
    _script_execution_lock(conn, _execution_row(review_iteration_id=_ITERATION_ID))
    _script_remediation_subject(conn, head_sha="c" * 40)
    _script_settle(conn, _settled_row("result_not_applied"))
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_RESULT_NOT_APPLIED
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == "result_not_applied"
    assert triage.calls == []
    assert not any("update openorc.executions" in sql for sql, _ in conn.executed)


def test_settlement_settles_a_current_remediation_result(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _remediation_admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row(review_iteration_id=_ITERATION_ID))
    _script_task_lock(conn, status="reviewing")
    _script_execution_lock(conn, _execution_row(review_iteration_id=_ITERATION_ID))
    _script_remediation_subject(conn, head_sha=_HEAD_SHA)
    _script_complete_execution(conn)
    _script_event_insert(conn, event_type=WorkflowEventType.EXECUTION_COMPLETED)
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT))
    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_COMPLETION_OBSERVATION,
    )
    assert outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
    assert triage.calls == []


# --- the establishment (session initialization) settlement, issue #324 ----------


_BARRIER_ROW = (None, None, None)  # account-operational barrier: no attempt state
_READY_PAYLOAD_CANDIDATE = json.dumps(
    {"type": "session_ready", "schema_version": 1, "status": "READY"}
)
_GARBAGE_CANDIDATE = "the agent is still working, no formal object yet"


def _initialization_receipt_row(
    *,
    status: str = "admitted",
    external_session_id: str = "ext-init-1",
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    run_id = _RUN_ID if status in ("admitted", "settled") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "session_initialization",
        "planning_bootstrap_claim",
        _STATE_TOKEN,
        "session_ready",
        run_id,
        status,
        outcome if outcome is not None else {},
        None,
        _OBSERVED,
        admitted_at,
        settled_at,
        _OBSERVED,
        external_session_id,
        {"adapter": "cline", "configuration": {}},
        {"reported_provider": "fake-provider"},
    )


def _settled_initialization_row(label: str, *, sequence: int | None = 7) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if sequence is not None:
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = sequence
    return _initialization_receipt_row(status="settled", outcome=outcome)


def _connecting_binding_row(external_session_id: str | None = None) -> tuple[Any, ...]:
    base = list(_session_row())
    base[5] = external_session_id
    base[6] = "connecting"
    base[7] = None
    base[11] = None
    return tuple(base)


def _ready_binding_row(external_session_id: str) -> tuple[Any, ...]:
    base = list(_session_row())
    base[5] = external_session_id
    base[6] = "ready"
    return tuple(base)


def _script_binding_by_id(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("from openorc.task_agent_sessions", "where id = %s"), row)


def _script_workspace_read(conn: ScriptedConnection) -> None:
    conn.on(
        _sql_has("from openorc.workspaces", "where id = %s"),
        (_WORKSPACE_ID, uuid4(), "platform", _OBSERVED, _OBSERVED, 5, "", None),
    )


def _script_barrier(conn: ScriptedConnection) -> None:
    conn.on(_sql_has("from openorc.profiles", "for key share"), _BARRIER_ROW)


def _script_initialize(conn: ScriptedConnection, ready_row: Any) -> None:
    conn.on(
        _sql_has(
            "update openorc.task_agent_sessions",
            "set external_session_id = %s",
        ),
        ready_row,
    )


def _establishment_observation(
    *,
    kind: HubRunObservationKind = HubRunObservationKind.RUN_COMPLETED,
    result_candidate: str | None = _READY_PAYLOAD_CANDIDATE,
) -> ObservedHubRunCompletion:
    return ObservedHubRunCompletion(
        external_session_id="ext-init-1",
        kind=kind,
        sequence=7,
        result_candidate=result_candidate,
    )


def test_initialization_settlement_finalizes_ready_atomically_with_the_receipt(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    _script_initialization_receipt_lookup(conn, _initialization_receipt_row())
    _script_receipt_for_update(conn, _initialization_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_binding_by_id(conn, _connecting_binding_row())
    _script_workspace_read(conn)
    _script_barrier(conn)
    _script_initialize(
        conn,
        _ready_binding_row("ext-init-1"),
    )
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_VALIDATED_RESULT))

    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_establishment_observation(),
    )

    assert outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.operation.value == "session_initialization"
    # The one atomic READY write carries the provisional external identity,
    # the receipt's stored snapshot, and the reported provenance.
    initialize_writes = [
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.task_agent_sessions set external_session_id" in sql
    ]
    assert len(initialize_writes) == 1
    (_sql, params) = initialize_writes[0]
    assert params is not None
    # (external_session_id, snapshot, provider, model, version, task, role)
    assert params[0] == "ext-init-1"
    assert params[2] == "fake-provider"
    settle_writes = [sql for sql, _ in conn.executed if "set status = 'settled'" in sql]
    assert len(settle_writes) == 1
    assert triage.calls == []


def test_initialization_failure_settles_truthfully_and_reports_level_2(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    _script_initialization_receipt_lookup(conn, _initialization_receipt_row())
    _script_receipt_for_update(conn, _initialization_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_binding_by_id(conn, _connecting_binding_row())
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_OBSERVED_FAILURE, sequence=None))
    _script_current_task(conn, status="planning")

    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_establishment_observation(kind=HubRunObservationKind.RUN_FAILED),
    )

    assert outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE
    # The binding is never rewritten: an observed initialization failure
    # leaves the binding CONNECTING, never LOST.
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)
    assert len(triage.calls) == 1
    operation = triage.calls[0]["operation"]
    assert operation.family.value == "planning_bootstrap_prepare"
    assert operation.original_attempt_kind.value == "planning_bootstrap_claim"
    assert triage.calls[0]["report"].effect_evidence is FailureEffectEvidence.KNOWN_FAILED
    assert triage.calls[0]["report"].error_code.value == "external_operation_failed"


def test_invalid_initialization_response_is_a_positively_established_protocol_failure(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    _script_initialization_receipt_lookup(conn, _initialization_receipt_row())
    _script_receipt_for_update(conn, _initialization_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_binding_by_id(conn, _connecting_binding_row())
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_OBSERVED_FAILURE, sequence=None))
    _script_current_task(conn, status="planning")

    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_establishment_observation(result_candidate=_GARBAGE_CANDIDATE),
    )

    assert outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code.value == "runtime_protocol"
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)


def test_initialization_completion_before_the_acknowledgement_is_staged(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    _script_initialization_receipt_lookup(conn, _initialization_receipt_row(status="claimed"))
    _script_receipt_for_update(conn, _initialization_receipt_row(status="claimed"))
    _script_last_consumed_sequence(conn, None)
    staged_id = uuid4()
    conn.on(
        _sql_has("insert into openorc.task_hub_run_early_completions"),
        (
            staged_id,
            _WORKSPACE_ID,
            _TASK_ID,
            _RECEIPT_ID,
            HubRunObservationKind.RUN_COMPLETED.value,
            7,
            _READY_PAYLOAD_CANDIDATE,
            _OBSERVED,
        ),
    )

    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_establishment_observation(),
    )

    # The early result is staged on the claimed receipt and consumed exactly
    # once by the acknowledgement attach: never ignored, never duplicated.
    assert outcome.kind is HubRunSettlementKind.STAGED_PENDING_ACKNOWLEDGEMENT
    inserts = [
        sql
        for sql, _ in conn.executed
        if "insert into openorc.task_hub_run_early_completions" in sql
    ]
    assert len(inserts) == 1


def test_moved_on_binding_settles_the_validated_result_as_not_applied(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    _script_initialization_receipt_lookup(conn, _initialization_receipt_row())
    _script_receipt_for_update(conn, _initialization_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_binding_by_id(conn, _ready_binding_row("ext-init-1"))
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_RESULT_NOT_APPLIED))

    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_establishment_observation(),
    )

    assert outcome.kind is HubRunSettlementKind.SETTLED_RESULT_NOT_APPLIED
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)


def test_exact_role_isolation_the_reviewer_settlement_never_touches_the_producer(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    _script_initialization_receipt_lookup(conn, _initialization_receipt_row())
    _script_receipt_for_update(conn, _initialization_receipt_row())
    _script_last_consumed_sequence(conn, None)
    reviewer_binding = list(_connecting_binding_row())
    reviewer_binding[3] = WorkflowRole.REVIEWER.value
    _script_binding_by_id(conn, tuple(reviewer_binding))
    _script_workspace_read(conn)
    _script_barrier(conn)
    ready_row = list(_ready_binding_row("ext-init-1"))
    ready_row[3] = WorkflowRole.REVIEWER.value
    _script_initialize(conn, tuple(ready_row))
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_VALIDATED_RESULT))

    outcome = settle_observed_hub_run_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        observation=_establishment_observation(),
    )

    assert outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
    initialize_writes = [
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.task_agent_sessions set external_session_id" in sql
    ]
    assert len(initialize_writes) == 1
    (_sql, params) = initialize_writes[0]
    assert params is not None
    # The resolution addressed the receipt's own binding: the Reviewer role,
    # never the Producer binding.
    assert params[6] == WorkflowRole.REVIEWER.value


def test_establishment_status_only_completed_is_never_success_evidence(
    triage: _RecordingTriage,
) -> None:
    """Issue #324: a no-prompt ``interactive`` session may carry a Cline
    ``completed``/``endedAt`` record before readiness; the status-only
    ``run.list`` consultation never treats it as ``session_ready`` or as a
    settled initialization."""
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _initialization_receipt_row(),
    )
    classification = classify_acknowledged_hub_run_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        observed_statuses=[ObservedHubRunStatus(run_id=_RUN_ID, status="completed")],
    )
    assert classification.kind is HubRunStatusClassificationKind.NEVER_SUCCESS_EVIDENCE
    assert triage.calls == []
    assert not any("update openorc.task_agent_sessions" in sql for sql, _ in conn.executed)


def test_establishment_status_only_failed_record_routes_the_establishment_failure(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    conn.on_repeat(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _initialization_receipt_row(),
    )
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_OBSERVED_FAILURE, sequence=None))
    _script_current_task(conn, status="planning")
    classification = classify_acknowledged_hub_run_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        observed_statuses=[ObservedHubRunStatus(run_id=_RUN_ID, status="failed")],
    )
    assert classification.kind is HubRunStatusClassificationKind.CLASSIFIED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["operation"].family.value == "planning_bootstrap_prepare"
