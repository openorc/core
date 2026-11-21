"""Deterministic service tests for the detached-turn correlation/settlement
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
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import (
    SETTLEMENT_INTERRUPTED_TURN,
    SETTLEMENT_OBSERVED_FAILURE,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
    SETTLEMENT_RESULT_NOT_APPLIED,
    SETTLEMENT_VALIDATED_RESULT,
    DetachedTurnObservationKind,
    NativeTurnEvidence,
    ObservedTurnEvidence,
    TaskDetachedTurnReceiptStatus,
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
    TurnStatusEvidenceVerdict,
    TurnTerminalClassification,
    TurnTerminalOutcome,
)
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
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
from openorc.protocol.models import ImplementationResult, PrResult, WorkspaceResult
from openorc.services import task_detached_turn_correlation as correlation_module
from openorc.services.errors import ConflictError, InvalidCommandError, StaleOperationError
from openorc.services.task_detached_turn_correlation import (
    TurnSettlementKind,
    TurnSettlementOutcome,
    TurnStartCorroboration,
    TurnStatusClassificationKind,
    attach_acknowledged_turn,
    check_turn_start_identity,
    classify_acknowledged_turn_status,
    record_task_detached_turn_receipt,
    settle_observed_turn_completion,
)
from tests.fakes.agent_runtime import implementation_result_candidate, pr_result_candidate
from tests.fakes.task_rows import task_row

_OBSERVED = datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_CONNECTION_ID = uuid4()
_SESSION_ID = uuid4()
_EXTERNAL_SESSION = "ext-session-1"
_RECEIPT_ID = uuid4()
_EXECUTION_ID = uuid4()
# The detached F5c retry's NEW resulting Execution and its F4 episode facts
# (issue #340 review): the receipt names the retry Execution while the
# authorized report must address the EXISTING episode's original identity.
_RETRY_EXECUTION_ID = uuid4()
_F4_ATTEMPT_ID = uuid4()
_EPISODE_ID = uuid4()
_STATE_TOKEN = uuid4()
_REVISION_ID = uuid4()
_ITERATION_ID = uuid4()
_LOOP_ID = uuid4()
_PR_ID = uuid4()
_HEAD_SHA = "b" * 40
_TURN_REFERENCE = "turn-1"
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
    admitted_turn_reference: str | None = None,
    outcome: dict[str, Any] | None = None,
    operation: str = "implementation_execution",
    contradicted: bool = False,
    created_external_session_id: str | None = None,
    original_attempt_id: UUID = _EXECUTION_ID,
    original_attempt_kind: str = "execution",
    expected_family: str = "implementation_result",
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
        original_attempt_kind,
        original_attempt_id,
        expected_family,
        admitted_turn_reference,
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
        None,
    )


def _staged_row(
    *,
    kind: str = "run.completed",
    candidate: str | None = None,
    interrupted: bool = False,
) -> tuple[Any, ...]:
    return (
        _STAGED_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _RECEIPT_ID,
        kind,
        41,
        candidate,
        interrupted,
        _OBSERVED,
    )


def _execution_row(
    *,
    status: str = "running",
    implementation_result: Any = None,
    producer_session_id: UUID | None = None,
    review_iteration_id: UUID | None = None,
    execution_id: UUID = _EXECUTION_ID,
) -> tuple[Any, ...]:
    return (
        execution_id,
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
    *,
    event_type: WorkflowEventType,
    failure_kind: str | None = None,
    execution_id: UUID = _EXECUTION_ID,
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
        execution_id,
        context,
        _OBSERVED,
    )


def _admitted_row() -> tuple[Any, ...]:
    return _receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE)


def _remediation_admitted_row() -> tuple[Any, ...]:
    return _receipt_row(
        status="admitted",
        admitted_turn_reference=_TURN_REFERENCE,
        operation="pr_remediation_execution",
    )


def _settled_row(
    label: str,
    *,
    marker: int | None = 41,
    original_attempt_id: UUID = _EXECUTION_ID,
    original_attempt_kind: str = "execution",
    expected_family: str = "implementation_result",
) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if marker is not None:
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = marker
    return _receipt_row(
        status="settled",
        admitted_turn_reference=_TURN_REFERENCE,
        outcome=outcome,
        original_attempt_id=original_attempt_id,
        original_attempt_kind=original_attempt_kind,
        expected_family=expected_family,
    )


def _retry_bound_attempt_row() -> tuple[Any, ...]:
    """The F4 authorized attempt bound set-once to the retry's Execution."""
    return (
        _F4_ATTEMPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _EPISODE_ID,
        1,
        "original",
        _EXECUTION_ID,
        "runtime_unavailable",
        _OBSERVED,
        _RETRY_EXECUTION_ID,
        None,
        _OBSERVED,
    )


def _retry_episode_row() -> tuple[Any, ...]:
    """The existing F4 failure episode rooted at the INITIAL Execution."""
    return (
        _EPISODE_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "implementation_execution",
        "execution",
        _EXECUTION_ID,
        {"execution_id": str(_EXECUTION_ID), "task_agent_session_id": str(_SESSION_ID)},
        None,
        _OBSERVED,
    )


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


def _script_fail_execution(conn: ScriptedConnection, *, execution_id: UUID = _EXECUTION_ID) -> None:
    conn.on(
        _sql_has("update openorc.executions", "set status = %s"),
        _execution_row(status="failed_final", execution_id=execution_id),
    )


def _script_event_insert(
    conn: ScriptedConnection,
    *,
    event_type: WorkflowEventType,
    failure_kind: str | None = None,
    execution_id: UUID = _EXECUTION_ID,
) -> None:
    conn.on(
        _sql_has("insert into openorc.workflow_events"),
        _event_row(event_type=event_type, failure_kind=failure_kind, execution_id=execution_id),
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


def _script_failure_binding_lookup(conn: ScriptedConnection, row: Any) -> None:
    """The F4 authorized-retry binding lookup in the failure-report resolution.

    ``None`` proves the failing Execution is not a durably authorized retry's
    effect row (the ORIGINAL report path); a bound-attempt row resolves the
    PRIOR_AUTHORIZED episode report (issue #340 review).
    """
    conn.on(
        _sql_has("from openorc.operational_failure_attempts", "where resulting_execution_id = %s"),
        row,
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
    receipt = record_task_detached_turn_receipt(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=_EXECUTION_ID,
    )
    assert receipt.id == _RECEIPT_ID
    assert receipt.status is TaskDetachedTurnReceiptStatus.CLAIMED
    assert receipt.expected_formal_family == "implementation_result"
    insert_sql, insert_params = conn.executed[0]
    assert "insert into openorc.task_hub_run_receipts" in " ".join(insert_sql.split())
    assert insert_params is not None and insert_params[6] == "implementation_result"


def test_record_receipt_refuses_a_second_outstanding_turn_for_the_session() -> None:
    conn = ScriptedConnection()
    conn.on(_sql_has("insert into openorc.task_hub_run_receipts"), UniqueViolation())
    with pytest.raises(ConflictError, match="outstanding detached-turn receipt"):
        record_task_detached_turn_receipt(
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
        record_task_detached_turn_receipt(
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
        record_task_detached_turn_receipt(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_attempt_id=uuid4(),
        )


# --- the runtime-neutral boundary fake ----------------------------------------------


class _ScriptedTurnEvidence(NativeTurnEvidence):
    """Opaque test carrier holding the facts the scripted classifier classifies.

    The boundary fake classifies its own invented evidence shape under its
    own neutral ordering rule; the shared settlement path consumes only the
    neutral verdicts, proving it depends on the runtime-neutral contract and
    never on Cline's evidence semantics.
    """

    __slots__ = ("attribution", "outcome", "marker", "statuses")

    def __init__(
        self,
        *,
        attribution: TurnAttribution = TurnAttribution.UNATTRIBUTABLE,
        outcome: TurnTerminalOutcome | None = None,
        marker: int | None = None,
        statuses: TurnStatusEvidenceVerdict = TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE,
    ) -> None:
        self.attribution = attribution
        self.outcome = outcome
        self.marker = marker
        self.statuses = statuses

    def __repr__(self) -> str:
        return "_ScriptedTurnEvidence(<withheld>)"


class _ScriptedClassifier:
    """The scripted runtime-neutral classifier of the boundary fake."""

    def classify_turn_start(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnAttribution:
        assert isinstance(evidence, _ScriptedTurnEvidence)
        return evidence.attribution

    def classify_turn_terminal(
        self,
        evidence: NativeTurnEvidence,
        *,
        consumed_ordering_markers: Sequence[int],
    ) -> TurnTerminalClassification:
        assert isinstance(evidence, _ScriptedTurnEvidence)
        assert evidence.outcome is not None
        # The fake's own ordering rule: a marker not newer than a consumed
        # marker is a stale replay; a marker-less observation is unordered.
        if evidence.marker is None:
            freshness = TurnFreshness.UNORDERED
        elif any(consumed >= evidence.marker for consumed in consumed_ordering_markers):
            freshness = TurnFreshness.STALE
        else:
            freshness = TurnFreshness.FRESH
        return TurnTerminalClassification(
            freshness=freshness,
            outcome=evidence.outcome,
            ordering_marker=evidence.marker,
        )

    def classify_turn_statuses(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnStatusEvidenceVerdict:
        assert isinstance(evidence, _ScriptedTurnEvidence)
        return evidence.statuses


_CLASSIFIER = _ScriptedClassifier()


def _terminal_evidence(
    *,
    kind: DetachedTurnObservationKind = DetachedTurnObservationKind.COMPLETED,
    marker: int | None = 41,
    candidate: str | None = _RESULT_CANDIDATE,
    interrupted: bool = False,
) -> ObservedTurnEvidence:
    """One terminal evidence item of the fake's own invented evidence shape."""
    return ObservedTurnEvidence(
        external_session_id=_EXTERNAL_SESSION,
        family=TurnEvidenceFamily.TURN_TERMINAL,
        native_evidence=_ScriptedTurnEvidence(
            outcome=TurnTerminalOutcome(
                kind=kind, result_candidate=candidate, interrupted=interrupted
            ),
            marker=marker,
        ),
    )


def _start_evidence(attribution: TurnAttribution) -> ObservedTurnEvidence:
    return ObservedTurnEvidence(
        external_session_id=_EXTERNAL_SESSION,
        family=TurnEvidenceFamily.TURN_START,
        native_evidence=_ScriptedTurnEvidence(attribution=attribution),
    )


def _status_evidence(verdict: TurnStatusEvidenceVerdict) -> NativeTurnEvidence:
    return _ScriptedTurnEvidence(statuses=verdict)


_COMPLETION_EVIDENCE = _terminal_evidence()


def test_attach_acknowledges_the_claimed_receipt_exactly_once() -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, None)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.ADMITTED
    attach_params = next(
        params
        for sql, params in conn.executed
        if "set status = 'admitted'" in " ".join(sql.split())
    )
    assert attach_params is not None and attach_params[0] == _TURN_REFERENCE
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)


def test_attach_refuses_a_receipt_that_is_no_longer_claimed() -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _admitted_row())
    with pytest.raises(StaleOperationError, match="no longer claimed"):
        attach_acknowledged_turn(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            receipt_id=_RECEIPT_ID,
            admitted_turn_reference=_TURN_REFERENCE,
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
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
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
    _script_failure_binding_lookup(conn, None)
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
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
    _script_failure_binding_lookup(conn, None)
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
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
    _script_settle(conn, _settled_row("observed_failure", marker=None))
    _script_current_task(conn)
    _script_failure_binding_lookup(conn, None)
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(marker=None, candidate=None),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code is OperationalFailureCode.RUNTIME_PROTOCOL


def test_the_observed_failure_of_a_detached_authorized_retry_reports_prior_authorized(
    triage: _RecordingTriage,
) -> None:
    """One focused F5c-retry proof (issue #340 review): the newly detached
    authorized-implementation retry shares the ``IMPLEMENTATION_EXECUTION``
    receipt family and its receipt names the NEW resulting Execution — so the
    observed failure resolves the F4 attempt's set-once
    ``resulting_execution_id`` binding and reports ``PRIOR_AUTHORIZED``
    against the EXISTING episode's original identity with the bound attempt
    and the exact resulting Execution, instead of establishing a competing
    original episode for the retry Execution."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(
        conn,
        _receipt_row(
            status="admitted",
            admitted_turn_reference=_TURN_REFERENCE,
            original_attempt_id=_RETRY_EXECUTION_ID,
        ),
    )
    _script_last_consumed_sequence(conn, None)
    _script_execution_read(conn, _execution_row(execution_id=_RETRY_EXECUTION_ID))
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row(execution_id=_RETRY_EXECUTION_ID))
    _script_fail_execution(conn, execution_id=_RETRY_EXECUTION_ID)
    _script_event_insert(
        conn,
        event_type=WorkflowEventType.EXECUTION_FAILED,
        failure_kind="observed_run_failure",
        execution_id=_RETRY_EXECUTION_ID,
    )
    _script_settle(
        conn, _settled_row("observed_failure", marker=None, original_attempt_id=_RETRY_EXECUTION_ID)
    )
    _script_current_task(conn)
    _script_failure_binding_lookup(conn, _retry_bound_attempt_row())
    conn.on(
        _sql_has("from openorc.operational_failure_episodes", "where id = %s"),
        _retry_episode_row(),
    )
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(marker=None, candidate=None),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    operation = triage.calls[0]["operation"]
    report = triage.calls[0]["report"]
    # The Level 2 report addresses the EXISTING F4 episode's original
    # identity — the initial Execution — never the retry Execution itself.
    assert operation.original_attempt_id == _EXECUTION_ID
    assert operation.original_attempt_kind is OriginalAttemptKind.EXECUTION
    assert operation.family is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    # The authorized attempt is the reporting source with its exact bound
    # effect row, disposing the retry within its original episode.
    assert report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED
    assert report.reporting_attempt_id == _F4_ATTEMPT_ID
    assert report.effect_attempt_id == _RETRY_EXECUTION_ID
    assert report.effect_evidence is FailureEffectEvidence.KNOWN_FAILED


# --- the settlement entry ------------------------------------------------------------


def test_settlement_resolves_within_the_authenticated_connection_scope() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    # The establishment fallback's provisional-identity lookup (issue #324)
    # is equally Connection-scoped: no outstanding initialization receipt
    # resolves a cross-Connection identifier either.
    _script_initialization_receipt_lookup(conn, None)
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.NO_OUTSTANDING_RUN
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
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.NO_OUTSTANDING_RUN
    assert outcome.receipt is None


def test_settlement_stages_an_early_completion_for_a_claimed_receipt() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_stage_insert(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.STAGED_PENDING_ACKNOWLEDGEMENT
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.CLAIMED
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
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.REJECTED_DUPLICATE


# --- the interrupted-turn settlement routing (issue #342) --------------------------


_INTENTS_LIST = _sql_has("from openorc.task_interrupt_intents")
_INTENT_ANNOTATE = _sql_has("update openorc.task_interrupt_intents")


def _interrupt_intent_row(
    *,
    status: str = "unsettled",
    outcome: dict[str, Any] | None = None,
    task_agent_session_id: UUID = _SESSION_ID,
) -> tuple[Any, ...]:
    """The 12-column ``task_interrupt_intents`` projection row."""
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        task_agent_session_id,
        None,
        WorkflowRole.PRODUCER.value,
        _EXTERNAL_SESSION,
        uuid4(),
        status,
        outcome if outcome is not None else {"dispatch": "acknowledged"},
        _OBSERVED,
        _OBSERVED,
    )


def _script_interrupt_intents(conn: ScriptedConnection, intents: list[tuple[Any, ...]]) -> None:
    conn.on(_INTENTS_LIST, intents)


def test_an_interrupted_terminal_settles_the_receipt_and_annotates_the_single_intent(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    intent = _interrupt_intent_row()
    annotated = _interrupt_intent_row(outcome={"observed_resolution": "interrupted_turn"})
    _script_interrupt_intents(conn, [intent])
    conn.on(_INTENT_ANNOTATE, annotated)
    _script_settle(conn, _settled_row(SETTLEMENT_INTERRUPTED_TURN))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(interrupted=True),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_INTERRUPTED_TURN
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] == 41
    _annotate_sql, annotate_params = next(
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.task_interrupt_intents" in " ".join(sql.split())
    )
    assert annotate_params is not None
    assert annotate_params[0].obj == {"observed_resolution": "interrupted_turn"}
    assert annotate_params[1] == intent[0]
    # The interrupted routing runs before every family routing: no Execution
    # subject read, no failure finalization, no Level 2 report, no block.
    assert not any("from openorc.executions" in sql for sql, _ in conn.executed)
    assert triage.calls == []


def test_a_failed_interrupted_terminal_never_reports_level_2(
    triage: _RecordingTriage,
) -> None:
    """A ``run.failed``-shaped terminal whose native evidence positively
    identifies an interrupted turn routes to the Interrupt settlement — never
    a synthetic failure disposition under the attempt family."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_interrupt_intents(conn, [_interrupt_intent_row()])
    conn.on(
        _INTENT_ANNOTATE,
        _interrupt_intent_row(outcome={"observed_resolution": "interrupted_turn"}),
    )
    _script_settle(conn, _settled_row(SETTLEMENT_INTERRUPTED_TURN))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            kind=DetachedTurnObservationKind.FAILED, candidate=None, interrupted=True
        ),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_INTERRUPTED_TURN
    assert triage.calls == []


def test_an_interrupted_terminal_without_a_matching_intent_settles_the_receipt_only(
    triage: _RecordingTriage,
) -> None:
    """No unsettled intent for the exact binding — the interruption already
    settled through its own branch, was durably recorded failed/unknown, or
    raced away — leaves the intent untouched while the turn's own terminal
    evidence still settles the receipt truthfully."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_interrupt_intents(conn, [])
    _script_settle(conn, _settled_row(SETTLEMENT_INTERRUPTED_TURN))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(interrupted=True),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
    assert not any(
        "update openorc.task_interrupt_intents" in " ".join(sql.split()) for sql, _ in conn.executed
    )
    assert triage.calls == []


def test_an_interrupted_terminal_with_an_ambiguous_intent_shape_fails_closed(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, None)
    _script_interrupt_intents(conn, [_interrupt_intent_row(), _interrupt_intent_row()])
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(interrupted=True),
    )
    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)
    assert not any(
        "update openorc.task_interrupt_intents" in " ".join(sql.split()) for sql, _ in conn.executed
    )
    assert triage.calls == []


def test_a_staged_interrupted_completion_routes_through_the_attach(
    triage: _RecordingTriage,
) -> None:
    """The staged early completion preserves its interrupted verdict through
    the admission race: the acknowledgement attach routes the positive
    interrupted-turn evidence exactly as a direct delivery."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, _staged_row(interrupted=True))
    _script_last_consumed_sequence(conn, None)
    _script_interrupt_intents(conn, [_interrupt_intent_row()])
    conn.on(
        _INTENT_ANNOTATE,
        _interrupt_intent_row(outcome={"observed_resolution": "interrupted_turn"}),
    )
    _script_settle(conn, _settled_row(SETTLEMENT_INTERRUPTED_TURN))
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_INTERRUPTED_TURN
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    delete_at = next(
        index for index, sql in enumerate(executed) if sql.startswith("delete from openorc")
    )
    settle_at = next(index for index, sql in enumerate(executed) if "set status = 'settled'" in sql)
    assert settle_at < delete_at


def test_settlement_settles_the_single_outstanding_turn_without_a_start_observation(
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
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
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
    _script_failure_binding_lookup(conn, None)
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            kind=DetachedTurnObservationKind.FAILED, marker=None, candidate=None
        ),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
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
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert triage.calls == []
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)
    assert not any("update openorc.executions" in sql for sql, _ in conn.executed)


# --- status-only reconciliation ------------------------------------------------------


def test_status_classification_routes_the_exact_failed_reference(
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
    _script_settle(conn, _settled_row("observed_failure", marker=None))
    _script_current_task(conn)
    _script_failure_binding_lookup(conn, None)
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        classifier=_CLASSIFIER,
        status_evidence=_status_evidence(TurnStatusEvidenceVerdict.FAILURE_EVIDENCE),
    )
    assert classification.kind is TurnStatusClassificationKind.CLASSIFIED_FAILURE
    assert classification.receipt is not None
    assert classification.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
    assert len(triage.calls) == 1


def test_status_classification_never_treats_completed_as_success_evidence(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _admitted_row(),
    )
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        classifier=_CLASSIFIER,
        status_evidence=_status_evidence(
            TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE
        ),
    )
    assert classification.kind is TurnStatusClassificationKind.NEVER_SUCCESS_EVIDENCE
    assert triage.calls == []
    assert not any("update openorc" in sql for sql, _ in conn.executed)


def test_status_classification_is_not_classifiable_unless_admitted() -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _receipt_row(),
    )
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        classifier=_CLASSIFIER,
        status_evidence=_status_evidence(TurnStatusEvidenceVerdict.FAILURE_EVIDENCE),
    )
    assert classification.kind is TurnStatusClassificationKind.NOT_CLASSIFIABLE


def test_status_classification_leaves_a_missing_record_unresolved() -> None:
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _admitted_row(),
    )
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        classifier=_CLASSIFIER,
        status_evidence=_status_evidence(TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE),
    )
    assert classification.kind is TurnStatusClassificationKind.UNRESOLVED


# --- the read-only run-start corroboration -------------------------------------------


def test_start_identity_corroboration_is_read_only_and_never_required() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn, repeat=True)
    _script_outstanding_receipt(conn, _admitted_row(), repeat=True)
    assert (
        check_turn_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            classifier=_CLASSIFIER,
            evidence=_start_evidence(TurnAttribution.ATTRIBUTED),
        )
        is TurnStartCorroboration.CORROBORATED
    )
    assert not any(
        " ".join(sql.split()).startswith(("update ", "insert ", "delete "))
        for sql, _ in conn.executed
    )


def test_start_identity_is_unattributable_before_admission_and_unowned_without_a_turn() -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _receipt_row())
    assert (
        check_turn_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            classifier=_CLASSIFIER,
            evidence=_start_evidence(TurnAttribution.ATTRIBUTED),
        )
        is TurnStartCorroboration.UNATTRIBUTABLE
    )
    conn = ScriptedConnection()
    _script_binding_lookup(conn, found=False)
    assert (
        check_turn_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            classifier=_CLASSIFIER,
            evidence=_start_evidence(TurnAttribution.ATTRIBUTED),
        )
        is TurnStartCorroboration.UNOWNED
    )


# --- the stale-replay safeguard ------------------------------------------------------


def test_settlement_rejects_a_stale_replay_after_a_consumed_settlement(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _admitted_row())
    _script_last_consumed_sequence(conn, 41)
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.REJECTED_STALE
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.ADMITTED
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
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT, marker=43))
    fresher = _terminal_evidence(marker=43)
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=fresher,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] == 43


# --- the explicit contradiction marker -----------------------------------------------


def test_settlement_leaves_a_contradicted_receipt_unresolved(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(
        conn,
        _receipt_row(contradicted=True, admitted_turn_reference=_TURN_REFERENCE, status="admitted"),
    )
    _script_last_consumed_sequence(conn, None)
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
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
        _receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE, contradicted=True),
    )
    assert (
        check_turn_start_identity(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            classifier=_CLASSIFIER,
            evidence=_start_evidence(TurnAttribution.CONTRADICTED),
        )
        is TurnStartCorroboration.CONTRADICTED
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
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
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
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_COMPLETION_EVIDENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
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
    run_id = _TURN_REFERENCE if status in ("admitted", "settled") else None
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
        None,
    )


def _settled_initialization_row(label: str, *, marker: int | None = 7) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if marker is not None:
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = marker
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


def _establishment_evidence(
    *,
    kind: DetachedTurnObservationKind = DetachedTurnObservationKind.COMPLETED,
    result_candidate: str | None = _READY_PAYLOAD_CANDIDATE,
) -> ObservedTurnEvidence:
    return _terminal_evidence(kind=kind, marker=7, candidate=result_candidate)


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

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_establishment_evidence(),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
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
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_OBSERVED_FAILURE, marker=None))
    _script_current_task(conn, status="planning")

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_establishment_evidence(kind=DetachedTurnObservationKind.FAILED),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
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
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_OBSERVED_FAILURE, marker=None))
    _script_current_task(conn, status="planning")

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_establishment_evidence(result_candidate=_GARBAGE_CANDIDATE),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
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
            "run.completed",
            7,
            _READY_PAYLOAD_CANDIDATE,
            False,
            _OBSERVED,
        ),
    )

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_establishment_evidence(),
    )

    # The early result is staged on the claimed receipt and consumed exactly
    # once by the acknowledgement attach: never ignored, never duplicated.
    assert outcome.kind is TurnSettlementKind.STAGED_PENDING_ACKNOWLEDGEMENT
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

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_establishment_evidence(),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
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

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_establishment_evidence(),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
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
    status-only consultation never treats it as ``session_ready`` or as a
    settled initialization."""
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _initialization_receipt_row(),
    )
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        classifier=_CLASSIFIER,
        status_evidence=_status_evidence(
            TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE
        ),
    )
    assert classification.kind is TurnStatusClassificationKind.NEVER_SUCCESS_EVIDENCE
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
    _script_settle(conn, _settled_initialization_row(SETTLEMENT_OBSERVED_FAILURE, marker=None))
    _script_current_task(conn, status="planning")
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        classifier=_CLASSIFIER,
        status_evidence=_status_evidence(TurnStatusEvidenceVerdict.FAILURE_EVIDENCE),
    )
    assert classification.kind is TurnStatusClassificationKind.CLASSIFIED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["operation"].family.value == "planning_bootstrap_prepare"


# --- the detached workspace-preparation (PREPARE) settlement (issue #334) -------


_PREPARE_CLAIM_TOKEN = uuid4()


def _prepare_receipt_row(
    *,
    status: str = "admitted",
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "planning_bootstrap_prepare",
        "planning_bootstrap_claim",
        _PREPARE_CLAIM_TOKEN,
        "workspace_result",
        _TURN_REFERENCE if status in ("admitted", "settled") else None,
        status,
        outcome if outcome is not None else {},
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


def _prepare_settled_row(label: str, *, marker: int | None = 41) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if marker is not None:
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = marker
    return _prepare_receipt_row(status="settled", outcome=outcome)


def _workspace_result_candidate(operation: str = "PREPARE", status: str = "COMPLETED") -> str:
    return json.dumps(
        {
            "type": "workspace_result",
            "schema_version": 1,
            "operation": operation,
            "status": status,
            "summary": "Reported by the scripted runtime.",
        }
    )


def _script_planning_task(conn: ScriptedConnection) -> None:
    """Script the current coarse-planning Task read (repeatable)."""
    conn.on_repeat(
        _sql_has_not("from openorc.tasks where id = %s", absent="planning_bootstrap"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status="planning",
            state_token=_STATE_TOKEN,
            source_requirements_fingerprint="a" * 64,
            current_plan_revision_id=None,
        ),
    )


def _script_bootstrap_evidence(conn: ScriptedConnection, *, stage: str | None) -> None:
    conn.on(
        _sql_has("planning_bootstrap_claim_token", "planning_bootstrap_stage"),
        (
            _PREPARE_CLAIM_TOKEN,
            _OBSERVED,
            stage,
            _OBSERVED,
        ),
    )


def _prepare_settlement_flow(
    triage: _RecordingTriage,
    *,
    stage: str | None = "prepare_dispatched",
    candidate: str | None,
    kind: DetachedTurnObservationKind = DetachedTurnObservationKind.COMPLETED,
    settle_label: str = SETTLEMENT_VALIDATED_RESULT,
) -> tuple[ScriptedConnection, TurnSettlementOutcome]:
    conn = ScriptedConnection()
    _script_planning_task(conn)
    _script_bootstrap_evidence(conn, stage=stage)
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _prepare_receipt_row())
    _script_receipt_for_update(conn, _prepare_receipt_row())
    _script_last_consumed_sequence(conn, None)
    conn.on(
        _sql_has("update openorc.tasks", "planning_bootstrap_stage = %s"),
        True,
    )
    _script_settle(conn, _prepare_settled_row(settle_label))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(kind=kind, marker=41, candidate=candidate),
    )
    return conn, outcome


def test_a_validated_plan_result_finalizes_the_claim_and_the_receipt(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _plan_settlement_flow(triage, candidate=_PLAN_CANDIDATE)

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.operation.value == "initial_plan_dispatch"
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert outcome.formal_result is not None
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    # The immutable revision, the pointer/token rotation, and the send-claim
    # settlement all committed inside the settlement's transaction.
    assert any("insert into openorc.plan_revisions" in sql for sql in executed)
    assert any("current_plan_revision_id" in sql for sql in executed)
    assert any("update openorc.task_runtime_send_claims" in sql for sql in executed)


def test_a_plan_completion_conflicting_with_the_claim_binding_is_left_unresolved(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _plan_settlement_flow(
        triage,
        candidate=_PLAN_CANDIDATE,
        claim_row=_plan_claim_row(kind="planning_revise_dispatch"),
    )

    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("insert into openorc.plan_revisions" in sql for sql in executed)


def test_a_failed_plan_terminal_settles_observed_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _plan_settlement_flow(
        triage,
        candidate=None,
        kind=DetachedTurnObservationKind.FAILED,
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    operation = triage.calls[0]["operation"]
    assert operation.family.value == "initial_plan_dispatch"
    assert operation.original_attempt_kind.value == "runtime_send_claim"
    assert operation.original_attempt_id == _CLAIM_ID
    assert triage.calls[0]["report"].error_code.value == "external_operation_failed"
    assert triage.calls[0]["expected_status"].value == "planning"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("insert into openorc.plan_revisions" in sql for sql in executed)


def test_a_malformed_plan_result_classifies_the_protocol_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _plan_settlement_flow(
        triage,
        candidate=_RESULT_CANDIDATE,
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["operation"].family.value == "initial_plan_dispatch"
    assert triage.calls[0]["report"].error_code.value == "runtime_protocol"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("insert into openorc.plan_revisions" in sql for sql in executed)


def test_validated_prepare_result_records_the_prepared_stage_with_the_receipt(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _prepare_settlement_flow(
        triage,
        candidate=_workspace_result_candidate(),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.operation.value == "planning_bootstrap_prepare"
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert isinstance(outcome.formal_result, WorkspaceResult)
    assert triage.calls == []
    stage_writes = [
        params
        for sql, params in conn.executed
        if "planning_bootstrap_stage = %s" in " ".join(sql.split())
    ]
    assert len(stage_writes) == 1
    assert "prepared" in str(stage_writes[0])


def test_moved_on_bootstrap_settles_the_validated_result_as_not_applied(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _prepare_settlement_flow(
        triage,
        stage="prepared",
        candidate=_workspace_result_candidate(),
        settle_label=SETTLEMENT_RESULT_NOT_APPLIED,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("planning_bootstrap_stage = %s" in sql for sql in executed)


def test_failed_prepare_terminal_settles_observed_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _prepare_settlement_flow(
        triage,
        candidate=None,
        kind=DetachedTurnObservationKind.FAILED,
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    operation = triage.calls[0]["operation"]
    assert operation.family.value == "planning_bootstrap_prepare"
    assert operation.original_attempt_id == _PREPARE_CLAIM_TOKEN
    assert triage.calls[0]["report"].error_code.value == "external_operation_failed"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("planning_bootstrap_stage = %s" in sql for sql in executed)


def test_failed_workspace_result_settles_observed_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _prepare_settlement_flow(
        triage,
        candidate=_workspace_result_candidate(status="FAILED"),
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code.value == "external_operation_failed"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("planning_bootstrap_stage = %s" in sql for sql in executed)


def test_wrong_operation_result_settles_observed_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _prepare_settlement_flow(
        triage,
        candidate=_workspace_result_candidate(operation="CLEANUP"),
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code.value == "external_operation_failed"


def test_malformed_result_settles_the_protocol_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _prepare_settlement_flow(
        triage,
        candidate="{not a formal response}",
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code.value == "runtime_protocol"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("planning_bootstrap_stage = %s" in sql for sql in executed)


# --- the plan-result routing (issue #335) ----------------------------------------


_CLAIM_ID = uuid4()
_BASE_SHA = "d" * 40
_PLAN_CANDIDATE = json.dumps(
    {"type": "plan_result", "schema_version": 1, "plan": "# The detached plan.\n"}
)


def _plan_session_row(role: str) -> tuple[Any, ...]:
    return (
        _SESSION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
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


def _plan_claim_row(
    *,
    kind: str = "initial_plan_dispatch",
    status: str = "claimed",
) -> tuple[Any, ...]:
    settled_at = _OBSERVED if status != "claimed" else None
    return (
        _CLAIM_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        kind,
        _SESSION_ID,
        _ITERATION_ID if kind == "planning_revise_dispatch" else None,
        None,
        None,
        _BASE_SHA,
        status,
        {},
        _OBSERVED,
        settled_at,
        _OBSERVED,
    )


def _plan_receipt_row(
    *,
    status: str = "admitted",
    outcome: dict[str, Any] | None = None,
    operation: str = "initial_plan_dispatch",
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        operation,
        "runtime_send_claim",
        _CLAIM_ID,
        "plan_result",
        _TURN_REFERENCE if status in ("admitted", "settled") else None,
        status,
        outcome if outcome is not None else {},
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


def _plan_settled_row(label: str, *, marker: int | None = 41) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if marker is not None:
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = marker
    return _plan_receipt_row(status="settled", outcome=outcome)


def _plan_settlement_flow(
    triage: _RecordingTriage,
    *,
    candidate: str | None,
    kind: DetachedTurnObservationKind = DetachedTurnObservationKind.COMPLETED,
    settle_label: str = SETTLEMENT_VALIDATED_RESULT,
    claim_row: tuple[Any, ...] | None = None,
    operation: str = "initial_plan_dispatch",
) -> tuple[ScriptedConnection, TurnSettlementOutcome]:
    conn = ScriptedConnection()
    _script_planning_task(conn)
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _plan_receipt_row(operation=operation))
    _script_receipt_for_update(conn, _plan_receipt_row(operation=operation))
    _script_last_consumed_sequence(conn, None)
    conn.on(
        _sql_has("from openorc.task_runtime_send_claims where id"),
        claim_row if claim_row is not None else _plan_claim_row(),
    )
    # The owning settlement's and the commit composition's binding reproofs,
    # in call order: the claimed Producer session, the Task's current
    # Reviewer binding, then the commit's producer/reviewer rechecks.
    for role in ("producer", "reviewer", "producer", "reviewer"):
        conn.on(
            _sql_has("from openorc.task_agent_sessions", "role = %s"),
            _plan_session_row(role),
        )
    if settle_label == SETTLEMENT_VALIDATED_RESULT:
        # The owning service's shared commit composition.
        conn.on(_sql_has("from openorc.plan_revisions where task_id"), [])
        conn.on(
            _sql_has("insert into openorc.plan_revisions"),
            (
                _REVISION_ID,
                _WORKSPACE_ID,
                _TASK_ID,
                1,
                "# The detached plan.\n",
                _BASE_SHA,
                _OBSERVED,
            ),
        )
        conn.on(
            _sql_has("update openorc.tasks", "current_plan_revision_id"),
            task_row(
                id=_TASK_ID,
                workspace_id=_WORKSPACE_ID,
                status="planning",
                state_token=_STATE_TOKEN,
                source_requirements_fingerprint="a" * 64,
                current_plan_revision_id=_REVISION_ID,
            ),
        )
        _script_event_insert(conn, event_type=WorkflowEventType.PLAN_REVISION_CREATED)
        conn.on(
            _sql_has("update openorc.task_runtime_send_claims", "settled_at = now()"),
            _plan_claim_row(status="settled"),
        )
    _script_settle(conn, _plan_settled_row(settle_label))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(kind=kind, marker=41, candidate=candidate),
    )
    return conn, outcome


# --- the planning-REVIEW family's settlement routing (issue #336) --------------

_REVIEW_LOOP_ID = uuid4()
_REVIEW_CANDIDATE = json.dumps(
    {
        "type": "review_result",
        "schema_version": 1,
        "outcome": "CHANGES_REQUESTED",
        "summary": "Needs work",
        "findings": [{"summary": "Clarify scope", "details": "Say which module."}],
    }
)


def _review_loop_row(*, status: str = "open") -> tuple[Any, ...]:
    return (
        _REVIEW_LOOP_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "planning",
        5,
        status,
        None,
        _OBSERVED,
    )


def _review_revision_row() -> tuple[Any, ...]:
    return (
        _REVISION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        1,
        "# The detached plan.\n",
        _BASE_SHA,
        _OBSERVED,
    )


def _review_iteration_row(*, outcome: str | None = None) -> tuple[Any, ...]:
    return (
        _ITERATION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REVIEW_LOOP_ID,
        1,
        _REVISION_ID,
        None,
        None,
        outcome,
        "Needs work" if outcome is not None else None,
        [{"summary": "Clarify scope", "details": "Say which module."}]
        if outcome is not None
        else None,
        _OBSERVED if outcome is not None else None,
        _OBSERVED,
    )


def _review_receipt_row(
    *,
    status: str = "admitted",
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "reviewer_review",
        "review_iteration",
        _ITERATION_ID,
        "review_result",
        _TURN_REFERENCE if status in ("admitted", "settled") else None,
        status,
        outcome if outcome is not None else {},
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


def _review_settled_row(label: str, *, marker: int | None = 41) -> tuple[Any, ...]:
    settled_outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if marker is not None:
        settled_outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = marker
    return _review_receipt_row(status="settled", outcome=settled_outcome)


def _script_review_planning_task(conn: ScriptedConnection) -> None:
    """Script the current coarse-planning Task read carrying the reviewed
    revision (repeatable across the routing's and the decision's reads)."""
    conn.on_repeat(
        _sql_has_not("from openorc.tasks where id = %s", absent="planning_bootstrap"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status="planning",
            state_token=_STATE_TOKEN,
            source_requirements_fingerprint="a" * 64,
            current_plan_revision_id=_REVISION_ID,
        ),
    )


def _script_review_iteration_read(
    conn: ScriptedConnection, row: tuple[Any, ...], *, times: int = 1
) -> None:
    for _ in range(times):
        conn.on(_sql_has("from openorc.review_iterations where id = %s"), row)


def _script_reviewer_binding_read(conn: ScriptedConnection, *, times: int = 1) -> None:
    for _ in range(times):
        conn.on(
            _sql_has("from openorc.task_agent_sessions", "role = %s"),
            _plan_session_row("reviewer"),
        )


def _review_settlement_flow(
    triage: _RecordingTriage,
    *,
    candidate: str | None,
    kind: DetachedTurnObservationKind = DetachedTurnObservationKind.COMPLETED,
    settle_label: str = SETTLEMENT_VALIDATED_RESULT,
    iteration_reads: list[tuple[Any, ...]] | None = None,
    binding_reads: int = 2,
    with_decision_composition: bool = True,
) -> tuple[ScriptedConnection, TurnSettlementOutcome]:
    conn = ScriptedConnection()
    _script_review_planning_task(conn)
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _review_receipt_row())
    _script_receipt_for_update(conn, _review_receipt_row())
    _script_last_consumed_sequence(conn, None)
    for row in (
        iteration_reads
        if iteration_reads is not None
        else [
            _review_iteration_row(),
            _review_iteration_row(),
            _review_iteration_row(),
        ]
    ):
        _script_review_iteration_read(conn, row)
    _script_reviewer_binding_read(conn, times=binding_reads)
    if with_decision_composition:
        conn.on(_sql_has("from openorc.plan_revisions where id"), _review_revision_row())
        conn.on(
            _sql_has("from openorc.review_loops where id", "for update"),
            _review_loop_row(),
        )
        conn.on(
            _sql_has("update openorc.review_iterations"),
            _review_iteration_row(outcome="changes_requested"),
        )
        conn.on(
            _sql_has("review_iterations where review_loop_id"),
            [_review_iteration_row(outcome="changes_requested")],
        )
        conn.on(_sql_has("from openorc.owner_gates where task_id"), [])
        _script_event_insert(conn, event_type=WorkflowEventType.PLAN_REVIEWED)
    _script_settle(conn, _review_settled_row(settle_label))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(kind=kind, marker=41, candidate=candidate),
    )
    return conn, outcome


def test_a_validated_review_result_finalizes_the_decision_and_the_receipt(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _review_settlement_flow(triage, candidate=_REVIEW_CANDIDATE)

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.operation.value == "reviewer_review"
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert outcome.formal_result is not None
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    # The one-shot result recording and the REVIEWER audit event committed
    # inside the settlement's transaction; no send claim exists to settle.
    assert any("update openorc.review_iterations" in sql for sql in executed)
    assert any("insert into openorc.workflow_events" in sql for sql in executed)
    assert not any("update openorc.task_runtime_send_claims" in sql for sql in executed)


def test_a_review_completion_conflicting_with_the_reserved_attempt_is_left_unresolved(
    triage: _RecordingTriage,
) -> None:
    """An attributed completion whose reserved iteration is already finalized
    (a duplicate or late delivery) is never decided twice."""
    conn, outcome = _review_settlement_flow(
        triage,
        candidate=_REVIEW_CANDIDATE,
        iteration_reads=[_review_iteration_row(outcome="changes_requested")],
        with_decision_composition=False,
        settle_label=SETTLEMENT_RESULT_NOT_APPLIED,
    )

    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("update openorc.review_iterations" in sql for sql in executed)


def test_a_review_completion_conflicting_with_the_reviewer_binding_is_left_unresolved(
    triage: _RecordingTriage,
) -> None:
    """A completion whose Reviewer session binding moved on never invents a
    match: the observation is left unresolved."""
    conn = ScriptedConnection()
    _script_review_planning_task(conn)
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _review_receipt_row())
    _script_receipt_for_update(conn, _review_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_review_iteration_read(conn, _review_iteration_row())
    # The Task's current REVIEWER binding is a different session row.
    conn.on(
        _sql_has("from openorc.task_agent_sessions", "role = %s"),
        (
            uuid4(),
            _WORKSPACE_ID,
            _TASK_ID,
            "reviewer",
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
        ),
    )
    _script_settle(conn, _review_settled_row(SETTLEMENT_RESULT_NOT_APPLIED))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            kind=DetachedTurnObservationKind.COMPLETED, marker=41, candidate=_REVIEW_CANDIDATE
        ),
    )

    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert triage.calls == []


def test_a_failed_review_terminal_settles_observed_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _review_settlement_flow(
        triage,
        candidate=None,
        kind=DetachedTurnObservationKind.FAILED,
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
        with_decision_composition=False,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    operation = triage.calls[0]["operation"]
    assert operation.family.value == "reviewer_review"
    assert operation.original_attempt_kind.value == "review_iteration"
    assert operation.original_attempt_id == _ITERATION_ID
    assert triage.calls[0]["report"].error_code.value == "external_operation_failed"
    assert triage.calls[0]["expected_status"].value == "planning"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("update openorc.review_iterations" in sql for sql in executed)


def test_a_malformed_review_result_classifies_the_protocol_failure_and_reports(
    triage: _RecordingTriage,
) -> None:
    conn, outcome = _review_settlement_flow(
        triage,
        candidate=_RESULT_CANDIDATE,
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
        with_decision_composition=False,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["operation"].family.value == "reviewer_review"
    assert triage.calls[0]["report"].error_code.value == "runtime_protocol"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("update openorc.review_iterations" in sql for sql in executed)


def test_a_moved_on_review_settles_the_validated_result_as_not_applied(
    triage: _RecordingTriage,
) -> None:
    """A validated result whose reserved attempt was finalized concurrently
    (the post-lock reproof sees the settled outcome) applies nothing: the
    receipt settles result-not-applied and no decision is fabricated."""
    conn, outcome = _review_settlement_flow(
        triage,
        candidate=_REVIEW_CANDIDATE,
        iteration_reads=[
            _review_iteration_row(),
            _review_iteration_row(),
            _review_iteration_row(outcome="changes_requested"),
        ],
        settle_label=SETTLEMENT_RESULT_NOT_APPLIED,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("insert into openorc.workflow_events" in sql for sql in executed)


# --- the PR-review settlement routing (issue #338) -------------------------------------

_PR_ITERATION_ID = uuid4()
_PR_RECORD_ID = uuid4()
_PR_HEAD = "c" * 40


def _review_pr_row() -> tuple[Any, ...]:
    """The canonical TaskPullRequest row at the reviewed head."""
    return (
        _PR_RECORD_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        uuid4(),
        9001,
        42,
        "feat/branch",
        "main",
        _PR_HEAD,
        "open",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _review_pr_loop_row(*, status: str = "open") -> tuple[Any, ...]:
    return (
        _REVIEW_LOOP_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "pr_review",
        5,
        status,
        None,
        _OBSERVED,
    )


def _review_pr_iteration_row(*, outcome: str | None = None) -> tuple[Any, ...]:
    """A PR/head review iteration: canonical pull request subject, no plan."""
    return (
        _PR_ITERATION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REVIEW_LOOP_ID,
        1,
        None,
        _PR_RECORD_ID,
        _PR_HEAD,
        outcome,
        "Needs work" if outcome is not None else None,
        [{"summary": "Clarify scope", "details": "Say which module."}]
        if outcome is not None
        else None,
        _OBSERVED if outcome is not None else None,
        _OBSERVED,
    )


def _review_pr_receipt_row(
    *,
    status: str = "admitted",
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "reviewer_review",
        "review_iteration",
        _PR_ITERATION_ID,
        "review_result",
        _TURN_REFERENCE if status in ("admitted", "settled") else None,
        status,
        outcome if outcome is not None else {},
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


def _review_pr_settled_row(label: str, *, marker: int | None = 41) -> tuple[Any, ...]:
    settled_outcome: dict[str, Any] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if marker is not None:
        settled_outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = marker
    return _review_pr_receipt_row(status="settled", outcome=settled_outcome)


def _script_review_reviewing_task(conn: ScriptedConnection) -> None:
    """Script the current coarse-reviewing Task read (repeatable)."""
    conn.on_repeat(
        _sql_has_not("from openorc.tasks where id = %s", absent="planning_bootstrap"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status="reviewing",
            state_token=_STATE_TOKEN,
            canonical_feature_branch="feat/branch",
            current_plan_revision_id=None,
            source_requirements_fingerprint="a" * 64,
        ),
    )


def _pr_review_settlement_flow(
    triage: _RecordingTriage,
    *,
    candidate: str | None,
    kind: DetachedTurnObservationKind = DetachedTurnObservationKind.COMPLETED,
    settle_label: str = SETTLEMENT_VALIDATED_RESULT,
    iteration_reads: list[tuple[Any, ...]] | None = None,
    binding_reads: int = 2,
    with_decision_composition: bool = True,
) -> tuple[ScriptedConnection, TurnSettlementOutcome]:
    conn = ScriptedConnection()
    _script_review_reviewing_task(conn)
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _review_pr_receipt_row())
    _script_receipt_for_update(conn, _review_pr_receipt_row())
    _script_last_consumed_sequence(conn, None)
    for row in (
        iteration_reads
        if iteration_reads is not None
        else [
            _review_pr_iteration_row(),
            _review_pr_iteration_row(),
        ]
    ):
        _script_review_iteration_read(conn, row)
    _script_reviewer_binding_read(conn, times=binding_reads)
    if with_decision_composition:
        conn.on(_sql_has("from openorc.task_pull_requests"), _review_pr_row())
        conn.on(
            _sql_has("from openorc.review_loops where id", "for update"),
            _review_pr_loop_row(),
        )
        conn.on(
            _sql_has("update openorc.review_iterations"),
            _review_pr_iteration_row(outcome="changes_requested"),
        )
        conn.on_repeat(
            _sql_has("review_iterations where review_loop_id"),
            [_review_pr_iteration_row(outcome="changes_requested")],
        )
        conn.on(_sql_has("from openorc.owner_gates where task_id"), [])
        _script_event_insert(conn, event_type=WorkflowEventType.PR_REVIEWED)
    _script_settle(conn, _review_pr_settled_row(settle_label))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(kind=kind, marker=41, candidate=candidate),
    )
    return conn, outcome


def test_a_validated_pr_review_result_settles_through_the_pr_decision(
    triage: _RecordingTriage,
) -> None:
    """A REVIEWER_REVIEW terminal on a PR/head iteration routes to the owning
    PR-review decision settlement (issue #338): the one-shot result recording,
    the REVIEWER ``PR_REVIEWED`` audit, and the receipt settle atomically —
    never through the planning decision composition."""
    conn, outcome = _pr_review_settlement_flow(triage, candidate=_REVIEW_CANDIDATE)

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.operation.value == "reviewer_review"
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert outcome.formal_result is not None
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert any("update openorc.review_iterations" in sql for sql in executed)
    assert any("insert into openorc.workflow_events" in sql for sql in executed)
    # The PR/head flavor resolved durably: the canonical pull request subject
    # was re-proven and the planning composition's revision read never ran.
    assert any("from openorc.task_pull_requests" in sql for sql in executed)
    assert not any("from openorc.plan_revisions" in sql for sql in executed)


def test_a_failed_pr_review_terminal_reports_with_the_reviewing_expected_status(
    triage: _RecordingTriage,
) -> None:
    """A PR/head review attempt's observed failure reports through the review
    family with the attempt flavor's durable ``reviewing`` expected status —
    never the flat family map's planning expectation (issue #338)."""
    conn, outcome = _pr_review_settlement_flow(
        triage,
        candidate=None,
        kind=DetachedTurnObservationKind.FAILED,
        settle_label=SETTLEMENT_OBSERVED_FAILURE,
        with_decision_composition=False,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    operation = triage.calls[0]["operation"]
    assert operation.family.value == "reviewer_review"
    assert operation.original_attempt_kind.value == "review_iteration"
    assert operation.original_attempt_id == _PR_ITERATION_ID
    assert triage.calls[0]["expected_status"].value == "reviewing"
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("update openorc.review_iterations" in sql for sql in executed)


def test_a_moved_on_pr_review_settles_the_validated_result_as_not_applied(
    triage: _RecordingTriage,
) -> None:
    """A validated PR/head result whose reserved attempt was finalized
    concurrently applies nothing: the receipt settles result-not-applied and
    no second verdict, gate, or event is fabricated."""
    conn, outcome = _pr_review_settlement_flow(
        triage,
        candidate=_REVIEW_CANDIDATE,
        iteration_reads=[
            _review_pr_iteration_row(),
            _review_pr_iteration_row(outcome="changes_requested"),
        ],
        settle_label=SETTLEMENT_RESULT_NOT_APPLIED,
        with_decision_composition=False,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("insert into openorc.workflow_events" in sql for sql in executed)


# --- the PR_COMPOSE settlement routing (issue #337) -----------------------------------

_PR_CLAIM_ID = uuid4()
_PR_GATE_ROW_ID = uuid4()
_PR_APPROVER_ID = uuid4()
_PR_RESULT_CANDIDATE = json.dumps(pr_result_candidate(title="feat: ship it", body="Done."))


def _pr_compose_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
) -> tuple[Any, ...]:
    """The PR_COMPOSE family's receipt row: rooted at the send claim."""
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "pr_compose_dispatch",
        "runtime_send_claim",
        _PR_CLAIM_ID,
        "pr_result",
        admitted_turn_reference,
        status,
        {},
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


def _pr_compose_claim_row(*, status: str = "claimed") -> tuple[Any, ...]:
    """The 14-column PR_COMPOSE send-claim row the route reads."""
    settled_at = _OBSERVED if status != "claimed" else None
    return (
        _PR_CLAIM_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "pr_compose_dispatch",
        _SESSION_ID,
        None,
        _PR_GATE_ROW_ID,
        "d" * 40,
        None,
        status,
        {},
        _OBSERVED,
        settled_at,
        _OBSERVED,
    )


def _script_pr_compose_settlement_subjects(conn: ScriptedConnection) -> None:
    """The owning settlement's subject re-derivation reads: the Task read
    under the exclusive Task row lock (the settlement's currentness
    serialization, issue #337 review), the absent canonical record, the
    Workspace-scope task read under the route-subject resolution, and the
    repeating gate/audit/session/barrier/Workspace/route reads."""
    _waiting_row = task_row(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        status="waiting_for_owner",
        state_token=_STATE_TOKEN,
        source_requirements_fingerprint="a" * 64,
        canonical_feature_branch="openorc/task-42-implementation",
    )
    conn.on(_sql_has("from openorc.tasks where id = %s for update"), _waiting_row)
    conn.on(_sql_has("from openorc.task_pull_requests"), None)
    conn.on(_sql_has_not("from openorc.tasks where id = %s", absent="for update"), _waiting_row)
    conn.on_repeat(
        _sql_has("from openorc.owner_gates"),
        (
            uuid4(),
            _WORKSPACE_ID,
            _TASK_ID,
            OwnerGateType.PR_AUTHORIZATION.value,
            OwnerGateStatus.APPROVED.value,
            None,
            "d" * 40,
            None,
            None,
            _OBSERVED,
            _OBSERVED,
        ),
    )
    conn.on_repeat(
        _sql_has("from openorc.workflow_events"),
        [
            (
                uuid4(),
                _WORKSPACE_ID,
                _TASK_ID,
                "owner_gate_resolved",
                "owner",
                str(_PR_APPROVER_ID),
                "owner_gate",
                _PR_GATE_ROW_ID,
                {"outcome": "approved"},
                _OBSERVED,
            )
        ],
    )
    conn.on_repeat(_sql_has("from openorc.task_agent_sessions"), _session_row())
    conn.on_repeat(
        _sql_has("from openorc.profiles"),
        (None, None, None),
    )
    conn.on_repeat(
        _sql_has("from openorc.workspaces"),
        (
            _WORKSPACE_ID,
            _PR_APPROVER_ID,
            "platform",
            _OBSERVED,
            _OBSERVED,
            2,
            "",
            None,
        ),
    )
    conn.on_repeat(
        _sql_has("from openorc.repositories"),
        (
            uuid4(),
            uuid4(),
            _WORKSPACE_ID,
            987654321,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            _OBSERVED,
            _OBSERVED,
            uuid4(),
        ),
    )
    conn.on_repeat(
        _sql_has("from openorc.github_installations"),
        (
            uuid4(),
            _WORKSPACE_ID,
            12345678,
            501,
            "octocat",
            "Organization",
            None,
            _OBSERVED,
            _OBSERVED,
        ),
    )


def test_a_validated_pr_result_settles_the_pr_compose_claim_and_receipt(
    triage: _RecordingTriage,
) -> None:
    """The PR_COMPOSE family's attributed completion funnels the validated
    ``pr_result`` through the owning settlement: the send claim and the
    receipt settle atomically and no Level 2 report runs (issue #337)."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _pr_compose_receipt_row())
    _script_attach(
        conn,
        _pr_compose_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE),
    )
    _script_staged(conn, _staged_row(candidate=_PR_RESULT_CANDIDATE))
    conn.on(
        _sql_has("from openorc.task_runtime_send_claims where id = %s"),
        _pr_compose_claim_row(),
    )
    _script_pr_compose_settlement_subjects(conn)
    conn.on(
        _sql_has("update openorc.task_runtime_send_claims"),
        _pr_compose_claim_row(status="settled"),
    )
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT))
    _script_consume(conn)

    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert isinstance(outcome.formal_result, PrResult)
    assert outcome.formal_result.title == "feat: ship it"
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    # The settlement's currentness observation is serialized on the Task row
    # lock (issue #337 review): the settlement writes no Task row of its own,
    # so the locked read is what excludes a concurrent transition.
    task_read_at = next(
        index
        for index, sql in enumerate(executed)
        if "from openorc.tasks where id" in sql and "for update" in sql
    )
    settle_at = next(index for index, sql in enumerate(executed) if "set status = 'settled'" in sql)
    assert task_read_at < settle_at
    delete_at = next(
        index for index, sql in enumerate(executed) if sql.startswith("delete from openorc")
    )
    assert settle_at < delete_at


def test_a_failed_pr_compose_terminal_reports_level_2_under_the_claim_family(
    triage: _RecordingTriage,
) -> None:
    """An observed PR_COMPOSE run failure settles the receipt truthfully and
    reports through the existing Level 2 machinery under the claim's own
    family at the composition's expected Task status — never a re-dispatch
    of the one-shot claim (issue #337)."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _pr_compose_receipt_row())
    _script_attach(
        conn, _pr_compose_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE)
    )
    _script_staged(conn, _staged_row(kind="run.failed"))
    conn.on(
        _sql_has("from openorc.task_runtime_send_claims where id = %s"),
        _pr_compose_claim_row(),
    )
    _script_settle(conn, _settled_row("observed_failure"))
    _script_current_task(conn, status="waiting_for_owner")
    _script_consume(conn)

    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    call = triage.calls[0]
    assert call["expected_status"].value == "waiting_for_owner"
    assert call["operation"].family.value == "pr_compose_dispatch"
    assert call["operation"].original_attempt_kind.value == "runtime_send_claim"
    assert call["operation"].original_attempt_id == _PR_CLAIM_ID
    assert call["report"].error_code is OperationalFailureCode.EXTERNAL_OPERATION_FAILED


# --- the detached Owner-authorized same-session Resume (issue #343) -------------

_RESUME_RECORD_ID = uuid4()
_RESUME_BLOCK_ID = uuid4()
_RESUME_OWNER_PROFILE_ID = uuid4()
_RESUME_STATE_TOKEN = uuid4()


def _resume_record_row(
    *,
    status: str = "dispatched",
    routing: dict[str, Any] | None = None,
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    return (
        _RESUME_RECORD_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        _RESUME_BLOCK_ID,
        WorkflowRole.PRODUCER.value,
        _EXTERNAL_SESSION,
        _RESUME_OWNER_PROFILE_ID,
        status,
        routing
        if routing is not None
        else {
            "operation": "implementation_execution",
            "subject_kind": "execution",
            "subject_id": str(_EXECUTION_ID),
            "expected_response_family": "implementation_result",
            "restored_status": "implementing",
            "authorized_state_token": str(_RESUME_STATE_TOKEN),
        },
        outcome if outcome is not None else {},
        _OBSERVED,
        _OBSERVED,
    )


def _resume_receipt_row(*, status: str = "claimed") -> tuple[Any, ...]:
    return _receipt_row(
        status=status,
        admitted_turn_reference=_TURN_REFERENCE if status != "claimed" else None,
        operation="session_resume",
        original_attempt_id=_RESUME_RECORD_ID,
        original_attempt_kind="owner_control_intent",
        expected_family="implementation_result",
    )


def _resume_settled_row(label: str) -> tuple[Any, ...]:
    return _settled_row(
        label,
        original_attempt_id=_RESUME_RECORD_ID,
        original_attempt_kind="owner_control_intent",
        expected_family="implementation_result",
    )


def _script_resume_record_read(conn: ScriptedConnection, row: Any, *, repeat: bool = False) -> None:
    register = conn.on_repeat if repeat else conn.on
    register(_sql_has("from openorc.task_resume_intents", "where id = %s"), row)


def _script_resume_record_outcome(conn: ScriptedConnection, row: Any) -> None:
    conn.on(_sql_has("update openorc.task_resume_intents", "set status = %s"), row)


def test_attach_settles_a_validated_resumed_implementation_result_atomically(
    triage: _RecordingTriage,
) -> None:
    """The resumed turn is the ORIGINAL interrupted IMPLEMENT continuing: the
    validated ``implementation_result`` finalizes the SAME Execution through
    the owning settlement, and the record's ``settled`` outcome and the
    receipt's validated settlement commit in one composition — with the
    handoff routed by the ORIGINAL family, never ``session_resume``."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _resume_receipt_row())
    _script_attach(conn, _resume_receipt_row(status="admitted"))
    _script_staged(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    _script_resume_record_read(conn, _resume_record_row())
    _script_execution_read(conn, _execution_row())
    _script_task_lock(conn)
    _script_implementation_subject(conn)
    _script_execution_lock(conn, _execution_row())
    _script_complete_execution(conn)
    _script_event_insert(conn, event_type=WorkflowEventType.EXECUTION_COMPLETED)
    _script_resume_record_outcome(
        conn, _resume_record_row(status="settled", outcome={"result": "processed"})
    )
    _script_settle(conn, _resume_settled_row(SETTLEMENT_VALIDATED_RESULT))
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert isinstance(outcome.formal_result, ImplementationResult)
    # The ONE post-commit handoff routes by the ORIGINAL workflow family.
    assert (
        outcome.handoff_operation is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    )
    assert triage.calls == []
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    record_at = next(
        index
        for index, sql in enumerate(executed)
        if sql.startswith("update openorc.task_resume_intents")
    )
    settle_at = next(index for index, sql in enumerate(executed) if "set status = 'settled'" in sql)
    assert record_at < settle_at


def test_the_observed_failure_of_a_resumed_turn_settles_the_record_failed(
    triage: _RecordingTriage,
) -> None:
    """A positively observed failed resumed run settles the receipt truthfully
    AND the record's truthful terminal ``failed`` outcome atomically with the
    Level 2 report under the record's own SESSION_RESUME family — post-effect
    evidence is never a pre-effect non-delivery, the original Execution stays
    exactly as it is, and the record stops vetoing Cancel (issue #343)."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _resume_receipt_row())
    _script_attach(conn, _resume_receipt_row(status="admitted"))
    _script_staged(conn, _staged_row(kind="run.failed"))
    _script_resume_record_read(conn, _resume_record_row(), repeat=True)
    _script_resume_record_outcome(
        conn,
        _resume_record_row(
            status="failed",
            outcome={
                "result": "observed_failure",
                "failure_kind": "observed_run_failure",
                "error_code": "external_operation_failed",
            },
        ),
    )
    _script_settle(conn, _resume_settled_row("observed_failure"))
    _script_current_task(conn)
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == "observed_failure"
    assert len(triage.calls) == 1
    call = triage.calls[0]
    # The Level 2 report roots at the record's own durable identity, at the
    # restored Task status the Owner authorization committed.
    assert call["operation"].family.value == "session_resume"
    assert call["operation"].original_attempt_kind.value == "owner_control_intent"
    assert call["operation"].original_attempt_id == _RESUME_RECORD_ID
    assert call["expected_status"].value == "implementing"
    assert call["report"].error_code is OperationalFailureCode.EXTERNAL_OPERATION_FAILED
    assert call["report"].effect_evidence is FailureEffectEvidence.KNOWN_FAILED
    record_params = next(
        params
        for sql, params in conn.executed
        if "update openorc.task_resume_intents" in " ".join(sql.split())
    )
    assert record_params is not None
    assert record_params[0] == "failed"
    # A post-effect failure never touches the original attempt's subjects.
    assert not any("from openorc.executions" in sql for sql, _ in conn.executed)


def test_a_malformed_resumed_result_is_a_protocol_failure_with_the_record_failed(
    triage: _RecordingTriage,
) -> None:
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _resume_receipt_row())
    _script_attach(conn, _resume_receipt_row(status="admitted"))
    _script_staged(conn, _staged_row(candidate="not a formal response"))
    _script_resume_record_read(conn, _resume_record_row(), repeat=True)
    _script_execution_read(conn, _execution_row())
    _script_resume_record_outcome(
        conn,
        _resume_record_row(
            status="failed",
            outcome={
                "result": "observed_failure",
                "failure_kind": "malformed_run_result",
                "error_code": "runtime_protocol",
            },
        ),
    )
    _script_settle(conn, _resume_settled_row("observed_failure"))
    _script_current_task(conn)
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert len(triage.calls) == 1
    assert triage.calls[0]["report"].error_code is OperationalFailureCode.RUNTIME_PROTOCOL
    record_params = next(
        params
        for sql, params in conn.executed
        if "update openorc.task_resume_intents" in " ".join(sql.split())
    )
    assert record_params is not None
    assert record_params[0] == "failed"
    assert record_params[1].obj == {
        "result": "observed_failure",
        "failure_kind": "malformed_run_result",
        "error_code": "runtime_protocol",
    }


def test_a_reinterrupted_resumed_turn_settles_the_record_interrupted(
    triage: _RecordingTriage,
) -> None:
    """The resumed turn itself positively ended by interruption: the receipt
    settles ``interrupted_turn`` and the record settles its truthful terminal
    ``interrupted`` outcome in the same composition — a newer Owner decision's
    episode governs and no Level 2 report is ever made (issues #342/#343)."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _resume_receipt_row(status="admitted"))
    _script_last_consumed_sequence(conn, None)
    _script_interrupt_intents(conn, [_interrupt_intent_row()])
    conn.on(
        _INTENT_ANNOTATE,
        _interrupt_intent_row(outcome={"observed_resolution": "interrupted_turn"}),
    )
    _script_resume_record_read(conn, _resume_record_row())
    _script_resume_record_outcome(
        conn, _resume_record_row(status="interrupted", outcome={"dispatch": "interrupted"})
    )
    _script_settle(conn, _resume_settled_row(SETTLEMENT_INTERRUPTED_TURN))
    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(interrupted=True),
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_INTERRUPTED_TURN
    record_params = next(
        params
        for sql, params in conn.executed
        if "update openorc.task_resume_intents" in " ".join(sql.split())
    )
    assert record_params is not None
    assert record_params[0] == "interrupted"
    assert triage.calls == []


def test_a_moved_on_resumed_subject_settles_result_not_applied_with_the_record_failed(
    triage: _RecordingTriage,
) -> None:
    """A validated result whose subject moved on settles the receipt
    ``result_not_applied`` and the record's truthful terminal ``failed``
    outcome with the not-applied provenance — the run ended, its result was
    never processed, and neither the receipt nor the record holds the
    session's continuation authorization open against newer episodes."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _resume_receipt_row())
    _script_attach(conn, _resume_receipt_row(status="admitted"))
    _script_staged(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    _script_resume_record_read(conn, _resume_record_row())
    _script_execution_read(conn, _execution_row())
    # The Task moved on from the restored implementing status: the owning
    # settlement's subject reproof returns None.
    _script_task_lock(conn, status="reviewing")
    _script_resume_record_outcome(
        conn, _resume_record_row(status="failed", outcome={"result": "result_not_applied"})
    )
    _script_settle(conn, _resume_settled_row(SETTLEMENT_RESULT_NOT_APPLIED))
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
    record_params = next(
        params
        for sql, params in conn.executed
        if "update openorc.task_resume_intents" in " ".join(sql.split())
    )
    assert record_params is not None
    assert record_params[0] == "failed"
    assert record_params[1].obj == {"result": "result_not_applied"}
    # A moved-on subject is a truthful not-applied settlement, never a
    # Level 2 operational failure.
    assert triage.calls == []


def test_a_resume_receipt_with_a_missing_record_is_left_unresolved(
    triage: _RecordingTriage,
) -> None:
    """The receipt's owner-control-intent record is the attribution root: a
    record that does not resolve in the addressed scope leaves the
    observation unresolved — never a fabricated match (fail closed)."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _resume_receipt_row())
    _script_attach(conn, _resume_receipt_row(status="admitted"))
    _script_staged(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    _script_resume_record_read(conn, None)
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert not any(
        "update openorc.task_resume_intents" in " ".join(sql.split()) for sql, _ in conn.executed
    )
    assert not any("set status = 'settled'" in sql for sql, _ in conn.executed)
    assert triage.calls == []


def test_a_resume_receipt_with_a_family_conflict_is_left_unresolved(
    triage: _RecordingTriage,
) -> None:
    """The record routing's original formal family is immutable authorization
    context: a receipt expecting a different family is an incoherent durable
    shape and the observation is left unresolved instead of guessing."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _resume_receipt_row())
    _script_attach(conn, _resume_receipt_row(status="admitted"))
    _script_staged(conn, _staged_row(candidate=_RESULT_CANDIDATE))
    _script_resume_record_read(
        conn,
        _resume_record_row(
            routing={
                "operation": "initial_plan_dispatch",
                "subject_kind": "runtime_send_claim",
                "subject_id": str(uuid4()),
                "expected_response_family": "plan_result",
                "restored_status": "planning",
                "authorized_state_token": str(_RESUME_STATE_TOKEN),
            }
        ),
    )
    _script_consume(conn)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )
    assert outcome.kind is TurnSettlementKind.LEFT_UNRESOLVED
    assert not any(
        "update openorc.task_resume_intents" in " ".join(sql.split()) for sql, _ in conn.executed
    )
    assert triage.calls == []


# --- the detached terminal cleanup (issue #344) ----------------------------------


def _cleanup_receipt_row(
    *, status: str = "admitted", outcome: dict[str, Any] | None = None
) -> tuple[Any, ...]:
    return _receipt_row(
        status=status,
        admitted_turn_reference=_TURN_REFERENCE if status in ("admitted", "settled") else None,
        outcome=outcome,
        operation="terminal_cleanup",
        original_attempt_id=_TASK_ID,
        original_attempt_kind="terminal_cleanup_claim",
        expected_family="workspace_result",
    )


def _cleanup_settled_row(label: str) -> tuple[Any, ...]:
    outcome: dict[str, Any] = {
        SETTLEMENT_OUTCOME_LABEL_KEY: label,
        SETTLEMENT_OUTCOME_SEQUENCE_KEY: 41,
    }
    return _cleanup_receipt_row(status="settled", outcome=outcome)


def _script_committed_cleanup_subject(conn: ScriptedConnection, *, settled: bool = False) -> None:
    """Script the terminalization-exception subject reproof read: a committed
    terminalization with a present, unsettled one-shot cleanup claim (the
    Task carries no ordinary currentness gate)."""
    conn.on(
        _sql_has("from openorc.tasks where id = %s for update"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status="waiting_for_owner",
            state_token=_STATE_TOKEN,
            source_requirements_fingerprint="a" * 64,
            terminalization_outcome="cancelled",
            terminalization_origin="owner_cancelled",
            terminalization_established_at=_OBSERVED,
            cleanup_send_claimed_at=_OBSERVED,
            cleanup_send_settled_at=_OBSERVED if settled else None,
        ),
    )


def _script_claim_settle(conn: ScriptedConnection) -> None:
    """Script the one-shot cleanup claim's inline settle (issue #345): the
    attributable routing settles the claim exactly once, atomically with the
    receipt's truthful settlement."""
    conn.on(
        _sql_has("update openorc.tasks", "set cleanup_send_settled_at = now()"),
        task_row(
            id=_TASK_ID,
            workspace_id=_WORKSPACE_ID,
            status="waiting_for_owner",
            state_token=_STATE_TOKEN,
            source_requirements_fingerprint="a" * 64,
            terminalization_outcome="cancelled",
            terminalization_origin="owner_cancelled",
            terminalization_established_at=_OBSERVED,
            cleanup_send_claimed_at=_OBSERVED,
            cleanup_send_settled_at=_OBSERVED,
        ),
    )


def test_a_validated_cleanup_result_settles_the_claim_and_receipt_once_without_archiving(
    triage: _RecordingTriage,
) -> None:
    """The observed CLEANUP_WORKSPACE completion validates against the
    canonical ``workspace_result`` family and settles the receipt truthfully
    exactly once — atomically with the one-shot claim's own settle (issue
    #345) — and NOTHING else: no Level 2 report and no archival from the
    routing itself. The settlement names the TERMINAL_CLEANUP handoff family
    so the ONE post-commit completion handoff routes the owning
    finalization's atomic archival."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _cleanup_receipt_row())
    _script_receipt_for_update(conn, _cleanup_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_committed_cleanup_subject(conn)
    _script_claim_settle(conn)
    _script_settle(conn, _cleanup_settled_row(SETTLEMENT_VALIDATED_RESULT))

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            marker=41,
            candidate=_workspace_result_candidate(operation="CLEANUP", status="COMPLETED"),
        ),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.operation.value == "terminal_cleanup"
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_VALIDATED_RESULT
    assert outcome.formal_result is not None
    assert outcome.handoff_operation is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP
    # The terminalization exception: the one-shot claim settles exactly once
    # with the receipt; the routing itself never archives and never reports
    # Level 2 machinery.
    assert triage.calls == []
    executed = [(" ".join(sql.split()), params) for sql, params in conn.executed]
    claim_settles = [params for sql, params in executed if "cleanup_send_settled_at = now()" in sql]
    assert claim_settles == [(_TASK_ID,)]
    assert not any("archived_at = now()" in sql for sql, _params in executed)


def test_a_moved_on_cleanup_subject_settles_result_not_applied(
    triage: _RecordingTriage,
) -> None:
    """A late observation whose one-shot claim window already closed (the
    owning disposition settled the claim) settles the receipt
    ``result_not_applied`` — it can never re-open cleanup, change the
    terminal truth, or undo an archival."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _cleanup_receipt_row())
    _script_receipt_for_update(conn, _cleanup_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_committed_cleanup_subject(conn, settled=True)
    _script_settle(conn, _cleanup_settled_row(SETTLEMENT_RESULT_NOT_APPLIED))

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            marker=41,
            candidate=_workspace_result_candidate(operation="CLEANUP", status="COMPLETED"),
        ),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_RESULT_NOT_APPLIED
    # A moved-on subject touches nothing: the claim window was already closed
    # by its owning disposition, so no claim settle ever executes.
    assert not any("cleanup_send_settled_at = now()" in sql for sql, _ in conn.executed)
    assert triage.calls == []


def test_an_observed_cleanup_failure_settles_truthfully_without_any_report(
    triage: _RecordingTriage,
) -> None:
    """A natively failed cleanup run settles the receipt ``observed_failure``
    atomically with the one-shot claim's own settle (issue #345) and stops:
    no Level 2 report, no disposition, no block, and no second send — the
    TERMINAL_CLEANUP handoff family routes the owning finalization's
    archival, which accepts the stale checkout without any redispatch."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _cleanup_receipt_row())
    _script_receipt_for_update(conn, _cleanup_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_committed_cleanup_subject(conn)
    _script_claim_settle(conn)
    _script_settle(conn, _cleanup_settled_row(SETTLEMENT_OBSERVED_FAILURE))

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            kind=DetachedTurnObservationKind.FAILED,
            marker=41,
        ),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.receipt is not None
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    assert outcome.handoff_operation is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP
    assert triage.calls == []
    assert any("cleanup_send_settled_at = now()" in sql for sql, _ in conn.executed)


def test_an_unusable_cleanup_result_settles_the_failure_and_never_re_sends(
    triage: _RecordingTriage,
) -> None:
    """A positively established unusable result (addressing another
    operation) settles the receipt truthfully as an observed failure with the
    one-shot claim's own settle (issue #345) — the same containment the
    blocking realization applied, never a re-send and never a disposition
    decision of its own; the handoff family routes the archival."""
    conn = ScriptedConnection()
    _script_binding_lookup(conn)
    _script_outstanding_receipt(conn, _cleanup_receipt_row())
    _script_receipt_for_update(conn, _cleanup_receipt_row())
    _script_last_consumed_sequence(conn, None)
    _script_committed_cleanup_subject(conn)
    _script_claim_settle(conn)
    _script_settle(conn, _cleanup_settled_row(SETTLEMENT_OBSERVED_FAILURE))

    outcome = settle_observed_turn_completion(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        classifier=_CLASSIFIER,
        evidence=_terminal_evidence(
            marker=41,
            candidate=_workspace_result_candidate(operation="PREPARE", status="COMPLETED"),
        ),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE
    assert outcome.handoff_operation is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP
    assert triage.calls == []
    assert any("cleanup_send_settled_at = now()" in sql for sql, _ in conn.executed)


def test_attach_consumes_a_staged_cleanup_result_through_the_new_routing(
    triage: _RecordingTriage,
) -> None:
    """An early CLEANUP_WORKSPACE completion staged during the admission race
    is consumed exactly once by the acknowledgement attach and routed through
    the terminal-cleanup settlement — the receipt and the one-shot claim
    settle atomically there (issue #345) and the handoff family is named, so
    the dispatching invocation's post-commit handoff owns the archival."""
    conn = ScriptedConnection()
    _script_receipt_for_update(conn, _cleanup_receipt_row(status="claimed"))
    _script_attach(conn, _cleanup_receipt_row())
    _script_staged(conn, _staged_row(candidate=_workspace_result_candidate("CLEANUP")))
    _script_committed_cleanup_subject(conn)
    _script_claim_settle(conn)
    _script_settle(conn, _cleanup_settled_row(SETTLEMENT_VALIDATED_RESULT))
    _script_consume(conn)

    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        admitted_turn_reference=_TURN_REFERENCE,
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    assert outcome.receipt.status.value == "settled"
    assert outcome.handoff_operation is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP
    assert triage.calls == []
    assert any("cleanup_send_settled_at = now()" in sql for sql, _ in conn.executed)
