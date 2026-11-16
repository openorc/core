"""The detached-turn boundary tests (issue #348).

The runtime-neutral proof of the remediation: a fake detached runtime and a
fake observation source — neither importing nor shaping anything Cline —
drive the same admission acknowledgement, terminal settlement, start-identity
corroboration, and status-only consultation the Cline integration serves,
and a source scan proves no Cline import or event/status/field vocabulary
crosses into the shared detached-turn contract, settlement policy,
observation host, or workflow decision services. The original one-shot claim
and the existing workflow finalizer remain authoritative throughout.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
from openorc.domain.detached_turns import (
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
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
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.services.task_detached_turn_correlation import (
    TurnSettlementKind,
    TurnStartCorroboration,
    TurnStatusClassificationKind,
    attach_acknowledged_turn,
    check_turn_start_identity,
    classify_acknowledged_turn_status,
    record_task_detached_turn_receipt,
    settle_observed_turn_completion,
)
from tests.test_services_task_detached_turn_correlation import (  # noqa: I001
    _CONNECTION_ID,
    _EXECUTION_ID,
    _EXTERNAL_SESSION,
    _RECEIPT_ID,
    _RESULT_CANDIDATE,
    _SESSION_ID,
    _TASK_ID,
    _TURN_REFERENCE,
    _WORKSPACE_ID,
    ScriptedConnection,
    WorkflowEventType,
    _admitted_row,
    _execution_row,
    _pool,
    _receipt_row,
    _script_attach,
    _script_binding_lookup,
    _script_complete_execution,
    _script_event_insert,
    _script_execution_lock,
    _script_execution_read,
    _script_implementation_subject,
    _script_last_consumed_sequence,
    _script_outstanding_receipt,
    _script_receipt_for_update,
    _script_settle,
    _script_staged,
    _script_task_lock,
    _settled_row,
    _sql_has,
)

#: The shared detached-turn modules whose import surface must stay free of
#: the concrete runtime integration. The composition roots (the API factory)
#: and the deliberately Cline-specific service modules are the sanctioned
#: selectors and are not on this list.
_SHARED_BOUNDARY_MODULES = (
    "src/openorc/domain/detached_turns.py",
    "src/openorc/adapters/agent_runtime/contract.py",
    "src/openorc/adapters/agent_runtime/detached.py",
    "src/openorc/adapters/agent_runtime/results.py",
    "src/openorc/adapters/agent_runtime/errors.py",
    "src/openorc/services/task_completion_handoff.py",
    "src/openorc/services/task_detached_turn_correlation.py",
    "src/openorc/services/task_detached_turn_observation.py",
    "src/openorc/services/session_runtime.py",
    "src/openorc/services/task_planning_bootstrap.py",
    "src/openorc/services/task_pending_runtime_interactions.py",
    "src/openorc/services/task_authorized_implementation_execution.py",
    "src/openorc/workers/invocation.py",
    "src/openorc/workers/task_workflow.py",
)

#: The runtime event/status/field vocabulary that must never appear in the
#: shared detached-turn modules. The persistence layer's documented storage
#: encoding mapping is the one deliberate exception and is scanned separately.
_BANNED_VOCABULARY = (
    "run.completed",
    "run.failed",
    "run.started",
    "run.list",
    "requestId",
    "runId",
)


class _BoundaryRuntime:
    """A runtime-neutral fake detached runtime: nothing Cline-shaped."""

    def __init__(self) -> None:
        self.admissions: list[tuple[str, str]] = []

    def allocate_detached_session(self, request: Any) -> Any:
        raise AssertionError("the boundary flow admits on an already-allocated session")

    def admit_detached_turn(self, session_id: str, content: str) -> AgentTurnAdmission:
        self.admissions.append((session_id, content))
        return AgentTurnAdmission(turn_reference="boundary-turn-7")


class _BoundaryEvidence(NativeTurnEvidence):
    """The fake integration's own invented evidence carrier."""

    __slots__ = ("label",)

    def __init__(self, label: str) -> None:
        self.label = label

    def __repr__(self) -> str:
        return "_BoundaryEvidence(<withheld>)"


class _BoundarySource:
    """A runtime-neutral fake observation source and classifier."""

    def classify_turn_start(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnAttribution:
        assert isinstance(evidence, _BoundaryEvidence)
        if evidence.label == "matching":
            return TurnAttribution.ATTRIBUTED
        if evidence.label == "foreign":
            return TurnAttribution.CONTRADICTED
        return TurnAttribution.UNATTRIBUTABLE

    def classify_turn_terminal(
        self,
        evidence: NativeTurnEvidence,
        *,
        consumed_ordering_markers: Sequence[int],
    ) -> TurnTerminalClassification:
        assert isinstance(evidence, _BoundaryEvidence)
        # The fake's own ordering rule: markers are small integers and a
        # marker not newer than a consumed one is a replay.
        marker = int(evidence.label) if evidence.label.isdigit() else None
        if marker is None:
            freshness = TurnFreshness.UNORDERED
        elif any(consumed >= marker for consumed in consumed_ordering_markers):
            freshness = TurnFreshness.STALE
        else:
            freshness = TurnFreshness.FRESH
        return TurnTerminalClassification(
            freshness=freshness,
            outcome=TurnTerminalOutcome(
                kind=DetachedTurnObservationKind.COMPLETED,
                result_candidate=_RESULT_CANDIDATE,
            ),
            ordering_marker=marker,
        )

    def classify_turn_statuses(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnStatusEvidenceVerdict:
        assert isinstance(evidence, _BoundaryEvidence)
        return TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE


def _terminal_evidence(label: str) -> ObservedTurnEvidence:
    return ObservedTurnEvidence(
        external_session_id=_EXTERNAL_SESSION,
        family=TurnEvidenceFamily.TURN_TERMINAL,
        native_evidence=_BoundaryEvidence(label),
    )


def _start_evidence(label: str) -> ObservedTurnEvidence:
    return ObservedTurnEvidence(
        external_session_id=_EXTERNAL_SESSION,
        family=TurnEvidenceFamily.TURN_START,
        native_evidence=_BoundaryEvidence(label),
    )


def test_the_shared_detached_turn_modules_import_no_runtime_integration() -> None:
    for relative in _SHARED_BOUNDARY_MODULES:
        tree = ast.parse((Path(__file__).parents[1] / relative).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                assert not name.startswith("openorc.adapters.cline"), (
                    f"{relative} imports the concrete runtime integration: {name}"
                )


def test_the_shared_detached_turn_modules_carry_no_runtime_vocabulary() -> None:
    for relative in _SHARED_BOUNDARY_MODULES:
        source = (Path(__file__).parents[1] / relative).read_text()
        for token in _BANNED_VOCABULARY:
            assert token not in source, f"{relative} carries the runtime vocabulary {token!r}"


def test_a_runtime_neutral_fake_drives_the_same_admission_and_settlement_path() -> None:
    runtime = _BoundaryRuntime()
    conn = ScriptedConnection()
    conn.on(
        _sql_has("insert into openorc.task_hub_run_receipts"),
        _receipt_row(),
    )

    receipt = record_task_detached_turn_receipt(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=_EXECUTION_ID,
    )
    assert receipt.status is TaskDetachedTurnReceiptStatus.CLAIMED
    admission = runtime.admit_detached_turn(_EXTERNAL_SESSION, "the composed canonical content")
    assert runtime.admissions == [(_EXTERNAL_SESSION, "the composed canonical content")]

    _script_receipt_for_update(conn, _receipt_row())
    _script_attach(conn, _admitted_row())
    _script_staged(conn, None)
    outcome = attach_acknowledged_turn(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=receipt.id,
        admitted_turn_reference=admission.turn_reference,
    )
    assert outcome.kind is TurnSettlementKind.ADMITTED
    attach_params = next(
        params
        for sql, params in conn.executed
        if "set status = 'admitted'" in " ".join(sql.split())
    )
    # The fake runtime's own opaque turn reference is exactly what the
    # durable attach persisted.
    assert attach_params is not None and attach_params[0] == "boundary-turn-7"
    assert outcome.receipt is not None
    assert outcome.receipt.admitted_turn_reference == _TURN_REFERENCE


def test_a_runtime_neutral_fake_source_settles_through_the_same_seam() -> None:
    source = _BoundarySource()
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
    _script_settle(conn, _settled_row(SETTLEMENT_VALIDATED_RESULT, marker=41))

    outcome = settle_observed_turn_completion(
        _pool(conn),
        classifier=source,
        workspace_id=_WORKSPACE_ID,
        connection_id=_CONNECTION_ID,
        evidence=_terminal_evidence("41"),
    )

    assert outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
    assert outcome.receipt is not None
    # The consumed ordering marker is the fake runtime's own token, recorded
    # under OpenOrc's stable durable encoding.
    assert outcome.receipt.outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] == 41
    # The evidence carrier is the fake's own invention, never a Cline type.
    assert (
        type(_terminal_evidence("41").native_evidence).__module__.startswith(
            "openorc.adapters.cline"
        )
        is False
    )


def test_a_runtime_neutral_fake_source_classifies_contradiction_and_status() -> None:
    source = _BoundarySource()

    # A contradictory start classification persists the receipt's set-once
    # contradiction marker through the shared durable machinery.
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
            classifier=source,
            workspace_id=_WORKSPACE_ID,
            connection_id=_CONNECTION_ID,
            evidence=_start_evidence("foreign"),
        )
        is TurnStartCorroboration.CONTRADICTED
    )
    assert any("set contradicted_at = now()" in sql for sql, _ in conn.executed)

    # A status-only consultation with no classified failure stays honestly
    # unresolved; nothing is fabricated from absence.
    conn = ScriptedConnection()
    conn.on(
        _sql_has("from openorc.task_hub_run_receipts", "where id = %s"),
        _admitted_row(),
    )
    classification = classify_acknowledged_turn_status(
        _pool(conn),
        classifier=source,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt_id=_RECEIPT_ID,
        status_evidence=_BoundaryEvidence("status"),
    )
    assert classification.kind is TurnStatusClassificationKind.UNRESOLVED


def test_the_storage_encoding_mapping_is_the_documented_persistence_exception() -> None:
    source = (Path(__file__).parents[1] / "src/openorc/persistence/detached_turns.py").read_text()
    # The durable kind tokens are OpenOrc's own stable storage encoding,
    # mapped at the persistence boundary; no other banned vocabulary appears.
    assert '"run.completed"' in source and '"run.failed"' in source
    for token in ("run.started", "run.list", "requestId", "runId"):
        assert token not in source, f"the persistence layer carries {token!r}"
