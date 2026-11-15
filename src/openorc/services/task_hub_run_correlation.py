"""The detached Hub-run correlation/settlement seam (issue #322).

The runtime-neutral application-service seam between OpenOrc's authorized
one-shot claims and the native Hub runs that execute them. The Hub ``runId``
is execution correlation evidence, never OpenOrc workflow authority: the
receipt adds no second claim identity, decides no workflow meaning, and the
workflow consequences always route through the existing owning services.

Dispatch side — the owning Level 1 service commits its existing one-shot
claim and the receipt companion in one short transaction (before any
enqueue), then acknowledges the admission: a positively identified Hub
refusal is a proven pre-effect failure (the receipt records ``refused`` so a
never-dispatched run cannot hold the session's slot), an acknowledged
admission attaches the exact native ``runId`` (CAS, exactly once), and an
uncertain admission stays conservatively claimed and is never redispatched.

Settlement side — an independently observed terminal completion is resolved
within its authenticated Connection/Workspace scope against the
authoritative ``TaskAgentSession`` binding (external identifiers are never a
cross-Connection match), then attributed by the serial correlation rule: at
most one unsettled OpenOrc-dispatched Hub run exists per exact binding (the
durable partial unique index is the backstop), so a fresh terminal event for
that session attributes to the outstanding acknowledged run. An observed
``run.started`` ``requestId`` equal to the acknowledged ``runId``
corroborates the binding but is never required; an explicitly observed
contradictory identity or another concrete attribution conflict leaves the
event unresolved rather than inventing a match.

Admission race — a terminal event that resolves to a receipt still
``claimed`` (acknowledgement not yet persisted) is durably staged on that
exact receipt and consumed exactly once by the acknowledgement attach, which
then routes it through the ordinary settlement: the early result is never
lost and an unidentified run is never prematurely settled. This is the
smallest safe handoff — no general event ledger, no replay machinery, no
polling, and no guaranteed crash recovery (a dispatch worker that dies
before attaching leaves the accepted v1 unresolved state).

Settlement routing — a successfully attributed completion must carry a
genuine native ``AgentResult``, pass the shared formal contract for the
receipt's expected family, and undergo the current-subject reproof before
the workflow transition; validated results, genuine observed failures, and
protocol failures route through the existing owning continuation/failure
services (the shared execution finalize, the truthful ``failed_final``
finalization with the ``observed_run_failure`` audit kind, and the Level 2
operation-failure report with ``KNOWN_FAILED`` evidence — never a
safe-retry classification). Settlement is atomic (one composition) and
idempotent (forward-only conditional writes; duplicates and stale
notifications are rejected no-ops via the receipt lifecycle and the consumed
sequence marker). A completion whose candidate is missing or fails the
shared contract leaves the receipt conservatively admitted and the attempt
unresolved — exactly the existing post-effect discipline.

Status-only reconciliation — at existing bounded reconciliation points, the
exact acknowledged ``runId`` reported ``failed`` by ``run.list`` classifies
through the same typed operation-failure path; status-only ``completed`` is
never success/result evidence.

v1 scope note: only the execution families (E6 implementation and E9
PR-remediation) carry settlement routing here; the remaining operation
families gain theirs with their own detached migrations (#325), and routing
refuses them fail-closed until then.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.domain.events import WorkflowEventActor
from openorc.domain.hub_runs import (
    SETTLEMENT_OBSERVED_FAILURE,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
    SETTLEMENT_RESULT_NOT_APPLIED,
    SETTLEMENT_VALIDATED_RESULT,
    HubRunObservationKind,
    ObservedHubRunCompletion,
    ObservedHubRunStatus,
    TaskHubRunReceipt,
    TaskHubRunReceiptStatus,
)
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    canonical_failure_subject,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import executions as execution_records
from openorc.persistence import hub_runs as hub_run_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.protocol.errors import ProtocolError
from openorc.protocol.models import (
    FORMAL_RESPONSE_FAMILIES,
    IMPLEMENTATION_RESULT_FAMILY,
    FormalResponse,
    ImplementationResult,
)
from openorc.protocol.parsing import parse_formal_response
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_authorized_implementation_execution import (
    IMPLEMENTATION_SETTLEMENT_SUBJECT,
    OBSERVED_RUN_FAILURE_KIND,
    REMEDIATION_SETTLEMENT_SUBJECT,
    ObservedExecutionSettlementSubject,
    finalize_observed_implementation_failure,
    settle_observed_implementation_result,
)
from openorc.services.task_operational_failure_triage import (
    OperationalFailureOperation,
    OperationalFailureReport,
    triage_operational_failure,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "HubRunSettlementKind",
    "HubRunSettlementOutcome",
    "HubRunStartCorroboration",
    "HubRunStatusClassification",
    "HubRunStatusClassificationKind",
    "attach_acknowledged_hub_run",
    "check_hub_run_start_identity",
    "classify_acknowledged_hub_run_status",
    "record_task_hub_run_receipt",
    "settle_observed_hub_run_completion",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_hub_run_correlation"
_ATTACH_SPAN_NAME: Final[str] = "task_hub_run_correlation.attach_acknowledgement"
_SETTLE_SPAN_NAME: Final[str] = "task_hub_run_correlation.settle_completion"
_CLASSIFY_SPAN_NAME: Final[str] = "task_hub_run_correlation.classify_status"

# The execution families with detached settlement routing in this seam; the
# remaining operation families gain theirs with their own detached migrations
# (#325) and are refused fail-closed until then.
_EXECUTION_SETTLEMENT_FAMILIES: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = (
    frozenset(
        {
            PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
        }
    )
)

# The family contracts the routing proves: the family's settlement subject
# (the owning services' typed reproof contract) and its exact expected
# formal family.
_EXECUTION_FAMILY_SUBJECTS: Final[
    Mapping[PendingTaskRuntimeInteractionOperation, ObservedExecutionSettlementSubject]
] = {
    PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION: (
        IMPLEMENTATION_SETTLEMENT_SUBJECT
    ),
    PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION: (
        REMEDIATION_SETTLEMENT_SUBJECT
    ),
}

# The Level 2 report's expected Task status per execution family, mirroring
# the owning dispatch services' subject contracts.
_FAMILY_EXPECTED_TASK_STATUS: Final[Mapping[PendingTaskRuntimeInteractionOperation, TaskStatus]] = {
    PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION: TaskStatus.IMPLEMENTING,
    PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION: TaskStatus.REVIEWING,
}

# The Hub's own status token that positively classifies a failed run.
_RUN_LIST_FAILED_STATUS: Final[str] = "failed"

# The openorc actor of the settlement's Level 2 report: the classification is
# OpenOrc's own typed observation, never a runtime- or Owner-attributed
# decision.
_OPENORC_ACTOR: Final[event_coordination.WorkflowActorContext] = (
    event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)
)


class HubRunSettlementKind(StrEnum):
    """The typed classification of one detached-run seam operation."""

    ADMITTED = "admitted"
    NO_OUTSTANDING_RUN = "no_outstanding_run"
    STAGED_PENDING_ACKNOWLEDGEMENT = "staged_pending_acknowledgement"
    SETTLED_VALIDATED_RESULT = "settled_validated_result"
    SETTLED_OBSERVED_FAILURE = "settled_observed_failure"
    SETTLED_RESULT_NOT_APPLIED = "settled_result_not_applied"
    LEFT_UNRESOLVED = "left_unresolved"
    REJECTED_DUPLICATE = "rejected_duplicate"


@dataclass(frozen=True, slots=True)
class HubRunSettlementOutcome:
    """The typed outcome of one acknowledgement attach or completion
    settlement.

    ``formal_result`` carries the parsed formal response exactly when a
    validated result completed the attempt; ``disposition`` carries the
    Level 2 disposition exactly when an observed failure was reported.
    """

    kind: HubRunSettlementKind
    receipt: TaskHubRunReceipt | None
    formal_result: FormalResponse | None = None
    disposition: object | None = None


class HubRunStatusClassificationKind(StrEnum):
    """The typed classification of one status-only reconciliation."""

    CLASSIFIED_FAILURE = "classified_failure"
    RESULT_NOT_APPLIED = "result_not_applied"
    NEVER_SUCCESS_EVIDENCE = "never_success_evidence"
    UNRESOLVED = "unresolved"
    NOT_CLASSIFIABLE = "not_classifiable"


@dataclass(frozen=True, slots=True)
class HubRunStatusClassification:
    """The typed outcome of one status-only reconciliation classification."""

    kind: HubRunStatusClassificationKind
    receipt: TaskHubRunReceipt | None
    disposition: object | None = None


class HubRunStartCorroboration(StrEnum):
    """The read-only corroboration classification of one observed run start.

    ``CORROBORATED``: the observed ``requestId`` equals the outstanding
    acknowledged run identity. ``CONTRADICTED``: an explicitly observed,
    concretely different run identity — recorded by selective logging, never
    mutated against, and never a settlement match. ``UNATTRIBUTABLE``: the
    receipt is still claimed (no acknowledged identity yet) or the start
    carried no request identity. ``UNOWNED``: no OpenOrc-dispatched run is
    outstanding for the resolved binding.
    """

    CORROBORATED = "corroborated"
    CONTRADICTED = "contradicted"
    UNATTRIBUTABLE = "unattributable"
    UNOWNED = "unowned"


# --- command-shape validation -------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_nonblank_command(value: object, name: str) -> str:
    """Reject a malformed non-empty-string command argument."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidCommandError(f"{name} must be a non-empty string")
    return value


def _settlement_outcome(label: str, sequence: int | None) -> dict[str, object]:
    """The canonical settlement outcome payload of one consumed observation."""
    outcome: dict[str, object] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if sequence is not None:
        # The durable consumed-sequence marker: the settlement evidence later
        # reconciliation reads to reject already-consumed notifications.
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = sequence
    return outcome


# --- the dispatch-side receipt companion ---------------------------------------


def record_task_hub_run_receipt(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    operation: PendingTaskRuntimeInteractionOperation,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
    expected_formal_family: str | None = None,
) -> TaskHubRunReceipt:
    """Record the claim-to-run receipt companion of one committed one-shot claim.

    Composed inside the owning Level 1 service's claim transaction (the
    caller's ``expected_state_token`` currentness revalidation owns the
    authorization; this helper adds no second claim identity): the receipt
    commits before any enqueue, so the admission race and the
    single-outstanding-run invariant are durable from the first possible
    effect. The execution families pin their exact expected formal family;
    every other family must name one explicitly until its own detached
    migration wires its routing. A violated single-outstanding-run invariant
    (a concurrent second dispatch for the same session binding) is a typed
    conflict — fail closed, never a second outstanding run.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(task_agent_session_id, "task_agent_session_id")
    _require_uuid_command(original_attempt_id, "original_attempt_id")
    if not isinstance(operation, PendingTaskRuntimeInteractionOperation):
        raise InvalidCommandError("operation must be a PendingTaskRuntimeInteractionOperation")
    if not isinstance(original_attempt_kind, OriginalAttemptKind):
        raise InvalidCommandError("original_attempt_kind must be an OriginalAttemptKind")
    if operation in _EXECUTION_SETTLEMENT_FAMILIES:
        if original_attempt_kind is not OriginalAttemptKind.EXECUTION:
            raise InvalidCommandError(
                "an execution-family hub-run receipt roots at the family's Execution identity"
            )
        if expected_formal_family is not None and (
            expected_formal_family != IMPLEMENTATION_RESULT_FAMILY
        ):
            raise InvalidCommandError(
                "an execution-family hub-run receipt expects the implementation_result family"
            )
        expected_formal_family = IMPLEMENTATION_RESULT_FAMILY
    elif expected_formal_family is None or expected_formal_family not in FORMAL_RESPONSE_FAMILIES:
        raise InvalidCommandError(
            "the receipt requires its expected formal family to name a canonical v1 response family"
        )
    try:
        return hub_run_records.create_task_hub_run_receipt(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=task_agent_session_id,
            operation=operation,
            original_attempt_kind=original_attempt_kind,
            original_attempt_id=original_attempt_id,
            expected_formal_family=expected_formal_family,
        )
    except UniqueViolation as error:
        raise ConflictError(
            "the session binding already carries an outstanding hub-run receipt; "
            "the dispatch is refused"
        ) from error


# --- the acknowledgement attach ------------------------------------------------


def attach_acknowledged_hub_run(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt_id: UUID,
    run_id: str,
) -> HubRunSettlementOutcome:
    """Attach one acknowledged native runId to its claimed receipt.

    The post-enqueue acknowledgement of the dispatch invocation: the receipt
    must still be claimed (a lost/uncertain acknowledgement is a typed
    uncertainty at the adapter boundary and never reaches this attach; a
    positively identified refusal is recorded through the owning pre-effect
    failure path before this attach is ever called). A staged early
    completion — a terminal event that arrived during the admission race —
    is consumed exactly once inside the same composition and routed through
    the ordinary settlement, so the early result settles with its now-known
    run identity instead of being lost. The composition is one short
    transaction; a failure rolls the attach and the routing back together.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(receipt_id, "receipt_id")
    _require_nonblank_command(run_id, "run_id")
    with application_span(_SERVICE_TRACER_SCOPE, _ATTACH_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ATTACH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            receipt = hub_run_records.get_task_hub_run_receipt_for_update(
                transaction_pool, receipt_id=receipt_id
            )
            if (
                receipt is None
                or receipt.workspace_id != workspace_id
                or receipt.task_id != task_id
            ):
                raise NotFoundError("the addressed hub-run receipt is not available in this task")
            if receipt.status is not TaskHubRunReceiptStatus.CLAIMED:
                raise StaleOperationError(
                    "the hub-run receipt is no longer claimed; the acknowledgement is stale"
                )
            attached = hub_run_records.attach_task_hub_run_acknowledgement(
                transaction_pool,
                receipt_id=receipt.id,
                task_id=task_id,
                workspace_id=workspace_id,
                run_id=run_id,
            )
            if attached is None:  # pragma: no cover - locked claimed receipt must attach
                raise StaleOperationError(
                    "the hub-run receipt could not be acknowledged; the attach is stale"
                )
            staged = hub_run_records.get_task_hub_run_early_completion_for_update(
                transaction_pool, receipt_id=receipt.id
            )
            if staged is None:
                return HubRunSettlementOutcome(kind=HubRunSettlementKind.ADMITTED, receipt=attached)
            outcome = _route_observed_terminal(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                receipt=attached,
                kind=staged.kind,
                sequence=staged.sequence,
                result_candidate=staged.result_candidate,
            )
            hub_run_records.consume_task_hub_run_early_completion(
                transaction_pool, completion_id=staged.id, receipt_id=receipt.id
            )
            return outcome


# --- the settlement routing ----------------------------------------------------


def _route_observed_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskHubRunReceipt,
    kind: HubRunObservationKind,
    sequence: int | None,
    result_candidate: str | None,
) -> HubRunSettlementOutcome:
    """Route one attributed terminal observation through its owning services.

    Called inside the caller's composition with the receipt row locked. The
    receipt's operation family selects the routing; a family without
    detached settlement routing yet is a fail-closed conflict (its own
    detached migration wires it), never a silent no-op.
    """
    if receipt.operation in _EXECUTION_SETTLEMENT_FAMILIES:
        return _route_execution_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            kind=kind,
            sequence=sequence,
            result_candidate=result_candidate,
        )
    raise ConflictError(
        "the receipt's operation family has no detached settlement routing yet; "
        "the observation is left unresolved"
    )


def _route_execution_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskHubRunReceipt,
    kind: HubRunObservationKind,
    sequence: int | None,
    result_candidate: str | None,
) -> HubRunSettlementOutcome:
    """Route one attributed terminal observation onto the execution attempt.

    The attribution congruence is proved first: the attempt must exist in
    the addressed scope and be anchored to exactly the receipt's session
    binding. A concrete attribution conflict leaves the observation
    unresolved — never a fabricated match. A terminal failure routes
    through the truthful failure finalization and the Level 2 report; a
    completion funnels the candidate through the shared formal contract and
    completes the still-running attempt through the shared finalize, or
    leaves the receipt conservatively admitted when the candidate is
    missing or fails the contract (the existing post-effect discipline).
    """
    execution = execution_records.get_execution(
        transaction_pool, execution_id=receipt.original_attempt_id
    )
    if (
        execution is None
        or execution.workspace_id != workspace_id
        or execution.task_id != task_id
        or execution.producer_session_id != receipt.task_agent_session_id
    ):
        logger.warning(
            "the observed hub-run completion conflicts with the receipt's attempt "
            "binding; the observation is left unresolved"
        )
        return HubRunSettlementOutcome(kind=HubRunSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if kind is HubRunObservationKind.RUN_FAILED:
        return _settle_execution_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            sequence=sequence,
        )
    if result_candidate is None:
        logger.warning(
            "the observed hub-run completion carries no native result; the attempt "
            "remains unresolved and the receipt stays conservatively admitted"
        )
        return HubRunSettlementOutcome(kind=HubRunSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    try:
        parsed = parse_formal_response(
            result_candidate, expected_family=receipt.expected_formal_family
        )
    except ProtocolError:
        logger.warning(
            "the observed hub-run completion failed the shared formal contract; "
            "the attempt remains unresolved and the receipt stays conservatively admitted"
        )
        return HubRunSettlementOutcome(kind=HubRunSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if not isinstance(parsed, ImplementationResult):
        # The family pairing is enforced at record time; this is the
        # fail-closed backstop, never a reinterpretation.
        logger.warning(
            "the observed hub-run completion was not the expected formal result "
            "family; the attempt remains unresolved"
        )
        return HubRunSettlementOutcome(kind=HubRunSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    finalized = settle_observed_implementation_result(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        execution_id=receipt.original_attempt_id,
        subject=_EXECUTION_FAMILY_SUBJECTS[receipt.operation],
        result=parsed,
    )
    if finalized is None:
        settled = hub_run_records.settle_task_hub_run_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, sequence),
        )
        return HubRunSettlementOutcome(
            kind=HubRunSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = hub_run_records.settle_task_hub_run_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, sequence),
    )
    return HubRunSettlementOutcome(
        kind=HubRunSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


def _settle_execution_failure(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskHubRunReceipt,
    sequence: int | None,
) -> HubRunSettlementOutcome:
    """Settle one positively observed genuine run failure.

    The still-running attempt finalizes ``failed_final`` with the truthful
    ``observed_run_failure`` audit atomically with the receipt's settlement,
    and the failure reports through the existing typed operation-failure
    path (Level 2): ``KNOWN_FAILED`` evidence is never a safe-retry
    classification, so the disposition is the policy's own bounded decision
    (escalation, no automatic redispatch). The report is a typed no-op when
    it no longer addresses current work. An attempt already resolved by
    another authority records only the truthful ``result_not_applied``
    settlement — no failure is fabricated against resolved history.
    """
    finalized = finalize_observed_implementation_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        execution_id=receipt.original_attempt_id,
        subject=_EXECUTION_FAMILY_SUBJECTS[receipt.operation],
        failure_kind=OBSERVED_RUN_FAILURE_KIND,
    )
    if finalized is None:
        settled = hub_run_records.settle_task_hub_run_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, sequence),
        )
        return HubRunSettlementOutcome(
            kind=HubRunSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = hub_run_records.settle_task_hub_run_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, sequence),
    )
    current = task_records.get_task(transaction_pool, task_id=task_id)
    if current is None or current.workspace_id != workspace_id:
        # Unreachable after the finalize's own locked reproof; kept explicit
        # for fail-closed safety.
        raise NotFoundError("the requested task is not available in this workspace")
    operation = OperationalFailureOperation(
        family=receipt.operation,
        original_attempt_kind=receipt.original_attempt_kind,
        original_attempt_id=receipt.original_attempt_id,
        subject=canonical_failure_subject(
            {
                "execution_id": str(receipt.original_attempt_id),
                "task_agent_session_id": str(receipt.task_agent_session_id),
            }
        ),
    )
    report = OperationalFailureReport(
        reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
        reporting_attempt_id=receipt.original_attempt_id,
        error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
        effect_evidence=FailureEffectEvidence.KNOWN_FAILED,
        diagnostic_context={"observation": "hub_run_failure"},
    )
    disposition = triage_operational_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=current.state_token,
        expected_status=_FAMILY_EXPECTED_TASK_STATUS[receipt.operation],
        expected_current_owner_gate_id=None,
        actor=_OPENORC_ACTOR,
        operation=operation,
        report=report,
    )
    return HubRunSettlementOutcome(
        kind=HubRunSettlementKind.SETTLED_OBSERVED_FAILURE,
        receipt=settled,
        disposition=disposition,
    )


# --- the public settlement and reconciliation entries ---------------------------


def settle_observed_hub_run_completion(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    connection_id: UUID,
    observation: ObservedHubRunCompletion,
) -> HubRunSettlementOutcome:
    """Settle one independently observed terminal Hub-run completion.

    The observation resolves within its authenticated Connection/Workspace
    scope against the authoritative session binding — external identifiers
    are never a cross-Connection match — and then attributes by the serial
    correlation rule: the single outstanding receipt of that exact binding.
    A claimed receipt (acknowledgement not yet persisted) stages the
    observation for its acknowledgement attach; an admitted receipt routes
    the observation through the owning services and settles; no outstanding
    receipt makes the event unowned, and a second early delivery for the
    same receipt is a rejected duplicate. Nothing here ever invents a match
    or treats an unattributable event as a workflow fact.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(connection_id, "connection_id")
    if not isinstance(observation, ObservedHubRunCompletion):
        raise InvalidCommandError("observation must be an ObservedHubRunCompletion")
    with application_span(_SERVICE_TRACER_SCOPE, _SETTLE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_SETTLE_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        binding = session_records.find_task_agent_session_binding(
            pool,
            workspace_id=workspace_id,
            connection_id=connection_id,
            external_session_id=observation.external_session_id,
        )
        if binding is None:
            logger.info(
                "the observed hub-run completion resolves to no session binding in "
                "this connection scope; the observation is unowned"
            )
            return HubRunSettlementOutcome(
                kind=HubRunSettlementKind.NO_OUTSTANDING_RUN, receipt=None
            )
        with composed_transaction(pool) as transaction_pool:
            receipt = hub_run_records.get_outstanding_task_hub_run_receipt_for_update(
                transaction_pool, task_agent_session_id=binding.id
            )
            if receipt is None or receipt.workspace_id != workspace_id:
                logger.info(
                    "the observed hub-run completion has no outstanding run for its "
                    "session binding; the observation is unowned"
                )
                return HubRunSettlementOutcome(
                    kind=HubRunSettlementKind.NO_OUTSTANDING_RUN, receipt=None
                )
            if receipt.status is TaskHubRunReceiptStatus.CLAIMED:
                try:
                    hub_run_records.stage_task_hub_run_early_completion(
                        transaction_pool,
                        receipt_id=receipt.id,
                        task_id=receipt.task_id,
                        workspace_id=receipt.workspace_id,
                        kind=observation.kind,
                        sequence=observation.sequence,
                        result_candidate=observation.result_candidate,
                    )
                except UniqueViolation:
                    logger.info(
                        "a second early completion for the same outstanding receipt "
                        "is a rejected duplicate"
                    )
                    return HubRunSettlementOutcome(
                        kind=HubRunSettlementKind.REJECTED_DUPLICATE, receipt=receipt
                    )
                return HubRunSettlementOutcome(
                    kind=HubRunSettlementKind.STAGED_PENDING_ACKNOWLEDGEMENT,
                    receipt=receipt,
                )
            return _route_observed_terminal(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=receipt.task_id,
                receipt=receipt,
                kind=observation.kind,
                sequence=observation.sequence,
                result_candidate=observation.result_candidate,
            )


def classify_acknowledged_hub_run_status(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt_id: UUID,
    observed_statuses: Sequence[ObservedHubRunStatus],
) -> HubRunStatusClassification:
    """Classify one status-only ``run.list`` reconciliation observation.

    At existing bounded reconciliation points: the exact acknowledged
    ``runId`` reported ``failed`` classifies through the existing typed
    operation-failure path (``KNOWN_FAILED`` — the run executed and failed,
    never a safe-retry classification); status-only ``completed`` is never
    success/result evidence and applies nothing; a missing or ambiguous
    record for the acknowledged identity is the honest unresolved state.
    Only an admitted receipt carries an identity to classify.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(receipt_id, "receipt_id")
    for observed in observed_statuses:
        if not isinstance(observed, ObservedHubRunStatus):
            raise InvalidCommandError("observed_statuses must carry ObservedHubRunStatus values")
    with application_span(_SERVICE_TRACER_SCOPE, _CLASSIFY_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_CLASSIFY_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        receipt = hub_run_records.get_task_hub_run_receipt(pool, receipt_id=receipt_id)
        if (
            receipt is None
            or receipt.workspace_id != workspace_id
            or receipt.task_id != task_id
            or receipt.status is not TaskHubRunReceiptStatus.ADMITTED
        ):
            return HubRunStatusClassification(
                kind=HubRunStatusClassificationKind.NOT_CLASSIFIABLE, receipt=receipt
            )
        assert receipt.run_id is not None  # narrowed by the admitted coherence
        matches = [observed for observed in observed_statuses if observed.run_id == receipt.run_id]
        if len(matches) != 1:
            # Missing record or an ambiguous duplicate identity: honest
            # unknown, never a classification guess.
            return HubRunStatusClassification(
                kind=HubRunStatusClassificationKind.UNRESOLVED, receipt=receipt
            )
        if matches[0].status != _RUN_LIST_FAILED_STATUS:
            if matches[0].status == "completed":
                logger.info(
                    "the status-only completed record of the acknowledged run is "
                    "never success or result evidence"
                )
                return HubRunStatusClassification(
                    kind=HubRunStatusClassificationKind.NEVER_SUCCESS_EVIDENCE,
                    receipt=receipt,
                )
            return HubRunStatusClassification(
                kind=HubRunStatusClassificationKind.UNRESOLVED, receipt=receipt
            )
        with composed_transaction(pool) as transaction_pool:
            locked = hub_run_records.get_task_hub_run_receipt_for_update(
                transaction_pool, receipt_id=receipt.id
            )
            if locked is None or locked.status is not TaskHubRunReceiptStatus.ADMITTED:
                return HubRunStatusClassification(
                    kind=HubRunStatusClassificationKind.NOT_CLASSIFIABLE, receipt=receipt
                )
            outcome = _settle_execution_failure(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                receipt=locked,
                sequence=None,
            )
        if outcome.kind is HubRunSettlementKind.SETTLED_OBSERVED_FAILURE:
            return HubRunStatusClassification(
                kind=HubRunStatusClassificationKind.CLASSIFIED_FAILURE,
                receipt=outcome.receipt,
                disposition=outcome.disposition,
            )
        if outcome.kind is HubRunSettlementKind.SETTLED_RESULT_NOT_APPLIED:
            return HubRunStatusClassification(
                kind=HubRunStatusClassificationKind.RESULT_NOT_APPLIED,
                receipt=outcome.receipt,
            )
        return HubRunStatusClassification(
            kind=HubRunStatusClassificationKind.UNRESOLVED, receipt=outcome.receipt
        )


def check_hub_run_start_identity(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    connection_id: UUID,
    external_session_id: str,
    request_id: str | None,
) -> HubRunStartCorroboration:
    """Read-only corroboration of one observed ``run.started`` identity.

    The observed ``requestId`` equal to the outstanding acknowledged run
    identity corroborates the binding; a concretely different identity is
    an explicitly observed contradiction, logged selectively, never mutated
    against, and never a settlement match. Read-only: corroboration is
    never required and never durable state.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(connection_id, "connection_id")
    _require_nonblank_command(external_session_id, "external_session_id")
    binding = session_records.find_task_agent_session_binding(
        pool,
        workspace_id=workspace_id,
        connection_id=connection_id,
        external_session_id=external_session_id,
    )
    if binding is None:
        return HubRunStartCorroboration.UNOWNED
    receipt = hub_run_records.get_outstanding_task_hub_run_receipt(
        pool, task_agent_session_id=binding.id
    )
    if receipt is None or receipt.workspace_id != workspace_id:
        return HubRunStartCorroboration.UNOWNED
    if (
        receipt.status is not TaskHubRunReceiptStatus.ADMITTED
        or receipt.run_id is None
        or request_id is None
    ):
        return HubRunStartCorroboration.UNATTRIBUTABLE
    if request_id == receipt.run_id:
        return HubRunStartCorroboration.CORROBORATED
    logger.warning(
        "the observed run start carries a concretely different identity than the "
        "outstanding acknowledged run; the event is not a settlement match"
    )
    return HubRunStartCorroboration.CONTRADICTED
