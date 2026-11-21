"""The detached-turn correlation/settlement seam (issues #322/#348).

The runtime-neutral application-service seam between OpenOrc's authorized
one-shot claims and the externally admitted runtime turns that execute them.
The acknowledged opaque turn reference is correlation evidence, never
OpenOrc workflow authority: the receipt adds no second claim identity,
decides no workflow meaning, and the workflow consequences always route
through the existing owning services.

Dispatch side — the owning Level 1 service commits its existing one-shot
claim and the receipt companion in one short transaction (before any
admission), then acknowledges the admission: a positively identified
refusal is a proven pre-effect failure (the receipt records ``refused`` so
a never-dispatched turn cannot hold the session's slot), an acknowledged
admission attaches the exact opaque turn reference (CAS, exactly once), and
an uncertain admission stays conservatively claimed and is never
redispatched.

Settlement side — an independently observed terminal observation is resolved
within its authenticated Connection/Workspace scope against the
authoritative ``TaskAgentSession`` binding (external identifiers are never a
cross-Connection match), then attributed by the serial correlation rule: at
most one unsettled OpenOrc-dispatched turn exists per exact binding (the
durable partial unique index is the backstop), so a fresh terminal
observation for that session attributes to the outstanding acknowledged
turn. The concrete runtime integration owns the native interpretation of
its own evidence (issue #348): which native observations speak about a
turn's start or terminal outcome, why a start identity attributes or
contradicts the outstanding reference, how its ordering evidence proves a
delivered observation stale, and what its status-only records mean. This
seam consumes only the integration's neutral
``TurnAttribution`` / ``TurnFreshness`` / ``TurnStatusEvidenceVerdict``
classifications and owns every durable consequence: stale replays are
rejected against the durable consumed ordering markers the session's
settled receipts already record — a delayed duplicate of a prior turn's
notification can never be attributed to the next outstanding turn — and an
explicitly contradictory start classification is recorded as the receipt's
set-once contradiction marker (permanently blocking that receipt's
settlement), while any other unresolved classification leaves the evidence
unresolved rather than inventing a match.

Admission race — a terminal observation that resolves to a receipt still
``claimed`` (acknowledgement not yet persisted) is durably staged on that
exact receipt and consumed exactly once by the acknowledgement attach,
which then routes it through the ordinary settlement: the early result is
never lost and an unidentified turn is never prematurely settled. This is
the smallest safe handoff — no general event ledger, no replay machinery,
no polling, and no guaranteed crash recovery (a dispatch worker that dies
before attaching leaves the accepted v1 unresolved state).

Settlement routing — a successfully attributed completion must carry a
genuine validated formal result, pass the shared formal contract for the
receipt's expected family, and undergo the full current-subject reproof
(including the family's authorization subject: the E5 approved
implementation gate over the current plan revision, or the E9 claimed
iteration still addressing the canonical PR's current reconciled head)
before the workflow transition. Validated results complete the attempt
through the shared finalize; genuine observed failures and positively
established malformed-result protocol failures (an attributed completion
whose result candidate is missing or fails the shared contract) finalize
the still-running attempt truthfully (``failed_final`` with the
``observed_run_failure`` / ``malformed_run_result`` audit kind) and report
through the existing Level 2 operation-failure path with ``KNOWN_FAILED``
evidence — never a safe-retry classification. Settlement is atomic (one
composition) and idempotent (forward-only conditional writes; duplicates,
stale notifications, and contradicted receipts are rejected no-ops via the
receipt lifecycle, the consumed ordering marker, and the contradiction
marker). Only genuinely missing or unattributable terminal evidence stays
unresolved.

Status-only reconciliation — at existing bounded reconciliation points, the
integration's status-only evidence classification for the exact outstanding
reference routes a positively classified failure through the same typed
operation-failure path; a completed status alone is never success or result
evidence.

v1 scope note: the execution families (E6 implementation and E9
PR-remediation), the session-initialization establishment (issue #324), the
detached workspace-preparation PREPARE control (issue #334), the plan-result
families (issue #335), the planning-REVIEW family (issue #336), and the
Owner-authorized same-session Resume family (issue #343, whose settlement
completes the record routing's ORIGINAL operation and hands the post-commit
routing to that original family) carry settlement routing here; the
remaining operation families gain theirs with their own detached migrations
(#325), and routing refuses them fail-closed until then.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Protocol
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import (
    OWNER_REVIEWER_DISCUSSION_FAMILY,
    SETTLEMENT_INTERRUPTED_TURN,
    SETTLEMENT_OBSERVED_FAILURE,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
    SETTLEMENT_RESULT_NOT_APPLIED,
    SETTLEMENT_VALIDATED_RESULT,
    DetachedTurnObservationKind,
    NativeTurnEvidence,
    ObservedTurnEvidence,
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
    TurnStatusEvidenceVerdict,
    TurnTerminalClassification,
    TurnTerminalOutcome,
)
from openorc.domain.events import WorkflowEventActor
from openorc.domain.intents import TaskResumeIntent, TaskResumeIntentStatus
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    canonical_failure_subject,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.send_claims import TaskRuntimeSendClaimStatus
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import TaskPlanningBootstrapStage, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import detached_turns as turn_records
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import intents as intent_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence import reviews as review_iteration_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.errors import ProtocolError
from openorc.protocol.models import (
    FORMAL_RESPONSE_FAMILIES,
    IMPLEMENTATION_RESULT_FAMILY,
    WORKSPACE_OPERATION_CLEANUP,
    WORKSPACE_OPERATION_PREPARE,
    WORKSPACE_RESULT_FAMILY,
    FormalResponse,
    ImplementationResult,
    PlanResult,
    PrResult,
    ReviewResult,
    SessionReady,
    WorkspaceResult,
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
    "TurnEvidenceClassifier",
    "TurnSettlementKind",
    "TurnSettlementOutcome",
    "TurnStartCorroboration",
    "TurnStatusClassification",
    "TurnStatusClassificationKind",
    "attach_acknowledged_turn",
    "check_turn_start_identity",
    "classify_acknowledged_turn_status",
    "record_task_detached_turn_receipt",
    "settle_observed_turn_completion",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_detached_turn_correlation"
_ATTACH_SPAN_NAME: Final[str] = "task_detached_turn_correlation.attach_acknowledgement"
_SETTLE_SPAN_NAME: Final[str] = "task_detached_turn_correlation.settle_completion"
_CLASSIFY_SPAN_NAME: Final[str] = "task_detached_turn_correlation.classify_status"

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

# The Level 2 report's expected Task status per settled family, mirroring
# the owning dispatch services' subject contracts. The REVIEWER_REVIEW
# entry is the planning flavor's flat expectation; the PR/head review
# flavor (issue #338) resolves its ``reviewing`` expectation from the
# reserved attempt's durable subject (_expected_failure_status).
_FAMILY_EXPECTED_TASK_STATUS: Final[Mapping[PendingTaskRuntimeInteractionOperation, TaskStatus]] = {
    PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION: TaskStatus.IMPLEMENTING,
    PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION: TaskStatus.REVIEWING,
    PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH: TaskStatus.PLANNING,
    PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH: TaskStatus.PLANNING,
    PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW: TaskStatus.PLANNING,
    PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH: TaskStatus.WAITING_FOR_OWNER,
    # The session-resume family's flat fallback (issue #343): the expected
    # status resolves from the record's immutable restored_status; this
    # entry only covers a record that is already gone, where the Level 2
    # report is a typed no-op regardless.
    PendingTaskRuntimeInteractionOperation.SESSION_RESUME: TaskStatus.PLANNING,
}

# The original operations a session-resume receipt's routing may name: the
# closed set of interrupted automated workflow operations the Owner Resume
# authorization derives. Any other decoded value is an unexpected durable
# shape and the observation fails closed.
_RESUME_SETTLEMENT_OPERATIONS: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = frozenset(
    {
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
        PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
    }
)

# The plan-result families with detached settlement routing in this seam
# (issue #335): the Producer plan dispatches whose validated ``plan_result``
# finalizes through the owning plan-production service's settlement.
_PLAN_SETTLEMENT_FAMILIES: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = frozenset(
    {
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
    }
)

# The review family with detached settlement routing in this seam: the
# Reviewer REVIEW dispatch whose validated ``review_result`` finalizes
# through the owning decision service's settlement for the reserved
# attempt's durable subject flavor — the planning-review settlement (issue
# #336) or the PR/head review settlement (issue #338).
_REVIEW_SETTLEMENT_FAMILIES: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = frozenset(
    {PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW}
)

# The PR-composition family with detached settlement routing in this seam
# (issue #337): the Producer PR_COMPOSE dispatch whose validated ``pr_result``
# finalizes through the owning PR-composition service's settlement. The
# validated presentation is the settlement's only output: the bounded
# publication is the completion handoff's post-settlement consequence in the
# same invocation, never a durable effect of this seam.
_PR_COMPOSE_SETTLEMENT_FAMILIES: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = (
    frozenset({PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH})
)

# The discussion family with detached settlement routing in this seam (issue
# #339): the Owner ↔ Reviewer discussion turn whose attributed completion is
# routed through the owning discussion service's settlement — the standalone
# formal-result recognition, the gate-specific decisions, and the
# direction-only audit, all atomic with the receipt's settlement.
_DISCUSSION_SETTLEMENT_FAMILIES: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = (
    frozenset({PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION})
)

# The terminal-cleanup family with detached settlement routing in this seam
# (issue #344): the terminalization exception's CLEANUP_WORKSPACE run whose
# validated ``workspace_result`` settles the receipt truthfully. The route
# settles neither the Task's one-shot cleanup claim nor the archival — that
# disposition (including the established finite deadline) is the owning
# finalization's (issue #345) — and the family is deliberately NOT a Level 2
# operational-failure family.
_TERMINAL_CLEANUP_SETTLEMENT_FAMILIES: Final[frozenset[PendingTaskRuntimeInteractionOperation]] = (
    frozenset({PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP})
)

# The safe failure-classification token the settlement records for a
# positively established malformed-result protocol failure: an attributed
# completion whose native result is missing or fails the shared formal
# contract (post-dispatch evidence — never a pre-effect classification, and
# therefore never a safe-retry basis).
_MALFORMED_RUN_RESULT_FAILURE_KIND: Final[str] = "malformed_run_result"

# The openorc actor of the settlement's Level 2 report: the classification is
# OpenOrc's own typed observation, never a runtime- or Owner-attributed
# decision.
_OPENORC_ACTOR: Final[event_coordination.WorkflowActorContext] = (
    event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)
)


class TurnEvidenceClassifier(Protocol):
    """The runtime integration's neutral classification of its own evidence.

    The concrete runtime integration owns the native interpretation of
    which evidence proves or counters an admitted turn (issue #348): the
    neutral families it delivers, why a start identity attributes or
    contradicts the outstanding reference, how its ordering evidence proves
    a delivered terminal observation stale, and what its status-only
    records mean. Classifications are pure functions of the opaque native
    evidence and the safe durable context the seam supplies — the
    outstanding opaque turn reference and the consumed ordering markers —
    and perform no I/O, no durable lookup, and no workflow decision. Shared
    code routes, persists, and acts on the verdicts; it never inspects the
    native carrier.
    """

    def classify_turn_start(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnAttribution:
        """Classify one delivered turn-start observation's attribution."""
        ...

    def classify_turn_terminal(
        self,
        evidence: NativeTurnEvidence,
        *,
        consumed_ordering_markers: Sequence[int],
    ) -> TurnTerminalClassification:
        """Classify one delivered terminal observation's freshness and outcome."""
        ...

    def classify_turn_statuses(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnStatusEvidenceVerdict:
        """Classify one bounded status-only evidence consultation."""
        ...


class TurnSettlementKind(StrEnum):
    """The typed classification of one detached-turn seam operation."""

    ADMITTED = "admitted"
    NO_OUTSTANDING_RUN = "no_outstanding_run"
    STAGED_PENDING_ACKNOWLEDGEMENT = "staged_pending_acknowledgement"
    SETTLED_VALIDATED_RESULT = "settled_validated_result"
    SETTLED_OBSERVED_FAILURE = "settled_observed_failure"
    SETTLED_RESULT_NOT_APPLIED = "settled_result_not_applied"
    SETTLED_INTERRUPTED_TURN = "settled_interrupted_turn"
    LEFT_UNRESOLVED = "left_unresolved"
    REJECTED_DUPLICATE = "rejected_duplicate"
    REJECTED_STALE = "rejected_stale"


@dataclass(frozen=True, slots=True)
class TurnSettlementOutcome:
    """The typed outcome of one acknowledgement attach or completion
    settlement.

    ``formal_result`` carries the parsed formal response exactly when a
    validated result completed the attempt; ``disposition`` carries the
    Level 2 disposition exactly when an observed failure was reported.
    ``handoff_operation`` carries the explicit family the ONE post-commit
    completion handoff must route, exactly when the settled receipt's own
    operation is a delivery-mechanics family whose continuation belongs to
    the ORIGINAL workflow operation (issue #343): the session-resume
    receipt's settlement completes the original attempt through the owning
    services, so the handoff routes by the record's original operation
    family — never by ``session_resume`` itself. ``None`` routes by the
    receipt's own operation, so every existing family's handoff behavior is
    unchanged.
    """

    kind: TurnSettlementKind
    receipt: TaskDetachedTurnReceipt | None
    formal_result: FormalResponse | None = None
    disposition: object | None = None
    handoff_operation: PendingTaskRuntimeInteractionOperation | None = None


class TurnStatusClassificationKind(StrEnum):
    """The typed classification of one status-only reconciliation."""

    CLASSIFIED_FAILURE = "classified_failure"
    RESULT_NOT_APPLIED = "result_not_applied"
    NEVER_SUCCESS_EVIDENCE = "never_success_evidence"
    UNRESOLVED = "unresolved"
    NOT_CLASSIFIABLE = "not_classifiable"


@dataclass(frozen=True, slots=True)
class TurnStatusClassification:
    """The typed outcome of one status-only reconciliation classification."""

    kind: TurnStatusClassificationKind
    receipt: TaskDetachedTurnReceipt | None
    disposition: object | None = None


class TurnStartCorroboration(StrEnum):
    """The read-only corroboration classification of one observed turn start.

    ``CORROBORATED``: the integration's classification positively
    attributes the observed start to the outstanding acknowledged turn
    reference. ``CONTRADICTED``: the integration's classification
    positively establishes a concretely different turn identity — recorded
    by selective logging as the receipt's permanent contradiction marker,
    never a settlement match. ``UNATTRIBUTABLE``: the receipt is still
    claimed (no acknowledged reference yet) or the start carried no
    identity claim to decide with. ``UNOWNED``: no OpenOrc-dispatched turn
    is outstanding for the resolved binding.
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


def _settlement_outcome(label: str, ordering_marker: int | None) -> dict[str, object]:
    """The canonical settlement outcome payload of one consumed observation."""
    outcome: dict[str, object] = {SETTLEMENT_OUTCOME_LABEL_KEY: label}
    if ordering_marker is not None:
        # The durable consumed ordering marker: the settlement evidence later
        # reconciliation reads to reject already-consumed notifications.
        outcome[SETTLEMENT_OUTCOME_SEQUENCE_KEY] = ordering_marker
    return outcome


# --- the dispatch-side receipt companion ---------------------------------------


def record_task_detached_turn_receipt(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    operation: PendingTaskRuntimeInteractionOperation,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
    expected_formal_family: str | None = None,
    created_external_session_id: str | None = None,
    effective_config_snapshot: Mapping[str, object] | None = None,
    created_session_provenance: Mapping[str, object] | None = None,
) -> TaskDetachedTurnReceipt:
    """Record the claim-to-turn receipt companion of one committed one-shot claim.

    Composed inside the owning Level 1 service's claim transaction (the
    caller's ``expected_state_token`` currentness revalidation owns the
    authorization; this helper adds no second claim identity): the receipt
    commits before any admission, so the admission race and the
    single-outstanding-turn invariant are durable from the first possible
    effect. The execution families pin their exact expected formal family;
    every other family must name one explicitly until its own detached
    migration wires its routing. A violated single-outstanding-turn invariant
    (a concurrent second dispatch for the same session binding) is a typed
    conflict — fail closed, never a second outstanding turn.

    The detached session establishment (issue #324) passes its minimum
    separate provisional establishment evidence (the provisional external
    identity, the dispatch-composed non-secret snapshot, and the optional
    reported provenance) on its ``session_initialization`` receipt; every
    other operation passes none, and the coherence is enforced by the
    migration's conditional CHECKs either way.
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
                "an execution-family detached-turn receipt roots at the family's Execution identity"
            )
        if expected_formal_family is not None and (
            expected_formal_family != IMPLEMENTATION_RESULT_FAMILY
        ):
            raise InvalidCommandError(
                "an execution-family detached-turn receipt expects the implementation_result family"
            )
        expected_formal_family = IMPLEMENTATION_RESULT_FAMILY
    elif operation is PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION:
        # The gate-bound discussion receipt (issue #339): its identity is the
        # exact pending current OwnerGate and its permitted result is a
        # permitted NON-formal attributed text, so the dedicated non-formal
        # discussion family token is pinned — never a canonical v1 response
        # family, never an invented formal REVIEW claim.
        if original_attempt_kind is not OriginalAttemptKind.OWNER_GATE_DISCUSSION:
            raise InvalidCommandError(
                "a discussion detached-turn receipt roots at its exact gate-bound "
                "owner_gate_discussion identity"
            )
        if expected_formal_family is not None and (
            expected_formal_family != OWNER_REVIEWER_DISCUSSION_FAMILY
        ):
            raise InvalidCommandError(
                "a discussion detached-turn receipt expects the dedicated "
                "non-formal discussion family"
            )
        expected_formal_family = OWNER_REVIEWER_DISCUSSION_FAMILY
    elif operation is PendingTaskRuntimeInteractionOperation.SESSION_RESUME:
        # The detached Owner-authorized same-session Resume receipt
        # (issue #343): its identity is the exact one-shot session-resume
        # record (the owner-control intent family the E10 intent identities
        # reserve) and its expected family is the ORIGINAL interrupted
        # workflow operation's canonical formal family, carried from the
        # record's immutable routing context — the resumed turn's result
        # validates against the original family's schema, never a
        # resume-specific one.
        if original_attempt_kind is not OriginalAttemptKind.OWNER_CONTROL_INTENT:
            raise InvalidCommandError(
                "a session-resume detached-turn receipt roots at its exact "
                "owner-control intent identity"
            )
        if expected_formal_family is None or expected_formal_family not in FORMAL_RESPONSE_FAMILIES:
            raise InvalidCommandError(
                "a session-resume detached-turn receipt expects the original "
                "workflow operation's canonical formal family"
            )
    elif operation is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP:
        # The detached terminal-cleanup receipt (issue #344): its identity
        # is the exact terminalized Task's own one-shot cleanup claim — the
        # receipt's original attempt is the Task UUID itself, with the
        # claim's committed timestamp and the terminal commitment re-proven
        # at correlation time. No Execution, bootstrap claim, or control
        # intent, and no parallel effect ledger beside the Task row's
        # ``cleanup_send_claimed_at``/``cleanup_send_settled_at`` evidence
        # pair. The expected family is the canonical ``workspace_result``
        # family the CLEANUP_WORKSPACE control answers; the operation is
        # the terminalization exception and deliberately carries no Level
        # 2/3 operational-failure semantics.
        if original_attempt_kind is not OriginalAttemptKind.TERMINAL_CLEANUP_CLAIM:
            raise InvalidCommandError(
                "a terminal-cleanup detached-turn receipt roots at its exact "
                "Task-scoped terminal_cleanup_claim identity"
            )
        if original_attempt_id != task_id:
            raise InvalidCommandError(
                "a terminal-cleanup detached-turn receipt is bound to the exact "
                "terminalized Task UUID"
            )
        if expected_formal_family is not None and expected_formal_family != WORKSPACE_RESULT_FAMILY:
            raise InvalidCommandError(
                "a terminal-cleanup detached-turn receipt expects the canonical "
                "workspace_result family"
            )
        expected_formal_family = WORKSPACE_RESULT_FAMILY
    elif expected_formal_family is None or expected_formal_family not in FORMAL_RESPONSE_FAMILIES:
        raise InvalidCommandError(
            "the receipt requires its expected formal family to name a canonical v1 response family"
        )
    if created_external_session_id is not None and (
        operation is not PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION
    ):
        raise InvalidCommandError(
            "only a session_initialization receipt carries provisional establishment evidence"
        )
    try:
        return turn_records.create_task_detached_turn_receipt(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=task_agent_session_id,
            operation=operation,
            original_attempt_kind=original_attempt_kind,
            original_attempt_id=original_attempt_id,
            expected_formal_family=expected_formal_family,
            created_external_session_id=created_external_session_id,
            effective_config_snapshot=effective_config_snapshot,
            created_session_provenance=created_session_provenance,
        )
    except UniqueViolation as error:
        raise ConflictError(
            "the session binding already carries an outstanding detached-turn receipt; "
            "the dispatch is refused"
        ) from error


# --- the acknowledgement attach ------------------------------------------------


def attach_acknowledged_turn(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt_id: UUID,
    admitted_turn_reference: str,
) -> TurnSettlementOutcome:
    """Attach one acknowledged opaque turn reference to its claimed receipt.

    The post-admission acknowledgement of the dispatch invocation: the receipt
    must still be claimed (a lost/uncertain acknowledgement is a typed
    uncertainty at the adapter boundary and never reaches this attach; a
    positively identified refusal is recorded through the owning pre-effect
    failure path before this attach is ever called). A staged early
    completion — a terminal event that arrived during the admission race —
    is consumed exactly once inside the same composition and routed through
    the ordinary settlement, so the early result settles with its now-known
    turn reference instead of being lost. The composition is one short
    transaction; a failure rolls the attach and the routing back together.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(receipt_id, "receipt_id")
    _require_nonblank_command(admitted_turn_reference, "admitted_turn_reference")
    with application_span(_SERVICE_TRACER_SCOPE, _ATTACH_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ATTACH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            receipt = turn_records.get_task_detached_turn_receipt_for_update(
                transaction_pool, receipt_id=receipt_id
            )
            if (
                receipt is None
                or receipt.workspace_id != workspace_id
                or receipt.task_id != task_id
            ):
                raise NotFoundError(
                    "the addressed detached-turn receipt is not available in this task"
                )
            if receipt.status is not TaskDetachedTurnReceiptStatus.CLAIMED:
                raise StaleOperationError(
                    "the detached-turn receipt is no longer claimed; the acknowledgement is stale"
                )
            attached = turn_records.attach_task_detached_turn_acknowledgement(
                transaction_pool,
                receipt_id=receipt.id,
                task_id=task_id,
                workspace_id=workspace_id,
                admitted_turn_reference=admitted_turn_reference,
            )
            if attached is None:  # pragma: no cover - locked claimed receipt must attach
                raise StaleOperationError(
                    "the detached-turn receipt could not be acknowledged; the attach is stale"
                )
            staged = turn_records.get_task_detached_turn_early_completion_for_update(
                transaction_pool, receipt_id=receipt.id
            )
            if staged is None:
                return TurnSettlementOutcome(kind=TurnSettlementKind.ADMITTED, receipt=attached)
            outcome = _route_observed_terminal(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                receipt=attached,
                outcome=TurnTerminalOutcome(
                    kind=staged.kind,
                    result_candidate=staged.result_candidate,
                    interrupted=staged.interrupted,
                ),
                ordering_marker=staged.ordering_marker,
            )
            turn_records.consume_task_detached_turn_early_completion(
                transaction_pool, completion_id=staged.id, receipt_id=receipt.id
            )
            return outcome


# --- the settlement routing ----------------------------------------------------


def _route_observed_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation through its owning services.

    Called inside the caller's composition with the receipt row locked. A
    receipt carrying the explicitly observed contradiction marker is never a
    settlement match — the observation is left unresolved. The receipt's
    operation family selects the routing; a family without detached
    settlement routing yet is a fail-closed conflict (its own detached
    migration wires it), never a silent no-op.
    """
    if receipt.contradicted_at is not None:
        logger.warning(
            "the outstanding turn's correlation was explicitly contradicted by an "
            "observed turn identity; the terminal observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.interrupted:
        # The positive interrupted-turn routing (issue #342), checked before
        # every family routing: an interrupted turn's terminal evidence
        # settles the receipt and the session's registered Interrupt intent
        # — never a result application, never a Level 2 failure report,
        # never a synthetic block.
        return _route_interrupted_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
        )
    if receipt.operation in _EXECUTION_SETTLEMENT_FAMILIES:
        return _route_execution_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation is PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION:
        return _route_establishment_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE:
        return _route_bootstrap_prepare_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation in _PLAN_SETTLEMENT_FAMILIES:
        return _route_plan_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation in _REVIEW_SETTLEMENT_FAMILIES:
        return _route_review_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation in _PR_COMPOSE_SETTLEMENT_FAMILIES:
        return _route_pr_compose_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation is PendingTaskRuntimeInteractionOperation.SESSION_RESUME:
        return _route_session_resume_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation in _DISCUSSION_SETTLEMENT_FAMILIES:
        return _route_discussion_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    if receipt.operation in _TERMINAL_CLEANUP_SETTLEMENT_FAMILIES:
        return _route_terminal_cleanup_terminal(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            outcome=outcome,
            ordering_marker=ordering_marker,
        )
    raise ConflictError(
        "the receipt's operation family has no detached settlement routing yet; "
        "the observation is left unresolved"
    )


def _route_interrupted_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation that positively ended by
    interruption (issue #342).

    Called inside the caller's composition with the receipt row locked,
    before any family routing. The turn's own native evidence positively
    identified it as ended by interruption — the same normalized signal the
    blocking-send path classifies as ``AgentRuntimeInterruptedTurnError`` —
    so this routing records exactly two durable facts in one short
    transaction and defers everything else:

    - The receipt settles truthfully with the ``interrupted_turn`` label,
      consuming the observation's ordering marker: the turn's terminal
      resolution is the receipt's own evidence, independent of any intent
      bookkeeping. Nothing is applied to any subject — no result, no
      failure finalization, no Level 2 report, no block — and the original
      Execution/ReviewIteration/claim identity and its historic audit stay
      exactly as they are, available for later Owner Resume or Cancel.
    - The exact session's unsettled Interrupt intent — the one outstanding
      interruption a registered Abort (Owner or cost composition) recorded
      for this binding — gains the same durable ``observed_resolution``
      observation the executing-service correlation records, when exactly
      one such intent exists. Zero matches (the intent already settled
      through its own branch, was durably recorded failed/unknown, or is
      the victim of a concurrent settlement) leave the intent untouched;
      an unexpected multiple is an unexpected durable shape that fails the
      whole observation closed instead of guessing which intent matched.

    The adapter's D6b guard settlement is deliberately NOT part of this
    composition: it is process-local best-effort hygiene, it runs strictly
    after this composition commits through the composition-rooted
    settlement seam (``settle_annotated_task_interrupt``), and the durable
    intent remains the authority either way.
    """
    intents = [
        intent
        for intent in intent_records.list_unsettled_task_interrupt_intents(
            transaction_pool, task_id=task_id
        )
        if intent.task_agent_session_id == receipt.task_agent_session_id
    ]
    if len(intents) > 1:
        logger.warning(
            "the interrupted turn's session carries more than one unsettled "
            "Interrupt intent; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if len(intents) == 1:
        intent_records.annotate_unsettled_task_interrupt_intent(
            transaction_pool,
            task_interrupt_intent_id=intents[0].id,
            outcome={"observed_resolution": "interrupted_turn"},
        )
    if receipt.operation is PendingTaskRuntimeInteractionOperation.SESSION_RESUME:
        # The resumed turn itself was positively interrupted (issue #343):
        # the resume record settles its truthful terminal ``interrupted``
        # outcome in the same composition — a newer Owner decision's episode
        # governs, the resolved gate stays history, and the record stops
        # holding the session's continuation authorization while keeping its
        # truthful provenance. An already-terminal record is an unexpected
        # durable shape under the locked admitted receipt: fail closed.
        recorded = intent_records.record_task_resume_intent_outcome(
            transaction_pool,
            task_resume_intent_id=receipt.original_attempt_id,
            status=TaskResumeIntentStatus.INTERRUPTED,
            outcome={"dispatch": "interrupted"},
        )
        if recorded is None:
            logger.warning(
                "the interrupted session-resume record was no longer "
                "non-terminal when its interrupted outcome was recorded; "
                "failing the observation closed"
            )
            raise ConflictError(
                "the session-resume record could not record its interrupted "
                "outcome; the observation is left unresolved"
            )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_INTERRUPTED_TURN, ordering_marker),
    )
    return TurnSettlementOutcome(kind=TurnSettlementKind.SETTLED_INTERRUPTED_TURN, receipt=settled)


def _route_plan_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the plan dispatch claim.

    The attribution congruence is proved first (issue #335): the claim must
    exist in the addressed scope, still be conservatively claimed, carry the
    receipt's exact family kind, and anchor to exactly the receipt's session
    binding. A concrete attribution conflict leaves the observation
    unresolved — never a fabricated match. A terminal failure settles the
    receipt truthfully and reports through the existing typed
    operation-failure path (Level 2) under the claim's own family — whose
    dispositions are escalation or no-op, never a re-dispatch of the one-shot
    claim. A completion funnels the candidate through the shared formal
    contract; a validated ``plan_result`` finalizes through the owning
    service's settlement (the immutable PlanRevision, the current-pointer and
    token rotation, the send-claim settlement, and the audit event, all
    atomic with the receipt's settlement), while a missing or
    contract-failing candidate is a positively established protocol failure
    of the turn.
    """
    # Deferred import: the owning service imports this module's dispatch
    # mechanics at module level.
    from openorc.services.task_plan_production import settle_observed_plan_result  # noqa: PLC0415

    claim = send_claim_records.get_task_runtime_send_claim(
        transaction_pool, claim_id=receipt.original_attempt_id
    )
    if (
        claim is None
        or claim.workspace_id != workspace_id
        or claim.task_id != task_id
        or claim.kind.value != receipt.operation.value
        or claim.status is not TaskRuntimeSendClaimStatus.CLAIMED
        or claim.task_agent_session_id != receipt.task_agent_session_id
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's claim "
            "binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
        )
    # A genuinely attributed completion must carry a usable formal result. A
    # missing candidate, a shared-contract rejection, or an unexpected
    # family type is a positively established protocol failure of the turn —
    # routed through the existing typed operation-failure machinery (never
    # silently unresolved evidence, never a safe retry) — while a validated
    # result finalizes the one-shot claim through the owning service.
    parsed: FormalResponse | None
    if outcome.result_candidate is not None:
        try:
            parsed = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            parsed = None
    else:
        parsed = None
    if not isinstance(parsed, PlanResult):
        logger.warning(
            "the observed terminal completion carries no usable formal result for its "
            "expected family; classifying the positively established protocol failure"
        )
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
        )
    finalized = settle_observed_plan_result(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        send_claim=claim,
        result=parsed,
    )
    if finalized is None:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


def _route_review_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the reserved iteration.

    The attribution congruence is proved first (issues #336/#338): the
    reserved ReviewIteration must exist in the addressed scope, still be
    unfinalized, and anchor to exactly the receipt's Reviewer session
    binding — the task's current durable ``(Task, REVIEWER)`` binding. A
    concrete attribution conflict leaves the observation unresolved — never
    a fabricated match, and a finalized iteration is never decided twice. A
    terminal failure settles the receipt truthfully and reports through the
    existing typed operation-failure path (Level 2) under the review
    family's own identity with the attempt flavor's expected Task status.
    A completion funnels the candidate through the shared formal contract; a
    validated ``review_result`` finalizes through the owning decision
    service's settlement for the reserved attempt's durable subject flavor
    — the planning settlement (issue #336) for a plan-subject iteration, the
    PR/head settlement (issue #338) for a canonical-pull-request/head
    iteration — with the one-shot result recording, the persisted-state
    decision, the audit events, and the receipt's settlement all atomic;
    a missing or contract-failing candidate is a positively established
    protocol failure of the turn.
    """
    # Deferred imports: the owning services import this module's dispatch
    # mechanics at module level.
    from openorc.services.task_planning_review_decision import (  # noqa: PLC0415
        settle_observed_planning_review,
    )
    from openorc.services.task_pr_review_decision import (  # noqa: PLC0415
        settle_observed_pr_review,
    )

    iteration = review_iteration_records.get_review_iteration(
        transaction_pool, review_iteration_id=receipt.original_attempt_id
    )
    if (
        iteration is None
        or iteration.workspace_id != workspace_id
        or iteration.task_id != task_id
        or iteration.outcome is not None
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's reserved "
            "review attempt binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if (
        binding is None
        or binding.id != receipt.task_agent_session_id
        or binding.lifecycle_status is not TaskSessionLifecycleStatus.READY
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's reviewer "
            "session binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
        )
    # A genuinely attributed completion must carry a usable formal result. A
    # missing candidate, a shared-contract rejection, or an unexpected
    # family type is a positively established protocol failure of the turn —
    # routed through the existing typed operation-failure machinery (never
    # silently unresolved evidence, never a safe retry) — while a validated
    # result finalizes the reserved attempt through the owning service.
    parsed: FormalResponse | None
    if outcome.result_candidate is not None:
        try:
            parsed = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            parsed = None
    else:
        parsed = None
    if not isinstance(parsed, ReviewResult):
        logger.warning(
            "the observed terminal completion carries no usable formal result for its "
            "expected family; classifying the positively established protocol failure"
        )
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
        )
    if iteration.plan_revision_id is not None:
        # The planning flavor (issue #336): the owning planning-review
        # decision settlement over the exact current PlanRevision.
        finalized = settle_observed_planning_review(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=receipt.original_attempt_id,
            expected_reviewer_session_id=receipt.task_agent_session_id,
            result=parsed,
        )
    else:
        # The PR/head flavor (issue #338): the owning PR-review decision
        # settlement over the exact canonical pull request/head. Its own
        # subject guards re-prove the PR-iteration flavor fail-closed, so a
        # malformed subject shape settles result-not-applied instead of
        # being silently reinterpreted.
        finalized = settle_observed_pr_review(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_iteration_id=receipt.original_attempt_id,
            expected_reviewer_session_id=receipt.task_agent_session_id,
            result=parsed,
        )
    if finalized is None:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


def _route_pr_compose_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the PR_COMPOSE claim.

    The PR-composition family's detached settlement (issue #337). The
    attribution congruence is proved first: the claim must exist in the
    addressed scope, still be conservatively claimed, carry the receipt's
    exact family kind, and anchor to exactly the receipt's session binding.
    A concrete attribution conflict leaves the observation unresolved —
    never a fabricated match. A terminal failure settles the receipt
    truthfully and reports through the existing typed operation-failure path
    (Level 2) under the claim's own family — whose dispositions are
    escalation or no-op, never a re-dispatch of the one-shot claim. A
    completion funnels the candidate through the shared formal contract; a
    validated ``pr_result`` finalizes through the owning service's
    settlement (the exact subject re-derivation and the send-claim
    settlement, atomic with the receipt's settlement), while a missing or
    contract-failing candidate is a positively established protocol failure
    of the turn.
    """
    # Deferred import: the owning service imports this module's dispatch
    # mechanics at module level.
    from openorc.services.task_pr_composition import (  # noqa: PLC0415
        settle_observed_pr_compose_result,
    )

    claim = send_claim_records.get_task_runtime_send_claim(
        transaction_pool, claim_id=receipt.original_attempt_id
    )
    if (
        claim is None
        or claim.workspace_id != workspace_id
        or claim.task_id != task_id
        or claim.kind.value != receipt.operation.value
        or claim.status is not TaskRuntimeSendClaimStatus.CLAIMED
        or claim.task_agent_session_id != receipt.task_agent_session_id
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's claim "
            "binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
        )
    # A genuinely attributed completion must carry a usable formal result. A
    # missing candidate, a shared-contract rejection, or an unexpected
    # family type is a positively established protocol failure of the turn —
    # routed through the existing typed operation-failure machinery (never
    # silently unresolved evidence, never a safe retry) — while a validated
    # result finalizes the one-shot claim through the owning service.
    parsed: FormalResponse | None
    if outcome.result_candidate is not None:
        try:
            parsed = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            parsed = None
    else:
        parsed = None
    if not isinstance(parsed, PrResult):
        logger.warning(
            "the observed terminal completion carries no usable formal result for its "
            "expected family; classifying the positively established protocol failure"
        )
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
        )
    subjects = settle_observed_pr_compose_result(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        send_claim=claim,
        result=parsed,
    )
    if subjects is None:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


def _route_discussion_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the gate-bound exchange.

    The attribution congruence is proved first (issue #339): the receipt's
    gate-bound discussion subject must exist in the addressed scope (the
    exact OwnerGate UUID) and anchor to exactly the receipt's Reviewer
    session binding, still READY. A concrete attribution conflict leaves the
    observation unresolved — never a fabricated match.

    A terminal failure settles the receipt truthfully and stops: an
    Owner-initiated advisory conversation is deliberately NOT a Level 2
    operational-failure family, so no triage report is made, no disposition
    is derived, and nothing is blocked — the Owner simply sends the next
    message. A completion routes the raw attributed text — never funneled
    through the shared formal contract, because the discussion's permitted
    result is a non-formal attributed text — to the owning discussion
    settlement: the standalone strict ``review_result`` recognition, the
    gate-specific decisions, and the direction-only audit, all atomic with
    the receipt's settlement. A settlement that proves the discussion stale
    (the gate moved on) settles the receipt ``result_not_applied``.
    """
    # Deferred import: the owning service imports this module's dispatch
    # mechanics at module level.
    from openorc.services.task_owner_reviewer_discussion import (  # noqa: PLC0415
        settle_observed_discussion_result,
    )

    gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=receipt.original_attempt_id)
    if (
        gate is None
        or gate.workspace_id != workspace_id
        or gate.task_id != task_id
        or gate.id != receipt.original_attempt_id
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's gate-bound "
            "discussion subject; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if (
        binding is None
        or binding.id != receipt.task_agent_session_id
        or binding.lifecycle_status is not TaskSessionLifecycleStatus.READY
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's reviewer "
            "session binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        # The advisory conversation's observed failure: the receipt settles
        # truthfully and nothing else happens. No Level 2 report, no
        # disposition, no block — a discussion turn is not consequential
        # automated workflow work, and its failure never holds authority.
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_OBSERVED_FAILURE, receipt=settled
        )
    settlement = settle_observed_discussion_result(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        receipt=receipt,
        result_text=outcome.result_candidate,
    )
    if settlement is None:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(settlement.settlement_label, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
    )


_ACCEPTED_CLEANUP_RESULT_STATUSES: Final[frozenset[str]] = frozenset({"COMPLETED", "FAILED"})


def _route_terminal_cleanup_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the terminal cleanup run.

    The terminalization exception (issue #344): the subject reproof checks
    the exact Task UUID's durable terminalization commitment and its one-shot
    cleanup claim — the claim's committed timestamp must match the receipt's
    own ``claimed_at`` — never an ordinary currentness gate that rejects
    terminal commitment, and no Level 2/3 machinery follows. The observed
    candidate is validated as the canonical ``workspace_result``
    (``operation=CLEANUP``, ``COMPLETED|FAILED``); a validated result, a
    native run failure, or a positively established unusable result settles
    the receipt truthfully exactly once. The route settles neither the Task's
    cleanup claim nor the archival: that disposition — including the
    established finite deadline — is the owning finalization's (issue #345),
    so a validated result alone never archives here and a successful enqueue
    was never cleanup success.
    """
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id or locked.id != task_id:
        logger.warning(
            "the observed terminal cleanup completion conflicts with the receipt's "
            "task subject; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if locked.terminalization_outcome is None or locked.cleanup_send_claimed_at is None:
        # The durable claim identity diverged from the receipt: a cleanup
        # receipt requires a committed terminalization and a present claim.
        # Never fabricated — the observation is left unresolved.
        logger.warning(
            "the terminal-cleanup receipt's subject carries no committed "
            "terminalization or cleanup claim; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if (
        locked.cleanup_send_settled_at is not None
        or locked.cleanup_send_claimed_at != receipt.claimed_at
    ):
        # A moved-on subject: the owning disposition closed the one-shot
        # claim window (or a different claim instant owns this Task), so a
        # late observation settles as not applied — it can never re-open
        # cleanup, change the terminal truth, or undo an archival.
        logger.warning(
            "the terminal-cleanup receipt's claim moved on; settling the observation as not applied"
        )
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        # The cleanup run's observed native failure: the receipt settles
        # truthfully and stops. No Level 2 report, no disposition, no block,
        # and no second send — the claim stays the owning disposition's
        # subject (issue #345).
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_OBSERVED_FAILURE, receipt=settled
        )
    parsed: WorkspaceResult | None
    if outcome.result_candidate is not None:
        try:
            candidate = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            candidate = None
        parsed = candidate if isinstance(candidate, WorkspaceResult) else None
    else:
        parsed = None
    if (
        parsed is None
        or parsed.operation != WORKSPACE_OPERATION_CLEANUP
        or parsed.status not in _ACCEPTED_CLEANUP_RESULT_STATUSES
    ):
        # A positively established unusable result (missing, protocol-invalid,
        # or addressing another operation): the receipt settles truthfully
        # and stops — the same containment the blocking realization applied,
        # never a re-send, never a disposition decision of its own.
        logger.warning(
            "the observed terminal cleanup completion carries no usable "
            "workspace result for its expected family; settling the receipt "
            "truthfully as an observed failure"
        )
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_OBSERVED_FAILURE, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


def _resumed_record_routing(
    record: TaskResumeIntent,
) -> tuple[PendingTaskRuntimeInteractionOperation, UUID, str] | None:
    """Decode one session-resume record's immutable routing context.

    Returns the ORIGINAL interrupted workflow operation, its exact durable
    subject identity, and the original expected formal family; an
    incoherent mapping is ``None`` — the observation is left unresolved
    instead of guessing. The routing was written once at the Owner Resume
    authorization and is never mutated.
    """
    operation_value = record.routing.get("operation")
    subject_value = record.routing.get("subject_id")
    family_value = record.routing.get("expected_response_family")
    if (
        not isinstance(operation_value, str)
        or not isinstance(subject_value, str)
        or not isinstance(family_value, str)
        or not family_value.strip()
    ):
        return None
    try:
        operation = PendingTaskRuntimeInteractionOperation(operation_value)
        subject_id = UUID(subject_value)
    except ValueError:
        return None
    return operation, subject_id, family_value


def _route_session_resume_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the resumed turn.

    The detached Owner-authorized same-session Resume's settlement routing
    (issue #343). The attribution congruence is proved first: the receipt's
    owner-control-intent record must exist in the addressed scope, still be
    the claimed in-flight ``dispatched`` authorization anchored to exactly
    the receipt's session binding, and its immutable routing context must
    decode coherently, carry exactly the receipt's expected formal family,
    and name a supported original operation. A concrete attribution
    conflict leaves the observation unresolved — never a fabricated match.

    The resumed turn is the ORIGINAL interrupted workflow operation
    continuing, so a completion's formal result validates against the
    ORIGINAL pending family's schema and exact current subject and
    finalizes through the owning services' EXISTING detached settlements —
    never a resume-specific settlement engine, never a new
    Execution/ReviewIteration/PlanRevision, never a re-created prompt, and
    no workflow authority is ever derived from the turn reference. The
    owning settlement, the record's terminal outcome, and the receipt's
    settlement are one composition. A moved-on subject settles the receipt
    ``result_not_applied`` with the record's truthful ``failed`` terminal
    (its provenance names the not-applied result — the run ended, its
    result was never processed); a validated result returns the ORIGINAL
    operation family as the ``handoff_operation``, so the ONE post-commit
    completion handoff routes the settled completion exactly as it routes
    that family's ordinary detached settlements.
    """
    record = intent_records.get_task_resume_intent(
        transaction_pool, task_resume_intent_id=receipt.original_attempt_id
    )
    if (
        record is None
        or record.workspace_id != workspace_id
        or record.task_id != task_id
        or record.status is not TaskResumeIntentStatus.DISPATCHED
        or record.task_agent_session_id != receipt.task_agent_session_id
    ):
        logger.warning(
            "the observed terminal completion conflicts with the receipt's "
            "session-resume record binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    routing = _resumed_record_routing(record)
    if routing is None or routing[2] != receipt.expected_formal_family:
        logger.warning(
            "the session-resume record's routing context conflicts with the "
            "receipt's expected formal family; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    operation, subject_id, _expected_family = routing
    if operation not in _RESUME_SETTLEMENT_OPERATIONS:
        logger.warning(
            "the session-resume record's routing names an unsupported original "
            "operation; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        # The resumed run's positively observed terminal failure: the same
        # truthful failure path as every family, reported under the
        # SESSION_RESUME family rooted at the record's own identity (which
        # also settles the record's terminal ``failed`` outcome).
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
        )

    def _protocol_failure() -> TurnSettlementOutcome:
        return _settle_observed_turn_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
        )

    # A genuinely attributed completion must carry a usable formal result
    # for the ORIGINAL family. A missing candidate, a shared-contract
    # rejection, or an unexpected family type is a positively established
    # protocol failure of the turn — never silently unresolved evidence.
    parsed: FormalResponse | None
    if outcome.result_candidate is not None:
        try:
            parsed = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            parsed = None
    else:
        parsed = None

    # __RESUME_ROUTE_FAMILIES__

    if operation in _EXECUTION_SETTLEMENT_FAMILIES:
        execution = execution_records.get_execution(transaction_pool, execution_id=subject_id)
        if (
            execution is None
            or execution.workspace_id != workspace_id
            or execution.task_id != task_id
            or execution.producer_session_id != receipt.task_agent_session_id
        ):
            logger.warning(
                "the observed terminal completion conflicts with the resumed "
                "turn's execution binding; the observation is left unresolved"
            )
            return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
        if not isinstance(parsed, ImplementationResult):
            return _protocol_failure()
        finalized = settle_observed_implementation_result(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            execution_id=subject_id,
            subject=_EXECUTION_FAMILY_SUBJECTS[operation],
            result=parsed,
        )
    elif operation in (
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
    ):
        # Deferred import: the owning service imports this module's dispatch
        # mechanics at module level.
        from openorc.services.task_plan_production import (  # noqa: PLC0415
            settle_observed_plan_result,
        )

        claim = send_claim_records.get_task_runtime_send_claim(
            transaction_pool, claim_id=subject_id
        )
        if (
            claim is None
            or claim.workspace_id != workspace_id
            or claim.task_id != task_id
            or claim.kind.value != operation.value
            or claim.status is not TaskRuntimeSendClaimStatus.CLAIMED
            or claim.task_agent_session_id != receipt.task_agent_session_id
        ):
            logger.warning(
                "the observed terminal completion conflicts with the resumed "
                "turn's dispatch claim binding; the observation is left unresolved"
            )
            return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
        if not isinstance(parsed, PlanResult):
            return _protocol_failure()
        finalized = settle_observed_plan_result(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            send_claim=claim,
            result=parsed,
        )

    elif operation is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW:
        # Deferred imports: the owning decision services import this
        # module's dispatch mechanics at module level.
        from openorc.services.task_planning_review_decision import (  # noqa: PLC0415
            settle_observed_planning_review,
        )
        from openorc.services.task_pr_review_decision import (  # noqa: PLC0415
            settle_observed_pr_review,
        )

        iteration = review_iteration_records.get_review_iteration(
            transaction_pool, review_iteration_id=subject_id
        )
        if (
            iteration is None
            or iteration.workspace_id != workspace_id
            or iteration.task_id != task_id
            or iteration.outcome is not None
        ):
            logger.warning(
                "the observed terminal completion conflicts with the resumed "
                "turn's reserved review attempt binding; the observation is "
                "left unresolved"
            )
            return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
        binding = get_task_agent_session(
            transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER
        )
        if (
            binding is None
            or binding.id != receipt.task_agent_session_id
            or binding.lifecycle_status is not TaskSessionLifecycleStatus.READY
        ):
            logger.warning(
                "the observed terminal completion conflicts with the resumed "
                "turn's reviewer session binding; the observation is left "
                "unresolved"
            )
            return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
        if not isinstance(parsed, ReviewResult):
            return _protocol_failure()
        if iteration.plan_revision_id is not None:
            # The planning flavor (issue #336): the owning planning-review
            # decision settlement over the exact current PlanRevision.
            finalized = settle_observed_planning_review(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                review_iteration_id=subject_id,
                expected_reviewer_session_id=receipt.task_agent_session_id,
                result=parsed,
            )
        else:
            # The PR/head flavor (issue #338): the owning PR-review decision
            # settlement over the exact canonical pull request/head.
            finalized = settle_observed_pr_review(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                review_iteration_id=subject_id,
                expected_reviewer_session_id=receipt.task_agent_session_id,
                result=parsed,
            )

    else:
        # The resumed bootstrap PREPARE control: the same subject proof and
        # attributable workspace-result semantics as the ordinary PREPARE
        # settlement routing (issue #334), over the record routing's exact
        # claim-token subject.
        from openorc.services.task_planning_bootstrap import (  # noqa: PLC0415
            settle_observed_bootstrap_prepare_result,
        )

        current = task_records.get_task(transaction_pool, task_id=task_id)
        if current is None or current.workspace_id != workspace_id:
            logger.warning(
                "the observed workspace-preparation completion conflicts with "
                "the resumed turn's task; the observation is left unresolved"
            )
            return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
        evidence = task_records.get_task_planning_bootstrap_evidence(
            transaction_pool, task_id=task_id
        )
        if (
            evidence is None
            or evidence.claim_token != subject_id
            or current.status is not TaskStatus.PLANNING
            or current.current_plan_revision_id is not None
            or evidence.stage not in (None, TaskPlanningBootstrapStage.PREPARE_DISPATCHED)
        ):
            # A moved-on subject: the bootstrap resolved past the resumed
            # PREPARE work. The truthful not-applied settlement with the
            # record's terminal outcome — never manufactured stage history.
            logger.warning(
                "the resumed workspace-preparation subject moved on; settling "
                "the validated observation as not applied"
            )
            finalized = False
        else:
            if not isinstance(parsed, WorkspaceResult):
                return _protocol_failure()
            if (
                parsed.operation != WORKSPACE_OPERATION_PREPARE
                or parsed.status != _WORKSPACE_RESULT_COMPLETED
            ):
                # The agent's validated honest report: the requested
                # preparation did not complete. The same truthful failure
                # path as every resumed turn failure.
                logger.warning(
                    "the resumed workspace-preparation result reports the "
                    "requested preparation as not completed; classifying the "
                    "positively established failure"
                )
                return _settle_observed_turn_failure(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    receipt=receipt,
                    ordering_marker=ordering_marker,
                    error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
                    failure_kind=_FAILED_WORKSPACE_RESULT_FAILURE_KIND,
                )
            applied = settle_observed_bootstrap_prepare_result(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                claim_token=subject_id,
            )
            finalized = applied

    if not finalized:
        # A moved-on subject: the validated result can no longer apply to
        # current work. The receipt settles ``result_not_applied`` and the
        # record settles its truthful terminal ``failed`` outcome with the
        # not-applied provenance — the run ended, its result was never
        # processed — so neither the receipt nor the record holds the
        # session's continuation authorization open against newer episodes.
        recorded = intent_records.record_task_resume_intent_outcome(
            transaction_pool,
            task_resume_intent_id=receipt.original_attempt_id,
            status=TaskResumeIntentStatus.FAILED,
            outcome={"result": "result_not_applied"},
        )
        if recorded is None:
            logger.warning(
                "the session-resume record was no longer non-terminal when "
                "its not-applied outcome was recorded; failing the observation "
                "closed"
            )
            raise ConflictError(
                "the session-resume record could not record its not-applied "
                "outcome; the observation is left unresolved"
            )
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    recorded = intent_records.record_task_resume_intent_outcome(
        transaction_pool,
        task_resume_intent_id=receipt.original_attempt_id,
        status=TaskResumeIntentStatus.SETTLED,
        outcome={"result": "processed"},
    )
    if recorded is None:
        logger.warning(
            "the session-resume record was no longer non-terminal when its "
            "settled outcome was recorded; failing the observation closed"
        )
        raise ConflictError(
            "the session-resume record could not settle with its processed "
            "result; the observation is left unresolved"
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
        handoff_operation=operation,
    )


def _expected_failure_status(
    transaction_pool: DatabasePool, receipt: TaskDetachedTurnReceipt
) -> TaskStatus:
    """The Level 2 report's expected Task status for one observed failure.

    The flat family map serves every family whose subject form is fixed. The
    ``REVIEWER_REVIEW`` family covers both review attempt flavors (issue
    #338), so its expected status resolves from the reserved iteration's
    durable subject: a plan-subject iteration runs on a ``planning`` Task
    and a canonical-pull-request/head iteration on a ``reviewing`` Task. An
    unavailable attempt falls back to the family's flat expectation — the
    triage re-proves every subject itself and the report is a typed no-op
    when it no longer addresses current work.
    """
    family_status = _FAMILY_EXPECTED_TASK_STATUS[receipt.operation]
    if receipt.operation is PendingTaskRuntimeInteractionOperation.SESSION_RESUME:
        # The resume record's immutable routing carries the restored Task
        # status the Owner authorization committed (issue #343). A missing
        # or malformed record falls back to the family's flat expectation —
        # immaterial in practice, because the triage's own verification
        # makes a record-less report a typed no-op.
        record = intent_records.get_task_resume_intent(
            transaction_pool, task_resume_intent_id=receipt.original_attempt_id
        )
        if record is not None:
            restored = record.routing.get("restored_status")
            if isinstance(restored, str):
                try:
                    return TaskStatus(restored)
                except ValueError:
                    pass
        return family_status
    if receipt.operation is not PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW:
        return family_status
    iteration = review_iteration_records.get_review_iteration(
        transaction_pool, review_iteration_id=receipt.original_attempt_id
    )
    if iteration is None or iteration.task_pull_request_id is None:
        return family_status
    return TaskStatus.REVIEWING


def _settle_observed_turn_failure(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    ordering_marker: int | None,
    error_code: OperationalFailureCode,
    failure_kind: str,
) -> TurnSettlementOutcome:
    """Settle one positively established genuine detached-turn failure.

    The receipt settles truthfully (``observed_failure``) and the failure
    reports through the existing typed operation-failure path (Level 2)
    under the receipt's own family — the same family the blocking dispatch's
    failures have always reported under, whose dispositions are escalation
    or no-op, never a re-dispatch of the one-shot claim or a fabricated
    verdict. The durable attempt itself stays conservatively unfinished: an
    observed run failure or a malformed result is post-effect evidence,
    never a proven pre-effect non-delivery. The report is a typed no-op when
    it no longer addresses current work.

    For a SESSION_RESUME receipt (issue #343) the composition additionally
    records the resume record's truthful terminal ``failed`` outcome —
    post-effect evidence with safe classified provenance — atomically with
    the receipt settlement and the Level 2 report, so the Owner Cancel
    action is never permanently vetoed by a record whose resumed run has
    definitively ended.
    """
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, ordering_marker),
    )
    if receipt.operation is PendingTaskRuntimeInteractionOperation.SESSION_RESUME:
        recorded = intent_records.record_task_resume_intent_outcome(
            transaction_pool,
            task_resume_intent_id=receipt.original_attempt_id,
            status=TaskResumeIntentStatus.FAILED,
            outcome={
                "result": "observed_failure",
                "failure_kind": failure_kind,
                "error_code": error_code.value,
            },
        )
        if recorded is None:
            # An already-terminal record under the locked admitted receipt is
            # an unexpected durable shape: fail the whole observation closed
            # instead of reporting a failure about settled work.
            logger.warning(
                "the failed session-resume record was no longer non-terminal "
                "when its failed outcome was recorded; failing the observation "
                "closed"
            )
            raise ConflictError(
                "the session-resume record could not record its failed "
                "outcome; the observation is left unresolved"
            )
    current = task_records.get_task(transaction_pool, task_id=task_id)
    if current is None or current.workspace_id != workspace_id:
        # Unreachable for a Task-scoped receipt; kept explicit for fail-closed
        # safety.
        raise NotFoundError("the requested task is not available in this workspace")
    operation = OperationalFailureOperation(
        family=receipt.operation,
        original_attempt_kind=receipt.original_attempt_kind,
        original_attempt_id=receipt.original_attempt_id,
        subject=canonical_failure_subject(
            {
                "claim_id": str(receipt.original_attempt_id),
                "task_agent_session_id": str(receipt.task_agent_session_id),
            }
        ),
    )
    report = OperationalFailureReport(
        reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
        reporting_attempt_id=receipt.original_attempt_id,
        error_code=error_code,
        effect_evidence=FailureEffectEvidence.KNOWN_FAILED,
        diagnostic_context={"observation": "observed_turn_failure"},
    )
    disposition = triage_operational_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=current.state_token,
        expected_status=_expected_failure_status(transaction_pool, receipt),
        expected_current_owner_gate_id=None,
        actor=_OPENORC_ACTOR,
        operation=operation,
        report=report,
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_OBSERVED_FAILURE,
        receipt=settled,
        disposition=disposition,
    )


def _route_execution_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
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
            "the observed terminal completion conflicts with the receipt's attempt "
            "binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        return _settle_execution_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
        )
    # A genuinely attributed completion must carry a usable formal result. A
    # missing candidate, a shared-contract rejection, or an unexpected
    # family type is a positively established protocol failure of the turn —
    # routed through the existing typed operation-failure machinery (never
    # silently unresolved evidence, never a safe retry) — while a validated
    # result completes the still-running attempt through the shared finalize.
    parsed: FormalResponse | None
    if outcome.result_candidate is not None:
        try:
            parsed = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            parsed = None
    else:
        parsed = None
    if not isinstance(parsed, ImplementationResult):
        # The family pairing is enforced at record time; an unexpected type
        # is the same fail-closed protocol classification, never a
        # reinterpretation.
        logger.warning(
            "the observed terminal completion carries no usable formal result for its "
            "expected family; classifying the positively established protocol failure"
        )
        return _settle_execution_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
        )
    finalized = settle_observed_implementation_result(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        execution_id=receipt.original_attempt_id,
        subject=_EXECUTION_FAMILY_SUBJECTS[receipt.operation],
        result=parsed,
    )
    if finalized is None:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


def _resolve_authorized_retry_failure_report(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    error_code: OperationalFailureCode,
) -> tuple[OperationalFailureOperation, OperationalFailureReport] | None:
    """Resolve one durably authorized retry's observed failure report.

    The F5c authorized-implementation retry shares the
    ``IMPLEMENTATION_EXECUTION`` receipt family with the initial dispatch,
    and its receipt's original-attempt identity is the NEW resulting
    Execution — so a plain original-attempt report would establish a
    competing failure episode instead of disposing the authorized retry
    inside its existing F4 episode (issue #340 review). The F4 attempt's
    set-once ``resulting_execution_id`` binding is the durable proof that
    this Execution is an authorized retry's effect row: when the binding
    resolves within the addressed scope and its episode matches the
    receipt's family and attempt kind exactly, the report retains the
    episode's original identity and reports ``PRIOR_AUTHORIZED`` with the
    bound attempt and the exact resulting Execution — the same enrichment
    the F5b coordinator applies to a blocking retry's failure report. An
    incongruent binding is a durable invariant violation and fails closed.
    Every other Execution (initial IMPLEMENT, PR remediation) reports the
    original attempt identity exactly as before.
    """
    bound_attempt = failure_records.find_operational_failure_attempt_by_resulting_execution(
        transaction_pool, resulting_execution_id=receipt.original_attempt_id
    )
    if bound_attempt is None:
        return None
    if bound_attempt.workspace_id != workspace_id or bound_attempt.task_id != task_id:
        raise ConflictError(
            "the authorized retry's failure binding addresses a different "
            "scope; the observed failure cannot be reported"
        )
    episode = failure_records.get_operational_failure_episode(
        transaction_pool, episode_id=bound_attempt.episode_id
    )
    if (
        episode is None
        or episode.workspace_id != workspace_id
        or episode.task_id != task_id
        or episode.operation_family is not receipt.operation
        or episode.original_attempt_kind is not receipt.original_attempt_kind
    ):
        raise ConflictError(
            "the authorized retry's failure episode does not match the "
            "observed receipt; the observed failure cannot be reported"
        )
    operation = OperationalFailureOperation(
        family=episode.operation_family,
        original_attempt_kind=episode.original_attempt_kind,
        original_attempt_id=episode.original_attempt_id,
        subject=dict(episode.subject),
    )
    report = OperationalFailureReport(
        reporting_attempt_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
        reporting_attempt_id=bound_attempt.id,
        error_code=error_code,
        effect_evidence=FailureEffectEvidence.KNOWN_FAILED,
        effect_attempt_id=receipt.original_attempt_id,
        diagnostic_context={"observation": "observed_turn_failure"},
    )
    return operation, report


def _settle_execution_failure(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    ordering_marker: int | None,
    error_code: OperationalFailureCode,
    failure_kind: str,
) -> TurnSettlementOutcome:
    """Settle one positively established genuine turn failure.

    The still-running attempt finalizes ``failed_final`` with the truthful
    failure-classification audit atomically with the receipt's settlement,
    and the failure reports through the existing typed operation-failure
    path (Level 2) with the caller's stable code — an observed turn failure
    or a positively established malformed-result protocol failure.
    ``KNOWN_FAILED`` evidence is never a safe-retry classification, so the
    disposition is the policy's own bounded decision (escalation, no
    automatic redispatch). The report is a typed no-op when it no longer
    addresses current work. An attempt already resolved by another
    authority records only the truthful ``result_not_applied`` settlement —
    no failure is fabricated against resolved history.
    """
    finalized = finalize_observed_implementation_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        execution_id=receipt.original_attempt_id,
        subject=_EXECUTION_FAMILY_SUBJECTS[receipt.operation],
        failure_kind=failure_kind,
    )
    if finalized is None:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, ordering_marker),
    )
    current = task_records.get_task(transaction_pool, task_id=task_id)
    if current is None or current.workspace_id != workspace_id:
        # Unreachable after the finalize's own locked reproof; kept explicit
        # for fail-closed safety.
        raise NotFoundError("the requested task is not available in this workspace")
    resolved = _resolve_authorized_retry_failure_report(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        receipt=receipt,
        error_code=error_code,
    )
    if resolved is not None:
        # The failing Execution is a durably authorized retry's effect row:
        # the report disposes within its existing F4 episode.
        operation, report = resolved
    else:
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
            error_code=error_code,
            effect_evidence=FailureEffectEvidence.KNOWN_FAILED,
            diagnostic_context={"observation": "observed_turn_failure"},
        )
    disposition = triage_operational_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=current.state_token,
        expected_status=_expected_failure_status(transaction_pool, receipt),
        expected_current_owner_gate_id=None,
        actor=_OPENORC_ACTOR,
        operation=operation,
        report=report,
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_OBSERVED_FAILURE,
        receipt=settled,
        disposition=disposition,
    )


# --- the establishment (session initialization) routing, issue #324 -------------


def _route_establishment_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the initialization turn.

    The binding congruence is proved first: the receipt's authoritative
    binding must exist in the addressed scope, belong to the exact Task, and
    be the CONNECTING binding the receipt was dispatched against. A concrete
    attribution conflict leaves the observation unresolved — never a
    fabricated match. A terminal failure settles the receipt truthfully (the
    initialization genuinely failed; the binding stays CONNECTING, never
    LOST) and reports through the existing Level 2 operation-failure path
    under the bootstrap claim's established family. A completion funnels the
    candidate through the shared formal contract: only a canonical
    ``session_ready`` with the READY status finalizes the binding to READY
    atomically with the receipt settlement, through the existing lifecycle
    service. A missing candidate or a contract rejection is a positively
    established protocol failure of the initialization — routed through the
    same truthful failure path, never silently unresolved evidence, never a
    safe retry.
    """
    # The lifecycle service is imported lazily: the establishment dispatch
    # (session_runtime) composes this seam's record/attach helpers at module
    # level, and this settlement routing composes its readiness finalizer —
    # the same established deferred-import pattern as the composition seams.
    from openorc.services.session_runtime import finalize_observed_initialization_readiness

    binding = session_records.get_task_agent_session_by_id(
        transaction_pool, task_agent_session_id=receipt.task_agent_session_id
    )
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        logger.warning(
            "the observed initialization completion conflicts with the receipt's "
            "session binding; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        return _settle_establishment_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
        )
    parsed: SessionReady | None
    if outcome.result_candidate is not None:
        try:
            candidate = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            candidate = None
        if isinstance(candidate, SessionReady) and candidate.status == "READY":
            parsed = candidate
        else:
            parsed = None
    else:
        parsed = None
    if parsed is None:
        logger.warning(
            "the observed initialization completion carries no valid READY "
            "session_ready; classifying the positively established protocol failure"
        )
        return _settle_establishment_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
        )
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.CONNECTING:
        # The receipt is outstanding only against the CONNECTING binding it
        # dispatched; a moved-on binding is the honest result-not-applied
        # settlement, never a rewrite of initialized history.
        logger.warning(
            "the initialization receipt's binding already moved past CONNECTING; "
            "settling the validated result as not applied"
        )
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    provenance = receipt.created_session_provenance or {}
    finalized = finalize_observed_initialization_readiness(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=binding.role,
        external_session_id=receipt.created_external_session_id or "",
        effective_configuration=dict(receipt.effective_config_snapshot or {}),
        reported_provider=_safe_provenance_str(provenance, "reported_provider"),
        reported_model=_safe_provenance_str(provenance, "reported_model"),
        reported_runtime_version=_safe_provenance_str(provenance, "reported_runtime_version"),
    )
    label = SETTLEMENT_VALIDATED_RESULT if finalized.applied else SETTLEMENT_RESULT_NOT_APPLIED
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(label, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=(
            TurnSettlementKind.SETTLED_VALIDATED_RESULT
            if finalized.applied
            else TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED
        ),
        receipt=settled,
        formal_result=parsed,
    )


def _safe_provenance_str(provenance: Mapping[str, object], key: str) -> str | None:
    """Read one optional safe string provenance fact."""
    value = provenance.get(key)
    return value if isinstance(value, str) and value.strip() else None


def _settle_bootstrap_claim_failure(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    ordering_marker: int | None,
    error_code: OperationalFailureCode,
    failure_kind: str,
    diagnostic_observation: str,
) -> TurnSettlementOutcome:
    """Settle one positively established genuine bootstrap-claim turn failure.

    The receipt settles truthfully (``observed_failure``) and the failure
    reports through the existing typed operation-failure path (Level 2)
    under the bootstrap claim's established family — the same family the
    blocking establishment's failures have always reported under. The
    establishment case keeps the binding CONNECTING (never LOST for an
    observed turn failure) and the establishment is never re-dispatched:
    the initialize-once discipline leaves the durable unresolved state to
    the existing recovery semantics. The durably dispatched PREPARE state
    (issue #334) is the family's still-current failed operation, so a
    confirmed detached PREPARE failure escalates through the existing
    disposition machinery to its blocking outcome instead of being silently
    rejected as stale. The report is a typed no-op when it no longer
    addresses current work.
    """
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_OBSERVED_FAILURE, ordering_marker),
    )
    current = task_records.get_task(transaction_pool, task_id=task_id)
    if current is None or current.workspace_id != workspace_id:
        # Unreachable for a Task-scoped receipt; kept explicit for fail-closed
        # safety.
        raise NotFoundError("the requested task is not available in this workspace")
    operation = OperationalFailureOperation(
        family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
        original_attempt_kind=receipt.original_attempt_kind,
        original_attempt_id=receipt.original_attempt_id,
        subject=canonical_failure_subject(
            {
                "planning_bootstrap_claim_token": str(receipt.original_attempt_id),
                "task_agent_session_id": str(receipt.task_agent_session_id),
            }
        ),
    )
    report = OperationalFailureReport(
        reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
        reporting_attempt_id=receipt.original_attempt_id,
        error_code=error_code,
        effect_evidence=FailureEffectEvidence.KNOWN_FAILED,
        diagnostic_context={"observation": diagnostic_observation},
    )
    disposition = triage_operational_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=current.state_token,
        expected_status=TaskStatus.PLANNING,
        expected_current_owner_gate_id=None,
        actor=_OPENORC_ACTOR,
        operation=operation,
        report=report,
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_OBSERVED_FAILURE,
        receipt=settled,
        disposition=disposition,
    )


def _settle_establishment_failure(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    ordering_marker: int | None,
    error_code: OperationalFailureCode,
    failure_kind: str,
) -> TurnSettlementOutcome:
    """Settle one positively established genuine initialization failure.

    The bootstrap-claim failure settlement with the establishment's own
    diagnostic observation; the initialization's binding stays CONNECTING
    and the establishment is never re-dispatched.
    """
    return _settle_bootstrap_claim_failure(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        receipt=receipt,
        ordering_marker=ordering_marker,
        error_code=error_code,
        failure_kind=failure_kind,
        diagnostic_observation="observed_turn_initialization_failure",
    )


# --- the detached workspace-preparation (PREPARE) routing, issue #334 ----------


# The canonical COMPLETED status value of the shared ``workspace_result``
# family contract (issue #181): the typed result vocabulary the settlement
# classifies the validated result's reported operation outcome against.
_WORKSPACE_RESULT_COMPLETED: Final = "COMPLETED"

# The safe failure-classification token the settlement records for a
# validated workspace result that honestly reports the requested workspace
# preparation as failed (or the wrong operation): the turn completed and
# delivered its formal result, and the result itself is the positive
# failure evidence.
_FAILED_WORKSPACE_RESULT_FAILURE_KIND: Final[str] = "failed_workspace_result"


def _route_bootstrap_prepare_terminal(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    outcome: TurnTerminalOutcome,
    ordering_marker: int | None,
) -> TurnSettlementOutcome:
    """Route one attributed terminal observation onto the PREPARE control.

    The claim congruence is proved first: the Task's bootstrap evidence must
    exist in the addressed scope with exactly the receipt's claim identity,
    the Task must still be current coarse-planning work without a published
    plan revision, and the stage evidence must still be the outstanding
    PREPARE state (unsettled, or the durably dispatched
    ``prepare_dispatched`` marker — issue #334). A moved-on subject is the
    honest result-not-applied settlement, never a rewrite of stage history.
    A terminal failure settles the receipt truthfully (``observed_failure``)
    and reports through the existing Level 2 operation-failure path under
    the bootstrap claim's family. A completion funnels the candidate through
    the shared formal contract: a validated ``workspace_result`` reporting
    the requested PREPARE operation as COMPLETED records the claim-bound
    ``prepared`` stage — through the owning bootstrap service's settlement,
    atomically with the receipt settlement — while a validated result
    reporting the operation as failed (or the wrong operation) is the
    agent's positively established failure, routed through the same truthful
    failure path. A missing candidate or a contract rejection is a
    positively established protocol failure of the turn. Every failure path
    escalates through the existing disposition machinery (the bootstrap
    claim has no legitimate new-attempt path) and never a safe retry.
    """
    # The owning bootstrap service is imported lazily: the bootstrap
    # continuation composes this seam's record/attach helpers at module
    # level, and this settlement routing composes the bootstrap's own stage
    # settlement — the same established deferred-import pattern as the
    # composition seams.
    from openorc.services.task_planning_bootstrap import (
        settle_observed_bootstrap_prepare_result,
    )

    current = task_records.get_task(transaction_pool, task_id=task_id)
    if current is None or current.workspace_id != workspace_id:
        logger.warning(
            "the observed workspace-preparation completion conflicts with the "
            "receipt's task; the observation is left unresolved"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.LEFT_UNRESOLVED, receipt=receipt)
    evidence = task_records.get_task_planning_bootstrap_evidence(transaction_pool, task_id=task_id)
    if (
        evidence is None
        or evidence.claim_token != receipt.original_attempt_id
        or current.status is not TaskStatus.PLANNING
        or current.current_plan_revision_id is not None
        or evidence.stage not in (None, TaskPlanningBootstrapStage.PREPARE_DISPATCHED)
    ):
        # A moved-on subject: the bootstrap resolved past the outstanding
        # PREPARE work (or the claim identity moved on), so the observation
        # settles as not applied instead of manufacturing stage history.
        logger.warning(
            "the workspace-preparation receipt's subject moved on; settling "
            "the validated observation as not applied"
        )
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    if outcome.kind is DetachedTurnObservationKind.FAILED:
        return _settle_bootstrap_claim_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=OBSERVED_RUN_FAILURE_KIND,
            diagnostic_observation="observed_turn_prepare_failure",
        )
    # __PREPARE_ROUTING_RESULT__

    parsed: WorkspaceResult | None
    if outcome.result_candidate is not None:
        try:
            candidate = parse_formal_response(
                outcome.result_candidate, expected_family=receipt.expected_formal_family
            )
        except ProtocolError:
            candidate = None
        parsed = candidate if isinstance(candidate, WorkspaceResult) else None
    else:
        parsed = None
    if parsed is None:
        logger.warning(
            "the observed workspace-preparation completion carries no usable "
            "formal result for its expected family; classifying the positively "
            "established protocol failure"
        )
        return _settle_bootstrap_claim_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.RUNTIME_PROTOCOL,
            failure_kind=_MALFORMED_RUN_RESULT_FAILURE_KIND,
            diagnostic_observation="observed_turn_prepare_failure",
        )
    if (
        parsed.operation != WORKSPACE_OPERATION_PREPARE
        or parsed.status != _WORKSPACE_RESULT_COMPLETED
    ):
        # The agent's validated honest report: the requested workspace
        # preparation did not complete. The same truthful failure path as
        # the blocking control's WorkspacePreparationFailedError — never a
        # prepared stage, never a new turn authorization.
        logger.warning(
            "the observed workspace-preparation result reports the requested "
            "preparation as not completed; classifying the positively "
            "established failure"
        )
        return _settle_bootstrap_claim_failure(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt=receipt,
            ordering_marker=ordering_marker,
            error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
            failure_kind=_FAILED_WORKSPACE_RESULT_FAILURE_KIND,
            diagnostic_observation="observed_turn_prepare_failure",
        )
    applied = settle_observed_bootstrap_prepare_result(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        claim_token=evidence.claim_token,
    )
    if not applied:
        settled = turn_records.settle_task_detached_turn_receipt(
            transaction_pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome=_settlement_outcome(SETTLEMENT_RESULT_NOT_APPLIED, ordering_marker),
        )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED, receipt=settled
        )
    settled = turn_records.settle_task_detached_turn_receipt(
        transaction_pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome=_settlement_outcome(SETTLEMENT_VALIDATED_RESULT, ordering_marker),
    )
    return TurnSettlementOutcome(
        kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        receipt=settled,
        formal_result=parsed,
    )


# --- the public settlement and reconciliation entries ---------------------------


def settle_observed_turn_completion(
    pool: DatabasePool,
    *,
    classifier: TurnEvidenceClassifier,
    workspace_id: UUID,
    connection_id: UUID,
    evidence: ObservedTurnEvidence,
) -> TurnSettlementOutcome:
    """Settle one independently observed terminal turn observation.

    The observation resolves within its authenticated Connection/Workspace
    scope against the authoritative session binding — external identifiers
    are never a cross-Connection match — and then attributes by the serial
    correlation rule: the single outstanding receipt of that exact binding.
    The runtime integration classifies the delivered evidence — its native
    ordering verdict against the session's durable consumed ordering
    markers, and the neutral terminal outcome — and this seam owns every
    durable consequence. A claimed receipt (acknowledgement not yet
    persisted) stages the observation for its acknowledgement attach; an
    admitted receipt routes the observation through the owning services and
    settles; no outstanding receipt makes the observation unowned, and a
    second early delivery for the same receipt is a rejected duplicate.
    Nothing here ever invents a match or treats an unattributable
    observation as a workflow fact.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(connection_id, "connection_id")
    if not isinstance(evidence, ObservedTurnEvidence):
        raise InvalidCommandError("evidence must be an ObservedTurnEvidence")
    if evidence.family is not TurnEvidenceFamily.TURN_TERMINAL:
        raise InvalidCommandError("terminal settlement requires TURN_TERMINAL evidence")
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
            external_session_id=evidence.external_session_id,
        )
        if binding is None:
            # The bound-identity resolution missed. A CONNECTING binding
            # deliberately carries no external identity, so the observation
            # may belong to an initializing session: resolve the outstanding
            # ``session_initialization`` receipt whose provisional
            # ``created_external_session_id`` matches, still strictly within
            # this authenticated Workspace/Connection scope. An
            # initialization receipt is resolved by its binding's row
            # identity from here on — the same serial-correlation machinery.
            initialization_receipt = turn_records.find_outstanding_session_initialization_receipt(
                pool,
                workspace_id=workspace_id,
                connection_id=connection_id,
                created_external_session_id=evidence.external_session_id,
            )
            if initialization_receipt is None:
                logger.info(
                    "the observed terminal completion resolves to no session binding in "
                    "this connection scope; the observation is unowned"
                )
                return TurnSettlementOutcome(
                    kind=TurnSettlementKind.NO_OUTSTANDING_RUN, receipt=None
                )
            with composed_transaction(pool) as transaction_pool:
                locked = turn_records.get_task_detached_turn_receipt_for_update(
                    transaction_pool, receipt_id=initialization_receipt.id
                )
                if (
                    locked is None
                    or locked.workspace_id != workspace_id
                    or locked.status
                    not in (
                        TaskDetachedTurnReceiptStatus.CLAIMED,
                        TaskDetachedTurnReceiptStatus.ADMITTED,
                    )
                ):
                    logger.info(
                        "the observed initialization completion has no outstanding "
                        "receipt in this connection scope; the observation is unowned"
                    )
                    return TurnSettlementOutcome(
                        kind=TurnSettlementKind.NO_OUTSTANDING_RUN, receipt=None
                    )
                return _settle_locked_receipt(
                    transaction_pool,
                    classifier=classifier,
                    workspace_id=workspace_id,
                    task_id=locked.task_id,
                    task_agent_session_id=locked.task_agent_session_id,
                    receipt=locked,
                    evidence=evidence,
                )
        with composed_transaction(pool) as transaction_pool:
            receipt = turn_records.get_outstanding_task_detached_turn_receipt_for_update(
                transaction_pool, task_agent_session_id=binding.id
            )
            if receipt is None or receipt.workspace_id != workspace_id:
                logger.info(
                    "the observed terminal completion has no outstanding turn for its "
                    "session binding; the observation is unowned"
                )
                return TurnSettlementOutcome(
                    kind=TurnSettlementKind.NO_OUTSTANDING_RUN, receipt=None
                )
            return _settle_locked_receipt(
                transaction_pool,
                classifier=classifier,
                workspace_id=workspace_id,
                task_id=receipt.task_id,
                task_agent_session_id=binding.id,
                receipt=receipt,
                evidence=evidence,
            )


def _settle_locked_receipt(
    transaction_pool: DatabasePool,
    *,
    classifier: TurnEvidenceClassifier,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    evidence: ObservedTurnEvidence,
) -> TurnSettlementOutcome:
    """Settle one locked outstanding receipt against one terminal observation.

    The shared post-lock settlement of both resolutions — the bound-external
    -identity lookup and the initialization receipt's provisional-identity
    lookup (issue #324). The runtime integration classifies the delivered
    evidence against the binding's durable consumed ordering markers; the
    seam applies its own durable consequences: a stale classification is a
    rejected replay, a claimed receipt stages the observation for its
    acknowledgement attach, and an admitted receipt routes through the
    owning services and settles.
    """
    # The stale-replay safeguard: the integration's ordering verdict against
    # the binding's consumed ordering markers proves an already-consumed or
    # replayed notification — never a fresh completion for the outstanding
    # turn. Evidence without ordering evidence is never rejected by this
    # check; its settleability is the shared durable rules' decision.
    consumed = turn_records.get_last_consumed_turn_ordering_marker(
        transaction_pool, task_agent_session_id=task_agent_session_id
    )
    classification = classifier.classify_turn_terminal(
        evidence.native_evidence,
        consumed_ordering_markers=() if consumed is None else (consumed,),
    )
    if classification.freshness is TurnFreshness.STALE:
        logger.info(
            "the observed terminal completion was classified stale against the "
            "session's consumed ordering marker; the stale duplicate is rejected"
        )
        return TurnSettlementOutcome(kind=TurnSettlementKind.REJECTED_STALE, receipt=receipt)
    if receipt.status is TaskDetachedTurnReceiptStatus.CLAIMED:
        try:
            turn_records.stage_task_detached_turn_early_completion(
                transaction_pool,
                receipt_id=receipt.id,
                task_id=receipt.task_id,
                workspace_id=receipt.workspace_id,
                kind=classification.outcome.kind,
                ordering_marker=classification.ordering_marker,
                result_candidate=classification.outcome.result_candidate,
                interrupted=classification.outcome.interrupted,
            )
        except UniqueViolation:
            logger.info(
                "a second early completion for the same outstanding receipt is a rejected duplicate"
            )
            return TurnSettlementOutcome(
                kind=TurnSettlementKind.REJECTED_DUPLICATE, receipt=receipt
            )
        return TurnSettlementOutcome(
            kind=TurnSettlementKind.STAGED_PENDING_ACKNOWLEDGEMENT,
            receipt=receipt,
        )
    return _route_observed_terminal(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        receipt=receipt,
        outcome=classification.outcome,
        ordering_marker=classification.ordering_marker,
    )


def classify_acknowledged_turn_status(
    pool: DatabasePool,
    *,
    classifier: TurnEvidenceClassifier,
    workspace_id: UUID,
    task_id: UUID,
    receipt_id: UUID,
    status_evidence: NativeTurnEvidence,
) -> TurnStatusClassification:
    """Classify one bounded status-only reconciliation consultation.

    At existing bounded reconciliation points the runtime integration
    classifies its own status-only records for the exact outstanding
    reference: positively classified failure evidence routes through the
    existing typed operation-failure path (``KNOWN_FAILED`` — the turn
    executed and failed, never a safe-retry classification); a completed
    status alone is never success/result evidence and applies nothing; and
    every other observed shape — a missing record, an ambiguous duplicate,
    or an unclassified state — is the honest unresolved state. Only an
    admitted receipt carries a reference to classify.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(receipt_id, "receipt_id")
    if not isinstance(status_evidence, NativeTurnEvidence):
        raise InvalidCommandError("status_evidence must be a NativeTurnEvidence carrier")
    with application_span(_SERVICE_TRACER_SCOPE, _CLASSIFY_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_CLASSIFY_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        receipt = turn_records.get_task_detached_turn_receipt(pool, receipt_id=receipt_id)
        if (
            receipt is None
            or receipt.workspace_id != workspace_id
            or receipt.task_id != task_id
            or receipt.status is not TaskDetachedTurnReceiptStatus.ADMITTED
        ):
            return TurnStatusClassification(
                kind=TurnStatusClassificationKind.NOT_CLASSIFIABLE, receipt=receipt
            )
        assert receipt.admitted_turn_reference is not None  # narrowed by the admitted coherence
        verdict = classifier.classify_turn_statuses(
            status_evidence, outstanding_turn_reference=receipt.admitted_turn_reference
        )
        if verdict is not TurnStatusEvidenceVerdict.FAILURE_EVIDENCE:
            if verdict is TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE:
                logger.info(
                    "the status-only completed record of the acknowledged turn is "
                    "never success or result evidence"
                )
                return TurnStatusClassification(
                    kind=TurnStatusClassificationKind.NEVER_SUCCESS_EVIDENCE,
                    receipt=receipt,
                )
            # Missing record, ambiguous duplicate identity, or an unclassified
            # state: honest unknown, never a classification guess.
            return TurnStatusClassification(
                kind=TurnStatusClassificationKind.UNRESOLVED, receipt=receipt
            )
        with composed_transaction(pool) as transaction_pool:
            locked = turn_records.get_task_detached_turn_receipt_for_update(
                transaction_pool, receipt_id=receipt.id
            )
            if locked is None or locked.status is not TaskDetachedTurnReceiptStatus.ADMITTED:
                return TurnStatusClassification(
                    kind=TurnStatusClassificationKind.NOT_CLASSIFIABLE, receipt=receipt
                )
            failure_route = (
                _settle_establishment_failure
                if locked.operation is PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION
                else _settle_execution_failure
            )
            outcome = failure_route(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                receipt=locked,
                ordering_marker=None,
                error_code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED,
                failure_kind=OBSERVED_RUN_FAILURE_KIND,
            )
        if outcome.kind is TurnSettlementKind.SETTLED_OBSERVED_FAILURE:
            return TurnStatusClassification(
                kind=TurnStatusClassificationKind.CLASSIFIED_FAILURE,
                receipt=outcome.receipt,
                disposition=outcome.disposition,
            )
        if outcome.kind is TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED:
            return TurnStatusClassification(
                kind=TurnStatusClassificationKind.RESULT_NOT_APPLIED,
                receipt=outcome.receipt,
            )
        return TurnStatusClassification(
            kind=TurnStatusClassificationKind.UNRESOLVED, receipt=outcome.receipt
        )


def check_turn_start_identity(
    pool: DatabasePool,
    *,
    classifier: TurnEvidenceClassifier,
    workspace_id: UUID,
    connection_id: UUID,
    evidence: ObservedTurnEvidence,
) -> TurnStartCorroboration:
    """Classify one observed turn start and record contradictions.

    The runtime integration owns the native interpretation of its start
    evidence (issue #348): its classification positively attributes the
    observed start to the outstanding acknowledged turn reference, or
    positively establishes a concretely different turn identity. A
    contradiction is logged selectively and recorded as the receipt's
    set-once ``contradicted_at`` marker, permanently blocking that receipt's
    settlement so the correlation stays honestly unresolved instead of
    inventing a match — the observation prevents subsequent settlement
    rather than being discarded. ``UNATTRIBUTABLE``: the receipt is still
    claimed (no acknowledged reference yet) or the start carried no identity
    claim to decide with. ``UNOWNED``: no OpenOrc-dispatched turn is
    outstanding for the resolved binding.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(connection_id, "connection_id")
    if not isinstance(evidence, ObservedTurnEvidence):
        raise InvalidCommandError("evidence must be an ObservedTurnEvidence")
    if evidence.family is not TurnEvidenceFamily.TURN_START:
        raise InvalidCommandError("start corroboration requires TURN_START evidence")
    binding = session_records.find_task_agent_session_binding(
        pool,
        workspace_id=workspace_id,
        connection_id=connection_id,
        external_session_id=evidence.external_session_id,
    )
    if binding is None:
        return TurnStartCorroboration.UNOWNED
    receipt = turn_records.get_outstanding_task_detached_turn_receipt(
        pool, task_agent_session_id=binding.id
    )
    if receipt is None or receipt.workspace_id != workspace_id:
        return TurnStartCorroboration.UNOWNED
    if (
        receipt.status is not TaskDetachedTurnReceiptStatus.ADMITTED
        or receipt.admitted_turn_reference is None
    ):
        return TurnStartCorroboration.UNATTRIBUTABLE
    verdict = classifier.classify_turn_start(
        evidence.native_evidence, outstanding_turn_reference=receipt.admitted_turn_reference
    )
    if verdict is TurnAttribution.CONTRADICTED:
        logger.warning(
            "the observed turn start was classified as a concretely different "
            "identity than the outstanding acknowledged reference; recording the "
            "contradiction marker and leaving the correlation unresolved"
        )
        turn_records.mark_task_detached_turn_receipt_contradicted(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
        )
        return TurnStartCorroboration.CONTRADICTED
    if verdict is TurnAttribution.ATTRIBUTED:
        return TurnStartCorroboration.CORROBORATED
    return TurnStartCorroboration.UNATTRIBUTABLE
