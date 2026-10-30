"""Level 2 operational-failure disposition (F4, issue #251).

One application-owned, transport-neutral disposition service for a current
logical operation's failure report. The service makes an authoritative,
durable, idempotent decision — authorize one bounded safe-network retry, or
escalate atomically through the #258 exceptional blocking foundation — and
it never invokes an adapter, enqueues work, sleeps, chooses gate actions, or
exposes a transport surface. Actual workflow invocation, Level 1 retrofit,
Owner actions, and deferred wakeups belong to F5/F6/F8/F11.

The command accepts trusted typed data only: stable machine-readable error
codes and classified effect-evidence classes
(:mod:`openorc.domain.operational_failures`), never exception objects,
exception-message text, queue status, raw provider HTTP errors, or a
caller-supplied ``retryable`` assertion. The stable code is retained
separately from the coarse ``TaskBlockReason`` and the OwnerGate type.

Decision flow — one short outer ``composed_transaction``, the Task row
locked first: missing or out-of-scope Task identity is the uniform
``NotFoundError``; every stale fact (moved-on token/status/current-gate
identity, archived or terminal Task, superseded subject, duplicate or
already-disposed report) is a typed no-op that applies nothing, so an
obsolete failure report never blocks healthy or superseded work. Under the
locks the service re-proves the exact durable operation, attempt, subject,
role/session binding, and latest claim/effect state for the reported
family; mutable evidence that can settle without rotating the Task token
(send claims, executions, session lifecycle) is serialized with focused
``FOR UPDATE`` reads in Task-lock-first order.

Retry eligibility is one explicit small matrix: the family must support a
distinct logical-attempt path (v1: only the implementation-execution
family, whose existing durable identity is the fresh per-Task Execution
row), the code must be the classified transient network failure, the
evidence must be positively pre-effect, durable corroboration must agree
(the ``execution_failed`` audit event's safe failure-kind for the exact
attempt), and the episode's three-additional-attempt budget must have room.
Everything else escalates: the F2 one-shot claim families have no legitimate
new-attempt path and always escalate, even on proven pre-effect network
failures. The authorized attempt is persisted with its ordinal and
exponential-backoff due time (2/4/8 s on an injectable clock) atomically
with the failing attempt's disposition; the unique backstops make a
duplicate or replayed triage return the same existing disposition or a
no-op, never a second authorization or a budget reset. Escalation calls the
F3 installation with ``BLOCK_RESOLUTION``, the code-mapped TaskBlock
reason, and a sanitized bounded context in the same transaction; F3 owns
the gate/status/token/audit writes in its nested savepoint, and the
episode's terminal disposition — the exact TaskBlock id — is stamped
set-once, so the disposition stays durably idempotent across restarts,
repeated reports, and later Owner recovery.

No raw exception chain, provider payload, credential, prompt, response, or
transcript is ever persisted or exported. No database transaction is held
across any external call.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from openorc.domain.blocks import TaskBlockReason, canonical_task_block_context
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventType
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureAttempt,
    OperationalFailureCode,
    OperationalFailureDomainError,
    OperationalFailureEpisode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    canonical_bounded_failure_context,
    canonical_failure_subject,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.send_claims import (
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
)
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskPlanningBootstrapStage, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import events as event_records
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence import pull_requests as pull_request_records
from openorc.persistence import reviews as review_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.task_exceptional_blocking import (
    ExceptionalBlockInstallation,
    install_exceptional_block_gate,
)
from openorc.services.task_subject_guards import require_current_plan_revision
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "DispositionKind",
    "NoOpReason",
    "OperationalFailureDisposition",
    "OperationalFailureOperation",
    "OperationalFailureReport",
    "bind_retry_attempt_execution",
    "triage_operational_failure",
]

_TRACER_SCOPE = "openorc.services.task_operational_failure_triage"
_TRIAGE_SPAN_NAME = "task_operational_failure_triage.triage_operational_failure"

# The audit-event subject discriminator for Execution-scoped events (the
# open validated-text vocabulary of event_coordination).
_EXECUTION_SUBJECT_TYPE = "execution"
# The safe machine classification the Level 1 seams record for a positively
# proven pre-effect runtime failure (task_runtime_send_claims and the
# authorized-implementation-execution service).
_RUNTIME_PRE_EFFECT_FAILURE_KIND = "runtime_pre_effect"


class DispositionKind(StrEnum):
    """The three disposition outcomes of one triage decision."""

    RETRY_AUTHORIZED = "retry_authorized"
    BLOCKED = "blocked"
    NO_OP = "no_op"


class NoOpReason(StrEnum):
    """Why a triage decision applied no new effect.

    ``STALE``: the report no longer addresses current work (moved-on
    token/status/gate identity, terminal or archived Task, superseded or
    settled subject, missing evidence). ``ALREADY_DISPOSED``: the reported
    failure already has its durable disposition (an existing successor
    authorization). ``ALREADY_TERMINALLY_DISPOSED``: the episode already
    escalated to its exact TaskBlock, which stays the disposition even after
    later Owner recovery.
    """

    STALE = "stale"
    ALREADY_DISPOSED = "already_disposed"
    ALREADY_TERMINALLY_DISPOSED = "already_terminally_disposed"


@dataclass(frozen=True, slots=True)
class OperationalFailureOperation:
    """The typed identity of the logical operation whose failure is reported.

    ``family`` is the closed pending-interaction operation vocabulary; the
    ``original_attempt`` pair names the existing durable claim/attempt
    identity the failure episode roots at; ``subject`` is the canonical
    safe-identifier mapping of the exact workflow subject.
    """

    family: PendingTaskRuntimeInteractionOperation
    original_attempt_kind: OriginalAttemptKind
    original_attempt_id: UUID
    subject: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class OperationalFailureReport:
    """The typed failure report for one attempt of the addressed operation.

    ``error_code`` and ``effect_evidence`` are trusted typed classifications
    the calling seam derives from the concrete error taxonomies — never
    exception text. ``effect_attempt_id`` is the durable effect row whose
    failure is reported when it differs from the original identity (the
    fresh Execution row an authorized additional attempt produced); the
    original identity row serves as the evidence for original-attempt
    reports. ``diagnostic_context`` must be sanitized and bounded.
    """

    reporting_attempt_kind: OperationalFailureSourceKind
    reporting_attempt_id: UUID
    error_code: OperationalFailureCode
    effect_evidence: FailureEffectEvidence
    effect_attempt_id: UUID | None = None
    diagnostic_context: Mapping[str, object] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class OperationalFailureDisposition:
    """The one typed outcome of a triage decision.

    ``RETRY_AUTHORIZED`` carries the durably persisted additional attempt
    (its id, ordinal, and ``not_before``); ``BLOCKED`` carries the F3
    installation (TaskBlock, pending exceptional gate, blocked Task);
    ``NO_OP`` carries its reason and, where one exists, the already-existing
    disposition identity — never a new effect.
    """

    kind: DispositionKind
    retry_attempt: OperationalFailureAttempt | None = None
    installation: ExceptionalBlockInstallation | None = None
    no_op_reason: NoOpReason | None = None
    existing_task_block_id: UUID | None = None
    existing_attempt_id: UUID | None = None


# The v1 retry-eligibility matrix: the explicit, small classification the
# service owns. Only the implementation-execution family has a supported
# distinct logical-attempt path (a fresh per-Task Execution row is the
# existing durable attempt identity); every F2 one-shot send claim, the
# bootstrap claim, the E3a iteration reservation, and the one-shot E9
# remediation claim have no legitimate new-attempt path and always escalate,
# even when the network failure was positively pre-effect. Only the
# classified transient-network pre-effect code is retry-eligible; uncertain
# evidence, every other code, and the fallback escalate (v1 has no
# idempotent/read-only workflow operation family).
_RETRY_ELIGIBLE_FAMILIES: frozenset[PendingTaskRuntimeInteractionOperation] = frozenset(
    {PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION}
)
_RETRY_ELIGIBLE_CODES: frozenset[OperationalFailureCode] = frozenset(
    {OperationalFailureCode.RUNTIME_UNAVAILABLE}
)

# The exponential backoff due-time policy: 2 s, 4 s, 8 s for the three
# additional attempts, computed at authorization on the injectable clock.
_RETRY_BACKOFF: Mapping[int, timedelta] = MappingProxyType(
    {
        1: timedelta(seconds=2),
        2: timedelta(seconds=4),
        3: timedelta(seconds=8),
    }
)

# The stable error code -> coarse TaskBlockReason mapping for escalation.
# The code is retained separately in the block context; the reason is the
# settled blocking vocabulary the exceptional gate pairs with.
_ESCALATION_REASONS: Mapping[OperationalFailureCode, TaskBlockReason] = MappingProxyType(
    {
        OperationalFailureCode.RUNTIME_UNAVAILABLE: TaskBlockReason.CONNECTION_UNAVAILABLE,
        OperationalFailureCode.RUNTIME_TIMEOUT: TaskBlockReason.RUNTIME_FAILURE,
        OperationalFailureCode.RUNTIME_DELIVERY_UNCERTAIN: TaskBlockReason.RUNTIME_FAILURE,
        OperationalFailureCode.RUNTIME_TRANSPORT: TaskBlockReason.RUNTIME_FAILURE,
        OperationalFailureCode.RUNTIME_PROTOCOL: TaskBlockReason.RUNTIME_FAILURE,
        OperationalFailureCode.AGENT_SESSION_LOST: TaskBlockReason.AGENT_SESSION_LOST,
        OperationalFailureCode.RUNTIME_CONFIGURATION_REJECTED: (
            TaskBlockReason.INVALID_CREDENTIALS
        ),
        OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN: (
            TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN
        ),
        OperationalFailureCode.EXTERNAL_OPERATION_FAILED: TaskBlockReason.UNKNOWN,
        OperationalFailureCode.EXTERNAL_OPERATION_REJECTED: TaskBlockReason.UNKNOWN,
        OperationalFailureCode.UNKNOWN: TaskBlockReason.UNKNOWN,
    }
)

# Which durable original-identity kinds can root each operation family; an
# incoherent pair is a malformed command. ``owner_control_intent`` pairs with
# no v1 family yet and therefore fails closed as a command shape error.
_FAMILY_IDENTITY_KINDS: Mapping[
    PendingTaskRuntimeInteractionOperation, frozenset[OriginalAttemptKind]
] = MappingProxyType(
    {
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE: frozenset(
            {OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM}
        ),
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION: frozenset(
            {OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM}
        ),
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH: frozenset(
            {OriginalAttemptKind.RUNTIME_SEND_CLAIM}
        ),
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH: frozenset(
            {OriginalAttemptKind.RUNTIME_SEND_CLAIM}
        ),
        PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW: frozenset(
            {OriginalAttemptKind.REVIEW_ITERATION}
        ),
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION: frozenset(
            {OriginalAttemptKind.EXECUTION}
        ),
        PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION: frozenset(
            {OriginalAttemptKind.EXECUTION}
        ),
        PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH: frozenset(
            {OriginalAttemptKind.RUNTIME_SEND_CLAIM}
        ),
    }
)

# The F2 send-claim kind each dispatch family's original identity resolves to.
_FAMILY_CLAIM_KINDS: Mapping[PendingTaskRuntimeInteractionOperation, TaskRuntimeSendClaimKind] = (
    MappingProxyType(
        {
            PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH: (
                TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH
            ),
            PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH: (
                TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH
            ),
            PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH: (
                TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH
            ),
        }
    )
)


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _stale_no_op(
    *,
    existing_task_block_id: UUID | None = None,
    existing_attempt_id: UUID | None = None,
) -> OperationalFailureDisposition:
    """The typed no-op for a report that no longer addresses current work."""
    return OperationalFailureDisposition(
        kind=DispositionKind.NO_OP,
        no_op_reason=NoOpReason.STALE,
        existing_task_block_id=existing_task_block_id,
        existing_attempt_id=existing_attempt_id,
    )


@dataclass(frozen=True, slots=True)
class _VerifiedFailure:
    """The per-family verification outcome when the report is current.

    ``session_lost`` records that the applicable role/session binding is
    durably terminal (LOST/ENDED): the failure report stays current, but the
    recovery-blocking reason is the more specific, durably proven session
    loss rather than the reported code's mapped reason.
    """

    session_lost: bool


def triage_operational_failure(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    expected_status: TaskStatus,
    expected_current_owner_gate_id: UUID | None,
    actor: WorkflowActorContext,
    operation: OperationalFailureOperation,
    report: OperationalFailureReport,
    clock: Callable[[], datetime] | None = None,
) -> OperationalFailureDisposition:
    """Decide one current logical operation's failure disposition.

    Returns the typed disposition — retry authorized (the persisted
    additional attempt with its ordinal and due time), blocked (the atomic
    F3 installation), or no-op (stale/duplicate/already disposed) — and
    never applies anything outside one short database-only composition.
    """
    # --- Command shape first: nothing about durable state is consulted. ---
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    if expected_current_owner_gate_id is not None:
        _require_uuid_command(expected_current_owner_gate_id, "expected_current_owner_gate_id")
    if not isinstance(expected_status, TaskStatus):
        raise InvalidCommandError("expected_status must be a TaskStatus")
    if not isinstance(actor, WorkflowActorContext):
        raise InvalidCommandError("actor must be a WorkflowActorContext")
    if not isinstance(operation, OperationalFailureOperation):
        raise InvalidCommandError("operation must be an OperationalFailureOperation")
    if not isinstance(operation.family, PendingTaskRuntimeInteractionOperation):
        raise InvalidCommandError("operation.family must be a pending-interaction operation")
    if not isinstance(operation.original_attempt_kind, OriginalAttemptKind):
        raise InvalidCommandError("operation.original_attempt_kind must be an OriginalAttemptKind")
    _require_uuid_command(operation.original_attempt_id, "operation.original_attempt_id")
    if operation.original_attempt_kind not in _FAMILY_IDENTITY_KINDS.get(
        operation.family, frozenset()
    ):
        raise InvalidCommandError(
            "the operation family does not pair with the supplied original attempt identity kind"
        )
    try:
        canonical_failure_subject(operation.subject)
    except (ValueError, OperationalFailureDomainError) as error:
        raise InvalidCommandError(
            "operation.subject must be a canonical JSON object of safe identifiers"
        ) from error
    if not isinstance(report, OperationalFailureReport):
        raise InvalidCommandError("report must be an OperationalFailureReport")
    if not isinstance(report.reporting_attempt_kind, OperationalFailureSourceKind):
        raise InvalidCommandError(
            "report.reporting_attempt_kind must be an OperationalFailureSourceKind"
        )
    _require_uuid_command(report.reporting_attempt_id, "report.reporting_attempt_id")
    if not isinstance(report.error_code, OperationalFailureCode):
        raise InvalidCommandError("report.error_code must be an OperationalFailureCode")
    if not isinstance(report.effect_evidence, FailureEffectEvidence):
        raise InvalidCommandError("report.effect_evidence must be a FailureEffectEvidence")
    if report.effect_attempt_id is not None:
        _require_uuid_command(report.effect_attempt_id, "report.effect_attempt_id")
    if (
        report.reporting_attempt_kind is OperationalFailureSourceKind.ORIGINAL
        and report.reporting_attempt_id != operation.original_attempt_id
    ):
        raise InvalidCommandError(
            "an original-attempt report must address the operation's original attempt identity"
        )
    if (
        report.reporting_attempt_kind is OperationalFailureSourceKind.ORIGINAL
        and report.effect_attempt_id is not None
        and report.effect_attempt_id != operation.original_attempt_id
    ):
        raise InvalidCommandError(
            "an original-attempt report must corroborate its exact original "
            "attempt identity, never a different effect row"
        )
    if (
        report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED
        and report.effect_attempt_id is None
    ):
        raise InvalidCommandError(
            "an authorized-attempt report must reference the durable effect "
            "row its execution produced"
        )
    try:
        canonical_bounded_failure_context(report.diagnostic_context)
    except OperationalFailureDomainError as error:
        raise InvalidCommandError(
            "report.diagnostic_context must be a sanitized, bounded canonical JSON object"
        ) from error
    now = clock if clock is not None else (lambda: datetime.now(UTC))
    # Attach only after the caller-supplied identifiers proved valid: a
    # malformed command is classified without exporting its values.
    with application_span(_TRACER_SCOPE, _TRIAGE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_TRIAGE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            return _triage_locked(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                expected_status=expected_status,
                expected_current_owner_gate_id=expected_current_owner_gate_id,
                actor=actor,
                operation=operation,
                report=report,
                now=now,
            )


def bind_retry_attempt_execution(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    retry_attempt_id: UUID,
    resulting_execution_id: UUID,
) -> OperationalFailureAttempt:
    """Bind one consumed retry authorization to its resulting Execution.

    The executing seam (the Level 1/F5 invocation that consumed the
    authorization) records the exact durable Execution it created from the
    authorized attempt: one short composition under the Task row lock, the
    authorization's episode not terminally disposed, the Execution in the
    same Task/Workspace scope, and a set-once conditional write. Rebinding
    the same Execution is an idempotent success; binding a different one is
    a conflict that fails closed. A later failure report about the
    authorization must address exactly this Execution.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    _require_uuid_command(retry_attempt_id, "retry_attempt_id")
    _require_uuid_command(resulting_execution_id, "resulting_execution_id")
    with composed_transaction(pool) as transaction_pool:
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = locked
        if task.archived_at is not None:
            raise StaleOperationError("the task has already been archived; the operation is stale")
        if task.state_token != expected_state_token:
            raise StaleOperationError(
                "the task state has moved on since this operation was prepared; "
                "the operation is stale"
            )
        attempt = failure_records.get_operational_failure_attempt(
            transaction_pool, attempt_id=retry_attempt_id
        )
        if attempt is None or attempt.workspace_id != workspace_id or attempt.task_id != task_id:
            raise NotFoundError("the requested retry attempt is not available for this task")
        episode = failure_records.get_operational_failure_episode(
            transaction_pool, episode_id=attempt.episode_id
        )
        if episode is None or episode.escalated_task_block_id is not None:
            raise ConflictError(
                "the retry attempt's failure episode is terminally disposed; "
                "the authorization can no longer be executed"
            )
        execution = execution_records.get_execution_for_update(
            transaction_pool, execution_id=resulting_execution_id
        )
        if (
            execution is None
            or execution.workspace_id != workspace_id
            or execution.task_id != task_id
        ):
            raise NotFoundError("the requested resulting execution is not available for this task")
        if attempt.resulting_execution_id is not None:
            if attempt.resulting_execution_id == resulting_execution_id:
                return attempt
            raise ConflictError(
                "the retry attempt is already bound to a different resulting execution"
            )
        bound = failure_records.bind_retry_attempt_execution(
            transaction_pool,
            attempt_id=retry_attempt_id,
            task_id=task_id,
            workspace_id=workspace_id,
            resulting_execution_id=resulting_execution_id,
        )
        if bound is None:
            raise ConflictError(
                "the retry attempt could not record its resulting execution; "
                "the binding composition is stale"
            )
        return bound


def _triage_locked(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    expected_status: TaskStatus,
    expected_current_owner_gate_id: UUID | None,
    actor: WorkflowActorContext,
    operation: OperationalFailureOperation,
    report: OperationalFailureReport,
    now: Callable[[], datetime],
) -> OperationalFailureDisposition:
    """The locked decision flow: every verification and write under the Task lock."""
    # The Task row lock first; every verification and write in this
    # composition happens under this lock.
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    task = locked
    # Every stale fact is a typed no-op: a delayed, replayed, or moved-on
    # failure report never applies to newer state and never blocks healthy
    # or superseded work.
    if task.archived_at is not None:
        return _stale_no_op()
    if task.state_token != expected_state_token:
        return _stale_no_op()
    if task.status is not expected_status:
        return _stale_no_op()
    if task.current_owner_gate_id != expected_current_owner_gate_id:
        return _stale_no_op()

    # Locate the failure episode rooted at the original durable attempt
    # identity. An existing episode is authoritative; a terminal one makes
    # every further report about it a no-op, whatever happened to the
    # TaskBlock afterwards.
    episode = failure_records.get_operational_failure_episode_by_original_attempt(
        transaction_pool,
        original_attempt_kind=operation.original_attempt_kind,
        original_attempt_id=operation.original_attempt_id,
    )
    attempts: list[OperationalFailureAttempt] = []
    if episode is not None:
        if (
            episode.workspace_id != workspace_id
            or episode.task_id != task_id
            or episode.operation_family is not operation.family
        ):
            return _stale_no_op()
        if episode.escalated_task_block_id is not None:
            return OperationalFailureDisposition(
                kind=DispositionKind.NO_OP,
                no_op_reason=NoOpReason.ALREADY_TERMINALLY_DISPOSED,
                existing_task_block_id=episode.escalated_task_block_id,
            )
        attempts = failure_records.list_operational_failure_attempts(
            transaction_pool, episode_id=episode.id
        )

    # Duplicate or already-disposed report: the unique backstop's read view.
    # A repeated report returns the existing disposition echo, never a
    # second successor or a budget reset.
    existing_successor = failure_records.get_operational_failure_attempt_by_source(
        transaction_pool,
        source_kind=report.reporting_attempt_kind,
        source_attempt_id=report.reporting_attempt_id,
    )
    if existing_successor is not None:
        return OperationalFailureDisposition(
            kind=DispositionKind.NO_OP,
            no_op_reason=NoOpReason.ALREADY_DISPOSED,
            existing_attempt_id=existing_successor.id,
        )

    # A prior-authorized report must address an existing authorized attempt
    # of exactly this episode whose consumed authorization was actually
    # executed — and whose durable resulting-Execution binding matches the
    # reported effect row exactly. An unbound or mismatched effect never
    # authorizes another retry.
    reported_attempt: OperationalFailureAttempt | None = None
    if report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED:
        reported_attempt = next((a for a in attempts if a.id == report.reporting_attempt_id), None)
        if (
            reported_attempt is None
            or episode is None
            or reported_attempt.resulting_execution_id is None
            or reported_attempt.resulting_execution_id != report.effect_attempt_id
        ):
            return _stale_no_op()

    # Per-family verification under the evidence locks: exact subject
    # currentness, outcome non-contradiction, and binding applicability.
    verified = _verify_family(
        transaction_pool,
        task=task,
        workspace_id=workspace_id,
        task_id=task_id,
        operation=operation,
        report=report,
    )
    if isinstance(verified, OperationalFailureDisposition):
        return verified

    # Eligibility: the explicit small matrix, evaluated only after the
    # report was proven current.
    effect_execution_id = (
        report.effect_attempt_id
        if report.effect_attempt_id is not None
        else operation.original_attempt_id
    )
    next_ordinal = reported_attempt.ordinal + 1 if reported_attempt is not None else 1
    retry_eligible = (
        operation.family in _RETRY_ELIGIBLE_FAMILIES
        and report.error_code in _RETRY_ELIGIBLE_CODES
        and report.effect_evidence is FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
        and next_ordinal <= 3
        and not verified.session_lost
    )
    if retry_eligible:
        # Positive durable corroboration is part of the proof: a bare
        # finalized status or a caller classification alone never authorizes.
        retry_eligible = _corroborate_pre_effect_execution_failure(
            transaction_pool,
            workspace_id=workspace_id,
            execution_id=effect_execution_id,
        )
    if retry_eligible:
        if episode is None:
            episode = failure_records.create_operational_failure_episode(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                operation_family=operation.family,
                original_attempt_kind=operation.original_attempt_kind,
                original_attempt_id=operation.original_attempt_id,
                subject=operation.subject,
            )
        attempt = failure_records.create_operational_failure_attempt(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            episode_id=episode.id,
            ordinal=next_ordinal,
            source_kind=report.reporting_attempt_kind,
            source_attempt_id=report.reporting_attempt_id,
            error_code=report.error_code,
            not_before=now() + _RETRY_BACKOFF[next_ordinal],
        )
        return OperationalFailureDisposition(
            kind=DispositionKind.RETRY_AUTHORIZED, retry_attempt=attempt
        )
    return _escalate(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        task=task,
        actor=actor,
        operation=operation,
        report=report,
        episode=episode,
        authorized_attempt_count=len(attempts),
        session_lost=verified.session_lost,
    )


def _verify_family(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    workspace_id: UUID,
    task_id: UUID,
    operation: OperationalFailureOperation,
    report: OperationalFailureReport,
) -> _VerifiedFailure | OperationalFailureDisposition:
    """Dispatch to the family's disposition-time verification."""
    family = operation.family
    if family is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE:
        return _verify_bootstrap_family(
            transaction_pool,
            task=task,
            task_id=task_id,
            operation=operation,
            expect_prepared_stage=False,
        )
    if family is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION:
        return _verify_bootstrap_family(
            transaction_pool,
            task=task,
            task_id=task_id,
            operation=operation,
            expect_prepared_stage=True,
        )
    if family in _FAMILY_CLAIM_KINDS:
        return _verify_send_claim_family(
            transaction_pool,
            task=task,
            workspace_id=workspace_id,
            task_id=task_id,
            operation=operation,
        )
    if family is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW:
        return _verify_reviewer_review_family(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            operation=operation,
        )
    return _verify_execution_family(
        transaction_pool,
        task=task,
        workspace_id=workspace_id,
        task_id=task_id,
        operation=operation,
        report=report,
    )


def _binding_session_lost(session_lifecycle: TaskSessionLifecycleStatus) -> bool:
    """Whether the locked binding is durably terminal (LOST or ENDED)."""
    return session_lifecycle in (
        TaskSessionLifecycleStatus.LOST,
        TaskSessionLifecycleStatus.ENDED,
    )


def _require_applicable_binding(
    transaction_pool: DatabasePool,
    *,
    task_id: UUID,
    role: WorkflowRole,
    expected_session_id: UUID | None,
) -> _VerifiedFailure | OperationalFailureDisposition:
    """Verify the role's current binding is applicable to the reported attempt.

    The lifecycle status moves through conditional updates that never rotate
    the Task state token, so the row is read under an exclusive lock. A
    missing binding or a bound-but-different session identity means the
    report no longer addresses the current session authority (a terminal
    binding is never replaced); a terminal binding keeps the report current
    but makes session loss the durably proven blocking fact.
    """
    binding = session_records.get_task_agent_session_for_update(
        transaction_pool, task_id=task_id, role=role
    )
    if binding is None:
        return _stale_no_op()
    if expected_session_id is not None and binding.id != expected_session_id:
        return _stale_no_op()
    return _VerifiedFailure(session_lost=_binding_session_lost(binding.lifecycle_status))


def _review_iteration_superseded(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    iteration: ReviewIteration,
) -> OperationalFailureDisposition | None:
    """Whether a review iteration's loop moved on past the reported subject.

    The iteration must sit in its still-OPEN loop with no later iteration:
    a later routed iteration means this subject's dispatch already completed
    or the loop moved on, so a failure report about it is stale.
    """
    loop = review_records.get_review_loop(transaction_pool, review_loop_id=iteration.review_loop_id)
    if (
        loop is None
        or loop.workspace_id != workspace_id
        or loop.task_id != iteration.task_id
        or loop.status is not ReviewLoopStatus.OPEN
    ):
        return _stale_no_op()
    later = any(
        other.iteration_number > iteration.iteration_number
        for other in review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=iteration.review_loop_id
        )
    )
    if later:
        return _stale_no_op()
    return None


def _verify_send_claim_family(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    workspace_id: UUID,
    task_id: UUID,
    operation: OperationalFailureOperation,
) -> _VerifiedFailure | OperationalFailureDisposition:
    """Verify one F2 dispatch family's claim, subject currency, and binding.

    The claim row is locked because it settles through conditional updates
    that never rotate the Task token. A settled claim contradicts any
    failure report: the operation completed. The per-kind subject checks
    mirror the exact subject currency each dispatch service proves at send
    time — an already-claimed send is never re-dispatched, so every
    disposition here is escalation or no-op, never a retry.
    """
    claim = send_claim_records.get_task_runtime_send_claim_for_update(
        transaction_pool, claim_id=operation.original_attempt_id
    )
    if claim is None or claim.workspace_id != workspace_id or claim.task_id != task_id:
        return _stale_no_op()
    if claim.kind is not _FAMILY_CLAIM_KINDS[operation.family]:
        return _stale_no_op()
    if claim.status is TaskRuntimeSendClaimStatus.SETTLED:
        return _stale_no_op()
    family = operation.family
    if family is PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH:
        # The dispatch is current only while no plan revision has been
        # published for the Task's planning phase.
        if task.status is not TaskStatus.PLANNING or task.current_plan_revision_id is not None:
            return _stale_no_op()
    elif family is PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH:
        # The routed change request must still be current: its prior
        # revision is still the Task's current plan revision, and the
        # iteration is the finalized CHANGES_REQUESTED subject of its
        # still-OPEN loop with no later iteration.
        if claim.review_iteration_id is None:
            return _stale_no_op()
        iteration = review_records.get_review_iteration(
            transaction_pool, review_iteration_id=claim.review_iteration_id
        )
        if (
            iteration is None
            or iteration.workspace_id != workspace_id
            or iteration.task_id != task_id
            or iteration.outcome is not ReviewOutcome.CHANGES_REQUESTED
            or task.current_plan_revision_id != iteration.plan_revision_id
        ):
            return _stale_no_op()
        stale = _review_iteration_superseded(
            transaction_pool, workspace_id=workspace_id, iteration=iteration
        )
        if stale is not None:
            return stale
    else:
        # The compose is current only while the Task carries no current gate
        # and the claim's historical APPROVED PR_AUTHORIZATION gate still
        # carries the exact approved head SHA the claim recorded.
        if task.current_owner_gate_id is not None:
            return _stale_no_op()
        if claim.owner_gate_id is None:
            return _stale_no_op()
        gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=claim.owner_gate_id)
        if (
            gate is None
            or gate.workspace_id != workspace_id
            or gate.task_id != task_id
            or gate.gate_type is not OwnerGateType.PR_AUTHORIZATION
            or gate.status is not OwnerGateStatus.APPROVED
            or gate.subject_head_sha != claim.subject_head_sha
        ):
            return _stale_no_op()
    return _require_applicable_binding(
        transaction_pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        expected_session_id=claim.task_agent_session_id,
    )


def _verify_bootstrap_family(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    task_id: UUID,
    operation: OperationalFailureOperation,
    expect_prepared_stage: bool,
) -> _VerifiedFailure | OperationalFailureDisposition:
    """Verify one planning-bootstrap family's stage evidence and binding.

    The bootstrap stage advances independently of the Task token, so the
    evidence is re-read under the Task lock: the reported stage must still
    be the outstanding one. A prepared stage contradicts a PREPARE-control
    failure (its validated result already landed); a ready_for_planning
    stage or a published plan revision means the bootstrap resolved and no
    report about it is current any more.
    """
    evidence = task_records.get_task_planning_bootstrap_evidence(transaction_pool, task_id=task_id)
    if evidence is None or evidence.claim_token != operation.original_attempt_id:
        return _stale_no_op()
    if task.status is not TaskStatus.PLANNING or task.current_plan_revision_id is not None:
        return _stale_no_op()
    if expect_prepared_stage:
        # The transition is outstanding only while the validated PREPARE
        # result has settled the prepared stage and the effectful
        # planning-context transition has not.
        if evidence.stage is not TaskPlanningBootstrapStage.PREPARED:
            return _stale_no_op()
    else:
        # The PREPARE control is outstanding only while no stage has settled.
        if evidence.stage is not None:
            return _stale_no_op()
    return _require_applicable_binding(
        transaction_pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        expected_session_id=None,
    )


def _verify_reviewer_review_family(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    operation: OperationalFailureOperation,
) -> _VerifiedFailure | OperationalFailureDisposition:
    """Verify the reserved ReviewIteration's currency and the Reviewer binding.

    The reserved unfinalized iteration is the outstanding REVIEW dispatch's
    durable identity. A finalized outcome contradicts the failure report
    (the review completed); a moved-on loop or a later iteration means the
    subject was superseded.
    """
    iteration = review_records.get_review_iteration(
        transaction_pool, review_iteration_id=operation.original_attempt_id
    )
    if (
        iteration is None
        or iteration.workspace_id != workspace_id
        or iteration.task_id != task_id
        or iteration.outcome is not None
    ):
        return _stale_no_op()
    stale = _review_iteration_superseded(
        transaction_pool, workspace_id=workspace_id, iteration=iteration
    )
    if stale is not None:
        return stale
    return _require_applicable_binding(
        transaction_pool,
        task_id=task_id,
        role=WorkflowRole.REVIEWER,
        expected_session_id=None,
    )


def _resolve_approved_implementation_gate(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    revision_id: UUID,
) -> OwnerGate | None:
    """Resolve the historical APPROVED IMPLEMENTATION_AUTHORIZATION gate.

    The post-approval implementation subject is the E5-approved historical
    gate bound to the exact current plan revision, resolved from durable
    Task gate history — never from a current gate pointer (the Task carries
    none after authorization). Exactly one approved gate per revision is
    the coherent history; anything else fails closed as stale.
    """
    gates = [
        gate
        for gate in gate_records.list_task_owner_gates(transaction_pool, task_id=task_id)
        if gate.workspace_id == workspace_id
        and gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
        and gate.status is OwnerGateStatus.APPROVED
        and gate.plan_revision_id == revision_id
    ]
    if len(gates) != 1:
        return None
    return gates[0]


def _verify_execution_family(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    workspace_id: UUID,
    task_id: UUID,
    operation: OperationalFailureOperation,
    report: OperationalFailureReport,
) -> _VerifiedFailure | OperationalFailureDisposition:
    """Verify one execution family's attempt, subject currency, and binding.

    The execution row is locked because it finalizes through conditional
    updates that never rotate the Task token. Each family mirrors its own
    Level 1 service's subject contract:

    - implementation execution: an ``IMPLEMENTING`` Task with no current
      gate, the exact current plan revision, and the historical APPROVED
      IMPLEMENTATION_AUTHORIZATION gate bound to that revision;
    - PR remediation execution: a ``REVIEWING`` Task whose canonical
      TaskPullRequest still carries exactly the claimed CHANGES_REQUESTED
      ReviewIteration's reviewed head — the head is the remediation subject.

    A succeeded attempt contradicts any failure report.
    """
    effect_execution_id = (
        report.effect_attempt_id
        if report.effect_attempt_id is not None
        else operation.original_attempt_id
    )
    execution = execution_records.get_execution_for_update(
        transaction_pool, execution_id=effect_execution_id
    )
    if execution is None or execution.workspace_id != workspace_id or execution.task_id != task_id:
        return _stale_no_op()
    if execution.status is ExecutionStatus.SUCCEEDED:
        return _stale_no_op()
    if operation.family is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION:
        if execution.review_iteration_id is not None:
            return _stale_no_op()
        if task.status is not TaskStatus.IMPLEMENTING or task.current_owner_gate_id is not None:
            return _stale_no_op()
        if task.current_plan_revision_id is None:
            return _stale_no_op()
        revision = require_current_plan_revision(
            transaction_pool, task=task, plan_revision_id=task.current_plan_revision_id
        )
        gate = _resolve_approved_implementation_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            revision_id=revision.id,
        )
        if gate is None:
            return _stale_no_op()
    else:
        if execution.review_iteration_id is None:
            return _stale_no_op()
        iteration = review_records.get_review_iteration(
            transaction_pool, review_iteration_id=execution.review_iteration_id
        )
        if (
            iteration is None
            or iteration.workspace_id != workspace_id
            or iteration.task_id != task_id
            or iteration.outcome is not ReviewOutcome.CHANGES_REQUESTED
        ):
            return _stale_no_op()
        loop = review_records.get_review_loop(
            transaction_pool, review_loop_id=iteration.review_loop_id
        )
        if (
            loop is None
            or loop.workspace_id != workspace_id
            or loop.task_id != task_id
            or loop.purpose is not ReviewLoopPurpose.PR_REVIEW
            or loop.status is not ReviewLoopStatus.OPEN
        ):
            return _stale_no_op()
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        if any(other.iteration_number > iteration.iteration_number for other in iterations):
            # A later routed iteration means this remediation already resolved.
            return _stale_no_op()
        if task.status is not TaskStatus.REVIEWING:
            return _stale_no_op()
        # The remediation subject is the exact current PR head: the canonical
        # TaskPullRequest must still carry exactly the addressed iteration's
        # reviewed head, and the iteration must still be the last finalized
        # result on that head — the E2 supersession rule the Level 1
        # remediation service proves before any dispatch. A moved head or a
        # newer same-head result supersedes the reported attempt.
        pull_request = pull_request_records.get_task_pull_request_for_task(
            transaction_pool, task_id=task_id
        )
        if (
            pull_request is None
            or pull_request.workspace_id != workspace_id
            or pull_request.task_id != task_id
            or pull_request.head_sha != iteration.reviewed_head_sha
            or pull_request.id != iteration.task_pull_request_id
        ):
            return _stale_no_op()
        finalized_on_head = [
            other
            for other in iterations
            if other.outcome is not None
            and other.task_pull_request_id == pull_request.id
            and other.reviewed_head_sha == pull_request.head_sha
        ]
        if not finalized_on_head or finalized_on_head[-1].id != iteration.id:
            return _stale_no_op()
    return _require_applicable_binding(
        transaction_pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        expected_session_id=execution.producer_session_id,
    )


def _corroborate_pre_effect_execution_failure(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    execution_id: UUID,
) -> bool:
    """Whether durable audit evidence agrees with a claimed pre-effect failure.

    The retry branch requires positive proof beyond the caller's
    classification: the ``execution_failed`` audit event of the exact
    reported attempt must exist and carry the safe machine failure-kind the
    Level 1 seams record for a positively proven pre-effect runtime failure.
    The audit history is used as historical evidence for a finalized
    attempt, never as current-workflow-state reconstruction. Missing or
    disagreeing evidence fails closed (never a retry).
    """
    events = event_records.find_events_by_subject(
        transaction_pool,
        workspace_id=workspace_id,
        subject_type=_EXECUTION_SUBJECT_TYPE,
        subject_id=execution_id,
        limit=1,
    )
    for event in events:
        if event.event_type is not WorkflowEventType.EXECUTION_FAILED:
            continue
        kind = event.context.get("failure_kind")
        return isinstance(kind, str) and kind == _RUNTIME_PRE_EFFECT_FAILURE_KIND
    return False


def _escalate(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task: Task,
    actor: WorkflowActorContext,
    operation: OperationalFailureOperation,
    report: OperationalFailureReport,
    episode: OperationalFailureEpisode | None,
    authorized_attempt_count: int,
    session_lost: bool,
) -> OperationalFailureDisposition:
    """Escalate atomically through F3 and stamp the episode's terminal disposition.

    The F3 installation owns the one guarded Task mutation, the pending
    exceptional gate, and both audit events inside its nested savepoint; the
    episode's exact TaskBlock reference is stamped set-once in the same
    composition. A rejected stamp is a durable-state invariant failure that
    fails closed: the whole composition rolls back.
    """
    if episode is None:
        episode = failure_records.create_operational_failure_episode(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            operation_family=operation.family,
            original_attempt_kind=operation.original_attempt_kind,
            original_attempt_id=operation.original_attempt_id,
            subject=operation.subject,
        )
    context: dict[str, object] = {
        "error_code": report.error_code.value,
        "effect_evidence": report.effect_evidence.value,
        "operation_family": operation.family.value,
        "original_attempt_kind": operation.original_attempt_kind.value,
        "original_attempt_id": str(operation.original_attempt_id),
        "failure_episode_id": str(episode.id),
        "reporting_attempt_kind": report.reporting_attempt_kind.value,
        "reporting_attempt_id": str(report.reporting_attempt_id),
        "authorized_retry_count": authorized_attempt_count,
    }
    if report.effect_attempt_id is not None:
        context["effect_attempt_id"] = str(report.effect_attempt_id)
    if report.diagnostic_context:
        context["diagnostics"] = dict(report.diagnostic_context)
    block_reason = (
        TaskBlockReason.AGENT_SESSION_LOST
        if session_lost
        else _ESCALATION_REASONS[report.error_code]
    )
    installation = install_exceptional_block_gate(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=task.state_token,
        expected_status=task.status,
        expected_current_owner_gate_id=task.current_owner_gate_id,
        gate_type=OwnerGateType.BLOCK_RESOLUTION,
        block_reason=block_reason,
        block_context=canonical_task_block_context(context),
        actor=actor,
    )
    marked = failure_records.mark_episode_escalated(
        transaction_pool,
        episode_id=episode.id,
        task_id=task_id,
        workspace_id=workspace_id,
        escalated_task_block_id=installation.task_block.id,
    )
    if marked is None:
        raise ConflictError(
            "the failure episode could not record its terminal disposition; "
            "the escalation composition is stale"
        )
    return OperationalFailureDisposition(kind=DispositionKind.BLOCKED, installation=installation)
