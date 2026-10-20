"""E6: authorized Producer execution and the accepted-review handoff (issue #81).

E6 reproduces exactly one proven manual step: after the human has approved
proceeding at the E5 gate (issue #80), the persisted accepted Reviewer
``review_result`` envelope is delivered to the **existing** Producer Cline
session, the session is natively switched PLAN→ACT on the same external
session ID, and the single model-visible implementation message is the
structural serialization of that accepted envelope. The service records one
Execution attempt and its outcome safely. It performs no additional plan
text, no separate implementation instructions, and no second conversational
context.

The authority distinction is crucial and enforced here: **Reviewer ACCEPTED
is the message delivered to the Producer; Owner APPROVED is permission for
OpenOrc to deliver it.** A Reviewer result alone never starts
implementation.

Composition (authorization precedes every adapter acquisition; no database
transaction spans an attachment or any runtime call):

1. **Admission and durable attempt reservation** (one short transaction):
   account-operational and Workspace-Owner scope, the exact current Task
   under the caller's ``expected_state_token``, a non-archived
   ``implementing`` Task with no current gate and an unchanged current
   PlanRevision, the E5-approved historical IMPLEMENTATION_AUTHORIZATION
   gate bound to exactly that plan, the CLOSED planning ReviewLoop whose
   latest finalized iteration for that revision is a Reviewer ACCEPTED, and
   the existing READY Producer binding — all derived from durable state
   under the Task row lock, never from caller-supplied acceptance. The
   initial-implementation exclusivity check refuses when any prior attempt
   exists (concurrent/replayed starts, earlier successes, unresolved
   dispatches, and older failed attempts alike: retries belong to the later
   recovery layer). ``create_execution`` reserves attempt 1 and the focused
   claim moves it ``queued → running`` with the F5 activity marker born in
   the same statement, committing the dispatch claim before any effectful
   ACT transition or send, with the safe Producer-attributed
   ``execution_started`` audit recorded atomically.
2. **The authority-gated external effects** (no transaction open): the
   deferred Producer attachment is acquired first, then the exact authorized
   subject set is **revalidated** (R1) before the native ``ImplementInteraction``
   realization on the same external session ID — an authority change during
   attachment refuses reconstruction and finalizes the never-dispatched
   attempt truthfully. After realization the optional D6c activity
   subscription is established, the exact subject set is revalidated again
   (R2) immediately before the one send, and the send carries the canonical
   accepted envelope with ``expected_family=IMPLEMENTATION_RESULT_FAMILY``,
   expecting exactly one typed ``ImplementationResult`` (whose schema pins
   ``status`` to ``COMPLETED``).
3. **The one-shot finalize** (one short transaction): the exact subject set
   and the still-running attempt are re-proven under the Task row lock, the
   validated canonical result payload and the ``succeeded`` stamp commit in
   one conditional statement, and the Producer-attributed
   ``execution_completed`` audit commits atomically. The result's ``branch``
   is a Producer discovery/routing claim only — never canonical Task branch
   or GitHub head state; E7 (issue #82) owns reconciliation and
   PR_AUTHORIZATION, and the Task is never advanced here.

Failure discipline: a known failure that provably occurred before any
runtime effect finalizes the attempt ``failed_transient``/``failed_final``
with a safe ``execution_failed`` audit; every post-claim uncertainty —
uncertain attachment or ACT reconstruction, timeout/disconnect, session
loss, invalid or nonmatching formal completion after possible work, a stale
authority discovery after realization, or a finalize that cannot commit an
already-returned result — leaves the attempt conservatively ``running``
(the durable potentially-dispatched record), reports a typed content-safe
error, and never resends, never fabricates success, and never grants retry
permission. Settlement belongs to the later recovery layer (#85/#87–#89).

The activity marker (issue #90's consumer) is advisory only: qualifying
exact-session observations (thinking, iteration/work polling, tool
lifecycle, non-formal commentary) advance ``last_qualifying_activity_at``
monotonically with database time while the attempt is running; passive
status/idle/ended notices, usage snapshots, and other sessions never do;
unavailable telemetry changes nothing; late callbacks cannot write finalized
history. No watchdog, scheduler, second telemetry pipeline, or
adapter-independent observation framework is built here, and no
RuntimeRequest approval routing, abort engine, or retry engine is installed.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from typing import Final, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.cline.activity import ClineActivityKind, ClineActivityObservation
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import Execution, ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import reviews as review_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import ImplementInteraction
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    SCHEMA_VERSION,
    ImplementationResult,
    ReviewResult,
)
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.review_result_envelope import (
    accepted_review_result_from_iteration,
    serialize_review_result,
)
from openorc.services.task_subject_guards import (
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "ActivitySubscriptionFactory",
    "AuthorizedImplementationExecution",
    "ProducerRuntimeAttachmentFactory",
    "execute_authorized_implementation",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_authorized_implementation_execution"
_EXECUTE_SPAN_NAME: Final[str] = "task_authorized_implementation_execution.execute"

# Safe, stable failure-classification tokens for the ``execution_failed``
# audit context (no exception text or error content ever enters the event).
_AUTHORITY_STALE_FAILURE_KIND: Final[str] = "authority_stale"
_RUNTIME_PRE_EFFECT_FAILURE_KIND: Final[str] = "runtime_pre_effect"

# The qualifying-activity vocabulary (issue #81, consumed by F5 #90):
# genuine implementation activity only. Passive runtime status/idle/ended
# notices, usage snapshots, and synthetic connection heartbeats never
# advance the marker.
_QUALIFYING_ACTIVITY_KINDS: Final[frozenset[ClineActivityKind]] = frozenset(
    {
        ClineActivityKind.THINKING_STARTED,
        ClineActivityKind.THINKING_ENDED,
        ClineActivityKind.COMMENTARY,
        ClineActivityKind.TOOL_STARTED,
        ClineActivityKind.TOOL_PROGRESS,
        ClineActivityKind.TOOL_COMPLETED,
        ClineActivityKind.TOOL_FAILED,
        ClineActivityKind.ITERATION_STARTED,
        ClineActivityKind.ITERATION_ENDED,
    }
)
_ACTIVITY_WRITE_MIN_INTERVAL_SECONDS: Final[float] = 2.0

# --- typed seams and outcomes -----------------------------------------------------

ProducerRuntimeAttachmentFactory = Callable[[], AbstractContextManager[AgentRuntimeAdapter]]
ActivitySubscriptionFactory = Callable[
    [AgentRuntimeAdapter, str, Callable[[ClineActivityObservation], None]],
    AbstractContextManager[None],
]


@dataclass(frozen=True, slots=True)
class AuthorizedImplementationExecution:
    """The typed successful E6 outcome, suitable for E7 (issue #82).

    Carries the finalized ``succeeded`` Execution with its persisted
    validated result, the typed ``ImplementationResult`` (whose ``branch``
    is a discovery/routing claim only), the post-write Task, the exact
    Producer binding, and the E5-approved historical gate as the durable
    authority evidence.
    """

    task: Task
    execution: Execution
    implementation_result: ImplementationResult
    producer_session: TaskAgentSession
    owner_gate: OwnerGate


@dataclass(frozen=True, slots=True)
class _ReservedAttempt:
    """The durable reservation produced by the admission transaction."""

    task: Task
    owner_gate: OwnerGate
    plan_revision: PlanRevision
    review_loop: ReviewLoop
    review_iteration: ReviewIteration
    accepted_review: ReviewResult
    producer_session: TaskAgentSession
    execution: Execution


class _QualifyingActivityHeartbeat:
    """The coalescing F5 marker listener for one running attempt.

    Qualifying exact-session observations advance the Execution's
    ``last_qualifying_activity_at`` through a short conditional write;
    passive kinds, other sessions, and late callbacks after finalization
    are dropped. Every failure — subscription, observation, or write — is
    advisory and must never alter workflow authority or break the attempt.
    """

    def __init__(self, pool: DatabasePool, *, execution_id: UUID, external_session_id: str) -> None:
        self._pool = pool
        self._execution_id = execution_id
        self._external_session_id = external_session_id
        self._last_write_monotonic: float | None = None

    def __call__(self, observation: ClineActivityObservation) -> None:
        if observation.session_id != self._external_session_id:
            return
        if observation.kind not in _QUALIFYING_ACTIVITY_KINDS:
            return
        now = time.monotonic()
        if (
            self._last_write_monotonic is not None
            and now - self._last_write_monotonic < _ACTIVITY_WRITE_MIN_INTERVAL_SECONDS
        ):
            return
        self._last_write_monotonic = now
        try:
            execution_records.record_execution_activity(self._pool, execution_id=self._execution_id)
        except Exception:  # noqa: BLE001 - advisory telemetry never breaks the attempt
            logger.debug(
                "the execution activity marker write failed; the observation is "
                "advisory and dropped"
            )


# --- command validation -----------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_producer_factory(value: object) -> ProducerRuntimeAttachmentFactory:
    """Require the deferred Producer attachment factory dependency."""
    if not callable(value):
        raise InvalidCommandError(
            "producer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Producer session"
        )
    return cast(ProducerRuntimeAttachmentFactory, value)


def _require_activity_factory(value: object) -> ActivitySubscriptionFactory | None:
    """Require the optional D6c activity subscription factory dependency."""
    if value is None:
        return None
    if not callable(value):
        raise InvalidCommandError(
            "activity_subscription_factory must be a callable returning one "
            "subscription context manager for the exact bound Producer session"
        )
    return cast(ActivitySubscriptionFactory, value)


# --- subject derivation -----------------------------------------------------------


def _require_implementing_task(task: Task) -> None:
    """Prove the addressed Task is a non-archived implementing Task."""
    if task.archived_at is not None or task.status is not TaskStatus.IMPLEMENTING:
        raise ConflictError("the addressed task is not a non-archived implementing task")
    if task.current_owner_gate_id is not None:
        raise ConflictError(
            "the task carries a current owner gate; implementation currentness has moved on"
        )


def _resolve_approved_implementation_gate(
    transaction_pool: DatabasePool, *, task: Task, revision_id: UUID
) -> OwnerGate:
    """Resolve the E5-approved historical gate bound to the exact plan.

    The gate is resolved from durable Task gate history — never from a
    detached gate status — and must be the Owner-approved
    IMPLEMENTATION_AUTHORIZATION gate whose subject is precisely the Task's
    current PlanRevision. Superseded pending history (#219), cancelled, and
    gates for other revisions never authorize anything.
    """
    gates = gate_records.list_task_owner_gates(transaction_pool, task_id=task.id)
    approved = [
        gate
        for gate in gates
        if gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
        and gate.status is OwnerGateStatus.APPROVED
        and gate.plan_revision_id == revision_id
    ]
    if not approved:
        raise ConflictError(
            "no Owner-approved implementation authorization gate exists for the task's "
            "current plan; implementation is not authorized"
        )
    return max(approved, key=lambda gate: (gate.created_at, gate.id))


def _resolve_closed_planning_loop(transaction_pool: DatabasePool, *, task: Task) -> ReviewLoop:
    """Resolve the single CLOSED planning ReviewLoop following E5 approval."""
    closed = review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.CLOSED
    )
    planning = [loop for loop in closed if loop.purpose is ReviewLoopPurpose.PLANNING]
    if len(planning) != 1:
        raise ConflictError(
            "the task does not carry exactly one closed planning review loop following "
            "the Owner's implementation authorization"
        )
    return planning[0]


def _resolve_accepted_iteration(
    transaction_pool: DatabasePool, *, loop: ReviewLoop, revision_id: UUID
) -> ReviewIteration:
    """Prove the latest finalized iteration for the exact revision is ACCEPTED."""
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    finalized = [
        iteration
        for iteration in iterations
        if iteration.decided_at is not None and iteration.plan_revision_id == revision_id
    ]
    if not finalized:
        raise ConflictError(
            "no finalized review iteration exists for the task's current plan revision"
        )
    latest = max(finalized, key=lambda iteration: iteration.iteration_number)
    if latest.outcome is not ReviewOutcome.ACCEPTED:
        raise ConflictError(
            "the latest finalized review iteration for the current plan is not a Reviewer "
            "acceptance; the persisted review evidence does not authorize implementation"
        )
    return latest


def _resolve_producer_session(
    transaction_pool: DatabasePool, *, task: Task, require_ready: bool
) -> TaskAgentSession:
    """Resolve the existing Producer binding — never allocated or replaced.

    The binding is the single Task/Producer session row with its immutable
    external session identity. ``require_ready`` selects whether the
    lifecycle must still be READY (admission and pre-send revalidations) or
    whether binding identity alone is re-proven (the finalize recheck, which
    must not refuse a valid result because the session later ended).
    """
    session = session_records.get_task_agent_session(
        transaction_pool, task_id=task.id, role=WorkflowRole.PRODUCER
    )
    if session is None or session.task_id != task.id or session.workspace_id != task.workspace_id:
        raise NotFoundError("the requested producer session is not available for this task")
    if session.external_session_id is None or not session.external_session_id.strip():
        raise ConflictError("the producer session binding carries no external session identity")
    if require_ready and session.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError("the producer session binding is not ready")
    return session


def _load_authorized_subjects(
    transaction_pool: DatabasePool, *, task: Task, require_ready_producer: bool
) -> tuple[OwnerGate, PlanRevision, ReviewLoop, ReviewIteration, TaskAgentSession]:
    """Derive the complete implementation authority from canonical state.

    Every fact is resolved from durable Task-bound state under the caller's
    Task row lock — never from a caller-supplied acceptance, a detached
    Owner-gate status, or a submitted JSON document detached from its
    durable Task/plan relationships.
    """
    if task.current_plan_revision_id is None:
        raise ConflictError("the task carries no current plan revision")
    revision = require_current_plan_revision(
        transaction_pool, task=task, plan_revision_id=task.current_plan_revision_id
    )
    gate = _resolve_approved_implementation_gate(
        transaction_pool, task=task, revision_id=revision.id
    )
    loop = _resolve_closed_planning_loop(transaction_pool, task=task)
    iteration = _resolve_accepted_iteration(transaction_pool, loop=loop, revision_id=revision.id)
    producer_session = _resolve_producer_session(
        transaction_pool, task=task, require_ready=require_ready_producer
    )
    return gate, revision, loop, iteration, producer_session


# --- reservation and revalidation -------------------------------------------------


def _reserve_initial_attempt(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
) -> _ReservedAttempt:
    """Admission and the durable attempt reservation, under the Task lock."""
    require_account_operational(transaction_pool, profile_id=profile_id)
    require_workspace_task(
        transaction_pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    task = require_current_task(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    _require_implementing_task(task)
    gate, revision, loop, iteration, producer_session = _load_authorized_subjects(
        transaction_pool, task=task, require_ready_producer=True
    )
    accepted_review = accepted_review_result_from_iteration(iteration)
    history = execution_records.list_task_executions(transaction_pool, task_id=task.id)
    if history:
        # Initial-implementation exclusivity: a concurrent/replayed start, an
        # earlier successful implementation, an unresolved prior dispatch, and
        # an older failed attempt alike refuse here. A retry or recovery is a
        # new attempt owned by the later explicit recovery layer, never an
        # automatic second dispatch from this seam.
        raise ConflictError(
            "the task already carries an implementation attempt; the initial authorized "
            "implementation dispatch is not available"
        )
    execution = execution_records.create_execution(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task.id,
        producer_session_id=producer_session.id,
        execution_number=1,
    )
    claimed = execution_records.claim_execution_running(transaction_pool, execution_id=execution.id)
    if claimed is None:
        raise StaleOperationError(
            "the reserved implementation attempt could not be claimed; the dispatch claim is stale"
        )
    event_coordination.record_execution_started_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task.id,
        execution_id=claimed.id,
        actor=event_coordination.producer_actor(producer_session.id),
    )
    return _ReservedAttempt(
        task=task,
        owner_gate=gate,
        plan_revision=revision,
        review_loop=loop,
        review_iteration=iteration,
        accepted_review=accepted_review,
        producer_session=producer_session,
        execution=claimed,
    )


def _revalidate_reserved_subjects(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    reserved: _ReservedAttempt,
    require_ready_producer: bool,
) -> Task:
    """Re-prove the exact authorized subject set under the Task row lock.

    Attachment and ACT reconstruction take wall-clock time, so every
    consequential runtime effect is preceded by this short database-only
    recheck (R1 before realization, R2 immediately before the send) and the
    finalize repeats it. Any moved subject is a stale operation: nothing is
    applied, nothing is fabricated, and no retry permission is granted.
    """
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    task = require_current_task(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    _require_implementing_task(task)
    gate, revision, loop, iteration, producer_session = _load_authorized_subjects(
        transaction_pool, task=task, require_ready_producer=require_ready_producer
    )
    if (
        gate.id != reserved.owner_gate.id
        or revision.id != reserved.plan_revision.id
        or loop.id != reserved.review_loop.id
        or iteration.id != reserved.review_iteration.id
        or producer_session.id != reserved.producer_session.id
    ):
        raise StaleOperationError(
            "the authorized implementation subjects have moved on; the operation is stale"
        )
    return task


# --- failure finalization and result shaping --------------------------------------


def _finalize_pre_effect_failure(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    execution_id: UUID,
    producer_session_id: UUID,
    next_status: ExecutionStatus,
    failure_kind: str,
) -> None:
    """Finalize a known failure that provably occurred before any effect.

    The conditional transition (``running`` → the truthful final status) and
    the safe ``execution_failed`` audit commit atomically; a concurrent
    finalize wins the conditional write and no duplicate audit is recorded.
    The attempt is never rewritten afterwards.
    """
    with composed_transaction(pool) as transaction_pool:
        finalized = execution_records.update_execution_status(
            transaction_pool,
            execution_id=execution_id,
            expected_status=ExecutionStatus.RUNNING,
            next_status=next_status,
        )
        if finalized is None:
            # Already finalized concurrently: never duplicate the audit.
            return
        event_coordination.record_execution_failed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            execution_id=execution_id,
            actor=event_coordination.producer_actor(producer_session_id),
            failure_kind=failure_kind,
        )


def _canonical_result_payload(result: ImplementationResult) -> dict[str, object]:
    """The exact validated canonical envelope persisted on the Execution."""
    payload: dict[str, object] = {
        "type": IMPLEMENTATION_RESULT_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "status": result.status,
        "branch": result.branch,
        "summary": result.summary,
        "changes": list(result.changes),
        "validation": list(result.validation),
    }
    if result.notes is not None:
        payload["notes"] = result.notes
    return payload


# --- the dispatch phase and the service entry -------------------------------------


def _execute_external_phase(
    pool: DatabasePool,
    *,
    reserved: _ReservedAttempt,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    message: str,
    producer_factory: ProducerRuntimeAttachmentFactory,
    activity_factory: ActivitySubscriptionFactory | None,
) -> AuthorizedImplementationExecution:
    """The authority-gated external phase: attach, realize, send, finalize."""
    producer_external_session_id = reserved.producer_session.external_session_id
    if producer_external_session_id is None:
        # Narrowed by the subject loader; kept explicit for fail-closed safety.
        raise ConflictError("the producer session binding carries no external session identity")
    with ExitStack() as attachment_stack:
        try:
            runtime = attachment_stack.enter_context(producer_factory())
        except ExternalOperationUncertainError:
            # Uncertain attachment: the claimed attempt stays conservatively
            # running (the potentially-dispatched record) and nothing resends.
            raise
        except ExternalOperationFailedError:
            # A known attachment failure provably occurred before any effect.
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=reserved.execution.id,
                producer_session_id=reserved.producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise
        # --- R1: authority revalidation after acquisition, before realization. ---
        try:
            with composed_transaction(pool) as transaction_pool:
                _revalidate_reserved_subjects(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    reserved=reserved,
                    require_ready_producer=True,
                )
        except (NotFoundError, ConflictError, StaleOperationError):
            # Authority moved on during attachment, before any runtime effect:
            # the never-dispatched attempt finalizes truthfully.
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=reserved.execution.id,
                producer_session_id=reserved.producer_session.id,
                next_status=ExecutionStatus.FAILED_FINAL,
                failure_kind=_AUTHORITY_STALE_FAILURE_KIND,
            )
            raise
        # --- the one native ACT handoff on the same external session ID. ---
        try:
            runtime.realize_control(producer_external_session_id, ImplementInteraction())
        except AgentRuntimeUncertainOutcomeError as error:
            raise ExternalOperationUncertainError(
                "the outcome of the native ACT reconstruction is unknown; the implementation "
                "attempt remains unresolved and nothing is resent"
            ) from error
        except AgentRuntimeError as error:
            _finalize_pre_effect_failure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                execution_id=reserved.execution.id,
                producer_session_id=reserved.producer_session.id,
                next_status=ExecutionStatus.FAILED_TRANSIENT,
                failure_kind=_RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise ExternalOperationFailedError(
                "the native ACT reconstruction failed before any implementation work was dispatched"
            ) from error

        return _send_and_finalize(
            pool,
            reserved=reserved,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            message=message,
            runtime=runtime,
            producer_external_session_id=producer_external_session_id,
            activity_factory=activity_factory,
        )


def _send_and_finalize(
    pool: DatabasePool,
    *,
    reserved: _ReservedAttempt,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    message: str,
    runtime: AgentRuntimeAdapter,
    producer_external_session_id: str,
    activity_factory: ActivitySubscriptionFactory | None,
) -> AuthorizedImplementationExecution:
    """The subscription, R2, the one send, and the one-shot finalize."""
    heartbeat = _QualifyingActivityHeartbeat(
        pool,
        execution_id=reserved.execution.id,
        external_session_id=producer_external_session_id,
    )
    with ExitStack() as subscription_stack:
        if activity_factory is not None:
            try:
                subscription_stack.enter_context(
                    activity_factory(runtime, producer_external_session_id, heartbeat)
                )
            except Exception:  # noqa: BLE001 - advisory telemetry never gates the attempt
                logger.debug("the execution activity subscription failed; observation is advisory")
        try:
            with composed_transaction(pool) as transaction_pool:
                _revalidate_reserved_subjects(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    reserved=reserved,
                    require_ready_producer=True,
                )
        except (NotFoundError, ConflictError, StaleOperationError) as error:
            # Realization already touched the runtime session, so an effect
            # may have begun: the attempt stays conservatively running.
            raise ExternalOperationUncertainError(
                "implementation authority moved on during the ACT reconstruction; the "
                "implementation send was refused and the attempt remains unresolved"
            ) from error
        try:
            result = runtime.send(
                producer_external_session_id,
                message,
                expected_family=IMPLEMENTATION_RESULT_FAMILY,
            )
        except AgentRuntimeUncertainOutcomeError as error:
            raise ExternalOperationUncertainError(
                "the outcome of the implementation send is unknown; the attempt remains "
                "potentially dispatched and is never resent"
            ) from error
        except AgentRuntimeError as error:
            raise ExternalOperationUncertainError(
                "the implementation send ended without a validated formal completion after "
                "possible implementation work; the attempt remains unresolved and is never "
                "resent"
            ) from error
        if not isinstance(result, ImplementationResult):
            raise ExternalOperationUncertainError(
                "the implementation completion was not the expected formal result family; "
                "the attempt remains unresolved and is never resent"
            )
    # --- the one-shot finalize (subscription closed; the lease is still open). ---
    with composed_transaction(pool) as transaction_pool:
        # Any revalidation failure here leaves the attempt conservatively
        # running: after effects may have begun, history is never rewritten
        # and no false outcome is fabricated.
        current_task = _revalidate_reserved_subjects(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            reserved=reserved,
            require_ready_producer=False,
        )
        finalized = execution_records.complete_execution_with_result(
            transaction_pool,
            execution_id=reserved.execution.id,
            implementation_result=_canonical_result_payload(result),
        )
        if finalized is None:
            raise StaleOperationError(
                "the implementation attempt is no longer running; finalization is stale"
            )
        event_coordination.record_execution_completed_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            execution_id=reserved.execution.id,
            actor=event_coordination.producer_actor(reserved.producer_session.id),
        )
    return AuthorizedImplementationExecution(
        task=current_task,
        execution=finalized,
        implementation_result=result,
        producer_session=reserved.producer_session,
        owner_gate=reserved.owner_gate,
    )


def execute_authorized_implementation(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    activity_subscription_factory: ActivitySubscriptionFactory | None = None,
) -> AuthorizedImplementationExecution:
    """Execute the E5-authorized implementation in the existing Producer session.

    One Owner-authenticated, exact-subject seam: the exact approved gate/plan
    handoff is re-derived from durable state (never caller-supplied), the
    original READY Producer session is resolved, and exactly one initial
    implementation attempt is reserved, dispatched, validated, and finalized.
    The caller cannot supply a free-text acceptance, choose a different
    Producer session, or replay a dispatched attempt.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    producer_factory = _require_producer_factory(producer_runtime_factory)
    activity_factory = _require_activity_factory(activity_subscription_factory)
    with application_span(_SERVICE_TRACER_SCOPE, _EXECUTE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_EXECUTE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase 1: admission and the durable attempt reservation. ---
        with composed_transaction(pool) as transaction_pool:
            reserved = _reserve_initial_attempt(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
        message = serialize_review_result(reserved.accepted_review)
        # --- Phases 2 and 3: the authority-gated effects and the finalize. ---
        return _execute_external_phase(
            pool,
            reserved=reserved,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            message=message,
            producer_factory=producer_factory,
            activity_factory=activity_factory,
        )
