"""TaskAgentSession runtime establishment, readiness, and lifecycle services
(issue #130).

This module completes an already-admitted CONNECTING TaskAgentSession binding
(reserved by the #68 capacity-admission service) through runtime
establishment, canonical OpenOrc initialization, validated ``session_ready``,
and the durable READY transition — plus the two focused lifecycle records for
genuine exact-session loss and normal explicit end.

``establish_task_agent_session`` is ONE logical readiness operation composed
in phases with no database transaction ever held across the runtime call:

1. short read phase — reload/validate the exact Task/role binding: CONNECTING
   proceeds, an exact already-READY binding is idempotent success, LOST/ENDED
   are terminal conflicts that never trigger replacement; the persisted
   ADMITTED Connection must exist, be Workspace-scoped, and be enabled. A
   later role-binding route change applies to future Task admissions only:
   it never invalidates an already-admitted Task/role/Connection, and this
   operation never silently switches to the new route (issue #189);
2. composition phase (no transaction) — compose the canonical #66 role
   initialization through the protocol boundary and deliberately assemble the
   NON-SECRET effective runtime/session configuration snapshot from the
   composition-selected admitted-Connection configuration (the required
   ``effective_configuration`` the caller read and selected once before this
   service and shares with the injected construction-context resolver) plus
   the durably-read admitted Connection's adapter type. The current
   Connection ``safe_config`` is never re-read here, so fresh construction
   and the durable READY snapshot agree by construction even across a
   between-reads Owner configuration edit; never raw credentials,
   initialization Markdown, rendered schemas, Workspace guidance,
   control/prompt bodies, or transcripts enter the snapshot;
3. external phase (no transaction) — invoke the #67
   ``AgentRuntimeAdapter.create_session`` readiness handshake exactly once for
   the exact admitted binding. A successful ready-session result already
   proves the fresh isolated external context, delivered canonical
   initialization, and a valid #65 ``session_ready``; the service never
   performs a second ordinary send to establish readiness, and runtime-specific
   allocation/initialization/response-extraction mechanics are adapter-side;
4. durable finalize phase (one short transaction) — install
   ``external_session_id``, the snapshot, and the reported provenance, and
   move CONNECTING -> READY exactly once through the existing atomic
   persistence primitive. Conditional-write rejection is reclassified from the
   reloaded binding: same exact READY identity is idempotent success, a
   different READY identity is a continuity conflict, LOST/ENDED are terminal
   conflicts, and a missing/mis-scoped binding fails closed. A persistence
   failure AFTER known external readiness is the typed recovery-required
   condition — never a denial of the known readiness, never a second create.

Known creation failure and uncertain creation outcome stay distinct: a known
failure (configuration rejection, unavailability, transport, or invalid
readiness) leaves the binding CONNECTING and raises the typed known-failure
error; an uncertain outcome (timeout, delivery loss) also leaves the binding
CONNECTING and raises the typed uncertain error with zero blind replay.

``record_task_agent_session_lost`` records genuine loss of the exact bound
external session (READY -> LOST) only from the normalized exact-session-loss
evidence; transient runtime/Hub unavailability is never classified as loss.
``end_task_agent_session`` records the explicit normal end of a CONNECTING or
READY binding (-> ENDED). Both write lifecycle facts on the SAME binding —
history is preserved, no successor session is created, and later workflow
services decide TaskBlock/recovery consequences.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Protocol, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import (
    DetachedTurnRuntime,
)
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentSessionAllocation,
    AgentSessionCreated,
    AgentSessionCreationRequest,
)
from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.ownership import Workspace
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import (
    TaskAgentSession,
    TaskAgentSessionDomainError,
    TaskSessionLifecycleStatus,
    canonical_effective_config_snapshot,
)
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence.connections import get_connection
from openorc.persistence.detached_turns import (
    get_outstanding_task_detached_turn_receipt,
    refuse_task_detached_turn_receipt,
)
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import (
    get_task_agent_session,
    initialize_task_agent_session,
    mark_task_agent_session_ended,
    mark_task_agent_session_lost,
)
from openorc.persistence.tasks import get_task, lock_workspace_task_arbitration
from openorc.protocol.initialization_assets import compose_initialization
from openorc.protocol.models import SESSION_READY_FAMILY
from openorc.services import task_detached_turn_correlation as turn_correlation
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    ObservationRegistrationRefusedError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "ConnectionNotEstablishableError",
    "DispatchedSessionEstablishment",
    "EstablishmentObservationRegistrar",
    "InitializationReadinessOutcome",
    "SessionContinuityConflictError",
    "SessionEstablishmentRecoveryRequiredError",
    "SessionTerminalConflictError",
    "dispatch_task_agent_session_establishment",
    "end_task_agent_session",
    "establish_task_agent_session",
    "finalize_observed_initialization_readiness",
    "record_task_agent_session_lost",
]

_SERVICE_TRACER_SCOPE = "openorc.services.session_runtime"
_ESTABLISH_SPAN_NAME = "session_runtime.establish_task_agent_session"
_RECORD_LOST_SPAN_NAME = "session_runtime.record_task_agent_session_lost"
_END_SPAN_NAME = "session_runtime.end_task_agent_session"

logger = logging.getLogger(__name__)


class SessionContinuityConflictError(ConflictError):
    """The binding's continuity facts disagree with current authorized state.

    An already-initialized binding carries a different external session
    identity than the one being finalized. Bindings are never silently
    repointed, replaced, or overwritten: the external session identity is
    immutable after initialization. A changed role-binding route is future
    admission state (issue #189), never an establishment continuity conflict
    against the persisted admitted Connection.
    """


class SessionTerminalConflictError(ConflictError):
    """The existing Task/role binding is a terminal historical session.

    A LOST or ENDED binding remains historical: it is never replaced, never
    reopened, and never re-established, so establishment never creates a
    successor session for it.
    """


class ConnectionNotEstablishableError(ConflictError):
    """The Connection routed for this role is currently disabled.

    ``enabled`` is Owner-controlled eligibility, nothing more: this outcome
    says OpenOrc may not invoke the routed runtime right now. It is never a
    runtime reachability, health, or initialization observation, and it is
    never classified as session loss.
    """


class SessionEstablishmentRecoveryRequiredError(ExternalOperationFailedError):
    """Known external readiness whose durable READY persistence failed.

    The #67 creation handshake returned a successful ready-session result —
    the external ready session exists — but the durable READY finalization
    failed or became ambiguous afterwards. The known external effect is never
    denied and never blindly replayed: no second session is created
    automatically, and recovery reconciles through the durable binding.
    """


def _require_uuid_command_field(value: object, field_name: str) -> UUID:
    """Classify a malformed command identifier as an invalid command.

    Command-shape validation happens inside the use-case span and before any
    telemetry annotation: the error message names only the field — the raw
    caller-supplied value is never echoed into the error message, telemetry,
    or any later effect.
    """
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"session runtime {field_name} must be a UUID")
    return value


def _require_workflow_role(role: object) -> WorkflowRole:
    """Classify a malformed workflow role as an invalid command."""
    if not isinstance(role, WorkflowRole):
        raise InvalidCommandError(
            "session runtime roles must be WorkflowRole (producer or reviewer) values"
        )
    return role


def _require_runtime_adapter(adapter: object) -> AgentRuntimeAdapter:
    """Classify a malformed adapter dependency as an invalid command."""
    if not isinstance(adapter, AgentRuntimeAdapter):
        raise InvalidCommandError("the supplied runtime must be an AgentRuntimeAdapter")
    return adapter


def _require_loss_evidence(evidence: object) -> AgentSessionNotFoundError:
    """Classify non-loss evidence as an invalid command.

    Genuine session loss requires the normalized exact-session-loss condition
    — the runtime answered that the exact addressed session is gone. Runtime
    unavailability, timeouts, uncertain outcomes, and configuration rejections
    are deliberately NOT sufficient: transient unavailability never becomes
    session loss.
    """
    if not isinstance(evidence, AgentSessionNotFoundError):
        raise InvalidCommandError(
            "session loss requires the normalized exact-session-loss evidence; "
            "transient runtime unavailability is not session loss"
        )
    return evidence


def _require_workspace(pool: DatabasePool, workspace_id: UUID) -> Workspace:
    """Load one Workspace or fail closed with the uniform not-found outcome."""
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:
        raise NotFoundError("the requested workspace is not available")
    return workspace


def _require_scoped_task(pool: DatabasePool, *, workspace_id: UUID, task_id: UUID) -> Task:
    """Load one Task of the given Workspace or fail closed uniformly."""
    task = get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    return task


def _require_scoped_binding(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession:
    """Load the exact scoped Task/role binding or fail closed uniformly.

    Sessions are addressed by ``(Task, role)``, not by session UUID; the
    binding's direct workspace/task scope is validated against the addressed
    Workspace. Missing and cross-Workspace/cross-Task subjects are the same
    caller-facing outcome (not found) so existence is never leaked.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=role)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested agent session is not available in this workspace")
    return binding


def _reload_scoped_binding(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession:
    """Reload the scoped binding after a rejected conditional write."""
    binding = get_task_agent_session(pool, task_id=task_id, role=role)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested agent session is not available in this workspace")
    return binding


def _require_effective_configuration(
    effective_configuration: Mapping[str, object],
) -> Mapping[str, object]:
    """Classify a malformed selected configuration as an invalid command.

    The composition-selected admitted-Connection configuration is the
    authoritative snapshot content, so it is validated in full canonical
    form at command shape: canonical JSON-object semantics over the whole
    value. Malformed caller data can then never escape into authorization,
    persistence mutation, or telemetry, and the raw value is never echoed
    into the error text.
    """
    if not isinstance(effective_configuration, Mapping):
        raise InvalidCommandError(
            "the selected effective configuration must be a canonical JSON object"
        )
    try:
        return canonical_effective_config_snapshot(effective_configuration)
    except TaskAgentSessionDomainError as error:
        raise InvalidCommandError(
            "the selected effective configuration must be a canonical JSON object"
        ) from error


def _assemble_effective_config_snapshot(
    adapter_type: AdapterType,
    effective_configuration: Mapping[str, object],
) -> Mapping[str, object]:
    """Deliberately assemble the non-secret effective configuration snapshot.

    The snapshot records exactly the composition-selected admitted-Connection
    configuration the caller supplied (``effective_configuration``, read and
    selected once before establishment and shared with the injected
    construction-context resolver's fresh-Producer bootstrap base) plus the
    durably-read admitted Connection's adapter type. The current Connection
    ``safe_config`` is never re-read here: a between-reads Owner edit is
    absorbed by the selection, so fresh construction and the durable READY
    snapshot agree by construction (issue #189). Nothing else enters the
    snapshot: never raw
    credentials/tokens/auth secrets, initialization Markdown, rendered
    schemas, Workspace guidance, control/prompt bodies, or transcripts.
    """
    try:
        return canonical_effective_config_snapshot(
            {
                "adapter": adapter_type.value,
                "configuration": dict(effective_configuration),
            }
        )
    except TaskAgentSessionDomainError as error:
        raise ConflictError(
            "the effective runtime/session configuration snapshot could not be canonicalized"
        ) from error


@dataclass(frozen=True, slots=True)
class InitializationReadinessOutcome:
    """The result of one READY finalization attempt (issue #324).

    ``applied`` is ``True`` when this finalization won the conditional write
    and installed the immutable initialization facts; ``False`` marks the
    idempotent reclassification — the same exact READY identity was already
    installed concurrently and nothing is rewritten. Any other reloaded
    state raises the module's continuity/terminal conflicts.
    """

    binding: TaskAgentSession
    applied: bool


def _finalize_ready_binding_in_transaction(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    external_session_id: str,
    snapshot: Mapping[str, object],
    reported_provider: str | None,
    reported_model: str | None,
    reported_runtime_version: str | None,
) -> InitializationReadinessOutcome:
    """Finalize the exact CONNECTING binding to READY within the caller's tx.

    The shared core of both READY finalizations: the blocking creation's
    phase 4 (issue #130) and the detached initialization settlement's
    readiness finalization (issue #324). The derived account-operational
    barrier (#97) runs before the write, so READY is never finalized past a
    claimed account deletion. The existing atomic persistence primitive is
    the only writer: it applies only while the binding is CONNECTING with a
    NULL identity, installs the exact external session identity, the
    non-secret effective configuration snapshot, and the reported provenance
    atomically, and stamps ``initialized_at``.

    A rejected conditional write is reclassified from the reloaded binding:
    the same exact READY identity is idempotent success, a different READY
    identity is a continuity conflict, LOST/ENDED are terminal conflicts, and
    a missing/mis-scoped binding fails closed.
    """
    workspace = _require_workspace(transaction_pool, workspace_id)
    require_account_operational(transaction_pool, profile_id=workspace.owner_profile_id)
    initialized = initialize_task_agent_session(
        transaction_pool,
        task_id=task_id,
        role=role,
        external_session_id=external_session_id,
        effective_config_snapshot=dict(snapshot),
        reported_provider=reported_provider,
        reported_model=reported_model,
        reported_runtime_version=reported_runtime_version,
    )
    if initialized is not None:
        return InitializationReadinessOutcome(initialized, applied=True)
    existing = _reload_scoped_binding(
        transaction_pool, workspace_id=workspace_id, task_id=task_id, role=role
    )
    if existing.lifecycle_status is TaskSessionLifecycleStatus.READY:
        if existing.external_session_id == external_session_id:
            # A concurrent finalization of the SAME identity won the
            # conditional write: idempotent success, never a rewrite.
            return InitializationReadinessOutcome(existing, applied=False)
        raise SessionContinuityConflictError(
            "the task/role session binding is already initialized with a different external session"
        )
    if existing.lifecycle_status in (
        TaskSessionLifecycleStatus.LOST,
        TaskSessionLifecycleStatus.ENDED,
    ):
        raise SessionTerminalConflictError(
            "the task/role session binding became a terminal historical "
            "session before READY finalization"
        )
    # The conditional write rejected the CONNECTING binding, yet the
    # reloaded binding is still CONNECTING: an unexpected durable-state
    # invariant failure that fails closed.
    raise ConflictError(
        "the task/role session binding did not finalize READY from its expected CONNECTING state"
    )


def _finalize_ready_binding(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    created: AgentSessionCreated,
    snapshot: Mapping[str, object],
) -> TaskAgentSession:
    """Finalize the exact CONNECTING binding to READY exactly once.

    One short database-only write transaction (the runtime creation already
    happened and is never undone) around the shared READY-finalization core.
    """
    with composed_transaction(pool) as transaction_pool:
        outcome = _finalize_ready_binding_in_transaction(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            external_session_id=created.external_session_id,
            snapshot=snapshot,
            reported_provider=created.reported_provider,
            reported_model=created.reported_model,
            reported_runtime_version=created.reported_runtime_version,
        )
        return outcome.binding


def establish_task_agent_session(
    pool: DatabasePool,
    adapter: AgentRuntimeAdapter,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    effective_configuration: Mapping[str, object],
) -> TaskAgentSession:
    """Establish one admitted Task/role binding through runtime readiness.

    Completes the exact CONNECTING binding reserved by the #68 admission
    service: one logical #67 ``create_session`` readiness handshake whose
    success already proves the fresh isolated external context, the delivered
    canonical #66 initialization, and a valid #65 ``session_ready`` — then the
    durable CONNECTING -> READY transition installing the exact external
    session identity, the non-secret effective configuration snapshot, and the
    reported provenance exactly once.

    ``effective_configuration`` is the complete non-secret selected
    admitted-Connection safe_config the composition read and selected ONCE
    before this service (issue #189). It is the authoritative snapshot
    content and is shared with the injected construction-context resolver's
    fresh-Producer bootstrap base, so the fresh construction and the durable
    READY snapshot agree by construction: an Owner configuration edit
    between the selection and the runtime call is absorbed by the selection
    instead of silently diverging. No database transaction is open when the
    selection is read.

    Returns the READY binding. An exact already-READY binding is idempotent
    success with no runtime call. Raises the typed authorization/not-found
    vocabulary (uniform :class:`NotFoundError` for a missing/unscoped
    Workspace, Task, binding, or Connection), the typed
    continuity/terminal conflicts defined by this module, and — for the
    runtime handshake — :class:`ExternalOperationFailedError` for known
    failures and :class:`ExternalOperationUncertainError` for uncertain
    outcomes; the binding always remains CONNECTING after either.

    Known external readiness whose durable finalization fails raises
    :class:`SessionEstablishmentRecoveryRequiredError` and never invokes the
    runtime again. No database transaction is ever held across the runtime
    call. All command-shape validation happens inside the use-case span and
    before telemetry annotation (the #109 service-span contract).
    """
    with application_span(_SERVICE_TRACER_SCOPE, _ESTABLISH_SPAN_NAME) as span:
        # Command-shape validation runs INSIDE the use-case span and BEFORE
        # telemetry annotation: malformed caller-supplied values raise
        # InvalidCommandError with their raw values never entering telemetry.
        _require_uuid_command_field(workspace_id, "workspace_id")
        _require_uuid_command_field(task_id, "task_id")
        _require_workflow_role(role)
        _require_runtime_adapter(adapter)
        _require_effective_configuration(effective_configuration)
        annotate_span(
            span,
            operation=_ESTABLISH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            workflow_role=role.value,
        )

        # --- Phase 1: exact admitted-binding validation (short reads, no
        # locks held, no external I/O). The derived account-operational
        # barrier (#97) is the FIRST lock acquisition. ---
        workspace = _require_workspace(pool, workspace_id)
        require_account_operational(pool, profile_id=workspace.owner_profile_id)
        _require_scoped_task(pool, workspace_id=workspace_id, task_id=task_id)
        binding = _require_scoped_binding(
            pool, workspace_id=workspace_id, task_id=task_id, role=role
        )
        # A changed role-binding route is future admission state (issue
        # #189): it never invalidates this already-admitted
        # Task/role/Connection, and this operation never re-points the
        # binding to the new route. The persisted admitted Connection is
        # what establishment resolves and validates.
        if binding.lifecycle_status is TaskSessionLifecycleStatus.READY:
            # Exact already-READY binding: idempotent success with no runtime
            # call and no rewrite of the immutable initialization facts.
            return binding
        if binding.lifecycle_status in (
            TaskSessionLifecycleStatus.LOST,
            TaskSessionLifecycleStatus.ENDED,
        ):
            raise SessionTerminalConflictError(
                "the existing task/role session binding is a terminal historical session"
            )
        connection = get_connection(pool, binding.connection_id)
        if connection is None or connection.workspace_id != workspace_id:
            raise NotFoundError("the requested connection is not available in this workspace")
        if not connection.enabled:
            raise ConnectionNotEstablishableError(
                "the connection routed for this role is currently disabled"
            )
        # --- Phase 2: composition (no transaction, no external I/O). The
        # canonical #66 initialization is composed through the protocol
        # boundary and the non-secret snapshot is assembled deliberately. ---
        initialization = compose_initialization(role.value, workspace.guidance)
        snapshot = _assemble_effective_config_snapshot(connection.adapter, effective_configuration)
        request = AgentSessionCreationRequest(
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            initialization=initialization,
        )

        # --- Phase 3: the ONE logical #67 readiness handshake (external, no
        # database transaction held; runtime-specific allocation, initialization
        # delivery, response extraction, and session_ready validation are
        # adapter-side mechanics of this one creation operation). ---
        try:
            created = adapter.create_session(request)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "task agent session establishment ended with an uncertain "
                "outcome; the binding remains CONNECTING"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the agent runtime session creation handshake is "
                "unknown; the binding remains CONNECTING"
            ) from error
        except AgentRuntimeError as error:
            # Known failure decided by the adapter boundary (configuration
            # rejection, unavailability before effect, transport, or an invalid
            # readiness response): never READY, never LOST, and no partial
            # external identity is persisted.
            raise ExternalOperationFailedError(
                "the agent runtime session creation failed before readiness; "
                "the binding remains CONNECTING"
            ) from error

        # --- Phase 4: durable READY finalization (one short transaction). ---
        try:
            return _finalize_ready_binding(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=role,
                created=created,
                snapshot=snapshot,
            )
        except ApplicationError:
            # Typed workflow classifications (not-found, barrier conflict,
            # continuity/terminal conflicts) propagate unchanged: they are the
            # boundary's own vocabulary.
            raise
        except Exception as error:
            # A durable persistence failure AFTER known external readiness is
            # never an infrastructure leak through the service boundary: it is
            # the typed recovery-required condition. The known readiness is
            # never denied and never replayed with a second create.
            raise SessionEstablishmentRecoveryRequiredError(
                "the ready agent session could not be durably finalized after "
                "its known runtime readiness; authoritative recovery is "
                "required and no second session is created"
            ) from error


class EstablishmentObservationRegistrar(Protocol):
    """The structural registration seam of the process's Hub-run observer.

    The positively-required pre-admission gate (issue #323): the
    establishment dispatch registers the exact created external session
    before any admission, so the pinned SDK's unreplayable terminal result
    can never be
    dispatched without active observation. Failures are the observer's typed
    refusals, routed through the pre-effect failure machinery.
    """

    def register_establishment_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Register one CONNECTING binding's initialization for observation."""
        ...


@dataclass(frozen=True, slots=True)
class DispatchedSessionEstablishment:
    """One detached initialization dispatch outcome (issue #324).

    The authoritative binding is returned exactly as the durable state now
    reads: CONNECTING with a NULL external identity while the initialization
    run is outstanding (the receipt carries the separate provisional
    establishment evidence), or READY when a staged early completion already
    settled within this dispatch. ``receipt`` is the binding's outstanding
    initialization receipt, or ``None`` only for the idempotent
    already-READY re-entry with nothing outstanding.
    """

    task_agent_session: TaskAgentSession
    receipt: TaskDetachedTurnReceipt | None


_DISPATCH_SPAN_NAME: Final = "session_runtime.dispatch_task_agent_session_establishment"
_READINESS_FINALIZE_SPAN_NAME: Final = "session_runtime.finalize_observed_initialization_readiness"


def _require_detached_capability(adapter: AgentRuntimeAdapter) -> DetachedTurnRuntime:
    """Fail closed unless the adapter implements the detached capability."""
    allocate = getattr(adapter, "allocate_detached_session", None)
    admit = getattr(adapter, "admit_detached_turn", None)
    if not callable(allocate) or not callable(admit):
        raise InvalidCommandError(
            "the runtime adapter does not implement the detached turn capability"
        )
    return cast(DetachedTurnRuntime, adapter)


def _require_observation_registrar(
    observation_registrar: object,
) -> EstablishmentObservationRegistrar:
    """Fail closed unless the registrar seam is usable."""
    register = getattr(observation_registrar, "register_establishment_session", None)
    if not callable(register):
        raise InvalidCommandError(
            "the establishment observation registrar does not implement the "
            "establishment registration seam"
        )
    return cast(EstablishmentObservationRegistrar, observation_registrar)


def dispatch_task_agent_session_establishment(
    pool: DatabasePool,
    adapter: AgentRuntimeAdapter,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    effective_configuration: Mapping[str, object],
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
    observation_registrar: EstablishmentObservationRegistrar,
) -> DispatchedSessionEstablishment:
    """Dispatch one admitted Task/role binding's initialization as a detached run.

    The detached counterpart of the #130 readiness establishment (issue
    #324): the same phase 1 binding validation and phase 2 composition, then
    the initialization becomes one admitted, independently completed agent
    turn — one promptless ``start`` of the v1 ``interactive`` construction
    bundle, one positively-required observation registration, and one
    detached-turn admission of the composed canonical initialization
    verbatim. No ``send`` and no prompt-inside-``start`` shortcut exists in
    this path, and no database transaction is ever held across a runtime
    call.

    Durable semantics:

    - The receipt is the minimum separate provisional establishment
      evidence, committed with the provisional external identity and the
      dispatch-composed non-secret configuration snapshot strictly before
      any admission. The authoritative binding stays CONNECTING with a NULL
      external identity: Hub admission is never readiness, never a bound
      identity, and never a durable final binding.
    - An outstanding initialization receipt makes this dispatch an
      idempotent no-op (the same receipt returned): a repeated bootstrap can
      never create or admit twice. A refused/failed receipt while
      CONNECTING is the caller's unresolved-establishment classification,
      not a re-dispatch authorization.
    - A positively identified refusal (unavailable before effect, Hub
      rejection, exact-session absence) marks the receipt refused and raises
      the typed known-failure error; an uncertain allocation or admission
      leaves the receipt conservatively claimed and raises the typed
      uncertain error with zero blind replay. Either way the binding remains
      CONNECTING and is never marked LOST for mere Hub/transport
      unavailability.
    - An early completion that arrived before the acknowledgement is staged
      on the receipt and routed through the ordinary settlement inside the
      acknowledgement attach, so a validated ``session_ready`` observed
      before the acknowledged turn reference was persisted is never ignored or duplicated.

    On success the caller owns the post-dispatch sequencing (the bootstrap
    continuation resumes when the observation settles readiness); the
    dispatching worker returns without waiting for ``session_ready``.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_SPAN_NAME) as span:
        # Command-shape validation runs INSIDE the use-case span and BEFORE
        # telemetry annotation: malformed caller-supplied values raise
        # InvalidCommandError with their raw values never entering telemetry.
        _require_uuid_command_field(workspace_id, "workspace_id")
        _require_uuid_command_field(task_id, "task_id")
        _require_workflow_role(role)
        _require_runtime_adapter(adapter)
        _require_effective_configuration(effective_configuration)
        _require_uuid_command_field(original_attempt_id, "original_attempt_id")
        if not isinstance(original_attempt_kind, OriginalAttemptKind):
            raise InvalidCommandError(
                "dispatch_task_agent_session_establishment requires an OriginalAttemptKind"
            )
        registrar = _require_observation_registrar(observation_registrar)
        runtime = _require_detached_capability(adapter)
        annotate_span(
            span,
            operation=_DISPATCH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            workflow_role=role.value,
        )

        # --- Phase 1: exact admitted-binding validation (short reads, no
        # locks held, no external I/O). The derived account-operational
        # barrier (#97) is the FIRST lock acquisition. ---
        workspace = _require_workspace(pool, workspace_id)
        require_account_operational(pool, profile_id=workspace.owner_profile_id)
        _require_scoped_task(pool, workspace_id=workspace_id, task_id=task_id)
        binding = _require_scoped_binding(
            pool, workspace_id=workspace_id, task_id=task_id, role=role
        )
        # A changed role-binding route is future admission state (issue
        # #189): it never invalidates this already-admitted
        # Task/role/Connection, and this operation never re-points the
        # binding to the new route.
        if binding.lifecycle_status is TaskSessionLifecycleStatus.READY:
            # Exact already-READY binding: idempotent success with no
            # runtime call and no rewrite of the immutable initialization
            # facts.
            return DispatchedSessionEstablishment(
                binding,
                get_outstanding_task_detached_turn_receipt(pool, task_agent_session_id=binding.id),
            )
        if binding.lifecycle_status in (
            TaskSessionLifecycleStatus.LOST,
            TaskSessionLifecycleStatus.ENDED,
        ):
            raise SessionTerminalConflictError(
                "the existing task/role session binding is a terminal historical session"
            )
        connection = get_connection(pool, binding.connection_id)
        if connection is None or connection.workspace_id != workspace_id:
            raise NotFoundError("the requested connection is not available in this workspace")
        if not connection.enabled:
            raise ConnectionNotEstablishableError(
                "the connection routed for this role is currently disabled"
            )

        # --- Replay guard: a durably dispatched establishment is never
        # re-created or re-admitted; the outstanding receipt is the proof. ---
        outstanding = get_outstanding_task_detached_turn_receipt(
            pool, task_agent_session_id=binding.id
        )
        if outstanding is not None:
            return DispatchedSessionEstablishment(binding, outstanding)

        # --- Phase 2: composition (no transaction, no external I/O). The
        # canonical #66 initialization is composed through the protocol
        # boundary and the non-secret snapshot is assembled deliberately. ---
        initialization = compose_initialization(role.value, workspace.guidance)
        snapshot = _assemble_effective_config_snapshot(connection.adapter, effective_configuration)
        request = AgentSessionCreationRequest(
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            initialization=initialization,
        )

        # --- Phase 3a: allocation (external, no transaction held). One
        # promptless ``start`` of the complete construction bundle; an
        # allocated-but-unconfirmed external identity, never readiness. ---
        try:
            allocation = runtime.allocate_detached_session(request)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "the detached session allocation ended with an uncertain "
                "outcome; the binding remains CONNECTING and no second "
                "allocation is ever attempted"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the detached session allocation is unknown; "
                "the binding remains CONNECTING and the establishment is "
                "never re-created"
            ) from error
        except AgentRuntimeError as error:
            raise ExternalOperationFailedError(
                "the detached session allocation failed before any effect; "
                "the binding remains CONNECTING"
            ) from error

        return _admit_detached_initialization(
            pool,
            registrar=registrar,
            runtime=runtime,
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            binding=binding,
            initialization=initialization,
            snapshot=snapshot,
            allocation=allocation,
            original_attempt_kind=original_attempt_kind,
            original_attempt_id=original_attempt_id,
        )


def _admit_detached_initialization(
    pool: DatabasePool,
    *,
    registrar: EstablishmentObservationRegistrar,
    runtime: DetachedTurnRuntime,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    binding: TaskAgentSession,
    initialization: str,
    snapshot: Mapping[str, object],
    allocation: AgentSessionAllocation,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
) -> DispatchedSessionEstablishment:
    """Record the provisional receipt, register observation, admit, attach."""
    # --- Phase 3b: the provisional establishment receipt (one short
    # transaction, strictly before any admission). The admission race and the
    # single-outstanding-run invariant are durable from the first possible
    # effect. ---
    try:
        receipt = turn_correlation.record_task_detached_turn_receipt(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=binding.id,
            operation=PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
            original_attempt_kind=original_attempt_kind,
            original_attempt_id=original_attempt_id,
            expected_formal_family=SESSION_READY_FAMILY,
            created_external_session_id=allocation.external_session_id,
            effective_config_snapshot=dict(snapshot),
            created_session_provenance=_reported_provenance(allocation),
        )
    except ApplicationError:
        raise
    except Exception as error:
        raise SessionEstablishmentRecoveryRequiredError(
            "the detached establishment receipt could not be durably "
            "recorded after its known session allocation; authoritative "
            "recovery is required and no second session is created"
        ) from error

    # --- Phase 3c: observation registration — the positively-required
    # pre-admission gate. A refusal is a proven pre-effect failure: the
    # just-recorded receipt is marked refused so a never-dispatched turn
    # cannot hold the session's single-outstanding slot. A typed observer
    # refusal propagates unchanged with its own safe reason; any other
    # failure is typed into the same refusal with the safe generic token. ---
    try:
        registrar.register_establishment_session(
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=binding.id,
            connection_id=binding.connection_id,
            external_session_id=allocation.external_session_id,
        )
    except ObservationRegistrationRefusedError:
        _refuse_dispatched_receipt_quietly(
            pool, receipt, refusal_kind="observation_registration_refused"
        )
        raise
    except Exception as error:
        _refuse_dispatched_receipt_quietly(
            pool, receipt, refusal_kind="observation_registration_refused"
        )
        raise ObservationRegistrationRefusedError(
            "the initialization observation registration failed before any "
            "admission; the dispatch is a proven pre-effect failure",
            reason=ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value,
        ) from error

    # --- Phase 3d: admission (external, no transaction held). One
    # detached-turn admission of the composed canonical initialization
    # verbatim. ---
    try:
        admission = runtime.admit_detached_turn(allocation.external_session_id, initialization)
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "the detached initialization admission ended with an uncertain "
            "outcome; the receipt stays conservatively claimed and is never "
            "re-dispatched"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the initialization admission is unknown; the "
            "receipt stays conservatively claimed and the establishment is "
            "never re-dispatched"
        ) from error
    except AgentRuntimeError as error:
        # A positively identified refusal (unreachable before effect, runtime
        # rejection, exact-session absence): proven pre-effect, so the
        # receipt is marked refused and the dispatch fails known.
        _refuse_dispatched_receipt_quietly(pool, receipt, refusal_kind="admission_refused")
        raise ExternalOperationRejectedError(
            "the initialization admission was refused before any effect; "
            "the receipt is refused and the binding remains CONNECTING"
        ) from error

    # --- Phase 3e: the acknowledgement attach (one short transaction). A
    # staged early completion is consumed exactly once here and routed
    # through the ordinary settlement, so a validated ``session_ready`` that
    # finished before the acknowledgement was persisted is correlated, never
    # ignored or duplicated. ---
    try:
        outcome = turn_correlation.attach_acknowledged_turn(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt_id=receipt.id,
            admitted_turn_reference=admission.turn_reference,
        )
    except ApplicationError:
        raise
    except Exception as error:
        raise SessionEstablishmentRecoveryRequiredError(
            "the initialization acknowledgement could not be durably "
            "attached after its known admission; authoritative recovery is "
            "required and the establishment is never re-dispatched"
        ) from error
    reloaded = _require_scoped_binding(pool, workspace_id=workspace_id, task_id=task_id, role=role)
    return DispatchedSessionEstablishment(reloaded, outcome.receipt)


def _reported_provenance(allocation: AgentSessionAllocation) -> dict[str, object] | None:
    """Assemble the optional safe provenance facts of one allocation."""
    provenance: dict[str, object] = {}
    if allocation.reported_provider is not None:
        provenance["reported_provider"] = allocation.reported_provider
    if allocation.reported_model is not None:
        provenance["reported_model"] = allocation.reported_model
    if allocation.reported_runtime_version is not None:
        provenance["reported_runtime_version"] = allocation.reported_runtime_version
    return provenance or None


def _refuse_dispatched_receipt_quietly(
    pool: DatabasePool, receipt: TaskDetachedTurnReceipt, *, refusal_kind: str
) -> None:
    """Mark a claimed receipt refused after a proven pre-effect failure.

    Best-effort containment: a persistence failure here must never mask the
    typed refusal the caller raises; the claimed receipt stays the honest
    unresolved-establishment state under the accepted v1 recovery bounds and
    the failure is logged selectively.
    """
    try:
        refused = refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
            refusal_kind=refusal_kind,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused initialization receipt could not be durably marked "
            "(%s); the claimed receipt stays the honest unresolved state",
            type(error).__name__,
        )
        return
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the initialization receipt was no longer claimed when its "
            "pre-effect refusal was recorded"
        )


def finalize_observed_initialization_readiness(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    external_session_id: str,
    effective_configuration: Mapping[str, object],
    reported_provider: str | None = None,
    reported_model: str | None = None,
    reported_runtime_version: str | None = None,
) -> InitializationReadinessOutcome:
    """Bind the validated initialization's READY facts within the caller's tx.

    The detached initialization settlement's readiness finalization (issue
    #324): the existing lifecycle service's atomic CONNECTING -> READY
    transition, driven from the settlement composition with the receipt
    locked. The immutable external session identity, the dispatch-composed
    non-secret effective configuration snapshot, and the initialized
    timestamp move atomically exactly once; a reloaded same-identity READY
    binding is the idempotent reclassification (``applied=False``), a
    different identity raises the continuity conflict, and a LOST/ENDED
    binding raises the terminal conflict — fail closed, never a rewrite,
    never a replacement session.

    ``effective_configuration`` is the receipt's stored dispatch-composed
    canonical snapshot passed through verbatim — the fresh construction and
    the durable READY snapshot agree by construction, and the settlement
    never re-derives configuration from mutable state.

    All command-shape validation happens inside the use-case span and before
    telemetry annotation (the #109 service-span contract).
    """
    with application_span(_SERVICE_TRACER_SCOPE, _READINESS_FINALIZE_SPAN_NAME) as span:
        _require_uuid_command_field(workspace_id, "workspace_id")
        _require_uuid_command_field(task_id, "task_id")
        _require_workflow_role(role)
        if not isinstance(external_session_id, str) or not external_session_id.strip():
            raise InvalidCommandError("external_session_id must be a non-empty string")
        _require_effective_configuration(effective_configuration)
        annotate_span(
            span,
            operation=_READINESS_FINALIZE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            workflow_role=role.value,
        )
        return _finalize_ready_binding_in_transaction(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            external_session_id=external_session_id,
            snapshot=dict(effective_configuration),
            reported_provider=reported_provider,
            reported_model=reported_model,
            reported_runtime_version=reported_runtime_version,
        )


def record_task_agent_session_lost(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    evidence: AgentSessionNotFoundError,
) -> TaskAgentSession:
    """Record genuine loss of the exact bound external session (READY -> LOST).

    The command must carry the normalized exact-session-loss evidence
    (:class:`AgentSessionNotFoundError` — the runtime answered that the exact
    addressed session no longer exists). Transient runtime/Hub/network
    unavailability, timeouts, and other normalized conditions are not loss
    evidence and fail closed as invalid commands, so unavailability never
    becomes lifecycle loss.

    LOST records lifecycle history on the SAME binding: the bound external
    session identity, Connection, and initialization facts are preserved, no
    successor session is created, and later workflow services decide
    TaskBlock/recovery consequences. Recording loss on an already-LOST binding
    is idempotent; a CONNECTING binding has no bound session to lose and an
    ENDED binding terminated normally, so both are conflicts. A missing or
    mis-scoped binding is the uniform not-found outcome.

    All command-shape validation happens inside the use-case span and before
    telemetry annotation (the #109 service-span contract).
    """
    with application_span(_SERVICE_TRACER_SCOPE, _RECORD_LOST_SPAN_NAME) as span:
        _require_uuid_command_field(workspace_id, "workspace_id")
        _require_uuid_command_field(task_id, "task_id")
        _require_workflow_role(role)
        _require_loss_evidence(evidence)
        annotate_span(
            span,
            operation=_RECORD_LOST_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            workflow_role=role.value,
        )
        with composed_transaction(pool) as transaction_pool:
            _require_workspace(transaction_pool, workspace_id)
            # The per-Workspace arbitration lock (issue #76): a READY -> LOST
            # transition releases active Connection capacity, so it composes
            # the same serialization boundary every Task-capacity arbitration
            # holds — a capacity release can never commit between an
            # arbitration's FIFO scan and its capacity decision. The
            # workspace read above takes no lock; this is the transaction's
            # first lock acquisition.
            lock_workspace_task_arbitration(transaction_pool, workspace_id=workspace_id)
            _require_scoped_task(transaction_pool, workspace_id=workspace_id, task_id=task_id)
            binding = _require_scoped_binding(
                transaction_pool, workspace_id=workspace_id, task_id=task_id, role=role
            )
            if binding.lifecycle_status is TaskSessionLifecycleStatus.LOST:
                # Idempotent: loss is already recorded on this binding.
                return binding
            if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
                raise ConflictError(
                    "only a READY task/role session binding can record genuine session loss"
                )
            lost = mark_task_agent_session_lost(transaction_pool, task_id=task_id, role=role)
            if lost is None:
                # A concurrent transition committed between the classification
                # read and the conditional write: reload and reclassify.
                reloaded = _reload_scoped_binding(
                    transaction_pool, workspace_id=workspace_id, task_id=task_id, role=role
                )
                if reloaded.lifecycle_status is TaskSessionLifecycleStatus.LOST:
                    return reloaded
                raise ConflictError(
                    "the task/role session binding state changed concurrently; "
                    "the loss record was not applied"
                )
            return lost


def end_task_agent_session(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
) -> TaskAgentSession:
    """Explicitly end a CONNECTING or READY binding (-> ENDED).

    Orchestration ends the binding normally: history is preserved (an
    initialized binding keeps its immutable external session identity and
    initialization facts; a CONNECTING binding ends with no bound identity),
    ``ended_at`` is stamped exactly once, and no successor session is created.

    Ending an already-ENDED binding is idempotent. A LOST binding is
    absorbing: explicit end cannot overwrite recorded loss. A missing or
    mis-scoped binding is the uniform not-found outcome. Later workflow
    services decide TaskBlock/recovery consequences.

    All command-shape validation happens inside the use-case span and before
    telemetry annotation (the #109 service-span contract).
    """
    with application_span(_SERVICE_TRACER_SCOPE, _END_SPAN_NAME) as span:
        _require_uuid_command_field(workspace_id, "workspace_id")
        _require_uuid_command_field(task_id, "task_id")
        _require_workflow_role(role)
        annotate_span(
            span,
            operation=_END_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            workflow_role=role.value,
        )
        with composed_transaction(pool) as transaction_pool:
            _require_workspace(transaction_pool, workspace_id)
            # The per-Workspace arbitration lock (issue #76): ending a
            # CONNECTING/READY binding releases active Connection capacity,
            # so it composes the same serialization boundary every
            # Task-capacity arbitration holds. The workspace read above takes
            # no lock; this is the transaction's first lock acquisition.
            lock_workspace_task_arbitration(transaction_pool, workspace_id=workspace_id)
            _require_scoped_task(transaction_pool, workspace_id=workspace_id, task_id=task_id)
            binding = _require_scoped_binding(
                transaction_pool, workspace_id=workspace_id, task_id=task_id, role=role
            )
            if binding.lifecycle_status is TaskSessionLifecycleStatus.ENDED:
                # Idempotent: the binding already ended normally.
                return binding
            if binding.lifecycle_status is TaskSessionLifecycleStatus.LOST:
                raise ConflictError(
                    "a lost task/role session binding is absorbing; explicit end "
                    "cannot overwrite recorded loss"
                )
            ended = mark_task_agent_session_ended(transaction_pool, task_id=task_id, role=role)
            if ended is None:
                # A concurrent transition committed between the classification
                # read and the conditional write: reload and reclassify.
                reloaded = _reload_scoped_binding(
                    transaction_pool, workspace_id=workspace_id, task_id=task_id, role=role
                )
                if reloaded.lifecycle_status is TaskSessionLifecycleStatus.ENDED:
                    return reloaded
                raise ConflictError(
                    "the task/role session binding state changed concurrently; "
                    "the end record was not applied"
                )
            return ended
