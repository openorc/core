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
   are terminal conflicts that never trigger replacement, and a binding
   pointing at a different Connection than the current authorized route is a
   continuity conflict; the routed Connection must exist, be Workspace-scoped,
   and be enabled;
2. composition phase (no transaction) — compose the canonical #66 role
   initialization through the protocol boundary and deliberately assemble the
   NON-SECRET effective runtime/session configuration snapshot (the routed
   Connection's adapter type plus its canonical ``safe_config``; never raw
   credentials, initialization Markdown, rendered schemas, Workspace guidance,
   control/prompt bodies, or transcripts);
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
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
)
from openorc.domain.connections import Connection, WorkflowRole
from openorc.domain.ownership import Workspace
from openorc.domain.sessions import (
    TaskAgentSession,
    TaskAgentSessionDomainError,
    TaskSessionLifecycleStatus,
    canonical_effective_config_snapshot,
)
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence.connections import get_connection, get_role_binding
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import (
    get_task_agent_session,
    initialize_task_agent_session,
    mark_task_agent_session_ended,
    mark_task_agent_session_lost,
)
from openorc.persistence.tasks import get_task
from openorc.protocol.initialization_assets import compose_initialization
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "ConnectionNotEstablishableError",
    "SessionContinuityConflictError",
    "SessionEstablishmentRecoveryRequiredError",
    "SessionTerminalConflictError",
    "end_task_agent_session",
    "establish_task_agent_session",
    "record_task_agent_session_lost",
]

_SERVICE_TRACER_SCOPE = "openorc.services.session_runtime"
_ESTABLISH_SPAN_NAME = "session_runtime.establish_task_agent_session"
_RECORD_LOST_SPAN_NAME = "session_runtime.record_task_agent_session_lost"
_END_SPAN_NAME = "session_runtime.end_task_agent_session"

logger = logging.getLogger(__name__)


class SessionContinuityConflictError(ConflictError):
    """The binding's continuity facts disagree with current authorized state.

    The exact Task/role binding points at a different Connection than the
    current authorized route, or an already-initialized binding carries a
    different external session identity than the one being finalized. Bindings
    are never silently repointed, replaced, or overwritten: the external
    session identity is immutable after initialization.
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


def _assemble_effective_config_snapshot(
    connection: Connection,
) -> Mapping[str, object]:
    """Deliberately assemble the non-secret effective configuration snapshot.

    The demonstrated v1 effective runtime/session configuration available at
    this service boundary is the routed Connection's adapter type plus its
    canonical non-secret ``safe_config`` (already canonical JSON by the
    Connection domain invariant). Nothing else enters the snapshot: never raw
    credentials/tokens/auth secrets, initialization Markdown, rendered
    schemas, Workspace guidance, control/prompt bodies, or transcripts.
    """
    try:
        return canonical_effective_config_snapshot(
            {
                "adapter": connection.adapter.value,
                "configuration": dict(connection.safe_config),
            }
        )
    except TaskAgentSessionDomainError as error:
        raise ConflictError(
            "the effective runtime/session configuration snapshot could not be canonicalized"
        ) from error


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
    happened and is never undone). The derived account-operational barrier
    (#97) is the FIRST lock acquisition, so READY is never finalized past a
    claimed account deletion. The existing atomic persistence primitive is the
    only writer: it applies only while the binding is CONNECTING with a NULL
    identity, installs the exact external session identity, the non-secret
    effective configuration snapshot, and the reported provenance atomically,
    and stamps ``initialized_at``.

    A rejected conditional write is reclassified from the reloaded binding:
    the same exact READY identity is idempotent success, a different READY
    identity is a continuity conflict, LOST/ENDED are terminal conflicts, and
    a missing/mis-scoped binding fails closed.
    """
    with composed_transaction(pool) as transaction_pool:
        workspace = _require_workspace(transaction_pool, workspace_id)
        require_account_operational(transaction_pool, profile_id=workspace.owner_profile_id)
        initialized = initialize_task_agent_session(
            transaction_pool,
            task_id=task_id,
            role=role,
            external_session_id=created.external_session_id,
            effective_config_snapshot=dict(snapshot),
            reported_provider=created.reported_provider,
            reported_model=created.reported_model,
            reported_runtime_version=created.reported_runtime_version,
        )
        if initialized is not None:
            return initialized
        existing = _reload_scoped_binding(
            transaction_pool, workspace_id=workspace_id, task_id=task_id, role=role
        )
        if existing.lifecycle_status is TaskSessionLifecycleStatus.READY:
            if existing.external_session_id == created.external_session_id:
                # A concurrent finalization of the SAME identity won the
                # conditional write: idempotent success, never a rewrite.
                return existing
            raise SessionContinuityConflictError(
                "the task/role session binding is already initialized with a "
                "different external session"
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
            "the task/role session binding did not finalize READY from its "
            "expected CONNECTING state"
        )


def establish_task_agent_session(
    pool: DatabasePool,
    adapter: AgentRuntimeAdapter,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
) -> TaskAgentSession:
    """Establish one admitted Task/role binding through runtime readiness.

    Completes the exact CONNECTING binding reserved by the #68 admission
    service: one logical #67 ``create_session`` readiness handshake whose
    success already proves the fresh isolated external context, the delivered
    canonical #66 initialization, and a valid #65 ``session_ready`` — then the
    durable CONNECTING -> READY transition installing the exact external
    session identity, the non-secret effective configuration snapshot, and the
    reported provenance exactly once.

    Returns the READY binding. An exact already-READY binding is idempotent
    success with no runtime call. Raises the typed authorization/not-found
    vocabulary (uniform :class:`NotFoundError` for a missing/unscoped
    Workspace, Task, binding, role binding, or Connection), the typed
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
        route = get_role_binding(pool, workspace_id=workspace_id, role=role)
        if route is None or route.workspace_id != workspace_id:
            raise NotFoundError(
                "the requested workflow role binding is not available in this workspace"
            )
        if route.connection_id != binding.connection_id:
            raise SessionContinuityConflictError(
                "the task/role session binding points at a different connection "
                "than the current route"
            )
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
        snapshot = _assemble_effective_config_snapshot(connection)
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
