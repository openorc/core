"""Atomic TaskAgentSession capacity admission and reservation (issue #68).

One application-service operation admits one or more required workflow roles
for the same Task as a single capacity decision: it resolves the authorized
Workspace/Task and the Owner-configured role -> Connection routing, locks
every distinct target Connection row in deterministic UUID order, counts the
active (CONNECTING or READY) occupancy under those locks, and either reserves
every missing ``(Task, role)`` binding as CONNECTING together, or reserves
nothing and reports a capacity wait. The decision is durable admission
control against Owner-configured ``session_capacity`` — never runtime-
discovered, never process-local memory, and never a runtime health check.

Composition order inside the one short transaction:

1. the account-operational barrier (issue #97) — the FIRST lock acquisition,
   so admission never holds Connection locks while waiting on the Profile and
   never creates new reservations past a claimed account deletion;
2. Workspace/Task authorization and the authorized role-binding resolution;
3. the deterministic Connection row locks (the serialization boundary for
   admission on each Connection);
4. existing-binding classification (idempotent reuse, route conflict,
   terminal historical session) and grouped occupancy counting;
5. the all-or-nothing capacity decision and reservation.

No external I/O of any kind occurs inside the transaction: this leaf performs
no Agent Runtime call — runtime creation, initialization, readiness, loss, and
end handling belong to later capabilities (issue #130). Waiting for capacity
is a returned result, deliberately not an exception: it is a scheduling state,
not a failure, and no speculative queue record is persisted here (Task FIFO
queueing is issue #76).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import (
    TaskAgentSession,
    TaskAgentSessionDomainError,
    TaskSessionLifecycleStatus,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.connections import list_connections_for_update
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import (
    count_active_task_agent_sessions_by_connection,
    ensure_task_agent_session,
    get_task_agent_session,
)
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import (
    require_workspace_role_binding,
    require_workspace_task,
)

__all__ = [
    "AdmissionStatus",
    "BlockedCapacityFact",
    "ConnectionNotAdmissibleError",
    "SessionAdmissionOutcome",
    "SessionRouteConflictError",
    "TerminalSessionConflictError",
    "admit_task_agent_sessions",
]

_SERVICE_TRACER_SCOPE = "openorc.services.session_admission"
_ADMIT_SPAN_NAME = "session_admission.admit_task_agent_sessions"


class SessionRouteConflictError(ConflictError):
    """The existing Task/role binding points at a different Connection.

    Current role-binding changes affect future Tasks and sessions only: they
    never silently repoint a historical Task session to the newly routed
    Connection, and admission never adopts different routing for an existing
    binding.
    """


class TerminalSessionConflictError(ConflictError):
    """The existing Task/role binding is a terminal historical session.

    A LOST or ENDED binding remains historical: it is never replaced, never
    reopened, and never re-admitted, so a successor session cannot be
    established for that Task/role through this operation.
    """


class ConnectionNotAdmissibleError(ConflictError):
    """A target Connection is currently disabled for OpenOrc use.

    ``enabled`` is Owner-controlled eligibility, nothing more: this outcome
    says the Owner has switched the Connection off for admission. It is never
    a runtime reachability, health, or initialization observation.
    """


class AdmissionStatus(StrEnum):
    """The two durable admission outcomes of one capacity decision."""

    ADMITTED = "admitted"
    AWAITING_CAPACITY = "awaiting_capacity"


@dataclass(frozen=True, slots=True)
class BlockedCapacityFact:
    """Safe scheduling/diagnostic facts for one blocking Connection.

    Only non-secret occupancy facts are carried: the blocking Connection
    identity, its Owner-configured capacity, and its current active occupancy
    at decision time. These are observations for scheduling/diagnostics —
    never a second copy of canonical state and never a persisted queue record.
    """

    connection_id: UUID
    session_capacity: int
    occupied_count: int


@dataclass(frozen=True, slots=True)
class SessionAdmissionOutcome:
    """The complete result of one atomic multi-role admission decision.

    ``ADMITTED`` carries the full requested role -> TaskAgentSession mapping
    (existing same-route CONNECTING/READY bindings returned as-is, missing
    roles newly reserved). ``AWAITING_CAPACITY`` carries an empty mapping and
    the blocking Connections' facts; it is a scheduling state, not a failure.
    """

    status: AdmissionStatus
    sessions: Mapping[WorkflowRole, TaskAgentSession]
    blocked: tuple[BlockedCapacityFact, ...]


def _normalize_requested_roles(roles: Sequence[WorkflowRole]) -> tuple[WorkflowRole, ...]:
    """Validate and normalize the requested roles: supported values, deduplicated.

    Command-shape validation happens before authorization or any database
    effect: the roles must be WorkflowRole members (Producer/Reviewer are the
    only supported workflow roles in v1) and the request must request at least
    one. Duplicates collapse deterministically so one request can never ask
    for two reservations of the same (Task, role).
    """
    if not isinstance(roles, Sequence) or isinstance(roles, (str, bytes)) or len(roles) == 0:
        raise InvalidCommandError(
            "at least one workflow role must be requested for session admission"
        )
    requested: list[WorkflowRole] = []
    for role in roles:
        if not isinstance(role, WorkflowRole):
            raise InvalidCommandError(
                "session admission roles must be WorkflowRole (producer or reviewer) values"
            )
        if role not in requested:
            requested.append(role)
    return tuple(requested)


def _connection_lock_order(connection_ids: Iterable[UUID]) -> tuple[UUID, ...]:
    """Return the deterministic ascending-UUID lock order over distinct Connections.

    The admission-side half of the serialization contract (issue #68): every
    admission path locks its target Connection rows in this order before
    counting occupancy and reserving, so overlapping Connection sets never
    interleave their lock acquisitions inconsistently and cannot deadlock.
    The locking SELECT's ``order by id`` is the durable guarantee that the
    rows are locked in exactly this order.
    """
    return tuple(sorted(set(connection_ids)))


def admit_task_agent_sessions(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    roles: Sequence[WorkflowRole],
) -> SessionAdmissionOutcome:
    """Admit and reserve the requested Task roles' agent sessions atomically.

    One short, database-only transaction decides every requested role
    together: all-or-nothing across roles and Connections. Existing
    same-route CONNECTING/READY bindings are returned unchanged and consume
    occupancy without consuming a duplicate reservation; a missing binding is
    established as CONNECTING only when every target Connection has capacity
    for the whole request.

    Raises the typed authorization/not-found vocabulary through the existing
    Workspace boundary (:class:`NotFoundError` for a missing/unowned
    Workspace, Task, role binding, or Connection; the uniform not-found
    outcome never distinguishes absence from cross-Workspace scope), and the
    typed conflicts defined by this module for a different-route existing
    binding, a terminal historical binding, or a disabled Connection. Waiting
    for capacity is not an error: it is the returned
    :class:`AdmissionStatus.AWAITING_CAPACITY` outcome with only safe facts.

    Concurrency correctness: the account-operational barrier is taken before
    any subject read or Connection lock; the Connection row locks are the
    serialization boundary for capacity on each Connection and are taken in
    deterministic ascending UUID order; every admission path must take the
    same locks before its capacity count and reservation — no process-local
    semaphore is authority. No external call occurs while the transaction is
    open, and no runtime adapter is invoked anywhere in this operation.
    """
    requested_roles = _normalize_requested_roles(roles)

    with application_span(_SERVICE_TRACER_SCOPE, _ADMIT_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ADMIT_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            # The account-operational barrier (issue #97) is the FIRST lock
            # acquisition of every guarded Owner mutation: before any subject
            # read and before any Connection row lock, so admission never
            # holds Connection locks while waiting on the Profile and never
            # creates new reservations past a claimed account deletion.
            require_account_operational(transaction_pool, profile_id=profile_id)

            # Exact Owner + Task scope via the #53 authorization boundary.
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )

            # Authorized role -> Connection routing for every requested role.
            routes: dict[WorkflowRole, UUID] = {}
            for role in requested_roles:
                binding = require_workspace_role_binding(
                    transaction_pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    role=role,
                )
                routes[role] = binding.connection_id

            target_ids = set(routes.values())
            lock_order = _connection_lock_order(target_ids)

            # The Connection row locks are the serialization boundary: taken
            # in deterministic order, and every classification below is made
            # from the locked rows (current durable state under the lock),
            # never from a racy pre-lock read.
            locked_connections = list_connections_for_update(
                transaction_pool, connection_ids=lock_order
            )
            locked_by_id = {connection.id: connection for connection in locked_connections}
            for connection_id in lock_order:
                connection = locked_by_id.get(connection_id)
                if connection is None or connection.workspace_id != workspace_id:
                    raise NotFoundError(
                        "the requested connection is not available in this workspace"
                    )
                if not connection.enabled:
                    raise ConnectionNotAdmissibleError(
                        "the connection routed for this role is currently disabled"
                    )

            # Classify existing bindings before counting: idempotent same-route
            # reuse, or the typed conflict outcomes, per role.
            existing_sessions: dict[WorkflowRole, TaskAgentSession] = {}
            needed_reservations: dict[UUID, int] = {}
            for role in requested_roles:
                session = get_task_agent_session(transaction_pool, task_id=task_id, role=role)
                if session is None:
                    route_id = routes[role]
                    needed_reservations[route_id] = needed_reservations.get(route_id, 0) + 1
                    continue
                if session.workspace_id != workspace_id or session.task_id != task_id:
                    raise NotFoundError(
                        "the requested agent session is not available in this workspace"
                    )
                if session.connection_id != routes[role]:
                    raise SessionRouteConflictError(
                        "the existing task/role session binding points at a different connection"
                    )
                if session.lifecycle_status in (
                    TaskSessionLifecycleStatus.LOST,
                    TaskSessionLifecycleStatus.ENDED,
                ):
                    raise TerminalSessionConflictError(
                        "the existing task/role session binding is a terminal historical session"
                    )
                existing_sessions[role] = session

            # Occupancy under the locks + new reservations needed per
            # Connection. A shared Connection's Producer and Reviewer each
            # consume their own slot.
            active_counts = count_active_task_agent_sessions_by_connection(
                transaction_pool, connection_ids=lock_order
            )
            blocked: list[BlockedCapacityFact] = []
            for connection_id in lock_order:
                connection = locked_by_id[connection_id]
                needed = needed_reservations.get(connection_id, 0)
                occupied = active_counts.get(connection_id, 0) + needed
                # Only NEW reservations are gated by capacity: a request whose
                # every role is already reserved (needed 0) returns its
                # existing bindings even where current occupancy exceeds the
                # configured capacity — for example after an Owner lowered
                # session_capacity while sessions were active. Existing
                # bindings consume occupancy, never a duplicate reservation.
                if needed > 0 and occupied > connection.session_capacity:
                    blocked.append(
                        BlockedCapacityFact(
                            connection_id=connection_id,
                            session_capacity=connection.session_capacity,
                            occupied_count=active_counts.get(connection_id, 0),
                        )
                    )
            if blocked:
                # Waiting for capacity is a scheduling state, not a failure:
                # nothing is reserved, nothing is written, and no speculative
                # queue record is persisted here.
                return SessionAdmissionOutcome(
                    status=AdmissionStatus.AWAITING_CAPACITY,
                    sessions=MappingProxyType({}),
                    blocked=tuple(blocked),
                )

            # Every Connection has capacity for the whole request: establish
            # all missing bindings as CONNECTING in this same transaction.
            # The existing establishment primitive keeps its idempotence and
            # different-route rejection semantics; a concurrent race that
            # converges on a disagreeing binding fails closed as a typed
            # route conflict, and the outer transaction rolls back the whole
            # admission.
            admitted: dict[WorkflowRole, TaskAgentSession] = dict(existing_sessions)
            for role in requested_roles:
                if role in admitted:
                    continue
                try:
                    session = ensure_task_agent_session(
                        transaction_pool,
                        workspace_id=workspace_id,
                        task_id=task_id,
                        role=role,
                        connection_id=routes[role],
                    )
                except TaskAgentSessionDomainError as exc:
                    raise SessionRouteConflictError(
                        "the task/role session binding conflicts with the current route"
                    ) from exc
                admitted[role] = session

            return SessionAdmissionOutcome(
                status=AdmissionStatus.ADMITTED,
                sessions=MappingProxyType(admitted),
                blocked=(),
            )
