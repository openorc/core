"""Repositories for Task agent session persistence.

Explicit SQL repositories over the ``openorc`` schema for TaskAgentSession
(Phase 1, issue #22). Rows map to transport-independent domain objects from
:mod:`openorc.domain.sessions`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Continuity invariants enforced here:

- ``ensure_task_agent_session`` is the idempotent establishment path for the
  one binding per (Task, role): an insert that conflicts reuses the existing
  row when it agrees on Workspace and Connection, and raises
  ``TaskAgentSessionDomainError`` when the binding points at a different
  Connection. Repeated establishment never creates a second row and never
  silently adopts different routing.
- ``initialize_task_agent_session`` is the only writer of
  ``external_session_id`` and applies only while the binding is CONNECTING
  with a NULL identity. The initialization facts (``external_session_id``,
  ``initialized_at``, and ``effective_config_snapshot``) move atomically with
  the binding. Once initialized, no code path in this module can
  replace the external session: LOST and ENDED are lifecycle states on the
  same row, never replacement triggers. ``effective_config_snapshot`` is
  written only from the caller-assembled explicit parameter through the
  domain canonicalizer; repositories never derive it from Connection rows
  and never write authentication material into it.
- ``mark_task_agent_session_lost`` applies only from READY (CONNECTING is by
  definition not a bound session that can be lost);
  ``mark_task_agent_session_ended`` applies from CONNECTING or READY and
  stamps the semantic ``ended_at`` atomically. Absorbing states reject
  further transitions by returning None: a stale/terminal no-op must not be
  retried blindly, and translating that outcome into typed application
  errors is a service-layer concern, not a persistence one.
- ``list_active_task_agent_sessions`` provides Connection-scoped active
  occupancy accounting (CONNECTING or READY) for later capacity admission
  services; admission decisions are not made here.
- ``list_task_agent_sessions_by_external_session_id`` is the read-only exact
  external-session lookup for the durable construction-context resolver
  (issue #189): the partial unique index guarantees at most one non-null
  identity per (Connection, external session), so zero matches and multiple
  matches are both returned as plain facts and classified by services —
  never guessed here.
- ``count_active_task_agent_sessions_by_connection`` is the grouped occupancy
  count over several Connections for the capacity admission service (issue
  #68): representation-level counting only, never an admission decision.
- The session cost checkpoint (issue #240) is the exact-scope, high-water
  write of one binding's retained aggregate Cline-reported USD cost, and the
  retained-cost aggregation is the role-agnostic read that derives a Task's
  observed expenditure from every TaskAgentSession matching the Task. No
  workflow policy is decided here: classification and enforcement belong to
  services.

Violated database invariants (uniqueness, foreign keys, CHECK constraints)
surface as driver exceptions (for example ``psycopg.errors.UniqueViolation``,
``ForeignKeyViolation``, and ``CheckViolation``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import (
    TaskAgentSession,
    TaskAgentSessionDomainError,
    TaskSessionLifecycleStatus,
    canonical_effective_config_snapshot,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "TaskRetainedSessionCosts",
    "checkpoint_task_agent_session_cost",
    "count_active_task_agent_sessions_by_connection",
    "ensure_task_agent_session",
    "get_task_agent_session",
    "get_task_agent_session_for_update",
    "initialize_task_agent_session",
    "list_active_task_agent_sessions",
    "list_task_agent_sessions",
    "list_task_agent_sessions_by_external_session_id",
    "mark_task_agent_session_ended",
    "mark_task_agent_session_lost",
    "sum_task_retained_session_costs",
]

_SESSION_COLUMNS = (
    "id, workspace_id, task_id, role, connection_id, external_session_id, "
    "lifecycle_status, effective_config_snapshot, "
    "reported_provider, reported_model, reported_runtime_version, "
    "initialized_at, ended_at, created_at, updated_at, "
    "retained_cost_usd, cost_observed_at"
)


def _session_from_row(row: Sequence[Any]) -> TaskAgentSession:
    return TaskAgentSession(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        role=WorkflowRole(row[3]),
        connection_id=row[4],
        external_session_id=row[5],
        lifecycle_status=TaskSessionLifecycleStatus(row[6]),
        effective_config_snapshot=row[7],
        reported_provider=row[8],
        reported_model=row[9],
        reported_runtime_version=row[10],
        initialized_at=None if row[11] is None else normalize_utc(row[11]),
        ended_at=None if row[12] is None else normalize_utc(row[12]),
        created_at=normalize_utc(row[13]),
        updated_at=normalize_utc(row[14]),
        retained_cost_usd=row[15],
        cost_observed_at=None if row[16] is None else normalize_utc(row[16]),
    )


def _require_role(role: object) -> WorkflowRole:
    if not isinstance(role, WorkflowRole):
        raise TaskAgentSessionDomainError(
            "TaskAgentSession.role must be a WorkflowRole (producer or reviewer)"
        )
    return role


def ensure_task_agent_session(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    connection_id: UUID,
) -> TaskAgentSession:
    """Establish the one Task/role session binding, idempotently.

    Exactly one binding exists per ``(task_id, role)``. When the binding does
    not exist it is created as CONNECTING (no external session identity yet):
    establishment semantics are explicit at the write boundary because the
    schema intentionally carries no lifecycle default.
    When it already exists, the existing row is returned unchanged — repeated
    establishment is idempotent and never creates a second row — but only
    when it agrees on the Workspace and the Connection: establishing the
    already-bound Task/role against a different Connection raises
    ``TaskAgentSessionDomainError`` instead of silently adopting, repointing,
    or replacing the binding. The database UNIQUE constraint is the durable
    backstop; a direct duplicate insert raises ``UniqueViolation``.
    """
    _require_role(role)
    with transaction(pool) as conn:
        # Establishment explicitly writes the CONNECTING lifecycle: the
        # schema deliberately declares lifecycle_status NOT NULL with no
        # default, so a fresh binding's semantics are set here, not implied.
        row = conn.execute(
            "insert into openorc.task_agent_sessions "
            "(workspace_id, task_id, role, connection_id, lifecycle_status) "
            "values (%s, %s, %s, %s, 'connecting') "
            "on conflict (task_id, role) do nothing "
            f"returning {_SESSION_COLUMNS}",
            (workspace_id, task_id, role.value, connection_id),
        ).fetchone()
        if row is None:
            # The (task_id, role) conflict means the binding already exists:
            # reuse it instead of creating a second row. The select runs in
            # the same transaction; READ COMMITTED re-reads see the committed
            # conflicting row.
            row = conn.execute(
                f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
                "where task_id = %s and role = %s",
                (task_id, role.value),
            ).fetchone()
    assert row is not None  # a conflict implies the existing row
    session = _session_from_row(row)
    if session.workspace_id != workspace_id:
        raise TaskAgentSessionDomainError(
            "the existing Task/role session binding belongs to a different Workspace"
        )
    if session.connection_id != connection_id:
        raise TaskAgentSessionDomainError(
            "the Task/role is already bound to a different Connection; establishment "
            "is not idempotent across Connections and the binding is never repointed "
            "or replaced"
        )
    return session


def get_task_agent_session(
    pool: DatabasePool, *, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession | None:
    """Return the single session binding for one Task role, or None."""
    _require_role(role)
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
            "where task_id = %s and role = %s",
            (task_id, role.value),
        ).fetchone()
    return None if row is None else _session_from_row(row)


def get_task_agent_session_for_update(
    pool: DatabasePool, *, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession | None:
    """Read one Task role's session binding under an exclusive row lock.

    Serves the Level 2 triage's evidence serialization (issue #251): the
    binding's lifecycle status moves through conditional updates (``lost``,
    ``ended``) that never rotate the Task state token, so a disposition
    decision that depends on the binding's applicability must observe it
    under the same row lock the lifecycle writers take. Composed inside a
    service composition, the lock is held on the transaction-scoped
    connection; outside one, only for this function's own short transaction.
    Translation into typed application outcomes is a service-layer concern.
    """
    _require_role(role)
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
            "where task_id = %s and role = %s for update",
            (task_id, role.value),
        ).fetchone()
    return None if row is None else _session_from_row(row)


def list_task_agent_sessions_by_external_session_id(
    pool: DatabasePool, *, external_session_id: str
) -> list[TaskAgentSession]:
    """List the bindings bound to one exact external session identity.

    Read-only lookup for the durable construction-context resolver (issue
    #189), keyed by the exact opaque non-null external-session identity. The
    schema's partial unique index guarantees at most one non-null identity
    per ``(Connection, external_session_id)``, so the ordinary result is
    exactly one row; zero matches and multiple matches (possible only across
    Connections) are both returned as plain facts and classified by the
    calling service — never guessed here. Deterministically ordered for
    stable diagnostics.
    """
    if not isinstance(external_session_id, str) or not external_session_id.strip():
        raise TaskAgentSessionDomainError(
            "external_session_id must be a non-empty opaque external-session identity"
        )
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
            "where external_session_id = %s "
            "order by connection_id, id",
            (external_session_id,),
        ).fetchall()
    return [_session_from_row(row) for row in rows]


def initialize_task_agent_session(
    pool: DatabasePool,
    *,
    task_id: UUID,
    role: WorkflowRole,
    external_session_id: str,
    effective_config_snapshot: Mapping[str, object],
    reported_provider: str | None = None,
    reported_model: str | None = None,
    reported_runtime_version: str | None = None,
) -> TaskAgentSession | None:
    """Initialize the binding's external session exactly once.

    The only writer of ``external_session_id``: the conditional UPDATE applies
    only while the binding is CONNECTING with a NULL identity, so an already
    initialized binding (READY, LOST, or ENDED) is a rejected no-op — a
    replacement session can never overwrite the bound identity, and a lost or
    ended binding can never be re-initialized. Sets READY, stamps
    ``initialized_at``, and captures the initialization facts, which move
    atomically with the bound identity.

    ``effective_config_snapshot`` is the required caller-assembled NON-SECRET
    effective runtime/session configuration snapshot, canonicalized through
    the domain and supplied as an explicit ``Jsonb`` adapter (like every
    jsonb write at this boundary). An empty mapping is valid when there are
    no concrete configurable values; NULL is not — initialization facts are
    never partially set. Repositories never derive the snapshot from
    Connection rows, and authentication material must never enter it. Once
    set, no update path in this module rewrites the snapshot: it is
    historical for the initialized session, and later Connection/role-binding
    configuration changes affect only future sessions.

    The reported provenance fields remain nullable opaque observations;
    absence is valid for them.

    Returns the updated binding, or ``None`` when the binding is missing or
    not in the CONNECTING state (a rejected no-op that must not be retried
    blindly; translating that outcome into typed application errors is a
    service-layer concern).
    """
    _require_role(role)
    if not isinstance(external_session_id, str) or not external_session_id.strip():
        raise TaskAgentSessionDomainError("external_session_id must be a non-empty opaque string")
    if effective_config_snapshot is None:
        raise TaskAgentSessionDomainError(
            "effective_config_snapshot must be supplied at initialization; use an "
            "empty mapping when there are no concrete configurable values"
        )
    snapshot = canonical_effective_config_snapshot(effective_config_snapshot)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_agent_sessions "
            "set external_session_id = %s, lifecycle_status = 'ready', "
            "effective_config_snapshot = %s, "
            "reported_provider = %s, reported_model = %s, reported_runtime_version = %s, "
            "initialized_at = now(), updated_at = now() "
            "where task_id = %s and role = %s "
            "and lifecycle_status = 'connecting' and external_session_id is null "
            f"returning {_SESSION_COLUMNS}",
            (
                external_session_id,
                Jsonb(dict(snapshot)),
                reported_provider,
                reported_model,
                reported_runtime_version,
                task_id,
                role.value,
            ),
        ).fetchone()
    return None if row is None else _session_from_row(row)


def mark_task_agent_session_lost(
    pool: DatabasePool, *, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession | None:
    """Record genuine loss of the exact external session on the same binding.

    Applies only from READY: CONNECTING is by definition not yet a bound
    session that can be lost, and LOST is absorbing. LOST is lifecycle state
    on the SAME binding — never a replacement trigger; the bound
    ``external_session_id`` is preserved. Later workflow services translate
    LOST into AGENT_SESSION_LOST blocking behavior; runtime/Hub unavailability
    is not session loss and is not represented here.

    Returns the updated binding, or ``None`` when the binding is missing, not
    READY, or already lost/ended (a rejected no-op that must not be retried
    blindly).
    """
    _require_role(role)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_agent_sessions "
            "set lifecycle_status = 'lost', updated_at = now() "
            "where task_id = %s and role = %s and lifecycle_status = 'ready' "
            f"returning {_SESSION_COLUMNS}",
            (task_id, role.value),
        ).fetchone()
    return None if row is None else _session_from_row(row)


def mark_task_agent_session_ended(
    pool: DatabasePool, *, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession | None:
    """End the binding's session, stamping the semantic ``ended_at``.

    Applies from CONNECTING (the establishment attempt ends before ever
    binding an external session — the binding stays valid with a NULL
    identity) or from READY (an initialized session terminates normally).
    ENDED is absorbing; ``ended_at`` is set exactly once, in the same
    statement as the status, and is never substituted by ``updated_at``.

    Returns the updated binding, or ``None`` when the binding is missing or
    already lost/ended (a rejected no-op that must not be retried blindly).
    """
    _require_role(role)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_agent_sessions "
            "set lifecycle_status = 'ended', ended_at = now(), updated_at = now() "
            "where task_id = %s and role = %s "
            "and lifecycle_status in ('connecting', 'ready') "
            f"returning {_SESSION_COLUMNS}",
            (task_id, role.value),
        ).fetchone()
    return None if row is None else _session_from_row(row)


def list_active_task_agent_sessions(
    pool: DatabasePool, *, connection_id: UUID
) -> list[TaskAgentSession]:
    """List the active (CONNECTING or READY) sessions bound to one Connection.

    Connection-scoped occupancy accounting for later capacity admission
    services: Producer and Reviewer sessions sharing one Connection each
    consume occupancy against that Connection's Owner-configured
    ``session_capacity``. Admission decisions are not made here.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
            "where connection_id = %s "
            "and lifecycle_status in ('connecting', 'ready') "
            "order by created_at, id",
            (connection_id,),
        ).fetchall()
    return [_session_from_row(row) for row in rows]


def count_active_task_agent_sessions_by_connection(
    pool: DatabasePool, *, connection_ids: Sequence[UUID]
) -> dict[UUID, int]:
    """Count the active (CONNECTING or READY) sessions per given Connection.

    One grouped occupancy query for the capacity admission service (issue
    #68): Connection-scoped counts of exactly the active statuses, mirroring
    :func:`list_active_task_agent_sessions`. Connections with no active
    sessions are absent from the returned mapping. The admission decision —
    comparing counts against a Connection's Owner-configured
    ``session_capacity`` and reserving — is not made here. Under admission
    concurrency the counts are meaningful only after the caller has locked
    the same Connection rows this query addresses.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            "select connection_id, count(*) from openorc.task_agent_sessions "
            "where connection_id = any(%s) "
            "and lifecycle_status in ('connecting', 'ready') "
            "group by connection_id",
            (list(connection_ids),),
        ).fetchall()
    return {row[0]: int(row[1]) for row in rows}


def list_task_agent_sessions(pool: DatabasePool, *, task_id: UUID) -> list[TaskAgentSession]:
    """List every TaskAgentSession binding of one Task, in deterministic order.

    The role-agnostic read behind the Task expenditure aggregate (issue
    #240): every binding row matching the exact Task — every lifecycle
    status and every role that exists — selected by ``task_id`` alone with
    no role predicate and no role branch anywhere in the statement. Whether
    a future workflow topology admits additional role bindings changes
    nothing about this read: ownership is the Task, not a role list.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_SESSION_COLUMNS} from openorc.task_agent_sessions "
            "where task_id = %s order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_session_from_row(row) for row in rows]


def checkpoint_task_agent_session_cost(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    connection_id: UUID,
    retained_cost_usd: Decimal,
) -> TaskAgentSession | None:
    """Retain one binding's aggregate reported cost as its high-water checkpoint.

    The narrow conditional checkpoint update (issue #240): the write applies
    only to the exact binding row addressed by its id AND its full
    Task/Workspace/Connection ownership tuple, and only when the reported
    amount is strictly greater than the currently retained one (or the
    binding has never reported). The retained value is therefore the
    high-water reading: repeated equal observations, out-of-order reads,
    and a Hub restart's counter reset can never decrease the retained
    amount or double count — the resulting undercount after a reset is
    accepted (no Hub epoch identity and no retrospective reconstruction
    exist). The observation instant is stamped by the database clock in the
    same statement, never substituted by ``updated_at``.

    The amount must be a nonnegative finite exact decimal (the domain and
    the durable CHECK enforce it). Returns the updated binding, or ``None``
    when the observation does not apply: a stale, foreign (cross-Task/
    Workspace/Connection), or not-higher reading matches no row and is a
    no-op that must not be retried blindly — classification is a
    service-layer concern.
    """
    if (
        isinstance(retained_cost_usd, bool)
        or not isinstance(retained_cost_usd, Decimal)
        or not retained_cost_usd.is_finite()
        or retained_cost_usd < 0
    ):
        raise TaskAgentSessionDomainError(
            "checkpoint_task_agent_session_cost requires a nonnegative finite "
            "USD amount (an exact decimal)"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_agent_sessions "
            "set retained_cost_usd = %s, cost_observed_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s "
            "and connection_id = %s "
            "and (retained_cost_usd is null or retained_cost_usd < %s) "
            f"returning {_SESSION_COLUMNS}",
            (
                retained_cost_usd,
                task_agent_session_id,
                task_id,
                workspace_id,
                connection_id,
                retained_cost_usd,
            ),
        ).fetchone()
    return None if row is None else _session_from_row(row)


@dataclass(frozen=True, slots=True)
class TaskRetainedSessionCosts:
    """The role-agnostic retained-cost facts of one Task's session bindings.

    ``known_total_usd`` is the sum of the retained known per-session cost
    checkpoints across every TaskAgentSession matching the Task — ``None``
    when no session has ever reported a cost (unknown, never a fabricated
    zero). ``has_unobserved_sessions`` records that at least one binding
    row carries no retained checkpoint, so a caller can distinguish a
    complete aggregate from one with unknown constituents without the
    aggregate ever pretending to be billing-grade.
    """

    known_total_usd: Decimal | None
    has_unobserved_sessions: bool


def sum_task_retained_session_costs(
    pool: DatabasePool, *, task_id: UUID
) -> TaskRetainedSessionCosts:
    """Aggregate the retained known session costs of one Task, by ownership only.

    The derived Task expenditure fact (issue #240): one aggregate statement
    over ``openorc.task_agent_sessions`` filtered by ``task_id`` alone —
    deliberately no role predicate and no role branch, so the aggregate is
    provably independent of the v1 role vocabulary and remains correct for
    any future binding topology. The per-session retained checkpoints are
    already high-water values (subagent usage is inside the runtime's
    aggregate scope and was never double-added at the checkpoint), so the
    sum counts every constituent exactly once.

    Persistence returns the database facts only; translating them into a
    budget decision belongs to services.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select sum(retained_cost_usd), "
            "coalesce(bool_or(retained_cost_usd is null), false) "
            "from openorc.task_agent_sessions where task_id = %s",
            (task_id,),
        ).fetchone()
    assert row is not None  # an aggregate over zero rows still yields one row
    return TaskRetainedSessionCosts(
        known_total_usd=row[0],
        has_unobserved_sessions=bool(row[1]),
    )
