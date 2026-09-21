"""Repositories for Connection and workflow role binding persistence.

Explicit SQL repositories over the ``openorc`` schema for Connection and
WorkflowRoleBinding (Phase 1, issue #20). Rows map to transport-independent
domain objects from :mod:`openorc.domain.connections`; instants returned from
Postgres are normalized to timezone-aware UTC at this boundary.

Violated database invariants surface as driver exceptions (for example
``psycopg.errors.UniqueViolation``, ``ForeignKeyViolation``, and
``CheckViolation``); translating them into typed application errors is a
service-layer concern, not a persistence one.

Credential ownership: these repositories never write or read raw OpenOrc-owned
credential material. OpenOrc-owned authentication is represented only through
the opaque nullable ``auth_reference`` boundary; runtime-owned provider/MCP/
tool credentials remain runtime-owned and are never modeled here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.connections import (
    AdapterType,
    Connection,
    WorkflowRole,
    WorkflowRoleBinding,
    canonical_safe_config,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_connection",
    "get_connection",
    "get_connection_for_update",
    "get_role_binding",
    "list_connection_bindings",
    "list_workspace_connections",
    "set_connection_auth_reference",
    "set_role_binding",
    "update_connection",
]

_CONNECTION_COLUMNS = (
    "id, workspace_id, adapter_type, name, safe_config, session_capacity, "
    "enabled, auth_reference, reported_provider, reported_model, created_at, updated_at"
)

_BINDING_COLUMNS = "id, workspace_id, role, connection_id, created_at, updated_at"


def _connection_from_row(row: Sequence[Any]) -> Connection:
    return Connection(
        id=row[0],
        workspace_id=row[1],
        adapter=AdapterType(row[2]),
        name=row[3],
        safe_config=row[4],
        session_capacity=row[5],
        enabled=row[6],
        auth_reference=row[7],
        reported_provider=row[8],
        reported_model=row[9],
        created_at=normalize_utc(row[10]),
        updated_at=normalize_utc(row[11]),
    )


def create_connection(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    adapter: AdapterType,
    name: str,
    safe_config: Mapping[str, object] | None = None,
    session_capacity: int = 1,
    enabled: bool = True,
    auth_reference: str | None = None,
    reported_provider: str | None = None,
    reported_model: str | None = None,
) -> Connection:
    """Insert one Connection scoped to its Workspace.

    ``session_capacity`` is Owner-configured admission control (default 1:
    concurrency is explicitly enabled by the Owner). ``auth_reference`` is the
    opaque OpenOrc-owned auth boundary; ``reported_provider``/``reported_model``
    are nullable runtime-reported observations, never configuration.

    ``safe_config`` is canonicalized through the domain (canonical JSON-object
    semantics) and supplied to the driver as an explicit ``Jsonb`` adapter —
    psycopg 3 does not adapt plain mappings to jsonb without one.
    """
    config = canonical_safe_config({} if safe_config is None else safe_config)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.connections "
            "(workspace_id, adapter_type, name, safe_config, session_capacity, enabled, "
            "auth_reference, reported_provider, reported_model) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_CONNECTION_COLUMNS}",
            (
                workspace_id,
                adapter.value,
                name,
                Jsonb(dict(config)),
                session_capacity,
                enabled,
                auth_reference,
                reported_provider,
                reported_model,
            ),
        ).fetchone()
    assert row is not None
    return _connection_from_row(row)


def get_connection(pool: DatabasePool, connection_id: UUID) -> Connection | None:
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_CONNECTION_COLUMNS} from openorc.connections where id = %s",
            (connection_id,),
        ).fetchone()
    return None if row is None else _connection_from_row(row)


def list_workspace_connections(pool: DatabasePool, *, workspace_id: UUID) -> list[Connection]:
    """List the Connections of one Workspace (Workspace Connection list path)."""
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_CONNECTION_COLUMNS} from openorc.connections "
            "where workspace_id = %s order by created_at, id",
            (workspace_id,),
        ).fetchall()
    return [_connection_from_row(row) for row in rows]


def update_connection(
    pool: DatabasePool,
    connection_id: UUID,
    *,
    name: str,
    safe_config: Mapping[str, object],
    session_capacity: int,
    enabled: bool,
    auth_reference: str | None,
) -> Connection | None:
    """Replace the mutable Owner configuration of one Connection.

    Identity, Workspace scope, adapter type, and the runtime-reported
    observation fields are never touched here. ``updated_at`` advances to the
    database clock. Returns ``None`` when the Connection does not exist.

    ``safe_config`` is canonicalized through the domain and supplied as an
    explicit ``Jsonb`` adapter, like every jsonb write at this boundary.
    """
    config = canonical_safe_config(safe_config)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.connections "
            "set name = %s, safe_config = %s, session_capacity = %s, enabled = %s, "
            "auth_reference = %s, updated_at = now() "
            "where id = %s "
            f"returning {_CONNECTION_COLUMNS}",
            (
                name,
                Jsonb(dict(config)),
                session_capacity,
                enabled,
                auth_reference,
                connection_id,
            ),
        ).fetchone()
    return None if row is None else _connection_from_row(row)


def get_connection_for_update(pool: DatabasePool, connection_id: UUID) -> Connection | None:
    """Row-locked read of one Connection for credential compositions (issue #55).

    The deliberate ``SELECT ... FOR UPDATE`` serializes credential
    configure/rotate against concurrent configuration changes and disconnect:
    the composition's decision about the current ``auth_reference`` is made
    under the same lock that guards the follow-up write. The lock is held
    only within the caller's short transaction (inside
    ``composed_transaction``, the outer composition).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_CONNECTION_COLUMNS} from openorc.connections where id = %s for update",
            (connection_id,),
        ).fetchone()
    return None if row is None else _connection_from_row(row)


def set_connection_auth_reference(
    pool: DatabasePool, connection_id: UUID, *, auth_reference: str
) -> Connection | None:
    """Install one opaque v1 auth_reference on a Connection (issue #55).

    Credential compositions call this after the row-locked read, inside the
    same outer transaction, so the Vault secret created alongside cannot
    commit without the reference (and vice versa). ``updated_at`` advances to
    the database clock. Returns ``None`` when the Connection does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.connections "
            "set auth_reference = %s, updated_at = now() "
            "where id = %s "
            f"returning {_CONNECTION_COLUMNS}",
            (auth_reference, connection_id),
        ).fetchone()
    return None if row is None else _connection_from_row(row)


def set_role_binding(
    pool: DatabasePool, *, workspace_id: UUID, role: WorkflowRole, connection_id: UUID
) -> WorkflowRoleBinding:
    """Point one workflow role at one Connection (mutable configuration upsert).

    Exactly one binding exists per ``(workspace_id, role)``. Re-calling this
    for the same ``(workspace_id, role)`` repoints the existing binding: the
    binding row identity (``id`` and ``created_at``) is preserved — a
    configuration change, not historical replacement. The upsert cannot point
    a binding at another Workspace's Connection: the composite foreign key
    makes that a ForeignKeyViolation.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.workflow_role_bindings (workspace_id, role, connection_id) "
            "values (%s, %s, %s) "
            "on conflict (workspace_id, role) do update "
            "set connection_id = excluded.connection_id, updated_at = now() "
            f"returning {_BINDING_COLUMNS}",
            (workspace_id, role.value, connection_id),
        ).fetchone()
    assert row is not None
    return _binding_from_row(row)


def get_role_binding(
    pool: DatabasePool, *, workspace_id: UUID, role: WorkflowRole
) -> WorkflowRoleBinding | None:
    """Return the single binding for one workflow role in one Workspace."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_BINDING_COLUMNS} from openorc.workflow_role_bindings "
            "where workspace_id = %s and role = %s",
            (workspace_id, role.value),
        ).fetchone()
    return None if row is None else _binding_from_row(row)


def list_connection_bindings(
    pool: DatabasePool, *, connection_id: UUID
) -> list[WorkflowRoleBinding]:
    """List the workflow role bindings that point at one Connection."""
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_BINDING_COLUMNS} from openorc.workflow_role_bindings "
            "where connection_id = %s order by created_at, id",
            (connection_id,),
        ).fetchall()
    return [_binding_from_row(row) for row in rows]


def _binding_from_row(row: Sequence[Any]) -> WorkflowRoleBinding:
    return WorkflowRoleBinding(
        id=row[0],
        workspace_id=row[1],
        role=WorkflowRole(row[2]),
        connection_id=row[3],
        created_at=normalize_utc(row[4]),
        updated_at=normalize_utc(row[5]),
    )
