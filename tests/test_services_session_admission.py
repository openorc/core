"""Deterministic tests for the TaskAgentSession admission service (issue #68).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the account-operational barrier composes FIRST (before
any subject read or Connection lock, per the issue #97 Owner-mutation rule),
the exact Owner/Task/role-binding authorization composition, the
deterministic Connection lock order, existing-binding classification
(idempotent reuse without duplicate capacity, route conflict, terminal
historical session), the all-or-nothing capacity decision across roles and
Connections, and that waiting for capacity writes nothing. Durable
concurrency semantics — the last-slot race, all-or-nothing reservation under
row locks, shared-Connection occupancy, and duplicate-free concurrent replay
— are proven by the integration-marked suite against a real Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.observability import OPERATION, TASK_ID, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import session_admission
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.session_admission import (
    AdmissionStatus,
    ConnectionNotAdmissibleError,
    SessionRouteConflictError,
    TerminalSessionConflictError,
    _connection_lock_order,
    admit_task_agent_sessions,
)

_OBSERVED = datetime(2026, 9, 26, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
    """Returns one canned result, like a psycopg cursor (row or row list)."""

    def __init__(self, result: tuple[Any, ...] | list[tuple[Any, ...]] | None) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._result is None:
            return None
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._result is None:
            return []
        if isinstance(self._result, list):
            return list(self._result)
        return [self._result]


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | list[tuple[Any, ...]] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("admission service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(workspace_id: Any, owner_profile_id: Any) -> tuple[Any, ...]:
    return (
        workspace_id,
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        "",
    )


def _task_row(workspace_id: Any) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        uuid.uuid4(),
        42,
        42,
        "ready_to_plan",
        None,
        None,
        uuid.uuid4(),
        None,
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _binding_row(workspace_id: Any, role: str, connection_id: Any) -> tuple[Any, ...]:
    # Canned row matching the #162 binding columns: the scripted flows use
    # the unconfigured configuration state.
    return (
        uuid.uuid4(),
        workspace_id,
        role,
        connection_id,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _connection_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    enabled: bool = True,
    capacity: int = 1,
) -> tuple[Any, ...]:
    return (
        connection_id,
        workspace_id,
        "cline",
        "primary cline hub",
        {},
        capacity,
        enabled,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _session_row(
    workspace_id: Any,
    task_id: Any,
    role: str,
    connection_id: Any,
    *,
    lifecycle_status: str = "connecting",
    external_session_id: str | None = None,
    initialized_at: Any = None,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        task_id,
        role,
        connection_id,
        external_session_id,
        lifecycle_status,
        {} if external_session_id is not None else None,
        None,
        None,
        None,
        initialized_at,
        _OBSERVED if lifecycle_status == "ended" else None,
        _OBSERVED,
        _OBSERVED,
    )


def _binding_reads(
    workspace_id: Any, owner_profile_id: Any, *routes: tuple[str, Any]
) -> list[tuple[Any, ...] | list[tuple[Any, ...]] | None]:
    """The workspace + binding canned pair for each requested role, in order."""
    reads: list[tuple[Any, ...] | list[tuple[Any, ...]] | None] = []
    for role, connection_id in routes:
        reads.append(_ws_row(workspace_id, owner_profile_id))
        reads.append(_binding_row(workspace_id, role, connection_id))
    return reads


def _session_insert_indexes(conn: ScriptedConnection) -> list[int]:
    return [
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "insert into openorc.task_agent_sessions" in sql
    ]


def test_single_role_admission_reserves_one_connecting_binding() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    session_row = _session_row(workspace_id, task_id, "producer", connection_id)
    conn = ScriptedConnection(
        [
            (None, None, None),  # account-operational barrier: no attempt state
            _ws_row(workspace_id, profile_id),  # require_workspace_task
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", connection_id)),
            [_connection_row(workspace_id, connection_id)],  # locked Connection rows
            None,  # no existing (Task, producer) binding
            [],  # no active occupancy on the Connection
            session_row,  # establishment returns the new CONNECTING binding
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER],
    )

    assert outcome.status is AdmissionStatus.ADMITTED
    assert outcome.blocked == ()
    assert set(outcome.sessions) == {WorkflowRole.PRODUCER}
    producer = outcome.sessions[WorkflowRole.PRODUCER]
    assert producer.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    assert producer.connection_id == connection_id
    assert producer.task_id == task_id

    # Composition order: the account-operational barrier is the FIRST
    # statement — before any subject read and before any Connection lock.
    assert "openorc.profiles" in conn.executed[0][0]
    assert "for key share" in conn.executed[0][0]
    assert "openorc.workspaces" in conn.executed[1][0]
    assert "openorc.tasks" in conn.executed[2][0]
    assert "openorc.workflow_role_bindings" in conn.executed[4][0]
    # The Connection lock is a single FOR UPDATE statement over the distinct
    # targets in ascending id order.
    lock_sql, lock_params = conn.executed[5]
    assert "openorc.connections" in lock_sql
    assert "where id = any(%s) order by id for update" in lock_sql
    assert lock_params == ([connection_id],)
    assert _session_insert_indexes(conn) == [8]


def test_two_roles_on_distinct_connections_admit_together() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    # Deliberately request the LARGER id first to prove the lock order is
    # ascending UUIDs, not request order.
    first_connection, second_connection = sorted((uuid.uuid4(), uuid.uuid4()))
    producer_row = _session_row(workspace_id, task_id, "producer", second_connection)
    reviewer_row = _session_row(workspace_id, task_id, "reviewer", first_connection)
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(
                workspace_id,
                profile_id,
                ("producer", second_connection),
                ("reviewer", first_connection),
            ),
            [
                _connection_row(workspace_id, first_connection),
                _connection_row(workspace_id, second_connection),
            ],
            None,  # producer binding absent
            None,  # reviewer binding absent
            [],  # no occupancy on either Connection
            producer_row,
            reviewer_row,
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )

    assert outcome.status is AdmissionStatus.ADMITTED
    assert set(outcome.sessions) == {WorkflowRole.PRODUCER, WorkflowRole.REVIEWER}
    lock_sql, lock_params = conn.executed[7]
    assert "order by id for update" in lock_sql
    # Deterministic ascending UUID order regardless of request order.
    assert lock_params == ([first_connection, second_connection],)
    inserts = _session_insert_indexes(conn)
    assert len(inserts) == 2


def test_two_roles_sharing_one_connection_each_consume_a_slot() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(
                workspace_id,
                profile_id,
                ("producer", connection_id),
                ("reviewer", connection_id),
            ),
            [_connection_row(workspace_id, connection_id, capacity=2)],
            None,
            None,
            [],
            _session_row(workspace_id, task_id, "producer", connection_id),
            _session_row(workspace_id, task_id, "reviewer", connection_id),
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )

    assert outcome.status is AdmissionStatus.ADMITTED
    assert outcome.blocked == ()
    # One shared Connection, two reserved bindings: both consume capacity.
    lock_sql, lock_params = conn.executed[7]
    assert lock_params == ([connection_id],)
    assert len(_session_insert_indexes(conn)) == 2
    count_sql, count_params = conn.executed[10]
    assert "group by connection_id" in count_sql
    assert count_params == ([connection_id],)


def test_capacity_one_shared_connection_admits_neither_role() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(
                workspace_id,
                profile_id,
                ("producer", connection_id),
                ("reviewer", connection_id),
            ),
            [_connection_row(workspace_id, connection_id, capacity=1)],
            None,
            None,
            [],
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )

    # All-or-nothing: a capacity-1 Connection cannot admit both roles, so
    # NEITHER is reserved and nothing is written.
    assert outcome.status is AdmissionStatus.AWAITING_CAPACITY
    assert outcome.sessions == {}
    assert outcome.blocked == (
        session_admission.BlockedCapacityFact(
            connection_id=connection_id,
            session_capacity=1,
            occupied_count=0,
        ),
    )
    assert _session_insert_indexes(conn) == []


def test_partial_capacity_across_two_connections_reserves_nothing() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    occupied_connection, free_connection = sorted((uuid.uuid4(), uuid.uuid4()))
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(
                workspace_id,
                profile_id,
                ("producer", occupied_connection),
                ("reviewer", free_connection),
            ),
            [
                _connection_row(workspace_id, occupied_connection, capacity=1),
                _connection_row(workspace_id, free_connection, capacity=1),
            ],
            None,
            None,
            [(occupied_connection, 1)],  # the producer route is already full
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )

    # One blocking Connection out of two blocks the WHOLE admission: the
    # role with room is not partially reserved.
    assert outcome.status is AdmissionStatus.AWAITING_CAPACITY
    assert outcome.sessions == {}
    assert outcome.blocked == (
        session_admission.BlockedCapacityFact(
            connection_id=occupied_connection,
            session_capacity=1,
            occupied_count=1,
        ),
    )
    assert _session_insert_indexes(conn) == []


def test_existing_connecting_binding_is_reused_without_duplicate_reservation() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    existing_producer = _session_row(workspace_id, task_id, "producer", connection_id)
    new_reviewer = _session_row(workspace_id, task_id, "reviewer", connection_id)
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(
                workspace_id,
                profile_id,
                ("producer", connection_id),
                ("reviewer", connection_id),
            ),
            [_connection_row(workspace_id, connection_id, capacity=2)],
            existing_producer,  # producer binding already established
            None,  # reviewer binding absent
            [(connection_id, 1)],  # the existing binding occupies one slot
            new_reviewer,
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )

    assert outcome.status is AdmissionStatus.ADMITTED
    # The existing CONNECTING binding is returned unchanged — same identity —
    # and consumed occupancy, but no duplicate reservation was created for it.
    assert outcome.sessions[WorkflowRole.PRODUCER].id == existing_producer[0]
    assert outcome.sessions[WorkflowRole.PRODUCER].lifecycle_status is (
        TaskSessionLifecycleStatus.CONNECTING
    )
    assert outcome.sessions[WorkflowRole.REVIEWER].connection_id == connection_id
    # Exactly one new binding was established: the reviewer's.
    inserts = _session_insert_indexes(conn)
    assert len(inserts) == 1
    insert_params = conn.executed[inserts[0]][1]
    assert insert_params is not None
    assert "reviewer" in insert_params


def test_existing_ready_bindings_are_returned_even_if_occupancy_exceeds_capacity() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    producer = _session_row(
        workspace_id,
        task_id,
        "producer",
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-producer",
        initialized_at=_OBSERVED,
    )
    reviewer = _session_row(
        workspace_id,
        task_id,
        "reviewer",
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-reviewer",
        initialized_at=_OBSERVED,
    )
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(
                workspace_id,
                profile_id,
                ("producer", connection_id),
                ("reviewer", connection_id),
            ),
            # The Owner lowered capacity after admission; occupancy (2) now
            # exceeds the configured capacity (1).
            [_connection_row(workspace_id, connection_id, capacity=1)],
            producer,
            reviewer,
            [(connection_id, 2)],
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )

    # No NEW reservation is requested, so the repeated admission returns the
    # existing initialized bindings without consuming duplicate capacity.
    assert outcome.status is AdmissionStatus.ADMITTED
    assert outcome.blocked == ()
    assert outcome.sessions[WorkflowRole.PRODUCER].id == producer[0]
    assert outcome.sessions[WorkflowRole.REVIEWER].id == reviewer[0]
    assert outcome.sessions[WorkflowRole.PRODUCER].lifecycle_status is (
        TaskSessionLifecycleStatus.READY
    )
    assert _session_insert_indexes(conn) == []


def test_role_binding_change_after_existing_binding_is_a_route_conflict() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    historical_connection, repointed_connection = sorted((uuid.uuid4(), uuid.uuid4()))
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", repointed_connection)),
            [_connection_row(workspace_id, repointed_connection)],
            # The binding was established against the historical Connection;
            # the role binding has since been repointed. Admission must not
            # silently repoint the historical Task session.
            _session_row(workspace_id, task_id, "producer", historical_connection),
        ]
    )

    with pytest.raises(SessionRouteConflictError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    # Nothing was written: the conflict is classified from durable state and
    # the whole transaction rolls back.
    assert _session_insert_indexes(conn) == []


@pytest.mark.parametrize("terminal_status", ["lost", "ended"])
def test_terminal_historical_bindings_are_never_replaced_or_reopened(
    terminal_status: str,
) -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", connection_id)),
            [_connection_row(workspace_id, connection_id)],
            _session_row(
                workspace_id,
                task_id,
                "producer",
                connection_id,
                lifecycle_status=terminal_status,
                external_session_id="ext-historical",
                initialized_at=_OBSERVED,
            ),
        ]
    )

    with pytest.raises(TerminalSessionConflictError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    # The LOST/ENDED binding remains historical: no successor row, no writes.
    assert _session_insert_indexes(conn) == []


def test_disabled_connection_is_not_admissible() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", connection_id)),
            [_connection_row(workspace_id, connection_id, enabled=False)],
        ]
    )

    with pytest.raises(ConnectionNotAdmissibleError) as raised:
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    # Disabled eligibility is a configuration outcome, not a runtime health
    # or reachability observation: the message says nothing about the runtime.
    message = str(raised.value).lower()
    assert "disabled" in message
    assert "unreachable" not in message and "health" not in message
    assert _session_insert_indexes(conn) == []


def test_missing_connection_fails_closed_as_not_found() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", connection_id)),
            [],  # the routed Connection row does not exist
        ]
    )

    with pytest.raises(NotFoundError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )
    assert _session_insert_indexes(conn) == []


def test_missing_role_binding_fails_closed_as_not_found() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            _ws_row(workspace_id, profile_id),
            None,  # no binding for the requested role
        ]
    )

    with pytest.raises(NotFoundError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )
    # The failure happens before any Connection lock or write.
    assert all("openorc.connections" not in sql for sql, _params in conn.executed)


def test_missing_task_fails_closed_as_not_found() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            None,  # the addressed Task does not exist
        ]
    )

    with pytest.raises(NotFoundError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )
    assert len(conn.executed) == 3


def test_account_deletion_attempt_blocks_admission_before_any_effect() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            ("active", uuid.uuid4(), _OBSERVED),  # durable deletion attempt state
        ]
    )

    with pytest.raises(ConflictError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    # The barrier is the ONLY statement executed: no subject read, no
    # Connection lock, and no reservation is created past the claim.
    assert len(conn.executed) == 1
    assert "openorc.profiles" in conn.executed[0][0]
    assert "for key share" in conn.executed[0][0]


def test_missing_profile_fails_closed_at_the_barrier() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection([None])  # the Profile row itself is absent

    with pytest.raises(NotFoundError):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )
    assert len(conn.executed) == 1


def test_invalid_command_shapes_are_rejected_before_any_database_access() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    conn = ScriptedConnection([])

    for bad_roles in ([], "producer", ["producer"], [None], [123]):
        with pytest.raises(InvalidCommandError):
            admit_task_agent_sessions(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                roles=cast(Any, bad_roles),
            )
    # Command-shape validation precedes authorization and every effect.
    assert conn.executed == []


def test_duplicate_requested_roles_collapse_to_one_reservation() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", connection_id)),
            [_connection_row(workspace_id, connection_id, capacity=2)],
            None,
            [],
            _session_row(workspace_id, task_id, "producer", connection_id),
        ]
    )

    outcome = admit_task_agent_sessions(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.PRODUCER],
    )

    assert outcome.status is AdmissionStatus.ADMITTED
    # One (Task, role) request: one binding read pair, one insert.
    binding_sqls = [
        sql for sql, _params in conn.executed if "openorc.workflow_role_bindings" in sql
    ]
    assert len(binding_sqls) == 1
    assert len(_session_insert_indexes(conn)) == 1


def test_deterministic_lock_order_is_distinct_and_ascending() -> None:
    ordered = sorted((uuid.uuid4(), uuid.uuid4(), uuid.uuid4()))
    shuffled = [ordered[2], ordered[0], ordered[2], ordered[1], ordered[0]]

    assert _connection_lock_order(shuffled) == (ordered[0], ordered[1], ordered[2])
    assert _connection_lock_order([]) == ()


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


@pytest.mark.parametrize("field_name", ["profile_id", "workspace_id", "task_id"])
@pytest.mark.parametrize("malformed", ["not-a-uuid", 123, None])
def test_malformed_identifiers_fail_inside_the_span_without_telemetry_or_database_access(
    field_name: str, malformed: Any
) -> None:
    # Regression (issue #109 service-span contract): malformed caller-supplied
    # identifiers are classified as InvalidCommandError INSIDE the use-case
    # span, before telemetry annotation, and their raw values never enter
    # exported span attributes — and no database access happens at all.
    provider, exporter = _local_provider_with_exporter()
    kwargs: dict[str, Any] = {
        "profile_id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
    }
    kwargs[field_name] = malformed
    conn = ScriptedConnection([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        admit_task_agent_sessions(
            _pool(conn),
            roles=[WorkflowRole.PRODUCER],
            **kwargs,
        )

    assert conn.executed == []
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "session_admission.admit_task_agent_sessions"
    assert exported.status is not None
    assert exported.status.status_code is StatusCode.ERROR
    assert exported.status.description == "InvalidCommandError"
    # Validation ran before annotation: no identifier attributes were attached.
    attributes = exported.attributes or {}
    assert WORKSPACE_ID not in attributes
    assert TASK_ID not in attributes
    if malformed is not None:
        assert str(malformed) not in str(attributes)


def test_malformed_roles_fail_inside_the_span_without_database_access() -> None:
    provider, exporter = _local_provider_with_exporter()
    conn = ScriptedConnection([])

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(InvalidCommandError),
    ):
        admit_task_agent_sessions(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            roles=cast(Any, ["producer"]),
        )

    assert conn.executed == []
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "session_admission.admit_task_agent_sessions"
    assert exported.status is not None
    assert exported.status.status_code is StatusCode.ERROR
    assert exported.status.description == "InvalidCommandError"
    # Validation ran before annotation: not even the safe identifiers were
    # attached, and the malformed role value never entered telemetry.
    assert not (exported.attributes or {})
    assert "producer" not in str(exported.attributes or {})


def test_admission_annotates_safe_identifiers_in_its_use_case_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (None, None, None),
            _ws_row(workspace_id, profile_id),
            _task_row(workspace_id),
            *_binding_reads(workspace_id, profile_id, ("producer", connection_id)),
            [_connection_row(workspace_id, connection_id)],
            None,
            [],
            _session_row(workspace_id, task_id, "producer", connection_id),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        outcome = admit_task_agent_sessions(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )
    assert outcome.status is AdmissionStatus.ADMITTED

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "session_admission.admit_task_agent_sessions"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[OPERATION] == "session_admission.admit_task_agent_sessions"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
    assert attributes[TASK_ID] == str(task_id)
