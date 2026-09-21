"""Integration-marked persistence tests for Task agent sessions (issue #22).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
Task/role session-binding invariants directly: idempotent establishment for
the same Task/role and Connection with deterministic failure against a
different one, one binding per (task, role) as a database backstop,
Producer/Reviewer sessions sharing one Connection as distinct sessions,
irreplaceable initialized external session identities scoped per Connection,
LOST/ENDED as lifecycle states on the same binding, Connection-scoped
occupancy counting, historical session-configuration stability, Workspace
scope agreement, and the initialization-coherence CHECKs. They are excluded
from the ordinary deterministic baseline by the repository pytest
configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_agent_session_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and
outside agent responsibility: in the normal Owner local-development flow the
target is the ephemeral non-production Supabase branch that the Owner-only
``devserver.sh`` command creates and later deletes (agents never invoke it).
The session fixture merely resets the ``openorc`` schema and applies the
committed migrations from scratch within the supplied database, and the suite
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation
from psycopg.types.json import Jsonb

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskAgentSessionDomainError, TaskSessionLifecycleStatus
from openorc.persistence import connections as connection_repositories
from openorc.persistence import sessions as session_repositories
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction.

    Repository calls run inside a nested psycopg transaction (a SAVEPOINT on
    the already-active per-test transaction), mirroring psycopg_pool's
    per-checkout transaction semantics: a successful call releases the
    savepoint, and an expected constraint violation rolls back only to it —
    the per-test transaction stays valid so the remaining assertions run
    instead of failing with ``InFailedSqlTransaction``.
    """

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            with self._connection.transaction():
                yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): every Profile needs its
    # backing Auth user row. The inserts roll back with the test transaction.
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(
    conn: Connection[Any], owner_profile_id: uuid.UUID, name: str = "workspace"
) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, name),
    )
    return workspace_id


def _insert_project(
    conn: Connection[Any], workspace_id: uuid.UUID, name: str = "project"
) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, name),
    )
    return project_id


def _insert_repository(
    conn: Connection[Any],
    *,
    project_id: uuid.UUID,
    workspace_id: uuid.UUID,
    github_repository_id: int,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            github_repository_id,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _ownership_chain(
    conn: Connection[Any], *, github_repository_id: int = 60_000_001
) -> tuple[uuid.UUID, uuid.UUID]:
    """Create Profile -> Workspace -> Project -> Repository; return (ws, repo)."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn,
        project_id=project_id,
        workspace_id=workspace_id,
        github_repository_id=github_repository_id,
    )
    return workspace_id, repository_id


def _insert_task(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int,
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan')",
        (task_id, workspace_id, repository_id, github_issue_id, 100),
    )
    return task_id


def _insert_connection(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    name: str = "primary hub",
    session_capacity: int = 1,
) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, session_capacity) "
        "values (%s, %s, 'cline', %s, %s)",
        (connection_id, workspace_id, name, session_capacity),
    )
    return connection_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def test_ensure_is_idempotent_for_the_same_task_role_and_connection(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9101
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    first = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    assert first.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    assert first.external_session_id is None
    assert first.initialized_at is None

    # Repeated establishment reuses the existing binding: same row, same
    # identity, no second row.
    second = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    assert second.id == first.id
    assert second.created_at == first.created_at
    assert second.connection_id == connection_id

    assert (
        _row_count(
            conn,
            "select count(*) from openorc.task_agent_sessions "
            "where task_id = %s and role = 'producer'",
            (task_id,),
        )
        == 1
    )


def test_producer_and_reviewer_can_share_one_connection_as_distinct_sessions(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9102
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    producer = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    reviewer = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.REVIEWER,
        connection_id=connection_id,
    )

    # The two roles share the physical runtime (same Connection) while
    # remaining distinct session bindings.
    assert producer.id != reviewer.id
    assert producer.role is WorkflowRole.PRODUCER
    assert reviewer.role is WorkflowRole.REVIEWER
    assert producer.connection_id == reviewer.connection_id == connection_id


def test_ensure_against_a_different_connection_fails_deterministically(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9103
    )
    first_connection = _insert_connection(conn, workspace_id=workspace_id, name="first")
    second_connection = _insert_connection(conn, workspace_id=workspace_id, name="second")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=first_connection,
    )
    # Establishing the already-bound Task/role against a different Connection
    # is a deterministic conflict: never a silent repoint, replacement, or
    # second row.
    with pytest.raises(TaskAgentSessionDomainError):
        session_repositories.ensure_task_agent_session(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            connection_id=second_connection,
        )

    binding = conn.execute(
        "select connection_id from openorc.task_agent_sessions "
        "where task_id = %s and role = 'producer'",
        (task_id,),
    ).fetchone()
    assert binding is not None
    assert binding[0] == first_connection
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.task_agent_sessions where task_id = %s",
            (task_id,),
        )
        == 1
    )


def test_direct_duplicate_insert_cannot_create_a_second_row(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9104
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)

    # The database UNIQUE constraint is the durable backstop behind the
    # idempotent repository path; establishment explicitly supplies the
    # CONNECTING lifecycle (the schema carries no default). (The direct
    # statement needs its own savepoint so the failed insert cannot abort
    # the per-test transaction.)
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_agent_sessions "
            "(workspace_id, task_id, role, connection_id, lifecycle_status) "
            "values (%s, %s, 'producer', %s, 'connecting')",
            (workspace_id, task_id, connection_id),
        )
        conn.execute(
            "insert into openorc.task_agent_sessions "
            "(workspace_id, task_id, role, connection_id, lifecycle_status) "
            "values (%s, %s, 'producer', %s, 'connecting')",
            (workspace_id, task_id, connection_id),
        )


def test_initialize_is_irreplaceable_and_sets_the_ready_facts(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9105
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    initialized = session_repositories.initialize_task_agent_session(
        pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-alpha",
        effective_config_snapshot={"stage": "plan"},
    )
    assert initialized is not None
    assert initialized.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert initialized.external_session_id == "ext-session-alpha"
    assert dict(initialized.effective_config_snapshot) == {"stage": "plan"}  # type: ignore[arg-type]
    assert initialized.initialized_at is not None

    # A replacement session can never overwrite the bound identity: the
    # conditional initialization is a rejected no-op on an initialized row.
    replacement = session_repositories.initialize_task_agent_session(
        pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-beta",
        effective_config_snapshot={"stage": "plan"},
    )
    assert replacement is None

    reloaded = session_repositories.get_task_agent_session(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    assert reloaded is not None
    assert reloaded.id == initialized.id
    assert reloaded.external_session_id == "ext-session-alpha"
    assert reloaded.lifecycle_status is TaskSessionLifecycleStatus.READY


def test_external_session_identity_is_non_reusable_within_a_connection(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    first_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9106
    )
    second_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9107
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=first_task,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=second_task,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    session_repositories.initialize_task_agent_session(
        pool,
        task_id=first_task,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-shared",
        effective_config_snapshot={"stage": "plan"},
    )

    # The same non-null external session identity cannot be bound to two
    # Task/role bindings on the same Connection.
    with pytest.raises(UniqueViolation):
        session_repositories.initialize_task_agent_session(
            pool,
            task_id=second_task,
            role=WorkflowRole.PRODUCER,
            external_session_id="ext-session-shared",
            effective_config_snapshot={"stage": "plan"},
        )
    # A distinct identity initializes normally.
    distinct = session_repositories.initialize_task_agent_session(
        pool,
        task_id=second_task,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-other",
        effective_config_snapshot={"stage": "plan"},
    )
    assert distinct is not None


def test_external_session_identity_is_scoped_per_connection(conn: Connection[Any]) -> None:
    first_workspace, first_repository = _ownership_chain(conn, github_repository_id=60_000_101)
    second_workspace, second_repository = _ownership_chain(conn, github_repository_id=60_000_102)
    first_task = _insert_task(
        conn, workspace_id=first_workspace, repository_id=first_repository, github_issue_id=9201
    )
    second_task = _insert_task(
        conn, workspace_id=second_workspace, repository_id=second_repository, github_issue_id=9202
    )
    first_connection = _insert_connection(conn, workspace_id=first_workspace, name="hub-a")
    second_connection = _insert_connection(conn, workspace_id=second_workspace, name="hub-b")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=first_workspace,
        task_id=first_task,
        role=WorkflowRole.PRODUCER,
        connection_id=first_connection,
    )
    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=second_workspace,
        task_id=second_task,
        role=WorkflowRole.PRODUCER,
        connection_id=second_connection,
    )
    first = session_repositories.initialize_task_agent_session(
        pool,
        task_id=first_task,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-same-opaque-string",
        effective_config_snapshot={"stage": "plan"},
    )
    second = session_repositories.initialize_task_agent_session(
        pool,
        task_id=second_task,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-same-opaque-string",
        effective_config_snapshot={"stage": "plan"},
    )
    # Identity uniqueness is scoped to the Connection: different Connections
    # may independently bind the same opaque string.
    assert first is not None and second is not None
    assert first.id != second.id


def test_lost_is_lifecycle_state_on_the_same_binding(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    ready_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9108
    )
    connecting_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9109
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=ready_task,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    session_repositories.initialize_task_agent_session(
        pool,
        task_id=ready_task,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-lost-later",
        effective_config_snapshot={"stage": "plan"},
    )
    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=connecting_task,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )

    lost = session_repositories.mark_task_agent_session_lost(
        pool, task_id=ready_task, role=WorkflowRole.PRODUCER
    )
    assert lost is not None
    assert lost.lifecycle_status is TaskSessionLifecycleStatus.LOST
    # LOST is lifecycle history on the SAME row, with the bound identity
    # preserved: never a replacement trigger.
    assert lost.initialized_at is not None
    assert lost.external_session_id == "ext-session-lost-later"

    # CONNECTING is not a bound session that can be lost.
    assert (
        session_repositories.mark_task_agent_session_lost(
            pool, task_id=connecting_task, role=WorkflowRole.PRODUCER
        )
        is None
    )
    # LOST is absorbing.
    assert (
        session_repositories.mark_task_agent_session_lost(
            pool, task_id=ready_task, role=WorkflowRole.PRODUCER
        )
        is None
    )

    reloaded = session_repositories.get_task_agent_session(
        pool, task_id=ready_task, role=WorkflowRole.PRODUCER
    )
    assert reloaded is not None
    assert reloaded.external_session_id == "ext-session-lost-later"


def test_ended_before_initialization_keeps_null_identity_with_ended_at(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9110
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    ended = session_repositories.mark_task_agent_session_ended(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    # The establishment attempt ended without ever binding an external
    # session: both coherent NULLs, plus the semantic ended_at timestamp.
    assert ended is not None
    assert ended.lifecycle_status is TaskSessionLifecycleStatus.ENDED
    assert ended.external_session_id is None
    assert ended.initialized_at is None
    assert ended.ended_at is not None


def test_ended_after_initialization_preserves_the_bound_identity(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9111
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    session_repositories.initialize_task_agent_session(
        pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-ends-later",
        effective_config_snapshot={"stage": "plan"},
    )
    ended = session_repositories.mark_task_agent_session_ended(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    assert ended is not None
    assert ended.lifecycle_status is TaskSessionLifecycleStatus.ENDED
    assert ended.external_session_id == "ext-session-ends-later"
    assert ended.initialized_at is not None
    assert ended.ended_at is not None
    # Generic updated_at does not substitute for the semantic timestamp.
    assert ended.updated_at is not None


def test_absorbing_terminal_states_reject_further_transitions(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    lost_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9112
    )
    ended_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9113
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    for task, identity in ((lost_task, "ext-session-a"), (ended_task, "ext-session-b")):
        session_repositories.ensure_task_agent_session(
            pool,
            workspace_id=workspace_id,
            task_id=task,
            role=WorkflowRole.PRODUCER,
            connection_id=connection_id,
        )
        session_repositories.initialize_task_agent_session(
            pool,
            task_id=task,
            role=WorkflowRole.PRODUCER,
            external_session_id=identity,
            effective_config_snapshot={"stage": "plan"},
        )
    session_repositories.mark_task_agent_session_lost(
        pool, task_id=lost_task, role=WorkflowRole.PRODUCER
    )
    session_repositories.mark_task_agent_session_ended(
        pool, task_id=ended_task, role=WorkflowRole.PRODUCER
    )

    # Lost and ended bindings reject every further transition.
    assert (
        session_repositories.mark_task_agent_session_ended(
            pool, task_id=lost_task, role=WorkflowRole.PRODUCER
        )
        is None
    )
    assert (
        session_repositories.initialize_task_agent_session(
            pool,
            task_id=lost_task,
            role=WorkflowRole.PRODUCER,
            external_session_id="ext-session-replacement",
            effective_config_snapshot={"stage": "plan"},
        )
        is None
    )
    assert (
        session_repositories.mark_task_agent_session_lost(
            pool, task_id=ended_task, role=WorkflowRole.PRODUCER
        )
        is None
    )
    assert (
        session_repositories.mark_task_agent_session_ended(
            pool, task_id=ended_task, role=WorkflowRole.PRODUCER
        )
        is None
    )


def test_shared_connection_occupancy_counts_against_capacity(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    producer_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9114
    )
    reviewer_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9115
    )
    # Owner-configured capacity of 1: concurrency is not enabled, yet the
    # shared Connection is where both roles' occupancy is accounted.
    connection_id = _insert_connection(
        conn, workspace_id=workspace_id, name="capacity hub", session_capacity=1
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    for task, role, identity in (
        (producer_task, WorkflowRole.PRODUCER, "ext-session-producer"),
        (reviewer_task, WorkflowRole.REVIEWER, "ext-session-reviewer"),
    ):
        session_repositories.ensure_task_agent_session(
            pool,
            workspace_id=workspace_id,
            task_id=task,
            role=role,
            connection_id=connection_id,
        )
        session_repositories.initialize_task_agent_session(
            pool,
            task_id=task,
            role=role,
            external_session_id=identity,
            effective_config_snapshot={"stage": "plan"},
        )

    # Capacity accounting is Connection-scoped: Producer and Reviewer
    # sessions sharing one Connection each consume occupancy against that
    # Connection's Owner-configured session_capacity. Both active sessions
    # are counted; the admission decision itself belongs to later services.
    active = session_repositories.list_active_task_agent_sessions(pool, connection_id=connection_id)
    assert len(active) == 2
    assert {session.role for session in active} == {WorkflowRole.PRODUCER, WorkflowRole.REVIEWER}
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.task_agent_sessions "
            "where connection_id = %s and lifecycle_status in ('connecting', 'ready')",
            (connection_id,),
        )
        == 2
    )


def test_later_configuration_changes_do_not_rewrite_historical_configuration(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9116
    )
    first_connection = _insert_connection(conn, workspace_id=workspace_id, name="first hub")
    second_connection = _insert_connection(conn, workspace_id=workspace_id, name="second hub")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=first_connection,
    )
    session_repositories.initialize_task_agent_session(
        pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-historical",
        effective_config_snapshot={"stage": "plan", "runtime": "cline"},
    )

    # Later Workspace role-binding and Connection configuration changes affect
    # only future sessions.
    connection_repositories.update_connection(
        pool,
        first_connection,
        name="renamed hub",
        safe_config={"base_url": "https://hub.example.com/changed"},
        session_capacity=4,
        enabled=False,
        auth_reference=None,
    )
    connection_repositories.set_role_binding(
        pool,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=second_connection,
    )

    reloaded = session_repositories.get_task_agent_session(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    assert reloaded is not None
    # The initialized session's historical configuration is untouched: the
    # snapshot, bound Connection, and identity are all preserved.
    assert dict(reloaded.effective_config_snapshot) == {  # type: ignore[arg-type]
        "stage": "plan",
        "runtime": "cline",
    }
    assert reloaded.connection_id == first_connection
    assert reloaded.external_session_id == "ext-session-historical"
    assert reloaded.lifecycle_status is TaskSessionLifecycleStatus.READY


def test_workspace_scope_consistency_is_enforced(conn: Connection[Any]) -> None:
    first_workspace, first_repository = _ownership_chain(conn, github_repository_id=60_000_201)
    second_workspace, _second_repository = _ownership_chain(conn, github_repository_id=60_000_202)
    # A Task in the first Workspace and a Connection in the second Workspace:
    # the composite foreign keys make cross-Workspace binding corruption
    # impossible through the repository path.
    task_id = _insert_task(
        conn, workspace_id=first_workspace, repository_id=first_repository, github_issue_id=9117
    )
    other_workspace_connection = _insert_connection(
        conn, workspace_id=second_workspace, name="other workspace hub"
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # The Connection-reference foreign key is DEFERRABLE INITIALLY DEFERRED
    # (issue #27 deletion semantics); a released repository SAVEPOINT does
    # not check it, so force the pending check at the assertion point.
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        session_repositories.ensure_task_agent_session(
            pool,
            workspace_id=first_workspace,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            connection_id=other_workspace_connection,
        )
        conn.execute(
            "set constraints openorc.task_agent_sessions_connection_id_workspace_id_fkey immediate"
        )


def test_initialization_coherence_rejects_mixed_state_rows(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9118
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    stamp = datetime(2026, 9, 17, 12, 0, 0, tzinfo=UTC)

    insert = (
        "insert into openorc.task_agent_sessions "
        "(workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, initialized_at, ended_at) "
        "values (%s, %s, 'producer', %s, %s, %s, %s, %s)"
    )

    # READY/LOST require all three initialization facts non-NULL; CONNECTING
    # requires all three NULL; ENDED permits either coherent form; mixed forms
    # match no branch and are rejected for every lifecycle status. These rows
    # omit the snapshot column (NULL by default), so any initialized status
    # below also violates the extended coherence.
    incoherent_rows = [
        # READY with the identity set but no initialization instant, and with
        # the snapshot NULL: partial initialization facts.
        ("ext-session-1", "ready", None, None),
        # READY without an initialized identity.
        (None, "ready", stamp, None),
        # CONNECTING is by definition not a bound session.
        ("ext-session-1", "connecting", None, None),
        (None, "connecting", stamp, None),
        # ENDED mixed forms: one NULL, one non-NULL.
        ("ext-session-1", "ended", None, stamp),
        (None, "ended", stamp, stamp),
        # ended_at without the ENDED status.
        (None, "connecting", None, stamp),
    ]
    for external_session_id, status, initialized_at, ended_at in incoherent_rows:
        with pytest.raises(CheckViolation), conn.transaction():
            conn.execute(
                insert,
                (
                    workspace_id,
                    task_id,
                    connection_id,
                    external_session_id,
                    status,
                    initialized_at,
                    ended_at,
                ),
            )

    second_task = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=9119
    )
    full_insert = (
        "insert into openorc.task_agent_sessions "
        "(workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, effective_config_snapshot, initialized_at, ended_at) "
        "values (%s, %s, 'producer', %s, %s, %s, %s, %s, %s)"
    )
    # The three initialization facts move atomically: an initialized status
    # with a NULL initialization instant (or snapshot) is rejected even when
    # the identity is set.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            full_insert,
            (
                workspace_id,
                second_task,
                connection_id,
                "ext-session-partial",
                "ready",
                Jsonb({"stage": "plan"}),
                None,
                None,
            ),
        )

    # The valid coherent forms commit: CONNECTING (all three NULL), ENDED
    # before initialization (all three NULL plus the semantic ended_at), and
    # a fully initialized READY row (all three set; an empty snapshot object
    # is valid when no concrete configurable values exist).
    conn.execute(insert, (workspace_id, task_id, connection_id, None, "connecting", None, None))
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(workspace_id, task_id, role, connection_id, lifecycle_status, ended_at) "
        "values (%s, %s, 'reviewer', %s, 'ended', %s)",
        (workspace_id, task_id, connection_id, stamp),
    )
    conn.execute(
        full_insert,
        (
            workspace_id,
            second_task,
            connection_id,
            "ext-session-full",
            "ready",
            Jsonb({}),
            stamp,
            None,
        ),
    )
