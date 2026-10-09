"""Integration-marked persistence tests for atomic TaskAgentSession admission.

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
admission/reservation invariants directly (issue #68): single-role capacity
reservation and idempotent replay, all-or-nothing multi-role reservation,
shared-Connection occupancy accounting under row locks, the real last-slot
race between concurrent Tasks, duplicate-free concurrent replay of the same
(Task, role), terminal/route conflict classification against real rows,
disabled eligibility, and the account-deletion barrier composition — both the
committed attempt state and the Profile-root lock serialization. They are
excluded from the ordinary deterministic baseline by the repository pytest
configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_session_admission_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and
outside agent responsibility (see tests/integration/conftest.py and
tests/README.md): the Owner-only ``devserver.sh --testdb`` flow creates and
later deletes the ephemeral branch; agents never invoke it. The suite skips
cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import LockNotAvailable, QueryCanceled

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.persistence import connections as connection_repositories
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence import sessions as session_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError
from openorc.services.session_admission import (
    AdmissionStatus,
    ConnectionNotAdmissibleError,
    SessionRouteConflictError,
    TerminalSessionConflictError,
    admit_task_agent_sessions,
)

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction.

    Service/repository calls run inside a nested psycopg transaction (a
    SAVEPOINT on the already-active per-test transaction), mirroring
    psycopg_pool's per-checkout transaction semantics; the admission
    composition's outer transaction commits on clean exit within the per-test
    transaction, which the fixture then rolls back.
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


def _apply_probe_timeouts(conn: Connection[Any]) -> None:
    """Apply the deliberate lock-probe timeouts with transaction-local SQL.

    ``lock_timeout`` maps to ``LockNotAvailable`` and ``statement_timeout`` to
    ``QueryCanceled``; whichever raises first is the blocking signal. Without
    one, a probe would wait indefinitely on a deliberately held root lock.
    """
    conn.execute("set local lock_timeout = '500ms'")
    conn.execute("set local statement_timeout = '2s'")


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary: every Profile needs its backing Auth
    # user row.
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(conn: Connection[Any], owner_profile_id: uuid.UUID) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, "workspace"),
    )
    return workspace_id


def _insert_project(conn: Connection[Any], workspace_id: uuid.UUID) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    return project_id


def _insert_repository(
    conn: Connection[Any], *, project_id: uuid.UUID, workspace_id: uuid.UUID
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
            60_000_001,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _ownership_chain(conn: Connection[Any]) -> tuple[uuid.UUID, uuid.UUID]:
    """Create Profile -> Workspace -> Project -> Repository; return (ws, repo)."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(conn, project_id=project_id, workspace_id=workspace_id)
    return workspace_id, repository_id


def _insert_task(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int = 100,
    github_issue_number: int = 100,
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "state_token, source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan', %s, %s)",
        (
            task_id,
            workspace_id,
            repository_id,
            github_issue_id,
            github_issue_number,
            uuid.uuid4(),
            "a" * 64,
        ),
    )
    return task_id


def _insert_connection(
    conn: Connection[Any], *, workspace_id: uuid.UUID, session_capacity: int = 1
) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, session_capacity) "
        "values (%s, %s, 'cline', 'primary hub', %s)",
        (connection_id, workspace_id, session_capacity),
    )
    return connection_id


def _set_role_binding(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    role: WorkflowRole,
    connection_id: uuid.UUID,
) -> None:
    conn.execute(
        "insert into openorc.workflow_role_bindings (workspace_id, role, connection_id) "
        "values (%s, %s, %s)",
        (workspace_id, role.value, connection_id),
    )


def _active_session_count(conn: Connection[Any], connection_id: uuid.UUID) -> int:
    row = conn.execute(
        "select count(*) from openorc.task_agent_sessions "
        "where connection_id = %s and lifecycle_status in ('connecting', 'ready')",
        (connection_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _cleanup_account(conn: Connection[Any], profile_id: uuid.UUID) -> None:
    """Remove committed seed rows through the sanctioned Auth-root cascade."""
    conn.execute("delete from auth.users where id = %s", (profile_id,))
    conn.commit()


def test_single_role_admission_reserves_one_connecting_binding_idempotently(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    profile_id = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert profile_id is not None
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    outcome = admit_task_agent_sessions(
        pool,
        profile_id=profile_id[0],
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER],
    )

    assert outcome.status is AdmissionStatus.ADMITTED
    assert outcome.blocked == ()
    producer = outcome.sessions[WorkflowRole.PRODUCER]
    assert producer.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    assert producer.connection_id == connection_id
    assert _active_session_count(conn, connection_id) == 1

    # Repeated admission for the same Task/roles is stable: the existing
    # CONNECTING binding is returned, no second row, no duplicate capacity.
    replay = admit_task_agent_sessions(
        pool,
        profile_id=profile_id[0],
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER],
    )
    assert replay.status is AdmissionStatus.ADMITTED
    assert replay.sessions[WorkflowRole.PRODUCER].id == producer.id
    assert _active_session_count(conn, connection_id) == 1


def test_multi_role_all_or_nothing_reserves_every_role_or_none(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=1)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.REVIEWER, connection_id=connection_id
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # A capacity-1 shared Connection cannot admit both roles: NEITHER is
    # reserved and no speculative row exists.
    outcome = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )
    assert outcome.status is AdmissionStatus.AWAITING_CAPACITY
    assert outcome.sessions == {}
    assert [fact.connection_id for fact in outcome.blocked] == [connection_id]
    assert outcome.blocked[0].session_capacity == 1
    assert outcome.blocked[0].occupied_count == 0
    assert _active_session_count(conn, connection_id) == 0

    # Raising the Owner-configured capacity admits the whole request at once.
    conn.execute(
        "update openorc.connections set session_capacity = 2 where id = %s",
        (connection_id,),
    )
    admitted = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )
    assert admitted.status is AdmissionStatus.ADMITTED
    assert set(admitted.sessions) == {WorkflowRole.PRODUCER, WorkflowRole.REVIEWER}
    assert _active_session_count(conn, connection_id) == 2


def test_shared_connection_occupancy_counting_under_row_locks(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    first_task = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=100,
        github_issue_number=100,
    )
    second_task = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=101,
        github_issue_number=101,
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=2)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.REVIEWER, connection_id=connection_id
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    first = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=first_task,
        roles=[WorkflowRole.PRODUCER, WorkflowRole.REVIEWER],
    )
    assert first.status is AdmissionStatus.ADMITTED
    assert _active_session_count(conn, connection_id) == 2

    # A second Task's admission counts the committed first-Task occupancy
    # under the same Connection row locks: 2 occupied + 1 needed > 2.
    second = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=second_task,
        roles=[WorkflowRole.PRODUCER],
    )
    assert second.status is AdmissionStatus.AWAITING_CAPACITY
    assert second.sessions == {}
    assert [(fact.connection_id, fact.occupied_count) for fact in second.blocked] == [
        (connection_id, 2)
    ]
    assert _active_session_count(conn, connection_id) == 2


def test_two_concurrent_tasks_cannot_oversubscribe_the_final_slot(
    conn: Connection[Any], migrated_database: str
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    first_task = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=100,
        github_issue_number=100,
    )
    second_task = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=101,
        github_issue_number=101,
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=1)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    # The seed rows must be visible to the racer connections: commit, and
    # remove them through the Auth-root cascade afterwards.
    conn.commit()

    statuses: list[str] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def racer(task_id: uuid.UUID) -> None:
        try:
            with connect(migrated_database) as racer_conn:
                barrier.wait()
                outcome = admit_task_agent_sessions(
                    cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                    profile_id=owner,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    roles=[WorkflowRole.PRODUCER],
                )
                statuses.append(outcome.status.value)
        except BaseException as exc:  # noqa: BLE001 — collected and asserted below
            errors.append(exc)

    threads = [threading.Thread(target=racer, args=(task,)) for task in (first_task, second_task)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(thread.is_alive() for thread in threads), "the last-slot race deadlocked"
        assert errors == []
        # Exactly one admission wins the final slot; the loser waits for
        # capacity instead of oversubscribing it.
        assert sorted(statuses) == ["admitted", "awaiting_capacity"]
        with connect(migrated_database) as verification:
            row = verification.execute(
                "select count(*) from openorc.task_agent_sessions where connection_id = %s",
                (connection_id,),
            ).fetchone()
            assert row is not None and row[0] == 1
    finally:
        _cleanup_account(conn, owner)


def test_concurrent_replay_of_the_same_task_role_creates_no_duplicate(
    conn: Connection[Any], migrated_database: str
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=1)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    conn.commit()

    statuses: list[str] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def racer() -> None:
        try:
            with connect(migrated_database) as racer_conn:
                barrier.wait()
                outcome = admit_task_agent_sessions(
                    cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                    profile_id=owner,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    roles=[WorkflowRole.PRODUCER],
                )
                statuses.append(outcome.status.value)
        except BaseException as exc:  # noqa: BLE001 — collected and asserted below
            errors.append(exc)

    threads = [threading.Thread(target=racer) for _ in range(2)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(thread.is_alive() for thread in threads), (
            "the concurrent replay race deadlocked"
        )
        assert errors == []
        # Concurrent replay of the same (Task, role) converges idempotently:
        # both admissions return the one binding.
        assert statuses == ["admitted", "admitted"]
        with connect(migrated_database) as verification:
            row = verification.execute(
                "select count(*) from openorc.task_agent_sessions "
                "where task_id = %s and role = 'producer'",
                (task_id,),
            ).fetchone()
            assert row is not None and row[0] == 1
    finally:
        _cleanup_account(conn, owner)


def test_role_binding_change_after_existing_binding_conflicts(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    historical_connection = _insert_connection(conn, workspace_id=workspace_id)
    repointed_connection = _insert_connection(conn, workspace_id=workspace_id)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=historical_connection,
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    established = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER],
    )
    assert established.status is AdmissionStatus.ADMITTED

    # The Owner repoints the role binding; the historical Task session is not
    # silently repointed with it.
    connection_repositories.set_role_binding(
        pool,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=repointed_connection,
        configured_provider=None,
        configured_model=None,
        role_prompt_override=None,
    )
    with pytest.raises(SessionRouteConflictError):
        admit_task_agent_sessions(
            pool,
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    reloaded = session_repositories.get_task_agent_session(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    assert reloaded is not None
    assert reloaded.connection_id == historical_connection
    assert reloaded.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    assert _active_session_count(conn, repointed_connection) == 0


@pytest.mark.parametrize("terminal_status", ["lost", "ended"])
def test_lost_and_ended_bindings_are_terminal_and_never_replaced(
    conn: Connection[Any], terminal_status: str
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    established = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER],
    )
    assert established.status is AdmissionStatus.ADMITTED
    external_id = None
    if terminal_status == "lost":
        # LOST applies only from READY: initialize the exact external session,
        # then record genuine loss on the same binding.
        initialized = session_repositories.initialize_task_agent_session(
            pool,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            external_session_id="ext-original",
            effective_config_snapshot={},
        )
        assert initialized is not None
        external_id = initialized.external_session_id
        lost = session_repositories.mark_task_agent_session_lost(
            pool, task_id=task_id, role=WorkflowRole.PRODUCER
        )
        assert lost is not None
    else:
        ended = session_repositories.mark_task_agent_session_ended(
            pool, task_id=task_id, role=WorkflowRole.PRODUCER
        )
        assert ended is not None

    with pytest.raises(TerminalSessionConflictError):
        admit_task_agent_sessions(
            pool,
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    # The terminal binding is historical: same row, same lifecycle, and the
    # bound identity was never replaced.
    reloaded = session_repositories.get_task_agent_session(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    assert reloaded is not None
    assert reloaded.lifecycle_status is TaskSessionLifecycleStatus(terminal_status)
    assert reloaded.external_session_id == external_id
    assert _active_session_count(conn, connection_id) == 0


def test_disabled_connection_is_ineligible_not_a_health_failure(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    conn.execute("update openorc.connections set enabled = false where id = %s", (connection_id,))
    with pytest.raises(ConnectionNotAdmissibleError):
        admit_task_agent_sessions(
            pool,
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )
    assert _active_session_count(conn, connection_id) == 0

    # Eligibility is Owner configuration only: re-enabling admits again.
    conn.execute("update openorc.connections set enabled = true where id = %s", (connection_id,))
    outcome = admit_task_agent_sessions(
        pool,
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=task_id,
        roles=[WorkflowRole.PRODUCER],
    )
    assert outcome.status is AdmissionStatus.ADMITTED
    assert _active_session_count(conn, connection_id) == 1


def test_committed_account_deletion_attempt_blocks_admission(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # A durable active deletion attempt: every guarded Owner mutation fails
    # closed until the attempt resolves (issue #97).
    attempt_id = uuid.uuid4()
    claimed = ownership_repositories.claim_account_deletion_attempt(
        pool, profile_id=owner, attempt_id=attempt_id
    )
    assert claimed is True

    with pytest.raises(ConflictError):
        admit_task_agent_sessions(
            pool,
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=[WorkflowRole.PRODUCER],
        )

    # No reservation was created past the claimed deletion.
    assert _active_session_count(conn, connection_id) == 0


def test_profile_root_lock_serializes_admission(
    conn: Connection[Any], migrated_database: str
) -> None:
    # Concurrency regression for the account-deletion barrier: admission is a
    # guarded Owner mutation, so its account-operational barrier (the Profile
    # FOR KEY SHARE read) is its FIRST lock acquisition. Concurrent with an
    # in-flight account-deletion claim (Profile-root FOR UPDATE), the whole
    # admission blocks before any subject read or Connection lock and creates
    # no reservation past the claim.
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    owner_row = conn.execute(
        "select owner_profile_id from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert owner_row is not None
    owner = owner_row[0]
    _set_role_binding(
        conn, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_id
    )
    conn.commit()

    outcomes: list[str] = []
    errors: list[BaseException] = []
    start = threading.Event()

    def racer() -> None:
        try:
            with connect(migrated_database) as racer_conn:
                start.wait()
                try:
                    with racer_conn.transaction():
                        # The deliberate probe timeouts turn a blocked barrier
                        # read into LockNotAvailable/QueryCanceled instead of
                        # deadlocking the test against the held Profile lock.
                        _apply_probe_timeouts(racer_conn)
                        outcome = admit_task_agent_sessions(
                            cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                            profile_id=owner,
                            workspace_id=workspace_id,
                            task_id=task_id,
                            roles=[WorkflowRole.PRODUCER],
                        )
                    outcomes.append(outcome.status.value)
                except (LockNotAvailable, QueryCanceled):
                    # A deliberate probe timeout raising on the blocked
                    # barrier read is direct evidence the Profile-root lock
                    # held before any reservation write (either timeout may
                    # fire first; both count as blocked).
                    outcomes.append("blocked")
                except ConflictError:
                    outcomes.append("rejected")
        except BaseException as exc:  # noqa: BLE001 — collected and asserted below
            errors.append(exc)

    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # The explicit outer transaction makes the repository's own scope a
    # nested SAVEPOINT on this connection, so the Profile-root FOR UPDATE
    # stays held for the whole racer probe: the repository function alone
    # opens and closes its own transaction block, which would release the
    # lock before the racer even starts.
    with conn.transaction():
        locked = ownership_repositories.get_profile_for_account_deletion(
            pool, profile_id=owner, lease_seconds=15.0
        )
        assert locked is not None

        thread = threading.Thread(target=racer)
        thread.start()
        start.set()
        thread.join(timeout=60)
        assert not thread.is_alive(), "the admission probe deadlocked on the held lock"

    try:
        assert errors == []
        # The Profile lock held for the whole probe: the admission's
        # account-operational barrier is its FIRST lock acquisition, so the
        # blocked read is the guard's FOR KEY SHARE — before any subject
        # read, Connection lock, or reservation.
        assert outcomes == ["blocked"]
        # No reservation was created past the claim.
        with connect(migrated_database) as verification:
            row = verification.execute(
                "select count(*) from openorc.task_agent_sessions where connection_id = %s",
                (connection_id,),
            ).fetchone()
            assert row is not None and row[0] == 0
    finally:
        _cleanup_account(conn, owner)
