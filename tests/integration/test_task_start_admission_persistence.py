"""Integration-marked persistence tests for Task start FIFO admission (issue #76).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
Task-start invariants directly against a real Postgres: the DB-clock
queue-entry timestamp and its iff-QUEUED CHECK, the ``(queued_at, id)`` FIFO
order, the Owner start's atomic claim (both role reservations + planning +
one planning_started event), the bounded sweep's FIFO advancement, no
overtaking of a feasible older waiter, no oversubscription, duplicate-free
concurrent sweeps, the durable-projection eligibility guard, the structural
configuration outcome, replay idempotence, and the capacity-release
serialization boundary (a lifecycle release blocked by the arbitration
lock). A scripted fake stands in only for the GitHub adapter seam; all
durable state is real. They are excluded from the ordinary deterministic
baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_start_admission_persistence.py

The suite consumes the database it is given and never provisions one (see
tests/integration/conftest.py and tests/README.md): the Owner-only
``devserver.sh --testdb`` flow creates and later deletes the ephemeral
branch; agents never invoke it. The suite skips cleanly when
``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import CheckViolation, LockNotAvailable, QueryCanceled

from openorc.adapters.github import (
    GitHubIssueObservation,
    GitHubRelatedIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.github_issues import github_issue_requirements_fingerprint
from openorc.domain.tasks import TaskStatus
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services.session_admission import AdmissionStatus, admit_task_agent_sessions
from openorc.services.session_runtime import end_task_agent_session
from openorc.services.task_start import (
    QueuedCandidateDisposition,
    TaskAlreadyPlanningError,
    TaskCapacityConfigurationError,
    TaskStartQueueReason,
    TaskStartStatus,
    reevaluate_queued_tasks,
    start_task_for_owner,
)

pytestmark = pytest.mark.integration

_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Steps")
_OBSERVED = datetime(2026, 10, 18, 12, 0, 0, tzinfo=UTC)
_GITHUB_INSTALLATION_ID = 50123456
_GITHUB_REPOSITORY_ID = 987654321


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

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


class _ScriptedGitHub:
    """Minimal canned adapter for the start path's fresh B3/B4 revalidation."""

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        return GitHubRepositoryObservation(
            github_repository_id=github_repository_id,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        return GitHubIssueObservation(
            github_issue_id=issue_number * 1000,
            issue_number=issue_number,
            title="Found a bug",
            body="Steps",
            state="open",
            provider_updated_at=_OBSERVED,
            is_pull_request=False,
        )

    def get_issue_blocked_by(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        return []

    def get_issue_parent(self, **_kwargs: Any) -> Any:
        raise AssertionError("the start path performs no hierarchy sync")

    def get_issue_sub_issues(self, **_kwargs: Any) -> Any:
        raise AssertionError("the start path performs no hierarchy sync")

    def resolve_related_issue_endpoints(
        self,
        *,
        github_installation_id: int,
        local_github_repository_id: int,
        fresh_owner_login: str,
        fresh_repository_name: str,
        related: list[GitHubRelatedIssueObservation],
    ) -> list[Any]:
        from openorc.adapters.github import GitHubRelatedIssueEndpoint

        local_address = (
            f"https://api.github.com/repos/{fresh_owner_login}/{fresh_repository_name}"
        ).lower()
        return [
            GitHubRelatedIssueEndpoint(
                github_repository_id=local_github_repository_id
                if observation.repository_url.lower() == local_address
                else 555,
                github_issue_id=observation.github_issue_id,
            )
            for observation in related
        ]

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the service must not compose raw #58 validation operations")

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not compose user-write validation")

    def get_repository_branch(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe branches")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe pull requests")

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not create pull requests")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project combined status")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not request merges")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")


@pytest.fixture(autouse=True)
def _pooler_neutral_connection(conn: Connection[Any]) -> None:
    """Keep this module's connections on the pooler-neutral prepare policy.

    The production DatabasePool pins ``prepare_threshold=None`` (the
    persistence pooler-neutral rule): a transaction-mode pooler reuses server
    sessions across client connections, so client-side auto-prepare collides
    on server-side prepared statement names (DuplicatePreparedStatement).
    """
    conn.prepare_threshold = None


def _apply_probe_timeouts(conn: Connection[Any]) -> None:
    conn.execute("set local lock_timeout = '500ms'")
    conn.execute("set local statement_timeout = '2s'")


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _ownership_chain(conn: Connection[Any]) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """Profile -> Workspace -> Project -> Repository + installation route."""
    profile_id = _insert_profile(conn)
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, profile_id, "workspace"),
    )
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    repository_id = uuid.uuid4()
    installation_record_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch, github_installation_id) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            _GITHUB_REPOSITORY_ID,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            installation_record_id,
        ),
    )
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, account_login, "
        "account_type) values (%s, %s, %s, %s, %s, %s)",
        (
            installation_record_id,
            workspace_id,
            _GITHUB_INSTALLATION_ID,
            501,
            "octocat",
            "Organization",
        ),
    )
    return profile_id, workspace_id, repository_id


def _insert_issue_projection(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int,
    issue_number: int,
    state: str = "open",
    fingerprint: str = _FINGERPRINT,
) -> None:
    conn.execute(
        "insert into openorc.github_issues "
        "(workspace_id, repository_id, github_issue_id, issue_number, title, body, "
        "state, requirements_fingerprint) values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            workspace_id,
            repository_id,
            github_issue_id,
            issue_number,
            "Found a bug",
            "Steps",
            state,
            fingerprint,
        ),
    )


def _insert_task(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int = 100000,
    github_issue_number: int = 100,
    status: str = "ready_to_plan",
    fingerprint: str = _FINGERPRINT,
) -> tuple[uuid.UUID, uuid.UUID]:
    task_id = uuid.uuid4()
    state_token = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "state_token, source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            task_id,
            workspace_id,
            repository_id,
            github_issue_id,
            github_issue_number,
            status,
            state_token,
            fingerprint,
        ),
    )
    return task_id, state_token


def _insert_connection(
    conn: Connection[Any], *, workspace_id: uuid.UUID, session_capacity: int = 2
) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, session_capacity) "
        "values (%s, %s, 'cline', 'primary hub', %s)",
        (connection_id, workspace_id, session_capacity),
    )
    return connection_id


def _set_role_bindings(
    conn: Connection[Any], *, workspace_id: uuid.UUID, connection_id: uuid.UUID
) -> None:
    for role in (WorkflowRole.PRODUCER, WorkflowRole.REVIEWER):
        conn.execute(
            "insert into openorc.workflow_role_bindings "
            "(workspace_id, role, connection_id) values (%s, %s, %s) "
            "on conflict (workspace_id, role) do update set connection_id = excluded.connection_id",
            (workspace_id, role.value, connection_id),
        )


def _queue_task(conn: Connection[Any], task_id: uuid.UUID, state_token: uuid.UUID) -> Any:
    row = conn.execute(
        "update openorc.tasks set status = 'queued', queued_at = now(), "
        "state_token = gen_random_uuid() where id = %s and state_token = %s "
        "returning queued_at, state_token",
        (task_id, state_token),
    ).fetchone()
    assert row is not None
    return row


def _active_session_count(conn: Connection[Any], connection_id: uuid.UUID) -> int:
    row = conn.execute(
        "select count(*) from openorc.task_agent_sessions "
        "where connection_id = %s and lifecycle_status in ('connecting', 'ready')",
        (connection_id,),
    ).fetchone()
    assert row is not None
    return row[0]


def _planning_event_count(conn: Connection[Any], task_id: uuid.UUID) -> int:
    row = conn.execute(
        "select count(*) from openorc.workflow_events "
        "where task_id = %s and event_type = 'planning_started'",
        (task_id,),
    ).fetchone()
    assert row is not None
    return row[0]


def _cleanup_account(conn: Connection[Any], profile_id: uuid.UUID) -> None:
    """Remove committed seed rows through the sanctioned Auth-root cascade."""
    conn.execute("delete from auth.users where id = %s", (profile_id,))
    conn.commit()


def test_queue_entry_timestamp_is_stable_iff_queued_and_fifo_ordered(
    conn: Connection[Any],
) -> None:
    _, workspace_id, repository_id = _ownership_chain(conn)
    first_id, first_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=200000
    )
    second_id, second_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=300000
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    first_queued = task_records.update_task_status(
        pool, first_id, expected_state_token=first_token, status=TaskStatus.QUEUED
    )
    assert first_queued is not None
    assert first_queued.status is TaskStatus.QUEUED
    assert first_queued.queued_at is not None

    # A repeated capacity check re-enters queued without rewriting the entry
    # timestamp: the durable key is stable across checks.
    replayed = task_records.update_task_status(
        pool, first_id, expected_state_token=first_queued.state_token, status=TaskStatus.QUEUED
    )
    assert replayed is not None
    assert replayed.queued_at == first_queued.queued_at

    # Leave the queue by an authorized path and the timestamp clears: the
    # iff-QUEUED invariant holds in both directions.
    claimed = task_records.update_task_status(
        pool, first_id, expected_state_token=replayed.state_token, status=TaskStatus.PLANNING
    )
    assert claimed is not None
    assert claimed.status is TaskStatus.PLANNING
    assert claimed.queued_at is None

    # The canonical queue read orders by the durable (queued_at, id) key: the
    # second Task queues first, then the first Task re-enters the queue by an
    # authorized path (fresh DB-clock entry) and sorts behind it.
    # Same-transaction stamps are equal by design (now() is transaction start
    # time), so the second Task's key is forced to a deterministically earlier
    # instant before the first Task re-enters the queue.
    _queue_task(conn, second_id, second_token)
    conn.execute(
        "update openorc.tasks set queued_at = now() - interval '1 hour' where id = %s",
        (second_id,),
    )
    second_key = conn.execute(
        "select queued_at from openorc.tasks where id = %s", (second_id,)
    ).fetchone()
    assert second_key is not None and second_key[0] is not None
    requeued = task_records.update_task_status(
        pool, first_id, expected_state_token=claimed.state_token, status=TaskStatus.QUEUED
    )
    assert requeued is not None
    assert requeued.status is TaskStatus.QUEUED
    assert requeued.queued_at is not None
    assert requeued.queued_at > second_key[0]
    listed = task_records.list_queued_tasks(pool, workspace_id=workspace_id, limit=10)
    assert [task.id for task in listed] == [second_id, first_id]

    # The database CHECK is the durable backstop: a non-queued row can never
    # carry a queue-entry timestamp (the iff invariant, enforced in SQL).
    with pytest.raises(CheckViolation):
        conn.execute(
            "update openorc.tasks set status = 'planning', queued_at = now() where id = %s",
            (second_id,),
        )


def test_owner_start_claims_planning_atomically_against_real_rows(
    conn: Connection[Any],
) -> None:
    owner, workspace_id, repository_id = _ownership_chain(conn)
    task_id, state_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=2)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=connection_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    outcome = start_task_for_owner(
        pool,
        _ScriptedGitHub(),
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=state_token,
    )

    assert outcome.status is TaskStartStatus.PLANNING
    assert outcome.queue_reason is None
    assert set(outcome.sessions) == {WorkflowRole.PRODUCER, WorkflowRole.REVIEWER}
    assert _active_session_count(conn, connection_id) == 2
    assert _planning_event_count(conn, task_id) == 1
    row = conn.execute(
        "select status, queued_at, state_token from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "planning"
    assert row[1] is None
    assert row[2] != state_token

    # Replaying the start for the claimed Task never issues another
    # admission, event, or session.
    with pytest.raises(TaskAlreadyPlanningError):
        start_task_for_owner(
            pool,
            _ScriptedGitHub(),
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=row[2],
        )
    assert _active_session_count(conn, connection_id) == 2
    assert _planning_event_count(conn, task_id) == 1


def test_structurally_infeasible_start_is_an_error_without_a_queue_write(
    conn: Connection[Any],
) -> None:
    owner, workspace_id, repository_id = _ownership_chain(conn)
    task_id, state_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id
    )
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=1)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=connection_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    with pytest.raises(TaskCapacityConfigurationError):
        start_task_for_owner(
            pool,
            _ScriptedGitHub(),
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=state_token,
        )

    row = conn.execute(
        "select status, queued_at from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "ready_to_plan"
    assert row[1] is None
    assert _active_session_count(conn, connection_id) == 0


def test_sweep_never_overtakes_the_older_waiter_and_holds_the_release_boundary(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    owner, workspace_id, repository_id = _ownership_chain(conn)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=2)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=connection_id)
    occupier_id, _occupier_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=100000
    )
    older_id, older_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=200000
    )
    newer_id, newer_token = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=300000,
        github_issue_number=300,
    )
    for issue_id, number in ((200000, 200), (300000, 300)):
        _insert_issue_projection(
            conn,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=issue_id,
            issue_number=number,
        )
    # Cross-connection racers need committed, visible seed rows; the owned
    # graph is removed again through the Auth-root cascade below.
    conn.commit()
    try:
        pool = cast(DatabasePool, _SingleConnectionPool(conn))

        # One active slot occupied by the occupier's Producer reservation.
        occupant = admit_task_agent_sessions(
            pool,
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=occupier_id,
            roles=[WorkflowRole.PRODUCER],
        )
        assert occupant.status is AdmissionStatus.ADMITTED
        assert _active_session_count(conn, connection_id) == 1

        # The older Task durably queues first; the explicit commit closes the
        # implicit fixture transaction so the newer start's entry stamp comes
        # from a strictly later transaction and the FIFO keys compare.
        older_queued = _queue_task(conn, older_id, older_token)
        conn.commit()

        # The newer start cannot overtake the older waiter: one free slot, both
        # Tasks need two, so the newer start queues behind with a later key.
        outcome = start_task_for_owner(
            pool,
            _ScriptedGitHub(),
            profile_id=owner,
            workspace_id=workspace_id,
            task_id=newer_id,
            expected_state_token=newer_token,
        )
        assert outcome.status is TaskStartStatus.QUEUED
        assert outcome.queue_reason is TaskStartQueueReason.AWAITING_CAPACITY
        newer_row = conn.execute(
            "select queued_at from openorc.tasks where id = %s", (newer_id,)
        ).fetchone()
        assert newer_row is not None and newer_row[0] is not None
        assert newer_row[0] > older_queued[0]

        # The capacity-releasing lifecycle transition composes the same
        # per-Workspace arbitration lock: an end attempted while an arbitration
        # window holds the lock is BLOCKED, never interleaved into a decision.
        advisory_key = workspace_id.int & ((1 << 63) - 1)
        release_outcomes: list[str] = []
        release_errors: list[BaseException] = []

        def release_attempt() -> None:
            try:
                with connect(migrated_database, prepare_threshold=None) as racer_conn:
                    racer_conn.execute("set lock_timeout = '500ms'")
                    racer_conn.execute("set statement_timeout = '2s'")
                    end_task_agent_session(
                        cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                        workspace_id=workspace_id,
                        task_id=occupier_id,
                        role=WorkflowRole.PRODUCER,
                    )
                release_outcomes.append("ended")
            except (LockNotAvailable, QueryCanceled):
                release_outcomes.append("blocked")
            except BaseException as error:  # noqa: BLE001 - collected and asserted below
                release_errors.append(error)

        with conn.transaction():
            conn.execute("select pg_advisory_xact_lock(%s)", (advisory_key,))
            thread = threading.Thread(target=release_attempt)
            thread.start()
            thread.join(timeout=30)
            assert not thread.is_alive(), "the release probe deadlocked on the held lock"
        assert release_errors == []
        assert release_outcomes == ["blocked"]
        assert _active_session_count(conn, connection_id) == 1

        # Now the release commits outside any arbitration window, freeing a slot.
        end_task_agent_session(
            pool, workspace_id=workspace_id, task_id=occupier_id, role=WorkflowRole.PRODUCER
        )
        assert _active_session_count(conn, connection_id) == 0

        # The wakeup sweep advances the OLDER waiter first; the newer Task stays
        # queued with its original key and no capacity is oversubscribed.
        sweep = reevaluate_queued_tasks(pool, workspace_id=workspace_id)
        # The older waiter advances; the newer Task stays queued behind it —
        # the sweep's own no-overtaking outcome.
        assert [entry.disposition for entry in sweep.outcomes] == [
            QueuedCandidateDisposition.ADMITTED,
            QueuedCandidateDisposition.STILL_QUEUED,
        ]
        assert sweep.exhausted is True
        older_status = conn.execute(
            "select status from openorc.tasks where id = %s", (older_id,)
        ).fetchone()
        assert older_status is not None and older_status[0] == "planning"
        assert _active_session_count(conn, connection_id) == 2
        assert _planning_event_count(conn, older_id) == 1
        newer_after = conn.execute(
            "select status, queued_at from openorc.tasks where id = %s", (newer_id,)
        ).fetchone()
        assert newer_after is not None
        assert newer_after[0] == "queued"
        assert newer_after[1] == newer_row[0]
    finally:
        _cleanup_account(conn, owner)


def test_concurrent_sweeps_never_duplicate_reservations_or_events(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    owner, workspace_id, repository_id = _ownership_chain(conn)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=2)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=connection_id)
    task_id, state_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id
    )
    _insert_issue_projection(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=100000,
        issue_number=100,
    )
    _queue_task(conn, task_id, state_token)
    # Cross-connection racers need committed, visible seed rows; the owned
    # graph is removed again through the Auth-root cascade below.
    conn.commit()
    try:
        outcomes: list[Any] = []
        errors: list[BaseException] = []

        def sweep() -> None:
            try:
                with connect(migrated_database, prepare_threshold=None) as racer_conn:
                    outcomes.append(
                        reevaluate_queued_tasks(
                            cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                            workspace_id=workspace_id,
                        )
                    )
            except BaseException as error:  # noqa: BLE001 - collected and asserted below
                errors.append(error)

        threads = [threading.Thread(target=sweep) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            # Serialized by the advisory lock; remote-branch statement latency
            # makes each sweep slow, so the join is generous.
            thread.join(timeout=300)
            assert not thread.is_alive()
        assert errors == []
        # Exactly one claim won the advisory-serialized arbitration: the other
        # sweep observed an empty queue and claimed nothing twice.
        admitted = sum(
            1
            for outcome in outcomes
            for entry in outcome.outcomes
            if entry.disposition is QueuedCandidateDisposition.ADMITTED
        )
        assert admitted == 1
        assert _active_session_count(conn, connection_id) == 2
        assert _planning_event_count(conn, task_id) == 1
        row = conn.execute("select status from openorc.tasks where id = %s", (task_id,)).fetchone()
        assert row is not None and row[0] == "planning"
    finally:
        _cleanup_account(conn, owner)


def test_eligibility_contradicted_candidate_is_never_advanced(conn: Connection[Any]) -> None:
    _, workspace_id, repository_id = _ownership_chain(conn)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=2)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=connection_id)
    task_id, state_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id
    )
    _insert_issue_projection(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=100000,
        issue_number=100,
        state="closed",
    )
    queued = _queue_task(conn, task_id, state_token)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    sweep = reevaluate_queued_tasks(pool, workspace_id=workspace_id)

    assert [entry.disposition for entry in sweep.outcomes] == [
        QueuedCandidateDisposition.INELIGIBLE
    ]
    assert sweep.outcomes[0].eligibility_reason is not None
    # The candidate stayed queued with its original durable key; no runtime
    # capacity was ever reserved for a known-changed subject.
    row = conn.execute(
        "select status, queued_at from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "queued"
    assert row[1] == queued[0]
    assert _active_session_count(conn, connection_id) == 0
    assert _planning_event_count(conn, task_id) == 0


def test_structurally_infeasible_candidate_surfaces_the_configuration_condition(
    conn: Connection[Any],
) -> None:
    _, workspace_id, repository_id = _ownership_chain(conn)
    shared_capacity_one = _insert_connection(conn, workspace_id=workspace_id, session_capacity=1)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=shared_capacity_one)
    task_id, state_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id
    )
    _insert_issue_projection(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=100000,
        issue_number=100,
    )
    queued = _queue_task(conn, task_id, state_token)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    sweep = reevaluate_queued_tasks(pool, workspace_id=workspace_id)

    assert [entry.disposition for entry in sweep.outcomes] == [
        QueuedCandidateDisposition.CONFIGURATION_ERROR
    ]
    # The candidate is not queued forever on an impossible configuration: the
    # actionable condition is surfaced and the original key is untouched.
    row = conn.execute(
        "select status, queued_at from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "queued"
    assert row[1] == queued[0]
    assert _active_session_count(conn, shared_capacity_one) == 0


def test_no_capacity_oversubscription_when_the_target_races_the_queue(
    conn: Connection[Any],
) -> None:
    owner, workspace_id, repository_id = _ownership_chain(conn)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=2)
    _set_role_bindings(conn, workspace_id=workspace_id, connection_id=connection_id)
    first_id, first_token = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id
    )
    second_id, second_token = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=300000,
        github_issue_number=300,
    )
    for issue_id, number in ((100000, 100), (300000, 300)):
        _insert_issue_projection(
            conn,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=issue_id,
            issue_number=number,
        )
    first_queued = _queue_task(conn, first_id, first_token)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # The sweep claims the older Task's two slots; the newer start queues
    # behind instead of oversubscribing the Connection.
    sweep = reevaluate_queued_tasks(pool, workspace_id=workspace_id)
    assert [entry.disposition for entry in sweep.outcomes] == [QueuedCandidateDisposition.ADMITTED]
    outcome = start_task_for_owner(
        pool,
        _ScriptedGitHub(),
        profile_id=owner,
        workspace_id=workspace_id,
        task_id=second_id,
        expected_state_token=second_token,
    )
    assert outcome.status is TaskStartStatus.QUEUED
    assert _active_session_count(conn, connection_id) == 2
    second_row = conn.execute(
        "select status, queued_at from openorc.tasks where id = %s", (second_id,)
    ).fetchone()
    assert second_row is not None
    assert second_row[0] == "queued"
    assert second_row[1] is not None
    assert second_row[1] > first_queued[0]
