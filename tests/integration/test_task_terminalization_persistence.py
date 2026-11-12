"""Integration-marked persistence tests for the terminalization commitment (issue #317).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
exact-Task terminalization seams directly: the commitment/evidence CHECK
coherence, the set-once conditional commit with token rotation, the one-shot
cleanup send claim (exactly one send admitted under concurrency), the settle
exactly once, the finalizer's atomic archival with the ENDED session
transition and Connection-capacity release, rollback-together under a forced
session-end failure, and concurrent finalizations archiving exactly once.
They are excluded from the ordinary deterministic baseline by the repository
pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_terminalization_persistence.py

The suite consumes the database it is given and never provisions one; the
session fixture resets the ``openorc`` schema and applies the committed
migrations from scratch within the supplied database, and the suite skips
cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import CheckViolation

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import TaskStatus, TaskTerminalizationOrigin
from openorc.persistence import sessions as session_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services.task_terminal_finalization import (
    TerminalFinalizationKind,
    finalize_task_terminalization,
)

pytestmark = pytest.mark.integration


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


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _ownership_chain(conn: Connection[Any]) -> tuple[uuid.UUID, uuid.UUID]:
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
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            61_000_001,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return workspace_id, repository_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, 100, 'waiting_for_owner', %s)",
        (task_id, workspace_id, repository_id, 610_001, "a" * 64),
    )
    return task_id


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

    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, session_capacity) "
        "values (%s, %s, 'cline', 'primary hub', %s)",
        (connection_id, workspace_id, session_capacity),
    )
    return connection_id


def _admit_sessions(
    pool: DatabasePool,
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    connection_id: uuid.UUID,
) -> None:
    producer = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    assert producer.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    session_repositories.initialize_task_agent_session(
        pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        external_session_id="producer-external-session",
        effective_config_snapshot={
            "adapter": "cline",
            "configuration": {"repository_base": "/srv/runtime/repositories"},
        },
    )
    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.REVIEWER,
        connection_id=connection_id,
    )


def _commit_terminalization(
    pool: DatabasePool, *, task_id: uuid.UUID, token: uuid.UUID
) -> uuid.UUID:
    committed = task_repositories.commit_task_terminalization(
        pool,
        task_id,
        expected_state_token=token,
        terminal_outcome=TaskStatus.CANCELLED,
        origin=TaskTerminalizationOrigin.OWNER_CANCELLED,
    )
    assert committed is not None
    return committed.state_token


def _task_facts(conn: Connection[Any], task_id: uuid.UUID) -> tuple[Any, ...]:
    row = conn.execute(
        "select status, archived_at, terminalization_outcome, terminalization_origin, "
        "terminal_cause, state_token from openorc.tasks where id = %s",
        (task_id,),
    ).fetchone()
    assert row is not None
    return row


def _binding_facts(conn: Connection[Any], task_id: uuid.UUID, role: str) -> tuple[str, Any] | None:
    row = conn.execute(
        "select lifecycle_status, ended_at from openorc.task_agent_sessions "
        "where task_id = %s and role = %s",
        (task_id, role),
    ).fetchone()
    return None if row is None else (row[0], row[1])


def _terminal_event_count(conn: Connection[Any], task_id: uuid.UUID) -> int:
    row = conn.execute(
        "select count(*) from openorc.workflow_events "
        "where task_id = %s and event_type = 'task_cancelled'",
        (task_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])

    assert row is not None
    return int(row[0])


# --- the commitment CHECK coherence -------------------------------------------------


@pytest.mark.parametrize(
    "assignments",
    [
        pytest.param(
            "terminalization_outcome = 'cancelled'",
            id="outcome-without-origin",
        ),
        pytest.param(
            "terminalization_outcome = 'cancelled', terminalization_origin = 'owner_cancelled'",
            id="commitment-without-timestamp",
        ),
        pytest.param(
            "terminalization_outcome = 'queued', terminalization_origin = 'owner_cancelled', "
            "terminalization_established_at = now()",
            id="nonterminal-outcome",
        ),
        pytest.param(
            "terminalization_outcome = 'cancelled', terminalization_origin = 'invented', "
            "terminalization_established_at = now()",
            id="invalid-origin",
        ),
        pytest.param(
            "cleanup_send_settled_at = now()",
            id="settled-without-claim",
        ),
        pytest.param(
            "cleanup_send_claimed_at = now()",
            id="claim-without-commitment",
        ),
    ],
)
def test_the_commitment_check_constraints_enforce_the_coherence_invariants(
    conn: Connection[Any],
    assignments: str,
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    # The assignments come from this module's own parametrize table; the cast
    # is the driver's injection-safe contract for composed statement text.
    statement = cast(LiteralString, f"update openorc.tasks set {assignments} where id = %s")
    # Context exit order is load-bearing: conn.transaction() is the INNERMOST
    # context, so the CheckViolation reaches psycopg's transaction exit FIRST,
    # which rolls back to the savepoint and re-raises for pytest.raises to
    # catch — keeping the per-test transaction usable afterwards.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(statement, (task_id,))
    # Nothing partial persisted: the row keeps its pre-commitment shape.
    facts = _task_facts(conn, task_id)
    assert facts[0] == "waiting_for_owner"
    assert facts[2] is None and facts[3] is None


# --- the set-once commit / claim / settle conditional writes -------------------------


def test_commit_claim_and_settle_are_set_once_and_conditionally(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    token = task_repositories.get_task(pool, task_id)
    assert token is not None
    original_token = token.state_token

    committed_token = _commit_terminalization(pool, task_id=task_id, token=original_token)
    facts = _task_facts(conn, task_id)
    assert facts[0] == "waiting_for_owner"  # the status is untouched pre-archival
    assert facts[1] is None
    assert facts[2] == "cancelled"
    assert facts[3] == "owner_cancelled"
    assert facts[5] == committed_token and facts[5] != original_token

    # A replayed commitment with the stale token is a rejected no-op.
    assert (
        task_repositories.commit_task_terminalization(
            pool,
            task_id,
            expected_state_token=original_token,
            terminal_outcome=TaskStatus.CANCELLED,
            origin=TaskTerminalizationOrigin.OWNER_CANCELLED,
        )
        is None
    )

    # The one-shot claim: exactly the NULL -> claimed transition, once.
    first_claim = task_repositories.claim_task_terminal_cleanup_send(pool, task_id)
    assert first_claim is not None
    assert task_repositories.claim_task_terminal_cleanup_send(pool, task_id) is None

    # The settle applies exactly once from the claim.
    settled = task_repositories.settle_task_terminal_cleanup_send(pool, task_id)
    assert settled is not None and settled.cleanup_send_settled_at is not None
    assert task_repositories.settle_task_terminal_cleanup_send(pool, task_id) is None

    # An archived Task admits neither a commitment nor a claim.
    archived = task_repositories.archive_task(
        pool,
        task_id,
        expected_state_token=committed_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert archived is not None
    assert task_repositories.claim_task_terminal_cleanup_send(pool, task_id) is None
    assert (
        task_repositories.commit_task_terminalization(
            pool,
            task_id,
            expected_state_token=archived.state_token,
            terminal_outcome=TaskStatus.CANCELLED,
            origin=TaskTerminalizationOrigin.OWNER_CANCELLED,
        )
        is None
    )


# --- the finalizer's atomic archival, session end, and capacity release -------------


def test_the_finalizer_archives_atomically_with_session_end_and_releases_capacity(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    _admit_sessions(pool, workspace_id=workspace_id, task_id=task_id, connection_id=connection_id)
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    _commit_terminalization(pool, task_id=task_id, token=task.state_token)

    outcome = finalize_task_terminalization(pool, None, workspace_id=workspace_id, task_id=task_id)

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    facts = _task_facts(conn, task_id)
    # The archived attempt carries the committed outcome; an Owner-attributed
    # cancellation stamps no terminal cause.
    assert facts[0] == "cancelled"
    assert facts[1] is not None
    assert facts[2] == "cancelled"
    assert facts[4] is None
    # Both active bindings ended exactly once; LOST/ENDED history untouched
    # semantics covered by the session suite.
    assert _binding_facts(conn, task_id, "producer") is not None
    producer_facts = cast(tuple[str, Any], _binding_facts(conn, task_id, "producer"))
    reviewer_facts = cast(tuple[str, Any], _binding_facts(conn, task_id, "reviewer"))
    assert producer_facts[0] == "ended" and producer_facts[1] is not None
    assert reviewer_facts[0] == "ended" and reviewer_facts[1] is not None
    # The focused capacity fact: the archived Task no longer occupies
    # Connection capacity.
    occupancy = session_repositories.count_active_task_agent_sessions_by_connection(
        pool, connection_ids=[connection_id]
    )
    assert occupancy == {}
    assert _terminal_event_count(conn, task_id) == 1


def test_the_trusted_closed_unmerged_origin_archives_with_the_terminal_cause(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    committed = task_repositories.commit_task_terminalization(
        pool,
        task_id,
        expected_state_token=task.state_token,
        terminal_outcome=TaskStatus.CANCELLED,
        origin=TaskTerminalizationOrigin.GITHUB_CLOSED_UNMERGED,
    )
    assert committed is not None

    outcome = finalize_task_terminalization(pool, None, workspace_id=workspace_id, task_id=task_id)

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    facts = _task_facts(conn, task_id)
    assert facts[4] == "pr_closed_unmerged"


def test_a_forced_session_end_failure_rolls_the_whole_finalization_back(
    conn: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    _admit_sessions(pool, workspace_id=workspace_id, task_id=task_id, connection_id=connection_id)
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    committed_token = _commit_terminalization(pool, task_id=task_id, token=task.state_token)

    def _forced_end(_pool: Any, *, task_id: Any, role: Any) -> Any:
        raise RuntimeError("forced end failure")

    monkeypatch.setattr(session_repositories, "mark_task_agent_session_ended", _forced_end)

    with pytest.raises(RuntimeError, match="forced end failure"):
        finalize_task_terminalization(pool, None, workspace_id=workspace_id, task_id=task_id)

    # The forced session-end failure rolls the archival, the terminal audit,
    # and the session finalization back together: the Task stays committed
    # and unarchived, both bindings stay active, and no terminal event
    # exists. A later observed delivery converges idempotently.
    facts = _task_facts(conn, task_id)
    assert facts[0] == "waiting_for_owner" and facts[1] is None
    assert facts[2] == "cancelled" and facts[5] == committed_token
    producer = _binding_facts(conn, task_id, "producer")
    reviewer = _binding_facts(conn, task_id, "reviewer")
    assert producer is not None and producer[0] in ("connecting", "ready")
    assert reviewer is not None and reviewer[0] in ("connecting", "ready")
    assert _terminal_event_count(conn, task_id) == 0

    assert _terminal_event_count(conn, task_id) == 0


def _racer_threads(target: Any, count: int = 2) -> tuple[list[Any], list[BaseException]]:
    """Run ``count`` racer threads; return (results, errors) in join order."""
    results: list[Any] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def runner() -> None:
        try:
            outcome = target()
            with lock:
                results.append(outcome)
        except BaseException as error:  # noqa: BLE001 - collected and asserted below
            with lock:
                errors.append(error)

    threads = [threading.Thread(target=runner) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive(), "the racer thread deadlocked"
    return results, errors


def test_concurrent_cleanup_send_claims_admit_exactly_one_send(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    token_row = conn.execute(
        "select state_token from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert token_row is not None
    conn.commit()  # publish the subject for the racer connections
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    _commit_terminalization(pool, task_id=task_id, token=token_row[0])

    def racer() -> Any:
        with connect(migrated_database, prepare_threshold=None) as racer_conn:
            claim = task_repositories.claim_task_terminal_cleanup_send(
                cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                task_id,
            )
            racer_conn.commit()
            return claim

    claims, errors = _racer_threads(racer)

    assert errors == []
    # Exactly one racer observes the NULL -> claimed CAS transition; the
    # concurrent duplicate send is structurally impossible.
    assert len(claims) == 2
    assert sum(1 for claim in claims if claim is not None) == 1


def test_concurrent_finalizations_archive_exactly_once(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    # Both active bindings exist before the racers run, so the concurrent
    # finalizations race the full archival + session-end + capacity release.
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    _admit_sessions(pool, workspace_id=workspace_id, task_id=task_id, connection_id=connection_id)
    token_row = conn.execute(
        "select state_token from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert token_row is not None
    conn.commit()  # publish the subject for the racer connections
    _commit_terminalization(pool, task_id=task_id, token=token_row[0])

    def racer() -> Any:
        with connect(migrated_database, prepare_threshold=None) as racer_conn:
            outcome = finalize_task_terminalization(
                cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                None,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            racer_conn.commit()
            return outcome.kind.value

    outcomes, errors = _racer_threads(racer)

    assert errors == []
    # Both bindings end; the conditional archival write admits exactly one
    # committed terminal outcome and exactly one terminal event.
    assert outcomes.count("archived") == 1
    assert outcomes.count("already_archived") == 1
    assert _terminal_event_count(conn, task_id) == 1
    facts = _task_facts(conn, task_id)
    assert facts[0] == "cancelled" and facts[1] is not None
    producer = _binding_facts(conn, task_id, "producer")
    reviewer = _binding_facts(conn, task_id, "reviewer")
    assert producer is not None and producer[0] == "ended"
    assert reviewer is not None and reviewer[0] == "ended"
    occupancy = session_repositories.count_active_task_agent_sessions_by_connection(
        pool, connection_ids=[connection_id]
    )
    assert occupancy == {}
