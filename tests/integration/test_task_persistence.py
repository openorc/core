"""Integration-marked persistence tests for the Task aggregate (issue #21).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable Task
invariants directly: one current Task per stable GitHub issue within a
Workspace Repository, archival independent from terminal outcome with
CANCELLED/COMPLETED history distinguishable, fresh Tasks after cancellation
and after a completed issue reopens, exclusive Task-level canonical-branch
ownership, Workspace-scope agreement, and stale ``state_token`` conditional
mutations failing while successful mutations rotate the token. They are
excluded from the ordinary deterministic baseline by the repository pytest
configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration tests/integration/test_task_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and outside
agent responsibility: in the normal Owner local-development flow the target is
the ephemeral non-production Supabase branch that the Owner-only
``devserver.sh`` command creates and later deletes (agents never invoke it).
The session fixture merely resets the ``openorc`` schema and applies the
committed migrations from scratch within the supplied database, and the suite
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.

Canonical branch ownership is proven at the Task aggregate level: the column
lives on ``openorc.tasks`` (no Execution table exists yet, and issue #24 will
keep Executions from ever owning branches), so these tests prove the
Task-level exclusivity and lookup facts directly.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation

from openorc.domain.tasks import TaskDomainError, TaskStatus
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("OPENORC_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("OPENORC_TEST_DATABASE_URL is not configured")
    return url


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Reset the openorc schema and apply all committed migrations from scratch."""
    with connect(database_url) as conn:
        conn.execute("drop schema if exists openorc cascade")
        for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            # Committed migration files are trusted repository content applied
            # wholesale; LiteralString is the driver's injection-safe query
            # contract, satisfied here by repository-controlled file text.
            migration_sql = cast(LiteralString, migration_path.read_text(encoding="utf-8"))
            conn.execute(migration_sql)
        conn.commit()
    return database_url


@pytest.fixture
def conn(migrated_database: str) -> Iterator[Connection[Any]]:
    """One connection per test; each test runs inside one rolled-back transaction."""
    with connect(migrated_database) as connection:
        yield connection
        connection.rollback()


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
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
    conn: Connection[Any], *, github_repository_id: int = 50_000_001
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


def test_two_current_tasks_for_the_same_issue_conflict(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    first = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    assert first.status is TaskStatus.READY_TO_PLAN
    assert first.archived_at is None

    # A second current Task for the same stable issue violates the partial
    # unique index, through the repository and through a direct insert alike.
    # (Repository calls are savepoint-isolated by the pool adapter; the direct
    # statement needs its own savepoint so the failed insert cannot abort the
    # per-test transaction.)
    with pytest.raises(UniqueViolation):
        task_repositories.create_task(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=9001,
            github_issue_number=42,
        )
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.tasks "
            "(workspace_id, repository_id, github_issue_id, github_issue_number, status) "
            "values (%s, %s, %s, %s, 'ready_to_plan')",
            (workspace_id, repository_id, 9001, 42),
        )

    # The stable identity (not the repository-local number) is what conflicts:
    # a different observed number cannot launder a duplicate current Task.
    with pytest.raises(UniqueViolation):
        task_repositories.create_task(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=9001,
            github_issue_number=43,
        )

    # Independent Tasks for different issues coexist.
    second = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9002,
        github_issue_number=43,
    )
    assert second.id != first.id


def test_tasks_for_the_same_external_issue_in_different_workspaces_coexist(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_a = _insert_workspace(conn, profile_id, name="workspace-a")
    workspace_b = _insert_workspace(conn, profile_id, name="workspace-b")
    project_a = _insert_project(conn, workspace_a)
    project_b = _insert_project(conn, workspace_b)
    repo_a = _insert_repository(
        conn, project_id=project_a, workspace_id=workspace_a, github_repository_id=777
    )
    repo_b = _insert_repository(
        conn, project_id=project_b, workspace_id=workspace_b, github_repository_id=777
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    task_a = task_repositories.create_task(
        pool,
        workspace_id=workspace_a,
        repository_id=repo_a,
        github_issue_id=555,
        github_issue_number=7,
    )
    task_b = task_repositories.create_task(
        pool,
        workspace_id=workspace_b,
        repository_id=repo_b,
        github_issue_id=555,
        github_issue_number=7,
    )
    assert task_a.id != task_b.id
    assert task_a.workspace_id != task_b.workspace_id


def test_task_workspace_scope_must_agree_with_the_repository(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # A Task whose direct Workspace scope disagrees with the Repository's
    # Workspace violates the composite foreign key.
    with pytest.raises(ForeignKeyViolation):
        task_repositories.create_task(
            pool,
            workspace_id=uuid.uuid4(),
            repository_id=repository_id,
            github_issue_id=9001,
            github_issue_number=42,
        )
    assert workspace_id is not None


def test_canonical_branch_ownership_is_exclusive_among_current_tasks(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    task_a = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    task_b = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9002,
        github_issue_number=43,
    )

    bound_a = task_repositories.bind_canonical_branch(
        pool,
        task_a.id,
        expected_state_token=task_a.state_token,
        canonical_feature_branch="openorc/task-42/producer",
    )
    assert bound_a is not None
    assert bound_a.canonical_feature_branch == "openorc/task-42/producer"
    assert bound_a.state_token != task_a.state_token

    # Two current Tasks in one Repository cannot own the same branch.
    with pytest.raises(UniqueViolation):
        task_repositories.bind_canonical_branch(
            pool,
            task_b.id,
            expected_state_token=task_b.state_token,
            canonical_feature_branch="openorc/task-42/producer",
        )

    # Different canonical branches coexist, and the lookup resolves the owner.
    task_repositories.bind_canonical_branch(
        pool,
        task_b.id,
        expected_state_token=task_b.state_token,
        canonical_feature_branch="openorc/task-43/producer",
    )
    owner = task_repositories.find_current_task_by_branch(
        pool, repository_id=repository_id, canonical_feature_branch="openorc/task-42/producer"
    )
    assert owner is not None and owner.id == task_a.id


def test_canonical_branch_binding_is_one_time(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    task = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    # The branch is created NULL; it is bound only after verification.
    assert task.canonical_feature_branch is None

    bound = task_repositories.bind_canonical_branch(
        pool,
        task.id,
        expected_state_token=task.state_token,
        canonical_feature_branch="openorc/task-42/producer",
    )
    assert bound is not None
    assert bound.canonical_feature_branch == "openorc/task-42/producer"
    rotated = bound.state_token

    # A current Task cannot rebind (switch) or release (unbind) its canonical
    # branch: the conditional bind applies only while the branch is NULL, so
    # both attempts are rejected no-ops that leave the row untouched.
    assert (
        task_repositories.bind_canonical_branch(
            pool,
            task.id,
            expected_state_token=rotated,
            canonical_feature_branch="openorc/task-42/switched",
        )
        is None
    )
    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.canonical_feature_branch == "openorc/task-42/producer"
    assert unchanged.state_token == rotated

    # Archival is what releases branch ownership for future Tasks.
    archived = task_repositories.archive_task(
        pool,
        task.id,
        expected_state_token=rotated,
        terminal_status=TaskStatus.COMPLETED,
    )
    assert archived is not None

    # A fresh Task for the same (now terminal) issue may bind that branch.
    fresh = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    fresh_bound = task_repositories.bind_canonical_branch(
        pool,
        fresh.id,
        expected_state_token=fresh.state_token,
        canonical_feature_branch="openorc/task-42/producer",
    )
    assert fresh_bound is not None
    assert fresh_bound.canonical_feature_branch == "openorc/task-42/producer"


def test_archived_attempt_releases_the_issue_and_the_branch(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    task_a = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    task_b = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9002,
        github_issue_number=43,
    )
    task_repositories.bind_canonical_branch(
        pool,
        task_a.id,
        expected_state_token=task_a.state_token,
        canonical_feature_branch="openorc/task-42/producer",
    )

    # Binding rotated task_a's token: archival must use the rotated token,
    # never the pre-bind one.
    bound = task_repositories.get_task(pool, task_a.id)
    assert bound is not None
    assert bound.state_token != task_a.state_token

    archived = task_repositories.archive_task(
        pool,
        task_a.id,
        expected_state_token=bound.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert archived is not None
    assert archived.status is TaskStatus.CANCELLED
    assert archived.archived_at is not None

    # A fresh Task for the open issue is allowed after cancellation...
    fresh = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    assert fresh.status is TaskStatus.READY_TO_PLAN
    assert fresh.archived_at is None
    assert fresh.id != task_a.id

    # ...and the released branch can be bound again by another current Task
    # in the Repository.
    rebound = task_repositories.bind_canonical_branch(
        pool,
        task_b.id,
        expected_state_token=task_b.state_token,
        canonical_feature_branch="openorc/task-42/producer",
    )
    assert rebound is not None
    assert rebound.canonical_feature_branch == "openorc/task-42/producer"


def test_cancelled_and_completed_attempts_remain_distinguishable_history(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    cancelled_attempt = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    task_repositories.archive_task(
        pool,
        cancelled_attempt.id,
        expected_state_token=cancelled_attempt.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    completed_attempt = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    task_repositories.archive_task(
        pool,
        completed_attempt.id,
        expected_state_token=completed_attempt.state_token,
        terminal_status=TaskStatus.COMPLETED,
    )
    current = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )

    attempts = task_repositories.list_issue_attempts(
        pool, repository_id=repository_id, github_issue_id=9001
    )
    # All three attempts remain distinct, distinguishable history. The
    # acceptance requirement is the distinct archived/current states per
    # attempt — not that same-transaction inserts form a strict
    # chronological sequence: PostgreSQL now() is transaction-stable, so
    # rows inserted beneath one outer transaction can share a created_at
    # value and the (created_at, id) ordering falls back to arbitrary UUID
    # order. Verify each expected attempt by ID instead of position.
    assert len(attempts) == 3
    attempts_by_id = {attempt.id: attempt for attempt in attempts}
    assert set(attempts_by_id) == {
        cancelled_attempt.id,
        completed_attempt.id,
        current.id,
    }

    cancelled_row = attempts_by_id[cancelled_attempt.id]
    # Both terminal attempts are archived history and remain distinguishable.
    assert cancelled_row.status is TaskStatus.CANCELLED
    assert cancelled_row.archived_at is not None

    completed_row = attempts_by_id[completed_attempt.id]
    assert completed_row.status is TaskStatus.COMPLETED
    assert completed_row.archived_at is not None

    current_row = attempts_by_id[current.id]
    assert current_row.status is TaskStatus.READY_TO_PLAN
    assert current_row.archived_at is None

    resolved = task_repositories.find_current_task_for_issue(
        pool, repository_id=repository_id, github_issue_id=9001
    )
    assert resolved is not None and resolved.id == current.id

    active = task_repositories.list_workspace_tasks(pool, workspace_id=workspace_id)
    history = task_repositories.list_workspace_tasks(
        pool, workspace_id=workspace_id, include_archived=True
    )
    assert {task.id for task in active} == {current.id}
    assert len(history) == 3


def test_fresh_task_after_a_completed_issue_is_reopened(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    completed = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    task_repositories.archive_task(
        pool,
        completed.id,
        expected_state_token=completed.state_token,
        terminal_status=TaskStatus.COMPLETED,
    )

    # GitHub reopen detection is out of scope for #21; what is proven here is
    # that a previously completed issue may gain a fresh current Task.
    reopened = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    assert reopened.id != completed.id
    assert reopened.status is TaskStatus.READY_TO_PLAN
    assert reopened.archived_at is None


def test_stale_state_token_mutations_fail_and_success_rotates_the_token(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    task = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    original_token = task.state_token

    # A stale token never mutates the newer subject.
    assert (
        task_repositories.update_task_status(
            pool,
            task.id,
            expected_state_token=uuid.uuid4(),
            status=TaskStatus.PLANNING,
        )
        is None
    )
    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.status is TaskStatus.READY_TO_PLAN
    assert unchanged.state_token == original_token

    # The matching token mutates the subject and rotates the token atomically.
    updated = task_repositories.update_task_status(
        pool,
        task.id,
        expected_state_token=original_token,
        status=TaskStatus.IMPLEMENTING,
    )
    assert updated is not None
    assert updated.status is TaskStatus.IMPLEMENTING
    rotated_token = updated.state_token
    assert rotated_token != original_token

    # The original token is now stale on every authoritative mutation path.
    assert (
        task_repositories.update_task_status(
            pool,
            task.id,
            expected_state_token=original_token,
            status=TaskStatus.REVIEWING,
        )
        is None
    )
    assert (
        task_repositories.bind_canonical_branch(
            pool,
            task.id,
            expected_state_token=original_token,
            canonical_feature_branch="openorc/task-42/producer",
        )
        is None
    )
    assert (
        task_repositories.archive_task(
            pool,
            task.id,
            expected_state_token=original_token,
            terminal_status=TaskStatus.CANCELLED,
        )
        is None
    )

    # The rotated token carries the authoritative mutations onward.
    bound = task_repositories.bind_canonical_branch(
        pool,
        task.id,
        expected_state_token=rotated_token,
        canonical_feature_branch="openorc/task-42/producer",
    )
    assert bound is not None and bound.canonical_feature_branch is not None
    archived = task_repositories.archive_task(
        pool,
        task.id,
        expected_state_token=bound.state_token,
        terminal_status=TaskStatus.COMPLETED,
    )
    assert archived is not None
    assert archived.archived_at is not None
    # Archived rows are outside every conditional mutation path.
    assert (
        task_repositories.update_task_status(
            pool,
            task.id,
            expected_state_token=archived.state_token,
            status=TaskStatus.PLANNING,
        )
        is None
    )


def test_terminal_transitions_go_exclusively_through_archive_task(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    task = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )

    # update_task_status is nonterminal-only at the API boundary.
    with pytest.raises(TaskDomainError, match="archive_task"):
        task_repositories.update_task_status(
            pool,
            task.id,
            expected_state_token=task.state_token,
            status=TaskStatus.CANCELLED,
        )
    with pytest.raises(TaskDomainError, match="archive_task"):
        task_repositories.update_task_status(
            pool,
            task.id,
            expected_state_token=task.state_token,
            status=TaskStatus.COMPLETED,
        )
    # archive_task requires a terminal status.
    with pytest.raises(TaskDomainError, match="terminal"):
        task_repositories.archive_task(
            pool,
            task.id,
            expected_state_token=task.state_token,
            terminal_status=TaskStatus.PLANNING,
        )

    # The row is untouched: no terminal status leaked through any path.
    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.status is TaskStatus.READY_TO_PLAN
    assert unchanged.archived_at is None
    assert unchanged.state_token == task.state_token


def test_archival_check_constraint_mirrors_the_domain_invariant(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = uuid.uuid4()
    # Direct setup supplies the explicit nonterminal status: the schema
    # intentionally carries no status default, and create_task() supplies
    # READY_TO_PLAN at the write boundary.
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan')",
        (task_id, workspace_id, repository_id, 9001, 42),
    )

    # A terminal status without archival violates the lifecycle CHECK...
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute("update openorc.tasks set status = 'cancelled' where id = %s", (task_id,))
    # ...and archival without a terminal outcome violates it too.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute("update openorc.tasks set archived_at = now() where id = %s", (task_id,))

    # A status outside the settled vocabulary is rejected outright.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.tasks set status = 'awaiting_review' where id = %s",
            (task_id,),
        )

    # Every expected violation was savepoint-isolated, so the transaction is
    # still healthy and the untouched row remains readable.
    row = conn.execute(
        "select status, archived_at from openorc.tasks where id = %s", (task_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "ready_to_plan"
    assert row[1] is None


def test_task_round_trip_and_instants_are_utc_normalized(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    created = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    fetched = task_repositories.get_task(pool, created.id)
    assert fetched == created
    for instant in (created.created_at, created.updated_at):
        assert isinstance(instant, datetime)
        assert instant.tzinfo is not None
        assert instant.utcoffset() == timedelta(0)
