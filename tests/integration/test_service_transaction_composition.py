"""Integration tests for the service transaction-composition primitive (issue #51).

These tests run against the explicitly supplied non-production Supabase
branch database (the existing conftest path) and prove the atomic-composition
behavior of ``openorc.services.transaction_composition.composed_transaction``
on real Postgres. They are excluded from the ordinary deterministic baseline
by the repository pytest configuration.

The module deliberately borrows the fixture connection through a passthrough
pool adapter that adds NO transaction or lifetime behavior of its own. That
is what makes the assertions below a regression tripwire for the primitive's
explicit outer transaction block:

- The adapter yields the fixture-owned connection unchanged, so the ONLY
  composition-wide transaction scope in this module is the one
  ``composed_transaction`` itself establishes. Over the fixture's already
  implicit transaction that explicit block is a psycopg SAVEPOINT (expected:
  psycopg keys block nesting off open-transaction state); the
  idle-pooled-connection outer ``BEGIN`` path is proven separately by the
  deterministic fake tests.
- The fixture's implicit transaction is block-free, so if the explicit outer
  block were ever removed, each repository operation would become an
  independent savepoint scope under the fixture transaction: the first
  write's scope would release and its rows would persist while the second
  write's failure rolls back only to its own savepoint. The first write
  would remain visible after the second fails, and the all-or-nothing
  assertion below would fail — catching the omission.
- On a standalone psycopg connection, ``with conn:`` would own
  commit/rollback and would commit the fixture's implicit transaction
  prematurely on a clean composition exit; the passthrough adapter adds none
  of that, so nothing here commits and the fixture rollback still discards
  every row at test end.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \
      .venv/bin/python -m pytest -m integration \
      tests/integration/test_service_transaction_composition.py

The suite consumes the database it is given and never provisions one;
provisioning and teardown of the target stay with the Owner-run
``devserver.sh --testdb`` flow. Agents never invoke that tooling.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import UniqueViolation

from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.persistence import events as event_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services.transaction_composition import composed_transaction

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


class _BorrowedConnectionPool:
    """Borrows the fixture-owned connection; adds no transaction or lifetime behavior.

    The fixture owns the connection's lifetime and its implicit transaction.
    This adapter only lends the connection to ``composed_transaction``: no
    ``with conn:`` wrapping (on a standalone psycopg connection that context
    owns commit/rollback and would commit the fixture's implicit transaction
    prematurely on a clean composition exit), no checkout bookkeeping, and
    nothing to close. It exists so the only composition-wide transaction
    scope in this module is the one the production primitive establishes.
    """

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    @contextmanager
    def connection(self) -> Iterator[Connection[Any]]:
        yield self._connection

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
    conn: Connection[Any], *, github_repository_id: int = 51_000_001
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


def _task_count(conn: Connection[Any], github_issue_id: int) -> int:
    row = conn.execute(
        "select count(*) from openorc.tasks where github_issue_id = %s",
        (github_issue_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_composition_rolls_back_all_repository_effects_when_a_later_write_fails(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _BorrowedConnectionPool(conn))

    with pytest.raises(UniqueViolation), composed_transaction(pool) as scoped:
        task_repositories.create_task(
            scoped,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=910_101,
            github_issue_number=42,
        )
        task_repositories.create_task(
            scoped,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=910_101,
            github_issue_number=42,
        )

    # The second (failing) write invalidated the whole composition: the first
    # write is gone with it, while the ownership rows composed outside the
    # transaction survive for the remaining assertions.
    assert _task_count(conn, 910_101) == 0
    row = conn.execute(
        "select count(*) from openorc.workspaces where id = %s", (workspace_id,)
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_composition_presents_all_composed_effects_together(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _BorrowedConnectionPool(conn))

    with composed_transaction(pool) as scoped:
        task = task_repositories.create_task(
            scoped,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=910_102,
            github_issue_number=43,
        )
        event = event_repositories.record_workflow_event(
            scoped,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=WorkflowEventActor.OPENORC,
            task_id=task.id,
        )

    assert _task_count(conn, 910_102) == 1
    row = conn.execute(
        "select count(*) from openorc.workflow_events where id = %s", (event.id,)
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_composition_recovers_from_a_handled_nested_failure(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _BorrowedConnectionPool(conn))

    with composed_transaction(pool) as scoped:
        task_repositories.create_task(
            scoped,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=920_101,
            github_issue_number=44,
        )
        with pytest.raises(UniqueViolation):
            task_repositories.create_task(
                scoped,
                workspace_id=workspace_id,
                repository_id=repository_id,
                github_issue_id=920_101,
                github_issue_number=44,
            )
        # The outer transaction survived the handled savepoint rollback.
        task_repositories.create_task(
            scoped,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=930_201,
            github_issue_number=45,
        )

    assert _task_count(conn, 920_101) == 1
    assert _task_count(conn, 930_201) == 1
