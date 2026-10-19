"""Integration-marked persistence tests for the planning-bootstrap claim
(issue #203).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
one-shot ``tasks.planning_bootstrap_claim_token`` fact directly: NULL on a
fresh Task, the exactly-once conditional write (successful once; rejected on
a stale token, on archival, and once any claim exists), the token rotation
committed with the claim, the read-back, and the fact's persistence through
the Task's lifetime. They are excluded from the ordinary deterministic
baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_planning_bootstrap_claim_persistence.py

The suite consumes the database it is given and never provisions one.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any

import pytest
from psycopg import Connection

from openorc.persistence.tasks import (
    claim_task_planning_bootstrap,
    get_task,
    get_task_planning_bootstrap_claim,
)

pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and
    transaction (the same SAVEPOINT semantics as the other persistence
    suites)."""

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


def _insert_task_chain(conn: Connection[Any]) -> tuple[Any, ...]:
    """Insert the Profile/Workspace/Project/Repository/Task chain and return
    (task_id, workspace_id, state_token)."""
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
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
        "html_url, is_private) values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            5_000_000,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
        ),
    )
    task_id = uuid.uuid4()
    state_token = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, "
        "status, source_requirements_fingerprint, state_token) "
        "values (%s, %s, %s, %s, %s, 'planning', %s, %s)",
        (task_id, workspace_id, repository_id, 9001, 42, "a" * 64, state_token),
    )
    return task_id, workspace_id, state_token


def test_claim_is_null_until_claimed_and_written_exactly_once(
    conn: Connection[Any],
) -> None:
    task_id, _workspace_id, state_token = _insert_task_chain(conn)
    pool = _SingleConnectionPool(conn)

    # A fresh Task is unclaimed.
    assert get_task_planning_bootstrap_claim(pool, task_id=task_id) is None

    # The first claim succeeds and rotates the token.
    claim_token = uuid.uuid4()
    post_claim_token = claim_task_planning_bootstrap(
        pool,
        task_id=task_id,
        expected_state_token=state_token,
        claim_token=claim_token,
    )
    assert post_claim_token is not None
    assert get_task_planning_bootstrap_claim(pool, task_id=task_id) == claim_token
    task = get_task(pool, task_id)
    assert task is not None
    assert task.state_token == post_claim_token

    # A second claim is rejected: the fact is one-shot regardless of token.
    assert (
        claim_task_planning_bootstrap(
            pool,
            task_id=task_id,
            expected_state_token=post_claim_token,
            claim_token=uuid.uuid4(),
        )
        is None
    )
    assert get_task_planning_bootstrap_claim(pool, task_id=task_id) == claim_token


def test_claim_is_rejected_on_a_stale_token_or_an_archived_task(
    conn: Connection[Any],
) -> None:
    task_id, _workspace_id, state_token = _insert_task_chain(conn)
    pool = _SingleConnectionPool(conn)

    # A stale expected token claims nothing.
    assert (
        claim_task_planning_bootstrap(
            pool,
            task_id=task_id,
            expected_state_token=uuid.uuid4(),
            claim_token=uuid.uuid4(),
        )
        is None
    )
    assert get_task_planning_bootstrap_claim(pool, task_id=task_id) is None

    # Archiving the Task closes the claim path for good; the fact stays NULL.
    conn.execute(
        "update openorc.tasks "
        "set status = 'cancelled', archived_at = now(), "
        "state_token = gen_random_uuid() where id = %s",
        (task_id,),
    )
    archived_task = get_task(pool, task_id)
    assert archived_task is not None
    assert (
        claim_task_planning_bootstrap(
            pool,
            task_id=task_id,
            expected_state_token=archived_task.state_token,
            claim_token=uuid.uuid4(),
        )
        is None
    )
    assert get_task_planning_bootstrap_claim(pool, task_id=task_id) is None
