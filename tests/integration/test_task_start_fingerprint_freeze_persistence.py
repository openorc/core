"""Real-Postgres seam for the Owner-Start fingerprint freeze (issue #233).

The deterministic suite cannot execute Postgres. This integration-marked
module proves the durable commitment semantics against the real schema: the
status-aware CHECK admits a NULL baseline only for the never-started states,
the Owner Start capture freezes the freshly verified fingerprint atomically
with the queued/planning transition and can never rewrite an existing frozen
baseline, a forced failure after the capture rolls back to a baseline-free
``ready_to_plan`` row, and the archival paths accept the valid NULL pre-start
state while preserving frozen baselines. Run explicitly by the Owner through
the documented ``--testdb`` command.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation

from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import archive_task, create_task, update_task_status
from openorc.services.transaction_composition import composed_transaction

pytestmark = pytest.mark.integration

_FINGERPRINT_A = "a" * 64
_FINGERPRINT_B = "b" * 64
_MALFORMED = "NOT-A-DIGEST"


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


def _create_workspace_graph(conn: Connection[Any]) -> tuple[uuid.UUID, uuid.UUID]:
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s) on conflict do nothing", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, 'ws')",
        (workspace_id, profile_id),
    )
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, 'p')",
        (project_id, workspace_id),
    )
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, html_url, "
        "is_private, default_branch) "
        "values (%s, %s, %s, %s, 'octocat', 'repo', 'https://github.com/octocat/repo', "
        "false, 'main')",
        (repository_id, project_id, workspace_id, 111),
    )
    return workspace_id, repository_id


def _fingerprint_of(conn: Connection[Any], task_id: uuid.UUID) -> tuple[str, Any]:
    row = conn.execute(
        "select status, source_requirements_fingerprint from openorc.tasks where id = %s",
        (task_id,),
    ).fetchone()
    assert row is not None
    return row[0], row[1]


def test_the_status_aware_check_admits_only_the_never_started_nulls(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _create_workspace_graph(conn)

    # The commitment CHECK is durably present under its explicit name: a
    # missing or misnamed constraint is a migration defect, not a test
    # outcome, and is called out directly.
    constraint = conn.execute(
        "select 1 from pg_constraint "
        "where conrelid = 'openorc.tasks'::regclass "
        "and conname = 'tasks_source_requirements_fingerprint_status_check' "
        "and contype = 'c'"
    ).fetchone()
    assert constraint is not None, (
        "the status-aware source-requirements CHECK is missing from openorc.tasks"
    )

    _QUEUED_AT = datetime(2026, 10, 23, 12, 0, 0, tzinfo=UTC)

    def _raw_insert(
        issue_id: int, status: str, fingerprint: str | None, *, archived: bool, queued: bool
    ) -> None:
        conn.execute(
            "insert into openorc.tasks "
            "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
            "archived_at, queued_at, source_requirements_fingerprint) "
            "values (%s, %s, %s, %s, 42, %s, %s, %s, %s)",
            (
                uuid.uuid4(),
                workspace_id,
                repository_id,
                issue_id,
                status,
                _QUEUED_AT if archived else None,
                _QUEUED_AT if queued else None,
                fingerprint,
            ),
        )

    # The never-started states admit the NULL baseline deterministically.
    _raw_insert(5001, "ready_to_plan", None, archived=False, queued=False)
    _raw_insert(5002, "cancelled", None, archived=True, queued=False)
    # Every started state is rejected by exactly the commitment CHECK: the
    # iff-QUEUED timestamp fact is satisfied so the fingerprint CHECK is the
    # only violated constraint, and the constraint name is asserted so no
    # other violation source can masquerade as the contract.
    for issue_id, status, archived, queued in (
        (5003, "queued", False, True),
        (5004, "planning", False, False),
        (5005, "completed", True, False),
    ):
        with pytest.raises(CheckViolation) as excinfo, conn.transaction():
            _raw_insert(issue_id, status, None, archived=archived, queued=queued)
        assert excinfo.value.diag.constraint_name == (
            "tasks_source_requirements_fingerprint_status_check"
        ), f"{status} with a NULL baseline must be rejected by the commitment CHECK"
    # A malformed non-NULL digest is rejected on a never-started state too.
    with pytest.raises(CheckViolation) as excinfo, conn.transaction():
        _raw_insert(5006, "ready_to_plan", _MALFORMED, archived=False, queued=False)
    assert excinfo.value.diag.constraint_name == (
        "tasks_source_requirements_fingerprint_status_check"
    )


def test_the_start_capture_freezes_the_baseline_atomically_with_the_transition(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _create_workspace_graph(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9001,
        github_issue_number=42,
    )
    assert task.source_requirements_fingerprint is None

    queued = update_task_status(
        pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.QUEUED,
        capture_source_requirements_fingerprint=_FINGERPRINT_A,
    )
    assert queued is not None
    assert queued.status is TaskStatus.QUEUED
    assert queued.queued_at is not None
    assert queued.source_requirements_fingerprint == _FINGERPRINT_A

    claimed = update_task_status(
        pool,
        task.id,
        expected_state_token=queued.state_token,
        status=TaskStatus.PLANNING,
    )
    assert claimed is not None
    assert claimed.status is TaskStatus.PLANNING
    assert claimed.queued_at is None
    # The frozen baseline survives the later transition untouched.
    assert claimed.source_requirements_fingerprint == _FINGERPRINT_A


def test_the_capture_never_rewrites_an_existing_frozen_baseline(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _create_workspace_graph(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9002,
        github_issue_number=43,
    )
    queued = update_task_status(
        pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.QUEUED,
        capture_source_requirements_fingerprint=_FINGERPRINT_A,
    )
    assert queued is not None

    claimed = update_task_status(
        pool,
        task.id,
        expected_state_token=queued.state_token,
        status=TaskStatus.PLANNING,
        capture_source_requirements_fingerprint=_FINGERPRINT_B,
    )
    assert claimed is not None
    # The coalesce capture can never re-baseline a started Task: the frozen
    # value from the first successful start is immutable.
    assert claimed.source_requirements_fingerprint == _FINGERPRINT_A


def test_a_forced_failure_after_the_capture_rolls_back_to_a_baseline_free_row(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _create_workspace_graph(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9003,
        github_issue_number=44,
    )

    with pytest.raises(RuntimeError), composed_transaction(pool) as transaction_pool:
        claimed = update_task_status(
            transaction_pool,
            task.id,
            expected_state_token=task.state_token,
            status=TaskStatus.QUEUED,
            capture_source_requirements_fingerprint=_FINGERPRINT_A,
        )
        assert claimed is not None
        raise RuntimeError("forced failure after the capture")

    # The losing/failed start left NO baseline and no transition: the Task
    # stays ready_to_plan with the NULL fingerprint it was intaken with.
    status, fingerprint = _fingerprint_of(conn, task.id)
    assert status == "ready_to_plan"
    assert fingerprint is None


def test_the_never_started_null_cancels_without_inventing_a_baseline(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _create_workspace_graph(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    never_started = create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9004,
        github_issue_number=45,
    )
    cancelled = archive_task(
        pool,
        never_started.id,
        expected_state_token=never_started.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert cancelled is not None
    assert cancelled.status is TaskStatus.CANCELLED
    assert cancelled.archived_at is not None
    assert cancelled.source_requirements_fingerprint is None


def test_cancelling_a_started_task_preserves_its_frozen_baseline(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _create_workspace_graph(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9005,
        github_issue_number=46,
    )
    queued = update_task_status(
        pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.QUEUED,
        capture_source_requirements_fingerprint=_FINGERPRINT_A,
    )
    assert queued is not None

    cancelled = archive_task(
        pool,
        task.id,
        expected_state_token=queued.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert cancelled is not None
    assert cancelled.status is TaskStatus.CANCELLED
    assert cancelled.archived_at is not None
    # The frozen baseline is immutable through the cancellation: never
    # erased, never invented.
    assert cancelled.source_requirements_fingerprint == _FINGERPRINT_A
