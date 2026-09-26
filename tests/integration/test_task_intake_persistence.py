"""Integration coverage for the authoritative Task intake (issue #60).

Real-Postgres, integration-marked coverage for the Task source baseline and
the current-Task race backstop the deterministic suite cannot execute: the
intake-created Task records the exact reconciled fingerprint, a lost
current-Task uniqueness race leaves exactly one current Task, and archived
attempts release the issue for a fresh attempt. Run explicitly by the Owner
through the documented ``--testdb`` command.
"""

from __future__ import annotations

import uuid

import pytest
from psycopg import Connection
from psycopg.errors import UniqueViolation

pytestmark = pytest.mark.integration

_FINGERPRINT_A = "a" * 64
_FINGERPRINT_B = "b" * 64


def _create_workspace_graph(conn: Connection) -> tuple[object, object]:
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
    return workspace_id, project_id


def _create_repository(
    conn: Connection, workspace_id: object, project_id: object, github_repository_id: int
) -> object:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, html_url, "
        "is_private, default_branch) "
        "values (%s, %s, %s, %s, 'octocat', 'repo', 'https://github.com/octocat/repo', "
        "false, 'main')",
        (repository_id, project_id, workspace_id, github_repository_id),
    )
    return repository_id


def _intake_task(
    conn: Connection,
    workspace_id: object,
    repository_id: object,
    github_issue_id: int,
    fingerprint: str,
    task_id: object | None = None,
) -> None:
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan', %s)",
        (
            task_id or uuid.uuid4(),
            workspace_id,
            repository_id,
            github_issue_id,
            42,
            fingerprint,
        ),
    )


def test_the_source_baseline_is_immutable_attempt_history(conn: Connection) -> None:
    workspace_id, project_id = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    _intake_task(conn, workspace_id, repository_id, 503, _FINGERPRINT_A)
    row = conn.execute(
        "select source_requirements_fingerprint from openorc.tasks "
        "where repository_id = %s and github_issue_id = %s",
        (repository_id, 503),
    ).fetchone()
    assert row is not None and row[0] == _FINGERPRINT_A
    # A later requirements change never rewrites the recorded baseline; a
    # fresh attempt for the same issue may carry a different one.
    conn.execute(
        "update openorc.tasks set archived_at = now(), status = 'cancelled' "
        "where repository_id = %s",
        (repository_id,),
    )
    _intake_task(conn, workspace_id, repository_id, 503, _FINGERPRINT_B)
    attempts = conn.execute(
        "select source_requirements_fingerprint from openorc.tasks "
        "where repository_id = %s order by created_at, id",
        (repository_id,),
    ).fetchall()
    assert [attempt[0] for attempt in attempts] == [_FINGERPRINT_A, _FINGERPRINT_B]


def test_a_racing_duplicate_intake_keeps_exactly_one_current_task(
    conn: Connection,
) -> None:
    workspace_id, project_id = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    _intake_task(conn, workspace_id, repository_id, 503, _FINGERPRINT_A, task_id=uuid.uuid4())
    with pytest.raises(UniqueViolation), conn.transaction():
        _intake_task(conn, workspace_id, repository_id, 503, _FINGERPRINT_A)
    current = conn.execute(
        "select count(*) from openorc.tasks where repository_id = %s and archived_at is null",
        (repository_id,),
    ).fetchone()
    assert current is not None and current[0] == 1
