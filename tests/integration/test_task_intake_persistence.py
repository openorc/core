"""Integration coverage for the authoritative Task intake (issue #60).

Real-Postgres, integration-marked coverage for the Task source baseline and
the current-Task race backstop the deterministic suite cannot execute: intake
records NO source baseline (issue #233 — Owner Start freezes it), an archived
attempt retains exactly the baseline it froze, and a lost current-Task
uniqueness race leaves exactly one current Task. Run explicitly by the Owner
through the documented ``--testdb`` command.
"""

from __future__ import annotations

import uuid

import pytest
from psycopg import Connection
from psycopg.errors import UniqueViolation

pytestmark = pytest.mark.integration

_FINGERPRINT_A = "a" * 64


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
    fingerprint: str | None = None,
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


def test_intake_records_no_baseline_and_history_retains_what_it_froze(
    conn: Connection,
) -> None:
    workspace_id, project_id = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    # A started attempt that reached its terminal outcome retains the frozen
    # baseline it committed at Owner Start (never rewritten, never erased).
    started_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "archived_at, source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'cancelled', now(), %s)",
        (started_id, workspace_id, repository_id, 503, 42, _FINGERPRINT_A),
    )
    # The fresh intake attempt for the same issue carries NO baseline: the
    # corrected contract freezes it only at a successful Owner Start.
    _intake_task(conn, workspace_id, repository_id, 503)
    attempts = conn.execute(
        "select source_requirements_fingerprint, archived_at, status from openorc.tasks "
        "where repository_id = %s order by created_at, id",
        (repository_id,),
    ).fetchall()
    assert len(attempts) == 2
    archived = [attempt for attempt in attempts if attempt[1] is not None]
    current = [attempt for attempt in attempts if attempt[1] is None]
    assert len(archived) == 1 and archived[0][0] == _FINGERPRINT_A
    assert len(current) == 1 and current[0][0] is None and current[0][2] == "ready_to_plan"


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
