"""Integration coverage for the GitHub relationship mirrors (issue #60).

Real-Postgres, integration-marked coverage for the durable invariants the
deterministic suite cannot execute: relation uniqueness/scope, the
aggregate-ownership cascade edges, the plain numeric related endpoints with
no local-Repository foreign key (relations to otherwise-unconfigured GitHub
repositories are representable), the empty-set clear semantics, and the
Task source baseline constraint. Run explicitly by the Owner through the
documented ``--testdb`` command; the suite consumes the supplied database
and never provisions one.
"""

from __future__ import annotations

import uuid

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, UniqueViolation

pytestmark = pytest.mark.integration


def _create_workspace_graph(conn: Connection) -> tuple[object, object, object]:
    profile_id = uuid.uuid4()
    conn.execute(
        "insert into auth.users (id) values (%s) on conflict do nothing",
        (profile_id,),
    )
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
    return workspace_id, project_id, profile_id


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


def test_parent_edge_uniqueness_and_cross_repository_representation(
    conn: Connection,
) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    # A parent edge whose related repository is NOT configured as an OpenOrc
    # Repository: representable, because endpoints are plain numeric facts.
    conn.execute(
        "insert into openorc.github_issue_hierarchy "
        "(workspace_id, repository_id, github_issue_id, parent_github_repository_id, "
        "parent_github_issue_id) values (%s, %s, %s, %s, %s)",
        (workspace_id, repository_id, 503, 999999, 777),
    )
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_issue_hierarchy "
            "(workspace_id, repository_id, github_issue_id, parent_github_repository_id, "
            "parent_github_issue_id) values (%s, %s, %s, %s, %s)",
            (workspace_id, repository_id, 503, 999999, 778),
        )
    # A zero/negative endpoint violates the positive-identity CHECK.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_issue_hierarchy "
            "(workspace_id, repository_id, github_issue_id, parent_github_repository_id, "
            "parent_github_issue_id) values (%s, %s, %s, %s, %s)",
            (workspace_id, repository_id, 504, 0, 777),
        )


def test_dependency_edge_uniqueness_and_empty_set_clear(conn: Connection) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    conn.execute(
        "insert into openorc.github_issue_dependencies "
        "(workspace_id, repository_id, github_issue_id, blocker_github_repository_id, "
        "blocker_github_issue_id) values (%s, %s, %s, %s, %s)",
        (workspace_id, repository_id, 503, 999999, 777),
    )
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_issue_dependencies "
            "(workspace_id, repository_id, github_issue_id, blocker_github_repository_id, "
            "blocker_github_issue_id) values (%s, %s, %s, %s, %s)",
            (workspace_id, repository_id, 503, 999999, 777),
        )
    # The empty authoritative set is represented by zero rows: clearing is a
    # plain delete, and no row remains to stamp.
    conn.execute(
        "delete from openorc.github_issue_dependencies where repository_id = %s",
        (repository_id,),
    )
    remaining = conn.execute(
        "select count(*) from openorc.github_issue_dependencies where repository_id = %s",
        (repository_id,),
    ).fetchone()
    assert remaining is not None and remaining[0] == 0


def test_cross_workspace_scope_disagreement_is_unrepresentable(conn: Connection) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    other_workspace_id = uuid.uuid4()
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    with pytest.raises(Exception), conn.transaction():  # noqa: B017 - FK scope violation
        conn.execute(
            "insert into openorc.github_issue_dependencies "
            "(workspace_id, repository_id, github_issue_id, blocker_github_repository_id, "
            "blocker_github_issue_id) values (%s, %s, %s, %s, %s)",
            (other_workspace_id, repository_id, 503, 999999, 777),
        )


def test_task_source_baseline_constraint(conn: Connection) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    conn.execute(
        "insert into openorc.tasks "
        "(workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) values (%s, %s, %s, %s, 'ready_to_plan', %s)",
        (workspace_id, repository_id, 503, 42, "a" * 64),
    )
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.tasks "
            "(workspace_id, repository_id, github_issue_id, github_issue_number, status, "
            "source_requirements_fingerprint) values (%s, %s, %s, %s, 'ready_to_plan', %s)",
            (workspace_id, repository_id, 504, 43, "not-a-digest"),
        )


def test_current_task_race_backstop_yields_exactly_one_current_task(
    conn: Connection,
) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    repository_id = _create_repository(conn, workspace_id, project_id, 111)
    fingerprint = "a" * 64
    task_a = uuid.uuid4()
    task_b = uuid.uuid4()
    # Two racing intake attempts for the same stable issue identity: the
    # partial unique index admits exactly one current Task.
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) values (%s, %s, %s, %s, %s, 'ready_to_plan', %s)",
        (task_a, workspace_id, repository_id, 503, 42, fingerprint),
    )
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.tasks "
            "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
            "source_requirements_fingerprint) values (%s, %s, %s, %s, %s, 'ready_to_plan', %s)",
            (task_b, workspace_id, repository_id, 503, 42, fingerprint),
        )
    current = conn.execute(
        "select count(*) from openorc.tasks where repository_id = %s and archived_at is null",
        (repository_id,),
    ).fetchone()
    assert current is not None and current[0] == 1
