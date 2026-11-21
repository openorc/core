"""Shared integration persistence fixtures (issues #84/#219).

Since #347 the removed blocking-path tests are gone; the surviving integration
modules import these shared seeded-database fixtures. The module deliberately
carries no test functions of its own."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg import Connection

_EXTERNAL_INSTALLATION_ID = 77_000_004


def _insert_project(conn: Connection[Any], workspace_id: uuid.UUID) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    return project_id


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
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


def _insert_workspace(conn: Connection[Any], owner_profile_id: uuid.UUID) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, "workspace"),
    )
    return workspace_id


def _insert_repository(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    github_installation_id: uuid.UUID,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch, github_installation_id) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            _insert_project(conn, workspace_id),
            workspace_id,
            77_000_005,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            # The production route resolver requires the repository-to-installation
            # relationship: the durable UUID of the same-Workspace installation
            # row, never the external numeric installation ID.
            github_installation_id,
        ),
    )
    return repository_id


def _insert_installation(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, %s, %s, %s)",
        (
            installation_id,
            workspace_id,
            _EXTERNAL_INSTALLATION_ID,
            77_000_006,
            "octocat",
            "Organization",
        ),
    )
    return installation_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, 7702, 42, "a" * 64),
    )
    return task_id


def _insert_connection(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name) "
        "values (%s, %s, 'cline', 'primary hub')",
        (connection_id, workspace_id),
    )
    return connection_id


def _insert_ready_session(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    connection_id: uuid.UUID,
    role: str,
    external_session_id: str,
) -> uuid.UUID:
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, %s, %s, %s, 'ready', '{}'::jsonb, now())",
        (session_id, workspace_id, task_id, role, connection_id, external_session_id),
    )
    return session_id
