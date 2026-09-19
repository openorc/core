"""Integration-marked persistence tests for the ownership foundation (issue #19).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
ownership invariants directly: caller-supplied unique Profile identity,
Workspace/Project ownership foreign keys, direct Workspace-scope consistency,
and per-Workspace repository uniqueness. They are excluded from the ordinary
deterministic baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \
      .venv/bin/python -m pytest -m integration tests/integration/test_ownership_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and outside
agent responsibility: in the normal Owner local-development flow the target is
the ephemeral non-production Supabase branch that the Owner-only
``devserver.sh`` command creates and later deletes (agents never invoke it).
The session fixture merely resets the ``openorc`` schema and applies the
committed migrations from scratch within the supplied database, and the suite
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation, UniqueViolation

from openorc.domain.ownership import GitHubRepositoryIdentity, RepositoryMetadata
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


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


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def test_profile_id_is_caller_supplied_and_unique(conn: Connection[Any]) -> None:
    profile_id = uuid.uuid4()

    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): create the backing Auth
    # user first.
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))

    row = conn.execute(
        "insert into openorc.profiles (id) values (%s) returning id",
        (profile_id,),
    ).fetchone()

    assert row is not None
    assert row[0] == profile_id  # the caller's Supabase Auth UUID, not generated

    with pytest.raises(UniqueViolation):
        conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))


def test_workspace_requires_existing_owner_profile(conn: Connection[Any]) -> None:
    with pytest.raises(ForeignKeyViolation):
        conn.execute(
            "insert into openorc.workspaces (owner_profile_id, name) values (%s, %s)",
            (uuid.uuid4(), "orphan workspace"),
        )


def test_project_requires_its_workspace(conn: Connection[Any]) -> None:
    with pytest.raises(ForeignKeyViolation):
        conn.execute(
            "insert into openorc.projects (workspace_id, name) values (%s, %s)",
            (uuid.uuid4(), "orphan project"),
        )


def test_repository_rejects_workspace_mismatch_with_project(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_one = _insert_workspace(conn, profile_id, name="workspace-one")
    workspace_two = _insert_workspace(conn, profile_id, name="workspace-two")
    project_in_one = _insert_project(conn, workspace_one, name="project-one")

    with pytest.raises(ForeignKeyViolation):
        _insert_repository(
            conn,
            project_id=project_in_one,
            workspace_id=workspace_two,  # disagrees with the owning project
            github_repository_id=1000,
        )


def test_repository_rejects_duplicate_github_identity_in_one_workspace(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_one = _insert_project(conn, workspace_id, name="project-one")
    project_two = _insert_project(conn, workspace_id, name="project-two")

    _insert_repository(
        conn, project_id=project_one, workspace_id=workspace_id, github_repository_id=2000
    )

    # Workspace-wide canonical identity: a second project cannot re-bind it.
    with pytest.raises(UniqueViolation):
        _insert_repository(
            conn,
            project_id=project_two,
            workspace_id=workspace_id,
            github_repository_id=2000,
        )


def test_same_github_repository_identity_in_two_workspaces_succeeds(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_one = _insert_workspace(conn, profile_id, name="workspace-one")
    workspace_two = _insert_workspace(conn, profile_id, name="workspace-two")
    project_one = _insert_project(conn, workspace_one, name="project-one")
    project_two = _insert_project(conn, workspace_two, name="project-two")

    repository_one = _insert_repository(
        conn, project_id=project_one, workspace_id=workspace_one, github_repository_id=3000
    )
    repository_two = _insert_repository(
        conn, project_id=project_two, workspace_id=workspace_two, github_repository_id=3000
    )

    assert repository_one != repository_two


def test_metadata_update_preserves_repository_identity(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn, project_id=project_id, workspace_id=workspace_id, github_repository_id=4000
    )

    row = conn.execute(
        "update openorc.repositories "
        "set owner_login = %s, name = %s, html_url = %s, is_private = %s, "
        "default_branch = %s, updated_at = now() "
        "where id = %s "
        "returning id, github_repository_id, owner_login, name, html_url, "
        "is_private, default_branch",
        (
            "renamed-owner",
            "renamed-repo",
            "https://github.com/renamed-owner/renamed-repo",
            True,
            "develop",
            repository_id,
        ),
    ).fetchone()

    assert row is not None
    assert row[0] == repository_id  # OpenOrc record identity unchanged
    assert row[1] == 4000  # stable GitHub repository identity unchanged
    assert (row[2], row[3], row[4], row[5], row[6]) == (
        "renamed-owner",
        "renamed-repo",
        "https://github.com/renamed-owner/renamed-repo",
        True,
        "develop",
    )


def test_repository_round_trip_through_persistence_layer(conn: Connection[Any]) -> None:
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): create the backing Auth
    # user first.
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    profile = ownership_repositories.create_profile(pool, profile_id=profile_id)
    workspace = ownership_repositories.create_workspace(
        pool, owner_profile_id=profile.id, name="round-trip workspace"
    )
    project = ownership_repositories.create_project(
        pool, workspace_id=workspace.id, name="round-trip project"
    )
    identity = GitHubRepositoryIdentity(github_repository_id=5000)
    metadata = RepositoryMetadata(
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )

    created = ownership_repositories.create_repository(
        pool,
        project_id=project.id,
        workspace_id=workspace.id,
        identity=identity,
        metadata=metadata,
    )

    assert ownership_repositories.get_repository(pool, created.id) == created
    assert (
        ownership_repositories.find_repository_by_github_identity(
            pool, workspace_id=workspace.id, identity=identity
        )
        == created
    )

    # Observed metadata is mutable; identity is not.
    updated = ownership_repositories.update_repository_metadata(
        pool,
        created.id,
        metadata=RepositoryMetadata(
            owner_login="new-owner",
            name="renamed",
            html_url="https://github.com/new-owner/renamed",
            is_private=True,
            default_branch="develop",
        ),
    )
    assert updated is not None
    assert updated.id == created.id
    assert updated.identity == identity
    assert updated.metadata.owner_login == "new-owner"

    # Instants crossing the boundary are timezone-aware and UTC-normalized.
    created_at = created.created_at
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is not None
    assert created_at.utcoffset() == timedelta(0)
