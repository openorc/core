"""Repositories for the ownership and repository-identity foundation.

Explicit SQL repositories over the ``openorc`` schema for Profile, Workspace,
Project, and Repository (Phase 1). Rows map to transport-independent domain
objects from :mod:`openorc.domain.ownership`; instants returned from Postgres
are normalized to timezone-aware UTC at this boundary.

Violated database invariants surface as driver exceptions (for example
``psycopg.errors.UniqueViolation`` and ``ForeignKeyViolation``); translating
them into typed application errors is a service-layer concern, not a
persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    Profile,
    Project,
    Repository,
    RepositoryMetadata,
    Workspace,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_profile",
    "create_project",
    "create_repository",
    "create_workspace",
    "find_repository_by_github_identity",
    "get_repository",
    "update_repository_metadata",
]


def _profile_from_row(row: Sequence[Any]) -> Profile:
    return Profile(id=row[0], created_at=normalize_utc(row[1]))


def _workspace_from_row(row: Sequence[Any]) -> Workspace:
    return Workspace(
        id=row[0],
        owner_profile_id=row[1],
        name=row[2],
        created_at=normalize_utc(row[3]),
        updated_at=normalize_utc(row[4]),
    )


def _project_from_row(row: Sequence[Any]) -> Project:
    return Project(
        id=row[0],
        workspace_id=row[1],
        name=row[2],
        created_at=normalize_utc(row[3]),
        updated_at=normalize_utc(row[4]),
    )


def _repository_from_row(row: Sequence[Any]) -> Repository:
    return Repository(
        id=row[0],
        project_id=row[1],
        workspace_id=row[2],
        identity=GitHubRepositoryIdentity(github_repository_id=row[3]),
        metadata=RepositoryMetadata(
            owner_login=row[4],
            name=row[5],
            html_url=row[6],
            is_private=row[7],
            default_branch=row[8],
        ),
        created_at=normalize_utc(row[9]),
        updated_at=normalize_utc(row[10]),
    )


def create_profile(pool: DatabasePool, *, profile_id: UUID) -> Profile:
    """Insert a Profile whose id is the corresponding Supabase Auth user UUID.

    The caller supplies the identity; the database never generates one.
    Inserting the same UUID twice violates the primary key (Profile/Auth is
    1:1).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.profiles (id) values (%s) returning id, created_at",
            (profile_id,),
        ).fetchone()
    assert row is not None
    return _profile_from_row(row)


def create_workspace(pool: DatabasePool, *, owner_profile_id: UUID, name: str) -> Workspace:
    """Insert a Workspace owned by exactly one Profile."""
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.workspaces (owner_profile_id, name) "
            "values (%s, %s) "
            "returning id, owner_profile_id, name, created_at, updated_at",
            (owner_profile_id, name),
        ).fetchone()
    assert row is not None
    return _workspace_from_row(row)


def create_project(pool: DatabasePool, *, workspace_id: UUID, name: str) -> Project:
    """Insert a Project belonging to one Workspace."""
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.projects (workspace_id, name) "
            "values (%s, %s) "
            "returning id, workspace_id, name, created_at, updated_at",
            (workspace_id, name),
        ).fetchone()
    assert row is not None
    return _project_from_row(row)


def create_repository(
    pool: DatabasePool,
    *,
    project_id: UUID,
    workspace_id: UUID,
    identity: GitHubRepositoryIdentity,
    metadata: RepositoryMetadata,
) -> Repository:
    """Insert one canonical Repository record for a GitHub repository identity.

    A second record binding the same GitHub repository identity to the same
    Workspace violates the ``(workspace_id, github_repository_id)`` unique
    constraint; the same identity in a different Workspace is a distinct,
    independent record.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.repositories "
            "(project_id, workspace_id, github_repository_id, owner_login, name, "
            "html_url, is_private, default_branch) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) "
            "returning id, project_id, workspace_id, github_repository_id, "
            "owner_login, name, html_url, is_private, default_branch, "
            "created_at, updated_at",
            (
                project_id,
                workspace_id,
                identity.github_repository_id,
                metadata.owner_login,
                metadata.name,
                metadata.html_url,
                metadata.is_private,
                metadata.default_branch,
            ),
        ).fetchone()
    assert row is not None
    return _repository_from_row(row)


def get_repository(pool: DatabasePool, repository_id: UUID) -> Repository | None:
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, project_id, workspace_id, github_repository_id, "
            "owner_login, name, html_url, is_private, default_branch, "
            "created_at, updated_at "
            "from openorc.repositories where id = %s",
            (repository_id,),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def find_repository_by_github_identity(
    pool: DatabasePool, *, workspace_id: UUID, identity: GitHubRepositoryIdentity
) -> Repository | None:
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, project_id, workspace_id, github_repository_id, "
            "owner_login, name, html_url, is_private, default_branch, "
            "created_at, updated_at "
            "from openorc.repositories "
            "where workspace_id = %s and github_repository_id = %s",
            (workspace_id, identity.github_repository_id),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def update_repository_metadata(
    pool: DatabasePool, repository_id: UUID, *, metadata: RepositoryMetadata
) -> Repository | None:
    """Replace the mutable observed metadata of one Repository record.

    Identity columns are never touched: the record keeps its OpenOrc id and
    its stable GitHub repository identity, and ``updated_at`` advances to the
    database clock. Returns ``None`` when the repository does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.repositories "
            "set owner_login = %s, name = %s, html_url = %s, is_private = %s, "
            "default_branch = %s, updated_at = now() "
            "where id = %s "
            "returning id, project_id, workspace_id, github_repository_id, "
            "owner_login, name, html_url, is_private, default_branch, "
            "created_at, updated_at",
            (
                metadata.owner_login,
                metadata.name,
                metadata.html_url,
                metadata.is_private,
                metadata.default_branch,
                repository_id,
            ),
        ).fetchone()
    return None if row is None else _repository_from_row(row)
