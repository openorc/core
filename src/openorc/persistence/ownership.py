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
    "ensure_profile",
    "find_repository_by_github_identity",
    "get_profile",
    "get_project",
    "get_repository",
    "get_workspace",
    "update_repository_metadata",
    "update_workspace_guidance",
    "update_workspace_review_iteration_limit",
]

# The full Workspace column list, including the first-class configuration
# settings added by issue #53 (review_iteration_limit, guidance).
_WORKSPACE_COLUMNS = (
    "id, owner_profile_id, name, created_at, updated_at, review_iteration_limit, guidance"
)


def _profile_from_row(row: Sequence[Any]) -> Profile:
    return Profile(id=row[0], created_at=normalize_utc(row[1]))


def _workspace_from_row(row: Sequence[Any]) -> Workspace:
    return Workspace(
        id=row[0],
        owner_profile_id=row[1],
        name=row[2],
        created_at=normalize_utc(row[3]),
        updated_at=normalize_utc(row[4]),
        review_iteration_limit=row[5],
        guidance=row[6],
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


def get_profile(pool: DatabasePool, profile_id: UUID) -> Profile | None:
    """Return the Profile with the given Supabase Auth user UUID, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, created_at from openorc.profiles where id = %s",
            (profile_id,),
        ).fetchone()
    return None if row is None else _profile_from_row(row)


def ensure_profile(pool: DatabasePool, *, profile_id: UUID) -> Profile:
    """Resolve or idempotently bootstrap the Profile for one Auth user UUID.

    ``insert ... on conflict (id) do nothing`` converges concurrent first
    requests for the same valid Supabase user onto exactly one Profile: the
    winner's insert commits, the loser's insert no-ops, and the fallback
    ``select`` (a fresh statement snapshot under ``READ COMMITTED``) returns
    the winner's row. Repeated calls are idempotent and never surface a
    duplicate-key error.

    Fail-closed account-identity boundary: ``openorc.profiles.id`` references
    ``auth.users (id) ON DELETE CASCADE`` (the single sanctioned Auth
    boundary, issue #27). If the backing Auth user row does not exist — for
    example, a previously issued but still cryptographically valid JWT whose
    Auth user has been permanently deleted — the insert raises
    ``psycopg.errors.ForeignKeyViolation``. Translating that into a typed
    authentication failure (never re-creating the identity) is the service
    layer's concern, not a persistence one.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.profiles (id) values (%s) "
            "on conflict (id) do nothing "
            "returning id, created_at",
            (profile_id,),
        ).fetchone()
        if row is None:
            # Another request created (or is committing) this Profile; the
            # on-conflict insert lost the race, so read the existing row.
            row = conn.execute(
                "select id, created_at from openorc.profiles where id = %s",
                (profile_id,),
            ).fetchone()
    assert row is not None
    return _profile_from_row(row)


def create_workspace(pool: DatabasePool, *, owner_profile_id: UUID, name: str) -> Workspace:
    """Insert a Workspace owned by exactly one Profile.

    The first-class Workspace configuration settings are not insert inputs:
    the durable column defaults supply them, so a fresh Workspace carries the
    configured review-iteration boundary (``review_iteration_limit`` default
    5) and blank guidance unless explicitly changed afterwards.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.workspaces (owner_profile_id, name) "
            "values (%s, %s) "
            f"returning {_WORKSPACE_COLUMNS}",
            (owner_profile_id, name),
        ).fetchone()
    assert row is not None
    return _workspace_from_row(row)


def get_workspace(pool: DatabasePool, workspace_id: UUID) -> Workspace | None:
    """Return one Workspace with its configuration settings, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s",
            (workspace_id,),
        ).fetchone()
    return None if row is None else _workspace_from_row(row)


def get_project(pool: DatabasePool, project_id: UUID) -> Project | None:
    """Return one Project by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, workspace_id, name, created_at, updated_at "
            "from openorc.projects where id = %s",
            (project_id,),
        ).fetchone()
    return None if row is None else _project_from_row(row)


def update_workspace_review_iteration_limit(
    pool: DatabasePool, workspace_id: UUID, *, review_iteration_limit: int
) -> tuple[Workspace, int, bool] | None:
    """Set the Workspace review-loop iteration limit (issue #53).

    One deliberate ``SELECT ... FOR UPDATE`` inside one short transaction
    captures the exact before-state, then the write happens only when the
    value actually differs. The locked same-transaction previous/new facts
    are the safe audit handoff for a consequential configuration-change
    event (#56) — the caller never re-reads a racy before-state. The change
    affects future ReviewLoops only: ``ReviewLoop.iteration_limit`` values
    stored on existing loops are immutable historical configuration and are
    never touched.

    Returns ``(updated Workspace, previous limit, changed)``; a no-op
    returns the unchanged Workspace with ``changed=False`` and does not
    advance ``updated_at``. Returns ``None`` when the Workspace does not
    exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s for update",
            (workspace_id,),
        ).fetchone()
        if row is None:
            return None
        previous = _workspace_from_row(row)
        if previous.review_iteration_limit == review_iteration_limit:
            return previous, previous.review_iteration_limit, False
        updated_row = conn.execute(
            "update openorc.workspaces "
            "set review_iteration_limit = %s, updated_at = now() "
            "where id = %s "
            f"returning {_WORKSPACE_COLUMNS}",
            (review_iteration_limit, workspace_id),
        ).fetchone()
    assert updated_row is not None
    return _workspace_from_row(updated_row), previous.review_iteration_limit, True


def update_workspace_guidance(
    pool: DatabasePool, workspace_id: UUID, *, guidance: str
) -> tuple[Workspace, str, bool] | None:
    """Set the Workspace guidance prose (issue #53).

    Guidance is one current, Owner-authored value: the write replaces the
    current value and creates no history, hash, snapshot, or revision. The
    same deliberate ``SELECT ... FOR UPDATE`` plus conditional-write shape as
    the review-limit update provides the exact locked before-state for the
    #56 audit handoff — the previous prose is returned to the caller but the
    guidance-change event needs only the semantic fact that the setting
    changed, never the prose itself. Returns ``(updated Workspace, previous
    guidance, changed)``; a no-op returns the unchanged Workspace with
    ``changed=False``. Returns ``None`` when the Workspace does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s for update",
            (workspace_id,),
        ).fetchone()
        if row is None:
            return None
        previous = _workspace_from_row(row)
        if previous.guidance == guidance:
            return previous, previous.guidance, False
        updated_row = conn.execute(
            "update openorc.workspaces "
            "set guidance = %s, updated_at = now() "
            "where id = %s "
            f"returning {_WORKSPACE_COLUMNS}",
            (guidance, workspace_id),
        ).fetchone()
    assert updated_row is not None
    return _workspace_from_row(updated_row), previous.guidance, True


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

    Identity is never touched: the record keeps its OpenOrc UUID and its stable
    external GitHub repository identity, and ``updated_at`` advances to the
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
