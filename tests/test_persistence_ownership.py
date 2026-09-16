"""Deterministic mapping tests for ownership repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows and a
fake pool/connection seam (mirroring the transaction-boundary fakes) to prove
row-to-domain-object mapping, UTC normalization at the persistence boundary,
parameterization, and empty-result handling. Database constraint behavior is
proven against a real database by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    Profile,
    Repository,
    RepositoryMetadata,
    Workspace,
)
from openorc.persistence.ownership import (
    create_profile,
    create_repository,
    create_workspace,
    find_repository_by_github_identity,
    get_repository,
    update_repository_metadata,
)
from openorc.persistence.pool import DatabasePool


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class FakeConnection:
    """Records executed SQL and returns canned rows."""

    def __init__(self, row: tuple[Any, ...] | None = None) -> None:
        self.row = row
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.row)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("ownership mapping tests never close pools")


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 16, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _repository_row(default_branch: str | None = "main") -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        987654321,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        default_branch,
        _observed_at(),
        _observed_at(),
    )


def test_create_profile_uses_the_caller_supplied_auth_uuid() -> None:
    profile_id = uuid.uuid4()
    conn = FakeConnection(row=(profile_id, _observed_at()))

    profile = create_profile(cast(DatabasePool, FakePool(conn)), profile_id=profile_id)

    assert profile == Profile(id=profile_id, created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC))
    assert profile.created_at.utcoffset() == timedelta(0)
    sql, params = conn.executed[0]
    assert "insert into openorc.profiles" in sql
    assert params == (profile_id,)


def test_create_workspace_maps_row_and_parameters() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    observed = _observed_at()
    conn = FakeConnection(row=(workspace_id, profile_id, "platform", observed, observed))

    workspace = create_workspace(
        cast(DatabasePool, FakePool(conn)), owner_profile_id=profile_id, name="platform"
    )

    assert workspace == Workspace(
        id=workspace_id,
        owner_profile_id=profile_id,
        name="platform",
        created_at=observed,
        updated_at=observed,
    )
    sql, params = conn.executed[0]
    assert "insert into openorc.workspaces" in sql
    assert params == (profile_id, "platform")


def test_create_repository_maps_row_to_domain_object() -> None:
    row = _repository_row()
    conn = FakeConnection(row=row)
    identity = GitHubRepositoryIdentity(github_repository_id=987654321)
    metadata = RepositoryMetadata(
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )

    created = create_repository(
        cast(DatabasePool, FakePool(conn)),
        project_id=row[1],
        workspace_id=row[2],
        identity=identity,
        metadata=metadata,
    )

    assert isinstance(created, Repository)
    assert created.id == row[0]
    assert created.identity == identity
    assert created.metadata == metadata
    assert created.created_at.utcoffset() == timedelta(0)
    assert created.updated_at.utcoffset() == timedelta(0)
    sql, params = conn.executed[0]
    assert "insert into openorc.repositories" in sql
    assert params == (
        row[1],
        row[2],
        987654321,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
    )


def test_get_repository_returns_none_when_missing() -> None:
    repository_id = uuid.uuid4()
    conn = FakeConnection(row=None)

    assert get_repository(cast(DatabasePool, FakePool(conn)), repository_id) is None

    sql, params = conn.executed[0]
    assert "from openorc.repositories where id = %s" in sql
    assert params == (repository_id,)


def test_find_repository_by_github_identity_scopes_to_the_workspace() -> None:
    row = _repository_row()
    conn = FakeConnection(row=row)
    identity = GitHubRepositoryIdentity(github_repository_id=987654321)

    found = find_repository_by_github_identity(
        cast(DatabasePool, FakePool(conn)), workspace_id=row[2], identity=identity
    )

    assert found is not None
    assert found.id == row[0]
    sql, params = conn.executed[0]
    assert "where workspace_id = %s and github_repository_id = %s" in sql
    assert params == (row[2], 987654321)


def test_update_repository_metadata_maps_updated_row_and_handles_missing() -> None:
    row = _repository_row(default_branch=None)
    conn = FakeConnection(row=row)
    metadata = RepositoryMetadata(
        owner_login="renamed-owner",
        name="renamed-repo",
        html_url="https://github.com/renamed-owner/renamed-repo",
        is_private=True,
        default_branch=None,
    )

    updated = update_repository_metadata(
        cast(DatabasePool, FakePool(conn)), row[0], metadata=metadata
    )

    assert updated is not None
    assert updated.id == row[0]
    assert updated.identity.github_repository_id == 987654321
    assert updated.metadata.default_branch is None
    sql, params = conn.executed[0]
    assert "update openorc.repositories" in sql
    assert params == (
        "renamed-owner",
        "renamed-repo",
        "https://github.com/renamed-owner/renamed-repo",
        True,
        None,
        row[0],
    )

    missing = FakeConnection(row=None)
    assert (
        update_repository_metadata(cast(DatabasePool, FakePool(missing)), row[0], metadata=metadata)
        is None
    )
