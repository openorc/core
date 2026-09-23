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
    Project,
    Repository,
    RepositoryMetadata,
    Workspace,
)
from openorc.persistence.ownership import (
    create_profile,
    create_repository,
    create_workspace,
    ensure_profile,
    find_repository_by_github_identity,
    get_profile,
    get_project,
    get_repository,
    get_workspace,
    set_repository_installation_route,
    update_repository_metadata,
    update_workspace_guidance,
    update_workspace_review_iteration_limit,
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
        None,
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


class ScriptedProfileConnection(FakeConnection):
    """FakeConnection variant playing back a sequence of results in order."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        super().__init__(row=None)
        self.results = list(results)

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))


def test_get_profile_maps_row_or_empty_result() -> None:
    profile_id = uuid.uuid4()
    conn = FakeConnection(row=(profile_id, _observed_at()))

    profile = get_profile(cast(DatabasePool, FakePool(conn)), profile_id)

    assert profile == Profile(id=profile_id, created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC))
    sql, params = conn.executed[0]
    assert "from openorc.profiles where id = %s" in sql
    assert params == (profile_id,)

    empty = get_profile(cast(DatabasePool, FakePool(FakeConnection(row=None))), profile_id)
    assert empty is None


def test_ensure_profile_maps_the_insert_returning_row() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedProfileConnection([(profile_id, _observed_at())])

    profile = ensure_profile(cast(DatabasePool, FakePool(conn)), profile_id=profile_id)

    assert profile == Profile(id=profile_id, created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC))
    sql, params = conn.executed[0]
    assert "insert into openorc.profiles" in sql
    assert "on conflict (id) do nothing" in sql
    assert params == (profile_id,)


def test_ensure_profile_falls_back_to_select_when_conflict_returns_no_row() -> None:
    # Losing the bootstrap race: the on-conflict insert returns no row and the
    # winner's row is read with a fresh select in the same transaction.
    profile_id = uuid.uuid4()
    conn = ScriptedProfileConnection([None, (profile_id, _observed_at())])

    profile = ensure_profile(cast(DatabasePool, FakePool(conn)), profile_id=profile_id)

    assert profile == Profile(id=profile_id, created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC))
    insert_sql, _ = conn.executed[0]
    select_sql, select_params = conn.executed[1]
    assert "on conflict (id) do nothing" in insert_sql
    assert "from openorc.profiles where id = %s" in select_sql
    assert select_params == (profile_id,)


def test_create_workspace_maps_row_and_parameters() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    observed = _observed_at()
    # The configuration settings are not insert inputs: the row is completed
    # by the durable column defaults (review_iteration_limit 5, guidance '').
    conn = FakeConnection(row=(workspace_id, profile_id, "platform", observed, observed, 5, ""))

    workspace = create_workspace(
        cast(DatabasePool, FakePool(conn)), owner_profile_id=profile_id, name="platform"
    )

    assert workspace == Workspace(
        id=workspace_id,
        owner_profile_id=profile_id,
        name="platform",
        created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        review_iteration_limit=5,
        guidance="",
    )
    assert workspace.created_at.utcoffset() == timedelta(0)
    sql, params = conn.executed[0]
    assert "insert into openorc.workspaces" in sql
    assert params == (profile_id, "platform")
    # The insert names no configuration columns; defaults apply durably.
    insert_columns = sql.split("(", 1)[1].split(")", 1)[0]
    assert "review_iteration_limit" not in insert_columns
    assert "guidance" not in insert_columns


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
    # Phase 1 rows carry no installation route: valid historical state, not
    # usable for GitHub operations until explicitly routed (issue #57).
    assert created.github_installation_id is None
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


def test_set_repository_installation_route_maps_the_route_write() -> None:
    row = _repository_row()
    installation_id = uuid.uuid4()
    routed_row = (*row[:11], installation_id)
    conn = FakeConnection(row=routed_row)

    updated = set_repository_installation_route(
        cast(DatabasePool, FakePool(conn)),
        repository_id=row[0],
        workspace_id=row[2],
        github_installation_id=installation_id,
    )

    assert updated is not None
    assert updated.id == row[0]
    assert updated.github_installation_id == installation_id
    sql, params = conn.executed[0]
    assert (
        "update openorc.repositories set github_installation_id = %s, updated_at = now() "
        "where id = %s and workspace_id = %s" in sql
    )
    assert params == (installation_id, row[0], row[2])

    # Clearing the route represents loss of configuration, not deletion.
    cleared = set_repository_installation_route(
        cast(DatabasePool, FakePool(FakeConnection(row=row))),
        repository_id=row[0],
        workspace_id=row[2],
        github_installation_id=None,
    )
    assert cleared is not None
    assert cleared.github_installation_id is None


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


def _workspace_row(
    workspace_id: uuid.UUID,
    owner_profile_id: uuid.UUID,
    *,
    review_iteration_limit: int = 5,
    guidance: str = "",
) -> tuple[Any, ...]:
    observed = _observed_at()
    return (
        workspace_id,
        owner_profile_id,
        "platform",
        observed,
        observed,
        review_iteration_limit,
        guidance,
    )


def _workspace_from_row_values(row: tuple[Any, ...]) -> Workspace:
    return Workspace(
        id=row[0],
        owner_profile_id=row[1],
        name=row[2],
        created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        review_iteration_limit=row[5],
        guidance=row[6],
    )


def test_get_workspace_maps_row_or_empty_result() -> None:
    workspace_id = uuid.uuid4()
    owner_profile_id = uuid.uuid4()
    row = _workspace_row(workspace_id, owner_profile_id, review_iteration_limit=7, guidance="prose")
    conn = FakeConnection(row=row)

    workspace = get_workspace(cast(DatabasePool, FakePool(conn)), workspace_id)

    assert workspace == Workspace(
        id=workspace_id,
        owner_profile_id=owner_profile_id,
        name="platform",
        created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        review_iteration_limit=7,
        guidance="prose",
    )
    sql, params = conn.executed[0]
    assert "from openorc.workspaces where id = %s" in sql
    assert params == (workspace_id,)

    empty = get_workspace(cast(DatabasePool, FakePool(FakeConnection(row=None))), workspace_id)
    assert empty is None


def test_get_project_maps_row_or_empty_result() -> None:
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    observed = _observed_at()
    conn = FakeConnection(row=(project_id, workspace_id, "project", observed, observed))

    project = get_project(cast(DatabasePool, FakePool(conn)), project_id)

    assert project == Project(
        id=project_id,
        workspace_id=workspace_id,
        name="project",
        created_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC),
    )
    sql, params = conn.executed[0]
    assert "from openorc.projects where id = %s" in sql
    assert params == (project_id,)

    empty = get_project(cast(DatabasePool, FakePool(FakeConnection(row=None))), project_id)
    assert empty is None


def test_update_workspace_review_iteration_limit_writes_only_on_change() -> None:
    workspace_id = uuid.uuid4()
    owner_profile_id = uuid.uuid4()
    old_row = _workspace_row(workspace_id, owner_profile_id, review_iteration_limit=5)
    new_row = _workspace_row(workspace_id, owner_profile_id, review_iteration_limit=7)
    conn = ScriptedProfileConnection([old_row, new_row])

    result = update_workspace_review_iteration_limit(
        cast(DatabasePool, FakePool(conn)), workspace_id, review_iteration_limit=7
    )
    assert result is not None
    workspace, previous, changed = result

    assert changed is True
    assert previous == 5
    assert workspace.review_iteration_limit == 7
    select_sql, select_params = conn.executed[0]
    assert "for update" in select_sql
    assert select_params == (workspace_id,)
    update_sql, update_params = conn.executed[1]
    assert "update openorc.workspaces" in update_sql
    assert "review_iteration_limit = %s" in update_sql
    assert "updated_at = now()" in update_sql
    assert update_params == (7, workspace_id)

    # Same-value write: no update statement, unchanged row, changed=False.
    same_conn = ScriptedProfileConnection([old_row])
    same_result = update_workspace_review_iteration_limit(
        cast(DatabasePool, FakePool(same_conn)), workspace_id, review_iteration_limit=5
    )
    assert same_result is not None
    workspace, previous, changed = same_result
    assert changed is False
    assert previous == 5
    assert workspace == _workspace_from_row_values(old_row)
    assert len(same_conn.executed) == 1

    missing_conn = ScriptedProfileConnection([None])
    assert (
        update_workspace_review_iteration_limit(
            cast(DatabasePool, FakePool(missing_conn)), workspace_id, review_iteration_limit=7
        )
        is None
    )


def test_update_workspace_guidance_replaces_the_current_value() -> None:
    workspace_id = uuid.uuid4()
    owner_profile_id = uuid.uuid4()
    prose = "Review findings carefully.\nΟἶναι νόμοι.\n✔ done"
    blank_row = _workspace_row(workspace_id, owner_profile_id, guidance="")
    prose_row = _workspace_row(workspace_id, owner_profile_id, guidance=prose)
    conn = ScriptedProfileConnection([blank_row, prose_row])

    guidance_result = update_workspace_guidance(
        cast(DatabasePool, FakePool(conn)), workspace_id, guidance=prose
    )
    assert guidance_result is not None
    workspace, previous, changed = guidance_result

    assert changed is True
    assert previous == ""
    assert workspace.guidance == prose  # arbitrary Owner-authored prose, verbatim

    update_sql, update_params = conn.executed[1]
    assert "update openorc.workspaces" in update_sql
    assert "guidance = %s" in update_sql
    assert "updated_at = now()" in update_sql
    assert update_params == (prose, workspace_id)

    # Reset to blank is itself a change; the previous prose is the before fact.
    reset_conn = ScriptedProfileConnection([prose_row, blank_row])
    reset_result = update_workspace_guidance(
        cast(DatabasePool, FakePool(reset_conn)), workspace_id, guidance=""
    )
    assert reset_result is not None
    _, previous, changed = reset_result
    assert changed is True
    assert previous == prose

    # Same-value write: no-op with changed=False and no UPDATE statement.
    noop_conn = ScriptedProfileConnection([prose_row])
    noop_result = update_workspace_guidance(
        cast(DatabasePool, FakePool(noop_conn)), workspace_id, guidance=prose
    )
    assert noop_result is not None
    _, previous, changed = noop_result
    assert changed is False
    assert previous == prose
    assert len(noop_conn.executed) == 1
