"""Deterministic tests for the GitHub installation services (issue #57).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the ownership-gated configuration and resolution flows —
guarded create/reconcile, bind/unbind, uniform anti-probing not-found
outcomes, fail-closed resolution of an unconfigured legacy Repository, and
observation-independent route resolution. Database defaults/constraints and
the real all-or-nothing rollback are proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from psycopg.errors import ForeignKeyViolation

from openorc.services import github_installations
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of every Owner-mutating flow; the scripted result row
# for an operational account carries the all-NULL attempt-state tuple.
_GUARD_OPERATIONAL_ROW = (None, None, None)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("github installation service tests never close pools")


def _pool(conn: ScriptedConnection) -> Any:
    return FakePool(conn)


def _ws_row(workspace_id: Any, profile_id: Any) -> tuple[Any, ...]:
    return (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")


def _installation_row(
    installation_id: Any,
    workspace_id: Any,
    suspended_at: datetime | None = None,
) -> tuple[Any, ...]:
    return (
        installation_id,
        workspace_id,
        12345678,
        501,
        "octocat",
        "Organization",
        suspended_at,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_row(
    repository_id: Any,
    project_id: Any,
    workspace_id: Any,
    route: Any = None,
) -> tuple[Any, ...]:
    return (
        repository_id,
        project_id,
        workspace_id,
        987654321,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        route,
    )


def test_record_workspace_installation_rejects_invalid_facts_before_any_database_work() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        github_installations.record_workspace_installation(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            github_installation_id=0,
            github_account_id=501,
            account_login="octocat",
            account_type="Organization",
            suspended_at=None,
        )
    assert conn.executed == []

    with pytest.raises(InvalidCommandError):
        github_installations.record_workspace_installation(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            github_installation_id=12345678,
            github_account_id=501,
            account_login="   ",
            account_type="Organization",
            suspended_at=None,
        )
    assert conn.executed == []

    with pytest.raises(InvalidCommandError):
        github_installations.record_workspace_installation(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            github_installation_id=12345678,
            github_account_id=501,
            account_login="octocat",
            account_type="Organization",
            suspended_at="soon",  # type: ignore[arg-type]
        )
    assert conn.executed == []


def test_record_workspace_installation_reconciles_through_the_guarded_composition() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _installation_row(installation_id, workspace_id),
        ]
    )

    installation = github_installations.record_workspace_installation(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        github_installation_id=12345678,
        github_account_id=501,
        account_login="octocat",
        account_type="Organization",
        suspended_at=None,
    )

    assert installation.id == installation_id
    assert installation.identity.github_installation_id == 12345678
    assert installation.account.login == "octocat"
    assert len(conn.executed) == 3
    guard_sql, guard_params = conn.executed[0]
    assert "for key share" in guard_sql
    assert guard_params == (profile_id,)
    ws_sql, ws_params = conn.executed[1]
    assert "from openorc.workspaces where id = %s" in ws_sql
    assert ws_params == (workspace_id,)
    upsert_sql, upsert_params = conn.executed[2]
    assert "insert into openorc.github_installations" in upsert_sql
    assert "on conflict (workspace_id, github_installation_id) do update" in upsert_sql
    assert upsert_params == (workspace_id, 12345678, 501, "octocat", "Organization", None)


def test_record_workspace_installation_fails_closed_for_a_foreign_workspace() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, None])

    with pytest.raises(NotFoundError):
        github_installations.record_workspace_installation(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            github_installation_id=12345678,
            github_account_id=501,
            account_login="octocat",
            account_type="Organization",
            suspended_at=None,
        )
    assert len(conn.executed) == 2
    assert all("insert" not in sql for sql, _ in conn.executed)


def test_bind_sets_the_route_through_the_guarded_composition() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    project_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, project_id, workspace_id),
            _installation_row(installation_id, workspace_id),
            _repository_row(repository_id, project_id, workspace_id, route=installation_id),
        ]
    )

    repository = github_installations.bind_repository_installation(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_installation_id=installation_id,
    )

    assert repository.github_installation_id == installation_id
    assert repository.identity.github_repository_id == 987654321
    assert len(conn.executed) == 5
    update_sql, update_params = conn.executed[4]
    assert (
        "update openorc.repositories set github_installation_id = %s, updated_at = now() "
        "where id = %s and workspace_id = %s" in update_sql
    )
    assert update_params == (installation_id, repository_id, workspace_id)
    # The stable repository identity is never part of the route write: only
    # the SET clause is the mutation (RETURNING legitimately names columns).
    set_clause = update_sql.split("set", 1)[1].split("where", 1)[0]
    assert "github_repository_id" not in set_clause
    assert "github_installation_id = %s, updated_at = now()" in set_clause


def test_bind_rejects_an_installation_of_another_workspace_without_writing() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id),
            # The installation record belongs to another Workspace.
            _installation_row(installation_id, uuid.uuid4()),
        ]
    )

    with pytest.raises(NotFoundError):
        github_installations.bind_repository_installation(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_installation_id=installation_id,
        )
    assert len(conn.executed) == 4
    assert all("update openorc.repositories" not in sql for sql, _ in conn.executed)


def test_bind_translates_a_route_fk_violation_into_the_typed_conflict() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id),
            _installation_row(installation_id, workspace_id),
            ForeignKeyViolation(),
        ]
    )

    with pytest.raises(ConflictError):
        github_installations.bind_repository_installation(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_installation_id=installation_id,
        )


def test_unbind_clears_the_route_without_deleting_identity() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, route=installation_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, route=None),
        ]
    )

    repository = github_installations.unbind_repository_installation(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
    )

    assert repository.github_installation_id is None
    assert repository.identity.github_repository_id == 987654321
    update_sql, update_params = conn.executed[3]
    assert "update openorc.repositories set github_installation_id = %s" in update_sql
    assert update_params == (None, repository_id, workspace_id)


def test_an_unconfigured_repository_fails_closed_on_resolution() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id),
        ]
    )

    with pytest.raises(NotFoundError):
        github_installations.require_configured_repository_installation_route(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
    assert len(conn.executed) == 2
    assert all("github_installations" not in sql for sql, _ in conn.executed)


def test_resolution_returns_the_exact_route_independently_of_observed_state() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, route=installation_id),
            # A suspended observation is carried but never consulted: the
            # configured route resolves regardless of observed state, and
            # deciding current usability/access is later reconciliation work.
            _installation_row(installation_id, workspace_id, suspended_at=_OBSERVED),
        ]
    )

    repository, installation = (
        github_installations.require_configured_repository_installation_route(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
    )

    assert repository.github_installation_id == installation_id
    assert installation.id == installation_id
    assert installation.suspended_at == _OBSERVED
    assert len(conn.executed) == 3


def test_resolution_is_uniformly_not_found_for_foreign_or_missing_workspaces() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()

    missing = ScriptedConnection([None])
    with pytest.raises(NotFoundError):
        github_installations.require_configured_repository_installation_route(
            _pool(missing),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
        )
    assert len(missing.executed) == 1

    foreign = ScriptedConnection([_ws_row(workspace_id, uuid.uuid4())])
    with pytest.raises(NotFoundError):
        github_installations.require_configured_repository_installation_route(
            _pool(foreign),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
        )
    assert len(foreign.executed) == 1
