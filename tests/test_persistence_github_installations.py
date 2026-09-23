"""Deterministic mapping tests for GitHub installation repositories (issue #57).

The ordinary suite cannot execute Postgres; these tests use canned rows and a
fake pool/connection seam (mirroring the transaction-boundary fakes) to prove
row-to-domain-object mapping, UTC normalization at the persistence boundary,
parameterization, the reconcile-upsert shape, and empty-result handling.
Database constraint behavior is proven against a real database by the
integration-marked suite.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationAccount,
    GitHubInstallationIdentity,
)
from openorc.persistence.github_installations import (
    create_or_reconcile_github_installation,
    delete_github_installation,
    get_github_installation,
    list_workspace_installations,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import NaiveDatetimeError

_OBSERVED_AT = datetime(2026, 9, 23, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
_UTC_OBSERVED = datetime(2026, 9, 23, 10, 0, 0, tzinfo=UTC)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [] if self._row is None else [self._row]


class FakeConnection:
    def __init__(self, row: tuple[Any, ...] | None = None) -> None:
        self.row = row
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.row)


class FakePool:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("github installation mapping tests never close pools")


def _pool(conn: FakeConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _installation_row(
    installation_id: uuid.UUID | None = None,
    workspace_id: uuid.UUID | None = None,
    suspended_at: datetime | None = None,
) -> tuple[Any, ...]:
    return (
        installation_id or uuid.uuid4(),
        workspace_id or uuid.uuid4(),
        12345678,
        501,
        "octocat",
        "Organization",
        suspended_at,
        _OBSERVED_AT,
        _OBSERVED_AT,
    )


def _identity() -> GitHubInstallationIdentity:
    return GitHubInstallationIdentity(github_installation_id=12345678)


def _account() -> GitHubInstallationAccount:
    return GitHubInstallationAccount(github_account_id=501, login="octocat", type="Organization")


def test_row_mapping_normalizes_instants_to_utc() -> None:
    installation_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    row = _installation_row(installation_id, workspace_id, suspended_at=_OBSERVED_AT)

    installation = get_github_installation(_pool(FakeConnection(row=row)), installation_id)

    assert installation == GitHubInstallation(
        id=installation_id,
        workspace_id=workspace_id,
        identity=GitHubInstallationIdentity(github_installation_id=12345678),
        account=GitHubInstallationAccount(
            github_account_id=501, login="octocat", type="Organization"
        ),
        suspended_at=_UTC_OBSERVED,
        created_at=_UTC_OBSERVED,
        updated_at=_UTC_OBSERVED,
    )
    assert installation is not None
    assert installation.suspended_at is not None
    assert installation.suspended_at.utcoffset() == timedelta(0)
    assert installation.created_at.utcoffset() == timedelta(0)


def test_get_github_installation_maps_empty_result() -> None:
    assert get_github_installation(_pool(FakeConnection(row=None)), uuid.uuid4()) is None


def test_create_or_reconcile_maps_the_upsert_and_parameters() -> None:
    workspace_id = uuid.uuid4()
    conn = FakeConnection(row=_installation_row(workspace_id=workspace_id))

    installation = create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_id,
        identity=_identity(),
        account=_account(),
        suspended_at=_UTC_OBSERVED,
    )

    assert installation.identity == _identity()
    sql, params = conn.executed[0]
    assert "insert into openorc.github_installations" in sql
    assert "on conflict (workspace_id, github_installation_id) do update" in sql
    # Reconciliation never rewrites the durable record identity: the update
    # set names observation columns only (word-boundary matched so
    # "github_account_id = excluded" cannot satisfy an "id" probe).
    assert not re.search(r"\bid = excluded\b", sql)
    assert not re.search(r"\bworkspace_id = excluded\b", sql)
    assert not re.search(r"\bgithub_installation_id = excluded\b", sql)
    # The GitHub account ID is a stable external identifier, not an
    # observation: reconciliation preserves the stored value.
    assert not re.search(r"\bgithub_account_id = excluded\b", sql)
    assert "updated_at = now()" in sql
    assert params == (workspace_id, 12345678, 501, "octocat", "Organization", _UTC_OBSERVED)


def test_list_workspace_installations_scopes_to_the_workspace() -> None:
    workspace_id = uuid.uuid4()
    conn = FakeConnection(row=_installation_row(workspace_id=workspace_id))

    installations = list_workspace_installations(_pool(conn), workspace_id=workspace_id)

    assert len(installations) == 1
    sql, params = conn.executed[0]
    assert "where workspace_id = %s order by created_at, id" in sql
    assert params == (workspace_id,)


def test_delete_github_installation_maps_the_row_and_forces_the_route_constraint() -> None:
    installation_id = uuid.uuid4()
    conn = FakeConnection(row=_installation_row(installation_id))

    deleted = delete_github_installation(_pool(conn), installation_id)

    assert deleted is not None
    assert deleted.id == installation_id
    delete_sql, delete_params = conn.executed[0]
    assert "delete from openorc.github_installations where id = %s" in delete_sql
    assert delete_params == (installation_id,)
    assert conn.executed[1][0] == (
        "set constraints openorc.repositories_github_installation_id_workspace_id_fkey immediate"
    )


def test_delete_github_installation_returns_none_without_forcing_constraints() -> None:
    conn = FakeConnection(row=None)

    assert delete_github_installation(_pool(conn), uuid.uuid4()) is None
    assert len(conn.executed) == 1
    assert "set constraints" not in conn.executed[0][0]


def test_reconcile_normalizes_a_non_utc_suspended_instant_to_utc() -> None:
    workspace_id = uuid.uuid4()
    conn = FakeConnection(row=_installation_row(workspace_id=workspace_id))

    create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_id,
        identity=_identity(),
        account=_account(),
        suspended_at=_OBSERVED_AT,
    )

    _, params = conn.executed[0]
    assert params is not None
    # The same instant, expressed in UTC: no session-timezone semantics may
    # apply at the TIMESTAMPTZ boundary.
    assert params[5] == _UTC_OBSERVED


def test_reconcile_rejects_a_naive_suspended_instant_at_the_boundary() -> None:
    naive = datetime(2026, 9, 23, 12, 0, 0)

    with pytest.raises(NaiveDatetimeError):
        create_or_reconcile_github_installation(
            _pool(FakeConnection(row=None)),
            workspace_id=uuid.uuid4(),
            identity=_identity(),
            account=_account(),
            suspended_at=naive,
        )
