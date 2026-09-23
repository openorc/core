"""Integration-marked persistence tests for GitHub installation routing (issue #57).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
installation-routing invariants directly: per-Workspace external installation
uniqueness with independent cross-Workspace representation, durable CHECK
behavior, the nullable legacy route on Phase 1 Repository rows, the composite
route foreign key that makes cross-Workspace routing unrepresentable, the
restrictive deletion boundary for routed installation records, and Workspace
aggregate deletion. They are excluded from the ordinary deterministic baseline
by the repository pytest configuration.

The suite consumes the database it is given and never provisions one;
provisioning and teardown sit outside the test suite and outside agent
responsibility. No external GitHub call is involved anywhere: this leaf is
pure OpenOrc-database work.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation

from openorc.domain.github_installations import (
    GitHubInstallationAccount,
    GitHubInstallationIdentity,
)
from openorc.domain.ownership import GitHubRepositoryIdentity, RepositoryMetadata
from openorc.persistence import deletion as deletion_repositories
from openorc.persistence import github_installations as installation_repositories
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import github_installations as installation_services
from openorc.services.errors import NotFoundError

pytestmark = pytest.mark.integration

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)


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


def test_installation_record_round_trip_and_reconcile(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    created = installation_repositories.create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_id,
        identity=GitHubInstallationIdentity(github_installation_id=12345678),
        account=GitHubInstallationAccount(
            github_account_id=501, login="octocat", type="Organization"
        ),
        suspended_at=None,
    )
    assert created.identity.github_installation_id == 12345678
    assert created.account.login == "octocat"
    assert created.suspended_at is None
    assert created.created_at.utcoffset() is not None

    # Reconciliation persists the reported facts as observations and never
    # replaces the durable record identity.
    reconciled = installation_repositories.create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_id,
        identity=GitHubInstallationIdentity(github_installation_id=12345678),
        account=GitHubInstallationAccount(github_account_id=501, login="renamed", type="User"),
        suspended_at=_OBSERVED,
    )
    assert reconciled.id == created.id
    assert reconciled.created_at == created.created_at
    assert reconciled.account.login == "renamed"
    assert reconciled.account.type == "User"
    assert reconciled.suspended_at == _OBSERVED

    loaded = installation_repositories.get_github_installation(_pool(conn), created.id)
    assert loaded is not None and loaded.id == created.id


def test_a_workspace_holds_several_installations_and_workspaces_stay_independent(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_a = _insert_workspace(conn, profile_id, name="a")
    workspace_b = _insert_workspace(conn, profile_id, name="b")

    first = installation_repositories.create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_a,
        identity=GitHubInstallationIdentity(github_installation_id=1001),
        account=GitHubInstallationAccount(github_account_id=1, login="org-a", type="Organization"),
        suspended_at=None,
    )
    second = installation_repositories.create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_a,
        identity=GitHubInstallationIdentity(github_installation_id=2002),
        account=GitHubInstallationAccount(github_account_id=2, login="org-b", type="Organization"),
        suspended_at=None,
    )
    assert first.id != second.id

    # The same external installation may be represented independently in
    # another Workspace — Workspace isolation stays explicit.
    mirrored = installation_repositories.create_or_reconcile_github_installation(
        _pool(conn),
        workspace_id=workspace_b,
        identity=GitHubInstallationIdentity(github_installation_id=1001),
        account=GitHubInstallationAccount(github_account_id=1, login="org-a", type="Organization"),
        suspended_at=None,
    )
    assert mirrored.id != first.id
    assert (
        len(
            installation_repositories.list_workspace_installations(
                _pool(conn), workspace_id=workspace_a
            )
        )
        == 2
    )
    assert (
        len(
            installation_repositories.list_workspace_installations(
                _pool(conn), workspace_id=workspace_b
            )
        )
        == 1
    )

    # The durable uniqueness backstop: a direct duplicate insert is rejected.
    with pytest.raises(UniqueViolation):
        conn.execute(
            "insert into openorc.github_installations "
            "(workspace_id, github_installation_id, github_account_id, "
            "account_login, account_type) "
            "values (%s, %s, %s, %s, %s)",
            (workspace_a, 1001, 1, "org-a", "Organization"),
        )


def _insert_installation(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    github_installation_id: int,
    github_account_id: int = 501,
    account_login: str = "octocat",
    account_type: str = "Organization",
    suspended_at: Any = None,
) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, account_login, "
        "account_type, suspended_at) values (%s, %s, %s, %s, %s, %s, %s)",
        (
            installation_id,
            workspace_id,
            github_installation_id,
            github_account_id,
            account_login,
            account_type,
            suspended_at,
        ),
    )
    return installation_id


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction.

    Repository calls run inside a nested psycopg transaction (a SAVEPOINT on
    the already-active per-test transaction): an expected constraint violation
    rolls back only to it — the per-test transaction stays valid so the
    remaining assertions run instead of failing with ``InFailedSqlTransaction``.
    """

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            with self._connection.transaction():
                yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("github installation tests never close pools")


def _pool(conn: Connection[Any]) -> DatabasePool:
    return cast(DatabasePool, _SingleConnectionPool(conn))


def _assert_settled(conn: Connection[Any]) -> None:
    """Force every deferred foreign key to be checked now, inside the test
    transaction: proves the operations left no pending referential violation
    (the same check the commit boundary would perform)."""
    conn.execute("set constraints all immediate")


def test_durable_checks_reject_invalid_installation_facts(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    for bad_insert in (
        (0, 501, "octocat", "Organization"),
        (12345678, 0, "octocat", "Organization"),
        (12345678, 501, "   ", "Organization"),
        (12345678, 501, "octocat", ""),
    ):
        with conn.transaction(), pytest.raises(CheckViolation):
            conn.execute(
                "insert into openorc.github_installations "
                "(workspace_id, github_installation_id, github_account_id, "
                "account_login, account_type) values (%s, %s, %s, %s, %s)",
                (workspace_id, *bad_insert),
            )


def test_legacy_repository_rows_carry_no_route(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)

    created = ownership_repositories.create_repository(
        _pool(conn),
        project_id=project_id,
        workspace_id=workspace_id,
        identity=GitHubRepositoryIdentity(github_repository_id=987654321),
        metadata=RepositoryMetadata(
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
    )

    # Phase 1 rows predate installation persistence: valid historical state,
    # not usable for GitHub operations until explicitly routed.
    assert created.github_installation_id is None


def test_route_service_round_trip_fails_closed_and_blocks_routed_deletion(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn, project_id=project_id, workspace_id=workspace_id, github_repository_id=987654321
    )

    installation = installation_services.record_workspace_installation(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        github_installation_id=12345678,
        github_account_id=501,
        account_login="octocat",
        account_type="Organization",
        suspended_at=None,
    )

    # The unconfigured legacy row fails closed before binding.
    with pytest.raises(NotFoundError):
        installation_services.require_configured_repository_installation_route(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )

    bound = installation_services.bind_repository_installation(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_installation_id=installation.id,
    )
    assert bound.github_installation_id == installation.id

    resolved_repository, resolved_installation = (
        installation_services.require_configured_repository_installation_route(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
    )
    assert resolved_repository.id == repository_id
    assert resolved_installation.id == installation.id

    # Hard deletion of the routed installation record is rejected before the
    # operation returns: the route foreign key never cascades away.
    with pytest.raises(ForeignKeyViolation):
        installation_repositories.delete_github_installation(_pool(conn), installation.id)
    assert ownership_repositories.get_repository(_pool(conn), repository_id) is not None

    # Unbinding is the explicit loss-of-route operation; deleting the record
    # afterwards is then possible and never silently un-routes anything.
    unbound = installation_services.unbind_repository_installation(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
    )
    assert unbound.github_installation_id is None
    deleted = installation_repositories.delete_github_installation(_pool(conn), installation.id)
    assert deleted is not None and deleted.id == installation.id
    assert ownership_repositories.get_repository(_pool(conn), repository_id) is not None


def test_cross_workspace_route_is_unrepresentable(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_a = _insert_workspace(conn, profile_id, name="a")
    workspace_b = _insert_workspace(conn, profile_id, name="b")
    project_a = _insert_project(conn, workspace_a)
    repository_a = _insert_repository(
        conn, project_id=project_a, workspace_id=workspace_a, github_repository_id=987654321
    )
    installation_b = _insert_installation(
        conn, workspace_id=workspace_b, github_installation_id=777
    )

    # Force the deferred route constraint immediate inside the test
    # transaction, then prove the composite foreign key rejects a route into
    # another Workspace at the database level.
    conn.execute(
        "set constraints openorc.repositories_github_installation_id_workspace_id_fkey immediate"
    )
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        conn.execute(
            "update openorc.repositories set github_installation_id = %s where id = %s",
            (installation_b, repository_a),
        )
    _assert_settled(conn)


def test_workspace_aggregate_deletion_removes_routed_repositories_and_installations(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn, project_id=project_id, workspace_id=workspace_id, github_repository_id=987654321
    )
    installation_id = _insert_installation(
        conn, workspace_id=workspace_id, github_installation_id=555
    )
    conn.execute(
        "update openorc.repositories set github_installation_id = %s where id = %s",
        (installation_id, repository_id),
    )

    deleted = deletion_repositories.delete_workspace(_pool(conn), workspace_id)

    assert deleted is not None and deleted.id == workspace_id
    _assert_settled(conn)
    installations_row = conn.execute(
        "select count(*) from openorc.github_installations where workspace_id = %s",
        (workspace_id,),
    ).fetchone()
    repositories_row = conn.execute(
        "select count(*) from openorc.repositories where workspace_id = %s",
        (workspace_id,),
    ).fetchone()
    assert installations_row is not None and installations_row[0] == 0
    assert repositories_row is not None and repositories_row[0] == 0
