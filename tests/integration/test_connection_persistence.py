"""Integration-marked persistence tests for Connection/role bindings (issue #20).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
Connection/WorkflowRoleBinding invariants directly: Workspace ownership,
scope-consistent bindings, one binding per role (with mutable repointing),
Producer/Reviewer sharing or separating Connections, Connection-scoped
Owner-configured capacity, and nullable opaque runtime-reported provider/model
observations. They are excluded from the ordinary deterministic baseline by
the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration tests/integration/test_connection_persistence.py

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

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation
from psycopg.types.json import Jsonb

from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.persistence import connections as connection_repositories
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("OPENORC_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("OPENORC_TEST_DATABASE_URL is not configured")
    return url


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Reset the openorc schema and apply all committed migrations from scratch."""
    with connect(database_url) as conn:
        conn.execute("drop schema if exists openorc cascade")
        for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            # Committed migration files are trusted repository content applied
            # wholesale; LiteralString is the driver's injection-safe query
            # contract, satisfied here by repository-controlled file text.
            migration_sql = cast(LiteralString, migration_path.read_text(encoding="utf-8"))
            conn.execute(migration_sql)
        conn.commit()
    return database_url


@pytest.fixture
def conn(migrated_database: str) -> Iterator[Connection[Any]]:
    """One connection per test; each test runs inside one rolled-back transaction."""
    with connect(migrated_database) as connection:
        yield connection
        connection.rollback()


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(conn: Connection[Any], owner_profile_id: uuid.UUID) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, "workspace"),
    )
    return workspace_id


def _insert_connection(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    name: str = "primary cline hub",
    session_capacity: int = 1,
    enabled: bool = True,
    safe_config: dict[str, Any] | None = None,
    auth_reference: str | None = None,
    reported_provider: str | None = None,
    reported_model: str | None = None,
) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections "
        "(id, workspace_id, adapter_type, name, safe_config, session_capacity, enabled, "
        "auth_reference, reported_provider, reported_model) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            connection_id,
            workspace_id,
            "cline",
            name,
            Jsonb({} if safe_config is None else safe_config),
            session_capacity,
            enabled,
            auth_reference,
            reported_provider,
            reported_model,
        ),
    )
    return connection_id


def _insert_binding(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    role: str,
    connection_id: uuid.UUID,
) -> uuid.UUID:
    binding_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workflow_role_bindings (id, workspace_id, role, connection_id) "
        "values (%s, %s, %s, %s)",
        (binding_id, workspace_id, role, connection_id),
    )
    return binding_id


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


def test_connection_requires_existing_workspace(conn: Connection[Any]) -> None:
    with pytest.raises(ForeignKeyViolation):
        _insert_connection(conn, workspace_id=uuid.uuid4())


def test_connection_defaults_encode_owner_admission_and_eligibility(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    row = conn.execute(
        "insert into openorc.connections (workspace_id, adapter_type, name) "
        "values (%s, %s, %s) returning session_capacity, enabled, safe_config, auth_reference",
        (workspace_id, "cline", "defaulted connection"),
    ).fetchone()

    assert row is not None
    session_capacity, enabled, safe_config, auth_reference = row
    # Default 1: concurrency must be explicitly enabled by the Owner.
    assert session_capacity == 1
    # Owner-controlled eligibility defaults to permitted; it never encodes
    # runtime reachability or health.
    assert enabled is True
    assert safe_config == {}
    # NULL auth_reference: no OpenOrc-owned auth reference is configured.
    assert auth_reference is None


def test_session_capacity_must_be_positive(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    with pytest.raises(CheckViolation):
        _insert_connection(conn, workspace_id=workspace_id, session_capacity=0)


def test_adapter_type_check_rejects_unknown_runtime(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    with pytest.raises(CheckViolation):
        conn.execute(
            "insert into openorc.connections (workspace_id, adapter_type, name) "
            "values (%s, %s, %s)",
            (workspace_id, "unknown-runtime", "unknown runtime"),
        )


def test_role_check_rejects_unknown_role(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)

    with pytest.raises(CheckViolation):
        _insert_binding(
            conn, workspace_id=workspace_id, role="navigator", connection_id=connection_id
        )


def test_blank_connection_name_is_rejected(conn: Connection[Any]) -> None:
    # The durable check mirrors the domain's nonblank-name rule: a blank
    # (empty or whitespace-only) name can never be committed.
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    with pytest.raises(CheckViolation):
        _insert_connection(conn, workspace_id=workspace_id, name="   ")


def test_blank_auth_reference_is_rejected(conn: Connection[Any]) -> None:
    # auth_reference is NULL (none configured) or nonblank; a blank string
    # would blur the NULL sentinel and can never be committed.
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    with pytest.raises(CheckViolation):
        _insert_connection(conn, workspace_id=workspace_id, auth_reference="   ")


def test_safe_config_must_be_a_json_object(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)

    with pytest.raises(CheckViolation):
        conn.execute(
            "insert into openorc.connections "
            "(id, workspace_id, adapter_type, name, safe_config) "
            "values (%s, %s, %s, %s, '[]'::jsonb)",
            (uuid.uuid4(), workspace_id, "cline", "non-object config"),
        )


def test_binding_cannot_reference_another_workspaces_connection(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_one = _insert_workspace(conn, profile_id)
    workspace_two = _insert_workspace(conn, profile_id)
    connection_in_one = _insert_connection(conn, workspace_id=workspace_one)

    # The composite (connection_id, workspace_id) foreign key makes direct
    # scope disagreement with the bound Connection's Workspace impossible.
    with pytest.raises(ForeignKeyViolation):
        _insert_binding(
            conn,
            workspace_id=workspace_two,
            role="producer",
            connection_id=connection_in_one,
        )


def test_one_binding_per_role_per_workspace(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_one = _insert_connection(conn, workspace_id=workspace_id)
    connection_two = _insert_connection(conn, workspace_id=workspace_id)

    _insert_binding(conn, workspace_id=workspace_id, role="producer", connection_id=connection_one)

    # One configured runtime binding per role in v1: no pools, no failover.
    with pytest.raises(UniqueViolation):
        _insert_binding(
            conn, workspace_id=workspace_id, role="producer", connection_id=connection_two
        )


def test_same_role_in_another_workspace_is_independent(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_one = _insert_workspace(conn, profile_id)
    workspace_two = _insert_workspace(conn, profile_id)
    connection_one = _insert_connection(conn, workspace_id=workspace_one)
    connection_two = _insert_connection(conn, workspace_id=workspace_two)

    producer_one = _insert_binding(
        conn, workspace_id=workspace_one, role="producer", connection_id=connection_one
    )
    producer_two = _insert_binding(
        conn, workspace_id=workspace_two, role="producer", connection_id=connection_two
    )
    assert producer_one != producer_two


def test_producer_and_reviewer_may_share_one_connection(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)

    producer_id = _insert_binding(
        conn, workspace_id=workspace_id, role="producer", connection_id=connection_id
    )
    reviewer_id = _insert_binding(
        conn, workspace_id=workspace_id, role="reviewer", connection_id=connection_id
    )

    assert producer_id != reviewer_id
    rows = conn.execute(
        "select role from openorc.workflow_role_bindings where connection_id = %s order by role",
        (connection_id,),
    ).fetchall()
    assert [row[0] for row in rows] == ["producer", "reviewer"]


def test_producer_and_reviewer_may_use_separate_connections(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    producer_connection = _insert_connection(conn, workspace_id=workspace_id)
    reviewer_connection = _insert_connection(conn, workspace_id=workspace_id)

    _insert_binding(
        conn, workspace_id=workspace_id, role="producer", connection_id=producer_connection
    )
    _insert_binding(
        conn, workspace_id=workspace_id, role="reviewer", connection_id=reviewer_connection
    )

    rows = conn.execute(
        "select connection_id from openorc.workflow_role_bindings where workspace_id = %s "
        "order by role",
        (workspace_id,),
    ).fetchall()
    assert rows[0][0] != rows[1][0]
    assert {rows[0][0], rows[1][0]} == {producer_connection, reviewer_connection}


def test_capacity_is_connection_scoped_and_owner_configured(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id, session_capacity=4)

    # Capacity lives on the Connection; the binding tables carry none.
    tables = conn.execute(
        "select table_name from information_schema.columns "
        "where table_schema = 'openorc' and column_name = 'session_capacity'"
    ).fetchall()
    assert [table[0] for table in tables] == ["connections"]

    row = conn.execute(
        "select session_capacity from openorc.connections where id = %s", (connection_id,)
    ).fetchone()
    assert row is not None
    # Owner-configured admission control, scoped to the Connection — never
    # discovered from the runtime.
    assert row[0] == 4


def test_reported_provider_model_accept_arbitrary_strings_and_null(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(
        conn,
        workspace_id=workspace_id,
        reported_provider="whatever-the-runtime-reported",
        reported_model="opaque-token-with-ünicode-⚡",
    )

    row = conn.execute(
        "select reported_provider, reported_model from openorc.connections where id = %s",
        (connection_id,),
    ).fetchone()
    assert row is not None
    # Runtime-reported observations accept arbitrary strings; they are never
    # configuration authority and never enum-constrained.
    assert row[0] == "whatever-the-runtime-reported"
    assert row[1] == "opaque-token-with-ünicode-⚡"

    conn.execute(
        "update openorc.connections set reported_provider = null, reported_model = null "
        "where id = %s",
        (connection_id,),
    )
    cleared = conn.execute(
        "select reported_provider, reported_model from openorc.connections where id = %s",
        (connection_id,),
    ).fetchone()
    assert cleared is not None
    assert cleared[0] is None
    assert cleared[1] is None


def test_repository_upsert_repoints_binding_and_preserves_identity(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_one = _insert_connection(conn, workspace_id=workspace_id)
    connection_two = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    original = connection_repositories.set_role_binding(
        pool, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_one
    )
    repointed = connection_repositories.set_role_binding(
        pool, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection_two
    )

    # A workflow role binding is mutable configuration: the same
    # (workspace, role) is repointed, keeping exactly one binding whose
    # identity and creation instant are preserved.
    assert repointed.id == original.id
    assert repointed.created_at == original.created_at
    assert repointed.connection_id == connection_two
    count = conn.execute(
        "select count(*) from openorc.workflow_role_bindings where workspace_id = %s and role = %s",
        (workspace_id, "producer"),
    ).fetchone()
    assert count is not None
    assert count[0] == 1


def test_direct_duplicate_insert_still_raises_unique_violation(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    connection_repositories.set_role_binding(
        pool, workspace_id=workspace_id, role=WorkflowRole.REVIEWER, connection_id=connection_id
    )

    # The database invariant is proven separately from repository upsert
    # behavior: a direct duplicate insert bypassing the repository raises.
    with pytest.raises(UniqueViolation):
        _insert_binding(
            conn, workspace_id=workspace_id, role="reviewer", connection_id=connection_id
        )


def test_connection_round_trip_through_persistence_layer(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    created = connection_repositories.create_connection(
        pool,
        workspace_id=workspace_id,
        adapter=AdapterType("cline"),
        name="round-trip hub",
        safe_config={"base_url": "https://hub.example.com"},
        session_capacity=2,
        enabled=True,
        auth_reference="vault://openorc/connection-auth/round-trip",
    )

    assert connection_repositories.get_connection(pool, created.id) == created
    assert connection_repositories.list_workspace_connections(pool, workspace_id=workspace_id) == [
        created
    ]

    updated = connection_repositories.update_connection(
        pool,
        created.id,
        name="renamed hub",
        safe_config={},
        session_capacity=5,
        enabled=False,
        auth_reference=None,
    )
    assert updated is not None
    assert updated.id == created.id
    assert updated.name == "renamed hub"
    assert updated.session_capacity == 5
    assert updated.enabled is False
    assert updated.auth_reference is None

    producer = connection_repositories.set_role_binding(
        pool, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=created.id
    )
    reviewer = connection_repositories.set_role_binding(
        pool, workspace_id=workspace_id, role=WorkflowRole.REVIEWER, connection_id=created.id
    )
    assert (
        connection_repositories.get_role_binding(
            pool, workspace_id=workspace_id, role=WorkflowRole.PRODUCER
        )
        == producer
    )
    assert producer != reviewer
    bindings = connection_repositories.list_connection_bindings(pool, connection_id=created.id)
    assert {binding.role for binding in bindings} == {WorkflowRole.PRODUCER, WorkflowRole.REVIEWER}

    # Instants crossing the boundary are timezone-aware and UTC-normalized.
    created_at = created.created_at
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is not None
    assert created_at.utcoffset() == timedelta(0)
