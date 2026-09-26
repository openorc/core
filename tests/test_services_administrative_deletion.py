"""Deterministic tests for the administrative deletion services (issue #97).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the fail-closed authorization composition (the
account-wide Owner-mutation barrier read first, then the #53 Workspace gate),
the atomic disconnect/hard-delete/purge/aggregate-deletion flows at the SQL
level, the Workspace-root lock ordering that keeps a child Connection from
entering the aggregate after the cleanup set is established, the typed
translation of the Phase 1 restrictive primitives, and that these paths make
zero GitHub/runtime adapter calls. Durable cascade/constraint behavior is
proven against a real Supabase branch by the integration-marked suite.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from psycopg.errors import ForeignKeyViolation

from openorc.observability import CONNECTION_ID, OPERATION, WORKSPACE_ID, injected_tracer_source
from openorc.persistence import runtime_control_secrets
from openorc.persistence.pool import DatabasePool
from openorc.services import administrative_deletion
from openorc.services.errors import ConflictError, NotFoundError

_OBSERVED = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)

_CREDENTIAL = "s3cr3t-credential-value"


class FakeCursor:
    """Returns one canned row (or canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None = None):
        self._row = row
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._rows is None:
            return [] if self._row is None else [self._row]
        return self._rows


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None | list[Any] | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.transaction_depth = 0

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, list):
            return FakeCursor(None, result)
        return FakeCursor(result)  # type: ignore[arg-type]

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        self.transaction_depth += 1
        try:
            yield
        finally:
            self.transaction_depth -= 1


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("administrative deletion tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _guard_operational_row() -> tuple[Any, ...]:
    # The barrier read (issue #97) composes first in every flow: an
    # operational account carries the all-NULL attempt-state tuple.
    return (None, None, None)


def _ws_row(owner_profile_id: Any, workspace_id: Any = None) -> tuple[Any, ...]:
    return (
        workspace_id if workspace_id is not None else uuid.uuid4(),
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        "",
    )


def _connection_row(
    workspace_id: Any,
    *,
    auth_reference: str | None = None,
    enabled: bool = True,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        "cline",
        "runtime",
        {},
        1,
        enabled,
        auth_reference,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _project_row(workspace_id: Any) -> tuple[Any, ...]:
    return (uuid.uuid4(), workspace_id, "platform", _OBSERVED, _OBSERVED)


def _repository_row(workspace_id: Any, project_id: Any) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        project_id,
        workspace_id,
        424242,
        "owner",
        "repo",
        "https://github.com/owner/repo",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        None,
    )


def _task_row(workspace_id: Any, *, archived: bool) -> tuple[Any, ...]:
    archived_at = _OBSERVED if archived else None
    return (
        uuid.uuid4(),
        workspace_id,
        uuid.uuid4(),
        991001,
        42,
        "completed" if archived else "ready_to_plan",
        archived_at,
        "feature-branch",
        uuid.uuid4(),
        uuid.uuid4(),
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def test_disconnect_removes_the_vault_secret_and_disconnects_atomically() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    locked = _connection_row(workspace_id, auth_reference=reference)
    disconnected = _connection_row(workspace_id, auth_reference=None, enabled=False)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            locked,
            (1,),
            disconnected,
        ]
    )

    result = administrative_deletion.disconnect_connection_credential(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=locked[0],
    )

    assert result.auth_reference is None
    assert result.enabled is False
    # One composed transaction: barrier read, ownership gate, row-locked
    # read, Vault secret deletion, then the disconnect mutation.
    sqls = [sql for sql, _ in conn.executed]
    assert "for key share" in sqls[0]
    assert "from openorc.workspaces where id = %s" in sqls[1]
    assert "from openorc.connections" in sqls[2] and "for update" in sqls[2]
    vault_sql, vault_params = conn.executed[3]
    assert vault_sql == "delete from vault.secrets where id = %s returning 1"
    assert vault_params == (secret_id,)
    disconnect_sql, disconnect_params = conn.executed[4]
    assert "update openorc.connections" in disconnect_sql
    assert "set enabled = false, auth_reference = null" in disconnect_sql
    assert disconnect_params == (locked[0],)


def test_disconnect_without_a_credential_disconnects_without_touching_vault() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(workspace_id, auth_reference=None)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            locked,
            _connection_row(workspace_id, auth_reference=None, enabled=False),
        ]
    )

    result = administrative_deletion.disconnect_connection_credential(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=locked[0],
    )

    assert result.enabled is False
    assert all("vault" not in sql for sql, _ in conn.executed)
    assert len(conn.executed) == 4


def test_disconnect_fails_closed_on_an_unrecognized_reference() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(workspace_id, auth_reference="vault://openorc/connection-auth/abc")
    conn = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), locked])

    with pytest.raises(ConflictError):
        administrative_deletion.disconnect_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
        )

    # Nothing was written: the unreconcilable reference aborts the cleanup.
    assert len(conn.executed) == 3
    assert all("vault" not in sql for sql, _ in conn.executed)
    assert all("update openorc.connections" not in sql for sql, _ in conn.executed)


def test_disconnect_fails_closed_on_a_dangling_reference() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            locked,
            None,  # Vault delete matched zero rows: dangling pointer.
        ]
    )

    with pytest.raises(ConflictError):
        administrative_deletion.disconnect_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
        )

    assert all("update openorc.connections" not in sql for sql, _ in conn.executed)


def test_disconnect_fails_closed_for_missing_and_cross_workspace_connections() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()

    missing = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), None])
    with pytest.raises(NotFoundError):
        administrative_deletion.disconnect_connection_credential(
            _pool(missing),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=uuid.uuid4(),
        )
    assert len(missing.executed) == 3

    other = _connection_row(uuid.uuid4())
    cross = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), other])
    with pytest.raises(NotFoundError):
        administrative_deletion.disconnect_connection_credential(
            _pool(cross),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=other[0],
        )
    assert len(cross.executed) == 3


def test_hard_delete_runs_the_credential_cleanup_before_the_delete() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    locked = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(secret_id),
    )
    deleted = _connection_row(workspace_id, auth_reference=None, enabled=False)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            locked,
            (1,),
            deleted,
            None,  # the forced-immediate constraint check statement
        ]
    )

    result = administrative_deletion.delete_connection_record(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=locked[0],
    )

    assert result.id == deleted[0]
    sqls = [sql for sql, _ in conn.executed]
    vault_index = next(index for index, sql in enumerate(sqls) if "vault.secrets" in sql)
    delete_index = next(
        index for index, sql in enumerate(sqls) if "delete from openorc.connections" in sql
    )
    assert vault_index < delete_index


def test_hard_delete_rejects_a_referenced_connection_preserving_history() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(workspace_id)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            locked,
            ForeignKeyViolation("referenced"),
        ]
    )

    with pytest.raises(ConflictError, match="historical workflow state"):
        administrative_deletion.delete_connection_record(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
        )


def test_purge_archived_task_record_purges_only_an_archived_attempt() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task = _task_row(workspace_id, archived=True)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id, workspace_id),
            task,  # require_workspace_task resolution
            task,  # purge's row-locked read
            None,  # current-pointer clear
            None,  # delete
        ]
    )

    purged = administrative_deletion.purge_archived_task_record(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task[0],
    )

    assert purged.id == task[0]
    sqls = [sql for sql, _ in conn.executed]
    assert "for key share" in sqls[0]
    assert any("update openorc.tasks" in sql and "current_plan_revision_id" in sql for sql in sqls)
    assert any("delete from openorc.tasks" in sql for sql in sqls)
    # Internal-state deletion only: no GitHub/runtime statements exist.
    assert all("vault" not in sql for sql, _ in conn.executed)


def test_purge_rejects_a_current_task_as_a_conflict() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task = _task_row(workspace_id, archived=False)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id, workspace_id),
            task,
            task,  # purge's locked read sees a current (non-archived) Task
        ]
    )

    with pytest.raises(ConflictError, match="not archived"):
        administrative_deletion.purge_archived_task_record(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task[0],
        )


def test_purge_resolution_fails_closed_uniformly() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    task = _task_row(workspace_id, archived=True)

    missing = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), None])
    with pytest.raises(NotFoundError):
        administrative_deletion.purge_archived_task_record(
            _pool(missing),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task[0],
        )
    assert len(missing.executed) == 3

    other_task = _task_row(uuid.uuid4(), archived=True)
    cross = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), other_task])
    with pytest.raises(NotFoundError):
        administrative_deletion.purge_archived_task_record(
            _pool(cross),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task[0],
        )


def test_delete_repository_record_scopes_to_the_authorized_workspace() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository = _repository_row(workspace_id, uuid.uuid4())
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id, workspace_id),
            repository,
            None,  # aggregate: tasks pointer clear
            None,  # aggregate: tasks delete
            repository,  # aggregate: repository delete returning
        ]
    )

    deleted = administrative_deletion.delete_repository_record(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        repository_id=repository[0],
    )

    assert deleted.id == repository[0]
    assert all("vault" not in sql for sql, _ in conn.executed)

    other_repository = _repository_row(uuid.uuid4(), uuid.uuid4())
    cross = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), other_repository])
    with pytest.raises(NotFoundError):
        administrative_deletion.delete_repository_record(
            _pool(cross),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository[0],
        )


def test_delete_project_record_scopes_to_the_authorized_workspace() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project = _project_row(workspace_id)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id, workspace_id),
            project,
            None,  # aggregate: tasks pointer clear
            None,  # aggregate: tasks delete
            None,  # aggregate: repositories delete
            project,  # aggregate: project delete returning
        ]
    )

    deleted = administrative_deletion.delete_project_record(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        project_id=project[0],
    )

    assert deleted.id == project[0]
    assert all("vault" not in sql for sql, _ in conn.executed)

    other = _project_row(uuid.uuid4())
    cross = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), other])
    with pytest.raises(NotFoundError):
        administrative_deletion.delete_project_record(
            _pool(cross),
            profile_id=profile_id,
            workspace_id=workspace_id,
            project_id=project[0],
        )


def test_delete_workspace_record_cleans_every_referenced_vault_secret() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_a = uuid.uuid4()
    connection_a = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(secret_a),
    )
    connection_b = _connection_row(workspace_id, auth_reference=None)
    workspace_row = (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")
    deleted_workspace = (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            workspace_row,  # Workspace root lock (re-validated under the lock)
            [connection_a, connection_b],  # locked enumeration (fetchall)
            (1,),  # Vault secret delete for connection_a
            None,  # aggregate: tasks pointer clear
            None,  # aggregate: tasks delete
            None,  # aggregate: role bindings delete
            None,  # aggregate: connections delete
            None,  # aggregate: github installations delete (issue #57)
            deleted_workspace,  # aggregate: workspaces delete returning
        ]
    )

    deleted = administrative_deletion.delete_workspace_record(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert deleted.id == workspace_id
    sqls = [sql for sql, _ in conn.executed]
    root_lock_index = next(
        index
        for index, sql in enumerate(sqls)
        if "from openorc.workspaces where id = %s for update" in sql
    )
    enumeration_index = next(
        index
        for index, sql in enumerate(sqls)
        if "from openorc.connections" in sql and "for update" in sql
    )
    vault_index = next(index for index, sql in enumerate(sqls) if "vault.secrets" in sql)
    aggregate_index = next(
        index for index, sql in enumerate(sqls) if "delete from openorc.workspaces" in sql
    )
    # The Workspace root is locked before the Connection enumeration, and the
    # referenced secret is cleaned before the aggregate delete.
    assert root_lock_index < enumeration_index < vault_index < aggregate_index
    vault_calls = [params for sql, params in conn.executed if "vault.secrets" in sql]
    assert vault_calls == [(secret_a,)]


def test_delete_workspace_with_zero_connections_still_locks_the_root() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    workspace_row = (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")
    deleted_workspace = (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            workspace_row,  # Workspace root lock
            [],  # zero Connections in the aggregate
            None,  # aggregate: tasks pointer clear
            None,  # aggregate: tasks delete
            None,  # aggregate: role bindings delete
            None,  # aggregate: connections delete
            None,  # aggregate: github installations delete (issue #57)
            deleted_workspace,  # aggregate: workspaces delete returning
        ]
    )

    deleted = administrative_deletion.delete_workspace_record(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert deleted.id == workspace_id
    sqls = [sql for sql, _ in conn.executed]
    root_lock_index = next(
        index
        for index, sql in enumerate(sqls)
        if "from openorc.workspaces where id = %s for update" in sql
    )
    aggregate_index = next(
        index for index, sql in enumerate(sqls) if "delete from openorc.workspaces" in sql
    )
    # Even with zero Connections the root lock precedes the aggregate delete:
    # a concurrent child INSERT cannot enter the aggregate after cleanup.
    assert root_lock_index < aggregate_index
    assert all("vault" not in sql for sql, _ in conn.executed)


def test_delete_workspace_resolution_fails_closed_uniformly() -> None:
    profile_id = uuid.uuid4()

    missing = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), None])
    with pytest.raises(NotFoundError):
        administrative_deletion.delete_workspace_record(
            _pool(missing), profile_id=profile_id, workspace_id=uuid.uuid4()
        )

    other_workspace = (uuid.uuid4(), uuid.uuid4(), "platform", _OBSERVED, _OBSERVED, 5, "")
    cross = ScriptedConnection([_guard_operational_row(), _ws_row(profile_id), other_workspace])
    with pytest.raises(NotFoundError):
        administrative_deletion.delete_workspace_record(
            _pool(cross), profile_id=profile_id, workspace_id=other_workspace[0]
        )


def test_every_deletion_flow_composes_the_account_barrier_first() -> None:
    # The Profile FOR KEY SHARE read is the FIRST lock acquisition of every
    # guarded mutation, before any subject-row lock (issue #97 ordering rule).
    source = inspect.getsource(administrative_deletion)
    for operation in (
        "disconnect_connection_credential",
        "delete_connection_record",
        "purge_archived_task_record",
        "delete_repository_record",
        "delete_project_record",
        "delete_workspace_record",
    ):
        operation_source = source.split(f"def {operation}(", 1)[1].split("\ndef ", 1)[0]
        first_guard = operation_source.index("require_account_operational(")
        subject_call = (
            operation_source.index("require_profile_workspace(")
            if "require_profile_workspace" in operation_source
            else -1
        )
        if subject_call == -1:
            subject_call = operation_source.index("require_workspace_")
        assert first_guard < subject_call


def test_aggregate_deletion_paths_import_no_github_or_runtime_adapters() -> None:
    # Zero GitHub/runtime adapter calls: the module importing no adapter is
    # the structural guarantee that ordinary aggregate deletion makes none.
    source = inspect.getsource(administrative_deletion)
    assert "openorc.adapters" not in source


def test_disconnect_opens_one_service_span_with_safe_attributes() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(workspace_id)
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            locked,
            _connection_row(workspace_id, auth_reference=None, enabled=False),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        administrative_deletion.disconnect_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "administrative_deletion.disconnect_connection_credential"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[OPERATION] == "administrative_deletion.disconnect_connection_credential"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
    assert attributes[CONNECTION_ID] == str(locked[0])


def test_delete_workspace_opens_one_service_span_with_safe_attributes() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    workspace_row = (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")
    conn = ScriptedConnection(
        [
            _guard_operational_row(),
            _ws_row(profile_id),
            workspace_row,
            [],
            None,
            None,
            None,
            None,
            None,
            workspace_row,
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        administrative_deletion.delete_workspace_record(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "administrative_deletion.delete_workspace_record"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[OPERATION] == "administrative_deletion.delete_workspace_record"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
