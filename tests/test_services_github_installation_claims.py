"""Deterministic tests for the GitHub installation claim services (issue #372).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the Owner-authorized claim surface — the claim command
persisting the stable identity ALONE (observation columns NULL in the
executed command), idempotent re-claim returning the durable record
unchanged, the issue #97 account-deletion barrier as the first lock
acquisition, the uniform not-found isolation outcomes, and the
ownership-gated reads. Durable insert/upsert behavior and the trusted
claim-establishment coalesce are proven by the persistence mapping tests,
the migration convention tests, and the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.observability import OPERATION, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import github_installation_claims
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of the claim mutation; the scripted result row for an
# operational account carries the all-NULL attempt-state tuple.
_GUARD_OPERATIONAL_ROW = (None, None, None)
_GUARD_DELETION_ACTIVE_ROW = ("active", uuid.uuid4(), _OBSERVED)


class FakeCursor:
    def __init__(self, result: Any) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result if isinstance(self._result, tuple) else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._result) if isinstance(self._result, list) else []


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[Any]) -> None:
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
        raise AssertionError("installation claim tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(workspace_id: Any, profile_id: Any) -> tuple[Any, ...]:
    return (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "", None)


def _claim_state_installation_row(installation_id: Any, workspace_id: Any) -> tuple[Any, ...]:
    return (
        installation_id,
        workspace_id,
        12345678,
        None,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _established_installation_row(installation_id: Any, workspace_id: Any) -> tuple[Any, ...]:
    return (
        installation_id,
        workspace_id,
        12345678,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def test_record_claim_rejects_an_invalid_identity_before_any_database_work() -> None:
    conn = ScriptedConnection([])

    for bad in (0, -1, True, 1.5, "12345678", None):
        with pytest.raises(InvalidCommandError):
            github_installation_claims.record_installation_claim(
                _pool(conn),
                profile_id=uuid.uuid4(),
                workspace_id=uuid.uuid4(),
                github_installation_id=bad,  # type: ignore[arg-type]
            )
    assert conn.executed == []


def test_record_claim_persists_the_identity_alone_in_claim_state() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _claim_state_installation_row(installation_id, workspace_id),
        ]
    )

    installation = github_installation_claims.record_installation_claim(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        github_installation_id=12345678,
    )

    # The claim command carries the stable external identity ALONE: the
    # executed insert binds no observation fact of any kind.
    assert installation.account is None
    assert installation.suspended_at is None
    assert installation.identity.github_installation_id == 12345678
    assert len(conn.executed) == 3
    insert_sql, insert_params = conn.executed[2]
    assert "insert into openorc.github_installations" in insert_sql
    assert "on conflict (workspace_id, github_installation_id) do nothing" in insert_sql
    assert "values (%s, %s, null, null, null, null)" in insert_sql
    assert insert_params == (workspace_id, 12345678)
    # No observation column is writable through the claim command: the
    # statement names none of them as a bound parameter.
    for column in ("github_account_id", "account_login", "account_type", "suspended_at"):
        assert f"{column} = %s" not in insert_sql


def test_record_claim_is_idempotent_when_the_record_already_exists() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            None,
            _established_installation_row(installation_id, workspace_id),
        ]
    )

    installation = github_installation_claims.record_installation_claim(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        github_installation_id=12345678,
    )

    # A re-asserted claim (or an established installation) returns the
    # durable record unchanged — a claim never updates an existing row.
    assert installation.account is not None
    assert installation.account.github_account_id == 501
    assert len(conn.executed) == 4
    fallback_sql, fallback_params = conn.executed[3]
    assert "select" in fallback_sql
    assert "where workspace_id = %s and github_installation_id = %s" in fallback_sql
    assert fallback_params == (workspace_id, 12345678)


def test_record_claim_fails_closed_during_an_account_deletion_attempt() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_DELETION_ACTIVE_ROW])

    with pytest.raises(ConflictError):
        github_installation_claims.record_installation_claim(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=uuid.uuid4(),
            github_installation_id=12345678,
        )

    # The barrier read ran and the guarded mutation was rejected before any
    # claim insert — and it would roll back with the composed transaction
    # even if one had raced in.
    assert len(conn.executed) == 1
    assert "openorc.profiles" in conn.executed[0][0]


def test_record_claim_rejects_an_unowned_workspace_uniformly() -> None:
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, None])

    with pytest.raises(NotFoundError):
        github_installation_claims.record_installation_claim(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            github_installation_id=12345678,
        )

    # A foreign Workspace is indistinguishable from an absent one, and no
    # claim insert ever ran.
    assert len(conn.executed) == 2
    assert "from openorc.workspaces" in conn.executed[1][0]


def test_list_claims_is_ownership_gated_and_workspace_scoped() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            [_claim_state_installation_row(installation_id, workspace_id)],
        ]
    )

    installations = github_installation_claims.list_workspace_installation_claims(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert len(installations) == 1
    assert installations[0].account is None
    sql, params = conn.executed[1]
    assert "where workspace_id = %s order by created_at, id" in sql
    assert params == (workspace_id,)


def test_read_claim_rejects_a_foreign_installation_uniformly() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _established_installation_row(uuid.uuid4(), uuid.uuid4()),
        ]
    )

    with pytest.raises(NotFoundError):
        github_installation_claims.read_installation_claim(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            installation_id=uuid.uuid4(),
        )


def test_read_claim_returns_the_workspace_record_with_its_recorded_state() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _established_installation_row(installation_id, workspace_id),
        ]
    )

    installation = github_installation_claims.read_installation_claim(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        installation_id=installation_id,
    )

    assert installation.id == installation_id
    assert installation.account is not None
    assert installation.account.login == "octocat"


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_record_claim_opens_its_service_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _claim_state_installation_row(uuid.uuid4(), workspace_id),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        github_installation_claims.record_installation_claim(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            github_installation_id=12345678,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "github_installation_claims.record_installation_claim"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID}
    assert attributes[WORKSPACE_ID] == str(workspace_id)
