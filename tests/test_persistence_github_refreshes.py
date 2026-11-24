"""Deterministic tests for the scoped-refresh tracking repositories (#375).

The ordinary suite cannot execute Postgres; these tests use scripted SQL
handlers over a fake pool/connection seam to prove the durable execution
tracking contract:

- the atomic admission shape (the Workspace row lock, the on-demand
  expired-record recovery, the active-overlap containment across both
  scopes, the bounded retention trim, and the accepted insert);
- the unique-violation race classification from re-read durable state;
- the claim CAS (``accepted → running`` with lease extension) and its
  refusal of an expired record;
- the terminal CAS (``running → terminal`` with summary/errors) and its
  refusal once the record moved on (no false completion);
- the known pre-execution refusal (``accepted → failed``);
- the verbatim status read.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.github_refreshes import (
    GitHubRefreshStatus,
    GitHubRefreshSummary,
    GitHubRefreshUnitError,
)
from openorc.persistence.github_refreshes import (
    GITHUB_REFRESH_ADMISSION_LEASE_SECONDS,
    admit_github_refresh,
    claim_github_refresh,
    complete_github_refresh,
    fail_unclaimed_github_refresh,
    get_github_refresh,
)
from openorc.persistence.pool import DatabasePool

_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_REFRESH_ID = uuid.uuid4()
_NOW = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)

_ERROR = GitHubRefreshUnitError(
    family="issue", code="failed_known", workspace_id=_WORKSPACE_ID, github_issue_id=503
)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, tuple[Any, ...] | None | Exception]] = []

    def on(self, sql_marker: str, result: tuple[Any, ...] | None | Exception) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("refresh persistence tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _refresh_row(
    *,
    status: str = "accepted",
    repository_id: Any = None,
    lease: datetime | None = _NOW,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    summary: Any = None,
    errors: Any = None,
) -> tuple[Any, ...]:
    return (
        _REFRESH_ID,
        _WORKSPACE_ID,
        repository_id,
        status,
        _NOW,
        started_at,
        lease,
        completed_at,
        summary,
        errors,
    )


def test_a_workspace_scope_admission_serializes_recovers_and_inserts() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.workspaces", (_WORKSPACE_ID,))
    conn.on("update openorc.github_refreshes", None)  # expired-record recovery
    conn.on("from openorc.github_refreshes", None)  # no active overlap
    conn.on("delete from openorc.github_refreshes", None)  # retention trim
    conn.on(
        "insert into openorc.github_refreshes",
        _refresh_row(status="accepted"),
    )

    outcome = admit_github_refresh(_pool(conn), workspace_id=_WORKSPACE_ID, repository_id=None)

    assert outcome.conflict is False
    assert outcome.workspace_missing is False
    assert outcome.record is not None
    assert outcome.record.status is GitHubRefreshStatus.ACCEPTED
    assert outcome.record.repository_id is None
    insert_sql, insert_params = conn.executed[-1]
    assert "insert into openorc.github_refreshes" in insert_sql
    assert insert_params is not None
    # The admitted lease is the bounded admission window.
    assert insert_params[-1] == GITHUB_REFRESH_ADMISSION_LEASE_SECONDS
    # The recovery and overlap predicates addressed the whole workspace scope.
    recovery_sql, recovery_params = conn.executed[1]
    assert "lease_expires_at < now()" in recovery_sql
    assert recovery_params == (
        "unknown",
        _WORKSPACE_ID,
        "accepted",
        "running",
    )
    overlap_sql, overlap_params = conn.executed[2]
    assert "lease_expires_at >= now() limit 1" in overlap_sql
    assert overlap_params == (_WORKSPACE_ID, "accepted", "running")


def test_a_repository_scope_admission_containment_covers_both_scopes() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.workspaces", (_WORKSPACE_ID,))
    conn.on("update openorc.github_refreshes", None)
    conn.on("from openorc.github_refreshes", None)
    conn.on("delete from openorc.github_refreshes", None)
    conn.on("insert into openorc.github_refreshes", _refresh_row(repository_id=_REPOSITORY_ID))

    outcome = admit_github_refresh(
        _pool(conn), workspace_id=_WORKSPACE_ID, repository_id=_REPOSITORY_ID
    )

    assert outcome.record is not None
    assert outcome.record.repository_id == _REPOSITORY_ID
    recovery_sql, recovery_params = conn.executed[1]
    assert "(repository_id = %s or repository_id is null)" in recovery_sql
    assert recovery_params == (
        "unknown",
        _WORKSPACE_ID,
        "accepted",
        "running",
        _REPOSITORY_ID,
    )
    overlap_sql, overlap_params = conn.executed[2]
    assert "(repository_id = %s or repository_id is null) limit 1" in overlap_sql
    assert overlap_params == (_WORKSPACE_ID, "accepted", "running", _REPOSITORY_ID)


def test_an_active_overlapping_record_conflicts_the_admission() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.workspaces", (_WORKSPACE_ID,))
    conn.on("update openorc.github_refreshes", None)
    conn.on("from openorc.github_refreshes", _refresh_row())

    outcome = admit_github_refresh(_pool(conn), workspace_id=_WORKSPACE_ID, repository_id=None)

    assert outcome.record is None
    assert outcome.conflict is True
    # Nothing was inserted and no retention trim ran after the conflict.
    assert not any("insert into" in sql.lower() for sql, _ in conn.executed)


def test_a_unique_violation_race_classifies_from_re_read_state_as_a_conflict() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.workspaces", (_WORKSPACE_ID,))
    conn.on("update openorc.github_refreshes", None)
    conn.on("from openorc.github_refreshes", None)
    conn.on("delete from openorc.github_refreshes", None)
    conn.on("insert into openorc.github_refreshes", UniqueViolation("duplicate key"))

    outcome = admit_github_refresh(_pool(conn), workspace_id=_WORKSPACE_ID, repository_id=None)

    assert outcome.record is None
    assert outcome.conflict is True


def test_a_missing_workspace_row_is_reported_not_found() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.workspaces", None)

    outcome = admit_github_refresh(_pool(conn), workspace_id=_WORKSPACE_ID, repository_id=None)

    assert outcome.workspace_missing is True
    assert outcome.record is None
    assert len(conn.executed) == 1


def test_the_claim_cas_extends_the_execution_lease() -> None:
    conn = ScriptedConnection()
    conn.on(
        "update openorc.github_refreshes",
        _refresh_row(status="running", started_at=_NOW),
    )

    record = claim_github_refresh(_pool(conn), refresh_id=_REFRESH_ID)

    assert record is not None
    assert record.status is GitHubRefreshStatus.RUNNING
    sql, params = conn.executed[0]
    assert "lease_expires_at >= now()" in sql
    assert params == ("running", 1800, _REFRESH_ID, "accepted")


def test_an_expired_record_is_never_claimable_by_a_stale_job() -> None:
    conn = ScriptedConnection()
    conn.on("update openorc.github_refreshes", None)

    assert claim_github_refresh(_pool(conn), refresh_id=_REFRESH_ID) is None


def test_the_terminal_cas_stamps_summary_and_errors() -> None:
    conn = ScriptedConnection()
    conn.on(
        "update openorc.github_refreshes",
        _refresh_row(
            status="completed",
            completed_at=_NOW,
            summary={"repositories_checked": 1, "issues_discovered": 0},
            errors=[],
        ),
    )

    record = complete_github_refresh(
        _pool(conn),
        refresh_id=_REFRESH_ID,
        status=GitHubRefreshStatus.COMPLETED,
        summary=GitHubRefreshSummary(repositories_checked=1),
        errors=(),
    )

    assert record is not None
    assert record.status is GitHubRefreshStatus.COMPLETED
    sql, params = conn.executed[0]
    assert params is not None
    assert params[0] == "completed"
    assert params[3] == _REFRESH_ID
    assert params[4] == "running"


def test_a_recovered_record_never_accepts_a_stale_completion() -> None:
    conn = ScriptedConnection()
    conn.on("update openorc.github_refreshes", None)

    record = complete_github_refresh(
        _pool(conn),
        refresh_id=_REFRESH_ID,
        status=GitHubRefreshStatus.PARTIALLY_COMPLETED,
        summary=GitHubRefreshSummary(),
        errors=(),
    )

    # The record moved on: no false completion was stamped.
    assert record is None


def test_a_known_pre_execution_refusal_is_a_durable_terminal_failure() -> None:
    conn = ScriptedConnection()
    conn.on(
        "update openorc.github_refreshes",
        _refresh_row(status="failed", completed_at=_NOW, errors=[{"family": "admission"}]),
    )

    record = fail_unclaimed_github_refresh(_pool(conn), refresh_id=_REFRESH_ID, errors=(_ERROR,))

    assert record is not None
    assert record.status is GitHubRefreshStatus.FAILED
    sql, params = conn.executed[0]
    assert params is not None
    assert params[0] == "failed"
    assert params[2] == _REFRESH_ID
    assert params[3] == "accepted"
    # The summary stays unset in the SET clause: no observation was ever
    # attempted (the word only appears in the RETURNING column list).
    assert "set status = %s, completed_at = now(), errors = %s" in sql


def test_the_status_read_is_verbatim() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.github_refreshes", _refresh_row(status="running", started_at=_NOW))

    record = get_github_refresh(_pool(conn), refresh_id=_REFRESH_ID)

    assert record is not None
    assert record.status is GitHubRefreshStatus.RUNNING
    assert record.lease_expires_at == _NOW


def test_malformed_arguments_are_rejected_before_any_sql() -> None:
    conn = ScriptedConnection()

    with pytest.raises(ValueError):
        admit_github_refresh(_pool(conn), workspace_id="not-a-uuid", repository_id=None)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        claim_github_refresh(_pool(conn), refresh_id="not-a-uuid")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        complete_github_refresh(
            _pool(conn),
            refresh_id=_REFRESH_ID,
            status=GitHubRefreshStatus.ACCEPTED,  # type: ignore[arg-type]
            summary=GitHubRefreshSummary(),
            errors=(),
        )

    assert conn.executed == []
