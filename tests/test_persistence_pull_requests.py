"""Deterministic mapping tests for TaskPullRequest repositories (issue #25).

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the review/gate mapping fakes) to
prove row-to-domain-object mapping, UTC normalization at the persistence
boundary, parameterization, the explicit-OPEN creation SQL (the migration
declares no lifecycle default), the observed-snapshot UPDATE semantics
(full replacement of head_ref/base_ref/head_sha/state/merged_at with
``updated_at`` advancing and ``github_pr_number`` deliberately excluded),
and the identity-vs-address lookup shapes. Database constraint behavior —
including ``unique (task_id)`` and ``unique (workspace_id, github_pr_id)``
— is proven against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.pull_requests import (
    TaskPullRequestDomainError,
    TaskPullRequestState,
)
from openorc.persistence import pull_requests as pull_requests_module
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import (
    create_task_pull_request,
    find_task_pull_request_by_github_identity,
    get_task_pull_request,
    get_task_pull_request_for_task,
    update_task_pull_request_observed,
)


class FakeCursor:
    """Returns one canned row (and optional canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    """Records executed SQL and returns canned rows, optionally per statement."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        responses: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.responses = list(responses) if responses is not None else None
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if self.responses is not None:
            response = self.responses.pop(0)
            return FakeCursor(response, None if response is None else [response])
        return FakeCursor(self.row, self.rows)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[FakeConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("mapping tests never close pools")


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 18, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _pull_request_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "repository_id": uuid.uuid4(),
        "github_pr_id": 900_719_925_474_099,
        "github_pr_number": 42,
        "head_ref": "openorc/task-42",
        "base_ref": "main",
        "head_sha": "0123456789abcdef0123456789abcdef01234567",
        "state": "open",
        "merged_at": None,
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["repository_id"],
        values["github_pr_id"],
        values["github_pr_number"],
        values["head_ref"],
        values["base_ref"],
        values["head_sha"],
        values["state"],
        values["merged_at"],
        values["created_at"],
        values["updated_at"],
    )


def test_create_task_pull_request_inserts_open_with_the_explicit_state() -> None:
    row = _pull_request_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    record = create_task_pull_request(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        repository_id=row[3],
        github_pr_id=row[4],
        github_pr_number=row[5],
        head_ref=row[6],
        base_ref=row[7],
        head_sha=row[8],
    )

    assert record.id == row[0]
    assert record.workspace_id == row[1]
    assert record.task_id == row[2]
    assert record.repository_id == row[3]
    assert record.github_pr_id == row[4]
    assert record.github_pr_number == row[5]
    assert record.state is TaskPullRequestState.OPEN
    assert record.merged_at is None
    assert record.created_at == _utc_observed_at()
    assert record.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.task_pull_requests" in sql
    # The observed lifecycle carries no database default: the initial open
    # state is explicit in the creation statement.
    assert "head_ref, base_ref, head_sha, state)" in sql
    assert params == (
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        "open",
    )
    assert len(fake_conn.executed) == 1


def test_create_task_pull_request_validates_payload_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    base: dict[str, Any] = {
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "repository_id": uuid.uuid4(),
        "github_pr_id": 100,
        "github_pr_number": 1,
        "head_ref": "openorc/task-1",
        "base_ref": "main",
        "head_sha": "0123456789abcdef0123456789abcdef01234567",
    }
    for overrides in (
        {"github_pr_id": 0},
        {"github_pr_number": True},
        {"head_ref": "   "},
        {"head_sha": None},
        {"task_id": "not-a-uuid"},
    ):
        with pytest.raises(TaskPullRequestDomainError):
            create_task_pull_request(pool, **{**base, **overrides})
    assert fake_conn.executed == []


def test_get_task_pull_request_maps_or_returns_none() -> None:
    row = _pull_request_row()
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    record = get_task_pull_request(pool, task_pull_request_id=row[0])
    assert record is not None
    assert record.id == row[0]
    assert record.updated_at == _utc_observed_at()

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_task_pull_request(empty_pool, task_pull_request_id=row[0]) is None


def test_get_task_pull_request_maps_the_closed_unmerged_canonical_record() -> None:
    # A closed-unmerged PR remains the Task's canonical record: the per-Task
    # lookup reaches it, observed as closed with no merge stamp.
    row = _pull_request_row(state="closed", merged_at=None)
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))
    record = get_task_pull_request_for_task(pool, task_id=row[2])
    assert record is not None
    assert record.task_id == row[2]
    assert record.state is TaskPullRequestState.CLOSED
    assert record.merged_at is None

    sql, params = fake_conn.executed[0]
    assert "where task_id = %s" in sql
    assert params == (row[2],)


def test_find_by_github_identity_keys_on_the_stable_identity() -> None:
    # The reconciliation lookup is the stable external ``github_pr_id``
    # within one Workspace; ``github_pr_number`` is repository-local
    # address metadata and never the lookup key (issue #25).
    row = _pull_request_row(github_pr_id=7_000_000_001, github_pr_number=42)
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    record = find_task_pull_request_by_github_identity(
        pool, workspace_id=row[1], github_pr_id=row[4]
    )
    assert record is not None
    assert record.github_pr_id == 7_000_000_001
    assert record.github_pr_number == 42

    sql, params = fake_conn.executed[0]
    assert "where workspace_id = %s and github_pr_id = %s" in sql
    assert "github_pr_number" not in sql.split("where")[1]
    assert params == (row[1], row[4])

    # Invalid identity payloads are caller errors before any SQL runs.
    empty_conn = FakeConnection()
    empty_pool = cast(DatabasePool, FakePool(empty_conn))
    with pytest.raises(TaskPullRequestDomainError):
        find_task_pull_request_by_github_identity(empty_pool, workspace_id=row[1], github_pr_id=0)
    with pytest.raises(TaskPullRequestDomainError):
        find_task_pull_request_by_github_identity(
            empty_pool,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            github_pr_id=1,
        )
    assert empty_conn.executed == []


def test_update_observed_replaces_the_full_observed_snapshot() -> None:
    # Reconciliation presents what GitHub currently reports: the whole
    # observed snapshot is replaced in one statement, ``updated_at``
    # advances, and the identity/address columns are never part of it.
    head = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    merged = _pull_request_row(
        head_sha=head,
        state="closed",
        merged_at=_observed_at(),
        updated_at=_observed_at(),
    )
    fake_conn = FakeConnection(merged)
    pool = cast(DatabasePool, FakePool(fake_conn))

    record = update_task_pull_request_observed(
        pool,
        task_pull_request_id=merged[0],
        head_ref="openorc/task-42",
        base_ref="main",
        head_sha=head,
        state=TaskPullRequestState.CLOSED,
        merged_at=_observed_at(),
    )

    assert record is not None
    assert record.state is TaskPullRequestState.CLOSED
    assert record.merged_at == _utc_observed_at()
    assert record.updated_at == _utc_observed_at()

    sql, params = fake_conn.executed[0]
    assert "update openorc.task_pull_requests" in sql
    assert "set head_ref = %s, base_ref = %s, head_sha = %s, state = %s, " in sql
    assert "merged_at = %s, updated_at = now()" in sql
    # The identity and address columns are never part of the snapshot: the
    # SET clause carries only the observed reconciliation state (the
    # RETURNING clause projects the full row and is bounded out).
    set_clause = sql.split("set")[1].split("where")[0]
    assert "github_pr_id" not in set_clause
    assert "github_pr_number" not in set_clause
    assert "repository_id" not in set_clause
    assert params == ("openorc/task-42", "main", head, "closed", _observed_at(), merged[0])


def test_update_observed_rejects_incoherent_snapshots_before_sql() -> None:
    # A merged observation must be closed, the state must be the observed
    # vocabulary, and the text fields must be non-blank: the payload is
    # validated here so a transaction cannot commit a snapshot the domain
    # and the database CHECK reject.
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    pr_id = uuid.uuid4()
    with pytest.raises(TaskPullRequestDomainError):
        update_task_pull_request_observed(
            pool,
            task_pull_request_id=pr_id,
            head_ref="h",
            base_ref="main",
            head_sha="0123456789abcdef0123456789abcdef01234567",
            state=TaskPullRequestState.OPEN,
            merged_at=_observed_at(),
        )
    with pytest.raises(TaskPullRequestDomainError):
        update_task_pull_request_observed(
            pool,
            task_pull_request_id=pr_id,
            head_ref="h",
            base_ref="main",
            head_sha="0123456789abcdef0123456789abcdef01234567",
            state="merged",  # type: ignore[arg-type]
            merged_at=None,
        )
    with pytest.raises(TaskPullRequestDomainError):
        update_task_pull_request_observed(
            pool,
            task_pull_request_id=pr_id,
            head_ref="",
            base_ref="main",
            head_sha="0123456789abcdef0123456789abcdef01234567",
            state=TaskPullRequestState.OPEN,
            merged_at=None,
        )
    with pytest.raises(TaskPullRequestDomainError):
        update_task_pull_request_observed(
            pool,
            task_pull_request_id="not-a-uuid",  # type: ignore[arg-type]
            head_ref="h",
            base_ref="main",
            head_sha="0123456789abcdef0123456789abcdef01234567",
            state=TaskPullRequestState.OPEN,
            merged_at=None,
        )
    assert fake_conn.executed == []


def test_update_observed_returns_none_for_a_missing_record() -> None:
    # A missing record is a rejected no-op that must not be retried blindly.
    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert (
        update_task_pull_request_observed(
            empty_pool,
            task_pull_request_id=uuid.uuid4(),
            head_ref="h",
            base_ref="main",
            head_sha="0123456789abcdef0123456789abcdef01234567",
            state=TaskPullRequestState.OPEN,
            merged_at=None,
        )
        is None
    )


def test_an_open_observation_never_carries_a_merge_stamp() -> None:
    # Reconciliation of an open PR reports merged_at=None (the coherent
    # open form); the repository accepts it unchanged.
    row = _pull_request_row(state="open", merged_at=None)
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))
    record = update_task_pull_request_observed(
        pool,
        task_pull_request_id=row[0],
        head_ref=row[6],
        base_ref=row[7],
        head_sha=row[8],
        state=TaskPullRequestState.OPEN,
        merged_at=None,
    )
    assert record is not None
    assert record.state is TaskPullRequestState.OPEN
    assert record.merged_at is None
    _, params = fake_conn.executed[0]
    assert params is not None and params[4] is None


def test_the_module_surface_carries_only_the_pr_repositories() -> None:
    assert set(pull_requests_module.__all__) == {
        "create_task_pull_request",
        "find_task_pull_request_by_github_identity",
        "get_task_pull_request",
        "get_task_pull_request_for_task",
        "update_task_pull_request_observed",
    }
