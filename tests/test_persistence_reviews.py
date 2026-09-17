"""Deterministic mapping tests for ReviewLoop and ReviewIteration repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the session/ownership mapping fakes)
to prove row-to-domain-object mapping, UTC normalization at the persistence
boundary, parameterization, the explicit-OPEN establishment SQL, the
absorbing close transition, the unfinalized-iteration insert, the atomic
one-shot finalize SQL (with the ``Jsonb`` findings adapter and the
``WHERE outcome IS NULL`` no-rewrite condition), and empty-result handling.
Database constraint behavior is proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from psycopg.types.json import Jsonb

from openorc.domain.reviews import (
    ReviewLoopDomainError,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.reviews import (
    close_review_loop,
    create_review_iteration,
    create_review_loop,
    get_review_iteration,
    get_review_loop,
    list_review_loop_iterations,
    list_task_review_loops,
    record_review_iteration_result,
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
    """Records executed SQL and returns canned rows."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
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
    return datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _loop_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "purpose": "planning",
        "iteration_limit": 5,
        "status": "open",
        "closed_at": None,
        "created_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["purpose"],
        values["iteration_limit"],
        values["status"],
        values["closed_at"],
        values["created_at"],
    )


def _iteration_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "review_loop_id": uuid.uuid4(),
        "iteration_number": 1,
        "plan_revision_id": uuid.uuid4(),
        "outcome": None,
        "summary": None,
        "findings": None,
        "decided_at": None,
        "created_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["review_loop_id"],
        values["iteration_number"],
        values["plan_revision_id"],
        values["outcome"],
        values["summary"],
        values["findings"],
        values["decided_at"],
        values["created_at"],
    )


def test_create_review_loop_inserts_open_with_explicit_effective_limit() -> None:
    row = _loop_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    loop = create_review_loop(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
    )

    assert loop.id == row[0]
    assert loop.purpose is ReviewLoopPurpose.PLANNING
    assert loop.iteration_limit == 5
    assert loop.status is ReviewLoopStatus.OPEN
    assert loop.closed_at is None
    assert loop.created_at == _utc_observed_at()
    assert loop.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.review_loops" in sql
    # Establishment semantics are explicit at the write boundary: the schema
    # carries no status default, so the INSERT itself supplies OPEN.
    assert "values (%s, %s, %s, %s, 'open')" in sql
    assert params == (row[1], row[2], "planning", 5)
    assert len(fake_conn.executed) == 1


def test_create_review_loop_validates_purpose_and_limit_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    with pytest.raises(ReviewLoopDomainError):
        create_review_loop(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            purpose="implementation_review",  # type: ignore[arg-type]
            iteration_limit=5,
        )
    with pytest.raises(ReviewLoopDomainError):
        create_review_loop(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            purpose=ReviewLoopPurpose.PR_REVIEW,
            iteration_limit=0,
        )
    assert fake_conn.executed == []


def test_get_review_loop_maps_closed_rows_or_returns_none() -> None:
    closed = _loop_row(status="closed", closed_at=_observed_at())
    pool = cast(DatabasePool, FakePool(FakeConnection(closed)))
    loop = get_review_loop(pool, review_loop_id=closed[0])
    assert loop is not None
    assert loop.status is ReviewLoopStatus.CLOSED
    assert loop.closed_at == _utc_observed_at()
    assert loop.closed_at is not None and loop.closed_at.utcoffset() == timedelta(0)

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_review_loop(empty_pool, review_loop_id=closed[0]) is None


def test_list_task_review_loops_filters_by_status() -> None:
    open_row = _loop_row(purpose="pr_review")
    fake_conn = FakeConnection(rows=[open_row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    loops = list_task_review_loops(pool, task_id=open_row[2], status=ReviewLoopStatus.OPEN)

    assert [loop.id for loop in loops] == [open_row[0]]
    assert [loop.purpose for loop in loops] == [ReviewLoopPurpose.PR_REVIEW]
    sql, params = fake_conn.executed[0]
    assert "status = %s" in sql
    assert params == (open_row[2], "open")

    unfiltered_conn = FakeConnection(rows=[open_row])
    unfiltered_pool = cast(DatabasePool, FakePool(unfiltered_conn))
    list_task_review_loops(unfiltered_pool, task_id=open_row[2])
    sql, params = unfiltered_conn.executed[0]
    assert "status = %s" not in sql
    assert params == (open_row[2],)


def test_close_review_loop_is_an_absorbing_conditional_transition() -> None:
    closed = _loop_row(status="closed", closed_at=_observed_at())
    fake_conn = FakeConnection(closed)
    pool = cast(DatabasePool, FakePool(fake_conn))

    loop = close_review_loop(pool, review_loop_id=closed[0])

    assert loop is not None
    assert loop.status is ReviewLoopStatus.CLOSED
    assert loop.closed_at == _utc_observed_at()
    sql, params = fake_conn.executed[0]
    assert "set status = 'closed', closed_at = now()" in sql
    assert "where id = %s and status = 'open'" in sql
    assert params == (closed[0],)

    missing = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert close_review_loop(missing, review_loop_id=closed[0]) is None


def test_create_review_iteration_inserts_unfinalized_with_the_exact_subject() -> None:
    row = _iteration_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    iteration = create_review_iteration(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        review_loop_id=row[3],
        iteration_number=row[4],
        plan_revision_id=row[5],
    )

    assert iteration.id == row[0]
    assert iteration.iteration_number == 1
    assert iteration.plan_revision_id == row[5]
    assert iteration.outcome is None
    assert iteration.summary is None
    assert iteration.findings is None
    assert iteration.decided_at is None
    assert iteration.created_at == _utc_observed_at()

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.review_iterations" in sql
    # An unfinalized iteration carries no result facts: the insert names
    # none of the four result columns.
    for excluded in ("outcome", "summary", "findings", "decided_at"):
        assert excluded not in sql.split("values")[0]
    assert params == (row[1], row[2], row[3], row[4], row[5])
    assert len(fake_conn.executed) == 1


def test_create_review_iteration_validates_subject_and_number_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    with pytest.raises(ReviewLoopDomainError):
        create_review_iteration(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            review_loop_id=uuid.uuid4(),
            iteration_number=0,
            plan_revision_id=uuid.uuid4(),
        )
    with pytest.raises(ReviewLoopDomainError):
        create_review_iteration(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            review_loop_id=uuid.uuid4(),
            iteration_number=1,
            plan_revision_id=None,  # type: ignore[arg-type]
        )
    assert fake_conn.executed == []


def test_list_review_loop_iterations_orders_by_number() -> None:
    first = _iteration_row(iteration_number=1)
    second = _iteration_row(iteration_number=2)
    fake_conn = FakeConnection(rows=[first, second])
    pool = cast(DatabasePool, FakePool(fake_conn))

    iterations = list_review_loop_iterations(pool, review_loop_id=first[3])

    assert [iteration.iteration_number for iteration in iterations] == [1, 2]
    sql, params = fake_conn.executed[0]
    assert "order by iteration_number" in sql
    assert params == (first[3],)


def test_get_review_iteration_maps_or_returns_none() -> None:
    row = _iteration_row()
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    iteration = get_review_iteration(pool, review_iteration_id=row[0])
    assert iteration is not None
    assert iteration.id == row[0]

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_review_iteration(empty_pool, review_iteration_id=row[0]) is None


def test_record_review_iteration_result_finalizes_atomically_with_the_jsonb_adapter() -> None:
    finalized = _iteration_row(
        outcome="changes_requested",
        summary="fix the loop",
        findings=[{"summary": "s", "details": "d"}],
        decided_at=_observed_at(),
    )
    fake_conn = FakeConnection(finalized)
    pool = cast(DatabasePool, FakePool(fake_conn))

    iteration = record_review_iteration_result(
        pool,
        review_iteration_id=finalized[0],
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="fix the loop",
        findings=({"summary": "s", "details": "d"},),
    )

    assert iteration is not None
    assert iteration.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert iteration.summary == "fix the loop"
    assert iteration.findings == [{"summary": "s", "details": "d"}]
    assert iteration.decided_at == _utc_observed_at()

    sql, params = fake_conn.executed[0]
    assert "update openorc.review_iterations" in sql
    # The complete result finalizes atomically in one statement, and the
    # no-rewrite condition applies only while the iteration is unfinalized.
    assert "set outcome = %s, summary = %s, findings = %s, decided_at = now()" in sql
    assert "where id = %s and outcome is null" in sql
    assert params is not None
    assert params[0] == "changes_requested"
    assert params[1] == "fix the loop"
    # The findings array is canonicalized (tuples never survive) and written
    # through the explicit Jsonb adapter.
    assert isinstance(params[2], Jsonb)
    assert params[2].obj == [{"summary": "s", "details": "d"}]
    assert params[3] == finalized[0]


def test_record_review_iteration_result_validates_the_reviewer_judgment() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    # Provider/runtime/protocol failures are not reviewer judgments.
    with pytest.raises(ReviewLoopDomainError):
        record_review_iteration_result(
            pool,
            review_iteration_id=uuid.uuid4(),
            outcome="failed",  # type: ignore[arg-type]
            summary="broken",
            findings=[],
        )
    with pytest.raises(ReviewLoopDomainError):
        record_review_iteration_result(
            pool,
            review_iteration_id=uuid.uuid4(),
            outcome=ReviewOutcome.ACCEPTED,
            summary="   ",
            findings=[],
        )
    with pytest.raises(ReviewLoopDomainError):
        record_review_iteration_result(
            pool,
            review_iteration_id=uuid.uuid4(),
            outcome=ReviewOutcome.CHANGES_REQUESTED,
            summary="fix",
            findings="not-an-array",  # type: ignore[arg-type]
        )
    assert fake_conn.executed == []


def test_record_review_iteration_result_never_rewrites_a_finalized_iteration() -> None:
    # A finalized iteration cannot change: the conditional UPDATE matches no
    # row, and the returned None must not be retried blindly.
    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert (
        record_review_iteration_result(
            empty_pool,
            review_iteration_id=uuid.uuid4(),
            outcome=ReviewOutcome.ACCEPTED,
            summary="rewrite attempt",
            findings=[],
        )
        is None
    )


def test_findings_are_canonicalized_before_the_jsonb_write() -> None:
    fake_conn = FakeConnection(_iteration_row())
    pool = cast(DatabasePool, FakePool(fake_conn))
    record_review_iteration_result(
        pool,
        review_iteration_id=uuid.uuid4(),
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="fix",
        findings=({"lines": (1, 2)},),
    )
    _, params = fake_conn.executed[0]
    assert params is not None
    assert isinstance(params[2], Jsonb)
    assert params[2].obj == [{"lines": [1, 2]}]
