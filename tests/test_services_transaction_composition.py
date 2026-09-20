"""Deterministic tests for the service transaction-composition primitive (issue #51).

The fakes emulate exactly the driver semantics the composition relies on,
verified against the pinned psycopg 3.3.5 and psycopg_pool 3.3.1 sources:

- ``psycopg_pool.ConnectionPool.connection()`` checks a connection out and
  applies the connection context (commit on clean exit, rollback on error);
  it opens NO transaction block of its own. ``pool.connection()`` alone
  therefore establishes no transaction.
- ``Connection.transaction()`` decides nesting from open-transaction state,
  not from a block stack: entered while no transaction is open it is an
  outer transaction (``BEGIN`` ... ``COMMIT``/``ROLLBACK``); entered while
  one is open it is a nested SAVEPOINT block (``SAVEPOINT`` ...
  ``RELEASE``, ``ROLLBACK TO`` on error). A statement executed outside any
  block opens an implicit transaction.

The event sequences below prove the load-bearing property: the FIRST
transaction block on the connection is opened by ``composed_transaction``
itself, before it yields, so every repository ``transaction(...)`` scope
through the yielded view is a nested SAVEPOINT and the composition commits
or rolls back as one.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, cast

import pytest

from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction
from openorc.services.transaction_composition import composed_transaction


class FakeConnection:
    """Emulates the psycopg Connection semantics the composition relies on."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self._in_transaction = False
        self._savepoint_depth = 0

    def commit(self) -> None:
        if self._in_transaction:
            self.events.append("commit")
            self._in_transaction = False
            self._savepoint_depth = 0

    def rollback(self) -> None:
        if self._in_transaction:
            self.events.append("rollback")
            self._in_transaction = False
            self._savepoint_depth = 0

    def execute(self, *args: Any, **kwargs: Any) -> None:
        self.events.append("execute")
        self._in_transaction = True  # implicit transaction opens (autocommit off)

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def transaction(self) -> AbstractContextManager[FakeConnection]:
        @contextmanager
        def block() -> Iterator[FakeConnection]:
            outer = not self._in_transaction
            if outer:
                self.events.append("begin")
                self._in_transaction = True
            else:
                self._savepoint_depth += 1
                self.events.append(f"savepoint:{self._savepoint_depth}")
            try:
                yield self
            except BaseException:
                if outer:
                    self.events.append("rollback")
                    self._in_transaction = False
                    self._savepoint_depth = 0
                else:
                    self.events.append(f"rollback_to:{self._savepoint_depth}")
                    self._savepoint_depth -= 1
                raise
            else:
                if outer:
                    self.events.append("commit")
                    self._in_transaction = False
                    self._savepoint_depth = 0
                else:
                    self.events.append(f"release:{self._savepoint_depth}")
                    self._savepoint_depth -= 1

        return block()


class FakePool:
    """Emulates psycopg_pool checkout semantics: raw checkout, no transaction block."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn
        self.returned = 0

    def connection(self) -> AbstractContextManager[FakeConnection]:
        @contextmanager
        def checkout() -> Iterator[FakeConnection]:
            self._conn.events.append("checkout")
            try:
                with self._conn:
                    yield self._conn
            finally:
                self.returned += 1
                self._conn.events.append("return")

        return checkout()

    def close(self) -> None:
        raise AssertionError("composition tests never close pools")


def test_composed_transaction_establishes_the_outer_transaction_itself() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, pool)) as scoped:
        with transaction(scoped) as first:
            assert first is conn
            first.execute("insert first write")
        with transaction(scoped) as second:
            assert second is conn
            second.execute("insert second write")

    # The FIRST block on the connection is the explicit outer transaction the
    # primitive opens before yielding; both repository scopes are savepoints
    # on it; the composition commits once and the connection returns to the
    # pool exactly once. A primitive that omitted its explicit block would
    # produce begin/commit per repository scope instead.
    assert conn.events == [
        "checkout",
        "begin",
        "savepoint:1",
        "execute",
        "release:1",
        "savepoint:1",
        "execute",
        "release:1",
        "commit",
        "return",
    ]
    assert pool.returned == 1


def test_connection_bound_pool_yields_the_exact_supplied_connection() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with (
        composed_transaction(cast(DatabasePool, pool)) as scoped,
        transaction(scoped) as yielded,
    ):
        assert yielded is conn

    assert conn.events == [
        "checkout",
        "begin",
        "savepoint:1",
        "release:1",
        "commit",
        "return",
    ]


def test_composition_rolls_back_the_whole_composition_when_a_later_write_fails() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with (
        pytest.raises(RuntimeError, match="second write failed"),
        composed_transaction(cast(DatabasePool, pool)) as scoped,
    ):
        with transaction(scoped) as first:
            first.execute("insert first write")
        with transaction(scoped) as second:
            second.execute("insert second write")
            raise RuntimeError("second write failed")

    # The failing scope rolls back only to its savepoint; the composition
    # block then rolls the whole outer transaction back, discarding the
    # first write's already-released effects with it.
    assert conn.events == [
        "checkout",
        "begin",
        "savepoint:1",
        "execute",
        "release:1",
        "savepoint:1",
        "execute",
        "rollback_to:1",
        "rollback",
        "return",
    ]
    assert pool.returned == 1


def test_handled_nested_failure_rolls_back_to_savepoint_and_keeps_outer_usable() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, pool)) as scoped:
        with transaction(scoped) as first:
            first.execute("insert first write")
        with (
            pytest.raises(RuntimeError, match="expected conflict"),
            transaction(scoped) as conflicting,
        ):
            conflicting.execute("insert conflicting write")
            raise RuntimeError("expected conflict")
        with transaction(scoped) as follow_up:
            follow_up.execute("insert follow-up write")

    # The handled failure rolled back only to its savepoint; the outer
    # transaction stayed usable and still committed the surviving writes.
    assert conn.events == [
        "checkout",
        "begin",
        "savepoint:1",
        "execute",
        "release:1",
        "savepoint:1",
        "execute",
        "rollback_to:1",
        "savepoint:1",
        "execute",
        "release:1",
        "commit",
        "return",
    ]


def test_scoped_pool_cannot_close_the_underlying_connection_or_pool() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, pool)) as scoped:
        with pytest.raises(RuntimeError, match="cannot close"):
            scoped.close()
        # Neither the connection state nor the pool checkout was disturbed.
        assert conn.events == ["checkout", "begin"]
        assert pool.returned == 0

    assert "commit" in conn.events
    assert pool.returned == 1


def test_scoped_pool_cannot_escape_the_outer_transaction_lifetime() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)
    leaked: DatabasePool | None = None

    with composed_transaction(cast(DatabasePool, pool)) as scoped:
        leaked = scoped
    assert leaked is not None

    with pytest.raises(RuntimeError, match="expired"), transaction(cast(DatabasePool, leaked)):
        pass  # pragma: no cover - the block can never be entered
