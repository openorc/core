"""Deterministic tests for the thin transaction boundary.

``transaction()`` must delegate entirely to psycopg_pool's native
connection-context semantics: commit on clean exit, rollback on error,
connection returned to the pool. The fakes emulate exactly those native
semantics, so the tests prove OpenOrc adds no transaction machinery of its
own and establishes no auto-commit behavior.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, cast

import pytest

from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction


class FakeConnection:
    """Emulates psycopg Connection context semantics, recording calls."""

    def __init__(self) -> None:
        self.events: list[str] = []

    def commit(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")

    def execute(self, *args: Any, **kwargs: Any) -> None:
        self.events.append("execute")

    def __enter__(self) -> FakeConnection:
        self.events.append("enter")
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        # psycopg's native connection-context behavior: commit on success,
        # rollback on error.
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.events.append("exit")


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn
        self.returned = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            try:
                with self._conn:
                    yield self._conn
            finally:
                self.returned += 1  # connection returned to the pool

        return managed()

    def close(self) -> None:
        raise AssertionError("transaction tests never close pools")


def test_transaction_yields_connection_and_delegates_commit() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with transaction(cast(DatabasePool, pool)) as yielded:
        assert yielded is conn
        yielded.execute("select 1")

    assert conn.events == ["enter", "execute", "commit", "exit"]
    assert pool.returned == 1


def test_transaction_rolls_back_and_propagates_on_error() -> None:
    conn = FakeConnection()
    pool = FakePool(conn)

    with pytest.raises(RuntimeError, match="boom"), transaction(cast(DatabasePool, pool)):
        raise RuntimeError("boom")

    assert conn.events == ["enter", "rollback", "exit"]
    assert pool.returned == 1
