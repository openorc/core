"""Thin transaction boundary over psycopg_pool connection-context semantics."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg import Connection

from openorc.persistence.pool import DatabasePool

__all__ = ["transaction"]


@contextmanager
def transaction(pool: DatabasePool) -> Iterator[Connection[Any]]:
    """Yield one pooled connection as a short, atomic transaction boundary.

    Delegates entirely to psycopg_pool's native connection-context semantics:
    the transaction commits on clean exit, rolls back on error, and the
    connection returns to the pool in every case. OpenOrc deliberately adds no
    transaction machinery of its own and no auto-commit behavior; isolation is
    the Postgres default (``READ COMMITTED``).

    The yielded transaction must stay short and must never remain open across
    an external call (GitHub, Cline, model inference, or any other external
    system). Row locks, where the domain requires them, are semantic
    ``SELECT ... FOR UPDATE`` statements authored by repositories inside this
    boundary.
    """
    with pool.connection() as conn:
        yield conn
