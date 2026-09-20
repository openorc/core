"""Transaction composition for application services.

Phase 1 repositories own short transactions: every repository function
opens its own ``transaction(pool)`` scope through the supplied process-local
``DatabasePool``. That is correct for one repository operation at a time,
but services also need atomic compositions of several existing repository
operations — for example a canonical mutation plus its WorkflowEvent
insert — without duplicating repository SQL or holding a transaction across
external I/O.

``composed_transaction`` is the one production-supported composition
primitive:

1. it opens ONE outer transaction from the process-local pool;
2. it yields a transaction-scoped ``DatabasePool`` view over that
   connection;
3. existing repository functions called through the view keep their public
   shape, and their ``transaction(...)`` scopes become nested psycopg
   transactions (SAVEPOINTs) on the outer transaction;
4. the whole composition commits together on clean exit, or rolls back
   together on error.

``pool.connection()`` only checks a connection out of the pool — it does
not establish a transaction. The explicit ``conn.transaction()`` entry
inside ``composed_transaction`` is what makes the composition one
transaction, and what guarantees repository scopes are nested SAVEPOINTs
rather than independent top-level transactions.

External-I/O rule (the boundary this module establishes; concrete external
workflows arrive with later service capabilities):

- database-only validation and mutation may be composed inside one short
  outer transaction;
- a GitHub, Supabase Auth HTTP, Cline/runtime, model, or any other external
  call occurs with NO database transaction open — never inside a
  ``composed_transaction`` block;
- after an external call, reload/reconcile current durable state before
  applying a consequential follow-up mutation;
- timeout or connection loss is never silently reclassified as success or
  a safe retry (see :mod:`openorc.services.errors`).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from psycopg import Connection

from openorc.persistence.pool import DatabasePool

__all__ = ["composed_transaction"]


class _ConnectionBoundPool:
    """Transaction-scoped ``DatabasePool`` view over one live connection.

    ``connection()`` opens a psycopg transaction block on the bound
    connection. Inside the ``composed_transaction`` outer block that is
    always a nested transaction (SAVEPOINT) — exactly what existing
    repository ``transaction(...)`` scopes need to participate in the
    composition while keeping their rollback-to-savepoint behavior on an
    expected inner failure.

    The view is deliberately not a pool: it never closes, replaces, or
    returns the underlying connection or the process pool, and it is
    expired when the outer transaction ends so it cannot escape the outer
    transaction's lifetime. Both obtaining a connection context and
    entering one re-check expiry, so a context obtained while the view was
    active cannot open the bound connection after the composition has
    ended.
    """

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection
        self._active = True

    def connection(self) -> AbstractContextManager[Connection[Any]]:
        self._require_active()

        @contextmanager
        def scoped() -> Iterator[Connection[Any]]:
            # Re-checked at entry: a context manager obtained while the view
            # was active must not open the bound connection's transaction
            # after the outer composition has ended — the connection has
            # returned to the process pool by then and may be reused
            # elsewhere. This is a nested transaction block on the
            # already-open outer transaction: psycopg turns it into a
            # SAVEPOINT whose release/rollback is confined to this
            # repository scope.
            self._require_active()
            with self._connection.transaction():
                yield self._connection

        return scoped()

    def close(self) -> None:
        raise RuntimeError(
            "a transaction-scoped pool view cannot close anything: the "
            "underlying connection and process pool are owned elsewhere"
        )

    def _expire(self) -> None:
        self._active = False

    def _require_active(self) -> None:
        if not self._active:
            raise RuntimeError(
                "the transaction-scoped pool view is expired: it cannot be "
                "used outside the composed_transaction block that yielded it"
            )


@contextmanager
def composed_transaction(pool: DatabasePool) -> Iterator[DatabasePool]:
    """Compose repository operations inside ONE explicit outer transaction.

    Checks one connection out of the process-local pool, explicitly enters
    a transaction block on it, and yields a transaction-scoped
    :class:`~openorc.persistence.pool.DatabasePool` view over that exact
    connection. Repository functions called through the view run their
    existing ``transaction(...)`` scopes as nested psycopg transactions
    (SAVEPOINTs), so a handled inner failure rolls back only to its own
    savepoint while the outer transaction stays usable.

    On clean exit the outer transaction commits — every composed repository
    effect together. On error it rolls back — every composed effect
    together. The connection returns to the pool in every case; the yielded
    view expires when the block ends and must not be used afterwards.

    The explicit outer transaction block is load-bearing:
    ``pool.connection()`` establishes no transaction of its own, so without
    it each repository scope would become an independent top-level
    transaction and the composition would silently lose atomicity.

    Keep the block short and database-only: never hold it open across a
    GitHub, Supabase Auth HTTP, Cline/runtime, model, or any other external
    call. See the module docstring for the external-I/O rule.
    """
    with pool.connection() as conn, conn.transaction():
        scoped = _ConnectionBoundPool(conn)
        try:
            yield scoped
        finally:
            scoped._expire()
