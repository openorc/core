"""Process-local Postgres connection pooling for OpenOrc persistence.

A connection pool belongs to exactly one OS process. The ``ConnectionPool``
object is created in the OS process that uses it, after any fork that process
has gone through: it must never exist somewhere it could be inherited across
an RQ fork boundary (the default RQ worker forks a child process per job), so
an RQ parent must not establish database pool/connection state before forking
its work horse.

Access is process-aware: the accessor tracks the owning PID and fails closed
on PID change. A changed process identity can never reuse, replace, or close
the previous pool; a stale fork-inherited pool is abandoned untouched. A
process creates a pool through :func:`open_database_pool` only when its
parent established no pool before the fork.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Protocol

from psycopg import Connection
from psycopg_pool import ConnectionPool

from openorc.config import Settings

__all__ = [
    "DatabasePool",
    "PoolOwnershipError",
    "PoolSpec",
    "close_database_pool",
    "default_pool_factory",
    "derive_pool_spec",
    "get_database_pool",
    "open_database_pool",
]

# Pooler-neutral connection defaults. Prepared statements are disabled because
# pooling middleware cannot be assumed to support them (see psycopg prepared
# statement documentation); no session-local state (search_path, SET/GUCs) is
# ever relied upon, so direct, session-pooler, and transaction-pooler
# endpoints behave identically.
CONNECTION_KWARGS: Mapping[str, Any] = {"prepare_threshold": None}


class PoolOwnershipError(Exception):
    """Raised when pool access would cross an OS process boundary."""


class DatabasePool(Protocol):
    """Structural contract of the OpenOrc process-local pool boundary."""

    def connection(self) -> AbstractContextManager[Connection[Any]]: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PoolSpec:
    """Derived construction inputs for one process-local pool."""

    conninfo: str
    min_size: int
    max_size: int
    timeout: float
    kwargs: Mapping[str, Any]


def derive_pool_spec(settings: Settings) -> PoolSpec:
    """Derive one process's pool construction inputs from its settings."""
    return PoolSpec(
        conninfo=settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        timeout=settings.db_pool_timeout,
        kwargs=dict(CONNECTION_KWARGS),
    )


def default_pool_factory(spec: PoolSpec) -> ConnectionPool[Connection[Any]]:
    """Build the real psycopg_pool pool. Called in the owning process only.

    ``open=True`` is explicit so the pool opens here, in the process that
    requested it, without relying on psycopg_pool's deprecated implicit-open
    default. Bounds are per process by construction.
    """
    return ConnectionPool(
        conninfo=spec.conninfo,
        min_size=spec.min_size,
        max_size=spec.max_size,
        timeout=spec.timeout,
        kwargs=dict(spec.kwargs),
        open=True,
    )


PoolFactory = Callable[[PoolSpec], DatabasePool]
PidProvider = Callable[[], int]

# Single process-local pool state. After a fork, the child sees a copy of
# these values with the parent's owner PID, which is exactly what makes the
# fail-closed ownership check below refuse inherited pools.
_pool: DatabasePool | None = None
_pool_owner_pid: int | None = None
_state_lock = threading.Lock()


def _pid(pid_provider: PidProvider | None) -> int:
    return os.getpid() if pid_provider is None else pid_provider()


def _resolve(settings: Settings | None) -> Settings:
    return Settings.from_env() if settings is None else settings


def get_database_pool(
    settings: Settings | None = None,
    *,
    pool_factory: PoolFactory | None = None,
    pid_provider: PidProvider | None = None,
) -> DatabasePool:
    """Return this process's connection pool, constructing it on first use.

    The pool object is created here, in the calling process, on the first
    access. A pool created by another process (for example inherited across an
    RQ fork boundary) is never returned and never touched: access fails closed
    with :class:`PoolOwnershipError`.
    """
    global _pool, _pool_owner_pid

    pid = _pid(pid_provider)

    # Fail closed before taking the lock so a forked child never blocks on (or
    # otherwise touches) pool state owned by its parent process.
    if _pool is not None and _pool_owner_pid != pid:
        raise PoolOwnershipError(
            f"database pool is owned by PID {_pool_owner_pid}; PID {pid} cannot "
            "reuse it across a process boundary"
        )

    with _state_lock:
        if _pool is not None:
            if _pool_owner_pid != pid:
                raise PoolOwnershipError(
                    f"database pool is owned by PID {_pool_owner_pid}; PID {pid} "
                    "cannot reuse it across a process boundary"
                )
            return _pool

        factory = default_pool_factory if pool_factory is None else pool_factory
        pool = factory(derive_pool_spec(_resolve(settings)))
        _pool = pool
        _pool_owner_pid = pid
        return pool


def open_database_pool(
    settings: Settings | None = None,
    *,
    pool_factory: PoolFactory | None = None,
    pid_provider: PidProvider | None = None,
) -> DatabasePool:
    """Construct a pool owned by the current process and register it.

    Process-entry code in a fresh process (for example, later RQ job-child
    wiring) may create its pool here only when the parent process established
    no pool before the fork. A pool registered by another process is never
    replaced, closed, or otherwise touched: access fails closed with
    :class:`PoolOwnershipError`. A process that already owns a registered pool
    must use :func:`get_database_pool`.
    """
    global _pool, _pool_owner_pid

    pid = _pid(pid_provider)

    # Fail closed before taking the lock, mirroring get_database_pool: a
    # forked child must never block on pool state owned by its parent process.
    if _pool is not None and _pool_owner_pid != pid:
        raise PoolOwnershipError(
            f"database pool is owned by PID {_pool_owner_pid}; PID {pid} cannot "
            "replace it across a process boundary"
        )

    with _state_lock:
        if _pool is not None:
            if _pool_owner_pid == pid:
                raise PoolOwnershipError(
                    f"PID {pid} already owns a database pool; use get_database_pool()"
                )
            raise PoolOwnershipError(
                f"database pool is owned by PID {_pool_owner_pid}; PID {pid} cannot "
                "replace it across a process boundary"
            )

        factory = default_pool_factory if pool_factory is None else pool_factory
        pool = factory(derive_pool_spec(_resolve(settings)))
        _pool = pool
        _pool_owner_pid = pid
        return pool


def close_database_pool(*, pid_provider: PidProvider | None = None) -> None:
    """Close the pool owned by the current process, if one exists.

    Closing refuses a foreign owner: a fork-inherited pool is never closed
    from the process that did not create it.
    """
    global _pool, _pool_owner_pid

    pid = _pid(pid_provider)

    with _state_lock:
        if _pool is None:
            return
        if _pool_owner_pid != pid:
            raise PoolOwnershipError(
                f"database pool is owned by PID {_pool_owner_pid}; PID {pid} "
                "cannot close it across a process boundary"
            )
        _pool.close()
        _pool = None
        _pool_owner_pid = None
