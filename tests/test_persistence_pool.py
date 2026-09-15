"""Deterministic tests for process-local Postgres pool ownership.

A connection pool belongs to exactly one OS process and must never be
inherited or reused across an RQ fork boundary (the default RQ worker forks a
child process per job). These tests prove the accessor semantics with
injected pool/pid seams; no database and no real pool is ever touched.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import pytest

from openorc.config import Settings
from openorc.persistence import pool as pool_module
from openorc.persistence.pool import (
    DatabasePool,
    PoolOwnershipError,
    PoolSpec,
    close_database_pool,
    default_pool_factory,
    derive_pool_spec,
    get_database_pool,
    open_database_pool,
)


class FakePool:
    """Records its construction spec and every method invoked on it."""

    def __init__(self, spec: PoolSpec) -> None:
        self.spec = spec
        self.events: list[str] = []

    def connection(self) -> Any:
        self.events.append("connection")
        raise AssertionError("pool ownership tests never open connections")

    def close(self) -> None:
        self.events.append("close")


def make_factory(recorder: list[PoolSpec]) -> Callable[[PoolSpec], DatabasePool]:
    def factory(spec: PoolSpec) -> DatabasePool:
        recorder.append(spec)
        return cast(DatabasePool, FakePool(spec))

    return factory


@pytest.fixture(autouse=True)
def _fresh_pool_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pool_module, "_pool", None)
    monkeypatch.setattr(pool_module, "_pool_owner_pid", None)


def test_first_access_constructs_pool_in_current_process(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory(
        database_url="postgresql://u:p@db.example.com:5432/openorc",
        db_pool_min=2,
        db_pool_max=7,
        db_pool_timeout=9.5,
    )

    pool = get_database_pool(
        settings,
        pool_factory=make_factory(specs),
        pid_provider=lambda: 4242,
    )

    assert isinstance(pool, FakePool)
    assert specs == [
        PoolSpec(
            conninfo="postgresql://u:p@db.example.com:5432/openorc",
            min_size=2,
            max_size=7,
            timeout=9.5,
            kwargs={"prepare_threshold": None},
        )
    ]


def test_repeat_access_returns_same_pool_without_reconstruction(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    first = get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)
    second = get_database_pool(
        settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242
    )

    assert first is second
    assert len(specs) == 1


def test_changed_process_identity_cannot_reuse_prior_pool(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    stale = get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)

    with pytest.raises(PoolOwnershipError, match="4242"):
        get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 9999)

    # The fork-inherited pool is abandoned untouched: never closed, never used.
    assert isinstance(stale, FakePool)
    assert stale.events == []


def test_open_database_pool_refuses_foreign_inherited_pool(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    stale = get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)

    with pytest.raises(PoolOwnershipError, match="4242"):
        open_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 9999)

    # Fail closed: the inherited pool was not replaced, closed, or touched in
    # any way, and the new process's pool factory was never invoked.
    assert len(specs) == 1
    assert isinstance(stale, FakePool)
    assert stale.events == []


def test_open_database_pool_constructs_when_no_pool_was_inherited(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    # A parent that established no pool before forking leaves its child free
    # to create and register its own process-local pool.
    fresh = open_database_pool(
        settings, pool_factory=make_factory(specs), pid_provider=lambda: 9999
    )
    reused = get_database_pool(
        settings, pool_factory=make_factory(specs), pid_provider=lambda: 9999
    )

    assert len(specs) == 1
    assert isinstance(fresh, FakePool)
    assert reused is fresh


def test_open_database_pool_rejects_second_pool_in_same_process(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    open_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)

    with pytest.raises(PoolOwnershipError, match="already owns"):
        open_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)

    assert len(specs) == 1


def test_close_database_pool_closes_only_the_owned_pool(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    owned = get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)
    close_database_pool(pid_provider=lambda: 4242)

    assert isinstance(owned, FakePool)
    assert owned.events == ["close"]
    # State is reset: the next access constructs a fresh pool for the process.
    again = get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)
    assert again is not owned
    assert len(specs) == 2


def test_close_database_pool_refuses_foreign_pool(
    settings_factory: Callable[..., Settings],
) -> None:
    specs: list[PoolSpec] = []
    settings = settings_factory()

    stale = get_database_pool(settings, pool_factory=make_factory(specs), pid_provider=lambda: 4242)

    with pytest.raises(PoolOwnershipError, match="4242"):
        close_database_pool(pid_provider=lambda: 9999)

    assert isinstance(stale, FakePool)
    assert stale.events == []


def test_derive_pool_spec_is_settings_only(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(
        database_url="postgresql://u:p@h:5432/db",
        db_pool_min=3,
        db_pool_max=9,
        db_pool_timeout=4.5,
    )

    spec = derive_pool_spec(settings)

    assert spec == PoolSpec(
        conninfo="postgresql://u:p@h:5432/db",
        min_size=3,
        max_size=9,
        timeout=4.5,
        kwargs={"prepare_threshold": None},
    )


def test_default_factory_wires_native_pool_for_process_local_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, Any] = {}

    class StubNativePool:
        def __init__(self, **kwargs: Any) -> None:
            recorded.update(kwargs)

    monkeypatch.setattr(pool_module, "ConnectionPool", StubNativePool)

    pool = default_pool_factory(
        PoolSpec(
            conninfo="postgresql://u:p@h:5432/db",
            min_size=2,
            max_size=6,
            timeout=11.0,
            kwargs={"prepare_threshold": None},
        )
    )

    assert isinstance(pool, StubNativePool)
    # Explicit open=True: the pool opens in the owning process during the
    # first accessor call, without psycopg_pool's deprecated implicit default.
    assert recorded == {
        "conninfo": "postgresql://u:p@h:5432/db",
        "min_size": 2,
        "max_size": 6,
        "timeout": 11.0,
        "kwargs": {"prepare_threshold": None},
        "open": True,
    }
