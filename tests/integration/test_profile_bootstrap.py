"""Integration-marked tests for Profile resolution and bootstrap (issue #52).

These tests run against the explicitly supplied non-production Supabase branch
database and prove the durable authentication bootstrap invariants directly:
idempotent ``ensure_profile`` bootstrap, exactly-one-Profile convergence under
concurrent first requests, and the fail-closed account-identity boundary — a
Profile is never created without its backing ``auth.users`` row, and the
service translates that FK violation into a typed authentication failure.

Run explicitly when a target has been made available (see tests/README.md).
The suite consumes the database it is given and never provisions one; it
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import ForeignKeyViolation

from openorc.domain.identity import AuthenticatedPrincipal
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services.authentication import authenticate
from openorc.services.errors import AuthenticationError

# Every test in this module requires the explicitly supplied non-production
# branch database (see tests/integration/conftest.py).
pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


class _StaticVerifier:
    """Minimal verifier double: always returns the fixed principal."""

    def __init__(self, principal: AuthenticatedPrincipal) -> None:
        self._principal = principal

    def verify(self, token: str) -> AuthenticatedPrincipal:
        return self._principal


def test_ensure_profile_bootstraps_once_and_is_idempotent(conn: Connection[Any]) -> None:
    user_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): every Profile needs its
    # backing Auth user row. The inserts roll back with the test transaction.
    conn.execute("insert into auth.users (id) values (%s)", (user_id,))
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    first = ownership_repositories.ensure_profile(pool, profile_id=user_id)
    second = ownership_repositories.ensure_profile(pool, profile_id=user_id)

    assert first.id == user_id
    assert second == first

    count = conn.execute(
        "select count(*) from openorc.profiles where id = %s", (user_id,)
    ).fetchone()
    assert count is not None
    assert count[0] == 1


def test_ensure_profile_fails_closed_without_the_backing_auth_user(
    conn: Connection[Any],
) -> None:
    with pytest.raises(ForeignKeyViolation):
        ownership_repositories.ensure_profile(
            cast(DatabasePool, _SingleConnectionPool(conn)), profile_id=uuid.uuid4()
        )


def test_service_authentication_fails_closed_for_a_deleted_auth_user(
    conn: Connection[Any],
) -> None:
    # A permanently deleted Auth user with a still-valid JWT: Profile
    # bootstrap is impossible (FK) and the service translates the violation
    # into a typed authentication failure instead of recreating the identity.
    principal = AuthenticatedPrincipal(user_id=uuid.uuid4())
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    with pytest.raises(AuthenticationError, match="no longer exists"):
        authenticate(
            pool,
            cast(Any, _StaticVerifier(principal)),
            token="unused-by-the-static-verifier",
        )


def test_service_authentication_resolves_the_bootstrapped_profile(
    conn: Connection[Any],
) -> None:
    user_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (user_id,))
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    verifier = _StaticVerifier(AuthenticatedPrincipal(user_id=user_id))

    result = authenticate(pool, cast(Any, verifier), token="unused-by-the-static-verifier")

    assert result.principal.user_id == user_id
    assert result.profile.id == user_id


def test_concurrent_bootstrap_converges_on_exactly_one_profile(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    user_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (user_id,))
    # The backing auth user must be visible to the racing connections, so this
    # one insert is committed; the test removes every row it created before
    # returning (see the cleanup at the end).
    conn.commit()

    profiles: list[uuid.UUID] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def bootstrap() -> None:
        try:
            with connect(migrated_database) as racer:
                barrier.wait()
                profile = ownership_repositories.ensure_profile(
                    cast(DatabasePool, _SingleConnectionPool(racer)), profile_id=user_id
                )
            profiles.append(profile.id)
        except BaseException as exc:  # noqa: BLE001 — collected and asserted below
            errors.append(exc)

    threads = [threading.Thread(target=bootstrap) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    try:
        assert errors == []
        # Both racers resolved the same single Profile: the unique index
        # serializes the race and the loser's on-conflict insert no-ops.
        assert profiles == [user_id, user_id]
        row = conn.execute(
            "select count(*) from openorc.profiles where id = %s", (user_id,)
        ).fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        # Explicit cleanup: the committed race rows cannot be undone by the
        # fixture rollback, so the account graph is deleted here (the auth
        # user delete cascades the Profile through the sanctioned FK).
        conn.execute("delete from openorc.profiles where id = %s", (user_id,))
        conn.execute("delete from auth.users where id = %s", (user_id,))
        conn.commit()
