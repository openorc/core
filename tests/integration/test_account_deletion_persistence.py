"""Integration-marked tests for the account-deletion persistence surface (#97).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
invariants the deterministic suite cannot: the composite CHECK that makes
impossible attempt-state tuples unrepresentable, the Workspace-root lock that
keeps a child Connection from entering the aggregate after the cleanup set is
established, the account-deletion Profile lock serializing guarded Owner
mutations, the attempt-state claim/clear lifecycle, and the Auth-root cascade
removing the attempt state together with the Profile. They are excluded from
the ordinary deterministic baseline by the repository pytest configuration.

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
from psycopg.errors import CheckViolation, LockNotAvailable, QueryCanceled

from openorc.domain.connections import AdapterType
from openorc.persistence import connections as connection_repositories
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational

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


def _seed_account(conn: Connection[Any], user_id: uuid.UUID) -> None:
    """Create one auth user + Profile pair on the given connection."""
    conn.execute("insert into auth.users (id) values (%s)", (user_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (user_id,))


def _apply_probe_timeouts(conn: Connection[Any]) -> None:
    """Apply the deliberate lock-probe timeouts with transaction-local SQL.

    The suite consumes the Supabase branch database through its pooler URL,
    where connection-startup ``options`` are not reliably forwarded, so the
    timeouts are set inside the racer's own transaction: they are then
    guaranteed to be in force on the exact server connection that executes
    the blocked statement. Whichever timeout raises first — ``lock_timeout``
    maps to ``LockNotAvailable`` and ``statement_timeout`` to
    ``QueryCanceled`` — is the blocking signal the racers treat as
    ``blocked``; without one, the probe would wait indefinitely on the
    deliberately held root lock.
    """
    conn.execute("set local lock_timeout = '500ms'")
    conn.execute("set local statement_timeout = '2s'")


def test_composite_check_rejects_impossible_state_tuples(conn: Connection[Any]) -> None:
    user_id = uuid.uuid4()
    _seed_account(conn, user_id)

    # Each deliberately rejected tuple is attempted inside a nested
    # transaction (a SAVEPOINT on this connection): the expected
    # CheckViolation rolls back only that savepoint, so the outer fixture
    # transaction — and the seeded account inside it — survives every probe.
    # A transaction-abort rollback here would also drop the seed, silently
    # turning a later invalid UPDATE into a zero-row write that never
    # evaluates the CHECK.

    # state without the attempt UUID/timestamp is unrepresentable.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.profiles set account_deletion_state = 'active' where id = %s",
            (user_id,),
        )

    # A state without the establishment time is unrepresentable.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.profiles "
            "set account_deletion_state = 'active', account_deletion_attempt_id = %s "
            "where id = %s",
            (uuid.uuid4(), user_id),
        )

    # A non-NULL attempt UUID with a NULL state is unrepresentable.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.profiles set account_deletion_attempt_id = %s where id = %s",
            (uuid.uuid4(), user_id),
        )

    # The coherent 'active' tuple is representable.
    attempt_id = uuid.uuid4()
    conn.execute(
        "update openorc.profiles "
        "set account_deletion_state = 'active', account_deletion_attempt_id = %s, "
        "account_deletion_started_at = now() where id = %s",
        (attempt_id, user_id),
    )
    row = conn.execute(
        "select account_deletion_state, account_deletion_attempt_id, "
        "account_deletion_started_at is not null from openorc.profiles where id = %s",
        (user_id,),
    ).fetchone()
    assert row is not None
    assert row == ("active", attempt_id, True)


def test_attempt_state_claim_and_clear_round_trip(conn: Connection[Any]) -> None:
    user_id = uuid.uuid4()
    _seed_account(conn, user_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    attempt_id = uuid.uuid4()
    claimed = ownership_repositories.claim_account_deletion_attempt(
        pool, profile_id=user_id, attempt_id=attempt_id
    )
    assert claimed is True

    exists, state = ownership_repositories.read_account_deletion_state_for_key_share(
        pool, profile_id=user_id
    )
    assert exists is True and state is not None
    assert state.state == "active" and state.attempt_id == attempt_id

    cleared = ownership_repositories.clear_account_deletion_attempt(
        pool, profile_id=user_id, attempt_id=attempt_id
    )
    assert cleared is True
    exists, state = ownership_repositories.read_account_deletion_state_for_key_share(
        pool, profile_id=user_id
    )
    assert exists is True and state is None


def test_account_barrier_fails_closed_on_attempt_state(conn: Connection[Any]) -> None:
    user_id = uuid.uuid4()
    _seed_account(conn, user_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    attempt_id = uuid.uuid4()
    assert ownership_repositories.claim_account_deletion_attempt(
        pool, profile_id=user_id, attempt_id=attempt_id
    )

    with pytest.raises(ConflictError, match="account is being deleted"):
        require_account_operational(pool, profile_id=user_id)

    cleared = ownership_repositories.clear_account_deletion_attempt(
        pool, profile_id=user_id, attempt_id=attempt_id
    )
    assert cleared is True
    require_account_operational(pool, profile_id=user_id)  # resumes


def test_account_barrier_fails_closed_for_a_missing_profile(conn: Connection[Any]) -> None:
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    with pytest.raises(NotFoundError):
        require_account_operational(pool, profile_id=uuid.uuid4())


def test_auth_root_cascade_removes_the_attempt_state_with_the_profile(
    conn: Connection[Any],
) -> None:
    user_id = uuid.uuid4()
    _seed_account(conn, user_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    attempt_id = uuid.uuid4()
    assert ownership_repositories.claim_account_deletion_attempt(
        pool, profile_id=user_id, attempt_id=attempt_id
    )

    # The sanctioned Auth-root deletion: the attempt state dies with the
    # Profile through the auth.users -> profiles ON DELETE CASCADE.
    conn.execute("delete from auth.users where id = %s", (user_id,))
    row = conn.execute("select count(*) from openorc.profiles where id = %s", (user_id,)).fetchone()
    assert row is not None
    assert row[0] == 0


def test_workspace_root_lock_blocks_child_inserts_during_the_cleanup_set(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    # Concurrency regression: while the Workspace root is locked across the
    # cleanup composition, a concurrent child INSERT (create_connection)
    # blocks — its foreign-key check needs a conflicting FOR KEY SHARE lock
    # on the parent row — so a Connection cannot enter the aggregate after
    # the cleanup set is established.
    user_id = uuid.uuid4()
    _seed_account(conn, user_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    workspace = ownership_repositories.create_workspace(pool, owner_profile_id=user_id, name="w")
    # The parent rows must be visible to the racer; commit and clean up.
    conn.commit()

    racer_outcomes: list[str] = []
    errors: list[BaseException] = []
    start = threading.Event()

    def racer() -> None:
        try:
            with connect(migrated_database) as racer_conn:
                start.wait()
                try:
                    with racer_conn.transaction():
                        _apply_probe_timeouts(racer_conn)
                        connection_repositories.create_connection(
                            cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                            workspace_id=workspace.id,
                            adapter=AdapterType.CLINE,
                            name="late-entry",
                        )
                    racer_outcomes.append("allowed")
                except (LockNotAvailable, QueryCanceled):
                    # A deliberate probe timeout raising on the blocked INSERT
                    # is direct evidence the Workspace-root lock held: either
                    # lock_timeout (LockNotAvailable) or statement_timeout
                    # (QueryCanceled) may fire first, and both count as
                    # blocked. QueryCanceled is accepted ONLY in this
                    # deliberate timeout-based contention probe; every other
                    # database error still surfaces through the collector
                    # below.
                    racer_outcomes.append("blocked")
        except BaseException as exc:  # noqa: BLE001 — collected and asserted below
            errors.append(exc)

    # Hold the root lock on the fixture connection (no commit: the lock is
    # held while the racer attempts its child INSERT).
    locked = ownership_repositories.get_workspace_for_update(pool, workspace.id)
    assert locked is not None

    thread = threading.Thread(target=racer)
    thread.start()
    start.set()
    thread.join()

    try:
        assert errors == []
        assert racer_outcomes == ["blocked"]
    finally:
        # The committed workspace rows are removed explicitly (the fixture
        # rollback cannot undo committed state; the auth-user delete cascades
        # the graph through the sanctioned FK).
        conn.execute("delete from auth.users where id = %s", (user_id,))
        conn.commit()


def test_account_deletion_profile_lock_serializes_guarded_mutations(
    conn: Connection[Any],
    migrated_database: str,
) -> None:
    # Concurrency regression: the Profile-root FOR UPDATE conflicts with the
    # guard's FOR KEY SHARE read — a guarded Owner mutation concurrent with
    # the in-flight revocation transaction blocks and then fails closed.
    user_id = uuid.uuid4()
    _seed_account(conn, user_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    conn.commit()

    racer_outcomes: list[str] = []
    errors: list[BaseException] = []
    start = threading.Event()

    def racer() -> None:
        try:
            with connect(migrated_database) as racer_conn:
                start.wait()
                try:
                    with racer_conn.transaction():
                        _apply_probe_timeouts(racer_conn)
                        require_account_operational(
                            cast(DatabasePool, _SingleConnectionPool(racer_conn)),
                            profile_id=user_id,
                        )
                    racer_outcomes.append("passed")
                except (LockNotAvailable, QueryCanceled):
                    # A deliberate probe timeout raising on the blocked
                    # FOR KEY SHARE read is direct evidence the Profile-root
                    # lock held: either lock_timeout (LockNotAvailable) or
                    # statement_timeout (QueryCanceled) may fire first, and
                    # both count as blocked. QueryCanceled is accepted ONLY
                    # in this deliberate timeout-based contention probe;
                    # every other database error still surfaces through the
                    # collector below.
                    racer_outcomes.append("blocked")
                except ConflictError:
                    racer_outcomes.append("rejected")
        except BaseException as exc:  # noqa: BLE001 — collected and asserted below
            errors.append(exc)

    locked = ownership_repositories.get_profile_for_account_deletion(
        pool, profile_id=user_id, lease_seconds=15.0
    )
    assert locked is not None

    thread = threading.Thread(target=racer)
    thread.start()
    start.set()
    thread.join()

    try:
        assert errors == []
        assert racer_outcomes in (["blocked"], ["rejected"])
    finally:
        conn.rollback()
        conn.execute("delete from auth.users where id = %s", (user_id,))
        conn.commit()
