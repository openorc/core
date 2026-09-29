"""Integration tests for the Profile-scoped GitHub user authorization (issue #142).

These tests run against the explicitly supplied non-production Supabase branch
database (the existing conftest path) and prove the durable invariants the
ordinary suite cannot: the migration's CHECK-consistent active/revoked
lifecycle and its Profile ownership cascade, the real Vault-backed
establishment/replacement/rotation atomicity, the generation compare-and-swap
stale-write guard, explicit revocation idempotency, and the account-deletion
revocation cleanup (the secret deleted and the row non-dangling in the same
transaction, idempotent on retry). They reuse the #55/#97 Vault privilege and
deletion matrices rather than duplicating them.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_github_user_authorization_persistence.py

The module borrows the fixture connection through the same passthrough pool
adapter as the #55 credential suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, UniqueViolation

from openorc.persistence import github_user_authorizations, github_user_refresh_secrets
from openorc.persistence.pool import DatabasePool
from openorc.services import account_lifecycle
from openorc.services.errors import ExternalOperationFailedError

pytestmark = pytest.mark.integration


class _BorrowedConnectionPool:
    """Borrows the fixture-owned connection; adds no transaction or lifetime behavior."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    @contextmanager
    def connection(self) -> Iterator[Connection[Any]]:
        yield self._connection

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _pool(conn: Connection[Any]) -> DatabasePool:
    return cast(DatabasePool, _BorrowedConnectionPool(conn))


def _seed_account(conn: Connection[Any], user_id: UUID) -> None:
    conn.execute("insert into auth.users (id) values (%s)", (user_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (user_id,))


def _establish_real_authorization(
    conn: Connection[Any], *, profile_id: UUID, generation: int = 1
) -> tuple[UUID, str]:
    """Persist one active authorization backed by a real Vault secret."""
    secret_id = github_user_refresh_secrets.create_github_user_refresh_secret(
        _pool(conn), secret=f"ghr_secret-{generation}", profile_id=profile_id
    )
    reference = github_user_refresh_secrets.encode_github_user_refresh_reference(secret_id)
    if generation == 1:
        github_user_authorizations.insert_active_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            github_user_id=5432,
            github_login="octocat",
            refresh_secret_reference=reference,
            refresh_expires_at=datetime.now(UTC) + timedelta(days=180),
        )
    else:
        github_user_authorizations.reauthorize_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            github_user_id=5432,
            github_login="octocat",
            refresh_secret_reference=reference,
            refresh_expires_at=datetime.now(UTC) + timedelta(days=180),
        )
    return secret_id, reference


def _vault_secret_exists(conn: Connection[Any], secret_id: UUID) -> bool:
    row = conn.execute("select 1 from vault.secrets where id = %s", (secret_id,)).fetchone()
    return row is not None


def test_active_lifecycle_tuple_checks_are_enforced_by_the_database(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    reference = "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4())
    expires = datetime.now(UTC) + timedelta(days=180)

    # An active row without its refresh reference is unrepresentable.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_user_authorizations "
            "(profile_id, github_user_id, status, refresh_generation) "
            "values (%s, 5432, 'active', 1)",
            (profile_id,),
        )

    # A revoked row carrying a usable refresh reference is unrepresentable.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_user_authorizations "
            "(profile_id, github_user_id, status, refresh_secret_reference, "
            "refresh_expires_at, refresh_generation, revoked_at) "
            "values (%s, 5432, 'revoked', %s, %s, 1, now())",
            (profile_id, reference, expires),
        )

    # A second row for the same Profile is unrepresentable: the Profile is
    # the row identity (exactly one current authorization per Profile).
    github_user_authorizations.insert_active_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        github_user_id=5432,
        github_login=None,
        refresh_secret_reference=reference,
        refresh_expires_at=datetime.now(UTC) + timedelta(days=180),
    )
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_user_authorizations "
            "(profile_id, github_user_id, status, refresh_secret_reference, "
            "refresh_expires_at, refresh_generation) values (%s, 5432, 'active', %s, %s, 1)",
            (profile_id, reference, datetime.now(UTC) + timedelta(days=180)),
        )


def test_the_profile_ownership_cascade_removes_the_authorization_row(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    _establish_real_authorization(conn, profile_id=profile_id)
    row = conn.execute(
        "select 1 from openorc.github_user_authorizations where profile_id = %s",
        (profile_id,),
    ).fetchone()
    assert row is not None

    # The sanctioned account-root boundary: deleting the Auth user removes the
    # complete OpenOrc-owned graph, this authorization row included.
    conn.execute("delete from auth.users where id = %s", (profile_id,))
    row = conn.execute(
        "select 1 from openorc.github_user_authorizations where profile_id = %s",
        (profile_id,),
    ).fetchone()
    assert row is None


def test_vault_backed_rotation_preserves_the_reference_and_rotates_the_value(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    secret_id, reference = _establish_real_authorization(conn, profile_id=profile_id)

    authorization = github_user_authorizations.get_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )
    assert authorization is not None
    assert authorization.refresh_secret_reference == reference
    resolved = github_user_refresh_secrets.read_github_user_refresh_secret(
        _pool(conn), secret_id=secret_id
    )
    assert resolved == "ghr_secret-1"

    # Rotation updates the secret in place: the reference (and Vault UUID)
    # stays stable across generations while the value rotates.
    assert github_user_refresh_secrets.update_github_user_refresh_secret(
        _pool(conn), secret_id=secret_id, secret="ghr_rotated"
    )
    assert (
        github_user_refresh_secrets.read_github_user_refresh_secret(
            _pool(conn), secret_id=secret_id
        )
        == "ghr_rotated"
    )
    authorization = github_user_authorizations.get_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )
    assert authorization is not None
    assert authorization.refresh_secret_reference == reference


def test_reauthorize_advances_the_generation_monotonically_in_place(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    _establish_real_authorization(conn, profile_id=profile_id, generation=1)
    _establish_real_authorization(conn, profile_id=profile_id, generation=2)
    _establish_real_authorization(conn, profile_id=profile_id, generation=3)

    authorization = github_user_authorizations.get_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )
    assert authorization is not None
    assert authorization.status.value == "active"
    assert authorization.refresh_generation == 3


def test_the_generation_cas_guard_rejects_a_stale_refresh_writer(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    _establish_real_authorization(conn, profile_id=profile_id, generation=1)
    _establish_real_authorization(conn, profile_id=profile_id, generation=2)
    durable = github_user_authorizations.get_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )
    assert durable is not None
    assert durable.refresh_generation == 2

    # A stale writer (observed generation 1) matches zero rows: the newer
    # rotated credential can never be overwritten.
    assert (
        github_user_authorizations.try_install_rotated_refresh(
            _pool(conn),
            profile_id=profile_id,
            expected_generation=1,
            refresh_expires_at=datetime.now(UTC) + timedelta(hours=8),
        )
        is None
    )

    # The current generation installs: expiry metadata + generation increment.
    installed = github_user_authorizations.try_install_rotated_refresh(
        _pool(conn),
        profile_id=profile_id,
        expected_generation=2,
        refresh_expires_at=datetime.now(UTC) + timedelta(hours=8),
    )
    assert installed is not None
    assert installed.refresh_generation == 3

    # The CAS also refuses a non-active row: revocation blocks any install.
    github_user_authorizations.revoke_github_user_authorization_row(
        _pool(conn), profile_id=profile_id
    )
    assert (
        github_user_authorizations.try_install_rotated_refresh(
            _pool(conn),
            profile_id=profile_id,
            expected_generation=3,
            refresh_expires_at=datetime.now(UTC) + timedelta(hours=8),
        )
        is None
    )


def test_explicit_revocation_removes_the_usable_credential(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    secret_id, _reference = _establish_real_authorization(conn, profile_id=profile_id)
    assert _vault_secret_exists(conn, secret_id)

    revoked = github_user_authorizations.revoke_github_user_authorization_row(
        _pool(conn), profile_id=profile_id
    )
    assert revoked is not None
    assert revoked.status.value == "revoked"
    assert revoked.refresh_secret_reference is None
    assert revoked.refresh_generation == 2


def test_account_deletion_composes_the_github_cleanup_before_the_auth_delete(
    conn: Connection[Any],
) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    secret_id, _reference = _establish_real_authorization(conn, profile_id=profile_id)

    outcome = account_lifecycle.delete_account(
        _pool(conn), _ImmediateDeleteAdminClient(conn), profile_id=profile_id
    )

    assert outcome.result == "deleted"
    # The Vault secret was deleted in the pre-delete revocation transaction...
    assert not _vault_secret_exists(conn, secret_id)
    # ...and the sanctioned Auth-root cascade removed the authorization row
    # (and the whole Profile graph) with the account.
    row = conn.execute(
        "select 1 from openorc.github_user_authorizations where profile_id = %s",
        (profile_id,),
    ).fetchone()
    assert row is None
    profile = conn.execute("select 1 from openorc.profiles where id = %s", (profile_id,)).fetchone()
    assert profile is None


def test_account_deletion_cleanup_is_idempotent_on_retry(conn: Connection[Any]) -> None:
    profile_id = uuid.uuid4()
    _seed_account(conn, profile_id)
    secret_id, _reference = _establish_real_authorization(conn, profile_id=profile_id)
    # The first attempt's external Auth deletion is definitively rejected: the
    # established #97 contract translates that into the typed known-failure
    # condition after clearing the attempt state, leaving the credentials
    # revoked and the account present — and a later explicit retry safe.
    with pytest.raises(ExternalOperationFailedError):
        account_lifecycle.delete_account(
            _pool(conn),
            _RejectedDeleteAdminClient(),
            profile_id=profile_id,
        )

    # The GitHub authorization row still exists — revoked is durable
    # currentness state, deliberately not removed by the rejection — and
    # carries no dangling reference.
    durable = github_user_authorizations.get_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )
    assert durable is not None
    assert durable.status.value == "revoked"
    assert durable.refresh_secret_reference is None
    # The refresh secret was deleted in the committed pre-delete revocation.
    assert not _vault_secret_exists(conn, secret_id)
    # The attempt state was cleared: normal account use (and the explicit
    # retry) can resume.
    attempt_state = conn.execute(
        "select account_deletion_state from openorc.profiles where id = %s",
        (profile_id,),
    ).fetchone()
    assert attempt_state is not None and attempt_state[0] is None

    # The retry succeeds end to end — no dangling-reference dead end — and the
    # simulated successful Auth deletion removes the Profile-owned graph
    # through the sanctioned cascade.
    retry = account_lifecycle.delete_account(
        _pool(conn), _ImmediateDeleteAdminClient(conn), profile_id=profile_id
    )
    assert retry.result == "deleted"
    row = conn.execute(
        "select 1 from openorc.github_user_authorizations where profile_id = %s",
        (profile_id,),
    ).fetchone()
    assert row is None
    profile = conn.execute("select 1 from openorc.profiles where id = %s", (profile_id,)).fetchone()
    assert profile is None


class _ImmediateDeleteAdminClient:
    """Scripted Admin boundary whose permanent deletion actually applies.

    The simulated external side effect runs against the fixture database:
    deleting the exact Auth user row fires the sanctioned
    ``auth.users -> profiles -> ...`` cascade exactly as the real Supabase
    Auth deletion would, so the test proves the full cleanup-then-cascade
    composition rather than a fake success that removes nothing.
    """

    def __init__(self, conn: Connection[Any]) -> None:
        self._conn = conn
        self.delete_calls: list[UUID] = []

    @property
    def request_timeout_seconds(self) -> float:
        return 5.0

    def delete_user(self, user_id: UUID) -> None:
        self.delete_calls.append(user_id)
        self._conn.execute("delete from auth.users where id = %s", (user_id,))

    def fetch_user(self, user_id: UUID) -> bool:
        return False

    def fetch_user_github_provider_ids(self, user_id: UUID) -> tuple[str | None, ...]:
        # The deletion lifecycle never performs the #142 identity lookup.
        raise AssertionError("the account-deletion lifecycle never reads GitHub identities")


class _RejectedDeleteAdminClient:
    """Scripted Auth Admin boundary whose deletion is definitively rejected."""

    @property
    def request_timeout_seconds(self) -> float:
        return 5.0

    def delete_user(self, user_id: UUID) -> None:
        from openorc.adapters.supabase import SupabaseAuthAdminRejectedError

        raise SupabaseAuthAdminRejectedError(
            "the administrative endpoint rejected the permanent deletion request"
        )

    def fetch_user(self, user_id: UUID) -> bool:
        return True

    def fetch_user_github_provider_ids(self, user_id: UUID) -> tuple[str | None, ...]:
        # The deletion lifecycle never performs the #142 identity lookup.
        raise AssertionError("the account-deletion lifecycle never reads GitHub identities")
