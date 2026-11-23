"""Deterministic mapping tests for the pending GitHub authorization flow repositories.

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove row-to-domain-object mapping, UTC normalization at the
persistence boundary, the opaque purpose-specific verifier reference codec
(never cross-parseable with the refresh-credential or Connection formats),
the atomic single-use consume (the conditional delete with its in-transaction
Vault decrypt and fail-closed reference handling), the no-decrypt discard,
and the atomic supersession/invalidation delete with its verifier-secret
cleanup. Database constraint behavior is proven against a real database by
the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlowPurpose,
)
from openorc.persistence.github_user_authorization_flows import (
    GitHubUserFlowVerifierReferenceError,
    consume_pending_flow,
    create_pending_flow,
    delete_pending_flow_with_verifier_secret,
    discard_pending_flow,
    encode_github_user_flow_verifier_reference,
    get_pending_flow,
    parse_github_user_flow_verifier_reference,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.transaction_composition import composed_transaction

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
_EXPIRES = _OBSERVED + timedelta(seconds=600)
_VERIFIER = "v" * 43


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL.

    Every executed statement also records the connection's transaction depth
    at the moment it ran, and the transaction boundary records whether it
    ever rolled back — the atomicity proofs need both.
    """

    def __init__(self, results: list[object]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.executed_depths: list[int] = []
        self.rolled_back = False
        self.transaction_depth = 0

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        self.executed_depths.append(self.transaction_depth)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(cast("tuple[Any, ...] | None", result))

    @contextmanager
    def transaction(self) -> Iterator[ScriptedConnection]:
        self.transaction_depth += 1
        try:
            yield self
        except BaseException:
            self.rolled_back = True
            raise
        finally:
            self.transaction_depth -= 1


class FakePool:
    """Counts every connection checkout out of the pool.

    The production defect this suite guards against (issue #373 review) is
    an operation whose row effects and Vault effects check out SEPARATE
    pool connections — on the real psycopg pool those are independent
    transactions. The scripted connection collapses every checkout onto one
    connection, so the checkout count is what proves the operations ran on
    one.
    """

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.checkouts = 0

    def connection(self) -> Any:
        self.checkouts += 1

        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("flow persistence tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _flow_row(
    profile_id: Any, *, state: str = "s" * 43, revocation_count: int = 0
) -> tuple[Any, ...]:
    return (
        profile_id,
        "authorize",
        state,
        "openorc:github-user-flow-verifier:v1:vault:" + str(uuid.uuid4()),
        revocation_count,
        _OBSERVED,
        _EXPIRES,
    )


def _reference(secret_id: Any) -> str:
    return f"openorc:github-user-flow-verifier:v1:vault:{secret_id}"


# --- opaque reference codec --------------------------------------------------


def test_reference_roundtrip_is_exact() -> None:
    secret_id = uuid.uuid4()

    reference = encode_github_user_flow_verifier_reference(secret_id)
    parsed = parse_github_user_flow_verifier_reference(reference)

    assert reference == _reference(secret_id)
    assert parsed == secret_id


@pytest.mark.parametrize(
    "value",
    [
        "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4()),  # the refresh purpose
        "openorc:connection-auth:v1:vault:" + str(uuid.uuid4()),  # the Connection purpose
        "not-a-reference",
        "openorc:github-user-flow-verifier:v1:vault:not-a-uuid",
        "openorc:github-user-flow-verifier:v2:vault:" + str(uuid.uuid4()),
        42,  # type: ignore[list-item]
    ],
)
def test_foreign_or_malformed_references_fail_closed(value: object) -> None:
    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        parse_github_user_flow_verifier_reference(value)  # type: ignore[arg-type]


def test_non_canonical_uuid_payload_fails_closed() -> None:
    secret_id = uuid.uuid4()

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        parse_github_user_flow_verifier_reference(_reference(secret_id).upper())


# --- create and read ---------------------------------------------------------


def test_create_stores_the_verifier_in_vault_and_inserts_the_flow() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (secret_id,),  # the Vault secret create
            _flow_row(profile_id, revocation_count=2),  # the insert returning the mapped row
        ]
    )

    flow = create_pending_flow(
        _pool(conn),
        profile_id=profile_id,
        purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
        state="s" * 43,
        verifier=_VERIFIER,
        observed_revocation_count=2,
        expires_at=_EXPIRES,
    )

    assert flow.profile_id == profile_id
    assert flow.purpose is GitHubUserAuthorizationFlowPurpose.AUTHORIZE
    assert flow.observed_revocation_count == 2
    create_sql, create_params = conn.executed[0]
    assert "vault.create_secret" in create_sql
    assert create_params is not None
    # The verifier value travels only in the Vault create; the description
    # carries safe identifiers only.
    assert create_params[0] == _VERIFIER
    assert f"profile {profile_id}" in create_params[1]
    insert_sql, insert_params = conn.executed[1]
    assert "insert into openorc.github_user_authorization_flows" in insert_sql
    assert insert_params is not None
    assert insert_params[0] == profile_id
    assert insert_params[1] == "authorize"
    assert insert_params[2] == "s" * 43
    # The stored reference is the opaque pointer, never the verifier value.
    assert _reference(secret_id) in str(insert_params[3])
    assert _VERIFIER not in str(insert_params)
    assert insert_params[4] == 2


def test_read_maps_the_pending_flow_row() -> None:
    profile_id = uuid.uuid4()
    row = _flow_row(profile_id, revocation_count=5)
    conn = ScriptedConnection([row])

    flow = get_pending_flow(_pool(conn), profile_id=profile_id)

    assert flow is not None
    assert flow.profile_id == profile_id
    assert flow.purpose is GitHubUserAuthorizationFlowPurpose.AUTHORIZE
    assert flow.observed_revocation_count == 5
    # Instants are normalized to timezone-aware UTC at the boundary.
    assert flow.started_at.tzinfo is not None
    assert flow.started_at == _OBSERVED
    assert flow.expires_at == _EXPIRES


def test_read_absent_pending_flow_is_none() -> None:
    conn = ScriptedConnection([None])

    assert get_pending_flow(_pool(conn), profile_id=uuid.uuid4()) is None


# --- the atomic consume ------------------------------------------------------


def test_consume_runs_row_delete_decrypt_and_cleanup_on_one_connection() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    row = _flow_row(profile_id, revocation_count=3)
    row = (row[0], row[1], row[2], _reference(secret_id), *row[4:])
    conn = ScriptedConnection(
        [
            row,  # the conditional delete returning the matched flow
            (_VERIFIER,),  # the in-transaction Vault decrypt
            (1,),  # the verifier secret delete
        ]
    )
    fake_pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool:
        consumed = consume_pending_flow(transaction_pool, state="s" * 43)

    assert consumed is not None
    flow, verifier = consumed
    assert flow.profile_id == profile_id
    assert flow.observed_revocation_count == 3
    assert verifier == _VERIFIER
    delete_sql, delete_params = conn.executed[0]
    # The delete IS the single-use enforcement: exact state, unexpired only.
    assert "delete from openorc.github_user_authorization_flows" in delete_sql
    assert "where state = %s and expires_at > now()" in delete_sql
    assert delete_params == ("s" * 43,)
    assert "vault.decrypted_secrets" in conn.executed[1][0]
    cleanup_sql, cleanup_params = conn.executed[2]
    assert "delete from vault.secrets" in cleanup_sql
    assert cleanup_params == (secret_id,)
    # Atomicity (issue #373 review): exactly ONE pool checkout — the row
    # delete, the decrypt, and the secret cleanup share ONE connection, so
    # they cannot be independent transactions on the real pool — and every
    # statement ran inside the ONE outer transaction (depth 1 is the
    # composed transaction block; the row delete and the Vault operations
    # run in its nested savepoints).
    assert fake_pool.checkouts == 1
    assert conn.executed_depths == [2, 3, 3]


def test_consume_reports_an_unknown_expired_or_replayed_state_as_none() -> None:
    conn = ScriptedConnection([None])
    fake_pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool:
        consumed = consume_pending_flow(transaction_pool, state="s" * 43)

    assert consumed is None
    # Nothing matched: one checkout, no Vault touch, nothing rolled back.
    assert fake_pool.checkouts == 1
    assert conn.rolled_back is False
    assert all("vault" not in sql for sql, _ in conn.executed)


def test_consume_fails_closed_and_rolls_back_on_a_dangling_reference() -> None:
    profile_id = uuid.uuid4()
    row = _flow_row(profile_id)
    conn = ScriptedConnection(
        [
            row,  # the delete matched
            None,  # the Vault decrypt reports the secret absent
        ]
    )
    fake_pool = FakePool(conn)

    with (
        pytest.raises(GitHubUserFlowVerifierReferenceError),
        composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool,
    ):
        consume_pending_flow(transaction_pool, state="s" * 43)

    # The reference parse and the decrypt ran; the secret cleanup never did.
    assert "vault.decrypted_secrets" in conn.executed[1][0]
    assert all("delete from vault.secrets" not in sql for sql, _ in conn.executed)
    # The ONE outer transaction rolled back: the row delete cannot have
    # committed without the cleanup, and the pending row is restored for the
    # caller's retry.
    assert fake_pool.checkouts == 1
    assert conn.rolled_back is True


def test_consume_fails_closed_and_rolls_back_when_the_secret_cleanup_fails() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    row = _flow_row(profile_id)
    row = (row[0], row[1], row[2], _reference(secret_id), *row[4:])
    conn = ScriptedConnection(
        [
            row,  # the delete matched
            (_VERIFIER,),  # the decrypt succeeded
            None,  # the secret cleanup reports the secret absent
        ]
    )
    fake_pool = FakePool(conn)

    with (
        pytest.raises(GitHubUserFlowVerifierReferenceError),
        composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool,
    ):
        consume_pending_flow(transaction_pool, state="s" * 43)

    # Cleanup failure is never silently tolerated: the secret delete was
    # attempted inside the ONE outer transaction, and the whole composition —
    # row delete included — rolled back with it.
    assert "delete from vault.secrets" in conn.executed[2][0]
    assert fake_pool.checkouts == 1
    assert conn.rolled_back is True


def test_consume_fails_closed_on_a_malformed_reference() -> None:
    profile_id = uuid.uuid4()
    row = _flow_row(profile_id)
    row = (row[0], row[1], row[2], "not-a-v1-reference", *row[4:])
    conn = ScriptedConnection([row])
    fake_pool = FakePool(conn)

    with (
        pytest.raises(GitHubUserFlowVerifierReferenceError),
        composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool,
    ):
        consume_pending_flow(transaction_pool, state="s" * 43)

    # The malformed reference is detected before any Vault decrypt, and the
    # outer transaction rolls the row delete back.
    assert len(conn.executed) == 1
    assert fake_pool.checkouts == 1
    assert conn.rolled_back is True


# --- the no-decrypt discard and the invalidation delete ----------------------


def test_discard_runs_row_delete_and_cleanup_on_one_connection() -> None:
    secret_id = uuid.uuid4()
    reference = _reference(secret_id)
    conn = ScriptedConnection([(reference,), (1,)])
    fake_pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool:
        discarded = discard_pending_flow(transaction_pool, state="s" * 43)

    assert discarded is True
    row_sql, row_params = conn.executed[0]
    assert "delete from openorc.github_user_authorization_flows" in row_sql
    assert "where state = %s" in row_sql
    assert "returning pkce_verifier_reference" in row_sql
    assert row_params == ("s" * 43,)
    # The declined flow's verifier secret is deleted with the row — without
    # any decryption (issue #373 review).
    vault_sql, vault_params = conn.executed[1]
    assert "delete from vault.secrets" in vault_sql
    assert vault_params == (secret_id,)
    assert all("vault.decrypted_secrets" not in sql for sql, _ in conn.executed)
    # Atomicity (issue #373 review): ONE pool checkout, both statements
    # inside the ONE outer transaction (depth 1 is the composed transaction
    # block; the row delete and the Vault cleanup run in its nested
    # savepoints).
    assert fake_pool.checkouts == 1
    assert conn.executed_depths == [2, 3]


def test_discard_of_an_absent_flow_is_false_and_touches_no_secret() -> None:
    conn = ScriptedConnection([None])
    fake_pool = FakePool(conn)

    with composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool:
        discarded = discard_pending_flow(transaction_pool, state="s" * 43)

    assert discarded is False
    assert len(conn.executed) == 1
    assert fake_pool.checkouts == 1
    assert all("vault" not in sql for sql, _ in conn.executed)


def test_discard_fails_closed_on_a_malformed_reference() -> None:
    conn = ScriptedConnection([("not-a-v1-reference",)])
    fake_pool = FakePool(conn)

    with (
        pytest.raises(GitHubUserFlowVerifierReferenceError),
        composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool,
    ):
        discard_pending_flow(transaction_pool, state="s" * 43)

    assert len(conn.executed) == 1
    assert fake_pool.checkouts == 1
    assert conn.rolled_back is True


def test_discard_fails_closed_when_the_secret_cleanup_fails() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(_reference(secret_id),), None])
    fake_pool = FakePool(conn)

    with (
        pytest.raises(GitHubUserFlowVerifierReferenceError),
        composed_transaction(cast(DatabasePool, fake_pool)) as transaction_pool,
    ):
        discard_pending_flow(transaction_pool, state="s" * 43)

    # The secret delete was attempted inside the ONE outer transaction, and
    # the whole composition — row delete included — rolled back with it.
    assert "delete from vault.secrets" in conn.executed[1][0]
    assert fake_pool.checkouts == 1
    assert conn.rolled_back is True


def test_invalidation_delete_removes_the_row_and_its_verifier_secret_atomically() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    row = _flow_row(profile_id)
    row = (row[0], row[1], row[2], _reference(secret_id), *row[4:])
    conn = ScriptedConnection(
        [
            (row[3],),  # the delete returning the verifier reference
            (1,),  # the Vault secret delete
        ]
    )

    deleted = delete_pending_flow_with_verifier_secret(_pool(conn), profile_id=profile_id)

    assert deleted is True
    row_sql, row_params = conn.executed[0]
    assert "delete from openorc.github_user_authorization_flows" in row_sql
    assert "where profile_id = %s" in row_sql
    assert row_params == (profile_id,)
    vault_sql, vault_params = conn.executed[1]
    assert "delete from vault.secrets" in vault_sql
    assert vault_params == (secret_id,)


def test_invalidation_delete_reports_an_absent_flow_as_false() -> None:
    conn = ScriptedConnection([None])

    deleted = delete_pending_flow_with_verifier_secret(_pool(conn), profile_id=uuid.uuid4())

    assert deleted is False
    # No Vault touch at all when nothing was pending.
    assert len(conn.executed) == 1


def test_invalidation_delete_fails_closed_on_a_dangling_verifier_reference() -> None:
    profile_id = uuid.uuid4()
    row = _flow_row(profile_id)
    conn = ScriptedConnection([(row[3],), None])

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        delete_pending_flow_with_verifier_secret(_pool(conn), profile_id=profile_id)

    assert "delete from vault.secrets" in conn.executed[1][0]


def test_invalidation_delete_fails_closed_on_a_malformed_reference() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([("not-a-v1-reference",)])

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        delete_pending_flow_with_verifier_secret(_pool(conn), profile_id=profile_id)

    assert len(conn.executed) == 1
