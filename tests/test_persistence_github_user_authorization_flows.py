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

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
_EXPIRES = _OBSERVED + timedelta(seconds=600)
_VERIFIER = "v" * 43


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[object]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.transaction_depth = 0

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(cast("tuple[Any, ...] | None", result))

    @contextmanager
    def transaction(self) -> Iterator[ScriptedConnection]:
        self.transaction_depth += 1
        try:
            yield self
        finally:
            self.transaction_depth -= 1


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
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


def test_consume_deletes_the_row_and_verifier_secret_in_one_transaction() -> None:
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

    consumed = consume_pending_flow(_pool(conn), state="s" * 43)

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
    decrypt_sql, decrypt_params = conn.executed[1]
    assert "vault.decrypted_secrets" in decrypt_sql
    assert decrypt_params == (secret_id,)
    # The consumed flow's verifier secret is deleted in the same transaction
    # (issue #373 review): no consumed flow ever orphans its secret.
    cleanup_sql, cleanup_params = conn.executed[2]
    assert "delete from vault.secrets" in cleanup_sql
    assert cleanup_params == (secret_id,)
    assert conn.transaction_depth == 0


def test_consume_reports_an_unknown_expired_or_replayed_state_as_none() -> None:
    conn = ScriptedConnection([None])

    assert consume_pending_flow(_pool(conn), state="s" * 43) is None


def test_consume_fails_closed_and_rolls_back_on_a_dangling_reference() -> None:
    profile_id = uuid.uuid4()
    row = _flow_row(profile_id)
    conn = ScriptedConnection(
        [
            row,  # the delete matched
            None,  # the Vault decrypt reports the secret absent
        ]
    )

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        consume_pending_flow(_pool(conn), state="s" * 43)

    # The reference parse and the decrypt ran; the secret cleanup never did —
    # and the rollback restores the pending row for the caller's retry.
    assert "vault.decrypted_secrets" in conn.executed[1][0]
    assert all("delete from vault.secrets" not in sql for sql, _ in conn.executed)


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

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        consume_pending_flow(_pool(conn), state="s" * 43)

    # Cleanup failure is never silently tolerated: the whole consumption —
    # including the row delete — rolls back.
    assert "delete from vault.secrets" in conn.executed[2][0]


def test_consume_fails_closed_on_a_malformed_reference() -> None:
    profile_id = uuid.uuid4()
    row = _flow_row(profile_id)
    row = (row[0], row[1], row[2], "not-a-v1-reference", *row[4:])
    conn = ScriptedConnection([row])

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        consume_pending_flow(_pool(conn), state="s" * 43)

    # The malformed reference is detected before any Vault decrypt.
    assert len(conn.executed) == 1


# --- the no-decrypt discard and the invalidation delete ----------------------


def test_discard_deletes_the_declined_flow_and_its_verifier_secret() -> None:
    secret_id = uuid.uuid4()
    reference = _reference(secret_id)
    conn = ScriptedConnection([(reference,), (1,)])

    discarded = discard_pending_flow(_pool(conn), state="s" * 43)

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


def test_discard_of_an_absent_flow_is_false_and_touches_no_secret() -> None:
    conn = ScriptedConnection([None])

    assert discard_pending_flow(_pool(conn), state="s" * 43) is False
    assert len(conn.executed) == 1
    assert all("vault" not in sql for sql, _ in conn.executed)


def test_discard_fails_closed_on_a_malformed_reference() -> None:
    conn = ScriptedConnection([("not-a-v1-reference",)])

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        discard_pending_flow(_pool(conn), state="s" * 43)

    assert len(conn.executed) == 1


def test_discard_fails_closed_when_the_secret_cleanup_fails() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(_reference(secret_id),), None])

    with pytest.raises(GitHubUserFlowVerifierReferenceError):
        discard_pending_flow(_pool(conn), state="s" * 43)

    assert "delete from vault.secrets" in conn.executed[1][0]


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
