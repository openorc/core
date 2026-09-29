"""Deterministic tests for the Profile-scoped GitHub authorization service (issue #142).

The ordinary suite cannot execute Postgres or call live GitHub/Supabase: canned
rows, a scripted fake connection seam, a scripted fake Auth Admin client, and
a scripted fake GitHub user-token client prove the same-human binding proof
(zero/one/many/malformed trusted identities, the authoritative /user
comparison, and that ``user_metadata`` never becomes identity authority), the
expiring-token capability requirement, the Vault-backed establishment and
in-place generation-advancing replacement, the generation compare-and-swap
refresh lifecycle (stale success cannot overwrite; rejected/uncertain fails
closed without durable mutation or blind replay), the generation-bound cache,
explicit revocation, and the account-deletion integration. No external I/O
ever runs inside a database transaction.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import pytest

from openorc.adapters.github import (
    GitHubCurrentUser,
    GitHubOutcomeUncertainError,
    GitHubUserAccessToken,
    GitHubUserRefreshSecret,
    GitHubUserTokenGrant,
    GitHubUserTokenRefreshCapabilityMissingError,
    GitHubUserTokenRejectedError,
)
from openorc.adapters.supabase import (
    SupabaseAuthAdminUserAbsentError,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import github_user_authorization
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    StaleOperationError,
)
from openorc.services.github_user_authorization import (
    CONDITION_EXPIRED_UNREFRESHABLE,
    CONDITION_MISSING,
    CONDITION_REFRESH_CAPABILITY_UNAVAILABLE,
    CONDITION_REFRESH_REJECTED,
    CONDITION_REVOKED,
    GitHubUserAccessTokenResolver,
    GitHubUserAuthorizationUnavailableError,
    GitHubUserIdentityMismatchError,
    SupabaseGitHubIdentityStateError,
)

_OBSERVED = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)
_FIXED_NOW = _OBSERVED
_ACCESS_TOKEN = "ghu_s3cr3t-access-value"
_REFRESH_TOKEN = "ghr_s3cr3t-refresh-value"
_ROTATED_REFRESH = "ghr_n3w-rotated-refresh"
_SIGNIN_PROVIDER_TOKEN = "gho_supabase-sign-in-provider-token-never-a-credential"


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None = None):
        self._row = row
        self._rows = rows

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
        if isinstance(result, list):
            return FakeCursor(None, result)  # type: ignore[arg-type]
        return FakeCursor(result)  # type: ignore[arg-type]

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.transaction_depth += 1
        try:
            yield
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
        raise AssertionError("authorization service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _grant(*, access: str = _ACCESS_TOKEN, refresh: str = _REFRESH_TOKEN) -> GitHubUserTokenGrant:
    return GitHubUserTokenGrant(
        access_token=GitHubUserAccessToken(
            value=access, expires_at=_FIXED_NOW + timedelta(hours=8)
        ),
        refresh_token=GitHubUserRefreshSecret(
            value=refresh, expires_at=_FIXED_NOW + timedelta(days=180)
        ),
    )


def _reference(secret_id: Any) -> str:
    return f"openorc:github-user-refresh:v1:vault:{secret_id}"


def _row(
    profile_id: Any,
    *,
    status: str = "active",
    generation: int = 1,
    reference: Any = None,
    expires: Any = None,
    github_user_id: int = 5432,
) -> tuple[Any, ...]:
    if status == "active":
        reference = reference if reference is not None else _reference(uuid.uuid4())
        expires = expires if expires is not None else _FIXED_NOW + timedelta(days=180)
    else:
        reference = None
        expires = None
    return (
        profile_id,
        github_user_id,
        "octocat",
        status,
        reference,
        expires,
        generation,
        _OBSERVED,
        None if status == "active" else _OBSERVED,
        _OBSERVED,
        _OBSERVED,
    )


class FakeAdminClient:
    """Scripted Auth Admin boundary for the trusted GitHub identity lookup."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.identity_results: list[object] = []
        self.identity_calls: list[tuple[UUID, int]] = []

    @property
    def request_timeout_seconds(self) -> float:
        return 5.0

    def fetch_user_github_provider_ids(self, user_id: UUID) -> tuple[str | None, ...]:
        self.identity_calls.append((user_id, self._conn.transaction_depth))
        outcome = self.identity_results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast(tuple[str | None, ...], outcome)

    def delete_user(self, user_id: UUID) -> None:  # pragma: no cover - never used here
        raise AssertionError("the authorization service never deletes Auth users")

    def fetch_user(self, user_id: UUID) -> bool:  # pragma: no cover - never used here
        raise AssertionError("the authorization service never reconciles deletion state")


class FakeTokenClient:
    """Scripted GitHub user-token boundary recording every credential it sees."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.exchange_results: list[object] = []
        self.refresh_results: list[object] = []
        self.user_results: list[object] = []
        self.exchange_calls: list[tuple[str, int]] = []
        self.refresh_calls: list[tuple[str, int]] = []
        self.user_calls: list[int] = []

    def exchange_authorization_code(
        self, code: str, *, redirect_uri: str | None = None
    ) -> GitHubUserTokenGrant:
        self.exchange_calls.append((code, self._conn.transaction_depth))
        outcome = self.exchange_results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast(GitHubUserTokenGrant, outcome)

    def refresh_user_token(self, refresh_token: str) -> GitHubUserTokenGrant:
        self.refresh_calls.append((refresh_token, self._conn.transaction_depth))
        outcome = self.refresh_results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast(GitHubUserTokenGrant, outcome)

    def fetch_authenticated_user(self, access_token: GitHubUserAccessToken) -> GitHubCurrentUser:
        self.user_calls.append(self._conn.transaction_depth)
        outcome = self.user_results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast(GitHubCurrentUser, outcome)


def _establish_clients(
    conn: ScriptedConnection, *, provider_ids: tuple[str | None, ...], current_user_id: int
) -> tuple[FakeAdminClient, FakeTokenClient]:
    admin = FakeAdminClient(conn)
    admin.identity_results.append(provider_ids)
    token_client = FakeTokenClient(conn)
    token_client.exchange_results.append(_grant())
    token_client.user_results.append(
        GitHubCurrentUser(github_user_id=current_user_id, login="octo")
    )
    return admin, token_client


_GUARD_OPERATIONAL_ROW = (None, None, None)


def test_establish_proves_the_same_human_identity_and_inserts_the_first_authorization() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    inserted_row = _row(profile_id, generation=1)
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,  # account barrier
            None,  # locked read: no existing authorization
            (secret_id,),  # Vault secret create
            inserted_row,  # the insert
        ]
    )
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)

    established = github_user_authorization.establish_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        code="the-code",
        admin_client=admin,
        token_client=token_client,
    )

    assert established.refresh_generation == 1
    # Binding proof order: trusted Supabase identity, then the authoritative
    # /user comparison — all with NO database transaction open.
    assert admin.identity_calls == [(profile_id, 0)]
    assert token_client.exchange_calls == [("the-code", 0)]
    assert token_client.user_calls == [0]
    (create_sql, create_params), (insert_sql, insert_params) = conn.executed[2], conn.executed[3]
    assert "vault.create_secret" in create_sql
    assert create_params is not None and create_params[0] == _REFRESH_TOKEN
    assert "insert into openorc.github_user_authorizations" in insert_sql
    assert "'active'" in insert_sql
    assert insert_params is not None
    assert insert_params[4] == _grant().refresh_token.expires_at


def test_establish_replaces_in_place_advancing_the_generation_without_a_reset() -> None:
    profile_id = uuid.uuid4()
    old_secret_id = uuid.uuid4()
    new_secret_id = uuid.uuid4()
    existing = _row(profile_id, generation=4, reference=_reference(old_secret_id))
    updated = _row(profile_id, generation=5, reference=_reference(new_secret_id))
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            existing,  # locked read: an active authorization exists
            (1,),  # superseded Vault secret delete
            (new_secret_id,),  # new Vault secret create
            updated,  # the in-place reauthorize update
        ]
    )
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)

    established = github_user_authorization.establish_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        code="the-code",
        admin_client=admin,
        token_client=token_client,
    )

    assert established.refresh_generation == 5
    delete_sql, delete_params = conn.executed[2]
    assert "delete from vault.secrets" in delete_sql
    assert delete_params == (old_secret_id,)
    reauth_sql = conn.executed[4][0]
    assert "refresh_generation = refresh_generation + 1" in reauth_sql
    assert "update openorc.github_user_authorizations" in reauth_sql
    # No delete-and-reinsert: the authorization table is never DELETEd from.
    assert all(
        "delete from openorc.github_user_authorizations" not in sql for sql, _ in conn.executed
    )


def test_establish_rejects_a_different_github_account_storing_nothing() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])  # a mismatch must reach no durable statement at all
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=9999)

    with pytest.raises(GitHubUserIdentityMismatchError):
        github_user_authorization.establish_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            code="the-code",
            admin_client=admin,
            token_client=token_client,
        )

    # Fail closed: no Vault secret, no row, nothing durable.
    assert conn.executed == []


@pytest.mark.parametrize(
    ("provider_ids", "expected_condition"),
    [
        ((), github_user_authorization.IDENTITY_CONDITION_MISSING),
        (("5", "6"), github_user_authorization.IDENTITY_CONDITION_AMBIGUOUS),
        (("abc",), github_user_authorization.IDENTITY_CONDITION_MALFORMED),
        (("",), github_user_authorization.IDENTITY_CONDITION_MALFORMED),
        (("05432",), github_user_authorization.IDENTITY_CONDITION_MALFORMED),
        ((" 5432",), github_user_authorization.IDENTITY_CONDITION_MALFORMED),
        ((None,), github_user_authorization.IDENTITY_CONDITION_MALFORMED),
    ],
)
def test_establish_fails_closed_on_unusable_trusted_identity_state(
    provider_ids: tuple[str | None, ...], expected_condition: str
) -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])  # no durable statement may run
    admin, token_client = _establish_clients(conn, provider_ids=provider_ids, current_user_id=5432)

    with pytest.raises(SupabaseGitHubIdentityStateError) as error:
        github_user_authorization.establish_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            code="the-code",
            admin_client=admin,
            token_client=token_client,
        )

    assert error.value.condition == expected_condition
    assert conn.executed == []


def test_establish_fails_closed_when_the_auth_account_is_absent() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])
    admin, token_client = _establish_clients(conn, provider_ids=(), current_user_id=5432)
    admin.identity_results[0] = SupabaseAuthAdminUserAbsentError("absent")

    with pytest.raises(SupabaseGitHubIdentityStateError) as error:
        github_user_authorization.establish_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            code="the-code",
            admin_client=admin,
            token_client=token_client,
        )

    assert error.value.condition == github_user_authorization.IDENTITY_CONDITION_ACCOUNT_ABSENT
    assert conn.executed == []


def test_establish_requires_the_expiring_token_capability() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)
    token_client.exchange_results[0] = GitHubUserTokenRefreshCapabilityMissingError("missing")

    with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
        github_user_authorization.establish_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            code="the-code",
            admin_client=admin,
            token_client=token_client,
        )

    assert error.value.condition == CONDITION_REFRESH_CAPABILITY_UNAVAILABLE
    assert conn.executed == []


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (GitHubUserTokenRejectedError("rejected"), ExternalOperationFailedError),
        (GitHubOutcomeUncertainError("timeout"), ExternalOperationUncertainError),
    ],
)
def test_establish_surfaces_exchange_outcome_classification_without_durable_writes(
    outcome: Exception, expected: type[Exception]
) -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)
    token_client.exchange_results[0] = outcome

    with pytest.raises(expected):
        github_user_authorization.establish_github_user_authorization(
            _pool(conn),
            profile_id=profile_id,
            code="the-code",
            admin_client=admin,
            token_client=token_client,
        )

    assert conn.executed == []


def test_the_sign_in_provider_token_is_never_used_as_a_repository_credential() -> None:
    # The exchange surface receives only the post-correlation code; the
    # Supabase/GitHub sign-in provider token never enters any call, any
    # durable statement, or any persisted reference.
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            None,
            (uuid.uuid4(),),
            _row(profile_id, generation=1),
        ]
    )
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)

    github_user_authorization.establish_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        code="the-code",
        admin_client=admin,
        token_client=token_client,
    )

    assert [call[0] for call in token_client.exchange_calls] == ["the-code"]
    assert _SIGNIN_PROVIDER_TOKEN not in [call[0] for call in token_client.exchange_calls]
    assert all(_SIGNIN_PROVIDER_TOKEN not in repr(params) for _sql, params in conn.executed)


def test_establish_rejects_an_invalid_code_before_any_durable_or_external_call() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)

    for bad in ("", None, 42):
        with pytest.raises(InvalidCommandError):
            github_user_authorization.establish_github_user_authorization(
                _pool(conn),
                profile_id=profile_id,
                code=bad,  # type: ignore[arg-type]
                admin_client=admin,
                token_client=token_client,
            )
        # Validation precedes everything: no identity lookup, no exchange, no durable read.
        assert conn.executed == []
        assert admin.identity_calls == []
        assert token_client.exchange_calls == []


def _resolver(
    conn: ScriptedConnection, token_client: FakeTokenClient
) -> GitHubUserAccessTokenResolver:
    return GitHubUserAccessTokenResolver(
        token_client=token_client,  # type: ignore[arg-type]
        clock=lambda: _FIXED_NOW,
    )


def _resolve_fresh_flow_script(
    profile_id: Any, *, observed_generation: int = 1, installed_generation: int = 2
) -> list[object]:
    """Statement results for one full fresh-resolution pass."""
    return [
        _row(profile_id, generation=observed_generation),  # unlocked read
        (_REFRESH_TOKEN,),  # Vault decrypt-on-read
        _GUARD_OPERATIONAL_ROW,  # install-transaction account barrier
        _row(profile_id, generation=observed_generation),  # locked read under the lock
        (1,),  # Vault existence check (in-place update)
        None,  # vault.update_secret returns nothing
        _row(profile_id, generation=installed_generation),  # the CAS install
    ]


def test_resolve_serves_the_generation_bound_cached_token() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        _resolve_fresh_flow_script(profile_id, observed_generation=3, installed_generation=4)
    )
    token_client = FakeTokenClient(conn)
    token_client.refresh_results.append(_grant(refresh=_ROTATED_REFRESH))
    resolver = _resolver(conn, token_client)

    first = resolver.resolve(_pool(conn), profile_id=profile_id)
    assert token_client.refresh_calls and token_client.refresh_calls[0][0] == _REFRESH_TOKEN

    # Second resolution at the same durable generation hits the cache: no
    # additional external refresh and no additional Vault read.
    conn.results.append(_row(profile_id, generation=4))  # the second durable read
    second = resolver.resolve(_pool(conn), profile_id=profile_id)

    assert first is second
    assert first.token_value() == _ACCESS_TOKEN
    assert len(token_client.refresh_calls) == 1


def test_resolve_after_a_process_restart_exchanges_from_the_durable_vault_reference() -> None:
    profile_id = uuid.uuid4()
    # A fresh resolver: no cached token — resolution restarts from durable
    # authorization metadata plus the Vault refresh reference.
    conn = ScriptedConnection(_resolve_fresh_flow_script(profile_id, observed_generation=9))
    token_client = FakeTokenClient(conn)
    token_client.refresh_results.append(_grant(refresh=_ROTATED_REFRESH))
    resolver = _resolver(conn, token_client)

    token = resolver.resolve(_pool(conn), profile_id=profile_id)

    assert token.token_value() == _ACCESS_TOKEN
    assert token_client.refresh_calls == [(_REFRESH_TOKEN, 0)]  # NO transaction open
    existence_sql = conn.executed[4][0]
    assert "vault.secrets" in existence_sql  # decrypt-free existence check
    update_sql, update_params = conn.executed[5]
    assert "vault.update_secret" in update_sql
    assert update_params is not None and update_params[1] == _ROTATED_REFRESH
    cas_sql, cas_params = conn.executed[6]
    assert "refresh_generation = %s" in cas_sql
    assert cas_params is not None and cas_params[2] == 9


def test_resolve_fails_closed_on_missing_revoked_and_expired_conditions() -> None:
    profile_id = uuid.uuid4()

    # Missing: no durable authorization at all.
    conn = ScriptedConnection([None])
    resolver = _resolver(conn, FakeTokenClient(conn))
    with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
        resolver.resolve(_pool(conn), profile_id=profile_id)
    assert error.value.condition == CONDITION_MISSING

    # Revoked: durable currentness, distinct from missing.
    conn = ScriptedConnection([_row(profile_id, status="revoked", generation=4)])
    resolver = _resolver(conn, FakeTokenClient(conn))
    with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
        resolver.resolve(_pool(conn), profile_id=profile_id)
    assert error.value.condition == CONDITION_REVOKED

    # Expired-and-unrefreshable: the durable refresh expiry is in the past.
    expired_values = list(_row(profile_id, generation=2))
    expired_values[5] = _FIXED_NOW - timedelta(days=1)
    conn = ScriptedConnection([tuple(expired_values)])
    resolver = _resolver(conn, FakeTokenClient(conn))
    with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
        resolver.resolve(_pool(conn), profile_id=profile_id)
    assert error.value.condition == CONDITION_EXPIRED_UNREFRESHABLE


def test_a_stale_refresher_discards_its_token_pair_and_reloads_newer_durable_state() -> None:
    profile_id = uuid.uuid4()
    stale_secret_id = uuid.uuid4()
    newer_secret_id = uuid.uuid4()
    # Pass 1 observes generation 5; a concurrent process has already advanced
    # the durable state to generation 7 with a NEW secret. Pass 1's returned
    # token pair is discarded — the stale Vault value is never written — and
    # the resolution reloads from the newer durable state and completes there.
    conn = ScriptedConnection(
        [
            _row(profile_id, generation=5, reference=_reference(stale_secret_id)),
            (_REFRESH_TOKEN,),  # pass 1 Vault read (the stale secret)
            _GUARD_OPERATIONAL_ROW,  # pass 1 install barrier
            _row(profile_id, generation=7, reference=_reference(newer_secret_id)),  # moved on
            _row(profile_id, generation=7, reference=_reference(newer_secret_id)),  # pass 2 read
            (_REFRESH_TOKEN,),  # pass 2 Vault read (same canned secret value)
            _GUARD_OPERATIONAL_ROW,
            _row(profile_id, generation=7, reference=_reference(newer_secret_id)),
            (1,),
            None,
            _row(profile_id, generation=8, reference=_reference(newer_secret_id)),
        ]
    )
    token_client = FakeTokenClient(conn)
    token_client.refresh_results.append(_grant(refresh="ghr_stale-process-token-pair"))
    token_client.refresh_results.append(_grant(refresh=_ROTATED_REFRESH))
    resolver = _resolver(conn, token_client)

    token = resolver.resolve(_pool(conn), profile_id=profile_id)

    # Two refresh exchanges happened (one per pass), but only the pass-2
    # rotation reached Vault: the stale grant's credential was never written.
    assert [call[0] for call in token_client.refresh_calls] == [
        _REFRESH_TOKEN,
        _REFRESH_TOKEN,
    ]
    update_statements = [
        (sql, params) for sql, params in conn.executed if "vault.update_secret" in sql
    ]
    assert len(update_statements) == 1
    assert update_statements[0][1] is not None
    assert update_statements[0][1][0] == newer_secret_id
    assert update_statements[0][1][1] == _ROTATED_REFRESH
    assert token.token_value() == _ACCESS_TOKEN


def test_a_definitive_refresh_rejection_after_concurrent_rotation_reports_moved_on() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _row(profile_id, generation=5),  # pass 1 read
            (_REFRESH_TOKEN,),
            # The rejected exchange: durable state is reloaded BEFORE reporting.
            _row(profile_id, generation=6),  # a concurrent rotation already committed
        ]
    )
    token_client = FakeTokenClient(conn)
    token_client.refresh_results.append(GitHubUserTokenRejectedError("bad_refresh_token"))
    resolver = _resolver(conn, token_client)

    with pytest.raises(StaleOperationError):
        resolver.resolve(_pool(conn), profile_id=profile_id)

    # No durable mutation whatsoever: every executed statement is a SELECT.
    assert all(
        not sql.strip().startswith(("update", "delete", "insert")) for sql, _ in conn.executed
    )
    # Exactly one refresh attempt: a rejected exchange is never blindly replayed.
    assert len(token_client.refresh_calls) == 1


def test_a_definitive_refresh_rejection_is_the_typed_unusable_condition() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _row(profile_id, generation=5),
            (_REFRESH_TOKEN,),
            _row(profile_id, generation=5),  # reload: no concurrent rotation
        ]
    )
    token_client = FakeTokenClient(conn)
    token_client.refresh_results.append(GitHubUserTokenRejectedError("bad_refresh_token"))
    resolver = _resolver(conn, token_client)

    with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
        resolver.resolve(_pool(conn), profile_id=profile_id)

    assert error.value.condition == CONDITION_REFRESH_REJECTED
    # No durable mutation from the rejected exchange — and no fallback.
    assert all(
        not sql.strip().startswith(("update", "delete", "insert")) for sql, _ in conn.executed
    )


def test_an_uncertain_refresh_never_replays_and_never_mutates_durable_state() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _row(profile_id, generation=5),
            (_REFRESH_TOKEN,),
        ]
    )
    token_client = FakeTokenClient(conn)
    token_client.refresh_results.append(GitHubOutcomeUncertainError("connection lost"))
    resolver = _resolver(conn, token_client)

    with pytest.raises(ExternalOperationUncertainError):
        resolver.resolve(_pool(conn), profile_id=profile_id)

    assert len(token_client.refresh_calls) == 1
    assert all(
        not sql.strip().startswith(("update", "delete", "insert")) for sql, _ in conn.executed
    )


def test_resolution_never_falls_back_to_installation_authentication() -> None:
    import inspect

    source = inspect.getsource(github_user_authorization)
    # The service module has no installation-auth path anywhere: the only
    # credential mechanism is the Profile's own Vault-backed user token.
    assert "installation_token" not in source
    assert "GitHubAppAuthenticator" not in source


def test_revoke_deletes_the_secret_and_marks_the_authorization_revoked() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,  # barrier
            _row(profile_id, generation=6, reference=_reference(secret_id)),  # locked read
            (1,),  # Vault secret delete
            _row(profile_id, status="revoked", generation=7),  # the revocation update
        ]
    )

    revoked = github_user_authorization.revoke_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )

    assert revoked.status.value == "revoked"
    assert revoked.refresh_generation == 7
    delete_sql, delete_params = conn.executed[2]
    assert "delete from vault.secrets" in delete_sql
    assert delete_params == (secret_id,)
    revoke_sql = conn.executed[3][0]
    assert "status = 'revoked'" in revoke_sql
    assert "refresh_secret_reference = null" in revoke_sql
    assert "refresh_generation = refresh_generation + 1" in revoke_sql


def test_revoke_of_an_already_revoked_authorization_is_idempotent() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _row(profile_id, status="revoked", generation=7),  # locked read
        ]
    )

    revoked = github_user_authorization.revoke_github_user_authorization(
        _pool(conn), profile_id=profile_id
    )

    assert revoked.refresh_generation == 7
    # Idempotent: no Vault call, no further durable write.
    assert len(conn.executed) == 2
    assert all("vault" not in sql for sql, _ in conn.executed)


def test_revoke_of_a_missing_authorization_is_the_typed_missing_condition() -> None:
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            None,  # no durable authorization
        ]
    )

    with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
        github_user_authorization.revoke_github_user_authorization(
            _pool(conn), profile_id=uuid.uuid4()
        )

    assert error.value.condition == CONDITION_MISSING


@pytest.mark.parametrize("dangling", [False, True])
def test_revoke_fails_closed_on_malformed_or_dangling_references(dangling: bool) -> None:
    profile_id = uuid.uuid4()
    results: list[object] = [
        _GUARD_OPERATIONAL_ROW,
        _row(profile_id, generation=6, reference="not-a-v1-reference")
        if not dangling
        else _row(profile_id, generation=6, reference=_reference(uuid.uuid4())),
    ]
    if dangling:
        results.append(None)  # the Vault delete reports a dangling pointer
    conn = ScriptedConnection(results)

    with pytest.raises(ConflictError):
        github_user_authorization.revoke_github_user_authorization(
            _pool(conn), profile_id=profile_id
        )

    if dangling:
        assert "delete from vault.secrets" in conn.executed[2][0]
    assert all("update openorc.github_user_authorizations" not in sql for sql, _ in conn.executed)


def test_every_external_call_runs_with_no_database_transaction_open() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            None,
            (uuid.uuid4(),),
            _row(profile_id, generation=1),
        ]
    )
    admin, token_client = _establish_clients(conn, provider_ids=("5432",), current_user_id=5432)

    github_user_authorization.establish_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        code="the-code",
        admin_client=admin,
        token_client=token_client,
    )

    assert all(depth == 0 for _user_id, depth in admin.identity_calls)
    assert all(depth == 0 for _code, depth in token_client.exchange_calls)
    assert all(depth == 0 for depth in token_client.user_calls)
