"""Deterministic tests for the GitHub authorization flow service (issue #373).

The ordinary suite cannot execute Postgres or call live GitHub/Supabase: a
scripted connection seam proves the start composition (the account-operational
barrier first, the pending-flow supersession, the purpose guards, the captured
revocation evidence, and the Vault-backed verifier install), the documented
authorize-URL safety (only public identity and correlation material in the
query; the verifier never leaves the Vault boundary), the atomic single-use
consume with its uniform detail-free unavailable condition, the exact
completion delegation to the #142 establishment (the same-human binding proof
and the captured revocation fence), the provider-terminated abort, the safe
status projection, and the Owner-facing revoke translation. The establishment
service's own matrices stay in its owning module; a completion recorder proves
the exact delegation seam only.
"""

from __future__ import annotations

import base64
import hashlib
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from openorc.adapters.github import GitHubUserTokenClient
from openorc.adapters.supabase import SupabaseAuthAdminClient
from openorc.domain.github_user_authorization import (
    GitHubUserAuthorization,
    GitHubUserAuthorizationStatus,
)
from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlowPurpose,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import github_user_authorization_flows
from openorc.services.errors import (
    ConflictError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.github_user_authorization import (
    CONDITION_MISSING,
    GitHubUserAuthorizationUnavailableError,
    GitHubUserIdentityMismatchError,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
_EXPIRES = _OBSERVED + timedelta(seconds=600)
_VERIFIER = "a" * 43
_STATE = "b" * 43
_CLIENT_ID = "Iv1.user-flow-client-id"

# The completion delegates to the monkeypatched #142 establishment, so the
# external client arguments never reach real code; the tests pass typed
# stand-in sentinels to satisfy the delegation signature.
_ANY_ADMIN = cast("SupabaseAuthAdminClient", object())
_ANY_TOKEN_CLIENT = cast("GitHubUserTokenClient", object())


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
        raise AssertionError("flow service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_GUARD_OPERATIONAL_ROW = (None, None, None)

# --- canned rows -------------------------------------------------------------


def _auth_row(
    profile_id: Any, *, status: str = "active", generation: int = 4, revocation_count: int = 0
) -> tuple[Any, ...]:
    revoked = status == "revoked"
    return (
        profile_id,
        5432,
        "octocat",
        status,
        None if revoked else "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4()),
        None if revoked else _OBSERVED,
        generation,
        revocation_count,
        _OBSERVED,
        _OBSERVED if revoked else None,
        _OBSERVED,
        _OBSERVED,
    )


def _flow_row(
    profile_id: Any, *, purpose: str = "authorize", revocation_count: int = 0
) -> tuple[Any, ...]:
    return (
        profile_id,
        purpose,
        _STATE,
        "openorc:github-user-flow-verifier:v1:vault:" + str(uuid.uuid4()),
        revocation_count,
        _OBSERVED,
        _EXPIRES,
    )


def _authorization(profile_id: Any, **overrides: Any) -> GitHubUserAuthorization:
    values: dict[str, Any] = {
        "profile_id": profile_id,
        "github_user_id": 5432,
        "github_login": "octocat",
        "status": GitHubUserAuthorizationStatus.ACTIVE,
        "refresh_secret_reference": "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4()),
        "refresh_expires_at": _OBSERVED,
        "refresh_generation": 5,
        "revocation_count": 0,
        "authorized_at": _OBSERVED,
        "revoked_at": None,
        "created_at": _OBSERVED,
        "updated_at": _OBSERVED,
    }
    values.update(overrides)
    return GitHubUserAuthorization(**values)


class _EstablishRecorder:
    """Records the exact completion delegation to the #142 establishment."""

    def __init__(self, outcome: Any = None, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._outcome = outcome
        self._error = error

    def __call__(self, pool: Any, **command: Any) -> Any:
        self.calls.append(command)
        if self._error is not None:
            raise self._error
        assert self._outcome is not None
        return self._outcome


@pytest.fixture
def establish_recorder(monkeypatch: pytest.MonkeyPatch):
    def _make(outcome: Any = None, error: Exception | None = None) -> _EstablishRecorder:
        recorder = _EstablishRecorder(outcome=outcome, error=error)
        monkeypatch.setattr(
            github_user_authorization_flows,
            "establish_github_user_authorization",
            recorder,
        )
        return recorder

    return _make


def _s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


# --- start -------------------------------------------------------------------


def test_start_composes_the_barrier_supersedes_and_installs_the_flow() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,  # the account barrier FIRST
            None,  # supersession: no prior pending flow
            None,  # the authorization read: none durably present
            (secret_id,),  # the Vault verifier create
            _flow_row(profile_id),  # the pending-flow insert
        ]
    )

    started = github_user_authorization_flows.start_github_user_authorization_flow(
        _pool(conn),
        profile_id=profile_id,
        purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
        user_flow_client_id=_CLIENT_ID,
        randomness=lambda: next(iter([_VERIFIER, _STATE])),
        clock=lambda: _OBSERVED,
    )

    # Ordering: the barrier, the supersession delete, the guard read, the
    # Vault create, the insert — one composed transaction.
    supersede_sql, supersede_params = conn.executed[1]
    assert "delete from openorc.github_user_authorization_flows" in supersede_sql
    assert supersede_params == (profile_id,)
    guard_sql, guard_params = conn.executed[2]
    assert "github_user_authorizations" in guard_sql
    assert guard_params == (profile_id,)
    create_sql, create_params = conn.executed[3]
    assert "vault.create_secret" in create_sql
    assert create_params is not None and create_params[0] == _VERIFIER
    insert_sql, insert_params = conn.executed[4]
    assert "insert into openorc.github_user_authorization_flows" in insert_sql
    assert insert_params is not None
    assert insert_params[1] == "authorize"
    assert insert_params[4] == 0  # the captured fence evidence: never authorized

    # The documented authorize URL: public identity/correlation material only.
    assert started.authorize_url.startswith("https://github.com/login/oauth/authorize?")
    query = dict(pair.split("=", 1) for pair in started.authorize_url.split("?", 1)[1].split("&"))
    assert set(query) == {"client_id", "state", "code_challenge", "code_challenge_method"}
    assert query["client_id"] == _CLIENT_ID
    assert query["state"] == _STATE
    assert query["code_challenge"] == _s256(_VERIFIER)
    assert query["code_challenge_method"] == "S256"
    # The PKCE verifier never enters the URL.
    assert _VERIFIER not in started.authorize_url
    assert started.purpose is GitHubUserAuthorizationFlowPurpose.AUTHORIZE
    assert started.expires_at == _EXPIRES


def test_start_captures_the_durable_revocation_count_of_an_existing_row() -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            None,  # supersession: none pending
            _auth_row(profile_id, status="revoked", revocation_count=3),  # guard read
            (secret_id,),  # Vault verifier create
            _flow_row(profile_id, purpose="reauthorize", revocation_count=3),  # the insert
        ]
    )

    started = github_user_authorization_flows.start_github_user_authorization_flow(
        _pool(conn),
        profile_id=profile_id,
        purpose=GitHubUserAuthorizationFlowPurpose.REAUTHORIZE,
        user_flow_client_id=_CLIENT_ID,
        randomness=lambda: next(iter([_VERIFIER, _STATE])),
    )

    assert started.purpose is GitHubUserAuthorizationFlowPurpose.REAUTHORIZE
    # The captured fence evidence is the durable count of the existing row:
    # this is what fences the eventual completion against intervening
    # revocations (issue #373).
    assert conn.executed[4][1] is not None
    assert conn.executed[4][1][4] == 3


# --- start guards and fail-closed inputs -------------------------------------


def test_start_authorize_conflicts_with_an_active_authorization() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            None,  # supersession: none pending
            _auth_row(profile_id, status="active"),  # an active authorization
        ]
    )

    with pytest.raises(ConflictError, match="reauthorization is the supported action"):
        github_user_authorization_flows.start_github_user_authorization_flow(
            _pool(conn),
            profile_id=profile_id,
            purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
            user_flow_client_id=_CLIENT_ID,
            randomness=lambda: next(iter([_VERIFIER, _STATE])),
        )

    # No Vault create and no insert followed the refused guard.
    assert all("vault.create_secret" not in sql for sql, _ in conn.executed)
    assert all(
        "insert into openorc.github_user_authorization_flows" not in sql for sql, _ in conn.executed
    )


def test_start_reauthorize_conflicts_with_no_authorization() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            None,
            None,  # no authorization row at all
        ]
    )

    with pytest.raises(ConflictError, match="authorization is the supported action"):
        github_user_authorization_flows.start_github_user_authorization_flow(
            _pool(conn),
            profile_id=profile_id,
            purpose=GitHubUserAuthorizationFlowPurpose.REAUTHORIZE,
            user_flow_client_id=_CLIENT_ID,
            randomness=lambda: next(iter([_VERIFIER, _STATE])),
        )

    assert all("vault.create_secret" not in sql for sql, _ in conn.executed)


def test_start_fails_closed_on_an_unconfigured_client_id_before_any_effect() -> None:
    conn = ScriptedConnection([])

    with pytest.raises(IntegrationNotConfiguredError):
        github_user_authorization_flows.start_github_user_authorization_flow(
            _pool(conn),
            profile_id=uuid.uuid4(),
            purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
            user_flow_client_id="",
            randomness=lambda: next(iter([_VERIFIER, _STATE])),
        )

    assert conn.executed == []


@pytest.mark.parametrize("bad_verifier", ["short", "v" * 129, "bad charset!"])
def test_start_fails_closed_on_a_malformed_generated_verifier(bad_verifier: str) -> None:
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        github_user_authorization_flows.start_github_user_authorization_flow(
            _pool(conn),
            profile_id=uuid.uuid4(),
            purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
            user_flow_client_id=_CLIENT_ID,
            randomness=lambda: next(iter([bad_verifier, _STATE])),
        )

    assert conn.executed == []


def test_start_fails_closed_on_a_non_positive_lifetime() -> None:
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        github_user_authorization_flows.start_github_user_authorization_flow(
            _pool(conn),
            profile_id=uuid.uuid4(),
            purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
            user_flow_client_id=_CLIENT_ID,
            randomness=lambda: next(iter([_VERIFIER, _STATE])),
            flow_lifetime_seconds=0,
        )

    assert conn.executed == []


# --- completion --------------------------------------------------------------


def test_complete_consumes_and_delegates_the_exact_fenced_establishment(
    establish_recorder,
) -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    row = _flow_row(profile_id, revocation_count=3)
    row = (
        row[0],
        row[1],
        row[2],
        f"openorc:github-user-flow-verifier:v1:vault:{secret_id}",
        *row[4:],
    )
    outcome = _authorization(profile_id)
    recorder = establish_recorder(outcome=outcome)
    conn = ScriptedConnection(
        [
            row,  # the conditional delete matched
            (_VERIFIER,),  # the in-transaction Vault decrypt
        ]
    )

    completed = github_user_authorization_flows.complete_github_user_authorization_flow(
        _pool(conn),
        state=_STATE,
        code="the-oauth-code",
        admin_client=_ANY_ADMIN,
        token_client=_ANY_TOKEN_CLIENT,
    )

    assert completed is outcome
    consume_sql, consume_params = conn.executed[0]
    assert "delete from openorc.github_user_authorization_flows" in consume_sql
    assert "where state = %s and expires_at > now()" in consume_sql
    assert consume_params == (_STATE,)
    assert "vault.decrypted_secrets" in conn.executed[1][0]
    # The exact delegation: the bound Profile, the presented code, the
    # correlation's verifier, and the captured fence evidence.
    assert recorder.calls == [
        {
            "profile_id": profile_id,
            "code": "the-oauth-code",
            "admin_client": recorder.calls[0]["admin_client"],
            "token_client": recorder.calls[0]["token_client"],
            "code_verifier": _VERIFIER,
            "expected_revocation_count": 3,
        }
    ]


def test_complete_refuses_unknown_expired_or_replayed_states_uniformly(
    establish_recorder,
) -> None:
    recorder = establish_recorder()
    conn = ScriptedConnection([None])

    with pytest.raises(
        github_user_authorization_flows.GitHubUserAuthorizationFlowUnavailableError
    ) as error:
        github_user_authorization_flows.complete_github_user_authorization_flow(
            _pool(conn),
            state=_STATE,
            code="the-oauth-code",
            admin_client=_ANY_ADMIN,
            token_client=_ANY_TOKEN_CLIENT,
        )

    # One uniform, detail-free condition — never a distinguishing oracle.
    assert error.value.condition == "unavailable"
    assert "state" not in str(error.value)
    assert recorder.calls == []
    assert "delete from openorc.github_user_authorization_flows" in conn.executed[0][0]


def test_complete_propagates_the_establishment_errors_unchanged(establish_recorder) -> None:
    profile_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    row = _flow_row(profile_id)
    row = (
        row[0],
        row[1],
        row[2],
        f"openorc:github-user-flow-verifier:v1:vault:{secret_id}",
        *row[4:],
    )
    recorder = establish_recorder(
        error=GitHubUserIdentityMismatchError(
            "the authorized GitHub account is not the account that signed in to this Profile"
        )
    )
    conn = ScriptedConnection([row, (_VERIFIER,)])

    with pytest.raises(GitHubUserIdentityMismatchError):
        github_user_authorization_flows.complete_github_user_authorization_flow(
            _pool(conn),
            state=_STATE,
            code="the-oauth-code",
            admin_client=_ANY_ADMIN,
            token_client=_ANY_TOKEN_CLIENT,
        )

    # The correlation is spent: any completion outcome leaves it consumed and
    # the caller retries from a fresh authenticated start.
    assert len(recorder.calls) == 1


@pytest.mark.parametrize(
    ("state_input", "code_input"),
    [("", "code"), (None, "code"), ("s" * 2049, "code"), ("state", ""), ("state", None)],
)
def test_complete_fails_closed_on_unusable_callback_inputs(
    establish_recorder, state_input: Any, code_input: Any
) -> None:
    recorder = establish_recorder()
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        github_user_authorization_flows.complete_github_user_authorization_flow(
            _pool(conn),
            state=state_input,
            code=code_input,
            admin_client=_ANY_ADMIN,
            token_client=_ANY_TOKEN_CLIENT,
        )

    # Nothing was read, consumed, or delegated.
    assert conn.executed == []
    assert recorder.calls == []


def test_abort_discards_the_provider_terminated_correlation() -> None:
    conn = ScriptedConnection([None])

    github_user_authorization_flows.abort_pending_github_user_authorization_flow(
        _pool(conn), state=_STATE
    )

    sql, params = conn.executed[0]
    assert "delete from openorc.github_user_authorization_flows" in sql
    assert "where state = %s" in sql
    assert params == (_STATE,)
    assert "vault" not in sql


# --- status projection and the revoke seam -----------------------------------


def test_status_projects_the_missing_revoked_and_active_states_safely() -> None:
    profile_id = uuid.uuid4()
    pending_row = _flow_row(profile_id, revocation_count=2)
    conn = ScriptedConnection([None, pending_row])  # no authorization, one pending flow

    missing = github_user_authorization_flows.read_github_user_authorization_status(
        _pool(conn), profile_id=profile_id
    )
    assert missing.status == "missing"
    assert missing.github_user_id is None
    assert missing.pending_flow is not None
    assert missing.pending_flow.purpose is GitHubUserAuthorizationFlowPurpose.AUTHORIZE
    assert missing.pending_flow.expires_at == _EXPIRES

    revoked_conn = ScriptedConnection(
        [_auth_row(profile_id, status="revoked", revocation_count=3), None]
    )
    revoked = github_user_authorization_flows.read_github_user_authorization_status(
        _pool(revoked_conn), profile_id=profile_id
    )
    assert revoked.status == "revoked"
    assert revoked.github_user_id == 5432
    assert revoked.refresh_expires_at is None
    assert revoked.pending_flow is None

    active_conn = ScriptedConnection([_auth_row(profile_id), None])
    active = github_user_authorization_flows.read_github_user_authorization_status(
        _pool(active_conn), profile_id=profile_id
    )
    assert active.status == "active"
    assert active.refresh_expires_at == _OBSERVED


def test_status_projection_is_structurally_credential_free() -> None:
    # No reference, verifier, correlation-state, or generation field exists on
    # the wire-bound projection; login is presentation metadata only.
    fields = github_user_authorization_flows.GitHubUserAuthorizationStatusRead.__dataclass_fields__
    forbidden = (
        "refresh_secret_reference",
        "pkce_verifier",
        "pkce_verifier_reference",
        "state",
        "refresh_generation",
        "revocation_count",
        "token",
    )
    assert not any(any(name in field for name in forbidden) for field in fields)


def test_revoke_seam_translates_missing_to_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing(pool: Any, *, profile_id: Any) -> Any:
        raise GitHubUserAuthorizationUnavailableError(
            condition=CONDITION_MISSING,
            message="this Profile has no GitHub authorization to revoke",
        )

    monkeypatch.setattr(
        github_user_authorization_flows,
        "revoke_github_user_authorization_service",
        _missing,
    )

    with pytest.raises(NotFoundError):
        github_user_authorization_flows.revoke_profile_github_user_authorization(
            _pool(ScriptedConnection([])), profile_id=uuid.uuid4()
        )


def test_revoke_seam_passes_the_durable_revocation_through(monkeypatch: pytest.MonkeyPatch) -> None:
    profile_id = uuid.uuid4()
    revoked = _authorization(
        profile_id,
        status=GitHubUserAuthorizationStatus.REVOKED,
        refresh_secret_reference=None,
        refresh_expires_at=None,
        revoked_at=_OBSERVED,
    )
    seen: list[Any] = []

    def _revoke(pool: Any, *, profile_id: Any) -> Any:
        seen.append(profile_id)
        return revoked

    monkeypatch.setattr(
        github_user_authorization_flows,
        "revoke_github_user_authorization_service",
        _revoke,
    )

    result = github_user_authorization_flows.revoke_profile_github_user_authorization(
        _pool(ScriptedConnection([])), profile_id=profile_id
    )

    assert result is revoked
    assert seen == [profile_id]
