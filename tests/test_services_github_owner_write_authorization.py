"""Deterministic tests for the Owner-accountable GitHub write boundary (issue #143).

Canned rows, a scripted SQL-connection seam, a scripted fake #142 resolver,
and a scripted fake GitHub adapter prove the composed boundary every
Owner-accountable GitHub write passes through: the account-operational
barrier composes FIRST, the exact Profile's owner-gated Workspace/Task
authorization follows, credential resolution happens only after both (the
typed unavailable-authorization conditions propagate with no
installation-token fallback), the user × installation × repository
intersection is proven for the exact routed identities, and a definitive
401-style rejection performs only the single bounded recovery the #142
lifecycle allows. Uncertain outcomes are never refreshed, retried, or
replayed. No live GitHub or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.github import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubProfileUserAccessToken,
    GitHubRateLimitedError,
    GitHubUserAccessToken,
)
from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    Repository,
    RepositoryMetadata,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.github_installation_route import (
    ResolvedRepositoryInstallationRoute,
)
from openorc.services.github_owner_write_authorization import (
    CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING,
    GitHubUserRepositoryAccessError,
    require_owner_write_authorization,
    resolve_owner_write_credential,
    resolve_owner_write_credential_after_rejection,
    translate_owner_write_failure,
)
from openorc.services.github_user_authorization import (
    CONDITION_MISSING,
    CONDITION_REVOKED,
    GitHubUserAuthorizationUnavailableError,
)

_OBSERVED = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)
_PROFILE_ID = uuid.uuid4()
_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_INSTALLATION_RECORD = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_STATE_TOKEN = uuid.uuid4()
_USER_TOKEN_VALUE = "ghu_owner_user_token"


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, tuple[Any, ...] | None | Exception]] = []

    def on(self, sql_marker: str, result: tuple[Any, ...] | None | Exception) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Nested psycopg transaction blocks (SAVEPOINTs under composition).
        yield


class FakePool:
    """Pool fake tracking checked-out connections for the transaction proof."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("owner-write boundary tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _guard_row() -> tuple[Any, ...]:
    return (None, None, None)


def _guard_active_attempt_row() -> tuple[Any, ...]:
    return ("active", uuid.uuid4(), _OBSERVED)


def _workspace_row(*, owner_profile_id: uuid.UUID = _PROFILE_ID) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner_profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")


def _task_row() -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        9001,
        42,
        "reviewing",
        None,
        "openorc/task-42-implementation",
        _STATE_TOKEN,
        None,
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _route() -> ResolvedRepositoryInstallationRoute:
    return ResolvedRepositoryInstallationRoute(
        repository=Repository(
            id=_REPOSITORY_ID,
            project_id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            identity=GitHubRepositoryIdentity(github_repository_id=_GITHUB_REPOSITORY_ID),
            metadata=RepositoryMetadata(
                owner_login="octocat",
                name="hello-world",
                html_url="https://github.com/octocat/hello-world",
                is_private=False,
                default_branch="main",
            ),
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
            github_installation_id=_INSTALLATION_RECORD,
        ),
        installation_id=_INSTALLATION_RECORD,
        github_installation_id=_EXTERNAL_INSTALLATION_ID,
    )


class FakeUserTokenResolver:
    """Scripted #142 resolver recording the exact Profile it resolves for."""

    def __init__(self) -> None:
        self.resolve_calls: list[uuid.UUID] = []
        self.evict_calls: list[uuid.UUID] = []
        self.resolve_error: Exception | None = None
        self._token = GitHubUserAccessToken(value=_USER_TOKEN_VALUE, expires_at=_OBSERVED)

    def resolve(self, pool: DatabasePool, *, profile_id: uuid.UUID) -> GitHubUserAccessToken:
        self.resolve_calls.append(profile_id)
        if self.resolve_error is not None:
            raise self.resolve_error
        return self._token

    def evict_cached_access_token(self, profile_id: uuid.UUID) -> None:
        self.evict_calls.append(profile_id)


class FakeGitHubAppClient:
    """Scripted fake proving the credential path and the no-transaction rule."""

    def __init__(self, pool: FakePool) -> None:
        self._pool = pool
        self.validation_errors: list[Exception] = []
        self.validation_calls: list[dict[str, Any]] = []

    def validate_user_installation_repository_access(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        github_repository_id: int,
    ) -> Any:
        assert self._pool.active_connections == 0, (
            "the boundary must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.validation_calls.append(
            {
                "profile_id": str(credential.profile_id),
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        if self.validation_errors:
            raise self.validation_errors.pop(0)
        return None

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the boundary never creates pull requests")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the boundary never merges pull requests")


def test_owner_write_authorization_composes_the_barrier_then_the_owner_boundary() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())

    task = require_owner_write_authorization(
        _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert task.id == _TASK_ID
    assert task.workspace_id == _WORKSPACE_ID
    # The account-deletion barrier read is the FIRST lock acquisition.
    assert "for key share" in conn.executed[0][0]


def test_an_unowned_workspace_is_the_uniform_not_found_before_any_credential() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row(owner_profile_id=uuid.uuid4()))
    resolver = FakeUserTokenResolver()

    with pytest.raises(NotFoundError):
        require_owner_write_authorization(
            _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    # The exact-Profile ownership gate failed first: no credential resolution.
    assert resolver.resolve_calls == []


def test_a_missing_profile_is_the_uniform_not_found_and_stops_before_the_workspace() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", None)
    resolver = FakeUserTokenResolver()

    with pytest.raises(NotFoundError):
        require_owner_write_authorization(
            _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    assert resolver.resolve_calls == []
    assert not any("from openorc.workspaces" in sql for sql, _ in conn.executed)


def test_an_active_account_deletion_attempt_rejects_the_write_before_the_workspace() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_active_attempt_row())
    resolver = FakeUserTokenResolver()

    with pytest.raises(ConflictError):
        require_owner_write_authorization(
            _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    assert resolver.resolve_calls == []
    assert len(conn.executed) == 1  # only the barrier read ran


def test_malformed_command_identities_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    resolver = FakeUserTokenResolver()

    with pytest.raises(InvalidCommandError):
        require_owner_write_authorization(
            _pool(conn),
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
        )
    with pytest.raises(InvalidCommandError):
        require_owner_write_authorization(
            _pool(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=None,  # type: ignore[arg-type]
        )

    assert conn.executed == []
    assert resolver.resolve_calls == []


def test_the_credential_resolves_the_exact_profile_and_proves_the_exact_route() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))

    credential = resolve_owner_write_credential(
        pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
    )

    assert isinstance(credential, GitHubProfileUserAccessToken)
    assert credential.profile_id == _PROFILE_ID
    assert resolver.resolve_calls == [_PROFILE_ID]
    assert github.validation_calls == [
        {
            "profile_id": str(_PROFILE_ID),
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "github_repository_id": _GITHUB_REPOSITORY_ID,
        }
    ]
    assert repr(credential) == "GitHubProfileUserAccessToken(<redacted>)"


def test_an_unavailable_user_authorization_propagates_with_no_fallback() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    for condition in (CONDITION_MISSING, CONDITION_REVOKED):
        resolver = FakeUserTokenResolver()
        resolver.resolve_error = GitHubUserAuthorizationUnavailableError(
            condition=condition, message="unusable"
        )
        github = FakeGitHubAppClient(FakePool(conn))

        with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
            resolve_owner_write_credential(
                pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
            )

        assert error.value.condition == condition
        # No fallback: the intersection proof never ran without a credential.
        assert github.validation_calls == []
        assert resolver.evict_calls == []


def test_an_intersection_failure_is_the_typed_condition_never_another_route() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))
    github.validation_errors.append(GitHubAuthorizationRejectedError("denied (status 403)"))

    with pytest.raises(GitHubUserRepositoryAccessError) as error:
        resolve_owner_write_credential(
            pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
        )

    assert error.value.condition == CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING
    # The proof ran once and failed closed: no second attempt, no eviction,
    # and the credential was resolved exactly once for the exact Profile.
    assert len(github.validation_calls) == 1
    assert resolver.evict_calls == []
    assert resolver.resolve_calls == [_PROFILE_ID]


def test_a_definitive_401_during_the_proof_recovers_once_and_retries_once() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))
    github.validation_errors.append(GitHubAuthenticationRejectedError("rejected (status 401)"))

    credential = resolve_owner_write_credential(
        pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
    )

    # The single bounded recovery allowed by #142: evict + re-resolve, then
    # the proof retried EXACTLY once and succeeded.
    assert resolver.evict_calls == [_PROFILE_ID]
    assert resolver.resolve_calls == [_PROFILE_ID, _PROFILE_ID]
    assert len(github.validation_calls) == 2
    assert credential.profile_id == _PROFILE_ID


def test_a_second_401_after_the_bounded_recovery_fails_closed() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))
    github.validation_errors.extend(
        [
            GitHubAuthenticationRejectedError("rejected (status 401)"),
            GitHubAuthenticationRejectedError("rejected again (status 401)"),
        ]
    )

    with pytest.raises(ExternalOperationFailedError):
        resolve_owner_write_credential(
            pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
        )

    # Exactly one recovery pass: one eviction, one re-resolution, one retry.
    assert resolver.evict_calls == [_PROFILE_ID]
    assert resolver.resolve_calls == [_PROFILE_ID, _PROFILE_ID]
    assert len(github.validation_calls) == 2


def test_uncertain_and_rate_limited_proof_outcomes_never_trigger_recovery() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))
    github.validation_errors.append(GitHubOutcomeUncertainError("the transport failed"))

    with pytest.raises(ExternalOperationUncertainError):
        resolve_owner_write_credential(
            pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
        )
    # An uncertain outcome is never refreshed, retried, or replayed.
    assert len(github.validation_calls) == 1
    assert resolver.evict_calls == []

    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))
    github.validation_errors.append(GitHubRateLimitedError("rate limited (status 429)"))
    with pytest.raises(ExternalOperationFailedError):
        resolve_owner_write_credential(
            pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
        )
    assert len(github.validation_calls) == 1
    assert resolver.evict_calls == []


def test_recovery_after_a_write_rejection_evicts_then_re_resolves_and_re_proves() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))

    credential = resolve_owner_write_credential_after_rejection(
        pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
    )

    # The bounded write-recovery path: evict the cached token, re-resolve
    # through the #142 lifecycle (exactly one resolution), and prove the
    # intersection with the fresh credential before the caller retries once.
    assert resolver.evict_calls == [_PROFILE_ID]
    assert resolver.resolve_calls == [_PROFILE_ID]
    assert len(github.validation_calls) == 1
    assert credential.profile_id == _PROFILE_ID


def test_a_401_during_the_recovery_re_proof_fails_closed_without_another_refresh() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(FakePool(conn))
    github.validation_errors.append(GitHubAuthenticationRejectedError("rejected (status 401)"))

    with pytest.raises(ExternalOperationFailedError):
        resolve_owner_write_credential_after_rejection(
            pool, resolver, github, profile_id=_PROFILE_ID, route=_route()
        )

    # The recovery pass is spent exactly once: one eviction, one
    # re-resolution, one re-proof — the rejected re-proof never initiates
    # another eviction/refresh cycle.
    assert resolver.evict_calls == [_PROFILE_ID]
    assert resolver.resolve_calls == [_PROFILE_ID]
    assert len(github.validation_calls) == 1


def test_the_write_failure_translation_is_honest_about_the_user_credential() -> None:
    error = translate_owner_write_failure(
        GitHubAuthenticationRejectedError("rejected (status 401)"),
        operation="merge request",
    )
    assert isinstance(error, ExternalOperationFailedError)
    assert "routed" not in str(error)
    assert "installation token" not in str(error)
    assert "user credential" in str(error)

    error = translate_owner_write_failure(
        GitHubAuthorizationRejectedError("denied (status 403)"), operation="merge request"
    )
    assert isinstance(error, ExternalOperationFailedError)
    assert "user authorization" in str(error)

    error = translate_owner_write_failure(
        GitHubRateLimitedError("rate limited (status 429)"), operation="merge request"
    )
    assert isinstance(error, ExternalOperationFailedError)
    assert "rate limited" in str(error)

    # The installation-flavored text belongs only to installation reads.
    from openorc.services.pull_request_publication import _translate_github_outcome

    read_error = _translate_github_outcome(GitHubAuthorizationRejectedError("denied"))
    assert isinstance(read_error, AuthorizationError)
    assert "routed github installation" in str(read_error)
