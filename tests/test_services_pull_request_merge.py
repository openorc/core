"""Deterministic tests for the exact-head merge request service (issue #63).

Canned rows and a scripted SQL-connection seam prove the merge primitive:
the exact-head authority guard (``require_task_pull_request_head``) rejecting
a stale caller before anything external, the no-database-transaction rule
across the GitHub calls, the documented merge outcome classification
(success → immediate authoritative reconciliation supplying the durable
merged facts; 409 head mismatch → stale operation; other definitive
rejection → known GitHub-owned policy failure; access/auth → the normalized
integration condition; timeout/uncertain → never automatically replayed —
the fake records exactly one merge request), and the sanctioned telemetry
vocabulary. No live GitHub or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.adapters.github import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubMergeRequestOutcome,
    GitHubMergeRequestResult,
    GitHubOutcomeUncertainError,
    GitHubProfileUserAccessToken,
    GitHubPullRequestObservation,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubUserAccessToken,
)
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.observability import (
    GITHUB_HEAD_SHA,
    GITHUB_PULL_REQUEST_NUMBER,
    OPERATION,
    TASK_ID,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import pull_request_merge
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_owner_write_authorization import (
    CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING,
    GitHubUserRepositoryAccessError,
)
from openorc.services.github_user_authorization import (
    CONDITION_MISSING,
    CONDITION_REVOKED,
    GitHubUserAuthorizationUnavailableError,
)

_OBSERVED = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
_MERGED_AT = datetime(2026, 9, 27, 14, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_PROFILE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_INSTALLATION_RECORD = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_MERGE_COMMIT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"

_SANCTIONED_ATTRIBUTE_NAMES = frozenset(
    {
        "openorc.request_id",
        "openorc.workspace_id",
        "openorc.task_id",
        "openorc.execution_id",
        "openorc.connection_id",
        "openorc.workflow_role",
        "openorc.operation",
        "openorc.github_installation_id",
        "openorc.github_repository",
        "openorc.github_issue_number",
        "openorc.github_pull_request_number",
        "openorc.github_head_sha",
        "openorc.rq_job_id",
    }
)


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
        raise AssertionError("merge service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _task_row() -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        9001,
        42,
        "waiting_for_owner",
        None,
        "openorc/task-42-implementation",
        uuid.uuid4(),
        None,
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _pull_request_row(
    *, head_sha: str = _HEAD_SHA, state: str = "open", merged_at: Any = None
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        _GITHUB_PR_ID,
        _PR_NUMBER,
        "openorc/task-42-implementation",
        "main",
        head_sha,
        state,
        merged_at,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_row() -> tuple[Any, ...]:
    return (
        _REPOSITORY_ID,
        uuid.uuid4(),
        _WORKSPACE_ID,
        _GITHUB_REPOSITORY_ID,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        _INSTALLATION_RECORD,
    )


def _installation_row() -> tuple[Any, ...]:
    return (
        _INSTALLATION_RECORD,
        _WORKSPACE_ID,
        _EXTERNAL_INSTALLATION_ID,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _workspace_row(*, owner_profile_id: uuid.UUID = _PROFILE_ID) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner_profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")


def _guard_row() -> tuple[Any, ...]:
    return (None, None, None)


def _guard_active_attempt_row() -> tuple[Any, ...]:
    return ("active", uuid.uuid4(), _OBSERVED)


class FakeUserTokenResolver:
    """Scripted #142 resolver recording the exact Profile it resolves for."""

    def __init__(self) -> None:
        self.resolve_calls: list[uuid.UUID] = []
        self.evict_calls: list[uuid.UUID] = []
        self.resolve_error: Exception | None = None
        self._token = GitHubUserAccessToken(value="ghu_owner_user_token", expires_at=_OBSERVED)

    def resolve(self, pool: DatabasePool, *, profile_id: uuid.UUID) -> GitHubUserAccessToken:
        self.resolve_calls.append(profile_id)
        if self.resolve_error is not None:
            raise self.resolve_error
        return self._token

    def evict_cached_access_token(self, profile_id: uuid.UUID) -> None:
        self.evict_calls.append(profile_id)


def _resolver(conn: ScriptedConnection) -> FakeUserTokenResolver:
    return FakeUserTokenResolver()


def _repository_observation() -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )


def _pr_observation(
    *, state: str = "closed", merged: bool = True, merged_at: datetime | None = _MERGED_AT
) -> GitHubPullRequestObservation:
    return GitHubPullRequestObservation(
        github_pr_id=_GITHUB_PR_ID,
        pull_number=_PR_NUMBER,
        head_ref="openorc/task-42-implementation",
        head_sha=_HEAD_SHA,
        base_ref="main",
        state=state,
        merged=merged,
        merged_at=merged_at,
    )


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self,
        *,
        pool: FakePool,
        repository_observation: GitHubRepositoryObservation | None = None,
        merge_result: GitHubMergeRequestResult | None = None,
        pull_request_observation: GitHubPullRequestObservation | None = None,
        error: Exception | None = None,
        merge_error: Exception | None = None,
        merge_errors: list[Exception] | None = None,
        intersection_errors: list[Exception] | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._merge_result = merge_result
        self._pull_request_observation = pull_request_observation
        self._error = error
        self._merge_error = merge_error
        self._merge_errors = list(merge_errors or [])
        self._intersection_errors = list(intersection_errors or [])
        self.repository_calls: list[dict[str, int]] = []
        self.merge_calls: list[dict[str, Any]] = []
        self.pull_request_calls: list[dict[str, Any]] = []
        self.validation_calls: list[dict[str, Any]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.repository_calls.append(
            {
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        if self._error is not None:
            raise self._error
        assert self._repository_observation is not None
        return self._repository_observation

    def get_repository_pull_request(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
    ) -> GitHubPullRequestObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.pull_request_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "pull_number": pull_number,
            }
        )
        assert self._pull_request_observation is not None
        return self._pull_request_observation

    def merge_pull_request(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
        expected_head_sha: str,
    ) -> GitHubMergeRequestResult:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.merge_calls.append(
            {
                "profile_id": str(credential.profile_id),
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "pull_number": pull_number,
                "expected_head_sha": expected_head_sha,
            }
        )
        if self._merge_errors:
            raise self._merge_errors.pop(0)
        if self._merge_error is not None:
            raise self._merge_error
        if self._error is not None:
            raise self._error
        assert self._merge_result is not None
        return self._merge_result

    def validate_user_installation_repository_access(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        github_repository_id: int,
    ) -> Any:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.validation_calls.append(
            {
                "profile_id": str(credential.profile_id),
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        if self._intersection_errors:
            raise self._intersection_errors.pop(0)
        return None

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not create pull requests")

    def get_repository_branch(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe branches")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project combined status")

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the service must not compose raw #58 validation operations")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")

    def get_repository_issue(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe issues")

    def get_issue_blocked_by(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe relationships")

    def get_issue_sub_issues(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe relationships")

    def get_issue_parent(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe relationships")

    def resolve_related_issue_endpoints(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not resolve related endpoints")


def _merge_success_scripts(conn: ScriptedConnection) -> None:
    """Script the full known-success flow: guard reads, merge, reconciliation.

    Phase 1 (guard): Task read, canonical-PR read, route reads. Phase 3 (the
    post-success authoritative reconciliation) repeats the read sequence and
    then the load-bearing write phase: Workspace read, Profile barrier read,
    Repository FOR UPDATE route revalidation, TaskPullRequest FOR UPDATE
    locked pre-image, and the observed-snapshot update carrying the merged
    facts.
    """
    merged_row = _pull_request_row(state="closed", merged_at=_MERGED_AT)
    for _ in range(2):
        conn.on("from openorc.profiles", _guard_row())

        conn.on("from openorc.workspaces", _workspace_row())

        conn.on("from openorc.tasks", _task_row())
        conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
        conn.on("from openorc.repositories where id", _repository_row())
        conn.on("from openorc.github_installations", _installation_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.profiles", (None, None, None))
    conn.on("from openorc.repositories where id", _repository_row())
    # The locked pre-image is still the open PR; the merged facts are what
    # this invocation's update establishes.
    conn.on("for update", _pull_request_row())
    conn.on("update openorc.task_pull_requests", merged_row)


def test_the_known_success_reconciles_the_durable_merged_facts() -> None:
    conn = ScriptedConnection()
    _merge_success_scripts(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    result = pull_request_merge.request_pull_request_merge(
        _pool(conn),
        github,
        _resolver(conn),
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_head_sha=_HEAD_SHA,
    )

    # The merge request was bound to the exact expected head, carried by the
    # accountable Profile's user credential.
    assert github.merge_calls == [
        {
            "profile_id": str(_PROFILE_ID),
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "owner_login": "octocat",
            "repository_name": "hello-world",
            "pull_number": _PR_NUMBER,
            "expected_head_sha": _HEAD_SHA,
        }
    ]
    assert result.merge_commit_sha == _MERGE_COMMIT_SHA
    assert result.merged_head_sha == _HEAD_SHA
    # The authoritative reconciliation supplies the durable merged facts.
    assert result.pull_request.state is TaskPullRequestState.CLOSED
    assert result.pull_request.merged_at == _MERGED_AT
    assert result.reconciliation.state_changed is True
    assert result.reconciliation.head_changed is False
    assert result.reconciliation.updated is True
    # Phase 1 now composes the account barrier + Workspace Owner
    # authorization reads before the guard reads (issue #143).
    assert len(conn.executed) == 15


def test_a_stale_caller_head_is_rejected_before_anything_external() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on(
        "from openorc.task_pull_requests where task_id",
        _pull_request_row(head_sha=_OTHER_SHA),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    with pytest.raises(StaleOperationError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    # The exact-head authority guard failed first: no GitHub call at all.
    assert github.repository_calls == []
    assert github.merge_calls == []


def test_the_documented_head_mismatch_is_a_stale_operation_never_replayed() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.HEAD_MISMATCH, merge_commit_sha=None
        ),
    )

    with pytest.raises(StaleOperationError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    # Exactly one merge request: GitHub's sha guard refused it, and the
    # service never blindly retries.
    assert len(github.merge_calls) == 1


def test_a_known_policy_state_rejection_is_a_known_failure() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.REJECTED, merge_commit_sha=None
        ),
    )

    with pytest.raises(ExternalOperationFailedError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    # GitHub owns the merge policy; OpenOrc owns no transition here.
    assert len(github.merge_calls) == 1
    assert not any(sql.startswith("update") for sql, _ in conn.executed)


def test_access_and_uncertain_merge_outcomes_classify_and_never_replay() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    access_lost = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubAuthorizationRejectedError("denied (status 403)"),
    )
    with pytest.raises(AuthorizationError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            access_lost,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )
    assert access_lost.merge_calls == []

    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    rate_limited = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubRateLimitedError("rate limited (status 403)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            rate_limited,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    auth_rejected = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_error=GitHubAuthenticationRejectedError("rejected (status 401)"),
    )
    auth_resolver = FakeUserTokenResolver()
    with pytest.raises(ExternalOperationFailedError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            auth_rejected,
            auth_resolver,
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )
    # A definitive 401-style non-delivery under the user credential performs
    # ONLY the bounded #142 recovery (one evict + one re-resolve + the
    # intersection re-proof) and then the merge retried EXACTLY once; the
    # second rejection is the known failure — never a further recovery.
    assert len(auth_rejected.merge_calls) == 2
    assert auth_resolver.evict_calls == [_PROFILE_ID]
    assert auth_resolver.resolve_calls == [_PROFILE_ID, _PROFILE_ID]
    assert len(auth_rejected.validation_calls) == 2

    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    uncertain = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_error=GitHubOutcomeUncertainError("the transport failed"),
    )
    with pytest.raises(ExternalOperationUncertainError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            uncertain,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )
    # An uncertain mutating call is never automatically replayed: exactly
    # one merge request was made and recovery belongs to the later layer.
    assert len(uncertain.merge_calls) == 1


def test_a_missing_canonical_record_is_the_uniform_not_found_with_no_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    with pytest.raises(NotFoundError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    assert github.repository_calls == []
    assert github.merge_calls == []


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(pool=FakePool(conn))

    with pytest.raises(InvalidCommandError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha="   ",
        )
    with pytest.raises(InvalidCommandError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )
    assert conn.executed == []


def test_telemetry_uses_only_the_sanctioned_attribute_vocabulary() -> None:
    conn = ScriptedConnection()
    _merge_success_scripts(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            _resolver(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    spans = exporter.get_finished_spans()
    assert "pull_request_merge.request_pull_request_merge" in [span.name for span in spans]
    for span in spans:
        assert set(span.attributes or {}) <= _SANCTIONED_ATTRIBUTE_NAMES
    service_span = next(
        span for span in spans if span.name == "pull_request_merge.request_pull_request_merge"
    )
    attributes = service_span.attributes or {}
    assert attributes[OPERATION] == "pull_request_merge.request_pull_request_merge"
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)
    assert attributes[GITHUB_HEAD_SHA] == _HEAD_SHA
    assert attributes[GITHUB_PULL_REQUEST_NUMBER] == _PR_NUMBER


def test_an_unowned_workspace_is_the_uniform_not_found_before_any_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row(owner_profile_id=uuid.uuid4()))
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    with pytest.raises(NotFoundError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            resolver,
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    # The exact-Profile ownership gate failed first: no credential
    # resolution and no GitHub call — another Profile is never substituted,
    # even if it could access the same repository.
    assert resolver.resolve_calls == []
    assert github.repository_calls == []
    assert github.merge_calls == []


def test_an_active_account_deletion_attempt_blocks_the_merge_before_any_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_active_attempt_row())
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    with pytest.raises(ConflictError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            resolver,
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    assert resolver.resolve_calls == []
    assert github.repository_calls == []
    assert github.merge_calls == []
    assert len(conn.executed) == 1  # only the barrier read ran


def test_a_missing_or_revoked_user_authorization_blocks_the_merge_with_no_fallback() -> None:
    for condition in (CONDITION_MISSING, CONDITION_REVOKED):
        conn = ScriptedConnection()
        conn.on("from openorc.profiles", _guard_row())
        conn.on("from openorc.workspaces", _workspace_row())
        conn.on("from openorc.tasks", _task_row())
        conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
        conn.on("from openorc.repositories where id", _repository_row())
        conn.on("from openorc.github_installations", _installation_row())
        resolver = FakeUserTokenResolver()
        resolver.resolve_error = GitHubUserAuthorizationUnavailableError(
            condition=condition, message="unusable"
        )
        github = FakeGitHubAppClient(
            pool=FakePool(conn),
            repository_observation=_repository_observation(),
            merge_result=GitHubMergeRequestResult(
                outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
            ),
        )

        with pytest.raises(GitHubUserAuthorizationUnavailableError) as error:
            pull_request_merge.request_pull_request_merge(
                _pool(conn),
                github,
                resolver,
                profile_id=_PROFILE_ID,
                workspace_id=_WORKSPACE_ID,
                task_id=_TASK_ID,
                expected_head_sha=_HEAD_SHA,
            )

        assert error.value.condition == condition
        # No installation-token fallback: no GitHub call happened at all.
        assert github.repository_calls == []
        assert github.merge_calls == []
        assert github.pull_request_calls == []
        assert resolver.evict_calls == []


def test_the_user_installation_repository_intersection_failure_blocks_the_merge() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
        intersection_errors=[GitHubAuthorizationRejectedError("denied (status 403)")],
    )

    with pytest.raises(GitHubUserRepositoryAccessError) as error:
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            resolver,
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    assert error.value.condition == CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING
    # The merge was never attempted on another route or credential.
    assert github.repository_calls == []
    assert github.merge_calls == []
    assert resolver.evict_calls == []


def test_another_profile_cannot_substitute_the_owner_even_with_repository_access() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.profiles", _guard_row())
    conn.on("from openorc.workspaces", _workspace_row(owner_profile_id=uuid.uuid4()))
    resolver = FakeUserTokenResolver()
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        merge_result=GitHubMergeRequestResult(
            outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=_MERGE_COMMIT_SHA
        ),
    )

    with pytest.raises(NotFoundError):
        pull_request_merge.request_pull_request_merge(
            _pool(conn),
            github,
            resolver,
            profile_id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_head_sha=_HEAD_SHA,
        )

    # The caller-supplied profile_id was never trusted as authority: the
    # Workspace Owner gate failed first and nothing was resolved or called.
    assert resolver.resolve_calls == []
    assert github.repository_calls == []
    assert github.merge_calls == []
