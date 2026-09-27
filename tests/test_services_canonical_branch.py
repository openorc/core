"""Deterministic tests for the canonical Task branch verification service
(issue #63).

Canned rows and a scripted SQL-connection seam prove the focused
verify-and-bind boundary: command classification before any state is
touched, the exact Phase 2A currentness/state-token guard, the GitHub-only
exact committed head SHA (never caller/runtime state), the missing/wrong-
repository branch as the normalized access condition, the bind-once
conflict for a differing claim once bound, the branch-ownership exclusivity
conflict translated from the durable unique index, stale-operation
classification, and the sanctioned telemetry vocabulary. No live GitHub or
Postgres access.
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
from psycopg.errors import UniqueViolation

from openorc.adapters.github import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubBranchObservation,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
)
from openorc.observability import OPERATION, TASK_ID, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import canonical_branch
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    StaleOperationError,
)

_OBSERVED = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_TOKEN = uuid.uuid4()
_NEW_TOKEN = uuid.uuid4()
_INSTALLATION_RECORD = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_CLAIMED_BRANCH = "openorc/task-42-implementation"
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"

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
        raise AssertionError("canonical branch service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _task_row(
    *,
    task_id: uuid.UUID = _TASK_ID,
    workspace_id: uuid.UUID = _WORKSPACE_ID,
    status: str = "implementing",
    archived_at: Any = None,
    canonical_feature_branch: str | None = None,
    state_token: uuid.UUID = _TOKEN,
) -> tuple[Any, ...]:
    return (
        task_id,
        workspace_id,
        _REPOSITORY_ID,
        9001,
        42,
        status,
        archived_at,
        canonical_feature_branch,
        state_token,
        None,
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_row(route: Any = _INSTALLATION_RECORD) -> tuple[Any, ...]:
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
        route,
    )


def _installation_row(route: Any = _INSTALLATION_RECORD) -> tuple[Any, ...]:
    return (
        route,
        _WORKSPACE_ID,
        _EXTERNAL_INSTALLATION_ID,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_observation() -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )


def _branch_observation(
    branch: str = _CLAIMED_BRANCH, sha: str = _HEAD_SHA
) -> GitHubBranchObservation:
    return GitHubBranchObservation(branch_name=branch, head_sha=sha)


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self,
        *,
        pool: FakePool,
        repository_observation: GitHubRepositoryObservation | None = None,
        branch_observation: GitHubBranchObservation | None = None,
        error: Exception | None = None,
        branch_error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._branch_observation = branch_observation
        self._error = error
        self._branch_error = branch_error
        self.repository_calls: list[dict[str, int]] = []
        self.branch_calls: list[dict[str, Any]] = []

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

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.branch_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "branch_name": branch_name,
            }
        )
        if self._branch_error is not None:
            raise self._branch_error
        assert self._branch_observation is not None
        return self._branch_observation

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

    def get_repository_by_address(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not resolve addresses")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe pull requests")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project combined status")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not request merges")


def _verify_scripts_unbound(conn: ScriptedConnection) -> None:
    """Script the unbound-claim flow: guard read, route reads, bind, update."""
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on(
        "update openorc.tasks",
        _task_row(canonical_feature_branch=_CLAIMED_BRANCH, state_token=_NEW_TOKEN),
    )


def test_a_verified_claim_binds_the_github_sourced_branch_and_head() -> None:
    conn = ScriptedConnection()
    _verify_scripts_unbound(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(),
    )

    result = canonical_branch.verify_canonical_task_branch(
        _pool(conn),
        github,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        claimed_branch=_CLAIMED_BRANCH,
    )

    assert result.bound_now is True
    assert result.branch == _CLAIMED_BRANCH
    assert result.verified_head_sha == _HEAD_SHA
    assert result.task.state_token == _NEW_TOKEN
    assert result.task.canonical_feature_branch == _CLAIMED_BRANCH
    # The branch read is addressed through the freshly observed address,
    # never stored mutable metadata.
    assert github.branch_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "owner_login": "octocat",
            "repository_name": "hello-world",
            "branch_name": _CLAIMED_BRANCH,
        }
    ]
    assert len(conn.executed) == 5


def test_the_exact_committed_sha_comes_from_github_not_the_caller() -> None:
    conn = ScriptedConnection()
    _verify_scripts_unbound(conn)
    github_sha = "fedcba9876543210fedcba9876543210fedcba98"
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(sha=github_sha),
    )

    result = canonical_branch.verify_canonical_task_branch(
        _pool(conn),
        github,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        claimed_branch=_CLAIMED_BRANCH,
    )

    assert result.verified_head_sha == github_sha


def test_a_missing_branch_is_the_normalized_access_condition() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_error=GitHubAuthorizationRejectedError("GitHub denied access (status 404)"),
    )

    with pytest.raises(AuthorizationError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )

    # Nothing was bound: the missing branch is an integration condition,
    # never permission to trust runtime-local state.
    assert not any("update openorc.tasks" in sql for sql, _ in conn.executed)


def test_a_differing_claim_once_bound_is_a_conflict_and_no_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row(canonical_feature_branch=_CLAIMED_BRANCH))
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(),
    )

    with pytest.raises(ConflictError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch="openorc/some-other-branch",
        )

    assert github.repository_calls == []
    assert github.branch_calls == []
    # Remediation continues on the canonical branch; nothing was rebound.
    assert len(conn.executed) == 1


def test_remediation_reverifies_the_bound_canonical_branch_head() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row(canonical_feature_branch=_CLAIMED_BRANCH))
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    remediated_sha = "fedcba9876543210fedcba9876543210fedcba98"
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(sha=remediated_sha),
    )

    result = canonical_branch.verify_canonical_task_branch(
        _pool(conn),
        github,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN,
        claimed_branch=_CLAIMED_BRANCH,
    )

    # Later remediation continues on the canonical branch rather than
    # replacing it: no binding write, a fresh GitHub-committed head.
    assert result.bound_now is False
    assert result.verified_head_sha == remediated_sha
    assert result.task.state_token == _TOKEN
    assert len(conn.executed) == 3


def test_stale_currentness_is_rejected_before_any_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row(state_token=_NEW_TOKEN))
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(),
    )

    with pytest.raises(StaleOperationError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )

    assert github.repository_calls == []
    assert github.branch_calls == []


def test_branch_ownership_collision_is_a_conflict() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    conn.on("from openorc.tasks", _task_row())
    conn.on(
        "update openorc.tasks",
        UniqueViolation("tasks_repository_canonical_branch_current_uniq"),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(),
    )

    with pytest.raises(ConflictError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )


def test_known_and_uncertain_provider_conditions_stay_distinct() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    rate_limited = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubRateLimitedError("rate limited (status 403)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            rate_limited,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )

    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    auth_rejected = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubAuthenticationRejectedError("rejected credential (status 401)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            auth_rejected,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )

    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    uncertain = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_error=GitHubOutcomeUncertainError("transport failed"),
    )
    with pytest.raises(ExternalOperationUncertainError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            uncertain,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(pool=FakePool(conn))

    with pytest.raises(InvalidCommandError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )
    with pytest.raises(InvalidCommandError):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch="   ",
        )
    assert conn.executed == []


def test_telemetry_uses_only_the_sanctioned_attribute_vocabulary() -> None:
    conn = ScriptedConnection()
    _verify_scripts_unbound(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        branch_observation=_branch_observation(),
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        canonical_branch.verify_canonical_task_branch(
            _pool(conn),
            github,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN,
            claimed_branch=_CLAIMED_BRANCH,
        )

    spans = exporter.get_finished_spans()
    assert "canonical_branch.verify_canonical_task_branch" in [span.name for span in spans]
    for span in spans:
        assert set(span.attributes or {}) <= _SANCTIONED_ATTRIBUTE_NAMES
    service_span = next(
        span for span in spans if span.name == "canonical_branch.verify_canonical_task_branch"
    )
    attributes = service_span.attributes or {}
    assert attributes[OPERATION] == "canonical_branch.verify_canonical_task_branch"
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)
