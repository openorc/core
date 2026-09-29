"""Deterministic tests for the commit checks/status projection service
(issue #63).

Canned rows and a scripted SQL-connection seam prove the read-only
projection boundary: the projection addresses the **exact durable reconciled
head SHA** (never a caller-supplied branch or head), presents GitHub-owned
check-run and combined-status facts without persisting anything, treats a
missing canonical record as the uniform not-found with no GitHub call, and
classifies provider conditions exactly like the reconciliation boundary. No
live GitHub or Postgres access.
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
    GitHubCheckRunObservation,
    GitHubCommitStatusesProjection,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubStatusContextObservation,
)
from openorc.observability import (
    GITHUB_HEAD_SHA,
    GITHUB_PULL_REQUEST_NUMBER,
    OPERATION,
    TASK_ID,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import commit_checks_projection
from openorc.services.errors import (
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_INSTALLATION_RECORD = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77
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
        raise AssertionError("checks projection tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


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
        uuid.uuid4(),
        None,
        None,
        "a" * 64,
        _OBSERVED,
        _OBSERVED,
    )


def _pull_request_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        _GITHUB_PR_ID,
        _PR_NUMBER,
        "openorc/task-42-implementation",
        "main",
        _HEAD_SHA,
        "open",
        None,
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


def _projection_scripts(conn: ScriptedConnection) -> None:
    """The four read-only statements: task, canonical PR, route, installation."""
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", _pull_request_row())
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())


def _repository_observation() -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self,
        *,
        pool: FakePool,
        repository_observation: GitHubRepositoryObservation | None = None,
        check_runs: list[GitHubCheckRunObservation] | None = None,
        combined_status: GitHubCommitStatusesProjection | None = None,
        error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._check_runs = check_runs if check_runs is not None else []
        self._combined_status = combined_status
        self._error = error
        self.repository_calls: list[dict[str, int]] = []
        self.check_calls: list[dict[str, Any]] = []
        self.status_calls: list[dict[str, Any]] = []

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

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not create pull requests")

    def get_commit_check_runs(
        self, *, github_installation_id: int, owner_login: str, repository_name: str, head_sha: str
    ) -> list[GitHubCheckRunObservation]:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.check_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "head_sha": head_sha,
            }
        )
        return list(self._check_runs)

    def get_commit_combined_status(
        self, *, github_installation_id: int, owner_login: str, repository_name: str, head_sha: str
    ) -> GitHubCommitStatusesProjection:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.status_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "head_sha": head_sha,
            }
        )
        assert self._combined_status is not None
        return self._combined_status

    def get_repository_branch(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe branches")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe pull requests")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not request merges")

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not compose user-write validation")

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


def test_the_projection_addresses_the_exact_reconciled_head() -> None:
    conn = ScriptedConnection()
    _projection_scripts(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        combined_status=GitHubCommitStatusesProjection(state="success", statuses=()),
    )

    result = commit_checks_projection.project_commit_checks(
        _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result.head_sha == _HEAD_SHA
    assert result.pull_request.github_pr_id == _GITHUB_PR_ID
    # Both CI/status surfaces are read for the exact durable head SHA.
    assert github.check_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "owner_login": "octocat",
            "repository_name": "hello-world",
            "head_sha": _HEAD_SHA,
        }
    ]
    assert github.status_calls == github.check_calls
    # Nothing durable was written: exactly the four read statements ran.
    assert len(conn.executed) == 4
    assert not any(sql.startswith(("update", "insert")) for sql, _ in conn.executed)


def test_both_present_pending_and_failing_observations_are_presented() -> None:
    conn = ScriptedConnection()
    _projection_scripts(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        check_runs=[
            GitHubCheckRunObservation(name="ci_unit", status="completed", conclusion="failure"),
            GitHubCheckRunObservation(name="ci_lint", status="in_progress", conclusion=None),
        ],
        combined_status=GitHubCommitStatusesProjection(
            state="pending",
            statuses=(GitHubStatusContextObservation(context="ci/legacy", state="pending"),),
        ),
    )

    result = commit_checks_projection.project_commit_checks(
        _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    # GitHub-owned facts, presented only — no OpenOrc pass/fail synthesis.
    assert [(run.name, run.status, run.conclusion) for run in result.check_runs] == [
        ("ci_unit", "completed", "failure"),
        ("ci_lint", "in_progress", None),
    ]
    assert result.combined_status_state == "pending"
    assert [(c.context, c.state) for c in result.status_contexts] == [("ci/legacy", "pending")]


def test_checks_only_and_statuses_only_surfaces_are_both_projected() -> None:
    conn = ScriptedConnection()
    _projection_scripts(conn)
    checks_only = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        check_runs=[GitHubCheckRunObservation(name="ci", status="queued", conclusion=None)],
        combined_status=GitHubCommitStatusesProjection(state="pending", statuses=()),
    )
    result = commit_checks_projection.project_commit_checks(
        _pool(conn), checks_only, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert len(result.check_runs) == 1
    assert result.status_contexts == ()

    conn = ScriptedConnection()
    _projection_scripts(conn)
    statuses_only = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        combined_status=GitHubCommitStatusesProjection(
            state="failure",
            statuses=(GitHubStatusContextObservation(context="ci/legacy", state="failure"),),
        ),
    )
    result = commit_checks_projection.project_commit_checks(
        _pool(conn), statuses_only, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )
    assert result.check_runs == ()
    assert result.combined_status_state == "failure"


def test_a_missing_canonical_record_is_the_uniform_not_found_with_no_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        combined_status=GitHubCommitStatusesProjection(state="pending", statuses=()),
    )

    with pytest.raises(NotFoundError):
        commit_checks_projection.project_commit_checks(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    assert github.repository_calls == []
    assert github.check_calls == []
    assert github.status_calls == []


def test_provider_conditions_classify_like_the_reconciliation_boundary() -> None:
    conn = ScriptedConnection()
    _projection_scripts(conn)
    access_lost = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubAuthorizationRejectedError("denied (status 404)"),
    )
    with pytest.raises(AuthorizationError):
        commit_checks_projection.project_commit_checks(
            _pool(conn), access_lost, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    conn = ScriptedConnection()
    _projection_scripts(conn)
    rate_limited = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubRateLimitedError("rate limited (status 403)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        commit_checks_projection.project_commit_checks(
            _pool(conn), rate_limited, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    conn = ScriptedConnection()
    _projection_scripts(conn)
    uncertain = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubOutcomeUncertainError("transport failed"),
    )
    with pytest.raises(ExternalOperationUncertainError):
        commit_checks_projection.project_commit_checks(
            _pool(conn), uncertain, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    conn = ScriptedConnection()
    _projection_scripts(conn)
    known_failure = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubAuthenticationRejectedError("rejected (status 401)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        commit_checks_projection.project_commit_checks(
            _pool(conn), known_failure, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(pool=FakePool(conn))

    with pytest.raises(InvalidCommandError):
        commit_checks_projection.project_commit_checks(
            _pool(conn),
            github,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK_ID,
        )
    assert conn.executed == []


def test_telemetry_uses_only_the_sanctioned_attribute_vocabulary() -> None:
    conn = ScriptedConnection()
    _projection_scripts(conn)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        combined_status=GitHubCommitStatusesProjection(state="success", statuses=()),
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        commit_checks_projection.project_commit_checks(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    spans = exporter.get_finished_spans()
    assert "commit_checks_projection.project_commit_checks" in [span.name for span in spans]
    for span in spans:
        assert set(span.attributes or {}) <= _SANCTIONED_ATTRIBUTE_NAMES
    service_span = next(
        span for span in spans if span.name == "commit_checks_projection.project_commit_checks"
    )
    attributes = service_span.attributes or {}
    assert attributes[OPERATION] == "commit_checks_projection.project_commit_checks"
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)
    assert attributes[GITHUB_PULL_REQUEST_NUMBER] == _PR_NUMBER
    assert attributes[GITHUB_HEAD_SHA] == _HEAD_SHA
