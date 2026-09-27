"""Deterministic tests for the TaskPullRequest reconciliation service (issue #63).

Canned rows and a scripted SQL-connection seam prove the trusted
reconciliation boundary: command classification before any state is touched,
the missing canonical record as the uniform not-found (no create/adoption
path), stable-PR-identity binding (a different stable identity under the
addressed number is a conflict, never a rebind), the no-database-transaction
rule across the authoritative GitHub reads, the load-bearing write-phase
order (derived account-deletion barrier, locked route revalidation, then the
strictly update-only serialized observed-snapshot write), the serialized
head/base/state before→current facts, and the sanctioned telemetry
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
    GitHubOutcomeUncertainError,
    GitHubPullRequestObservation,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
)
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.observability import (
    GITHUB_PULL_REQUEST_NUMBER,
    OPERATION,
    TASK_ID,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import task_pull_request_reconciliation
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)

_OBSERVED = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
_MERGED_AT = datetime(2026, 9, 27, 14, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_INSTALLATION_RECORD = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_REMEDIATED_SHA = "fedcba9876543210fedcba9876543210fedcba98"

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
        raise AssertionError("pull request reconciliation tests never close pools")


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


def _pull_request_row(
    *,
    head_sha: str = _HEAD_SHA,
    head_ref: str = "openorc/task-42-implementation",
    base_ref: str = "main",
    state: str = "open",
    merged_at: Any = None,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        _GITHUB_PR_ID,
        _PR_NUMBER,
        head_ref,
        base_ref,
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


def _workspace_row() -> tuple[Any, ...]:
    return (_WORKSPACE_ID, uuid.uuid4(), "platform", _OBSERVED, _OBSERVED, 5, "")


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
    *,
    github_pr_id: int = _GITHUB_PR_ID,
    pull_number: int = _PR_NUMBER,
    head_ref: str = "openorc/task-42-implementation",
    head_sha: str = _HEAD_SHA,
    base_ref: str = "main",
    state: str = "open",
    merged: bool = False,
    merged_at: datetime | None = None,
) -> GitHubPullRequestObservation:
    return GitHubPullRequestObservation(
        github_pr_id=github_pr_id,
        pull_number=pull_number,
        head_ref=head_ref,
        head_sha=head_sha,
        base_ref=base_ref,
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
        pull_request_observation: GitHubPullRequestObservation | None = None,
        error: Exception | None = None,
        pull_request_error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._pull_request_observation = pull_request_observation
        self._error = error
        self._pull_request_error = pull_request_error
        self.repository_calls: list[dict[str, int]] = []
        self.pull_request_calls: list[dict[str, Any]] = []

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
        if self._pull_request_error is not None:
            raise self._pull_request_error
        assert self._pull_request_observation is not None
        return self._pull_request_observation

    def get_repository_branch(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe branches")

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not create pull requests")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project combined status")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not request merges")

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


def _reconcile_scripts(
    conn: ScriptedConnection,
    *,
    locked_row: tuple[Any, ...],
    updated_row: tuple[Any, ...] | None,
    write_phase_route: Any = _INSTALLATION_RECORD,
) -> None:
    """Script one full reconcile: reads, then the load-bearing write phase.

    The handler order mirrors the service's exact execute order: Task read,
    canonical-PR read, route resolution (repository, installation), then the
    write phase (Workspace read, Profile barrier read, Repository FOR UPDATE
    route revalidation, TaskPullRequest FOR UPDATE locked pre-image, and —
    only for a changed observation — the observed-snapshot update).
    """
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", locked_row)
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.profiles", (None, None, None))
    conn.on("from openorc.repositories where id", _repository_row()[:11] + (write_phase_route,))
    conn.on("for update", locked_row)
    if updated_row is not None:
        conn.on("update openorc.task_pull_requests", updated_row)


def test_a_head_change_reconciliation_reports_the_new_review_subject() -> None:
    locked = _pull_request_row()
    updated = _pull_request_row(head_sha=_REMEDIATED_SHA, head_ref="openorc/task-42-remediated")
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=updated)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(
            head_sha=_REMEDIATED_SHA, head_ref="openorc/task-42-remediated"
        ),
    )

    result = task_pull_request_reconciliation.reconcile_task_pull_request(
        _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result.updated is True
    # A changed PR head changes the review subject; the base was untouched.
    assert result.head_changed is True
    assert result.base_changed is False
    assert result.state_changed is False
    assert result.previous_head_sha == _HEAD_SHA
    assert result.pull_request.head_sha == _REMEDIATED_SHA
    assert result.pull_request.github_pr_id == _GITHUB_PR_ID
    # The PR read is addressed through the freshly observed address.
    assert github.pull_request_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "owner_login": "octocat",
            "repository_name": "hello-world",
            "pull_number": _PR_NUMBER,
        }
    ]
    assert len(conn.executed) == 9


def test_a_base_only_change_with_the_same_head_does_not_change_the_review_subject() -> None:
    locked = _pull_request_row()
    updated = _pull_request_row(base_ref="release")
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=updated)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(base_ref="release"),
    )

    result = task_pull_request_reconciliation.reconcile_task_pull_request(
        _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    # Base movement alone is never an OpenOrc stale-review condition.
    assert result.head_changed is False
    assert result.base_changed is True
    assert result.state_changed is False
    assert result.pull_request.base_ref == "release"
    assert result.pull_request.head_sha == _HEAD_SHA


def test_an_unchanged_observation_is_a_true_durable_no_op() -> None:
    locked = _pull_request_row()
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
    )

    result = task_pull_request_reconciliation.reconcile_task_pull_request(
        _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result.updated is False
    assert result.head_changed is False
    assert result.base_changed is False
    assert result.state_changed is False
    # No update statement ran at all.
    assert len(conn.executed) == 8
    assert not any(sql.startswith("update") for sql, _ in conn.executed)


def test_a_merged_observation_is_the_closed_lifecycle_fact() -> None:
    locked = _pull_request_row()
    updated = _pull_request_row(state="closed", merged_at=_MERGED_AT)
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=updated)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(state="closed", merged=True, merged_at=_MERGED_AT),
    )

    result = task_pull_request_reconciliation.reconcile_task_pull_request(
        _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result.state_changed is True
    assert result.pull_request.state is TaskPullRequestState.CLOSED
    assert result.pull_request.merged_at == _MERGED_AT


def test_a_different_stable_identity_is_a_conflict_never_a_rebind() -> None:
    locked = _pull_request_row()
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(github_pr_id=42),
    )

    with pytest.raises(ConflictError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    # The conflict classifies before the write phase: only the four reads ran.
    assert len(conn.executed) == 4


def test_a_missing_canonical_record_is_the_uniform_not_found_with_no_github_call() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
    )

    with pytest.raises(NotFoundError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    # Reconciliation never creates or adopts: no GitHub call was made.
    assert github.repository_calls == []
    assert github.pull_request_calls == []


def test_a_route_that_moved_during_the_reads_is_stale_and_applies_nothing() -> None:
    locked = _pull_request_row()
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=None, write_phase_route=None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
    )

    with pytest.raises(StaleOperationError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    # The observation authorized through installation A was never committed
    # after the route moved: no observed-snapshot write ran.
    assert not any(sql.startswith("update") for sql, _ in conn.executed)


def test_a_missing_row_at_the_write_phase_is_the_uniform_not_found() -> None:
    locked = _pull_request_row()
    conn = ScriptedConnection()
    conn.on("from openorc.tasks", _task_row())
    conn.on("from openorc.task_pull_requests where task_id", locked)
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("from openorc.github_installations", _installation_row())
    conn.on("from openorc.workspaces", _workspace_row())
    conn.on("from openorc.profiles", (None, None, None))
    conn.on("from openorc.repositories where id", _repository_row())
    conn.on("for update", None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
    )

    with pytest.raises(NotFoundError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    assert len(conn.executed) == 8


def test_provider_conditions_classify_exactly_like_the_reconciliation_boundary() -> None:
    locked = _pull_request_row()

    def _reads(conn: ScriptedConnection) -> None:
        conn.on("from openorc.tasks", _task_row())
        conn.on("from openorc.task_pull_requests where task_id", locked)
        conn.on("from openorc.repositories where id", _repository_row())
        conn.on("from openorc.github_installations", _installation_row())

    conn = ScriptedConnection()
    _reads(conn)
    access_lost = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubAuthorizationRejectedError("denied (status 404)"),
    )
    with pytest.raises(AuthorizationError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), access_lost, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    conn = ScriptedConnection()
    _reads(conn)
    rate_limited = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        error=GitHubRateLimitedError("rate limited (status 403)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), rate_limited, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    conn = ScriptedConnection()
    _reads(conn)
    uncertain = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_error=GitHubOutcomeUncertainError("transport failed"),
    )
    with pytest.raises(ExternalOperationUncertainError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), uncertain, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    conn = ScriptedConnection()
    _reads(conn)
    known_failure = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_error=GitHubAuthenticationRejectedError("rejected (status 401)"),
    )
    with pytest.raises(ExternalOperationFailedError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), known_failure, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(pool=FakePool(conn))

    with pytest.raises(InvalidCommandError):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn),
            github,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK_ID,
        )
    assert conn.executed == []


def test_telemetry_uses_only_the_sanctioned_attribute_vocabulary() -> None:
    locked = _pull_request_row()
    conn = ScriptedConnection()
    _reconcile_scripts(conn, locked_row=locked, updated_row=None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        pull_request_observation=_pr_observation(),
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        task_pull_request_reconciliation.reconcile_task_pull_request(
            _pool(conn), github, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
        )

    spans = exporter.get_finished_spans()
    assert "task_pull_request_reconciliation.reconcile_task_pull_request" in [
        span.name for span in spans
    ]
    for span in spans:
        assert set(span.attributes or {}) <= _SANCTIONED_ATTRIBUTE_NAMES
    service_span = next(
        span
        for span in spans
        if span.name == "task_pull_request_reconciliation.reconcile_task_pull_request"
    )
    attributes = service_span.attributes or {}
    assert attributes[OPERATION] == ("task_pull_request_reconciliation.reconcile_task_pull_request")
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)
    assert attributes[GITHUB_PULL_REQUEST_NUMBER] == _PR_NUMBER
