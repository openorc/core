"""Deterministic tests for the GitHub reconciliation services (issue #59).

Canned rows and a scripted SQL-connection seam prove the trusted, system-
capable reconciliation boundary:

- command classification before any state is touched;
- the no-database-transaction rule across the authoritative GitHub reads;
- stable Repository/Issue identity: rename/transfer metadata refresh never
  rebinds identity, and issue-number reuse against a different stable issue
  is a classified conflict with no rebind;
- the serialized exactly-once creation/change facts and the true durable
  no-op;
- the classified outcome translation and the sanctioned telemetry vocabulary.

No live GitHub or Postgres access.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from psycopg.errors import UniqueViolation

from openorc.adapters.github import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubIssueObservation,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.github_issues import github_issue_requirements_fingerprint
from openorc.observability import (
    GITHUB_ISSUE_NUMBER,
    OPERATION,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import github_reconciliation
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)
_INSTALLATION_RECORD = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
# Real digests of the default observation and of a changed title/body: the
# service computes the fingerprint from the observation, so the scripted
# durable rows must carry the matching canonical values.
_FINGERPRINT_A = github_issue_requirements_fingerprint("Found a bug", "Requirements body")
_FINGERPRINT_B = github_issue_requirements_fingerprint("Found a bug v2", "Requirements body")

# The sanctioned safe attribute vocabulary (issue #108), derived from the
# boundary module itself: the service spans may carry only these names.
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
del GITHUB_ISSUE_NUMBER


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
        raise AssertionError("github reconciliation service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self,
        *,
        pool: FakePool,
        repository_observation: GitHubRepositoryObservation | None = None,
        issue_observation: GitHubIssueObservation | None = None,
        error: Exception | None = None,
        issue_error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._issue_observation = issue_observation
        self._error = error
        self._issue_error = issue_error
        self.repository_calls: list[dict[str, int]] = []
        self.issue_calls: list[dict[str, Any]] = []

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

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.issue_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "issue_number": issue_number,
            }
        )
        if self._issue_error is not None:
            raise self._issue_error
        assert self._issue_observation is not None
        return self._issue_observation

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the service must not compose raw #58 validation operations")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")

    def get_issue_blocked_by(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("this fake must not observe relationships")

    def get_issue_sub_issues(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("this fake must not observe relationships")

    def get_issue_parent(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("this fake must not observe relationships")

    def get_repository_by_address(
        self, *, github_installation_id: int, owner_login: str, repository_name: str
    ) -> int:
        raise AssertionError("this fake must not resolve addresses")


def _ws_row(workspace_id: Any, profile_id: Any) -> tuple[Any, ...]:
    return (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")


def _installation_row(installation_id: Any, workspace_id: Any) -> tuple[Any, ...]:
    return (
        installation_id,
        workspace_id,
        _EXTERNAL_INSTALLATION_ID,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_row(
    repository_id: Any,
    project_id: Any,
    workspace_id: Any,
    route: Any,
    *,
    owner_login: str = "octocat",
) -> tuple[Any, ...]:
    return (
        repository_id,
        project_id,
        workspace_id,
        _GITHUB_REPOSITORY_ID,
        owner_login,
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        route,
    )


def _issue_row(
    *,
    workspace_id: Any | None = None,
    fingerprint: str = _FINGERPRINT_A,
    state: str = "open",
    title: str = "Found a bug",
    body: str | None = "Requirements body",
    issue_number: int = _ISSUE_NUMBER,
    github_issue_id: int = _GITHUB_ISSUE_ID,
    provider_updated_at: datetime | None = _OBSERVED,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id or uuid.uuid4(),
        uuid.uuid4(),
        github_issue_id,
        issue_number,
        title,
        body,
        state,
        fingerprint,
        provider_updated_at,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_observation(
    *, owner: str = "octocat", name: str = "hello-world"
) -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login=owner,
        name=name,
        html_url=f"https://github.com/{owner}/{name}",
        is_private=False,
        default_branch="main",
    )


def _issue_observation(
    *,
    github_issue_id: int = _GITHUB_ISSUE_ID,
    title: str = "Found a bug",
    body: str | None = "Requirements body",
    state: str = "open",
    is_pull_request: bool = False,
    provider_updated_at: datetime = _OBSERVED,
) -> GitHubIssueObservation:
    return GitHubIssueObservation(
        github_issue_id=github_issue_id,
        issue_number=_ISSUE_NUMBER,
        title=title,
        body=body,
        state=state,
        provider_updated_at=provider_updated_at,
        is_pull_request=is_pull_request,
    )


def _route_resolution_scripts(conn: ScriptedConnection, route: Any, workspace_id: Any) -> None:
    """Script the two route-resolution reads (repository, then installation)."""
    conn.on(
        "from openorc.repositories where id",
        _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route),
    )
    if route is not None:
        conn.on("from openorc.github_installations", _installation_row(route, workspace_id))


def _write_phase_scripts(
    conn: ScriptedConnection,
    workspace_id: Any,
    repository_row: tuple[Any, ...] | None,
    *,
    barrier_row: tuple[Any, ...] | None = (None, None, None),
) -> None:
    """Script the composed write phase in its load-bearing order.

    The order is: Workspace read (derived-barrier ownership), the Profile
    ``FOR KEY SHARE`` account-deletion barrier read, then the Repository
    ``FOR UPDATE`` currentness/route/metadata read.
    """
    conn.on("from openorc.workspaces", _ws_row(workspace_id, uuid.uuid4()))
    if barrier_row is not None:
        conn.on("from openorc.profiles", barrier_row)
    conn.on("for update", repository_row)


def _reconcile_scripts_inserted(conn: ScriptedConnection, workspace_id: Any) -> None:
    """Script a first-projection reconcile: no row, then the insert."""
    conn.on("from openorc.github_issues", None)
    conn.on("insert into openorc.github_issues", _issue_row(workspace_id=workspace_id))


def _reconcile_scripts_unchanged(conn: ScriptedConnection, workspace_id: Any) -> None:
    """Script an identical second reconcile: locked row, no write."""
    conn.on("from openorc.github_issues", _issue_row(workspace_id=workspace_id))


def _happy_scripts(conn: ScriptedConnection, workspace_id: Any, route: Any) -> None:
    """Script one complete unchanged reconciliation."""
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
    )
    _reconcile_scripts_unchanged(conn, workspace_id)


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()

    with pytest.raises(InvalidCommandError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            repository_id=repository_id,
            issue_number=_ISSUE_NUMBER,
        )
    with pytest.raises(InvalidCommandError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=True,
        )
    with pytest.raises(InvalidCommandError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=0,
        )
    with pytest.raises(InvalidCommandError):
        github_reconciliation.reconcile_repository_issue_for_owner(
            _pool(conn),
            github,
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=_ISSUE_NUMBER,
        )

    assert conn.executed == []
    assert github.repository_calls == []
    assert github.issue_calls == []


def test_the_system_primitive_resolves_the_route_without_an_authenticated_actor() -> None:
    # The trusted primitive's signature carries durably resolved identity
    # only — no Profile parameter exists to fabricate.
    signature = inspect.signature(github_reconciliation.reconcile_repository_issue)
    assert set(signature.parameters) == {
        "pool",
        "github",
        "workspace_id",
        "repository_id",
        "issue_number",
    }


@pytest.mark.parametrize(
    ("route", "description"),
    [
        (None, "unconfigured route"),
        (_INSTALLATION_RECORD, "absent installation record"),
    ],
)
def test_unusable_routes_fail_closed_as_the_uniform_not_found(route: Any, description: str) -> None:
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection()
    if route is None:
        conn.on(
            "from openorc.repositories where id",
            _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, None),
        )
    else:
        # The repository resolves with the route, but the installation
        # record is gone/inconsistent: the route read returns no row.
        conn.on(
            "from openorc.repositories where id",
            _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route),
        )
        conn.on("from openorc.github_installations", None)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    with pytest.raises(NotFoundError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    assert github.repository_calls == []


def test_a_foreign_workspace_repository_is_the_uniform_not_found() -> None:
    foreign_workspace = uuid.uuid4()
    requested_workspace = uuid.uuid4()
    conn = ScriptedConnection()
    conn.on(
        "from openorc.repositories where id",
        _repository_row(uuid.uuid4(), uuid.uuid4(), foreign_workspace, _INSTALLATION_RECORD),
    )
    github = FakeGitHubAppClient(pool=FakePool(conn))

    with pytest.raises(NotFoundError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=requested_workspace,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    assert github.repository_calls == []


def test_a_first_projection_creates_the_issue_and_reports_creation() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
    )
    _reconcile_scripts_inserted(conn, workspace_id)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    result = github_reconciliation.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=uuid.uuid4(),
        issue_number=_ISSUE_NUMBER,
    )

    assert result.issue_created is True
    assert result.requirements_changed is False
    assert result.issue_state_changed is False
    assert result.previous_fingerprint is None
    assert result.repository_metadata_changed is False
    assert result.issue.identity.github_issue_id == _GITHUB_ISSUE_ID
    assert result.issue.workspace_id == workspace_id
    # The adapter was addressed by the stable repository identity and the
    # routed external installation, and the issue read by the fresh owner/name.
    assert github.repository_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "github_repository_id": _GITHUB_REPOSITORY_ID,
        }
    ]
    assert github.issue_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "owner_login": "octocat",
            "repository_name": "hello-world",
            "issue_number": _ISSUE_NUMBER,
        }
    ]


def test_requirements_changes_are_detected_title_only_body_only_and_both() -> None:
    cases: list[tuple[dict[str, Any], str]] = [
        ({"title": "Renamed requirements"}, _FINGERPRINT_B),
        ({"body": "Changed body"}, _FINGERPRINT_B),
        ({"title": "New title", "body": "New body"}, _FINGERPRINT_B),
    ]
    for kwargs, fingerprint in cases:
        workspace_id = uuid.uuid4()
        route = _INSTALLATION_RECORD
        conn = ScriptedConnection()
        _route_resolution_scripts(conn, route, workspace_id)
        _write_phase_scripts(
            conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
        )
        # The locked projection row holds the pre-image fingerprint; the
        # update fires and returns the post-write row.
        conn.on("from openorc.github_issues", _issue_row(workspace_id=workspace_id))
        conn.on(
            "update openorc.github_issues",
            _issue_row(workspace_id=workspace_id, fingerprint=fingerprint),
        )
        github = FakeGitHubAppClient(
            pool=FakePool(conn),
            repository_observation=_repository_observation(),
            issue_observation=_issue_observation(**kwargs),
        )

        result = github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

        assert result.requirements_changed is True
        assert result.issue_created is False
        # A requirements-only durable write is not an open/closed state change.
        assert result.issue_state_changed is False
        assert result.previous_fingerprint == _FINGERPRINT_A


def test_a_state_only_change_updates_the_projection_but_not_the_fingerprint() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
    )
    conn.on("from openorc.github_issues", _issue_row(workspace_id=workspace_id))
    conn.on(
        "update openorc.github_issues",
        _issue_row(workspace_id=workspace_id, state="closed", fingerprint=_FINGERPRINT_A),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(state="closed"),
    )

    result = github_reconciliation.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=uuid.uuid4(),
        issue_number=_ISSUE_NUMBER,
    )

    assert result.issue_state_changed is True
    assert result.requirements_changed is False
    assert result.previous_fingerprint == _FINGERPRINT_A


def test_a_requirements_and_state_change_reports_both_semantic_flags() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
    )
    conn.on("from openorc.github_issues", _issue_row(workspace_id=workspace_id))
    conn.on(
        "update openorc.github_issues",
        _issue_row(
            workspace_id=workspace_id,
            title="Found a bug v2",
            state="closed",
            fingerprint=_FINGERPRINT_B,
        ),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(title="Found a bug v2", state="closed"),
    )

    result = github_reconciliation.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=uuid.uuid4(),
        issue_number=_ISSUE_NUMBER,
    )

    assert result.issue_created is False
    assert result.requirements_changed is True
    assert result.issue_state_changed is True
    assert result.previous_fingerprint == _FINGERPRINT_A


def test_a_provider_timestamp_only_change_is_not_a_semantic_change() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
    )
    conn.on("from openorc.github_issues", _issue_row(workspace_id=workspace_id))
    conn.on(
        "update openorc.github_issues",
        _issue_row(
            workspace_id=workspace_id,
            provider_updated_at=_OBSERVED + timedelta(hours=1),
        ),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(provider_updated_at=_OBSERVED + timedelta(hours=1)),
    )

    result = github_reconciliation.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=uuid.uuid4(),
        issue_number=_ISSUE_NUMBER,
    )

    # The projection row is durably advanced, but only provider metadata
    # changed: neither semantic flag moves.
    assert result.issue_created is False
    assert result.requirements_changed is False
    assert result.issue_state_changed is False
    assert result.previous_fingerprint == _FINGERPRINT_A
    assert result.issue.provider_updated_at == _OBSERVED + timedelta(hours=1)


def test_an_unchanged_authoritative_state_is_a_durable_no_op() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _happy_scripts(conn, workspace_id, route)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    result = github_reconciliation.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=uuid.uuid4(),
        issue_number=_ISSUE_NUMBER,
    )

    assert result.issue_created is False
    assert result.requirements_changed is False
    assert result.issue_state_changed is False
    assert result.repository_metadata_changed is False
    assert result.previous_fingerprint == _FINGERPRINT_A
    # No durable write at all: only reads are executed.
    assert not any(
        sql.lstrip().lower().startswith(("update", "insert", "delete")) for sql, _ in conn.executed
    )


def test_a_rename_or_transfer_refreshes_metadata_without_rebinding_identity() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    repository_id = uuid.uuid4()
    conn = ScriptedConnection()
    conn.on(
        "from openorc.repositories where id",
        _repository_row(repository_id, uuid.uuid4(), workspace_id, route),
    )
    conn.on("from openorc.github_installations", _installation_row(route, workspace_id))
    _write_phase_scripts(
        conn, workspace_id, _repository_row(repository_id, uuid.uuid4(), workspace_id, route)
    )
    conn.on(
        "update openorc.repositories",
        _repository_row(
            repository_id, uuid.uuid4(), workspace_id, route, owner_login="renamed-org"
        ),
    )
    _reconcile_scripts_unchanged(conn, workspace_id)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(owner="renamed-org"),
        issue_observation=_issue_observation(),
    )

    result = github_reconciliation.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=repository_id,
        issue_number=_ISSUE_NUMBER,
    )

    assert result.repository_metadata_changed is True
    # The stable external identity is preserved on the returned record; the
    # mutable presentation metadata is refreshed.
    assert result.repository.identity.github_repository_id == _GITHUB_REPOSITORY_ID
    assert result.repository.metadata.owner_login == "renamed-org"
    update_sql, _ = next(
        (sql, p) for sql, p in conn.executed if sql.lstrip().lower().startswith("update")
    )
    assert "update openorc.repositories" in update_sql
    assert "set" in update_sql


def test_a_route_rebound_between_read_and_write_is_stale_and_applies_nothing() -> None:
    workspace_id = uuid.uuid4()
    other_installation = uuid.uuid4()
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, _INSTALLATION_RECORD, workspace_id)
    _write_phase_scripts(
        conn,
        workspace_id,
        # The reloaded row shows the route rebound to installation B.
        _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, other_installation),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    with pytest.raises(StaleOperationError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    # Nothing was applied after the route revalidation read.
    assert not any(
        sql.lstrip().lower().startswith(("update", "insert")) for sql, _ in conn.executed
    )


def test_a_route_unbound_between_read_and_write_is_stale_and_applies_nothing() -> None:
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, _INSTALLATION_RECORD, workspace_id)
    _write_phase_scripts(
        conn,
        workspace_id,
        # The reloaded row shows the route cleared.
        _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, None),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    with pytest.raises(StaleOperationError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    assert not any(
        sql.lstrip().lower().startswith(("update", "insert")) for sql, _ in conn.executed
    )


def test_an_active_account_deletion_fails_closed_before_any_write() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn,
        workspace_id,
        _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route),
        barrier_row=("active", uuid.uuid4(), _OBSERVED),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    with pytest.raises(ConflictError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    # The barrier is the FIRST lock acquisition: no projection/repo write
    # ran after it.
    assert not any(
        sql.lstrip().lower().startswith(("update", "insert")) for sql, _ in conn.executed
    )


def test_issue_number_reuse_against_another_identity_is_a_conflict_without_rebind() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route)
    )
    # The serialized reconcile: no row for our stable identity, the insert
    # races and loses, and the re-read proves the number maps to a different
    # stable issue (deliberately independent of the reported constraint).
    conn.on("from openorc.github_issues", None)
    conn.on("insert into openorc.github_issues", UniqueViolation("duplicate key"))
    conn.on("from openorc.github_issues", None)
    conn.on(
        "from openorc.github_issues",
        _issue_row(workspace_id=workspace_id, github_issue_id=999),
    )
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    with pytest.raises(ConflictError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    # Identity is never rebound: exactly one insert attempt, no update, and
    # no second insert after the classification.
    inserts = [sql for sql, _ in conn.executed if "insert into" in sql.lower()]
    assert len(inserts) == 1
    assert not any(sql.lstrip().lower().startswith("update") for sql, _ in conn.executed)


def test_a_pull_request_number_is_a_conflict_not_an_issue_projection() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, route, workspace_id)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(is_pull_request=True),
    )

    with pytest.raises(ConflictError):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    # The write phase never starts for a PR-shaped number.
    assert not any("from openorc.workspaces" in sql for sql, _ in conn.executed)


@pytest.mark.parametrize(
    ("adapter_error", "expected_error"),
    [
        (GitHubAuthorizationRejectedError("denied"), AuthorizationError),
        (GitHubRateLimitedError("throttled"), ExternalOperationFailedError),
        (GitHubAuthenticationRejectedError("401"), ExternalOperationFailedError),
        (GitHubRequestRejectedError("422"), ExternalOperationFailedError),
        (GitHubOutcomeUncertainError("timeout"), ExternalOperationUncertainError),
    ],
)
def test_adapter_outcomes_translate_into_the_typed_vocabulary(
    adapter_error: Exception, expected_error: type[Exception]
) -> None:
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection()
    _route_resolution_scripts(conn, _INSTALLATION_RECORD, workspace_id)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        error=adapter_error,
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    with pytest.raises(expected_error) as caught:
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    # A rate limit is a known provider condition, deliberately NOT
    # classified as lost authorization.
    if isinstance(adapter_error, GitHubRateLimitedError):
        assert not isinstance(caught.value, AuthorizationError)


def test_the_owner_wrapper_composes_ownership_and_anti_probing_not_found() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    # Owner wrapper: the #53 workspace ownership read, the #57 repository
    # scope read, then the primitive's own route resolution and write phase.
    conn.on("from openorc.workspaces", _ws_row(workspace_id, profile_id))
    conn.on(
        "from openorc.repositories where id",
        _repository_row(repository_id, uuid.uuid4(), workspace_id, route),
    )
    _route_resolution_scripts(conn, route, workspace_id)
    _write_phase_scripts(
        conn, workspace_id, _repository_row(repository_id, uuid.uuid4(), workspace_id, route)
    )
    _reconcile_scripts_unchanged(conn, workspace_id)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    result = github_reconciliation.reconcile_repository_issue_for_owner(
        _pool(conn),
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        repository_id=repository_id,
        issue_number=_ISSUE_NUMBER,
    )

    assert result.issue_created is False

    # A foreign-Workspace probe is the uniform not-found, before anything
    # else is addressed.
    foreign_conn = ScriptedConnection()
    foreign_conn.on("from openorc.workspaces", _ws_row(workspace_id, uuid.uuid4()))
    with pytest.raises(NotFoundError):
        github_reconciliation.reconcile_repository_issue_for_owner(
            _pool(foreign_conn),
            FakeGitHubAppClient(pool=FakePool(foreign_conn)),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=_ISSUE_NUMBER,
        )
    assert len(foreign_conn.executed) == 1


def test_telemetry_uses_only_the_sanctioned_attribute_vocabulary() -> None:
    workspace_id = uuid.uuid4()
    route = _INSTALLATION_RECORD
    conn = ScriptedConnection()
    _happy_scripts(conn, workspace_id, route)
    github = FakeGitHubAppClient(
        pool=FakePool(conn),
        repository_observation=_repository_observation(),
        issue_observation=_issue_observation(),
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        github_reconciliation.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=uuid.uuid4(),
            issue_number=_ISSUE_NUMBER,
        )

    spans = exporter.get_finished_spans()
    span_names = [span.name for span in spans]
    assert "github_reconciliation.reconcile_repository_issue" in span_names
    for span in spans:
        assert set(span.attributes or {}) <= _SANCTIONED_ATTRIBUTE_NAMES
    service_span = next(
        span for span in spans if span.name == "github_reconciliation.reconcile_repository_issue"
    )
    attributes = service_span.attributes or {}
    assert attributes[OPERATION] == "github_reconciliation.reconcile_repository_issue"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
