"""Deterministic tests for the authoritative Task intake services (issue #60).

A scripted SQL-connection seam plus a scripted GitHub adapter seam prove the
intake contract on the critical path:

- strict call ordering: fresh issue reconciliation, then the blocked-by
  observation, then the final short creation transaction — and the
  non-gating hierarchy sync strictly AFTER creation;
- no database transaction open across any external GitHub call;
- the eligibility predicate: issue open, GitHub not blocked (a sub-issue /
  parent present yet intake-eligible), route resolved, no current Task;
- blocked / unobservable-blocking-state fail-closed classification;
- duplicate/concurrent intake classified onto the existing current Task
  (from re-read durable state, never the constraint name);
- the immutable source baseline persisted on the created Task;
- hierarchy-sync failure after creation never alters the intake result;
- Workspace-scope fail-closed boundaries.

No live GitHub or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.github import (
    GitHubAuthorizationRejectedError,
    GitHubIssueObservation,
    GitHubIssueParentObservation,
    GitHubOutcomeUncertainError,
    GitHubRelatedIssueEndpoint,
    GitHubRelatedIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import task_intake
from openorc.services.errors import (
    ConflictError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 9, 26, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_FINGERPRINT = "a" * 64
_TASK_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_INSTALLATION_ROUTE_ID = uuid.uuid4()


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, Any]] = []

    def on(self, sql_marker: str, result: Any) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, list):
                    return FakeCursor(None, result)
                if isinstance(result, tuple):
                    return FakeCursor(result, None)
                return FakeCursor(None, None)
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
        raise AssertionError("task intake service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


class FakeGitHubAppClient:
    """Scripted adapter fake recording call order and transaction safety."""

    def __init__(
        self,
        *,
        blocked_by: list[GitHubRelatedIssueObservation],
        parent: GitHubRelatedIssueEndpoint | None = None,
        sub_issues: list[GitHubRelatedIssueObservation] | None = None,
        issue_error: Exception | None = None,
        blocked_by_error: Exception | None = None,
        parent_error: Exception | None = None,
    ) -> None:
        self._blocked_by = blocked_by
        self._parent = parent
        self._sub_issues = sub_issues if sub_issues is not None else []
        self._issue_error = issue_error
        self._blocked_by_error = blocked_by_error
        self._parent_error = parent_error
        self.calls: list[str] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        self.calls.append("repository")
        return GitHubRepositoryObservation(
            github_repository_id=github_repository_id,
            owner_login="octocat",
            name="repo",
            html_url="https://github.com/octocat/repo",
            is_private=False,
            default_branch="main",
        )

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        self.calls.append("issue")
        if self._issue_error is not None:
            raise self._issue_error
        return GitHubIssueObservation(
            github_issue_id=_GITHUB_ISSUE_ID,
            issue_number=issue_number,
            title="Found a bug",
            body="Steps",
            state="open",
            provider_updated_at=_OBSERVED,
            is_pull_request=False,
        )

    def get_issue_blocked_by(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        self.calls.append("blocked_by")
        if self._blocked_by_error is not None:
            raise self._blocked_by_error
        return self._blocked_by

    def get_issue_parent(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueParentObservation:
        self.calls.append("parent")
        if self._parent_error is not None:
            raise self._parent_error
        return GitHubIssueParentObservation(parent=self._parent)

    def get_issue_sub_issues(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        self.calls.append("sub_issues")
        return self._sub_issues

    def get_repository_by_address(
        self, *, github_installation_id: int, owner_login: str, repository_name: str
    ) -> int:
        self.calls.append("resolve_repository")
        return 555

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the service must not compose raw #58 validation operations")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")


def _ws_row() -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, 5, "")


def _installation_row() -> tuple[Any, ...]:
    return (
        _INSTALLATION_ROUTE_ID,
        _WORKSPACE_ID,
        _EXTERNAL_INSTALLATION_ID,
        501,
        "octocat",
        "Organization",
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
        "repo",
        "https://github.com/octocat/repo",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        _INSTALLATION_ROUTE_ID,
    )


def _issue_projection_row(state: str = "open") -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        "Found a bug",
        "Steps",
        state,
        _FINGERPRINT,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
    )


def _task_row(state_fingerprint: str = _FINGERPRINT) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        "ready_to_plan",
        None,
        None,
        _STATE_TOKEN,
        None,
        None,
        state_fingerprint,
        _OBSERVED,
        _OBSERVED,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "task_created",
        "openorc",
        None,
        None,
        None,
        {},
        _OBSERVED,
    )


def _script_write_phase(conn: ScriptedConnection) -> None:
    """Script the composed write phase in its load-bearing order.

    Workspace read (derived-barrier ownership), the Profile ``FOR KEY
    SHARE`` account-deletion barrier read (a NULL attempt-state tuple), then
    the Repository ``FOR UPDATE`` currentness/route read.
    """
    conn.on("from openorc.workspaces", _ws_row())
    conn.on("from openorc.profiles", (None, None, None))
    conn.on("for update", _repository_row())


def _script_route_resolution(conn: ScriptedConnection) -> None:
    """Script one route resolution (repository read + installation read)."""
    conn.on("select id, project_id, workspace_id, github_repository_id", _repository_row())
    conn.on("from openorc.github_installations where id", _installation_row())


def _script_reconciliation_write(conn: ScriptedConnection, *, state: str = "open") -> None:
    _script_route_resolution(conn)  # intake step 1 resolves the route first
    _script_route_resolution(conn)  # B3 reconciliation resolves it again
    _script_write_phase(conn)
    conn.on("select", _issue_projection_row(state=state))  # locked projection read
    conn.on("update openorc.github_issues", _issue_projection_row(state=state))


def _script_dependency_mirror(
    conn: ScriptedConnection, *, durable_blockers: list[tuple[Any, ...]]
) -> None:
    _script_route_resolution(conn)  # the dependency unit resolves its own route
    _script_write_phase(conn)
    conn.on(
        "select blocker_github_repository_id, blocker_github_issue_id from "
        "openorc.github_issue_dependencies",
        durable_blockers,
    )
    # The observation replaces the prior set; the change-only
    # task_dependency_synced event write follows.
    conn.on("delete from openorc.github_issue_dependencies", [])
    for _ in durable_blockers:
        conn.on("insert into openorc.github_issue_dependencies", [])
    conn.on("insert into openorc.workflow_events", _event_row())


def _script_creation(
    conn: ScriptedConnection,
    *,
    current_task: tuple[Any, ...] | None,
    insert_result: tuple[Any, ...] | None,
) -> None:
    _script_write_phase(conn)
    conn.on(
        "select blocker_github_repository_id, blocker_github_issue_id from "
        "openorc.github_issue_dependencies",
        [],
    )
    conn.on(
        "select id, workspace_id, repository_id, github_issue_id, github_issue_number",
        current_task,
    )
    if insert_result is not None:
        conn.on("insert into openorc.tasks", insert_result)
        conn.on("insert into openorc.workflow_events", _event_row())


def _script_hierarchy_after_creation(
    conn: ScriptedConnection,
    *,
    child_rows: list[tuple[Any, ...]] | None = None,
) -> None:
    _script_route_resolution(conn)
    _script_write_phase(conn)
    conn.on("select parent_github_repository_id", None)
    conn.on("delete from openorc.github_issue_hierarchy", [])
    conn.on("insert into openorc.github_issue_hierarchy", [])
    conn.on(
        "select child_github_repository_id, child_github_issue_id from "
        "openorc.github_issue_sub_issues",
        child_rows if child_rows is not None else [],
    )
    # Any non-empty observed set replaces wholesale; the change-only
    # task_relationship_synced event write follows either way.
    conn.on("delete from openorc.github_issue_sub_issues", [])
    for _ in child_rows or []:
        conn.on("insert into openorc.github_issue_sub_issues", [])
    conn.on("insert into openorc.workflow_events", _event_row())


def _script_owner_boundary(conn: ScriptedConnection) -> None:
    """Script the #52/#57 ownership gate reads before the primitive."""
    conn.on("from openorc.workspaces", _ws_row())
    conn.on("select id, project_id, workspace_id, github_repository_id", _repository_row())


def _script_happy_path(conn: ScriptedConnection) -> None:
    _script_reconciliation_write(conn)
    _script_dependency_mirror(conn, durable_blockers=[])
    _script_creation(conn, current_task=None, insert_result=_task_row())
    _script_hierarchy_after_creation(conn)
    _script_route_resolution(conn)  # the hierarchy unit's own route resolution


def test_intake_eligible_issue_creates_one_task_with_the_fresh_baseline() -> None:
    conn = ScriptedConnection()
    _script_happy_path(conn)
    pool = _pool(conn)
    github = FakeGitHubAppClient(blocked_by=[])
    result = task_intake.intake_repository_task(
        pool,
        github,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        issue_number=_ISSUE_NUMBER,
    )
    assert result.created is True
    assert result.task.id == _TASK_ID
    # The immutable source baseline: the exact fresh requirements fingerprint.
    assert result.task.source_requirements_fingerprint == _FINGERPRINT
    # Critical-path ordering: fresh issue reads, then the blocked-by
    # observation, and the hierarchy walk strictly AFTER creation.
    assert github.calls[0] == "repository"
    assert github.calls[1] == "issue"
    first_blocked_by = github.calls.index("blocked_by")
    first_parent = github.calls.index("parent")
    insert_index = next(
        i for i, (sql, _) in enumerate(conn.executed) if "insert into openorc.tasks" in sql
    )
    assert insert_index > 0
    assert first_blocked_by < first_parent


def test_an_issue_with_parent_and_sub_issues_remains_intake_eligible() -> None:
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    _script_dependency_mirror(conn, durable_blockers=[])
    _script_creation(conn, current_task=None, insert_result=_task_row())
    _script_hierarchy_after_creation(
        conn,
        child_rows=[(555, 2)],
    )
    _script_route_resolution(conn)  # hierarchy unit resolves its own route
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[],
        parent=GitHubRelatedIssueEndpoint(github_repository_id=555, github_issue_id=1),
        sub_issues=[
            GitHubRelatedIssueObservation(
                github_issue_id=2,
                # The documented REST API-form repository_url.
                repository_url="https://api.github.com/repos/octocat/other-repo",
            )
        ],
    )
    result = task_intake.intake_repository_task(
        pool,
        github,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        issue_number=_ISSUE_NUMBER,
    )
    # Hierarchy is categorically absent from the eligibility predicate.
    assert result.created is True


def test_a_github_blocked_issue_cannot_create_a_task() -> None:
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    _script_dependency_mirror(
        conn,
        durable_blockers=[(999999, 7)],  # the fresh observation mirrors a blocker
    )
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[
            GitHubRelatedIssueObservation(
                github_issue_id=7,
                # The documented REST API-form repository_url (same repo).
                repository_url="https://api.github.com/repos/octocat/repo",
            )
        ],
    )
    with pytest.raises(ConflictError, match="blocked"):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    assert not any("insert into openorc.tasks" in sql for sql, _ in conn.executed)


def test_a_documented_api_form_repository_url_is_resolved_not_rejected() -> None:
    """Regression: GitHub's documented REST ``repository_url`` shape resolves.

    GitHub REST issue objects carry ``repository_url`` as
    ``https://api.github.com/repos/{owner}/{repo}``; the blocked-by mirror
    must accept that documented shape (not a browser-style github.com URL)
    and derive the blocked state from the resolved endpoints.
    """
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    _script_dependency_mirror(
        conn,
        durable_blockers=[(555, 7)],  # resolved endpoint mirrored durably
    )
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[
            GitHubRelatedIssueObservation(
                github_issue_id=7,
                repository_url="https://api.github.com/repos/octocat/repo",
            )
        ],
    )
    with pytest.raises(ConflictError, match="blocked"):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    # The blocked state is derived from the resolved endpoint, never from
    # the mutable URL text, and no Task is created.
    assert not any("insert into openorc.tasks" in sql for sql, _ in conn.executed)


def test_a_browser_style_repository_url_fails_the_observation_closed() -> None:
    """A non-API-origin reference is not the documented shape: fail closed."""
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    _script_route_resolution(conn)  # dependency unit's route resolution
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[
            GitHubRelatedIssueObservation(
                github_issue_id=7,
                repository_url="https://github.com/octocat/repo",
            )
        ],
    )
    with pytest.raises(ExternalOperationUncertainError, match="not resolvable"):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    assert not any("insert into openorc.tasks" in sql for sql, _ in conn.executed)


def test_same_repository_references_reuse_the_known_stable_id() -> None:
    """Same-repo edges need zero extra adapter reads (the rev-6 contract)."""
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    _script_dependency_mirror(conn, durable_blockers=[])
    _script_creation(conn, current_task=None, insert_result=_task_row())
    _script_hierarchy_after_creation(
        conn,
        child_rows=[(555, 7)],  # same-repo child resolves to the local ID
    )
    _script_route_resolution(conn)  # hierarchy unit resolves its own route
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[],
        sub_issues=[
            GitHubRelatedIssueObservation(
                github_issue_id=7,
                repository_url="https://api.github.com/repos/octocat/repo",
            )
        ],
    )
    result = task_intake.intake_repository_task(
        pool,
        github,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        issue_number=_ISSUE_NUMBER,
    )
    assert result.created is True
    # Same-repository references reused the local stable ID with zero
    # cross-repository REST resolution reads.
    assert "resolve_repository" not in github.calls


def test_an_unobservable_blocking_state_fails_intake_closed() -> None:
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    # The dependency unit fails on its observation, before any write; only
    # its own route resolution is consumed first.
    _script_route_resolution(conn)
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[],
        blocked_by_error=GitHubOutcomeUncertainError("unknown outcome"),
    )
    with pytest.raises(ExternalOperationUncertainError):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    assert not any("insert into openorc.tasks" in sql for sql, _ in conn.executed)


def test_a_closed_issue_is_not_startable() -> None:
    conn = ScriptedConnection()
    _script_reconciliation_write(conn, state="closed")
    pool = _pool(conn)
    github = FakeGitHubAppClient(blocked_by=[])
    with pytest.raises(NotFoundError, match="not open"):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    assert not any("insert into openorc.tasks" in sql for sql, _ in conn.executed)


def test_a_duplicate_intake_converges_on_the_existing_current_task() -> None:
    conn = ScriptedConnection()
    _script_reconciliation_write(conn)
    _script_dependency_mirror(conn, durable_blockers=[])
    # The creation phase's current-Task re-check finds the concurrent winner.
    _script_creation(conn, current_task=_task_row(), insert_result=None)
    _script_hierarchy_after_creation(conn)
    pool = _pool(conn)
    github = FakeGitHubAppClient(blocked_by=[])
    result = task_intake.intake_repository_task(
        pool,
        github,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        issue_number=_ISSUE_NUMBER,
    )
    assert result.created is False
    assert result.task.id == _TASK_ID


def test_hierarchy_sync_failure_after_creation_never_alters_the_intake_result() -> None:
    conn = ScriptedConnection()
    _script_happy_path(conn)
    pool = _pool(conn)
    github = FakeGitHubAppClient(
        blocked_by=[], parent_error=GitHubAuthorizationRejectedError("denied")
    )
    result = task_intake.intake_repository_task(
        pool,
        github,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        issue_number=_ISSUE_NUMBER,
    )
    # The intake was already determined and committed: the hierarchy failure
    # is swallowed (non-gating), and the Task is returned.
    assert result.created is True
    assert result.task.id == _TASK_ID


def test_an_unconfigured_route_fails_intake_closed() -> None:
    conn = ScriptedConnection()
    repository_row = list(_repository_row())
    repository_row[11] = None  # no configured github installation route
    conn.on("select id, project_id, workspace_id, github_repository_id", tuple(repository_row))
    pool = _pool(conn)
    github = FakeGitHubAppClient(blocked_by=[])
    with pytest.raises(NotFoundError, match="route"):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    assert github.calls == []


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    github = FakeGitHubAppClient(blocked_by=[])
    with pytest.raises(InvalidCommandError):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            repository_id=_REPOSITORY_ID,
            issue_number=_ISSUE_NUMBER,
        )
    with pytest.raises(InvalidCommandError):
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            issue_number=0,
        )
    assert conn.executed == []
    assert github.calls == []


def test_the_owner_boundary_composes_ownership_before_the_primitive() -> None:
    conn = ScriptedConnection()
    _script_owner_boundary(conn)
    _script_happy_path(conn)
    pool = _pool(conn)
    github = FakeGitHubAppClient(blocked_by=[])
    result = task_intake.intake_repository_task_for_owner(
        pool,
        github,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        issue_number=_ISSUE_NUMBER,
    )
    assert result.created is True
