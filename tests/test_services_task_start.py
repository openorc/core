"""Deterministic tests for Task start, structural capacity, and FIFO admission (issue #76).

A scripted SQL-connection seam plus a scripted GitHub adapter seam prove the
start contract on its critical path:

- strict phase ordering: the authorization transaction (account-operational
  barrier FIRST, exact Workspace/Task ownership, subject load and
  startability precheck) closes before any external GitHub call, and the
  arbitration transaction composes the barrier FIRST and the per-Workspace
  advisory lock before its locked re-read;
- no database transaction open across any external GitHub call (the fake
  pool tracks checked-out connections);
- the typed non-start conditions: already-planning, queued, stale source
  fingerprint, identity loss, blocked/closed issue — each fails closed
  WITHOUT a queue write and, for the precheck conditions, without any
  GitHub call;
- structural feasibility from the complete required multiset: an impossible
  configuration is an actionable error, never a queue state;
- temporary capacity occupation durably queues the Task with no
  reservations;
- the bounded FIFO establishment pass advances admissible older candidates
  (OPENORC actor claims) before the target decides, examines every page,
  and conservatively defers on budget exhaustion;
- the durable projection-based eligibility guard never advances a
  known-changed queued candidate and makes no GitHub call;
- the sweep re-evaluation uses the Workspace's durable Owner and surfaces
  per-candidate conditions without stalling.

Durable concurrency semantics are proven by the integration-marked suite
against a real Postgres. No live GitHub or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.github import (
    GitHubIssueObservation,
    GitHubRelatedIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services import task_start
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.task_intake import IssueBlockedEligibilityError
from openorc.services.task_start import (
    QueuedCandidateDisposition,
    TaskAlreadyPlanningError,
    TaskCapacityConfigurationError,
    TaskEligibilityFailure,
    TaskSourceDriftError,
    TaskSourceIdentityLossError,
    TaskStartQueueReason,
    TaskStartStateConflictError,
    TaskStartStatus,
    reevaluate_queued_tasks,
    start_task_for_owner,
)

_OBSERVED = datetime(2026, 10, 18, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_INSTALLATION_ID = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_FINGERPRINT = "a" * 64
_TASK_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_CONNECTION_ID = uuid.uuid4()

_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception
_Predicate = Callable[[str, tuple[Any, ...] | None], bool]


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


def _sql_has(*tokens: str) -> _Predicate:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


def _sql_lacks(predicate: _Predicate, *tokens: str) -> _Predicate:
    def wrapped(sql: str, params: tuple[Any, ...] | None) -> bool:
        return predicate(sql, params) and not any(token in sql for token in tokens)

    return wrapped


def _role_is(role: str) -> Callable[[_Predicate], _Predicate]:
    def wrap(base: _Predicate) -> _Predicate:
        def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
            return (
                base(sql, params) and params is not None and len(params) > 1 and params[1] == role
            )

        return predicate

    return wrap


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler.

    Handlers are consumed in registration order unless registered as
    repeating, so same-shaped statements with different scripted outcomes
    (for example the dependency-mirror read in the freshness phase versus
    the eligibility guard's read) script in call order.
    """

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, _CannedResult, bool]] = []

    def on(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, False))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (predicate, result, once) in enumerate(self._handlers):
            if predicate(lowered, params):
                if once:
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
        raise AssertionError("task start service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row() -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, 5, "")


def _installation_row() -> tuple[Any, ...]:
    return (
        _INSTALLATION_ID,
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
        _INSTALLATION_ID,
    )


def _issue_projection_row(
    state: str = "open", github_issue_id: int = _GITHUB_ISSUE_ID, fingerprint: str = _FINGERPRINT
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        github_issue_id,
        _ISSUE_NUMBER,
        "Found a bug",
        "Steps",
        state,
        fingerprint,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
    )


def _task_row(
    *,
    task_id: Any = _TASK_ID,
    status: str = "ready_to_plan",
    state_token: Any = _STATE_TOKEN,
    queued_at: Any = None,
    fingerprint: str = _FINGERPRINT,
) -> tuple[Any, ...]:
    return (
        task_id,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        status,
        None,
        None,
        state_token,
        None,
        None,
        fingerprint,
        queued_at,
        _OBSERVED,
        _OBSERVED,
    )


def _connection_row(
    connection_id: Any = _CONNECTION_ID, *, capacity: int = 2, enabled: bool = True
) -> tuple[Any, ...]:
    return (
        connection_id,
        _WORKSPACE_ID,
        "cline",
        "primary cline hub",
        {},
        capacity,
        enabled,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _binding_row(role: str, connection_id: Any = _CONNECTION_ID) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        role,
        connection_id,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _session_row(
    role: str,
    connection_id: Any = _CONNECTION_ID,
    *,
    task_id: Any = _TASK_ID,
    lifecycle_status: str = "connecting",
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        task_id,
        role,
        connection_id,
        None,
        lifecycle_status,
        None,
        None,
        None,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _event_row(actor_type: str = "owner", actor_id: Any = _OWNER_PROFILE_ID) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "planning_started",
        actor_type,
        None if actor_id is None else str(actor_id),
        None,
        None,
        {},
        _OBSERVED,
    )


class FakeGitHubAppClient:
    """Scripted adapter fake recording call order — zero calls is assertable.

    When constructed with the test's pool fake, each call also records how
    many pool connections were checked out at that moment: any value above
    zero means a database transaction was open across external I/O.
    """

    def __init__(
        self, *, blocked_by: list[GitHubRelatedIssueObservation] | None = None, pool: Any = None
    ) -> None:
        self._blocked_by = blocked_by if blocked_by is not None else []
        self._pool = pool
        self.calls: list[str] = []
        self.active_connections_during_calls: list[int] = []

    def _record(self, call: str) -> None:
        self.calls.append(call)
        if self._pool is not None:
            self.active_connections_during_calls.append(self._pool.active_connections)

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        self._record("repository")
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
        self._record("issue")
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
        self._record("blocked_by")
        return self._blocked_by

    def get_issue_parent(self, **_kwargs: Any) -> Any:
        self._record("parent")
        raise AssertionError("the start path performs no hierarchy synchronization")

    def get_issue_sub_issues(self, **_kwargs: Any) -> Any:
        self._record("sub_issues")
        raise AssertionError("the start path performs no hierarchy synchronization")

    def resolve_related_issue_endpoints(
        self,
        *,
        github_installation_id: int,
        local_github_repository_id: int,
        fresh_owner_login: str,
        fresh_repository_name: str,
        related: list[GitHubRelatedIssueObservation],
    ) -> list[Any]:
        self._record("resolve_related_endpoints")
        # Same-repository references reuse the local stable identity.
        from openorc.adapters.github import GitHubRelatedIssueEndpoint

        local_address = (
            f"https://api.github.com/repos/{fresh_owner_login}/{fresh_repository_name}"
        ).lower()
        return [
            GitHubRelatedIssueEndpoint(
                github_repository_id=local_github_repository_id
                if observation.repository_url.lower() == local_address
                else 555,
                github_issue_id=observation.github_issue_id,
            )
            for observation in related
        ]

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the service must not compose raw #58 validation operations")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")

    def get_repository_branch(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe branches")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not observe pull requests")

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not create pull requests")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not project combined status")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not request merges")

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not compose user-write validation")


_BARRIER_ROW = (None, None, None)


def _script_baseline(conn: ScriptedConnection) -> None:
    """Uniform repeating reads every composed cycle performs identically.

    Consumable overrides registered before these take precedence for their
    first matching calls; every later identical read keeps returning the
    stable rows.
    """
    conn.on_repeat(_sql_has("openorc.profiles", "for key share"), _BARRIER_ROW)
    conn.on_repeat(_sql_has("from openorc.workspaces"), _ws_row())
    conn.on_repeat(_sql_has("pg_advisory_xact_lock"), (True,))
    conn.on_repeat(_sql_has("openorc.repositories", "for update"), _repository_row())
    conn.on_repeat(
        _sql_lacks(_sql_has("from openorc.repositories"), "for update"), _repository_row()
    )
    conn.on_repeat(_sql_has("from openorc.github_installations where id"), _installation_row())
    conn.on_repeat(_sql_has("openorc.connections", "for update"), [_connection_row()])
    conn.on_repeat(_sql_has("update openorc.github_issues"), _issue_projection_row())
    conn.on_repeat(_sql_has("openorc.github_issues", "for update"), _issue_projection_row())
    conn.on_repeat(
        _sql_lacks(_sql_has("from openorc.github_issues"), "for update", "update openorc"),
        _issue_projection_row(),
    )
    conn.on_repeat(
        _sql_lacks(_sql_has("from openorc.github_issue_dependencies"), "delete", "insert"), []
    )
    conn.on_repeat(_sql_has("delete from openorc.github_issue_dependencies"), [])
    conn.on_repeat(_sql_has("insert into openorc.github_issue_dependencies"), [])
    conn.on_repeat(
        _role_is("producer")(_sql_has("workflow_role_bindings")), _binding_row("producer")
    )
    conn.on_repeat(
        _role_is("reviewer")(_sql_has("workflow_role_bindings")), _binding_row("reviewer")
    )


def _script_task_reads(conn: ScriptedConnection, *, task_id: Any, row: tuple[Any, ...]) -> None:
    """Bound task reads (locked re-read and plain reads) to one exact subject."""

    def locked(sql: str, params: tuple[Any, ...] | None) -> bool:
        return "openorc.tasks" in sql and "for update" in sql and params == (task_id,)

    def plain(sql: str, params: tuple[Any, ...] | None) -> bool:
        return (
            "from openorc.tasks where id" in sql
            and "for update" not in sql
            and "update openorc.tasks" not in sql
            and "status = 'queued'" not in sql
            and params == (task_id,)
        )

    conn.on_repeat(locked, row)
    conn.on_repeat(plain, row)


def _script_task_update(
    conn: ScriptedConnection, *, task_id: Any, status_value: str, row: tuple[Any, ...]
) -> None:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return (
            "update openorc.tasks" in sql
            and params is not None
            and params[0] == status_value
            and params[3] == task_id
        )

    conn.on_repeat(predicate, row)


def _session_read_predicate(task_id: Any, role: str) -> _Predicate:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return (
            "from openorc.task_agent_sessions where task_id" in sql
            and "insert" not in sql
            and params == (task_id, role)
        )

    return predicate


def _session_insert_predicate(role: str) -> _Predicate:
    # ensure_task_agent_session inserts (workspace_id, task_id, role, connection_id).
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return (
            "insert into openorc.task_agent_sessions" in sql
            and params is not None
            and params[2] == role
        )

    return predicate


def _script_session_reads(
    conn: ScriptedConnection, *, task_id: Any, producer: Any = None, reviewer: Any = None
) -> None:
    conn.on_repeat(_session_read_predicate(task_id, "producer"), producer)
    conn.on_repeat(_session_read_predicate(task_id, "reviewer"), reviewer)


def _script_session_inserts(conn: ScriptedConnection, *, producer: Any, reviewer: Any) -> None:
    conn.on_repeat(_session_insert_predicate("producer"), producer)
    conn.on_repeat(_session_insert_predicate("reviewer"), reviewer)


def _script_occupancy(conn: ScriptedConnection, rows: list[tuple[Any, Any]]) -> None:
    conn.on_repeat(_sql_has("select connection_id, count(*)"), rows)


def _script_events(conn: ScriptedConnection) -> None:
    conn.on_repeat(_event_predicate("owner"), _event_row("owner", _OWNER_PROFILE_ID))
    conn.on_repeat(_event_predicate("openorc"), _event_row("openorc", None))


def _event_predicate(actor_type: str) -> _Predicate:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return (
            "insert into openorc.workflow_events" in sql
            and params is not None
            and params[3] == actor_type
        )

    return predicate


def _queue_page(conn: ScriptedConnection, rows: list[tuple[Any, ...]]) -> None:
    """One consumable queue page; pages script in call order."""
    conn.on(_sql_has("select", "status = 'queued'"), rows)


def test_malformed_commands_fail_before_any_database_or_github_effect() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(pool=FakePool(conn))

    for kwargs in (
        {**_start_kwargs(), "profile_id": "not-a-uuid"},
        {**_start_kwargs(), "workspace_id": 42},
        {**_start_kwargs(), "task_id": None},
        {**_start_kwargs(), "expected_state_token": "not-a-uuid"},
    ):
        with pytest.raises(InvalidCommandError):
            start_task_for_owner(_pool(conn), github, **kwargs)

    assert conn.executed == []
    assert github.calls == []


def test_owner_start_claims_planning_with_both_reservations_and_one_owner_event() -> None:
    conn = ScriptedConnection()
    pool = FakePool(conn)
    github = FakeGitHubAppClient(pool=pool)
    producer_row = _session_row("producer")
    reviewer_row = _session_row("reviewer")
    _script_success_start(
        conn, capacity=2, occupancy=[], producer_session=producer_row, reviewer_session=reviewer_row
    )
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert outcome.status is TaskStartStatus.PLANNING
    assert outcome.queue_reason is None
    assert outcome.task.status is TaskStatus.PLANNING
    assert outcome.task.queued_at is None
    assert set(outcome.sessions) == {WorkflowRole.PRODUCER, WorkflowRole.REVIEWER}
    assert outcome.sessions[WorkflowRole.PRODUCER].id == producer_row[0]
    assert outcome.sessions[WorkflowRole.REVIEWER].id == reviewer_row[0]

    # Phase A: the account-operational barrier is the FIRST statement, and
    # the exact ownership reads follow before the subject load.
    assert "openorc.profiles" in conn.executed[0][0]
    assert "for key share" in conn.executed[0][0]
    assert "openorc.workspaces" in conn.executed[1][0]
    assert "openorc.tasks" in conn.executed[2][0]
    # Phase C: the per-Workspace advisory lock precedes the locked re-read.
    lock_index = next(
        index for index, (sql, _) in enumerate(conn.executed) if "pg_advisory_xact_lock" in sql
    )
    re_read_index = next(
        index
        for index, (sql, _) in enumerate(conn.executed)
        if "for update" in sql and "openorc.tasks" in sql
    )
    assert lock_index < re_read_index
    # The claim is one logical fact: the planning write and exactly one
    # planning_started event carry the OWNER actor.
    planning_updates = [
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.tasks" in sql and params is not None and params[0] == "planning"
    ]
    assert len(planning_updates) == 1
    owner_events = [
        params
        for sql, params in conn.executed
        if "insert into openorc.workflow_events" in sql
        and params is not None
        and params[3] == "owner"
    ]
    assert len(owner_events) == 1
    assert owner_events[0][:7] == (
        _WORKSPACE_ID,
        _TASK_ID,
        "planning_started",
        "owner",
        str(_OWNER_PROFILE_ID),
        None,
        None,
    )
    assert getattr(owner_events[0][7], "obj", owner_events[0][7]) == {}
    # Phase A closed before external I/O: every GitHub call saw zero checked
    # out pool connections.
    assert github.active_connections_during_calls == [0] * len(github.calls)
    assert github.calls == [
        "repository",
        "issue",
        "repository",
        "blocked_by",
        "resolve_related_endpoints",
    ]


@pytest.mark.parametrize(
    "barrier_error", [ConflictError("the account is being deleted"), NotFoundError("no account")]
)
def test_blocked_or_missing_account_caller_never_triggers_github(barrier_error: Exception) -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    conn.on(_sql_has("openorc.profiles", "for key share"), barrier_error)
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=_task_row())

    with pytest.raises(type(barrier_error)):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert github.calls == []
    _assert_no_task_writes(conn)


def test_foreign_workspace_or_task_is_uniform_not_found_without_github() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    # The ownership read resolves no Workspace for this Profile: uniform
    # not-found, and the system-capable reconciliation is never reached.
    conn.on(_sql_has("from openorc.workspaces"), None)
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=_task_row())

    with pytest.raises(NotFoundError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert github.calls == []
    _assert_no_task_writes(conn)


@pytest.mark.parametrize(
    ("status", "queued_at", "expected_error"),
    [
        ("planning", None, TaskAlreadyPlanningError),
        ("queued", _OBSERVED, TaskStartStateConflictError),
        ("waiting_for_owner", None, TaskStartStateConflictError),
        ("blocked", None, TaskStartStateConflictError),
    ],
)
def test_non_startable_states_conflict_before_any_github_work(
    status: str, queued_at: Any, expected_error: type[Exception]
) -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    row = _task_row(status=status, queued_at=queued_at)
    _script_task_reads(conn, task_id=_TASK_ID, row=row)
    _script_baseline(conn)

    with pytest.raises(expected_error):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert github.calls == []
    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_stale_source_fingerprint_fails_closed_without_queueing() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    drifted = _issue_projection_row(fingerprint="b" * 64)
    # Both the locked reconcile read and its returning write carry the fresh
    # (drifted) requirements fingerprint.
    conn.on(_sql_has("openorc.github_issues", "for update"), drifted)
    conn.on(_sql_has("update openorc.github_issues"), drifted)
    _script_success_start(conn)

    with pytest.raises(TaskSourceDriftError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert "issue" in github.calls
    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_source_identity_loss_fails_closed() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    moved = _issue_projection_row(github_issue_id=999_999)
    conn.on(_sql_has("openorc.github_issues", "for update"), moved)
    conn.on(_sql_has("update openorc.github_issues"), moved)
    _script_success_start(conn)

    with pytest.raises(TaskSourceIdentityLossError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    _assert_no_task_writes(conn)


def test_blocked_issue_is_not_capacity_waiting() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient(
        blocked_by=[
            GitHubRelatedIssueObservation(
                github_issue_id=606,
                repository_url="https://api.github.com/repos/octocat/repo",
            )
        ]
    )
    _script_success_start(conn)

    with pytest.raises(IssueBlockedEligibilityError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert "blocked_by" in github.calls
    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_closed_issue_is_not_startable() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    closed = _issue_projection_row(state="closed")
    conn.on(_sql_has("openorc.github_issues", "for update"), closed)
    conn.on(_sql_has("update openorc.github_issues"), closed)
    _script_success_start(conn)

    with pytest.raises(NotFoundError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    _assert_no_task_writes(conn)


def test_stale_state_token_in_arbitration_is_stale() -> None:
    from openorc.services.errors import StaleOperationError

    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    _script_success_start(conn, task_row=_task_row(state_token=uuid.uuid4()))
    _queue_page(conn, [])

    with pytest.raises(StaleOperationError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    # Phase B ran (the token moved between the phases), but no claim or
    # queue write applied against the moved state.
    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_shared_capacity_one_is_an_actionable_error_never_a_queue() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    conn.on_repeat(_sql_has("openorc.connections", "for update"), [_connection_row(capacity=1)])
    _script_success_start(conn)

    with pytest.raises(TaskCapacityConfigurationError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    # The structurally impossible combination is rejected BEFORE any queue
    # write, even with no competing Tasks at all.
    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_existing_producer_reservation_on_shared_capacity_one_is_structural() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    conn.on_repeat(_sql_has("openorc.connections", "for update"), [_connection_row(capacity=1)])
    _script_success_start(conn)
    # The Task already holds a valid CONNECTING Producer reservation on the
    # shared capacity-1 Connection: the complete required multiset (2) still
    # exceeds capacity (1) — #68's new-reservation math alone would report a
    # temporary wait and queue forever.
    conn.on_repeat(
        _session_read_predicate(_TASK_ID, "producer"),
        _session_row("producer", lifecycle_status="connecting"),
    )

    with pytest.raises(TaskCapacityConfigurationError):
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_temporarily_occupied_capacity_queues_without_reservations() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    # Shared capacity-2 Connection with one other Task's active slot: the
    # structural verdict is feasible, the #68 decision is a capacity wait.
    _script_success_start(conn, occupancy=[(_CONNECTION_ID, 1)])
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert outcome.status is TaskStartStatus.QUEUED
    assert outcome.queue_reason is TaskStartQueueReason.AWAITING_CAPACITY
    assert outcome.task.status is TaskStatus.QUEUED
    assert outcome.task.queued_at is not None
    assert outcome.sessions == {}
    # Queued with no reservations, and the queue entry write carried the
    # DB-clock CASE (the entering-queued branch).
    queued_updates = [
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.tasks" in sql and params is not None and params[0] == "queued"
    ]
    assert len(queued_updates) == 1
    assert "when %s = 'queued' and status <> 'queued' then now()" in queued_updates[0][0]
    _assert_no_session_reservations(conn)
    assert not any("insert into openorc.workflow_events" in sql for sql, _ in conn.executed)


def test_distinct_capacity_one_connections_admit_together() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    first_connection, second_connection = sorted((uuid.uuid4(), uuid.uuid4()))
    conn.on_repeat(
        _sql_has("openorc.connections", "for update"),
        [
            _connection_row(first_connection, capacity=1),
            _connection_row(second_connection, capacity=1),
        ],
    )
    conn.on_repeat(
        _role_is("producer")(_sql_has("workflow_role_bindings")),
        _binding_row("producer", second_connection),
    )
    conn.on_repeat(
        _role_is("reviewer")(_sql_has("workflow_role_bindings")),
        _binding_row("reviewer", first_connection),
    )
    producer_row = _session_row("producer", second_connection)
    reviewer_row = _session_row("reviewer", first_connection)
    _script_success_start(
        conn, occupancy=[], producer_session=producer_row, reviewer_session=reviewer_row
    )
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert outcome.status is TaskStartStatus.PLANNING
    assert outcome.sessions[WorkflowRole.PRODUCER].connection_id == second_connection
    assert outcome.sessions[WorkflowRole.REVIEWER].connection_id == first_connection
    lock_sql, lock_params = next(
        (sql, params)
        for sql, params in conn.executed
        if "openorc.connections" in sql and "for update" in sql
    )
    # The two distinct capacity-1 Connections lock in ascending UUID order.
    assert lock_params == (sorted([first_connection, second_connection]),)
    assert "order by id for update" in lock_sql


_CANDIDATE_ID = uuid.uuid4()
_CANDIDATE_TOKEN = uuid.uuid4()
_CANDIDATE_QUEUED_AT = datetime(2026, 10, 18, 11, 0, 0, tzinfo=UTC)


def _candidate_row(
    *, task_id: Any = _CANDIDATE_ID, state_token: Any = _CANDIDATE_TOKEN
) -> tuple[Any, ...]:
    return _task_row(
        task_id=task_id, status="queued", state_token=state_token, queued_at=_CANDIDATE_QUEUED_AT
    )


def _script_candidate_claim(conn: ScriptedConnection, *, task_id: Any) -> None:
    """Script one queued candidate's full admissible arbitration."""
    _script_task_reads(conn, task_id=task_id, row=_candidate_row(task_id=task_id))
    _script_task_update(
        conn,
        task_id=task_id,
        status_value="planning",
        row=_task_row(task_id=task_id, status="planning", state_token=uuid.uuid4()),
    )
    _script_session_reads(conn, task_id=task_id)
    _script_session_inserts(
        conn,
        producer=_session_row("producer", task_id=task_id),
        reviewer=_session_row("reviewer", task_id=task_id),
    )


def test_establishment_pass_advances_the_older_candidate_before_the_target() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    _script_success_start(conn)
    _script_candidate_claim(conn, task_id=_CANDIDATE_ID)
    _queue_page(conn, [_candidate_row()])
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    # The older queued candidate was advanced FIRST (FIFO), then the target
    # claimed the remaining capacity.
    assert outcome.status is TaskStartStatus.PLANNING
    candidate_update_index = next(
        index
        for index, (sql, params) in enumerate(conn.executed)
        if "update openorc.tasks" in sql and params is not None and params[3] == _CANDIDATE_ID
    )
    target_update_index = next(
        index
        for index, (sql, params) in enumerate(conn.executed)
        if "update openorc.tasks" in sql
        and params is not None
        and params[3] == _TASK_ID
        and params[0] == "planning"
    )
    assert candidate_update_index < target_update_index
    # The advanced candidate's claim carries the OPENORC system actor; the
    # explicitly started Task carries the OWNER actor.
    openorc_events = [
        params
        for sql, params in conn.executed
        if "insert into openorc.workflow_events" in sql
        and params is not None
        and params[3] == "openorc"
    ]
    assert len(openorc_events) == 1
    assert openorc_events[0][1] == _CANDIDATE_ID
    owner_events = [
        params
        for sql, params in conn.executed
        if "insert into openorc.workflow_events" in sql
        and params is not None
        and params[3] == "owner"
    ]
    assert owner_events[0][1] == _TASK_ID


def test_establishment_pass_consuming_capacity_queues_the_target() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    # The candidate's admission consumes the connection's free slots; the
    # target's subsequent admission decision observes the new occupancy.
    conn.on(_sql_has("select connection_id, count(*)"), [])
    conn.on(_sql_has("select connection_id, count(*)"), [(_CONNECTION_ID, 2)])
    _script_success_start(conn)
    _script_candidate_claim(conn, task_id=_CANDIDATE_ID)
    _queue_page(conn, [_candidate_row()])
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert outcome.status is TaskStartStatus.QUEUED
    assert outcome.queue_reason is TaskStartQueueReason.AWAITING_CAPACITY
    assert outcome.task.status is TaskStatus.QUEUED
    # The advanced candidate is not rolled back: its claim stands ahead of
    # the target in FIFO order.
    assert any(
        "update openorc.tasks" in sql
        and params is not None
        and params[3] == _CANDIDATE_ID
        and params[0] == "planning"
        for sql, params in conn.executed
    )


def test_multi_page_queue_is_fully_examined_before_the_target_decides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(task_start, "_QUEUE_PAGE_LIMIT", 1)
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    candidate_b_id = uuid.uuid4()
    _script_success_start(conn)
    _script_candidate_claim(conn, task_id=_CANDIDATE_ID)
    _script_candidate_claim(conn, task_id=candidate_b_id)
    _queue_page(conn, [_candidate_row()])
    _queue_page(conn, [_candidate_row(task_id=candidate_b_id)])
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert outcome.status is TaskStartStatus.PLANNING
    # Every queued candidate across pages was claimed before the target.
    planning_targets = [
        params[3]
        for sql, params in conn.executed
        if "update openorc.tasks" in sql and params is not None and params[0] == "planning"
    ]
    assert planning_targets == [_CANDIDATE_ID, candidate_b_id, _TASK_ID]


def test_budget_exhaustion_defers_the_target_conservatively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(task_start, "_ARBITRATION_CANDIDATE_BUDGET", 1)
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    _script_success_start(conn)
    _script_candidate_claim(conn, task_id=_CANDIDATE_ID)
    _queue_page(conn, [_candidate_row(), _candidate_row(task_id=uuid.uuid4())])
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    # The budget bounded the scan after the first candidate: the target
    # durably queues with the observable deferral reason instead of
    # claiming on unexamined candidates.
    assert outcome.status is TaskStartStatus.QUEUED
    assert outcome.queue_reason is TaskStartQueueReason.ARBITRATION_DEFERRED
    planning_updates = [
        params[3]
        for sql, params in conn.executed
        if "update openorc.tasks" in sql and params is not None and params[0] == "planning"
    ]
    assert planning_updates == [_CANDIDATE_ID]


def test_durable_projection_contradiction_never_advances_the_candidate() -> None:
    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    # Target's guard first (open), then the candidate's guard observes the
    # durable mirror reporting the issue closed. Consumables register before
    # the repeating baseline takes over for any later read.
    conn.on(
        _sql_lacks(_sql_has("from openorc.github_issues"), "for update", "update openorc"),
        _issue_projection_row(),
    )
    conn.on(
        _sql_lacks(_sql_has("from openorc.github_issues"), "for update", "update openorc"),
        _issue_projection_row(state="closed"),
    )
    _script_success_start(conn)
    _script_task_reads(conn, task_id=_CANDIDATE_ID, row=_candidate_row())
    _queue_page(conn, [_candidate_row()])
    _queue_page(conn, [])

    outcome = start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert outcome.status is TaskStartStatus.PLANNING
    assert any(
        "update openorc.tasks" in sql
        and params is not None
        and params[3] == _TASK_ID
        and params[0] == "planning"
        for sql, params in conn.executed
    )
    # The contradicted candidate was never claimed and stayed queued with
    # its original entry timestamp.
    assert not any(
        "update openorc.tasks" in sql and params is not None and params[3] == _CANDIDATE_ID
        for sql, params in conn.executed
    )


def test_target_eligibility_contradiction_comes_from_the_mirror_only() -> None:
    from openorc.services.task_start import TaskEligibilityContradictedError

    conn = ScriptedConnection()
    github = FakeGitHubAppClient()
    conn.on(
        _sql_lacks(_sql_has("from openorc.github_issues"), "for update", "update openorc"),
        _issue_projection_row(state="closed"),
    )
    _script_success_start(conn)
    _queue_page(conn, [])

    with pytest.raises(TaskEligibilityContradictedError) as excinfo:
        start_task_for_owner(_pool(conn), github, **_start_kwargs())

    assert excinfo.value.reason is TaskEligibilityFailure.CLOSED_IN_PROJECTION
    # The fresh GitHub path succeeded (Phase B ran); the contradiction was
    # observed against the durable mirror alone.
    assert "issue" in github.calls
    _assert_no_task_writes(conn)


def test_sweep_advances_candidates_with_the_workspace_owner_as_the_barrier() -> None:
    conn = ScriptedConnection()
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=_task_row())
    _script_candidate_claim(conn, task_id=_CANDIDATE_ID)
    conn.on_repeat(_sql_has("select connection_id, count(*)"), [])
    _script_events(conn)
    conn.on_repeat(_sql_has("insert into openorc.workflow_events"), _event_row("github", None))
    _queue_page(conn, [_candidate_row()])
    _queue_page(conn, [])

    outcome = reevaluate_queued_tasks(_pool(conn), workspace_id=_WORKSPACE_ID)

    assert outcome.exhausted is True
    assert [entry.disposition for entry in outcome.outcomes] == [
        QueuedCandidateDisposition.ADMITTED
    ]
    # The system invocation composes the account barrier for the Workspace's
    # durable Owner and claims with the OPENORC actor.
    barrier_params = [
        params
        for sql, params in conn.executed
        if "openorc.profiles" in sql and "for key share" in sql and params is not None
    ]
    assert barrier_params and set(barrier_params) == {(_OWNER_PROFILE_ID,)}
    openorc_events = [
        params
        for sql, params in conn.executed
        if "insert into openorc.workflow_events" in sql
        and params is not None
        and params[3] == "openorc"
    ]
    assert len(openorc_events) == 1
    assert openorc_events[0][1] == _CANDIDATE_ID


def _assert_no_task_writes(conn: ScriptedConnection) -> None:
    assert not any("update openorc.tasks" in sql for sql, _ in conn.executed)


def _assert_no_session_reservations(conn: ScriptedConnection) -> None:
    assert not any("insert into openorc.task_agent_sessions" in sql for sql, _ in conn.executed)


def _start_kwargs() -> dict[str, Any]:
    return {
        "profile_id": _OWNER_PROFILE_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "expected_state_token": _STATE_TOKEN,
    }


def _script_success_start(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    capacity: int = 2,
    occupancy: list[tuple[Any, Any]] | None = None,
    producer_session: Any = None,
    reviewer_session: Any = None,
) -> tuple[Any, ...]:
    """Script the full Owner-start happy path up to the final admission.

    The caller scripts the final admission outcome (occupancy) and the
    post-claim/queue rows itself; the returned row is the claimed planning
    Task by default.
    """
    row = task_row if task_row is not None else _task_row()
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=row)
    _script_task_update(
        conn,
        task_id=_TASK_ID,
        status_value="planning",
        row=_task_row(status="planning", state_token=uuid.uuid4()),
    )
    _script_task_update(
        conn,
        task_id=_TASK_ID,
        status_value="queued",
        row=_task_row(status="queued", state_token=uuid.uuid4(), queued_at=_OBSERVED),
    )
    _script_session_reads(conn, task_id=_TASK_ID)
    _script_session_inserts(
        conn,
        producer=producer_session if producer_session is not None else _session_row("producer"),
        reviewer=reviewer_session if reviewer_session is not None else _session_row("reviewer"),
    )
    _script_occupancy(conn, occupancy if occupancy is not None else [])
    _script_events(conn)
    # Fallback for other event vocabulary along the path (for example the
    # dependency mirror's github-actor task_dependency_synced event).
    conn.on_repeat(_sql_has("insert into openorc.workflow_events"), _event_row("github", None))
    return row


def test_sweep_surfaces_conditions_without_stalling_remaining_candidates() -> None:
    conn = ScriptedConnection()
    candidate_b_id = uuid.uuid4()
    # First guard read (oldest candidate): the mirror reports the issue closed.
    conn.on(
        _sql_lacks(_sql_has("from openorc.github_issues"), "for update", "update openorc"),
        _issue_projection_row(state="closed"),
    )
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=_task_row())
    _script_task_reads(conn, task_id=_CANDIDATE_ID, row=_candidate_row())
    _script_candidate_claim(conn, task_id=candidate_b_id)
    conn.on_repeat(_sql_has("select connection_id, count(*)"), [])
    _script_events(conn)
    conn.on_repeat(_sql_has("insert into openorc.workflow_events"), _event_row("github", None))
    _queue_page(conn, [_candidate_row(), _candidate_row(task_id=candidate_b_id)])
    _queue_page(conn, [])

    outcome = reevaluate_queued_tasks(_pool(conn), workspace_id=_WORKSPACE_ID)

    assert outcome.exhausted is True
    assert [entry.disposition for entry in outcome.outcomes] == [
        QueuedCandidateDisposition.INELIGIBLE,
        QueuedCandidateDisposition.ADMITTED,
    ]
    assert outcome.outcomes[0].eligibility_reason is TaskEligibilityFailure.CLOSED_IN_PROJECTION
    # The ineligible oldest candidate stayed queued; the next candidate
    # still advanced.
    assert not any(
        "update openorc.tasks" in sql and params is not None and params[3] == _CANDIDATE_ID
        for sql, params in conn.executed
    )
    assert any(
        "update openorc.tasks" in sql
        and params is not None
        and params[3] == candidate_b_id
        and params[0] == "planning"
        for sql, params in conn.executed
    )


def test_sweep_structurally_infeasible_candidate_surfaces_the_configuration_error() -> None:
    conn = ScriptedConnection()
    # Registered before the baseline so the capacity-1 Connection wins.
    conn.on_repeat(_sql_has("openorc.connections", "for update"), [_connection_row(capacity=1)])
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=_task_row())
    _script_task_reads(conn, task_id=_CANDIDATE_ID, row=_candidate_row())
    _script_session_reads(
        conn,
        task_id=_CANDIDATE_ID,
        producer=_session_row("producer", task_id=_CANDIDATE_ID, lifecycle_status="connecting"),
        reviewer=None,
    )
    conn.on_repeat(_sql_has("select connection_id, count(*)"), [])
    _script_events(conn)
    conn.on_repeat(_sql_has("insert into openorc.workflow_events"), _event_row("github", None))
    _queue_page(conn, [_candidate_row()])
    _queue_page(conn, [])

    outcome = reevaluate_queued_tasks(_pool(conn), workspace_id=_WORKSPACE_ID)

    assert outcome.exhausted is True
    assert [entry.disposition for entry in outcome.outcomes] == [
        QueuedCandidateDisposition.CONFIGURATION_ERROR
    ]
    _assert_no_task_writes(conn)
    _assert_no_session_reservations(conn)


def test_sweep_missing_workspace_fails_closed() -> None:
    conn = ScriptedConnection()
    conn.on(_sql_has("from openorc.workspaces"), None)
    _script_baseline(conn)

    with pytest.raises(NotFoundError):
        reevaluate_queued_tasks(_pool(conn), workspace_id=uuid.uuid4())

    _assert_no_task_writes(conn)


def test_sweep_malformed_workspace_command_is_invalid_before_any_effect() -> None:
    conn = ScriptedConnection()

    with pytest.raises(InvalidCommandError):
        reevaluate_queued_tasks(_pool(conn), workspace_id="not-a-uuid")  # type: ignore[arg-type]

    assert conn.executed == []


def test_missing_role_binding_is_a_candidate_conflict_that_never_stalls_followers() -> None:
    conn = ScriptedConnection()
    candidate_b_id = uuid.uuid4()
    # The oldest candidate's Producer role binding is gone (a routing change
    # after it queued): the first binding read resolves nothing.
    conn.on(_role_is("producer")(_sql_has("workflow_role_bindings")), None)
    _script_baseline(conn)
    _script_task_reads(conn, task_id=_TASK_ID, row=_task_row())
    _script_task_reads(conn, task_id=_CANDIDATE_ID, row=_candidate_row())
    _script_candidate_claim(conn, task_id=candidate_b_id)
    conn.on_repeat(_sql_has("select connection_id, count(*)"), [])
    _script_events(conn)
    conn.on_repeat(_sql_has("insert into openorc.workflow_events"), _event_row("github", None))
    _queue_page(conn, [_candidate_row(), _candidate_row(task_id=candidate_b_id)])
    _queue_page(conn, [])

    outcome = reevaluate_queued_tasks(_pool(conn), workspace_id=_WORKSPACE_ID)

    assert outcome.exhausted is True
    assert [entry.disposition for entry in outcome.outcomes] == [
        QueuedCandidateDisposition.CONFLICT,
        QueuedCandidateDisposition.ADMITTED,
    ]
    # The binding-less candidate stayed queued with its original durable key
    # and the eligible follower still advanced in the same sweep.
    assert not any(
        "update openorc.tasks" in sql and params is not None and params[3] == _CANDIDATE_ID
        for sql, params in conn.executed
    )
    assert any(
        "update openorc.tasks" in sql
        and params is not None
        and params[3] == candidate_b_id
        and params[0] == "planning"
        for sql, params in conn.executed
    )
