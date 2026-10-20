"""Deterministic tests for the E7 exact-head PR authorization gate (issue #82).

A scripted SQL-connection seam and a routing fake GitHub App client prove
the E7 seams on their critical paths:

- trigger and E6 handoff: only a durable successful *current* E6 Execution
  with a validated persisted ``implementation_result`` opens the gate; the
  branch is discovered from that persisted result (no caller SHA, no
  webhook path, no runtime call), and wrong status, stale token, prior
  gate, existing canonical PR, missing/failed/foreign execution, invalid
  persisted result shapes, and a producer-binding mismatch refuse before
  any effect;
- authoritative source/branch: the routed GitHub Repository supplies the
  issue reconciliation (B3) and the committed branch head (the shared B7
  read seam); access loss, rate limits, uncertain reads, closed source
  issues, and stable-identity mismatches are their own typed fail-closed
  refusals — never a gate and never fabricated requirements drift;
- the atomic drift block: confirmed fingerprint drift produces exactly one
  ``github_source_changed`` TaskBlock, the ``implementing → blocked``
  transition with a rotated token, and one ``task_blocked`` audit — no
  gate — and a forced late failure rolls the composition back; a
  subsequent presentation refuses at admission;
- atomic presentation: one verified branch bind, one exact-head
  ``PR_AUTHORIZATION`` gate, ``implementing → waiting_for_owner``, rotated
  token, and the matching OPENORC audit commit together; forced late
  failures roll the whole composition back; replayed/concurrent
  presentations cannot create a second gate; a bound-branch continuation
  is safe while a different-branch claim is impossible;
- Owner authority: the correct Profile's exact-gate Approve resolves it
  OWNER-attributed and yields a durable approving Profile + head; Cancel
  archives the Task terminal ``cancelled``; stale/foreign/replayed
  decisions apply nothing; approval never dispatches PR composition or
  GitHub create.

No live GitHub, runtime, or Postgres access; the new SQL
composition seams are proven by the integration-marked suite against a
real Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.github import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubBranchObservation,
    GitHubIssueObservation,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.github_issues import github_issue_requirements_fingerprint
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_pr_authorization import (
    approve_pr_authorization,
    cancel_pr_authorization,
    open_pr_authorization,
)

_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OTHER_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_OTHER_PROFILE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_PR_GATE_ID = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_EXECUTION_1_ID = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_TOKEN_3 = uuid.uuid4()
_TOKEN_4 = uuid.uuid4()
_BASE_SHA = "b" * 40
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_CLAIMED_BRANCH = "openorc/task-42-implementation"
_INSTALLATION_RECORD = uuid.uuid4()
_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Requirements body")
_DRIFTED_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug v2", "Requirements body")
_RESULT_PAYLOAD: dict[str, object] = {
    "type": "implementation_result",
    "schema_version": 1,
    "status": "COMPLETED",
    "branch": _CLAIMED_BRANCH,
    "summary": "Implemented the retry loop",
    "changes": ["Added the retry loop"],
    "validation": ["pytest tests/test_retry.py"],
}


def _sql_has(*markers: str) -> Callable[[str, tuple[Any, ...] | None], bool]:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(marker in sql for marker in markers)

    return predicate


def _sql_lacks(
    base: Callable[[str, tuple[Any, ...] | None], bool], *markers: str
) -> Callable[[str, tuple[Any, ...] | None], bool]:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return base(sql, params) and not any(marker in sql for marker in markers)

    return predicate


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler.

    Handlers are consumed in registration order unless registered as
    repeating, so same-shaped reads with different scripted outcomes script
    in call order.
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
        raise AssertionError("pr authorization tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_Predicate = Callable[[str, tuple[Any, ...] | None], bool]
_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | Exception | None

_ACCOUNT_BARRIER = _sql_has("from openorc.profiles", "for key share")
_WORKSPACE_READ = _sql_has("from openorc.workspaces")
_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_PR_READ = _sql_has("from openorc.task_pull_requests")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATES_INSERT = _sql_has("insert into openorc.owner_gates")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")
_REPO_READ = _sql_lacks(_sql_has("from openorc.repositories"), "for update")
_REPO_READ_LOCKED = _sql_has("from openorc.repositories", "for update")
_INSTALLATION_READ = _sql_has("from openorc.github_installations")
_ISSUE_READ_LOCKED = _sql_has("from openorc.github_issues", "for update")
_ISSUE_FIND = _sql_lacks(_sql_has("from openorc.github_issues"), "for update")
_ISSUE_UPDATE = _sql_has("update openorc.github_issues set")
_BRANCH_BIND = _sql_has("update openorc.tasks set canonical_feature_branch")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks set current_owner_gate_id")
_STATUS_UPDATE = _sql_has("update openorc.tasks set status", "queued_at = case")
_ARCHIVE_WRITE = _sql_has("update openorc.tasks set status", "archived_at = now()")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _ws_row(
    workspace_id: Any = _WORKSPACE_ID, profile_id: Any = _OWNER_PROFILE_ID
) -> tuple[Any, ...]:
    return (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 2, "")


def _task_row(
    *,
    status: str = "implementing",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
    gate_id: Any = None,
    canonical_branch: Any = None,
    archived: bool = False,
    workspace_id: Any = _WORKSPACE_ID,
    fingerprint: str = _FINGERPRINT,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        workspace_id,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        status,
        _OBSERVED if archived else None,
        canonical_branch,
        state_token,
        current_plan_revision_id,
        gate_id,
        fingerprint,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _revision_row(
    *, revision_id: Any = _REVISION_1_ID, number: int = 1, workspace_id: Any = _WORKSPACE_ID
) -> tuple[Any, ...]:
    return (revision_id, workspace_id, _TASK_ID, number, "# Plan", _BASE_SHA, _OBSERVED)


def _loop_row(*, status: str = "closed", closed_at: Any = _OBSERVED) -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, "planning", 2, status, closed_at, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any = _ITERATION_1_ID,
    number: int = 1,
    plan_revision_id: Any = _REVISION_1_ID,
    outcome: str | None = "accepted",
    decided_at: Any = _OBSERVED,
) -> tuple[Any, ...]:
    return (
        iteration_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _LOOP_ID,
        number,
        plan_revision_id,
        None,
        None,
        outcome,
        "Looks good",
        [],
        decided_at,
        _OBSERVED,
    )


def _gate_row(
    *,
    gate_id: Any = _GATE_1_ID,
    gate_type: str = "implementation_authorization",
    status: str = "approved",
    plan_revision_id: Any = _REVISION_1_ID,
    subject_head_sha: Any = None,
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        plan_revision_id,
        subject_head_sha,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _pr_gate_row(*, gate_id: Any = _PR_GATE_ID, status: str = "pending") -> tuple[Any, ...]:
    return _gate_row(
        gate_id=gate_id,
        gate_type="pr_authorization",
        status=status,
        plan_revision_id=None,
        subject_head_sha=_HEAD_SHA,
    )


def _session_row() -> tuple[Any, ...]:
    return (
        _SESSION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "producer",
        uuid.uuid4(),
        "ext-producer-session",
        "ready",
        {"repository_base": "/workspace/task-42"},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _execution_row(
    *,
    execution_id: Any = _EXECUTION_1_ID,
    number: int = 1,
    status: str = "succeeded",
    result: Any = _RESULT_PAYLOAD,
    producer_session_id: Any = _SESSION_ID,
) -> tuple[Any, ...]:
    return (
        execution_id,
        _WORKSPACE_ID,
        _TASK_ID,
        producer_session_id,
        number,
        status,
        _OBSERVED,
        _OBSERVED,
        result,
        None,
    )


def _repository_row(
    *, workspace_id: Any = _WORKSPACE_ID, installation_record: Any = None
) -> tuple[Any, ...]:
    return (
        _REPOSITORY_ID,
        uuid.uuid4(),
        workspace_id,
        _GITHUB_REPOSITORY_ID,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        installation_record if installation_record is not None else _INSTALLATION_RECORD,
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


def _issue_row(
    *,
    fingerprint: str = _FINGERPRINT,
    state: str = "open",
    github_issue_id: int = _GITHUB_ISSUE_ID,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        github_issue_id,
        _ISSUE_NUMBER,
        "Found a bug",
        "Requirements body",
        state,
        fingerprint,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
    )


def _pull_request_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        777001,
        101,
        _CLAIMED_BRANCH,
        "main",
        _HEAD_SHA,
        "open",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "owner_gate_created",
        "openorc",
        None,
        "owner_gate",
        _PR_GATE_ID,
        {},
        _OBSERVED,
    )


def _block_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "github_source_changed",
        {
            "github_issue_id": _GITHUB_ISSUE_ID,
            "expected_fingerprint": _FINGERPRINT,
            "observed_fingerprint": _DRIFTED_FINGERPRINT,
        },
        None,
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


def _issue_observation(
    *,
    github_issue_id: int = _GITHUB_ISSUE_ID,
    state: str = "open",
    title: str = "Found a bug",
) -> GitHubIssueObservation:
    return GitHubIssueObservation(
        github_issue_id=github_issue_id,
        issue_number=_ISSUE_NUMBER,
        title=title,
        body="Requirements body",
        state=state,
        provider_updated_at=_OBSERVED,
        is_pull_request=False,
    )


def _branch_observation() -> GitHubBranchObservation:
    return GitHubBranchObservation(branch_name=_CLAIMED_BRANCH, head_sha=_HEAD_SHA)


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self,
        *,
        pool: FakePool,
        repository_observation: GitHubRepositoryObservation | None = None,
        issue_observation: GitHubIssueObservation | None = None,
        branch_observation: GitHubBranchObservation | None = None,
        repository_error: Exception | None = None,
        issue_error: Exception | None = None,
        branch_error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._issue_observation = issue_observation
        self._branch_observation = branch_observation
        self._repository_error = repository_error
        self._issue_error = issue_error
        self._branch_error = branch_error
        self.repository_calls: list[dict[str, int]] = []
        self.issue_calls: list[dict[str, Any]] = []
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
        if self._repository_error is not None:
            raise self._repository_error
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

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not create pull requests")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe pull requests")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not request merges")

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not compose raw #58 validation operations")

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not compose user-write validation")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not project combined status")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the E7 seam must not compose raw adapter operations")

    def get_issue_blocked_by(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe relationships")

    def get_issue_sub_issues(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe relationships")

    def get_issue_parent(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe relationships")

    def resolve_related_issue_endpoints(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not resolve related endpoints")


# --- event/write observation helpers ------------------------------------------------


def _event_types(conn: ScriptedConnection) -> list[str]:
    types: list[str] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None):
            assert params is not None
            types.append(cast(str, params[2]))
    return types


def _event_params_of_type(conn: ScriptedConnection, event_type: str) -> list[tuple[Any, ...]]:
    matched: list[tuple[Any, ...]] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None and params[2] == event_type:
            matched.append(params)
    return matched


def _event_context(params: tuple[Any, ...]) -> Any:
    """Unwrap the event context (the insert path wraps it in Jsonb)."""
    return getattr(params[7], "obj", params[7])


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


def _write_predicates_fired(conn: ScriptedConnection) -> bool:
    return any(
        _count(conn, predicate) > 0
        for predicate in (
            _BRANCH_BIND,
            _GATES_INSERT,
            _TASK_GATE_WRITE,
            _STATUS_UPDATE,
            _ARCHIVE_WRITE,
            _BLOCK_INSERT,
        )
    )


# --- scripted sequences (in service call order) -------------------------------------


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    """Same-shaped reads whose result never changes across the whole flow."""
    conn.on_repeat(_ACCOUNT_BARRIER, (None, None, None))
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on_repeat(_EVENT_INSERT, _event_row())
    conn.on_repeat(_SESSION_READ, _session_row())


def _script_authority_reads(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    revision_row: tuple[Any, ...] | None = None,
    gates: list[tuple[Any, ...]] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    iterations: list[tuple[Any, ...]] | None = None,
    executions: list[tuple[Any, ...]] | None = None,
    pr_row: tuple[Any, ...] | None = None,
) -> None:
    """The admission/authority reads of one composition, in call order."""
    conn.on(_TASK_READ, task_row)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, task_row)  # get_task_for_update
    conn.on(_TASK_READ, task_row)  # require_current_task
    conn.on(_PR_READ, pr_row)  # no canonical PR record
    conn.on(_REVISION_READ, revision_row if revision_row is not None else _revision_row())
    conn.on(_GATES_LIST, gates if gates is not None else [_gate_row()])
    conn.on(_LOOPS_LIST, loops if loops is not None else [_loop_row()])
    conn.on(_ITERATIONS_LIST, iterations if iterations is not None else [_iteration_row()])
    conn.on(_EXECUTIONS_LIST, executions if executions is not None else [_execution_row()])


def _script_b3_reconciliation(
    conn: ScriptedConnection,
    *,
    locked_issue_row: tuple[Any, ...] | None = None,
    updated_issue_row: tuple[Any, ...] | None = None,
) -> None:
    """The B3 route resolution, locked reconcile read, and optional update."""
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())
    conn.on(_REPO_READ_LOCKED, _repository_row())
    conn.on(_ISSUE_READ_LOCKED, locked_issue_row if locked_issue_row is not None else _issue_row())
    if updated_issue_row is not None:
        conn.on(_ISSUE_UPDATE, updated_issue_row)


def _script_branch_observation(conn: ScriptedConnection) -> None:
    """The shared B7 read seam's route resolution (the reads are external)."""
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())


def _script_open_success(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gates: list[tuple[Any, ...]] | None = None,
    iterations: list[tuple[Any, ...]] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    executions: list[tuple[Any, ...]] | None = None,
    issue_row: tuple[Any, ...] | None = None,
    fail_branch_bind: bool = False,
    fail_gate_install: bool = False,
    fail_status: bool = False,
) -> None:
    """The full presentation composition in call order."""
    _script_repeating_reads(conn)
    admission_task = task_row if task_row is not None else _task_row()
    _script_authority_reads(
        conn,
        task_row=admission_task,
        gates=gates,
        loops=loops,
        iterations=iterations,
        executions=executions,
    )
    _script_b3_reconciliation(conn, locked_issue_row=issue_row)
    _script_branch_observation(conn)
    # --- Phase C: re-proofs, bind, gate, transition, audit. ---
    conn.on(_TASK_READ_LOCKED, admission_task)
    conn.on(_TASK_READ, admission_task)
    conn.on(_PR_READ, None)
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, gates if gates is not None else [_gate_row()])
    conn.on(_LOOPS_LIST, loops if loops is not None else [_loop_row()])
    conn.on(_ITERATIONS_LIST, iterations if iterations is not None else [_iteration_row()])
    conn.on(_EXECUTIONS_LIST, executions if executions is not None else [_execution_row()])
    conn.on(_ISSUE_FIND, issue_row if issue_row is not None else _issue_row())
    if fail_branch_bind:
        conn.on(_BRANCH_BIND, RuntimeError("forced late bind failure"))
    else:
        conn.on(_BRANCH_BIND, _task_row(state_token=_TOKEN_2, canonical_branch=_CLAIMED_BRANCH))
    conn.on(_GATES_INSERT, _pr_gate_row())
    if fail_gate_install:
        conn.on(_TASK_GATE_WRITE, RuntimeError("forced late gate install failure"))
    else:
        conn.on(
            _TASK_GATE_WRITE,
            _task_row(state_token=_TOKEN_3, gate_id=_PR_GATE_ID, canonical_branch=_CLAIMED_BRANCH),
        )
    if fail_status:
        conn.on(_STATUS_UPDATE, RuntimeError("forced late status failure"))
    else:
        conn.on(
            _STATUS_UPDATE,
            _task_row(
                status="waiting_for_owner",
                state_token=_TOKEN_4,
                gate_id=_PR_GATE_ID,
                canonical_branch=_CLAIMED_BRANCH,
            ),
        )


def _script_drift_block(
    conn: ScriptedConnection,
    *,
    blocked_task_row: tuple[Any, ...] | None = None,
    fail_status: bool = False,
) -> None:
    """The atomic drift-block composition in call order (after the B3 update)."""
    _script_repeating_reads(conn)
    drifting_task = _task_row()
    _script_authority_reads(conn, task_row=drifting_task)
    _script_b3_reconciliation(
        conn,
        locked_issue_row=_issue_row(fingerprint=_FINGERPRINT),
        updated_issue_row=_issue_row(fingerprint=_DRIFTED_FINGERPRINT),
    )
    conn.on(_TASK_READ_LOCKED, drifting_task)
    conn.on(_TASK_READ, drifting_task)
    conn.on(_PR_READ, None)
    conn.on(_ISSUE_FIND, _issue_row(fingerprint=_DRIFTED_FINGERPRINT))
    conn.on(_BLOCK_INSERT, _block_row())
    if fail_status:
        conn.on(_STATUS_UPDATE, RuntimeError("forced late block transition failure"))
    else:
        conn.on(
            _STATUS_UPDATE,
            blocked_task_row
            if blocked_task_row is not None
            else _task_row(status="blocked", state_token=_TOKEN_2),
        )


def _script_approve(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gate_row: tuple[Any, ...] | None = None,
    executions: list[tuple[Any, ...]] | None = None,
    fail_resolution: bool = False,
    stale_current_gate: bool = False,
) -> None:
    """The Owner approval composition in call order."""
    _script_repeating_reads(conn)
    waiting = (
        task_row
        if task_row is not None
        else _task_row(
            status="waiting_for_owner", gate_id=_PR_GATE_ID, canonical_branch=_CLAIMED_BRANCH
        )
    )
    gate = gate_row if gate_row is not None else _pr_gate_row()
    conn.on(_TASK_READ, waiting)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, waiting)  # get_task_for_update
    conn.on(_TASK_READ, waiting)  # require_current_task
    conn.on(_GATE_READ, gate)  # require_current_owner_gate
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, [_gate_row()])
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_ITERATIONS_LIST, [_iteration_row()])
    conn.on(_EXECUTIONS_LIST, executions if executions is not None else [_execution_row()])
    conn.on(_GATE_READ, gate)  # the resolution's gate read
    locked_current = (
        _task_row(status="waiting_for_owner", gate_id=None, canonical_branch=_CLAIMED_BRANCH)
        if stale_current_gate
        else waiting
    )
    conn.on(_TASK_READ_LOCKED, locked_current)  # the resolution's Task row lock
    if fail_resolution:
        conn.on(_GATE_UPDATE, RuntimeError("forced late resolution failure"))
    else:
        conn.on(_GATE_UPDATE, _pr_gate_row(status="approved"))
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(
            status="waiting_for_owner",
            state_token=_TOKEN_2,
            gate_id=None,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )


def _script_cancel(
    conn: ScriptedConnection,
    *,
    fail_archive: bool = False,
) -> None:
    """The Owner cancellation composition in call order."""
    _script_repeating_reads(conn)
    waiting = _task_row(
        status="waiting_for_owner", gate_id=_PR_GATE_ID, canonical_branch=_CLAIMED_BRANCH
    )
    conn.on(_TASK_READ, waiting)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, waiting)  # get_task_for_update
    conn.on(_TASK_READ, waiting)  # require_current_task
    conn.on(_GATE_READ, _pr_gate_row())  # require_current_owner_gate
    conn.on(_GATE_READ, _pr_gate_row())  # the resolution's gate read
    conn.on(_TASK_READ_LOCKED, waiting)  # the resolution's Task row lock
    conn.on(_GATE_UPDATE, _pr_gate_row(status="cancelled"))
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(
            status="waiting_for_owner",
            state_token=_TOKEN_2,
            gate_id=None,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )
    if fail_archive:
        conn.on(_ARCHIVE_WRITE, RuntimeError("forced late cancellation failure"))
    else:
        conn.on(
            _ARCHIVE_WRITE,
            _task_row(
                status="cancelled",
                state_token=_TOKEN_3,
                gate_id=None,
                canonical_branch=_CLAIMED_BRANCH,
                archived=True,
            ),
        )


# --- service call helpers -----------------------------------------------------------


def _fake_github(pool: FakePool, **kwargs: Any) -> FakeGitHubAppClient:
    defaults: dict[str, Any] = {
        "repository_observation": _repository_observation(),
        "issue_observation": _issue_observation(),
        "branch_observation": _branch_observation(),
    }
    defaults.update(kwargs)
    return FakeGitHubAppClient(pool=pool, **defaults)


def _open_call(
    pool: DatabasePool,
    github: Any,
    *,
    expected_state_token: Any = _STATE_TOKEN,
    execution_id: Any = None,
    workspace_id: Any = _WORKSPACE_ID,
) -> Any:
    return open_pr_authorization(
        pool,
        github,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=workspace_id,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        execution_id=execution_id,
    )


def _approve_call(pool: DatabasePool, *, expected_state_token: Any = _STATE_TOKEN) -> Any:
    return approve_pr_authorization(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_PR_GATE_ID,
        expected_state_token=expected_state_token,
    )


def _cancel_call(pool: DatabasePool, *, expected_state_token: Any = _STATE_TOKEN) -> Any:
    return cancel_pr_authorization(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_PR_GATE_ID,
        expected_state_token=expected_state_token,
    )


# --- command classification ---------------------------------------------------------


def test_malformed_commands_are_classified_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))

    with pytest.raises(InvalidCommandError):
        _open_call(pool, github, expected_state_token="not-a-uuid")
    with pytest.raises(InvalidCommandError):
        open_pr_authorization(
            pool,
            github,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            execution_id="not-a-uuid",  # type: ignore[arg-type]
        )
    with pytest.raises(InvalidCommandError):
        approve_pr_authorization(
            pool,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id="not-a-uuid",  # type: ignore[arg-type]
            expected_state_token=_STATE_TOKEN,
        )
    with pytest.raises(InvalidCommandError):
        cancel_pr_authorization(
            pool,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_PR_GATE_ID,
            expected_state_token="not-a-uuid",  # type: ignore[arg-type]
        )

    assert conn.executed == []
    assert github.repository_calls == [] and github.issue_calls == []
    assert github.branch_calls == []


# --- the exact-head gate presentation -----------------------------------------------


def test_a_successful_presentation_binds_and_presents_the_exact_head_gate() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn)
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))

    outcome = _open_call(pool, github)

    assert outcome.task.status is TaskStatus.WAITING_FOR_OWNER
    assert outcome.task.state_token == _TOKEN_4
    assert outcome.task.current_owner_gate_id == _PR_GATE_ID
    assert outcome.owner_gate.status is OwnerGateStatus.PENDING
    assert outcome.owner_gate.gate_type is OwnerGateType.PR_AUTHORIZATION
    assert outcome.owner_gate.plan_revision_id is None
    assert outcome.owner_gate.task_pull_request_id is None
    assert outcome.owner_gate.subject_head_sha == _HEAD_SHA
    assert outcome.canonical_branch == _CLAIMED_BRANCH
    assert outcome.verified_head_sha == _HEAD_SHA
    # GitHub was addressed by stable identity only, twice (B3 + the B7 seam).
    assert github.repository_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "github_repository_id": _GITHUB_REPOSITORY_ID,
        },
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "github_repository_id": _GITHUB_REPOSITORY_ID,
        },
    ]
    assert github.branch_calls == [
        {
            "github_installation_id": _EXTERNAL_INSTALLATION_ID,
            "owner_login": "octocat",
            "repository_name": "hello-world",
            "branch_name": _CLAIMED_BRANCH,
        }
    ]
    # One bind, one new exact-head gate row, one install, one transition.
    assert _count(conn, _BRANCH_BIND) == 1
    (bind_params,) = [params for sql, params in conn.executed if _BRANCH_BIND(sql, None)]
    assert bind_params is not None
    assert bind_params[0] == _CLAIMED_BRANCH
    assert _count(conn, _GATES_INSERT) == 1
    (gate_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert gate_params is not None
    assert gate_params[2] == "pr_authorization"
    assert gate_params[3] == "pending"
    assert gate_params[4] is None and gate_params[5] == _HEAD_SHA and gate_params[6] is None
    assert _count(conn, _TASK_GATE_WRITE) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _count(conn, _BLOCK_INSERT) == 0
    # Exactly the safe OPENORC audit fact; no HEAD_VERIFIED invention.
    assert _event_types(conn) == ["owner_gate_created"]
    (gate_event,) = _event_params_of_type(conn, "owner_gate_created")
    assert gate_event[3] == "openorc" and gate_event[4] is None
    assert gate_event[5] == "owner_gate" and gate_event[6] == _PR_GATE_ID


def _assert_refused_before_any_effect(
    conn: ScriptedConnection, pool: DatabasePool, github: FakeGitHubAppClient, excinfo: Any
) -> None:
    assert not _write_predicates_fired(conn)
    assert github.issue_calls == [] and github.branch_calls == []
    assert excinfo


def test_a_wrong_pre_gate_status_refuses_before_any_effect() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, task_row=_task_row(status="planning"))
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_a_stale_state_token_refuses_before_any_effect() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, task_row=_task_row(state_token=_TOKEN_2))
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(StaleOperationError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_an_existing_current_gate_refuses_the_presentation() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, task_row=_task_row(gate_id=_PR_GATE_ID))
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_an_existing_canonical_pull_request_record_refuses_the_presentation() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn)
    # Re-script the two PR reads to return an existing canonical record.
    conn._handlers = [
        (predicate, result, once)
        for predicate, result, once in conn._handlers
        if not (predicate is _PR_READ and result is None)
    ]
    conn.on_repeat(_PR_READ, _pull_request_row())  # any row proves existence
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_a_missing_implementation_attempt_refuses_the_presentation() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, executions=[])
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_an_unresolved_dispatch_is_never_a_qualifying_success() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, executions=[_execution_row(status="running", result=None)])
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_a_foreign_named_execution_refuses_the_presentation() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn)
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github, execution_id=uuid.uuid4())
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


def test_a_foreign_producer_session_reference_refuses_the_presentation() -> None:
    conn = ScriptedConnection()
    _script_open_success(
        conn,
        executions=[_execution_row(producer_session_id=uuid.uuid4())],
    )
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


@pytest.mark.parametrize(
    "payload",
    [
        {**_RESULT_PAYLOAD, "type": "review_result"},
        {**_RESULT_PAYLOAD, "schema_version": 2},
        {**_RESULT_PAYLOAD, "status": "RUNNING"},
        {**_RESULT_PAYLOAD, "branch": "   "},
        {"type": "implementation_result"},
    ],
)
def test_invalid_persisted_result_shapes_fail_closed_before_github(
    payload: dict[str, object],
) -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, executions=[_execution_row(result=payload)])
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


# --- authoritative source/branch classification -------------------------------------


@pytest.mark.parametrize(
    ("issue_error", "expected"),
    [
        (GitHubAuthorizationRejectedError("lost access"), AuthorizationError),
        (GitHubRateLimitedError("rate limited"), ExternalOperationFailedError),
        (GitHubAuthenticationRejectedError("rejected"), ExternalOperationFailedError),
        (GitHubRequestRejectedError("rejected"), ExternalOperationFailedError),
        (GitHubOutcomeUncertainError("unknown"), ExternalOperationUncertainError),
    ],
)
def test_source_integration_failures_refuse_without_block_or_gate(
    issue_error: Exception, expected: type[Exception]
) -> None:
    conn = ScriptedConnection()
    _script_open_success(conn)
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool), issue_error=issue_error)
    with pytest.raises(expected) as excinfo:
        _open_call(pool, github)
    assert not _write_predicates_fired(conn)
    assert github.branch_calls == []
    assert excinfo


def test_a_closed_source_issue_refuses_without_fabricating_drift() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, issue_row=_issue_row(state="closed"))
    pool = _pool(conn)
    github = _fake_github(
        cast(FakePool, pool), issue_observation=_issue_observation(state="closed")
    )
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    # No drift was fabricated: the fingerprint never changed, so no block,
    # no status transition, no gate, and no issue rewrite.
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _ISSUE_UPDATE) == 0
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _BRANCH_BIND) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []
    assert github.branch_calls == []
    assert excinfo


def test_a_stable_identity_mismatch_refuses_without_drift() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, issue_row=_issue_row(github_issue_id=999))
    pool = _pool(conn)
    github = _fake_github(
        cast(FakePool, pool), issue_observation=_issue_observation(github_issue_id=999)
    )
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []
    assert github.branch_calls == []
    assert excinfo


def test_confirmed_drift_blocks_atomically_and_creates_no_gate() -> None:
    conn = ScriptedConnection()
    _script_drift_block(conn)
    pool = _pool(conn)
    github = _fake_github(
        cast(FakePool, pool),
        issue_observation=_issue_observation(title="Found a bug v2"),
    )
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    # The atomic drift-block composition: one TaskBlock, one BLOCKED
    # transition, one audit event — and no PR gate, no branch bind.
    assert _count(conn, _BLOCK_INSERT) == 1
    (block_params,) = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    assert block_params is not None
    assert block_params[2] == "github_source_changed"
    # The create path wraps the canonical context in the explicit Jsonb adapter.
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context == {
        "github_issue_id": _GITHUB_ISSUE_ID,
        "expected_fingerprint": _FINGERPRINT,
        "observed_fingerprint": _DRIFTED_FINGERPRINT,
    }
    assert _count(conn, _STATUS_UPDATE) == 1
    (status_params,) = [params for sql, params in conn.executed if _STATUS_UPDATE(sql, None)]
    assert status_params is not None
    assert status_params[0] == "blocked"
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _BRANCH_BIND) == 0
    assert _event_types(conn) == ["task_blocked"]
    (block_event,) = _event_params_of_type(conn, "task_blocked")
    assert block_event[3] == "openorc" and block_event[4] is None
    assert block_event[5] == "task_block" and block_event[6]
    assert excinfo


def test_a_forced_late_drift_failure_aborts_the_block_composition() -> None:
    conn = ScriptedConnection()
    _script_drift_block(conn, fail_status=True)
    pool = _pool(conn)
    github = _fake_github(
        cast(FakePool, pool),
        issue_observation=_issue_observation(title="Found a bug v2"),
    )
    with pytest.raises(RuntimeError):
        _open_call(pool, github)
    # The failed transition aborts the composition before the audit event:
    # nothing after the failure point was written (the real-Postgres
    # integration suite proves the full rollback).
    assert _event_types(conn) == []
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _BRANCH_BIND) == 0


def test_a_blocked_task_refuses_any_later_presentation() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, task_row=_task_row(status="blocked", state_token=_TOKEN_2))
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github, expected_state_token=_TOKEN_2)
    _assert_refused_before_any_effect(conn, pool, github, excinfo.value)


@pytest.mark.parametrize(
    ("branch_error", "expected"),
    [
        (GitHubAuthorizationRejectedError("missing branch"), AuthorizationError),
        (GitHubRateLimitedError("rate limited"), ExternalOperationFailedError),
        (GitHubOutcomeUncertainError("unknown"), ExternalOperationUncertainError),
    ],
)
def test_branch_verification_failures_refuse_without_gate_or_bind(
    branch_error: Exception, expected: type[Exception]
) -> None:
    conn = ScriptedConnection()
    _script_open_success(conn)
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool), branch_error=branch_error)
    with pytest.raises(expected):
        _open_call(pool, github)
    # The source reconciled (B3 ran) but nothing durable was changed.
    assert not _write_predicates_fired(conn)
    assert _event_types(conn) == []


def test_a_different_branch_claim_against_a_bound_task_is_impossible() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, task_row=_task_row(canonical_branch="other/task-42-branch"))
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github)
    # Refused before any GitHub read: the bind-once canonical branch stands.
    assert github.issue_calls == [] and github.branch_calls == []
    assert not _write_predicates_fired(conn)
    assert excinfo


def test_a_bound_branch_continuation_reobserves_and_presents_without_rebinding() -> None:
    conn = ScriptedConnection()
    _script_open_success(conn, task_row=_task_row(canonical_branch=_CLAIMED_BRANCH))
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    outcome = _open_call(pool, github)
    assert outcome.canonical_branch == _CLAIMED_BRANCH
    assert outcome.verified_head_sha == _HEAD_SHA
    # The same-branch continuation skipped the one-time bind entirely.
    assert _count(conn, _BRANCH_BIND) == 0
    assert _count(conn, _GATES_INSERT) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    # The branch read was addressed by the bound canonical name.
    assert github.branch_calls[0]["branch_name"] == _CLAIMED_BRANCH


@pytest.mark.parametrize("failure", ["fail_branch_bind", "fail_gate_install", "fail_status"])
def test_forced_late_presentation_failures_abort_before_the_audit(failure: str) -> None:
    conn = ScriptedConnection()
    if failure == "fail_branch_bind":
        _script_open_success(conn, fail_branch_bind=True)
    elif failure == "fail_gate_install":
        _script_open_success(conn, fail_gate_install=True)
    else:
        _script_open_success(conn, fail_status=True)
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(RuntimeError):
        _open_call(pool, github)
    # The composition aborted at the forced failure; nothing after it
    # committed (the real-Postgres suite proves the full rollback).
    assert _event_types(conn) == []
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _count(conn, _BLOCK_INSERT) == 0


def test_a_replayed_presentation_cannot_create_a_second_gate() -> None:
    conn = ScriptedConnection()
    # Post-commit durable state: the Task is already waiting on the gate.
    _script_open_success(
        conn,
        task_row=_task_row(
            status="waiting_for_owner",
            state_token=_TOKEN_4,
            gate_id=_PR_GATE_ID,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool))
    with pytest.raises(ConflictError) as excinfo:
        _open_call(pool, github, expected_state_token=_TOKEN_4)
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _BRANCH_BIND) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert excinfo


# --- Owner authority: exactly Approve / Cancel ---------------------------------------


def test_the_correct_profile_approves_the_exact_gate_with_owner_attribution() -> None:
    conn = ScriptedConnection()
    _script_approve(conn)
    pool = _pool(conn)

    outcome = _approve_call(pool)

    assert outcome.owner_gate.status is OwnerGateStatus.APPROVED
    assert outcome.owner_gate.gate_type is OwnerGateType.PR_AUTHORIZATION
    assert outcome.owner_gate.subject_head_sha == _HEAD_SHA
    assert outcome.owner_gate.decided_at is not None
    # The Task stays waiting_for_owner: no REVIEWING transition, no archival.
    assert outcome.task.status is TaskStatus.WAITING_FOR_OWNER
    assert outcome.task.state_token == _TOKEN_2
    assert outcome.task.current_owner_gate_id is None
    assert outcome.task.canonical_feature_branch == _CLAIMED_BRANCH
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _count(conn, _GATES_INSERT) == 0
    (resolved_params,) = [params for sql, params in conn.executed if _GATE_UPDATE(sql, None)]
    assert resolved_params is not None
    assert resolved_params[0] == "approved"
    assert _event_types(conn) == ["owner_gate_resolved"]
    (resolved_event,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert resolved_event[3] == "owner"
    assert resolved_event[4] == str(_OWNER_PROFILE_ID)
    assert resolved_event[5] == "owner_gate" and resolved_event[6] == _PR_GATE_ID
    assert _event_context(resolved_event) == {"outcome": "approved"}


def test_a_foreign_profile_learns_nothing_and_applies_nothing() -> None:
    conn = ScriptedConnection()
    _script_approve(conn)
    pool = _pool(conn)
    with pytest.raises(NotFoundError):
        approve_pr_authorization(
            pool,
            profile_id=_OTHER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_PR_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_stale_token_approves_nothing() -> None:
    conn = ScriptedConnection()
    _script_approve(
        conn,
        task_row=_task_row(
            status="waiting_for_owner",
            state_token=_TOKEN_3,
            gate_id=_PR_GATE_ID,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )
    pool = _pool(conn)
    with pytest.raises(StaleOperationError):
        _approve_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_only_the_current_pr_authorization_gate_approves_through_this_seam() -> None:
    conn = ScriptedConnection()
    _script_approve(conn, gate_row=_gate_row())  # implementation gate, not PR gate
    pool = _pool(conn)
    with pytest.raises(ConflictError):
        _approve_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_an_unbound_canonical_branch_refuses_the_approval() -> None:
    conn = ScriptedConnection()
    _script_approve(
        conn,
        task_row=_task_row(status="waiting_for_owner", gate_id=_PR_GATE_ID, canonical_branch=None),
    )
    pool = _pool(conn)
    with pytest.raises(ConflictError):
        _approve_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_branch_claim_mismatching_the_bound_branch_refuses_the_approval() -> None:
    conn = ScriptedConnection()
    _script_approve(
        conn,
        executions=[_execution_row(result={**_RESULT_PAYLOAD, "branch": "other/branch"})],
    )
    pool = _pool(conn)
    with pytest.raises(ConflictError):
        _approve_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_replayed_approval_is_a_one_shot_no_op() -> None:
    conn = ScriptedConnection()
    _script_approve(
        conn,
        gate_row=_pr_gate_row(status="approved"),
    )
    pool = _pool(conn)
    with pytest.raises(StaleOperationError):
        _approve_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_stale_replaced_current_gate_approves_nothing() -> None:
    conn = ScriptedConnection()
    _script_approve(conn, stale_current_gate=True)
    pool = _pool(conn)
    with pytest.raises(StaleOperationError):
        _approve_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_cancel_archives_the_task_terminal_cancelled() -> None:
    conn = ScriptedConnection()
    _script_cancel(conn)
    pool = _pool(conn)

    outcome = _cancel_call(pool)

    assert outcome.owner_gate.status is OwnerGateStatus.CANCELLED
    assert outcome.owner_gate.gate_type is OwnerGateType.PR_AUTHORIZATION
    assert outcome.task.status is TaskStatus.CANCELLED
    assert outcome.task.archived_at is not None
    assert _count(conn, _ARCHIVE_WRITE) == 1
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _BRANCH_BIND) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == ["owner_gate_resolved", "task_cancelled"]
    (resolved_event,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert resolved_event[3] == "owner" and resolved_event[4] == str(_OWNER_PROFILE_ID)
    assert _event_context(resolved_event) == {"outcome": "cancelled"}
    (terminal_event,) = _event_params_of_type(conn, "task_cancelled")
    assert terminal_event[3] == "owner" and terminal_event[4] == str(_OWNER_PROFILE_ID)


def test_a_forced_late_cancellation_failure_aborts_the_composition() -> None:
    conn = ScriptedConnection()
    _script_cancel(conn, fail_archive=True)
    pool = _pool(conn)
    with pytest.raises(RuntimeError):
        _cancel_call(pool)
    # The terminal event never committed after the failed archival.
    assert _event_params_of_type(conn, "task_cancelled") == []
    assert _count(conn, _BRANCH_BIND) == 0
    assert _count(conn, _GATES_INSERT) == 0


def test_only_the_current_pr_authorization_gate_cancels_through_this_seam() -> None:
    conn = ScriptedConnection()
    _script_cancel(conn)
    # The current gate is the implementation gate, not the PR gate.
    conn._handlers = [
        (predicate, result, once)
        for predicate, result, once in conn._handlers
        if predicate is not _GATE_READ
    ]
    conn.on_repeat(_GATE_READ, _gate_row(status="pending"))
    pool = _pool(conn)
    with pytest.raises(ConflictError):
        _cancel_call(pool)
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []
