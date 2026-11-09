"""Deterministic tests for the Owner Ignore action (issue #289, F8d).

A scripted SQL-connection seam proves the ONE Owner-authorized Ignore
command for an exactly-evidenced GitHub requirements change:

- both interrupted boundaries resume to their existing approval gate in the
  SAME commit: the E5 boundary (ordinary E3b ``planning`` shape and mediated
  E3c settled shape) reaches the existing Approve Plan gate, and the E7
  boundary reaches the existing Authorize PR gate with its one-time
  canonical-branch bind — no unblocked-but-unpresented state ever exists;
- the acknowledged fingerprint overwrites the Task's single stored value
  with exactly the block's observed digest, the exceptional gate resolves
  APPROVED with truthful OWNER provenance, and the resolved TaskBlock
  retains its previous/observed evidence untouched;
- the fresh exactness verification refuses a stale observation — the GitHub
  issue's current fingerprint must be exactly the block's observed value —
  without any write, leaving the Task blocked at its pending gate;
- unsafe or unverified conditions (stale token, wrong gate type,
  unclassifiable or uneligible interrupted boundary, durable drift moved on
  at commit) fail closed with zero writes;
- the E7 continuation additionally proves the branch-head observation
  through the shared B7 seam before the commit.

No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.github import (
    GitHubBranchObservation,
    GitHubIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.github_issues import github_issue_requirements_fingerprint
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, StaleOperationError
from openorc.services.task_source_change_ignore import (
    IgnoredSourceChangeBoundary,
    ignore_github_source_change,
)

_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_TOKEN_3 = uuid.uuid4()
_TOKEN_4 = uuid.uuid4()
_TOKEN_5 = uuid.uuid4()
_TOKEN_6 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_SETTLED_GATE_ID = uuid.uuid4()
_E5_GATE_ID = uuid.uuid4()
_PR_GATE_ID = uuid.uuid4()
_DRIFT_GATE_ID = uuid.uuid4()
_DRIFT_BLOCK_ID = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_EXECUTION_1_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_CLAIMED_BRANCH = "openorc/task-42-implementation"
_OTHER_BRANCH = "other/task-42-branch"
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_ISSUE_ID = 503
_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Requirements body")
_ACKNOWLEDGED_FINGERPRINT = github_issue_requirements_fingerprint(
    "Found a bug v2", "Requirements body"
)
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_RESULT_PAYLOAD: dict[str, object] = {
    "type": "implementation_result",
    "schema_version": 1,
    "status": "COMPLETED",
    "branch": _CLAIMED_BRANCH,
    "summary": "Implemented the retry loop",
    "changes": ["Added the retry loop"],
    "validation": ["pytest tests/test_retry.py"],
}

_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception
_Predicate = Callable[[str, tuple[Any, ...] | None], bool]


def _sql_has(*tokens: str) -> _Predicate:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


def _sql_lacks(predicate: _Predicate, *tokens: str) -> _Predicate:
    def wrapped(sql: str, params: tuple[Any, ...] | None) -> bool:
        return predicate(sql, params) and not any(token in sql for token in tokens)

    return wrapped


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler."""

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
        raise AssertionError("ignore action tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ignore_call(
    pool: DatabasePool,
    github: Any,
    *,
    expected_state_token: Any = _STATE_TOKEN,
    owner_gate_id: Any = _DRIFT_GATE_ID,
) -> Any:
    return ignore_github_source_change(
        pool,
        github,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=owner_gate_id,
        expected_state_token=expected_state_token,
    )


_ACCOUNT_BARRIER = _sql_has("from openorc.profiles", "for key share")
_WS_READ = _sql_has("from openorc.workspaces where id")
_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks", "current_owner_gate_id")
_ACKNOWLEDGE_WRITE = _sql_has("update openorc.tasks set source_requirements_fingerprint")
_STATUS_UPDATE = _sql_has("update openorc.tasks set status", "queued_at = case")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_UPDATE = _sql_has("update openorc.owner_gates set")
_GATES_INSERT = _sql_has("insert into openorc.owner_gates")
_BLOCK_READ = _sql_has("from openorc.task_blocks where id")
_BLOCK_UPDATE = _sql_has("update openorc.task_blocks set")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_LOOP_READ_LOCKED = _sql_lacks(
    _sql_has("from openorc.review_loops where id", "for update"), "select status, purpose"
)
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")
_PR_READ = _sql_has("from openorc.task_pull_requests where task_id")
_BRANCH_BIND = _sql_has("update openorc.tasks set canonical_feature_branch")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_REPO_READ = _sql_lacks(_sql_has("from openorc.repositories"), "for update")
_REPO_READ_LOCKED = _sql_has("from openorc.repositories", "for update")
_INSTALLATION_READ = _sql_has("from openorc.github_installations")
_ISSUE_READ_LOCKED = _sql_has("from openorc.github_issues", "for update")
_ISSUE_FIND = _sql_lacks(_sql_has("from openorc.github_issues"), "for update")
_ISSUE_UPDATE = _sql_has("update openorc.github_issues set")

_INSTALLATION_READ_ID = uuid.uuid4()


def _ws_row() -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, 2, "", None)


def _task_row(
    *,
    status: str = "blocked",
    state_token: Any = _STATE_TOKEN,
    gate_id: Any = _DRIFT_GATE_ID,
    fingerprint: str = _FINGERPRINT,
    canonical_branch: Any = None,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        42,
        status,
        None,
        canonical_branch,
        state_token,
        _REVISION_1_ID,
        gate_id,
        fingerprint,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
        None,
    )


def _revision_row() -> tuple[Any, ...]:
    return (_REVISION_1_ID, _WORKSPACE_ID, _TASK_ID, 1, _PLAN_TEXT, _BASE_SHA, _OBSERVED)


def _loop_row(*, status: str = "open") -> tuple[Any, ...]:
    closed_at = _OBSERVED if status == "closed" else None
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, "planning", 2, status, closed_at, _OBSERVED)


def _iteration_row() -> tuple[Any, ...]:
    return (
        _ITERATION_1_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _LOOP_ID,
        1,
        _REVISION_1_ID,
        None,
        None,
        "accepted",
        "Looks good",
        [],
        _OBSERVED,
        _OBSERVED,
    )


def _gate_row(
    *,
    gate_id: Any = _DRIFT_GATE_ID,
    gate_type: str = "github_issue_change",
    status: str = "pending",
) -> tuple[Any, ...]:
    row = [
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        None,
        None,
        None,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    ]
    if gate_type == "github_issue_change":
        row[8] = _DRIFT_BLOCK_ID  # the gate's TaskBlock subject binding
    return tuple(row)


def _block_row(*, resolved: bool = False) -> tuple[Any, ...]:
    return (
        _DRIFT_BLOCK_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "github_source_changed",
        {
            "github_issue_id": _GITHUB_ISSUE_ID,
            "expected_fingerprint": _FINGERPRINT,
            "observed_fingerprint": _ACKNOWLEDGED_FINGERPRINT,
        },
        _OBSERVED if resolved else None,
        _OBSERVED,
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
        None,
        None,
    )


def _execution_row() -> tuple[Any, ...]:
    return (
        _EXECUTION_1_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        1,
        "succeeded",
        _OBSERVED,
        _OBSERVED,
        _RESULT_PAYLOAD,
        None,
        None,
    )


def _pr_gate_row() -> tuple[Any, ...]:
    row = list(_gate_row(gate_id=_PR_GATE_ID, gate_type="pr_authorization", status="pending"))
    row[6] = _HEAD_SHA  # the exact-head subject
    return tuple(row)


def _e5_gate_row() -> tuple[Any, ...]:
    row = list(
        _gate_row(gate_id=_E5_GATE_ID, gate_type="implementation_authorization", status="pending")
    )
    row[5] = _REVISION_1_ID  # the exact-plan subject
    return tuple(row)


def _settled_gate_row() -> tuple[Any, ...]:
    row = list(_gate_row(gate_id=_SETTLED_GATE_ID, gate_type="review_resolution", status="settled"))
    row[5] = _REVISION_1_ID  # the plan-subject settled acceptance
    return tuple(row)


def _approved_e5_gate_row() -> tuple[Any, ...]:
    row = list(
        _gate_row(gate_id=_E5_GATE_ID, gate_type="implementation_authorization", status="approved")
    )
    row[5] = _REVISION_1_ID  # the exact-plan subject
    return tuple(row)


def _event_row(event_type: str = "owner_gate_resolved") -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type,
        "owner",
        str(_OWNER_PROFILE_ID),
        "owner_gate",
        _DRIFT_GATE_ID,
        {},
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
        _INSTALLATION_READ_ID,
    )


def _installation_row() -> tuple[Any, ...]:
    return (
        _INSTALLATION_READ_ID,
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
    fingerprint: str = _ACKNOWLEDGED_FINGERPRINT,
    title: str = "Found a bug v2",
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        42,
        title,
        "Requirements body",
        "open",
        fingerprint,
        _OBSERVED,
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


def _issue_observation() -> GitHubIssueObservation:
    return GitHubIssueObservation(
        github_issue_id=_GITHUB_ISSUE_ID,
        issue_number=42,
        title="Found a bug v2",
        body="Requirements body",
        state="open",
        provider_updated_at=_OBSERVED,
        is_pull_request=False,
    )


def _baseline_issue_observation() -> GitHubIssueObservation:
    """The pre-drift title/body: the observation reconciles to the baseline
    fingerprint, not the block's acknowledged observed value."""
    return GitHubIssueObservation(
        github_issue_id=_GITHUB_ISSUE_ID,
        issue_number=42,
        title="Found a bug",
        body="Requirements body",
        state="open",
        provider_updated_at=_OBSERVED,
        is_pull_request=False,
    )


def _branch_observation() -> GitHubBranchObservation:
    return GitHubBranchObservation(branch_name=_CLAIMED_BRANCH, head_sha=_HEAD_SHA)


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self, *, pool: Any, issue_observation: GitHubIssueObservation | None = None
    ) -> None:
        self._pool = pool
        self._issue_observation = (
            issue_observation if issue_observation is not None else _issue_observation()
        )
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
        return _repository_observation()

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
        self.issue_calls.append({"issue_number": issue_number})
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
        self.branch_calls.append({"branch_name": branch_name})
        return _branch_observation()


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


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    conn.on_repeat(_ACCOUNT_BARRIER, (None, None, None))
    conn.on_repeat(_WS_READ, _ws_row())
    conn.on_repeat(_EVENT_INSERT, _event_row())
    # The Producer binding is re-proven by both the authority derivation and
    # the qualifying-execution derivation, several times per composition; the
    # binding identity never changes across the flow.
    conn.on_repeat(_SESSION_READ, _session_row())


def _script_verify_reads_e5(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    gates: list[tuple[Any, ...]] | None = None,
) -> None:
    """The Phase-1 verification transaction, E5-boundary shape, in call order."""
    _script_repeating_reads(conn)
    blocked = task_row if task_row is not None else _task_row()
    conn.on(_TASK_READ, blocked)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, blocked)  # get_task_for_update
    conn.on(_TASK_READ, blocked)  # require_current_task
    conn.on(_GATE_READ, _gate_row())  # require_current_owner_gate
    conn.on(_BLOCK_READ, _block_row())  # the gate's unresolved block subject
    conn.on(_LOOPS_LIST, [_loop_row()] if loops is None else loops)  # classification
    conn.on(_REVISION_READ, _revision_row())  # the continuation's revision
    conn.on(_LOOPS_LIST, [_loop_row()])  # the continuation's open-loop re-read
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, [_iteration_row()])
    conn.on(_GATES_LIST, gates if gates is not None else [_gate_row()])


def _script_verify_reads_e7(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gates: list[tuple[Any, ...]] | None = None,
) -> None:
    """The Phase-1 verification transaction, E7-boundary shape, in call order."""
    _script_repeating_reads(conn)
    blocked = task_row if task_row is not None else _task_row()
    conn.on(_TASK_READ, blocked)
    conn.on(_TASK_READ_LOCKED, blocked)
    conn.on(_TASK_READ, blocked)
    conn.on(_GATE_READ, _gate_row())
    conn.on(_BLOCK_READ, _block_row())
    conn.on(_LOOPS_LIST, [_loop_row(status="closed")])  # classification
    conn.on(_REVISION_READ, _revision_row())  # the continuation's revision
    conn.on(
        _GATES_LIST,
        gates if gates is not None else [_gate_row(), _approved_e5_gate_row()],
    )
    conn.on(_LOOPS_LIST, [_loop_row(status="closed")])  # the closed planning loop
    conn.on(_ITERATIONS_LIST, [_iteration_row()])
    conn.on(_EXECUTIONS_LIST, [_execution_row()])  # the qualifying completion


def _script_b3_reconciliation(conn: ScriptedConnection) -> None:
    """The fresh B3 exactness read (the projection already carries the observed
    fingerprint, so the reconcile performs no update)."""
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())
    conn.on(_REPO_READ_LOCKED, _repository_row())
    conn.on(_ISSUE_READ_LOCKED, _issue_row())


def _script_branch_observation(conn: ScriptedConnection) -> None:
    """The shared B7 read seam's route resolution (the reads are external)."""
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())


def _script_resolution_effects(conn: ScriptedConnection, *, restored_row: tuple[Any, ...]) -> None:
    """The resolution, acknowledge, and audit effects of the Phase-3 commit."""
    blocked = _task_row()
    cleared = _task_row(state_token=_TOKEN_2, gate_id=None)
    conn.on(_TASK_READ_LOCKED, blocked)  # get_task_for_update
    conn.on(_TASK_READ, blocked)  # require_current_task
    conn.on(_GATE_READ, _gate_row())  # require_current_owner_gate
    conn.on(_BLOCK_READ, _block_row())
    conn.on(_ISSUE_FIND, _issue_row())  # the durable exactness re-proof
    conn.on(_GATE_READ, _gate_row())  # the resolution's gate read
    conn.on(_TASK_READ_LOCKED, blocked)  # the resolution's Task row lock
    conn.on(_GATE_UPDATE, _gate_row(status="approved"))
    conn.on(_TASK_GATE_WRITE, cleared)  # the pointer clears under a rotated token
    conn.on(_BLOCK_UPDATE, _block_row(resolved=True))
    conn.on(_ACKNOWLEDGE_WRITE, restored_row)


def _script_apply_reads_e5(
    conn: ScriptedConnection,
    *,
    restored_status: str = "planning",
    status_write: bool = True,
) -> None:
    """The Phase-3 commit through the nested E5 presentation, in call order."""
    restored = _task_row(
        status=restored_status,
        state_token=_TOKEN_3,
        gate_id=None,
        fingerprint=_ACKNOWLEDGED_FINGERPRINT,
    )
    _script_resolution_effects(conn, restored_row=restored)
    # --- the nested E5 presentation (SAVEPOINT-composed). ---
    conn.on(_TASK_READ, restored)
    conn.on(_TASK_READ_LOCKED, restored)
    conn.on(_TASK_READ, restored)
    conn.on(_ISSUE_FIND, _issue_row())  # the durable re-proof vs the new baseline
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, [_iteration_row()])
    conn.on(_GATES_INSERT, _e5_gate_row())
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(
            status=restored_status,
            state_token=_TOKEN_4,
            gate_id=_E5_GATE_ID,
            fingerprint=_ACKNOWLEDGED_FINGERPRINT,
        ),
    )
    if status_write:
        conn.on(
            _STATUS_UPDATE,
            _task_row(
                status="waiting_for_owner",
                state_token=_TOKEN_5,
                gate_id=_E5_GATE_ID,
                fingerprint=_ACKNOWLEDGED_FINGERPRINT,
            ),
        )


def _script_apply_reads_e7(conn: ScriptedConnection) -> None:
    """The Phase-3 commit through the nested E7 presentation, in call order."""
    restored = _task_row(
        status="implementing",
        state_token=_TOKEN_3,
        gate_id=None,
        fingerprint=_ACKNOWLEDGED_FINGERPRINT,
    )
    _script_resolution_effects(conn, restored_row=restored)
    # --- the nested E7 presentation (SAVEPOINT-composed): the claim is
    # derived fresh, then the presentation body re-proves every fact. ---
    for _phase in ("claim", "body"):
        conn.on(_TASK_READ, restored)
        conn.on(_TASK_READ_LOCKED, restored)
        conn.on(_TASK_READ, restored)
        conn.on(_PR_READ, None)  # no canonical PR record exists
        conn.on(_REVISION_READ, _revision_row())
        conn.on(_GATES_LIST, [_gate_row(), _approved_e5_gate_row()])
        conn.on(_LOOPS_LIST, [_loop_row(status="closed")])
        conn.on(_ITERATIONS_LIST, [_iteration_row()])
        conn.on(_EXECUTIONS_LIST, [_execution_row()])
    conn.on(_ISSUE_FIND, _issue_row())  # the durable re-proof vs the new baseline
    conn.on(
        _BRANCH_BIND,
        _task_row(
            status="implementing",
            state_token=_TOKEN_4,
            gate_id=None,
            fingerprint=_ACKNOWLEDGED_FINGERPRINT,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )
    conn.on(_GATES_INSERT, _pr_gate_row())
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(
            status="implementing",
            state_token=_TOKEN_5,
            gate_id=_PR_GATE_ID,
            fingerprint=_ACKNOWLEDGED_FINGERPRINT,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )
    conn.on(
        _STATUS_UPDATE,
        _task_row(
            status="waiting_for_owner",
            state_token=_TOKEN_6,
            gate_id=_PR_GATE_ID,
            fingerprint=_ACKNOWLEDGED_FINGERPRINT,
            canonical_branch=_CLAIMED_BRANCH,
        ),
    )


def _script_ignore_success_e5(conn: ScriptedConnection, *, settled: bool = False) -> None:
    """The full three-phase E5-boundary Ignore composition, in call order."""
    _script_verify_reads_e5(
        conn,
        gates=[_gate_row(), _settled_gate_row()] if settled else None,
    )
    _script_b3_reconciliation(conn)
    _script_apply_reads_e5(
        conn,
        restored_status="waiting_for_owner" if settled else "planning",
        status_write=not settled,
    )


def _script_ignore_success_e7(conn: ScriptedConnection) -> None:
    """The full three-phase E7-boundary Ignore composition, in call order."""
    _script_verify_reads_e7(conn)
    _script_b3_reconciliation(conn)
    _script_branch_observation(conn)
    _script_apply_reads_e7(conn)


def _assert_no_writes(conn: ScriptedConnection) -> None:
    """The fail-closed seam: no gate, block, task, or presentation write."""
    for predicate in (
        _GATE_UPDATE,
        _TASK_GATE_WRITE,
        _ACKNOWLEDGE_WRITE,
        _STATUS_UPDATE,
        _BLOCK_UPDATE,
        _GATES_INSERT,
        _BRANCH_BIND,
    ):
        assert _count(conn, predicate) == 0


# --- the happy compositions ----------------------------------------------------------


def test_an_e5_interrupted_ignore_reaches_the_existing_plan_gate() -> None:
    """The ordinary E3b shape: the Ignore overwrites the Task's single
    fingerprint with exactly the block's observed value, resolves the
    exceptional gate APPROVED with truthful OWNER provenance, resolves the
    block with its evidence retained, and reaches the existing Approve Plan
    gate — all in one commit, with no unblocked-but-unpresented state."""
    conn = ScriptedConnection()
    _script_ignore_success_e5(conn)
    result = _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    assert result.boundary is IgnoredSourceChangeBoundary.IMPLEMENTATION_AUTHORIZATION
    assert result.task.status is TaskStatus.WAITING_FOR_OWNER
    assert result.resolved_owner_gate.status is OwnerGateStatus.APPROVED
    assert result.resolved_task_block.resolved_at is not None
    # The resolved block's previous/observed evidence is retained untouched.
    assert result.resolved_task_block.context["expected_fingerprint"] == _FINGERPRINT
    assert result.resolved_task_block.context["observed_fingerprint"] == _ACKNOWLEDGED_FINGERPRINT
    assert result.acknowledged_fingerprint == _ACKNOWLEDGED_FINGERPRINT
    assert result.presented.owner_gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
    assert result.presented.task.status is TaskStatus.WAITING_FOR_OWNER
    # The guarded acknowledge statement overwrites the baseline exactly once.
    assert _count(conn, _ACKNOWLEDGE_WRITE) == 1
    (acknowledge_params,) = [
        params for sql, params in conn.executed if _ACKNOWLEDGE_WRITE(sql, None)
    ]
    assert acknowledge_params is not None
    assert acknowledge_params[0] == _ACKNOWLEDGED_FINGERPRINT
    assert acknowledge_params[1] == "planning"
    assert acknowledge_params[3] == _TOKEN_2  # the post-resolution rotated token
    # The truthful OWNER audit and the presentation's own audits.
    assert _event_types(conn) == ["owner_gate_resolved", "owner_gate_created", "plan_ready"]
    (resolved_event,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert _event_context(resolved_event) == {"outcome": "approved"}
    # The interrupted boundary's existing gate is the one presented.
    (gate_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert gate_params is not None
    assert gate_params[2] == "implementation_authorization"
    assert _count(conn, _BRANCH_BIND) == 0


def test_the_e3c_settled_shape_restores_waiting_for_owner() -> None:
    """The mediated E3c shape: the settled plan-subject REVIEW_RESOLUTION gate
    identifies the interrupted boundary's pre-block status, so the Task
    restores to ``waiting_for_owner`` and the presentation performs no
    planning-to-waiting transition write."""
    conn = ScriptedConnection()
    _script_ignore_success_e5(conn, settled=True)
    result = _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    assert result.task.status is TaskStatus.WAITING_FOR_OWNER
    assert result.presented.task.status is TaskStatus.WAITING_FOR_OWNER
    (acknowledge_params,) = [
        params for sql, params in conn.executed if _ACKNOWLEDGE_WRITE(sql, None)
    ]
    assert acknowledge_params is not None
    assert acknowledge_params[1] == "waiting_for_owner"
    assert _count(conn, _STATUS_UPDATE) == 0


def test_an_e7_interrupted_ignore_reaches_the_existing_pr_gate() -> None:
    """The implementation-to-PR boundary: the Ignore acknowledges the change,
    restores the pre-gate implementing state, observes the branch head through
    the shared B7 seam, and reaches the existing Authorize PR gate with its
    one-time canonical-branch bind — in the same commit."""
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_ignore_success_e7(conn)
    github = FakeGitHubAppClient(pool=pool)
    result = _ignore_call(pool, github)
    assert result.boundary is IgnoredSourceChangeBoundary.PR_AUTHORIZATION
    assert result.task.status is TaskStatus.WAITING_FOR_OWNER
    assert result.resolved_owner_gate.status is OwnerGateStatus.APPROVED
    assert result.acknowledged_fingerprint == _ACKNOWLEDGED_FINGERPRINT
    assert result.presented.owner_gate.gate_type is OwnerGateType.PR_AUTHORIZATION
    assert result.presented.canonical_branch == _CLAIMED_BRANCH
    assert result.presented.verified_head_sha == _HEAD_SHA
    # The B7 observation addressed the claimed branch; the bind happened once.
    assert github.branch_calls == [{"branch_name": _CLAIMED_BRANCH}]
    assert _count(conn, _BRANCH_BIND) == 1
    assert _count(conn, _ACKNOWLEDGE_WRITE) == 1
    (gate_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert gate_params is not None
    assert gate_params[2] == "pr_authorization"
    assert gate_params[5] == _HEAD_SHA  # the exact-head subject


def test_a_mismatched_bound_branch_refuses_the_e7_ignore_without_any_write() -> None:
    """The E7 continuation preserves the normal presentation's canonical-
    branch authority guard: a bound canonical branch that differs from the
    completed implementation's branch claim is an unsafe continuation —
    the fresh exactness phase refuses before any head read and before any
    write, leaving the original Ignore gate and its unresolved TaskBlock
    exactly as they were (re-proven again inside the final atomic
    composition's fresh claim derivation)."""
    conn = ScriptedConnection()
    _script_verify_reads_e7(conn, task_row=_task_row(canonical_branch=_OTHER_BRANCH))
    _script_b3_reconciliation(conn)
    with pytest.raises(ConflictError, match="already bound; remediation continues"):
        _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    _assert_no_writes(conn)
    assert _event_types(conn) == []


# --- the fail-closed seams -----------------------------------------------------------


def test_a_stale_observed_fingerprint_refuses_without_any_write() -> None:
    """The fresh exactness verification: the GitHub issue's current fingerprint
    must be exactly the block's observed value — anything else changes
    nothing and the Task stays blocked at its pending gate."""
    conn = ScriptedConnection()
    _script_verify_reads_e5(conn)
    # The fresh observation reconciles back to the pre-drift baseline: the
    # locked projection row already carries it, so the reconcile is a no-op.
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())
    conn.on(_REPO_READ_LOCKED, _repository_row())
    conn.on(_ISSUE_READ_LOCKED, _issue_row(fingerprint=_FINGERPRINT, title="Found a bug"))
    with pytest.raises(ConflictError, match="cannot acknowledge a stale observation"):
        _ignore_call(
            _pool(conn),
            FakeGitHubAppClient(pool=_pool(conn), issue_observation=_baseline_issue_observation()),
        )
    _assert_no_writes(conn)
    assert _event_types(conn) == []


def test_an_unclassifiable_boundary_refuses_without_any_write() -> None:
    """Durable state that names neither boundary (here: both an OPEN and a
    CLOSED planning loop) refuses the whole Ignore with zero writes."""
    conn = ScriptedConnection()
    _script_verify_reads_e5(conn, loops=[_loop_row(), _loop_row(status="closed")])
    with pytest.raises(ConflictError, match="could not be established"):
        _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    _assert_no_writes(conn)
    assert _event_types(conn) == []


def test_a_stale_token_refuses_without_any_write() -> None:
    """A moved-on state token is a typed stale operation that applies nothing."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    moved_on = _task_row(state_token=_TOKEN_2)
    conn.on(_TASK_READ, moved_on)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, moved_on)
    conn.on(_TASK_READ, moved_on)  # require_current_task: the token mismatch
    with pytest.raises(StaleOperationError, match="moved on"):
        _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    _assert_no_writes(conn)
    assert _event_types(conn) == []


def test_a_wrong_gate_type_refuses_without_any_write() -> None:
    """Only the exact current pending GITHUB_ISSUE_CHANGE gate ignores through
    this seam; any other current gate type fails closed."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    blocked = _task_row()
    conn.on(_TASK_READ, blocked)
    conn.on(_TASK_READ_LOCKED, blocked)
    conn.on(_TASK_READ, blocked)
    pr_gate = list(
        _gate_row(gate_id=_DRIFT_GATE_ID, gate_type="pr_authorization", status="pending")
    )
    pr_gate[6] = _HEAD_SHA  # the head-SHA subject the gate shape requires
    conn.on(_GATE_READ, tuple(pr_gate))
    with pytest.raises(ConflictError, match="only the current github issue change gate"):
        _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    _assert_no_writes(conn)
    assert _event_types(conn) == []


def test_durable_drift_moved_on_at_commit_refuses_without_any_write() -> None:
    """The commit's durable exactness re-proof: if the freshly written
    projection no longer differs from the baseline, the ignore is stale and
    the whole composition applies nothing."""
    conn = ScriptedConnection()
    _script_verify_reads_e5(conn)
    _script_b3_reconciliation(conn)
    blocked = _task_row()
    conn.on(_TASK_READ_LOCKED, blocked)
    conn.on(_TASK_READ, blocked)
    conn.on(_GATE_READ, _gate_row())
    conn.on(_BLOCK_READ, _block_row())
    conn.on(_ISSUE_FIND, _issue_row(fingerprint=_FINGERPRINT))  # the drift is gone
    with pytest.raises(StaleOperationError, match="no longer differ"):
        _ignore_call(_pool(conn), FakeGitHubAppClient(pool=_pool(conn)))
    _assert_no_writes(conn)
    assert _event_types(conn) == []
