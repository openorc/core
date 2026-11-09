"""Deterministic tests for the shared source-requirements comparison (issue #289).

A scripted SQL-connection seam proves the ONE comparison/blocking policy the
E5 and E7 approval-gate presentations share:

- the comparison precondition: a freshly reconciled projection resolving to a
  different stable issue identity is a typed fail-closed conflict that
  fabricates no drift;
- a matching fingerprint returns the fresh value and performs no write — the
  caller follows its existing gate-presentation path unchanged;
- a confirmed change takes the shared atomic drift-block path: one
  ``github_source_changed`` TaskBlock with its exact expected/observed
  fingerprint evidence, its pending Level 3-selected
  ``GITHUB_ISSUE_CHANGE`` OwnerGate, the guarded ``blocked`` transition,
  and the ``task_blocked``/``owner_gate_created`` audits — commit or roll
  back together — and the typed workflow conflict refuses the presentation;
- the drift block re-proves durable drift under the Task row lock: a
  baseline that already matches is a typed stale operation that applies
  nothing;
- the durable staleness re-proof used inside presentation transactions
  returns the projection on a match and refuses a durable drift;
- issue closure is deliberately not a comparison outcome: the policy never
  inspects the projection's open/closed state.

No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.domain.github_issues import (
    GitHubIssueIdentity,
    GitHubIssueProjection,
    GitHubIssueState,
    github_issue_requirements_fingerprint,
)
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, StaleOperationError
from openorc.services.task_source_requirements import (
    block_source_requirements_drift,
    compare_source_fingerprint_or_block,
    require_unchanged_durable_source_state,
)

_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_DRIFT_GATE_ID = uuid.uuid4()
_DRIFT_BLOCK_ID = uuid.uuid4()
_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Requirements body")
_ACKNOWLEDGED_FINGERPRINT = github_issue_requirements_fingerprint(
    "Found a bug v2", "Requirements body"
)
_DRIFTED_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug v3", "Requirements body")

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
        raise AssertionError("source requirements tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_ACCOUNT_BARRIER = _sql_has("from openorc.profiles", "for key share")
_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_ISSUE_FIND = _sql_lacks(_sql_has("from openorc.github_issues"), "for update")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_GATES_INSERT = _sql_has("insert into openorc.owner_gates")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks", "current_owner_gate_id")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _task(
    *,
    status: TaskStatus = TaskStatus.IMPLEMENTING,
    state_token: Any = _STATE_TOKEN,
    fingerprint: str = _FINGERPRINT,
    gate_id: Any = None,
) -> Task:
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        github_issue_number=_ISSUE_NUMBER,
        status=status,
        archived_at=None,
        queued_at=None,
        canonical_feature_branch=None,
        state_token=state_token,
        current_plan_revision_id=uuid.uuid4(),
        current_owner_gate_id=gate_id,
        source_requirements_fingerprint=fingerprint,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        terminal_cause=None,
        authorized_task_budget_usd=None,
    )


def _projection(*, fingerprint: str = _FINGERPRINT, state: str = "open") -> GitHubIssueProjection:
    return GitHubIssueProjection(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        identity=GitHubIssueIdentity(github_issue_id=_GITHUB_ISSUE_ID),
        issue_number=_ISSUE_NUMBER,
        title="Found a bug",
        body="Requirements body",
        state=GitHubIssueState(state),
        requirements_fingerprint=fingerprint,
        provider_updated_at=_OBSERVED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _task_row(
    *,
    status: str = "implementing",
    state_token: Any = _STATE_TOKEN,
    gate_id: Any = None,
    fingerprint: str = _FINGERPRINT,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        status,
        None,
        None,
        state_token,
        uuid.uuid4(),
        gate_id,
        fingerprint,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
        None,
    )


def _issue_row(*, fingerprint: str = _FINGERPRINT) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        "Found a bug",
        "Requirements body",
        "open",
        fingerprint,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
    )


def _block_row(*, expected: str = _FINGERPRINT, observed: str = _ACKNOWLEDGED_FINGERPRINT) -> Any:
    return (
        _DRIFT_BLOCK_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "github_source_changed",
        {
            "github_issue_id": _GITHUB_ISSUE_ID,
            "expected_fingerprint": expected,
            "observed_fingerprint": observed,
        },
        None,
        _OBSERVED,
    )


def _drift_gate_row() -> tuple[Any, ...]:
    row = [
        _DRIFT_GATE_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "github_issue_change",
        "pending",
        None,
        None,
        None,
        None,
        None,
        _OBSERVED,
    ]
    row[8] = _DRIFT_BLOCK_ID  # the gate's TaskBlock subject binding
    return tuple(row)


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "task_blocked",
        "openorc",
        None,
        None,
        None,
        {},
        _OBSERVED,
    )


def _event_types(conn: ScriptedConnection) -> list[str]:
    types: list[str] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None):
            assert params is not None
            types.append(cast(str, params[2]))
    return types


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    conn.on_repeat(_ACCOUNT_BARRIER, (None, None, None))
    conn.on_repeat(_EVENT_INSERT, _event_row())


def _script_drift_block(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    issue_row: tuple[Any, ...] | None = None,
) -> None:
    """The shared atomic drift-block composition in call order."""
    _script_repeating_reads(conn)
    drifting = task_row if task_row is not None else _task_row()
    conn.on(_TASK_READ_LOCKED, drifting)
    conn.on(_TASK_READ, drifting)
    conn.on(_ISSUE_FIND, issue_row if issue_row is not None else _issue_row())
    conn.on(_TASK_READ_LOCKED, drifting)  # the installer's locked re-read
    conn.on(_TASK_READ, drifting)  # the installer's currentness re-proof
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_GATES_INSERT, _drift_gate_row())
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(status="blocked", state_token=_TOKEN_2, gate_id=_DRIFT_GATE_ID),
    )


# --- the shared comparison -----------------------------------------------------------


def test_a_matching_fingerprint_returns_the_fresh_value_without_any_write() -> None:
    conn = ScriptedConnection()
    fingerprint = compare_source_fingerprint_or_block(
        _pool(conn),
        task=_task(),
        projection=_projection(),
        profile_id=_OWNER_PROFILE_ID,
        expected_state_token=_STATE_TOKEN,
    )
    assert fingerprint == _FINGERPRINT
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert conn.executed == []


def test_a_closed_projection_state_is_not_a_comparison_outcome() -> None:
    """Issue closure alone neither vetoes nor blocks: the policy compares the
    requirements fingerprint only."""
    fingerprint = compare_source_fingerprint_or_block(
        _pool(ScriptedConnection()),
        task=_task(),
        projection=_projection(state="closed"),
        profile_id=_OWNER_PROFILE_ID,
        expected_state_token=_STATE_TOKEN,
    )
    assert fingerprint == _FINGERPRINT


def test_a_different_stable_identity_refuses_without_drift() -> None:
    conn = ScriptedConnection()
    other = GitHubIssueProjection(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        identity=GitHubIssueIdentity(github_issue_id=999),
        issue_number=_ISSUE_NUMBER,
        title="Found a bug",
        body="Requirements body",
        state=GitHubIssueState.OPEN,
        requirements_fingerprint=_DRIFTED_FINGERPRINT,
        provider_updated_at=_OBSERVED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )
    with pytest.raises(ConflictError, match="different stable issue identity"):
        compare_source_fingerprint_or_block(
            _pool(conn),
            task=_task(),
            projection=other,
            profile_id=_OWNER_PROFILE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert _count(conn, _BLOCK_INSERT) == 0
    assert conn.executed == []


def test_a_confirmed_change_takes_the_shared_atomic_drift_block_path() -> None:
    conn = ScriptedConnection()
    _script_drift_block(conn, issue_row=_issue_row(fingerprint=_ACKNOWLEDGED_FINGERPRINT))
    with pytest.raises(ConflictError, match="blocked as github_source_changed"):
        compare_source_fingerprint_or_block(
            _pool(conn),
            task=_task(),
            projection=_projection(fingerprint=_ACKNOWLEDGED_FINGERPRINT),
            profile_id=_OWNER_PROFILE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert _count(conn, _BLOCK_INSERT) == 1
    (block_params,) = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    assert block_params is not None
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context == {
        "github_issue_id": _GITHUB_ISSUE_ID,
        "expected_fingerprint": _FINGERPRINT,
        "observed_fingerprint": _ACKNOWLEDGED_FINGERPRINT,
    }
    assert _count(conn, _GATES_INSERT) == 1
    (gate_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert gate_params is not None
    assert gate_params[2] == "github_issue_change"
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]


def test_a_later_distinct_change_declares_its_own_exact_evidence() -> None:
    """The changed-again seam: a further drift after an acknowledged value
    declares expected=acknowledged, observed=newer — the resolved block's
    retained history is never consulted."""
    conn = ScriptedConnection()
    _script_drift_block(
        conn,
        task_row=_task_row(fingerprint=_ACKNOWLEDGED_FINGERPRINT),
        issue_row=_issue_row(fingerprint=_DRIFTED_FINGERPRINT),
    )
    with pytest.raises(ConflictError):
        compare_source_fingerprint_or_block(
            _pool(conn),
            task=_task(fingerprint=_ACKNOWLEDGED_FINGERPRINT),
            projection=_projection(fingerprint=_DRIFTED_FINGERPRINT),
            profile_id=_OWNER_PROFILE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    (block_params,) = [params for sql, params in conn.executed if _BLOCK_INSERT(sql, None)]
    assert block_params is not None
    block_context = getattr(block_params[3], "obj", block_params[3])
    assert block_context == {
        "github_issue_id": _GITHUB_ISSUE_ID,
        "expected_fingerprint": _ACKNOWLEDGED_FINGERPRINT,
        "observed_fingerprint": _DRIFTED_FINGERPRINT,
    }


def test_a_stale_drift_block_applies_nothing() -> None:
    """The composition's durable re-proof: a baseline that already matches the
    freshly written projection is a typed stale operation with no writes."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    task = _task_row()
    conn.on(_TASK_READ_LOCKED, task)
    conn.on(_TASK_READ, task)
    conn.on(_ISSUE_FIND, _issue_row())  # the projection already matches
    with pytest.raises(StaleOperationError, match="no longer differ"):
        block_source_requirements_drift(
            _pool(conn),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _GATES_INSERT) == 0


# --- the durable staleness re-proof --------------------------------------------------


def test_the_durable_re_proof_returns_the_projection_on_a_match() -> None:
    conn = ScriptedConnection()
    conn.on(_ISSUE_FIND, _issue_row())
    projection = require_unchanged_durable_source_state(_pool(conn), task=_task())
    assert projection.requirements_fingerprint == _FINGERPRINT


def test_the_durable_re_proof_refuses_a_durable_drift() -> None:
    conn = ScriptedConnection()
    conn.on(_ISSUE_FIND, _issue_row(fingerprint=_ACKNOWLEDGED_FINGERPRINT))
    with pytest.raises(ConflictError, match="changed during presentation"):
        require_unchanged_durable_source_state(_pool(conn), task=_task())


def test_the_durable_re_proof_refuses_a_missing_projection() -> None:
    conn = ScriptedConnection()
    conn.on(_ISSUE_FIND, None)
    with pytest.raises(ConflictError, match="no longer available"):
        require_unchanged_durable_source_state(_pool(conn), task=_task())
