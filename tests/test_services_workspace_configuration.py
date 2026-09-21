"""Deterministic tests for the Workspace configuration service (issue #53).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the ownership-gated configuration flows — defaults,
valid updates, invalid-command rejection, no-op semantics, and the safe #56
audit-handoff shape (no guidance prose, no ReviewLoop history rewrite).
Database defaults/constraints are proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import workspace_configuration
from openorc.services.errors import InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("configuration tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(
    owner_profile_id: Any,
    *,
    review_iteration_limit: int = 5,
    guidance: str = "",
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        review_iteration_limit,
        guidance,
    )


def test_get_workspace_configuration_is_ownership_gated() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()

    # The default-boundary row the durable defaults produce: limit 5,
    # blank guidance.
    workspace = workspace_configuration.get_workspace_configuration(
        _pool(ScriptedConnection([_ws_row(profile_id)])),
        profile_id=profile_id,
        workspace_id=workspace_id,
    )
    assert workspace.review_iteration_limit == DEFAULT_REVIEW_LOOP_ITERATION_LIMIT
    assert workspace.guidance == ""

    # Another Profile's Workspace is uniformly not found.
    with pytest.raises(NotFoundError):
        workspace_configuration.get_workspace_configuration(
            _pool(ScriptedConnection([_ws_row(uuid.uuid4())])),
            profile_id=profile_id,
            workspace_id=workspace_id,
        )


def test_set_guidance_replaces_the_current_value_without_history() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    prose = "Always re-run the full suite before dispatching the Reviewer.\n第二段落。"
    blank_row = _ws_row(profile_id, guidance="")
    prose_row = _ws_row(profile_id, guidance=prose)
    conn = ScriptedConnection([_ws_row(profile_id), blank_row, prose_row])

    result = workspace_configuration.set_guidance(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, guidance=prose
    )

    assert result == workspace_configuration.WorkspaceGuidanceUpdate(
        workspace_id=workspace_id, changed=True
    )
    update_sql, update_params = conn.executed[2]
    assert "update openorc.workspaces" in update_sql
    assert update_params == (prose, workspace_id)
    assert all("review_loops" not in sql for sql, _ in conn.executed)

    # The handoff result carries only the semantic change fact — the
    # dataclass has no field that could carry the prose into event context.
    assert set(workspace_configuration.WorkspaceGuidanceUpdate.__dataclass_fields__) == {
        "workspace_id",
        "changed",
    }

    # Resetting to blank is itself a change of the current value.
    reset = ScriptedConnection([_ws_row(profile_id), prose_row, blank_row])
    result = workspace_configuration.set_guidance(
        _pool(reset), profile_id=profile_id, workspace_id=workspace_id, guidance=""
    )
    assert result.changed is True

    # Same-value write: no-op.
    noop = ScriptedConnection([_ws_row(profile_id), prose_row])
    result = workspace_configuration.set_guidance(
        _pool(noop), profile_id=profile_id, workspace_id=workspace_id, guidance=prose
    )
    assert result.changed is False
    assert len(noop.executed) == 2


def test_set_guidance_rejects_non_string_input_and_fails_closed_for_non_owners() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])

    with pytest.raises(InvalidCommandError):
        workspace_configuration.set_guidance(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=uuid.uuid4(),
            guidance=None,  # type: ignore[arg-type]
        )
    assert conn.executed == []

    with pytest.raises(NotFoundError):
        workspace_configuration.set_guidance(
            _pool(ScriptedConnection([_ws_row(uuid.uuid4())])),
            profile_id=profile_id,
            workspace_id=uuid.uuid4(),
            guidance="prose",
        )


def test_set_review_iteration_limit_updates_future_loop_configuration() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    old_row = _ws_row(profile_id, review_iteration_limit=5)
    new_row = _ws_row(profile_id, review_iteration_limit=7)
    conn = ScriptedConnection([_ws_row(profile_id), old_row, new_row])

    result = workspace_configuration.set_review_iteration_limit(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, review_iteration_limit=7
    )

    assert result == workspace_configuration.ReviewIterationLimitUpdate(
        workspace_id=workspace_id,
        changed=True,
        previous_review_iteration_limit=5,
        new_review_iteration_limit=7,
    )
    # One composed transaction: ownership gate, row-locked before-state,
    # then the conditional write.
    ownership_sql, _ = conn.executed[0]
    assert "from openorc.workspaces where id = %s" in ownership_sql
    assert "for update" not in ownership_sql
    lock_sql, _ = conn.executed[1]
    assert "for update" in lock_sql
    update_sql, update_params = conn.executed[2]
    assert "update openorc.workspaces" in update_sql
    assert update_params == (7, workspace_id)
    # Existing ReviewLoop history is never touched by the configuration flow.
    assert all("review_loops" not in sql for sql, _ in conn.executed)

    # The updated value is exactly what a newly created ReviewLoop is
    # supplied through the setting boundary (the loop's own persistence
    # round-trip is proven by the reviews tests and integration suite).
    loop = ReviewLoop(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        task_id=uuid.uuid4(),
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=result.new_review_iteration_limit,
        status=ReviewLoopStatus.OPEN,
        closed_at=None,
        created_at=_OBSERVED,
    )
    assert loop.iteration_limit == 7


def test_set_review_iteration_limit_rejects_invalid_commands_before_any_io() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([])

    for bad in (0, -1, 1.5, "5", True):
        with pytest.raises(InvalidCommandError):
            workspace_configuration.set_review_iteration_limit(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=uuid.uuid4(),
                review_iteration_limit=bad,  # type: ignore[arg-type]
            )

    assert conn.executed == []


def test_set_review_iteration_limit_same_value_is_a_no_op() -> None:
    profile_id = uuid.uuid4()
    row = _ws_row(profile_id, review_iteration_limit=5)
    conn = ScriptedConnection([_ws_row(profile_id), row])

    result = workspace_configuration.set_review_iteration_limit(
        _pool(conn), profile_id=profile_id, workspace_id=uuid.uuid4(), review_iteration_limit=5
    )

    assert result.changed is False
    assert result.previous_review_iteration_limit == 5
    assert result.new_review_iteration_limit == 5
    # Ownership select + locked before-state select; no UPDATE executed.
    assert len(conn.executed) == 2


def test_set_review_iteration_limit_fails_closed_for_non_owners() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_ws_row(uuid.uuid4())])

    with pytest.raises(NotFoundError):
        workspace_configuration.set_review_iteration_limit(
            _pool(conn), profile_id=profile_id, workspace_id=uuid.uuid4(), review_iteration_limit=7
        )

    # Only the ownership gate ran; no configuration write was attempted.
    assert len(conn.executed) == 1
