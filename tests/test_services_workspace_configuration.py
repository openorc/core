"""Deterministic tests for the Workspace configuration service (issue #53).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the ownership-gated configuration flows — defaults,
valid updates, invalid-command rejection, no-op semantics — and the #56
audit coordination: an actual change records its
``WORKSPACE_CONFIGURATION_CHANGED`` event inside the same composed
transaction with the exact safe actor/subject/context mapping (no guidance
prose, no ReviewLoop history rewrite), a failed event insertion leaves the
mutation uncommitted, and no-op/failed paths write no event. Database
defaults/constraints and the real all-or-nothing rollback are proven by the
integration-marked suite.
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

from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
)
from openorc.observability import OPERATION, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import workspace_configuration
from openorc.services.errors import InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of every set_* flow; the scripted result row for an
# operational account carries the all-NULL attempt-state tuple. Read-only
# flows (get_workspace_configuration) compose no barrier.
_GUARD_OPERATIONAL_ROW = (None, None, None)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

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


def _event_row(
    *,
    actor_id: str,
    context: dict[str, object] | None = None,
) -> tuple[Any, ...]:
    """A canned `record_workflow_event` INSERT ... RETURNING row."""
    return (
        uuid.uuid4(),
        uuid.uuid4(),
        None,
        "workspace_configuration_changed",
        "owner",
        actor_id,
        "workspace",
        uuid.uuid4(),
        context or {},
        _OBSERVED,
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
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            blank_row,
            prose_row,
            _event_row(actor_id=str(profile_id)),
        ]
    )

    result = workspace_configuration.set_guidance(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, guidance=prose
    )

    assert result == workspace_configuration.WorkspaceGuidanceUpdate(
        workspace_id=workspace_id, changed=True
    )
    update_sql, update_params = conn.executed[3]
    assert "update openorc.workspaces" in update_sql
    assert update_params == (prose, workspace_id)
    assert all("review_loops" not in sql for sql, _ in conn.executed)

    # The coordinated #56 event: Workspace-scoped, subject the Workspace,
    # OWNER actor carrying the authenticated Profile UUID, and a context
    # that identifies only the guidance setting change.
    event_sql, event_params = conn.executed[4]
    assert "insert into openorc.workflow_events" in event_sql
    assert event_params is not None
    assert event_params[0] == workspace_id
    assert event_params[1] is None
    assert event_params[2] == "workspace_configuration_changed"
    assert event_params[3] == "owner"
    assert event_params[4] == str(profile_id)
    assert event_params[5] == "workspace"
    assert event_params[6] == workspace_id
    assert event_params[7].obj == {"setting": "guidance"}

    # Owner-authored prose is never copied into the event: no prose appears
    # in the executed statement or its parameters.
    for sql, params in conn.executed:
        assert prose not in sql
        assert prose not in repr(params)

    # The handoff result carries only the semantic change fact — the
    # dataclass has no field that could carry the prose into event context.
    assert set(workspace_configuration.WorkspaceGuidanceUpdate.__dataclass_fields__) == {
        "workspace_id",
        "changed",
    }

    # Resetting to blank is itself a change of the current value — and it
    # records the same semantic event, never a prose delta.
    reset = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            prose_row,
            blank_row,
            _event_row(actor_id=str(profile_id)),
        ]
    )
    result = workspace_configuration.set_guidance(
        _pool(reset), profile_id=profile_id, workspace_id=workspace_id, guidance=""
    )
    assert result.changed is True
    assert len(reset.executed) == 5
    assert reset.executed[4][1] is not None
    assert reset.executed[4][1][7].obj == {"setting": "guidance"}
    assert all(prose not in repr(params) for _, params in reset.executed)

    # Same-value write: no-op, and no event insert.
    noop = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(profile_id), prose_row])
    result = workspace_configuration.set_guidance(
        _pool(noop), profile_id=profile_id, workspace_id=workspace_id, guidance=prose
    )
    assert result.changed is False
    assert len(noop.executed) == 3
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in noop.executed)


def test_set_guidance_event_insert_failure_rolls_back_the_mutation() -> None:
    """A failed event insertion propagates — the canonical change never stands alone.

    The deterministic seam proves the event insert is attempted inside the
    same composition after the row-locked update; the real all-or-nothing
    rollback is proven by the integration-marked suite.
    """
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    prose = "Updated guidance."
    blank_row = _ws_row(profile_id, guidance="")
    prose_row = _ws_row(profile_id, guidance=prose)
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            blank_row,
            prose_row,
            RuntimeError("event insertion failed"),
        ]
    )

    with pytest.raises(RuntimeError, match="event insertion failed"):
        workspace_configuration.set_guidance(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id, guidance=prose
        )

    assert len(conn.executed) == 5
    assert "update openorc.workspaces" in conn.executed[3][0]
    assert "insert into openorc.workflow_events" in conn.executed[4][0]


def test_set_guidance_fails_closed_for_non_owners_with_no_event() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(uuid.uuid4())])

    with pytest.raises(NotFoundError):
        workspace_configuration.set_guidance(
            _pool(conn), profile_id=profile_id, workspace_id=uuid.uuid4(), guidance="prose"
        )

    # The barrier and ownership-gate reads ran: neither the write nor any event insert.
    assert len(conn.executed) == 2
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


def test_set_guidance_rejects_non_string_input() -> None:
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


def test_set_review_iteration_limit_updates_future_loop_configuration() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    old_row = _ws_row(profile_id, review_iteration_limit=5)
    new_row = _ws_row(profile_id, review_iteration_limit=7)
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            old_row,
            new_row,
            _event_row(actor_id=str(profile_id)),
        ]
    )

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
    ownership_sql, _ = conn.executed[1]
    assert "from openorc.workspaces where id = %s" in ownership_sql
    assert "for update" not in ownership_sql
    lock_sql, _ = conn.executed[2]
    assert "for update" in lock_sql
    update_sql, update_params = conn.executed[3]
    assert "update openorc.workspaces" in update_sql
    assert update_params == (7, workspace_id)
    # Existing ReviewLoop history is never touched by the configuration flow.
    assert all("review_loops" not in sql for sql, _ in conn.executed)

    # The coordinated #56 event: setting key plus the exact locked
    # previous/new integers — the Workspace row is never shadowed.
    event_sql, event_params = conn.executed[4]
    assert "insert into openorc.workflow_events" in event_sql
    assert event_params is not None
    assert event_params[0] == workspace_id
    assert event_params[1] is None
    assert event_params[2] == "workspace_configuration_changed"
    assert event_params[3] == "owner"
    assert event_params[4] == str(profile_id)
    assert event_params[5] == "workspace"
    assert event_params[6] == workspace_id
    assert event_params[7].obj == {
        "setting": "review_iteration_limit",
        "previous": 5,
        "new": 7,
    }

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


def test_set_review_iteration_limit_event_insert_failure_rolls_back_the_mutation() -> None:
    """A failed event insertion propagates — the canonical change never stands alone."""
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id, review_iteration_limit=5),
            _ws_row(profile_id, review_iteration_limit=5),
            _ws_row(profile_id, review_iteration_limit=7),
            RuntimeError("event insertion failed"),
        ]
    )

    with pytest.raises(RuntimeError, match="event insertion failed"):
        workspace_configuration.set_review_iteration_limit(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id, review_iteration_limit=7
        )

    assert len(conn.executed) == 5
    assert "update openorc.workspaces" in conn.executed[3][0]
    assert "insert into openorc.workflow_events" in conn.executed[4][0]


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
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(profile_id), row])

    result = workspace_configuration.set_review_iteration_limit(
        _pool(conn), profile_id=profile_id, workspace_id=uuid.uuid4(), review_iteration_limit=5
    )

    assert result.changed is False
    assert result.previous_review_iteration_limit == 5
    assert result.new_review_iteration_limit == 5
    # Barrier read, ownership select, and locked before-state select; no
    # UPDATE executed and
    # no event insert attempted.
    assert len(conn.executed) == 3
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


def test_set_review_iteration_limit_fails_closed_for_non_owners() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, _ws_row(uuid.uuid4())])

    with pytest.raises(NotFoundError):
        workspace_configuration.set_review_iteration_limit(
            _pool(conn), profile_id=profile_id, workspace_id=uuid.uuid4(), review_iteration_limit=7
        )

    # The barrier and ownership-gate reads ran; no configuration write and
    # no event insert was attempted.
    assert len(conn.executed) == 2
    assert all("insert into openorc.workflow_events" not in sql for sql, _ in conn.executed)


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_get_workspace_configuration_opens_its_service_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        workspace_configuration.get_workspace_configuration(
            _pool(ScriptedConnection([_ws_row(profile_id)])),
            profile_id=profile_id,
            workspace_id=workspace_id,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "workspace_configuration.get_workspace_configuration"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID}
    assert attributes[WORKSPACE_ID] == str(workspace_id)


def test_set_guidance_opens_its_service_span_without_the_prose() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    prose = "Always re-run the full suite before dispatching the Reviewer."
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            _ws_row(profile_id, guidance=""),
            _ws_row(profile_id, guidance=prose),
            _event_row(actor_id=str(profile_id)),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        workspace_configuration.set_guidance(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id, guidance=prose
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "workspace_configuration.set_guidance"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID}
    assert attributes[OPERATION] == "workspace_configuration.set_guidance"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
    # The safe vocabulary is the only supported attribute path: the Owner
    # guidance prose never appears in exported telemetry.
    assert all(prose not in str(value) for value in attributes.values())


def test_set_review_iteration_limit_opens_its_service_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(profile_id),
            _ws_row(profile_id, review_iteration_limit=5),
            _ws_row(profile_id, review_iteration_limit=7),
            _event_row(actor_id=str(profile_id)),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        result = workspace_configuration.set_review_iteration_limit(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id, review_iteration_limit=7
        )

    assert result.changed is True
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "workspace_configuration.set_review_iteration_limit"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID}
    assert attributes[OPERATION] == "workspace_configuration.set_review_iteration_limit"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
