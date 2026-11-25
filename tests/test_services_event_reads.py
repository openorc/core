"""Focused tests for the authorized WorkflowEvent audit read composition (issue #377).

Proven here: the bounded keyset page composition behind the Workspace and
Task event reads (guard-before-read, the limit+1 has-more probe, the opaque
``next_cursor`` bound to the exact scope and filter set, the newest-first
ordering, and the safe public projection), the cursor codec's scope/filter
binding (a cursor from another scope or filter set is a typed invalid
command, never a silent filter change), the canonical filter validation
(vocabulary values, UTC-normalized timezone-aware bounds, range ordering),
and the bounded limit.

The guard, transaction, and persistence seams are shared vocabulary owned by
their own suites — these tests script them, never re-prove them.
"""

from __future__ import annotations

import base64
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.events import WorkflowEvent, WorkflowEventActor, WorkflowEventType
from openorc.persistence.pool import DatabasePool
from openorc.services import event_reads
from openorc.services.errors import InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 11, 24, 8, 30, 0, tzinfo=UTC)
_PROFILE_ID = uuid.uuid4()
_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_EVENT_ID = uuid.uuid4()

_WorkspaceScope = "w"
_TaskScope = "t"


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


_Predicate = Callable[[str, tuple[Any, ...] | None], bool]


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, Any, bool]] = []

    def on(self, predicate: _Predicate, result: Any) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: Any) -> None:
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
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("event read tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _sql_has(*tokens: str) -> _Predicate:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


def _ws_row() -> tuple[Any, ...]:
    return (
        _WORKSPACE_ID,
        _PROFILE_ID,
        "engineering",
        _OBSERVED,
        _OBSERVED,
        5,
        "",
        None,
    )


def _task_row(**overrides: Any) -> tuple[Any, ...]:
    from tests.fakes.task_rows import task_row

    return task_row(id=_TASK_ID, workspace_id=_WORKSPACE_ID, **overrides)


def _row_event(
    *,
    event_type: WorkflowEventType = WorkflowEventType.TASK_STARTED,
    actor_type: str = "owner",
    actor_id: str | None = None,
    subject_type: str | None = None,
    subject_id: Any = None,
    context: dict[str, object] | None = None,
    created_at: datetime = _OBSERVED,
    event_id: Any = None,
) -> tuple[Any, ...]:
    """One canned ``openorc.workflow_events`` page-row (repository shape)."""
    return (
        event_id or uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type.value,
        actor_type,
        actor_id,
        subject_type,
        subject_id,
        context or {},
        created_at,
    )


def _script_workspace_guard(conn: ScriptedConnection) -> None:
    conn.on_repeat(_sql_has("from openorc.workspaces"), _ws_row())


def _script_task_guard(conn: ScriptedConnection) -> None:
    conn.on_repeat(_sql_has("from openorc.workspaces"), _ws_row())
    conn.on_repeat(_sql_has("from openorc.tasks"), _task_row())


def _script_read_snapshot(conn: ScriptedConnection) -> None:
    conn.on_repeat(_sql_has("set transaction isolation"), None)


def _domain_event(**overrides: Any) -> WorkflowEvent:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "event_type": WorkflowEventType.TASK_CREATED,
        "actor_type": WorkflowEventActor.OPENORC,
        "actor_id": None,
        "subject_type": None,
        "subject_id": None,
        "context": {},
        "created_at": _OBSERVED,
    }
    values.update(overrides)
    return WorkflowEvent(**values)


class TestEventPageCursorCodec:
    def test_encode_then_decode_round_trips_scope_filters_and_key(self) -> None:
        filters = event_reads.EventPageFilters(
            event_type=WorkflowEventType.TASK_STARTED,
            actor_type=WorkflowEventActor.OWNER,
            from_created_at=datetime(2026, 11, 1, tzinfo=UTC),
            to_created_at=datetime(2026, 12, 1, tzinfo=UTC),
        )
        cursor = event_reads.encode_event_page_cursor(
            _TaskScope, _TASK_ID, filters, _OBSERVED, _EVENT_ID
        )
        decoded = event_reads.decode_event_page_cursor(cursor)
        assert decoded.scope_key == _TaskScope
        assert decoded.scope_id == _TASK_ID
        assert decoded.event_type is WorkflowEventType.TASK_STARTED
        assert decoded.actor_type is WorkflowEventActor.OWNER
        assert decoded.from_created_at == datetime(2026, 11, 1, tzinfo=UTC)
        assert decoded.to_created_at == datetime(2026, 12, 1, tzinfo=UTC)
        assert decoded.after_created_at == _OBSERVED
        assert decoded.after_event_id == _EVENT_ID

    def test_unbounded_filters_round_trip_as_absent(self) -> None:
        filters = event_reads.EventPageFilters(
            event_type=None, actor_type=None, from_created_at=None, to_created_at=None
        )
        cursor = event_reads.encode_event_page_cursor(
            _WorkspaceScope, _WORKSPACE_ID, filters, _OBSERVED, _EVENT_ID
        )
        decoded = event_reads.decode_event_page_cursor(cursor)
        assert decoded.scope_key == _WorkspaceScope
        assert decoded.event_type is None
        assert decoded.actor_type is None
        assert decoded.from_created_at is None
        assert decoded.to_created_at is None

    @pytest.mark.parametrize(
        "cursor",
        [
            "",
            "not-a-cursor",
            base64.urlsafe_b64encode(b"x:1:2").decode("ascii"),
            base64.urlsafe_b64encode(
                f"e:{_TaskScope}:{_TASK_ID}:no_such_type::::::::".encode("ascii")
            ).decode("ascii"),
        ],
    )
    def test_an_unusable_cursor_is_a_typed_invalid_command(self, cursor: str) -> None:
        with pytest.raises(InvalidCommandError):
            event_reads.decode_event_page_cursor(cursor)


class TestWorkspaceEventsPage:
    def test_lists_newest_first_behind_the_guard_with_safe_projection(self) -> None:
        later_id, earlier_id = uuid.uuid4(), uuid.uuid4()
        rows = [
            _row_event(
                event_type=WorkflowEventType.OWNER_GATE_RESOLVED,
                subject_type="owner_gate",
                subject_id=uuid.uuid4(),
                context={"outcome": "approved", "unexpected_key": "x"},
                created_at=_OBSERVED + timedelta(seconds=2),
                event_id=later_id,
            ),
            _row_event(
                created_at=_OBSERVED + timedelta(seconds=1),
                event_id=earlier_id,
            ),
        ]
        conn = ScriptedConnection()
        _script_workspace_guard(conn)
        _script_read_snapshot(conn)
        conn.on_repeat(_sql_has("from openorc.workflow_events"), rows)

        events, next_cursor = event_reads.list_workspace_events_for_owner(
            _pool(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            limit=2,
        )

        # The Workspace guard runs before any composed read.
        assert "from openorc.workspaces" in conn.executed[0][0]
        assert any("from openorc.workflow_events" in sql for sql, _ in conn.executed)
        assert len(events) == 2
        # Newest-first over the durable key, with actor/subject provenance
        # exactly as stored and the context projected through the
        # established shape (the unknown key omitted, the safe value kept).
        assert events[0].id == later_id
        assert events[1].id == earlier_id
        assert events[0].event_type == "owner_gate_resolved"
        assert events[0].subject_type == "owner_gate"
        assert events[0].context == {"outcome": "approved"}
        assert events[1].event_type == "task_started"
        assert events[1].context == {}

    def test_an_exhausted_page_returns_a_null_cursor(self) -> None:
        conn = ScriptedConnection()
        _script_workspace_guard(conn)
        _script_read_snapshot(conn)
        conn.on_repeat(_sql_has("from openorc.workflow_events"), [_row_event()])

        events, next_cursor = event_reads.list_workspace_events_for_owner(
            _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )

        assert len(events) == 1
        assert next_cursor is None

    def test_a_following_page_continues_after_the_last_key(self) -> None:
        conn = ScriptedConnection()
        _script_workspace_guard(conn)
        _script_read_snapshot(conn)
        conn.on_repeat(_sql_has("from openorc.workflow_events"), [_row_event()])
        filters = event_reads.EventPageFilters(
            event_type=None, actor_type=None, from_created_at=None, to_created_at=None
        )
        cursor = event_reads.encode_event_page_cursor(
            _WorkspaceScope, _WORKSPACE_ID, filters, _OBSERVED, _EVENT_ID
        )

        _, _ = event_reads.list_workspace_events_for_owner(
            _pool(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            cursor=cursor,
            limit=1,
        )

        page_sql, page_params = next(
            (sql, params) for sql, params in conn.executed if "from openorc.workflow_events" in sql
        )
        # The keyset continuation is the row comparison strictly after the
        # cursor's last examined key, with the limit+1 has-more probe.
        assert "(created_at, id) < " in page_sql
        assert page_params is not None
        assert page_params[1] == _OBSERVED
        assert page_params[2] == _EVENT_ID
        assert page_params[3] == 2

    def test_a_cursor_from_another_scope_is_a_typed_invalid_command(self) -> None:
        conn = ScriptedConnection()
        filters = event_reads.EventPageFilters(
            event_type=None, actor_type=None, from_created_at=None, to_created_at=None
        )
        task_cursor = event_reads.encode_event_page_cursor(
            _TaskScope, _TASK_ID, filters, _OBSERVED, _EVENT_ID
        )

        with pytest.raises(InvalidCommandError):
            event_reads.list_workspace_events_for_owner(
                _pool(conn),
                profile_id=_PROFILE_ID,
                workspace_id=_WORKSPACE_ID,
                cursor=task_cursor,
            )
        # Rejected before any read runs.
        assert conn.executed == []

    def test_a_cursor_from_another_filter_set_is_a_typed_invalid_command(self) -> None:
        conn = ScriptedConnection()
        bound = event_reads.EventPageFilters(
            event_type=WorkflowEventType.TASK_STARTED,
            actor_type=None,
            from_created_at=None,
            to_created_at=None,
        )
        cursor = event_reads.encode_event_page_cursor(
            _WorkspaceScope, _WORKSPACE_ID, bound, _OBSERVED, _EVENT_ID
        )

        with pytest.raises(InvalidCommandError):
            event_reads.list_workspace_events_for_owner(
                _pool(conn),
                profile_id=_PROFILE_ID,
                workspace_id=_WORKSPACE_ID,
                cursor=cursor,
            )
        assert conn.executed == []

    def test_a_foreign_workspace_is_the_uniform_not_found_outcome(self) -> None:
        conn = ScriptedConnection()
        foreign_row = (_WORKSPACE_ID, uuid.uuid4(), "other", _OBSERVED, _OBSERVED, 5, "", None)
        conn.on_repeat(_sql_has("from openorc.workspaces"), foreign_row)

        with pytest.raises(NotFoundError):
            event_reads.list_workspace_events_for_owner(
                _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
            )
        assert not any("from openorc.workflow_events" in sql for sql, _ in conn.executed)


class TestTaskEventsPage:
    def test_lists_the_task_scope_behind_the_task_guard_including_archived(self) -> None:
        conn = ScriptedConnection()
        _script_task_guard(conn)
        _script_read_snapshot(conn)
        conn.on_repeat(
            _sql_has("from openorc.workflow_events"),
            [_row_event(event_id=_EVENT_ID)],
        )

        events, next_cursor = event_reads.list_task_events_for_owner(
            _pool(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
        )

        # Guard reads first (Workspace ownership, then the exact Task), then
        # the read snapshot, then the Task-scoped page. The same guard admits
        # archived attempts (no archival predicate exists on the resolver).
        assert "from openorc.workspaces" in conn.executed[0][0]
        assert "from openorc.tasks" in conn.executed[1][0]
        assert any("set transaction isolation" in sql for sql, _ in conn.executed[:3])
        assert len(events) == 1
        assert events[0].id == _EVENT_ID
        assert next_cursor is None
        task_page_sql, task_page_params = next(
            (sql, params) for sql, params in conn.executed if "from openorc.workflow_events" in sql
        )
        # The page is Task-scoped and unfiltered here.
        assert "task_id = %s" in task_page_sql
        assert "event_type = %s" not in task_page_sql
        assert task_page_params is not None
        assert task_page_params[0] == _TASK_ID
        assert task_page_params[-1] == 51

    def test_a_foreign_or_absent_task_is_the_uniform_not_found_outcome(self) -> None:
        conn = ScriptedConnection()
        _script_workspace_guard(conn)
        conn.on_repeat(_sql_has("from openorc.tasks"), None)

        with pytest.raises(NotFoundError):
            event_reads.list_task_events_for_owner(
                _pool(conn),
                profile_id=_PROFILE_ID,
                workspace_id=_WORKSPACE_ID,
                task_id=_TASK_ID,
            )
        assert not any("from openorc.workflow_events" in sql for sql, _ in conn.executed)


class TestFilterValidation:
    def test_malformed_commands_fail_before_any_database_read(self) -> None:
        conn = ScriptedConnection()
        naive = datetime(2026, 11, 1, 0, 0, 0)
        overrides: list[dict[str, Any]] = [
            {"profile_id": "not-a-uuid"},
            {"workspace_id": 42},
            {"event_type": "not-a-type"},
            {"actor_type": "human"},
            {"from_created_at": naive},
            {"to_created_at": naive},
            {"limit": 0},
            {"limit": event_reads.EVENT_PAGE_LIMIT_MAX + 1},
            {"limit": True},
        ]
        for override in overrides:
            command: dict[str, Any] = {
                "profile_id": _PROFILE_ID,
                "workspace_id": _WORKSPACE_ID,
                **override,
            }
            with pytest.raises(InvalidCommandError):
                event_reads.list_workspace_events_for_owner(_pool(conn), **command)
        assert conn.executed == []

    def test_a_reversed_time_range_is_a_typed_invalid_command(self) -> None:
        conn = ScriptedConnection()
        with pytest.raises(InvalidCommandError):
            event_reads.list_workspace_events_for_owner(
                _pool(conn),
                profile_id=_PROFILE_ID,
                workspace_id=_WORKSPACE_ID,
                from_created_at=_OBSERVED,
                to_created_at=_OBSERVED - timedelta(seconds=1),
            )
        assert conn.executed == []

    def test_non_utc_bounds_are_normalized_and_never_rejected(self) -> None:
        conn = ScriptedConnection()
        _script_workspace_guard(conn)
        _script_read_snapshot(conn)
        conn.on_repeat(_sql_has("from openorc.workflow_events"), [])

        offset_bound = (_OBSERVED + timedelta(hours=2)).astimezone(timezone(timedelta(hours=2)))
        events, next_cursor = event_reads.list_workspace_events_for_owner(
            _pool(conn),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            from_created_at=offset_bound,
            to_created_at=offset_bound + timedelta(hours=1),
        )

        assert events == []
        assert next_cursor is None
        page_sql, page_params = next(
            (sql, params) for sql, params in conn.executed if "from openorc.workflow_events" in sql
        )
        assert "created_at >= %s" in page_sql
        assert "created_at < %s" in page_sql
        assert page_params is not None
        # Both bounds UTC-normalized (the offset bound is _OBSERVED + 2h).
        assert page_params[1] == _OBSERVED + timedelta(hours=2)


class TestSafePublicProjection:
    def test_established_safe_context_values_are_preserved_verbatim(self) -> None:
        established: list[tuple[WorkflowEventType, dict[str, object]]] = [
            (
                WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
                {"setting": "review_iteration_limit", "previous": 5, "new": 7},
            ),
            (
                WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
                {"setting": "default_task_budget_usd", "previous": None, "new": "12.50"},
            ),
            (
                WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
                {"setting": "workflow_role_binding", "role": "producer"},
            ),
            (
                WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
                {"setting": "connection", "connection_id": str(uuid.uuid4())},
            ),
            (WorkflowEventType.OWNER_GATE_RESOLVED, {"outcome": "approved"}),
            (
                WorkflowEventType.OWNER_GATE_RESOLVED,
                {
                    "outcome": "settled",
                    "review_iteration_id": str(uuid.uuid4()),
                    "review_outcome": "accepted",
                },
            ),
            (
                WorkflowEventType.OWNER_GATE_RESOLVED,
                {
                    "outcome": "approved",
                    "cost_effective_threshold_usd": "3.75",
                    "cost_observed_usd": "1.25",
                    "cost_authorized_budget_usd": "25.00",
                },
            ),
            (
                WorkflowEventType.OWNER_REVIEWER_DISCUSSION_MESSAGE,
                {"direction": "owner_to_reviewer"},
            ),
            (WorkflowEventType.EXECUTION_FAILED, {"failure_kind": "stale_authority"}),
            (WorkflowEventType.PLAN_REVIEWED, {"outcome": "accepted"}),
            (WorkflowEventType.PR_REVIEWED, {"outcome": "changes_requested"}),
            (WorkflowEventType.MERGE_REQUESTED, {"owner_gate_id": str(uuid.uuid4())}),
            (
                WorkflowEventType.TASK_RELATIONSHIP_SYNCED,
                {"repository_id": str(uuid.uuid4()), "github_issue_id": 42},
            ),
        ]
        for event_type, context in established:
            event = _domain_event(event_type=event_type, context=context)
            assert event_reads.project_public_context(event) == context

    def test_unknown_context_keys_are_omitted(self) -> None:
        event = _domain_event(
            event_type=WorkflowEventType.OWNER_GATE_RESOLVED,
            context={"outcome": "approved", "summary": "long free prose", "extra": [1, 2]},
        )
        assert event_reads.project_public_context(event) == {"outcome": "approved"}

    def test_unsafe_values_under_allowlisted_keys_are_omitted(self) -> None:
        unsafe: list[tuple[WorkflowEventType, dict[str, object]]] = [
            # A non-canonical token under a closed-vocabulary key.
            (WorkflowEventType.OWNER_GATE_RESOLVED, {"outcome": "<script>alert(1)</script>"}),
            (WorkflowEventType.PLAN_REVIEWED, {"outcome": "meh"}),
            # A non-UUID value under an identity-shaped key.
            (WorkflowEventType.MERGE_REQUESTED, {"owner_gate_id": "not-a-uuid"}),
            (WorkflowEventType.TASK_RELATIONSHIP_SYNCED, {"repository_id": 42}),
            # A string where the established shape is an exact integer.
            (WorkflowEventType.TASK_RELATIONSHIP_SYNCED, {"github_issue_id": "42"}),
            # A non-finite decimal string under a money-shaped key.
            (WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED, {"previous": "NaN"}),
            (WorkflowEventType.OWNER_GATE_RESOLVED, {"cost_observed_usd": "Infinity"}),
            # Free-form/error-shaped content under the bounded-token key.
            (
                WorkflowEventType.EXECUTION_FAILED,
                {"failure_kind": "x" * (event_reads._SAFE_TOKEN_MAX_LENGTH + 1)},
            ),
            (WorkflowEventType.EXECUTION_FAILED, {"failure_kind": "stack trace:\nboom"}),
            (WorkflowEventType.EXECUTION_FAILED, {"failure_kind": "  padded  "}),
            (WorkflowEventType.EXECUTION_FAILED, {"failure_kind": 7}),
        ]
        for event_type, context in unsafe:
            event = _domain_event(event_type=event_type, context=context)
            assert event_reads.project_public_context(event) == {}

    def test_an_event_type_with_no_context_projects_the_empty_object(self) -> None:
        # The lifecycle facts (task_created, task_started, planning_started,
        # terminal events, ...) carry no context by design; a stored context
        # shape outside the vocabulary projects to nothing.
        event = _domain_event(event_type=WorkflowEventType.TASK_STARTED, context={"surprise": 1})
        assert event_reads.project_public_context(event) == {}
        no_context = _domain_event(event_type=WorkflowEventType.TASK_CREATED)
        assert event_reads.project_public_context(no_context) == {}

    def test_actor_and_subject_provenance_are_preserved_exactly_as_stored(self) -> None:
        gate_id = uuid.uuid4()
        conn = ScriptedConnection()
        _script_workspace_guard(conn)
        _script_read_snapshot(conn)
        conn.on_repeat(
            _sql_has("from openorc.workflow_events"),
            [
                _row_event(
                    event_type=WorkflowEventType.OWNER_GATE_RESOLVED,
                    actor_type="owner",
                    actor_id=str(_PROFILE_ID),
                    subject_type="owner_gate",
                    subject_id=gate_id,
                    context={"outcome": "approved"},
                ),
                _row_event(
                    actor_type="github",
                    actor_id=None,
                    created_at=_OBSERVED + timedelta(seconds=1),
                ),
            ],
        )

        events, _ = event_reads.list_workspace_events_for_owner(
            _pool(conn), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )

        assert events[0].actor_type == "owner"
        assert events[0].actor_id == str(_PROFILE_ID)
        assert events[0].subject_type == "owner_gate"
        assert events[0].subject_id == gate_id
        assert events[1].actor_type == "github"
        assert events[1].actor_id is None
        assert events[1].task_id == _TASK_ID
