"""Deterministic tests for the WorkflowEvent coordination boundary (issue #56).

The ordinary suite cannot execute Postgres: a scripted fake connection seam
proves the typed actor context (safe OWNER identity — the canonical Profile
UUID; no tokens, display names, or prose as actor material) and each
coordinated event mapping's exact shape — event type, scope, subject
reference, and the small event-specific context. A failed event insertion
propagates so the paired canonical mutation can never stand alone; the real
all-or-nothing composition rollback and the database CHECK vocabulary are
proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import InvalidCommandError

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)

_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()


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
        # nested repository scope under it.
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
        raise AssertionError("coordination tests never close pools")


def _pool(
    results: list[tuple[Any, ...] | None | Exception],
) -> tuple[DatabasePool, ScriptedConnection]:
    conn = ScriptedConnection(results)
    return cast(DatabasePool, FakePool(conn)), conn


def _event_row(context: dict[str, object] | None = None) -> tuple[Any, ...]:
    """A canned `record_workflow_event` INSERT ... RETURNING row."""
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        None,
        "workspace_configuration_changed",
        "owner",
        str(_OWNER_PROFILE_ID),
        "workspace",
        _WORKSPACE_ID,
        context or {},
        _OBSERVED,
    )


class TestWorkflowActorContext:
    def test_owner_actor_carries_the_canonical_profile_uuid(self) -> None:
        actor = event_coordination.owner_actor(_OWNER_PROFILE_ID)
        assert actor.actor_type is WorkflowEventActor.OWNER
        assert actor.actor_id == str(_OWNER_PROFILE_ID)

    def test_owner_requires_a_uuid_actor_identity(self) -> None:
        # A GitHub login, display name, or email is not an OpenOrc actor id.
        for unsafe in ("octocat-github-login", "octo@example.com", "  "):
            with pytest.raises(InvalidCommandError):
                event_coordination.WorkflowActorContext(WorkflowEventActor.OWNER, unsafe)

    def test_owner_requires_an_actor_identity_at_all(self) -> None:
        with pytest.raises(InvalidCommandError):
            event_coordination.WorkflowActorContext(WorkflowEventActor.OWNER, None)

    def test_actor_id_must_be_a_nonblank_opaque_string_when_present(self) -> None:
        with pytest.raises(InvalidCommandError):
            event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, "")

    def test_actor_type_is_the_locked_vocabulary(self) -> None:
        with pytest.raises(InvalidCommandError):
            event_coordination.WorkflowActorContext("human", None)  # type: ignore[arg-type]

    def test_non_owner_actor_may_be_identity_free(self) -> None:
        actor = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)
        assert actor.actor_id is None


class TestReviewLimitEvent:
    def test_maps_the_workspace_scoped_configuration_change(self) -> None:
        pool, conn = _pool([_event_row()])
        event_coordination.record_review_iteration_limit_changed_event(
            pool,
            workspace_id=_WORKSPACE_ID,
            actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
            previous_limit=5,
            new_limit=7,
        )
        assert len(conn.executed) == 1
        sql, params = conn.executed[0]
        assert "insert into openorc.workflow_events" in sql
        assert params is not None
        assert params[:7] == (
            _WORKSPACE_ID,
            None,
            "workspace_configuration_changed",
            "owner",
            str(_OWNER_PROFILE_ID),
            "workspace",
            _WORKSPACE_ID,
        )
        assert params[7].obj == {"setting": "review_iteration_limit", "previous": 5, "new": 7}

    def test_rejects_non_integer_limits_before_any_write(self) -> None:
        pool, conn = _pool([])
        for bad in ("5", 1.5, True):
            with pytest.raises(InvalidCommandError):
                event_coordination.record_review_iteration_limit_changed_event(
                    pool,
                    workspace_id=_WORKSPACE_ID,
                    actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
                    previous_limit=bad,  # type: ignore[arg-type]
                    new_limit=7,
                )
        assert conn.executed == []


class TestGuidanceEvent:
    def test_context_identifies_only_the_setting_change(self) -> None:
        pool, conn = _pool([_event_row()])
        event_coordination.record_guidance_changed_event(
            pool,
            workspace_id=_WORKSPACE_ID,
            actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
        )
        assert len(conn.executed) == 1
        sql, params = conn.executed[0]
        assert "insert into openorc.workflow_events" in sql
        assert params is not None
        # No prose parameter exists to accept, so no prose can be copied:
        # the helper's signature is the structural guarantee.
        assert params[7].obj == {"setting": "guidance"}
        assert "some owner prose" not in repr(params)


class TestTaskTerminalEvent:
    def test_cancellation_maps_to_task_cancelled_without_a_subject_pair(self) -> None:
        pool, conn = _pool(
            [
                (
                    uuid.uuid4(),
                    _WORKSPACE_ID,
                    _TASK_ID,
                    "task_cancelled",
                    "owner",
                    str(_OWNER_PROFILE_ID),
                    None,
                    None,
                    {},
                    _OBSERVED,
                )
            ]
        )
        event_coordination.record_task_terminal_event(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
            terminal_status=TaskStatus.CANCELLED,
        )
        assert len(conn.executed) == 1
        sql, params = conn.executed[0]
        assert "insert into openorc.workflow_events" in sql
        assert params is not None
        assert params[:7] == (
            _WORKSPACE_ID,
            _TASK_ID,
            "task_cancelled",
            "owner",
            str(_OWNER_PROFILE_ID),
            None,
            None,
        )
        # The terminal archival event carries no context payload.
        assert params[7].obj == {}

    def test_completion_maps_to_task_completed(self) -> None:
        pool, conn = _pool(
            [
                (
                    uuid.uuid4(),
                    _WORKSPACE_ID,
                    _TASK_ID,
                    "task_completed",
                    "owner",
                    str(_OWNER_PROFILE_ID),
                    None,
                    None,
                    {},
                    _OBSERVED,
                )
            ]
        )
        event_coordination.record_task_terminal_event(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
            terminal_status=TaskStatus.COMPLETED,
        )
        assert conn.executed[0][1] is not None
        assert conn.executed[0][1][2] == "task_completed"

    def test_nonterminal_status_is_never_fabricated_into_an_event(self) -> None:
        pool, conn = _pool([])
        with pytest.raises(InvalidCommandError):
            event_coordination.record_task_terminal_event(
                pool,
                workspace_id=_WORKSPACE_ID,
                task_id=_TASK_ID,
                actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
                terminal_status=TaskStatus.IMPLEMENTING,
            )
        assert conn.executed == []


class TestOwnerGateResolvedEvent:
    def test_maps_the_gate_subject_over_the_task_scope_with_the_outcome(self) -> None:
        pool, conn = _pool(
            [
                (
                    uuid.uuid4(),
                    _WORKSPACE_ID,
                    _TASK_ID,
                    "owner_gate_resolved",
                    "owner",
                    str(_OWNER_PROFILE_ID),
                    "owner_gate",
                    _GATE_ID,
                    {"outcome": "rejected"},
                    _OBSERVED,
                )
            ]
        )
        event_coordination.record_owner_gate_resolved_event(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_ID,
            actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
            outcome=OwnerGateStatus.REJECTED,
        )
        assert len(conn.executed) == 1
        sql, params = conn.executed[0]
        assert "insert into openorc.workflow_events" in sql
        assert params is not None
        assert params[0] == _WORKSPACE_ID
        assert params[1] == _TASK_ID
        assert params[2] == "owner_gate_resolved"
        assert params[5] == "owner_gate"
        assert params[6] == _GATE_ID
        assert params[7].obj == {"outcome": "rejected"}

    def test_pending_outcome_is_never_an_event(self) -> None:
        pool, conn = _pool([])
        with pytest.raises(InvalidCommandError):
            event_coordination.record_owner_gate_resolved_event(
                pool,
                workspace_id=_WORKSPACE_ID,
                task_id=_TASK_ID,
                owner_gate_id=_GATE_ID,
                actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
                outcome=OwnerGateStatus.PENDING,
            )
        assert conn.executed == []


class TestEventInsertionFailure:
    def test_failure_propagates_for_the_calling_composition(self) -> None:
        """The event insert is the last coordinated step; its failure must be
        observable by the service composition so the canonical mutation rolls
        back with it (real rollback proven by the integration suite)."""
        pool, conn = _pool([RuntimeError("event insertion failed")])
        with pytest.raises(RuntimeError, match="event insertion failed"):
            event_coordination.record_guidance_changed_event(
                pool,
                workspace_id=_WORKSPACE_ID,
                actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
            )
        assert len(conn.executed) == 1
