"""Deterministic tests for the exceptional block-gate installation (issue #258).

Scripted SQL-connection seams prove the F3 foundation service on its
critical paths:

- a successful installation commits ONE atomic composition — the fresh
  unresolved TaskBlock, the pending exceptional OwnerGate bound to it as
  its only subject, the guarded current-gate pointer installation with the
  ``blocked`` transition and one ``state_token`` rotation, and exactly the
  two truthful audit events (``task_blocked``, ``owner_gate_created``)
  under the supplied actor — with the Task row locked first and every
  operation sharing one transaction-scoped connection;
- the ``queued_at`` timestamp clears when a queued Task is blocked;
- a prior current pending gate is superseded truthfully: the new gate
  becomes current, the prior gate row is never rewritten, and no
  fictitious ``owner_gate_resolved`` event exists;
- stale tokens, moved-on caller-observed status or current-gate identity,
  losing concurrent installations, and missing/out-of-scope subjects apply
  nothing;
- command shape (identity, exceptional gate type, eligible source status,
  gate/reason pairing, canonical JSON-object context, actor) is validated
  before any database access.

No live runtime or Postgres access.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.services import event_coordination
from openorc.services.errors import InvalidCommandError, NotFoundError, StaleOperationError
from openorc.services.task_exceptional_blocking import install_exceptional_block_gate
from tests.test_services_task_pr_review_decision import (
    _EVENT_INSERT,
    _OBSERVED,
    _event_returned_row,
    _event_types,
    _gate_row,
)
from tests.test_services_task_pr_review_dispatch import (
    _OWNER_PROFILE_ID,
    _STATE_TOKEN,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    ScriptedConnection,
    _count,
    _pool,
    _sql_has,
    _task_row,
)

_ROTATED_TOKEN = uuid4()

_GATE_READ = _sql_has("from openorc.owner_gates where id")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_TASK_BLOCK_UPDATE = _sql_has("update openorc.tasks", "status = 'blocked'")

_GATE_ID = uuid4()
_PRIOR_GATE_ID = uuid4()
_BLOCK_ID = uuid4()


def _blocked_task_row(**overrides: Any) -> tuple[Any, ...]:
    return _task_row(
        status="blocked",
        state_token=_ROTATED_TOKEN,
        current_gate_id=_GATE_ID,
        **overrides,
    )


def _block_row(
    *,
    block_id: Any = _BLOCK_ID,
    reason: str = "owner_action_required",
    context: Any = None,
) -> tuple[Any, ...]:
    return (
        block_id,
        _WORKSPACE_ID,
        _TASK_ID,
        reason,
        {} if context is None else context,
        None,
        _OBSERVED,
    )


def _exceptional_gate_row(gate_type: str = "block_resolution") -> tuple[Any, ...]:
    row = list(
        _gate_row(
            gate_id=_GATE_ID,
            gate_type=gate_type,
            head_sha=None,
            pr_id=None,
        )
    )
    row[8] = _BLOCK_ID  # the gate's TaskBlock subject
    return tuple(row)


def _script_success(
    conn: ScriptedConnection,
    *,
    gate_type: str,
    blocked_row: tuple[Any, ...] | None = None,
) -> None:
    conn.on(_TASK_READ_LOCKED, _task_row(status="implementing"))
    conn.on(_TASK_READ, _task_row(status="implementing"))
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_GATE_INSERT, _exceptional_gate_row(gate_type))
    conn.on(_TASK_BLOCK_UPDATE, blocked_row if blocked_row is not None else _blocked_task_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))


def _install(
    conn: ScriptedConnection,
    *,
    gate_type: Any = OwnerGateType.BLOCK_RESOLUTION,
    block_reason: Any = TaskBlockReason.OWNER_ACTION_REQUIRED,
    expected_status: Any = TaskStatus.IMPLEMENTING,
    expected_current_owner_gate_id: Any = None,
    expected_state_token: Any = _STATE_TOKEN,
    block_context: Any = None,
    **overrides: Any,
):
    return install_exceptional_block_gate(
        _pool(conn),
        workspace_id=overrides.get("workspace_id", _WORKSPACE_ID),
        task_id=overrides.get("task_id", _TASK_ID),
        expected_state_token=expected_state_token,
        expected_status=expected_status,
        expected_current_owner_gate_id=expected_current_owner_gate_id,
        gate_type=gate_type,
        block_reason=block_reason,
        block_context=block_context if block_context is not None else {"origin": "owner_abort"},
        actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
    )


def test_a_block_resolution_installation_commits_one_atomic_composition() -> None:
    conn = ScriptedConnection()
    _script_success(conn, gate_type="block_resolution")
    pool = _pool(conn)

    result = install_exceptional_block_gate(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        expected_status=TaskStatus.IMPLEMENTING,
        expected_current_owner_gate_id=None,
        gate_type=OwnerGateType.BLOCK_RESOLUTION,
        block_reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
        block_context={"origin": "owner_abort"},
        actor=event_coordination.owner_actor(_OWNER_PROFILE_ID),
    )

    # The created facts: one unresolved block, the pending exceptional gate
    # bound to it as its only subject, and the post-mutation blocked Task.
    assert result.task_block.id == _BLOCK_ID
    assert result.task_block.reason is TaskBlockReason.OWNER_ACTION_REQUIRED
    assert result.task_block.resolved_at is None
    assert result.owner_gate.id == _GATE_ID
    assert result.owner_gate.gate_type is OwnerGateType.BLOCK_RESOLUTION
    assert result.owner_gate.status is OwnerGateStatus.PENDING
    assert result.owner_gate.task_block_id == _BLOCK_ID
    assert result.owner_gate.plan_revision_id is None
    assert result.owner_gate.subject_head_sha is None
    assert result.owner_gate.task_pull_request_id is None
    assert result.task.status is TaskStatus.BLOCKED
    assert result.task.current_owner_gate_id == _GATE_ID
    assert result.task.state_token == _ROTATED_TOKEN
    # Exactly one transaction-scoped connection checkout for the whole
    # composition, and exactly seven statements: locked Task read,
    # classification read, block insert, gate insert, guarded blocking
    # update, and two audit events.
    assert cast(Any, pool).checkouts == 1
    assert _count(conn, _TASK_READ_LOCKED) == 1
    assert _count(conn, _TASK_READ) == 1
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _GATE_INSERT) == 1
    assert _count(conn, _TASK_BLOCK_UPDATE) == 1
    assert _count(conn, _EVENT_INSERT) == 2
    assert len(conn.executed) == 7
    # The Task row lock is the first statement.
    assert _TASK_READ_LOCKED(conn.executed[0][0], None)
    # The gate insert carries the exact TaskBlock subject.
    gate_sql, gate_params = next(
        (sql, params) for sql, params in conn.executed if _GATE_INSERT(sql, None)
    )
    assert gate_params is not None
    assert "task_block_id" in gate_sql
    assert gate_params[-1] == _BLOCK_ID
    # The guarded update binds the exact token/status/pointer identity and
    # rotates the token once.
    update_sql, update_params = next(
        (sql, params) for sql, params in conn.executed if _TASK_BLOCK_UPDATE(sql, None)
    )
    assert update_params is not None
    assert update_params == (_GATE_ID, _TASK_ID, _STATE_TOKEN, "implementing", None, _GATE_ID)
    assert update_sql.count("gen_random_uuid()") == 1
    # Exactly the two truthful audit events under the supplied OWNER actor.
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    event_params = [params for sql, params in conn.executed if _EVENT_INSERT(sql, None)]
    assert all(
        params is not None and params[3] == "owner" and params[4] == str(_OWNER_PROFILE_ID)
        for params in event_params
    )


def test_the_queued_timestamp_clears_when_a_queued_task_is_blocked() -> None:
    conn = ScriptedConnection()
    queued_row = list(_task_row(status="queued"))
    queued_row[12] = _OBSERVED  # queued_at set while queued
    queued_row = tuple(queued_row)
    conn.on(_TASK_READ_LOCKED, queued_row)
    conn.on(_TASK_READ, queued_row)
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_GATE_INSERT, _exceptional_gate_row())
    conn.on(_TASK_BLOCK_UPDATE, _blocked_task_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))

    result = _install(
        conn,
        expected_status=TaskStatus.QUEUED,
        block_reason=TaskBlockReason.CONNECTION_UNAVAILABLE,
    )

    assert result.task.status is TaskStatus.BLOCKED
    assert result.task.queued_at is None
    update_sql, update_params = next(
        (sql, params) for sql, params in conn.executed if _TASK_BLOCK_UPDATE(sql, None)
    )
    assert update_params is not None
    assert "queued_at = case when status = 'queued' then null else queued_at end" in update_sql
    assert update_params == (_GATE_ID, _TASK_ID, _STATE_TOKEN, "queued", None, _GATE_ID)


def test_a_prior_current_pending_gate_is_superseded_without_a_false_outcome() -> None:
    # The caller observed a prior current pending gate; the new exceptional
    # gate replaces it as current while the prior gate row is never
    # rewritten — it stays pending history with no terminal stamp and no
    # fictitious owner_gate_resolved event.
    conn = ScriptedConnection()
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(_GATE_READ, _gate_row(gate_id=_PRIOR_GATE_ID, gate_type="pr_authorization", pr_id=None))
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_GATE_INSERT, _exceptional_gate_row())
    conn.on(_TASK_BLOCK_UPDATE, _blocked_task_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))

    result = _install(
        conn,
        expected_status=TaskStatus.WAITING_FOR_OWNER,
        expected_current_owner_gate_id=_PRIOR_GATE_ID,
    )

    assert result.task.current_owner_gate_id == _GATE_ID
    assert result.task.status is TaskStatus.BLOCKED
    update_sql, update_params = next(
        (sql, params) for sql, params in conn.executed if _TASK_BLOCK_UPDATE(sql, None)
    )
    assert update_params == (
        _GATE_ID,
        _TASK_ID,
        _STATE_TOKEN,
        "waiting_for_owner",
        _PRIOR_GATE_ID,
        _GATE_ID,
    )
    # The prior gate row is never rewritten and no owner_gate_resolved
    # event is fabricated for the supersession.
    assert _count(conn, _sql_has("update openorc.owner_gates")) == 0
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]


def test_a_stale_token_applies_nothing() -> None:
    # A replayed/obsolete token is refused before any write: the locked
    # read and the classification read observe the moved-on token and the
    # composition raises with no block, gate, status change, or event.
    moved_on = uuid4()
    conn = ScriptedConnection()
    conn.on(_TASK_READ_LOCKED, _task_row(status="implementing", state_token=moved_on))
    conn.on(_TASK_READ, _task_row(status="implementing", state_token=moved_on))

    with pytest.raises(StaleOperationError):
        _install(conn)

    assert _count(conn, _TASK_READ_LOCKED) == 1
    assert len(conn.executed) == 2
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_moved_on_status_or_current_gate_identity_is_stale() -> None:
    # The caller-observed status no longer matches the locked durable row.
    conn = ScriptedConnection()
    conn.on(_TASK_READ_LOCKED, _task_row(status="reviewing"))
    conn.on(_TASK_READ, _task_row(status="reviewing"))
    with pytest.raises(StaleOperationError):
        _install(conn, expected_status=TaskStatus.IMPLEMENTING)
    assert len(conn.executed) == 2

    # The caller observed no current gate, but one is now installed.
    conn = ScriptedConnection()
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="implementing", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="implementing", current_gate_id=_PRIOR_GATE_ID),
    )
    with pytest.raises(StaleOperationError):
        _install(conn)
    assert len(conn.executed) == 2

    # The caller observed a specific current gate, but the pointer moved.
    conn = ScriptedConnection()
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    with pytest.raises(StaleOperationError):
        _install(
            conn,
            expected_status=TaskStatus.WAITING_FOR_OWNER,
            expected_current_owner_gate_id=_GATE_ID,
        )
    assert len(conn.executed) == 2


def test_a_prior_gate_that_lost_pending_status_is_stale() -> None:
    conn = ScriptedConnection()
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(
        _GATE_READ,
        _gate_row(
            gate_id=_PRIOR_GATE_ID, gate_type="pr_authorization", status="approved", pr_id=None
        ),
    )

    with pytest.raises(StaleOperationError):
        _install(
            conn,
            expected_status=TaskStatus.WAITING_FOR_OWNER,
            expected_current_owner_gate_id=_PRIOR_GATE_ID,
        )

    # The refused installation wrote nothing: no block, no gate, no Task
    # mutation, no event.
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _TASK_BLOCK_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_missing_or_out_of_scope_subjects_are_not_found() -> None:
    # A missing Task.
    conn = ScriptedConnection()
    conn.on(_TASK_READ_LOCKED, None)
    with pytest.raises(NotFoundError):
        _install(conn)
    assert len(conn.executed) == 1

    # A cross-Workspace Task is the same uniform not-found outcome.
    conn = ScriptedConnection()
    conn.on(_TASK_READ_LOCKED, _task_row(workspace_id=uuid4()))
    with pytest.raises(NotFoundError):
        _install(conn)
    assert len(conn.executed) == 1

    # The observed prior gate is missing or belongs to another Task.
    conn = ScriptedConnection()
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_PRIOR_GATE_ID),
    )
    conn.on(_GATE_READ, None)
    with pytest.raises(NotFoundError):
        _install(
            conn,
            expected_status=TaskStatus.WAITING_FOR_OWNER,
            expected_current_owner_gate_id=_PRIOR_GATE_ID,
        )
    assert _count(conn, _BLOCK_INSERT) == 0


def test_a_losing_concurrent_install_commits_nothing() -> None:
    # The durable backstop refused the guarded blocking write (a raced
    # winner serialized first): the composition rolls back — no block, no
    # gate, no Task mutation, no event survives it.
    conn = ScriptedConnection()
    conn.on(_TASK_READ_LOCKED, _task_row(status="implementing"))
    conn.on(_TASK_READ, _task_row(status="implementing"))
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_GATE_INSERT, _exceptional_gate_row())
    conn.on(_TASK_BLOCK_UPDATE, None)

    with pytest.raises(StaleOperationError):
        _install(conn)

    assert _count(conn, _TASK_BLOCK_UPDATE) == 1
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(conn.executed) == 5


def test_command_shape_is_validated_before_any_database_access() -> None:
    # Malformed identity, non-exceptional gate types, ineligible source
    # statuses, pairing violations, non-canonical contexts, and a bad actor
    # are all InvalidCommandError classified without touching the database.
    conn = ScriptedConnection()
    base = dict()
    with pytest.raises(InvalidCommandError):
        _install(conn, workspace_id="not-a-uuid", **base)  # type: ignore[arg-type]
    with pytest.raises(InvalidCommandError):
        _install(conn, task_id="not-a-uuid", **base)  # type: ignore[arg-type]
    with pytest.raises(InvalidCommandError):
        _install(conn, expected_state_token="not-a-uuid", **base)  # type: ignore[arg-type]
    with pytest.raises(InvalidCommandError):
        _install(conn, expected_current_owner_gate_id="not-a-uuid", **base)  # type: ignore[arg-type]
    # Ordinary gate types are not installable through this foundation.
    for gate_type in (
        OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        OwnerGateType.PR_AUTHORIZATION,
        OwnerGateType.MERGE_DECISION,
        OwnerGateType.REVIEW_RESOLUTION,
    ):
        with pytest.raises(InvalidCommandError):
            _install(conn, gate_type=gate_type, block_reason=TaskBlockReason.UNKNOWN, **base)
    with pytest.raises(InvalidCommandError):
        _install(conn, gate_type="chat_decision", block_reason=TaskBlockReason.UNKNOWN, **base)  # type: ignore[arg-type]
    # Ineligible source statuses.
    for status in (
        TaskStatus.READY_TO_PLAN,
        TaskStatus.BLOCKED,
        TaskStatus.CANCELLED,
        TaskStatus.COMPLETED,
    ):
        with pytest.raises(InvalidCommandError):
            _install(conn, expected_status=status, **base)
    with pytest.raises(InvalidCommandError):
        _install(conn, expected_status="planning", **base)  # type: ignore[arg-type]
    # Gate/reason pairing violations.
    with pytest.raises(InvalidCommandError):
        _install(
            conn,
            gate_type=OwnerGateType.GITHUB_ISSUE_CHANGE,
            block_reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
            **base,
        )
    with pytest.raises(InvalidCommandError):
        _install(
            conn,
            gate_type=OwnerGateType.GITHUB_STATE_MISMATCH,
            block_reason=TaskBlockReason.GITHUB_SOURCE_CHANGED,
            **base,
        )
    with pytest.raises(InvalidCommandError):
        _install(
            conn,
            gate_type=OwnerGateType.BLOCK_RESOLUTION,
            block_reason=TaskBlockReason.GITHUB_SOURCE_CHANGED,
            **base,
        )
    with pytest.raises(InvalidCommandError):
        _install(
            conn,
            gate_type=OwnerGateType.BLOCK_RESOLUTION,
            block_reason=TaskBlockReason.PR_CLOSED_UNMERGED,
            **base,
        )
    with pytest.raises(InvalidCommandError):
        _install(conn, block_reason="not_a_reason", **base)  # type: ignore[arg-type]
    # The context must be a canonical JSON object.
    for bad_context in (
        ["not", "a", "mapping"],
        {"nested": {1: "non-string-key"}},
        {"nan": float("nan")},
    ):
        with pytest.raises(InvalidCommandError):
            _install(conn, block_context=bad_context, **base)
    # The actor must be a WorkflowActorContext.
    with pytest.raises(InvalidCommandError):
        install_exceptional_block_gate(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            expected_status=TaskStatus.IMPLEMENTING,
            expected_current_owner_gate_id=None,
            gate_type=OwnerGateType.BLOCK_RESOLUTION,
            block_reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
            block_context={},
            actor="owner",  # type: ignore[arg-type]
        )
    assert conn.executed == []


def test_the_block_reason_pairing_accepts_the_applicable_reasons() -> None:
    # BLOCK_RESOLUTION accepts the operational and explicit-stop reasons
    # (including owner_action_required and cost_limit, and a caller-proven
    # stale_operation); GITHUB_ISSUE_CHANGE accepts exactly
    # github_source_changed; GITHUB_STATE_MISMATCH accepts exactly
    # stale_operation.
    for reason in (
        TaskBlockReason.OWNER_ACTION_REQUIRED,
        TaskBlockReason.COST_LIMIT,
        TaskBlockReason.STALE_OPERATION,
        TaskBlockReason.RUNTIME_FAILURE,
        TaskBlockReason.AGENT_SESSION_LOST,
        TaskBlockReason.CONNECTION_UNAVAILABLE,
        TaskBlockReason.INVALID_CREDENTIALS,
        TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN,
        TaskBlockReason.RUNTIME_REQUEST_REJECTED,
        TaskBlockReason.REVIEW_FAILURE,
        TaskBlockReason.UNKNOWN,
    ):
        conn = ScriptedConnection()
        _script_success(conn, gate_type="block_resolution")
        result = _install(conn, block_reason=reason)
        # The canned returned row carries a static reason; the durable fact
        # is proven on the INSERT parameters.
        insert_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
        assert insert_params is not None
        assert insert_params[2] == reason.value
        assert result.task.status is TaskStatus.BLOCKED
    for gate_type, reason in (
        (OwnerGateType.GITHUB_ISSUE_CHANGE, TaskBlockReason.GITHUB_SOURCE_CHANGED),
        (OwnerGateType.GITHUB_STATE_MISMATCH, TaskBlockReason.STALE_OPERATION),
    ):
        conn = ScriptedConnection()
        _script_success(conn, gate_type=gate_type.value)
        result = _install(conn, gate_type=gate_type, block_reason=reason)
        assert result.owner_gate.gate_type is gate_type
        # The durable reason/type facts are proven on the INSERT parameters.
        insert_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
        assert insert_params is not None
        assert insert_params[2] == reason.value
        gate_params = next(params for sql, params in conn.executed if _GATE_INSERT(sql, None))
        assert gate_params is not None
        assert gate_params[2] == gate_type.value
        assert gate_params[-1] == result.task_block.id
