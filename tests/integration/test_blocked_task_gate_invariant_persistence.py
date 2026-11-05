"""Durable universal BLOCKED Task invariant enforcement (issue #282, F7c).

The targeted real-Postgres checks the cutover owns, run against the fully
migrated schema (the shared session fixture applies every committed
migration):

- the F3 installer's atomic multitable composition satisfies the committed-
  state invariant: its deferred trigger events validate the final blocked
  Task — one unresolved TaskBlock, one pending exceptional OwnerGate bound
  to it, the pointer installed;
- direct SQL refusals, each proven at the committed-state boundary via
  ``SET CONSTRAINTS ... IMMEDIATE``: clearing the blocked pointer, resolving
  the bound gate, rebinding the gate's TaskBlock subject away from the sole
  unresolved block, resolving the bound block, and deleting the bound block
  (FK-restricted);
- the universal backstop: a second unresolved TaskBlock for one Task is
  unrepresentable — and never a requirement that a Task have one;
- the deliberate converse exemptions: archival clears the pointer while the
  pending exceptional gate and its unresolved block remain as truthful
  terminal history, and a superseded pending ordinary gate stays legal
  history under the blocked Task's new exceptional gate.

No live GitHub, runtime, or Supabase Auth access. The suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation, RaiseException, UniqueViolation

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence import blocks as block_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.blocks import list_current_task_blocks
from openorc.persistence.gates import get_owner_gate
from openorc.persistence.pool import DatabasePool
from tests.integration.test_exceptional_block_gate_persistence import (
    _install,
    _task_fixture,
)

pytestmark = pytest.mark.integration


def _flush(conn: Connection[Any]) -> None:
    """Validate every pending deferred trigger event now, then restore deferral."""
    conn.execute("set constraints all immediate")
    conn.execute("set constraints all deferred")


def _expect_refusal(
    conn: Connection[Any], sql: LiteralString, params: tuple[Any, ...], match: str
) -> None:
    """Run one violating statement and prove the committed state refuses it.

    The violating mutation and the forced trigger flush run inside a
    savepoint: the refusal rolls the mutation back, leaving the seeded
    conforming state intact for the outer (rolled-back) test transaction.
    """
    with pytest.raises(RaiseException, match=match), conn.transaction():
        conn.execute(sql, params)
        _flush(conn)
    conn.execute("set constraints all deferred")


def _blocked_task(conn: Connection[Any]) -> tuple[dict[str, Any], Any]:
    """One current blocked Task installed through the real F3 composition."""
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    result = _install(facts["pool"], facts)
    task = task_repositories.get_task(facts["pool"], facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.current_owner_gate_id == result.owner_gate.id
    return facts, result


def test_the_installer_composition_satisfies_the_committed_invariant(
    conn: Connection[Any],
) -> None:
    facts, result = _blocked_task(conn)
    # The composition's own committed state passes the deferred triggers:
    # forcing the pending events to validate now must not raise.
    _flush(conn)

    assert list_current_task_blocks(facts["pool"], task_id=facts["task_id"])
    gate = get_owner_gate(facts["pool"], owner_gate_id=result.owner_gate.id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.gate_type is OwnerGateType.BLOCK_RESOLUTION


def test_the_invariant_accepts_the_level3_blocking_case_gate_types(
    conn: Connection[Any],
) -> None:
    # F8a (issue #286): a committed current BLOCKED Task whose current
    # pending exceptional gate is cost_limit or owner_abort, bound to its
    # sole unresolved block, satisfies the widened F7c invariant.
    for gate_type, reason in (
        (OwnerGateType.COST_LIMIT, TaskBlockReason.COST_LIMIT),
        (OwnerGateType.OWNER_ABORT, TaskBlockReason.OWNER_ACTION_REQUIRED),
    ):
        facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
        result = _install(facts["pool"], facts, gate_type=gate_type, block_reason=reason)
        # The composition's own committed state passes the deferred
        # triggers under the widened exceptional-type list.
        _flush(conn)
        task = task_repositories.get_task(facts["pool"], facts["task_id"])
        assert task is not None
        assert task.status is TaskStatus.BLOCKED
        assert task.current_owner_gate_id == result.owner_gate.id
        gate = get_owner_gate(facts["pool"], owner_gate_id=result.owner_gate.id)
        assert gate is not None
        assert gate.gate_type is gate_type
        assert gate.status is OwnerGateStatus.PENDING
        assert gate.task_block_id == result.task_block.id


def test_the_blocked_pointer_cannot_be_cleared_while_blocked(
    conn: Connection[Any],
) -> None:
    facts, _ = _blocked_task(conn)
    _expect_refusal(
        conn,
        "update openorc.tasks set current_owner_gate_id = null where id = %s",
        (facts["task_id"],),
        match="has no current OwnerGate pointer",
    )


def test_the_bound_gate_cannot_be_resolved_while_the_task_is_blocked(
    conn: Connection[Any],
) -> None:
    facts, result = _blocked_task(conn)
    _expect_refusal(
        conn,
        "update openorc.owner_gates set status = 'approved', decided_at = now() where id = %s",
        (result.owner_gate.id,),
        match="is not a pending exceptional gate",
    )


def test_the_bound_gate_cannot_lose_its_subject_binding(
    conn: Connection[Any],
) -> None:
    facts, result = _blocked_task(conn)
    # A resolved sibling block exists as history; rebinding the gate's
    # TaskBlock subject away from the sole unresolved block must refuse.
    conn.execute(
        "insert into openorc.task_blocks (workspace_id, task_id, reason, context, resolved_at) "
        "values (%s, %s, 'runtime_failure', '{}'::jsonb, now())",
        (facts["workspace_id"], facts["task_id"]),
    )
    sibling_row = conn.execute(
        "select id from openorc.task_blocks where task_id = %s and resolved_at is not null",
        (facts["task_id"],),
    ).fetchone()
    assert sibling_row is not None
    _expect_refusal(
        conn,
        "update openorc.owner_gates set task_block_id = %s where id = %s",
        (sibling_row[0], result.owner_gate.id),
        match="not bound to its sole unresolved task block",
    )


def test_the_bound_block_cannot_be_resolved_while_the_task_is_blocked(
    conn: Connection[Any],
) -> None:
    facts, result = _blocked_task(conn)
    _expect_refusal(
        conn,
        "update openorc.task_blocks set resolved_at = now() where id = %s",
        (result.task_block.id,),
        match="not bound to its sole unresolved task block",
    )


def test_a_second_unresolved_block_is_unrepresentable(conn: Connection[Any]) -> None:
    facts, _ = _blocked_task(conn)
    # The universal backstop is an immediate partial unique index: the second
    # unresolved TaskBlock is refused at statement time. It also never
    # requires a Task to have an unresolved block at all.
    with pytest.raises(UniqueViolation), conn.transaction():
        block_repositories.create_task_block(
            facts["pool"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            reason=TaskBlockReason.RUNTIME_FAILURE,
            context={},
        )


def test_the_bound_block_cannot_be_deleted_while_bound(conn: Connection[Any]) -> None:
    facts, result = _blocked_task(conn)
    # Deleting the bound block of a current blocked Task's current gate is
    # refused at the committed-state boundary by whichever deferred check
    # fires first — the invariant trigger (the blocked Task would lose its
    # exact exceptional authority) or the NO ACTION DEFERRABLE INITIALLY
    # DEFERRED gate-subject linkage FK. Either refusal proves the invalid
    # deletion cannot persist.
    with pytest.raises((ForeignKeyViolation, RaiseException)), conn.transaction():
        conn.execute(
            "delete from openorc.task_blocks where id = %s",
            (result.task_block.id,),
        )
        _flush(conn)


def test_archival_clears_the_pointer_and_retains_the_pending_gate_as_history(
    conn: Connection[Any],
) -> None:
    facts, result = _blocked_task(conn)
    # The exact archival shape archive_task applies on a confirmed GitHub
    # merge completion or trusted closed-unmerged cancellation: the pointer
    # clears and the pending exceptional gate stays as truthful history.
    archived = task_repositories.archive_task(
        facts["pool"],
        facts["task_id"],
        expected_state_token=result.task.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert archived is not None
    assert archived.status is TaskStatus.CANCELLED
    assert archived.current_owner_gate_id is None
    # The converse invariant is deliberately not imposed: the committed state
    # — archived Task, still-pending exceptional gate, still-unresolved
    # block — must validate cleanly.
    _flush(conn)
    gate = get_owner_gate(facts["pool"], owner_gate_id=result.owner_gate.id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    blocks = list_current_task_blocks(facts["pool"], task_id=facts["task_id"])
    assert [block.id for block in blocks] == [result.task_block.id]


def test_a_pending_superseded_ordinary_gate_remains_legal_history(
    conn: Connection[Any],
) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    pool = cast(DatabasePool, facts["pool"])
    prior_gate_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.owner_gates "
        "(id, workspace_id, task_id, gate_type, status, subject_head_sha) "
        "values (%s, %s, %s, 'pr_authorization', 'pending', %s)",
        (prior_gate_id, facts["workspace_id"], facts["task_id"], "a" * 40),
    )
    conn.execute(
        "update openorc.tasks set status = 'waiting_for_owner', "
        "current_owner_gate_id = %s where id = %s",
        (prior_gate_id, facts["task_id"]),
    )
    current = task_repositories.get_task(pool, facts["task_id"])
    assert current is not None and current.current_owner_gate_id == prior_gate_id

    result = _install(
        pool,
        facts,
        expected_status=TaskStatus.WAITING_FOR_OWNER,
        expected_current_owner_gate_id=prior_gate_id,
    )
    _flush(conn)

    prior = get_owner_gate(pool, owner_gate_id=prior_gate_id)
    assert prior is not None
    assert prior.status is OwnerGateStatus.PENDING
    assert prior.decided_at is None
    assert result.task.current_owner_gate_id == result.owner_gate.id
