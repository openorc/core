"""Durable atomicity of the Owner cancellation over an exceptional BLOCKED gate (issue #278).

The targeted real-Postgres checks the shared cancellation seam owns over
the #258 exceptional blocking foundation:

- the one shared Owner cancellation composition resolves a safely settled
  ``BLOCKED`` Task's exact current exceptional gate ``CANCELLED`` with its
  ``decided_at`` stamp, archives the Task terminal ``CANCELLED`` through
  the guarded archival primitive on the post-resolution rotated token,
  clears the current-gate pointer, and commits exactly the gate-resolution
  and terminal audit events — all atomically;
- the gate's bound TaskBlock stays unresolved history and the archival
  never rewrites it;
- a stale replay with the pre-cancellation token applies nothing.

No live GitHub, runtime, or Supabase Auth access. The suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence import tasks as task_repositories
from openorc.persistence.blocks import list_task_blocks
from openorc.persistence.gates import get_owner_gate
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import StaleOperationError
from openorc.services.task_cancellation import cancel_task_composition
from tests.integration.test_exceptional_block_gate_persistence import (
    _event_counts,
    _install,
    _task_fixture,
)

pytestmark = pytest.mark.integration


def _blocked_facts(conn: Connection[Any]) -> dict[str, Any]:
    """One Workspace whose Task is exceptionally blocked with a current gate."""
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    pool = cast(DatabasePool, facts["pool"])
    installation = _install(pool, facts)
    facts["installation"] = installation
    facts["state_token"] = installation.task.state_token
    return facts


def test_the_owner_cancellation_resolves_the_exceptional_gate_and_archives_atomically(
    conn: Connection[Any],
) -> None:
    facts = _blocked_facts(conn)
    pool = cast(DatabasePool, facts["pool"])
    installation = facts["installation"]
    task_id = cast(uuid.UUID, facts["task_id"])

    composition = cancel_task_composition(
        pool,
        profile_id=cast(uuid.UUID, facts["profile_id"]),
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=task_id,
        expected_state_token=cast(uuid.UUID, installation.task.state_token),
        owner_gate_id=installation.owner_gate.id,
    )

    assert composition.task.status is TaskStatus.CANCELLED
    assert composition.task.archived_at is not None
    assert composition.task.current_owner_gate_id is None
    assert composition.task.terminal_cause is None
    # The post-resolution rotated token is what the archival consumed.
    assert composition.task.state_token != installation.task.state_token
    assert composition.owner_gate is not None
    assert composition.owner_gate.status is OwnerGateStatus.CANCELLED
    assert composition.owner_gate.decided_at is not None
    gate = get_owner_gate(pool, owner_gate_id=installation.owner_gate.id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.CANCELLED
    assert gate.gate_type is OwnerGateType.BLOCK_RESOLUTION
    assert gate.task_block_id == installation.task_block.id
    # The gate's exact TaskBlock subject stays unresolved history.
    blocks = list_task_blocks(pool, task_id=task_id)
    assert [block.id for block in blocks] == [installation.task_block.id]
    assert blocks[0].resolved_at is None
    assert blocks[0].reason is TaskBlockReason.OWNER_ACTION_REQUIRED
    # Exactly the truthful audit stream: the installation pair, the
    # OWNER-attributed gate resolution, and the one terminal event.
    assert _event_counts(conn, task_id) == {
        "task_blocked": 1,
        "owner_gate_created": 1,
        "owner_gate_resolved": 1,
        "task_cancelled": 1,
    }


def test_a_stale_replay_after_the_cancellation_applies_nothing(
    conn: Connection[Any],
) -> None:
    facts = _blocked_facts(conn)
    pool = cast(DatabasePool, facts["pool"])
    installation = facts["installation"]
    task_id = cast(uuid.UUID, facts["task_id"])

    first = cancel_task_composition(
        pool,
        profile_id=cast(uuid.UUID, facts["profile_id"]),
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=task_id,
        expected_state_token=cast(uuid.UUID, installation.task.state_token),
        owner_gate_id=installation.owner_gate.id,
    )
    assert first.task.status is TaskStatus.CANCELLED

    # A replay with the pre-archival token is stale history: the archived
    # Task never mutates, the gate never re-resolves, and no event is
    # appended.
    with pytest.raises(StaleOperationError):
        cancel_task_composition(
            pool,
            profile_id=cast(uuid.UUID, facts["profile_id"]),
            workspace_id=cast(uuid.UUID, facts["workspace_id"]),
            task_id=task_id,
            expected_state_token=cast(uuid.UUID, installation.task.state_token),
            owner_gate_id=installation.owner_gate.id,
        )

    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    assert task.status is TaskStatus.CANCELLED
    assert task.state_token == first.task.state_token
    gate = get_owner_gate(pool, owner_gate_id=installation.owner_gate.id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.CANCELLED
    assert _event_counts(conn, task_id) == {
        "task_blocked": 1,
        "owner_gate_created": 1,
        "owner_gate_resolved": 1,
        "task_cancelled": 1,
    }
