"""Durable atomicity of the exceptional block-gate foundation (issue #258).

The targeted real-Postgres checks the F3 seam owns:

- the installation commits atomically from an active Task: the unresolved
  TaskBlock, the pending exceptional OwnerGate bound to it as its only
  subject, the ``blocked`` transition with one rotated token, and both
  truthful audit events — no fictitious ``owner_gate_resolved``;
- the queue-entry timestamp clears when a queued Task is blocked;
- a prior current pending gate is superseded truthfully: the new gate
  becomes current while the prior gate row stays pending history with no
  Owner outcome and no ``decided_at``;
- the composite foreign key refuses a cross-Task/Workspace TaskBlock
  subject, and the subject-coherence CHECK keeps the ordinary subject
  forms and refuses a TaskBlock subject on an ordinary gate;
- a stale or replayed installation (obsolete token / moved-on current-gate
  identity) applies nothing: no block, no gate, no status change, no
  events, and the installed gate's block is unresolved at install.

No live GitHub, runtime, or Supabase Auth access. The suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence import tasks as task_repositories
from openorc.persistence.blocks import list_task_blocks
from openorc.persistence.gates import get_owner_gate, list_task_owner_gates
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import StaleOperationError
from openorc.services.task_exceptional_blocking import install_exceptional_block_gate
from tests.integration.test_task_planning_gate_change_request_persistence import (
    _insert_installation,
    _insert_profile,
    _insert_repository,
    _insert_task,
    _insert_workspace,
    _SingleConnectionPool,
)

pytestmark = pytest.mark.integration

_OWNER_PROFILE = uuid.uuid4()


def _event_types(conn: Connection[Any], task_id: uuid.UUID) -> list[str]:
    rows = conn.execute(
        "select event_type from openorc.workflow_events where task_id = %s order by created_at, id",
        (task_id,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _task_fixture(conn: Connection[Any], *, status: TaskStatus) -> dict[str, Any]:
    """One Workspace with a Task moved to the requested nonterminal status."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    moved = task_repositories.update_task_status(
        pool,
        task_id,
        expected_state_token=task.state_token,
        status=status,
    )
    assert moved is not None
    return {
        "pool": pool,
        "conn": conn,
        "workspace_id": workspace_id,
        "repository_id": repository_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "state_token": moved.state_token,
        "status": status,
    }


def _install(pool: DatabasePool, facts: dict[str, Any], **overrides: Any) -> Any:
    return install_exceptional_block_gate(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=overrides.get("expected_state_token", facts["state_token"]),
        expected_status=overrides.get("expected_status", facts["status"]),
        expected_current_owner_gate_id=overrides.get("expected_current_owner_gate_id"),
        gate_type=overrides.get("gate_type", OwnerGateType.BLOCK_RESOLUTION),
        block_reason=overrides.get("block_reason", TaskBlockReason.OWNER_ACTION_REQUIRED),
        block_context=overrides.get("block_context", {"origin": "owner_abort"}),
        actor=event_coordination.owner_actor(facts["profile_id"]),
    )


def test_the_installation_commits_atomically_from_an_active_task(
    conn: Connection[Any],
) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    pool = facts["pool"]

    result = _install(pool, facts)

    # The fresh block is unresolved and carries its reason/context.
    blocks = list_task_blocks(pool, task_id=facts["task_id"])
    assert [block.id for block in blocks] == [result.task_block.id]
    assert blocks[0].resolved_at is None
    assert blocks[0].reason is TaskBlockReason.OWNER_ACTION_REQUIRED
    # The pending exceptional gate binds exactly that block as its subject.
    gate = get_owner_gate(pool, owner_gate_id=result.owner_gate.id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    assert gate.gate_type is OwnerGateType.BLOCK_RESOLUTION
    assert gate.task_block_id == result.task_block.id
    assert gate.plan_revision_id is None
    assert gate.subject_head_sha is None
    assert gate.task_pull_request_id is None
    # The Task is blocked on a rotated token with the gate installed current.
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.current_owner_gate_id == result.owner_gate.id
    assert task.state_token != facts["state_token"]
    assert task.state_token == result.task.state_token
    # Exactly the two truthful audit events committed with the composition.
    assert _event_types(conn, facts["task_id"]) == ["task_blocked", "owner_gate_created"]


def test_the_queued_timestamp_clears_when_a_queued_task_is_blocked(
    conn: Connection[Any],
) -> None:
    facts = _task_fixture(conn, status=TaskStatus.QUEUED)
    pool = facts["pool"]
    queued = task_repositories.get_task(pool, facts["task_id"])
    assert queued is not None and queued.queued_at is not None
    facts["state_token"] = queued.state_token

    result = _install(pool, facts, expected_status=TaskStatus.QUEUED)

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.queued_at is None
    assert task.state_token == result.task.state_token


def test_a_prior_current_pending_gate_is_superseded_without_a_false_outcome(
    conn: Connection[Any],
) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    pool = facts["pool"]
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
    facts["state_token"] = current.state_token
    facts["status"] = TaskStatus.WAITING_FOR_OWNER

    result = _install(
        pool,
        facts,
        expected_status=TaskStatus.WAITING_FOR_OWNER,
        expected_current_owner_gate_id=prior_gate_id,
    )

    # The new exceptional gate is the Task's current gate; the prior gate
    # is untouched pending history with no manufactured Owner outcome.
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.current_owner_gate_id == result.owner_gate.id
    prior = get_owner_gate(pool, owner_gate_id=prior_gate_id)
    assert prior is not None
    assert prior.status is OwnerGateStatus.PENDING
    assert prior.decided_at is None
    # Exactly the two truthful events: no fictitious owner_gate_resolved.
    assert _event_types(conn, facts["task_id"]) == ["task_blocked", "owner_gate_created"]
    assert len(list_task_owner_gates(pool, task_id=facts["task_id"])) == 2


def test_the_task_block_subject_fk_refuses_a_cross_task_block(
    conn: Connection[Any],
) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    other_task_id = _insert_task(
        conn, workspace_id=facts["workspace_id"], repository_id=facts["repository_id"]
    )
    other_block_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_blocks (id, workspace_id, task_id, reason, context) "
        "values (%s, %s, %s, 'runtime_failure', '{}')",
        (other_block_id, facts["workspace_id"], other_task_id),
    )

    # The gate's TaskBlock subject must be the same Task and Workspace.
    with pytest.raises(ForeignKeyViolation):
        conn.execute(
            "insert into openorc.owner_gates "
            "(id, workspace_id, task_id, gate_type, status, task_block_id) "
            "values (%s, %s, %s, 'block_resolution', 'pending', %s)",
            (uuid.uuid4(), facts["workspace_id"], facts["task_id"], other_block_id),
        )

    # An ordinary gate never carries a TaskBlock subject.
    with pytest.raises(CheckViolation):
        conn.execute(
            "insert into openorc.owner_gates "
            "(id, workspace_id, task_id, gate_type, status, plan_revision_id, task_block_id) "
            "values (%s, %s, %s, 'implementation_authorization', 'pending', %s, %s)",
            (
                uuid.uuid4(),
                facts["workspace_id"],
                facts["task_id"],
                uuid.uuid4(),
                uuid.uuid4(),
            ),
        )


def test_a_stale_or_replayed_installation_applies_nothing(conn: Connection[Any]) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    pool = facts["pool"]
    obsolete_token = facts["state_token"]

    first = _install(pool, facts)

    # A replay of the obsolete token and the observed (empty) current-gate
    # identity is a stale operation that applies nothing at all.
    with pytest.raises(StaleOperationError):
        _install(
            pool,
            facts,
            expected_state_token=obsolete_token,
            expected_status=TaskStatus.IMPLEMENTING,
            expected_current_owner_gate_id=None,
        )

    blocks = list_task_blocks(pool, task_id=facts["task_id"])
    assert [block.id for block in blocks] == [first.task_block.id]
    assert blocks[0].resolved_at is None  # unresolved at install, still current
    gates = list_task_owner_gates(pool, task_id=facts["task_id"])
    assert [gate.id for gate in gates] == [first.owner_gate.id]
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.current_owner_gate_id == first.owner_gate.id
    assert task.state_token == first.task.state_token
    assert _event_types(conn, facts["task_id"]) == ["task_blocked", "owner_gate_created"]
