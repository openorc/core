"""Repositories for TaskBlock persistence.

Explicit SQL repositories over the ``openorc`` schema for TaskBlock, the
durable blocking condition with its reason and recovery context (Phase 1,
issue #24). Rows map to transport-independent domain objects from
:mod:`openorc.domain.blocks`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

- Creation persists the settled reason vocabulary and the reason-specific
  recovery/continuation ``context`` (canonical JSON object through the
  domain canonicalizer and an explicit ``Jsonb`` adapter). ReviewLoop
  iteration-limit exhaustion is not a reason: it is represented through
  the Task's ``waiting_for_owner`` status plus a REVIEW_RESOLUTION
  OwnerGate, never as a TaskBlock.
- Resolution is one-shot: ``resolve_task_block`` applies only while the
  block is current (``resolved_at IS NULL``), stamping the semantic
  ``resolved_at`` exactly once. A resolved block remains historical and
  retains its reason and context intact.
- Recovery is reason/context-specific: persistence encodes no universal
  blocked-to-next-state transition. Which recovery applies, and when a
  block resolves, belong to later orchestration.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.blocks import (
    TaskBlock,
    TaskBlockDomainError,
    TaskBlockReason,
    canonical_task_block_context,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_task_block",
    "get_task_block",
    "list_current_task_blocks",
    "list_task_blocks",
    "resolve_task_block",
]

_TASK_BLOCK_COLUMNS = "id, workspace_id, task_id, reason, context, resolved_at, created_at"


def _task_block_from_row(row: Sequence[Any]) -> TaskBlock:
    return TaskBlock(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        reason=TaskBlockReason(row[3]),
        context=row[4],
        resolved_at=None if row[5] is None else normalize_utc(row[5]),
        created_at=normalize_utc(row[6]),
    )


def create_task_block(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    reason: TaskBlockReason,
    context: Any,
) -> TaskBlock:
    """Insert one current TaskBlock with its reason and recovery context.

    ``reason`` must be from the settled v1 vocabulary (an empty mapping is
    a valid context when no concrete recovery facts exist). The context is
    canonicalized through the domain (canonical JSON-object semantics) and
    written through the explicit ``Jsonb`` adapter. The block is created
    current (``resolved_at IS NULL``); recovery and resolution are later,
    orchestration-owned.
    """
    if not isinstance(reason, TaskBlockReason):
        raise TaskBlockDomainError(
            "create_task_block requires a TaskBlockReason from the settled v1 "
            "vocabulary; ReviewLoop exhaustion is not a TaskBlock reason"
        )
    canonical_context = canonical_task_block_context(context)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_blocks "
            "(workspace_id, task_id, reason, context) "
            "values (%s, %s, %s, %s) "
            f"returning {_TASK_BLOCK_COLUMNS}",
            (workspace_id, task_id, reason.value, Jsonb(dict(canonical_context))),
        ).fetchone()
    assert row is not None
    return _task_block_from_row(row)


def get_task_block(pool: DatabasePool, *, task_block_id: UUID) -> TaskBlock | None:
    """Return one TaskBlock by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_BLOCK_COLUMNS} from openorc.task_blocks where id = %s",
            (task_block_id,),
        ).fetchone()
    return None if row is None else _task_block_from_row(row)


def list_task_blocks(pool: DatabasePool, *, task_id: UUID) -> list[TaskBlock]:
    """List a Task's complete block history, in creation order.

    Every block of the Task — current and resolved alike — is retained:
    resolved blocks remain historical evidence with their reason and
    recovery context intact.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_BLOCK_COLUMNS} from openorc.task_blocks "
            "where task_id = %s order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_task_block_from_row(row) for row in rows]


def list_current_task_blocks(pool: DatabasePool, *, task_id: UUID) -> list[TaskBlock]:
    """List a Task's current (unresolved) blocks, in creation order."""
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_BLOCK_COLUMNS} from openorc.task_blocks "
            "where task_id = %s and resolved_at is null order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_task_block_from_row(row) for row in rows]


def resolve_task_block(pool: DatabasePool, *, task_block_id: UUID) -> TaskBlock | None:
    """Stamp one current block resolved, exactly once.

    The update applies only while the block is current (``resolved_at IS
    NULL``) and stamps the semantic ``resolved_at`` atomically with it. The
    block's reason and recovery context are retained untouched: a resolved
    block is historical evidence. ``None`` means the block is missing or
    already resolved (a rejected no-op that must not be retried blindly).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_blocks "
            "set resolved_at = now() "
            "where id = %s and resolved_at is null "
            f"returning {_TASK_BLOCK_COLUMNS}",
            (task_block_id,),
        ).fetchone()
    return None if row is None else _task_block_from_row(row)
