"""Bounded per-collection history reads for the authorized Task history (issue
#376).

The separate workflow-history endpoint's read surface: every potentially
large record family of one Task — plan revisions, review loops, review
iterations, OwnerGates, Executions, TaskBlocks, runtime requests, the
intent evidence, the detached-turn receipts, and the runtime send-claim
attempt ledger — reads as a bounded, deterministically ordered keyset page
with its continuation key, so a client loads a long history without
unbounded aggregates or re-fetching unrelated collections. No new history
store, no event sourcing, no transcript history: these are the existing
durable records, paged.

The reads compose through the established transaction-composition seam: the
service opens ONE ``composed_transaction`` and passes the transaction-scoped
pool view. Database-only and side-effect free; the Task/Workspace scope is
enforced by the queries, so a foreign Task is indistinguishable from
addressing something absent.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from openorc.domain.blocks import TaskBlock
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.executions import Execution
from openorc.domain.gates import OwnerGate
from openorc.domain.intents import (
    TaskInterruptIntent,
    TaskPullRequestCloseIntent,
    TaskResumeIntent,
)
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import ReviewIteration, ReviewLoop
from openorc.domain.runtime_requests import RuntimeRequest
from openorc.domain.send_claims import TaskRuntimeSendClaim
from openorc.persistence.blocks import _TASK_BLOCK_COLUMNS, _task_block_from_row
from openorc.persistence.detached_turns import (
    _RECEIPT_COLUMNS,
    _receipt_from_row,
)
from openorc.persistence.executions import _EXECUTION_COLUMNS, _execution_from_row
from openorc.persistence.gates import _OWNER_GATE_COLUMNS, _owner_gate_from_row
from openorc.persistence.intents import (
    _TASK_INTERRUPT_INTENT_COLUMNS,
    _TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS,
    _TASK_RESUME_INTENT_COLUMNS,
    _close_intent_from_row,
    _interrupt_intent_from_row,
    _resume_intent_from_row,
)
from openorc.persistence.planning import (
    _PLAN_REVISION_COLUMNS,
    _plan_revision_from_row,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.reviews import (
    _REVIEW_ITERATION_COLUMNS,
    _REVIEW_LOOP_COLUMNS,
    _review_iteration_from_row,
    _review_loop_from_row,
)
from openorc.persistence.runtime_requests import (
    _RUNTIME_REQUEST_COLUMNS,
    _runtime_request_from_row,
)
from openorc.persistence.send_claims import _CLAIM_COLUMNS, _claim_from_row
from openorc.persistence.tasks import TaskDomainError, normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "list_task_history_owner_gates_page",
    "list_task_history_executions_page",
    "list_task_history_intents_page",
    "list_task_history_plan_revisions_page",
    "list_task_history_receipts_page",
    "list_task_history_review_iterations_page",
    "list_task_history_review_loops_page",
    "list_task_history_runtime_requests_page",
    "list_task_history_send_claims_page",
    "list_task_history_task_blocks_page",
]


def _require_scope(workspace_id: UUID, task_id: UUID) -> None:
    if not isinstance(workspace_id, UUID):
        raise TaskDomainError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")


def _require_page_args(limit: int, after_key: Sequence[Any] | None, key_size: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise TaskDomainError("the history page limit must be a positive integer")
    if after_key is not None and len(after_key) != key_size:
        raise TaskDomainError("the history page cursor must carry the full key")


def list_task_history_plan_revisions_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_created_at: datetime | None = None,
    after_plan_revision_id: UUID | None = None,
    limit: int,
) -> list[PlanRevision]:
    """Return one bounded page of the Task's immutable plan revisions (#376)."""
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_created_at, after_plan_revision_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_created_at is not None:
        predicate += " and (created_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_created_at), after_plan_revision_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_PLAN_REVISION_COLUMNS} from openorc.plan_revisions "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_plan_revision_from_row(row) for row in rows]


def list_task_history_review_loops_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_created_at: datetime | None = None,
    after_review_loop_id: UUID | None = None,
    limit: int,
) -> list[ReviewLoop]:
    """Return one bounded page of the Task's review loops (#376)."""
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_created_at, after_review_loop_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_created_at is not None:
        predicate += " and (created_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_created_at), after_review_loop_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_REVIEW_LOOP_COLUMNS} from openorc.review_loops "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_review_loop_from_row(row) for row in rows]


def list_task_history_review_iterations_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_review_loop_id: UUID | None = None,
    after_iteration_number: int | None = None,
    after_review_iteration_id: UUID | None = None,
    limit: int,
) -> list[ReviewIteration]:
    """Return one bounded page of the Task's review iterations (#376).

    Ordered by the per-loop ``(review_loop_id, iteration_number)`` identity —
    the settled v1 iteration numbering — advanced by the full key.
    """
    _require_scope(workspace_id, task_id)
    _require_page_args(
        limit, (after_review_loop_id, after_iteration_number, after_review_iteration_id), 3
    )
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_review_loop_id is not None:
        predicate += " and (review_loop_id, iteration_number, id) > (%s, %s, %s)"
        params.extend([after_review_loop_id, after_iteration_number, after_review_iteration_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_REVIEW_ITERATION_COLUMNS} from openorc.review_iterations "
            f"where {predicate} order by review_loop_id, iteration_number, id limit %s",
            tuple(params),
        ).fetchall()
    return [_review_iteration_from_row(row) for row in rows]


def list_task_history_owner_gates_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_created_at: datetime | None = None,
    after_owner_gate_id: UUID | None = None,
    limit: int,
) -> list[OwnerGate]:
    """Return one bounded page of the Task's complete gate history (#376)."""
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_created_at, after_owner_gate_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_created_at is not None:
        predicate += " and (created_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_created_at), after_owner_gate_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_OWNER_GATE_COLUMNS} from openorc.owner_gates "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_owner_gate_from_row(row) for row in rows]


def list_task_history_executions_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_execution_number: int | None = None,
    after_execution_id: UUID | None = None,
    limit: int,
) -> list[Execution]:
    """Return one bounded page of the Task's attempt records (#376).

    Ordered by the per-Task ``execution_number`` sequence — identity and
    history lookup, never exclusivity.
    """
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_execution_number, after_execution_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_execution_number is not None:
        predicate += " and (execution_number, id) > (%s, %s)"
        params.extend([after_execution_number, after_execution_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_EXECUTION_COLUMNS} from openorc.executions "
            f"where {predicate} order by execution_number, id limit %s",
            tuple(params),
        ).fetchall()
    return [_execution_from_row(row) for row in rows]


def list_task_history_task_blocks_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_created_at: datetime | None = None,
    after_task_block_id: UUID | None = None,
    limit: int,
) -> list[TaskBlock]:
    """Return one bounded page of the Task's block history (#376)."""
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_created_at, after_task_block_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_created_at is not None:
        predicate += " and (created_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_created_at), after_task_block_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_BLOCK_COLUMNS} from openorc.task_blocks "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_task_block_from_row(row) for row in rows]


def list_task_history_runtime_requests_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_created_at: datetime | None = None,
    after_runtime_request_id: UUID | None = None,
    limit: int,
) -> list[RuntimeRequest]:
    """Return one bounded page of the Task's runtime requests (#376)."""
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_created_at, after_runtime_request_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_created_at is not None:
        predicate += " and (created_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_created_at), after_runtime_request_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_RUNTIME_REQUEST_COLUMNS} from openorc.runtime_requests "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_runtime_request_from_row(row) for row in rows]


def list_task_history_intents_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_created_at: datetime | None = None,
    after_intent_id: UUID | None = None,
    limit: int,
) -> tuple[list[TaskInterruptIntent], list[TaskResumeIntent], TaskPullRequestCloseIntent | None]:
    """Return one bounded page of the Task's intent evidence (#376).

    The interrupt and resume intents page on the shared ``(created_at, id)``
    key; the Task's PR-close obligation is a single record (the latest) and
    rides the same page bound.
    """
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_created_at, after_intent_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_created_at is not None:
        predicate += " and (created_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_created_at), after_intent_id])
    params.append(limit)
    with transaction(pool) as conn:
        interrupt_rows = conn.execute(
            f"select {_TASK_INTERRUPT_INTENT_COLUMNS} from openorc.task_interrupt_intents "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
        resume_rows = conn.execute(
            f"select {_TASK_RESUME_INTENT_COLUMNS} from openorc.task_resume_intents "
            f"where {predicate} order by created_at, id limit %s",
            tuple(params),
        ).fetchall()
        close_row = conn.execute(
            f"select {_TASK_PULL_REQUEST_CLOSE_INTENT_COLUMNS} "
            "from openorc.task_pull_request_close_intents where task_id = %s "
            "order by created_at desc, id desc limit 1",
            (task_id,),
        ).fetchone()
    return (
        [_interrupt_intent_from_row(row) for row in interrupt_rows],
        [_resume_intent_from_row(row) for row in resume_rows],
        None if close_row is None else _close_intent_from_row(close_row),
    )


def list_task_history_receipts_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_claimed_at: datetime | None = None,
    after_receipt_id: UUID | None = None,
    limit: int,
) -> list[TaskDetachedTurnReceipt]:
    """Return one bounded page of the Task's detached-turn receipts (#376)."""
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_claimed_at, after_receipt_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_claimed_at is not None:
        predicate += " and (claimed_at, id) < (%s, %s)"
        params.extend([normalize_utc(after_claimed_at), after_receipt_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            f"where {predicate} order by claimed_at desc, id desc limit %s",
            tuple(params),
        ).fetchall()
    return [_receipt_from_row(row) for row in rows]


def list_task_history_send_claims_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    after_claimed_at: datetime | None = None,
    after_send_claim_id: UUID | None = None,
    limit: int,
) -> list[TaskRuntimeSendClaim]:
    """Return one bounded page of the Task's runtime send-claim ledger (#376).

    The Level-1 attempt evidence: every durable one-shot send claim — the
    claimed outstanding facts and the terminal outcome records alike — is
    the attempt/recovery lifecycle's existing durable ledger.
    """
    _require_scope(workspace_id, task_id)
    _require_page_args(limit, (after_claimed_at, after_send_claim_id), 2)
    predicate = "workspace_id = %s and task_id = %s"
    params: list[Any] = [workspace_id, task_id]
    if after_claimed_at is not None:
        predicate += " and (claimed_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_claimed_at), after_send_claim_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_CLAIM_COLUMNS} from openorc.task_runtime_send_claims "
            f"where {predicate} order by claimed_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_claim_from_row(row) for row in rows]
