"""The authorized Task workflow-history composition (issue #376).

``GET /workspaces/{workspace_id}/tasks/{task_id}/history`` returns
historical domain records, not reconstructed audit events: the Task's
plan revisions, review loops and their exact review iterations, OwnerGates,
Executions, TaskBlocks, runtime requests, the intent evidence, the
detached-turn receipts, and the runtime send-claim attempt ledger — the
existing durable records exposed through safe explicit typed projections
that preserve canonical identifiers, statuses, exact plan/PR/head links,
immutable plan contents, finalized review summaries/findings, relevant
execution results, and recorded decision/resolution timestamps.

Each potentially large collection is bounded independently and advanced by
its own opaque keyset continuation; a documented ``collections`` selection
lets a client load specific collections without unbounded aggregates or
re-fetching unrelated ones. History includes current records where
applicable and NEVER carries actionable menus on historical gates. No new
history store, no event sourcing, no transcript history — G2c (issue #377)
owns the WorkflowEvent audit/history surface. No raw secrets, runtime
payloads, prompts, or transcripts are ever exposed.
"""

from __future__ import annotations

import base64
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool
from openorc.persistence.task_history import (
    list_task_history_executions_page,
    list_task_history_intents_page,
    list_task_history_owner_gates_page,
    list_task_history_plan_revisions_page,
    list_task_history_receipts_page,
    list_task_history_review_iterations_page,
    list_task_history_review_loops_page,
    list_task_history_runtime_requests_page,
    list_task_history_send_claims_page,
    list_task_history_task_blocks_page,
)
from openorc.services.errors import InvalidCommandError
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "HISTORY_COLLECTION_PAGE_LIMIT",
    "HISTORY_COLLECTIONS",
    "HistoryCollection",
    "TaskHistory",
    "decode_history_cursor",
    "encode_history_cursor",
    "list_task_history_for_owner",
]

# The per-collection page bound: one authorized history request returns at
# most this many items per selected collection, advanced by that
# collection's opaque continuation cursor.
HISTORY_COLLECTION_PAGE_LIMIT = 50

# The documented collection-selection vocabulary (issue #376 §6).
HISTORY_COLLECTIONS: tuple[str, ...] = (
    "plan_revisions",
    "review_loops",
    "review_iterations",
    "owner_gates",
    "executions",
    "task_blocks",
    "runtime_requests",
    "interrupt_intents",
    "resume_intents",
    "pr_close_intent",
    "receipts",
    "send_claims",
)

_READS_TRACER_SCOPE = "openorc.services.task_history"
_HISTORY_SPAN_NAME = "task_history.list_task_history_for_owner"


@dataclass(frozen=True, slots=True)
class HistoryCollection:
    """One bounded history collection with its opaque continuation (#376).

    ``items`` are the collection's typed domain records for this page;
    ``next_cursor`` is the opaque per-collection continuation (``None`` when
    the page exhausted the collection); ``total`` is the collection's full
    durable count where the composition counts it, so a client distinguishes
    a complete page from a bounded one — never silently truncated.
    """

    name: str
    items: tuple[object, ...]
    next_cursor: str | None
    total: int | None


@dataclass(frozen=True, slots=True)
class TaskHistory:
    """The addressed Task's selected history collections (issue #376)."""

    task_id: UUID
    collections: tuple[HistoryCollection, ...]


def encode_history_cursor(collection: str, *key_parts: object) -> str:
    """Return the opaque per-collection keyset continuation cursor.

    Key parts join with ``|`` (never present inside ISO instants or UUIDs),
    so the decode splits the prefix from the parts unambiguously.
    """
    rendered = "|".join(
        part.isoformat() if isinstance(part, datetime) else str(part) for part in key_parts
    )
    raw = f"h:{collection}:{rendered}".encode()
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_history_cursor(cursor: str) -> tuple[str, tuple[str, ...]]:
    """Decode one opaque history cursor, or raise the typed invalid command."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        head, separator, rest = raw.partition(":")
        collection, separator, rest = rest.partition(":")
        if head != "h" or not separator:
            raise ValueError("unexpected cursor shape")
        if collection not in HISTORY_COLLECTIONS:
            raise ValueError("unknown collection")
        return collection, tuple(rest.split("|")) if rest else ()
    except (ValueError, UnicodeDecodeError) as error:
        raise InvalidCommandError(
            "the cursor is not a valid workflow-history cursor"
        ) from error


def _require_uuid_command(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _selected_collections(requested: Sequence[str] | None) -> tuple[str, ...]:
    """Validate the documented collection selection (issue #376)."""
    if requested is None:
        return HISTORY_COLLECTIONS
    if not requested:
        raise InvalidCommandError(
            "the collections selection must name at least one documented collection"
        )
    for name in requested:
        if name not in HISTORY_COLLECTIONS:
            raise InvalidCommandError(
                "the collections selection must use the documented collection vocabulary"
            )
    return tuple(dict.fromkeys(requested))


def _page_collection(
    tx_view: DatabasePool,
    *,
    collection: str,
    workspace_id: UUID,
    task_id: UUID,
    key: tuple[str, ...] | None,
    limit: int,
) -> HistoryCollection:
    """Page one history collection by its own deterministic key (#376)."""

    def _two_key_instant(parts: tuple[str, ...]) -> tuple[datetime, UUID] | None:
        if parts is None:
            return None
        return datetime.fromisoformat(parts[0]), UUID(parts[1])

    if collection == "plan_revisions":
        after = _two_key_instant(key)
        items = list_task_history_plan_revisions_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_plan_revision_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "review_loops":
        after = _two_key_instant(key)
        items = list_task_history_review_loops_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_review_loop_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "owner_gates":
        after = _two_key_instant(key)
        items = list_task_history_owner_gates_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_owner_gate_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "task_blocks":
        after = _two_key_instant(key)
        items = list_task_history_task_blocks_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_task_block_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "runtime_requests":
        after = _two_key_instant(key)
        items = list_task_history_runtime_requests_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_runtime_request_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "executions":
        after = (
            None
            if key is None
            else (int(key[0]), UUID(key[1]))
        )
        items = list_task_history_executions_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_execution_number=None if after is None else after[0],
            after_execution_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].execution_number, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "review_iterations":
        after = None if key is None else (UUID(key[0]), int(key[1]), UUID(key[2]))
        items = list_task_history_review_iterations_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_review_loop_id=None if after is None else after[0],
            after_iteration_number=None if after is None else after[1],
            after_review_iteration_id=None if after is None else after[2],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(
                collection,
                items[-1].review_loop_id,
                items[-1].iteration_number,
                items[-1].id,
            )
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "interrupt_intents":
        after = _two_key_instant(key)
        interrupts, _, _ = list_task_history_intents_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_intent_id=None if after is None else after[1],
            limit=limit + 1,
        )
        items = interrupts
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "resume_intents":
        after = _two_key_instant(key)
        _, resumes, _ = list_task_history_intents_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None if after is None else after[0],
            after_intent_id=None if after is None else after[1],
            limit=limit + 1,
        )
        items = resumes
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].created_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "pr_close_intent":
        _, _, close = list_task_history_intents_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_created_at=None,
            after_intent_id=None,
            limit=1,
        )
        items = () if close is None else (close,)
        return HistoryCollection(name=collection, items=items, next_cursor=None, total=len(items))
    if collection == "receipts":
        after = _two_key_instant(key)
        items = list_task_history_receipts_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_claimed_at=None if after is None else after[0],
            after_receipt_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].claimed_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    if collection == "send_claims":
        after = _two_key_instant(key)
        items = list_task_history_send_claims_page(
            tx_view,
            workspace_id=workspace_id,
            task_id=task_id,
            after_claimed_at=None if after is None else after[0],
            after_send_claim_id=None if after is None else after[1],
            limit=limit + 1,
        )
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = (
            encode_history_cursor(collection, items[-1].claimed_at, items[-1].id)
            if has_more and items
            else None
        )
        return HistoryCollection(
            name=collection, items=tuple(items), next_cursor=next_cursor, total=None
        )
    raise InvalidCommandError("the collections selection must use the documented vocabulary")


def list_task_history_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    collections: Sequence[str] | None = None,
    cursor: str | None = None,
    limit: int = HISTORY_COLLECTION_PAGE_LIMIT,
) -> TaskHistory:
    """Return the addressed Task's selected history collections (issue #376).

    Owner-authorized, database-only, and side-effect free. Without a cursor,
    every selected collection returns its first bounded page; with a cursor,
    exactly the cursor's collection continues (the documented per-collection
    continuation: a client re-requests with the collection's ``next_cursor``
    and that collection selected). A foreign-Workspace or absent Task is the
    uniform not-found outcome, never a probing oracle. The composition is
    one coherent durable observation; the whole composed transaction is a
    bounded read (per-collection page + the PR-close single record).
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise InvalidCommandError("the history page limit must be a positive integer")
    selected = _selected_collections(collections)
    paged_collection: str | None = None
    key: tuple[str, ...] | None = None
    if cursor is not None:
        paged_collection, key = decode_history_cursor(cursor)
    with application_span(_READS_TRACER_SCOPE, _HISTORY_SPAN_NAME) as span:
        annotate_span(span, operation=_HISTORY_SPAN_NAME)
        require_workspace_task(
            pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
        )
        with composed_transaction(pool) as tx_view:
            if paged_collection is not None:
                page = _page_collection(
                    tx_view,
                    collection=paged_collection,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    key=key,
                    limit=limit,
                )
                return TaskHistory(task_id=task_id, collections=(page,))
            pages = tuple(
                _page_collection(
                    tx_view,
                    collection=collection,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    key=None,
                    limit=limit,
                )
                for collection in selected
            )
            return TaskHistory(task_id=task_id, collections=pages)
