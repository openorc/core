"""Focused tests for the authorized Task history composition (issue #376).

Proven here: the documented collection-selection vocabulary, the per-collection
opaque keyset continuation (a cursor pages exactly its collection), the
single-record PR-close collection, and the closed cursor validation. The
typed projections' safety (canonical IDs/statuses, no raw rows, no secrets,
no transcripts, no actionable menus on historical gates) rides the domain
records the composition returns.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from openorc.services import task_history
from openorc.services.task_history import (
    HISTORY_COLLECTIONS,
    decode_history_cursor,
    encode_history_cursor,
)


def test_the_collection_vocabulary_is_the_documented_set() -> None:
    assert HISTORY_COLLECTIONS == (
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


def test_the_collection_selection_validates_the_documented_vocabulary() -> None:
    assert task_history._selected_collections(None) == HISTORY_COLLECTIONS
    assert task_history._selected_collections(["owner_gates"]) == ("owner_gates",)
    # Duplicates collapse; order is the caller's selection order.
    assert task_history._selected_collections(
        ["owner_gates", "owner_gates", "executions"]
    ) == ("owner_gates", "executions")

    with pytest.raises(task_history.InvalidCommandError):
        task_history._selected_collections([])
    with pytest.raises(task_history.InvalidCommandError):
        task_history._selected_collections(["audit_events"])  # G2c #377's surface


def test_per_collection_cursors_round_trip_and_page_exactly_their_collection() -> None:
    cursor = encode_history_cursor("owner_gates", "2026-11-24T12:00:00+00:00", "gate-uuid")
    collection, key = decode_history_cursor(cursor)

    assert collection == "owner_gates"
    assert key == ("2026-11-24T12:00:00+00:00", "gate-uuid")

    with pytest.raises(task_history.InvalidCommandError):
        decode_history_cursor("not-a-cursor")
    with pytest.raises(task_history.InvalidCommandError):
        decode_history_cursor(encode_history_cursor("audit_events", "x"))


class _ScriptedPool:
    """Records that history pages compose through one transaction view."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    def connection(self):  # noqa: ANN201 - test double
        raise AssertionError("history pages never open an independent transaction")


def test_pr_close_intent_is_a_single_record_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PR-close obligation is one record per Task: its page never carries
    a continuation cursor and reports its durable total."""
    calls: list[str] = []

    def _intents_page(pool: Any, **kwargs: Any) -> tuple[list[Any], list[Any], Any]:
        calls.append(kwargs.get("after_created_at"))
        return [], [], None

    monkeypatch.setattr(task_history, "list_task_history_intents_page", _intents_page)

    page = task_history._page_collection(
        _ScriptedPool(),
        collection="pr_close_intent",
        workspace_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        key=None,
        limit=50,
    )

    assert page.name == "pr_close_intent"
    assert page.items == ()
    assert page.next_cursor is None
    assert page.total == 0
    assert calls == [None]
