"""E9 §2 shared PR-review persistence fixtures (issue #84).

Since #347 the removed blocking-path tests are gone; the surviving Level 2/3
and lifecycle test modules import these shared scripted-connection fixtures.
The module deliberately carries no test functions of its own."""

from __future__ import annotations

import uuid
from typing import Any

from tests.test_services_task_pr_review_dispatch import (
    _HEAD_SHA,
    _OBSERVED,
    _PR_ID,
    _TASK_ID,
    _WORKSPACE_ID,
    ScriptedConnection,
    _sql_has,
)

_GATE_READ = _sql_has("from openorc.owner_gates where id")


_GATE_INSERT = _sql_has("insert into openorc.owner_gates")


_GATE_UPDATE = _sql_has("update openorc.owner_gates")


_TASK_GATE_INSTALL = _sql_has("update openorc.tasks", "current_owner_gate_id = %s")


_TASK_GATE_CLEAR = _sql_has("update openorc.tasks", "current_owner_gate_id = null")


_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _event_returned_row(event_type: str) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type,
        "reviewer",
        "session-actor",
        None,
        None,
        {},
        _OBSERVED,
    )


def _event_types(conn: ScriptedConnection) -> list[str]:
    types: list[str] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None:
            types.append(str(params[2]))
    return types


def _gate_row(
    *,
    gate_id: Any,
    gate_type: str,
    head_sha: str | None = _HEAD_SHA,
    pr_id: Any | None = _PR_ID,
    status: str = "pending",
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        None,
        head_sha,
        pr_id,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )
