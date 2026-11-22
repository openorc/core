"""Focused regular-mode lifespan tests for the F11 startup queue recovery (issue #257).

The serving lifespan runs the bounded post-restart recovery pass in both
execution modes. These tests prove the regular-mode wiring at the transport
boundary: the startup recovery enumerates the scripted queued/unclaimed
facts, re-proves every candidate from committed state, and delivers the lost
queue wake and the unclaimed planning-bootstrap handoff through the serving
process's own bounded local executor, while a claimed bootstrap stays
excluded. No live Postgres, Valkey, GitHub, or Cline is touched.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from openorc.api import app as app_module
from openorc.api.app import create_app
from openorc.config import Settings
from openorc.domain.tasks import TaskStatus
from openorc.observability import shutdown_observability
from openorc.workers import invocation
from openorc.workers.invocation import (
    LateBoundDetachedTurnObserver,
    LateBoundProcessSubmitter,
    OperationDependencies,
    OperationKind,
)
from tests.fakes.task_rows import task_row
from tests.test_services_task_detached_turn_observation import (
    ScriptedConnection,
    _pool,
    _Predicate,
    _sql_has,
)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000101")
_OWNER_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000102")
_REPOSITORY_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000103")
_UNCLAIMED_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000104")
_CLAIMED_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000105")
_UNCLAIMED_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000106")
_CLAIMED_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000107")
_NOW = datetime(2026, 11, 22, 12, 0, 0, tzinfo=UTC)


class _RecordingHandler:
    """The patched F1 handler double recording its delivery facts."""

    def __init__(self, kind: OperationKind) -> None:
        self.kind = kind
        self.calls: list[dict[str, Any]] = []

    def __call__(self, payload: Any, *, dependencies: OperationDependencies) -> None:
        self.calls.append(
            {
                "payload": payload,
                "thread": threading.current_thread(),
                "pool": dependencies.database_pool(),
            }
        )


def _planning_row(*, task_id: UUID, state_token: UUID) -> tuple[Any, ...]:
    return task_row(
        id=task_id,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        github_issue_number=42,
        status=TaskStatus.PLANNING,
        state_token=state_token,
        source_requirements_fingerprint="a" * 64,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _ws_row() -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _NOW, _NOW, 5, "", None)


def _task_read() -> _Predicate:
    """Match one plain current-Task read (never the focused claim read)."""

    def matches(sql: str) -> bool:
        lowered = " ".join(sql.split()).lower()
        return (
            "from openorc.tasks where id" in lowered
            and "planning_bootstrap_claim_token" not in lowered
            and "for update" not in lowered
            and "update openorc.tasks" not in lowered
        )

    return _Predicate(fn=matches)


def _claim_read() -> _Predicate:
    """Match the focused one-shot bootstrap-claim read."""

    def matches(sql: str) -> bool:
        lowered = " ".join(sql.split()).lower()
        return (
            "planning_bootstrap_claim_token" in lowered
            and "select" in lowered
            and "update" not in lowered
        )

    return _Predicate(fn=matches)


def _script_recovery_facts(conn: ScriptedConnection) -> None:
    """Script one queued-workspace subject and two planning bootstrap facts.

    The classifier's reads arrive in enumeration order — the unclaimed
    subject's task read and claim read first, then the claimed subject's —
    so they are scripted as consumable handlers in exactly that order.
    """
    conn.on_repeat(
        _sql_has("select distinct workspace_id from openorc.tasks"),
        [(_WORKSPACE_ID,)],
    )
    conn.on_repeat(
        _sql_has("planning_bootstrap_claim_token is null"),
        [
            _planning_row(task_id=_UNCLAIMED_ID, state_token=_UNCLAIMED_TOKEN),
            _planning_row(task_id=_CLAIMED_ID, state_token=_CLAIMED_TOKEN),
        ],
    )
    conn.on(_task_read(), _planning_row(task_id=_UNCLAIMED_ID, state_token=_UNCLAIMED_TOKEN))
    conn.on(_claim_read(), None)
    conn.on(_task_read(), _planning_row(task_id=_CLAIMED_ID, state_token=_CLAIMED_TOKEN))
    conn.on(_claim_read(), (_CLAIMED_TOKEN,))
    conn.on_repeat(_sql_has("from openorc.workspaces"), _ws_row())


def _regular_dependencies(settings: Settings, conn: ScriptedConnection) -> OperationDependencies:
    return OperationDependencies(
        settings=settings,
        database_pool_provider=lambda _settings: _pool(conn),
        continuation_submitter_provider=LateBoundProcessSubmitter(),
        detached_turn_observer_provider=LateBoundDetachedTurnObserver(),
    )


def test_regular_mode_startup_recovers_a_lost_wake_and_the_unclaimed_bootstrap(
    settings_factory: Callable[..., Settings], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ScriptedConnection()
    _script_recovery_facts(conn)
    wake_handler = _RecordingHandler(OperationKind.TASK_QUEUE_WAKE)
    bootstrap_handler = _RecordingHandler(OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN)

    def patched_handlers() -> dict[OperationKind, Any]:
        table = dict(invocation.build_production_handlers())
        table[OperationKind.TASK_QUEUE_WAKE] = wake_handler
        table[OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN] = bootstrap_handler
        return table

    settings = settings_factory()
    app = create_app(settings)
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: _regular_dependencies(settings, conn),
        raising=True,
    )
    monkeypatch.setattr(app_module, "build_production_handlers", patched_handlers, raising=True)

    with TestClient(app):
        # The lifespan owns the executor and delivers through it.
        assert app.state.operation_executor is not None
        assert app.state.operation_submitter is app.state.operation_executor

    # After the lifespan exits the executor has drained: every admitted
    # recovery delivery has executed on an executor worker thread.
    assert len(wake_handler.calls) == 1
    wake_payload = wake_handler.calls[0]["payload"]
    assert isinstance(wake_payload, invocation.QueueWakePayload)
    assert wake_payload.workspace_id == str(_WORKSPACE_ID)
    assert wake_handler.calls[0]["thread"] is not threading.main_thread()
    assert wake_handler.calls[0]["pool"] is not None

    # The unclaimed bootstrap handoff is recovered with the exact re-proved
    # token and the accountable Workspace Owner identity.
    assert len(bootstrap_handler.calls) == 1
    bootstrap_payload = bootstrap_handler.calls[0]["payload"]
    assert isinstance(bootstrap_payload, invocation.TaskPlanningBootstrapAndInitialPlanPayload)
    assert bootstrap_payload.task.workspace_id == str(_WORKSPACE_ID)
    assert bootstrap_payload.task.task_id == str(_UNCLAIMED_ID)
    assert bootstrap_payload.expected_state_token == str(_UNCLAIMED_TOKEN)
    assert bootstrap_payload.profile_id == str(_OWNER_PROFILE_ID)
    assert bootstrap_handler.calls[0]["thread"] is not threading.main_thread()

    # The enumerated planning subject whose E1b claim is durably present is
    # positively excluded: no bootstrap handoff is ever arranged for it.
    assert all(
        call["payload"].task.task_id != str(_CLAIMED_ID)
        for call in bootstrap_handler.calls
        if isinstance(call["payload"], invocation.TaskPlanningBootstrapAndInitialPlanPayload)
    )


def test_regular_mode_startup_recovery_failure_is_contained(
    settings_factory: Callable[..., Settings], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ScriptedConnection()
    # No recovery facts scripted: the enumeration read hits no handler and
    # the contained pass must not fail the serving lifespan.
    wake_handler = _RecordingHandler(OperationKind.TASK_QUEUE_WAKE)

    def patched_handlers() -> dict[OperationKind, Any]:
        table = dict(invocation.build_production_handlers())
        table[OperationKind.TASK_QUEUE_WAKE] = wake_handler
        return table

    settings = settings_factory()
    app = create_app(settings)
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: _regular_dependencies(settings, conn),
        raising=True,
    )
    monkeypatch.setattr(app_module, "build_production_handlers", patched_handlers, raising=True)

    with TestClient(app) as client:
        assert app.state.operation_executor is not None
        response = client.get("/healthz")

    # The serving process stays healthy and the recovery submitted nothing.
    assert response.status_code == 200
    assert wake_handler.calls == []
    shutdown_observability()
