"""Focused tests for the Workspace stream subscription policy (issue #399).

Proven here: the bounded thread-to-asyncio handoff (bounded staging buffer,
the single coalesced wake, terminal-before-data drain ordering), the
Workspace scoping and generation-currentness drops at the consumer callback,
the terminal facts (buffer overflow closes only the affected subscription;
listener loss is recorded synchronously and never delayed behind staged
data), the earlier-of credential/lifetime deadline policy, the composed
account-identity + Workspace-ownership revalidation, and the batch
projection (data_changed mapped through the closed vocabulary; workflow_event
resolved through the focused authorized G2c read with unresolvable events
omitted).

The listener transport, the notification contract, and the G2c read
composition are owned by their own suites — these tests script them, never
re-prove them. The subscription is driven directly on the test thread, which
is exactly how the listener's dispatcher thread invokes the consumer
callback; no event loop is needed to prove the handoff bounds.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from psycopg.errors import ForeignKeyViolation

from openorc.notifications.contract import (
    DataChangedNotification,
    NotificationResourceType,
    WorkflowEventNotification,
)
from openorc.notifications.listener import (
    NotificationDelivery,
    NotificationDeliveryLost,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import workspace_stream
from openorc.services.errors import AuthenticationError, InvalidCommandError, NotFoundError
from openorc.services.event_reads import PublicWorkflowEvent
from openorc.services.workspace_stream import (
    STREAM_STAGING_BUFFER_DEPTH,
    WorkspaceStreamSubscription,
    WorkspaceStreamTermination,
)

_NOW = datetime(2026, 11, 26, 12, 0, 0, tzinfo=UTC)
_PROFILE_ID = uuid.uuid4()
_WORKSPACE_ID = uuid.uuid4()
_FOREIGN_WORKSPACE_ID = uuid.uuid4()
_OBSERVED = datetime(2026, 11, 26, 11, 30, 0, tzinfo=UTC)


def _workflow_hint(
    workspace_id: Any = _WORKSPACE_ID, event_id: Any = None
) -> WorkflowEventNotification:
    return WorkflowEventNotification(
        workspace_id=workspace_id, workflow_event_id=event_id or uuid.uuid4()
    )


def _data_changed(
    workspace_id: Any = _WORKSPACE_ID,
    resource_type: NotificationResourceType = NotificationResourceType.TASK,
) -> DataChangedNotification:
    return DataChangedNotification(
        workspace_id=workspace_id, resource_type=resource_type, resource_id=uuid.uuid4()
    )


def _delivery(message: Any, generation: int = 1) -> NotificationDelivery:
    return NotificationDelivery(generation=generation, message=message)


class _Harness:
    """Deterministic subscription harness: recording wake and controlled generation."""

    def __init__(self, *, staging_depth: int = STREAM_STAGING_BUFFER_DEPTH) -> None:
        self.wakes = 0
        self.generation = 1
        self.subscription = WorkspaceStreamSubscription(
            workspace_id=_WORKSPACE_ID,
            schedule_wake=self._wake,
            current_generation=lambda: self.generation,
            staging_depth=staging_depth,
        )

    def _wake(self) -> None:
        self.wakes += 1


def cast_pool() -> DatabasePool:
    """The scripted seams never touch a real pool."""
    return None  # type: ignore[return-value]


# --- deadline policy ----------------------------------------------------------


def test_the_deadline_is_the_earlier_of_credential_expiry_and_lifetime() -> None:
    earlier_expiry = datetime(2026, 11, 26, 12, 0, 5, tzinfo=UTC)
    assert (
        workspace_stream.stream_deadline(earlier_expiry, now=_NOW, max_lifetime_seconds=900.0)
        == earlier_expiry
    )
    later_expiry = datetime(2026, 11, 26, 13, 0, 0, tzinfo=UTC)
    assert workspace_stream.stream_deadline(
        later_expiry, now=_NOW, max_lifetime_seconds=900.0
    ) == _NOW + timedelta(seconds=900)


def test_the_deadline_normalizes_a_non_utc_expiry() -> None:
    offset_expiry = datetime(2026, 11, 26, 14, 0, 5, tzinfo=timezone(timedelta(hours=2)))
    assert workspace_stream.stream_deadline(
        offset_expiry, now=_NOW, max_lifetime_seconds=900.0
    ) == datetime(2026, 11, 26, 12, 0, 5, tzinfo=UTC)


@pytest.mark.parametrize(
    ("expires_at", "now", "lifetime"),
    [
        (_NOW.replace(tzinfo=None), _NOW, 900.0),
        (_NOW, _NOW.replace(tzinfo=None), 900.0),
        (_NOW, _NOW, 0),
        (_NOW, _NOW, -5.0),
    ],
)
def test_malformed_deadline_inputs_are_typed_invalid_commands(
    expires_at: datetime, now: datetime, lifetime: float
) -> None:
    with pytest.raises(InvalidCommandError):
        workspace_stream.stream_deadline(expires_at, now=now, max_lifetime_seconds=lifetime)


# --- revalidation composition -------------------------------------------------


def test_revalidation_composes_the_account_identity_and_ownership_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _ensure_profile(pool: Any, *, profile_id: Any) -> None:
        calls.append("ensure_profile")

    def _require_ownership(pool: Any, *, profile_id: Any, workspace_id: Any) -> None:
        assert profile_id == _PROFILE_ID
        assert workspace_id == _WORKSPACE_ID
        calls.append("require_profile_workspace")

    monkeypatch.setattr(workspace_stream, "ensure_profile", _ensure_profile)
    monkeypatch.setattr(workspace_stream, "require_profile_workspace", _require_ownership)

    workspace_stream.revalidate_workspace_stream_access(
        cast_pool(), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
    )

    # Both established checks ran, identity first — never ownership alone.
    assert calls == ["ensure_profile", "require_profile_workspace"]


def test_a_deleted_auth_account_fails_closed_on_revalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _ensure_profile(pool: Any, *, profile_id: Any) -> None:
        raise ForeignKeyViolation("deleted auth user")

    def _require_ownership(pool: Any, **command: Any) -> None:
        raise AssertionError("ownership is never re-proved after identity fails closed")

    monkeypatch.setattr(workspace_stream, "ensure_profile", _ensure_profile)
    monkeypatch.setattr(workspace_stream, "require_profile_workspace", _require_ownership)

    with pytest.raises(AuthenticationError, match="no longer exists"):
        workspace_stream.revalidate_workspace_stream_access(
            cast_pool(), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )


def test_lost_ownership_is_the_typed_not_found_on_revalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workspace_stream, "ensure_profile", lambda pool, **command: None)

    def _require_ownership(pool: Any, **command: Any) -> None:
        raise NotFoundError("the requested workspace is not available to this Profile")

    monkeypatch.setattr(workspace_stream, "require_profile_workspace", _require_ownership)

    with pytest.raises(NotFoundError):
        workspace_stream.revalidate_workspace_stream_access(
            cast_pool(), profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )


# --- batch projection ---------------------------------------------------------


def test_the_batch_maps_data_changed_and_resolves_workflow_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    changed_hint = _data_changed()
    event_hint = _workflow_hint()
    read_calls: list[Any] = []

    def _read(pool: Any, *, profile_id: Any, workspace_id: Any, workflow_event_id: Any) -> Any:
        read_calls.append(workflow_event_id)
        return PublicWorkflowEvent(
            id=workflow_event_id,
            workspace_id=_WORKSPACE_ID,
            task_id=None,
            event_type="task_started",
            actor_type="owner",
            actor_id=str(_PROFILE_ID),
            subject_type=None,
            subject_id=None,
            context={"outcome": "approved"},
            created_at=_OBSERVED,
        )

    monkeypatch.setattr(workspace_stream, "read_workflow_event_for_owner", _read)

    messages = workspace_stream.project_stream_batch(
        cast_pool(),
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        batch=[changed_hint, event_hint],
    )

    assert len(messages) == 2
    changed = messages[0]
    assert isinstance(changed, workspace_stream.WorkspaceStreamDataChanged)
    assert (changed.workspace_id, changed.resource_type, changed.resource_id) == (
        _WORKSPACE_ID,
        "task",
        changed_hint.resource_id,
    )
    event = messages[1]
    assert isinstance(event, workspace_stream.WorkspaceStreamWorkflowEvent)
    assert event.event.event_type == "task_started"
    assert event.event.context == {"outcome": "approved"}
    assert read_calls == [event_hint.workflow_event_id]


def test_the_batch_omits_unresolvable_and_foreign_hints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _read(pool: Any, **command: Any) -> Any:
        raise NotFoundError("the requested workflow event is not available in this workspace")

    monkeypatch.setattr(workspace_stream, "read_workflow_event_for_owner", _read)

    messages = workspace_stream.project_stream_batch(
        cast_pool(),
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        batch=[
            _workflow_hint(workspace_id=_FOREIGN_WORKSPACE_ID),
            _data_changed(workspace_id=_FOREIGN_WORKSPACE_ID),
            _workflow_hint(),
        ],
    )

    # The foreign hints never resolve (dropped before any read) and the
    # unresolvable hinted event is omitted — never forwarded raw.
    assert messages == []


# --- the bounded handoff ------------------------------------------------------


def test_foreign_and_stale_deliveries_are_dropped_without_waking() -> None:
    harness = _Harness()
    subscription = harness.subscription

    subscription.on_listener_event(_delivery(_data_changed(workspace_id=_FOREIGN_WORKSPACE_ID)))
    harness.generation = 2
    subscription.on_listener_event(_delivery(_data_changed(), generation=1))
    harness.generation = 1

    terminal, batch = subscription.take_batch()
    assert terminal is None
    assert batch == []
    assert harness.wakes == 0


def test_deliveries_stage_bounded_with_one_pending_wake() -> None:
    harness = _Harness(staging_depth=8)
    subscription = harness.subscription

    for _ in range(5):
        subscription.on_listener_event(_delivery(_data_changed()))

    # Five deliveries coalesced into exactly one pending wake handoff.
    assert harness.wakes == 1
    terminal, batch = subscription.take_batch()
    assert terminal is None
    assert len(batch) == 5

    # After the buffer drains empty the flag re-arms and the next delivery
    # schedules a fresh wake.
    subscription.on_listener_event(_delivery(_data_changed()))
    assert harness.wakes == 2
    terminal, batch = subscription.take_batch()
    assert terminal is None
    assert len(batch) == 1


def test_buffer_overflow_is_terminal_for_the_affected_subscription_only() -> None:
    harness = _Harness(staging_depth=2)
    subscription = harness.subscription

    subscription.on_listener_event(_delivery(_data_changed()))
    subscription.on_listener_event(_delivery(_data_changed()))
    subscription.on_listener_event(_delivery(_data_changed()))

    assert subscription.terminal is WorkspaceStreamTermination.BUFFER_OVERFLOW
    terminal, batch = subscription.take_batch()
    assert terminal is WorkspaceStreamTermination.BUFFER_OVERFLOW
    # The two genuinely staged items are returned but the drain never emits
    # past a terminal fact; the overflow closed this subscription only.
    assert len(batch) == 2

    # Later deliveries are dropped forever; the wake stays coalesced.
    wakes_before = harness.wakes
    subscription.on_listener_event(_delivery(_data_changed()))
    assert harness.wakes == wakes_before
    assert subscription.take_batch() == (
        WorkspaceStreamTermination.BUFFER_OVERFLOW,
        [],
    )


def test_listener_loss_terminates_promptly_even_when_data_is_staged() -> None:
    harness = _Harness(staging_depth=8)
    subscription = harness.subscription

    subscription.on_listener_event(_delivery(_data_changed()))
    subscription.on_listener_event(_delivery(_workflow_hint()))
    subscription.on_listener_event(NotificationDeliveryLost(generation=harness.generation))

    # The loss is the terminal fact; the drain sees it before the staged data
    # and the stream closes without ever emitting the staged items.
    terminal, batch = subscription.take_batch()
    assert terminal is WorkspaceStreamTermination.DELIVERY_LOST
    assert len(batch) == 2


def test_terminal_facts_and_late_deliveries_are_dropped() -> None:
    harness = _Harness()
    subscription = harness.subscription
    subscription.on_listener_event(_delivery(_data_changed()))
    subscription.on_listener_event(NotificationDeliveryLost(generation=harness.generation))
    assert subscription.terminal is WorkspaceStreamTermination.DELIVERY_LOST
    wakes_before = harness.wakes

    # Everything after a terminal fact is dropped — further loss signals and
    # further deliveries alike (a readiness signal is by contract a no-op).
    subscription.on_listener_event(NotificationDeliveryLost(generation=harness.generation))
    subscription.on_listener_event(_delivery(_data_changed()))
    assert harness.wakes == wakes_before
    terminal, batch = subscription.take_batch()
    assert terminal is WorkspaceStreamTermination.DELIVERY_LOST
    # The late delivery was dropped; only the pre-terminal staged item is
    # returned, and the drain never emits past the terminal fact.
    assert len(batch) == 1


def test_take_batch_rearms_the_wake_and_mark_closed_drops_late_deliveries() -> None:
    harness = _Harness()
    subscription = harness.subscription

    subscription.on_listener_event(_delivery(_data_changed()))
    subscription.take_batch()
    subscription.mark_closed()

    wakes_before = harness.wakes
    subscription.on_listener_event(_delivery(_data_changed()))
    subscription.on_listener_event(NotificationDeliveryLost(generation=harness.generation))
    assert harness.wakes == wakes_before
    assert subscription.take_batch() == (None, [])


def test_two_subscriptions_are_isolated_from_each_others_overflow() -> None:
    overflowing = _Harness(staging_depth=1)
    healthy = _Harness(staging_depth=8)

    overflowing.subscription.on_listener_event(_delivery(_data_changed()))
    overflowing.subscription.on_listener_event(_delivery(_data_changed()))
    assert overflowing.subscription.terminal is WorkspaceStreamTermination.BUFFER_OVERFLOW

    # The healthy subscription staged and woke independently: the shared
    # listener dispatcher is never blocked or delayed by the closed one.
    healthy.subscription.on_listener_event(_delivery(_workflow_hint()))
    terminal, batch = healthy.subscription.take_batch()
    assert terminal is None
    assert len(batch) == 1
    assert healthy.wakes == 1
