"""Integration test for the G4a notification transport over real PostgreSQL.

The single focused cross-connection test (issue #397): the actual implemented
listener and its typed consumer interface run against a second raw writer
connection issuing only ``pg_notify`` SQL — no application tables or triggers
are required. It proves on the real pinned psycopg 3 transport that the outer
commit makes notifications visible, that transaction and nested-savepoint
rollback suppress them, that identical channel+payload notifications coalesce
within one transaction (guaranteed by PostgreSQL), and that readiness is
published only after LISTEN is active with typed messages flowing end-to-end
through the listener's bounded delivery interface.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_notification_transport_cross_connection.py

The suite consumes the database it is given and never provisions one; it
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent. The testdb flow
supplies the branch database's session-pooler connection (port 5432), which
is session-capable — exactly the endpoint class the listener requires.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from time import monotonic
from typing import Any
from uuid import uuid4

import pytest
from psycopg import Connection, connect

from openorc.notifications.contract import (
    NOTIFICATION_CHANNEL,
    DataChangedNotification,
    NotificationResourceType,
    WorkflowEventNotification,
    encode_notification_payload,
)
from openorc.notifications.listener import (
    NotificationConsumerEvent,
    NotificationDelivery,
    NotificationDeliveryReady,
    PostgresNotificationListener,
)

pytestmark = pytest.mark.integration


class EventCollector:
    """Non-blocking consumer collecting every delivered typed event."""

    def __init__(self) -> None:
        self.events: list[NotificationConsumerEvent] = []
        self._lock = threading.Lock()

    def __call__(self, event: NotificationConsumerEvent) -> None:
        with self._lock:
            self.events.append(event)

    def snapshot(self) -> list[NotificationConsumerEvent]:
        with self._lock:
            return list(self.events)


def _wait_until(predicate: Callable[[], bool], *, timeout: float = 30.0) -> None:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("the awaited condition was not met within the timeout")


def _settle(seconds: float = 2.0) -> None:
    """Bounded settle window for an expected-absent notification."""
    time.sleep(seconds)


def _message_count(events: list[NotificationConsumerEvent], message: object) -> int:
    # The revised listener delivers messages inside generation-bearing
    # NotificationDelivery wrappers; compare the wrapped message field.
    return sum(
        1
        for event in events
        if isinstance(event, NotificationDelivery) and event.message == message
    )


def _has_ready_signal(events: list[NotificationConsumerEvent]) -> bool:
    return any(isinstance(event, NotificationDeliveryReady) for event in events)


class _SavepointRollback(Exception):
    """Control-flow exception that rolls back only the nested savepoint."""


def _notify(writer: Connection[Any], payload: str) -> None:
    """Publish one notification on the fixed channel through the writer."""
    writer.execute("SELECT pg_notify(%s, %s)", [NOTIFICATION_CHANNEL, payload])


def test_notification_transport_cross_connection(database_url: str) -> None:
    listener = PostgresNotificationListener(database_url)
    collector = EventCollector()
    listener.register_consumer(collector)

    listener.start()
    try:
        # Explicit readiness only after LISTEN is active on the live
        # connection, and only once the dispatched readiness signal has
        # actually reached the consumer interface.
        _wait_until(lambda: listener.ready)
        _wait_until(lambda: _has_ready_signal(collector.snapshot()))
        events = collector.snapshot()
        assert events and isinstance(events[0], NotificationDeliveryReady)
        assert listener.generation >= 1

        workspace_id = uuid4()
        workflow_message = WorkflowEventNotification(
            workspace_id=workspace_id, workflow_event_id=uuid4()
        )
        project_message = DataChangedNotification(
            workspace_id=workspace_id,
            resource_type=NotificationResourceType.PROJECT,
            resource_id=uuid4(),
        )
        task_message = DataChangedNotification(
            workspace_id=workspace_id,
            resource_type=NotificationResourceType.TASK,
            resource_id=uuid4(),
        )
        suppressed_message = DataChangedNotification(
            workspace_id=workspace_id,
            resource_type=NotificationResourceType.REPOSITORY,
            resource_id=uuid4(),
        )
        savepoint_message = DataChangedNotification(
            workspace_id=workspace_id,
            resource_type=NotificationResourceType.CONNECTION,
            resource_id=uuid4(),
        )

        with connect(database_url) as writer:
            # The outer commit makes the notification visible to the listener.
            _notify(writer, encode_notification_payload(workflow_message))
            writer.commit()
            _wait_until(lambda: _message_count(collector.snapshot(), workflow_message) == 1)

            # A rolled-back transaction suppresses its notification entirely.
            _notify(writer, encode_notification_payload(suppressed_message))
            writer.rollback()
            _settle()
            assert _message_count(collector.snapshot(), suppressed_message) == 0

            # A nested savepoint rollback suppresses only the savepoint's
            # notification; the outer commit publishes the rest. The savepoint
            # is rolled back exactly the way composed-transaction scopes roll
            # back: an exception inside the nested transaction context, caught
            # inside the still-open outer transaction.
            with writer.transaction():
                _notify(writer, encode_notification_payload(project_message))
                try:
                    with writer.transaction():
                        _notify(writer, encode_notification_payload(savepoint_message))
                        raise _SavepointRollback
                except _SavepointRollback:
                    pass
                _notify(writer, encode_notification_payload(task_message))
            _wait_until(lambda: _message_count(collector.snapshot(), project_message) == 1)
            _wait_until(lambda: _message_count(collector.snapshot(), task_message) == 1)
            assert _message_count(collector.snapshot(), savepoint_message) == 0

            # Identical channel+payload within one transaction coalesce into
            # one delivered notification (guaranteed by PostgreSQL).
            with writer.transaction():
                for _ in range(2):
                    _notify(writer, encode_notification_payload(project_message))
            _wait_until(lambda: _message_count(collector.snapshot(), project_message) == 2)
    finally:
        listener.stop()
    assert listener.ready is False
