"""Deterministic tests for the process-local notification listener (issue #397).

The reader/dispatcher machinery runs against psycopg3-shaped fake
connections: bounded notify-tick stop, coalesced generation invalidation
under saturation, authoritative readiness (a superseded readiness snapshot
can never establish downstream continuity), contained consumer failures,
and a clean idempotent stop. No database and no real connection is touched.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from time import monotonic
from typing import cast
from uuid import uuid4

import pytest
from psycopg import OperationalError

from openorc.notifications.contract import (
    NOTIFICATION_CHANNEL,
    DataChangedNotification,
    NotificationResourceType,
    WorkflowEventNotification,
    encode_notification_payload,
)
from openorc.notifications.listener import (
    BOUNDED_DELIVERY_QUEUE_DEPTH,
    ConnectionFactory,
    NotificationConsumerEvent,
    NotificationDelivery,
    NotificationDeliveryLost,
    NotificationDeliveryReady,
    PostgresNotificationListener,
    _DeliveryState,
)

_LISTENER_CONNINFO = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"


class FakeNotify:
    """psycopg3-shaped notification record."""

    def __init__(self, channel: str, payload: str) -> None:
        self.channel = channel
        self.payload = payload


class FakeConnection:
    """psycopg3-shaped scripted connection for the reader seam.

    ``notifies`` mirrors the pinned driver's generator semantics: it yields
    the scripted notifications, then blocks until closed or until the
    ``timeout`` deadline passes — the generator ends at the deadline and is
    re-invoked by the reader, exactly like the real ``notifies()``.
    """

    def __init__(
        self,
        *,
        notifications: Sequence[FakeNotify] = (),
        execute_error: Exception | None = None,
        notifies_error: Exception | None = None,
    ) -> None:
        self.executed_sql: list[str] = []
        self.closed = False
        self._pending: list[FakeNotify] = list(notifications)
        self._execute_error = execute_error
        self._notifies_error = notifies_error

    def execute(self, sql: str) -> None:
        self.executed_sql.append(sql)
        if self._execute_error is not None:
            raise self._execute_error

    def notifies(
        self, *, timeout: float | None = None, stop_after: int | None = None
    ) -> Iterator[FakeNotify]:
        if self._notifies_error is not None:
            raise self._notifies_error
        pending, self._pending = self._pending, []
        yield from pending
        deadline = monotonic() + (timeout if timeout is not None else 0.0)
        while not self.closed:
            if timeout is not None and monotonic() >= deadline:
                return
            time.sleep(0.01)

    def close(self) -> None:
        self.closed = True


class EventCollector:
    """Non-blocking consumer collecting every delivered typed event."""

    def __init__(self) -> None:
        self.events: list[NotificationConsumerEvent] = []

    def __call__(self, event: NotificationConsumerEvent) -> None:
        self.events.append(event)

    def wait_for(self, count: int, *, timeout: float = 5.0) -> bool:
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            if len(self.events) >= count:
                return True
            time.sleep(0.01)
        return len(self.events) >= count


def wait_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> bool:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def make_connect_factory(
    *connections: FakeConnection,
) -> ConnectionFactory:
    scripted = iter(connections)

    def factory(conninfo: str) -> FakeConnection:
        try:
            return next(scripted)
        except StopIteration:
            raise AssertionError("no scripted connection left") from None

    return cast(ConnectionFactory, factory)


def static_connect(connection: FakeConnection) -> ConnectionFactory:
    return cast(ConnectionFactory, lambda _conninfo: connection)


def workflow_message() -> WorkflowEventNotification:
    return WorkflowEventNotification(workspace_id=uuid4(), workflow_event_id=uuid4())


def data_changed_message() -> DataChangedNotification:
    return DataChangedNotification(
        workspace_id=uuid4(),
        resource_type=NotificationResourceType.TASK,
        resource_id=uuid4(),
    )


def workflow_payload() -> str:
    return encode_notification_payload(workflow_message())


def _listener(
    *,
    connect: ConnectionFactory | None = None,
    delivery_queue_depth: int = BOUNDED_DELIVERY_QUEUE_DEPTH,
) -> PostgresNotificationListener:
    return PostgresNotificationListener(
        _LISTENER_CONNINFO,
        connect=connect,
        delivery_queue_depth=delivery_queue_depth,
    )


def test_readiness_requires_successful_listen() -> None:
    listener = _listener()
    collector = EventCollector()
    listener.register_consumer(collector)

    assert listener.ready is False
    assert listener.generation == 0
    assert listener._run_dispatch_cycle() is False
    assert collector.events == []

    listener._note_listen_ready()

    assert listener.ready is True
    assert listener._run_dispatch_cycle() is False
    assert collector.events == [NotificationDeliveryReady(generation=1)]


def test_delivers_generation_tagged_messages_in_order() -> None:
    listener = _listener()
    collector = EventCollector()
    listener.register_consumer(collector)
    listener._note_listen_ready()

    first = workflow_message()
    second = data_changed_message()
    listener._deliver_incoming_payload(encode_notification_payload(first))
    listener._deliver_incoming_payload(encode_notification_payload(second))

    assert listener._run_dispatch_cycle() is False
    assert collector.events[0] == NotificationDeliveryReady(generation=1)
    assert collector.events[1] == NotificationDelivery(generation=1, message=first)
    assert collector.events[2] == NotificationDelivery(generation=1, message=second)


def test_overflow_invalidates_generation_and_suppresses_stale_entries() -> None:
    listener = _listener(delivery_queue_depth=2)
    collector = EventCollector()
    listener.register_consumer(collector)
    listener._note_listen_ready()
    assert listener.generation == 1
    assert listener.ready is True

    first = workflow_message()
    second = workflow_message()
    third = workflow_message()
    listener._deliver_incoming_payload(encode_notification_payload(first))
    listener._deliver_incoming_payload(encode_notification_payload(second))
    # The bounded queue is full: the next message triggers the overflow path
    # and is dropped — the reader never blocks on the queue.
    listener._deliver_incoming_payload(encode_notification_payload(third))
    assert listener.generation == 2

    assert listener._run_dispatch_cycle() is False

    # The coalesced loss plus the authoritative readiness arrive first; the
    # two stale pre-break entries are suppressed, never delivered as current
    # messages, and the invalidation is communicated with no further
    # PostgreSQL notifications.
    assert collector.events == [
        NotificationDeliveryLost(generation=2),
        NotificationDeliveryReady(generation=2),
    ]

    recovered = workflow_message()
    listener._deliver_incoming_payload(encode_notification_payload(recovered))
    assert listener._run_dispatch_cycle() is False
    assert collector.events[2] == NotificationDelivery(generation=2, message=recovered)


def test_multi_episode_collapse_supersedes_false_readiness() -> None:
    listener = _listener(delivery_queue_depth=2)
    collector = EventCollector()
    listener.register_consumer(collector)
    listener._note_listen_ready()  # generation 1: pending readiness
    listener._note_overflow()  # generation 2: pending loss + pending readiness
    listener._note_overflow()  # generation 3: coalesces into the same state
    listener._note_connection_lost()  # generation 4: supersedes pending readiness

    assert listener.ready is False
    assert listener._run_dispatch_cycle() is False

    # Exactly one consolidated loss carrying the current generation and NO
    # readiness signal: the intermediate episodes collapsed and the pending
    # readiness was superseded by the connection loss.
    assert collector.events == [NotificationDeliveryLost(generation=4)]

    # Eventual recovery is communicated with no further PostgreSQL
    # notifications: readiness for the genuinely-live new generation.
    listener._note_listen_ready()
    assert listener._run_dispatch_cycle() is False
    assert collector.events == [
        NotificationDeliveryLost(generation=4),
        NotificationDeliveryReady(generation=5),
    ]


def test_connection_loss_supersedes_pending_readiness() -> None:
    listener = _listener()
    collector = EventCollector()
    listener.register_consumer(collector)
    listener._note_listen_ready()  # generation 1: pending readiness set
    listener._note_connection_lost()  # generation 2: pending readiness superseded

    assert listener.ready is False
    assert listener._run_dispatch_cycle() is False

    assert collector.events == [NotificationDeliveryLost(generation=2)]


def test_malformed_payloads_are_rejected_fail_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    listener = _listener()
    collector = EventCollector()
    listener.register_consumer(collector)
    listener._note_listen_ready()

    with caplog.at_level(logging.WARNING, logger="openorc.notifications.listener"):
        listener._deliver_incoming_payload("not json")
        listener._deliver_incoming_payload('{"kind":"unknown"}')

    assert listener._run_dispatch_cycle() is False
    assert collector.events == [NotificationDeliveryReady(generation=1)]
    assert listener.ready is True
    assert listener.generation == 1
    assert any("malformed notification payload" in record.getMessage() for record in caplog.records)


def test_consumer_failure_is_contained_per_consumer(
    caplog: pytest.LogCaptureFixture,
) -> None:
    listener = _listener()
    collector = EventCollector()

    def failing(event: NotificationConsumerEvent) -> None:
        raise RuntimeError("boom")

    listener.register_consumer(failing)
    listener.register_consumer(collector)
    listener._note_listen_ready()

    with caplog.at_level(logging.WARNING, logger="openorc.notifications.listener"):
        assert listener._run_dispatch_cycle() is False

    assert collector.events == [NotificationDeliveryReady(generation=1)]
    assert any("RuntimeError" in record.getMessage() for record in caplog.records)


def test_registration_records_generation_and_unregister_stops_delivery() -> None:
    listener = _listener()
    first_collector = EventCollector()
    second_collector = EventCollector()

    first = listener.register_consumer(first_collector)
    assert first.generation == 0
    listener._note_listen_ready()
    second = listener.register_consumer(second_collector)
    assert second.generation == 1

    listener.unregister_consumer(first)
    assert listener._run_dispatch_cycle() is False

    assert first_collector.events == []
    assert second_collector.events == [NotificationDeliveryReady(generation=1)]


def test_stop_supersedes_pending_readiness_and_ends_dispatch() -> None:
    listener = _listener()
    collector = EventCollector()
    listener.register_consumer(collector)
    listener._note_listen_ready()

    listener.stop()

    assert listener.ready is False
    assert listener._run_dispatch_cycle() is True
    assert collector.events == []


def test_stopped_listener_refuses_late_transitions() -> None:
    # Shutdown is irreversible: a late connection establishment, overflow, or
    # connection failure after the stop can never restore readiness, churn
    # the generation, or pend control signals.
    state = _DeliveryState()
    state.note_stopped()

    state.note_listen_ready()
    state.note_overflow()
    state.note_connection_lost()

    assert state.stopped is True
    assert state.ready is False
    assert state.generation == 0
    assert state.pending_loss is False
    assert state.pending_ready is False


def test_invalidation_during_dispatch_gates_remaining_consumers() -> None:
    listener = _listener()
    gate_collector = EventCollector()
    late_collector = EventCollector()

    def invalidating_consumer(event: NotificationConsumerEvent) -> None:
        gate_collector.events.append(event)
        if isinstance(event, NotificationDelivery):
            # Invalidate the delivery generation while the dispatcher is
            # handing the message to the registered consumer snapshot.
            listener._note_connection_lost()

    listener.register_consumer(invalidating_consumer)
    listener.register_consumer(late_collector)
    listener._note_listen_ready()

    # Flush the initial readiness signal so both consumers are established
    # on the quiescent current generation before the message dispatch.
    assert listener._run_dispatch_cycle() is False
    assert gate_collector.events == [NotificationDeliveryReady(generation=1)]
    assert late_collector.events == [NotificationDeliveryReady(generation=1)]

    message = workflow_message()
    listener._deliver_incoming_payload(encode_notification_payload(message))

    assert listener._run_dispatch_cycle() is False

    # The first consumer received the then-current delivery; its callback
    # invalidated the generation, so the remaining consumer of the same
    # snapshot never receives the now-stale delivery.
    assert gate_collector.events == [
        NotificationDeliveryReady(generation=1),
        NotificationDelivery(generation=1, message=message),
    ]
    assert late_collector.events == [NotificationDeliveryReady(generation=1)]

    # The coalesced loss is a monotone fact and is handed to every consumer
    # on the following cycle — with no further PostgreSQL notifications.
    assert listener._run_dispatch_cycle() is False
    assert gate_collector.events == [
        NotificationDeliveryReady(generation=1),
        NotificationDelivery(generation=1, message=message),
        NotificationDeliveryLost(generation=2),
    ]
    assert late_collector.events == [
        NotificationDeliveryReady(generation=1),
        NotificationDeliveryLost(generation=2),
    ]


def test_queue_depth_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        _listener(delivery_queue_depth=0)


def test_start_delivers_readiness_and_messages_and_stops_cleanly() -> None:
    first = workflow_message()
    second = data_changed_message()
    connection = FakeConnection(
        notifications=[
            FakeNotify(NOTIFICATION_CHANNEL, encode_notification_payload(first)),
            FakeNotify(NOTIFICATION_CHANNEL, encode_notification_payload(second)),
        ]
    )
    listener = _listener(connect=static_connect(connection))
    collector = EventCollector()
    listener.register_consumer(collector)

    listener.start()
    assert wait_until(lambda: listener.ready) is True
    assert connection.executed_sql == [f"LISTEN {NOTIFICATION_CHANNEL}"]
    assert collector.wait_for(3) is True
    assert collector.events[0] == NotificationDeliveryReady(generation=1)
    assert collector.events[1] == NotificationDelivery(generation=1, message=first)
    assert collector.events[2] == NotificationDelivery(generation=1, message=second)

    listener.stop()
    assert connection.closed is True
    assert listener._reader is not None and not listener._reader.is_alive()
    assert listener._dispatcher is not None and not listener._dispatcher.is_alive()
    listener.stop()  # idempotent clean stop


def test_reconnect_restores_readiness_with_a_new_generation() -> None:
    # The first connection is lost during consumption; the reader reconnects
    # after bounded backoff and readiness is re-established on a fresh
    # generation — with the consolidated loss preceding the new readiness.
    lost = FakeConnection(notifies_error=OperationalError("connection lost"))
    recovered = FakeConnection(notifications=[FakeNotify(NOTIFICATION_CHANNEL, workflow_payload())])
    listener = _listener(connect=make_connect_factory(lost, recovered))
    collector = EventCollector()
    listener.register_consumer(collector)

    listener.start()
    assert wait_until(lambda: listener.ready and listener.generation == 3) is True
    assert lost.closed is True
    assert recovered.executed_sql == [f"LISTEN {NOTIFICATION_CHANNEL}"]

    assert collector.wait_for(3) is True
    losses = [e for e in collector.events if isinstance(e, NotificationDeliveryLost)]
    readies = [e for e in collector.events if isinstance(e, NotificationDeliveryReady)]
    messages = [e for e in collector.events if isinstance(e, NotificationDelivery)]
    assert len(losses) >= 1
    assert losses[-1].generation <= listener.generation
    assert readies and readies[-1].generation == listener.generation
    assert len(messages) == 1
    assert messages[0].generation == listener.generation

    listener.stop()
    assert recovered.closed is True


def test_start_is_idempotent() -> None:
    connection = FakeConnection()
    listener = _listener(connect=static_connect(connection))

    listener.start()
    reader = listener._reader
    dispatcher = listener._dispatcher
    assert reader is not None and dispatcher is not None

    listener.start()

    assert listener._reader is reader
    assert listener._dispatcher is dispatcher
    listener.stop()


class BlockingConnectFactory:
    """Connection factory that blocks in establishment until released."""

    def __init__(self) -> None:
        self.establishing = threading.Event()
        self.release = threading.Event()
        self.connection: FakeConnection | None = None

    def __call__(self, conninfo: str) -> FakeConnection:
        self.establishing.set()
        assert self.release.wait(5.0), "the blocked establishment was never released"
        self.connection = FakeConnection()
        return self.connection


def test_stop_during_connection_establishment_cannot_restore_readiness() -> None:
    # Shutdown occurring while a connection attempt is in flight is
    # irreversible: the late establishment and its LISTEN execution must
    # never restore readiness on the already-stopped listener.
    factory = BlockingConnectFactory()
    listener = _listener(connect=cast(ConnectionFactory, factory))
    collector = EventCollector()
    listener.register_consumer(collector)

    listener.start()
    assert factory.establishing.wait(5.0) is True

    stopper = threading.Thread(target=listener.stop)
    stopper.start()
    # Release the establishment only after the stop has been signaled.
    assert wait_until(lambda: listener._stop_event.is_set()) is True
    factory.release.set()
    stopper.join(10.0)

    assert listener.ready is False
    assert listener.generation == 0
    assert factory.connection is not None and factory.connection.closed is True
    assert listener._reader is not None and not listener._reader.is_alive()
    assert collector.events == []
