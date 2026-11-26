"""Process-local PostgreSQL notification listener for the API serving process.

One dedicated autocommit LISTEN connection per serving process, independent
of the ordinary request pool, consuming the fixed OpenOrc notification
channel through the pinned psycopg 3 ``Connection.notifies()`` generator and
delivering typed identifier-only messages plus readiness and loss signals to
registered consumers through a small bounded process-local interface.

Lifecycle: built and started by the API serving lifespan, exactly once per
serving process in either execution mode — never at import, in
``create_app()``, in an RQ parent or workhorse, or per browser connection.
The listener must be constructed and started in the OS process that serves
it; like every process-local connection state it is never inherited across a
fork boundary.

Transport facts honored by this module:

- LISTEN is session-bound: the listener endpoint must be a direct or
  session-mode-capable PostgreSQL endpoint. The resolution seam refuses the
  known transaction-pooler port instead of silently subscribing through a
  mode that would never deliver, and requires an explicitly supplied
  listener DSN to carry the same database name as ``DATABASE_URL`` — a guard
  against demonstrable configuration mistakes, never proof of database
  identity. That both endpoints target the same PostgreSQL deployment and
  database remains a documented operator configuration invariant.
- Delivery continuity is generation-based with no replay, cursor, sequence,
  notification persistence, or subscriber ledger. Loss, overflow, and
  reconnect invalidate the old delivery generation and communicate the need
  for fresh downstream synchronization; consumers own resynchronization.
- Bounded by construction: a fixed-depth hand-off queue (the reader never
  blocks on it), O(1) coalesced control state (no control queue, no
  per-generation backlog), a bounded notify tick, a bounded connect timeout,
  a rate-bounded capped reconnect backoff, and a bounded join on clean stop.
- Notification-consumer failure is contained per consumer and never rolls
  back already committed work or initiates Level 2/3 workflow handling.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, LiteralString, cast
from urllib.parse import urlparse

import psycopg
from psycopg import Connection

from openorc.config import (
    DATABASE_URL_SCHEMES,
    DATABASE_URL_VAR,
    DB_LISTENER_URL_VAR,
    DB_TRANSACTION_POOLER_PORTS,
    ConfigurationError,
    Settings,
)
from openorc.notifications.contract import (
    NOTIFICATION_CHANNEL,
    NotificationContractError,
    NotificationMessage,
    decode_notification_payload,
)

logger = logging.getLogger(__name__)

__all__ = [
    "BOUNDED_DELIVERY_QUEUE_DEPTH",
    "NOTIFY_WAIT_TICK_SECONDS",
    "NotificationConsumer",
    "NotificationConsumerEvent",
    "NotificationConsumerRegistration",
    "NotificationDeliveryLost",
    "NotificationDeliveryReady",
    "PostgresNotificationListener",
    "build_db_notification_listener",
    "resolve_db_listener_conninfo",
]

# Fixed non-configurable bounds (issue #397). Small by construction; there
# are deliberately no environment knobs for these.
BOUNDED_DELIVERY_QUEUE_DEPTH: Final[int] = 128
# The bounded notify wait: the reader re-checks its stop event at least once
# per tick, bounding stop latency and reconnect-loop responsiveness.
NOTIFY_WAIT_TICK_SECONDS: Final[float] = 1.0
# Bounded connection establishment: libpq raises ConnectionTimeout on expiry.
LISTENER_CONNECT_TIMEOUT_SECONDS: Final[int] = 10
RECONNECT_BACKOFF_INITIAL_SECONDS: Final[float] = 0.5
RECONNECT_BACKOFF_MAX_SECONDS: Final[float] = 8.0
# The dispatcher's bounded wake tick: control-signal latency stays bounded
# even when no further PostgreSQL notifications ever arrive.
DISPATCHER_WAKE_TICK_SECONDS: Final[float] = 0.5
THREAD_JOIN_TIMEOUT_SECONDS: Final[float] = 10.0

# The channel name is a compile-time module constant; LISTEN cannot be
# server-side parameterized, and the identifier needs no dynamic composition.
_LISTEN_SQL: Final[str] = f"LISTEN {NOTIFICATION_CHANNEL}"


def resolve_db_listener_conninfo(settings: Settings) -> str:
    """Resolve the effective notification-listener endpoint from settings.

    An unset ``OPENORC_DB_LISTENER_URL`` falls back to ``DATABASE_URL`` — the
    documented safe local/direct default. Guards enforced here (the only
    consumer of the setting, so worker processes neither require nor validate
    it):

    - accepted URL schemes (the same postgresql/postgres set as
      ``DATABASE_URL``);
    - the effective endpoint must not target a known transaction-pooler port:
      LISTEN is session-bound and cannot deliver through a transaction-pooling
      endpoint, so the configuration is refused instead of silently used;
    - an explicitly supplied listener DSN must carry the same database name
      as ``DATABASE_URL``. Database-name agreement is a guard against
      demonstrable configuration mistakes, NOT proof of database identity:
      that both endpoints target the same PostgreSQL deployment and database
      remains a documented operator configuration invariant.

    Error messages never echo the DSN values (credential material).
    """
    explicit = settings.db_listener_url
    conninfo = explicit if explicit is not None else settings.database_url
    parsed = urlparse(conninfo)
    if parsed.scheme.lower() not in DATABASE_URL_SCHEMES:
        raise ConfigurationError(
            "the notification listener endpoint (from "
            f"{DB_LISTENER_URL_VAR} or its {DATABASE_URL_VAR} fallback) must "
            f"use one of the schemes {', '.join(sorted(DATABASE_URL_SCHEMES))}"
        )
    try:
        listener_port = parsed.port
    except ValueError:
        raise ConfigurationError(
            f"the notification listener endpoint ({DB_LISTENER_URL_VAR} or its "
            f"{DATABASE_URL_VAR} fallback) is malformed"
        ) from None
    if listener_port is not None and listener_port in DB_TRANSACTION_POOLER_PORTS:
        raise ConfigurationError(
            "the notification listener endpoint targets the known "
            f"transaction-pooler port {listener_port}; LISTEN is session-bound "
            "and cannot deliver through a transaction-pooling endpoint. Supply "
            f"{DB_LISTENER_URL_VAR} pointing at a direct or session-mode "
            "endpoint for the same database"
        )
    if explicit is not None:
        listener_database = parsed.path.lstrip("/")
        base_database = urlparse(settings.database_url).path.lstrip("/")
        if not listener_database or not base_database:
            raise ConfigurationError(
                f"both {DB_LISTENER_URL_VAR} and {DATABASE_URL_VAR} must carry "
                "an explicit database name so the listener's target can be "
                "checked against the ordinary persistence endpoint"
            )
        if listener_database != base_database:
            raise ConfigurationError(
                f"{DB_LISTENER_URL_VAR} database name does not agree with the "
                f"{DATABASE_URL_VAR} database name; the listener must target "
                "the same database as ordinary persistence"
            )
    return conninfo


@dataclass(frozen=True, slots=True)
class NotificationDeliveryLost:
    """Typed loss signal: delivery continuity was invalidated.

    Everything delivered before this signal belongs to invalidated earlier
    generations. ``generation`` is the listener's current generation at
    coalescing time — an opaque diagnostic; the authoritative fact is the
    signal itself, which requires fresh downstream synchronization.
    Intermediate loss/reconnect/overflow episodes coalesce into one signal.
    """

    generation: int


@dataclass(frozen=True, slots=True)
class NotificationDeliveryReady:
    """Typed readiness signal: the listener delivers for ``generation``.

    Readiness is authoritative: this signal is emitted only for a generation
    whose dedicated LISTEN connection is actually active. Superseded pending
    readiness (cleared by connection loss or shutdown) is never emitted.
    """

    generation: int


NotificationConsumerEvent = (
    NotificationMessage | NotificationDeliveryLost | NotificationDeliveryReady
)
NotificationConsumer = Callable[[NotificationConsumerEvent], None]


@dataclass(frozen=True, slots=True)
class NotificationConsumerRegistration:
    """Generation-aware consumer registration handle.

    ``generation`` records the delivery generation at registration so a
    consumer can defensively detect that its subscription began before a
    later invalidation it has not yet observed.
    """

    consumer_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class _ControlTake:
    """One lock-protected coalesced control-state read."""

    generation: int
    ready: bool
    pending_loss: bool
    pending_ready: bool
    stopped: bool


class _DeliveryState:
    """O(1) coalesced delivery control state.

    All access happens under the listener's state lock. The state carries the
    authoritative readiness flag, the monotonic delivery generation, and at
    most one pending loss and one pending readiness fact — intermediate
    episodes coalesce, so repeated connection losses, reconnects, and
    overflow episodes cannot accumulate an unbounded pending backlog.
    """

    __slots__ = ("generation", "pending_loss", "pending_ready", "ready", "stopped")

    def __init__(self) -> None:
        self.generation = 0
        self.ready = False
        self.pending_loss = False
        self.pending_ready = False
        self.stopped = False

    def note_listen_ready(self) -> None:
        """Successful LISTEN established readiness for a fresh generation."""
        self.generation += 1
        self.ready = True
        self.pending_ready = True

    def note_overflow(self) -> None:
        """Delivery overflow on a healthy LISTEN connection.

        A fresh delivery generation begins and requires downstream
        resynchronization (the pre-break buffered entries become stale by
        construction); the connection itself remains ready, so the fresh
        generation is genuinely live.
        """
        self.generation += 1
        self.pending_loss = True
        self.pending_ready = True

    def note_connection_lost(self) -> None:
        """Connection loss invalidates readiness and supersedes pending readiness."""
        self.generation += 1
        self.ready = False
        self.pending_ready = False
        self.pending_loss = True

    def note_stopped(self) -> None:
        """Clean stop: readiness ends and any pending readiness is superseded."""
        self.ready = False
        self.pending_ready = False
        self.stopped = True

    def take_pending(self) -> _ControlTake:
        """Coalesce and clear the pending control facts for one dispatch cycle.

        The pending flags collapse to the current generation's truth;
        ``pending_ready`` is meaningful only while ``ready`` is true.
        """
        take = _ControlTake(
            generation=self.generation,
            ready=self.ready,
            pending_loss=self.pending_loss,
            pending_ready=self.pending_ready,
            stopped=self.stopped,
        )
        self.pending_loss = False
        self.pending_ready = False
        return take


@dataclass(frozen=True, slots=True)
class _DeliveryEntry:
    """One decoded message tagged with the generation it was read in."""

    generation: int
    message: NotificationMessage


ConnectionFactory = Callable[[str], Connection[Any]]


def _default_connect(conninfo: str) -> Connection[Any]:
    """Open the dedicated autocommit LISTEN connection (owning process only).

    Bounded establishment through libpq's ``connect_timeout``; prepared
    statements stay disabled for pooler-neutral consistency with the
    ordinary persistence connections.
    """
    return psycopg.connect(
        conninfo,
        autocommit=True,
        prepare_threshold=None,
        connect_timeout=LISTENER_CONNECT_TIMEOUT_SECONDS,
    )


class PostgresNotificationListener:
    """One process-local dedicated LISTEN connection and its bounded
    process-local delivery interface for downstream G4c consumption.

    The listener owns two daemon threads: a reader that exclusively owns the
    dedicated connection (the ``notifies()`` generator holds the connection
    lock for its lifetime, so nothing else may touch the connection) and a
    dispatcher that hands typed events to registered consumers. Delivery is
    bounded and coalesced per the module docstring; consumers must be
    non-blocking and own their own resynchronization.

    There is deliberately no per-client queue, browser authorization, public
    message serialization, replay, cursor, or subscriber ledger here.
    """

    def __init__(
        self,
        conninfo: str,
        *,
        connect: ConnectionFactory | None = None,
        delivery_queue_depth: int = BOUNDED_DELIVERY_QUEUE_DEPTH,
    ) -> None:
        if delivery_queue_depth < 1:
            raise ValueError("delivery_queue_depth must be positive")
        self._conninfo = conninfo
        self._connect: ConnectionFactory = _default_connect if connect is None else connect
        self._deliveries: queue.Queue[_DeliveryEntry] = queue.Queue(maxsize=delivery_queue_depth)
        self._state_lock = threading.Lock()
        self._state = _DeliveryState()
        self._wake = threading.Event()
        self._stop_event = threading.Event()
        self._registration_lock = threading.Lock()
        self._consumers: dict[int, NotificationConsumer] = {}
        self._next_consumer_id = 0
        self._reader: threading.Thread | None = None
        self._dispatcher: threading.Thread | None = None
        self._started = False

    @property
    def ready(self) -> bool:
        """Whether the current generation has an active LISTEN connection."""
        with self._state_lock:
            return self._state.ready

    @property
    def generation(self) -> int:
        """The current opaque monotonic delivery generation."""
        with self._state_lock:
            return self._state.generation

    def start(self) -> None:
        """Start the reader and dispatcher threads. Idempotent; never restarts
        a stopped listener. Must run in the owning serving process."""
        with self._state_lock:
            if self._started or self._state.stopped:
                return
            self._started = True
        reader = threading.Thread(
            target=self._run_reader,
            name="openorc-notification-reader",
            daemon=True,
        )
        dispatcher = threading.Thread(
            target=self._run_dispatcher,
            name="openorc-notification-dispatcher",
            daemon=True,
        )
        self._reader = reader
        self._dispatcher = dispatcher
        dispatcher.start()
        reader.start()

    def stop(self) -> None:
        """Signal a clean stop and join both threads with a bounded timeout.

        Idempotent. The reader thread closes its own connection — a foreign
        close could not take the connection lock the ``notifies()`` generator
        holds. Readiness ends and any pending readiness is superseded, so no
        readiness signal can be emitted after the stop.
        """
        self._stop_event.set()
        with self._state_lock:
            self._state.note_stopped()
        self._wake.set()
        for thread in (self._reader, self._dispatcher):
            if thread is not None:
                thread.join(THREAD_JOIN_TIMEOUT_SECONDS)

    def register_consumer(self, callback: NotificationConsumer) -> NotificationConsumerRegistration:
        """Register one non-blocking typed consumer.

        The handle records the delivery generation at registration. A
        consumer subscribed during a pending invalidation episode still
        receives the coalesced loss signal; consumer callbacks must never
        block and must not raise (failures are contained per consumer).
        """
        with self._state_lock:
            generation = self._state.generation
        with self._registration_lock:
            consumer_id = self._next_consumer_id
            self._next_consumer_id += 1
            self._consumers[consumer_id] = callback
        return NotificationConsumerRegistration(consumer_id=consumer_id, generation=generation)

    def unregister_consumer(self, registration: NotificationConsumerRegistration) -> None:
        """Remove one consumer registration. Unknown handles are ignored."""
        with self._registration_lock:
            self._consumers.pop(registration.consumer_id, None)

    # -- reader thread ---------------------------------------------------

    def _run_reader(self) -> None:
        """Own the dedicated connection: bounded connect, LISTEN, consume,
        reconnect with capped backoff, and clean stop."""
        backoff = RECONNECT_BACKOFF_INITIAL_SECONDS
        while not self._stop_event.is_set():
            connection: Connection[Any] | None = None
            try:
                connection = self._connect(self._conninfo)
                # The channel constant is compile-time module text; the cast
                # satisfies the driver's injection-safe LiteralString query
                # contract for repository-controlled non-literal SQL, as
                # elsewhere in the repository.
                connection.execute(cast(LiteralString, _LISTEN_SQL))
            except Exception as error:  # noqa: BLE001 - contained connection failure
                logger.warning(
                    "the notification listener connection attempt failed (%s); "
                    "retrying with bounded backoff",
                    type(error).__name__,
                )
                self._note_connection_lost()
                self._close_quietly(connection)
                if self._wait_before_reconnect(backoff):
                    return
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_SECONDS)
                continue
            # Readiness is published only after LISTEN is active on the live
            # connection — never before.
            self._note_listen_ready()
            backoff = RECONNECT_BACKOFF_INITIAL_SECONDS
            try:
                self._consume(connection)
            except Exception as error:  # noqa: BLE001 - contained connection failure
                if self._stop_event.is_set():
                    self._close_quietly(connection)
                    return
                logger.warning(
                    "the notification listener connection was lost (%s); "
                    "reconnecting with bounded backoff",
                    type(error).__name__,
                )
                self._note_connection_lost()
                self._close_quietly(connection)
                if self._wait_before_reconnect(backoff):
                    return
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_SECONDS)
            else:
                # The consume loop returned because the stop event fired.
                self._close_quietly(connection)
                return

    def _consume(self, connection: Connection[Any]) -> None:
        """Consume notifications through the pinned psycopg 3 generator.

        The synchronous ``Connection.notifies()`` generator ends at each
        bounded ``timeout`` deadline and is re-invoked after a stop-event
        check; psycopg drains its internal notification backlog at each
        re-invocation, so nothing received between invocations is lost. The
        generator holds the connection lock for its lifetime: this thread
        exclusively owns the connection.
        """
        while not self._stop_event.is_set():
            for notify in connection.notifies(timeout=NOTIFY_WAIT_TICK_SECONDS):
                # Exactly one channel is listened to; other channels would be
                # a server-side misconfiguration and are skipped defensively.
                if notify.channel != NOTIFICATION_CHANNEL:
                    continue
                self._deliver_incoming_payload(notify.payload)

    def _deliver_incoming_payload(self, payload: str) -> None:
        """Decode one raw payload and hand it to the bounded queue.

        Malformed or unknown payload shapes are rejected fail-closed with a
        sanitized diagnostic. When the bounded queue is full, the message is
        dropped and the coalesced overflow invalidation marks the delivery
        generation stale — the reader never blocks on the queue.
        """
        try:
            message = decode_notification_payload(payload)
        except NotificationContractError as error:
            logger.warning("rejected a malformed notification payload: %s", error)
            return
        with self._state_lock:
            if self._state.stopped:
                return
            generation = self._state.generation
        entry = _DeliveryEntry(generation=generation, message=message)
        try:
            self._deliveries.put_nowait(entry)
        except queue.Full:
            self._note_overflow()

    def _wait_before_reconnect(self, delay: float) -> bool:
        """Wait the capped backoff delay, interruptible by the stop event."""
        return self._stop_event.wait(delay)

    def _close_quietly(self, connection: Connection[Any] | None) -> None:
        if connection is None:
            return
        try:
            connection.close()
        except Exception as error:  # noqa: BLE001 - shutdown hygiene
            logger.debug(
                "the notification listener connection close failed (%s)",
                type(error).__name__,
            )

    # -- control-state transitions ----------------------------------------

    def _note_listen_ready(self) -> None:
        with self._state_lock:
            self._state.note_listen_ready()
        self._wake.set()

    def _note_overflow(self) -> None:
        with self._state_lock:
            self._state.note_overflow()
        self._wake.set()

    def _note_connection_lost(self) -> None:
        with self._state_lock:
            self._state.note_connection_lost()
        self._wake.set()

    # -- dispatcher thread -------------------------------------------------

    def _run_dispatcher(self) -> None:
        while True:
            self._wake.wait(DISPATCHER_WAKE_TICK_SECONDS)
            if self._run_dispatch_cycle():
                return

    def _take_state(self) -> _ControlTake:
        """One lock-protected coalesced take that also clears the wake event.

        Clearing the event inside the same critical section as the take keeps
        reader transitions (flags + wake) and dispatcher takes race-free: a
        transition that lands after a take always re-sets the event.
        """
        with self._state_lock:
            take = self._state.take_pending()
            self._wake.clear()
        return take

    def _run_dispatch_cycle(self) -> bool:
        """Run one dispatch cycle. Returns True when the dispatcher must exit.

        Coalesced control signals are emitted first, then queued deliveries.
        ``DeliveryReady`` is emitted only when the taken generation is still
        the current generation AND readiness is still authoritative at
        delivery time — a readiness snapshot concurrently superseded by a
        connection loss or shutdown can never establish downstream
        continuity. Queued entries whose tagged generation differs from the
        freshly-read current generation are stale and are never delivered as
        current messages.
        """
        take = self._take_state()
        if take.stopped:
            return True
        self._emit_control_signals(take)
        while True:
            try:
                entry = self._deliveries.get_nowait()
            except queue.Empty:
                return False
            take = self._take_state()
            if take.stopped:
                return True
            self._emit_control_signals(take)
            if entry.generation != take.generation:
                continue
            self._dispatch_event(entry.message)

    def _emit_control_signals(self, take: _ControlTake) -> None:
        """Emit the coalesced loss signal, then an authoritative readiness.

        A pending readiness is delivered only when the taken generation is
        still current and the state still reports readiness at delivery time;
        superseded readiness (connection loss or shutdown cleared it) is
        dropped, so no readiness without an active LISTEN is ever emitted.
        """
        if take.pending_loss:
            self._dispatch_event(NotificationDeliveryLost(generation=take.generation))
        if take.pending_ready:
            with self._state_lock:
                readiness_current = (
                    not self._state.stopped
                    and self._state.ready
                    and self._state.generation == take.generation
                )
            if readiness_current:
                self._dispatch_event(NotificationDeliveryReady(generation=take.generation))

    def _dispatch_event(self, event: NotificationConsumerEvent) -> None:
        """Hand one typed event to every registered consumer.

        Consumer failures are contained per consumer with sanitized
        diagnostics and never affect other consumers, the listener threads,
        or any committed work.
        """
        with self._registration_lock:
            consumers = tuple(self._consumers.items())
        for consumer_id, callback in consumers:
            try:
                callback(event)
            except Exception as error:  # noqa: BLE001 - contained consumer failure
                logger.warning(
                    "notification consumer %s failed with %s; delivery to the "
                    "remaining consumers continues",
                    consumer_id,
                    type(error).__name__,
                )


def build_db_notification_listener(settings: Settings) -> PostgresNotificationListener:
    """Resolve the dedicated listener endpoint and build the listener.

    Called by the API serving lifespan, once per serving process in either
    execution mode. Endpoint misconfiguration fails here, at serving startup.
    """
    return PostgresNotificationListener(resolve_db_listener_conninfo(settings))
