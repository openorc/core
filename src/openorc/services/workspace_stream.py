"""Authorized Workspace live-update stream services (issue #399, G4c).

The subscription policy behind the one authorized Workspace SSE live-update
endpoint: Owner-authorized access re-proof, Workspace-scoped delivery of
G4a's transient internal notification hints as the two public stream message
kinds, the bounded per-client handoff from the listener's dispatcher thread
into the serving event loop, and the stream's termination policy. All of it
is downstream of the notifications boundary, which deliberately owns no
browser transport, no authorization, and no per-client state.

Boundary facts this module owns:

- Notifications are internal hints, never authorization input. Every hint is
  scoped to the already-authorized Workspace, checked for delivery-generation
  currentness, and mapped onto the two named public message kinds:
  ``workflow_event`` (the hinted durable audit fact re-read through the
  focused authorized G2c read — never the raw notification, never raw rows or
  context) and ``data_changed`` (the identifier-only invalidation hint passed
  through the closed resource vocabulary, deletion-agnostic: the resource
  does not need to still exist to receive a correctly scoped invalidation).
- The thread-to-asyncio handoff is bounded end to end. One bounded staging
  buffer per subscription — the per-client buffer itself, with no second
  queue anywhere in the path — plus at most one pending wake handoff per
  subscription (a flag re-arms only when the buffer drains empty) means
  pending delivery callbacks can never accumulate ahead of the buffer's
  overflow protection. Overflow and listener loss are terminal facts recorded
  synchronously in the delivering thread and checked before staged data on
  every drain, so closing the affected subscription is never delayed behind
  accumulated notifications. The consumer callback stays non-blocking and
  O(1): scope and currentness checks, one bounded append, one coalesced wake.
  Only the affected subscription closes on overflow (reconnect starts over);
  the shared listener dispatcher and every other consumer are unaffected.
- Termination policy: a stream ends no later than the earlier of the verified
  credential expiry (the principal's verified ``exp``, surfaced by the #52
  authentication boundary) and a fixed, non-configurable maximum stream
  lifetime; and on any lost revalidation — the fixed-cadence re-proof composes
  the established FK-backed account-identity check (``ensure_profile``) with
  Workspace ownership (``require_profile_workspace``), including on quiet
  streams. Authorization is never inferred from a prior notification, and no
  mid-stream token re-verification or refresh mechanism exists: the client's
  documented reconnect is the credential-renewal path.
- Delivery is best-effort by design: messages may be lost, duplicated, or
  reordered; there is no replay, cursor, sequence ID, acknowledgement, or
  subscriber state. Durable reads through the existing G1/G2 APIs remain
  authoritative.

Consumer failures are contained per subscription and never affect the shared
listener, other consumers, or any committed work; stream delivery has no
Level 2/3 operational-failure path.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Final
from uuid import UUID

from psycopg import errors as psycopg_errors

from openorc.notifications.contract import (
    DataChangedNotification,
    NotificationMessage,
    WorkflowEventNotification,
)
from openorc.notifications.listener import (
    NotificationConsumerEvent,
    NotificationDelivery,
    NotificationDeliveryLost,
)
from openorc.persistence.ownership import ensure_profile
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import AuthenticationError, InvalidCommandError, NotFoundError
from openorc.services.event_reads import (
    PublicWorkflowEvent,
    read_workflow_event_for_owner,
)
from openorc.services.workspace_authorization import require_profile_workspace

__all__ = [
    "STREAM_MAX_LIFETIME_SECONDS",
    "STREAM_REVALIDATION_INTERVAL_SECONDS",
    "STREAM_STAGING_BUFFER_DEPTH",
    "WorkspaceStreamDataChanged",
    "WorkspaceStreamMessage",
    "WorkspaceStreamSubscription",
    "WorkspaceStreamTermination",
    "WorkspaceStreamWorkflowEvent",
    "project_stream_batch",
    "revalidate_workspace_stream_access",
    "stream_deadline",
]

# Fixed, non-configurable stream bounds (issue #399). There are deliberately
# no environment knobs: the maximum stream lifetime is the deployment-
# appropriate ceiling that additionally bounds every stream by its verified
# credential expiry, the revalidation cadence re-proofs access on quiet
# streams, and the staging depth is the complete per-client buffering bound.
STREAM_MAX_LIFETIME_SECONDS: Final[float] = 900.0
STREAM_REVALIDATION_INTERVAL_SECONDS: Final[float] = 60.0
STREAM_STAGING_BUFFER_DEPTH: Final[int] = 64

_CurrentGeneration = Callable[[], int]
_ScheduleWake = Callable[[], None]


class WorkspaceStreamTermination(StrEnum):
    """The closed terminal-fact vocabulary for one stream subscription.

    The reason is a safe diagnostic for the owning process only — it is never
    a third application message on the wire: the stream simply ends and the
    client reconnects.
    """

    CREDENTIAL_EXPIRED = "credential_expired"
    LIFETIME_ELAPSED = "lifetime_elapsed"
    DELIVERY_LOST = "delivery_lost"
    BUFFER_OVERFLOW = "buffer_overflow"
    ACCESS_LOST = "access_lost"
    ACCOUNT_UNAVAILABLE = "account_unavailable"


@dataclass(frozen=True, slots=True)
class WorkspaceStreamWorkflowEvent:
    """One resolved public WorkflowEvent audit fact for the live stream.

    The exact G2c public projection (safe allowlisted context included) of
    the hinted durable event — never the notification itself.
    """

    event: PublicWorkflowEvent


@dataclass(frozen=True, slots=True)
class WorkspaceStreamDataChanged:
    """One identifier-only invalidation hint for the live stream.

    The closed resource vocabulary and internal UUID identities pass through
    exactly as published by G4b; the workspace resource's ID equals the
    Workspace ID and the role_binding resource ID is the binding row's own
    identity (or the broader Workspace hint as published).
    """

    workspace_id: UUID
    resource_type: str
    resource_id: UUID


WorkspaceStreamMessage = WorkspaceStreamWorkflowEvent | WorkspaceStreamDataChanged


def stream_deadline(
    expires_at: datetime, *, now: datetime, max_lifetime_seconds: float
) -> datetime:
    """Return the stream's hard termination deadline (issue #399).

    The earlier of the verified credential expiry and the fixed maximum
    stream lifetime: a stream never outlives its credential, and never runs
    past the deployment-appropriate ceiling. Both instants must be
    timezone-aware; a non-UTC expiry is normalized.
    """
    expiry_utc = _require_aware_instant(expires_at, "the credential expiry")
    now_utc = _require_aware_instant(now, "the current instant")
    if (
        isinstance(max_lifetime_seconds, bool)
        or not isinstance(max_lifetime_seconds, (int, float))
        or max_lifetime_seconds <= 0
    ):
        raise InvalidCommandError("the maximum stream lifetime must be a positive number")
    lifetime_deadline = now_utc + timedelta(seconds=max_lifetime_seconds)
    return min(expiry_utc, lifetime_deadline)


def revalidate_workspace_stream_access(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> None:
    """Re-prove account identity and Workspace ownership for a live stream.

    The stream's bounded-cadence revalidation (and its open-time preflight)
    composes the two established authorization checks and nothing else: the
    FK-backed account-identity boundary — a deleted Auth account fails closed
    exactly as at authentication time — and the Workspace ownership resolver.
    Any failure is the typed application vocabulary and terminates the
    affected stream; a prior notification is never authorization input.
    """
    _require_uuid(profile_id, "profile_id")
    _require_uuid(workspace_id, "workspace_id")
    try:
        ensure_profile(pool, profile_id=profile_id)
    except psycopg_errors.ForeignKeyViolation as exc:
        raise AuthenticationError(
            "authentication failed: the account backing this session no longer exists"
        ) from exc
    require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)


def project_stream_batch(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    batch: list[NotificationMessage],
) -> list[WorkspaceStreamMessage]:
    """Project one staged hint batch into the public stream messages.

    ``data_changed`` hints map directly onto the public invalidation message
    (deletion-agnostic; the closed vocabulary is already decoded and scoped).
    ``workflow_event`` hints resolve through the focused authorized G2c read —
    the same projection and safe context allowlist the listings serve — and a
    hinted event that no longer resolves (deleted, absent, foreign) is
    omitted, never forwarded raw. The read is the authorization mechanism per
    hinted event; the notification is never trusted as one.
    """
    _require_uuid(profile_id, "profile_id")
    _require_uuid(workspace_id, "workspace_id")
    messages: list[WorkspaceStreamMessage] = []
    for notification in batch:
        if isinstance(notification, DataChangedNotification):
            if notification.workspace_id != workspace_id:
                continue
            messages.append(
                WorkspaceStreamDataChanged(
                    workspace_id=notification.workspace_id,
                    resource_type=notification.resource_type.value,
                    resource_id=notification.resource_id,
                )
            )
            continue
        if isinstance(notification, WorkflowEventNotification):
            if notification.workspace_id != workspace_id:
                continue
            try:
                event = read_workflow_event_for_owner(
                    pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    workflow_event_id=notification.workflow_event_id,
                )
            except NotFoundError:
                # Deleted, absent, or foreign events are omitted: the stream
                # carries only resolvable authorized audit facts.
                continue
            messages.append(WorkspaceStreamWorkflowEvent(event=event))
    return messages


class WorkspaceStreamSubscription:
    """One authorized per-client subscription bridging the shared listener's
    dispatcher thread into the serving event loop.

    The complete handoff is bounded: deliveries are staged only into the
    bounded per-subscription buffer (the per-client buffer itself — no second
    queue exists in the path), and at most one wake handoff is pending at any
    time (the flag re-arms only when the buffer drains empty), so pending
    delivery callbacks can never accumulate ahead of the buffer's overflow
    protection. Terminal facts (listener loss, buffer overflow) are recorded
    synchronously in the delivering thread and checked before staged data on
    every drain, so closing the affected subscription is never delayed behind
    accumulated notifications, and the callback never blocks the shared
    dispatcher.

    Foreign-Workspace hints and stale-generation deliveries are dropped at
    the callback: no cross-Workspace fan-out exists even for multiple tabs or
    clients of the same Workspace, and a stale delivery never establishes
    continuity.
    """

    def __init__(
        self,
        *,
        workspace_id: UUID,
        schedule_wake: _ScheduleWake,
        current_generation: _CurrentGeneration,
        staging_depth: int = STREAM_STAGING_BUFFER_DEPTH,
    ) -> None:
        if not isinstance(workspace_id, UUID):
            raise InvalidCommandError("workspace_id must be a UUID")
        if not isinstance(staging_depth, int) or staging_depth < 1:
            raise InvalidCommandError("the staging buffer depth must be a positive integer")
        self._workspace_id = workspace_id
        self._schedule_wake = schedule_wake
        self._current_generation = current_generation
        self._staging_depth = staging_depth
        self._lock = threading.Lock()
        self._staged: list[NotificationMessage] = []
        self._terminal: WorkspaceStreamTermination | None = None
        self._handoff_pending = False
        self._closed = False

    @property
    def workspace_id(self) -> UUID:
        """The subscribed Workspace (the one authorization scope)."""
        return self._workspace_id

    @property
    def terminal(self) -> WorkspaceStreamTermination | None:
        """The recorded terminal fact, if any (a monotone, one-way fact)."""
        with self._lock:
            return self._terminal

    def on_listener_event(self, event: NotificationConsumerEvent) -> None:
        """The registered consumer callback (runs on the listener's thread).

        Never blocks and never raises to the listener: scope and generation
        currentness checks run first, then one bounded append with a coalesced
        wake. Terminal facts — listener loss and buffer overflow — are
        recorded here synchronously so the drain closes the stream without
        waiting behind staged data. A readiness signal is deliberately a
        no-op: open-time readiness is proven by the transport before the
        stream starts, and listener loss owns every invalidation.
        """
        need_wake = False
        try:
            with self._lock:
                if self._closed or self._terminal is not None:
                    return
                if isinstance(event, NotificationDeliveryLost):
                    # The monotone loss fact: terminal immediately, checked
                    # before any staged data at the next drain.
                    self._terminal = WorkspaceStreamTermination.DELIVERY_LOST
                    need_wake = True
                elif isinstance(event, NotificationDelivery):
                    # The documented downstream currentness check: a delivery
                    # is current only while its generation is still the
                    # listener's current generation.
                    if event.generation != self._current_generation():
                        return
                    if event.message.workspace_id != self._workspace_id:
                        return
                    if len(self._staged) >= self._staging_depth:
                        self._terminal = WorkspaceStreamTermination.BUFFER_OVERFLOW
                        need_wake = True
                    else:
                        self._staged.append(event.message)
                        if not self._handoff_pending:
                            self._handoff_pending = True
                            need_wake = True
        finally:
            if need_wake:
                # A closed serving loop (process shutdown) has no wake
                # destination: the response generator is already ending.
                # Contained; the callback still returned.
                with contextlib.suppress(RuntimeError):
                    self._schedule_wake()

    def take_batch(self) -> tuple[WorkspaceStreamTermination | None, list[NotificationMessage]]:
        """Take one drain step (event loop side).

        Returns the terminal fact (if any) together with the atomically
        cleared staged batch; the caller checks the terminal first and never
        emits staged data past it. The pending-wake flag re-arms here, so a
        delivery staged after this take schedules a fresh wake.
        """
        with self._lock:
            self._handoff_pending = False
            batch = self._staged
            self._staged = []
            return self._terminal, batch

    def mark_closed(self) -> None:
        """Mark the subscription closed (cleanup; the transport unregisters it).

        Late listener-thread deliveries are dropped and the buffer is
        released.
        """
        with self._lock:
            self._closed = True
            self._staged = []


def _require_uuid(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any work runs."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_aware_instant(value: object, name: str) -> datetime:
    """Require a timezone-aware instant, normalized to UTC (never naive)."""
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.tzinfo.utcoffset(value) is None
    ):
        raise InvalidCommandError(f"{name} must be a timezone-aware timestamp")
    return value.astimezone(UTC)
