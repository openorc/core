"""Authorized Workspace SSE live-update transport (issue #399, G4c).

The one authorized live-update endpoint for application clients: a
per-Workspace ``text/event-stream`` subscription consuming the shared
process-local notification listener's typed hints and delivering exactly two
named application message kinds — ``workflow_event`` (the hinted durable
audit fact re-read through the focused authorized G2c read, serialized with
the existing :class:`WorkflowEventRead` DTO) and ``data_changed`` (the
identifier-only invalidation hint through the closed resource vocabulary).
SSE comment lines are transport-level only: the initial readiness comment
and periodic heartbeats — never a third application message.

Transport rules owned here:

- Authorization is re-proved before the response starts (off the event loop),
  rechecked after registration together with the subscription's terminal
  facts (an invalidated subscription or a listener that lost readiness fails
  closed as 503 — the readiness comment is never emitted for a dead stream),
  and enforced volume-independently for the stream's whole lifetime: the
  deadline and the fixed bounded-cadence re-proof run at the top of every
  bounded drain cycle and every emitted frame is re-checked, so sustained
  delivery or a slow consumer can never bypass credential expiry, the fixed
  maximum stream lifetime, or a known invalidation. Known access loss ends
  the stream; the documented client reconnect re-subscribes and reloads.
- The consumer is registered and listener readiness is positively checked
  before the initial readiness comment is emitted; an unavailable listener
  fails closed (503) instead of reporting a healthy live stream. There is no
  replay before or after the comment — a fresh subscription reloads current
  resources and bounded history through the existing G1/G2 APIs.
- The complete per-client handoff is bounded (bounded staging buffer plus
  the subscription's single coalesced wake); overflow closes only the
  affected stream. Every blocking database step (preflight, per-batch
  projection, revalidation) runs off the event loop in short transactions —
  no transaction or pooled connection is held across the stream lifetime,
  and no thread or connection exists per event.
- Delivery is best-effort: no Last-Event-ID, cursor, sequence ID,
  acknowledgement, or subscriber state. Clients deduplicate
  ``workflow_event`` by the event UUID and treat durable reads as
  authoritative.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from starlette.responses import StreamingResponse

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import (
    AuthenticatedSession,
    authenticated_session,
)
from openorc.api.schemas.events import WorkflowEventRead
from openorc.api.schemas.stream import WorkspaceStreamDataChangedRead
from openorc.config import Settings
from openorc.notifications.listener import NotificationConsumerRegistration
from openorc.persistence.pool import get_database_pool
from openorc.services import workspace_stream
from openorc.services.errors import ApplicationError, IntegrationNotConfiguredError
from openorc.services.event_reads import PublicWorkflowEvent
from openorc.services.workspace_stream import (
    WorkspaceStreamMessage,
    WorkspaceStreamSubscription,
)

router = APIRouter(tags=["workspace-stream"])

AuthenticatedStreamSession = Annotated[AuthenticatedSession, Depends(authenticated_session)]

_STREAM_PATH = "/workspaces/{workspace_id}/stream"

# Fixed, non-configurable transport cadence (issue #399): SSE comment
# heartbeats keep quiet streams observable; the revalidation cadence and
# every other bound live in the service. There are deliberately no
# environment knobs.
_HEARTBEAT_INTERVAL_SECONDS = 15.0

# The drain cap per enforcement cycle (issue #399): the deadline check and
# the periodic authorization revalidation run at least once per cycle
# regardless of notification volume, and every emitted frame is re-checked
# against the deadline and the subscription's terminal facts — sustained
# delivery can never bypass either bound.
_MAX_DRAIN_BATCHES_PER_CYCLE = 4

# The transport-level readiness comment: emitted strictly after the consumer
# is registered on a positively ready listener, before any application
# message. A client treats it as the live subscription's start.
_READY_COMMENT = ": openorc workspace stream ready\n\n"
_HEARTBEAT_COMMENT = ": heartbeat\n\n"

_STREAM_OK_200: dict[str, Any] = {
    "description": (
        "Authorized Workspace SSE live stream (issue #399). One subscription "
        "covers every Task and Repository in the Workspace and carries exactly "
        "two named application message kinds: workflow_event — the durable "
        "WorkflowEvent audit fact in the exact G2c WorkflowEventRead shape "
        "(id, workspace_id, nullable task_id, event_type, actor_type, nullable "
        "actor_id, paired nullable subject_type/subject_id, safe context, "
        "created_at UTC; deduplicate by id) — and data_changed — the "
        'identifier-only invalidation envelope {"workspace_id", '
        '"resource_type", "resource_id"} over the closed vocabulary '
        "(workspace, project, repository, issue, task, connection, "
        "role_binding, refresh; the resource may already be deleted). SSE "
        "comment lines are transport-level only: the initial ': openorc "
        "workspace stream ready' comment signals the live subscription's start "
        "(register-before-readiness; no replay before or after it) and ': "
        "heartbeat' comments keep quiet streams observable. Delivery is "
        "best-effort: messages may be lost, duplicated, or reordered; there is "
        "no Last-Event-ID, cursor, watermark, replay, or acknowledgement. "
        "Clients reconnect by establishing a fresh subscription (with a fresh "
        "session JWT — the stream ends no later than credential expiry), then "
        "reload relevant current resources and bounded audit history through "
        "the existing reads, buffering and coalescing invalidations during "
        "those reads; durable reads are always authoritative."
    ),
    "content": {"text/event-stream": {"schema": {"type": "string"}}},
}

_STREAM_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: _STREAM_OK_200,
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    503: DOCUMENTED_UNAVAILABLE_503,
}

_ScheduleWake = Callable[[], None]


def _utcnow() -> datetime:
    """The stream's clock seam (issue #399): injectable for deterministic tests."""
    return datetime.now(UTC)


def _bind_wake(wake: asyncio.Event) -> _ScheduleWake:
    """Bind the subscription's wake handoff to the serving event loop.

    The returned callable runs on the listener's dispatcher thread: the
    loop-indirect set is the only thread-safe signal and it never blocks.
    """
    loop = asyncio.get_running_loop()

    def schedule() -> None:
        loop.call_soon_threadsafe(wake.set)

    return schedule


def _workflow_event_to_wire(event: PublicWorkflowEvent) -> WorkflowEventRead:
    """Map one projected public event to its exact G2c wire shape (issue #399)."""
    return WorkflowEventRead(
        id=event.id,
        workspace_id=event.workspace_id,
        task_id=event.task_id,
        event_type=event.event_type,
        actor_type=event.actor_type,
        actor_id=event.actor_id,
        subject_type=event.subject_type,
        subject_id=event.subject_id,
        context=dict(event.context),
        created_at=event.created_at,
    )


def _frame_for(message: WorkspaceStreamMessage) -> str:
    """Frame one public stream message as its exact named SSE application message.

    Exactly two kinds exist: ``workflow_event`` carries the G2c DTO JSON and
    ``data_changed`` carries the identifier-only invalidation envelope; each
    JSON body is single-line, so the ``event``/``data`` framing is exact.
    """
    if isinstance(message, workspace_stream.WorkspaceStreamWorkflowEvent):
        payload = _workflow_event_to_wire(message.event).model_dump_json()
        return f"event: workflow_event\ndata: {payload}\n\n"
    payload = WorkspaceStreamDataChangedRead(
        workspace_id=message.workspace_id,
        resource_type=message.resource_type,
        resource_id=message.resource_id,
    ).model_dump_json()
    return f"event: data_changed\ndata: {payload}\n\n"


@router.get(
    _STREAM_PATH,
    operation_id="stream_workspace_updates",
    responses=_STREAM_RESPONSES,
    response_class=StreamingResponse,
)
async def stream_workspace_updates(
    request: Request,
    session: AuthenticatedStreamSession,
    workspace_id: UUID,
) -> StreamingResponse:
    """Subscribe to one Workspace's authorized live-update stream (issue #399).

    One subscription covers every Task and Repository in the Workspace.
    Authorization is re-proved off the event loop before the response starts;
    the consumer is registered and listener readiness is positively checked
    before the initial readiness comment; and the stream ends no later than
    the earlier of the verified credential expiry and the fixed maximum
    lifetime. Termination is silent by design (the stream simply ends); the
    client reconnects and reloads.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    listener = request.app.state.notification_listener
    # The open-time preflight (off the event loop): the established
    # account-identity and Workspace-ownership checks re-proved before the
    # response starts; typed failures map to the documented HTTP errors.
    await asyncio.to_thread(
        workspace_stream.revalidate_workspace_stream_access,
        pool,
        profile_id=session.profile.id,
        workspace_id=workspace_id,
    )
    # Honest live-stream readiness: an unavailable listener fails closed
    # instead of reporting a healthy live stream. Readiness is checked
    # strictly before registration and the readiness comment.
    if not listener.ready:
        raise IntegrationNotConfiguredError(
            "the live Workspace update stream is not available in this serving process"
        )
    # The hard termination deadline: the earlier of the verified credential
    # expiry and the fixed maximum stream lifetime (issue #399).
    deadline = workspace_stream.stream_deadline(
        session.expires_at,
        now=_utcnow(),
        max_lifetime_seconds=workspace_stream.STREAM_MAX_LIFETIME_SECONDS,
    )
    wake = asyncio.Event()
    subscription = WorkspaceStreamSubscription(
        workspace_id=workspace_id,
        schedule_wake=_bind_wake(wake),
        current_generation=lambda: listener.generation,
        staging_depth=workspace_stream.STREAM_STAGING_BUFFER_DEPTH,
    )
    # Registration precedes the response and the readiness comment: no
    # healthy stream is ever reported without a live consumer.
    registration: NotificationConsumerRegistration = listener.register_consumer(
        subscription.on_listener_event
    )
    # The registration raced the listener's health: recheck readiness and the
    # subscription's terminal facts before any readiness signal. A listener
    # that lost readiness, or a subscription already invalidated between the
    # registration and here (a loss or overflow landed first), fails closed
    # as 503 — the readiness comment is never emitted for a dead stream and
    # the consumer is cleaned up.
    if not listener.ready or subscription.terminal is not None:
        listener.unregister_consumer(registration)
        subscription.mark_closed()
        raise IntegrationNotConfiguredError(
            "the live Workspace update stream is not available in this serving process"
        )
    return StreamingResponse(
        _stream_events(
            pool=pool,
            subscription=subscription,
            registration=registration,
            listener=listener,
            profile_id=session.profile.id,
            workspace_id=workspace_id,
            deadline=deadline,
            wake=wake,
            heartbeat_interval=_HEARTBEAT_INTERVAL_SECONDS,
            revalidation_interval=workspace_stream.STREAM_REVALIDATION_INTERVAL_SECONDS,
            drain_batch_cap=_MAX_DRAIN_BATCHES_PER_CYCLE,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


async def _stream_events(
    *,
    pool: Any,
    subscription: WorkspaceStreamSubscription,
    registration: NotificationConsumerRegistration,
    listener: Any,
    profile_id: UUID,
    workspace_id: UUID,
    deadline: datetime,
    wake: asyncio.Event,
    heartbeat_interval: float,
    revalidation_interval: float,
    drain_batch_cap: int,
) -> AsyncIterator[str]:
    """Run one authorized stream to its termination (issue #399).

    Drains the subscription's bounded staging buffer terminal-first, frames
    the two named application message kinds, emits comment-line heartbeats,
    and re-proves access on the fixed bounded cadence — including on quiet
    streams. Enforcement is volume-independent: the deadline and the
    authorization revalidation run at the top of every bounded drain cycle,
    and every emitted frame is re-checked against the deadline and the
    subscription's terminal facts, so sustained delivery or a slow consumer
    can never carry an already-drained batch past credential expiry or past a
    known invalidation. Termination is the generator ending: the deadline
    (credential expiry or maximum lifetime), a listener-loss or overflow
    terminal fact, or any typed revalidation failure. Cleanup unregisters
    the consumer and releases the buffer on every exit path, including
    client disconnect and shutdown.
    """
    try:
        # Readiness is re-checked before the comment: an invalidated
        # subscription (defense in depth behind the route's post-registration
        # recheck) never reports a healthy live stream.
        if subscription.terminal is not None or not listener.ready:
            return
        # Transport-level readiness: strictly after registration on a
        # positively ready listener, before any application message. No
        # replay exists before or after it.
        yield _READY_COMMENT
        next_heartbeat = _utcnow() + timedelta(seconds=heartbeat_interval)
        next_revalidation = _utcnow() + timedelta(seconds=revalidation_interval)
        while True:
            # The per-cycle enforcement checkpoint: deadline first, then the
            # fixed-cadence re-proof — both effective regardless of
            # notification volume because the drain below is bounded.
            now = _utcnow()
            if now >= deadline:
                return
            if now >= next_revalidation:
                # The fixed-cadence re-proof (off the event loop), including
                # on quiet streams; a typed failure terminates the stream.
                await asyncio.to_thread(
                    workspace_stream.revalidate_workspace_stream_access,
                    pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                )
                next_revalidation = now + timedelta(seconds=revalidation_interval)
            # Bounded drain: at most ``drain_batch_cap`` batches per cycle so
            # the enforcement checkpoint above is always reached.
            for _ in range(drain_batch_cap):
                terminal, batch = subscription.take_batch()
                if terminal is not None:
                    return
                if not batch:
                    break
                messages = await asyncio.to_thread(
                    workspace_stream.project_stream_batch,
                    pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    batch=batch,
                )
                for message in messages:
                    # Per-frame re-check: an already-drained batch never
                    # emits past credential expiry or a known invalidation.
                    if _utcnow() >= deadline or subscription.terminal is not None:
                        return
                    yield _frame_for(message)
            # Bounded wait: the next wake, heartbeat, revalidation, or the
            # deadline — whichever comes first.
            wait_until = min(next_heartbeat, next_revalidation, deadline)
            remaining = (wait_until - _utcnow()).total_seconds()
            if remaining > 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wake.wait(), timeout=remaining)
            wake.clear()
            now = _utcnow()
            if now >= next_heartbeat:
                yield _HEARTBEAT_COMMENT
                next_heartbeat = now + timedelta(seconds=heartbeat_interval)
    except ApplicationError:
        # Access/identity loss and every other typed application failure
        # terminate the affected stream only; the client reconnects.
        return
    finally:
        # Cleanup on every exit path: termination, client disconnect,
        # cancellation, and shutdown. Late listener deliveries are dropped.
        listener.unregister_consumer(registration)
        subscription.mark_closed()
