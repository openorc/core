"""Transport tests for the authorized Workspace SSE live stream (issue #399).

Route-level coverage of the streaming contract, driven by a deterministic
fake of the G4a listener delivery interface injected at the lifespan seam:
the initial readiness comment ordering (register-before-readiness), the two
exact named wire message kinds (the G2c ``WorkflowEventRead`` DTO JSON and
the identifier-only ``data_changed`` envelope), the focused authorized
resolution of hinted events with unresolvable events omitted, cross-Workspace
access and hint routing, the fail-closed unavailable listener (503), the
uniform 401, credential-expiry termination through the clock seam, the
bounded slow-subscriber overflow closing only the affected subscription, no
historical replay, and cleanup unregistration. The auth matrices, the G2c
projection internals, and the listener transport are owned by their own
suites and only scripted here.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

import openorc.api.app as app_module
from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.dependencies.authentication import AuthenticatedSession
from openorc.api.routers import workspace_stream as stream_router
from openorc.api.schemas.events import WorkflowEventRead
from openorc.api.schemas.stream import WorkspaceStreamDataChangedRead
from openorc.domain.ownership import Profile
from openorc.notifications.contract import (
    DataChangedNotification,
    NotificationResourceType,
    WorkflowEventNotification,
)
from openorc.notifications.listener import (
    NotificationConsumerRegistration,
    NotificationDelivery,
    NotificationDeliveryLost,
)
from openorc.observability import shutdown_observability
from openorc.services import event_reads, workspace_stream
from openorc.services.errors import AuthenticationError, NotFoundError
from openorc.services.event_reads import PublicWorkflowEvent
from openorc.workers.invocation import (
    LateBoundDetachedTurnObserver,
    LateBoundProcessSubmitter,
    OperationDependencies,
)

_OBSERVED = datetime(2026, 11, 26, 11, 30, 0, tzinfo=UTC)
_READY_COMMENT = ": openorc workspace stream ready\n\n"


class FakeNotificationListener:
    """Deterministic fake of the G4a delivery interface at the lifespan seam.

    ``burst_on_register`` scripts events delivered synchronously inside
    ``register_consumer`` — exactly how a listener handoff can land between
    registration and the stream generator's first drain, and the deterministic
    way to prove overflow-before-first-drain.
    """

    def __init__(self, *, ready: bool = True, burst_on_register: list[Any] | None = None) -> None:
        self._ready = ready
        self.generation_value = 1
        self.consumers: list[Any] = []
        self.unregistered: list[Any] = []
        self.burst_on_register = list(burst_on_register or [])
        self.start_calls = 0
        self.stop_calls = 0

    def start(self) -> None:
        self.start_calls += 1

    def stop(self) -> None:
        self.stop_calls += 1

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def generation(self) -> int:
        return self.generation_value

    def register_consumer(self, callback: Any) -> NotificationConsumerRegistration:
        self.consumers.append(callback)
        registration = NotificationConsumerRegistration(
            consumer_id=len(self.consumers), generation=self.generation_value
        )
        for event in self.burst_on_register:
            callback(event)
        return registration

    def unregister_consumer(self, registration: Any) -> None:
        self.unregistered.append(registration)

    def dispatch(self, event: Any) -> None:
        for callback in tuple(self.consumers):
            callback(event)


@pytest.fixture()
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


def _session(profile: Profile, *, expires_at: datetime) -> AuthenticatedSession:
    return AuthenticatedSession(profile=profile, expires_at=expires_at)


@contextmanager
def _stream_client(
    monkeypatch: pytest.MonkeyPatch,
    settings: Any,
    session: AuthenticatedSession,
    listener: FakeNotificationListener,
) -> Iterator[TestClient]:
    """Serve the app once with the fake listener bound at the lifespan seam.

    The serving lifespan runs exactly like production (fake listener; the
    bounded recovery passes run and fail contained in milliseconds against
    the no-provider dependency seam, exactly like the established lifespan
    tests) so ``app.state.notification_listener`` is the fake the route
    consumes; observability's process-global state is shut down after the
    serving context.
    """
    monkeypatch.setattr(app_module, "_build_notification_listener", lambda _settings: listener)
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: OperationDependencies(
            settings=settings,
            # The regular-mode late-bound seams the lifespan binds; no
            # database pool provider, so the bounded recovery passes fail
            # contained in milliseconds instead of dialing a database.
            continuation_submitter_provider=LateBoundProcessSubmitter(),
            detached_turn_observer_provider=LateBoundDetachedTurnObserver(),
        ),
        raising=True,
    )
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_session] = lambda: session
    with TestClient(app) as client:
        yield client
    shutdown_observability()


def _public_event(event_id: uuid.UUID, workspace_id: uuid.UUID) -> PublicWorkflowEvent:
    return PublicWorkflowEvent(
        id=event_id,
        workspace_id=workspace_id,
        task_id=None,
        event_type="task_started",
        actor_type="owner",
        actor_id="actor-1",
        subject_type=None,
        subject_id=None,
        context={"outcome": "approved"},
        created_at=_OBSERVED,
    )


def _project_seam(
    monkeypatch: pytest.MonkeyPatch,
    *,
    drained: threading.Event | None = None,
    wf_event: PublicWorkflowEvent | None = None,
    read_error: Exception | None = None,
    release: threading.Event | None = None,
) -> list[Any]:
    """Script the focused G2c resolution and record the real batch projection.

    The real ``project_stream_batch`` runs (proving the wiring); only the
    focused authorized read it composes is scripted. ``drained`` fires after
    the first projected batch so a driver can terminate the stream
    deterministically only after the staged messages were emitted;
    ``release``, when supplied, blocks the projection's return until the
    driver sets it — the deterministic way to land invalidation between the
    drain and the frame emission.
    """
    calls: list[Any] = []

    def _read(pool: Any, *, profile_id: Any, workspace_id: Any, workflow_event_id: Any) -> Any:
        calls.append(("read", workflow_event_id))
        if read_error is not None:
            raise read_error
        assert wf_event is not None
        return wf_event

    monkeypatch.setattr(workspace_stream, "read_workflow_event_for_owner", _read)

    def _project(pool: Any, *, profile_id: Any, workspace_id: Any, batch: Any) -> Any:
        messages = _real_project(
            pool, profile_id=profile_id, workspace_id=workspace_id, batch=batch
        )
        calls.append(("project", list(batch)))
        if drained is not None:
            drained.set()
        if release is not None:
            assert release.wait(5), "the driver never released the projection"
        return messages

    monkeypatch.setattr(stream_router.workspace_stream, "project_stream_batch", _project)
    return calls


_real_project = workspace_stream.project_stream_batch


def _revalidation_seam(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    calls: list[Any] = []

    def _revalidate(pool: Any, *, profile_id: Any, workspace_id: Any) -> None:
        calls.append((profile_id, workspace_id))

    monkeypatch.setattr(
        stream_router.workspace_stream, "revalidate_workspace_stream_access", _revalidate
    )
    return calls


def _no_historical_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stream never pages history and never calls a provider."""

    def _forbidden(*command: Any) -> Any:
        raise AssertionError("the stream must never read historical event pages")

    monkeypatch.setattr(event_reads, "list_workspace_events_for_owner", _forbidden)
    monkeypatch.setattr(event_reads, "list_task_events_for_owner", _forbidden)


def _frame_seam(monkeypatch: pytest.MonkeyPatch, *, expected: int) -> threading.Event:
    """Fire once the expected number of application frames has been framed.

    Framing precedes yielding, so a driver that waits for this gate knows the
    expected frames are already past their per-frame suppression checks and
    will be emitted; a loss dispatched after the gate cannot suppress them.
    """
    framed = threading.Event()
    count = {"n": 0}
    real_frame_for = stream_router._frame_for

    def _frame(message: Any) -> str:
        payload = real_frame_for(message)
        count["n"] += 1
        if count["n"] >= expected:
            framed.set()
        return payload

    monkeypatch.setattr(stream_router, "_frame_for", _frame)
    return framed


def _drive_loss_after(
    drained: threading.Event | None,
    listener: FakeNotificationListener,
    *,
    gates: tuple[threading.Event, ...] = (),
    script: list[Any] | None = None,
    pre_loss_delay: float = 0.0,
) -> threading.Thread:
    """Terminate the stream deterministically: scripted events, then loss."""

    def run() -> None:
        try:
            if drained is not None:
                assert drained.wait(5), "the projected batch never drained"
            for gate in gates:
                assert gate.wait(5), "the expected frames were never framed"
            if pre_loss_delay:
                time.sleep(pre_loss_delay)
            for event in script or []:
                listener.dispatch(event)
        finally:
            listener.dispatch(NotificationDeliveryLost(generation=listener.generation_value))

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def _parse_frames(body: str) -> list[tuple[str, str]]:
    frames: list[tuple[str, str]] = []
    for block in body.split("\n\n"):
        if not block or block.lstrip().startswith(":"):
            continue
        event_name: str | None = None
        data: str | None = None
        for line in block.split("\n"):
            if line.startswith("event: "):
                event_name = line[len("event: ") :]
            elif line.startswith("data: "):
                data = line[len("data: ") :]
        assert event_name is not None and data is not None
        frames.append((event_name, data))
    return frames


# --- the authorized live stream ------------------------------------------------


def test_the_stream_delivers_both_exact_wire_kinds_after_the_readiness_comment(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    event_id = uuid.uuid4()
    wf_hint = WorkflowEventNotification(workspace_id=workspace_id, workflow_event_id=event_id)
    dc_hint = DataChangedNotification(
        workspace_id=workspace_id,
        resource_type=NotificationResourceType.TASK,
        resource_id=uuid.uuid4(),
    )
    listener = FakeNotificationListener(
        burst_on_register=[
            NotificationDelivery(generation=1, message=wf_hint),
            NotificationDelivery(generation=1, message=dc_hint),
        ]
    )
    drained = threading.Event()
    calls = _project_seam(
        monkeypatch, drained=drained, wf_event=_public_event(event_id, workspace_id)
    )
    framed = _frame_seam(monkeypatch, expected=2)
    revalidations = _revalidation_seam(monkeypatch)
    _no_historical_reads(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))

    driver = _drive_loss_after(drained, listener, gates=(framed,))
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
    driver.join(5)
    assert not driver.is_alive()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    body = response.text
    # Register-before-readiness: the readiness comment is the stream's first
    # bytes and no application frame ever precedes it.
    assert body.startswith(_READY_COMMENT)
    frames = _parse_frames(body)
    assert [name for name, _ in frames] == ["workflow_event", "data_changed"]
    # Exact wire shapes: the G2c DTO JSON and the identifier-only envelope.
    expected_wf = WorkflowEventRead(
        id=event_id,
        workspace_id=workspace_id,
        task_id=None,
        event_type="task_started",
        actor_type="owner",
        actor_id="actor-1",
        subject_type=None,
        subject_id=None,
        context={"outcome": "approved"},
        created_at=_OBSERVED,
    ).model_dump_json()
    assert frames[0][1] == expected_wf
    expected_dc = WorkspaceStreamDataChangedRead(
        workspace_id=workspace_id,
        resource_type="task",
        resource_id=dc_hint.resource_id,
    ).model_dump_json()
    assert frames[1][1] == expected_dc
    # The hinted event was resolved by its UUID through the focused read (run
    # inside the batch projection) — never paged from history, never
    # forwarded raw.
    assert calls == [("read", event_id), ("project", [wf_hint, dc_hint])]
    assert revalidations == [(owner_profile.id, workspace_id)]
    # Cleanup unregistered exactly this subscription after the stream ended.
    assert len(listener.unregistered) == 1


def test_a_hinted_event_that_no_longer_resolves_is_omitted_while_invalidations_flow(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    event_id = uuid.uuid4()
    wf_hint = WorkflowEventNotification(workspace_id=workspace_id, workflow_event_id=event_id)
    dc_hint = DataChangedNotification(
        workspace_id=workspace_id,
        resource_type=NotificationResourceType.CONNECTION,
        resource_id=uuid.uuid4(),
    )
    listener = FakeNotificationListener(
        burst_on_register=[
            NotificationDelivery(generation=1, message=wf_hint),
            NotificationDelivery(generation=1, message=dc_hint),
        ]
    )
    drained = threading.Event()
    _project_seam(
        monkeypatch,
        drained=drained,
        read_error=NotFoundError("the requested workflow event is not available in this workspace"),
    )
    framed = _frame_seam(monkeypatch, expected=1)
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))

    driver = _drive_loss_after(drained, listener, gates=(framed,))
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
    driver.join(5)
    assert not driver.is_alive()

    assert response.status_code == 200
    frames = _parse_frames(response.text)
    # The deleted/absent event is omitted fail-closed; the deletion-agnostic
    # invalidation still flows.
    assert [name for name, _ in frames] == ["data_changed"]
    assert (
        WorkspaceStreamDataChangedRead(
            workspace_id=workspace_id,
            resource_type="connection",
            resource_id=dc_hint.resource_id,
        ).model_dump_json()
        == frames[0][1]
    )


# --- access, availability, and credential bounds -------------------------------


def test_a_foreign_workspace_is_the_uniform_404_before_any_stream(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    listener = FakeNotificationListener()
    calls: list[Any] = []

    def _revalidate(pool: Any, *, profile_id: Any, workspace_id: Any) -> None:
        calls.append(workspace_id)
        raise NotFoundError("the requested workspace is not available to this Profile")

    monkeypatch.setattr(
        stream_router.workspace_stream, "revalidate_workspace_stream_access", _revalidate
    )
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")

    assert response.status_code == 404
    # The preflight ran off the event loop and nothing was ever registered.
    assert calls == [workspace_id]
    assert listener.consumers == []
    assert listener.unregistered == []


def test_an_unavailable_listener_fails_closed_as_503(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    listener = FakeNotificationListener(ready=False)
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")

    # A stream is never reported healthy without a ready listener: fail
    # closed before any registration or readiness signal.
    assert response.status_code == 503
    assert listener.consumers == []
    assert listener.unregistered == []


def test_missing_or_malformed_credentials_are_a_uniform_401(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any
) -> None:
    workspace_id = uuid.uuid4()
    listener = FakeNotificationListener()
    tokens: list[str] = []

    def _fake_get_database_pool(settings: Any) -> object:
        return object()

    def _fake_authenticate(pool: Any, verifier: Any, *, token: str) -> Any:
        tokens.append(token)
        raise AuthenticationError("authentication failed: the access token is not valid")

    monkeypatch.setattr(authentication_dependency, "get_database_pool", _fake_get_database_pool)
    monkeypatch.setattr(authentication_dependency, "authenticate", _fake_authenticate)
    monkeypatch.setattr(
        app_module,
        "_build_notification_listener",
        lambda _settings: listener,
    )
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: OperationDependencies(
            settings=settings,
            continuation_submitter_provider=LateBoundProcessSubmitter(),
            detached_turn_observer_provider=LateBoundDetachedTurnObserver(),
        ),
        raising=True,
    )
    settings = settings_factory(supabase_url="https://supabase.example.com")
    monkeypatch.setattr(app_module, "_build_notification_listener", lambda _settings: listener)
    app = create_app(settings)
    with TestClient(app) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
    shutdown_observability()

    assert response.status_code == 401
    assert tokens == []
    assert listener.consumers == []


def test_the_stream_terminates_no_later_than_credential_expiry(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    t0 = datetime(2026, 11, 26, 12, 0, 0, tzinfo=UTC)
    expiry = t0 + timedelta(seconds=60)
    clock_calls = {"count": 0}

    def _fake_utcnow() -> datetime:
        clock_calls["count"] += 1
        # The route's deadline call, the generator's setup and first cycle
        # run at t0; one enforcement cycle elapses past the patched revalidation
        # interval but still inside the credential validity; every later check
        # is past the credential expiry.
        if clock_calls["count"] <= 4:
            return t0
        if clock_calls["count"] <= 6:
            return t0 + timedelta(seconds=0.06)
        return t0 + timedelta(hours=1)

    monkeypatch.setattr(stream_router, "_utcnow", _fake_utcnow)
    # The quiet stream's bounded waits are the patched transport ticks, and
    # the revalidation cadence is patched to fire inside the credential
    # validity window.
    monkeypatch.setattr(stream_router, "_HEARTBEAT_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(workspace_stream, "STREAM_REVALIDATION_INTERVAL_SECONDS", 0.05)
    deadline_calls: list[Any] = []
    real_deadline = workspace_stream.stream_deadline

    def _deadline(expires_at: datetime, *, now: datetime, max_lifetime_seconds: float) -> datetime:
        deadline_calls.append((expires_at, max_lifetime_seconds))
        return real_deadline(expires_at, now=now, max_lifetime_seconds=max_lifetime_seconds)

    monkeypatch.setattr(stream_router.workspace_stream, "stream_deadline", _deadline)
    revalidations = _revalidation_seam(monkeypatch)
    listener = FakeNotificationListener()
    session = _session(owner_profile, expires_at=expiry)
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")

    assert response.status_code == 200
    body = response.text
    assert body.startswith(_READY_COMMENT)
    # Termination at expiry is silent: no application message is ever sent —
    # only the transport-level heartbeat comments kept the quiet stream
    # observable, and the bounded revalidation ran while the credential was
    # still valid.
    assert "event:" not in body
    assert ": heartbeat" in body
    assert deadline_calls == [(expiry, workspace_stream.STREAM_MAX_LIFETIME_SECONDS)]
    assert len(revalidations) == 1
    assert len(listener.unregistered) == 1


# --- subscription bounds and isolation -----------------------------------------


def test_a_slow_subscriber_overflow_fails_the_stream_closed_before_any_frame(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    # Shrink the bound: the complete handoff is the staging buffer plus at
    # most one pending wake, so a burst staged while the drain is blocked
    # overflows the buffer — terminally, before the stream ever starts.
    monkeypatch.setattr(workspace_stream, "STREAM_STAGING_BUFFER_DEPTH", 1)
    burst = [
        NotificationDelivery(
            generation=1,
            message=DataChangedNotification(
                workspace_id=workspace_id,
                resource_type=NotificationResourceType.TASK,
                resource_id=uuid.uuid4(),
            ),
        )
        for _ in range(50)
    ]
    listener = FakeNotificationListener(burst_on_register=burst)
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")

    # The post-registration recheck catches the terminal overflow before any
    # readiness signal: fail closed (503), no readiness comment, no frame,
    # and the fifty callbacks all completed without blocking the dispatcher
    # (the request returned at all). Only this subscription is affected.
    assert response.status_code == 503
    assert _READY_COMMENT not in response.text
    assert "event:" not in response.text
    assert len(listener.unregistered) == 1


def test_foreign_workspace_hints_never_reach_the_stream(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    foreign_hint = DataChangedNotification(
        workspace_id=uuid.uuid4(),
        resource_type=NotificationResourceType.TASK,
        resource_id=uuid.uuid4(),
    )
    listener = FakeNotificationListener(
        burst_on_register=[NotificationDelivery(generation=1, message=foreign_hint)]
    )
    # A hint staged for another Workspace is dropped at the callback: no wake,
    # no projection, no frame. The driver ends the stream after the transport
    # ticks; only the quiet-stream heartbeats are ever emitted.
    monkeypatch.setattr(stream_router, "_HEARTBEAT_INTERVAL_SECONDS", 0.05)
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))

    driver = _drive_loss_after(None, listener, pre_loss_delay=0.25)
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
    driver.join(5)
    assert not driver.is_alive()

    assert response.status_code == 200
    body = response.text
    assert body.startswith(_READY_COMMENT)
    assert "event:" not in body
    assert ": heartbeat" in body
    assert len(listener.unregistered) == 1


def test_independent_connections_receive_only_their_own_workspace_messages(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any
) -> None:
    for workspace_id in (uuid.uuid4(), uuid.uuid4()):
        other_workspace = uuid.uuid4()
        event_id = uuid.uuid4()
        own_hint = WorkflowEventNotification(workspace_id=workspace_id, workflow_event_id=event_id)
        foreign_hint = WorkflowEventNotification(
            workspace_id=other_workspace, workflow_event_id=uuid.uuid4()
        )
        listener = FakeNotificationListener(
            burst_on_register=[
                NotificationDelivery(generation=1, message=own_hint),
                NotificationDelivery(generation=1, message=foreign_hint),
            ]
        )
        drained = threading.Event()
        _project_seam(monkeypatch, drained=drained, wf_event=_public_event(event_id, workspace_id))
        framed = _frame_seam(monkeypatch, expected=1)
        _revalidation_seam(monkeypatch)
        profile = Profile(id=uuid.uuid4(), created_at=_OBSERVED)
        session = _session(profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))

        driver = _drive_loss_after(drained, listener, gates=(framed,))
        with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
            response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
        driver.join(5)
        assert not driver.is_alive()

        assert response.status_code == 200
        frames = _parse_frames(response.text)
        # One Workspace subscription covers all of its Tasks/Repositories and
        # never fans out to or from another Workspace — even for other tabs.
        assert [name for name, _ in frames] == ["workflow_event"]
        assert str(event_id) in frames[0][1]
        assert str(other_workspace) not in frames[0][1]


# --- remediated enforcement bounds (PR review) ---------------------------------


def test_a_subscription_invalidated_before_the_stream_starts_fails_closed(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    # The loss lands between the registration and the route's post-registration
    # recheck: the readiness comment is never emitted for a dead stream.
    listener = FakeNotificationListener(burst_on_register=[NotificationDeliveryLost(generation=1)])
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")

    assert response.status_code == 503
    assert _READY_COMMENT not in response.text
    assert "event:" not in response.text
    # The raced consumer is cleaned up: exactly one unregistration, no
    # healthy stream ever reported.
    assert len(listener.unregistered) == 1


def test_no_application_frame_is_emitted_after_invalidation_is_known(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    # Three messages are drained (and projected) BEFORE the loss lands: the
    # per-frame recheck must still suppress every frame of the already-drained
    # batch once the invalidation is known.
    burst = [
        NotificationDelivery(
            generation=1,
            message=DataChangedNotification(
                workspace_id=workspace_id,
                resource_type=NotificationResourceType.TASK,
                resource_id=uuid.uuid4(),
            ),
        )
        for _ in range(3)
    ]
    listener = FakeNotificationListener(burst_on_register=burst)
    drained = threading.Event()
    loss_dispatched = threading.Event()
    _project_seam(monkeypatch, drained=drained, release=loss_dispatched)
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))

    def run() -> None:
        try:
            assert drained.wait(5), "the staged batch never drained"
            listener.dispatch(NotificationDeliveryLost(generation=listener.generation_value))
        finally:
            loss_dispatched.set()

    driver = threading.Thread(target=run, daemon=True)
    driver.start()
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
    driver.join(5)
    assert not driver.is_alive()

    assert response.status_code == 200
    # The readiness comment was truthfully emitted (the stream was live and
    # the batch drained before the loss), but no application frame of the
    # already-drained batch flows past the known invalidation.
    assert response.text == _READY_COMMENT
    assert len(listener.unregistered) == 1


def test_sustained_delivery_cannot_bypass_credential_expiry(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    t0 = datetime(2026, 11, 26, 12, 0, 0, tzinfo=UTC)
    expiry = t0 + timedelta(seconds=60)
    clock_calls = {"count": 0}

    def _fake_utcnow() -> datetime:
        clock_calls["count"] += 1
        # The route's deadline call, the generator's setup and first cycle
        # run at t0; the first emitted frame's recheck still runs at t0; every
        # later per-frame recheck is past the credential expiry.
        if clock_calls["count"] <= 5:
            return t0
        return t0 + timedelta(hours=1)

    monkeypatch.setattr(stream_router, "_utcnow", _fake_utcnow)
    burst = [
        NotificationDelivery(
            generation=1,
            message=DataChangedNotification(
                workspace_id=workspace_id,
                resource_type=NotificationResourceType.TASK,
                resource_id=uuid.uuid4(),
            ),
        )
        for _ in range(6)
    ]
    listener = FakeNotificationListener(burst_on_register=burst)
    _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=expiry)
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")

    assert response.status_code == 200
    body = response.text
    assert body.startswith(_READY_COMMENT)
    frames = _parse_frames(body)
    # Sustained (already-staged) delivery cannot carry the drained batch past
    # credential expiry: exactly the frame whose recheck preceded the expiry
    # was emitted; every later frame was suppressed and the stream ended.
    assert [name for name, _ in frames] == ["data_changed"]
    assert len(listener.unregistered) == 1


def test_sustained_delivery_cannot_bypass_authorization_revalidation(
    monkeypatch: pytest.MonkeyPatch, settings_factory: Any, owner_profile: Profile
) -> None:
    workspace_id = uuid.uuid4()
    # Sustained delivery keeps the staging buffer non-empty across the whole
    # revalidation interval: the bounded drain cap must still hand control to
    # the enforcement checkpoint, where the re-proof runs.
    monkeypatch.setattr(workspace_stream, "STREAM_REVALIDATION_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(stream_router, "_HEARTBEAT_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(workspace_stream, "STREAM_STAGING_BUFFER_DEPTH", 256)
    listener = FakeNotificationListener()
    revalidations = _revalidation_seam(monkeypatch)
    session = _session(owner_profile, expires_at=datetime(2099, 1, 1, tzinfo=UTC))

    def run() -> None:
        try:
            # Sustained delivery spanning several revalidation intervals, each
            # message waking the drain — the buffer never empties for long and
            # never overflows (bounded depth 256).
            for _ in range(40):
                listener.dispatch(
                    NotificationDelivery(
                        generation=1,
                        message=DataChangedNotification(
                            workspace_id=workspace_id,
                            resource_type=NotificationResourceType.TASK,
                            resource_id=uuid.uuid4(),
                        ),
                    )
                )
                time.sleep(0.005)
        finally:
            listener.dispatch(NotificationDeliveryLost(generation=listener.generation_value))

    driver = threading.Thread(target=run, daemon=True)
    driver.start()
    with _stream_client(monkeypatch, settings_factory(), session, listener) as client:
        response = client.get(f"/api/v1/workspaces/{workspace_id}/stream")
    driver.join(5)
    assert not driver.is_alive()

    assert response.status_code == 200
    body = response.text
    assert body.startswith(_READY_COMMENT)
    # The re-proof ran while notifications kept flowing — the enforcement
    # checkpoint is reached under sustained delivery, not only on quiet
    # streams — and the messages delivered throughout.
    assert len(revalidations) >= 1
    assert len(_parse_frames(body)) >= 1
    assert len(listener.unregistered) == 1
