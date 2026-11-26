"""Integration test for the G4c Workspace stream over real PostgreSQL (issue #399).

The representative end-to-end chain the leaf requires: real G4b trigger
publication → the real G4a listener → the G4c subscription's bounded handoff
→ the public stream projection over real durable rows. Two independent
subscriptions prove Workspace-scoped delivery across independent connections
(no cross-Workspace fan-out); a non-audit configuration change proves the
``data_changed`` kind, a committed audit fact proves the ``workflow_event``
kind resolved through the focused authorized read, and a contract-shaped hint
for an unresolvable event proves the omission rule.

Run explicitly when a target has been made available (see tests/README.md):

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_workspace_stream_live_delivery.py

The suite consumes the database it is given and never provisions one; it
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent. Unlike the
rollback-isolated shared fixtures, this suite commits (notifications are
visible only after the outer commit), so every test seeds its own freshly
generated identities and the ephemeral branch database is reset once per
session by the shared conftest bootstrap. The testdb flow supplies the branch
database's session-pooler connection (port 5432), which is session-capable —
exactly the endpoint class the real listener requires. The HTTP framing of
the two wire message kinds is proven deterministically by the ordinary API
suite; this test proves the real delivery chain behind it.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from psycopg import Connection, connect

from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.notifications.contract import (
    WorkflowEventNotification,
    encode_notification_payload,
)
from openorc.notifications.listener import (
    NotificationConsumerRegistration,
    PostgresNotificationListener,
)
from openorc.persistence import events as event_repositories
from openorc.persistence import ownership as ownership_repositories
from openorc.services.workspace_stream import (
    WorkspaceStreamDataChanged,
    WorkspaceStreamSubscription,
    WorkspaceStreamWorkflowEvent,
    project_stream_batch,
)
from tests.integration.persistence_fixtures import (
    _insert_profile,
    _insert_workspace,
    _SingleConnectionPool,
)

pytestmark = pytest.mark.integration

_QUIESCE_SECONDS = 0.75


@pytest.fixture
def writer(migrated_database: str) -> Iterator[Connection[Any]]:
    """One autocommit writer connection: every mutation commits on statement."""
    connection = connect(migrated_database, autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def pool(writer: Connection[Any]) -> _SingleConnectionPool:
    return _SingleConnectionPool(writer)


@pytest.fixture
def listener(migrated_database: str) -> Iterator[PostgresNotificationListener]:
    """The real listener over the branch database's session-capable endpoint."""
    listener = PostgresNotificationListener(migrated_database)
    listener.start()
    assert listener.ready, "the real listener never became ready"
    yield listener
    listener.stop()


def _subscribe(
    listener: PostgresNotificationListener, workspace_id: uuid.UUID
) -> tuple[WorkspaceStreamSubscription, NotificationConsumerRegistration]:
    subscription = WorkspaceStreamSubscription(
        workspace_id=workspace_id,
        schedule_wake=lambda: None,
        current_generation=lambda: listener.generation,
    )
    return subscription, listener.register_consumer(subscription.on_listener_event)


def _await_batch(
    subscription: WorkspaceStreamSubscription, *, timeout: float = 10.0
) -> tuple[Any, list[Any]]:
    """Poll the subscription until a delivery or terminal fact lands."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        terminal, batch = subscription.take_batch()
        if terminal is not None or batch:
            return terminal, batch
        time.sleep(0.02)
    return None, []


def _quiesce(subscription: WorkspaceStreamSubscription) -> list[Any]:
    """Prove a subscription stays quiet: nothing deliverable within the window."""
    _, batch = _await_batch(subscription, timeout=_QUIESCE_SECONDS)
    return batch


def test_real_publications_deliver_through_independent_workspace_subscriptions(
    writer: Connection[Any], pool: _SingleConnectionPool, listener: PostgresNotificationListener
) -> None:
    profile_a = _insert_profile(writer)
    workspace_a = _insert_workspace(writer, profile_a)
    profile_b = _insert_profile(writer)
    workspace_b = _insert_workspace(writer, profile_b)
    subscription_a, registration_a = _subscribe(listener, workspace_a)
    subscription_b, registration_b = _subscribe(listener, workspace_b)
    try:
        # 1. A non-audit configuration change publishes the workspace hint;
        #    only the subscribed Workspace's stream stages it.
        ownership_repositories.update_workspace_guidance(
            pool, workspace_a, guidance="stream guidance"
        )
        terminal_a, batch_a = _await_batch(subscription_a)
        assert terminal_a is None
        messages_a = project_stream_batch(
            pool, profile_id=profile_a, workspace_id=workspace_a, batch=batch_a
        )
        assert any(
            isinstance(message, WorkspaceStreamDataChanged)
            and message.resource_type == "workspace"
            and message.resource_id == workspace_a
            for message in messages_a
        )
        # The other Workspace's subscription is quiet: foreign hints are
        # dropped at the consumer callback — no cross-Workspace fan-out.
        assert _quiesce(subscription_b) == []

        # 2. A committed audit fact publishes the workflow_event hint; the
        #    subscription resolves it through the focused authorized read and
        #    projects the real durable record.
        event = event_repositories.record_workflow_event(
            pool,
            workspace_id=workspace_a,
            event_type=WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
            actor_type=WorkflowEventActor.OWNER,
            actor_id=str(profile_a),
            context={"setting": "guidance", "new": "stream guidance"},
        )
        terminal_a, batch_a = _await_batch(subscription_a)
        assert terminal_a is None
        messages_a = project_stream_batch(
            pool, profile_id=profile_a, workspace_id=workspace_a, batch=batch_a
        )
        events = [
            message for message in messages_a if isinstance(message, WorkspaceStreamWorkflowEvent)
        ]
        assert len(events) == 1
        assert events[0].event.id == event.id
        assert events[0].event.workspace_id == workspace_a
        # The real G2c safe-context projection ran over the durable row.
        assert events[0].event.context == {"setting": "guidance", "new": "stream guidance"}
        assert _quiesce(subscription_b) == []

        # 3. A contract-shaped hint for an event that does not exist is
        #    staged (in scope) but omitted from the public projection — never
        #    forwarded raw, never an error.
        absent_hint = encode_notification_payload(
            WorkflowEventNotification(workspace_id=workspace_a, workflow_event_id=uuid.uuid4())
        )
        writer.execute("select pg_notify('openorc_notifications', %s)", (absent_hint,))
        terminal_a, batch_a = _await_batch(subscription_a)
        assert terminal_a is None
        assert batch_a, "the real listener delivered the well-formed hint"
        messages_a = project_stream_batch(
            pool, profile_id=profile_a, workspace_id=workspace_a, batch=batch_a
        )
        assert messages_a == []
    finally:
        listener.unregister_consumer(registration_a)
        listener.unregister_consumer(registration_b)
