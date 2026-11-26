"""Internal PostgreSQL notification transport for OpenOrc (issue #397).

The focused boundary for the shared G4 notification infrastructure: the
fixed identifier-only channel and envelope contract, and the process-local
dedicated LISTEN connection with its bounded coalesced delivery interface.
G4b consumes the SQL channel/payload contract; G4c consumes the typed
messages and readiness/loss signals. Neither authorization, browser
transport, nor workflow authority lives here.
"""

from __future__ import annotations

from openorc.notifications.contract import (
    DATA_CHANGED_ENVELOPE_KEYS,
    KIND_DATA_CHANGED,
    KIND_WORKFLOW_EVENT,
    NOTIFICATION_CHANNEL,
    WORKFLOW_EVENT_ENVELOPE_KEYS,
    DataChangedNotification,
    NotificationContractError,
    NotificationMessage,
    NotificationResourceType,
    WorkflowEventNotification,
    decode_notification_payload,
    encode_notification_payload,
)
from openorc.notifications.listener import (
    BOUNDED_DELIVERY_QUEUE_DEPTH,
    NOTIFY_WAIT_TICK_SECONDS,
    NotificationConsumer,
    NotificationConsumerEvent,
    NotificationConsumerRegistration,
    NotificationDelivery,
    NotificationDeliveryLost,
    NotificationDeliveryReady,
    PostgresNotificationListener,
    build_db_notification_listener,
    resolve_db_listener_conninfo,
)

__all__ = [
    "BOUNDED_DELIVERY_QUEUE_DEPTH",
    "DATA_CHANGED_ENVELOPE_KEYS",
    "KIND_DATA_CHANGED",
    "KIND_WORKFLOW_EVENT",
    "NOTIFICATION_CHANNEL",
    "NOTIFY_WAIT_TICK_SECONDS",
    "WORKFLOW_EVENT_ENVELOPE_KEYS",
    "DataChangedNotification",
    "NotificationConsumer",
    "NotificationConsumerEvent",
    "NotificationConsumerRegistration",
    "NotificationContractError",
    "NotificationDelivery",
    "NotificationDeliveryLost",
    "NotificationDeliveryReady",
    "NotificationMessage",
    "NotificationResourceType",
    "PostgresNotificationListener",
    "WorkflowEventNotification",
    "build_db_notification_listener",
    "decode_notification_payload",
    "encode_notification_payload",
    "resolve_db_listener_conninfo",
]
