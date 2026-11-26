"""Deterministic tests for the fixed notification payload contract (issue #397).

The channel and the two typed identifier-only envelopes are a fixed
documented contract shared by G4b SQL publication and G4c consumption. These
tests prove canonical encoding, strict fail-closed decoding, and that
rejected shapes never leak payload-derived values into diagnostics.
"""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from openorc.notifications.contract import (
    NOTIFICATION_CHANNEL,
    DataChangedNotification,
    NotificationContractError,
    NotificationResourceType,
    WorkflowEventNotification,
    decode_notification_payload,
    encode_notification_payload,
)


def test_channel_is_the_one_fixed_constant() -> None:
    assert NOTIFICATION_CHANNEL == "openorc_notifications"


def test_encodes_workflow_event_envelope_canonically() -> None:
    workspace_id = UUID("00000000-0000-0000-0000-000000000001")
    workflow_event_id = UUID("00000000-0000-0000-0000-000000000002")

    payload = encode_notification_payload(
        WorkflowEventNotification(workspace_id=workspace_id, workflow_event_id=workflow_event_id)
    )

    assert payload == (
        '{"kind":"workflow_event",'
        '"workspace_id":"00000000-0000-0000-0000-000000000001",'
        '"workflow_event_id":"00000000-0000-0000-0000-000000000002"}'
    )


def test_encodes_data_changed_envelope_canonically() -> None:
    payload = encode_notification_payload(
        DataChangedNotification(
            workspace_id=UUID("00000000-0000-0000-0000-000000000003"),
            resource_type=NotificationResourceType.TASK,
            resource_id=UUID("00000000-0000-0000-0000-000000000004"),
        )
    )

    assert payload == (
        '{"kind":"data_changed",'
        '"workspace_id":"00000000-0000-0000-0000-000000000003",'
        '"resource_type":"task",'
        '"resource_id":"00000000-0000-0000-0000-000000000004"}'
    )


def test_decode_round_trips_both_message_kinds() -> None:
    workflow_event = WorkflowEventNotification(workspace_id=uuid4(), workflow_event_id=uuid4())
    data_changed = DataChangedNotification(
        workspace_id=uuid4(),
        resource_type=NotificationResourceType.ROLE_BINDING,
        resource_id=uuid4(),
    )

    decoded_workflow_event = decode_notification_payload(
        encode_notification_payload(workflow_event)
    )
    decoded_data_changed = decode_notification_payload(encode_notification_payload(data_changed))

    assert decoded_workflow_event == workflow_event
    assert decoded_data_changed == data_changed


def test_decode_is_insensitive_to_key_order_and_whitespace() -> None:
    envelope = {
        "workflow_event_id": "00000000-0000-0000-0000-000000000002",
        "workspace_id": "00000000-0000-0000-0000-000000000001",
        "kind": "workflow_event",
    }

    decoded = decode_notification_payload(json.dumps(envelope, indent=2))

    assert decoded == WorkflowEventNotification(
        workspace_id=UUID("00000000-0000-0000-0000-000000000001"),
        workflow_event_id=UUID("00000000-0000-0000-0000-000000000002"),
    )


def test_resource_vocabulary_is_closed() -> None:
    assert {member.value for member in NotificationResourceType} == {
        "workspace",
        "project",
        "repository",
        "issue",
        "task",
        "connection",
        "role_binding",
        "refresh",
    }


def test_workspace_resource_identity_round_trips() -> None:
    workspace_id = uuid4()

    decoded = decode_notification_payload(
        encode_notification_payload(
            DataChangedNotification(
                workspace_id=workspace_id,
                resource_type=NotificationResourceType.WORKSPACE,
                resource_id=workspace_id,
            )
        )
    )

    assert decoded == DataChangedNotification(
        workspace_id=workspace_id,
        resource_type=NotificationResourceType.WORKSPACE,
        resource_id=workspace_id,
    )


@pytest.mark.parametrize(
    ("payload", "expected_fragment"),
    [
        ("not json at all", "not valid JSON"),
        ('["a", "b"]', "not a JSON object"),
        ('"just a string"', "not a JSON object"),
        ("42", "not a JSON object"),
        ("{}", "missing or unknown kind"),
        ('{"kind":"something_else"}', "missing or unknown kind"),
        ('{"kind":"workflow_event"}', "keys do not match"),
        ('{"kind":"data_changed","workspace_id":"x"}', "keys do not match"),
        (
            '{"kind":"workflow_event","workspace_id":"'
            '00000000-0000-0000-0000-000000000001",'
            '"workflow_event_id":"00000000-0000-0000-0000-000000000002",'
            '"extra":"value"}',
            "keys do not match",
        ),
        (
            '{"kind":"workflow_event","workspace_id":42,'
            '"workflow_event_id":"00000000-0000-0000-0000-000000000002"}',
            "not a string",
        ),
        (
            '{"kind":"workflow_event","workspace_id":"not-a-uuid",'
            '"workflow_event_id":"00000000-0000-0000-0000-000000000002"}',
            "not a UUID string",
        ),
        (
            '{"kind":"workflow_event","workspace_id":'
            '"00000000-0000-0000-0000-000000000001",'
            '"workflow_event_id":"{00000000-0000-0000-0000-000000000002}"}',
            "canonical",
        ),
        (
            '{"kind":"workflow_event","workspace_id":'
            '"00000000-0000-0000-0000-000000000001",'
            '"workflow_event_id":"00000000-0000-0000-0000-00000000002"}',
            "not a UUID string",
        ),
        (
            '{"kind":"data_changed","workspace_id":'
            '"00000000-0000-0000-0000-000000000001",'
            '"resource_type":"workflow_run",'
            '"resource_id":"00000000-0000-0000-0000-000000000002"}',
            "fixed resource vocabulary",
        ),
        (
            '{"kind":"data_changed","workspace_id":'
            '"00000000-0000-0000-0000-000000000001",'
            '"resource_type":42,'
            '"resource_id":"00000000-0000-0000-0000-000000000002"}',
            "not a string",
        ),
    ],
)
def test_malformed_shapes_fail_closed(payload: str, expected_fragment: str) -> None:
    with pytest.raises(NotificationContractError) as raised:
        decode_notification_payload(payload)

    assert expected_fragment in str(raised.value)


def test_uppercase_uuid_is_rejected_as_non_canonical() -> None:
    payload = (
        '{"kind":"workflow_event",'
        '"workspace_id":"00000000-0000-0000-0000-00000000000A",'
        '"workflow_event_id":"00000000-0000-0000-0000-000000000002"}'
    )

    with pytest.raises(NotificationContractError, match="canonical"):
        decode_notification_payload(payload)


def test_rejection_diagnostics_never_echo_payload_values() -> None:
    payload = (
        '{"kind":"workflow_event","workspace_id":"SECRET-VALUE-ALPHA",'
        '"workflow_event_id":"SECRET-VALUE-BETA","leaked":"SECRET-VALUE-GAMMA"}'
    )

    with pytest.raises(NotificationContractError) as raised:
        decode_notification_payload(payload)

    message = str(raised.value)
    for secret in ("SECRET-VALUE-ALPHA", "SECRET-VALUE-BETA", "SECRET-VALUE-GAMMA"):
        assert secret not in message
