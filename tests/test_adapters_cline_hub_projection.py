"""Adapter-level tests for the Hub-run observation projection (issue #323).

The projection's own behavior only: terminal kind mapping with verbatim
sequence and candidate extraction, run-start identity extraction, status-only
record projection, and the fail-closed handling of unattributable or
uninterpretable observations. The established protocol, bridge, and
settlement matrices are not re-proven here.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pytest

from openorc.adapters.cline import (
    ClineHubRunStartObservation,
    HubRunProjectionError,
    project_hub_run_start,
    project_hub_run_statuses,
    project_terminal_hub_run,
)
from openorc.adapters.cline.values import ClineHubEvent, JsonObject
from openorc.domain.hub_runs import HubRunObservationKind, ObservedHubRunStatus


def test_a_terminal_completed_envelope_projects_its_own_identity_and_candidate() -> None:
    event = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=9,
        payload={"reason": "finished", "result": {"text": '{"ok": true}'}},
    )

    observation = project_terminal_hub_run(event)

    assert observation is not None
    assert observation.external_session_id == "sess-1"
    assert observation.kind is HubRunObservationKind.RUN_COMPLETED
    assert observation.sequence == 9
    assert observation.result_candidate == '{"ok": true}'


def test_a_terminal_failed_envelope_projects_the_failed_kind_and_its_candidate_verbatim() -> None:
    event = ClineHubEvent(
        session_id="sess-1",
        kind="run.failed",
        sequence=11,
        payload={"reason": "error", "result": {"text": "partial"}, "snapshot": {}},
    )

    observation = project_terminal_hub_run(event)

    assert observation is not None
    assert observation.kind is HubRunObservationKind.RUN_FAILED
    assert observation.result_candidate == "partial"


def test_a_completed_run_without_a_usable_candidate_text_is_a_legitimate_observation() -> None:
    event = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=12,
        payload={"reason": "finished", "result": {}},
    )

    observation = project_terminal_hub_run(event)

    assert observation is not None
    assert observation.result_candidate is None


def test_a_terminal_envelope_without_session_identity_is_unattributable_and_never_invented() -> (
    None
):
    event = ClineHubEvent(
        session_id=None, kind="run.completed", sequence=13, payload={"result": {"text": "x"}}
    )

    assert project_terminal_hub_run(event) is None


def test_non_terminal_events_project_to_nothing() -> None:
    for kind in ("run.started", "usage.updated", "session.updated"):
        event = ClineHubEvent(session_id="sess-1", kind=kind, sequence=1, payload={})

        assert project_terminal_hub_run(event) is None


def test_a_run_start_projects_the_payload_request_identity_and_nothing_else() -> None:
    event = ClineHubEvent(
        session_id="sess-1", kind="run.started", sequence=7, payload={"requestId": "hub-run-1"}
    )

    observation = project_hub_run_start(event)

    assert observation == ClineHubRunStartObservation(
        external_session_id="sess-1", request_id="hub-run-1", sequence=7
    )


def test_a_run_start_without_a_request_identity_or_session_identity_is_unattributable() -> None:
    identity_less = ClineHubEvent(
        session_id=None, kind="run.started", sequence=7, payload={"requestId": "r"}
    )
    request_less = ClineHubEvent(
        session_id="sess-1", kind="run.started", sequence=7, payload={"snapshot": {}}
    )

    assert project_hub_run_start(identity_less) is None
    assert project_hub_run_start(request_less) == ClineHubRunStartObservation(
        external_session_id="sess-1", request_id=None, sequence=7
    )


def test_status_only_records_project_the_hub_identity_and_state_verbatim() -> None:
    records = [
        {"runId": "r1", "state": "queued", "acceptedAt": 1794712612934},
        {"runId": "r2", "state": "failed", "error": "session not found"},
    ]

    assert project_hub_run_statuses(records) == [
        ObservedHubRunStatus(run_id="r1", status="queued"),
        ObservedHubRunStatus(run_id="r2", status="failed"),
    ]


def test_an_uninterpretable_status_record_fails_closed_instead_of_being_dropped() -> None:
    with pytest.raises(HubRunProjectionError):
        project_hub_run_statuses([{"runId": "r1", "state": "failed"}, {"state": "failed"}])
    with pytest.raises(HubRunProjectionError):
        project_hub_run_statuses(cast(Sequence[JsonObject], ["not-a-record"]))
