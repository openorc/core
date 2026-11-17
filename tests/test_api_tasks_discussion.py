"""Tests for the thin Task discussion and conversation transport (issue #339).

Route-level coverage of the HTTP mapping: the authenticated-Profile
dependency, the typed application-error mapping, and transport thinness —
the routes delegate every decision to the patched application-service
boundary, and the discussion POST maps the three submission outcomes
honestly (202 accepted-for-delivery is never delivery; a positively refused
submission releases the receipt and fails; an uncertain submission fails
with the receipt conservatively claimed).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import tasks as tasks_router
from openorc.domain.detached_turns import TaskDetachedTurnReceiptStatus
from openorc.domain.ownership import Profile
from openorc.services.errors import (
    ConflictError,
)
from openorc.workers.invocation import (
    OperationKind,
    SubmissionOutcome,
    SubmissionRefusalReason,
    SubmissionResult,
)

_OBSERVED = datetime(2026, 11, 16, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_RECEIPT_ID = uuid.uuid4()
_REVIEWER_SESSION_ID = uuid.uuid4()


def _prepared() -> Any:
    """The service's prepared result as a shape-compatible stub."""
    receipt = SimpleNamespace(
        id=_RECEIPT_ID,
        status=TaskDetachedTurnReceiptStatus.CLAIMED,
        claimed_at=_OBSERVED,
    )
    return SimpleNamespace(
        receipt=receipt,
        owner_gate=SimpleNamespace(id=_GATE_ID),
        reviewer_session=SimpleNamespace(id=_REVIEWER_SESSION_ID),
        task=SimpleNamespace(id=_TASK_ID),
    )


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


def _client(settings: Any, owner_profile: Profile, operation_submitter: Any = None) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    if operation_submitter is not None:
        app.state.operation_submitter = operation_submitter
    return TestClient(app)


@pytest.fixture
def discussion_seam(monkeypatch: pytest.MonkeyPatch):
    """Patch the router module's service boundary with recorded doubles."""

    def _make(
        *,
        prepared: Any = None,
        prepared_error: Exception | None = None,
        status_result: Any = None,
        conversation_result: Any = None,
    ) -> Any:
        calls: dict[str, list[Any]] = {
            "prepare": [],
            "release": [],
            "status": [],
            "conversation": [],
            "submits": [],
        }

        def _prepare(pool: Any, **command: Any) -> Any:
            calls["prepare"].append(command)
            if prepared_error is not None:
                raise prepared_error
            assert prepared is not None
            return prepared

        def _release(pool: Any, **command: Any) -> None:
            calls["release"].append(command)

        def _status(pool: Any, **command: Any) -> Any:
            calls["status"].append(command)
            return status_result

        def _conversation(pool: Any, **command: Any) -> Any:
            calls["conversation"].append(command)
            if isinstance(conversation_result, Exception):
                raise conversation_result
            assert conversation_result is not None
            return conversation_result

        class _Submitter:
            outcome = SubmissionOutcome.ACCEPTED_FOR_DELIVERY
            reason: Any = None

            def submit(self, invocation: Any) -> SubmissionResult:
                calls["submits"].append(invocation)
                if self.outcome is SubmissionOutcome.KNOWN_REFUSED:
                    assert self.reason is not None
                    return SubmissionResult.refused(self.reason)
                if self.outcome is SubmissionOutcome.UNCERTAIN:
                    return SubmissionResult.uncertain()
                return SubmissionResult.accepted()

        monkeypatch.setattr(tasks_router, "get_database_pool", lambda settings: object())
        monkeypatch.setattr(tasks_router, "prepare_owner_reviewer_discussion", _prepare)
        monkeypatch.setattr(tasks_router, "release_prepared_discussion_receipt", _release)
        monkeypatch.setattr(tasks_router, "get_owner_gate_discussion_status", _status)
        monkeypatch.setattr(tasks_router, "project_task_reviewer_conversation", _conversation)
        monkeypatch.setattr(
            tasks_router, "default_reviewer_neutral_directory_provider", lambda settings: "/tmp"
        )
        return calls, _Submitter()

    return _make


def _post_url() -> str:
    return (
        f"/api/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}"
        f"/owner-gates/{_GATE_ID}/discussion-message"
    )


_STATUS_URL = (
    f"/api/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/owner-gates/{_GATE_ID}/discussion-status"
)
_CONVERSATION_URL = f"/api/workspaces/{_WORKSPACE_ID}/tasks/{_TASK_ID}/reviewer-conversation"
_MESSAGE = {"message": "Owner question: is the retry backoff sufficient?"}


# --- the discussion POST: the three submission outcomes ------------------------------


def test_post_accepted_submission_returns_202_with_the_admission_ack(
    settings_factory, owner_profile, discussion_seam
) -> None:
    calls, submitter = discussion_seam(prepared=_prepared())
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_post_url(), json=_MESSAGE)

    assert response.status_code == 202
    body = response.json()
    assert body["receipt_id"] == str(_RECEIPT_ID)
    assert body["owner_gate_id"] == str(_GATE_ID)
    assert body["reviewer_session_id"] == str(_REVIEWER_SESSION_ID)
    assert body["task_id"] == str(_TASK_ID)
    assert body["status"] == "claimed"
    assert body["claimed_at"]
    # The route submits the typed discussion operation carrying the verbatim
    # message and releases nothing.
    assert len(calls["submits"]) == 1
    assert calls["submits"][0].kind is OperationKind.TASK_OWNER_REVIEWER_DISCUSSION
    assert calls["submits"][0].payload.message == _MESSAGE["message"]
    assert calls["release"] == []


def test_post_maps_a_prepare_conflict_to_409_without_submitting(
    settings_factory, owner_profile, discussion_seam
) -> None:
    calls, submitter = discussion_seam(prepared_error=ConflictError("outstanding run"))
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_post_url(), json=_MESSAGE)

    assert response.status_code == 409
    assert calls["submits"] == []
    assert calls["release"] == []


def test_post_releases_the_receipt_and_fails_on_a_known_submission_refusal(
    settings_factory, owner_profile, discussion_seam
) -> None:
    calls, submitter = discussion_seam(prepared=_prepared())
    submitter.outcome = SubmissionOutcome.KNOWN_REFUSED
    submitter.reason = SubmissionRefusalReason.EXECUTOR_SATURATED
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_post_url(), json=_MESSAGE)

    # The positively refused submission releases the prepared receipt (no
    # worker will ever run) and the response is the honest failure — never
    # 202.
    assert response.status_code == 503
    assert len(calls["release"]) == 1
    assert calls["release"][0]["receipt_id"] == _RECEIPT_ID


def test_post_keeps_the_receipt_claimed_on_an_uncertain_submission(
    settings_factory, owner_profile, discussion_seam
) -> None:
    calls, submitter = discussion_seam(prepared=_prepared())
    submitter.outcome = SubmissionOutcome.UNCERTAIN
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.post(_post_url(), json=_MESSAGE)

    # An uncertain submission is neither acceptance nor proven non-delivery:
    # the receipt stays conservatively claimed and the response fails without
    # ever claiming non-delivery.
    assert response.status_code == 503
    assert response.json()["detail"]
    assert calls["release"] == []


# --- the status projection and the conversation read ------------------------------


def test_get_status_maps_the_projection(settings_factory, owner_profile, discussion_seam) -> None:
    projected = SimpleNamespace(
        receipt_id=_RECEIPT_ID,
        status=TaskDetachedTurnReceiptStatus.SETTLED,
        claimed_at=_OBSERVED,
        admission_attempted_at=_OBSERVED,
        admitted_at=_OBSERVED,
        settled_at=_OBSERVED,
        outcome_label="discussion_exchange",
    )
    calls, submitter = discussion_seam(status_result=projected)
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.get(_STATUS_URL)

    assert response.status_code == 200
    body = response.json()
    assert body["receipt_id"] == str(_RECEIPT_ID)
    assert body["status"] == "settled"
    assert body["outcome_label"] == "discussion_exchange"
    assert body["admission_attempted_at"]
    assert calls["status"][0]["owner_gate_id"] == _GATE_ID


def test_get_status_is_404_for_a_foreign_gate(
    settings_factory, owner_profile, discussion_seam
) -> None:
    calls, submitter = discussion_seam()
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.get(_STATUS_URL)

    assert response.status_code == 404


def test_get_conversation_maps_the_transient_projection(
    settings_factory, owner_profile, discussion_seam
) -> None:
    projected = SimpleNamespace(
        task_id=_TASK_ID,
        reviewer_session_id=_REVIEWER_SESSION_ID,
        external_session_id="ext-session-2",
        messages=(
            SimpleNamespace(index=0, kind="say", text="Reviewer message one."),
            SimpleNamespace(index=1, kind=None, text=None),
        ),
    )
    calls, submitter = discussion_seam(conversation_result=projected)
    client = _client(settings_factory(), owner_profile, submitter)

    response = client.get(_CONVERSATION_URL)

    assert response.status_code == 200
    body = response.json()
    assert body["task_id"] == str(_TASK_ID)
    assert body["reviewer_session_id"] == str(_REVIEWER_SESSION_ID)
    assert body["messages"] == [
        {"index": 0, "kind": "say", "text": "Reviewer message one."},
        {"index": 1, "kind": None, "text": None},
    ]
    assert calls["conversation"][0]["task_id"] == _TASK_ID
