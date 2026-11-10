"""Tests for the thin GitHub webhook transport (issues #61 and #120).

Route-level coverage of the HTTP mapping: exact raw-body fidelity through the
seam, uniform detail-free 401 signature rejection, 400 header validation, 204
acknowledgement of accepted/duplicate/ignored/unusable outcomes, and the
fail-closed 503 when the webhook secret is unconfigured. The route is proven
thin: it delegates to the (patched) intake/dispatch facade — the single
application-service boundary that owns every duplicate/relevance/processing
decision and the submission — and contains no reconciliation, dispatch-decision,
or workflow logic of its own. Accepted deliveries are submitted through the
serving process's active F1 submitter via the F1-backed submission bridge
(issue #255): only the delivery GUID travels, and 204 means durable
acceptance, never completed reconciliation.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.routers import github_webhooks as webhook_router
from openorc.config import ExecutionMode, Settings
from openorc.domain.github_webhooks import (
    GitHubWebhookDelivery,
    GitHubWebhookDeliveryClassification,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
)
from openorc.services.errors import (
    AuthenticationError,
    IntegrationNotConfiguredError,
)
from openorc.services.github_webhook_dispatch import GitHubWebhookIntakeAndDispatch
from openorc.services.github_webhook_intake import GitHubWebhookIntake
from openorc.workers import invocation
from openorc.workers.github_operations import F1WebhookDispatchSubmission
from openorc.workers.invocation import (
    OperationInvocation,
    OperationKind,
    SubmissionResult,
    WebhookDeliveryDispatchPayload,
    decode_operation_invocation,
)

SECRET = "test-webhook-secret"
BODY = (
    b'{"action": "edited", "installation": {"id": 1}, '
    b'"repository": {"id": 2}, "issue": {"number": 3}}'
)


def _signature(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def _settings(
    *, with_secret: bool = True, execution_mode: ExecutionMode = ExecutionMode.REGULAR
) -> Settings:
    return Settings(
        environment="test",
        api_host="127.0.0.1",
        api_port=3999,
        api_reload=False,
        execution_mode=execution_mode,
        valkey_url="redis://127.0.0.1:6379/0",
        database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        db_pool_min=1,
        db_pool_max=5,
        db_pool_timeout=5.0,
        github_webhook_secret=SECRET if with_secret else None,
    )


class _RecordingSubmitter:
    """The scripted F1 submitter injected at the route's app-state seam."""

    def __init__(self) -> None:
        self.submissions: list[OperationInvocation] = []

    def submit(self, submission: OperationInvocation) -> SubmissionResult:
        self.submissions.append(submission)
        return SubmissionResult.accepted()


class _FakeQueue:
    """The patched rq.Queue recording the queue name and enqueue arguments."""

    def __init__(self, name: str, connection: Any = None) -> None:
        self.name = name
        self.connection = connection
        self.enqueued: list[tuple[Any, tuple[Any, ...]]] = []

    def enqueue(self, func: Any, *args: Any) -> object:
        self.enqueued.append((func, args))
        return object()


class _FacadeSeam:
    """The patched intake/dispatch seam: recorded invocations and wiring.

    The facade and the process-local pool acquisition are patched at the
    router-module boundary, so the route is proven to obtain the pool inside
    the threadpool callable — never a real pool, never an event-loop
    acquisition — to hand the facade the F1-backed submission bridge over the
    app-state submitter, and to own no duplicate/relevance/processing or
    submission decision of its own.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.pool_acquisitions: list[Settings] = []
        self.pool: object = object()
        self.invoke_submission = False


@pytest.fixture
def facade_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        result: Any = None, error: Exception | None = None, *, invoke_submission: bool = False
    ) -> _FacadeSeam:
        seam = _FacadeSeam()
        seam.invoke_submission = invoke_submission

        def _fake_get_database_pool(settings: Settings) -> object:
            seam.pool_acquisitions.append(settings)
            return seam.pool

        def _facade(
            pool: Any,
            settings: Settings,
            submission: Any,
            *,
            raw_body: bytes,
            signature_header: str | None,
            event_name: str,
            delivery_guid: str,
        ) -> Any:
            seam.calls.append(
                {
                    "pool": pool,
                    "submission": submission,
                    "raw_body": raw_body,
                    "signature_header": signature_header,
                    "event_name": event_name,
                    "delivery_guid": delivery_guid,
                }
            )
            if seam.invoke_submission:
                # The real facade submits eligible deliveries after durable
                # intake; the bridge then submits the F1 invocation through
                # the app-state submitter.
                submission.enqueue(delivery_guid)
            if error is not None:
                raise error
            return result

        monkeypatch.setattr(webhook_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(webhook_router, "intake_and_dispatch_github_webhook", _facade)
        return seam

    return _make


def _client(settings: Settings) -> TestClient:
    app = create_app(settings)
    # The serving lifespan owns the real submitter; these transport tests run
    # without the lifespan and inject a recording F1 submitter at the same
    # app-state seam the route reads (issue #255).
    app.state.operation_submitter = _RecordingSubmitter()
    return TestClient(app, raise_server_exceptions=False)


def _headers(*, signature: str | None = _signature(BODY)) -> dict[str, str]:
    headers = {"X-GitHub-Event": "issues", "X-GitHub-Delivery": "guid-1"}
    if signature is not None:
        headers["X-Hub-Signature-256"] = signature
    return headers


def test_the_route_passes_the_exact_raw_body_and_headers_to_the_service(facade_seam) -> None:
    settings = _settings()
    seam = facade_seam()
    client = _client(settings)

    response = client.post(
        "/api/github/webhooks", content=BODY, headers=_headers(signature=_signature(BODY))
    )

    assert response.status_code == 204
    assert len(seam.calls) == 1
    # Exact raw bytes: no re-serialization or normalization.
    assert seam.calls[0]["raw_body"] == BODY
    assert seam.calls[0]["signature_header"] == _signature(BODY)
    assert seam.calls[0]["event_name"] == "issues"
    assert seam.calls[0]["delivery_guid"] == "guid-1"
    # The pool was acquired through the (patched) seam inside the threadpool
    # callable and is the pool the facade received.
    assert seam.calls[0]["pool"] is seam.pool
    assert seam.pool_acquisitions == [settings]
    # The submission seam is the F1-backed bridge over the app-state
    # submitter, built by the route's threadpool callable: the route owns no
    # submission decision.
    assert isinstance(seam.calls[0]["submission"], F1WebhookDispatchSubmission)


# --- the F1 submission seam (issue #255) ---------------------------------------


def test_an_accepted_delivery_submits_the_f1_invocation_through_the_active_submitter(
    facade_seam,
) -> None:
    seam = facade_seam(invoke_submission=True)
    app = create_app(_settings())
    submitter = _RecordingSubmitter()
    app.state.operation_submitter = submitter
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

    # 204 means durable acceptance — not completed reconciliation; the
    # submission outcome is deliberately not an HTTP contract.
    assert response.status_code == 204
    assert len(seam.calls) == 1
    # Only the delivery GUID traveled in the F1 invocation.
    assert submitter.submissions == [
        OperationInvocation(
            kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
            payload=WebhookDeliveryDispatchPayload(delivery_guid="guid-1"),
        )
    ]


def test_an_accepted_delivery_submits_the_f1_invocation_through_the_queue_seam(
    facade_seam, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(execution_mode=ExecutionMode.QUEUE)
    seam = facade_seam(invoke_submission=True)
    app = create_app(settings)
    app.state.operation_submitter = invocation.RQQueueOperationSubmission(settings)
    client = TestClient(app, raise_server_exceptions=False)
    queue = _FakeQueue("openorc:default")
    monkeypatch.setattr(invocation, "create_redis_client", lambda _settings: object())
    monkeypatch.setattr(invocation.rq, "Queue", lambda name, connection=None: queue)

    response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

    assert response.status_code == 204
    assert len(seam.calls) == 1
    # The queue-mode submitter encoded exactly one F1 wire invocation
    # carrying only the delivery GUID.
    assert len(queue.enqueued) == 1
    func, args = queue.enqueued[0]
    assert func == invocation._ENTRYPOINT_PATH
    wire_invocation = decode_operation_invocation(args[0])
    assert wire_invocation.kind is OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH
    assert wire_invocation.payload == WebhookDeliveryDispatchPayload(delivery_guid="guid-1")


def test_regular_mode_never_contacts_the_queue_backend(facade_seam, monkeypatch) -> None:
    seam = facade_seam(invoke_submission=True)
    app = create_app(_settings())
    submitter = _RecordingSubmitter()
    app.state.operation_submitter = submitter
    client = TestClient(app, raise_server_exceptions=False)
    touched: list[Settings] = []

    def _record(settings: Settings) -> object:
        touched.append(settings)
        return object()

    monkeypatch.setattr(invocation, "create_redis_client", _record)

    response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

    assert response.status_code == 204
    assert submitter.submissions != []
    # Regular mode never touches Valkey: the submission went through the
    # local F1 submitter only.
    assert touched == []
    assert seam.calls != []


@pytest.mark.parametrize("signature", [None, "sha256=" + "a" * 64, "bogus"])
def test_signature_failures_map_to_a_uniform_401(facade_seam, signature: str | None) -> None:
    facade_seam(error=AuthenticationError("rejected"))
    app = create_app(_settings())
    submitter = _RecordingSubmitter()
    app.state.operation_submitter = submitter
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post(
        "/api/github/webhooks", content=BODY, headers=_headers(signature=signature)
    )

    assert response.status_code == 401
    # Uniform, detail-free rejection: no probing detail leaves the boundary.
    assert response.content == b""
    # A rejected signature never reaches dispatch submission: no F1
    # invocation was ever built from an unverified delivery.
    assert submitter.submissions == []


def test_missing_delivery_or_event_headers_map_to_400(facade_seam) -> None:
    facade_seam()
    client = _client(_settings())
    signature = _signature(BODY)

    missing_delivery = client.post(
        "/api/github/webhooks",
        content=BODY,
        headers={"X-Hub-Signature-256": signature, "X-GitHub-Event": "issues"},
    )
    missing_event = client.post(
        "/api/github/webhooks",
        content=BODY,
        headers={"X-Hub-Signature-256": signature, "X-GitHub-Delivery": "guid-1"},
    )

    assert missing_delivery.status_code == 400
    assert missing_event.status_code == 400


def test_an_unconfigured_webhook_secret_maps_to_503(facade_seam) -> None:
    facade_seam(error=IntegrationNotConfiguredError("not configured"))
    client = _client(_settings(with_secret=False))

    response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

    assert response.status_code == 503


def test_accepted_duplicate_ignored_and_unusable_all_acknowledge_204(facade_seam) -> None:
    delivery = GitHubWebhookDelivery(
        id=uuid.uuid4(),
        delivery_guid="guid-1",
        event_name="issues",
        action="edited",
        classification=GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.ISSUE_STATE,
        routing_resolution=GitHubWebhookRoutingResolution.RESOLVED,
        github_installation_id=1,
        github_repository_id=2,
        github_issue_number=3,
        github_pull_request_number=None,
        received_at=datetime.now(UTC),
        processed_at=None,
    )
    client = _client(_settings())
    outcomes = (
        (
            "accepted",
            GitHubWebhookIntakeAndDispatch(intake=GitHubWebhookIntake(delivery=delivery)),
        ),
        (
            "duplicate",
            GitHubWebhookIntakeAndDispatch(
                intake=GitHubWebhookIntake(delivery=None, duplicate=True)
            ),
        ),
        (
            "ignored",
            GitHubWebhookIntakeAndDispatch(
                intake=GitHubWebhookIntake(delivery=delivery, ignored=True)
            ),
        ),
        (
            "unusable",
            GitHubWebhookIntakeAndDispatch(
                intake=GitHubWebhookIntake(delivery=delivery, unusable=True)
            ),
        ),
    )

    for name, outcome in outcomes:
        seam = facade_seam(result=outcome)

        response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

        assert response.status_code == 204, name
        # The route branches on no intake semantics: every outcome maps to
        # exactly one facade invocation and a detail-free acknowledgement.
        assert len(seam.calls) == 1, name


def test_the_route_registers_on_the_application(facade_seam) -> None:
    facade_seam()
    client = _client(_settings())

    response = client.post("/api/github/webhooks", content=b"")

    # The route exists (bad headers -> 400, not 404).
    assert response.status_code == 400


def test_an_oversized_body_is_rejected_without_any_intake(facade_seam) -> None:
    seam = facade_seam()
    client = _client(_settings())

    response = client.post(
        "/api/github/webhooks",
        content=b"x" * (webhook_router._MAX_WEBHOOK_BODY_BYTES + 1),
        headers=_headers(),
    )

    # The fast Content-Length precheck rejects without reading or intake.
    assert response.status_code == 413
    assert seam.calls == []
    assert seam.pool_acquisitions == []


# --- the bounded streaming body reader (issue #61 review fix) -----------------


def test_bounded_reader_preserves_exact_bytes_under_the_limit() -> None:
    async def stream():
        yield b"chunk-one-"
        yield b"chunk-two"

    assert asyncio.run(webhook_router._read_bounded_body(stream())) == b"chunk-one-chunk-two"


def test_bounded_reader_stops_reading_once_the_limit_is_crossed() -> None:
    pulled: list[int] = []

    async def unbounded_stream():
        while True:
            chunk = b"x" * (1024 * 1024)
            pulled.append(len(chunk))
            yield chunk

    result = asyncio.run(webhook_router._read_bounded_body(unbounded_stream()))

    assert result is None
    # Reading stopped once the bound was crossed (plus at most the crossing
    # chunk): the unbounded generator was abandoned, never drained, so an
    # arbitrarily large request can never be fully buffered.
    assert sum(pulled) <= webhook_router._MAX_WEBHOOK_BODY_BYTES + 1024 * 1024


@pytest.mark.parametrize(
    "declared", [str(webhook_router._MAX_WEBHOOK_BODY_BYTES + 1), "99999999999"]
)
def test_an_oversized_declared_length_fails_the_precheck(declared: str) -> None:
    assert webhook_router._declared_length_exceeds_limit(declared)


@pytest.mark.parametrize(
    "declared", [None, "", "not-a-number", "0", str(webhook_router._MAX_WEBHOOK_BODY_BYTES)]
)
def test_non_limiting_or_unparseable_lengths_are_left_to_the_streaming_bound(
    declared: str | None,
) -> None:
    assert not webhook_router._declared_length_exceeds_limit(declared)
