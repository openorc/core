"""Deterministic tests for the authenticated GitHub webhook intake service (issue #61).

Fake-seam coverage of the intake flow: signature verification ahead of any
payload-derived effect, the typed error vocabulary, first-delivery
acceptance with resolved routing, idempotent duplicate acknowledgement,
fail-closed routing observations, no authoritative reconciliation invocation
(spy), the raw payload never reaching persistence, and the safe telemetry
vocabulary.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.config import Settings
from openorc.domain.github_webhooks import GitHubWebhookRoutingResolution
from openorc.observability import injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import github_webhook_intake
from openorc.services.errors import (
    AuthenticationError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
)

SECRET = "test-webhook-secret"


# --- fake persistence/routing seams -------------------------------------------


class FakeDeliveryRepo:
    def __init__(self) -> None:
        self.recorded: list[Any] = []
        self.linked: list[tuple[Any, list[Any]]] = []
        self.accepted_guids: set[str] = set()

    def record_github_webhook_delivery(self, pool: DatabasePool, intake: Any) -> Any:
        self.recorded.append(intake)
        if intake.delivery_guid in self.accepted_guids:
            return None
        self.accepted_guids.add(intake.delivery_guid)
        from openorc.domain.github_webhooks import GitHubWebhookDelivery

        return GitHubWebhookDelivery(
            id=uuid.uuid4(),
            delivery_guid=intake.delivery_guid,
            event_name=intake.event_name,
            action=intake.action,
            classification=intake.classification,
            routing_target=intake.routing_target,
            routing_resolution=intake.routing_resolution,
            github_installation_id=intake.github_installation_id,
            github_repository_id=intake.github_repository_id,
            github_issue_number=intake.github_issue_number,
            github_pull_request_number=intake.github_pull_request_number,
            received_at=datetime.now(UTC),
        )

    def record_webhook_delivery_routes(
        self, pool: DatabasePool, *, delivery_id: Any, routes: list[Any]
    ) -> None:
        self.linked.append((delivery_id, list(routes)))


class FakeRoutingRepo:
    def __init__(self, resolution: Any) -> None:
        self.resolution = resolution
        self.calls: list[tuple[int, int]] = []

    def resolve_github_webhook_routes(
        self, pool: DatabasePool, *, github_installation_id: int, github_repository_id: int
    ) -> Any:
        self.calls.append((github_installation_id, github_repository_id))
        return self.resolution


# --- harness -------------------------------------------------------------------


class RecordingReconciler:
    """Spy over every authoritative-reconciliation entrypoint the module could call."""

    def __init__(self) -> None:
        self.calls: list[str] = []


def _settings() -> Settings:
    return Settings(
        environment="test",
        api_host="127.0.0.1",
        api_port=3999,
        api_reload=False,
        valkey_url="redis://127.0.0.1:6379/0",
        database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        db_pool_min=1,
        db_pool_max=5,
        db_pool_timeout=5.0,
        github_webhook_secret=SECRET,
    )


def _issue_payload_bytes() -> bytes:
    return json.dumps(
        {
            "action": "edited",
            "installation": {"id": 12345678},
            "repository": {"id": 987654321},
            "issue": {"number": 42, "title": "TITLE", "body": "BODY"},
        }
    ).encode("utf-8")


def _signature(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


class _FakeConnection:
    @contextmanager
    def transaction(self) -> Any:
        yield


class FakePool:
    """A pool seam whose transaction scope composes the scripted repositories."""

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield _FakeConnection()

        return managed()

    def close(self) -> None:
        raise AssertionError("intake service tests never close pools")


def _pool() -> DatabasePool:
    return cast(DatabasePool, FakePool())


class Harness:
    _VALID_SIGNATURE = object()

    def __init__(self, *, preaccepted: bool = False) -> None:
        self.deliveries = FakeDeliveryRepo()
        if preaccepted:
            self.deliveries.accepted_guids.add("guid-1")
        from openorc.domain.github_webhooks import (
            GitHubWebhookRouteResolution,
            GitHubWebhookRoutingResolution,
        )

        self.routing = FakeRoutingRepo(
            GitHubWebhookRouteResolution(
                resolution=GitHubWebhookRoutingResolution.UNCONFIGURED_REPOSITORY, routes=()
            )
        )

    def intake(
        self,
        *,
        body: bytes | None = None,
        signature: Any = _VALID_SIGNATURE,
        event: str = "issues",
        guid: str = "guid-1",
        settings: Settings | None = None,
    ) -> Any:
        raw = body if body is not None else _issue_payload_bytes()
        header = _signature(raw) if signature is Harness._VALID_SIGNATURE else signature
        return github_webhook_intake.intake_github_webhook(
            cast(DatabasePool, FakePool()),
            settings if settings is not None else _settings(),
            raw_body=raw,
            signature_header=header,
            event_name=event,
            delivery_guid=guid,
        )


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    def _patch(resolution: Any | None = None, *, preaccepted: bool = False) -> Harness:
        harness = Harness(preaccepted=preaccepted)
        if resolution is not None:
            harness.routing = FakeRoutingRepo(resolution)
        monkeypatch.setattr(
            github_webhook_intake,
            "record_github_webhook_delivery",
            harness.deliveries.record_github_webhook_delivery,
        )
        monkeypatch.setattr(
            github_webhook_intake,
            "record_webhook_delivery_routes",
            harness.deliveries.record_webhook_delivery_routes,
        )
        monkeypatch.setattr(
            github_webhook_intake,
            "resolve_github_webhook_routes",
            harness.routing.resolve_github_webhook_routes,
        )
        return harness

    return _patch


# --- the required behavioral matrix --------------------------------------------


def test_first_delivery_is_accepted_with_resolved_routing(patched) -> None:
    from openorc.domain.github_webhooks import (
        GitHubWebhookResolvedRoute,
        GitHubWebhookRouteResolution,
    )

    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    harness = patched(
        resolution=GitHubWebhookRouteResolution(
            resolution=GitHubWebhookRoutingResolution.RESOLVED,
            routes=(
                GitHubWebhookResolvedRoute(workspace_id=workspace_id, repository_id=repository_id),
            ),
        )
    )

    result = harness.intake()

    assert result.delivery is not None
    assert not result.duplicate
    recorded = harness.deliveries.recorded[0]
    assert recorded.classification.value == "relevant"
    assert recorded.routing_resolution is GitHubWebhookRoutingResolution.RESOLVED
    # The resolved Workspace routing facts are durably linked.
    assert len(harness.deliveries.linked) == 1
    linked_delivery_id, routes = harness.deliveries.linked[0]
    assert linked_delivery_id == result.delivery.id
    assert routes == [
        GitHubWebhookResolvedRoute(workspace_id=workspace_id, repository_id=repository_id)
    ]
    # Resolution ran against the payload's stable provider identity.
    assert harness.routing.calls == [(12345678, 987654321)]


def test_the_raw_payload_is_never_persisted(patched) -> None:
    harness = patched()

    harness.intake()

    for intake in harness.deliveries.recorded:
        assert "TITLE" not in repr(intake)
        assert "BODY" not in repr(intake)
        fields = github_webhook_intake.GitHubWebhookIntake.__dataclass_fields__
        assert "payload" not in fields
        assert "raw_body" not in fields


def test_duplicate_guid_is_idempotently_acknowledged(patched) -> None:
    harness = patched(preaccepted=True)

    result = harness.intake()

    assert result.delivery is None
    assert result.duplicate
    # No routing linkage is written for the duplicate.
    assert harness.deliveries.linked == []


def test_ignored_events_are_acknowledged_without_routing_resolution(patched) -> None:
    harness = patched()

    result = harness.intake(event="ping", guid="guid-ignored")

    assert result.delivery is not None
    assert result.ignored
    assert harness.routing.calls == []
    assert harness.deliveries.linked == []
    recorded = harness.deliveries.recorded[0]
    assert recorded.routing_target is None
    assert recorded.routing_resolution is None


def test_unusable_payloads_are_safely_classified(patched) -> None:
    harness = patched()

    result = harness.intake(body=b"{not json", guid="guid-unusable")

    assert result.delivery is not None
    assert result.unusable
    assert harness.routing.calls == []


@pytest.mark.parametrize(
    ("signature", "expected_error"),
    [
        (None, AuthenticationError),
        ("sha256=" + "a" * 64, AuthenticationError),
        ("not-a-signature", AuthenticationError),
    ],
)
def test_signature_failures_are_typed_and_precede_all_effects(
    patched, signature: str | None, expected_error: type[Exception]
) -> None:
    harness = patched()

    with pytest.raises(expected_error):
        harness.intake(signature=signature)

    # No payload-derived application effect occurred.
    assert harness.deliveries.recorded == []
    assert harness.routing.calls == []


def test_a_body_byte_change_invalidates_the_signature(patched) -> None:
    harness = patched()
    body = _issue_payload_bytes()
    signature = _signature(body)
    tampered = body.replace(b"42", b"43")

    with pytest.raises(AuthenticationError):
        harness.intake(body=tampered, signature=signature)

    assert harness.deliveries.recorded == []


def test_an_unconfigured_webhook_secret_fails_closed(patched) -> None:
    harness = patched()
    settings = _settings()
    object.__setattr__(settings, "github_webhook_secret", None)

    with pytest.raises(IntegrationNotConfiguredError):
        harness.intake(settings=settings)

    assert harness.deliveries.recorded == []


@pytest.mark.parametrize("guid", ["", "   "])
def test_missing_or_blank_delivery_guid_is_an_invalid_command(patched, guid: str) -> None:
    harness = patched()

    with pytest.raises(InvalidCommandError):
        harness.intake(guid=guid)

    assert harness.deliveries.recorded == []


def test_unmapped_and_mismatched_routes_fail_closed_without_linkages(patched) -> None:
    from openorc.domain.github_webhooks import (
        GitHubWebhookRouteResolution,
    )

    for resolution in (
        GitHubWebhookRouteResolution(
            resolution=GitHubWebhookRoutingResolution.UNMAPPED_INSTALLATION
        ),
        GitHubWebhookRouteResolution(resolution=GitHubWebhookRoutingResolution.ROUTE_MISMATCH),
        GitHubWebhookRouteResolution(
            resolution=GitHubWebhookRoutingResolution.UNCONFIGURED_REPOSITORY
        ),
    ):
        harness = patched(resolution=resolution)

        result = harness.intake(guid=f"guid-{resolution.resolution.value}")

        assert result.delivery is not None
        # The bounded observation is persisted; no routing linkage exists and
        # no Workspace/Repository authority was created.
        recorded = harness.deliveries.recorded[-1]
        assert recorded.routing_resolution is resolution.resolution
        assert harness.deliveries.linked == []


def test_multi_workspace_legitimate_mapping_fans_out(patched) -> None:
    from openorc.domain.github_webhooks import (
        GitHubWebhookResolvedRoute,
        GitHubWebhookRouteResolution,
    )

    workspace_a = uuid.uuid4()
    workspace_b = uuid.uuid4()
    repository_a = uuid.uuid4()
    repository_b = uuid.uuid4()
    harness = patched(
        resolution=GitHubWebhookRouteResolution(
            resolution=GitHubWebhookRoutingResolution.RESOLVED,
            routes=(
                GitHubWebhookResolvedRoute(workspace_id=workspace_a, repository_id=repository_a),
                GitHubWebhookResolvedRoute(workspace_id=workspace_b, repository_id=repository_b),
            ),
        )
    )

    harness.intake(guid="guid-fanout")

    _, routes = harness.deliveries.linked[0]
    assert {route.workspace_id for route in routes} == {workspace_a, workspace_b}


def test_intake_never_invokes_authoritative_reconciliation(patched, monkeypatch) -> None:
    """Spy assertion: no B3/B4/B7 reconciliation entrypoint is reachable from intake."""
    harness = patched()

    # Poison every reconciliation module entrypoint with a loud failure: if
    # intake called any of them, the test would error rather than pass.
    import openorc.services.github_issue_relations as relations
    import openorc.services.github_reconciliation as reconciliation

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("intake must never invoke authoritative reconciliation")

    for module in (relations, reconciliation):
        for name in dir(module):
            if name.startswith("reconcile") or name.startswith("synchronize"):
                monkeypatch.setattr(module, name, _boom)

    harness.intake()


def test_intake_telemetry_carries_only_the_safe_vocabulary(patched) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with injected_tracer_source(lambda _scope: provider.get_tracer("test")):
        harness = patched()
        harness.intake(guid="guid-tele")

    provider.shutdown()
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attributes = dict(spans[0].attributes or {})
    assert attributes["openorc.operation"] == "github_webhook_intake.intake_github_webhook"
    # Payload, signature, secret, and customer content never become attributes.
    flat = " ".join(str(value) for value in attributes.values())
    for forbidden in (SECRET, "sha256=", "TITLE", "BODY"):
        assert forbidden not in flat


def test_intake_result_never_carries_the_secret_or_signature(patched) -> None:
    harness = patched()

    result = harness.intake(guid="guid-repr")

    representation = repr(result)
    assert SECRET not in representation
    assert "sha256=" not in representation
    assert "TITLE" not in representation
