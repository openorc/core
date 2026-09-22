"""Deterministic tests for the Supabase Auth Admin adapter boundary (issue #97).

An injected transport seam proves the outcome classification (success,
confirmed absence, definitive rejection, unknown outcome), the exact request
contract — the non-JWT secret API key travels on the ``apikey`` header only,
never as ``Authorization: Bearer`` and never in a URL — the fail-fast
construction rules, key redaction on every surface, and the representative
external-adapter span boundary. No live Supabase network access.
"""

from __future__ import annotations

import logging
import urllib.error
from collections.abc import Mapping
from uuid import UUID, uuid4

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.adapters.supabase.admin import (
    AUTH_ADMIN_USERS_PATH,
    HttpSupabaseAuthAdminClient,
    SupabaseAuthAdminOutcomeUnknownError,
    SupabaseAuthAdminRejectedError,
    SupabaseAuthAdminUserAbsentError,
)
from openorc.config import ConfigurationError
from openorc.observability import OPERATION, injected_tracer_source

_PROJECT_URL = "https://project.example.supabase.co"
_SECRET_KEY = "sb_secret_test-key-value-do-not-leak"


class FakeAdminTransport:
    """Injectable Admin transport recording calls; serves queued statuses.

    Each queued entry is either a ``(status, body)`` tuple or an exception
    instance to raise once (network-level failures the classification maps to
    unknown outcomes).
    """

    def __init__(self, results: list[tuple[int, bytes] | Exception]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str, Mapping[str, str], float]] = []

    def __call__(
        self, url: str, method: str, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, bytes]:
        self.calls.append((url, method, dict(headers), timeout))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _client(transport: FakeAdminTransport) -> HttpSupabaseAuthAdminClient:
    return HttpSupabaseAuthAdminClient(
        project_url=_PROJECT_URL, secret_key=_SECRET_KEY, fetch=transport
    )


def _admin_url(user_id: UUID) -> str:
    return _PROJECT_URL + AUTH_ADMIN_USERS_PATH + f"/{user_id}"


def test_delete_success_returns_without_error() -> None:
    transport = FakeAdminTransport([(204, b"")])
    user_id = uuid4()

    _client(transport).delete_user(user_id)

    assert len(transport.calls) == 1
    url, method, headers, timeout = transport.calls[0]
    assert url == _admin_url(user_id)
    assert method == "DELETE"
    # The current Supabase API-key contract: publishable/secret keys are not
    # JWTs — the credential travels on the apikey header only.
    assert headers["apikey"] == _SECRET_KEY
    assert "Authorization" not in headers
    assert timeout == 5.0


def test_delete_confirmed_absent_is_a_classified_end_state_not_a_failure() -> None:
    transport = FakeAdminTransport([(404, b'{"error": "not found"}')])

    with pytest.raises(SupabaseAuthAdminUserAbsentError):
        _client(transport).delete_user(uuid4())


@pytest.mark.parametrize("status", [400, 401, 403, 422])
def test_delete_definitive_rejection_is_a_known_failure(status: int) -> None:
    transport = FakeAdminTransport([(status, b'{"error": "rejected"}')])

    with pytest.raises(SupabaseAuthAdminRejectedError):
        _client(transport).delete_user(uuid4())


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_delete_server_error_leaves_the_outcome_unknown(status: int) -> None:
    # A destructive write's effect can be masked by an intermediary: a 5xx
    # answer is never reclassified as a known failure.
    transport = FakeAdminTransport([(status, b"")])

    with pytest.raises(SupabaseAuthAdminOutcomeUnknownError):
        _client(transport).delete_user(uuid4())


@pytest.mark.parametrize(
    "transport_failure",
    [
        urllib.error.URLError("connection refused"),
        TimeoutError("timed out"),
        OSError("network is down"),
    ],
)
def test_delete_transport_failure_leaves_the_outcome_unknown(
    transport_failure: Exception,
) -> None:
    transport = FakeAdminTransport([transport_failure])

    with pytest.raises(SupabaseAuthAdminOutcomeUnknownError):
        _client(transport).delete_user(uuid4())


def test_fetch_reports_presence_by_status() -> None:
    present = FakeAdminTransport([(200, b'{"id": "..."}')])
    absent = FakeAdminTransport([(404, b'{"error": "not found"}')])

    assert _client(present).fetch_user(uuid4()) is True
    assert _client(absent).fetch_user(uuid4()) is False

    assert present.calls[0][1] == "GET"
    assert absent.calls[0][1] == "GET"
    assert absent.calls[0][2]["apikey"] == _SECRET_KEY
    assert "Authorization" not in absent.calls[0][2]


@pytest.mark.parametrize("status", [400, 401, 403])
def test_fetch_definitive_rejection_is_a_known_failure(status: int) -> None:
    transport = FakeAdminTransport([(status, b'{"error": "rejected"}')])

    with pytest.raises(SupabaseAuthAdminRejectedError):
        _client(transport).fetch_user(uuid4())


@pytest.mark.parametrize(
    "result",
    [(500, b""), urllib.error.URLError("connection refused")],
)
def test_fetch_outcome_unknown_for_5xx_and_transport_failures(
    result: tuple[int, bytes] | Exception,
) -> None:
    transport = FakeAdminTransport([result])

    with pytest.raises(SupabaseAuthAdminOutcomeUnknownError):
        _client(transport).fetch_user(uuid4())


def test_trailing_slash_on_the_project_url_does_not_change_the_endpoint() -> None:
    transport = FakeAdminTransport([(204, b"")])
    user_id = uuid4()
    client = HttpSupabaseAuthAdminClient(
        project_url=_PROJECT_URL + "/", secret_key=_SECRET_KEY, fetch=transport
    )

    client.delete_user(user_id)

    assert transport.calls[0][0] == _admin_url(user_id)


def test_construction_fails_fast_without_usable_configuration() -> None:
    for bad_key in ["", "   "]:
        with pytest.raises(ConfigurationError, match="secret key"):
            HttpSupabaseAuthAdminClient(project_url=_PROJECT_URL, secret_key=bad_key)
    with pytest.raises(ConfigurationError, match="project URL"):
        HttpSupabaseAuthAdminClient(project_url="not-a-url", secret_key=_SECRET_KEY)
    for bad_timeout in [-1.0]:
        with pytest.raises(ConfigurationError, match="timeout"):
            HttpSupabaseAuthAdminClient(
                project_url=_PROJECT_URL, secret_key=_SECRET_KEY, timeout_seconds=bad_timeout
            )


def test_the_secret_key_is_redacted_from_ordinary_representation() -> None:
    client = _client(FakeAdminTransport([(204, b"")]))

    assert _SECRET_KEY not in repr(client)
    assert _SECRET_KEY not in str(client)
    assert repr(client) == str(client)


def test_rejections_never_contain_the_key_or_the_project_url(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = FakeAdminTransport([(401, b'{"error": "invalid key"}')])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.adapters.supabase.admin"),
        pytest.raises(SupabaseAuthAdminRejectedError) as error,
    ):
        _client(transport).delete_user(uuid4())

    message = str(error.value)
    assert _SECRET_KEY not in message
    assert _PROJECT_URL not in message
    for record in caplog.records:
        assert _SECRET_KEY not in record.getMessage()
        assert _PROJECT_URL not in record.getMessage()


def test_unknown_outcome_logging_stays_safe(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = FakeAdminTransport([urllib.error.URLError("connection refused")])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.adapters.supabase.admin"),
        pytest.raises(SupabaseAuthAdminOutcomeUnknownError),
    ):
        _client(transport).delete_user(uuid4())

    warnings = [
        record for record in caplog.records if record.name == "openorc.adapters.supabase.admin"
    ]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert _SECRET_KEY not in message
    assert _PROJECT_URL not in message


def test_delete_opens_one_external_adapter_span_with_safe_attributes() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    transport = FakeAdminTransport([(204, b"")])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        _client(transport).delete_user(uuid4())

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "supabase.auth_admin_delete_user"
    attributes = exported.attributes
    assert attributes is not None
    # Only the safe operation name is attachable: the request URL (project
    # reference) and the apikey header (credential material) have no
    # supported path into telemetry.
    assert set(attributes) == {OPERATION}
    assert attributes[OPERATION] == "supabase.auth_admin_delete_user"
    assert _SECRET_KEY not in str(attributes)


def test_request_timeout_seconds_exposes_the_bounded_request_timeout() -> None:
    transport = FakeAdminTransport([(204, b"")])
    client = HttpSupabaseAuthAdminClient(
        project_url=_PROJECT_URL, secret_key=_SECRET_KEY, timeout_seconds=2.5, fetch=transport
    )

    assert client.request_timeout_seconds == 2.5
    client.delete_user(uuid4())
    assert transport.calls[0][3] == 2.5


def test_the_adapter_imports_no_service_modules() -> None:
    # Dependency direction: the adapter owns transport mechanics only and
    # never imports openorc.services.
    import inspect

    import openorc.adapters.supabase.admin as admin_module

    source = inspect.getsource(admin_module)
    assert "from openorc.services" not in source
    assert "import openorc.services" not in source
