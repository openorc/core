"""Deterministic tests for the GitHub repository installation-access service
(issue #58).

Canned rows and a scripted fake connection seam prove the owner-gated route
composition with the #57 resolver (uniform anti-probing not-found outcomes
for unconfigured routes), the translation of the adapter's classified
outcomes into the typed application vocabulary, that no database transaction
is ever open across the GitHub call, the explicit absence of any
PAT/human-OAuth credential parameter, and the sanctioned telemetry
vocabulary. No live GitHub or Postgres access.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.adapters.github import (
    GitHubAccessValidation,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
    GitHubWorkflowCapability,
)
from openorc.observability import (
    CONNECTION_ID,
    EXECUTION_ID,
    GITHUB_HEAD_SHA,
    GITHUB_INSTALLATION_ID,
    GITHUB_ISSUE_NUMBER,
    GITHUB_PULL_REQUEST_NUMBER,
    GITHUB_REPOSITORY,
    OPERATION,
    REQUEST_ID,
    RQ_JOB_ID,
    TASK_ID,
    WORKFLOW_ROLE,
    WORKSPACE_ID,
    annotate_span,
    application_span,
    injected_tracer_source,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import github_repository_access
from openorc.services.errors import (
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)

# The sanctioned safe attribute vocabulary, derived from the boundary module
# itself (issue #108) plus the installation identifier added by this leaf
# (issue #58): the service's spans may carry only these names.
_SANCTIONED_ATTRIBUTE_NAMES = frozenset(
    {
        REQUEST_ID,
        WORKSPACE_ID,
        TASK_ID,
        EXECUTION_ID,
        CONNECTION_ID,
        WORKFLOW_ROLE,
        OPERATION,
        GITHUB_INSTALLATION_ID,
        GITHUB_REPOSITORY,
        GITHUB_ISSUE_NUMBER,
        GITHUB_PULL_REQUEST_NUMBER,
        GITHUB_HEAD_SHA,
        RQ_JOB_ID,
    }
)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    """Pool fake that tracks checked-out connections for the transaction proof.

    The service must never hold a database transaction open across the
    external GitHub call: the fake GitHub facade asserts that no connection
    is checked out at invocation time.
    """

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("github repository access service tests never close pools")


class FakeGitHubAppClient:
    """The adapter Protocol fake recording the exact routing arguments."""

    def __init__(
        self,
        *,
        pool: FakePool,
        result: GitHubAccessValidation | None = None,
        error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._result = result
        self._error = error
        self.calls: list[dict[str, int]] = []

    def validate_installation_repository_access(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubAccessValidation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.calls.append(
            {
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> Any:
        raise AssertionError("the service must not compose raw reconciliation operations")

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("the service must not compose raw reconciliation operations")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(workspace_id: Any, profile_id: Any) -> tuple[Any, ...]:
    return (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")


def _installation_row(installation_id: Any, workspace_id: Any) -> tuple[Any, ...]:
    return (
        installation_id,
        workspace_id,
        12345678,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _repository_row(
    repository_id: Any,
    project_id: Any,
    workspace_id: Any,
    route: Any,
) -> tuple[Any, ...]:
    return (
        repository_id,
        project_id,
        workspace_id,
        987654321,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        route,
    )


def _granted_validation(
    *, github_installation_id: int = 12345678, github_repository_id: int = 987654321
) -> GitHubAccessValidation:
    return GitHubAccessValidation(
        github_installation_id=github_installation_id,
        github_repository_id=github_repository_id,
        capabilities=frozenset(GitHubWorkflowCapability),
        subscribed_events=frozenset({"issues", "pull_request", "push"}),
    )


def test_validation_returns_the_granted_facts_for_the_exact_route() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, installation_id),
            _installation_row(installation_id, workspace_id),
        ]
    )
    pool = FakePool(conn)
    expected = _granted_validation()
    github = FakeGitHubAppClient(pool=pool, result=expected)

    result = github_repository_access.validate_repository_installation_access(
        _pool(conn),
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        repository_id=repository_id,
    )

    assert result is expected
    # The adapter receives the exact stable identities resolved from the
    # #57 route: the routed installation and the repository's stable ID.
    assert github.calls == [{"github_installation_id": 12345678, "github_repository_id": 987654321}]


def test_an_unconfigured_repository_fails_closed_as_uniform_not_found() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, None),
        ]
    )
    github = FakeGitHubAppClient(pool=FakePool(conn), result=_granted_validation())

    with pytest.raises(NotFoundError):
        github_repository_access.validate_repository_installation_access(
            _pool(conn),
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )

    # The unconfigured route is not GitHub-authorized: the adapter is never
    # composed and no installation is guessed.
    assert github.calls == []


@pytest.mark.parametrize(
    ("adapter_error", "expected_error"),
    [
        (GitHubAuthorizationRejectedError("denied"), AuthorizationError),
        (GitHubRateLimitedError("rate limited"), ExternalOperationFailedError),
        (GitHubAuthenticationRejectedError("rejected"), ExternalOperationFailedError),
        (GitHubRequestRejectedError("rejected"), ExternalOperationFailedError),
        (GitHubOutcomeUncertainError("unknown"), ExternalOperationUncertainError),
    ],
)
def test_adapter_outcomes_translate_into_the_typed_application_vocabulary(
    adapter_error: Exception, expected_error: type[Exception]
) -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, installation_id),
            _installation_row(installation_id, workspace_id),
        ]
    )
    github = FakeGitHubAppClient(pool=FakePool(conn), error=adapter_error)

    with pytest.raises(expected_error):
        github_repository_access.validate_repository_installation_access(
            _pool(conn),
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )


def test_rate_limits_never_translate_into_lost_authorization() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, installation_id),
            _installation_row(installation_id, workspace_id),
        ]
    )
    github = FakeGitHubAppClient(pool=FakePool(conn), error=GitHubRateLimitedError("throttled"))

    with pytest.raises(ExternalOperationFailedError) as caught:
        github_repository_access.validate_repository_installation_access(
            _pool(conn),
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )

    # A rate limit is a known provider condition, deliberately NOT a
    # classified authorization absence.
    assert not isinstance(caught.value, AuthorizationError)


@pytest.mark.parametrize("field", ["profile_id", "workspace_id", "repository_id"])
def test_malformed_commands_are_classified_before_any_state_is_touched(field: str) -> None:
    conn = ScriptedConnection([])
    github = FakeGitHubAppClient(pool=FakePool(conn), result=_granted_validation())

    kwargs: dict[str, Any] = {
        "profile_id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "repository_id": uuid.uuid4(),
    }
    kwargs[field] = "not-a-uuid"

    with pytest.raises(InvalidCommandError):
        github_repository_access.validate_repository_installation_access(
            _pool(conn), github, **kwargs
        )

    assert conn.executed == []
    assert github.calls == []


def test_the_service_surface_carries_no_human_credential_parameter() -> None:
    # No PAT or human OAuth token fallback exists: the service signature has
    # no parameter for credential material of any kind.
    signature = inspect.signature(github_repository_access.validate_repository_installation_access)

    assert set(signature.parameters) == {
        "pool",
        "github",
        "profile_id",
        "workspace_id",
        "repository_id",
    }


def test_telemetry_uses_only_the_sanctioned_attribute_vocabulary() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, installation_id),
            _installation_row(installation_id, workspace_id),
        ]
    )
    github = FakeGitHubAppClient(pool=FakePool(conn), result=_granted_validation())

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        github_repository_access.validate_repository_installation_access(
            _pool(conn),
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )

    spans = exporter.get_finished_spans()
    span_names = [span.name for span in spans]
    # The service span and the composed #57 resolver span are both recorded.
    assert "github_repository_access.validate_repository_installation_access" in span_names
    assert "github_installations.require_configured_repository_installation_route" in span_names
    service_span = next(
        span
        for span in spans
        if span.name == "github_repository_access.validate_repository_installation_access"
    )
    attributes = service_span.attributes or {}
    assert set(attributes) <= _SANCTIONED_ATTRIBUTE_NAMES
    assert (
        attributes[OPERATION] == "github_repository_access.validate_repository_installation_access"
    )
    assert attributes[WORKSPACE_ID] == str(workspace_id)


def test_span_annotation_helper_accepts_the_new_sanctioned_attribute() -> None:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        application_span("openorc.test", "annotated") as span,
    ):
        annotate_span(
            span,
            operation="annotated",
            github_installation_id="12345678",
        )

    attributes = exporter.get_finished_spans()[0].attributes or {}
    assert attributes[GITHUB_INSTALLATION_ID] == "12345678"
