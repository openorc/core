"""Deterministic fake-seam tests for the GitHub webhook dispatch services (issue #120).

Covers the required behavioral matrix: target→capability routing per durable
routing target; irrelevant/unsupported and unresolved-routing terminal
handling; issue-family dispatch through the authoritative intake composition;
relation fan-out over durable issue projections; repository-metadata
re-observation (including installation-scoped fan-out); canonical-PR
reconciliation by stable identity; checks/push fan-out; multi-route
determinism; duplicate/replay idempotence; enqueue outcome classification
(known failure vs uncertain outcome are distinct); bounded processed-marking
semantics; the thin-route facade composing intake with submission; safe
telemetry vocabulary; and the no-database-transaction rule across the queue
seam and every external invocation boundary. The delivery/route/PR/projection
reads run the real persistence statements against a scripted connection, so
the transaction proof is not vacuous. No live GitHub, Valkey, or Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.domain.github_webhooks import (
    GitHubWebhookDeliveryClassification,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
)
from openorc.observability import injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import github_webhook_dispatch
from openorc.services.errors import (
    AuthenticationError,
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
)
from openorc.services.github_webhook_dispatch import GitHubWebhookDispatchRouteStatus

_NOW = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
_DELIVERY_ID = uuid.uuid4()
_WORKSPACE_A = uuid.uuid4()
_WORKSPACE_B = uuid.uuid4()
_REPOSITORY_A = uuid.uuid4()
_REPOSITORY_B = uuid.uuid4()
_TASK_ID = uuid.uuid4()

# The sanctioned safe attribute vocabulary (issue #108), derived from the
# boundary module itself: dispatch spans may carry only these names.
_SANCTIONED_ATTRIBUTE_NAMES = frozenset(
    {
        "openorc.request_id",
        "openorc.workspace_id",
        "openorc.task_id",
        "openorc.execution_id",
        "openorc.connection_id",
        "openorc.workflow_role",
        "openorc.operation",
        "openorc.github_installation_id",
        "openorc.github_repository",
        "openorc.github_issue_number",
        "openorc.github_pull_request_number",
        "openorc.github_head_sha",
        "openorc.rq_job_id",
    }
)


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]] | tuple[Any, ...] | None) -> None:
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._rows is None:
            return None
        if isinstance(self._rows, tuple):
            return self._rows
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._rows is None or isinstance(self._rows, tuple):
            return []
        return self._rows


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, tuple[Any, ...] | list[tuple[Any, ...]] | None]] = []

    def on(self, sql_marker: str, result: tuple[Any, ...] | list[tuple[Any, ...]] | None) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Nested psycopg transaction blocks (SAVEPOINTs under composition).
        yield


class FakePool:
    """Pool fake tracking checked-out connections for the transaction proof."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("github webhook dispatch tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _github_client() -> Any:
    """A typed ``Any`` stand-in for the adapter the capability fakes never touch."""
    return object()


def _delivery_row(
    *,
    delivery_guid: str = "guid-1",
    event_name: str = "issues",
    action: str | None = "edited",
    classification: str = "relevant",
    routing_target: str | None = "issue_state",
    routing_resolution: str | None = "resolved",
    github_installation_id: int | None = 123,
    github_repository_id: int | None = 456,
    github_issue_number: int | None = 42,
    github_pull_request_number: int | None = None,
) -> tuple[Any, ...]:
    return (
        _DELIVERY_ID,
        delivery_guid,
        event_name,
        action,
        classification,
        routing_target,
        routing_resolution,
        github_installation_id,
        github_repository_id,
        github_issue_number,
        github_pull_request_number,
        _NOW,
        None,
    )


def _route_row(workspace_id: uuid.UUID, repository_id: uuid.UUID) -> tuple[Any, ...]:
    return (uuid.uuid4(), _DELIVERY_ID, workspace_id, repository_id, _NOW)


def _issue_projection_row(
    workspace_id: uuid.UUID, repository_id: uuid.UUID, github_issue_id: int, issue_number: int
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        repository_id,
        github_issue_id,
        issue_number,
        "Tracked issue",
        None,
        "open",
        "a" * 64,
        None,
        _NOW,
        _NOW,
    )


def _pull_request_row(task_id: uuid.UUID, repository_id: uuid.UUID) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_A,
        task_id,
        repository_id,
        555,
        7,
        "openorc/task-1",
        "main",
        "b" * 40,
        "open",
        None,
        _NOW,
        _NOW,
    )


class FakeSubmission:
    """The queue seam fake asserting no open transaction at the enqueue boundary."""

    def __init__(self, pool: FakePool, outcome: Any) -> None:
        self._pool = pool
        self._outcome = outcome
        self.enqueued: list[str] = []

    def enqueue(self, delivery_guid: str) -> Any:
        assert self._pool.active_connections == 0, (
            "no database transaction may span the queue enqueue"
        )
        self.enqueued.append(delivery_guid)
        return self._outcome


class RecordingCapability:
    """A capability fake asserting no open transaction at the invocation boundary."""

    def __init__(
        self, pool: FakePool, *, error: Exception | None = None, fail_after: int | None = None
    ) -> None:
        self._pool = pool
        self._error = error
        self._fail_after = fail_after
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        assert self._pool.active_connections == 0, (
            "no database transaction may span the external reconciliation invocation"
        )
        call_index = len(self.calls)
        self.calls.append(dict(kwargs))
        should_fail = self._error is not None and (
            self._fail_after is None or call_index >= self._fail_after
        )
        if should_fail:
            assert self._error is not None
            raise self._error


class Harness:
    """Scripted persistence plus recording capability fakes, wired by the fixture."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self.conn = conn
        self.pool_fake = FakePool(conn)
        self.pool = cast(DatabasePool, self.pool_fake)
        self.intake = RecordingCapability(self.pool_fake)
        self.dependencies = RecordingCapability(self.pool_fake)
        self.hierarchy = RecordingCapability(self.pool_fake)
        self.repository_observation = RecordingCapability(self.pool_fake)
        self.pull_request_reconcile = RecordingCapability(self.pool_fake)
        self.checks_projection = RecordingCapability(self.pool_fake)


@pytest.fixture
def harness_factory(monkeypatch: pytest.MonkeyPatch):
    def _make() -> Harness:
        harness = Harness(ScriptedConnection())
        monkeypatch.setattr(
            github_webhook_dispatch.task_intake, "intake_repository_task", harness.intake
        )
        monkeypatch.setattr(
            github_webhook_dispatch.github_issue_relations,
            "synchronize_repository_issue_dependencies",
            harness.dependencies,
        )
        monkeypatch.setattr(
            github_webhook_dispatch.github_issue_relations,
            "synchronize_repository_issue_hierarchy",
            harness.hierarchy,
        )
        monkeypatch.setattr(
            github_webhook_dispatch.github_reconciliation,
            "reconcile_repository_observation",
            harness.repository_observation,
        )
        monkeypatch.setattr(
            github_webhook_dispatch.task_pull_request_reconciliation,
            "reconcile_task_pull_request",
            harness.pull_request_reconcile,
        )
        monkeypatch.setattr(
            github_webhook_dispatch.commit_checks_projection,
            "project_commit_checks",
            harness.checks_projection,
        )
        return harness

    return _make


def _script_delivery(conn: ScriptedConnection, row: tuple[Any, ...] | None) -> None:
    conn.on("from openorc.github_webhook_deliveries", row)


def _script_routes(conn: ScriptedConnection, rows: list[tuple[Any, ...]]) -> None:
    conn.on("from openorc.github_webhook_delivery_routes", rows)


def _script_processed(conn: ScriptedConnection, delivery_id: uuid.UUID | None) -> None:
    conn.on("set processed_at = now()", None if delivery_id is None else (delivery_id,))


def _script_issues(conn: ScriptedConnection, rows: list[tuple[Any, ...]]) -> None:
    conn.on("from openorc.github_issues", rows)


def _script_pull_request(conn: ScriptedConnection, row: tuple[Any, ...] | None) -> None:
    conn.on("limit 1", row)


def _script_pull_requests(conn: ScriptedConnection, rows: list[tuple[Any, ...]]) -> None:
    conn.on("from openorc.task_pull_requests", rows)


def _accepted_delivery() -> Any:
    """Build the durable delivery record the submission boundary reloads."""
    from openorc.domain.github_webhooks import GitHubWebhookDelivery

    return GitHubWebhookDelivery(
        id=_DELIVERY_ID,
        delivery_guid="guid-1",
        event_name="issues",
        action="edited",
        classification=GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.ISSUE_STATE,
        routing_resolution=GitHubWebhookRoutingResolution.RESOLVED,
        github_installation_id=123,
        github_repository_id=456,
        github_issue_number=42,
        github_pull_request_number=None,
        received_at=_NOW,
        processed_at=None,
    )


def _settings() -> Any:
    from openorc.config import Settings

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
        github_webhook_secret="test-webhook-secret",
    )


# --- dispatch: the authoritative reconciliation routes -------------------------


def test_issue_state_dispatch_invokes_the_authoritative_intake_composition(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert [
        (outcome.workspace_id, outcome.repository_id, outcome.status.value)
        for outcome in result.route_outcomes
    ] == [(_WORKSPACE_A, _REPOSITORY_A, "reconciled")]
    assert harness.intake.calls == [
        {
            "workspace_id": _WORKSPACE_A,
            "repository_id": _REPOSITORY_A,
            "issue_number": 42,
        }
    ]
    assert harness.dependencies.calls == []


def test_a_closed_issue_typedly_declines_and_marks_the_delivery_processed(harness_factory) -> None:
    from openorc.services.errors import NotFoundError as _NF

    harness = harness_factory()
    harness.intake._error = _NF("the addressed GitHub issue is not open")
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.DECLINED
    assert harness.intake.calls != []


def test_a_blocked_issue_typedly_declines_and_marks_the_delivery_processed(harness_factory) -> None:
    from openorc.services.errors import ConflictError

    harness = harness_factory()
    harness.intake._error = ConflictError("GitHub currently reports the addressed issue as blocked")
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.DECLINED


def test_issue_state_known_provider_failure_leaves_the_delivery_recoverable(
    harness_factory,
) -> None:
    harness = harness_factory()
    harness.intake._error = ExternalOperationFailedError("rate limited")
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is False
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.FAILED_KNOWN
    # No processed-marking was attempted: the unmatched scripted handler
    # proves the bounded recovery metadata stayed untouched.
    assert harness.intake.calls != []


def test_issue_state_uncertain_outcome_stays_uncertain_and_recoverable(harness_factory) -> None:
    harness = harness_factory()
    harness.intake._error = ExternalOperationUncertainError("unknown outcome")
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is False
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.FAILED_UNCERTAIN


def test_issue_relations_dispatch_fans_out_over_tracked_issue_projections(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="sub_issues",
            routing_target="issue_relations",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_issues(
        harness.conn,
        [
            _issue_projection_row(_WORKSPACE_A, _REPOSITORY_A, 503, 42),
            _issue_projection_row(_WORKSPACE_A, _REPOSITORY_A, 504, 43),
        ],
    )
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.dependencies.calls == [
        {
            "workspace_id": _WORKSPACE_A,
            "repository_id": _REPOSITORY_A,
            "issue_number": 42,
            "github_issue_id": 503,
        },
        {
            "workspace_id": _WORKSPACE_A,
            "repository_id": _REPOSITORY_A,
            "issue_number": 43,
            "github_issue_id": 504,
        },
    ]
    assert harness.hierarchy.calls == harness.dependencies.calls


def test_issue_relations_with_no_tracked_issues_reconciles_nothing_and_marks_processed(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="issue_dependencies",
            routing_target="issue_relations",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_issues(harness.conn, [])
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.RECONCILED
    assert harness.dependencies.calls == []
    assert harness.hierarchy.calls == []


def test_issue_relations_failure_fails_the_route_closed(harness_factory) -> None:
    harness = harness_factory()
    harness.dependencies._error = AuthorizationError("access lost")
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="sub_issues",
            routing_target="issue_relations",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_issues(harness.conn, [_issue_projection_row(_WORKSPACE_A, _REPOSITORY_A, 503, 42)])

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is False
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.FAILED_KNOWN
    # The hierarchy unit was never reached: the dependency unit failed the
    # route closed and left the prior mirror byte-identical.
    assert harness.hierarchy.calls == []


def test_repository_metadata_dispatch_invokes_the_authoritative_observation(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="repository",
            routing_target="repository_metadata",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.repository_observation.calls == [
        {"workspace_id": _WORKSPACE_A, "repository_id": _REPOSITORY_A}
    ]


def test_installation_scoped_delivery_fans_out_over_the_installation_routes(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="installation",
            routing_target="repository_metadata",
            github_repository_id=None,
            github_issue_number=None,
        ),
    )
    _script_routes(
        harness.conn,
        [
            _route_row(_WORKSPACE_A, _REPOSITORY_A),
            _route_row(_WORKSPACE_B, _REPOSITORY_B),
        ],
    )
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.repository_observation.calls == [
        {"workspace_id": _WORKSPACE_A, "repository_id": _REPOSITORY_A},
        {"workspace_id": _WORKSPACE_B, "repository_id": _REPOSITORY_B},
    ]


def test_repository_metadata_access_loss_fails_known_and_stays_recoverable(harness_factory) -> None:
    harness = harness_factory()
    harness.repository_observation._error = AuthorizationError("access lost")
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="repository",
            routing_target="repository_metadata",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is False


def test_pull_request_state_dispatch_reconciles_the_canonical_record_by_number(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="pull_request",
            action="closed",
            routing_target="pull_request_state",
            github_issue_number=None,
            github_pull_request_number=7,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_pull_request(harness.conn, _pull_request_row(_TASK_ID, _REPOSITORY_A))
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.pull_request_reconcile.calls == [
        {"workspace_id": _WORKSPACE_A, "task_id": _TASK_ID}
    ]


def test_a_pull_request_number_without_a_canonical_record_typedly_declines(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="pull_request",
            action="closed",
            routing_target="pull_request_state",
            github_issue_number=None,
            github_pull_request_number=999,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_pull_request(harness.conn, None)
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert result.route_outcomes[0].status is GitHubWebhookDispatchRouteStatus.DECLINED
    assert harness.pull_request_reconcile.calls == []


def test_pull_request_identity_dispatch_routes_to_the_same_b7_reconciliation(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="pull_request",
            action="synchronize",
            routing_target="task_branch_or_pull_request",
            github_issue_number=None,
            github_pull_request_number=7,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_pull_request(harness.conn, _pull_request_row(_TASK_ID, _REPOSITORY_A))
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.pull_request_reconcile.calls == [
        {"workspace_id": _WORKSPACE_A, "task_id": _TASK_ID}
    ]


def test_push_dispatch_reconciles_every_canonical_record_in_the_repository(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="push",
            action=None,
            routing_target="task_branch_or_pull_request",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_pull_requests(
        harness.conn,
        [
            _pull_request_row(_TASK_ID, _REPOSITORY_A),
            _pull_request_row(uuid.uuid4(), _REPOSITORY_A),
        ],
    )
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert len(harness.pull_request_reconcile.calls) == 2
    assert all(
        call["workspace_id"] == _WORKSPACE_A for call in harness.pull_request_reconcile.calls
    )


def test_checks_dispatch_projects_the_authoritative_check_surface(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="check_run",
            action="completed",
            routing_target="checks",
            github_issue_number=None,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_pull_requests(harness.conn, [_pull_request_row(_TASK_ID, _REPOSITORY_A)])
    _script_processed(harness.conn, _DELIVERY_ID)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.checks_projection.calls == [{"workspace_id": _WORKSPACE_A, "task_id": _TASK_ID}]


def test_multi_route_dispatch_marks_processed_only_when_every_route_completes(
    harness_factory,
) -> None:
    harness = harness_factory()
    # The first route reconciles; the second fails as a known provider
    # condition, so the delivery stays recoverable as a whole.
    harness.repository_observation._error = AuthorizationError("access lost")
    harness.repository_observation._fail_after = 1
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="repository",
            routing_target="repository_metadata",
            github_issue_number=None,
        ),
    )
    _script_routes(
        harness.conn,
        [
            _route_row(_WORKSPACE_A, _REPOSITORY_A),
            _route_row(_WORKSPACE_B, _REPOSITORY_B),
        ],
    )

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is False
    assert [outcome.status for outcome in result.route_outcomes] == [
        GitHubWebhookDispatchRouteStatus.RECONCILED,
        GitHubWebhookDispatchRouteStatus.FAILED_KNOWN,
    ]


def test_a_replayed_dispatch_job_converges_idempotently(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    # A replay finds the delivery already marked: the idempotent conditional
    # update answers False and the authoritative re-read remains a no-op.
    _script_processed(harness.conn, None)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-1"
    )

    assert result.processed is True
    assert harness.intake.calls == [
        {"workspace_id": _WORKSPACE_A, "repository_id": _REPOSITORY_A, "issue_number": 42}
    ]


def test_an_unknown_delivery_guid_is_a_typed_safe_outcome(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, None)

    result = github_webhook_dispatch.dispatch_github_webhook_delivery(
        harness.pool, _github_client(), delivery_guid="guid-unknown"
    )

    assert result.delivery is None
    assert result.processed is False
    assert result.route_outcomes == ()


# --- dispatch submission: the enqueue boundary ---------------------------------


def _submit(harness: Harness, submission: FakeSubmission) -> Any:
    return github_webhook_dispatch.submit_github_webhook_dispatch(
        harness.pool, submission, delivery_guid="guid-1"
    )


def test_submission_enqueues_resolved_deliveries_with_only_the_delivery_guid(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)

    result = _submit(harness, submission)

    assert result.enqueued is True
    assert result.enqueue_outcome is None
    assert result.delivery is not None
    # The queue payload is exactly the provider delivery GUID — the only safe
    # stable identity; no payload content, routing detail, or extra state.
    assert submission.enqueued == ["guid-1"]


def test_submission_marks_relevant_unresolved_deliveries_terminal(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="installation",
            routing_target="repository_metadata",
            routing_resolution="unmapped_installation",
            github_repository_id=None,
            github_issue_number=None,
        ),
    )
    _script_processed(harness.conn, _DELIVERY_ID)
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)

    result = _submit(harness, submission)

    assert result.enqueued is False
    assert result.enqueue_outcome is None
    # Nothing outstanding: the bounded recovery instant was written and no
    # queue submission happened.
    assert submission.enqueued == []
    assert any("set processed_at = now()" in sql for sql, _ in harness.conn.executed)


def test_submission_leaves_ignored_and_unusable_deliveries_untouched(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(classification="ignored", routing_target=None, routing_resolution=None),
    )
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)

    result = _submit(harness, submission)

    assert result.enqueued is False
    assert submission.enqueued == []
    # Ignored deliveries require no recovery metadata: no processed-marking
    # statement was executed (the unmatched handler would have raised).
    assert not any("set processed_at" in sql for sql, _ in harness.conn.executed)


def test_submission_known_enqueue_failure_is_distinct_and_recoverable(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    submission = FakeSubmission(
        harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.KNOWN_FAILED
    )

    result = _submit(harness, submission)

    assert result.enqueued is False
    assert result.enqueue_outcome is github_webhook_dispatch.EnqueueOutcome.KNOWN_FAILED
    # Known enqueue failure is classified, never retried here, and the
    # delivery stays recoverable (no processed-marking was attempted).
    assert not any("set processed_at" in sql for sql, _ in harness.conn.executed)


def test_submission_uncertain_outcome_never_reenqueues_or_reconciles(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.UNCERTAIN)

    result = _submit(harness, submission)

    assert result.enqueued is False
    assert result.enqueue_outcome is github_webhook_dispatch.EnqueueOutcome.UNCERTAIN
    # The uncertain submission enqueued exactly once (no internal retry), and
    # no external workflow operation was triggered at submission: unresolved
    # processing stays recoverable by #62's independent path.
    assert submission.enqueued == ["guid-1"]
    assert harness.intake.calls == []
    assert harness.repository_observation.calls == []
    assert not any("set processed_at" in sql for sql, _ in harness.conn.executed)


def test_submission_of_an_unknown_delivery_identity_is_a_typed_safe_outcome(
    harness_factory,
) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, None)
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)

    result = github_webhook_dispatch.submit_github_webhook_dispatch(
        harness.pool, submission, delivery_guid="guid-unknown"
    )

    assert result.delivery is None
    assert result.enqueued is False
    assert submission.enqueued == []


# --- the facade composing #61 intake with dispatch submission -------------------


def _intake_result(**overrides: Any) -> Any:
    from openorc.services.github_webhook_intake import GitHubWebhookIntake

    values: dict[str, Any] = {"delivery": None, "duplicate": False}
    values.update(overrides)
    return GitHubWebhookIntake(**values)


def test_the_facade_submits_accepted_deliveries_and_skips_duplicates(
    harness_factory, monkeypatch
) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)
    monkeypatch.setattr(
        github_webhook_dispatch,
        "intake_github_webhook",
        lambda *a, **k: _intake_result(delivery=_accepted_delivery()),
    )

    result = github_webhook_dispatch.intake_and_dispatch_github_webhook(
        harness.pool,
        _settings(),
        submission,
        raw_body=b"{}",
        signature_header="sha256=aa",
        event_name="issues",
        delivery_guid="guid-1",
    )

    assert result.intake.delivery is not None
    assert result.submission is not None
    assert result.submission.enqueued is True
    assert submission.enqueued == ["guid-1"]


def test_the_facade_skips_submission_for_duplicates(harness_factory, monkeypatch) -> None:
    harness = harness_factory()
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)
    monkeypatch.setattr(
        github_webhook_dispatch,
        "intake_github_webhook",
        lambda *a, **k: _intake_result(duplicate=True),
    )

    result = github_webhook_dispatch.intake_and_dispatch_github_webhook(
        harness.pool,
        _settings(),
        submission,
        raw_body=b"{}",
        signature_header="sha256=aa",
        event_name="issues",
        delivery_guid="guid-1",
    )

    assert result.intake.duplicate is True
    assert result.submission is None
    assert submission.enqueued == []
    # No durable dispatch work happened for the duplicate ack.
    assert not any(
        "from openorc.github_webhook_deliveries" in sql for sql, _ in harness.conn.executed
    )


def test_the_facade_propagates_typed_intake_errors_to_the_transport(
    harness_factory, monkeypatch
) -> None:
    harness = harness_factory()
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)

    def _raise(*args: Any, **kwargs: Any) -> None:
        raise AuthenticationError("rejected")

    monkeypatch.setattr(github_webhook_dispatch, "intake_github_webhook", _raise)

    with pytest.raises(AuthenticationError):
        github_webhook_dispatch.intake_and_dispatch_github_webhook(
            harness.pool,
            _settings(),
            submission,
            raw_body=b"{}",
            signature_header=None,
            event_name="issues",
            delivery_guid="guid-1",
        )
    assert submission.enqueued == []


def test_the_facade_rejects_a_malformed_delivery_identity(harness_factory) -> None:
    harness = harness_factory()
    submission = FakeSubmission(harness.pool_fake, github_webhook_dispatch.EnqueueOutcome.ENQUEUED)

    with pytest.raises(InvalidCommandError):
        github_webhook_dispatch.intake_and_dispatch_github_webhook(
            harness.pool,
            _settings(),
            submission,
            raw_body=b"{}",
            signature_header=None,
            event_name="issues",
            delivery_guid="   ",
        )


# --- telemetry: the safe vocabulary only ---------------------------------------


def test_dispatch_telemetry_carries_only_the_safe_vocabulary(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(harness.conn, _delivery_row())
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_processed(harness.conn, _DELIVERY_ID)

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with injected_tracer_source(lambda _scope: provider.get_tracer("test")):
        github_webhook_dispatch.dispatch_github_webhook_delivery(
            harness.pool, _github_client(), delivery_guid="guid-1"
        )

    provider.shutdown()
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attributes = dict(spans[0].attributes or {})
    assert attributes["openorc.operation"] == (
        "github_webhook_dispatch.dispatch_github_webhook_delivery"
    )
    assert set(attributes) <= _SANCTIONED_ATTRIBUTE_NAMES
    assert attributes["openorc.github_issue_number"] == 42


def test_dispatch_telemetry_for_a_pull_request_delivery(harness_factory) -> None:
    harness = harness_factory()
    _script_delivery(
        harness.conn,
        _delivery_row(
            event_name="pull_request",
            action="closed",
            routing_target="pull_request_state",
            github_issue_number=None,
            github_pull_request_number=7,
        ),
    )
    _script_routes(harness.conn, [_route_row(_WORKSPACE_A, _REPOSITORY_A)])
    _script_pull_request(harness.conn, _pull_request_row(_TASK_ID, _REPOSITORY_A))
    _script_processed(harness.conn, _DELIVERY_ID)

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with injected_tracer_source(lambda _scope: provider.get_tracer("test")):
        github_webhook_dispatch.dispatch_github_webhook_delivery(
            harness.pool, _github_client(), delivery_guid="guid-1"
        )

    provider.shutdown()
    spans = exporter.get_finished_spans()
    attributes = dict(spans[0].attributes or {})
    assert set(attributes) <= _SANCTIONED_ATTRIBUTE_NAMES
    assert attributes["openorc.github_pull_request_number"] == 7
