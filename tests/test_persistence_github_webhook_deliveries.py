"""Deterministic mapping tests for the GitHub webhook delivery repositories (issue #61).

The ordinary suite cannot execute Postgres; these tests use canned rows and a
fake pool/connection seam to prove row-to-domain mapping, the
ON CONFLICT DO NOTHING deduplication statement shape (insert returning a row
versus a duplicate returning nothing), the set-based routing-linkage insert,
and UTC normalization. Database constraint behavior is proven against a real
database by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

from openorc.domain.github_webhooks import (
    GitHubWebhookDeliveryClassification,
    GitHubWebhookDeliveryIntake,
    GitHubWebhookResolvedRoute,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
)
from openorc.persistence.github_webhook_deliveries import (
    get_github_webhook_delivery,
    list_github_webhook_delivery_routes,
    mark_github_webhook_delivery_processed,
    record_github_webhook_delivery,
    record_webhook_delivery_routes,
)
from openorc.persistence.pool import DatabasePool

_OBSERVED_AT = datetime(2026, 9, 26, 14, 0, 0, tzinfo=timezone(timedelta(hours=2)))
_UTC_OBSERVED = datetime(2026, 9, 26, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [] if self._row is None else [self._row]


class FakeConnection:
    def __init__(self, row: tuple[Any, ...] | None = None) -> None:
        self.row = row
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.row)


class FakePool:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("webhook delivery mapping tests never close pools")


def _pool(conn: FakeConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _delivery_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        "guid-1",
        "issues",
        "edited",
        "relevant",
        "issue_state",
        "resolved",
        123,
        456,
        42,
        None,
        _OBSERVED_AT,
        None,
    )


def _intake() -> GitHubWebhookDeliveryIntake:
    return GitHubWebhookDeliveryIntake(
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
    )


def test_record_maps_the_accepted_delivery_row_and_normalizes_to_utc() -> None:
    conn = FakeConnection(row=_delivery_row())

    delivery = record_github_webhook_delivery(_pool(conn), _intake())

    assert delivery is not None
    assert delivery.delivery_guid == "guid-1"
    assert delivery.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert delivery.routing_target is GitHubWebhookRoutingTarget.ISSUE_STATE
    assert delivery.routing_resolution is GitHubWebhookRoutingResolution.RESOLVED
    assert delivery.received_at == _UTC_OBSERVED
    sql, params = conn.executed[0]
    assert "insert into openorc.github_webhook_deliveries" in sql
    assert "on conflict (delivery_guid) do nothing" in sql
    assert "returning" in sql
    assert params == (
        "guid-1",
        "issues",
        "edited",
        "relevant",
        "issue_state",
        "resolved",
        123,
        456,
        42,
        None,
    )


def test_a_duplicate_guid_returns_none_with_no_second_insert_path() -> None:
    # The conflict-arbitrated statement returns no row for an already-accepted
    # GUID: the single insert performs both insert and dedup classification.
    conn = FakeConnection(row=None)

    delivery = record_github_webhook_delivery(_pool(conn), _intake())

    assert delivery is None
    assert len(conn.executed) == 1


def test_route_persistence_uses_the_set_based_insert() -> None:
    conn = FakeConnection()
    workspace_a = uuid.uuid4()
    workspace_b = uuid.uuid4()
    repository_a = uuid.uuid4()
    repository_b = uuid.uuid4()

    record_webhook_delivery_routes(
        _pool(conn),
        delivery_id=uuid.uuid4(),
        routes=(
            GitHubWebhookResolvedRoute(workspace_id=workspace_a, repository_id=repository_a),
            GitHubWebhookResolvedRoute(workspace_id=workspace_b, repository_id=repository_b),
        ),
    )

    sql, params = conn.executed[0]
    assert "insert into openorc.github_webhook_delivery_routes" in sql
    assert "unnest" in sql
    assert params is not None
    assert list(params[1]) == [workspace_a, workspace_b]
    assert list(params[2]) == [repository_a, repository_b]


def test_empty_route_set_persists_nothing() -> None:
    conn = FakeConnection()

    record_webhook_delivery_routes(_pool(conn), delivery_id=uuid.uuid4(), routes=())

    assert conn.executed == []


def test_route_listing_maps_rows_deterministically() -> None:
    delivery_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    conn = FakeConnection(
        row=(uuid.uuid4(), delivery_id, workspace_id, repository_id, _OBSERVED_AT)
    )

    routes = list_github_webhook_delivery_routes(_pool(conn), delivery_id=delivery_id)

    assert len(routes) == 1
    assert routes[0].delivery_id == delivery_id
    assert routes[0].workspace_id == workspace_id
    assert routes[0].repository_id == repository_id
    assert routes[0].created_at == _UTC_OBSERVED
    sql, params = conn.executed[0]
    assert "order by workspace_id, repository_id" in sql
    assert params == (delivery_id,)


def test_get_maps_the_delivery_row_for_the_provider_guid() -> None:
    delivery_id = uuid.uuid4()
    conn = FakeConnection(
        row=(
            delivery_id,
            "guid-1",
            "issues",
            "edited",
            "relevant",
            "issue_state",
            "resolved",
            123,
            456,
            42,
            None,
            _OBSERVED_AT,
            _OBSERVED_AT,
        )
    )

    delivery = get_github_webhook_delivery(_pool(conn), delivery_guid="guid-1")

    assert delivery is not None
    assert delivery.id == delivery_id
    assert delivery.received_at == _UTC_OBSERVED
    assert delivery.processed_at == _UTC_OBSERVED
    sql, params = conn.executed[0]
    assert "where delivery_guid = %s" in sql
    assert params == ("guid-1",)


def test_get_returns_none_for_an_unknown_guid() -> None:
    conn = FakeConnection(row=None)

    assert get_github_webhook_delivery(_pool(conn), delivery_guid="guid-x") is None


def test_processed_marking_is_a_conditional_idempotent_update() -> None:
    delivery_id = uuid.uuid4()
    conn = FakeConnection(row=(delivery_id,))

    marked = mark_github_webhook_delivery_processed(_pool(conn), delivery_id=delivery_id)

    assert marked is True
    sql, params = conn.executed[0]
    assert "set processed_at = now()" in sql
    assert "where id = %s and processed_at is null" in sql
    assert params == (delivery_id,)


def test_already_processed_or_absent_marking_returns_false() -> None:
    conn = FakeConnection(row=None)

    assert mark_github_webhook_delivery_processed(_pool(conn), delivery_id=uuid.uuid4()) is False
