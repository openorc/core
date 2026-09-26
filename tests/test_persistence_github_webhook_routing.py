"""Deterministic statement-shape tests for the webhook routing resolver (issue #61).

The ordinary suite cannot execute Postgres; these tests use a scripted fake
connection seam to prove the exact resolution statements: candidate
installation lookup by stable external ID, exact route matching joined on
the explicit installation route, and the fail-closed classification reads
(unmapped installation, unconfigured repository, route mismatch). Durable
resolution/cascade behavior is proven against a real database by the
integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from openorc.domain.github_webhooks import GitHubWebhookRoutingResolution
from openorc.persistence.github_webhook_routing import resolve_github_webhook_routes
from openorc.persistence.pool import DatabasePool


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
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[list[tuple[Any, ...]] | tuple[Any, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("webhook routing resolution tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def test_an_unmapped_installation_resolves_without_route_statements() -> None:
    conn = ScriptedConnection(results=[[]])  # no candidate workspaces

    resolution = resolve_github_webhook_routes(
        _pool(conn), github_installation_id=123, github_repository_id=456
    )

    assert resolution.resolution is GitHubWebhookRoutingResolution.UNMAPPED_INSTALLATION
    assert resolution.routes == ()
    sql, params = conn.executed[0]
    assert "from openorc.github_installations" in sql
    assert "where github_installation_id = %s" in sql
    assert params == (123,)
    # The fail-closed observation needs no further routing statements.
    assert len(conn.executed) == 1


def test_exact_route_matches_resolve_with_workspace_ordered_fan_out() -> None:
    workspace_a = uuid.uuid4()
    workspace_b = uuid.uuid4()
    repository_a = uuid.uuid4()
    repository_b = uuid.uuid4()
    conn = ScriptedConnection(
        results=[
            [(workspace_a,), (workspace_b,)],
            [(repository_a, workspace_a), (repository_b, workspace_b)],
        ]
    )

    resolution = resolve_github_webhook_routes(
        _pool(conn), github_installation_id=123, github_repository_id=456
    )

    assert resolution.resolution is GitHubWebhookRoutingResolution.RESOLVED
    assert [(route.workspace_id, route.repository_id) for route in resolution.routes] == [
        (workspace_a, repository_a),
        (workspace_b, repository_b),
    ]
    route_sql, route_params = conn.executed[1]
    assert "join openorc.github_installations gi" in route_sql
    assert "gi.id = r.github_installation_id" in route_sql
    assert "gi.workspace_id = r.workspace_id" in route_sql
    assert "r.github_repository_id = %s" in route_sql
    assert "gi.github_installation_id = %s" in route_sql
    assert route_params == (456, 123)


def test_no_exact_match_with_a_configured_repository_classifies_route_mismatch() -> None:
    conn = ScriptedConnection(results=[[(uuid.uuid4(),)], [], [(1,)]])

    resolution = resolve_github_webhook_routes(
        _pool(conn), github_installation_id=123, github_repository_id=456
    )

    assert resolution.resolution is GitHubWebhookRoutingResolution.ROUTE_MISMATCH
    assert resolution.routes == ()
    configured_sql, params = conn.executed[2]
    assert "from openorc.repositories where github_repository_id = %s" in configured_sql
    assert params == (456,)


def test_no_exact_match_without_a_configured_repository_classifies_unconfigured() -> None:
    conn = ScriptedConnection(results=[[(uuid.uuid4(),)], [], None])

    resolution = resolve_github_webhook_routes(
        _pool(conn), github_installation_id=123, github_repository_id=456
    )

    assert resolution.resolution is GitHubWebhookRoutingResolution.UNCONFIGURED_REPOSITORY
    assert resolution.routes == ()
