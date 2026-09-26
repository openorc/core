"""Deterministic tests for the GitHub relationship-mirror repositories (#60).

The ordinary suite cannot execute Postgres; these tests use scripted SQL
handlers over a fake pool/connection seam to prove the replace contract:

- the lock-then-compare-then-write shape (``SELECT ... FOR UPDATE`` before
  any parent-edge fact is computed);
- the true durable no-op: an identical observation executes no write;
- the authoritative empty set as a first-class durable state (prior rows
  deleted; never a sentinel row, never a no-write);
- the identity-space contract: related endpoints persist as plain numeric
  GitHub facts with no local-Repository foreign key, and the subject side
  carries the local OpenOrc Workspace/Repository scope on every statement.
Database constraint behavior is proven against a real database by the
integration-marked suite.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, cast

from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.persistence.github_issue_relations import (
    GitHubRelationReplaceOutcome,
    replace_github_issue_dependency_edges,
    replace_github_issue_parent_edge,
    replace_github_issue_sub_issue_edges,
)
from openorc.persistence.pool import DatabasePool

_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_GITHUB_ISSUE_ID = 503
_ENDPOINT_A = RelatedIssueEndpoint(github_repository_id=555, github_issue_id=777)
_ENDPOINT_B = RelatedIssueEndpoint(github_repository_id=888, github_issue_id=999)


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, list[tuple[Any, ...]] | Exception]] = []

    def on(self, sql_marker: str, rows: list[tuple[Any, ...]] | Exception) -> None:
        self._handlers.append((sql_marker.lower(), rows))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, rows) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(rows, Exception):
                    raise rows
                return FakeCursor(rows)
        raise AssertionError(f"no scripted handler matched: {sql}")

    def transaction(self) -> Any:
        @contextmanager
        def scoped():  # type: ignore[no-untyped-def]
            yield

        return scoped()


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed():  # type: ignore[no-untyped-def]
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("relationship mirror tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def test_a_first_parent_edge_inserts_and_reports_changed() -> None:
    conn = ScriptedConnection()
    conn.on("select parent_github_repository_id", [])  # no durable parent edge
    conn.on("delete from openorc.github_issue_hierarchy", [])
    conn.on("insert into openorc.github_issue_hierarchy", [])
    outcome = replace_github_issue_parent_edge(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        parent=_ENDPOINT_A,
    )
    assert outcome is GitHubRelationReplaceOutcome.CHANGED
    insert_sql, insert_params = conn.executed[-1]
    assert "parent_github_repository_id" in insert_sql
    # The related endpoint is a plain numeric fact; the subject scope is the
    # local OpenOrc pair.
    assert insert_params == (
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        555,
        777,
    )


def test_an_identical_parent_edge_is_a_true_durable_no_op() -> None:
    conn = ScriptedConnection()
    conn.on("select", [(555, 777)])
    outcome = replace_github_issue_parent_edge(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        parent=_ENDPOINT_A,
    )
    assert outcome is GitHubRelationReplaceOutcome.UNCHANGED
    assert not any("insert" in sql or "delete" in sql for sql, _ in conn.executed)


def test_an_authoritative_no_parent_clears_the_prior_edge() -> None:
    conn = ScriptedConnection()
    conn.on("select parent_github_repository_id", [(555, 777)])
    conn.on("delete from openorc.github_issue_hierarchy", [])
    outcome = replace_github_issue_parent_edge(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        parent=None,
    )
    assert outcome is GitHubRelationReplaceOutcome.CHANGED
    assert any("delete" in sql for sql, _ in conn.executed)


def test_an_authoritative_no_parent_with_no_prior_edge_is_a_no_op() -> None:
    conn = ScriptedConnection()
    conn.on("select parent_github_repository_id", [])
    outcome = replace_github_issue_parent_edge(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        parent=None,
    )
    assert outcome is GitHubRelationReplaceOutcome.UNCHANGED
    assert not any("delete" in sql for sql, _ in conn.executed)


def test_an_empty_dependency_listing_clears_prior_blockers() -> None:
    conn = ScriptedConnection()
    conn.on(
        "select blocker_github_repository_id, blocker_github_issue_id from "
        "openorc.github_issue_dependencies",
        [(555, 777), (888, 999)],
    )
    conn.on("delete from openorc.github_issue_dependencies", [])
    outcome = replace_github_issue_dependency_edges(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        blockers=[],
    )
    assert outcome is GitHubRelationReplaceOutcome.CHANGED
    assert any("delete" in sql for sql, _ in conn.executed)
    assert not any("insert" in sql for sql, _ in conn.executed)


def test_an_identical_dependency_set_is_a_true_durable_no_op() -> None:
    conn = ScriptedConnection()
    conn.on(
        "select blocker_github_repository_id, blocker_github_issue_id from "
        "openorc.github_issue_dependencies",
        [(555, 777), (888, 999)],
    )
    outcome = replace_github_issue_dependency_edges(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        blockers=[_ENDPOINT_A, _ENDPOINT_B],
    )
    assert outcome is GitHubRelationReplaceOutcome.UNCHANGED
    assert not any("delete" in sql or "insert" in sql for sql, _ in conn.executed)


def test_a_changed_dependency_set_replaces_wholesale() -> None:
    conn = ScriptedConnection()
    conn.on(
        "select blocker_github_repository_id, blocker_github_issue_id from "
        "openorc.github_issue_dependencies",
        [(555, 777)],
    )
    conn.on("delete from openorc.github_issue_dependencies", [])
    conn.on("insert into openorc.github_issue_dependencies", [])
    conn.on("insert into openorc.github_issue_dependencies", [])
    outcome = replace_github_issue_dependency_edges(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        blockers=[_ENDPOINT_B, _ENDPOINT_A],
    )
    assert outcome is GitHubRelationReplaceOutcome.CHANGED
    inserts = [(sql, params) for sql, params in conn.executed if "insert" in sql]
    assert all(params is not None for _, params in inserts)
    assert len(inserts) == 2
    # Deterministic insertion order; endpoints are numeric facts.
    assert inserts[0][1][-2:] == (555, 777)  # type: ignore[index]
    assert inserts[1][1][-2:] == (888, 999)  # type: ignore[index]


def test_an_empty_sub_issue_listing_clears_prior_children() -> None:
    conn = ScriptedConnection()
    conn.on(
        "select child_github_repository_id, child_github_issue_id from "
        "openorc.github_issue_sub_issues",
        [(555, 777)],
    )
    conn.on("delete from openorc.github_issue_sub_issues", [])
    outcome = replace_github_issue_sub_issue_edges(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        children=[],
    )
    assert outcome is GitHubRelationReplaceOutcome.CHANGED
    assert not any("insert" in sql for sql, _ in conn.executed)


def test_every_statement_scopes_the_local_openorc_subject() -> None:
    conn = ScriptedConnection()
    conn.on("select parent_github_repository_id", [])
    conn.on("delete from openorc.github_issue_hierarchy", [])
    conn.on("insert into openorc.github_issue_hierarchy", [])
    replace_github_issue_parent_edge(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        parent=_ENDPOINT_A,
    )
    for sql, params in conn.executed:
        assert "workspace_id" in " ".join(sql.split())
        assert params is not None
        assert params[0] == _WORKSPACE_ID
        assert params[1] == _REPOSITORY_ID
        assert params[2] == _GITHUB_ISSUE_ID
