"""Repositories for the GitHub issue relationship mirrors (issue #60).

Explicit SQL repositories over the ``openorc`` schema for the
presentation-only hierarchy/dependency mirrors of one Workspace Repository,
independent from the Task aggregate. Rows map to transport-independent domain
objects from :mod:`openorc.domain.github_issue_relations`.

Synchronization semantics (deterministic, idempotent, race-safe):

- Every replace makes the durable mirror EXACTLY match the fresh observation
  passed by the caller: absent rows are deleted, present endpoints inserted.
  An observation identical to current durable state performs no write at all
  and classifies as ``UNCHANGED`` (change-only event emission builds on
  this). The functions are safe for missed/duplicate/out-of-order
  notifications: a repeated observation converges on the same durable state.
- An authoritative EMPTY observation is a first-class outcome: it deletes the
  prior rows (clearing a parent edge, or recording that GitHub currently
  reports no blocked-by dependency) rather than stamping anything. The
  mirrors carry no freshness columns — they are, by definition, the last
  successfully observed projection.
- Related endpoints are stored as plain stable numeric GitHub facts with no
  foreign key to :mod:`openorc.persistence.ownership` repositories: a
  related repository need not be configured as an OpenOrc Repository in this
  Workspace, and mutable owner/name data is never identity.
- Durable identity and Workspace scope are immutable on every update path.

Violated database invariants surface as driver exceptions; translating them
into typed application errors is a service-layer concern, not a persistence
one.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Any
from uuid import UUID

from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction

__all__ = [
    "GitHubRelationReplaceOutcome",
    "find_github_issue_parent_edge",
    "find_github_issue_sub_issue_edges",
    "list_github_issue_blocked_by_edges",
    "replace_github_issue_dependency_edges",
    "replace_github_issue_parent_edge",
    "replace_github_issue_sub_issue_edges",
]


class GitHubRelationReplaceOutcome(Enum):
    """The classified durable outcome of one mirror replacement.

    ``CHANGED`` — this invocation durably advanced the mirror (created,
    updated, deleted, or reshaped at least one row). ``UNCHANGED`` — the
    incoming observation was identical to current durable state and no write
    occurred at all (change-only event emission classifies on this).
    """

    CHANGED = "changed"
    UNCHANGED = "unchanged"


def _subject_params(
    workspace_id: UUID, repository_id: UUID, github_issue_id: int
) -> tuple[UUID, UUID, int]:
    return (workspace_id, repository_id, github_issue_id)


def _endpoint_set(values: Sequence[RelatedIssueEndpoint]) -> set[tuple[int, int]]:
    return {(value.github_repository_id, value.github_issue_id) for value in values}


def replace_github_issue_parent_edge(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    parent: RelatedIssueEndpoint | None,
) -> GitHubRelationReplaceOutcome:
    """Make the subject's parent edge exactly match one authoritative fact.

    ``parent`` present replaces/inserts the single parent-edge row;
    ``parent`` is ``None`` for an authoritative no-parent observation and
    deletes any prior edge. An identical current row performs no write.
    """
    params = _subject_params(workspace_id, repository_id, github_issue_id)
    with transaction(pool) as conn:
        row = conn.execute(
            "select parent_github_repository_id, parent_github_issue_id "
            "from openorc.github_issue_hierarchy "
            "where workspace_id = %s and repository_id = %s and github_issue_id = %s "
            "for update",
            params,
        ).fetchone()
        if parent is None:
            if row is None:
                return GitHubRelationReplaceOutcome.UNCHANGED
            conn.execute(
                "delete from openorc.github_issue_hierarchy "
                "where workspace_id = %s and repository_id = %s and github_issue_id = %s",
                params,
            )
            return GitHubRelationReplaceOutcome.CHANGED
        desired = (parent.github_repository_id, parent.github_issue_id)
        if row is not None and (row[0], row[1]) == desired:
            return GitHubRelationReplaceOutcome.UNCHANGED
        conn.execute(
            "delete from openorc.github_issue_hierarchy "
            "where workspace_id = %s and repository_id = %s and github_issue_id = %s",
            params,
        )
        conn.execute(
            "insert into openorc.github_issue_hierarchy "
            "(workspace_id, repository_id, github_issue_id, "
            "parent_github_repository_id, parent_github_issue_id) "
            "values (%s, %s, %s, %s, %s)",
            (*params, desired[0], desired[1]),
        )
        return GitHubRelationReplaceOutcome.CHANGED


def _replace_endpoint_edges(
    conn: Any,
    *,
    table: str,
    endpoint_columns: tuple[str, str],
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    endpoints: Sequence[RelatedIssueEndpoint],
) -> GitHubRelationReplaceOutcome:
    """Make one endpoint-edge mirror exactly match the observed set.

    The set comparison happens before any write: an identical observation is
    a true durable no-op; a different observation replaces the prior set
    wholesale (including the empty set) inside the caller's transaction.
    """
    params = _subject_params(workspace_id, repository_id, github_issue_id)
    where = (
        "where workspace_id = %s and repository_id = %s and github_issue_id = %s "
        f"order by {endpoint_columns[0]}, {endpoint_columns[1]}"
    )
    rows = conn.execute(
        f"select {endpoint_columns[0]}, {endpoint_columns[1]} from openorc.{table} {where}",
        params,
    ).fetchall()
    current = {(row[0], row[1]) for row in rows}
    desired = _endpoint_set(endpoints)
    if current == desired:
        return GitHubRelationReplaceOutcome.UNCHANGED
    conn.execute(
        f"delete from openorc.{table} "
        "where workspace_id = %s and repository_id = %s and github_issue_id = %s",
        params,
    )
    for endpoint in sorted(desired):
        conn.execute(
            f"insert into openorc.{table} "
            "(workspace_id, repository_id, github_issue_id, "
            f"{endpoint_columns[0]}, {endpoint_columns[1]}) values (%s, %s, %s, %s, %s)",
            (*params, endpoint[0], endpoint[1]),
        )
    return GitHubRelationReplaceOutcome.CHANGED


def replace_github_issue_sub_issue_edges(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    children: Sequence[RelatedIssueEndpoint],
) -> GitHubRelationReplaceOutcome:
    """Make the subject's sub-issue mirror exactly match one authoritative listing.

    An empty authoritative listing deletes the prior child rows: the empty
    set is a first-class durable state, never a sentinel row.
    """
    with transaction(pool) as conn:
        return _replace_endpoint_edges(
            conn,
            table="github_issue_sub_issues",
            endpoint_columns=("child_github_repository_id", "child_github_issue_id"),
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            endpoints=children,
        )


def replace_github_issue_dependency_edges(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    blockers: Sequence[RelatedIssueEndpoint],
) -> GitHubRelationReplaceOutcome:
    """Make the subject's blocked-by mirror exactly match one authoritative listing.

    An empty authoritative listing deletes the prior blocker rows: GitHub
    currently reporting no blocked-by dependency is the durable not-blocked
    state, derived from the (now empty) edge set.
    """
    with transaction(pool) as conn:
        return _replace_endpoint_edges(
            conn,
            table="github_issue_dependencies",
            endpoint_columns=("blocker_github_repository_id", "blocker_github_issue_id"),
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            endpoints=blockers,
        )


def find_github_issue_parent_edge(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> RelatedIssueEndpoint | None:
    """Return the subject's current parent endpoint, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            "select parent_github_repository_id, parent_github_issue_id "
            "from openorc.github_issue_hierarchy "
            "where repository_id = %s and github_issue_id = %s",
            (repository_id, github_issue_id),
        ).fetchone()
    if row is None:
        return None
    return RelatedIssueEndpoint(github_repository_id=row[0], github_issue_id=row[1])


def find_github_issue_sub_issue_edges(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> list[RelatedIssueEndpoint]:
    """Return the subject's current observed child edges, deterministically ordered."""
    with transaction(pool) as conn:
        rows = conn.execute(
            "select child_github_repository_id, child_github_issue_id "
            "from openorc.github_issue_sub_issues "
            "where repository_id = %s and github_issue_id = %s "
            "order by child_github_repository_id, child_github_issue_id",
            (repository_id, github_issue_id),
        ).fetchall()
    return [
        RelatedIssueEndpoint(github_repository_id=row[0], github_issue_id=row[1]) for row in rows
    ]


def list_github_issue_blocked_by_edges(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> list[RelatedIssueEndpoint]:
    """Return the subject's current blocked-by edges, deterministically ordered.

    An empty result is the durable not-blocked state of the last successful
    observation; the caller re-reads fresh authoritative state before any
    consequential intake decision.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            "select blocker_github_repository_id, blocker_github_issue_id "
            "from openorc.github_issue_dependencies "
            "where repository_id = %s and github_issue_id = %s "
            "order by blocker_github_repository_id, blocker_github_issue_id",
            (repository_id, github_issue_id),
        ).fetchall()
    return [
        RelatedIssueEndpoint(github_repository_id=row[0], github_issue_id=row[1]) for row in rows
    ]
