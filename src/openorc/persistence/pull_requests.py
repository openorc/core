"""Repositories for TaskPullRequest persistence.

Explicit SQL repositories over the ``openorc`` schema for the one canonical
pull request record per Task (Phase 1, issue #25). Rows map to
transport-independent domain objects from
:mod:`openorc.domain.pull_requests`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Identity and reconciliation: ``github_pr_id`` is the stable external
GitHub PR identity, persisted separately from the repository-local
``github_pr_number`` address metadata. ``unique (task_id)`` is full-history
— one Task has exactly one TaskPullRequest for its whole v1 lifetime, with
no replacement-PR rows; a closed-unmerged PR remains the canonical record
and later workflow services block against it rather than replacing it.
``unique (workspace_id, github_pr_id)`` canonicalizes one PR record per
GitHub PR identity per Workspace and doubles as the reconciliation lookup.

Observed reconciliation state — ``head_ref``, ``base_ref``, ``head_sha``,
``state``, ``merged_at`` — is updated in place by
:func:`update_task_pull_request_observed` as a full observed snapshot with
``updated_at`` advancing. The current observed ``head_sha`` changes across
remediation rounds while the PR identity stays stable; the exact reviewed
head SHAs are historical facts on the review records, never rewritten
here. ``github_pr_number`` is creation-time address metadata and is not
part of the observed snapshot (mutable display fields are never sole
identity).

Violated database invariants (uniqueness, foreign keys, CHECK constraints)
surface as driver exceptions (for example ``psycopg.errors.UniqueViolation``
and ``ForeignKeyViolation``); translating driver exceptions into typed
application errors is a service-layer concern, not a persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from openorc.domain.pull_requests import (
    TaskPullRequest,
    TaskPullRequestDomainError,
    TaskPullRequestState,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_task_pull_request",
    "find_task_pull_request_by_github_identity",
    "get_task_pull_request",
    "get_task_pull_request_for_task",
    "update_task_pull_request_observed",
]

_TASK_PULL_REQUEST_COLUMNS = (
    "id, workspace_id, task_id, repository_id, github_pr_id, github_pr_number, "
    "head_ref, base_ref, head_sha, state, merged_at, created_at, updated_at"
)


def _task_pull_request_from_row(row: Sequence[Any]) -> TaskPullRequest:
    merged_at = row[10]
    return TaskPullRequest(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        repository_id=row[3],
        github_pr_id=row[4],
        github_pr_number=row[5],
        head_ref=row[6],
        base_ref=row[7],
        head_sha=row[8],
        state=TaskPullRequestState(row[9]),
        merged_at=None if merged_at is None else normalize_utc(merged_at),
        created_at=normalize_utc(row[11]),
        updated_at=normalize_utc(row[12]),
    )


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a positive integer")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a non-empty string")


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a UUID")


def create_task_pull_request(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    repository_id: UUID,
    github_pr_id: int,
    github_pr_number: int,
    head_ref: str,
    base_ref: str,
    head_sha: str,
) -> TaskPullRequest:
    """Insert the canonical TaskPullRequest record for a Task.

    The record is created in the observed ``open`` state with no merge
    stamp (inserted explicitly — the migration declares no lifecycle
    default); reconciliation updates the observed state afterwards through
    :func:`update_task_pull_request_observed`. The database constraints are
    the durable backstops: ``unique (task_id)`` keeps exactly one PR per
    Task for the whole v1 lifetime (a second record for the same Task —
    including after the first closes unmerged — raises ``UniqueViolation``);
    ``unique (workspace_id, github_pr_id)`` keeps one canonical record per
    GitHub PR identity per Workspace; and the composite foreign keys keep
    Task/Repository/Workspace ownership in agreement
    (``ForeignKeyViolation``).
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(repository_id, "repository_id")
    _require_positive_int(github_pr_id, "github_pr_id")
    _require_positive_int(github_pr_number, "github_pr_number")
    _require_nonblank_str(head_ref, "head_ref")
    _require_nonblank_str(base_ref, "base_ref")
    _require_nonblank_str(head_sha, "head_sha")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_pull_requests "
            "(workspace_id, task_id, repository_id, github_pr_id, github_pr_number, "
            "head_ref, base_ref, head_sha, state) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_TASK_PULL_REQUEST_COLUMNS}",
            (
                workspace_id,
                task_id,
                repository_id,
                github_pr_id,
                github_pr_number,
                head_ref,
                base_ref,
                head_sha,
                TaskPullRequestState.OPEN.value,
            ),
        ).fetchone()
    assert row is not None
    return _task_pull_request_from_row(row)


def get_task_pull_request(
    pool: DatabasePool, *, task_pull_request_id: UUID
) -> TaskPullRequest | None:
    """Return one TaskPullRequest by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests where id = %s",
            (task_pull_request_id,),
        ).fetchone()
    return None if row is None else _task_pull_request_from_row(row)


def get_task_pull_request_for_task(pool: DatabasePool, *, task_id: UUID) -> TaskPullRequest | None:
    """Return the Task's canonical PR record, or ``None`` when none exists.

    ``unique (task_id)`` guarantees at most one record; this is the
    per-Task lookup and the closed-unmerged canonical record stays
    reachable through it.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where task_id = %s",
            (task_id,),
        ).fetchone()
    return None if row is None else _task_pull_request_from_row(row)


def find_task_pull_request_by_github_identity(
    pool: DatabasePool, *, workspace_id: UUID, github_pr_id: int
) -> TaskPullRequest | None:
    """Find a Workspace's canonical record for one GitHub PR identity.

    ``github_pr_id`` is the stable external identity used for
    reconciliation (``github_pr_number`` is address metadata and never the
    lookup key); ``unique (workspace_id, github_pr_id)`` guarantees at most
    one canonical record per GitHub PR identity per Workspace.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_positive_int(github_pr_id, "github_pr_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where workspace_id = %s and github_pr_id = %s",
            (workspace_id, github_pr_id),
        ).fetchone()
    return None if row is None else _task_pull_request_from_row(row)


def update_task_pull_request_observed(
    pool: DatabasePool,
    *,
    task_pull_request_id: UUID,
    head_ref: str,
    base_ref: str,
    head_sha: str,
    state: TaskPullRequestState,
    merged_at: datetime | None,
) -> TaskPullRequest | None:
    """Replace the mutable observed reconciliation state as one snapshot.

    Writes the full observed snapshot — ``head_ref``, ``base_ref``, the
    current ``head_sha``, the observed ``state``, and the observed
    ``merged_at`` — advancing ``updated_at``. Reconciliation semantics: the
    caller presents what GitHub currently reports for the PR, and the
    record stores it as observed state. ``head_sha`` is the PR's current
    observed head and legitimately changes across remediation rounds while
    the PR identity stays stable; ``github_pr_number`` is deliberately not
    part of the snapshot (creation-time address metadata, never mutable
    identity). A merged snapshot requires the closed state (the domain and
    the database CHECK agree); an ``open`` observation carries
    ``merged_at=None``.

    The payload is validated here (mirroring the domain and the database
    CHECK) so a transaction cannot commit an observed snapshot the domain
    rejects. Returns the updated record, or ``None`` when the record is
    missing.
    """
    _require_uuid(task_pull_request_id, "id")
    _require_nonblank_str(head_ref, "head_ref")
    _require_nonblank_str(base_ref, "base_ref")
    _require_nonblank_str(head_sha, "head_sha")
    if not isinstance(state, TaskPullRequestState):
        raise TaskPullRequestDomainError(
            "update_task_pull_request_observed requires a TaskPullRequestState"
        )
    if merged_at is not None and state is not TaskPullRequestState.CLOSED:
        raise TaskPullRequestDomainError(
            "a merged TaskPullRequest is closed: merged_at requires the closed state"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_pull_requests "
            "set head_ref = %s, base_ref = %s, head_sha = %s, state = %s, "
            "merged_at = %s, updated_at = now() "
            "where id = %s "
            f"returning {_TASK_PULL_REQUEST_COLUMNS}",
            (head_ref, base_ref, head_sha, state.value, merged_at, task_pull_request_id),
        ).fetchone()
    return None if row is None else _task_pull_request_from_row(row)
