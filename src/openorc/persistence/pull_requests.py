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

Reconciliation (issue #63): :func:`reconcile_task_pull_request_observed` is
the serialized, strictly **update-only** observed-snapshot write. The
existing row is locked (``SELECT ... FOR UPDATE``) under its exact
Workspace/Task/record scope, the incoming observation is compared against
the locked pre-image, and only an actually-different snapshot is written;
the serialized pre-image is returned so head-change/base-change facts are
computed from durable state, never a racy re-read. There is deliberately no
insert path: an absent row or mismatched scope is the ``MISSING`` outcome —
the canonical record's only creator is the later race-safe PR publication
operation, and no external-PR adoption path exists.

Violated database invariants (uniqueness, foreign keys, CHECK constraints)
surface as driver exceptions (for example ``psycopg.errors.UniqueViolation``
and ``ForeignKeyViolation``); translating driver exceptions into typed
application errors is a service-layer concern, not a persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
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
    "TaskPullRequestReconcileOutcome",
    "TaskPullRequestReconcileResult",
    "create_task_pull_request",
    "find_task_pull_request_by_github_identity",
    "find_task_pull_request_by_number",
    "get_task_pull_request",
    "get_task_pull_request_for_task",
    "list_task_pull_requests_for_repository",
    "reconcile_task_pull_request_observed",
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


def find_task_pull_request_by_number(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID, github_pr_number: int
) -> TaskPullRequest | None:
    """Find the Workspace Repository's canonical record for one PR number.

    Webhook dispatch (#120) addresses canonical PRs by the repository-local
    ``github_pr_number`` the delivery carried — mutable address metadata,
    never identity: reconciliation (``reconcile_task_pull_request``) binds to
    the record's stable ``github_pr_id`` and fails closed when the number
    reports a different stable identity. The record carries its own
    ``repository_id``, so no join is needed; the lookup is deterministic and
    single-row by construction.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(repository_id, "repository_id")
    _require_positive_int(github_pr_number, "github_pr_number")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where workspace_id = %s and repository_id = %s and github_pr_number = %s "
            "order by task_id limit 1",
            (workspace_id, repository_id, github_pr_number),
        ).fetchone()
    return None if row is None else _task_pull_request_from_row(row)


def list_task_pull_requests_for_repository(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> list[TaskPullRequest]:
    """List the canonical PR records of one Workspace Repository, deterministically ordered.

    Repository-scoped webhook fan-out (#120): push/check notifications carry
    no pull-request identity, so dispatch re-reads the fresh authoritative
    state of every canonical record in the routed repository.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(repository_id, "repository_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where workspace_id = %s and repository_id = %s order by task_id",
            (workspace_id, repository_id),
        ).fetchall()
    return [_task_pull_request_from_row(row) for row in rows]


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


class TaskPullRequestReconcileOutcome(Enum):
    """The classified durable outcome of one observed-snapshot reconciliation.

    Strictly update-only (issue #63): reconciliation addresses the one
    already-created canonical record and never inserts or replaces it — the
    canonical TaskPullRequest's only creator is the later race-safe PR
    publication operation, and there is no external-PR adoption path.

    - ``UPDATED`` — this invocation durably advanced the existing record's
      observed snapshot; the serialized pre-image describes what changed.
    - ``UNCHANGED`` — the observation matched current durable state exactly;
      no write was performed and ``updated_at`` is preserved.
    - ``MISSING`` — the addressed canonical record (or its Workspace/Task
      scope) does not exist durably; nothing was written.
    """

    UPDATED = "updated"
    UNCHANGED = "unchanged"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class TaskPullRequestReconcileResult:
    """The serialized before→current facts of one observed-snapshot reconcile.

    ``previous_*`` is the pre-image locked under ``SELECT ... FOR UPDATE``
    before any write, so the head-change/base-change/state-change facts a
    caller computes are computed from durable serialized state, never from a
    racy re-read. ``pull_request`` is the post-write record (``None`` only
    for ``MISSING``).
    """

    outcome: TaskPullRequestReconcileOutcome
    pull_request: TaskPullRequest | None
    previous_head_ref: str | None
    previous_base_ref: str | None
    previous_head_sha: str | None
    previous_state: TaskPullRequestState | None
    previous_merged_at: datetime | None


def reconcile_task_pull_request_observed(
    pool: DatabasePool,
    *,
    task_pull_request_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    head_ref: str,
    base_ref: str,
    head_sha: str,
    state: TaskPullRequestState,
    merged_at: datetime | None,
) -> TaskPullRequestReconcileResult:
    """Reconcile the one canonical record's observed snapshot, update-only.

    The serialized write: the existing row is locked (``SELECT ... FOR
    UPDATE``) addressed by its exact Workspace/Task/record scope, the
    incoming observation is compared against the locked pre-image, and an
    actually-different snapshot is updated in place as one whole snapshot
    with ``updated_at`` advancing. An identical observation is a true durable
    no-op (``updated_at`` preserved). There is deliberately no insert path:
    an absent row — or a scope that no longer matches — is the ``MISSING``
    outcome, and the caller applies the typed application-error semantics.

    The payload is validated here (mirroring the domain and the database
    CHECK) before any SQL runs.
    """
    _require_uuid(task_pull_request_id, "id")
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_nonblank_str(head_ref, "head_ref")
    _require_nonblank_str(base_ref, "base_ref")
    _require_nonblank_str(head_sha, "head_sha")
    if not isinstance(state, TaskPullRequestState):
        raise TaskPullRequestDomainError(
            "reconcile_task_pull_request_observed requires a TaskPullRequestState"
        )
    if merged_at is not None and state is not TaskPullRequestState.CLOSED:
        raise TaskPullRequestDomainError(
            "a merged TaskPullRequest is closed: merged_at requires the closed state"
        )
    with transaction(pool) as conn:
        current_row = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where id = %s and workspace_id = %s and task_id = %s for update",
            (task_pull_request_id, workspace_id, task_id),
        ).fetchone()
        if current_row is None:
            return TaskPullRequestReconcileResult(
                outcome=TaskPullRequestReconcileOutcome.MISSING,
                pull_request=None,
                previous_head_ref=None,
                previous_base_ref=None,
                previous_head_sha=None,
                previous_state=None,
                previous_merged_at=None,
            )
        current = _task_pull_request_from_row(current_row)
        if (
            current.head_ref == head_ref
            and current.base_ref == base_ref
            and current.head_sha == head_sha
            and current.state == state
            and current.merged_at == merged_at
        ):
            return TaskPullRequestReconcileResult(
                outcome=TaskPullRequestReconcileOutcome.UNCHANGED,
                pull_request=current,
                previous_head_ref=current.head_ref,
                previous_base_ref=current.base_ref,
                previous_head_sha=current.head_sha,
                previous_state=current.state,
                previous_merged_at=current.merged_at,
            )
        row = conn.execute(
            "update openorc.task_pull_requests "
            "set head_ref = %s, base_ref = %s, head_sha = %s, state = %s, "
            "merged_at = %s, updated_at = now() "
            "where id = %s "
            f"returning {_TASK_PULL_REQUEST_COLUMNS}",
            (head_ref, base_ref, head_sha, state.value, merged_at, task_pull_request_id),
        ).fetchone()
        assert row is not None  # the FOR UPDATE lock excludes concurrent deletion
        return TaskPullRequestReconcileResult(
            outcome=TaskPullRequestReconcileOutcome.UPDATED,
            pull_request=_task_pull_request_from_row(row),
            previous_head_ref=current.head_ref,
            previous_base_ref=current.base_ref,
            previous_head_sha=current.head_sha,
            previous_state=current.state,
            previous_merged_at=current.merged_at,
        )
