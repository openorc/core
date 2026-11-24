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
    "get_task_pull_request_for_task_for_update",
    "get_task_pull_requests_for_tasks",
    "list_task_pull_requests_for_repository",
    "reconcile_task_pull_request_observed",
    "update_task_pull_request_observed",
]

_TASK_PULL_REQUEST_COLUMNS = (
    "id, workspace_id, task_id, repository_id, github_pr_id, github_pr_number, "
    "head_ref, base_ref, head_sha, state, merged_at, created_at, updated_at, "
    "title, body, html_url, user_id, user_login, draft, "
    "provider_created_at, provider_updated_at, provider_closed_at, base_sha, "
    "mergeable, merge_state_status, pr_mergeability_observed_at, pr_mergeability_head_sha"
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
        title=row[13],
        body=row[14],
        html_url=row[15],
        user_id=row[16],
        user_login=row[17],
        draft=row[18],
        provider_created_at=None if row[19] is None else normalize_utc(row[19]),
        provider_updated_at=None if row[20] is None else normalize_utc(row[20]),
        provider_closed_at=None if row[21] is None else normalize_utc(row[21]),
        base_sha=row[22],
        mergeable=row[23],
        merge_state_status=row[24],
        pr_mergeability_observed_at=(None if row[25] is None else normalize_utc(row[25])),
        pr_mergeability_head_sha=row[26],
    )


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a positive integer")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise TaskPullRequestDomainError(f"TaskPullRequest.{name} must be a non-empty string")


def _validate_observed_facts(
    *,
    title: str | None,
    html_url: str | None,
    user_id: int | None,
    user_login: str | None,
    draft: bool | None,
    base_sha: str | None,
    mergeable: bool | None,
    merge_state_status: str | None,
) -> None:
    """Validate the #376 observed presentation facts before any SQL runs.

    Mirrors the domain's nullable coherence: the user subset is present or
    absent together, and every optional fact is nonblank-or-none. ``body`` is
    GitHub's documented nullable verbatim fact and carries no nonblank rule.
    """
    if title is not None:
        _require_nonblank_str(title, "title")
    if html_url is not None:
        _require_nonblank_str(html_url, "html_url")
    if user_login is not None:
        _require_nonblank_str(user_login, "user_login")
    if base_sha is not None:
        _require_nonblank_str(base_sha, "base_sha")
    if merge_state_status is not None:
        _require_nonblank_str(merge_state_status, "merge_state_status")
    if (user_id is None) != (user_login is None):
        raise TaskPullRequestDomainError("the observed user subset is present or absent together")
    if user_id is not None:
        _require_positive_int(user_id, "user_id")
    if draft is not None and not isinstance(draft, bool):
        raise TaskPullRequestDomainError("the observed draft flag must be None or a boolean")
    if mergeable is not None and not isinstance(mergeable, bool):
        raise TaskPullRequestDomainError("the observed mergeability must be None or a boolean")


# The observed-snapshot SET fragment shared by the #376-extended write paths:
# the core observed facts, then the presentation facts, then the head-bound
# mergeability observation (stamped only when the observation reports a
# mergeability reading — a no-reading write never touches the retained pair).
_OBSERVED_SNAPSHOT_SET_COLUMNS = (
    "head_ref",
    "base_ref",
    "head_sha",
    "state",
    "merged_at",
    "title",
    "body",
    "html_url",
    "user_id",
    "user_login",
    "draft",
    "provider_created_at",
    "provider_updated_at",
    "provider_closed_at",
    "base_sha",
)


# The SET fragments of one observed-snapshot write, literal per variant so the
# composed statements stay provably literal SQL (the driver's query typing).
# The mergeability variant: a reported reading — including GitHub's documented
# not-yet-determined null/`unknown` — replaces the retained pair and binds it
# to the exact observed head with a fresh observation instant. A stale result
# is never presented as the new head's observation.
_OBSERVED_SNAPSHOT_WRITE_SQL = ", ".join(
    (
        "head_ref = %s",
        "base_ref = %s",
        "head_sha = %s",
        "state = %s",
        "merged_at = %s",
        "title = %s",
        "body = %s",
        "html_url = %s",
        "user_id = %s",
        "user_login = %s",
        "draft = %s",
        "provider_created_at = %s",
        "provider_updated_at = %s",
        "provider_closed_at = %s",
        "base_sha = %s",
        "updated_at = now()",
    )
)
# The literal insert statement fragments of one canonical-PR publication row:
# the base 21-column projection plus the optional head-bound mergeability
# observation pair, stamped exactly at the created head.
_INSERT_COLUMNS = (
    "workspace_id, task_id, repository_id, github_pr_id, github_pr_number, "
    "head_ref, base_ref, head_sha, state, title, body, html_url, user_id, "
    "user_login, draft, provider_created_at, provider_updated_at, "
    "provider_closed_at, base_sha, mergeable, merge_state_status"
)
_INSERT_VALUES = "%s, " * 20 + "%s"
_INSERT_MERGEABILITY_COLUMNS = ", pr_mergeability_observed_at, pr_mergeability_head_sha"
_INSERT_MERGEABILITY_VALUES = ", now(), %s"


_OBSERVED_SNAPSHOT_WRITE_SQL_MERGEABILITY = ", ".join(
    (
        "head_ref = %s",
        "base_ref = %s",
        "head_sha = %s",
        "state = %s",
        "merged_at = %s",
        "title = %s",
        "body = %s",
        "html_url = %s",
        "user_id = %s",
        "user_login = %s",
        "draft = %s",
        "provider_created_at = %s",
        "provider_updated_at = %s",
        "provider_closed_at = %s",
        "base_sha = %s",
        "mergeable = %s",
        "merge_state_status = %s",
        "pr_mergeability_observed_at = now()",
        "pr_mergeability_head_sha = head_sha",
        "updated_at = now()",
    )
)


def _observed_snapshot_write_params(
    *,
    head_ref: str,
    base_ref: str,
    head_sha: str,
    state: TaskPullRequestState,
    merged_at: datetime | None,
    title: str | None,
    body: str | None,
    html_url: str | None,
    user_id: int | None,
    user_login: str | None,
    draft: bool | None,
    provider_created_at: datetime | None,
    provider_updated_at: datetime | None,
    provider_closed_at: datetime | None,
    base_sha: str | None,
    mergeable: bool | None,
    merge_state_status: str | None,
) -> list[Any]:
    """Return the write parameters matching :func:`_observed_snapshot_write_sql`."""
    params: list[Any] = [
        head_ref,
        base_ref,
        head_sha,
        state.value,
        merged_at,
        title,
        body,
        html_url,
        user_id,
        user_login,
        draft,
        provider_created_at,
        provider_updated_at,
        provider_closed_at,
        base_sha,
    ]
    if mergeable is not None or merge_state_status is not None:
        params.extend([mergeable, merge_state_status])
    return params


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
    # The #376 observed presentation facts from the same authoritative PR
    # observation (issue #376): all optional — absent facts stay NULL until a
    # successful reconciliation observes them. When the observation reports a
    # mergeability reading, the head-bound observation pair is stamped
    # atomically with creation.
    title: str | None = None,
    body: str | None = None,
    html_url: str | None = None,
    user_id: int | None = None,
    user_login: str | None = None,
    draft: bool | None = None,
    provider_created_at: datetime | None = None,
    provider_updated_at: datetime | None = None,
    provider_closed_at: datetime | None = None,
    base_sha: str | None = None,
    mergeable: bool | None = None,
    merge_state_status: str | None = None,
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

    The optional #376 presentation facts carry exactly what the same
    authoritative observation reported (never fabricated); when it reports a
    mergeability reading, ``pr_mergeability_observed_at``/``pr_mergeability_head_sha``
    stamp the observation to the exact created head in the same insert.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(repository_id, "repository_id")
    _require_positive_int(github_pr_id, "github_pr_id")
    _require_positive_int(github_pr_number, "github_pr_number")
    _require_nonblank_str(head_ref, "head_ref")
    _require_nonblank_str(base_ref, "base_ref")
    _require_nonblank_str(head_sha, "head_sha")
    _validate_observed_facts(
        title=title,
        html_url=html_url,
        user_id=user_id,
        user_login=user_login,
        draft=draft,
        base_sha=base_sha,
        mergeable=mergeable,
        merge_state_status=merge_state_status,
    )
    mergeability_reported = mergeable is not None or merge_state_status is not None
    values: list[Any] = [
        workspace_id,
        task_id,
        repository_id,
        github_pr_id,
        github_pr_number,
        head_ref,
        base_ref,
        head_sha,
        TaskPullRequestState.OPEN.value,
        title,
        body,
        html_url,
        user_id,
        user_login,
        draft,
        provider_created_at,
        provider_updated_at,
        provider_closed_at,
        base_sha,
        mergeable,
        merge_state_status,
    ]
    if mergeability_reported:
        # The head-bound observation pair stamps exactly the created head.
        values.append(head_sha)
    with transaction(pool) as conn:
        row = conn.execute(
            f"insert into openorc.task_pull_requests "
            f"({_INSERT_COLUMNS}"
            f"{_INSERT_MERGEABILITY_COLUMNS if mergeability_reported else ''}) "
            f"values ({_INSERT_VALUES}"
            f"{_INSERT_MERGEABILITY_VALUES if mergeability_reported else ''}) "
            f"returning {_TASK_PULL_REQUEST_COLUMNS}",
            tuple(values),
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


def get_task_pull_request_for_task_for_update(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskPullRequest | None:
    """Return the Task's canonical PR record locked ``FOR UPDATE``.

    The row-locking read for workflow consequences whose terminal evidence
    must stay serialized against concurrent observed-snapshot reconciliation
    (issue #229): the lock is held to the end of the caller's transaction, so
    a reconciled merged or closed fact cannot commit between the evidence
    read and the consequence's own durable writes. Same ``unique (task_id)``
    canonical resolution and explicit Workspace scope as the ordinary read;
    ``None`` when the record (or its exact scope) does not exist durably.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where task_id = %s and workspace_id = %s for update",
            (task_id, workspace_id),
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


def get_task_pull_requests_for_tasks(
    pool: DatabasePool, *, task_ids: Sequence[UUID]
) -> dict[UUID, TaskPullRequest]:
    """Return the canonical PR records of the addressed Tasks, batched (#376).

    The authorized Task list's canonical-PR presentation lookup: one query
    over the page's Task identities — at most one record per Task holds by
    the ``unique (task_id)`` invariant, so the mapping is one-to-one and an
    absent id is the honest no-canonical-PR fact.
    """
    distinct_ids = list(dict.fromkeys(task_ids))
    if not distinct_ids:
        return {}
    for task_id in distinct_ids:
        _require_uuid(task_id, "task_ids")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_PULL_REQUEST_COLUMNS} from openorc.task_pull_requests "
            "where task_id = any(%s) order by task_id",
            (distinct_ids,),
        ).fetchall()
    return {record.task_id: record for record in map(_task_pull_request_from_row, rows)}


def update_task_pull_request_observed(
    pool: DatabasePool,
    *,
    task_pull_request_id: UUID,
    head_ref: str,
    base_ref: str,
    head_sha: str,
    state: TaskPullRequestState,
    merged_at: datetime | None,
    # The #376 observed presentation facts (issue #376): optional — a caller
    # that reports none leaves the retained mergeability observation pair
    # untouched; a caller that reports a mergeability reading (including
    # GitHub's documented not-yet-determined null/`unknown`) replaces the
    # pair and binds it to the exact observed head.
    title: str | None = None,
    body: str | None = None,
    html_url: str | None = None,
    user_id: int | None = None,
    user_login: str | None = None,
    draft: bool | None = None,
    provider_created_at: datetime | None = None,
    provider_updated_at: datetime | None = None,
    provider_closed_at: datetime | None = None,
    base_sha: str | None = None,
    mergeable: bool | None = None,
    merge_state_status: str | None = None,
) -> TaskPullRequest | None:
    """Replace the mutable observed reconciliation state as one snapshot.

    Writes the full observed snapshot — ``head_ref``, ``base_ref``, the
    current ``head_sha``, the observed ``state``, and the observed
    ``merged_at``, plus the #376 presentation facts — advancing
    ``updated_at``. Reconciliation semantics: the caller presents what
    GitHub currently reports for the PR, and the record stores it as
    observed state. ``head_sha`` is the PR's current observed head and
    legitimately changes across remediation rounds while the PR identity
    stays stable; ``github_pr_number`` is deliberately not part of the
    snapshot (creation-time address metadata, never mutable identity). A
    merged snapshot requires the closed state (the domain and the database
    CHECK agree); an ``open`` observation carries ``merged_at=None``.

    Mergeability is head-bound: a reported reading — even GitHub's
    documented not-yet-determined reading — replaces the retained pair and
    stamps ``pr_mergeability_observed_at`` to the exact observed head, so a
    stale result is never presented as the new head's observation. A caller
    reporting no mergeability reading writes nothing into the pair.

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
    _validate_observed_facts(
        title=title,
        html_url=html_url,
        user_id=user_id,
        user_login=user_login,
        draft=draft,
        base_sha=base_sha,
        mergeable=mergeable,
        merge_state_status=merge_state_status,
    )
    mergeability_reported = mergeable is not None or merge_state_status is not None
    with transaction(pool) as conn:
        row = conn.execute(
            f"update openorc.task_pull_requests "
            f"set {
                (
                    _OBSERVED_SNAPSHOT_WRITE_SQL_MERGEABILITY
                    if mergeability_reported
                    else _OBSERVED_SNAPSHOT_WRITE_SQL
                )
            } "
            f"where id = %s "
            f"returning {_TASK_PULL_REQUEST_COLUMNS}",
            (
                *_observed_snapshot_write_params(
                    head_ref=head_ref,
                    base_ref=base_ref,
                    head_sha=head_sha,
                    state=state,
                    merged_at=merged_at,
                    title=title,
                    body=body,
                    html_url=html_url,
                    user_id=user_id,
                    user_login=user_login,
                    draft=draft,
                    provider_created_at=provider_created_at,
                    provider_updated_at=provider_updated_at,
                    provider_closed_at=provider_closed_at,
                    base_sha=base_sha,
                    mergeable=mergeable,
                    merge_state_status=merge_state_status,
                ),
                task_pull_request_id,
            ),
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
    # The #376 observed presentation facts (issue #376): optional — a caller
    # that reports none leaves the retained mergeability observation pair
    # untouched; a caller that reports a mergeability reading (including
    # GitHub's documented not-yet-determined null/`unknown`) replaces the
    # pair and binds it to the exact observed head.
    title: str | None = None,
    body: str | None = None,
    html_url: str | None = None,
    user_id: int | None = None,
    user_login: str | None = None,
    draft: bool | None = None,
    provider_created_at: datetime | None = None,
    provider_updated_at: datetime | None = None,
    provider_closed_at: datetime | None = None,
    base_sha: str | None = None,
    mergeable: bool | None = None,
    merge_state_status: str | None = None,
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

    The #376 presentation facts participate in the same whole-snapshot
    comparison. Mergeability is head-bound: a reported reading — even
    GitHub's documented not-yet-determined reading — replaces the retained
    pair and stamps ``pr_mergeability_observed_at`` to the exact observed
    head, so a stale result is never presented as the new head's
    observation; a no-reading write leaves the retained pair untouched.

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
    _validate_observed_facts(
        title=title,
        html_url=html_url,
        user_id=user_id,
        user_login=user_login,
        draft=draft,
        base_sha=base_sha,
        mergeable=mergeable,
        merge_state_status=merge_state_status,
    )
    mergeability_reported = mergeable is not None or merge_state_status is not None
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
        # The identical-observation durable no-op: the whole observed snapshot
        # must match, and a reported mergeability reading matches only when
        # the retained pair already binds the exact observed head with the
        # same readings (otherwise the observation is new evidence).
        unchanged = (
            current.head_ref == head_ref
            and current.base_ref == base_ref
            and current.head_sha == head_sha
            and current.state == state
            and current.merged_at == merged_at
            and current.title == title
            and current.body == body
            and current.html_url == html_url
            and current.user_id == user_id
            and current.user_login == user_login
            and current.draft == draft
            and current.provider_created_at == provider_created_at
            and current.provider_updated_at == provider_updated_at
            and current.provider_closed_at == provider_closed_at
            and current.base_sha == base_sha
            and (
                not mergeability_reported
                or (
                    current.mergeable == mergeable
                    and current.merge_state_status == merge_state_status
                    and current.pr_mergeability_head_sha == head_sha
                )
            )
        )
        if unchanged:
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
            f"update openorc.task_pull_requests "
            f"set {
                (
                    _OBSERVED_SNAPSHOT_WRITE_SQL_MERGEABILITY
                    if mergeability_reported
                    else _OBSERVED_SNAPSHOT_WRITE_SQL
                )
            } "
            f"where id = %s "
            f"returning {_TASK_PULL_REQUEST_COLUMNS}",
            (
                *_observed_snapshot_write_params(
                    head_ref=head_ref,
                    base_ref=base_ref,
                    head_sha=head_sha,
                    state=state,
                    merged_at=merged_at,
                    title=title,
                    body=body,
                    html_url=html_url,
                    user_id=user_id,
                    user_login=user_login,
                    draft=draft,
                    provider_created_at=provider_created_at,
                    provider_updated_at=provider_updated_at,
                    provider_closed_at=provider_closed_at,
                    base_sha=base_sha,
                    mergeable=mergeable,
                    merge_state_status=merge_state_status,
                ),
                task_pull_request_id,
            ),
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
