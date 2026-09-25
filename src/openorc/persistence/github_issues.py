"""Repositories for the durable GitHub issue projection (issue #59).

Explicit SQL repositories over the ``openorc`` schema for the authoritative
GitHub Issue observations of one Workspace Repository, independent from the
Task aggregate. Rows map to transport-independent domain objects from
:mod:`openorc.domain.github_issues`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Reconciliation semantics implemented here (deterministic, race-safe):

- The projection is keyed by ``(repository_id, github_issue_id)`` — the
  stable external issue identity within one Workspace Repository. The
  repository-local ``issue_number`` is durable address metadata with its own
  uniqueness, so issue-number reuse can never race past or substitute for
  stable identity.
- :func:`reconcile_github_issue` is the serialized write: it locks the
  existing projection row (``SELECT ... FOR UPDATE``) before computing any
  before/current fact, applies a conditional update only when a mutable
  observation field actually differs (``IS DISTINCT FROM`` — an unchanged
  reconciliation performs no write at all and preserves ``updated_at``), and
  classifies insert races from re-read durable state rather than from which
  unique constraint PostgreSQL reported. A number durably mapped to a
  different stable identity is never rebound: the caller receives the
  classified outcome and applies the typed application-error semantics.
- Durable identity and Workspace scope are immutable on every update path.

Violated database invariants surface as driver exceptions (for example
``psycopg.errors.UniqueViolation``); translating the classified outcomes
into typed application errors is a service-layer concern, not a persistence
one.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.domain.github_issues import (
    GitHubIssueIdentity,
    GitHubIssueProjection,
    GitHubIssueState,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "GitHubIssueReconcileOutcome",
    "GitHubIssueReconcileResult",
    "find_github_issue",
    "find_github_issue_by_number",
    "reconcile_github_issue",
]

_GITHUB_ISSUE_COLUMNS = (
    "id, workspace_id, repository_id, github_issue_id, issue_number, title, body, "
    "state, requirements_fingerprint, provider_updated_at, created_at, updated_at"
)

# A concurrent first-projection insert is retried a small, bounded number of
# times: each attempt opens its own savepoint, so a lost insert race rolls
# back only the attempt and the re-read classification path converges onto
# the winner's committed row. The bound exists so an unresolvable concurrent
# state fails closed instead of spinning.
_MAX_INSERT_ATTEMPTS = 3


class GitHubIssueReconcileOutcome(Enum):
    """The classified durable outcome of one projected issue reconciliation.

    The outcomes are facts about the durable before→current transition this
    invocation actually established after conflict serialization:

    - ``INSERTED`` — this invocation created the projection row (exactly one
      concurrent invocation can observe this).
    - ``UPDATED`` — this invocation durably advanced an existing projection.
    - ``UNCHANGED`` — the authoritative state matched the persisted state:
      a true durable no-op (no write, ``updated_at`` preserved).
    - ``NUMBER_CONFLICT`` — the issue number durably maps to a different
      stable issue identity. Identity is never rebound.
    - ``IDENTITY_NUMBER_MISMATCH`` — the stable identity is durably recorded
      under a different issue number. Neither fact is rewritten.
    - ``UNRESOLVABLE`` — neither classification could be proven from current
      durable state; the caller fails closed.
    """

    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    NUMBER_CONFLICT = "number_conflict"
    IDENTITY_NUMBER_MISMATCH = "identity_number_mismatch"
    UNRESOLVABLE = "unresolvable"


@dataclass(frozen=True, slots=True)
class GitHubIssueReconcileResult:
    """The serialized reconcile outcome and the current durable projection.

    ``projection`` is the post-write durable row (or the conflicting durable
    row for the conflict outcomes, or ``None`` when unresolvable).
    ``previous_fingerprint`` is the serialized pre-image fingerprint of an
    ``UPDATED`` outcome, the unchanged fingerprint for ``UNCHANGED``, and
    ``None`` for ``INSERTED`` (creation is by definition not a requirements
    change). ``previous_state`` is the serialized pre-image open/closed
    ``GitHubIssueState`` under the identical population rule — the pre-image
    state for ``UPDATED``, ``UNCHANGED``, and the classified conflict rows,
    and ``None`` for ``INSERTED`` and ``UNRESOLVABLE`` — so callers can
    compare previous vs current state after the serialized write.
    """

    outcome: GitHubIssueReconcileOutcome
    projection: GitHubIssueProjection | None
    previous_fingerprint: str | None
    previous_state: GitHubIssueState | None


def _github_issue_from_row(row: Sequence[Any]) -> GitHubIssueProjection:
    return GitHubIssueProjection(
        id=row[0],
        workspace_id=row[1],
        repository_id=row[2],
        identity=GitHubIssueIdentity(github_issue_id=row[3]),
        issue_number=row[4],
        title=row[5],
        body=row[6],
        state=GitHubIssueState(row[7]),
        requirements_fingerprint=row[8],
        provider_updated_at=None if row[9] is None else normalize_utc(row[9]),
        created_at=normalize_utc(row[10]),
        updated_at=normalize_utc(row[11]),
    )


def _select_for_update_by_identity(conn: Any, repository_id: UUID, github_issue_id: int) -> Any:
    return conn.execute(
        f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
        "where repository_id = %s and github_issue_id = %s for update",
        (repository_id, github_issue_id),
    ).fetchone()


def _select_for_update_by_number(conn: Any, repository_id: UUID, issue_number: int) -> Any:
    return conn.execute(
        f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
        "where repository_id = %s and issue_number = %s for update",
        (repository_id, issue_number),
    ).fetchone()


def _apply_conditional_update(
    conn: Any,
    current: GitHubIssueProjection,
    *,
    issue_number: int,
    title: str,
    body: str | None,
    state: GitHubIssueState,
    requirements_fingerprint: str,
    provider_updated_at: datetime | None,
) -> GitHubIssueReconcileResult:
    """Apply the serialized, change-only update against the locked row.

    The caller holds the row lock, so the before-image is the true durable
    state. The ``IS DISTINCT FROM`` guard is the durable expression of the
    same-value no-op: an identical observation writes nothing, and
    ``updated_at`` only advances when canonical observed state actually
    changes. Stable identity and Workspace scope are never rewritten.
    """
    if (
        current.issue_number == issue_number
        and current.title == title
        and current.body == body
        and current.state is state
        and current.requirements_fingerprint == requirements_fingerprint
        and current.provider_updated_at == provider_updated_at
    ):
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.UNCHANGED,
            projection=current,
            previous_fingerprint=current.requirements_fingerprint,
            previous_state=current.state,
        )
    row = conn.execute(
        f"update openorc.github_issues set "
        "issue_number = %s, title = %s, body = %s, state = %s, "
        "requirements_fingerprint = %s, provider_updated_at = %s, updated_at = now() "
        "where repository_id = %s and github_issue_id = %s "
        "and (issue_number, title, body, state, requirements_fingerprint, "
        "provider_updated_at) is distinct from (%s, %s, %s, %s, %s, %s) "
        f"returning {_GITHUB_ISSUE_COLUMNS}",
        (
            issue_number,
            title,
            body,
            state.value,
            requirements_fingerprint,
            provider_updated_at,
            current.repository_id,
            current.identity.github_issue_id,
            issue_number,
            title,
            body,
            state.value,
            requirements_fingerprint,
            provider_updated_at,
        ),
    ).fetchone()
    if row is None:  # pragma: no cover - the row lock excludes concurrent change
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.UNCHANGED,
            projection=current,
            previous_fingerprint=current.requirements_fingerprint,
            previous_state=current.state,
        )
    return GitHubIssueReconcileResult(
        outcome=GitHubIssueReconcileOutcome.UPDATED,
        projection=_github_issue_from_row(row),
        previous_fingerprint=current.requirements_fingerprint,
        previous_state=current.state,
    )


def _reconcile_locked_row(
    conn: Any,
    *,
    repository_id: UUID,
    github_issue_id: int,
    issue_number: int,
    title: str,
    body: str | None,
    state: GitHubIssueState,
    requirements_fingerprint: str,
    provider_updated_at: datetime | None,
) -> GitHubIssueReconcileResult | None:
    """Reconcile the projection row if it exists, locking it first.

    Returns ``None`` when no row exists for the stable identity so the
    caller can attempt the bounded insert. When a row exists, the mismatch
    or serialized update/no-op outcome is computed from the locked durable
    state — the true before-image for every returned fact.
    """
    locked = _select_for_update_by_identity(conn, repository_id, github_issue_id)
    if locked is None:
        return None
    current = _github_issue_from_row(locked)
    if current.issue_number != issue_number:
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.IDENTITY_NUMBER_MISMATCH,
            projection=current,
            previous_fingerprint=current.requirements_fingerprint,
            previous_state=current.state,
        )
    return _apply_conditional_update(
        conn,
        current,
        issue_number=issue_number,
        title=title,
        body=body,
        state=state,
        requirements_fingerprint=requirements_fingerprint,
        provider_updated_at=provider_updated_at,
    )


def _classify_insert_race(
    conn: Any, *, repository_id: UUID, issue_number: int, github_issue_id: int
) -> GitHubIssueReconcileResult | None:
    """Classify a lost insert race from re-read durable state.

    A concurrent reconcile won the insert race. The reported constraint is
    deliberately never consulted: a tuple-identical concurrent insert
    violates both unique invariants. Re-read both durable mappings under
    lock — the stable-identity mapping and the number mapping — and
    classify only from current state. A same-identity winner returns
    ``None`` (the caller's loop re-enters the locked read, converging into
    the update/no-op path); a differently-mapped number is the durable
    ``NUMBER_CONFLICT``; a state that proves neither case returns ``None``
    for a bounded retry and, ultimately, the fail-closed outcome.
    """
    identity_row = _select_for_update_by_identity(conn, repository_id, github_issue_id)
    if identity_row is not None:
        current = _github_issue_from_row(identity_row)
        if current.issue_number != issue_number:
            return GitHubIssueReconcileResult(
                outcome=GitHubIssueReconcileOutcome.IDENTITY_NUMBER_MISMATCH,
                projection=current,
                previous_fingerprint=current.requirements_fingerprint,
                previous_state=current.state,
            )
        return None
    number_row = _select_for_update_by_number(conn, repository_id, issue_number)
    if number_row is not None:
        conflicting = _github_issue_from_row(number_row)
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.NUMBER_CONFLICT,
            projection=conflicting,
            previous_fingerprint=conflicting.requirements_fingerprint,
            previous_state=conflicting.state,
        )
    return None


def reconcile_github_issue(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    issue_number: int,
    title: str,
    body: str | None,
    state: GitHubIssueState,
    requirements_fingerprint: str,
    provider_updated_at: datetime | None,
) -> GitHubIssueReconcileResult:
    """Serialize one authoritative issue observation into durable state.

    The lock-then-compute-then-write contract: the existing projection row is
    locked by stable identity before any fact is computed, so concurrent
    reconciliations of the same issue serialize and every returned fact
    describes the durable transition this invocation actually established. A
    first projection attempts the insert inside its own savepoint; a lost
    race is classified by re-reading both durable mappings — never from
    which unique constraint PostgreSQL reported — and the same-identity case
    converges through the locked update/no-op path. A number durably mapped
    to another stable identity, or an unresolvable serialized state, is
    returned as a classified conflict for the service layer to fail closed
    on; identity is never rebound here.
    """
    if not isinstance(repository_id, UUID) or not isinstance(workspace_id, UUID):
        raise ValueError("workspace_id and repository_id must be UUIDs")
    if (
        isinstance(github_issue_id, bool)
        or not isinstance(github_issue_id, int)
        or github_issue_id <= 0
    ):
        raise ValueError("github_issue_id must be a positive integer")
    if isinstance(issue_number, bool) or not isinstance(issue_number, int) or issue_number <= 0:
        raise ValueError("issue_number must be a positive integer")
    if not isinstance(state, GitHubIssueState):
        raise ValueError("state must be a GitHubIssueState value")
    if provider_updated_at is not None:
        provider_updated_at = normalize_utc(provider_updated_at)
    insert_params: tuple[Any, ...] = (
        workspace_id,
        repository_id,
        github_issue_id,
        issue_number,
        title,
        body,
        state.value,
        requirements_fingerprint,
        provider_updated_at,
    )
    with transaction(pool) as conn:
        for _attempt in range(_MAX_INSERT_ATTEMPTS):
            locked_result = _reconcile_locked_row(
                conn,
                repository_id=repository_id,
                github_issue_id=github_issue_id,
                issue_number=issue_number,
                title=title,
                body=body,
                state=state,
                requirements_fingerprint=requirements_fingerprint,
                provider_updated_at=provider_updated_at,
            )
            if locked_result is not None:
                return locked_result
            try:
                with conn.transaction():
                    inserted = conn.execute(
                        "insert into openorc.github_issues "
                        "(workspace_id, repository_id, github_issue_id, issue_number, "
                        "title, body, state, requirements_fingerprint, provider_updated_at) "
                        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                        f"returning {_GITHUB_ISSUE_COLUMNS}",
                        insert_params,
                    ).fetchone()
                    assert inserted is not None
                    return GitHubIssueReconcileResult(
                        outcome=GitHubIssueReconcileOutcome.INSERTED,
                        projection=_github_issue_from_row(inserted),
                        previous_fingerprint=None,
                        previous_state=None,
                    )
            except UniqueViolation:
                # A concurrent reconcile won the insert race: classify from
                # the re-read durable mappings, not from the constraint name.
                classified = _classify_insert_race(
                    conn,
                    repository_id=repository_id,
                    issue_number=issue_number,
                    github_issue_id=github_issue_id,
                )
                if classified is not None:
                    return classified
                # The conflicting row is not visible (a racing transaction
                # has since rolled back): retry the bounded insert.
        # The insert kept conflicting without a classifiable durable row:
        # fail closed rather than guessing.
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.UNRESOLVABLE,
            projection=None,
            previous_fingerprint=None,
            previous_state=None,
        )


def find_github_issue(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> GitHubIssueProjection | None:
    """Return the projection for one stable issue identity, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where repository_id = %s and github_issue_id = %s",
            (repository_id, github_issue_id),
        ).fetchone()
    return None if row is None else _github_issue_from_row(row)


def find_github_issue_by_number(
    pool: DatabasePool, *, repository_id: UUID, issue_number: int
) -> GitHubIssueProjection | None:
    """Return the projection for one repository-local issue number, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where repository_id = %s and issue_number = %s",
            (repository_id, issue_number),
        ).fetchone()
    return None if row is None else _github_issue_from_row(row)
