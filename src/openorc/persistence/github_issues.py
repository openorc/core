"""Repositories for the durable GitHub issue projection (issues #59, #375).

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
  observation fact actually differs (``IS DISTINCT FROM`` — an unchanged
  reconciliation performs no write at all and preserves ``updated_at``), and
  classifies insert races from re-read durable state rather than from which
  unique constraint PostgreSQL reported. A number durably mapped to a
  different stable identity is never rebound: the caller receives the
  classified outcome and applies the typed application-error semantics. The
  compared and written fact set is one coherent
  :class:`GitHubIssueObservedFacts` snapshot: the #59 core facts plus the
  #375 presentation facts. A presentation-only change persists through this
  path and never touches the requirements fingerprint; an identical
  observation remains a true durable no-op.
- Durable identity and Workspace scope are immutable on every update path.

Read-side support (#375): the authorized issue read surface consumes
:func:`find_github_issue_by_id` (one projection by its internal UUID, scoped
to the addressed Workspace Repository) and
:func:`list_repository_github_issues_page` (the deterministic keyset-paged,
state-filtered bounded list). :func:`stamp_github_issue_relations_observation`
records the minimal observation-evidence instants for the successful
relations synchronizations (#60) so the read surface can distinguish an
authoritative empty relation observation from "never observed".

Violated database invariants surface as driver exceptions (for example
``psycopg.errors.UniqueViolation``); translating the classified outcomes
into typed application errors is a service-layer concern, not a persistence
one.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from openorc.domain.github_issues import (
    GitHubIssueDomainError,
    GitHubIssueIdentity,
    GitHubIssueObservedFacts,
    GitHubIssueProjection,
    GitHubIssueState,
    github_issue_assignees_from_json,
    github_issue_assignees_json,
    github_issue_labels_from_json,
    github_issue_labels_json,
    github_issue_milestone_from_json,
    github_issue_milestone_json,
    github_issue_user_from_json,
    github_issue_user_json,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "GitHubIssuePageState",
    "GitHubIssueReconcileOutcome",
    "GitHubIssueReconcileResult",
    "find_github_issue",
    "find_github_issue_by_id",
    "find_github_issue_by_number",
    "find_github_issue_projections_by_identity",
    "list_open_repository_github_issues",
    "list_repository_github_issues",
    "list_repository_github_issues_page",
    "reconcile_github_issue",
    "stamp_github_issue_relations_observation",
]

_GITHUB_ISSUE_COLUMNS = (
    "id, workspace_id, repository_id, github_issue_id, issue_number, title, body, "
    "state, requirements_fingerprint, provider_updated_at, created_at, updated_at, "
    "state_reason, html_url, provider_created_at, provider_closed_at, user_json, "
    "labels_json, assignees_json, milestone_json, hierarchy_observed_at, "
    "dependencies_observed_at"
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
        state_reason=row[12],
        html_url=row[13],
        provider_created_at=None if row[14] is None else normalize_utc(row[14]),
        provider_closed_at=None if row[15] is None else normalize_utc(row[15]),
        user=github_issue_user_from_json(row[16]),
        labels=github_issue_labels_from_json(row[17]),
        assignees=github_issue_assignees_from_json(row[18]),
        milestone=github_issue_milestone_from_json(row[19]),
        hierarchy_observed_at=None if row[20] is None else normalize_utc(row[20]),
        dependencies_observed_at=None if row[21] is None else normalize_utc(row[21]),
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


def _facts_json_wrapped(facts: GitHubIssueObservedFacts) -> tuple[Any, ...]:
    """Return the snapshot's SQL parameters with nested facts Jsonb-wrapped.

    Nested presentation facts cross as explicit canonical ``Jsonb`` values
    (or ``NULL`` when unobserved), matching the canonical JSON shapes the
    domain boundary defines.
    """
    return (
        facts.issue_number,
        facts.title,
        facts.body,
        facts.state.value,
        facts.requirements_fingerprint,
        facts.provider_updated_at,
        facts.state_reason,
        facts.html_url,
        facts.provider_created_at,
        facts.provider_closed_at,
        Jsonb(github_issue_user_json(facts.user)) if facts.user is not None else None,
        Jsonb(github_issue_labels_json(facts.labels)) if facts.labels is not None else None,
        Jsonb(github_issue_assignees_json(facts.assignees))
        if facts.assignees is not None
        else None,
        Jsonb(github_issue_milestone_json(facts.milestone))
        if facts.milestone is not None
        else None,
    )


def _facts_match_durable(current: GitHubIssueProjection, facts: GitHubIssueObservedFacts) -> bool:
    """Whether the durable projection already carries exactly these facts."""
    return (
        current.issue_number == facts.issue_number
        and current.title == facts.title
        and current.body == facts.body
        and current.state is facts.state
        and current.requirements_fingerprint == facts.requirements_fingerprint
        and current.provider_updated_at == facts.provider_updated_at
        and current.state_reason == facts.state_reason
        and current.html_url == facts.html_url
        and current.provider_created_at == facts.provider_created_at
        and current.provider_closed_at == facts.provider_closed_at
        and current.user == facts.user
        and current.labels == facts.labels
        and current.assignees == facts.assignees
        and current.milestone == facts.milestone
    )


def _apply_conditional_update(
    conn: Any,
    current: GitHubIssueProjection,
    *,
    facts: GitHubIssueObservedFacts,
) -> GitHubIssueReconcileResult:
    """Apply the serialized, change-only update against the locked row.

    The caller holds the row lock, so the before-image is the true durable
    state. The ``IS DISTINCT FROM`` guard is the durable expression of the
    same-value no-op: an identical observation writes nothing, and
    ``updated_at`` only advances when canonical observed state actually
    changes — including the #375 presentation facts, whose change is a real
    durable observation change that never moves the requirements
    fingerprint. Stable identity and Workspace scope are never rewritten.
    """
    if _facts_match_durable(current, facts):
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.UNCHANGED,
            projection=current,
            previous_fingerprint=current.requirements_fingerprint,
            previous_state=current.state,
        )
    values = _facts_json_wrapped(facts)
    row = conn.execute(
        f"update openorc.github_issues set "
        "issue_number = %s, title = %s, body = %s, state = %s, "
        "requirements_fingerprint = %s, provider_updated_at = %s, "
        "state_reason = %s, html_url = %s, provider_created_at = %s, "
        "provider_closed_at = %s, user_json = %s, labels_json = %s, "
        "assignees_json = %s, milestone_json = %s, updated_at = now() "
        "where repository_id = %s and github_issue_id = %s "
        "and (issue_number, title, body, state, requirements_fingerprint, "
        "provider_updated_at, state_reason, html_url, provider_created_at, "
        "provider_closed_at, user_json, labels_json, assignees_json, milestone_json) "
        "is distinct from "
        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        f"returning {_GITHUB_ISSUE_COLUMNS}",
        (
            *values,
            current.repository_id,
            current.identity.github_issue_id,
            *values,
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
    facts: GitHubIssueObservedFacts,
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
    if current.issue_number != facts.issue_number:
        return GitHubIssueReconcileResult(
            outcome=GitHubIssueReconcileOutcome.IDENTITY_NUMBER_MISMATCH,
            projection=current,
            previous_fingerprint=current.requirements_fingerprint,
            previous_state=current.state,
        )
    return _apply_conditional_update(conn, current, facts=facts)


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


def _normalize_facts_instants(facts: GitHubIssueObservedFacts) -> GitHubIssueObservedFacts:
    """Normalize the snapshot's provider instants to timezone-aware UTC.

    Python datetimes crossing this boundary are timezone-aware and
    normalized to UTC (the persistence time rule); the domain dataclass
    rejects naive instants downstream, so this normalization is the
    boundary's own obligation, applied uniformly to every provider instant.
    """
    return dataclasses.replace(
        facts,
        provider_updated_at=(
            None if facts.provider_updated_at is None else normalize_utc(facts.provider_updated_at)
        ),
        provider_created_at=(
            None if facts.provider_created_at is None else normalize_utc(facts.provider_created_at)
        ),
        provider_closed_at=(
            None if facts.provider_closed_at is None else normalize_utc(facts.provider_closed_at)
        ),
    )


def reconcile_github_issue(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    facts: GitHubIssueObservedFacts,
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
    on; identity is never rebound here. The compared and written fact set is
    the caller's whole :class:`GitHubIssueObservedFacts` snapshot (instants
    normalized to UTC at this boundary).
    """
    if not isinstance(repository_id, UUID) or not isinstance(workspace_id, UUID):
        raise ValueError("workspace_id and repository_id must be UUIDs")
    if (
        isinstance(github_issue_id, bool)
        or not isinstance(github_issue_id, int)
        or github_issue_id <= 0
    ):
        raise ValueError("github_issue_id must be a positive integer")
    normalized = _normalize_facts_instants(facts)
    values = _facts_json_wrapped(normalized)
    insert_params: tuple[Any, ...] = (
        workspace_id,
        repository_id,
        github_issue_id,
        *values,
    )
    with transaction(pool) as conn:
        for _attempt in range(_MAX_INSERT_ATTEMPTS):
            locked_result = _reconcile_locked_row(
                conn,
                repository_id=repository_id,
                github_issue_id=github_issue_id,
                facts=normalized,
            )
            if locked_result is not None:
                return locked_result
            try:
                with conn.transaction():
                    inserted = conn.execute(
                        "insert into openorc.github_issues "
                        "(workspace_id, repository_id, github_issue_id, issue_number, "
                        "title, body, state, requirements_fingerprint, provider_updated_at, "
                        "state_reason, html_url, provider_created_at, provider_closed_at, "
                        "user_json, labels_json, assignees_json, milestone_json) "
                        "values ("
                        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
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
                    issue_number=normalized.issue_number,
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


def list_repository_github_issues(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> list[GitHubIssueProjection]:
    """List one Workspace Repository's durable issue projections, deterministically ordered.

    Repository-scoped webhook fan-out (#120): relation notifications
    (``sub_issues``/``issue_dependencies``) carry no issue identity, so
    dispatch re-synchronizes the tracked issues' authoritative relation state
    from their durable stable identities.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where workspace_id = %s and repository_id = %s order by github_issue_id",
            (workspace_id, repository_id),
        ).fetchall()
    return [_github_issue_from_row(row) for row in rows]


def list_open_repository_github_issues(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> list[GitHubIssueProjection]:
    """List one Workspace Repository's open tracked issue projections, deterministically ordered.

    The bounded current-work issue set for the #62 repeatable reconciliation
    sweep: only projections whose durable observed state is open participate
    (closed historical issues have no current reconciliation subject unless a
    current Task still backs them, which the sweep composes separately from
    the durable Task set). Database-only; ordered by stable issue identity.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where workspace_id = %s and repository_id = %s and state = %s "
            "order by github_issue_id",
            (workspace_id, repository_id, GitHubIssueState.OPEN.value),
        ).fetchall()
    return [_github_issue_from_row(row) for row in rows]


class GitHubIssuePageState(StrEnum):
    """The documented state-filter vocabulary of the authorized issue list.

    Values are the documented API filter tokens (issue #375): the durable
    ``open``/``closed`` observed states and the explicit ``all`` unfiltered
    form. The default list state is ``open``.
    """

    OPEN = "open"
    CLOSED = "closed"
    ALL = "all"


def find_github_issue_by_id(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID, issue_id: UUID
) -> GitHubIssueProjection | None:
    """Return one projection by its internal record identity, scope-checked.

    The authorized detail read (#375): the internal projection UUID is the
    only address, and the row must belong to the addressed Workspace
    Repository — a foreign scope is the uniform not-found outcome, never a
    cross-Workspace oracle.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where id = %s and workspace_id = %s and repository_id = %s",
            (issue_id, workspace_id, repository_id),
        ).fetchone()
    return None if row is None else _github_issue_from_row(row)


def find_github_issue_projections_by_identity(
    pool: DatabasePool, *, repository_id: UUID, github_issue_ids: Sequence[int]
) -> dict[int, GitHubIssueProjection]:
    """Return the projections of the addressed stable issue identities, batched (#376).

    The authorized Task list's GitHub-issue summary lookup: one query over
    the page's stable issue identities, scoped to the exact Repository so a
    foreign identity is indistinguishable from an unprojected one. An absent
    id is the honest not-yet-projected fact the wire presents as nulls.
    """
    distinct_ids = list(dict.fromkeys(github_issue_ids))
    if not distinct_ids:
        return {}
    for issue_id in distinct_ids:
        if isinstance(issue_id, bool) or not isinstance(issue_id, int) or issue_id <= 0:
            raise GitHubIssueDomainError("github_issue_ids must all be positive integers")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where repository_id = %s and github_issue_id = any(%s) "
            "order by github_issue_id",
            (repository_id, distinct_ids),
        ).fetchall()
    return {
        projection.identity.github_issue_id: projection
        for projection in map(_github_issue_from_row, rows)
    }


def list_repository_github_issues_page(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    state: GitHubIssuePageState,
    after_issue_number: int | None,
    limit: int,
) -> list[GitHubIssueProjection]:
    """Return one deterministic keyset page of the repository's projections.

    The authorized bounded list (#375): ordered by the durable address
    metadata (``issue_number`` ascending, unique per repository), advanced
    by the previous page's last number (the keyset cursor), and explicitly
    limited so one page stays bounded. The state filter selects the durable
    observed state vocabulary; ``all`` carries no state predicate.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    if not isinstance(state, GitHubIssuePageState):
        raise ValueError("state must be a GitHubIssuePageState value")
    if after_issue_number is not None and (
        isinstance(after_issue_number, bool)
        or not isinstance(after_issue_number, int)
        or after_issue_number <= 0
    ):
        raise ValueError("after_issue_number must be a positive integer or None")
    state_predicate = "" if state is GitHubIssuePageState.ALL else " and state = %s"
    state_param: tuple[Any, ...] = () if state is GitHubIssuePageState.ALL else (state.value,)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_GITHUB_ISSUE_COLUMNS} from openorc.github_issues "
            "where workspace_id = %s and repository_id = %s"
            f"{state_predicate} "
            "and (%s::bigint is null or issue_number > %s::bigint) "
            "order by issue_number asc limit %s",
            (
                workspace_id,
                repository_id,
                *state_param,
                after_issue_number,
                after_issue_number,
                limit,
            ),
        ).fetchall()
    return [_github_issue_from_row(row) for row in rows]


def stamp_github_issue_relations_observation(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    hierarchy: bool = False,
    dependencies: bool = False,
) -> None:
    """Record the minimal observation-evidence instants of a successful sync.

    Called only by a relations synchronization that completed successfully:
    the addressed evidence column(s) advance to the observation instant so
    the read surface can distinguish an authoritative empty relation
    observation from "never observed". A failed or unobservable relations
    read never calls this, so the prior evidence stays byte-identical — the
    mirrors' own no-stamp rule (issue #60) extended to the evidence. The
    evidence is presentation observation context, never workflow fact.
    """
    if not hierarchy and not dependencies:
        raise ValueError("at least one evidence family must be stamped")
    # The supported evidence combinations are static SQL: the dynamic-identifier
    # builder never applies to value-free SET clauses.
    if hierarchy and dependencies:
        statement = (
            "update openorc.github_issues set "
            "hierarchy_observed_at = now(), dependencies_observed_at = now() "
            "where workspace_id = %s and repository_id = %s and github_issue_id = %s"
        )
    elif hierarchy:
        statement = (
            "update openorc.github_issues set hierarchy_observed_at = now() "
            "where workspace_id = %s and repository_id = %s and github_issue_id = %s"
        )
    else:
        statement = (
            "update openorc.github_issues set dependencies_observed_at = now() "
            "where workspace_id = %s and repository_id = %s and github_issue_id = %s"
        )
    with transaction(pool) as conn:
        conn.execute(
            statement,
            (workspace_id, repository_id, github_issue_id),
        )
