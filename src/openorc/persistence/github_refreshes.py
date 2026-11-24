"""Repositories for the durable scoped-refresh execution tracking (issue #375).

Explicit SQL repositories over the ``openorc.github_refreshes`` table — the
minimum durable refresh-specific execution tracking, not a generic job
framework and never workflow authority. Every operation is a short
transaction with no external I/O open.

Semantics implemented here (deterministic, race-safe, on-demand):

- :func:`admit_github_refresh` is the atomic admission decision. It locks
  the addressed Workspace row (``SELECT ... FOR UPDATE``) so the
  cross-scope containment decision is serialized per Workspace, recovers
  the addressed scope's expired accepted/running records to ``unknown``
  (lost completion evidence is never success and an expired record is never
  claimable by a stale job), checks the active-overlap containment rules
  (a workspace-scope request conflicts with any active record in the
  Workspace; a repository-scope request conflicts with the Workspace-scope
  active record or an active record for that Repository), trims the
  bounded terminal-row retention, and inserts the ``accepted`` record. The
  two partial unique indexes over the active statuses are the durable
  backstop: a racing overlapping insert classifies from re-read durable
  state as a conflict, never from the reported constraint name.
- :func:`claim_github_refresh` is the executing job's CAS
  (``accepted → running``), refused for an expired or already-recovered
  record: stale jobs never claim expired work.
- :func:`complete_github_refresh` is the executing job's terminal CAS
  (``running → {completed, partially_completed, failed, unknown}``) with
  the summary/error payload. An execution completing after another
  admission recovered its record cannot falsely stamp completion: the CAS
  precondition is ``status = 'running'``, and its already-committed
  convergent reconciliation effects remain safe.
- :func:`get_github_refresh` returns the stored row verbatim; the status
  projection (an expired ``accepted``/``running`` row presented as
  ``unknown``) is a read-side service concern, never a mutation here.

Violation of the active-uniqueness invariants surfaces as a driver
exception; translating classified outcomes into typed application errors is
a service-layer concern.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from openorc.domain.github_refreshes import (
    GitHubRefreshRecord,
    GitHubRefreshStatus,
    GitHubRefreshSummary,
    GitHubRefreshUnitError,
    github_refresh_summary_from_json,
    github_refresh_summary_json,
    github_refresh_unit_errors_from_json,
    github_refresh_unit_errors_json,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "GITHUB_REFRESH_ADMISSION_LEASE_SECONDS",
    "GITHUB_REFRESH_EXECUTION_LEASE_SECONDS",
    "GITHUB_REFRESH_RETENTION_SECONDS",
    "GitHubRefreshAdmissionOutcome",
    "admit_github_refresh",
    "claim_github_refresh",
    "complete_github_refresh",
    "get_github_refresh",
]

# The bounded admission lease: the window an accepted record may stay
# accepted while its F1 submission is delivered and picked up. A record
# still accepted past this window has lost its delivery evidence and is
# recoverable to ``unknown`` at the next overlapping-scope admission.
GITHUB_REFRESH_ADMISSION_LEASE_SECONDS = 600
# The bounded execution lease: the generous execution window one claimed
# refresh may run in before the next overlapping-scope admission deems its
# completion evidence lost. Bounded provider work per refresh keeps actual
# executions comfortably inside this window.
GITHUB_REFRESH_EXECUTION_LEASE_SECONDS = 1800
# The bounded terminal-record retention: terminal rows older than this are
# trimmed at admission time; nothing grows unboundedly, and Workspace
# deletion removes the rest through its ownership cascade.
GITHUB_REFRESH_RETENTION_SECONDS = 7 * 24 * 3600

_REFRESH_COLUMNS = (
    "id, workspace_id, repository_id, status, requested_at, started_at, "
    "lease_expires_at, completed_at, summary, errors"
)


@dataclass(frozen=True, slots=True)
class GitHubRefreshAdmissionOutcome:
    """The classified outcome of one atomic admission decision.

    ``record`` is the newly accepted record on success and ``None``
    otherwise; ``conflict`` states whether an active overlapping record
    contained the admission; ``workspace_missing`` states whether the
    addressed Workspace vanished between authorization and admission (a
    deletion race the caller fails closed on). Exactly one fact applies.
    """

    record: GitHubRefreshRecord | None
    conflict: bool
    workspace_missing: bool = False


def _refresh_from_row(row: Sequence[Any]) -> GitHubRefreshRecord:
    return GitHubRefreshRecord(
        id=row[0],
        workspace_id=row[1],
        repository_id=row[2],
        status=GitHubRefreshStatus(row[3]),
        requested_at=normalize_utc(row[4]),
        started_at=None if row[5] is None else normalize_utc(row[5]),
        lease_expires_at=None if row[6] is None else normalize_utc(row[6]),
        completed_at=None if row[7] is None else normalize_utc(row[7]),
        summary=github_refresh_summary_from_json(row[8]),
        errors=github_refresh_unit_errors_from_json(row[9]),
    )


def _insert_refresh(
    conn: Any, *, workspace_id: UUID, repository_id: UUID | None
) -> GitHubRefreshRecord:
    row = conn.execute(
        "insert into openorc.github_refreshes "
        "(workspace_id, repository_id, status, lease_expires_at) "
        "values (%s, %s, %s, now() + make_interval(secs => %s)) "
        f"returning {_REFRESH_COLUMNS}",
        (
            workspace_id,
            repository_id,
            GitHubRefreshStatus.ACCEPTED.value,
            GITHUB_REFRESH_ADMISSION_LEASE_SECONDS,
        ),
    ).fetchone()
    assert row is not None
    return _refresh_from_row(row)


def admit_github_refresh(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID | None,
) -> GitHubRefreshAdmissionOutcome:
    """Atomically admit one scoped refresh (the module-docstring contract).

    One short transaction: the Workspace row lock serializes the decision,
    the addressed scope's expired records recover to ``unknown``, the
    active-overlap containment is checked, bounded retention is trimmed, and
    the accepted record is inserted. A racing overlapping insert classifies
    from re-read durable state as a conflict.
    """
    if not isinstance(workspace_id, UUID):
        raise ValueError("workspace_id must be a UUID")
    if repository_id is not None and not isinstance(repository_id, UUID):
        raise ValueError("repository_id must be a UUID or None")
    workspace_scope = repository_id is None
    with transaction(pool) as conn:
        locked_workspace = conn.execute(
            "select id from openorc.workspaces where id = %s for update",
            (workspace_id,),
        ).fetchone()
        if locked_workspace is None:
            return GitHubRefreshAdmissionOutcome(
                record=None, conflict=False, workspace_missing=True
            )
        # On-demand recovery: the addressed scope's expired accepted/running
        # records are truthful ``unknown`` outcomes (lost completion
        # evidence); they can no longer be claimed or completed.
        if workspace_scope:
            conn.execute(
                "update openorc.github_refreshes set status = %s "
                "where workspace_id = %s and status in (%s, %s) "
                "and lease_expires_at is not null and lease_expires_at < now()",
                (
                    GitHubRefreshStatus.UNKNOWN.value,
                    workspace_id,
                    GitHubRefreshStatus.ACCEPTED.value,
                    GitHubRefreshStatus.RUNNING.value,
                ),
            )
        else:
            conn.execute(
                "update openorc.github_refreshes set status = %s "
                "where workspace_id = %s and status in (%s, %s) "
                "and lease_expires_at is not null and lease_expires_at < now() "
                "and (repository_id = %s or repository_id is null)",
                (
                    GitHubRefreshStatus.UNKNOWN.value,
                    workspace_id,
                    GitHubRefreshStatus.ACCEPTED.value,
                    GitHubRefreshStatus.RUNNING.value,
                    repository_id,
                ),
            )
        # Active-overlap containment. A workspace-scope request conflicts
        # with any active record in the Workspace; a repository-scope
        # request conflicts with the Workspace-scope active record or an
        # active record for that Repository.
        if workspace_scope:
            active = conn.execute(
                f"select {_REFRESH_COLUMNS} from openorc.github_refreshes "
                "where workspace_id = %s and status in (%s, %s) "
                "and lease_expires_at >= now() limit 1",
                (
                    workspace_id,
                    GitHubRefreshStatus.ACCEPTED.value,
                    GitHubRefreshStatus.RUNNING.value,
                ),
            ).fetchone()
        else:
            active = conn.execute(
                f"select {_REFRESH_COLUMNS} from openorc.github_refreshes "
                "where workspace_id = %s and status in (%s, %s) "
                "and lease_expires_at >= now() "
                "and (repository_id = %s or repository_id is null) limit 1",
                (
                    workspace_id,
                    GitHubRefreshStatus.ACCEPTED.value,
                    GitHubRefreshStatus.RUNNING.value,
                    repository_id,
                ),
            ).fetchone()
        if active is not None:
            return GitHubRefreshAdmissionOutcome(record=None, conflict=True)
        # Bounded retention: terminal rows older than the retention window
        # are trimmed here — nothing grows unboundedly.
        conn.execute(
            "delete from openorc.github_refreshes "
            "where workspace_id = %s and status not in (%s, %s) "
            "and requested_at < now() - make_interval(secs => %s)",
            (
                workspace_id,
                GitHubRefreshStatus.ACCEPTED.value,
                GitHubRefreshStatus.RUNNING.value,
                GITHUB_REFRESH_RETENTION_SECONDS,
            ),
        )
        try:
            with conn.transaction():
                record = _insert_refresh(
                    conn, workspace_id=workspace_id, repository_id=repository_id
                )
                return GitHubRefreshAdmissionOutcome(record=record, conflict=False)
        except UniqueViolation:
            # A racing overlapping admission won the active-uniqueness
            # backstop: classify from re-read durable state, never from the
            # reported constraint name.
            return GitHubRefreshAdmissionOutcome(record=None, conflict=True)


def fail_unclaimed_github_refresh(
    pool: DatabasePool,
    *,
    refresh_id: UUID,
    errors: tuple[GitHubRefreshUnitError, ...],
) -> GitHubRefreshRecord | None:
    """Record a known pre-execution refusal (``accepted → failed``).

    The admission service's truthful outcome for a F1 submission the
    operation boundary refused before delivery: the refresh will not run,
    the record is durably ``failed`` with its bounded sanitized error, and
    the summary stays unset (no observation was ever attempted). Only a
    record still durably ``accepted`` accepts the outcome; an already
    claimed or recovered record returns ``None``.
    """
    if not isinstance(refresh_id, UUID):
        raise ValueError("refresh_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            f"update openorc.github_refreshes set "
            "status = %s, completed_at = now(), errors = %s "
            "where id = %s and status = %s "
            f"returning {_REFRESH_COLUMNS}",
            (
                GitHubRefreshStatus.FAILED.value,
                Jsonb(github_refresh_unit_errors_json(errors)),
                refresh_id,
                GitHubRefreshStatus.ACCEPTED.value,
            ),
        ).fetchone()
    return None if row is None else _refresh_from_row(row)


def claim_github_refresh(pool: DatabasePool, *, refresh_id: UUID) -> GitHubRefreshRecord | None:
    """Claim one accepted record for execution (``accepted → running``).

    The executing job's CAS. An expired or already-recovered record is
    refused (``None``): stale jobs never claim expired work, and the record
    stays recoverable to ``unknown`` at the next overlapping-scope
    admission.
    """
    if not isinstance(refresh_id, UUID):
        raise ValueError("refresh_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            f"update openorc.github_refreshes set "
            "status = %s, started_at = now(), "
            "lease_expires_at = now() + make_interval(secs => %s) "
            "where id = %s and status = %s "
            "and lease_expires_at is not null and lease_expires_at >= now() "
            f"returning {_REFRESH_COLUMNS}",
            (
                GitHubRefreshStatus.RUNNING.value,
                GITHUB_REFRESH_EXECUTION_LEASE_SECONDS,
                refresh_id,
                GitHubRefreshStatus.ACCEPTED.value,
            ),
        ).fetchone()
    return None if row is None else _refresh_from_row(row)


def complete_github_refresh(
    pool: DatabasePool,
    *,
    refresh_id: UUID,
    status: GitHubRefreshStatus,
    summary: GitHubRefreshSummary,
    errors: tuple[GitHubRefreshUnitError, ...],
) -> GitHubRefreshRecord | None:
    """Stamp one claimed execution's terminal outcome (``running → status``).

    The executing job's terminal CAS. Only a record still durably
    ``running`` accepts the outcome: an execution completing after another
    admission recovered its record cannot falsely stamp completion, and its
    already-committed convergent reconciliation effects remain safe.
    """
    if not isinstance(refresh_id, UUID):
        raise ValueError("refresh_id must be a UUID")
    if status not in (
        GitHubRefreshStatus.COMPLETED,
        GitHubRefreshStatus.PARTIALLY_COMPLETED,
        GitHubRefreshStatus.FAILED,
        GitHubRefreshStatus.UNKNOWN,
    ):
        raise ValueError("status must be a terminal refresh outcome")
    with transaction(pool) as conn:
        row = conn.execute(
            f"update openorc.github_refreshes set "
            "status = %s, completed_at = now(), summary = %s, errors = %s "
            "where id = %s and status = %s "
            f"returning {_REFRESH_COLUMNS}",
            (
                status.value,
                Jsonb(github_refresh_summary_json(summary)),
                Jsonb(github_refresh_unit_errors_json(errors)),
                refresh_id,
                GitHubRefreshStatus.RUNNING.value,
            ),
        ).fetchone()
    return None if row is None else _refresh_from_row(row)


def get_github_refresh(pool: DatabasePool, *, refresh_id: UUID) -> GitHubRefreshRecord | None:
    """Return one stored refresh record verbatim, or ``None``."""
    if not isinstance(refresh_id, UUID):
        raise ValueError("refresh_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_REFRESH_COLUMNS} from openorc.github_refreshes where id = %s",
            (refresh_id,),
        ).fetchone()
    return None if row is None else _refresh_from_row(row)
