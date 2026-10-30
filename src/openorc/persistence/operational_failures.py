"""Repositories for operational-failure disposition state (F4, issue #251).

Explicit SQL repositories over the ``openorc`` schema for
``openorc.operational_failure_episodes`` and
``openorc.operational_failure_attempts`` — the durable failure-episode and
authorized-attempt facts of the Level 2 operational-failure disposition.
Rows map to transport-independent domain objects from
:mod:`openorc.domain.operational_failures`; instants returned from Postgres
are normalized to timezone-aware UTC at this boundary. Subjects are canonical
JSON objects through the domain canonicalizer and an explicit ``Jsonb``
adapter.

The disposition lifecycle is enforced by conditional statements and the
migration's unique backstops only:

- the episode insert is protected by the
  ``operational_failure_episodes_original_attempt_uniq`` unique constraint:
  exactly one episode per original durable logical attempt identity, so a
  repeated or raced failure report can never mint a second episode. A
  violated uniqueness surfaces as ``UniqueViolation``; translating it into
  typed application outcomes is a service-layer concern.
- the attempt insert is protected by
  ``operational_failure_attempts_episode_ordinal_uniq`` (one ordinal per
  episode) and ``operational_failure_attempts_source_attempt_uniq`` (at most
  one successor per failed attempt across the whole history): a duplicate or
  replayed disposition is a rejected no-op, never a second authorization.
- the escalation marking is a conditional set-once transition
  (``escalated_task_block_id IS NULL`` -> the exact TaskBlock id): the
  episode's terminal disposition is written exactly once, atomically with
  the F3 installation that owns it, and never cleared afterwards.

Rows carry only safe identity/classification facts. No prompts, exception
text or stacks, provider messages, credentials, or transcripts belong here.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.operational_failures import (
    OperationalFailureAttempt,
    OperationalFailureCode,
    OperationalFailureEpisode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    canonical_failure_subject,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_operational_failure_attempt",
    "create_operational_failure_episode",
    "get_operational_failure_attempt",
    "get_operational_failure_attempt_by_source",
    "get_operational_failure_episode",
    "get_operational_failure_episode_by_original_attempt",
    "list_operational_failure_attempts",
    "mark_episode_escalated",
]

_EPISODE_COLUMNS = (
    "id, workspace_id, task_id, operation_family, original_attempt_kind, "
    "original_attempt_id, subject, escalated_task_block_id, created_at"
)

_ATTEMPT_COLUMNS = (
    "id, workspace_id, task_id, episode_id, ordinal, source_kind, "
    "source_attempt_id, error_code, not_before, created_at"
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"operational-failure {name} must be a UUID")


def _episode_from_row(row: Sequence[Any]) -> OperationalFailureEpisode:
    return OperationalFailureEpisode(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        operation_family=PendingTaskRuntimeInteractionOperation(row[3]),
        original_attempt_kind=OriginalAttemptKind(row[4]),
        original_attempt_id=row[5],
        subject=row[6],
        escalated_task_block_id=row[7],
        created_at=normalize_utc(row[8]),
    )


def _attempt_from_row(row: Sequence[Any]) -> OperationalFailureAttempt:
    return OperationalFailureAttempt(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        episode_id=row[3],
        ordinal=row[4],
        source_kind=OperationalFailureSourceKind(row[5]),
        source_attempt_id=row[6],
        error_code=OperationalFailureCode(row[7]),
        not_before=normalize_utc(row[8]),
        created_at=normalize_utc(row[9]),
    )


def get_operational_failure_episode_by_original_attempt(
    pool: DatabasePool,
    *,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
) -> OperationalFailureEpisode | None:
    """Return the episode rooted at one original attempt identity, or ``None``."""
    _require_uuid(original_attempt_id, "original_attempt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_EPISODE_COLUMNS} from openorc.operational_failure_episodes "
            "where original_attempt_kind = %s and original_attempt_id = %s",
            (original_attempt_kind.value, original_attempt_id),
        ).fetchone()
    return None if row is None else _episode_from_row(row)


def get_operational_failure_episode(
    pool: DatabasePool, *, episode_id: UUID
) -> OperationalFailureEpisode | None:
    """Return one failure episode by id, or ``None`` when it does not exist."""
    _require_uuid(episode_id, "episode_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_EPISODE_COLUMNS} from openorc.operational_failure_episodes where id = %s",
            (episode_id,),
        ).fetchone()
    return None if row is None else _episode_from_row(row)


def create_operational_failure_episode(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    operation_family: PendingTaskRuntimeInteractionOperation,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
    subject: Any,
) -> OperationalFailureEpisode:
    """Insert one failure episode exactly once for its original attempt identity.

    The unique ``(original_attempt_kind, original_attempt_id)`` backstop
    rejects a second episode for the same logical attempt with
    ``UniqueViolation``. The composite foreign key keeps the episode in the
    addressed Task/Workspace scope (``ForeignKeyViolation`` otherwise).
    ``created_at`` is database-stamped.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(original_attempt_id, "original_attempt_id")
    canonical_subject = canonical_failure_subject(subject)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.operational_failure_episodes "
            "(workspace_id, task_id, operation_family, original_attempt_kind, "
            "original_attempt_id, subject) "
            "values (%s, %s, %s, %s, %s, %s) "
            f"returning {_EPISODE_COLUMNS}",
            (
                workspace_id,
                task_id,
                operation_family.value,
                original_attempt_kind.value,
                original_attempt_id,
                Jsonb(dict(canonical_subject)),
            ),
        ).fetchone()
    assert row is not None
    return _episode_from_row(row)


def list_operational_failure_attempts(
    pool: DatabasePool, *, episode_id: UUID
) -> list[OperationalFailureAttempt]:
    """List an episode's authorized attempts in ordinal order."""
    _require_uuid(episode_id, "episode_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_ATTEMPT_COLUMNS} from openorc.operational_failure_attempts "
            "where episode_id = %s order by ordinal",
            (episode_id,),
        ).fetchall()
    return [_attempt_from_row(row) for row in rows]


def get_operational_failure_attempt(
    pool: DatabasePool, *, attempt_id: UUID
) -> OperationalFailureAttempt | None:
    """Return one authorized attempt by id, or ``None`` when it does not exist."""
    _require_uuid(attempt_id, "attempt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_ATTEMPT_COLUMNS} from openorc.operational_failure_attempts where id = %s",
            (attempt_id,),
        ).fetchone()
    return None if row is None else _attempt_from_row(row)


def get_operational_failure_attempt_by_source(
    pool: DatabasePool,
    *,
    source_kind: OperationalFailureSourceKind,
    source_attempt_id: UUID,
) -> OperationalFailureAttempt | None:
    """Return the one successor of a failed attempt, or ``None`` when undisposed.

    The ``operational_failure_attempts_source_attempt_uniq`` unique
    constraint guarantees at most one successor per failed attempt across
    the whole history; this read resolves it for duplicate-report detection
    and idempotent disposition echoes.
    """
    _require_uuid(source_attempt_id, "source_attempt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_ATTEMPT_COLUMNS} from openorc.operational_failure_attempts "
            "where source_kind = %s and source_attempt_id = %s",
            (source_kind.value, source_attempt_id),
        ).fetchone()
    return None if row is None else _attempt_from_row(row)


def create_operational_failure_attempt(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    episode_id: UUID,
    ordinal: int,
    source_kind: OperationalFailureSourceKind,
    source_attempt_id: UUID,
    error_code: OperationalFailureCode,
    not_before: Any,
) -> OperationalFailureAttempt:
    """Insert one authorized additional attempt exactly once.

    The episode/ordinal and source-attempt unique backstops reject a second
    successor for the same failed attempt with ``UniqueViolation``; the
    composite foreign key keeps the attempt in its episode's Task/Workspace
    scope. ``not_before`` must be a timezone-aware UTC instant computed by
    the calling service's injectable clock at authorization time.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(episode_id, "episode_id")
    _require_uuid(source_attempt_id, "source_attempt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.operational_failure_attempts "
            "(workspace_id, task_id, episode_id, ordinal, source_kind, "
            "source_attempt_id, error_code, not_before) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_ATTEMPT_COLUMNS}",
            (
                workspace_id,
                task_id,
                episode_id,
                ordinal,
                source_kind.value,
                source_attempt_id,
                error_code.value,
                not_before,
            ),
        ).fetchone()
    assert row is not None
    return _attempt_from_row(row)


def mark_episode_escalated(
    pool: DatabasePool,
    *,
    episode_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    escalated_task_block_id: UUID,
) -> OperationalFailureEpisode | None:
    """Stamp the episode's terminal disposition exactly once.

    The conditional update applies only while the episode is in the addressed
    Task/Workspace scope and still unescalated
    (``escalated_task_block_id IS NULL``), writing the exact TaskBlock the
    F3 installation returned. ``None`` means the episode is missing, out of
    scope, or already terminally disposed — a rejected no-op that must never
    be retried blindly; the calling service fails closed instead of marking
    twice. The stamp is never cleared afterwards.
    """
    _require_uuid(episode_id, "episode_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(escalated_task_block_id, "escalated_task_block_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.operational_failure_episodes "
            "set escalated_task_block_id = %s "
            "where id = %s and task_id = %s and workspace_id = %s "
            "and escalated_task_block_id is null "
            f"returning {_EPISODE_COLUMNS}",
            (escalated_task_block_id, episode_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _episode_from_row(row)
