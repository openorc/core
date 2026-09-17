"""Repositories for PlanRevision persistence.

Explicit SQL repositories over the ``openorc`` schema for PlanRevision, the
versioned, immutable Producer plan artifact (Phase 1, issue #23). Rows map
to transport-independent domain objects from
:mod:`openorc.domain.planning`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Immutability is structural: no repository function updates a PlanRevision.
A changed plan is a fresh revision — a new row with the next per-Task
``revision_number`` — so a Task's planning history stays complete and
intact. The per-Task ``unique (task_id, revision_number)`` constraint is the
durable backstop: a duplicate version raises ``UniqueViolation`` (translating
driver exceptions into typed application errors is a service-layer concern).

The Task's current-plan pointer is written through
:func:`openorc.persistence.tasks.set_current_plan_revision`, alongside the
other conditional Task-state mutations.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.planning import PlanRevision, PlanRevisionDomainError
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_plan_revision",
    "get_plan_revision",
    "list_task_plan_revisions",
]

_PLAN_REVISION_COLUMNS = (
    "id, workspace_id, task_id, revision_number, content, repository_base_sha, created_at"
)


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PlanRevisionDomainError(f"PlanRevision.{name} must be a positive integer")


def _plan_revision_from_row(row: Sequence[Any]) -> PlanRevision:
    return PlanRevision(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        revision_number=row[3],
        content=row[4],
        repository_base_sha=row[5],
        created_at=normalize_utc(row[6]),
    )


def create_plan_revision(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    revision_number: int,
    content: str,
    repository_base_sha: str,
) -> PlanRevision:
    """Insert one versioned, immutable plan revision for a Task.

    The caller supplies the next ``revision_number`` for the Task; the
    per-Task unique constraint makes a duplicate version impossible (a
    ``UniqueViolation`` is the durable backstop). ``content`` is the exact
    Producer plan text and ``repository_base_sha`` is the audit/context base
    the revision was created and reviewed against; both must be non-empty.
    There is no update path: a changed plan is a fresh revision, and later
    repository-base movement neither invalidates an accepted/authorized
    revision nor authorizes rewriting one.
    """
    _require_positive_int(revision_number, "revision_number")
    if not isinstance(content, str) or not content.strip():
        raise PlanRevisionDomainError("PlanRevision.content must be a non-empty string")
    if not isinstance(repository_base_sha, str) or not repository_base_sha.strip():
        raise PlanRevisionDomainError("PlanRevision.repository_base_sha must be a non-empty string")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.plan_revisions "
            "(workspace_id, task_id, revision_number, content, repository_base_sha) "
            "values (%s, %s, %s, %s, %s) "
            f"returning {_PLAN_REVISION_COLUMNS}",
            (workspace_id, task_id, revision_number, content, repository_base_sha),
        ).fetchone()
    assert row is not None
    return _plan_revision_from_row(row)


def get_plan_revision(pool: DatabasePool, *, plan_revision_id: UUID) -> PlanRevision | None:
    """Return one PlanRevision by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_PLAN_REVISION_COLUMNS} from openorc.plan_revisions where id = %s",
            (plan_revision_id,),
        ).fetchone()
    return None if row is None else _plan_revision_from_row(row)


def list_task_plan_revisions(pool: DatabasePool, *, task_id: UUID) -> list[PlanRevision]:
    """List a Task's complete planning history, ordered by revision number.

    Every revision of the Task — current and superseded alike — is
    immutable history: the current-plan pointer identifies the authoritative
    revision without deleting or rewriting older ones. Serves the
    ``(task_id, revision_number)`` planning-history lookup.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_PLAN_REVISION_COLUMNS} from openorc.plan_revisions "
            "where task_id = %s order by revision_number",
            (task_id,),
        ).fetchall()
    return [_plan_revision_from_row(row) for row in rows]
