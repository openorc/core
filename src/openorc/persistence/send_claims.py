"""Repositories for durable runtime-send claims (F2, issue #87).

Explicit SQL repositories over the ``openorc`` schema for
``openorc.task_runtime_send_claims`` — the one durable pre-send identity of
each consequential automated Task→agent-runtime dispatch (the initial PLAN
dispatch, the planning REVISE dispatch, and the PR_COMPOSE dispatch). Rows
map to transport-independent domain objects from
:mod:`openorc.domain.send_claims`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary. Outcomes are canonical
JSON objects through the domain canonicalizer and an explicit ``Jsonb``
adapter.

The claim lifecycle is enforced by conditional statements only:

- the claim insert is the exactly-once pre-send fact; the partial unique
  indexes per kind (one initial-PLAN claim per Task, one planning-REVISE
  claim per finalized planning ReviewIteration, one PR_COMPOSE claim per
  approved PR_AUTHORIZATION OwnerGate) are the durable one-shot backstops,
  independent of any Task state token. Violated uniqueness surfaces as
  ``UniqueViolation``; translating it into typed application errors is a
  service-layer concern.
- settlement and known-non-delivery are one-shot forward-only transitions:
  each applies only while the row is still ``claimed``, only for the
  addressed Task/Workspace scope, and only to a terminal target status, so
  a raced or duplicated settlement is a rejected no-op (``None``) that must
  not be retried blindly. A row left ``claimed`` is the conservatively
  potentially-dispatched record: it is never replayed and never rewritten.
- the subject pre-check read serves the service-layer locked claim
  transaction (the Task row lock serializes same-Task claimers; the
  unique indexes keep that guarantee durable even without it).

Raw secrets, prompts, plan text, review content, reasoning, formal response
bodies, and SDK data never belong in these rows.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.send_claims import (
    TaskRuntimeSendClaim,
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
    canonical_send_claim_outcome,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "create_task_runtime_send_claim",
    "get_task_runtime_send_claim",
    "get_task_runtime_send_claim_for_subject",
    "list_task_runtime_send_claims",
    "record_task_runtime_send_claim_known_non_delivery",
    "settle_task_runtime_send_claim",
]

_CLAIM_COLUMNS = (
    "id, workspace_id, task_id, kind, task_agent_session_id, review_iteration_id, "
    "owner_gate_id, subject_head_sha, status, outcome, claimed_at, settled_at, updated_at"
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"send claim {name} must be a UUID")


def _claim_from_row(row: Sequence[Any]) -> TaskRuntimeSendClaim:
    return TaskRuntimeSendClaim(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        kind=TaskRuntimeSendClaimKind(row[3]),
        task_agent_session_id=row[4],
        review_iteration_id=row[5],
        owner_gate_id=row[6],
        subject_head_sha=row[7],
        status=TaskRuntimeSendClaimStatus(row[8]),
        outcome=row[9],
        claimed_at=normalize_utc(row[10]),
        settled_at=None if row[11] is None else normalize_utc(row[11]),
        updated_at=normalize_utc(row[12]),
    )


def _require_subject_form(
    kind: TaskRuntimeSendClaimKind,
    *,
    review_iteration_id: UUID | None,
    owner_gate_id: UUID | None,
) -> None:
    """Reject a subject form that disagrees with the closed claim kind."""
    if kind is TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH:
        if review_iteration_id is not None or owner_gate_id is not None:
            raise ValueError(
                "an initial_plan_dispatch claim addresses no review iteration or owner gate"
            )
    elif kind is TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH:
        if review_iteration_id is None or owner_gate_id is not None:
            raise ValueError(
                "a planning_revise_dispatch claim addresses exactly one finalized "
                "planning ReviewIteration"
            )
    elif kind is TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH:
        if owner_gate_id is None or review_iteration_id is not None:
            raise ValueError(
                "a pr_compose_dispatch claim addresses exactly one approved PR_AUTHORIZATION gate"
            )
    else:
        raise ValueError(f"unknown send claim kind: {kind!r}")


def create_task_runtime_send_claim(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    kind: TaskRuntimeSendClaimKind,
    task_agent_session_id: UUID,
    review_iteration_id: UUID | None = None,
    owner_gate_id: UUID | None = None,
    subject_head_sha: str | None = None,
) -> TaskRuntimeSendClaim:
    """Insert one claimed send identity exactly once, before any runtime effect.

    The claim transaction owns the locked Task/currentness revalidation; this
    repository operation owns the durable representation. The partial unique
    index for the kind is the exactly-once backstop: a second claim for the
    same logical subject — completed, known-failed, and unresolved claims
    alike — raises ``UniqueViolation``. The composite subject foreign keys
    keep every claimed subject in the same Task/Workspace scope; violations
    surface as ``ForeignKeyViolation``. ``claimed_at`` is database-stamped.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    if not isinstance(kind, TaskRuntimeSendClaimKind):
        raise ValueError("create_task_runtime_send_claim requires a TaskRuntimeSendClaimKind")
    _require_subject_form(
        kind,
        review_iteration_id=review_iteration_id,
        owner_gate_id=owner_gate_id,
    )
    if subject_head_sha is not None and (
        not isinstance(subject_head_sha, str) or not subject_head_sha.strip()
    ):
        raise ValueError("send claim subject_head_sha must be None or a non-empty string")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_runtime_send_claims "
            "(workspace_id, task_id, kind, task_agent_session_id, review_iteration_id, "
            "owner_gate_id, subject_head_sha) "
            "values (%s, %s, %s, %s, %s, %s, %s) "
            f"returning {_CLAIM_COLUMNS}",
            (
                workspace_id,
                task_id,
                kind.value,
                task_agent_session_id,
                review_iteration_id,
                owner_gate_id,
                subject_head_sha,
            ),
        ).fetchone()
    assert row is not None
    return _claim_from_row(row)


def get_task_runtime_send_claim(
    pool: DatabasePool, *, claim_id: UUID
) -> TaskRuntimeSendClaim | None:
    """Return one send claim by id, or ``None`` when it does not exist."""
    _require_uuid(claim_id, "claim_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_CLAIM_COLUMNS} from openorc.task_runtime_send_claims where id = %s",
            (claim_id,),
        ).fetchone()
    return None if row is None else _claim_from_row(row)


def get_task_runtime_send_claim_for_subject(
    pool: DatabasePool,
    *,
    task_id: UUID,
    kind: TaskRuntimeSendClaimKind,
    review_iteration_id: UUID | None = None,
    owner_gate_id: UUID | None = None,
) -> TaskRuntimeSendClaim | None:
    """Return the Task's existing claim for one exact kind/subject, or ``None``.

    The locked pre-check read of the claim transaction: under the Task row
    lock a same-Task claimer serializes, so an existing row means the
    subject was already claimed — whatever its outcome — and the
    continuation must never dispatch a runtime effect.
    """
    _require_uuid(task_id, "task_id")
    if not isinstance(kind, TaskRuntimeSendClaimKind):
        raise ValueError("get_task_runtime_send_claim_for_subject requires a kind")
    _require_subject_form(
        kind,
        review_iteration_id=review_iteration_id,
        owner_gate_id=owner_gate_id,
    )
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_CLAIM_COLUMNS} from openorc.task_runtime_send_claims "
            "where task_id = %s and kind = %s and review_iteration_id is not distinct "
            "from %s and owner_gate_id is not distinct from %s",
            (task_id, kind.value, review_iteration_id, owner_gate_id),
        ).fetchone()
    return None if row is None else _claim_from_row(row)


def settle_task_runtime_send_claim(
    pool: DatabasePool,
    *,
    claim_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    outcome: Any,
) -> TaskRuntimeSendClaim | None:
    """Advance one claimed send to ``settled`` exactly once.

    The one-shot settlement of a validated formal result that completed the
    operation: applies only while the claim is still ``claimed`` and the
    addressed Task/Workspace scope matches, stamps ``settled_at`` and the
    canonical safe outcome together, and commits nothing else. ``None``
    means the claim is missing, out of scope, or already terminal (a
    rejected no-op that must not be retried blindly); the caller fails
    closed rather than rewriting history. A settlement that never commits
    — a crash or a rolled-back finalize composition — leaves the claim
    conservatively claimed.
    """
    _require_uuid(claim_id, "claim_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    canonical_outcome = canonical_send_claim_outcome(outcome)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_runtime_send_claims "
            "set status = %s, outcome = %s, settled_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s and status = 'claimed' "
            f"returning {_CLAIM_COLUMNS}",
            (
                TaskRuntimeSendClaimStatus.SETTLED.value,
                Jsonb(dict(canonical_outcome)),
                claim_id,
                task_id,
                workspace_id,
            ),
        ).fetchone()
    return None if row is None else _claim_from_row(row)


def record_task_runtime_send_claim_known_non_delivery(
    pool: DatabasePool,
    *,
    claim_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    failure_kind: str,
) -> TaskRuntimeSendClaim | None:
    """Advance one claimed send to ``known_nondelivery`` exactly once.

    The truthful terminal record of an error positively proven to precede
    any runtime effect: applies only while the claim is still ``claimed``
    and the addressed Task/Workspace scope matches, and records the safe
    failure-kind label as the canonical outcome. ``None`` means the claim
    is missing, out of scope, or already terminal (a rejected no-op that
    must not be retried blindly). Post-effect or unproven failures never
    reach this transition: they leave the claim conservatively claimed.
    """
    _require_uuid(claim_id, "claim_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(failure_kind, str) or not failure_kind.strip():
        raise ValueError("send claim failure_kind must be a non-empty string")
    canonical_outcome = canonical_send_claim_outcome({"failure_kind": failure_kind})
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_runtime_send_claims "
            "set status = %s, outcome = %s, settled_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s and status = 'claimed' "
            f"returning {_CLAIM_COLUMNS}",
            (
                TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY.value,
                Jsonb(dict(canonical_outcome)),
                claim_id,
                task_id,
                workspace_id,
            ),
        ).fetchone()
    return None if row is None else _claim_from_row(row)


def list_task_runtime_send_claims(
    pool: DatabasePool, *, task_id: UUID
) -> list[TaskRuntimeSendClaim]:
    """List a Task's send claims, claimed and terminal alike.

    The full claim history is retained: terminal rows are immutable outcome
    records, and claimed rows are the outstanding-work facts the
    runtime-neutral pending-interaction projection reads. Deterministically
    ordered for stable reads.
    """
    _require_uuid(task_id, "task_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_CLAIM_COLUMNS} from openorc.task_runtime_send_claims "
            "where task_id = %s order by claimed_at, id",
            (task_id,),
        ).fetchall()
    return [_claim_from_row(row) for row in rows]
