"""Repositories for the durable Hub-run correlation/settlement seam
(detached agent turns, issue #322).

Explicit SQL repositories over the ``openorc`` schema for
``openorc.task_hub_run_receipts`` — the durable claim-to-run receipt binding
an authorized one-shot claim identity to the exact authoritative
``TaskAgentSession`` binding and, after admission, to the acknowledged
native Hub ``runId`` — and for ``openorc.task_hub_run_early_completions``,
the smallest safe handoff for a terminal observation that arrives before its
receipt's acknowledgement is persisted.

The receipt lifecycle is enforced by conditional statements only:

- the receipt insert is the pre-enqueue claim companion, committed inside
  the owning service's claim transaction; the partial unique index of at
  most one unsettled receipt per session binding is the durable
  single-outstanding-run backstop, and a violated uniqueness surfaces as
  ``UniqueViolation`` for the service layer to translate;
- the acknowledgement attach, settlement, and refusal are one-shot
  forward-only conditional transitions scoped to the addressed
  Task/Workspace and current status, so a raced or duplicated transition is
  a rejected no-op (``None``) that must never be retried blindly;
- the staged early completion is inserted only while its receipt is
  outstanding, is unique per receipt, and is consumed by an exactly-once
  delete composed inside the acknowledgement attach's settlement.

Rows map to transport-independent domain objects from
:mod:`openorc.domain.hub_runs`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary, and outcomes pass through
the shared domain canonicalizer with an explicit ``Jsonb`` adapter. No
external call ever occurs inside these transactions.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.hub_runs import (
    HubRunObservationKind,
    TaskHubRunEarlyCompletion,
    TaskHubRunReceipt,
    TaskHubRunReceiptStatus,
)
from openorc.domain.intents import canonical_intent_outcome
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "attach_task_hub_run_acknowledgement",
    "consume_task_hub_run_early_completion",
    "create_task_hub_run_receipt",
    "get_outstanding_task_hub_run_receipt",
    "get_outstanding_task_hub_run_receipt_for_update",
    "get_task_hub_run_early_completion_for_update",
    "get_task_hub_run_receipt",
    "get_task_hub_run_receipt_for_update",
    "refuse_task_hub_run_receipt",
    "settle_task_hub_run_receipt",
    "stage_task_hub_run_early_completion",
]

_RECEIPT_COLUMNS = (
    "id, workspace_id, task_id, task_agent_session_id, operation, "
    "original_attempt_kind, original_attempt_id, expected_formal_family, "
    "run_id, status, outcome, claimed_at, admitted_at, settled_at, updated_at"
)

_EARLY_COMPLETION_COLUMNS = (
    "id, workspace_id, task_id, task_hub_run_receipt_id, kind, sequence, "
    "result_candidate, staged_at"
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"hub-run receipt {name} must be a UUID")


def _receipt_from_row(row: Sequence[Any]) -> TaskHubRunReceipt:
    return TaskHubRunReceipt(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        task_agent_session_id=row[3],
        operation=PendingTaskRuntimeInteractionOperation(row[4]),
        original_attempt_kind=OriginalAttemptKind(row[5]),
        original_attempt_id=row[6],
        expected_formal_family=row[7],
        run_id=row[8],
        status=TaskHubRunReceiptStatus(row[9]),
        outcome=row[10],
        claimed_at=normalize_utc(row[11]),
        admitted_at=None if row[12] is None else normalize_utc(row[12]),
        settled_at=None if row[13] is None else normalize_utc(row[13]),
        updated_at=normalize_utc(row[14]),
    )


def _early_completion_from_row(row: Sequence[Any]) -> TaskHubRunEarlyCompletion:
    return TaskHubRunEarlyCompletion(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        task_hub_run_receipt_id=row[3],
        kind=HubRunObservationKind(row[4]),
        sequence=row[5],
        result_candidate=row[6],
        staged_at=normalize_utc(row[7]),
    )


def _canonical_outcome(outcome: Mapping[str, object]) -> dict[str, object]:
    return dict(canonical_intent_outcome(outcome))


def create_task_hub_run_receipt(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    operation: PendingTaskRuntimeInteractionOperation,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
    expected_formal_family: str,
) -> TaskHubRunReceipt:
    """Insert one claimed Hub-run receipt exactly once, before any enqueue.

    Composed inside the owning service's claim transaction, this is the
    receipt companion of the committed one-shot claim. The partial unique
    index of at most one unsettled receipt per session binding is the
    durable single-outstanding-run backstop: a concurrent second dispatch
    for the same session raises ``UniqueViolation`` for the service layer to
    translate. The composite subject foreign keys keep every receipt in the
    same Task/Workspace scope; violations surface as
    ``ForeignKeyViolation``. ``claimed_at`` is database-stamped.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    _require_uuid(original_attempt_id, "original_attempt_id")
    if not isinstance(operation, PendingTaskRuntimeInteractionOperation):
        raise ValueError("create_task_hub_run_receipt requires a pending-interaction operation")
    if not isinstance(original_attempt_kind, OriginalAttemptKind):
        raise ValueError("create_task_hub_run_receipt requires an OriginalAttemptKind")
    if not isinstance(expected_formal_family, str) or not expected_formal_family.strip():
        raise ValueError("create_task_hub_run_receipt requires a non-empty formal family")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_hub_run_receipts "
            "(workspace_id, task_id, task_agent_session_id, operation, "
            "original_attempt_kind, original_attempt_id, expected_formal_family) "
            "values (%s, %s, %s, %s, %s, %s, %s) "
            f"returning {_RECEIPT_COLUMNS}",
            (
                workspace_id,
                task_id,
                task_agent_session_id,
                operation.value,
                original_attempt_kind.value,
                original_attempt_id,
                expected_formal_family,
            ),
        ).fetchone()
    assert row is not None
    return _receipt_from_row(row)


def get_task_hub_run_receipt(pool: DatabasePool, *, receipt_id: UUID) -> TaskHubRunReceipt | None:
    """Read one Hub-run receipt by its exact identity, or ``None``."""
    _require_uuid(receipt_id, "receipt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts where id = %s",
            (receipt_id,),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def get_task_hub_run_receipt_for_update(
    pool: DatabasePool, *, receipt_id: UUID
) -> TaskHubRunReceipt | None:
    """Read one Hub-run receipt under an exclusive row lock.

    Serves the acknowledgement attach and the settlement routing: the
    forward-only transitions are conditional writes serialized by this row
    lock, so the classification observes the exact pre-image its own writes
    commit against.
    """
    _require_uuid(receipt_id, "receipt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            "where id = %s for update",
            (receipt_id,),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def get_outstanding_task_hub_run_receipt(
    pool: DatabasePool, *, task_agent_session_id: UUID
) -> TaskHubRunReceipt | None:
    """Return the session binding's single unsettled (claimed/admitted) receipt.

    The serial-correlation lookup: at most one unsettled OpenOrc-dispatched
    Hub run exists per exact authoritative session binding (the partial
    unique index is the durable backstop). ``None`` means no OpenOrc-run is
    outstanding for the binding — an observed terminal event for it is
    unowned and never attributed.
    """
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            "where task_agent_session_id = %s and status in ('claimed', 'admitted')",
            (task_agent_session_id,),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def get_outstanding_task_hub_run_receipt_for_update(
    pool: DatabasePool, *, task_agent_session_id: UUID
) -> TaskHubRunReceipt | None:
    """The locked variant of the serial-correlation lookup.

    The settlement routing locks the outstanding receipt so its forward-only
    settlement serializes against the acknowledgement attach and any
    concurrent delivery of the same observation.
    """
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            "where task_agent_session_id = %s and status in ('claimed', 'admitted') "
            "for update",
            (task_agent_session_id,),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def attach_task_hub_run_acknowledgement(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    run_id: str,
) -> TaskHubRunReceipt | None:
    """Attach one acknowledged native runId to a claimed receipt, exactly once.

    The one-shot CAS ``claimed`` → ``admitted``: applies only while the
    receipt is still claimed and the addressed Task/Workspace scope matches,
    stamps the database admission instant, and commits nothing else.
    ``None`` means the receipt is missing, out of scope, or no longer
    claimed — a rejected no-op that must never be retried blindly and never
    rewrites an already-attached identity.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("hub-run receipt run_id must be a non-empty string")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_hub_run_receipts "
            "set status = 'admitted', run_id = %s, admitted_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s and status = 'claimed' "
            f"returning {_RECEIPT_COLUMNS}",
            (run_id, receipt_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def settle_task_hub_run_receipt(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    outcome: Mapping[str, object],
) -> TaskHubRunReceipt | None:
    """Advance one admitted receipt to ``settled`` exactly once.

    The terminal record of one consumed terminal observation routed through
    the owning services: applies only while the receipt is still ``admitted``
    and the addressed Task/Workspace scope matches, stamps ``settled_at``
    and the canonical safe outcome (the settlement label plus the consumed
    sequence marker when the observation carried one) together. ``None``
    means the receipt is missing, out of scope, or already terminal — a
    rejected no-op that is never retried blindly. A settlement that never
    commits — a crash or a rolled-back finalize composition — leaves the
    receipt conservatively admitted.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    canonical_outcome = _canonical_outcome(outcome)
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_hub_run_receipts "
            "set status = 'settled', outcome = %s, settled_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s and status = 'admitted' "
            f"returning {_RECEIPT_COLUMNS}",
            (Jsonb(canonical_outcome), receipt_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def refuse_task_hub_run_receipt(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    refusal_kind: str,
) -> TaskHubRunReceipt | None:
    """Advance one claimed receipt to ``refused`` exactly once.

    The truthful terminal record of a positively proven pre-effect admission
    refusal: the run never became a Hub run, so it must not hold the
    session's single-outstanding slot. Applies only while the receipt is
    still ``claimed`` and the addressed Task/Workspace scope matches.
    ``None`` means the receipt is missing, out of scope, or already terminal
    — a rejected no-op that is never retried blindly.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(refusal_kind, str) or not refusal_kind.strip():
        raise ValueError("hub-run receipt refusal_kind must be a non-empty string")
    canonical_outcome = _canonical_outcome({"refusal_kind": refusal_kind})
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_hub_run_receipts "
            "set status = 'refused', outcome = %s, settled_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s and status = 'claimed' "
            f"returning {_RECEIPT_COLUMNS}",
            (Jsonb(canonical_outcome), receipt_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def stage_task_hub_run_early_completion(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    kind: HubRunObservationKind,
    sequence: int | None,
    result_candidate: str | None,
) -> TaskHubRunEarlyCompletion:
    """Stage one early terminal observation on its claimed receipt.

    The admission-race handoff: the observation arrived while the receipt
    was still claimed (acknowledgement not yet persisted). The insert is
    unique per receipt — a second terminal delivery for the same single
    outstanding run raises ``UniqueViolation`` and is classified by the
    service layer as a duplicate, never consumed twice.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(kind, HubRunObservationKind):
        raise ValueError("stage_task_hub_run_early_completion requires an observation kind")
    if sequence is not None and type(sequence) is not int:
        raise ValueError("stage_task_hub_run_early_completion requires an integer sequence")
    if result_candidate is not None and (
        not isinstance(result_candidate, str) or not result_candidate.strip()
    ):
        raise ValueError(
            "stage_task_hub_run_early_completion requires a non-empty result candidate"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_hub_run_early_completions "
            "(workspace_id, task_id, task_hub_run_receipt_id, kind, sequence, "
            "result_candidate) values (%s, %s, %s, %s, %s, %s) "
            f"returning {_EARLY_COMPLETION_COLUMNS}",
            (
                workspace_id,
                task_id,
                receipt_id,
                kind.value,
                sequence,
                result_candidate,
            ),
        ).fetchone()
    assert row is not None
    return _early_completion_from_row(row)


def get_task_hub_run_early_completion_for_update(
    pool: DatabasePool, *, receipt_id: UUID
) -> TaskHubRunEarlyCompletion | None:
    """Read the receipt's staged early completion under an exclusive row lock.

    Serves the acknowledgement attach's consumption: the staged observation
    is read, routed, and deleted inside the attach's settlement composition,
    so the exactly-once consumption serializes against any concurrent
    delivery.
    """
    _require_uuid(receipt_id, "receipt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_EARLY_COMPLETION_COLUMNS} from openorc.task_hub_run_early_completions "
            "where task_hub_run_receipt_id = %s for update",
            (receipt_id,),
        ).fetchone()
    return None if row is None else _early_completion_from_row(row)


def consume_task_hub_run_early_completion(
    pool: DatabasePool, *, completion_id: UUID, receipt_id: UUID
) -> bool:
    """Delete one staged early completion exactly once.

    The staged formal candidate is unvalidated agent-produced content held
    only for the acknowledgement handoff: once the observation has been
    routed (whatever its classification), the row is deleted so it never
    becomes permanent durable history. ``False`` means the row is missing or
    belongs to another receipt — a rejected no-op.
    """
    _require_uuid(completion_id, "completion_id")
    _require_uuid(receipt_id, "receipt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from openorc.task_hub_run_early_completions "
            "where id = %s and task_hub_run_receipt_id = %s returning id",
            (completion_id, receipt_id),
        ).fetchone()
    return row is not None
