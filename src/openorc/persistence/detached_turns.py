"""Repositories for the durable detached-turn correlation/settlement seam
(detached agent turns, issues #322/#348).

Explicit SQL repositories over the ``openorc`` schema for
``openorc.task_hub_run_receipts`` — the durable claim-to-turn receipt binding
an authorized one-shot claim identity to the exact authoritative
``TaskAgentSession`` binding and, after admission, to the acknowledged opaque
turn reference — and for ``openorc.task_hub_run_early_completions``,
the smallest safe handoff for a terminal observation that arrives before its
receipt's acknowledgement is persisted. The historical table and column names
are retained deliberately as stable physical encoding (issue #348): no
runtime-specific schema change is ever required by the detached-turn
boundary.

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
:mod:`openorc.domain.detached_turns`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary, and outcomes pass through
the shared domain canonicalizer with an explicit ``Jsonb`` adapter. No
external call ever occurs inside these transactions.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.detached_turns import (
    DetachedTurnObservationKind,
    TaskDetachedTurnEarlyCompletion,
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.intents import canonical_intent_outcome
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import canonical_effective_config_snapshot
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

# The stable storage encoding of the neutral terminal observation kinds:
# OpenOrc's own durable tokens, unchanged since the receipts' introduction
# (issue #322) and mapped at this boundary so a runtime integration's native
# event vocabulary can never reach the stored encoding (issue #348).
_OBSERVATION_KIND_TOKENS: Final[Mapping[DetachedTurnObservationKind, str]] = {
    DetachedTurnObservationKind.COMPLETED: "run.completed",
    DetachedTurnObservationKind.FAILED: "run.failed",
}
_OBSERVATION_KINDS_BY_TOKEN: Final[Mapping[str, DetachedTurnObservationKind]] = {
    token: kind for kind, token in _OBSERVATION_KIND_TOKENS.items()
}

__all__ = [
    "attach_task_detached_turn_acknowledgement",
    "consume_task_detached_turn_early_completion",
    "create_task_detached_turn_receipt",
    "find_outstanding_session_initialization_receipt",
    "get_last_consumed_turn_ordering_marker",
    "get_latest_session_initialization_receipt",
    "get_outstanding_task_detached_turn_receipt",
    "get_outstanding_task_detached_turn_receipt_for_update",
    "get_task_detached_turn_early_completion_for_update",
    "get_task_detached_turn_receipt",
    "get_task_detached_turn_receipt_for_update",
    "list_outstanding_session_initialization_receipts",
    "mark_task_detached_turn_receipt_contradicted",
    "refuse_task_detached_turn_receipt",
    "settle_task_detached_turn_receipt",
    "stage_task_detached_turn_early_completion",
]

_RECEIPT_COLUMNS = (
    "id, workspace_id, task_id, task_agent_session_id, operation, "
    "original_attempt_kind, original_attempt_id, expected_formal_family, "
    "run_id, status, outcome, contradicted_at, claimed_at, admitted_at, "
    "settled_at, updated_at, created_external_session_id, "
    "effective_config_snapshot, created_session_provenance"
)

# The same receipt projection aliased for the establishment settlement's
# binding join, where the authoritative ``task_agent_sessions`` row shares
# the statement.
_RECEIPT_COLUMNS_ALIASED = (
    "r.id, r.workspace_id, r.task_id, r.task_agent_session_id, r.operation, "
    "r.original_attempt_kind, r.original_attempt_id, r.expected_formal_family, "
    "r.run_id, r.status, r.outcome, r.contradicted_at, r.claimed_at, "
    "r.admitted_at, r.settled_at, r.updated_at, r.created_external_session_id, "
    "r.effective_config_snapshot, r.created_session_provenance"
)

_EARLY_COMPLETION_COLUMNS = (
    "id, workspace_id, task_id, task_hub_run_receipt_id, kind, sequence, "
    "result_candidate, staged_at"
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"detached-turn receipt {name} must be a UUID")


def _require_stored_observation_kind(token: object) -> DetachedTurnObservationKind:
    """Translate one stored kind token into its neutral observation kind."""
    kind = _OBSERVATION_KINDS_BY_TOKEN.get(token) if isinstance(token, str) else None
    if kind is None:
        raise ValueError("stored detached-turn early-completion kind is not a known token")
    return kind


def _receipt_from_row(row: Sequence[Any]) -> TaskDetachedTurnReceipt:
    return TaskDetachedTurnReceipt(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        task_agent_session_id=row[3],
        operation=PendingTaskRuntimeInteractionOperation(row[4]),
        original_attempt_kind=OriginalAttemptKind(row[5]),
        original_attempt_id=row[6],
        expected_formal_family=row[7],
        admitted_turn_reference=row[8],
        status=TaskDetachedTurnReceiptStatus(row[9]),
        outcome=row[10],
        contradicted_at=None if row[11] is None else normalize_utc(row[11]),
        claimed_at=normalize_utc(row[12]),
        admitted_at=None if row[13] is None else normalize_utc(row[13]),
        settled_at=None if row[14] is None else normalize_utc(row[14]),
        updated_at=normalize_utc(row[15]),
        created_external_session_id=row[16],
        effective_config_snapshot=row[17],
        created_session_provenance=row[18],
    )


def _early_completion_from_row(row: Sequence[Any]) -> TaskDetachedTurnEarlyCompletion:
    return TaskDetachedTurnEarlyCompletion(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        task_detached_turn_receipt_id=row[3],
        kind=_require_stored_observation_kind(row[4]),
        ordering_marker=row[5],
        result_candidate=row[6],
        staged_at=normalize_utc(row[7]),
    )


def _canonical_outcome(outcome: Mapping[str, object]) -> dict[str, object]:
    return dict(canonical_intent_outcome(outcome))


def create_task_detached_turn_receipt(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_agent_session_id: UUID,
    operation: PendingTaskRuntimeInteractionOperation,
    original_attempt_kind: OriginalAttemptKind,
    original_attempt_id: UUID,
    expected_formal_family: str,
    created_external_session_id: str | None = None,
    effective_config_snapshot: Mapping[str, object] | None = None,
    created_session_provenance: Mapping[str, object] | None = None,
) -> TaskDetachedTurnReceipt:
    """Insert one claimed Hub-run receipt exactly once, before any enqueue.

    Composed inside the owning service's claim transaction, this is the
    receipt companion of the committed one-shot claim. The partial unique
    index of at most one unsettled receipt per session binding is the
    durable single-outstanding-run backstop: a concurrent second dispatch
    for the same session raises ``UniqueViolation`` for the service layer to
    translate. The composite subject foreign keys keep every receipt in the
    same Task/Workspace scope; violations surface as
    ``ForeignKeyViolation``. ``claimed_at`` is database-stamped.

    The detached session establishment (issue #324) passes its minimum
    separate provisional establishment evidence: the allocation-returned
    provisional external identity, the dispatch-composed non-secret
    effective configuration snapshot, and the optional runtime-reported
    provenance. Every other operation passes none; the table's conditional
    coherence CHECKs enforce the per-operation shape either way.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    _require_uuid(original_attempt_id, "original_attempt_id")
    if not isinstance(operation, PendingTaskRuntimeInteractionOperation):
        raise ValueError(
            "create_task_detached_turn_receipt requires a pending-interaction operation"
        )
    if not isinstance(original_attempt_kind, OriginalAttemptKind):
        raise ValueError("create_task_detached_turn_receipt requires an OriginalAttemptKind")
    if not isinstance(expected_formal_family, str) or not expected_formal_family.strip():
        raise ValueError("create_task_detached_turn_receipt requires a non-empty formal family")
    if created_external_session_id is not None and (
        not isinstance(created_external_session_id, str) or not created_external_session_id.strip()
    ):
        raise ValueError(
            "create_task_detached_turn_receipt requires a non-empty provisional external "
            "session identity when one is supplied"
        )
    if effective_config_snapshot is not None and not isinstance(effective_config_snapshot, Mapping):
        raise ValueError(
            "create_task_detached_turn_receipt requires a mapping effective configuration "
            "snapshot when one is supplied"
        )
    snapshot = (
        None
        if effective_config_snapshot is None
        else dict(canonical_effective_config_snapshot(effective_config_snapshot))
    )
    provenance = (
        None
        if created_session_provenance is None
        else dict(canonical_intent_outcome(created_session_provenance))
    )
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.task_hub_run_receipts "
            "(workspace_id, task_id, task_agent_session_id, operation, "
            "original_attempt_kind, original_attempt_id, expected_formal_family, "
            "created_external_session_id, effective_config_snapshot, "
            "created_session_provenance) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_RECEIPT_COLUMNS}",
            (
                workspace_id,
                task_id,
                task_agent_session_id,
                operation.value,
                original_attempt_kind.value,
                original_attempt_id,
                expected_formal_family,
                created_external_session_id,
                Jsonb(snapshot) if snapshot is not None else None,
                Jsonb(provenance) if provenance is not None else None,
            ),
        ).fetchone()
    assert row is not None
    return _receipt_from_row(row)


def get_task_detached_turn_receipt(
    pool: DatabasePool, *, receipt_id: UUID
) -> TaskDetachedTurnReceipt | None:
    """Read one Hub-run receipt by its exact identity, or ``None``."""
    _require_uuid(receipt_id, "receipt_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts where id = %s",
            (receipt_id,),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def get_task_detached_turn_receipt_for_update(
    pool: DatabasePool, *, receipt_id: UUID
) -> TaskDetachedTurnReceipt | None:
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


def get_outstanding_task_detached_turn_receipt(
    pool: DatabasePool, *, task_agent_session_id: UUID
) -> TaskDetachedTurnReceipt | None:
    """Return the session binding's single unsettled (claimed/admitted) receipt.

    The serial-correlation lookup: at most one unsettled OpenOrc-dispatched
    turn exists per exact authoritative session binding (the partial
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


def get_outstanding_task_detached_turn_receipt_for_update(
    pool: DatabasePool, *, task_agent_session_id: UUID
) -> TaskDetachedTurnReceipt | None:
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


def find_outstanding_session_initialization_receipt(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    connection_id: UUID,
    created_external_session_id: str,
) -> TaskDetachedTurnReceipt | None:
    """Resolve the outstanding initialization receipt of one provisional session.

    The detached session establishment's settlement lookup (issue #324): a
    CONNECTING binding deliberately carries no bound external identity, so
    an observed terminal event for an initializing session resolves through
    its outstanding ``session_initialization`` receipt's provisional
    ``created_external_session_id`` instead. The resolution joins the
    authoritative ``task_agent_sessions`` binding so the Workspace and
    Connection scope is enforced by the binding's own columns — an external
    identifier never matches across Connections. ``None`` means no
    outstanding initialization receipt in this scope carries the provisional
    identity: the observation is unowned and is classified by the calling
    service, never guessed here.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_uuid(connection_id, "connection_id")
    if not isinstance(created_external_session_id, str) or (
        not created_external_session_id.strip()
    ):
        raise ValueError(
            "created_external_session_id must be a non-empty opaque external-session identity"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS_ALIASED} "
            "from openorc.task_hub_run_receipts r "
            "join openorc.task_agent_sessions s "
            "on s.id = r.task_agent_session_id "
            "and s.task_id = r.task_id and s.workspace_id = r.workspace_id "
            "where r.workspace_id = %s and s.connection_id = %s "
            "and r.created_external_session_id = %s "
            "and r.operation = 'session_initialization' "
            "and r.status in ('claimed', 'admitted') "
            "order by r.claimed_at, r.id",
            (workspace_id, connection_id, created_external_session_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def get_latest_session_initialization_receipt(
    pool: DatabasePool,
    *,
    task_agent_session_id: UUID,
) -> TaskDetachedTurnReceipt | None:
    """Return the binding's most recent initialization receipt, any status.

    The claimed-bootstrap disposition's establishment provenance read (issue
    #324): a READY binding whose latest initialization receipt is settled
    was established by the detached machinery and is provably past its
    initialization; a READY binding with no initialization receipt at all
    was established by the blocking handshake (or predates the receipt
    vocabulary), and its post-establishment bootstrap work stays honestly
    unprovable. ``None`` means the binding never carried an initialization
    receipt.
    """
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            "where task_agent_session_id = %s "
            "and operation = 'session_initialization' "
            "order by claimed_at desc, id desc "
            "limit 1",
            (task_agent_session_id,),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def list_outstanding_session_initialization_receipts(
    pool: DatabasePool,
    *,
    task_id: UUID,
) -> list[TaskDetachedTurnReceipt]:
    """List a Task's outstanding initialization receipts, ordered stably.

    The pending-interaction projection's detached-establishment read (issue
    #324): the outstanding ``session_initialization`` receipts are the
    durable proof that enqueued initialization turns are still unobserved.
    Deterministically ordered for stable reads; an empty list is the honest
    no-outstanding-initialization fact.
    """
    _require_uuid(task_id, "task_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_RECEIPT_COLUMNS} from openorc.task_hub_run_receipts "
            "where task_id = %s "
            "and operation = 'session_initialization' "
            "and status in ('claimed', 'admitted') "
            "order by claimed_at, id",
            (task_id,),
        ).fetchall()
    return [_receipt_from_row(row) for row in rows]


def attach_task_detached_turn_acknowledgement(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    admitted_turn_reference: str,
) -> TaskDetachedTurnReceipt | None:
    """Attach one acknowledged opaque turn reference to a claimed receipt, once.

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
    if not isinstance(admitted_turn_reference, str) or not admitted_turn_reference.strip():
        raise ValueError("detached-turn receipt admitted_turn_reference must be a non-empty string")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_hub_run_receipts "
            "set status = 'admitted', run_id = %s, admitted_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s and status = 'claimed' "
            f"returning {_RECEIPT_COLUMNS}",
            (admitted_turn_reference, receipt_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def settle_task_detached_turn_receipt(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    outcome: Mapping[str, object],
) -> TaskDetachedTurnReceipt | None:
    """Advance one admitted receipt to ``settled`` exactly once.

    The terminal record of one consumed terminal observation routed through
    the owning services: applies only while the receipt is still ``admitted``
    and the addressed Task/Workspace scope matches, stamps ``settled_at``
    and the canonical safe outcome (the settlement label plus the consumed
    ordering marker when the observation carried one) together. ``None``
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
            "and contradicted_at is null "
            f"returning {_RECEIPT_COLUMNS}",
            (Jsonb(canonical_outcome), receipt_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def mark_task_detached_turn_receipt_contradicted(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
) -> TaskDetachedTurnReceipt | None:
    """Set the explicit contradictory-run-identity marker exactly once.

    An explicitly observed contradictory turn identity that concretely
    differs from the outstanding acknowledged reference makes that receipt's correlation
    unsafe: the set-once marker permanently blocks its settlement (the
    settlement CAS refuses a contradicted receipt), so the correlation stays
    honestly unresolved instead of inventing a match. ``None`` means the
    receipt is missing, out of scope, not admitted, or already marked — a
    rejected no-op that is never retried blindly.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.task_hub_run_receipts "
            "set contradicted_at = now(), updated_at = now() "
            "where id = %s and task_id = %s and workspace_id = %s "
            "and status = 'admitted' and contradicted_at is null "
            f"returning {_RECEIPT_COLUMNS}",
            (receipt_id, task_id, workspace_id),
        ).fetchone()
    return None if row is None else _receipt_from_row(row)


def get_last_consumed_turn_ordering_marker(
    pool: DatabasePool, *, task_agent_session_id: UUID
) -> int | None:
    """Return the session's highest consumed terminal ordering marker.

    The stale-replay evidence: every settled receipt of the binding records
    the durable ordering token of the terminal observation it consumed, in
    the runtime integration's own ordering semantics. A newly observed
    terminal evidence item the integration classifies as not newer than
    that marker is an already-consumed or stale duplicate notification,
    never a fresh completion. ``None`` means the session has no consumed
    ordering marker yet. The token's interpretation lives behind the
    runtime integration's classification (issue #348); this repository owns
    only its durable storage.
    """
    _require_uuid(task_agent_session_id, "task_agent_session_id")
    with transaction(pool) as conn:
        row = conn.execute(
            "select max((outcome ->> 'consumed_sequence')::bigint) "
            "from openorc.task_hub_run_receipts "
            "where task_agent_session_id = %s and status = 'settled' "
            "and outcome ? 'consumed_sequence'",
            (task_agent_session_id,),
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def refuse_task_detached_turn_receipt(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    refusal_kind: str,
) -> TaskDetachedTurnReceipt | None:
    """Advance one claimed receipt to ``refused`` exactly once.

    The truthful terminal record of a positively proven pre-effect admission
    refusal: the turn was never admitted, so it must not hold the
    session's single-outstanding slot. Applies only while the receipt is
    still ``claimed`` and the addressed Task/Workspace scope matches.
    ``None`` means the receipt is missing, out of scope, or already terminal
    — a rejected no-op that is never retried blindly.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(refusal_kind, str) or not refusal_kind.strip():
        raise ValueError("detached-turn receipt refusal_kind must be a non-empty string")
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


def stage_task_detached_turn_early_completion(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
    kind: DetachedTurnObservationKind,
    ordering_marker: int | None,
    result_candidate: str | None,
) -> TaskDetachedTurnEarlyCompletion:
    """Stage one early terminal observation on its claimed receipt.

    The admission-race handoff: the observation arrived while the receipt
    was still claimed (acknowledgement not yet persisted). The insert is
    unique per receipt — a second terminal delivery for the same single
    outstanding turn raises ``UniqueViolation`` and is classified by the
    service layer as a duplicate, never consumed twice.
    """
    _require_uuid(receipt_id, "receipt_id")
    _require_uuid(task_id, "task_id")
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(kind, DetachedTurnObservationKind):
        raise ValueError("stage_task_detached_turn_early_completion requires an observation kind")
    if ordering_marker is not None and type(ordering_marker) is not int:
        raise ValueError(
            "stage_task_detached_turn_early_completion requires an integer ordering marker"
        )
    if result_candidate is not None and (
        not isinstance(result_candidate, str) or not result_candidate.strip()
    ):
        raise ValueError(
            "stage_task_detached_turn_early_completion requires a non-empty result candidate"
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
                _OBSERVATION_KIND_TOKENS[kind],
                ordering_marker,
                result_candidate,
            ),
        ).fetchone()
    assert row is not None
    return _early_completion_from_row(row)


def get_task_detached_turn_early_completion_for_update(
    pool: DatabasePool, *, receipt_id: UUID
) -> TaskDetachedTurnEarlyCompletion | None:
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


def consume_task_detached_turn_early_completion(
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
