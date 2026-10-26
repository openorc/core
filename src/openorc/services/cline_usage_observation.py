"""Best-effort per-session Cline usage observation integration (issue #240).

The small reusable per-session seam that wires an already-admitted Cline
attachment into the cost-accounting service. It is deliberately not a
workflow authority and not a scheduler: no timer, no parallel polling
engine, no unbounded callback threads, and no per-event database writes.

- One observer owns exactly one exact bound Task/role/Connection session on
  one attached ``ClineAdapter``. :meth:`SessionUsageCostObserver.start`
  subscribes to the adapter's typed ``USAGE_OBSERVED`` activity stream;
  :meth:`SessionUsageCostObserver.dispose` unsubscribes idempotently, and
  disposal never implies a workflow decision.
- Live ``USAGE_OBSERVED`` signals only REQUEST a cumulative re-read: the
  observer coalesces bursts through per-session single-flight evaluation —
  at most one evaluation runs at a time, arrivals during an evaluation
  collapse into one trailing re-run, and coalesced callbacks never flood
  the database (the high-water checkpoint makes repeated equal readings
  no-op writes).
- The cumulative ``read_activity_snapshot`` read is SDK I/O and happens
  with NO database transaction open; only its typed aggregate result is
  handed to ``openorc.services.task_cost_accounting.observe_session_usage``,
  which opens its own fresh short transaction. Only the runtime's
  session-wide ``aggregate_usage`` scope is a cost source: the root/
  lead-agent ``usage`` scope is never added to it and never substituted
  for a missing aggregate — a root-only reading is unknown, never the
  Task-session aggregate.
- Every evaluation is best-effort: an adapter failure, an unavailable
  reading, or a foreign/stale observation never fails the workflow
  interaction, never retries, and never fabricates a decision. A composed
  cost-limit block's interrupt dispatches are handled post-commit through
  the shared E10b dispatch machinery, exactly like the interactive Owner
  Abort's.
- Observers are constructed only over a durably bound session identity
  resolved by trusted composition code; the identity tuple is re-proven by
  the exact-scope checkpoint on every evaluation.
"""

from __future__ import annotations

import logging
import threading
from decimal import Decimal, InvalidOperation
from uuid import UUID

from openorc.adapters.agent_runtime import AgentRuntimeError
from openorc.adapters.cline import (
    ClineActivityKind,
    ClineActivityObservation,
    ClineActivitySnapshot,
    ClineAdapter,
    Subscription,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import task_abort, task_cost_accounting

__all__ = ["SessionUsageCostObserver", "aggregate_snapshot_cost_usd"]

logger = logging.getLogger(__name__)


def aggregate_snapshot_cost_usd(snapshot: ClineActivitySnapshot) -> Decimal | None:
    """Extract the aggregate USD total from one typed activity snapshot.

    ONLY the runtime's session-wide ``aggregate_usage`` scope is a cost
    source: the root/lead-agent ``usage`` scope is never added to it and
    never substituted for a missing aggregate — an absent scope or reading
    is unknown (``None``), and a malformed, non-numeric, or non-finite
    value is unusable rather than invented. The returned value is an exact
    decimal, never a binary float.
    """
    if snapshot.aggregate_usage is None:
        return None
    total = snapshot.aggregate_usage.total_cost
    if total is None:
        return None
    if isinstance(total, Decimal):
        value: Decimal | None = total
    elif isinstance(total, (int, float)) and not isinstance(total, bool):
        try:
            value = Decimal(str(total))
        except InvalidOperation:
            return None
    else:
        return None
    if not value.is_finite() or value < 0:
        return None
    return value


class SessionUsageCostObserver:
    """Best-effort usage observation over one exact attached Task/role session.

    The caller resolves the durable binding identity (trusted composition
    or an established session workflow) and owns the observer's lifecycle:
    ``start`` subscribes, ``dispose`` unsubscribes. Every evaluation goes
    through :func:`aggregate_snapshot_cost_usd` and the cost-accounting
    service; the observer itself decides nothing about workflow meaning.
    """

    def __init__(
        self,
        pool: DatabasePool,
        cline_adapter: ClineAdapter,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        if not isinstance(workspace_id, UUID):
            raise ValueError("workspace_id must be a UUID")
        if not isinstance(task_id, UUID):
            raise ValueError("task_id must be a UUID")
        if not isinstance(task_agent_session_id, UUID):
            raise ValueError("task_agent_session_id must be a UUID")
        if not isinstance(connection_id, UUID):
            raise ValueError("connection_id must be a UUID")
        if not isinstance(external_session_id, str) or not external_session_id.strip():
            raise ValueError("external_session_id must be a non-empty string")
        self._pool = pool
        self._cline_adapter = cline_adapter
        self._workspace_id = workspace_id
        self._task_id = task_id
        self._task_agent_session_id = task_agent_session_id
        self._connection_id = connection_id
        self._external_session_id = external_session_id
        # Per-session single-flight coalescing state. The lock guards the
        # flags only — never a database transaction and never an SDK call.
        self._lock = threading.Lock()
        self._subscription: Subscription | None = None
        self._closed = False
        self._in_evaluation = False
        self._pending = False

    def start(self) -> None:
        """Subscribe the live ``USAGE_OBSERVED`` listener (idempotent while active)."""
        with self._lock:
            if self._closed or self._subscription is not None:
                return
            self._subscription = self._cline_adapter.subscribe_activity(
                self._external_session_id, self._on_observation
            )

    def dispose(self) -> None:
        """Unsubscribe idempotently; disposal never implies a workflow decision."""
        with self._lock:
            if self._subscription is not None:
                self._subscription.unsubscribe()
                self._subscription = None
            self._closed = True

    def observe_now(self) -> None:
        """Run one cumulative checkpoint at a natural interaction/lease boundary.

        The same best-effort evaluation the live listener requests, invoked
        explicitly so workflow operations can checkpoint at session/lease
        boundaries without any timer or polling engine. Never raises to the
        workflow caller.
        """
        self._evaluate_once()

    def _on_observation(self, observation: ClineActivityObservation) -> None:
        # Only usage observations request a cumulative re-read; every other
        # activity kind is projection material, not a cost signal.
        if observation.kind is not ClineActivityKind.USAGE_OBSERVED:
            return
        self._evaluate_once()

    def _evaluate_once(self) -> None:
        with self._lock:
            if self._closed or self._in_evaluation:
                self._pending = True
                return
            self._in_evaluation = True
        try:
            while True:
                self._run_evaluation()
                with self._lock:
                    again = self._pending and not self._closed
                    self._pending = False
                if not again:
                    break
        finally:
            with self._lock:
                self._in_evaluation = False

    def _run_evaluation(self) -> None:
        # The snapshot read is SDK I/O with no database transaction open;
        # only the typed aggregate value crosses into the service's own
        # fresh short transaction.
        try:
            snapshot = self._cline_adapter.read_activity_snapshot(self._external_session_id)
        except AgentRuntimeError as error:
            logger.warning(
                "cline usage snapshot read failed (%s) for task session %s",
                type(error).__name__,
                self._task_agent_session_id,
            )
            return
        observation = task_cost_accounting.observe_session_usage(
            self._pool,
            workspace_id=self._workspace_id,
            task_id=self._task_id,
            task_agent_session_id=self._task_agent_session_id,
            connection_id=self._connection_id,
            aggregate_total_cost=aggregate_snapshot_cost_usd(snapshot),
        )
        self._handle_dispatches(observation)

    def _handle_dispatches(self, observation: task_cost_accounting.SessionCostObservation) -> None:
        # Post-commit interrupt dispatch through the shared E10b machinery,
        # exactly like the interactive Owner Abort. A dispatch failure is
        # recorded on the durable intent by that machinery; an unexpected
        # error stays a best-effort observation and never fails the
        # workflow interaction — the durable intent is the truth.
        for intent, external_session_id in observation.interrupt_dispatches:
            try:
                task_abort.dispatch_registered_task_interrupt(
                    self._pool,
                    self._cline_adapter,
                    intent=intent,
                    external_session_id=external_session_id,
                )
            except Exception as error:  # noqa: BLE001 - best-effort boundary
                logger.warning(
                    "cost-limit interrupt dispatch ended unexpectedly (%s) for task session %s",
                    type(error).__name__,
                    self._task_agent_session_id,
                )
