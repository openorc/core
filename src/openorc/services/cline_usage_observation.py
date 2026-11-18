"""Best-effort per-session usage observation and cost evaluation (issue #240).

The small reusable per-session seam that wires a live usage signal into the
cost-accounting service. It is deliberately not a workflow authority and not
a scheduler: no timer, no parallel polling engine, no unbounded callback
threads, and no per-event database writes.

- One evaluation engine (:class:`SessionCostEvaluationDriver`) owns the
  per-session single-flight evaluation mechanics for one exact bound
  Task/role/Connection session identity, parameterized by three trusted
  caller seams: the cumulative aggregate usage reading, the autonomous-turn
  attestation sampled per evaluation, and the post-commit Interrupt
  dispatch. The lease-attached observer (:class:`SessionUsageCostObserver`)
  and the detached-turn observation host (issue #341) compose the same
  engine with their own seams — the evaluation order, coalescing, and
  best-effort containment are identical everywhere, and every cost/budget
  decision stays in ``task_cost_accounting`` and the shared E10b machinery.
- Live usage signals only REQUEST a cumulative re-read: the driver coalesces
  bursts through per-session single-flight evaluation — at most one
  evaluation runs at a time, arrivals during an evaluation collapse into one
  trailing re-run, the exit decision and the evaluator slot reset are one
  atomic critical section so a notification can never be dropped in the
  handoff interval, and coalesced callbacks never flood the database (the
  high-water checkpoint makes repeated equal readings no-op writes).
- The cumulative usage read is SDK I/O and happens with NO database
  transaction open; only its typed aggregate value is handed to
  ``openorc.services.task_cost_accounting.observe_session_usage``, which
  opens its own fresh short transaction. The attestation is sampled AFTER
  the reading and immediately before the service call. Only the runtime's
  session-wide ``aggregate_usage`` scope is a cost source: the root/
  lead-agent ``usage`` scope is never added to it and never substituted for
  a missing aggregate — a root-only reading is unknown, never the
  Task-session aggregate.
- Every evaluation is best-effort: an unavailable reading, a failed
  attestation, or a foreign/stale observation never fails the workflow
  interaction, never retries, and never fabricates a decision. A composed
  cost-limit block's interrupt dispatches are handled post-commit through
  the caller's dispatch seam (the shared E10b machinery), exactly like the
  interactive Owner Abort's.
- Evaluators are constructed only over a durably bound session identity
  resolved by trusted composition code; the identity tuple is re-proven by
  the exact-scope checkpoint on every evaluation.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from decimal import Decimal
from uuid import UUID

from openorc.adapters.cline import (
    ClineActivityKind,
    ClineActivityObservation,
    ClineActivitySnapshot,
    ClineAdapter,
    Subscription,
    aggregate_usage_cost_usd,
)
from openorc.domain.intents import TaskInterruptIntent
from openorc.persistence.pool import DatabasePool
from openorc.services import task_abort, task_cost_accounting

__all__ = [
    "SessionCostEvaluationDriver",
    "SessionUsageCostObserver",
    "aggregate_snapshot_cost_usd",
]

logger = logging.getLogger(__name__)

#: The trusted post-commit Interrupt dispatch seam: one registered exact-session
#: Interrupt intent, dispatched strictly after the blocking composition committed
#: with no database transaction open. The lease-attached observer binds the
#: shared E10b dispatch machinery over its own adapter; the detached-turn
#: observation host binds the composition-rooted dispatcher its process
#: composed.
InterruptDispatch = Callable[[TaskInterruptIntent, str], None]


def aggregate_snapshot_cost_usd(snapshot: ClineActivitySnapshot) -> Decimal | None:
    """Extract the aggregate USD total from one typed activity snapshot.

    ONLY the runtime's session-wide ``aggregate_usage`` scope is a cost
    source: the root/lead-agent ``usage`` scope is never added to it and
    never substituted for a missing aggregate — an absent scope or reading
    is unknown (``None``), and a malformed, non-numeric, or non-finite
    value is unusable rather than invented. The returned value is an exact
    decimal, never a binary float. The conversion itself is the pinned
    integration's own interpretation
    (:func:`openorc.adapters.cline.aggregate_usage_cost_usd`).
    """
    return aggregate_usage_cost_usd(snapshot.aggregate_usage)


class SessionCostEvaluationDriver:
    """The best-effort cumulative cost-evaluation engine of one exact session.

    Extracted from the lease-attached usage observer so the detached-turn
    observation host (issue #341) drives the identical evaluation from the
    process-resident observation lifetime while the dispatch worker is gone.
    The driver owns ONLY the evaluation mechanics — the single-flight
    coalescing, the sampling order (cumulative reading, then the
    autonomous-turn attestation immediately before the service call), and
    the best-effort containment. It decides nothing about workflow meaning:
    the cost rule lives in ``task_cost_accounting`` and the block
    composition in the shared E10b machinery.

    ``read_cumulative_usage`` returns the sanitized cumulative aggregate
    value (``None`` is unknown truth) and may raise — the evaluation is
    contained best-effort. ``attest_autonomous_turn`` returns whether the
    session's own autonomous model-backed turn is attested in flight right
    now; any failure degrades to ``False`` — enforcement is never attempted
    on unavailable evidence. ``dispatch_interrupt`` handles one composed
    block's post-commit Interrupt dispatches.
    """

    def __init__(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        read_cumulative_usage: Callable[[], Decimal | None],
        attest_autonomous_turn: Callable[[], bool],
        dispatch_interrupt: InterruptDispatch,
    ) -> None:
        if not isinstance(workspace_id, UUID):
            raise ValueError("workspace_id must be a UUID")
        if not isinstance(task_id, UUID):
            raise ValueError("task_id must be a UUID")
        if not isinstance(task_agent_session_id, UUID):
            raise ValueError("task_agent_session_id must be a UUID")
        if not isinstance(connection_id, UUID):
            raise ValueError("connection_id must be a UUID")
        if not callable(read_cumulative_usage):
            raise ValueError("read_cumulative_usage must be callable")
        if not callable(attest_autonomous_turn):
            raise ValueError("attest_autonomous_turn must be callable")
        if not callable(dispatch_interrupt):
            raise ValueError("dispatch_interrupt must be callable")
        self._pool = pool
        self._workspace_id = workspace_id
        self._task_id = task_id
        self._task_agent_session_id = task_agent_session_id
        self._connection_id = connection_id
        self._read_cumulative_usage = read_cumulative_usage
        self._attest_autonomous_turn = attest_autonomous_turn
        self._dispatch_interrupt = dispatch_interrupt
        # Per-session single-flight coalescing state. The lock guards the
        # flags only — never a database transaction and never an SDK call.
        self._lock = threading.Lock()
        self._closed = False
        self._in_evaluation = False
        self._pending = False

    def request_evaluation(self) -> None:
        """Request one coalesced cumulative evaluation (best-effort, never raises)."""
        with self._lock:
            if self._closed or self._in_evaluation:
                self._pending = True
                return
            self._in_evaluation = True
        try:
            while True:
                self._run_evaluation()
                with self._lock:
                    # The exit decision and the in-evaluation reset are ONE
                    # atomic critical section: a notification that arrives
                    # while an evaluation is running is either consumed by
                    # this evaluator's trailing re-run or handled by a newly
                    # admitted evaluator that sees the reset flag — it can
                    # never be dropped in a handoff interval between the two.
                    if self._closed or not self._pending:
                        self._in_evaluation = False
                        return
                    self._pending = False
        except BaseException:
            # A truly unexpected escape must not leave this session's
            # observation permanently stalled: release the evaluator slot
            # and re-raise. Notifications that arrived during the dying
            # evaluation keep their pending flag and are consumed by the
            # next admitted evaluator. The normal exit path above already
            # reset the flag atomically, so this handler never races it.
            with self._lock:
                self._in_evaluation = False
            raise

    def dispose(self) -> None:
        """Stop further evaluations; idempotent. Disposal implies no workflow decision."""
        with self._lock:
            self._closed = True

    def _run_evaluation(self) -> None:
        # The cumulative usage read is SDK I/O with no database transaction
        # open; only its typed aggregate value crosses into the service's
        # own fresh short transaction.
        try:
            aggregate_total_cost = self._read_cumulative_usage()
        except Exception as error:  # noqa: BLE001 - best-effort reading
            logger.warning(
                "the cumulative usage reading failed (%s) for task session %s",
                type(error).__name__,
                self._task_agent_session_id,
            )
            return
        # The autonomous-turn attestation (issue #291, F8f; issue #341):
        # sampled AFTER the usage read and immediately before the service
        # call, from the caller's trusted attestation seam. A turn that
        # completed between the usage signal and this moment reads
        # unattested — the idle crossing then checkpoints without blocking,
        # deferring enforcement to the next active autonomous turn. An
        # attestation failure degrades to unattested: enforcement is never
        # attempted on unavailable evidence.
        try:
            send_in_flight = bool(self._attest_autonomous_turn())
        except Exception as error:  # noqa: BLE001 - best-effort attestation
            logger.warning(
                "the autonomous-turn attestation read failed (%s) for task session %s",
                type(error).__name__,
                self._task_agent_session_id,
            )
            send_in_flight = False
        try:
            observation = task_cost_accounting.observe_session_usage(
                self._pool,
                workspace_id=self._workspace_id,
                task_id=self._task_id,
                task_agent_session_id=self._task_agent_session_id,
                connection_id=self._connection_id,
                aggregate_total_cost=aggregate_total_cost,
                send_in_flight=send_in_flight,
            )
        except Exception as error:  # noqa: BLE001 - best-effort boundary
            # A transient persistence failure, an account-operational
            # refusal, or any other service error is contained here: the
            # live observation stays best-effort and never disrupts the
            # workflow interaction through the caller's subscribed
            # listener. Only the safe exception type name is logged — never
            # arbitrary exception text. Direct application-service callers
            # keep their typed errors untouched; this boundary is the
            # driver's alone.
            logger.warning(
                "the usage cost observation failed (%s) for task session %s",
                type(error).__name__,
                self._task_agent_session_id,
            )
            return
        self._handle_dispatches(observation)

    def _handle_dispatches(self, observation: task_cost_accounting.SessionCostObservation) -> None:
        # Post-commit interrupt dispatch through the caller's trusted seam
        # (the shared E10b machinery), exactly like the interactive Owner
        # Abort. A dispatch failure is recorded on the durable intent by
        # that machinery; an unexpected error stays a best-effort
        # observation and never fails the workflow interaction — the
        # durable intent is the truth.
        for intent, external_session_id in observation.interrupt_dispatches:
            try:
                self._dispatch_interrupt(intent, external_session_id)
            except Exception as error:  # noqa: BLE001 - best-effort boundary
                logger.warning(
                    "cost-limit interrupt dispatch ended unexpectedly (%s) for task session %s",
                    type(error).__name__,
                    self._task_agent_session_id,
                )


class SessionUsageCostObserver:
    """Best-effort usage observation over one exact attached Task/role session.

    The caller resolves the durable binding identity (trusted composition
    or an established session workflow) and owns the observer's lifecycle:
    ``start`` subscribes, ``dispose`` unsubscribes. Every evaluation goes
    through the shared :class:`SessionCostEvaluationDriver` — composed here
    over this attachment's snapshot read, its own synchronous-call marker
    (issue #291, F8f), and the shared E10b dispatch machinery — and the
    cost-accounting service; the observer itself decides nothing about
    workflow meaning.
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
        self._external_session_id = external_session_id
        # The shared evaluation engine over this attachment's trusted seams:
        # the snapshot read (SDK I/O, no transaction open), this adapter's
        # own per-session autonomous-turn marker, and the shared E10b
        # dispatch machinery bound to the same adapter.
        self._driver = SessionCostEvaluationDriver(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=task_agent_session_id,
            connection_id=connection_id,
            read_cumulative_usage=self._read_snapshot_cost,
            attest_autonomous_turn=lambda: self._cline_adapter.has_send_in_flight(
                self._external_session_id
            ),
            dispatch_interrupt=self._dispatch_interrupt,
        )
        # The subscription guard. The lock guards the subscription handle
        # only — never a database transaction and never an SDK call.
        self._lock = threading.Lock()
        self._subscription: Subscription | None = None

    def _read_snapshot_cost(self) -> Decimal | None:
        return aggregate_snapshot_cost_usd(
            self._cline_adapter.read_activity_snapshot(self._external_session_id)
        )

    def _dispatch_interrupt(self, intent: TaskInterruptIntent, external_session_id: str) -> None:
        task_abort.dispatch_registered_task_interrupt(
            self._pool,
            self._cline_adapter,
            intent=intent,
            external_session_id=external_session_id,
        )

    def start(self) -> None:
        """Subscribe the live ``USAGE_OBSERVED`` listener (idempotent while active)."""
        with self._lock:
            if self._subscription is not None:
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
        self._driver.dispose()

    def observe_now(self) -> None:
        """Run one cumulative checkpoint at a natural interaction/lease boundary.

        The same best-effort evaluation the live listener requests, invoked
        explicitly so workflow operations can checkpoint at session/lease
        boundaries without any timer or polling engine. Never raises to the
        workflow caller.
        """
        self._driver.request_evaluation()

    def _on_observation(self, observation: ClineActivityObservation) -> None:
        # Only usage observations request a cumulative re-read; every other
        # activity kind is projection material, not a cost signal.
        if observation.kind is not ClineActivityKind.USAGE_OBSERVED:
            return
        self._driver.request_evaluation()
