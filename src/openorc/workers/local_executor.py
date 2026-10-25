"""Bounded local operation executor for regular execution mode (issue #86).

The serving API process owns exactly one executor: fixed bounded parallelism
(active workers) plus a separate bounded pending-admission capacity, so both
running and waiting work stay bounded. Admission refuses before acceptance
on saturation or shutdown with a typed known refusal — never a silent drop,
an indefinitely blocked HTTP request, or an unbounded thread per job.

Shutdown stops new admission first, then performs a bounded graceful
drain/accounting of accepted work. The drain outcome is informational only:
a timeout or process death never asserts that an accepted operation was not
executed, and F1 creates no durable claims that would require restart or
cutover reconciliation (later leaves, F2/F4).

Handler failures are contained with safe type classification (matching the
existing job discipline): arbitrary exception content never reaches
operational logs, a failure never turns into reported operation success,
and capacity is always released.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from openorc.observability import annotate_span, application_span
from openorc.workers.invocation import (
    OperationDependencies,
    OperationHandler,
    OperationInvocation,
    OperationKind,
    SubmissionRefusalReason,
    SubmissionResult,
)

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.workers.local_executor"

# The bounded graceful-drain default for executor shutdown: enough time for
# normal operation completion, deliberately finite. A drained-exceeded
# shutdown never claims anything about the outstanding operations' outcome.
DEFAULT_DRAIN_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class DrainResult:
    """The bounded drain accounting of one executor shutdown.

    Informational only: an incomplete drain never asserts that accepted
    operations were not executed and never implies safe retryability.
    """

    drained: bool
    outstanding: int


class BoundedLocalExecutor:
    """The bounded local executor owned by the serving process in regular mode.

    Handlers and process-local dependencies are resolved/executed inside
    this process; tests inject both. Submission is the regular-mode
    :class:`~openorc.workers.invocation.OperationSubmitter` seam.
    """

    def __init__(
        self,
        *,
        workers: int,
        pending: int,
        handlers: Mapping[OperationKind, OperationHandler],
        dependencies: OperationDependencies,
    ) -> None:
        if workers < 1 or pending < 1:
            raise ValueError("the bounded local executor requires positive capacity bounds")
        self._handlers: dict[OperationKind, OperationHandler] = dict(handlers)
        self._dependencies = dependencies
        # Active parallelism is bounded by the pool; waiting work is bounded
        # by the pending-admission gate below. Both bounds are distinct.
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="openorc-operation")
        self._pending_capacity = pending
        self._condition = threading.Condition()
        self._active = 0
        self._pending = 0
        self._shutting_down = False

    @property
    def active_operations(self) -> int:
        """The number of operations currently executing (diagnostics/tests)."""
        with self._condition:
            return self._active

    @property
    def pending_operations(self) -> int:
        """The number of accepted operations waiting to start (diagnostics/tests)."""
        with self._condition:
            return self._pending

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        """Admit one invocation, or refuse before acceptance.

        Refusal is immediate and typed: saturated pending capacity, an
        executor shutting down, or a kind with no registered handler. The
        transport makes no Task-state or authorization decision.
        """
        with self._condition:
            if self._shutting_down:
                return SubmissionResult.refused(SubmissionRefusalReason.EXECUTOR_SHUTTING_DOWN)
            if invocation.kind not in self._handlers:
                return SubmissionResult.refused(SubmissionRefusalReason.NO_HANDLER_FOR_KIND)
            if self._pending >= self._pending_capacity:
                return SubmissionResult.refused(SubmissionRefusalReason.EXECUTOR_SATURATED)
            self._pending += 1
        try:
            self._pool.submit(self._execute, invocation)
        except RuntimeError:
            # The pool can only be shut down by this process's own shutdown
            # path; treat the late race as a refusal, never as acceptance.
            with self._condition:
                self._pending -= 1
            return SubmissionResult.refused(SubmissionRefusalReason.EXECUTOR_SHUTTING_DOWN)
        return SubmissionResult.accepted()

    def shutdown(self, *, drain_timeout: float = DEFAULT_DRAIN_TIMEOUT_SECONDS) -> DrainResult:
        """Stop new admission, then attempt a bounded graceful drain.

        Accepted-but-unfinished work is never reclassified as a known
        non-delivery: the returned accounting is informational only.
        """
        with self._condition:
            self._shutting_down = True
            drained = self._condition.wait_for(
                lambda: self._active + self._pending == 0, timeout=drain_timeout
            )
            outstanding = self._active + self._pending
        self._pool.shutdown(wait=drained)
        if not drained:
            logger.warning(
                "bounded local executor shut down with %d accepted operation(s) "
                "still admitted; their outcome is not asserted",
                outstanding,
            )
        return DrainResult(drained=drained, outstanding=outstanding)

    def _execute(self, invocation: OperationInvocation) -> None:
        """Run one admitted operation, containing failure with safe classification."""
        with self._condition:
            self._pending -= 1
            self._active += 1
        try:
            with application_span(
                _TRACER_SCOPE, f"local_executor.execute_operation.{invocation.kind.value}"
            ) as span:
                annotate_span(span, operation=invocation.kind.value)
                self._handlers[invocation.kind](invocation.payload, dependencies=self._dependencies)
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            # The span boundary classified the failure safely; arbitrary
            # exception content never reaches operational logs, and the
            # failure never turns into reported operation success.
            logger.warning(
                "local operation execution did not complete (error type %s)",
                type(error).__name__,
            )
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()
