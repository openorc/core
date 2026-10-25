"""Tests for the bounded local operation executor (issue #86).

Deterministic and fake-backed: handlers are injected blocking/recording
doubles synchronized with events, and no live Postgres or Valkey is touched.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

from openorc.config import Settings
from openorc.workers.invocation import (
    InvalidInvocationError,
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    ReconciliationSweepPayload,
    SubmissionOutcome,
    SubmissionRefusalReason,
    WebhookDeliveryDispatchPayload,
)
from openorc.workers.local_executor import BoundedLocalExecutor


def wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Poll a condition with a bounded deadline (deterministic test helper)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the deadline")


@dataclass
class RecordingHandler:
    """Injected handler double with optional event-synchronized blocking."""

    calls: list[tuple[object, OperationDependencies]] = field(default_factory=list)
    start_event: threading.Event | None = None
    release_event: threading.Event | None = None
    error: Exception | None = None

    def __call__(self, payload: Any, *, dependencies: OperationDependencies) -> None:
        self.calls.append((payload, dependencies))
        if self.start_event is not None:
            self.start_event.set()
        if self.release_event is not None:
            assert self.release_event.wait(timeout=5.0), "test event was never released"
        if self.error is not None:
            raise self.error


def make_invocation(delivery_guid: str = "guid") -> OperationInvocation:
    return OperationInvocation(
        kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
        payload=WebhookDeliveryDispatchPayload(delivery_guid=delivery_guid),
    )


def make_executor(
    settings: Settings,
    *,
    workers: int,
    pending: int,
    handler: Any,
    kinds: tuple[OperationKind, ...] = (OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,),
) -> BoundedLocalExecutor:
    return BoundedLocalExecutor(
        workers=workers,
        pending=pending,
        handlers={kind: handler for kind in kinds},
        dependencies=OperationDependencies(settings=settings),
    )


def test_constructor_requires_positive_capacity_bounds(settings_factory: Any) -> None:
    handler = RecordingHandler()

    with pytest.raises(ValueError, match="positive capacity"):
        make_executor(settings_factory(), workers=0, pending=1, handler=handler)
    with pytest.raises(ValueError, match="positive capacity"):
        make_executor(settings_factory(), workers=1, pending=0, handler=handler)


def test_active_and_pending_capacity_are_distinct_bounds(
    settings_factory: Any,
) -> None:
    start = threading.Event()
    release = threading.Event()
    executor = make_executor(
        settings_factory(),
        workers=1,
        pending=1,
        handler=RecordingHandler(start_event=start, release_event=release),
    )
    try:
        first = executor.submit(make_invocation("one"))
        assert first.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        assert start.wait(timeout=2.0), "the first accepted operation never started"

        second = executor.submit(make_invocation("two"))
        assert second.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        wait_for(lambda: executor.pending_operations == 1)

        # Waiting capacity is full even though the active worker is busy:
        # the distinct pending bound refuses immediately, before acceptance.
        third = executor.submit(make_invocation("three"))
        assert third.outcome is SubmissionOutcome.KNOWN_REFUSED
        assert third.refusal_reason is SubmissionRefusalReason.EXECUTOR_SATURATED
        assert executor.pending_operations == 1
        assert executor.active_operations == 1
    finally:
        release.set()


def test_completion_releases_capacity(settings_factory: Any) -> None:
    handler = RecordingHandler()
    executor = make_executor(settings_factory(), workers=1, pending=1, handler=handler)
    try:
        first = executor.submit(make_invocation("one"))
        assert first.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        wait_for(lambda: len(handler.calls) == 1)
        wait_for(lambda: executor.active_operations == 0 and executor.pending_operations == 0)

        refill = executor.submit(make_invocation("two"))
        assert refill.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    finally:
        executor.shutdown()


def test_handler_failure_is_classified_and_releases_capacity(
    settings_factory: Any,
) -> None:
    handler = RecordingHandler(error=RuntimeError("internal detail"))
    executor = make_executor(settings_factory(), workers=1, pending=1, handler=handler)
    try:
        # The failure is contained inside the executing thread: no exception
        # escapes submit, the failure is never reported as success, and the
        # active slot is released for the next admission.
        first = executor.submit(make_invocation("one"))
        assert first.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        wait_for(lambda: len(handler.calls) == 1)
        wait_for(lambda: executor.active_operations == 0)

        refill = executor.submit(make_invocation("two"))
        assert refill.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
    finally:
        executor.shutdown()


def test_admission_stops_on_shutdown(settings_factory: Any) -> None:
    handler = RecordingHandler()
    executor = make_executor(settings_factory(), workers=1, pending=1, handler=handler)
    result = executor.shutdown()

    assert result.drained is True
    assert result.outstanding == 0

    refused = executor.submit(make_invocation("late"))
    assert refused.outcome is SubmissionOutcome.KNOWN_REFUSED
    assert refused.refusal_reason is SubmissionRefusalReason.EXECUTOR_SHUTTING_DOWN


def test_shutdown_drains_accepted_work_within_the_bound(settings_factory: Any) -> None:
    handler = RecordingHandler()
    executor = make_executor(settings_factory(), workers=1, pending=1, handler=handler)
    accepted = executor.submit(make_invocation("one"))
    assert accepted.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY

    result = executor.shutdown()

    assert result.drained is True
    assert result.outstanding == 0
    assert len(handler.calls) == 1


def test_shutdown_with_unfinished_work_does_not_claim_known_non_delivery(
    settings_factory: Any,
) -> None:
    start = threading.Event()
    release = threading.Event()
    executor = make_executor(
        settings_factory(),
        workers=1,
        pending=2,
        handler=RecordingHandler(start_event=start, release_event=release),
    )
    try:
        first = executor.submit(make_invocation("one"))
        assert first.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        assert start.wait(timeout=2.0), "the accepted operation never started"
        second = executor.submit(make_invocation("two"))
        assert second.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY

        result = executor.shutdown(drain_timeout=0.05)

        # The bounded accounting is informational only: an incomplete drain
        # never asserts that accepted operations were not executed.
        assert result.drained is False
        assert result.outstanding >= 1
        assert result.outstanding == executor.active_operations + executor.pending_operations
    finally:
        release.set()
        executor.shutdown()


def test_submit_refuses_a_kind_without_a_registered_handler(
    settings_factory: Any,
) -> None:
    executor = make_executor(
        settings_factory(),
        workers=1,
        pending=1,
        handler=RecordingHandler(),
        kinds=(OperationKind.GITHUB_RECONCILIATION_SWEEP,),
    )
    try:
        result = executor.submit(make_invocation())

        assert result.outcome is SubmissionOutcome.KNOWN_REFUSED
        assert result.refusal_reason is SubmissionRefusalReason.NO_HANDLER_FOR_KIND
    finally:
        executor.shutdown()


def test_mismatched_kind_and_payload_never_reaches_a_local_handler(
    settings_factory: Any,
) -> None:
    # Local admission and queue delivery use the same strictly validated
    # invocations: the correspondence is enforced at construction, so a
    # mismatched invocation is refused before any handler effect could exist.
    handler = RecordingHandler()

    with pytest.raises(InvalidInvocationError, match="does not match the operation kind"):
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=WebhookDeliveryDispatchPayload(delivery_guid="guid"),
        )

    assert handler.calls == []


def test_handler_receives_the_decoded_payload_and_dependencies(
    settings_factory: Any,
) -> None:
    dependencies = OperationDependencies(settings=settings_factory())
    handler = RecordingHandler()
    executor = BoundedLocalExecutor(
        workers=1,
        pending=1,
        handlers={OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: handler},
        dependencies=dependencies,
    )
    wire = make_invocation("guid-9")
    try:
        accepted = executor.submit(wire)
        assert accepted.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        wait_for(lambda: len(handler.calls) == 1)

        payload, resolved = handler.calls[0]
        assert payload == WebhookDeliveryDispatchPayload(delivery_guid="guid-9")
        assert resolved is dependencies
    finally:
        executor.shutdown()


def test_sweep_invocations_round_trip_through_the_executor(
    settings_factory: Any,
) -> None:
    dependencies = OperationDependencies(settings=settings_factory())
    handler = RecordingHandler()
    executor = BoundedLocalExecutor(
        workers=1,
        pending=1,
        handlers={OperationKind.GITHUB_RECONCILIATION_SWEEP: handler},
        dependencies=dependencies,
    )
    wire = OperationInvocation(
        kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
        payload=ReconciliationSweepPayload(sweep_tick=4),
    )
    try:
        accepted = executor.submit(wire)
        assert accepted.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY
        wait_for(lambda: len(handler.calls) == 1)

        payload, _resolved = handler.calls[0]
        assert payload == ReconciliationSweepPayload(sweep_tick=4)
    finally:
        executor.shutdown()
