"""Deterministic per-session usage-observation integration behavior (issue #240).

The observer is best-effort plumbing between an attached Cline runtime and
the cost-accounting service. These tests prove the seam discipline with
deterministic fakes: aggregate-scope-only cost extraction, single-flight
coalescing of live ``USAGE_OBSERVED`` signals, no database transaction
across the SDK snapshot read, silent unknown on adapter failure, post-commit
interrupt dispatch through the shared E10b machinery, and lifecycle
idempotence — never any workflow decision made here.
"""

from __future__ import annotations

import threading
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from openorc.adapters.agent_runtime import AgentRuntimeError
from openorc.adapters.cline import ClineActivityKind, ClineActivitySnapshot
from openorc.adapters.cline.activity import ClineAccumulatedUsage
from openorc.services import task_abort, task_cost_accounting
from openorc.services.cline_usage_observation import (
    SessionUsageCostObserver,
    aggregate_snapshot_cost_usd,
)

_WS = uuid4()
_TASK = uuid4()
_SESSION = uuid4()
_CONN = uuid4()
_EXTERNAL = "fake-session-1"


class _FakeSubscription:
    def __init__(self) -> None:
        self.unsubscribed = False

    def unsubscribe(self) -> None:
        self.unsubscribed = True


class _FakeAdapter:
    """Duck-typed adapter seam recording subscriptions and snapshot reads."""

    def __init__(self, snapshot: ClineActivitySnapshot | None = None) -> None:
        self.subscriptions: list[tuple[str, Any]] = []
        self.snapshot_reads = 0
        self._snapshot = snapshot
        self._subscription: _FakeSubscription | None = None

    def subscribe_activity(self, session_id: str, listener: Any) -> _FakeSubscription:
        self.subscriptions.append((session_id, listener))
        self._subscription = _FakeSubscription()
        return self._subscription

    def read_activity_snapshot(self, session_id: str) -> Any:  # noqa: ANN401 - test fake
        self.snapshot_reads += 1
        return self._snapshot

    def emit_usage(self) -> None:
        assert self.subscriptions, "no subscription to deliver to"
        _, listener = self.subscriptions[0]
        listener(SimpleNamespace(kind=ClineActivityKind.USAGE_OBSERVED))


def _snapshot(
    *, aggregate: ClineAccumulatedUsage | None = None, usage: ClineAccumulatedUsage | None = None
) -> ClineActivitySnapshot:
    return ClineActivitySnapshot(session_id=_EXTERNAL, usage=usage, aggregate_usage=aggregate)


def _observer(adapter: _FakeAdapter) -> SessionUsageCostObserver:
    # The service seams are monkeypatched at module level in these tests, so
    # the pool is only ever passed through and never used.
    return SessionUsageCostObserver(
        SimpleNamespace(),  # type: ignore[arg-type]
        adapter,  # type: ignore[arg-type]
        workspace_id=_WS,
        task_id=_TASK,
        task_agent_session_id=_SESSION,
        connection_id=_CONN,
        external_session_id=_EXTERNAL,
    )


def test_aggregate_extraction_uses_only_the_aggregate_scope() -> None:
    # The session-wide aggregate scope is the only cost source; a root-only
    # reading is unknown, never substituted for the Task-session aggregate.
    assert aggregate_snapshot_cost_usd(
        _snapshot(
            aggregate=ClineAccumulatedUsage(total_cost=0.2),
            usage=ClineAccumulatedUsage(total_cost=5.0),
        )
    ) == Decimal("0.2")
    root_only = _snapshot(usage=ClineAccumulatedUsage(total_cost=5.0))
    assert aggregate_snapshot_cost_usd(root_only) is None
    assert aggregate_snapshot_cost_usd(_snapshot()) is None
    no_reading = _snapshot(aggregate=ClineAccumulatedUsage(input_tokens=10))
    assert aggregate_snapshot_cost_usd(no_reading) is None
    # A malformed non-numeric total is unusable rather than invented.
    malformed = SimpleNamespace(aggregate_usage=SimpleNamespace(total_cost="oops"))
    assert aggregate_snapshot_cost_usd(malformed) is None  # type: ignore[arg-type]


def test_live_usage_observations_coalesce_into_single_flight_evaluations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Decimal | None] = []
    monkeypatch.setattr(
        task_cost_accounting,
        "observe_session_usage",
        lambda pool, **kwargs: (
            calls.append(kwargs["aggregate_total_cost"]),
            SimpleNamespace(interrupt_dispatches=()),
        )[1],
    )
    snapshot = _snapshot(aggregate=ClineAccumulatedUsage(total_cost=0.4))

    # The first evaluation blocks inside the SDK snapshot read (SDK I/O with
    # no database transaction open); arrivals during an evaluation coalesce
    # into exactly one trailing re-run.
    main_blocked = threading.Event()
    second_done = threading.Event()
    release = threading.Event()

    class _BlockingAdapter(_FakeAdapter):
        def read_activity_snapshot(self, session_id: str) -> Any:  # noqa: ANN401 - test fake
            self.snapshot_reads += 1
            if self.snapshot_reads == 1:
                main_blocked.set()
                assert release.wait(timeout=5)
            return self._snapshot

    blocking_adapter = _BlockingAdapter(snapshot=snapshot)
    observer = _observer(blocking_adapter)
    observer.start()

    def emit_second() -> None:
        assert main_blocked.wait(timeout=5)
        blocking_adapter.emit_usage()
        blocking_adapter.emit_usage()
        second_done.set()
        release.set()

    second = threading.Thread(target=emit_second)
    second.start()
    blocking_adapter.emit_usage()  # runs the first (blocked) evaluation inline
    second.join(timeout=5)

    # Three live signals produced exactly two evaluations: the in-flight
    # one and one trailing coalesced re-run.
    assert blocking_adapter.snapshot_reads == 2
    assert len(calls) == 2
    assert calls == [Decimal("0.4"), Decimal("0.4")]
    observer.dispose()


def test_non_usage_observations_never_request_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[bool] = []

    def _fail(**kwargs: Any) -> None:
        called.append(True)

    monkeypatch.setattr(task_cost_accounting, "observe_session_usage", _fail)
    adapter = _FakeAdapter()
    observer = _observer(adapter)
    observer.start()
    _, listener = adapter.subscriptions[0]
    listener(SimpleNamespace(kind=ClineActivityKind.TOOL_STARTED))
    listener(SimpleNamespace(kind=ClineActivityKind.RUNTIME_STATUS))

    assert adapter.snapshot_reads == 0
    assert called == []
    observer.dispose()


def test_snapshot_read_failure_is_a_silent_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail(**kwargs: Any) -> None:
        raise AssertionError("the service must not be called on an unknown reading")

    monkeypatch.setattr(task_cost_accounting, "observe_session_usage", _fail)

    def _raise(session_id: str) -> ClineActivitySnapshot:
        raise AgentRuntimeError("hub unavailable")

    adapter = _FakeAdapter()
    adapter.read_activity_snapshot = _raise  # type: ignore[method-assign]
    observer = _observer(adapter)
    observer.start()

    adapter.emit_usage()  # must not raise to the workflow interaction

    assert adapter.snapshot_reads == 0
    observer.dispose()


def test_blocked_observation_dispatches_registered_interrupts_post_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = SimpleNamespace(task_agent_session_id=_SESSION, workflow_role="producer")
    dispatches: list[tuple[Any, str]] = []
    monkeypatch.setattr(
        task_cost_accounting,
        "observe_session_usage",
        lambda pool, **kwargs: SimpleNamespace(
            interrupt_dispatches=((intent, "external-1"),),
        ),
    )
    monkeypatch.setattr(
        task_abort,
        "dispatch_registered_task_interrupt",
        lambda pool, adapter_, *, intent, external_session_id: dispatches.append(
            (intent, external_session_id)
        ),
    )
    adapter = _FakeAdapter(snapshot=_snapshot(aggregate=ClineAccumulatedUsage(total_cost=9.0)))
    observer = _observer(adapter)
    observer.start()

    adapter.emit_usage()

    assert dispatches == [(intent, "external-1")]
    observer.dispose()


def test_dispatch_failure_never_fails_the_workflow_interaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        task_cost_accounting,
        "observe_session_usage",
        lambda pool, **kwargs: SimpleNamespace(
            interrupt_dispatches=((SimpleNamespace(), "external-1"),),
        ),
    )

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("unexpected dispatch failure")

    monkeypatch.setattr(task_abort, "dispatch_registered_task_interrupt", _boom)
    adapter = _FakeAdapter(snapshot=_snapshot(aggregate=ClineAccumulatedUsage(total_cost=9.0)))
    observer = _observer(adapter)
    observer.start()

    adapter.emit_usage()  # best-effort: the durable intent records the truth

    observer.dispose()


def test_observe_now_checkpoints_without_subscription(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Decimal | None] = []
    monkeypatch.setattr(
        task_cost_accounting,
        "observe_session_usage",
        lambda pool, **kwargs: (
            calls.append(kwargs["aggregate_total_cost"]),
            SimpleNamespace(interrupt_dispatches=()),
        )[1],
    )
    adapter = _FakeAdapter(snapshot=_snapshot(aggregate=ClineAccumulatedUsage(total_cost=1.0)))
    observer = _observer(adapter)

    observer.observe_now()

    assert adapter.subscriptions == []
    assert adapter.snapshot_reads == 1
    assert calls == [Decimal("1.0")]


def test_start_and_dispose_are_idempotent_and_disposal_stops_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        task_cost_accounting,
        "observe_session_usage",
        lambda pool, **kwargs: SimpleNamespace(interrupt_dispatches=()),
    )
    adapter = _FakeAdapter(snapshot=_snapshot(aggregate=ClineAccumulatedUsage(total_cost=1.0)))
    observer = _observer(adapter)

    observer.start()
    observer.start()  # idempotent while active
    assert len(adapter.subscriptions) == 1

    observer.dispose()
    observer.dispose()  # idempotent
    assert adapter._subscription is not None
    assert adapter._subscription.unsubscribed is True

    adapter.emit_usage()  # disposed: no evaluation runs
    assert adapter.snapshot_reads == 0


def test_service_failure_is_contained_at_the_observer_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A transient persistence failure, an account-operational refusal, or
    # any other application-service error must stay best-effort at the
    # listener boundary: it never propagates into the adapter's subscribed
    # listener and never disrupts the workflow interaction. (Direct
    # application-service callers keep their typed errors untouched.)
    def _boom(pool: Any, **kwargs: Any) -> None:
        raise RuntimeError("transient persistence failure")

    monkeypatch.setattr(task_cost_accounting, "observe_session_usage", _boom)
    adapter = _FakeAdapter(snapshot=_snapshot(aggregate=ClineAccumulatedUsage(total_cost=1.0)))
    observer = _observer(adapter)
    observer.start()

    adapter.emit_usage()  # must not raise through the listener

    assert adapter.snapshot_reads == 1
    observer.dispose()


class _ReleaseHookLock:
    """A lock wrapper that fires an armed hook exactly once, right after a release.

    The deterministic seam for the evaluator handoff race: arming the hook
    before the evaluator's exit-decision critical section makes the very
    next release — the handoff — run a concurrent emit inline.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._hook: Any = None

    def arm(self, hook: Any) -> None:
        self._hook = hook

    def __enter__(self) -> _ReleaseHookLock:
        self._lock.acquire()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._lock.release()
        hook = self._hook
        if hook is not None:
            self._hook = None
            hook()


def test_the_evaluation_handoff_never_drops_a_pending_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Regression for the single-flight handoff race: the exit decision and
    # the in-evaluation reset are one atomic critical section. A usage
    # notification that arrives exactly in the handoff interval must be
    # handled by a newly admitted evaluator, never silently dropped while
    # an active turn may still be crossing its budget.
    calls: list[Decimal | None] = []
    monkeypatch.setattr(
        task_cost_accounting,
        "observe_session_usage",
        lambda pool, **kwargs: (
            calls.append(kwargs["aggregate_total_cost"]),
            SimpleNamespace(interrupt_dispatches=()),
        )[1],
    )
    snapshot = _snapshot(aggregate=ClineAccumulatedUsage(total_cost=0.4))
    main_blocked = threading.Event()
    second_read_started = threading.Event()
    release_first = threading.Event()
    release_second = threading.Event()

    class _BlockingAdapter(_FakeAdapter):
        def read_activity_snapshot(self, session_id: str) -> Any:  # noqa: ANN401 - test fake
            self.snapshot_reads += 1
            if self.snapshot_reads == 1:
                main_blocked.set()
                assert release_first.wait(timeout=5)
            elif self.snapshot_reads == 2:
                second_read_started.set()
                assert release_second.wait(timeout=5)
            return self._snapshot

    blocking_adapter = _BlockingAdapter(snapshot=snapshot)
    observer = _observer(blocking_adapter)
    hook_lock = _ReleaseHookLock()
    # The deterministic handoff seam: swap in a lock wrapper that fires an
    # armed hook exactly once, right after a release.
    observer._lock = hook_lock  # type: ignore[assignment]
    observer.start()

    def emit_second() -> None:
        # Coalesced into the first evaluator's trailing re-run; then, while
        # that trailing evaluation blocks in its SDK read, arm the handoff
        # hook and release it: the next lock release is the evaluator's
        # exit decision, and a notification arriving exactly there must be
        # handled by a newly admitted evaluator.
        assert main_blocked.wait(timeout=5)
        blocking_adapter.emit_usage()
        release_first.set()
        assert second_read_started.wait(timeout=5)
        hook_lock.arm(lambda: blocking_adapter.emit_usage())
        release_second.set()

    second = threading.Thread(target=emit_second)
    second.start()
    blocking_adapter.emit_usage()  # evaluation 1 (blocked), then trailing evaluation 2
    second.join(timeout=5)

    # Three evaluations ran: the blocked first, the trailing coalesced
    # second, and the handoff notification's own evaluation. With the
    # dropped-notification bug the handoff emit would only set a pending
    # flag nobody consumes and the read count would stay at two.
    assert blocking_adapter.snapshot_reads == 3
    assert len(calls) == 3
    observer.dispose()


def test_observer_identity_shape_fails_closed() -> None:
    adapter = _FakeAdapter()
    with pytest.raises(ValueError):
        SessionUsageCostObserver(
            SimpleNamespace(),  # type: ignore[arg-type]
            adapter,  # type: ignore[arg-type]
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK,
            task_agent_session_id=_SESSION,
            connection_id=_CONN,
            external_session_id=_EXTERNAL,
        )
    with pytest.raises(ValueError):
        SessionUsageCostObserver(
            SimpleNamespace(),  # type: ignore[arg-type]
            adapter,  # type: ignore[arg-type]
            workspace_id=_WS,
            task_id=_TASK,
            task_agent_session_id=_SESSION,
            connection_id=_CONN,
            external_session_id="   ",
        )
