"""Deterministic tests for the shared authoritative terminal finalization (issue #317).

Scripted SQL-connection seams, a stubbed workspace read, a stubbed terminal
event recorder, and a scripted cleanup runtime prove the finalizer's critical
paths on their ordinary suite layer:

- the authority recheck: only a committed, non-archived terminalization
  finalizes; a late or duplicate delivery after archival is the idempotent
  no-op and a missing commitment is a typed stale refusal;
- the one-shot cleanup claim: an owned attempt runs its bounded cleanup
  before the archival and settles the claim; a concurrently unsettled claim
  defers archival (``CLEANUP_IN_FLIGHT``) — a duplicate finalizer never ends
  a session, or releases capacity, mid-attempt; a settled claim never
  resends; a failed claim read classifies from a fresh durable read and the
  archival transaction rechecks the window under the Task row lock;
- the mandatory archival: the per-Workspace arbitration advisory lock is the
  transaction's first lock, the guarded archival commits exactly the
  committed outcome (with the trusted closed-unmerged terminal cause), the
  remaining active Producer and Reviewer bindings transition to ENDED in
  that order, and the terminal event carries the origin-selected truthful
  actor provenance — all in one composed transaction;
- the arrangement seam: a KNOWN-REFUSED delivery and an unwired seam fall
  back in-process to the SAME archive-only finalization; an uncertain or
  failing submission is contained, arranges nothing, and is never
  resubmitted.

No live Postgres, GitHub, or Cline access; real conditional-SQL
classification and atomicity are proven by the integration-marked suite.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services import task_terminal_finalization
from openorc.services.errors import StaleOperationError
from openorc.services.task_terminal_finalization import (
    TerminalFinalizationKind,
    TerminalFinalizationSubmissionOutcome,
    arrange_task_terminal_finalization,
    finalize_task_terminalization,
)
from tests.fakes.task_rows import task_row

_OBSERVED = datetime(2026, 11, 12, 12, 0, 0, tzinfo=UTC)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")
_OWNER_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_CONNECTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000004")


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("finalization tests never close pools")


def _pool(
    results: list[tuple[Any, ...] | None | Exception],
) -> tuple[DatabasePool, ScriptedConnection]:
    conn = ScriptedConnection(results)
    return cast(DatabasePool, FakePool(conn)), conn


def _committed_row(
    *,
    outcome: str = "cancelled",
    origin: str = "owner_cancelled",
    claimed: Any = None,
    settled: Any = None,
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        status="waiting_for_owner",
        source_requirements_fingerprint="a" * 64,
        terminalization_outcome=outcome,
        terminalization_origin=origin,
        terminalization_established_at=_OBSERVED,
        cleanup_send_claimed_at=claimed,
        cleanup_send_settled_at=settled,
    )


def _archived_row(
    *, outcome: str = "cancelled", origin: str = "owner_cancelled"
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        status=outcome,
        source_requirements_fingerprint="a" * 64,
        archived_at=_OBSERVED,
        terminalization_outcome=outcome,
        terminalization_origin=origin,
        terminalization_established_at=_OBSERVED,
    )


def _ended_session_row(*, role: str) -> tuple[Any, ...]:
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        f"{role}-external-session",
        "ended",
        {},
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


class _RecordingEvent:
    """The stubbed terminal-event recorder capturing the actor provenance."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self, _pool: Any, *, workspace_id: Any, task_id: Any, actor: Any, terminal_status: Any
    ) -> None:
        self.calls.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "actor_type": actor.actor_type,
                "actor_id": actor.actor_id,
                "terminal_status": terminal_status,
            }
        )


class _RecordingRuntime:
    """The stubbed cleanup realization seam recording each invocation."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, pool: Any, runtime: Any, *, workspace_id: Any, task_id: Any) -> Any:
        self.calls.append({"workspace_id": workspace_id, "task_id": task_id})
        return SimpleNamespace(disposition="cleanup_sent")


class _RecordingSubmission:
    def __init__(self, outcome: Any = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.outcome = outcome
        self.error: Exception | None = None

    def submit(self, *, workspace_id: UUID, task_id: UUID) -> Any:
        self.calls.append({"workspace_id": workspace_id, "task_id": task_id})
        if self.error is not None:
            raise self.error
        return self.outcome


def _install_seams(
    monkeypatch: pytest.MonkeyPatch,
    *,
    owner_profile_id: UUID = _OWNER_PROFILE_ID,
    event: _RecordingEvent | None = None,
) -> _RecordingEvent:
    """Stub the workspace read and the terminal-event recorder at module seams."""
    monkeypatch.setattr(
        task_terminal_finalization,
        "get_workspace",
        lambda _pool, workspace_id: SimpleNamespace(owner_profile_id=owner_profile_id),
    )
    recorder = event if event is not None else _RecordingEvent()
    monkeypatch.setattr(
        task_terminal_finalization.event_coordination,
        "record_task_terminal_event",
        recorder,
    )
    return recorder


def _archive_update_index(conn: ScriptedConnection) -> int:
    return next(
        index for index, (sql, _params) in enumerate(conn.executed) if "archived_at = now()" in sql
    )


# --- the mandatory archive-only archival ------------------------------------------


def test_an_archive_only_finalization_archives_the_committed_terminalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = _install_seams(monkeypatch)
    pool, conn = _pool(
        [
            _committed_row(),
            (True,),
            _committed_row(),
            _archived_row(),
            _ended_session_row(role="producer"),
            _ended_session_row(role="reviewer"),
        ]
    )

    outcome = finalize_task_terminalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert outcome.task is not None and outcome.task.status is TaskStatus.CANCELLED
    assert outcome.task.archived_at is not None
    assert [binding.role for binding in outcome.released_session_bindings] == [
        WorkflowRole.PRODUCER,
        WorkflowRole.REVIEWER,
    ]
    assert event.calls == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "actor_type": WorkflowEventActor.OWNER,
            "actor_id": str(_OWNER_PROFILE_ID),
            "terminal_status": TaskStatus.CANCELLED,
        }
    ]
    # The arbitration advisory lock is the transaction's FIRST lock (after
    # the Phase-A authority read only); the archival precedes the session
    # ends, which precede the event recorder.
    lock_index = next(
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "pg_advisory_xact_lock" in sql
    )
    assert lock_index == 1
    assert _archive_update_index(conn) < next(
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "task_agent_sessions" in sql and "lifecycle_status = 'ended'" in sql
    )


def test_a_task_without_active_bindings_archives_with_no_release_fact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    pool, conn = _pool(
        [
            _committed_row(),
            (True,),
            _committed_row(),
            _archived_row(),
            None,
            None,
            None,
            None,
        ]
    )

    outcome = finalize_task_terminalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert outcome.released_session_bindings == ()


def test_the_trusted_closed_unmerged_origin_archives_with_the_canonical_terminal_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = _install_seams(monkeypatch)
    pool, conn = _pool(
        [
            _committed_row(origin="github_closed_unmerged"),
            (True,),
            _committed_row(origin="github_closed_unmerged"),
            _archived_row(origin="github_closed_unmerged"),
            None,
            None,
            None,
            None,
        ]
    )

    outcome = finalize_task_terminalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    archive_params = conn.executed[_archive_update_index(conn)][1]
    assert archive_params is not None and archive_params[1] == "pr_closed_unmerged"
    assert event.calls[0]["actor_type"] is WorkflowEventActor.GITHUB
    assert event.calls[0]["actor_id"] is None


def test_the_confirmed_merge_origin_archives_completed_with_the_openorc_actor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = _install_seams(monkeypatch)
    pool, conn = _pool(
        [
            _committed_row(outcome="completed", origin="github_confirmed_merged"),
            (True,),
            _committed_row(outcome="completed", origin="github_confirmed_merged"),
            _archived_row(outcome="completed", origin="github_confirmed_merged"),
            None,
            None,
            None,
            None,
        ]
    )

    outcome = finalize_task_terminalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert outcome.task is not None and outcome.task.status is TaskStatus.COMPLETED
    archive_params = conn.executed[_archive_update_index(conn)][1]
    assert archive_params is not None and archive_params[1] is None
    assert event.calls[0]["actor_type"] is WorkflowEventActor.OPENORC

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert outcome.released_session_bindings == ()


# --- the authority recheck and the idempotent replays ------------------------------


def test_a_late_duplicate_after_archival_is_the_idempotent_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    pool, conn = _pool([_archived_row()])

    outcome = finalize_task_terminalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ALREADY_ARCHIVED
    assert outcome.task is not None and outcome.task.archived_at is not None
    assert outcome.released_session_bindings == ()
    assert conn.executed == [conn.executed[0]]  # the single authority read only


def test_a_missing_commitment_is_a_stale_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_seams(monkeypatch)
    pool, conn = _pool([task_row(id=_TASK_ID, workspace_id=_WORKSPACE_ID)])

    with pytest.raises(StaleOperationError, match="no committed terminalization"):
        finalize_task_terminalization(pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)

    assert len(conn.executed) == 1


# --- the one-shot cleanup claim lifecycle ------------------------------------------


def test_an_owned_cleanup_attempt_runs_before_the_archival_and_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    runtime = _RecordingRuntime()
    monkeypatch.setattr(task_terminal_finalization, "execute_terminal_checkout_cleanup", runtime)
    pool, conn = _pool(
        [
            _committed_row(),
            (_OBSERVED,),  # the one-shot claim CAS
            _committed_row(claimed=_OBSERVED, settled=_OBSERVED),  # the settle
            (True,),
            _committed_row(claimed=_OBSERVED, settled=_OBSERVED),
            _archived_row(),
            None,
            None,
            None,
            None,
        ]
    )

    outcome = finalize_task_terminalization(
        pool, cast(Any, object()), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert runtime.calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]
    # The bounded attempt (and its settle) complete strictly before the
    # mandatory archival.
    assert _archive_update_index(conn) > next(
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "cleanup_send_settled_at = now()" in sql
    )


def test_a_concurrently_unsettled_claim_defers_the_archival(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    runtime = _RecordingRuntime()
    monkeypatch.setattr(task_terminal_finalization, "execute_terminal_checkout_cleanup", runtime)
    pool, conn = _pool(
        [
            _committed_row(),
            None,  # the claim CAS: another finalizer owns the attempt
            _committed_row(claimed=_OBSERVED),  # the fresh read: unsettled
        ]
    )

    outcome = finalize_task_terminalization(
        pool, cast(Any, object()), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.CLEANUP_IN_FLIGHT
    assert runtime.calls == []
    assert conn.executed == [conn.executed[0], conn.executed[1], conn.executed[2]]


def test_an_unsettled_claim_blocks_the_archival_under_the_task_row_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    pool, conn = _pool(
        [
            _committed_row(claimed=_OBSERVED),  # archive-only mode sees the claim
            (True,),
            _committed_row(claimed=_OBSERVED),  # the locked re-read confirms it
        ]
    )

    outcome = finalize_task_terminalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.CLEANUP_IN_FLIGHT
    assert all("archived_at = now()" not in sql for sql, _params in conn.executed)


def test_a_failed_claim_read_classifies_from_a_fresh_durable_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    pool, conn = _pool(
        [
            _committed_row(),
            RuntimeError("claim read failed"),  # contained: unknown claim state
            _committed_row(),  # the fresh read: unclaimed → archive-only
            (True,),
            _committed_row(),
            _archived_row(),
            None,
            None,
            None,
            None,
        ]
    )

    outcome = finalize_task_terminalization(
        pool, cast(Any, object()), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED


def test_a_settled_claim_never_resends_and_proceeds_to_archival(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    runtime = _RecordingRuntime()
    monkeypatch.setattr(task_terminal_finalization, "execute_terminal_checkout_cleanup", runtime)
    pool, conn = _pool(
        [
            _committed_row(),
            None,  # the claim CAS: already claimed
            _committed_row(claimed=_OBSERVED, settled=_OBSERVED),  # fresh read: settled
            (True,),
            _committed_row(claimed=_OBSERVED, settled=_OBSERVED),
            _archived_row(),
            None,
            None,
            None,
            None,
        ]
    )

    outcome = finalize_task_terminalization(
        pool, cast(Any, object()), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert runtime.calls == []


# --- the arrangement seam -----------------------------------------------------------


def test_a_known_refused_submission_falls_back_to_the_archive_only_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    submission = _RecordingSubmission(outcome=TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED)
    pool, conn = _pool(
        [
            _committed_row(),
            (True,),
            _committed_row(),
            _archived_row(),
            None,
            None,
            None,
            None,
        ]
    )

    arrangement = arrange_task_terminal_finalization(
        pool, submission, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert arrangement.submission_outcome is TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED
    assert arrangement.finalization is not None
    assert arrangement.finalization.kind is TerminalFinalizationKind.ARCHIVED
    assert submission.calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_an_unwired_seam_falls_back_to_the_archive_only_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    pool, _conn = _pool(
        [
            _committed_row(),
            (True,),
            _committed_row(),
            _archived_row(),
            None,
            None,
            None,
            None,
        ]
    )

    arrangement = arrange_task_terminal_finalization(
        pool, None, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert arrangement.finalization is not None
    assert arrangement.finalization.kind is TerminalFinalizationKind.ARCHIVED


def test_an_uncertain_submission_arranges_nothing_and_never_resubmits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    submission = _RecordingSubmission(outcome=TerminalFinalizationSubmissionOutcome.UNCERTAIN)
    pool, conn = _pool([])

    arrangement = arrange_task_terminal_finalization(
        pool, submission, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert arrangement.submission_outcome is TerminalFinalizationSubmissionOutcome.UNCERTAIN
    assert arrangement.finalization is None
    assert submission.calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]
    assert conn.executed == []


def test_a_failing_submission_is_contained_as_uncertain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_seams(monkeypatch)
    submission = _RecordingSubmission()
    submission.error = RuntimeError("transport exploded")
    pool, conn = _pool([])

    arrangement = arrange_task_terminal_finalization(
        pool, submission, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert arrangement.submission_outcome is TerminalFinalizationSubmissionOutcome.UNCERTAIN
    assert arrangement.finalization is None
    assert conn.executed == []
