"""Detached E4 seam tests: Owner ↔ Reviewer discussion (issue #339).

A scripted SQL-connection seam plus the #69 fake Agent Runtime's detached
capability, a recording observation registrar, and a recording completion
handoff drive the real service composition. The suite proves the new
detached discussion behavior and its critical cross-boundary seams — not the
established review-result parser matrix or the owning E3c/E9 settlement
internals (their own suites keep those): prepare authorization and the
gate-bound receipt commit; the one-shot admission stamp and the verbatim
enqueue; the refusal and uncertainty paths; the settlement routing by gate
context; the status projection; and that no executed SQL ever carries the
message text.
"""

from __future__ import annotations

import uuid
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import patch as mock_patch

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.detached_turns import (
    OWNER_REVIEWER_DISCUSSION_FAMILY,
    SETTLEMENT_DISCUSSION_EXCHANGE,
    SETTLEMENT_DISCUSSION_FORMAL_CONTINUATION,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.pool import DatabasePool
from openorc.services import task_owner_reviewer_discussion as discussion
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    StaleOperationError,
)
from openorc.services.task_detached_turn_observation import ObservationRegistrationRefusedError
from openorc.services.task_owner_reviewer_discussion import (
    admit_owner_reviewer_discussion_turn,
    get_owner_gate_discussion_status,
    prepare_owner_reviewer_discussion,
    release_prepared_discussion_receipt,
    settle_observed_discussion_result,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    accepted_review_result_candidate,
    changes_requested_review_result_candidate,
)
from tests.fakes.task_rows import task_row

_OBSERVED = datetime(2026, 11, 16, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_OTHER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_CONNECTION_ID = uuid.uuid4()
_REVIEWER_SESSION_ID = uuid.uuid4()
_REVIEWER_EXTERNAL_ID = "fake-session-2"
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_GATE_2_ID = uuid.uuid4()
_PR_ID = uuid.uuid4()
_RECEIPT_ID = uuid.uuid4()
_HEAD_SHA = "b" * 40
_BASE_SHA = "c" * 40
_OWNER_MESSAGE = "Owner question: is the retry backoff sufficient for the failure mode?"
_DISCUSSION_EVENT_TYPE = "owner_reviewer_discussion_message"

_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception
_Predicate = Any


def _sql_has(*tokens: str) -> _Predicate:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


def _sql_lacks(predicate: _Predicate, *tokens: str) -> _Predicate:
    def wrapped(sql: str, params: tuple[Any, ...] | None) -> bool:
        return predicate(sql, params) and not any(token in sql for token in tokens)

    return wrapped


def _role_is(base: _Predicate, role: str) -> _Predicate:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return base(sql, params) and params is not None and len(params) > 1 and params[1] == role

    return predicate


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, _CannedResult, bool]] = []

    def on(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, False))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (predicate, result, once) in enumerate(self._handlers):
            if predicate(lowered, params):
                if once:
                    del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, list):
                    return FakeCursor(None, result)
                if isinstance(result, tuple):
                    return FakeCursor(result, None)
                return FakeCursor(None, None)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Any:
        # Nested psycopg transaction blocks (SAVEPOINTs under composition).
        yield


class FakePool:
    """Pool fake tracking checked-out connections for the transaction proof."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:  # noqa: ANN401 - test seam
        @contextmanager
        def managed() -> Any:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("discussion tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


# --- durable row shapes ---------------------------------------------------------


def _ws_row(*, owner: Any = _OWNER_PROFILE_ID) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner, "platform", _OBSERVED, _OBSERVED, 2, "", None)


def _task_row(
    *,
    status: str = "waiting_for_owner",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
    gate_id: Any = _GATE_1_ID,
    archived_at: Any = None,
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        status=status,
        archived_at=archived_at,
        state_token=state_token,
        current_plan_revision_id=current_plan_revision_id,
        current_owner_gate_id=gate_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _session_row(
    role: str,
    *,
    session_id: Any,
    external_id: str | None,
    lifecycle: str | None = None,
) -> tuple[Any, ...]:
    initialized = external_id is not None
    resolved_lifecycle = (
        lifecycle if lifecycle is not None else ("ready" if initialized else "connecting")
    )
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        resolved_lifecycle,
        {"adapter": "cline"} if initialized else None,
        None,
        None,
        None,
        _OBSERVED if initialized else None,
        _OBSERVED if resolved_lifecycle == "ended" else None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _revision_row(*, revision_id: Any = _REVISION_1_ID) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, 1, "# Plan", _BASE_SHA, _OBSERVED)


def _pr_row(*, head_sha: str = _HEAD_SHA) -> tuple[Any, ...]:
    return (
        _PR_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        555001,
        7,
        "feature/issue-42",
        "main",
        head_sha,
        "open",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _gate_row(
    *,
    gate_id: Any = _GATE_1_ID,
    gate_type: str = "review_resolution",
    plan_revision_id: Any | None = _REVISION_1_ID,
    status: str = "pending",
    head_sha: str | None = None,
    pr_id: Any | None = None,
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        plan_revision_id,
        head_sha,
        pr_id,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _reviewer_ready_row() -> tuple[Any, ...]:
    return _session_row(
        "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
    )


def _discussion_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
    outcome: dict[str, Any] | None = None,
    attempted: bool = False,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    if outcome is None:
        outcome = {"settlement": "validated_result"} if status == "settled" else {}
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REVIEWER_SESSION_ID,
        "owner_reviewer_discussion",
        "owner_gate_discussion",
        _GATE_1_ID,
        OWNER_REVIEWER_DISCUSSION_FAMILY,
        admitted_turn_reference,
        status,
        outcome if outcome is not None else {},
        None,
        _OBSERVED,
        admitted_at,
        settled_at,
        _OBSERVED,
        None,
        None,
        None,
        _OBSERVED if attempted else None,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "plan_reviewed",
        "reviewer",
        None,
        None,
        None,
        {},
        _OBSERVED,
    )


# --- SQL predicates -------------------------------------------------------------


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_PR_READ = _sql_has("from openorc.task_pull_requests where task_id")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_RECEIPT_INSERT = _sql_has("insert into openorc.task_hub_run_receipts")
_RECEIPT_FOR_UPDATE = _sql_lacks(
    _sql_has("from openorc.task_hub_run_receipts where id", "for update"),
    "admission_attempted_at is null",
)
_RECEIPT_STAMP = _sql_has(
    "update openorc.task_hub_run_receipts",
    "status = 'claimed' and admission_attempted_at is null",
)
_RECEIPT_ATTACH = _sql_has("update openorc.task_hub_run_receipts", "status = 'admitted'")
_RECEIPT_REFUSE = _sql_has("update openorc.task_hub_run_receipts", "status = 'refused'")


def _event_params_of_type(conn: ScriptedConnection, event_type: str) -> list[tuple[Any, ...]]:
    matched: list[tuple[Any, ...]] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None and params[2] == event_type:
            matched.append(params)
    return matched


def _assert_no_message_text_in_sql(conn: ScriptedConnection) -> None:
    """No executed SQL ever carries the Owner message text."""
    for sql, params in conn.executed:
        blob = sql + repr(params)
        assert _OWNER_MESSAGE not in blob


# --- test fixtures --------------------------------------------------------------


class _ReviewerAttachment:
    """Deferred Reviewer attachment probe: counts acquisitions and disposals."""

    def __init__(self, runtime: FakeAgentRuntimeAdapter, *, on_acquire: Any = None) -> None:
        self._runtime = runtime
        self._on_acquire = on_acquire
        self.acquisitions = 0
        self.disposals = 0
        self.bindings: list[Any] = []

    def __call__(self, binding: Any) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.bindings.append(binding)
        self.acquisitions += 1
        if self._on_acquire is not None:
            self._on_acquire()
        return self

    def __enter__(self) -> FakeAgentRuntimeAdapter:
        return self._runtime

    def __exit__(self, *_exc: object) -> bool:
        self.disposals += 1
        return False


class _ProbedAdmitAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving every detached admission runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._discussion_pool = pool

    def admit_detached_turn(self, session_id: str, content: str) -> Any:
        assert self._discussion_pool.active_connections == 0, (
            "the service must never hold a database transaction open across the admission"
        )
        return super().admit_detached_turn(session_id, content)


def _prepared_admit_adapter(pool: FakePool) -> _ProbedAdmitAdapter:
    """A fake runtime carrying the Task's two existing sessions.

    The Producer session is created first so the Reviewer session's external
    identity is exactly ``fake-session-2`` (``_REVIEWER_EXTERNAL_ID``), the
    admitted READY session the scripted binding rows carry.
    """
    from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
    from openorc.domain.connections import WorkflowRole
    from tests.fakes.agent_runtime import session_ready_candidate

    runtime = _ProbedAdmitAdapter(pool)
    runtime.queue_creation(session_ready_candidate())
    runtime.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    runtime.queue_creation(session_ready_candidate())
    reviewer = runtime.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.REVIEWER,
            initialization="prepared",
        )
    )
    assert reviewer.external_session_id == _REVIEWER_EXTERNAL_ID
    return runtime


class _Registrar:
    """Recording observation registrar; scripts refusal or unexpected failure."""

    def __init__(self, *, refused: bool = False, broken: bool = False) -> None:
        self._refused = refused
        self._broken = broken
        self.calls: list[dict[str, Any]] = []

    def register_session(self, **kwargs: Any) -> None:
        self.calls.append(dict(kwargs))
        if self._refused:
            from openorc.services.task_detached_turn_observation import (
                ObservationRegistrationRefusalReason,
            )

            raise ObservationRegistrationRefusedError(
                "refused by the scripted registrar",
                reason=ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value,
            )
        if self._broken:
            raise RuntimeError("unexpected registrar failure")


class _Handoff:
    """Recording post-commit completion handoff probe."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def handoff_settled_completion(self, **kwargs: Any) -> None:
        self.calls.append(dict(kwargs))


def _script_authorization(conn: ScriptedConnection) -> None:
    """The invariant account-barrier and Workspace/Task scope reads."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())


def _script_load_subjects(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    gate_row: tuple[Any, ...],
    revision_row: tuple[Any, ...] | None = None,
    pr_row: tuple[Any, ...] | None = None,
    session_row: tuple[Any, ...] | None = None,
) -> None:
    """One discussion-subject loading round.

    The Workspace/Task scope read re-reads the Task plainly; the loader's
    own currentness read takes the row lock. Both observe the same row.
    """
    conn.on_repeat(_TASK_READ, task_row)
    conn.on_repeat(_TASK_READ_LOCKED, task_row)
    conn.on_repeat(_GATE_READ, gate_row)
    if revision_row is not None:
        conn.on_repeat(_REVISION_READ, revision_row)
    if pr_row is not None:
        conn.on_repeat(_PR_READ, pr_row)
    if session_row is not None:
        conn.on_repeat(_role_is(_SESSION_READ, "reviewer"), session_row)


def _prepare(
    conn: ScriptedConnection,
    *,
    profile_id: Any = _OWNER_PROFILE_ID,
    gate_id: Any = _GATE_1_ID,
    message: str = _OWNER_MESSAGE,
) -> Any:
    return prepare_owner_reviewer_discussion(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=gate_id,
        message=message,
    )


def _admit(
    conn: ScriptedConnection,
    runtime: FakeAgentRuntimeAdapter,
    *,
    profile_id: Any = _OWNER_PROFILE_ID,
    gate_id: Any = _GATE_1_ID,
    registrar: _Registrar | None = None,
    handoff: _Handoff | None = None,
    message: str = _OWNER_MESSAGE,
) -> Any:
    return admit_owner_reviewer_discussion_turn(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=gate_id,
        receipt_id=_RECEIPT_ID,
        message=message,
        reviewer_runtime_factory=_ReviewerAttachment(runtime),
        observation_registrar=registrar if registrar is not None else _Registrar(),
        completion_handoff=handoff if handoff is not None else _Handoff(),
    )


def _admit_happy_script(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gate_row: tuple[Any, ...] | None = None,
) -> None:
    """Script the admission flow through the acknowledgement attach."""
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=task_row if task_row is not None else _task_row(),
        gate_row=gate_row if gate_row is not None else _gate_row(),
        revision_row=_revision_row(),
        session_row=_reviewer_ready_row(),
    )
    # The receipt row lock is taken twice: the admission reproof and the
    # acknowledgement attach.
    conn.on_repeat(_RECEIPT_FOR_UPDATE, _discussion_receipt_row())
    conn.on(_RECEIPT_STAMP, (_RECEIPT_ID,))
    conn.on(
        _RECEIPT_ATTACH,
        _discussion_receipt_row(status="admitted", admitted_turn_reference="fake-run-1"),
    )
    # No staged early completion exists for a normal attach.
    conn.on(
        _sql_has("from openorc.task_hub_run_early_completions", "for update"),
        None,
    )


# --- prepare ----------------------------------------------------------------------


def test_prepare_commits_the_gate_bound_receipt_and_proves_the_subjects() -> None:
    conn = ScriptedConnection()
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(),
        session_row=_reviewer_ready_row(),
    )
    conn.on(_RECEIPT_INSERT, _discussion_receipt_row())

    prepared = _prepare(conn)

    assert prepared.owner_gate.id == _GATE_1_ID
    assert prepared.reviewer_session.id == _REVIEWER_SESSION_ID
    assert prepared.task.state_token == _STATE_TOKEN
    insert_params = next(
        params for sql, params in conn.executed if _RECEIPT_INSERT(sql, None) and params is not None
    )
    # The gate-bound receipt companion: the exact OwnerGate identity, the
    # dedicated gate-bound attempt kind, and the dedicated non-formal family.
    assert insert_params[3] == "owner_reviewer_discussion"
    assert insert_params[4] == "owner_gate_discussion"
    assert str(insert_params[5]) == str(_GATE_1_ID)
    assert insert_params[6] == OWNER_REVIEWER_DISCUSSION_FAMILY
    assert str(insert_params[2]) == str(_REVIEWER_SESSION_ID)
    _assert_no_message_text_in_sql(conn)


def test_prepare_refuses_a_non_permitted_gate_type() -> None:
    conn = ScriptedConnection()
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(
            gate_id=_GATE_2_ID,
            gate_type="pr_authorization",
            plan_revision_id=None,
            head_sha=_HEAD_SHA,
        ),
        session_row=_reviewer_ready_row(),
    )

    with pytest.raises(ConflictError, match="not admitted at this owner gate type"):
        _prepare(conn, gate_id=_GATE_2_ID)
    assert not any(_RECEIPT_INSERT(sql, None) for sql, _ in conn.executed)


def test_prepare_refuses_an_outstanding_run_with_a_conflict() -> None:
    conn = ScriptedConnection()
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(),
        session_row=_reviewer_ready_row(),
    )
    conn.on(_RECEIPT_INSERT, UniqueViolation("outstanding run"))

    with pytest.raises(ConflictError, match="outstanding detached-turn receipt"):
        _prepare(conn)


@pytest.mark.parametrize(
    ("task_overrides", "gate_overrides", "session_row", "match"),
    [
        ({"status": "reviewing"}, {}, None, "not waiting on its owner gate"),
        ({}, {"status": "approved"}, None, "no longer pending"),
        (
            {},
            {},
            _session_row(
                "reviewer",
                session_id=_REVIEWER_SESSION_ID,
                external_id=_REVIEWER_EXTERNAL_ID,
                lifecycle="lost",
            ),
            "not READY",
        ),
        ({"status": "cancelled", "archived_at": _OBSERVED}, {}, None, "already been archived"),
    ],
)
def test_prepare_rejects_before_any_attachment(
    task_overrides: dict[str, Any],
    gate_overrides: dict[str, Any],
    session_row: tuple[Any, ...] | None,
    match: str,
) -> None:
    conn = ScriptedConnection()
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(**task_overrides),
        gate_row=_gate_row(**gate_overrides),
        revision_row=_revision_row(),
        session_row=session_row if session_row is not None else _reviewer_ready_row(),
    )

    with pytest.raises((ConflictError, StaleOperationError), match=match):
        _prepare(conn)
    # No attachment was ever acquired (no runtime exists in this test at all)
    # and no receipt was recorded.
    assert not any(_RECEIPT_INSERT(sql, None) for sql, _ in conn.executed)


def test_prepare_rejects_a_malformed_message_before_any_read() -> None:
    with pytest.raises(InvalidCommandError, match="non-empty string"):
        _prepare(ScriptedConnection(), message="   ")
    with pytest.raises(InvalidCommandError, match="bounded"):
        _prepare(ScriptedConnection(), message="x" * 20_001)


# --- admit ------------------------------------------------------------------------


def test_admit_stamps_once_and_enqueues_the_verbatim_message() -> None:
    conn = ScriptedConnection()
    pool = cast(FakePool, _pool(conn))
    runtime = _prepared_admit_adapter(pool)
    registrar = _Registrar()
    handoff = _Handoff()
    _admit_happy_script(conn)

    admitted = _admit(conn, runtime, registrar=registrar, handoff=handoff)

    # The one-shot admission stamp was taken exactly once, strictly before
    # the enqueue.
    stamps = [sql for sql, _ in conn.executed if _RECEIPT_STAMP(sql, None)]
    assert len(stamps) == 1
    # The verbatim message was admitted on the exact Reviewer session and no
    # database transaction was open across the admission.
    assert runtime.detached_admissions() == ((_REVIEWER_EXTERNAL_ID, _OWNER_MESSAGE),)
    # The positive observation registration addressed the exact session.
    assert registrar.calls == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "task_agent_session_id": _REVIEWER_SESSION_ID,
            "connection_id": _CONNECTION_ID,
            "external_session_id": _REVIEWER_EXTERNAL_ID,
        }
    ]
    assert admitted.receipt.status is TaskDetachedTurnReceiptStatus.ADMITTED
    assert admitted.receipt.admitted_turn_reference == "fake-run-1"
    # A normal attach did not settle anything: no handoff call.
    assert handoff.calls == []
    _assert_no_message_text_in_sql(conn)


def test_a_stamped_receipt_refuses_re_admission_and_never_re_enqueues() -> None:
    conn = ScriptedConnection()
    runtime = FakeAgentRuntimeAdapter()
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(),
        session_row=_reviewer_ready_row(),
    )
    conn.on(_RECEIPT_FOR_UPDATE, _discussion_receipt_row(attempted=True))

    with pytest.raises(StaleOperationError, match="never re-enqueued"):
        _admit(conn, runtime)

    assert not any(_RECEIPT_STAMP(sql, None) for sql, _ in conn.executed)


def test_a_settled_receipt_refuses_admission_as_stale() -> None:
    conn = ScriptedConnection()
    runtime = FakeAgentRuntimeAdapter()
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(),
        session_row=_reviewer_ready_row(),
    )
    conn.on(
        _RECEIPT_FOR_UPDATE,
        _discussion_receipt_row(status="settled", admitted_turn_reference="fake-run-1"),
    )

    with pytest.raises(StaleOperationError, match="no longer claimed"):
        _admit(conn, runtime)


class _FailingAttachment:
    """Deferred Reviewer attachment probe whose acquisition always fails."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.acquisitions = 0

    def __call__(self, binding: Any) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.acquisitions += 1
        raise self._error

    def __enter__(self) -> FakeAgentRuntimeAdapter:  # pragma: no cover - never entered
        raise AssertionError("a failing attachment is never entered")

    def __exit__(self, *_exc: object) -> bool:  # pragma: no cover - never entered
        return False


def test_a_failed_attachment_marks_the_receipt_refused_and_frees_the_slot() -> None:
    conn = ScriptedConnection()
    runtime = FakeAgentRuntimeAdapter()
    # A positively proven pre-admission attachment failure (a disabled
    # connection, an unusable credential, an invalid configuration, a failed
    # SDK attachment): the message was never enqueued.
    attachment = _FailingAttachment(
        ExternalOperationFailedError("the attachment refused: connection disabled")
    )
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(),
        session_row=_reviewer_ready_row(),
    )
    # The stamp commits before the attachment (the one-shot gate), and the
    # refusal write follows the proven attachment failure.
    conn.on(_RECEIPT_FOR_UPDATE, _discussion_receipt_row())
    conn.on(_RECEIPT_STAMP, (_RECEIPT_ID,))
    conn.on(
        _RECEIPT_REFUSE,
        _discussion_receipt_row(status="refused", outcome={"refusal_kind": "x"}),
    )

    # A typed attachment failure keeps its identity (re-raised after the
    # refusal write); an untyped one is classified safely.
    with pytest.raises(ExternalOperationFailedError, match="attachment refused"):
        admit_owner_reviewer_discussion_turn(
            _pool(conn),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_1_ID,
            receipt_id=_RECEIPT_ID,
            message=_OWNER_MESSAGE,
            reviewer_runtime_factory=attachment,
            observation_registrar=_Registrar(),
            completion_handoff=_Handoff(),
        )

    # The slot is freed: the refusal write ran, and nothing was enqueued.
    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in conn.executed)
    assert runtime.detached_admissions() == ()
    assert attachment.acquisitions == 1


def test_a_refused_registration_marks_the_receipt_refused_without_enqueueing() -> None:
    from openorc.adapters.agent_runtime.errors import AgentSessionNotFoundError

    conn = ScriptedConnection()
    runtime = FakeAgentRuntimeAdapter()
    registrar = _Registrar(refused=True)
    _admit_happy_script(conn)
    conn.on(
        _RECEIPT_REFUSE,
        _discussion_receipt_row(status="refused", outcome={"refusal_kind": "x"}),
    )

    with pytest.raises(ExternalOperationFailedError, match="never dispatched"):
        _admit(conn, runtime, registrar=registrar)

    assert runtime.detached_admissions() == ()
    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in conn.executed)
    assert AgentSessionNotFoundError is not None  # imported with the module


def test_a_positively_refused_runtime_admission_marks_the_receipt_refused() -> None:
    from openorc.adapters.agent_runtime.errors import AgentSessionNotFoundError

    conn = ScriptedConnection()
    runtime = FakeAgentRuntimeAdapter()
    runtime.queue_detached_admission(AgentSessionNotFoundError("session gone"))
    _admit_happy_script(conn)
    conn.on(
        _RECEIPT_REFUSE,
        _discussion_receipt_row(status="refused", outcome={"refusal_kind": "x"}),
    )

    with pytest.raises(ExternalOperationRejectedError, match="never dispatched"):
        _admit(conn, runtime)
    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in conn.executed)


def test_an_uncertain_admission_stays_claimed_and_never_re_enqueues() -> None:
    from openorc.adapters.agent_runtime.errors import AgentRuntimeUncertainOutcomeError

    conn = ScriptedConnection()
    pool = cast(FakePool, _pool(conn))
    runtime = _prepared_admit_adapter(pool)
    runtime.queue_detached_admission(AgentRuntimeUncertainOutcomeError("transport lost"))
    _admit_happy_script(conn)

    with pytest.raises(ExternalOperationUncertainError, match="never re-dispatched"):
        _admit(conn, runtime)

    # The receipt stays conservatively claimed: no refusal write, no attach.
    assert not any(_RECEIPT_REFUSE(sql, None) for sql, _ in conn.executed)
    assert not any(_RECEIPT_ATTACH(sql, None) for sql, _ in conn.executed)
    assert len([sql for sql, _ in conn.executed if _RECEIPT_STAMP(sql, None)]) == 1


def test_release_prepared_discussion_receipt_frees_the_slot_after_a_refusal() -> None:
    conn = ScriptedConnection()
    conn.on(
        _RECEIPT_REFUSE,
        _discussion_receipt_row(status="refused", outcome={"refusal_kind": "x"}),
    )

    release_prepared_discussion_receipt(
        _pool(conn), receipt_id=_RECEIPT_ID, task_id=_TASK_ID, workspace_id=_WORKSPACE_ID
    )
    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in conn.executed)


# --- settlement -------------------------------------------------------------------


class _PhaseRecorder:
    """Recording double for one owning settlement transaction phase."""

    def __init__(self, outcome: Any = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._outcome = outcome

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append({"args": args, "kwargs": dict(kwargs)})
        return self._outcome


def _accepted_text(summary: str = "Everything checks out.") -> str:
    import json

    return json.dumps(accepted_review_result_candidate(summary))


def _changes_requested_text(summary: str = "Needs another pass") -> str:
    import json

    return json.dumps(
        changes_requested_review_result_candidate(summary, [("Clarify", "Say which module.")])
    )


def _settle_script(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gate_row: tuple[Any, ...] | None = None,
    revision_row: tuple[Any, ...] | None = None,
    pr_row: tuple[Any, ...] | None = None,
) -> None:
    """Script the settlement service's own reads: lock, gate, subject, session."""
    _script_authorization(conn)
    _script_load_subjects(
        conn,
        task_row=task_row if task_row is not None else _task_row(),
        gate_row=gate_row if gate_row is not None else _gate_row(),
        revision_row=revision_row if revision_row is not None else _revision_row(),
        pr_row=pr_row,
        session_row=_reviewer_ready_row(),
    )
    conn.on_repeat(_EVENT_INSERT, _event_row())


def _settle(
    conn: ScriptedConnection,
    *,
    result_text: str | None,
    gate_id: Any = _GATE_1_ID,
) -> Any:
    from openorc.domain.detached_turns import TaskDetachedTurnReceipt

    stub = TaskDetachedTurnReceipt(
        id=_RECEIPT_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_REVIEWER_SESSION_ID,
        operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
        original_attempt_kind=OriginalAttemptKind.OWNER_GATE_DISCUSSION,
        original_attempt_id=gate_id,
        expected_formal_family=OWNER_REVIEWER_DISCUSSION_FAMILY,
        admitted_turn_reference="fake-run-1",
        status=TaskDetachedTurnReceiptStatus.ADMITTED,
        outcome={},
        contradicted_at=None,
        claimed_at=_OBSERVED,
        admitted_at=_OBSERVED,
        settled_at=None,
        updated_at=_OBSERVED,
    )
    return settle_observed_discussion_result(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        receipt=stub,
        result_text=result_text,
    )


def _owning_phases_patched() -> Any:
    """Patch all four owning settlement transaction phases with recorders."""
    from contextlib import ExitStack

    recorder_names = (
        "settle_observed_planning_review_resolution",
        "supersede_observed_implementation_authorization_with_changes",
        "settle_observed_pr_review_resolution",
        "retire_observed_pr_merge_decision_with_changes",
    )
    recorders = {name: _PhaseRecorder() for name in recorder_names}

    @contextmanager
    def patched() -> Any:
        with ExitStack() as stack:
            for name, recorder in recorders.items():
                stack.enter_context(mock_patch.object(discussion, name, recorder))
            yield recorders

    return patched()


def test_an_ordinary_completion_records_only_the_two_direction_events() -> None:
    conn = ScriptedConnection()
    _settle_script(conn)

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text="Ordinary prose about the plan.")

    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.ORDINARY_EXCHANGE
    assert settlement.settlement_label == SETTLEMENT_DISCUSSION_EXCHANGE
    # Exactly two direction-only facts; never any text. The direction value
    # is the only conversation-shaped fact and rides the event's context.
    events = _event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)
    assert len(events) == 2
    blobs = [repr(params) for params in events]
    assert sum("owner_to_reviewer" in blob for blob in blobs) == 1
    assert sum("reviewer_to_owner" in blob for blob in blobs) == 1
    # No owning settlement was routed, and no message text reached any SQL.
    for phase in phases.values():
        assert phase.calls == []
    _assert_no_message_text_in_sql(conn)


def test_a_formal_result_at_the_plan_review_resolution_gate_routes_to_e3c() -> None:
    conn = ScriptedConnection()
    _settle_script(conn)

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_accepted_text())

    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.FORMAL_CONTINUATION
    assert settlement.settlement_label == SETTLEMENT_DISCUSSION_FORMAL_CONTINUATION
    # Any standalone schema-valid formal result settles the plan-subject
    # resolution gate through the owning E3c phase with the discussion audit.
    assert len(phases["settle_observed_planning_review_resolution"].calls) == 1
    routed = phases["settle_observed_planning_review_resolution"].calls[0]["kwargs"]
    assert routed["record_discussion_message_events"] is True
    assert routed["expected_state_token"] == _STATE_TOKEN
    for name in (
        "supersede_observed_implementation_authorization_with_changes",
        "settle_observed_pr_review_resolution",
        "retire_observed_pr_merge_decision_with_changes",
    ):
        assert phases[name].calls == []
    _assert_no_message_text_in_sql(conn)


def test_a_changes_requested_at_the_implementation_gate_supersedes_it() -> None:
    conn = ScriptedConnection()
    _settle_script(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(
            gate_id=_GATE_2_ID,
            gate_type="implementation_authorization",
            plan_revision_id=_REVISION_1_ID,
        ),
    )

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_changes_requested_text(), gate_id=_GATE_2_ID)

    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.FORMAL_CONTINUATION
    assert len(phases["supersede_observed_implementation_authorization_with_changes"].calls) == 1
    for name in (
        "settle_observed_planning_review_resolution",
        "settle_observed_pr_review_resolution",
        "retire_observed_pr_merge_decision_with_changes",
    ):
        assert phases[name].calls == []


def test_an_accepted_at_the_implementation_gate_stays_advisory() -> None:
    conn = ScriptedConnection()
    _settle_script(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(
            gate_id=_GATE_2_ID,
            gate_type="implementation_authorization",
            plan_revision_id=_REVISION_1_ID,
        ),
    )

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_accepted_text(), gate_id=_GATE_2_ID)

    # Issue #219: a standalone ACCEPTED at the approval gate is ordinary
    # advisory conversation; the Owner may still approve.
    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.ORDINARY_EXCHANGE
    for phase in phases.values():
        assert phase.calls == []
    assert len(_event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)) == 2


def test_a_changes_requested_at_the_merge_gate_retires_it() -> None:
    conn = ScriptedConnection()
    _settle_script(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(
            gate_id=_GATE_2_ID,
            gate_type="merge_decision",
            plan_revision_id=None,
            head_sha=_HEAD_SHA,
            pr_id=_PR_ID,
        ),
        pr_row=_pr_row(),
    )

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_changes_requested_text(), gate_id=_GATE_2_ID)

    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.FORMAL_CONTINUATION
    assert len(phases["retire_observed_pr_merge_decision_with_changes"].calls) == 1
    for name in (
        "settle_observed_planning_review_resolution",
        "supersede_observed_implementation_authorization_with_changes",
        "settle_observed_pr_review_resolution",
    ):
        assert phases[name].calls == []


def test_an_accepted_at_the_merge_gate_stays_advisory() -> None:
    conn = ScriptedConnection()
    _settle_script(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(
            gate_id=_GATE_2_ID,
            gate_type="merge_decision",
            plan_revision_id=None,
            head_sha=_HEAD_SHA,
            pr_id=_PR_ID,
        ),
        pr_row=_pr_row(),
    )

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_accepted_text(), gate_id=_GATE_2_ID)

    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.ORDINARY_EXCHANGE
    for phase in phases.values():
        assert phase.calls == []
    assert len(_event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)) == 2


def test_a_formal_result_at_a_pr_head_review_resolution_gate_routes_to_e9() -> None:
    conn = ScriptedConnection()
    _settle_script(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(
            gate_id=_GATE_2_ID,
            gate_type="review_resolution",
            plan_revision_id=None,
            head_sha=_HEAD_SHA,
            pr_id=_PR_ID,
        ),
        pr_row=_pr_row(),
    )

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_changes_requested_text(), gate_id=_GATE_2_ID)

    assert settlement is not None
    assert settlement.kind is discussion.ObservedDiscussionSettlementKind.FORMAL_CONTINUATION
    assert len(phases["settle_observed_pr_review_resolution"].calls) == 1
    for name in (
        "settle_observed_planning_review_resolution",
        "supersede_observed_implementation_authorization_with_changes",
        "retire_observed_pr_merge_decision_with_changes",
    ):
        assert phases[name].calls == []


def test_a_moved_on_gate_applies_nothing() -> None:
    conn = ScriptedConnection()
    # The gate the receipt roots at is no longer the Task's current gate.
    _settle_script(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(gate_id=_GATE_1_ID),
    )

    with _owning_phases_patched() as phases:
        settlement = _settle(conn, result_text=_changes_requested_text())

    assert settlement is None
    for phase in phases.values():
        assert phase.calls == []
    assert _event_params_of_type(conn, _DISCUSSION_EVENT_TYPE) == []


# --- the status projection ----------------------------------------------------------


_STATUS_QUERY = _sql_has("from openorc.task_hub_run_receipts", "original_attempt_kind = %s")


def _script_status_receipts(conn: ScriptedConnection, receipts: list[tuple[Any, ...]]) -> None:
    _script_authorization(conn)
    # The Workspace/Task scope read re-reads the Task plainly.
    conn.on_repeat(_TASK_READ, _task_row())
    conn.on_repeat(_STATUS_QUERY, receipts)


def test_the_status_projection_reports_the_outstanding_receipt_first() -> None:
    conn = ScriptedConnection()
    _script_status_receipts(conn, [_discussion_receipt_row()])

    status = get_owner_gate_discussion_status(
        _pool(conn),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_1_ID,
    )
    assert status is not None
    assert status.receipt_id == _RECEIPT_ID
    assert status.status is TaskDetachedTurnReceiptStatus.CLAIMED
    assert status.admission_attempted_at is None
    assert status.outcome_label is None


def test_the_status_projection_extracts_the_safe_settlement_label() -> None:
    conn = ScriptedConnection()
    _script_status_receipts(
        conn,
        [
            _discussion_receipt_row(
                status="settled",
                admitted_turn_reference="fake-run-1",
                outcome={
                    "settlement": SETTLEMENT_DISCUSSION_EXCHANGE,
                    "consumed_sequence": 41,
                },
            )
        ],
    )

    status = get_owner_gate_discussion_status(
        _pool(conn),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_1_ID,
    )
    assert status is not None
    assert status.status is TaskDetachedTurnReceiptStatus.SETTLED
    assert status.outcome_label == SETTLEMENT_DISCUSSION_EXCHANGE


def test_the_status_projection_reports_a_proven_refusal_label() -> None:
    conn = ScriptedConnection()
    _script_status_receipts(
        conn,
        [
            _discussion_receipt_row(
                status="refused", outcome={"refusal_kind": "admission_submission_refused"}
            )
        ],
    )

    status = get_owner_gate_discussion_status(
        _pool(conn),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_1_ID,
    )
    assert status is not None
    assert status.status is TaskDetachedTurnReceiptStatus.REFUSED
    assert status.outcome_label == "admission_submission_refused"


def test_the_status_projection_is_none_for_a_foreign_gate() -> None:
    conn = ScriptedConnection()
    _script_status_receipts(conn, [])

    status = get_owner_gate_discussion_status(
        _pool(conn),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_2_ID,
    )
    assert status is None
