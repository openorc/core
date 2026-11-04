"""Deterministic tests for the F5f authorized presentation retry (issue #269).

A monkeypatched module-seam harness proves the retry seam on its critical
paths:

- the authorized due retry consumes the attempt set-once and re-enters ONLY
  the guarded E7 presentation — never the implementation effect, never a new
  Execution, never a second branch bind or gate; the invocation's attempt
  evidence names the consumed attempt identity (the consumed attempt's own
  UUID as both the original and effect binding, the presentation family's
  no-resulting-Execution shape);
- the refusal table refuses before any effect: premature wakeup,
  replayed/already-consumed authorization, terminally disposed episode,
  non-latest or unconsumed-sibling chain inconsistency, stale token, wrong
  episode family, or a root Execution that is no longer the successful
  implementation attempt;
- a failed guarded presentation propagates its operational error unchanged
  for the F5b coordinator's typed handoff with the consumption exactly once
  — nothing is re-consumed, re-sent, or settled here.

No live runtime, Postgres, or GitHub access.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from openorc.adapters.github import GitHubTransportUncertainError
from openorc.domain.executions import ExecutionStatus
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    ExternalOperationUncertainError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_authorized_pr_authorization_retry import (
    execute_authorized_pr_authorization_retry,
)
from openorc.services.task_operational_failure_triage import triage_operational_failure
from openorc.services.task_workflow_operation_coordinator import coordinate_task_workflow_operation
from tests.test_services_task_operational_failure_triage import (
    _ATTEMPT_BY_SOURCE,
    _ATTEMPT_INSERT,
    _ATTEMPTS_LIST,
    _EPISODE_BY_ORIGINAL,
    _EPISODE_ESCALATE,
    _EPISODE_INSERT,
    _EXECUTION_LOCKED,
    _TASK_BLOCK_UPDATE,
)
from tests.test_services_task_pr_authorization import (
    _BLOCK_INSERT,
    _GATES_INSERT,
    _GATES_LIST,
    _TASK_READ,
    _TASK_READ_LOCKED,
    FakePool,
    ScriptedConnection,
    _count,
    _execution_row,
    _fake_github,
    _gate_row,
    _pool,
    _script_authority_reads,
    _script_b3_reconciliation,
    _script_repeating_reads,
    _sql_has,
    _sql_lacks,
    _task_row,
)
from tests.test_services_task_pr_authorization import (
    _EXECUTION_1_ID as _ROOT_EXECUTION_ID,
)
from tests.test_services_task_pr_authorization import (
    _OWNER_PROFILE_ID as _FLOW_PROFILE_ID,
)
from tests.test_services_task_pr_authorization import (
    _STATE_TOKEN as _FLOW_STATE_TOKEN,
)
from tests.test_services_task_pr_authorization import (
    _TASK_ID as _FLOW_TASK_ID,
)
from tests.test_services_task_pr_authorization import (
    _WORKSPACE_ID as _FLOW_WORKSPACE_ID,
)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c1")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c2")
_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c3")
_STATE_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c4")
_EXECUTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c5")
_EPISODE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c6")
_ATTEMPT_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-0000000000c7")
_OBSERVED = datetime(2026, 11, 4, 12, 0, 0, tzinfo=UTC)


def _cast_pool() -> DatabasePool:
    return cast(DatabasePool, object())


def _cast_github() -> Any:
    return cast(Any, object())


def _domain_task() -> SimpleNamespace:
    return SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        status=TaskStatus.IMPLEMENTING,
        current_owner_gate_id=None,
        state_token=_STATE_TOKEN,
    )


def _domain_attempt(**overrides: Any) -> SimpleNamespace:
    facts: dict[str, Any] = {
        "id": _ATTEMPT_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "episode_id": _EPISODE_ID,
        "ordinal": 1,
        "consumed_at": None,
        "not_before": _OBSERVED,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _domain_episode(**overrides: Any) -> SimpleNamespace:
    facts: dict[str, Any] = {
        "id": _EPISODE_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "operation_family": (
            PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
        ),
        "original_attempt_kind": OriginalAttemptKind.EXECUTION,
        "original_attempt_id": _EXECUTION_ID,
        "escalated_task_block_id": None,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _domain_execution(**overrides: Any) -> SimpleNamespace:
    facts: dict[str, Any] = {
        "id": _EXECUTION_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "status": ExecutionStatus.SUCCEEDED,
        "review_iteration_id": None,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _recording(*results: Any) -> Any:
    """A recording stub returning the scripted results in order."""
    calls: list[dict[str, Any]] = []
    scripted = list(results)
    sticky: list[Any] = [scripted[-1] if scripted else None]

    def stub(*args: Any, **kwargs: Any) -> Any:
        calls.append({"args": args, "kwargs": kwargs})
        if scripted:
            sticky[0] = scripted.pop(0)
        result = sticky[0]
        if isinstance(result, Exception):
            raise result
        return result

    stub.calls = calls  # type: ignore[attr-defined]
    return stub


class _Harness:
    """Wire every module seam the F5f retry seam touches with recording fakes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, task: Any | None = None) -> None:
        self.task = task if task is not None else _domain_task()
        self.attempt = _domain_attempt()
        self.episode = _domain_episode()
        self.execution = _domain_execution()

        self.authorize = _recording(None)
        self.workspace_scope = _recording(None)
        self.current_task = _recording(self.task)
        self.get_attempt = _recording(self.attempt)
        self.get_episode = _recording(self.episode)
        self.list_attempts = _recording([self.attempt])
        self.consume = _recording(self.attempt)
        self.get_execution = _recording(self.execution)
        self.present = _recording(SimpleNamespace(task=self.task, owner_gate=SimpleNamespace()))
        self.evidence = InvocationAttemptEvidence()

        import openorc.services.task_authorized_pr_authorization_retry as service

        self._service = service
        monkeypatch.setattr(service, "require_account_operational", self.authorize)
        monkeypatch.setattr(service, "require_workspace_task", self.workspace_scope)
        monkeypatch.setattr(service, "require_current_task", self.current_task)
        monkeypatch.setattr(
            service,
            "failure_records",
            SimpleNamespace(
                get_operational_failure_attempt=self.get_attempt,
                get_operational_failure_episode=self.get_episode,
                list_operational_failure_attempts=self.list_attempts,
                consume_operational_failure_attempt=self.consume,
            ),
        )
        monkeypatch.setattr(
            service,
            "execution_records",
            SimpleNamespace(get_execution=self.get_execution),
        )
        monkeypatch.setattr(service, "open_pr_authorization", self.present)

    def run(self, *, clock: Any = None) -> Any:
        return execute_authorized_pr_authorization_retry(
            _cast_pool(),
            _cast_github(),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            retry_attempt_id=_ATTEMPT_ID,
            attempt_evidence=self.evidence,
            clock=clock if clock is not None else (lambda: _OBSERVED),
        )


def test_the_authorized_due_retry_consumes_and_represent_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.run()

    # The guarded presentation re-entered exactly once with the episode's
    # root Execution identity and the exact post-E6 token.
    assert len(harness.present.calls) == 1
    assert harness.present.calls[0]["kwargs"]["expected_state_token"] is _STATE_TOKEN
    assert harness.present.calls[0]["kwargs"]["execution_id"] == _EXECUTION_ID
    # The one-shot consumption happened first, on the exact attempt.
    assert len(harness.consume.calls) == 1
    assert harness.consume.calls[0]["kwargs"]["attempt_id"] == _ATTEMPT_ID
    # The guarded presentation's admission recording is directed at a
    # throwaway collector: E7's unconditional admission identity must never
    # overwrite the invocation's consumed-attempt handoff identity.
    assert harness.present.calls[0]["kwargs"]["attempt_evidence"] is not harness.evidence
    # The invocation's handoff identity names the consumed attempt with the
    # presentation family's no-resulting-Execution binding shape.
    identity = harness.evidence.latest_identity
    assert identity is not None
    assert (
        identity.family
        is PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
    )
    assert identity.original_attempt_kind is OriginalAttemptKind.EXECUTION
    assert identity.original_attempt_id == _ATTEMPT_ID
    assert identity.effect_attempt_id == _ATTEMPT_ID
    assert identity.phase == "pr_authorization_retry_presentation"


def test_premature_wakeup_is_refused_before_any_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    with pytest.raises(StaleOperationError, match="not yet due"):
        harness.run(clock=lambda: _OBSERVED - timedelta(seconds=1))
    assert harness.consume.calls == []
    assert harness.present.calls == []


def test_replayed_consumption_never_reaches_the_presentation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.attempt.consumed_at = _OBSERVED
    with pytest.raises(ConflictError, match="already been consumed"):
        harness.run()
    assert harness.consume.calls == []
    assert harness.present.calls == []


def test_a_terminally_disposed_episode_refuses_before_any_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.episode.escalated_task_block_id = uuid4()
    with pytest.raises(ConflictError, match="terminally disposed"):
        harness.run()
    assert harness.consume.calls == []
    assert harness.present.calls == []


def _repatch_chain(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    import openorc.services.task_authorized_pr_authorization_retry as service

    monkeypatch.setattr(
        service,
        "failure_records",
        SimpleNamespace(
            get_operational_failure_attempt=harness.get_attempt,
            get_operational_failure_episode=harness.get_episode,
            list_operational_failure_attempts=harness.list_attempts,
            consume_operational_failure_attempt=harness.consume,
        ),
    )


def test_a_non_latest_attempt_refuses_before_any_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.list_attempts = _recording([SimpleNamespace(id=uuid4(), consumed_at=_OBSERVED)])
    _repatch_chain(harness, monkeypatch)
    with pytest.raises(ConflictError, match="latest attempt"):
        harness.run()
    assert harness.consume.calls == []
    assert harness.present.calls == []


def test_a_root_execution_no_longer_succeeded_refuses_before_any_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.execution.status = ExecutionStatus.FAILED_TRANSIENT
    with pytest.raises(ConflictError, match="successful implementation attempt"):
        harness.run()
    assert harness.consume.calls == []
    assert harness.present.calls == []


def test_a_failed_guarded_presentation_propagates_with_the_consumption_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    error = ExternalOperationUncertainError("the outcome of the GitHub read is unknown")
    error.__cause__ = GitHubTransportUncertainError("the transport failed transiently")
    harness.present = _recording(error)
    import openorc.services.task_authorized_pr_authorization_retry as service

    monkeypatch.setattr(service, "open_pr_authorization", harness.present)

    with pytest.raises(ExternalOperationUncertainError) as excinfo:
        harness.run()

    # The operational failure propagates unchanged for the F5b coordinator's
    # typed handoff; the consumption happened exactly once and is never
    # re-consumed, re-sent, or settled here.
    assert excinfo.value is error
    assert len(harness.consume.calls) == 1
    assert len(harness.present.calls) == 1
    identity = harness.evidence.latest_identity
    assert identity is not None
    assert identity.effect_attempt_id == _ATTEMPT_ID


# --- cross-boundary regression: the real retry-seam-to-E7 evidence sequence ---


_FLOW_EPISODE_ID = uuid4()
_FAILURE_ATTEMPT_ID = uuid4()
_SECOND_ATTEMPT_ID = uuid4()
_THIRD_ATTEMPT_ID = uuid4()
_F4_BLOCK_ID = uuid4()
_F4_GATE_ID = uuid4()
_F4_ROTATED_TOKEN = uuid4()

# The retry-seam's own reads and the F5b/F4 reads share SQL shapes with
# different scripted outcomes in call order.
_FAILURE_ATTEMPT_READ = _sql_has("from openorc.operational_failure_attempts where id")
_FAILURE_EPISODE_READ = _sql_has("from openorc.operational_failure_episodes where id")
_FAILURE_ATTEMPT_CONSUME = _sql_has("update openorc.operational_failure_attempts", "consumed_at")
_EXECUTION_PLAIN = _sql_lacks(_sql_has("from openorc.executions where id"), "for update")


def _failure_episode_row(*, escalated: Any = None) -> tuple[Any, ...]:
    return (
        _FLOW_EPISODE_ID,
        _FLOW_WORKSPACE_ID,
        _FLOW_TASK_ID,
        "exact_head_pr_authorization_presentation",
        "execution",
        _ROOT_EXECUTION_ID,
        {"execution_id": str(_ROOT_EXECUTION_ID)},
        escalated,
        _OBSERVED,
    )


def _failure_attempt_row(
    *,
    attempt_id: Any = _FAILURE_ATTEMPT_ID,
    ordinal: int = 1,
    source_kind: str = "original",
    source_attempt_id: Any = _ROOT_EXECUTION_ID,
    consumed_at: Any = None,
    error_code: str = "external_transport_unavailable",
) -> tuple[Any, ...]:
    return (
        attempt_id,
        _FLOW_WORKSPACE_ID,
        _FLOW_TASK_ID,
        _FLOW_EPISODE_ID,
        ordinal,
        source_kind,
        source_attempt_id,
        error_code,
        _OBSERVED,
        None,
        consumed_at,
        _OBSERVED,
    )


def _escalation_block_row() -> tuple[Any, ...]:
    return (
        _F4_BLOCK_ID,
        _FLOW_WORKSPACE_ID,
        _FLOW_TASK_ID,
        "external_operation_uncertain",
        {},
        None,
        _OBSERVED,
    )


def _escalation_gate_row() -> tuple[Any, ...]:
    return (
        _F4_GATE_ID,
        _FLOW_WORKSPACE_ID,
        _FLOW_TASK_ID,
        "block_resolution",
        "pending",
        None,
        None,
        None,
        _F4_BLOCK_ID,
        None,
        _OBSERVED,
    )


def _consumed_variant(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """The same attempt row with its set-once consumption stamp applied."""
    return _failure_attempt_row(
        attempt_id=row[0],
        ordinal=row[4],
        source_kind=row[5],
        source_attempt_id=row[6],
        error_code=row[7],
        consumed_at=_OBSERVED,
    )


def _script_authorized_retry_failure_flow(
    conn: ScriptedConnection,
    *,
    loader_attempts_rows: list[tuple[Any, ...]],
    disposed_attempts_rows: list[tuple[Any, ...]],
    escalate: bool = False,
) -> None:
    """The whole authorized-retry failure flow in call order: the retry
    seam's admission + one-shot consumption, the REAL E7 admission/authority
    reads, the failing B3 read, the F5b resolution reads, and F4's locked
    disposition flow."""
    task_row = _task_row()
    latest_unconsumed = loader_attempts_rows[-1]
    # --- retry-seam admission + one-shot consumption. ---
    _script_repeating_reads(conn)
    conn.on(_TASK_READ, task_row)  # require_workspace_task
    conn.on(_TASK_READ_LOCKED, task_row)  # require_current_task
    conn.on(_TASK_READ, task_row)  # require_current_task
    conn.on(_FAILURE_ATTEMPT_READ, latest_unconsumed)
    conn.on(_FAILURE_EPISODE_READ, _failure_episode_row())
    conn.on(_ATTEMPTS_LIST, loader_attempts_rows)
    conn.on(_EXECUTION_PLAIN, _execution_row())
    conn.on(_FAILURE_ATTEMPT_CONSUME, _consumed_variant(latest_unconsumed))
    # --- the REAL E7 Phase-A admission + authority derivation. ---
    _script_repeating_reads(conn)
    _script_authority_reads(conn, task_row=task_row)
    # --- the failing B3 reconciliation (the fake raises at the issue read). ---
    _script_b3_reconciliation(conn)
    # --- the F5b coordinator's resolution reads. ---
    conn.on(_EXECUTION_PLAIN, _execution_row())
    conn.on(_TASK_READ, task_row)
    conn.on(_FAILURE_ATTEMPT_READ, _consumed_variant(latest_unconsumed))
    conn.on(_FAILURE_EPISODE_READ, _failure_episode_row())
    # --- F4's locked disposition flow. ---
    conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_EPISODE_BY_ORIGINAL, _failure_episode_row())
    conn.on(_ATTEMPTS_LIST, disposed_attempts_rows)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_EXECUTION_LOCKED, _execution_row())
    conn.on(_GATES_LIST, [_gate_row()])
    if escalate:
        conn.on(_TASK_READ_LOCKED, task_row)
        conn.on(_TASK_READ, task_row)
        conn.on(_BLOCK_INSERT, _escalation_block_row())
        conn.on(_GATES_INSERT, _escalation_gate_row())
        conn.on(
            _TASK_BLOCK_UPDATE,
            _task_row(status="blocked", state_token=_F4_ROTATED_TOKEN, gate_id=_F4_GATE_ID),
        )
        conn.on(_EPISODE_ESCALATE, _failure_episode_row(escalated=_F4_BLOCK_ID))
    else:
        conn.on(_EXECUTION_LOCKED, _execution_row())
        conn.on(_TASK_READ_LOCKED, task_row)
        conn.on(
            _ATTEMPT_INSERT,
            _failure_attempt_row(
                attempt_id=_SECOND_ATTEMPT_ID,
                ordinal=2,
                source_kind="prior_authorized",
                source_attempt_id=_FAILURE_ATTEMPT_ID,
                consumed_at=_OBSERVED,
            ),
        )


def _transport_read_error() -> ExternalOperationUncertainError:
    error = ExternalOperationUncertainError("the outcome of the GitHub issue read is unknown")
    error.__cause__ = GitHubTransportUncertainError("the transport failed transiently")
    return error


def _run_retry_through_the_coordinator(
    pool: Any,
    github: Any,
    recorded_triage_calls: list[dict[str, Any]],
    collectors: list[InvocationAttemptEvidence],
    *,
    retry_attempt_id: Any = _FAILURE_ATTEMPT_ID,
) -> None:
    def recording_triage(pool_arg: Any, **kwargs: Any) -> Any:
        recorded_triage_calls.append(kwargs)
        return triage_operational_failure(pool_arg, **kwargs)

    def level_one(collector: InvocationAttemptEvidence) -> None:
        collectors.append(collector)
        execute_authorized_pr_authorization_retry(
            pool,
            github,
            profile_id=_FLOW_PROFILE_ID,
            workspace_id=_FLOW_WORKSPACE_ID,
            task_id=_FLOW_TASK_ID,
            expected_state_token=_FLOW_STATE_TOKEN,
            retry_attempt_id=retry_attempt_id,
            attempt_evidence=collector,
            clock=lambda: _OBSERVED,
        )

    return coordinate_task_workflow_operation(
        pool,
        expected_status=TaskStatus.IMPLEMENTING,
        workspace_id=_FLOW_WORKSPACE_ID,
        task_id=_FLOW_TASK_ID,
        profile_id=_FLOW_PROFILE_ID,
        expected_state_token=_FLOW_STATE_TOKEN,
        invoke_level_one=level_one,
        triage=recording_triage,
    )


def test_the_authorized_retry_failure_reports_prior_authorized_and_advances_the_episode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cross-boundary regression (review finding, issue #269): the REAL
    retry seam calls the REAL E7 presentation, which records its admission
    identity into a throwaway collector — the invocation's handoff identity
    stays the consumed attempt, so a failing B3/B7 read reports
    PRIOR_AUTHORIZED provenance to F4 and the existing episode's attempt
    accounting advances (ordinal 2 authorized), never a duplicate
    already-disposed original report."""
    conn = ScriptedConnection()
    _script_authorized_retry_failure_flow(
        conn,
        loader_attempts_rows=[_failure_attempt_row()],
        disposed_attempts_rows=[_failure_attempt_row(consumed_at=_OBSERVED)],
    )
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool), issue_error=_transport_read_error())
    collectors: list[InvocationAttemptEvidence] = []
    recorded: list[dict[str, Any]] = []

    with pytest.raises(ExternalOperationUncertainError) as excinfo:
        _run_retry_through_the_coordinator(pool, github, recorded, collectors)

    assert excinfo.value.__cause__ is not None
    # The consumed attempt is STILL the invocation's latest reporting
    # identity after the real E7 admission — not overwritten by the root
    # Execution identity.
    assert len(collectors) == 1
    identity = collectors[0].latest_identity
    assert identity is not None
    assert (
        identity.family
        is PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
    )
    assert identity.original_attempt_id == _FAILURE_ATTEMPT_ID
    assert identity.effect_attempt_id == _FAILURE_ATTEMPT_ID
    assert collectors[0].current_task_token == _FLOW_STATE_TOKEN
    # F4 received the PRIOR_AUTHORIZED report against the episode's original
    # Execution identity with the retry-eligible transport code and the
    # structurally proven pre-effect evidence.
    assert len(recorded) == 1
    kwargs = recorded[0]
    assert kwargs["expected_state_token"] == _FLOW_STATE_TOKEN
    assert kwargs["expected_status"] is TaskStatus.IMPLEMENTING
    operation = kwargs["operation"]
    assert (
        operation.family
        is PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
    )
    assert operation.original_attempt_id == _ROOT_EXECUTION_ID
    report = kwargs["report"]
    assert report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED
    assert report.reporting_attempt_id == _FAILURE_ATTEMPT_ID
    assert report.effect_attempt_id == _FAILURE_ATTEMPT_ID
    assert report.error_code is OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE
    assert report.effect_evidence is FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
    # F4 advanced the EXISTING episode's attempt accounting: one ordinal-2
    # prior-authorized successor, and no second episode was minted.
    assert _count(conn, _ATTEMPT_INSERT) == 1
    (attempt_params,) = [params for sql, params in conn.executed if _ATTEMPT_INSERT(sql, None)]
    assert attempt_params is not None
    assert attempt_params[3] == 2
    assert attempt_params[4] == "prior_authorized"
    assert attempt_params[5] == _FAILURE_ATTEMPT_ID
    assert _count(conn, _EPISODE_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 0


def test_the_exhausted_presentation_budget_escalates_through_the_real_seams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same cross-boundary sequence with the three-attempt budget
    exhausted: the consumed third attempt's transport failure still reports
    PRIOR_AUTHORIZED provenance, F4 authorizes nothing, and the episode
    escalates through the F3 BLOCK_RESOLUTION installation."""
    first = _failure_attempt_row(
        attempt_id=uuid4(),
        ordinal=1,
        source_kind="original",
        source_attempt_id=_ROOT_EXECUTION_ID,
        consumed_at=_OBSERVED,
    )
    second = _failure_attempt_row(
        attempt_id=_SECOND_ATTEMPT_ID,
        ordinal=2,
        source_kind="prior_authorized",
        source_attempt_id=_FAILURE_ATTEMPT_ID,
        consumed_at=_OBSERVED,
    )
    third_unconsumed = _failure_attempt_row(
        attempt_id=_THIRD_ATTEMPT_ID,
        ordinal=3,
        source_kind="prior_authorized",
        source_attempt_id=_SECOND_ATTEMPT_ID,
    )
    third_consumed = _failure_attempt_row(
        attempt_id=_THIRD_ATTEMPT_ID,
        ordinal=3,
        source_kind="prior_authorized",
        source_attempt_id=_SECOND_ATTEMPT_ID,
        consumed_at=_OBSERVED,
    )
    conn = ScriptedConnection()
    _script_authorized_retry_failure_flow(
        conn,
        loader_attempts_rows=[first, second, third_unconsumed],
        disposed_attempts_rows=[first, second, third_consumed],
        escalate=True,
    )
    pool = _pool(conn)
    github = _fake_github(cast(FakePool, pool), issue_error=_transport_read_error())
    collectors: list[InvocationAttemptEvidence] = []
    recorded: list[dict[str, Any]] = []

    with pytest.raises(ExternalOperationUncertainError):
        _run_retry_through_the_coordinator(
            pool, github, recorded, collectors, retry_attempt_id=_THIRD_ATTEMPT_ID
        )

    # The consumed third attempt is the reporting identity throughout.
    assert len(collectors) == 1
    identity = collectors[0].latest_identity
    assert identity is not None
    assert identity.original_attempt_id == _THIRD_ATTEMPT_ID
    assert identity.effect_attempt_id == _THIRD_ATTEMPT_ID
    assert len(recorded) == 1
    report = recorded[0]["report"]
    assert report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED
    assert report.reporting_attempt_id == _THIRD_ATTEMPT_ID
    assert report.effect_attempt_id == _THIRD_ATTEMPT_ID
    # F4 authorized nothing and escalated: the F3 block/gate/BLOCKED
    # composition committed and the episode received its terminal stamp.
    assert _count(conn, _ATTEMPT_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _GATES_INSERT) == 1
    assert _count(conn, _EPISODE_ESCALATE) == 1
