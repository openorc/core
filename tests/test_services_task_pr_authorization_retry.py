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
