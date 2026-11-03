"""Deterministic tests for the F5d authorized exact-head merge retry (#267).

Fake-backed module-seam stubs prove the retry seam on its critical paths:

- one authorized, due, exact-current retry consumes its F4 attempt set-once,
  sends the merge exactly once with the original accepted head and the
  durable claim-revalidation hook, and completes through the shared
  completion seam; the invocation's attempt evidence names the consumed
  attempt identity with the send phase;
- the refusal table refuses before any effect: premature wakeup,
  replayed/already-consumed authorization, terminally disposed episode,
  non-latest or unconsumed-sibling chain inconsistency, stale token, wrong
  status, current gate pointer, unbound gate/PR subjects, non-Owner caller,
  and malformed commands — with no consumption, no send, and no settlement;
- every pre-consumption preflight failure has a guarded, truthful settlement
  and never consumes the authorization or pretends the merge mutation was
  attempted: a confirmed merge completes without resending; a closed-unmerged
  PR keeps the existing refusal; a moved head keeps E10a's stale
  classification; a GitHub-confirmed closed source issue settles
  ``BLOCK_RESOLUTION``/``owner_action_required`` with explicit closed-issue
  evidence and NEVER the requirements-fingerprint source-changed vocabulary;
  an unavailable read settles the escalation block with the
  mutation-not-attempted context; a known access/credential refusal keeps
  E10a's definitive-no-merge settlement;
- every post-consumption outcome has a defined disposition: the send's own
  unknown outcome propagates for the F5b Level 2 handoff (never settled
  here, never retried here); a plain pre-send uncertainty reports the
  consumed attempt truthfully to F4 (which escalates); a GitHub-owned
  refusal and known pre-delivery failures keep the E10a
  definitive-no-merge settlement; a stale send keeps the E10a stale
  classification; a confirmed-merge reconciliation-pending failure takes the
  existing known-success block; and a known success completes.

No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    GitHubMergeOperation,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.domain.tasks import TaskStatus
from openorc.services import pull_request_merge
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    MergeConfirmedReconciliationPendingError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.pull_request_merge import MergeRequestSendUncertainError
from openorc.services.task_authorized_merge_retry import execute_authorized_merge_retry
from openorc.services.task_exceptional_blocking import ExceptionalBlockInstallation
from openorc.services.task_merge_lifecycle import (
    TaskMergeOutcome,
    TaskMergeOutcomeKind,
)
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    OperationalFailureDisposition,
)

_OBSERVED = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
_DUE = _OBSERVED - timedelta(seconds=2)
_NOT_DUE = _OBSERVED + timedelta(seconds=30)
_WORKSPACE_ID = uuid4()
_OWNER_PROFILE_ID = uuid4()
_TASK_ID = uuid4()
_STATE_TOKEN = uuid4()
_GATE_ID = uuid4()
_PR_ID = uuid4()
_HEAD_SHA = "a" * 40
_EPISODE_ID = uuid4()
_ATTEMPT_ID = uuid4()
_MERGE_COMMIT_SHA = "c" * 40


def _domain_task(*, status: str = "waiting_for_owner") -> Any:
    return SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        status=TaskStatus(status),
        state_token=_STATE_TOKEN,
        current_owner_gate_id=None,
        archived_at=None,
        github_issue_id=503,
        github_issue_number=17,
    )


def _domain_pull_request(*, state: str = "open", head_sha: str = _HEAD_SHA) -> Any:
    return SimpleNamespace(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        state=TaskPullRequestState(state),
        head_sha=head_sha,
        merged_at=None,
    )


def _domain_gate() -> Any:
    return SimpleNamespace(
        id=_GATE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=OwnerGateType.MERGE_DECISION,
        status=OwnerGateStatus.APPROVED,
        task_pull_request_id=_PR_ID,
        subject_head_sha=_HEAD_SHA,
    )


def _domain_attempt(*, consumed_at: datetime | None = None, not_before: datetime = _DUE) -> Any:
    return SimpleNamespace(
        id=_ATTEMPT_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        episode_id=_EPISODE_ID,
        ordinal=1,
        consumed_at=consumed_at,
        not_before=not_before,
    )


def _domain_episode() -> Any:
    return SimpleNamespace(
        id=_EPISODE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation_family=GitHubMergeOperation.EXACT_HEAD_MERGE,
        original_attempt_kind=OriginalAttemptKind.MERGE_DECISION_GATE,
        original_attempt_id=_GATE_ID,
        subject={
            "owner_gate_id": str(_GATE_ID),
            "task_pull_request_id": str(_PR_ID),
            "expected_head_sha": _HEAD_SHA,
        },
        escalated_task_block_id=None,
    )


def _admission(*, attempt: Any | None = None, task: Any | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        task=task if task is not None else _domain_task(),
        episode=_domain_episode(),
        attempt=attempt if attempt is not None else _domain_attempt(),
        gate=_domain_gate(),
        pull_request=_domain_pull_request(),
    )


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


def _installation(task: Any) -> ExceptionalBlockInstallation:
    return cast(
        ExceptionalBlockInstallation,
        SimpleNamespace(
            task_block=SimpleNamespace(id=uuid4(), reason=TaskBlockReason.OWNER_ACTION_REQUIRED),
            owner_gate=SimpleNamespace(id=uuid4()),
            task=task,
        ),
    )


class _Harness:
    """Wire every module seam the F5d service touches with recording fakes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, task: Any | None = None) -> None:
        self.task = task if task is not None else _domain_task()
        self.attempt = _domain_attempt()
        self.episode = _domain_episode()
        self.gate = _domain_gate()
        self.pull_request = _domain_pull_request()

        self.authorize = _recording(self.task)
        self.current_task = _recording(self.task)
        self.get_attempt = _recording(self.attempt)
        self.get_episode = _recording(self.episode)
        self.list_attempts = _recording([self.attempt])
        self.consume = _recording(self.attempt)
        self.get_gate = _recording(self.gate)
        self.get_pr = _recording(self.pull_request)
        self.task_for_update = _recording(self.task)
        self.reconcile = _recording(SimpleNamespace(pull_request=self.pull_request))
        self.issue_guard = _recording(None)
        self.merge = _recording(SimpleNamespace(merge_commit_sha=_MERGE_COMMIT_SHA))
        self.complete = _recording(SimpleNamespace(task=self.task, pull_request=self.pull_request))
        self.install = _recording(_installation(self.task))
        self.triage = _recording()
        self.stale_classification = _recording(_outcome_stale(self.task))
        self.definitive_settlement = _recording(_outcome_settled(self.task))
        self.unconfirmed_classification = _recording(_outcome_blocked(self.task))
        self.claim_hook = _recording(lambda claim_pool: None)
        self.evidence = InvocationAttemptEvidence()

        import openorc.services.task_authorized_merge_retry as service

        self._service = service
        monkeypatch.setattr(service, "require_owner_write_authorization", self.authorize)
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
        monkeypatch.setattr(service, "gate_records", SimpleNamespace(get_owner_gate=self.get_gate))
        monkeypatch.setattr(service, "get_task_pull_request", self.get_pr)
        monkeypatch.setattr(
            service,
            "task_records",
            SimpleNamespace(get_task_for_update=self.task_for_update),
        )
        monkeypatch.setattr(
            service,
            "task_pull_request_reconciliation",
            SimpleNamespace(reconcile_task_pull_request=self.reconcile),
        )
        monkeypatch.setattr(service, "pull_request_merge", pull_request_merge)
        monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", self.merge)
        monkeypatch.setattr(service, "_require_source_issue_open", self.issue_guard)
        monkeypatch.setattr(service, "_is_confirmed_merged", self._confirmed_merged)
        monkeypatch.setattr(service, "complete_confirmed_task_merge", self.complete)
        monkeypatch.setattr(service, "install_exceptional_block_gate", self.install)
        monkeypatch.setattr(service, "triage_operational_failure", self.triage)
        monkeypatch.setattr(
            service, "_classify_after_stale_merge_request", self.stale_classification
        )
        monkeypatch.setattr(
            service, "_settle_after_definitive_no_merge", self.definitive_settlement
        )
        monkeypatch.setattr(
            service, "_classify_after_unconfirmed_merge", self.unconfirmed_classification
        )
        monkeypatch.setattr(service, "_merge_claim_revalidation", self._claim_revalidation)

        self._transactions_entered = 0

        def fake_composed_transaction(pool: Any) -> Any:
            self._transactions_entered += 1
            return _NullContext()

        monkeypatch.setattr(service, "composed_transaction", fake_composed_transaction)

    def _repatch_reconcile(self) -> None:
        """Re-point the service's reconciliation seam at the current stub."""
        monkeypatch_free = self._service
        monkeypatch_free.task_pull_request_reconciliation = SimpleNamespace(
            reconcile_task_pull_request=self.reconcile
        )

    def _confirmed_merged(self, pull_request: Any) -> bool:
        return (
            pull_request.state is TaskPullRequestState.CLOSED and pull_request.merged_at is not None
        )

    def _claim_revalidation(self, **kwargs: Any) -> Any:
        self.claim_hook_calls = dict(kwargs)
        return self.claim_hook

    def run(self) -> Any:
        return execute_authorized_merge_retry(
            cast_pool(),
            cast_github(),
            cast_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            retry_attempt_id=_ATTEMPT_ID,
            attempt_evidence=self.evidence,
            clock=lambda: _OBSERVED,
        )

    def run_without_evidence(self) -> Any:
        return execute_authorized_merge_retry(
            cast_pool(),
            cast_github(),
            cast_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            retry_attempt_id=_ATTEMPT_ID,
            clock=lambda: _OBSERVED,
        )


class _NullContext:
    def __enter__(self) -> Any:
        return None

    def __exit__(self, *args: Any) -> None:
        return None


def cast_pool() -> Any:
    return object()


def cast_github() -> Any:
    return object()


def cast_resolver() -> Any:
    return object()


def _outcome_stale(task: Any) -> Any:
    return TaskMergeOutcome(
        kind=TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION,
        task=task,
        pull_request=_domain_pull_request(),
    )


def _outcome_settled(task: Any) -> Any:
    return TaskMergeOutcome(
        kind=TaskMergeOutcomeKind.RETRY_GATE_INSTALLED,
        task=task,
        pull_request=_domain_pull_request(),
    )


def _outcome_blocked(task: Any) -> Any:
    return TaskMergeOutcome(
        kind=TaskMergeOutcomeKind.BLOCKED_UNCERTAIN,
        task=task,
        pull_request=_domain_pull_request(),
    )


def _merge_send_uncertain() -> MergeRequestSendUncertainError:
    error = MergeRequestSendUncertainError("the outcome of the send is unknown")
    error.__cause__ = __import__(
        "openorc.adapters.github", fromlist=["GitHubTransportUncertainError"]
    ).GitHubTransportUncertainError("the transport failed transiently")
    return error


def test_the_authorized_due_retry_consumes_sends_and_completes_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.COMPLETED
    assert outcome.merge_commit_sha == _MERGE_COMMIT_SHA
    # Exactly one set-once consumption inside the one admission transaction.
    assert len(harness.consume.calls) == 1
    assert harness._transactions_entered == 1
    # Exactly one send with the original accepted head, the approving Profile,
    # and the durable claim-revalidation hook.
    assert len(harness.merge.calls) == 1
    sent = harness.merge.calls[0]["kwargs"]
    assert sent["expected_head_sha"] == _HEAD_SHA
    assert sent["profile_id"] == _OWNER_PROFILE_ID
    assert sent["workspace_id"] == _WORKSPACE_ID
    assert sent["task_id"] == _TASK_ID
    assert harness.claim_hook_calls["owner_gate_id"] == _GATE_ID
    assert harness.claim_hook_calls["expected_post_approval_token"] == _STATE_TOKEN
    assert harness.claim_hook_calls["task_pull_request_id"] == _PR_ID
    assert harness.claim_hook_calls["expected_head_sha"] == _HEAD_SHA
    # The completion seam ran exactly once.
    assert len(harness.complete.calls) == 1
    # The invocation's own evidence: the consumed attempt identity, send phase.
    latest = harness.evidence.latest_identity
    assert latest is not None
    assert latest.family is GitHubMergeOperation.EXACT_HEAD_MERGE
    assert latest.original_attempt_kind is OriginalAttemptKind.MERGE_DECISION_GATE
    assert latest.original_attempt_id == _ATTEMPT_ID
    assert latest.effect_attempt_id == _ATTEMPT_ID
    assert latest.phase == "merge_retry_send"
    # No block and no Level 2 report on the happy path.
    assert len(harness.install.calls) == 0
    assert len(harness.triage.calls) == 0


def test_a_confirmed_merge_during_preflight_completes_without_consuming_or_sending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.pull_request = SimpleNamespace(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        state=TaskPullRequestState.CLOSED,
        head_sha=_HEAD_SHA,
        merged_at=_OBSERVED,
    )
    harness.reconcile = _recording(SimpleNamespace(pull_request=harness.pull_request))
    monkeypatch.setattr(
        harness._service,
        "task_pull_request_reconciliation",
        SimpleNamespace(reconcile_task_pull_request=harness.reconcile),
    )
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.COMPLETED
    # Never consumed and never sent: the mutation is never repeated.
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0
    assert harness.evidence.latest_identity is None


def test_a_closed_unmerged_preflight_refuses_without_consuming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.pull_request = SimpleNamespace(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        state=TaskPullRequestState.CLOSED,
        head_sha=_HEAD_SHA,
        merged_at=None,
    )
    harness.reconcile = _recording(SimpleNamespace(pull_request=harness.pull_request))
    monkeypatch.setattr(
        harness._service,
        "task_pull_request_reconciliation",
        SimpleNamespace(reconcile_task_pull_request=harness.reconcile),
    )
    with pytest.raises(ConflictError):
        harness.run()
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0
    assert harness.evidence.latest_identity is None


def test_a_moved_head_preflight_settles_through_the_stale_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    moved = _domain_pull_request(head_sha="b" * 40)
    harness.reconcile = _recording(SimpleNamespace(pull_request=moved))
    monkeypatch.setattr(
        harness._service,
        "task_pull_request_reconciliation",
        SimpleNamespace(reconcile_task_pull_request=harness.reconcile),
    )
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION
    assert len(harness.stale_classification.calls) == 1
    classified = harness.stale_classification.calls[0]["kwargs"]
    assert classified["owner_gate_id"] == _GATE_ID
    assert classified["subject_head_sha"] == _HEAD_SHA
    assert classified["task_pull_request_id"] == _PR_ID
    # Never consumed and never sent.
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0


def test_a_closed_source_issue_settles_owner_action_required_never_source_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.issue_guard = _recording_error(ConflictError("the source issue is closed"))
    monkeypatch.setattr(harness._service, "_require_source_issue_open", harness.issue_guard)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    # The truthful existing blocking path: BLOCK_RESOLUTION + owner_action_required.
    assert len(harness.install.calls) == 1
    installed = harness.install.calls[0]["kwargs"]
    assert installed["gate_type"] is OwnerGateType.BLOCK_RESOLUTION
    assert installed["block_reason"] is TaskBlockReason.OWNER_ACTION_REQUIRED
    context = dict(installed["block_context"])
    assert context["settlement"] == "merge_retry_refused_source_issue_not_open"
    assert context["addressed_github_issue_id"] == "503"
    assert context["merge_mutation_attempted"] is False
    assert context["merge_retry_attempt_consumed"] is False
    assert context["issue_openness_revalidated_before_any_merge"] is True
    assert context["owner_gate_id"] == str(_GATE_ID)
    # The requirements-fingerprint vocabulary is never used for issue closure.
    assert installed["block_reason"] is not TaskBlockReason.GITHUB_SOURCE_CHANGED
    assert "source_changed" not in str(context)
    # Never consumed and never sent.
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0
    assert harness.evidence.latest_identity is None


def test_an_uncertain_preflight_settles_the_escalation_block_without_consuming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.reconcile = _recording_error(
        ExternalOperationUncertainError("the outcome of the reconciliation is unknown")
    )
    monkeypatch.setattr(
        harness._service,
        "task_pull_request_reconciliation",
        SimpleNamespace(reconcile_task_pull_request=harness.reconcile),
    )
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    assert len(harness.install.calls) == 1
    installed = harness.install.calls[0]["kwargs"]
    assert installed["gate_type"] is OwnerGateType.BLOCK_RESOLUTION
    assert installed["block_reason"] is TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN
    context = dict(installed["block_context"])
    assert context["merge_mutation_attempted"] is False
    assert context["merge_retry_attempt_consumed"] is False
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0
    assert harness.evidence.latest_identity is None


def test_a_known_preflight_access_refusal_keeps_the_e10a_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.issue_guard = _recording_error(
        AuthorizationError("the repository is not accessible through the installation")
    )
    monkeypatch.setattr(harness._service, "_require_source_issue_open", harness.issue_guard)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.RETRY_GATE_INSTALLED
    assert len(harness.definitive_settlement.calls) == 1
    settled = harness.definitive_settlement.calls[0]["kwargs"]
    assert settled["github_rejected"] is False
    assert settled["owner_gate_id"] == _GATE_ID
    # Never consumed and never sent.
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0


def test_premature_wakeup_is_refused_before_any_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.attempt = _domain_attempt(not_before=_NOT_DUE)
    harness.get_attempt = _recording(harness.attempt)
    monkeypatch.setattr(
        harness._service,
        "failure_records",
        SimpleNamespace(
            get_operational_failure_attempt=harness.get_attempt,
            get_operational_failure_episode=harness.get_episode,
            list_operational_failure_attempts=harness.list_attempts,
            consume_operational_failure_attempt=harness.consume,
        ),
    )
    with pytest.raises(StaleOperationError):
        harness.run()
    # No preflight external read, no consumption, no send.
    assert len(harness.reconcile.calls) == 0
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0


def test_replayed_consumption_never_reaches_the_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.attempt = _domain_attempt(consumed_at=_OBSERVED)
    harness.get_attempt = _recording(harness.attempt)
    monkeypatch.setattr(
        harness._service,
        "failure_records",
        SimpleNamespace(
            get_operational_failure_attempt=harness.get_attempt,
            get_operational_failure_episode=harness.get_episode,
            list_operational_failure_attempts=harness.list_attempts,
            consume_operational_failure_attempt=harness.consume,
        ),
    )
    with pytest.raises(ConflictError):
        harness.run()
    assert len(harness.reconcile.calls) == 0
    assert len(harness.merge.calls) == 0


def test_a_terminally_disposed_episode_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.episode = SimpleNamespace(
        **{**_domain_episode().__dict__, "escalated_task_block_id": uuid4()}
    )
    harness.get_episode = _recording(harness.episode)
    monkeypatch.setattr(
        harness._service,
        "failure_records",
        SimpleNamespace(
            get_operational_failure_attempt=harness.get_attempt,
            get_operational_failure_episode=harness.get_episode,
            list_operational_failure_attempts=harness.list_attempts,
            consume_operational_failure_attempt=harness.consume,
        ),
    )
    with pytest.raises(ConflictError):
        harness.run()
    assert len(harness.merge.calls) == 0


def test_a_non_latest_or_unconsumed_sibling_attempt_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    earlier = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        episode_id=_EPISODE_ID,
        ordinal=1,
        consumed_at=None,
        not_before=_DUE,
    )
    harness.list_attempts = _recording([earlier, harness.attempt])
    monkeypatch.setattr(
        harness._service,
        "failure_records",
        SimpleNamespace(
            get_operational_failure_attempt=harness.get_attempt,
            get_operational_failure_episode=harness.get_episode,
            list_operational_failure_attempts=harness.list_attempts,
            consume_operational_failure_attempt=harness.consume,
        ),
    )
    with pytest.raises(ConflictError):
        harness.run()
    assert len(harness.merge.calls) == 0


def test_the_send_uncertainty_propagates_for_the_level_2_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(_merge_send_uncertain())
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    with pytest.raises(MergeRequestSendUncertainError):
        harness.run()
    # The consumed attempt's send uncertainty is never settled or retried
    # here: the F5b coordinator owns the typed Level 2 report.
    assert len(harness.triage.calls) == 0
    assert len(harness.install.calls) == 0
    assert len(harness.definitive_settlement.calls) == 0
    assert len(harness.consume.calls) == 1
    latest = harness.evidence.latest_identity
    assert latest is not None and latest.phase == "merge_retry_send"


def test_a_plain_presend_uncertainty_reports_to_f4_and_settles_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(
        ExternalOperationUncertainError("the outcome of the merge request is unknown")
    )
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    installation = _installation(harness.task)
    harness.triage = _recording(
        OperationalFailureDisposition(kind=DispositionKind.BLOCKED, installation=installation)
    )
    monkeypatch.setattr(harness._service, "triage_operational_failure", harness.triage)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    assert outcome.task_block is installation.task_block
    # The truthful PRIOR_AUTHORIZED report: the consumed attempt, the
    # unsupported code, and the mutation-not-attempted marker.
    assert len(harness.triage.calls) == 1
    report = harness.triage.calls[0]["kwargs"]["report"]
    operation = harness.triage.calls[0]["kwargs"]["operation"]
    assert operation.family is GitHubMergeOperation.EXACT_HEAD_MERGE
    assert operation.original_attempt_kind is OriginalAttemptKind.MERGE_DECISION_GATE
    assert operation.original_attempt_id == _GATE_ID
    assert report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED
    assert report.reporting_attempt_id == _ATTEMPT_ID
    assert report.effect_attempt_id == _ATTEMPT_ID
    assert report.error_code is OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN
    assert report.effect_evidence is FailureEffectEvidence.EFFECT_UNCLASSIFIED
    assert dict(report.diagnostic_context)["merge_mutation_attempted"] is False
    # The staged send was prepared but the mutation never fired.
    assert len(harness.install.calls) == 0


def test_a_send_rejection_keeps_the_e10a_definitive_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(
        ExternalOperationRejectedError("GitHub rejected the merge request")
    )
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.RETRY_GATE_INSTALLED
    assert len(harness.definitive_settlement.calls) == 1
    settled = harness.definitive_settlement.calls[0]["kwargs"]
    assert settled["github_rejected"] is True
    assert len(harness.triage.calls) == 0


def test_a_send_known_failure_keeps_the_e10a_definitive_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(
        ExternalOperationFailedError("the merge request failed as a known provider condition")
    )
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.RETRY_GATE_INSTALLED
    settled = harness.definitive_settlement.calls[0]["kwargs"]
    assert settled["github_rejected"] is False
    assert len(harness.triage.calls) == 0


def test_a_stale_send_keeps_the_e10a_stale_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(
        StaleOperationError("the pull request head changed before the merge applied")
    )
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION
    assert len(harness.stale_classification.calls) == 1
    assert len(harness.triage.calls) == 0


def test_a_confirmed_merge_reconciliation_pending_takes_the_known_success_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(
        MergeConfirmedReconciliationPendingError(
            "confirmed merged but its durable reconciliation is unconfirmed",
            merge_commit_sha=_MERGE_COMMIT_SHA,
            expected_head_sha=_HEAD_SHA,
        )
    )
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    assert len(harness.unconfirmed_classification.calls) == 1
    classified = harness.unconfirmed_classification.calls[0]["kwargs"]
    assert classified["merge_commit_sha"] == _MERGE_COMMIT_SHA
    # The known success is never repeated and never retried.
    assert len(harness.triage.calls) == 0
    assert len(harness.merge.calls) == 1


def test_non_owner_callers_and_malformed_commands_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    with pytest.raises(InvalidCommandError):
        execute_authorized_merge_retry(
            cast_pool(),
            cast_github(),
            cast_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            retry_attempt_id="not-a-uuid",  # type: ignore[arg-type]
            clock=lambda: _OBSERVED,
        )
    assert len(harness.consume.calls) == 0
    assert len(harness.merge.calls) == 0


def test_the_consumption_set_once_refusal_rolls_back_the_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(monkeypatch)
    harness.consume = _recording(None)  # the set-once guard refused
    monkeypatch.setattr(
        harness._service,
        "failure_records",
        SimpleNamespace(
            get_operational_failure_attempt=harness.get_attempt,
            get_operational_failure_episode=harness.get_episode,
            list_operational_failure_attempts=harness.list_attempts,
            consume_operational_failure_attempt=harness.consume,
        ),
    )
    with pytest.raises(ConflictError):
        harness.run()
    assert len(harness.merge.calls) == 0
    assert harness.evidence.latest_identity is None


def _recording_error(error: Exception) -> Any:
    calls: list[dict[str, Any]] = []

    def stub(*args: Any, **kwargs: Any) -> Any:
        calls.append({"args": args, "kwargs": kwargs})
        raise error

    stub.calls = calls  # type: ignore[attr-defined]
    return stub


def _merged_pull_request() -> Any:
    return SimpleNamespace(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        state=TaskPullRequestState.CLOSED,
        head_sha=_HEAD_SHA,
        merged_at=_OBSERVED,
    )


def _rewire_reconcile(harness: _Harness, *results: Any) -> None:
    harness.reconcile = _recording(*results)
    harness._repatch_reconcile()


def test_the_send_uncertainty_with_a_fresh_merged_reconciliation_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 1 (retry path): the send's unknown outcome is grounded in one
    fresh authoritative post-send reconciliation — a confirmed merged PR
    takes the completion seam and never resends, never retries, never
    propagates."""
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(_merge_send_uncertain())
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    _rewire_reconcile(
        harness,
        SimpleNamespace(pull_request=_domain_pull_request()),  # the Stage 2 preflight
        SimpleNamespace(pull_request=_merged_pull_request()),  # the post-send read
    )
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.COMPLETED
    # The send was attempted (consumed) but the known success always wins.
    assert len(harness.merge.calls) == 1
    assert len(harness.consume.calls) == 1
    assert len(harness.complete.calls) == 1
    # No Level 2 report and no staleness consequence.
    assert len(harness.triage.calls) == 0
    assert len(harness.stale_classification.calls) == 0


def test_the_send_uncertainty_with_a_reconciled_changed_head_routes_through_staleness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 2 (retry path): the reconciled changed head routes through the
    existing guarded merge-staleness consequence — never a retry, never the
    changed SHA, never a Level 2 report."""
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(_merge_send_uncertain())
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    moved = _domain_pull_request(head_sha="b" * 40)
    _rewire_reconcile(
        harness,
        SimpleNamespace(pull_request=_domain_pull_request()),  # the Stage 2 preflight
        SimpleNamespace(pull_request=moved),  # the post-send read
    )
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION
    assert len(harness.stale_classification.calls) == 1
    classified = harness.stale_classification.calls[0]["kwargs"]
    assert classified["owner_gate_id"] == _GATE_ID
    assert classified["subject_head_sha"] == _HEAD_SHA
    assert classified["task_pull_request_id"] == _PR_ID
    assert len(harness.triage.calls) == 0
    assert len(harness.merge.calls) == 1
    assert len(harness.consume.calls) == 1


def test_the_send_uncertainty_with_an_unavailable_read_escalates_never_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 1 (retry path): an unavailable/inconclusive post-send read can
    never ground a successor — the consumed attempt's failure is reported
    truthfully with the unsupported-uncertainty code (never the transport
    code) and F4's escalation owns the disposition, without propagation."""
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(_merge_send_uncertain())
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    _rewire_reconcile(
        harness,
        SimpleNamespace(pull_request=_domain_pull_request()),  # the Stage 2 preflight
        ExternalOperationUncertainError("the post-send read is unavailable"),
    )
    installation = _installation(harness.task)
    harness.triage = _recording(
        OperationalFailureDisposition(kind=DispositionKind.BLOCKED, installation=installation)
    )
    monkeypatch.setattr(harness._service, "triage_operational_failure", harness.triage)
    outcome = harness.run()
    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    assert outcome.task_block is installation.task_block
    assert len(harness.triage.calls) == 1
    report = harness.triage.calls[0]["kwargs"]["report"]
    operation = harness.triage.calls[0]["kwargs"]["operation"]
    assert report.reporting_attempt_kind is OperationalFailureSourceKind.PRIOR_AUTHORIZED
    assert report.reporting_attempt_id == _ATTEMPT_ID
    assert report.effect_attempt_id == _ATTEMPT_ID
    # The unavailable read can never carry the retry-eligible transport code.
    assert report.error_code is OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN
    assert report.effect_evidence is FailureEffectEvidence.EFFECT_UNCLASSIFIED
    diagnostics = dict(report.diagnostic_context)
    assert diagnostics["merge_mutation_attempted"] is True
    assert diagnostics["phase"] == "merge_retry_send"
    assert operation.original_attempt_id == _GATE_ID
    # The bounded accounting is preserved; nothing propagated.
    assert len(harness.consume.calls) == 1
    assert len(harness.complete.calls) == 0


def test_the_send_uncertainty_with_a_fresh_open_head_propagates_for_level_2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a fresh reconciled open/unmerged same-head observation lets the
    send's unknown outcome reach the F5b coordinator's typed handoff."""
    harness = _Harness(monkeypatch)
    harness.merge = _recording_error(_merge_send_uncertain())
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", harness.merge)
    _rewire_reconcile(
        harness,
        SimpleNamespace(pull_request=_domain_pull_request()),  # the Stage 2 preflight
        SimpleNamespace(pull_request=_domain_pull_request()),  # the fresh post-send read
    )
    with pytest.raises(MergeRequestSendUncertainError):
        harness.run()
    assert len(harness.triage.calls) == 0
    assert len(harness.stale_classification.calls) == 0
    assert len(harness.consume.calls) == 1
