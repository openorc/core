"""Deterministic tests for the detached terminal cleanup dispatch (issue #344).

The regular-mode detached dispatch of the terminal finalizer's optional
Producer CLEANUP_WORKSPACE turn, on top of the #317 finalizer suite (which
keeps proving the blocking queue-mode interim and the mandatory archival):

- the explicit dispatch selection: the blocking interim keeps the
  established flow; a regular-mode process with an unavailable observation
  positively skips the optional cleanup (no claim, no receipt, no send) and
  archives;
- the eligibility-before-claim ordering: a positively established
  ineligible or unsafe terminalization skips without a claim, receipt, or
  admission, and the mandatory archival proceeds;
- the atomic claim + receipt: the one-shot claim and its ``terminal_cleanup``
  receipt companion (rooted at the exact Task UUID, expecting the canonical
  ``workspace_result`` family) commit in one short transaction before any
  effect; a single-outstanding conflict or an under-lock claim race rolls
  back and classifies as a skip, never a second enqueue;
- the admission: the positively required registration gate, ONE verbatim
  admission of the canonical rendered instruction at the exact external
  session and original base, the acknowledgement attach, and the
  ``CLEANUP_OUTSTANDING`` exit without settling — a proven pre-effect
  refusal refuses the receipt, settles the claim, and falls back
  archive-only; an uncertain admission stays claimed and defers archival.

No live Postgres, GitHub, or Cline access; the persistence seams are
monkeypatched at the module boundary the same way the detached-resume
dispatch tests (issue #343) wire them.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from openorc.adapters.agent_runtime import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import (
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import CleanupWorkspaceInteraction
from openorc.protocol.models import WORKSPACE_RESULT_FAMILY
from openorc.services import task_terminal_finalization as finalization_module
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    ObservationRegistrationRefusedError,
)
from openorc.services.task_detached_turn_correlation import (
    TurnSettlementKind,
    TurnSettlementOutcome,
)
from openorc.services.task_terminal_cleanup import (
    TerminalCheckoutCleanupEstablishment,
    TerminalCheckoutCleanupRuntime,
    TerminalCleanupEstablishmentKind,
)
from openorc.services.task_terminal_finalization import (
    TerminalCleanupDispatch,
    TerminalCleanupDispatchSelection,
    TerminalFinalizationKind,
    TerminalFinalizationOutcome,
    finalize_task_terminalization,
)

_OBSERVED = datetime(2026, 11, 21, 12, 0, 0, tzinfo=UTC)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")
_OWNER_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_CONNECTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000004")
_BINDING_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000005")
_REPOSITORY_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000006")
_EXTERNAL_SESSION_ID = "producer-external-session"
_ORIGINAL_BASE = "/srv/runtime/repositories"


def _pool() -> DatabasePool:
    """The detached tests never touch a real pool; persistence is patched."""
    return cast(DatabasePool, object())


class _DetachedCleanupRuntime(TerminalCheckoutCleanupRuntime):
    """A cleanup runtime fake exposing the detached admission capability.

    The capability is duck-typed by the finalizer exactly as the production
    Cline composition exposes it; the fake records the exact admission
    arguments so the tests assert the serialized same-session reconstruction
    inputs without any real Cline. The blocking realization is never reached
    on the detached path.
    """

    def __init__(self, outcome: str = "ok") -> None:
        self._outcome = outcome
        self.admissions: list[dict[str, Any]] = []

    def cleanup_checkout(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        working_directory: str,
        interaction: Any,
    ) -> Any:
        raise AssertionError("the detached dispatch tests never realize blocking cleanup")

    def admit_cleanup_turn(
        self,
        pool: Any,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        working_directory: str,
        interaction: Any,
    ) -> AgentTurnAdmission:
        self.admissions.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "external_session_id": external_session_id,
                "working_directory": working_directory,
                "interaction": interaction,
            }
        )
        if self._outcome == "uncertain":
            raise AgentRuntimeUncertainOutcomeError("unknown admission outcome")
        if self._outcome == "refused":
            raise AgentRuntimeError("the runtime refused the admission")
        if self._outcome == "attachment_failed":
            raise ExternalOperationFailedError(
                "the cline runtime attachment failed before any workflow effect"
            )
        if self._outcome == "attachment_uncertain":
            raise ExternalOperationUncertainError(
                "the outcome of the cline runtime attachment is unknown; "
                "no workflow effect was dispatched and nothing is retried here"
            )
        if self._outcome == "unexpected":
            raise RuntimeError("an unclassified composition failure")
        return AgentTurnAdmission(turn_reference="fake-run-344")


class _BlockingOnlyRuntime(TerminalCheckoutCleanupRuntime):
    """A cleanup runtime fake with NO detached admission capability."""

    def cleanup_checkout(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        working_directory: str,
        interaction: Any,
    ) -> Any:
        raise AssertionError("the detached dispatch tests never realize blocking cleanup")


class _CleanupRegistrar:
    """The structural registration-seam fake (the positive pre-admission gate)."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.registered: list[dict[str, Any]] = []

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        self.registered.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "task_agent_session_id": task_agent_session_id,
                "connection_id": connection_id,
                "external_session_id": external_session_id,
            }
        )
        if self.refuse:
            raise ObservationRegistrationRefusedError(
                "registration refused",
                reason="fixture_refused",
            )


class _DetachedCalls:
    """Every recorded seam crossing of the detached dispatch tests."""

    def __init__(self) -> None:
        self.claims: list[UUID] = []
        self.settles: list[UUID] = []
        self.receipts: list[dict[str, Any]] = []
        self.refusals: list[dict[str, Any]] = []
        self.attaches: list[dict[str, Any]] = []
        self.archive_calls: list[dict[str, Any]] = []
        self.blocking_executions: list[dict[str, Any]] = []


def _cleanup_task() -> SimpleNamespace:
    """A committed, not-yet-archived terminalization for the detached tests."""
    return SimpleNamespace(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_number=4311,
        status=TaskStatus.WAITING_FOR_OWNER,
        archived_at=None,
        terminalization_outcome="cancelled",
        terminalization_origin="owner_cancelled",
        terminalization_established_at=_OBSERVED,
        cleanup_send_claimed_at=None,
        cleanup_send_settled_at=None,
    )


def _cleanup_binding() -> SimpleNamespace:
    return SimpleNamespace(
        id=_BINDING_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=WorkflowRole.PRODUCER,
        connection_id=_CONNECTION_ID,
        external_session_id=_EXTERNAL_SESSION_ID,
        lifecycle_status=TaskSessionLifecycleStatus.READY,
    )


def _cleanup_interaction() -> CleanupWorkspaceInteraction:
    return CleanupWorkspaceInteraction(
        repository_full_name="octocat/admitted-repo",
        issue_number=4311,
        task_uuid=_TASK_ID,
        target_directory=f"{_ORIGINAL_BASE}/{_TASK_ID}",
    )


def _eligible_establishment() -> TerminalCheckoutCleanupEstablishment:
    return TerminalCheckoutCleanupEstablishment(
        kind=TerminalCleanupEstablishmentKind.ELIGIBLE,
        task=cast(Task, _cleanup_task()),
        binding=cast(TaskAgentSession, _cleanup_binding()),
        interaction=_cleanup_interaction(),
        external_session_id=_EXTERNAL_SESSION_ID,
        original_base=_ORIGINAL_BASE,
    )


def _skipped_establishment(
    kind: TerminalCleanupEstablishmentKind,
) -> TerminalCheckoutCleanupEstablishment:
    return TerminalCheckoutCleanupEstablishment(kind=kind)


def _cleanup_receipt() -> TaskDetachedTurnReceipt:
    return TaskDetachedTurnReceipt(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_BINDING_ID,
        operation=PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP,
        original_attempt_kind=OriginalAttemptKind.TERMINAL_CLEANUP_CLAIM,
        original_attempt_id=_TASK_ID,
        expected_formal_family=WORKSPACE_RESULT_FAMILY,
        admitted_turn_reference=None,
        status=TaskDetachedTurnReceiptStatus.CLAIMED,
        outcome={},
        contradicted_at=None,
        claimed_at=_OBSERVED,
        admitted_at=None,
        settled_at=None,
        updated_at=_OBSERVED,
    )


def _wire_detached(
    monkeypatch: pytest.MonkeyPatch,
    calls: _DetachedCalls,
    *,
    establishment: TerminalCheckoutCleanupEstablishment,
    claim: bool = True,
    claimed_row: bool = False,
    receipt_error: Exception | None = None,
    attach_outcome: Any = None,
) -> TaskDetachedTurnReceipt:
    """Wire the detached dispatch's persistence seams at the module boundary."""

    @contextmanager
    def _fake_tx(pool: Any) -> Iterator[Any]:
        yield pool

    monkeypatch.setattr(finalization_module, "composed_transaction", _fake_tx)
    task = establishment.task if establishment.task is not None else cast(Task, _cleanup_task())
    locked_task = task
    if claimed_row:
        locked_task = SimpleNamespace(
            **{
                **{name: getattr(task, name) for name in vars(task)},
                "cleanup_send_claimed_at": _OBSERVED,
            }
        )
    monkeypatch.setattr(
        finalization_module.task_records,
        "get_task",
        lambda pool, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        finalization_module.task_records,
        "get_task_for_update",
        lambda pool, task_id: locked_task if task_id == _TASK_ID else None,
    )

    def _claim(pool: Any, task_id: UUID) -> Any:
        calls.claims.append(task_id)
        return _OBSERVED if claim else None

    monkeypatch.setattr(
        finalization_module.task_records, "claim_task_terminal_cleanup_send", _claim
    )

    def _settle(pool: Any, task_id: UUID) -> Any:
        calls.settles.append(task_id)
        return task

    monkeypatch.setattr(
        finalization_module.task_records, "settle_task_terminal_cleanup_send", _settle
    )
    monkeypatch.setattr(
        finalization_module.session_records,
        "get_task_agent_session",
        lambda pool, *, task_id, role: (
            establishment.binding if role is WorkflowRole.PRODUCER else None
        ),
    )
    monkeypatch.setattr(
        finalization_module,
        "establish_terminal_checkout_cleanup",
        lambda pool, *, workspace_id, task_id: establishment,
    )

    made_receipt = _cleanup_receipt()

    def _record(transaction_pool: Any, **kwargs: Any) -> TaskDetachedTurnReceipt:
        calls.receipts.append(dict(kwargs))
        if receipt_error is not None:
            raise receipt_error
        return made_receipt

    monkeypatch.setattr(finalization_module, "record_task_detached_turn_receipt", _record)

    def _refuse(
        pool: Any, *, receipt_id: UUID, task_id: UUID, workspace_id: UUID, refusal_kind: str
    ) -> Any:
        calls.refusals.append({"receipt_id": receipt_id, "refusal_kind": refusal_kind})
        return made_receipt

    monkeypatch.setattr(
        finalization_module.turn_records, "refuse_task_detached_turn_receipt", _refuse
    )

    def _attach(
        pool: Any,
        *,
        workspace_id: UUID,
        task_id: UUID,
        receipt_id: UUID,
        admitted_turn_reference: str,
    ) -> Any:
        calls.attaches.append(
            {"receipt_id": receipt_id, "admitted_turn_reference": admitted_turn_reference}
        )
        if attach_outcome is not None:
            return attach_outcome
        return TurnSettlementOutcome(kind=TurnSettlementKind.ADMITTED, receipt=None)

    monkeypatch.setattr(finalization_module, "attach_acknowledged_turn", _attach)

    def _archive(pool: Any, *, workspace_id: UUID, task_id: UUID) -> TerminalFinalizationOutcome:
        calls.archive_calls.append({"workspace_id": workspace_id, "task_id": task_id})
        return TerminalFinalizationOutcome(
            kind=TerminalFinalizationKind.ARCHIVED,
            task=task,
            released_session_bindings=(),
        )

    monkeypatch.setattr(finalization_module, "_archive_committed_terminalization", _archive)

    def _blocking(pool: Any, runtime: Any, *, workspace_id: UUID, task_id: UUID) -> Any:
        calls.blocking_executions.append({"workspace_id": workspace_id, "task_id": task_id})
        raise AssertionError("the detached dispatch tests never reach the blocking realization")

    monkeypatch.setattr(finalization_module, "execute_terminal_checkout_cleanup", _blocking)
    return made_receipt


def test_a_committed_terminalization_admits_one_detached_cleanup_and_exits_outstanding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Committed terminalization → eligibility → single claim + receipt →
    registration → ONE verbatim enqueue at the same-ID ACT context inputs →
    short dispatch: ``CLEANUP_OUTSTANDING``, no settle, no archival, and a
    successful enqueue that was never cleanup success."""
    calls = _DetachedCalls()
    establishment = _eligible_establishment()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    receipt = _wire_detached(monkeypatch, calls, establishment=establishment)

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.CLEANUP_OUTSTANDING
    # The claim was taken exactly once and its receipt companion roots at the
    # exact Task UUID with the canonical workspace_result family.
    assert calls.claims == [_TASK_ID]
    assert len(calls.receipts) == 1
    receipt_kwargs = calls.receipts[0]
    assert receipt_kwargs["operation"] is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP
    assert receipt_kwargs["original_attempt_kind"] is OriginalAttemptKind.TERMINAL_CLEANUP_CLAIM
    assert receipt_kwargs["original_attempt_id"] == _TASK_ID
    assert receipt_kwargs["expected_formal_family"] == WORKSPACE_RESULT_FAMILY
    assert receipt_kwargs["task_agent_session_id"] == _BINDING_ID
    # The positively required registration addresses the exact binding.
    assert registrar.registered == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "task_agent_session_id": _BINDING_ID,
            "connection_id": _CONNECTION_ID,
            "external_session_id": _EXTERNAL_SESSION_ID,
        }
    ]
    # ONE admission of the canonical rendered instruction verbatim, addressed
    # to the exact external session at the caller-derived original base.
    interaction = establishment.interaction
    assert interaction is not None
    assert runtime.admissions == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "external_session_id": _EXTERNAL_SESSION_ID,
            "working_directory": _ORIGINAL_BASE,
            "interaction": interaction,
        }
    ]
    assert runtime.admissions[0]["interaction"].render() == interaction.render()
    # The acknowledgement attach consumed the turn reference exactly once.
    assert calls.attaches == [{"receipt_id": receipt.id, "admitted_turn_reference": "fake-run-344"}]
    # An admitted attempt stays claimed: no settle, no refusal, no archival.
    assert calls.settles == []
    assert calls.refusals == []
    assert calls.archive_calls == []
    assert calls.blocking_executions == []


def test_an_ineligible_or_unsafe_terminalization_skips_without_claim_or_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positively established ineligible/unsafe case (no READY binding, no
    applicable checkout, or outstanding Producer work) takes the archive-only
    path without a claim, a receipt, a registration, or an admission — the
    mandatory archival never depends on optional housekeeping."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    _wire_detached(
        monkeypatch,
        calls,
        establishment=_skipped_establishment(
            TerminalCleanupEstablishmentKind.SKIPPED_UNSAFE_OUTSTANDING_WORK
        ),
    )

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.claims == []
    assert calls.receipts == []
    assert calls.refusals == []
    assert registrar.registered == []
    assert runtime.admissions == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_a_capability_less_runtime_skips_without_any_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The detached capability is duck-typed fail-closed BEFORE any claim: a
    runtime without it cannot serve the detached dispatch, and nothing is
    claimed that could never be dispatched."""
    calls = _DetachedCalls()
    runtime = _BlockingOnlyRuntime()
    registrar = _CleanupRegistrar()
    _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.claims == []
    assert calls.receipts == []
    assert registrar.registered == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_an_unavailable_regular_mode_observation_skips_archive_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A regular-mode process whose detached-turn observation cannot resolve
    positively skips the optional cleanup (no claim, no receipt, no blocking
    send) and proceeds to the mandatory archival — unavailable observation
    never selects blocking execution."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.OBSERVATION_UNAVAILABLE
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.claims == []
    assert calls.receipts == []
    assert registrar.registered == []
    assert runtime.admissions == []
    assert calls.blocking_executions == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_a_proven_registration_refusal_settles_the_claim_and_archives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positively established pre-admission registration refusal refuses the
    just-claimed receipt, settles the one-shot claim, and continues to the
    established archive-only finalization — never a re-send, never a
    stranded claimed receipt, never Level 2/3 machinery."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar(refuse=True)
    receipt = _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.claims == [_TASK_ID]
    assert calls.refusals == [
        {"receipt_id": receipt.id, "refusal_kind": "observation_registration_refused"}
    ]
    assert calls.settles == [_TASK_ID]
    assert runtime.admissions == []
    assert calls.attaches == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_a_proven_admission_refusal_settles_the_claim_and_archives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positively identified admission refusal (including the adapter's
    D6a/D6b quiescence discipline) is proven pre-effect: the receipt is
    refused, the claim settles, and the archive-only finalization follows."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime(outcome="refused")
    registrar = _CleanupRegistrar()
    receipt = _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert len(runtime.admissions) == 1
    assert calls.refusals == [{"receipt_id": receipt.id, "refusal_kind": "admission_refused"}]
    assert calls.settles == [_TASK_ID]
    assert calls.attaches == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_a_proven_attachment_failure_settles_the_claim_and_archives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production composition's attachment seam (issue #344 review) fails
    with typed application errors strictly BEFORE the enqueue is ever
    reached — the seam attests no workflow effect was dispatched. The proven
    pre-effect failure refuses the receipt, settles the claim, and continues
    the archive-only finalization instead of stranding the terminalization."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime(outcome="attachment_failed")
    registrar = _CleanupRegistrar()
    receipt = _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.claims == [_TASK_ID]
    assert calls.refusals == [
        {"receipt_id": receipt.id, "refusal_kind": "runtime_attachment_failed"}
    ]
    assert calls.settles == [_TASK_ID]
    assert calls.attaches == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_an_uncertain_attachment_is_a_proven_pre_effect_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncertain ATTACHMENT is not an uncertain ADMISSION: the attachment
    lease's own classification attests no workflow effect was dispatched (the
    attach precedes the enqueue), so the proven pre-effect accounting applies
    — receipt refused, claim settled, archive-only — while an uncertain
    enqueue outcome would stay conservatively outstanding."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime(outcome="attachment_uncertain")
    registrar = _CleanupRegistrar()
    receipt = _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.refusals == [
        {"receipt_id": receipt.id, "refusal_kind": "runtime_attachment_failed"}
    ]
    assert calls.settles == [_TASK_ID]
    assert calls.attaches == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_an_unclassified_admission_failure_stays_conservatively_outstanding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unclassified failure inside the composition: whether the turn was
    enqueued cannot be ruled out, so the attempt stays conservatively
    outstanding — never a proven non-delivery, never a re-send, and the
    independent settlement/deadline disposition (#345) owns the later
    disposition."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime(outcome="unexpected")
    registrar = _CleanupRegistrar()
    _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.CLEANUP_OUTSTANDING
    assert calls.refusals == []
    assert calls.settles == []
    assert calls.attaches == []
    assert calls.archive_calls == []


def test_an_uncertain_admission_stays_claimed_and_defers_archival(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncertain admission is conservatively outstanding: the receipt stays
    claimed, the claim stays unsettled, archival stays deferred (capacity
    held), and the instruction is never re-dispatched."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime(outcome="uncertain")
    registrar = _CleanupRegistrar()
    _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.CLEANUP_OUTSTANDING
    assert len(runtime.admissions) == 1
    assert calls.refusals == []
    assert calls.settles == []
    assert calls.attaches == []
    assert calls.archive_calls == []


def test_a_single_outstanding_conflict_at_the_claim_skips_and_archives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concurrent unsettled receipt appearing on the binding between the
    eligibility read and the claim transaction violates the
    single-outstanding backstop: the whole claim+receipt composition rolls
    back, no admission follows, and the archival proceeds."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    _wire_detached(
        monkeypatch,
        calls,
        establishment=_eligible_establishment(),
        receipt_error=ConflictError(
            "the session binding already carries an outstanding detached-turn receipt"
        ),
    )

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    # The receipt insert was attempted exactly once and the composition was
    # refused; no claim window can strand, no admission followed, and the
    # archival reported the truthful idempotent outcome.
    assert len(calls.receipts) == 1
    assert calls.refusals == []
    assert runtime.admissions == []
    assert registrar.registered == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_an_under_lock_claim_race_never_enqueues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claim that moved on between the establishment and the locked reproof
    (a concurrent finalizer won the CAS) skips without a receipt or
    admission; the archival continuation's own window guard owns the truthful
    deferred-or-idempotent outcome."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    _wire_detached(
        monkeypatch,
        calls,
        establishment=_eligible_establishment(),
        claimed_row=True,
    )

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.claims == []
    assert calls.receipts == []
    assert runtime.admissions == []
    assert registrar.registered == []
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_the_queue_mode_interim_keeps_the_blocking_realization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The explicitly selected blocking interim keeps the established #317
    flow — the blocking realization is invoked, never the detached admission,
    and the claim settles in the invocation as before."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    _wire_detached(monkeypatch, calls, establishment=_eligible_establishment())
    # Unblock the blocking seam for this test: it records the execution.
    monkeypatch.setattr(
        finalization_module,
        "execute_terminal_checkout_cleanup",
        lambda pool, runtime, *, workspace_id, task_id: calls.blocking_executions.append(
            {"workspace_id": workspace_id, "task_id": task_id}
        ),
    )

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.BLOCKING_QUEUE_INTERIM
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.ARCHIVED
    assert calls.blocking_executions == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]
    # The detached seams never ran.
    assert calls.receipts == []
    assert runtime.admissions == []
    assert registrar.registered == []
    # The blocking interim settles the claim in the invocation, as before.
    assert calls.settles == [_TASK_ID]
    assert calls.archive_calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]


def test_a_staged_early_completion_consumes_once_at_the_attach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A terminal observation that resolved to the receipt during the
    admission race is consumed exactly once by the acknowledgement attach and
    routes through the terminal-cleanup settlement; the dispatch still exits
    ``CLEANUP_OUTSTANDING`` with the claim unsettled — the archival
    disposition stays the owning finalization's (issue #345)."""
    calls = _DetachedCalls()
    runtime = _DetachedCleanupRuntime()
    registrar = _CleanupRegistrar()
    receipt = _wire_detached(
        monkeypatch,
        calls,
        establishment=_eligible_establishment(),
        attach_outcome=TurnSettlementOutcome(
            kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT,
            receipt=None,
        ),
    )

    outcome = finalize_task_terminalization(
        _pool(),
        runtime,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        dispatch=TerminalCleanupDispatch(
            selection=TerminalCleanupDispatchSelection.DETACHED_REGULAR,
            observation_registrar=registrar,
        ),
    )

    assert outcome.kind is TerminalFinalizationKind.CLEANUP_OUTSTANDING
    assert calls.attaches == [{"receipt_id": receipt.id, "admitted_turn_reference": "fake-run-344"}]
    assert calls.settles == []
    assert calls.refusals == []
    assert calls.archive_calls == []
