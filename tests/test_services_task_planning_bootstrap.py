"""Deterministic tests for the planning-bootstrap continuation (issue #203).

Scripted ``AgentRuntimeAdapter`` doubles (the #69 fake runtime) plus a small
injected ``TaskBootstrapRuntimeComposition`` fake stand in for the runtime
exactly as the issue requires, and a scripted fake connection seam proves the
phase composition against canned rows: the one-shot claim written exactly
once before any runtime effect, the stage matrix (first establishment,
mixed/both-READY re-entry fail-closed with zero runtime effects, terminal
conflicts, stale tokens), the per-role READY-reuse/CONNECTING-establish
branch, the pre-PREPARE exact-subject recheck, the authoritative checkout
target from the original snapshot base, one typed PREPARE to the exact bound
Producer, the post-PREPARE and final rechecks, and the runtime-neutral
planning-context transition with no premature PLAN. Durable establishment,
admission, protocol, and reconstruction semantics are already proven by the
existing suites and are not duplicated here.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeTimeoutError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import TaskStatus
from openorc.observability import TASK_ID, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import PrepareWorkspaceInteraction
from openorc.protocol.models import WORKSPACE_RESULT_FAMILY
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    ObservationRegistrationRefusedError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    triage_operational_failure,
)
from openorc.services.task_planning_bootstrap import (
    PlanningBootstrapTerminalConflictError,
    PlanningBootstrapUnresolvedError,
    TaskBootstrapEstablishmentLease,
    TaskBootstrapRuntimeComposition,
    TaskPlanningBootstrapDispatchOutcome,
    TaskPlanningPreparationOutcome,
    WorkspacePreparationFailedError,
    complete_settled_bootstrap_prepare_transition,
    prepare_task_for_planning,
    settle_observed_bootstrap_prepare_result,
)
from openorc.services.task_workflow_operation_coordinator import (
    coordinate_task_workflow_operation,
)
from tests.fakes.agent_runtime import FakeAgentRuntimeAdapter, session_ready_candidate
from tests.fakes.task_rows import task_row
from tests.test_services_task_pr_review_decision import _event_returned_row

_OBSERVED = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)
_BARRIER_ROW = (None, None, None)  # account-operational barrier: no attempt state

_REPOSITORY_BASE = "/srv/selected/repositories"
_NEUTRAL_DIRECTORY = "/srv/neutral/review"
_SELECTED_CONFIGURATION = {"repository_base": _REPOSITORY_BASE}
_READY_SNAPSHOT = {"adapter": "cline", "configuration": _SELECTED_CONFIGURATION}


# One canned statement outcome: a row tuple, a row list, None (empty
# result), a callable evaluated against the connection when it is popped, or
# an exception instance the seam raises instead of returning.
_CannedResult = Any


class FakeCursor:
    """Returns one canned result, like a psycopg cursor (row or row list)."""

    def __init__(self, result: Any) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._result is None:
            return None
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._result is None:
            return []
        if isinstance(self._result, list):
            return list(self._result)
        return [self._result]


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL
    and the connection/transaction lifecycle. A canned result may be a
    callable evaluated against the connection at pop time, so a later read
    can observe a value an earlier statement in the same continuation wrote
    (the one-shot bootstrap claim token)."""

    def __init__(self, results: Sequence[Any]) -> None:
        self.results: list[Any] = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.lifecycle: list[str] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        if callable(result):
            result = result(self)
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        self.lifecycle.append("tx_enter")
        try:
            yield
        finally:
            self.lifecycle.append("tx_exit")


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        conn = self._conn

        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            conn.lifecycle.append("pool_enter")
            try:
                yield conn
            finally:
                conn.lifecycle.append("pool_exit")

        return managed()

    def close(self) -> None:
        raise AssertionError("planning bootstrap service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _claim_token_from_written_claims(conn: ScriptedConnection) -> tuple[Any, ...]:
    """Resolve the claim token an earlier claim statement wrote in this
    continuation, for a later claim-fact read's canned row."""
    for sql, params in conn.executed:
        if (
            "update openorc.tasks" in sql
            and "planning_bootstrap_claim_token = %s" in sql
            and params is not None
        ):
            return (params[0],)
    raise AssertionError("no planning-bootstrap claim statement has executed yet")


def _ws_row(workspace_id: Any, owner_profile_id: Any, *, guidance: str = "") -> tuple[Any, ...]:
    return (
        workspace_id,
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        5,
        guidance,
        None,
    )


def _task_row(
    workspace_id: Any,
    *,
    row_id: Any = None,
    status: str = "planning",
    state_token: Any = None,
) -> tuple[Any, ...]:
    return task_row(
        id=row_id if row_id is not None else uuid.uuid4(),
        workspace_id=workspace_id,
        status=status,
        state_token=state_token if state_token is not None else uuid.uuid4(),
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _repository_row(task_row: tuple[Any, ...]) -> tuple[Any, ...]:
    return (
        task_row[2],  # repository_id
        uuid.uuid4(),  # project_id
        task_row[1],  # workspace_id
        5_000_000,  # github_repository_id
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        None,
    )


def _connection_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    enabled: bool = True,
    safe_config: Any = None,
) -> tuple[Any, ...]:
    return (
        connection_id,
        workspace_id,
        "cline",
        "primary cline hub",
        {} if safe_config is None else safe_config,
        2,
        enabled,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _session_row(
    workspace_id: Any,
    task_id: Any,
    role: str,
    connection_id: Any,
    *,
    row_id: Any = None,
    lifecycle_status: str = "connecting",
    external_session_id: str | None = None,
    initialized_at: Any = None,
    snapshot: Any = None,
) -> tuple[Any, ...]:
    return (
        row_id if row_id is not None else uuid.uuid4(),
        workspace_id,
        task_id,
        role,
        connection_id,
        external_session_id,
        lifecycle_status,
        snapshot if snapshot is not None else ({} if external_session_id is not None else None),
        None,
        None,
        None,
        initialized_at,
        _OBSERVED if lifecycle_status == "ended" else None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _ready_session_row(
    workspace_id: Any,
    task_id: Any,
    role: str,
    connection_id: Any,
    *,
    external_session_id: str,
) -> tuple[Any, ...]:
    return _session_row(
        workspace_id,
        task_id,
        role,
        connection_id,
        lifecycle_status="ready",
        external_session_id=external_session_id,
        initialized_at=_OBSERVED,
        snapshot=json.loads(json.dumps(_READY_SNAPSHOT)),
    )


def _workspace_result_candidate(operation: str, status: str) -> dict[str, Any]:
    return {
        "type": "workspace_result",
        "schema_version": 1,
        "operation": operation,
        "status": status,
        "summary": "Reported by the scripted runtime.",
    }


class _ScriptedBootstrapRuntime(TaskBootstrapRuntimeComposition):
    """Small injected composition fake: one adapter per role, scripted
    establishment/interaction leases, and a recorded planning transition."""

    def __init__(
        self,
        adapters: dict[WorkflowRole, AgentRuntimeAdapter],
        *,
        selected_configuration: dict[str, Any],
        producer_send_outcomes: Sequence[Any] = (),
    ) -> None:
        self._adapters: dict[WorkflowRole, Any] = dict(adapters)
        self._selected_configuration = selected_configuration
        self._producer_send_outcomes = list(producer_send_outcomes)
        self.establishment_leases: list[tuple[WorkflowRole, str | None]] = []
        self.interaction_leases: list[tuple[WorkflowRole, str]] = []
        self.transitions: list[tuple[str, str]] = []

    def establishment_lease(
        self, pool: Any, *, workspace_id: Any, task_id: Any, role: Any, working_directory: Any
    ) -> Any:
        self.establishment_leases.append((role, working_directory))

        @contextmanager
        def lease() -> Iterator[TaskBootstrapEstablishmentLease]:
            yield TaskBootstrapEstablishmentLease(
                adapter=self._adapters[role],
                selected_configuration=dict(self._selected_configuration),
            )

        return lease()

    def interaction_lease(
        self, pool: Any, *, workspace_id: Any, task_id: Any, role: Any, external_session_id: str
    ) -> Any:
        self.interaction_leases.append((role, external_session_id))
        # The scripted send outcomes are queued here, when the session the
        # establishment phase created already exists on the fake adapter.
        adapter = self._adapters[role]
        assert isinstance(adapter, FakeAgentRuntimeAdapter)
        for outcome in self._producer_send_outcomes:
            adapter.queue_send(external_session_id, outcome)
        self._producer_send_outcomes = []

        @contextmanager
        def lease() -> Iterator[AgentRuntimeAdapter]:
            yield self._adapters[role]

        return lease()

    def fake_adapters(self) -> tuple[FakeAgentRuntimeAdapter, ...]:
        return tuple(
            adapter
            for adapter in self._adapters.values()
            if isinstance(adapter, FakeAgentRuntimeAdapter)
        )

    def fake_adapter(self, role: WorkflowRole) -> FakeAgentRuntimeAdapter:
        adapter = self._adapters[role]
        assert isinstance(adapter, FakeAgentRuntimeAdapter)
        return adapter

    def enter_planning_checkout(
        self,
        pool: Any,
        *,
        workspace_id: Any,
        task_id: Any,
        external_session_id: str,
        checkout_directory: str,
    ) -> None:
        self.transitions.append((external_session_id, checkout_directory))


def _archived_task_row(workspace_id: Any, row_id: Any, state_token: Any) -> tuple[Any, ...]:
    row = list(_task_row(workspace_id, row_id=row_id, status="cancelled", state_token=state_token))
    row[6] = _OBSERVED  # archived_at
    return tuple(row)


def _entry_results(
    workspace_id: Any,
    owner_profile_id: Any,
    task_row: tuple[Any, ...],
    *,
    claim: tuple[Any, ...] | None,
    producer_row: tuple[Any, ...] | None,
    reviewer_row: tuple[Any, ...],
    claim_write_result: tuple[Any, ...] | None,
    post_claim_task_row: tuple[Any, ...] | None,
    claim_attempted: bool = True,
    classifier_results: Sequence[Any] = (),
    claim_evidence_row: tuple[Any, ...] | None = None,
) -> list[Any]:
    """The entry/claim transaction's canned statement sequence, in order.

    ``claim_evidence_row`` (claimed entries only) plays back the disposition
    matrix's stage-evidence read; ``classifier_results`` plays back its
    outstanding-initialization-receipt reads in the exact order the
    classifier issues them.
    """
    results: list[Any] = [
        _BARRIER_ROW,  # require_account_operational
        None,  # the per-Workspace arbitration advisory lock
        _ws_row(workspace_id, owner_profile_id),  # require_workspace_task: workspace
        task_row,  # require_workspace_task: task
        task_row,  # require_current_task
        claim,  # the one-shot claim fact read
        producer_row,
        reviewer_row,
    ]
    if claim is not None:
        # The claimed-bootstrap disposition matrix (issue #324) reads the
        # stage evidence and the durable establishment receipts inside the
        # same transaction.
        results.append(claim_evidence_row)
        results.extend(classifier_results)
    if claim_attempted and claim is None:
        # The exactly-once claim write executes only on the one provably safe
        # first establishment (its result may be a rejected None); the
        # post-claim Task re-read follows only a successful claim. A claimed
        # entry never writes.
        results.append(claim_write_result)
        if claim_write_result is not None:
            assert post_claim_task_row is not None
            results.append(post_claim_task_row)
    return results


def _establish_results(
    workspace_id: Any,
    owner_profile_id: Any,
    task_row: tuple[Any, ...],
    binding_row: tuple[Any, ...],
    connection_row: tuple[Any, ...],
    ready_row: tuple[Any, ...] | None,
) -> list[Any]:
    """One role's #130 establishment canned sequence (read + finalize)."""
    return [
        _ws_row(workspace_id, owner_profile_id),
        _BARRIER_ROW,
        task_row,
        binding_row,
        connection_row,
        _ws_row(workspace_id, owner_profile_id),
        _BARRIER_ROW,
        ready_row if ready_row is not None else (None,),
    ]


def _recheck_results(
    task_row: tuple[Any, ...],
    claim_result: Any,
    producer_ready_row: tuple[Any, ...],
    reviewer_ready_row: tuple[Any, ...],
) -> list[Any]:
    """One exact-subject recheck transaction's canned sequence, in order."""
    return [
        _BARRIER_ROW,
        task_row,
        claim_result,
        producer_ready_row,
        reviewer_ready_row,
    ]


def _base_repository_results(
    producer_ready_row: tuple[Any, ...],
    connection_row: tuple[Any, ...],
    repository_row: tuple[Any, ...],
) -> list[Any]:
    """The pre-PREPARE durable recovery sequence, in order."""
    return [
        producer_ready_row,
        connection_row,
        repository_row,
    ]


class _Bootstrap:
    """Assembled deterministic fixtures for one bootstrap continuation."""

    def __init__(
        self,
        *,
        producer_external_id: str = "fake-session-1",
        reviewer_external_id: str = "fake-session-2",
    ) -> None:
        self.workspace_id = uuid.uuid4()
        self.owner_profile_id = uuid.uuid4()
        self.task_id = uuid.uuid4()
        self.connection_id = uuid.uuid4()
        self.pre_claim_token = uuid.uuid4()
        self.post_claim_token = uuid.uuid4()
        self.task_row = _task_row(
            self.workspace_id, row_id=self.task_id, state_token=self.pre_claim_token
        )
        self.post_claim_task_row = _task_row(
            self.workspace_id,
            row_id=self.task_id,
            status="planning",
            state_token=self.post_claim_token,
        )
        self.producer_connecting_row = _session_row(
            self.workspace_id, self.task_id, "producer", self.connection_id
        )
        self.reviewer_connecting_row = _session_row(
            self.workspace_id, self.task_id, "reviewer", self.connection_id
        )
        self.connection_row = _connection_row(
            self.workspace_id, self.connection_id, safe_config=dict(_SELECTED_CONFIGURATION)
        )
        self.repository_row = _repository_row(self.task_row)
        self.producer_ready_row = _ready_session_row(
            self.workspace_id,
            self.task_id,
            "producer",
            self.connection_id,
            external_session_id=producer_external_id,
        )
        self.reviewer_ready_row = _ready_session_row(
            self.workspace_id,
            self.task_id,
            "reviewer",
            self.connection_id,
            external_session_id=reviewer_external_id,
        )
        self.checkout_directory = f"{_REPOSITORY_BASE}/{self.task_id}"

    def adapters(
        self, *, producer_outcomes: Sequence[Any] | None = None
    ) -> tuple[FakeAgentRuntimeAdapter, FakeAgentRuntimeAdapter]:
        producer = FakeAgentRuntimeAdapter()
        producer.queue_creation(session_ready_candidate())
        for outcome in producer_outcomes or ():
            producer.queue_send(self.producer_ready_row[5], outcome)
        reviewer = FakeAgentRuntimeAdapter()
        reviewer.queue_creation(session_ready_candidate())
        return producer, reviewer

    def runtime(
        self, *, producer_send_outcomes: Sequence[Any] | None = None
    ) -> _ScriptedBootstrapRuntime:
        producer, reviewer = self.adapters()
        outcomes = (
            (_workspace_result_candidate("PREPARE", "COMPLETED"),)
            if producer_send_outcomes is None
            else tuple(producer_send_outcomes)
        )
        return _ScriptedBootstrapRuntime(
            {WorkflowRole.PRODUCER: producer, WorkflowRole.REVIEWER: reviewer},
            selected_configuration=dict(_SELECTED_CONFIGURATION),
            producer_send_outcomes=outcomes,
        )

    def entry_results(self) -> list[Any]:
        return _entry_results(
            self.workspace_id,
            self.owner_profile_id,
            self.task_row,
            claim=None,
            producer_row=self.producer_connecting_row,
            reviewer_row=self.reviewer_connecting_row,
            claim_write_result=(self.post_claim_token,),
            post_claim_task_row=self.post_claim_task_row,
        )

    def happy_path_results(self) -> list[Any]:
        return [
            *self.entry_results(),
            *_establish_results(
                self.workspace_id,
                self.owner_profile_id,
                self.task_row,
                self.producer_connecting_row,
                self.connection_row,
                self.producer_ready_row,
            ),
            *_establish_results(
                self.workspace_id,
                self.owner_profile_id,
                self.task_row,
                self.reviewer_connecting_row,
                self.connection_row,
                self.reviewer_ready_row,
            ),
            *_recheck_results(
                self.post_claim_task_row,
                _claim_token_from_written_claims,
                self.producer_ready_row,
                self.reviewer_ready_row,
            ),
            *_base_repository_results(
                self.producer_ready_row,
                self.connection_row,
                self.repository_row,
            ),
            # The detached bootstrap's durable PREPARE-dispatch marker
            # (issue #324): the stage evidence advances to
            # ``prepare_dispatched`` strictly before the PREPARE send.
            True,
            # The F2 stage evidence (issue #87): the PREPARE control's
            # validated result settles the forward-only ``prepared`` stage,
            # and the settled planning-context transition settles
            # ``ready_for_planning`` — two observational writes between the
            # base observation and the post-PREPARE recheck, and between the
            # context transition and the final recheck.
            True,
            *_recheck_results(
                self.post_claim_task_row,
                _claim_token_from_written_claims,
                self.producer_ready_row,
                self.reviewer_ready_row,
            ),
            True,
            *_recheck_results(
                self.post_claim_task_row,
                _claim_token_from_written_claims,
                self.producer_ready_row,
                self.reviewer_ready_row,
            ),
        ]

    def run(self, conn: ScriptedConnection, runtime: Any) -> Any:
        return self.run_with_token(conn, runtime, expected_state_token=self.pre_claim_token)

    def run_with_token(
        self, conn: ScriptedConnection, runtime: Any, *, expected_state_token: Any
    ) -> Any:
        return prepare_task_for_planning(
            _pool(conn),
            runtime,
            workspace_id=self.workspace_id,
            task_id=self.task_id,
            profile_id=self.owner_profile_id,
            expected_state_token=expected_state_token,
            reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
        )


def _claim_statement_index(conn: ScriptedConnection) -> int:
    for index, (sql, _params) in enumerate(conn.executed):
        if "update openorc.tasks" in sql and "planning_bootstrap_claim_token = %s" in sql:
            return index
    raise AssertionError("no planning-bootstrap claim statement executed")


def _statement_indices(conn: ScriptedConnection, fragment: str) -> list[int]:
    return [index for index, (sql, _params) in enumerate(conn.executed) if fragment in sql]


def test_successful_bootstrap_claims_once_establishes_prepares_and_transitions() -> None:
    env = _Bootstrap()
    conn = ScriptedConnection(env.happy_path_results())
    runtime = env.runtime()

    outcome = env.run(conn, runtime)

    assert outcome.task.state_token == env.post_claim_token
    assert outcome.task.status.value == "planning"
    assert outcome.producer_session.external_session_id == "fake-session-1"
    assert outcome.producer_session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert outcome.reviewer_session.external_session_id == "fake-session-2"
    assert outcome.reviewer_session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert outcome.producer_checkout_directory == env.checkout_directory

    # The one-shot claim is written exactly once, before any runtime effect;
    # the F2 stage-evidence writes (issue #87) bind to the same claim token in
    # their WHERE clause and are counted separately below.
    claim_updates = _statement_indices(conn, "set planning_bootstrap_claim_token = %s")
    assert len(claim_updates) == 1
    establish_workspace_reads = _statement_indices(conn, "from openorc.workspaces")
    # The entry transaction's ownership read precedes the claim; the
    # establishment phase's own workspace reads follow it.
    assert establish_workspace_reads[1] > claim_updates[0]

    # The three forward-only stage settlements: the durable PREPARE-dispatch
    # marker (issue #324) strictly before the send, ``prepared`` after the
    # validated workspace result (issue #87), and ``ready_for_planning``
    # after the planning-context transition — bound to the exact claim
    # identity, and never a state-token rotation or a second claim.
    stage_writes = [
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "planning_bootstrap_stage = %s" in sql
    ]
    assert len(stage_writes) == 3
    assert claim_updates[0] < stage_writes[0] < stage_writes[1] < stage_writes[2]

    # Exactly the two role-session READY finalizations, and no other
    # session write.
    session_updates = _statement_indices(conn, "update openorc.task_agent_sessions")
    assert len(session_updates) == 2

    # The composition leases: the Producer fresh lease selects its
    # configuration without a working directory; the Reviewer fresh lease
    # uses the supplied neutral directory; the PREPARE interaction lease
    # addresses the exact bound Producer; one planning transition.
    assert runtime.establishment_leases == [
        (WorkflowRole.PRODUCER, None),
        (WorkflowRole.REVIEWER, _NEUTRAL_DIRECTORY),
    ]
    assert runtime.interaction_leases == [(WorkflowRole.PRODUCER, "fake-session-1")]
    assert runtime.transitions == [("fake-session-1", env.checkout_directory)]

    # One formal PREPARE send to the exact Producer session; no plan-family
    # send anywhere.
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    sends = producer_adapter.submitted_sends()
    assert len(sends) == 1
    assert sends[0].external_session_id == "fake-session-1"
    assert sends[0].formal
    assert sends[0].expected_family == WORKSPACE_RESULT_FAMILY


def test_establishment_shares_one_selected_configuration_with_the_ready_snapshot() -> None:
    env = _Bootstrap()
    conn = ScriptedConnection(env.happy_path_results())
    runtime = env.runtime()

    env.run(conn, runtime)

    # The fresh lease's selected configuration is exactly the configuration
    # the durable READY snapshot records (the establish service assembles
    # the snapshot from the effective configuration the continuation passes
    # verbatim; the canned READY rows carry the same snapshot).
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    (producer_request,) = producer_adapter.creation_requests()
    assert producer_request.role is WorkflowRole.PRODUCER
    assert producer_request.workspace_id == env.workspace_id
    assert producer_request.task_id == env.task_id
    reviewer_adapter = runtime.fake_adapter(WorkflowRole.REVIEWER)
    (reviewer_request,) = reviewer_adapter.creation_requests()
    assert reviewer_request.role is WorkflowRole.REVIEWER
    assert _READY_SNAPSHOT["configuration"] == _SELECTED_CONFIGURATION


def test_checkout_target_is_the_original_snapshot_base_plus_task_uuid() -> None:
    env = _Bootstrap()
    conn = ScriptedConnection(env.happy_path_results())
    runtime = env.runtime()

    outcome = env.run(conn, runtime)

    # The only target is <original snapshot base>/<Task UUID>, derived from
    # the durable READY snapshot (the canned rows carry the same base the
    # fresh lease selected), never from caller or agent text.
    assert outcome.producer_checkout_directory == f"{_REPOSITORY_BASE}/{env.task_id}"
    prepare_sends = runtime.fake_adapter(WorkflowRole.PRODUCER).submitted_sends()
    assert len(prepare_sends) == 1
    assert f"{env.task_id}" in prepare_sends[0].message
    assert _REPOSITORY_BASE in prepare_sends[0].message


def test_reentry_after_uncertain_creation_with_the_refreshed_token_has_zero_effects() -> None:
    env = _Bootstrap()
    # Invocation A: the claim commits, then the Producer's creation times
    # out — the authoritative facts are retained and nothing is retried.
    first_results = [
        *env.entry_results(),
        *_establish_results(
            env.workspace_id,
            env.owner_profile_id,
            env.task_row,
            env.producer_connecting_row,
            env.connection_row,
            None,  # the finalize phase never runs
        ),
    ]
    first_runtime = _ScriptedBootstrapRuntime(
        {
            WorkflowRole.PRODUCER: FakeAgentRuntimeAdapter(),
            WorkflowRole.REVIEWER: FakeAgentRuntimeAdapter(),
        },
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    first_runtime.fake_adapter(WorkflowRole.PRODUCER).queue_creation(
        AgentRuntimeTimeoutError("the creation handshake timed out")
    )
    first_conn = ScriptedConnection(first_results)
    with pytest.raises(ExternalOperationUncertainError):
        env.run(first_conn, first_runtime)
    assert first_runtime.transitions == []
    assert len(first_runtime.fake_adapter(WorkflowRole.PRODUCER).creation_requests()) == 1

    # Invocation B re-enters with the REFRESHED current token (T1) — the
    # exact duplicate-bootstrap scenario token rotation alone cannot stop.
    # The durable one-shot claim fact does: the second invocation raises the
    # typed unresolved error and produces zero runtime effects.
    second_results = _entry_results(
        env.workspace_id,
        env.owner_profile_id,
        env.post_claim_task_row,
        claim=(uuid.uuid4(),),  # a prior claim exists
        producer_row=env.producer_connecting_row,
        reviewer_row=env.reviewer_connecting_row,
        claim_write_result=None,
        post_claim_task_row=None,
        claim_evidence_row=(uuid.uuid4(), None, None, None),
        classifier_results=(None, None),  # no outstanding initialization receipts
    )
    second_runtime = _ScriptedBootstrapRuntime(
        {
            WorkflowRole.PRODUCER: FakeAgentRuntimeAdapter(),
            WorkflowRole.REVIEWER: FakeAgentRuntimeAdapter(),
        },
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    second_conn = ScriptedConnection(second_results)
    with pytest.raises(PlanningBootstrapUnresolvedError):
        env.run_with_token(second_conn, second_runtime, expected_state_token=env.post_claim_token)
    second_adapter = second_runtime.fake_adapter(WorkflowRole.PRODUCER)
    assert second_adapter.creation_requests() == ()
    assert second_adapter.submitted_sends() == ()
    assert second_runtime.establishment_leases == []
    assert second_runtime.interaction_leases == []
    assert second_runtime.transitions == []
    # No durable write at all on the re-entry: no claim rewrite and no
    # session finalize.
    assert _statement_indices(second_conn, "update openorc.") == []


def test_reentry_with_the_pre_claim_token_is_stale_with_zero_effects() -> None:
    env = _Bootstrap()
    stale_task_row = _task_row(env.workspace_id, row_id=env.task_id, state_token=uuid.uuid4())
    conn = ScriptedConnection(
        _entry_results(
            env.workspace_id,
            env.owner_profile_id,
            stale_task_row,
            claim=(uuid.uuid4(),),
            producer_row=env.producer_connecting_row,
            reviewer_row=env.reviewer_connecting_row,
            claim_write_result=None,
            post_claim_task_row=None,
        )
    )
    runtime = _ScriptedBootstrapRuntime(
        {
            WorkflowRole.PRODUCER: FakeAgentRuntimeAdapter(),
            WorkflowRole.REVIEWER: FakeAgentRuntimeAdapter(),
        },
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    with pytest.raises(StaleOperationError):
        env.run(conn, runtime)
    assert runtime.establishment_leases == []
    assert runtime.transitions == []
    for adapter in runtime.fake_adapters():
        assert adapter.creation_requests() == ()


def test_mixed_entry_fails_closed_in_both_orientations() -> None:
    ready_first = _Bootstrap()
    results = _entry_results(
        ready_first.workspace_id,
        ready_first.owner_profile_id,
        ready_first.post_claim_task_row,
        claim=(uuid.uuid4(),),
        producer_row=ready_first.producer_ready_row,
        reviewer_row=ready_first.reviewer_connecting_row,
        claim_write_result=None,
        post_claim_task_row=None,
        claim_evidence_row=(uuid.uuid4(), None, None, None),
        classifier_results=(
            None,
            None,
            None,  # the READY binding carries no initialization receipt either
        ),
    )
    runtime = _ScriptedBootstrapRuntime(
        {
            WorkflowRole.PRODUCER: FakeAgentRuntimeAdapter(),
            WorkflowRole.REVIEWER: FakeAgentRuntimeAdapter(),
        },
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    conn = ScriptedConnection(results)
    with pytest.raises(PlanningBootstrapUnresolvedError):
        ready_first.run_with_token(conn, runtime, expected_state_token=ready_first.post_claim_token)
    assert runtime.establishment_leases == []
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    for adapter in runtime.fake_adapters():
        assert adapter.creation_requests() == ()
        assert adapter.submitted_sends() == ()
    assert _statement_indices(conn, "update openorc.") == []

    connecting_first = _Bootstrap()
    results = _entry_results(
        connecting_first.workspace_id,
        connecting_first.owner_profile_id,
        connecting_first.post_claim_task_row,
        claim=(uuid.uuid4(),),
        producer_row=connecting_first.producer_connecting_row,
        reviewer_row=connecting_first.reviewer_ready_row,
        claim_write_result=None,
        post_claim_task_row=None,
        claim_evidence_row=(uuid.uuid4(), None, None, None),
        classifier_results=(
            None,
            None,
            None,  # the READY binding carries no initialization receipt either
        ),
    )
    runtime = _ScriptedBootstrapRuntime(
        {
            WorkflowRole.PRODUCER: FakeAgentRuntimeAdapter(),
            WorkflowRole.REVIEWER: FakeAgentRuntimeAdapter(),
        },
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    conn = ScriptedConnection(results)
    with pytest.raises(PlanningBootstrapUnresolvedError):
        connecting_first.run_with_token(
            conn, runtime, expected_state_token=connecting_first.post_claim_token
        )
    assert runtime.establishment_leases == []
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert _statement_indices(conn, "update openorc.") == []


def test_both_ready_entry_fails_closed_without_any_prepare() -> None:
    env = _Bootstrap()
    conn = ScriptedConnection(
        _entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(uuid.uuid4(),),
            producer_row=env.producer_ready_row,
            reviewer_row=env.reviewer_ready_row,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(uuid.uuid4(), None, None, None),
            classifier_results=(
                None,
                None,
                None,  # neither READY binding carries an initialization receipt
                None,
            ),
        )
    )
    runtime = env.runtime()
    with pytest.raises(PlanningBootstrapUnresolvedError):
        env.run_with_token(conn, runtime, expected_state_token=env.post_claim_token)
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert _statement_indices(conn, "update openorc.") == []


def test_terminal_session_binding_fails_closed() -> None:
    for lifecycle in ("lost", "ended"):
        env = _Bootstrap()
        terminal_row = _session_row(
            env.workspace_id,
            env.task_id,
            "producer",
            env.connection_id,
            lifecycle_status=lifecycle,
            external_session_id="gone",
            initialized_at=_OBSERVED,
        )
        conn = ScriptedConnection(
            _entry_results(
                env.workspace_id,
                env.owner_profile_id,
                env.post_claim_task_row,
                claim=(uuid.uuid4(),),
                producer_row=terminal_row,
                reviewer_row=env.reviewer_connecting_row,
                claim_write_result=None,
                post_claim_task_row=None,
            )
        )
        runtime = env.runtime()
        with pytest.raises(PlanningBootstrapTerminalConflictError):
            env.run_with_token(conn, runtime, expected_state_token=env.post_claim_token)
        assert runtime.establishment_leases == []
        assert runtime.transitions == []


def test_stale_token_and_non_planning_task_fail_closed_at_entry() -> None:
    env = _Bootstrap()
    stale_row = _task_row(env.workspace_id, row_id=env.task_id, state_token=uuid.uuid4())
    conn = ScriptedConnection(
        _entry_results(
            env.workspace_id,
            env.owner_profile_id,
            stale_row,
            claim=None,
            producer_row=env.producer_connecting_row,
            reviewer_row=env.reviewer_connecting_row,
            claim_write_result=None,
            post_claim_task_row=None,
        )
    )
    runtime = env.runtime()
    with pytest.raises(StaleOperationError):
        env.run(conn, runtime)
    assert runtime.establishment_leases == []
    assert _statement_indices(conn, "update openorc.") == []

    ready_task = _Bootstrap()
    ready_task.task_row = _task_row(
        ready_task.workspace_id,
        row_id=ready_task.task_id,
        status="ready_to_plan",
        state_token=ready_task.pre_claim_token,
    )
    conn = ScriptedConnection(
        _entry_results(
            ready_task.workspace_id,
            ready_task.owner_profile_id,
            ready_task.task_row,
            claim=None,
            producer_row=ready_task.producer_connecting_row,
            reviewer_row=ready_task.reviewer_connecting_row,
            claim_write_result=None,
            post_claim_task_row=None,
        )
    )
    runtime = ready_task.runtime()
    with pytest.raises(ConflictError):
        ready_task.run(conn, runtime)
    assert runtime.establishment_leases == []


def test_concurrent_duplicate_claim_loser_is_stale_with_zero_effects() -> None:
    env = _Bootstrap()
    # The exactly-once claim write is lost to a concurrent continuation: the
    # loser's conditional write returns None, the invocation dispatches
    # nothing, and it never retries.
    results = _entry_results(
        env.workspace_id,
        env.owner_profile_id,
        env.task_row,
        claim=None,
        producer_row=env.producer_connecting_row,
        reviewer_row=env.reviewer_connecting_row,
        claim_write_result=None,  # the IS-NULL claim predicate rejected it
        post_claim_task_row=None,
    )
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    with pytest.raises(StaleOperationError):
        env.run(conn, runtime)
    assert runtime.establishment_leases == []
    assert runtime.transitions == []
    for adapter in runtime.fake_adapters():
        assert adapter.creation_requests() == ()
    assert _statement_indices(conn, "update openorc.task_agent_sessions") == []


def test_missing_binding_fails_closed_uniformly() -> None:
    env = _Bootstrap()
    conn = ScriptedConnection(
        _entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.task_row,
            claim=None,
            producer_row=None,
            reviewer_row=env.reviewer_connecting_row,
            claim_write_result=None,
            post_claim_task_row=None,
        )
    )
    runtime = env.runtime()
    with pytest.raises(NotFoundError):
        env.run(conn, runtime)
    assert runtime.establishment_leases == []


def test_known_establishment_failure_stops_progression() -> None:
    env = _Bootstrap()
    results = [
        *env.entry_results(),
        *_establish_results(
            env.workspace_id,
            env.owner_profile_id,
            env.task_row,
            env.producer_connecting_row,
            env.connection_row,
            None,
        ),
    ]
    runtime = _ScriptedBootstrapRuntime(
        {
            WorkflowRole.PRODUCER: FakeAgentRuntimeAdapter(),
            WorkflowRole.REVIEWER: FakeAgentRuntimeAdapter(),
        },
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    runtime.fake_adapter(WorkflowRole.PRODUCER).queue_creation(
        AgentRuntimeUnavailableError("the runtime is unavailable before any effect")
    )
    conn = ScriptedConnection(results)
    with pytest.raises(ExternalOperationFailedError):
        env.run(conn, runtime)
    # The binding stays CONNECTING, the Reviewer is never established, and
    # nothing is retried: the continuation is over.
    assert len(runtime.fake_adapter(WorkflowRole.PRODUCER).creation_requests()) == 1
    assert runtime.fake_adapter(WorkflowRole.REVIEWER).creation_requests() == ()
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert _statement_indices(conn, "update openorc.task_agent_sessions") == []


def test_prepared_result_failures_stop_progression_without_any_transition() -> None:
    failed = _Bootstrap()
    runtime = failed.runtime(
        producer_send_outcomes=(_workspace_result_candidate("PREPARE", "FAILED"),)
    )
    conn = ScriptedConnection(failed.happy_path_results())
    with pytest.raises(WorkspacePreparationFailedError):
        failed.run(conn, runtime)
    assert runtime.transitions == []
    # A FAILED preparation never authorizes a re-send or a cleanup: exactly
    # one PREPARE realization happened.
    assert len(runtime.fake_adapter(WorkflowRole.PRODUCER).submitted_sends()) == 1

    wrong_operation = _Bootstrap()
    runtime = wrong_operation.runtime(
        producer_send_outcomes=(_workspace_result_candidate("CLEANUP", "COMPLETED"),)
    )
    conn = ScriptedConnection(wrong_operation.happy_path_results())
    with pytest.raises(WorkspacePreparationFailedError):
        wrong_operation.run(conn, runtime)
    assert runtime.transitions == []

    uncertain = _Bootstrap()
    runtime = uncertain.runtime(
        producer_send_outcomes=(
            AgentRuntimeDeliveryUncertainError("the prepared send outcome is unknown"),
        )
    )
    conn = ScriptedConnection(uncertain.happy_path_results())
    with pytest.raises(ExternalOperationUncertainError):
        uncertain.run(conn, runtime)
    # The uncertain preparation is never retried and never replayed.
    assert len(runtime.fake_adapter(WorkflowRole.PRODUCER).submitted_sends()) == 1
    assert runtime.transitions == []


def test_task_rotated_during_establishment_blocks_prepare() -> None:
    env = _Bootstrap()
    rotated_row = _task_row(
        env.workspace_id,
        row_id=env.task_id,
        status="planning",
        state_token=uuid.uuid4(),  # a concurrent authoritative Task mutation
    )
    results = [
        *env.entry_results(),
        *_establish_results(
            env.workspace_id,
            env.owner_profile_id,
            env.task_row,
            env.producer_connecting_row,
            env.connection_row,
            env.producer_ready_row,
        ),
        *_establish_results(
            env.workspace_id,
            env.owner_profile_id,
            env.task_row,
            env.reviewer_connecting_row,
            env.connection_row,
            env.reviewer_ready_row,
        ),
        *_recheck_results(
            rotated_row,
            _claim_token_from_written_claims,
            env.producer_ready_row,
            env.reviewer_ready_row,
        ),
    ]
    runtime = env.runtime()
    conn = ScriptedConnection(results)
    with pytest.raises(StaleOperationError):
        env.run(conn, runtime)
    # The stale bootstrap subject never receives the checkout-creating
    # PREPARE effect.
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert runtime.fake_adapter(WorkflowRole.PRODUCER).submitted_sends() == ()


def test_command_shape_fails_closed_before_any_read() -> None:
    env = _Bootstrap()
    conn = ScriptedConnection([])
    with pytest.raises(InvalidCommandError):
        env.run(conn, "not-a-composition")
    with pytest.raises(InvalidCommandError):
        prepare_task_for_planning(
            _pool(conn),
            env.runtime(),
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.pre_claim_token,
            reviewer_neutral_directory="relative/checkout",
        )
    with pytest.raises(InvalidCommandError):
        prepare_task_for_planning(
            _pool(conn),
            env.runtime(),
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.pre_claim_token,
            reviewer_neutral_directory="/srv/../escape",
        )
    assert conn.executed == []


def test_service_span_carries_only_safe_annotations_and_fails_closed_safely() -> None:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    env = _Bootstrap()
    conn = ScriptedConnection(env.happy_path_results())
    runtime = env.runtime()
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        env.run(conn, runtime)
    (service_span,) = [
        span
        for span in exporter.get_finished_spans()
        if span.name == "task_planning_bootstrap.prepare_task_for_planning"
    ]
    attributes = service_span.attributes or {}
    assert attributes[WORKSPACE_ID] == str(env.workspace_id)
    assert attributes[TASK_ID] == str(env.task_id)
    assert service_span.status is not None
    assert service_span.status.status_code is StatusCode.UNSET

    failed_provider = TracerProvider()
    failed_exporter = InMemorySpanExporter()
    failed_provider.add_span_processor(SimpleSpanProcessor(failed_exporter))
    failing = _Bootstrap()
    failing_conn = ScriptedConnection(
        _entry_results(
            failing.workspace_id,
            failing.owner_profile_id,
            failing.post_claim_task_row,
            claim=(uuid.uuid4(),),
            producer_row=failing.producer_ready_row,
            reviewer_row=failing.reviewer_ready_row,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(uuid.uuid4(), None, None, None),
            classifier_results=(
                None,
                None,
                None,  # neither READY binding carries an initialization receipt
                None,
            ),
        )
    )
    with (
        injected_tracer_source(lambda name: failed_provider.get_tracer(name)),
        pytest.raises(PlanningBootstrapUnresolvedError),
    ):
        failing.run_with_token(
            failing_conn, failing.runtime(), expected_state_token=failing.post_claim_token
        )
    (failed_span,) = [
        span
        for span in failed_exporter.get_finished_spans()
        if span.name == "task_planning_bootstrap.prepare_task_for_planning"
    ]
    assert failed_span.status is not None
    assert failed_span.status.status_code is StatusCode.ERROR
    # The failure telemetry is a safe classification only: the description
    # is the exception type name, never arbitrary message content.
    assert failed_span.status.description == "PlanningBootstrapUnresolvedError"


# __CHUNK_APPEND_MARKER__


# --- the detached establishment dispatch and readiness resumption (issue #324) ---


def _detached_initialization_receipt_row(
    workspace_id: Any, task_id: Any, session_id: Any
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        task_id,
        session_id,
        "session_initialization",
        "planning_bootstrap_claim",
        uuid.uuid4(),
        "session_ready",
        None,
        "claimed",
        {},
        None,
        _OBSERVED,
        None,
        None,
        _OBSERVED,
        "ext-init-9",
        {"adapter": "cline", "configuration": {}},
        None,
        None,
    )


def _settled_initialization_receipt_row(
    workspace_id: Any, task_id: Any, session_id: Any, external_session_id: str
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        task_id,
        session_id,
        "session_initialization",
        "planning_bootstrap_claim",
        uuid.uuid4(),
        "session_ready",
        "run-324",
        "settled",
        {"settlement": "validated_result", "consumed_sequence": 7},
        None,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        external_session_id,
        {"adapter": "cline", "configuration": {}},
        None,
        None,
    )


def _detached_prepare_receipt_row(
    env: _Bootstrap, session_id: Any, claim_token: Any
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        env.workspace_id,
        env.task_id,
        session_id,
        "planning_bootstrap_prepare",
        "planning_bootstrap_claim",
        claim_token,
        "workspace_result",
        None,
        "claimed",
        {},
        None,
        _OBSERVED,
        None,
        None,
        _OBSERVED,
        None,
        None,
        None,
        None,
    )


def _dispatch_phase_results(
    env: _Bootstrap,
    binding_row: tuple[Any, ...],
    *,
    receipt_row: tuple[Any, ...] | None,
) -> list[Any]:
    """One detached dispatch's canned sequence: phase-1 reads, the replay
    guard's outstanding-receipt read, the receipt insert, the attach (lock,
    runId attach, staged read), and the post-attach binding reload."""
    results: list[Any] = [
        _ws_row(env.workspace_id, env.owner_profile_id),
        _BARRIER_ROW,
        _task_row(env.workspace_id),
        binding_row,
        env.connection_row,
        None,  # no outstanding receipt
    ]
    if receipt_row is not None:
        results.append(receipt_row)
        results.append(receipt_row)  # the attach's for-update lock
        results.append(receipt_row)  # the runId attach returns the admitted row
        results.append(None)  # no staged early completion
        results.append(binding_row)  # the post-attach binding reload
    return results


def test_detached_bootstrap_dispatches_and_returns_without_waiting() -> None:
    env = _Bootstrap()
    producer_receipt = _detached_initialization_receipt_row(
        env.workspace_id, env.task_id, env.producer_connecting_row[0]
    )
    reviewer_receipt = _detached_initialization_receipt_row(
        env.workspace_id, env.task_id, env.reviewer_connecting_row[0]
    )
    results = [
        *env.entry_results(),
        # Producer dispatch phase.
        *_dispatch_phase_results(env, env.producer_connecting_row, receipt_row=producer_receipt),
        # Reviewer dispatch phase.
        *_dispatch_phase_results(env, env.reviewer_connecting_row, receipt_row=reviewer_receipt),
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    handoff = _RecordingCompletionHandoff()

    outcome = prepare_task_for_planning(
        _pool(conn),
        runtime,
        workspace_id=env.workspace_id,
        task_id=env.task_id,
        profile_id=env.owner_profile_id,
        expected_state_token=env.pre_claim_token,
        reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
        observation_registrar=_RecordingRegistrar(),
        completion_handoff=handoff,
    )
    assert isinstance(outcome, TaskPlanningBootstrapDispatchOutcome)
    assert outcome.task.state_token == env.post_claim_token
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert handoff.calls == [
        (
            env.workspace_id,
            env.task_id,
            PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
        )
    ]
    # Both roles dispatched: the fake adapters record the detached capability
    # calls, never the blocking handshake.
    producer = runtime.fake_adapter(WorkflowRole.PRODUCER)
    reviewer = runtime.fake_adapter(WorkflowRole.REVIEWER)
    assert len(producer.detached_allocation_requests()) == 1
    assert len(producer.detached_admissions()) == 1
    assert len(reviewer.detached_allocation_requests()) == 1
    assert len(reviewer.detached_admissions()) == 1
    for adapter in (producer, reviewer):
        assert adapter.creation_requests() == ()
        assert adapter.submitted_sends() == ()
    inserts = [
        sql for sql, _ in conn.executed if "insert into openorc.task_hub_run_receipts" in sql
    ]
    assert len(inserts) == 2


def test_in_progress_replay_dispatches_nothing() -> None:
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_receipt = _detached_initialization_receipt_row(
        env.workspace_id, env.task_id, env.producer_connecting_row[0]
    )
    results = _entry_results(
        env.workspace_id,
        env.owner_profile_id,
        env.post_claim_task_row,
        claim=(claim_token,),
        producer_row=env.producer_connecting_row,
        reviewer_row=env.reviewer_connecting_row,
        claim_write_result=None,
        post_claim_task_row=None,
        claim_evidence_row=(claim_token, _OBSERVED, None, None),
        classifier_results=(producer_receipt, producer_receipt),
    )
    conn = ScriptedConnection(results)
    runtime = env.runtime()

    outcome = prepare_task_for_planning(
        _pool(conn),
        runtime,
        workspace_id=env.workspace_id,
        task_id=env.task_id,
        profile_id=env.owner_profile_id,
        expected_state_token=env.post_claim_token,
        reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
        observation_registrar=_RecordingRegistrar(),
        completion_handoff=_RecordingCompletionHandoff(),
    )

    # The claimed in-progress bootstrap is an idempotent no-effect replay:
    # no establishment lease, no runtime call, no durable write.
    assert isinstance(outcome, TaskPlanningBootstrapDispatchOutcome)
    assert outcome.claim_token == claim_token
    assert runtime.establishment_leases == []
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    for adapter in runtime.fake_adapters():
        assert adapter.detached_allocation_requests() == ()
        assert adapter.creation_requests() == ()
    assert _statement_indices(conn, "update openorc.") == []


def test_blocking_prepare_completes_the_bootstrap_without_a_registrar() -> None:
    """The queue-mode interim (until #328): the blocking PREPARE unchanged."""
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    results = [
        *_entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, None, None),
            classifier_results=(
                None,  # no outstanding producer receipt
                None,  # no outstanding reviewer receipt
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, producer_ready[0], "ext-p"
                ),
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, reviewer_ready[0], "ext-r"
                ),
            ),
        ),
        # The establishment loop skips both READY bindings; the pre-PREPARE
        # recheck, the durable base recovery, and the post-PREPARE tail follow.
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        *_base_repository_results(producer_ready, env.connection_row, env.repository_row),
        True,  # the durable PREPARE-dispatch marker (issue #324)
        True,  # the validated workspace result settles ``prepared``
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        True,  # the settled transition settles ``ready_for_planning``
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    # The READY bindings address sessions the runtime already owns: seed the
    # fake adapters with the allocated sessions the rows bind so the lease
    # can queue the scripted control outcome against the exact session.
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    reviewer_adapter = runtime.fake_adapter(WorkflowRole.REVIEWER)
    producer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.PRODUCER,
            initialization="seed",
        )
    )
    reviewer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.REVIEWER,
            initialization="seed",
        )
    )

    outcome = prepare_task_for_planning(
        _pool(conn),
        runtime,
        workspace_id=env.workspace_id,
        task_id=env.task_id,
        profile_id=env.owner_profile_id,
        expected_state_token=env.post_claim_token,
        reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
    )

    # The blocking control completes the whole bootstrap: the durable
    # PREPARE-dispatch marker was committed strictly before the send, and
    # the prepared-for-planning handoff completes.
    assert isinstance(outcome, TaskPlanningPreparationOutcome)
    assert outcome.producer_session.external_session_id == "fake-session-1"
    assert runtime.establishment_leases == []
    assert runtime.interaction_leases == [(WorkflowRole.PRODUCER, "fake-session-1")]
    assert runtime.transitions == [("fake-session-1", env.checkout_directory)]
    stage_writes = [
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "planning_bootstrap_stage = %s" in sql
    ]
    assert len(stage_writes) == 3


def test_readiness_resumed_continuation_dispatches_the_detached_prepare() -> None:
    """The regular-mode readiness-resumed continuation (issue #334).

    The established bootstrap proceeds past establishment into the detached
    workspace-preparation dispatch: the durable ``prepare_dispatched``
    marker and the detached-turn receipt commit in one short transaction
    strictly before the positive observation registration and the verbatim
    admission of the rendered canonical control, and the dispatching
    continuation returns without waiting for the workspace result.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    receipt_row = _detached_prepare_receipt_row(env, producer_ready[0], claim_token)
    registrar = _RecordingRegistrar()
    handoff = _RecordingCompletionHandoff()
    results = [
        *_entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, None, None),
            classifier_results=(
                None,  # no outstanding producer receipt
                None,  # no outstanding reviewer receipt
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, producer_ready[0], "ext-p"
                ),
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, reviewer_ready[0], "ext-r"
                ),
            ),
        ),
        # The establishment loop skips both READY bindings; the pre-PREPARE
        # recheck and the durable base recovery follow.
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        *_base_repository_results(producer_ready, env.connection_row, env.repository_row),
        # The detached PREPARE dispatch transaction: the stage evidence read,
        # the durable PREPARE-dispatch marker, and the detached-turn receipt.
        (claim_token, _OBSERVED, None, None),  # the fail-closed pre-check's evidence read
        (claim_token, _OBSERVED, None, None),
        True,
        receipt_row,
        # The acknowledgement attach: the for-update lock, the attach, and
        # the staged early-completion read (none staged).
        receipt_row,
        receipt_row,
        None,
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    # The detached establishments happened in an earlier process: seed the
    # fake adapters with the allocated sessions the READY rows bind
    # (fake-session-1 / fake-session-2 in creation order).
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    reviewer_adapter = runtime.fake_adapter(WorkflowRole.REVIEWER)
    producer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.PRODUCER,
            initialization="seed",
        )
    )
    reviewer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.REVIEWER,
            initialization="seed",
        )
    )

    outcome = prepare_task_for_planning(
        _pool(conn),
        runtime,
        workspace_id=env.workspace_id,
        task_id=env.task_id,
        profile_id=env.owner_profile_id,
        expected_state_token=env.post_claim_token,
        reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
        observation_registrar=registrar,
        completion_handoff=handoff,
    )

    # The detached PREPARE is durably dispatched: the worker returns without
    # waiting for the workspace result, and the observed completion owns the
    # settlement and the resumption.
    assert isinstance(outcome, TaskPlanningBootstrapDispatchOutcome)
    assert outcome.claim_token == claim_token
    assert outcome.post_claim_token == env.post_claim_token
    assert runtime.establishment_leases == []
    assert runtime.interaction_leases == [(WorkflowRole.PRODUCER, "fake-session-1")]
    assert runtime.transitions == []
    assert producer_adapter.detached_admissions() == (
        (
            "fake-session-1",
            PrepareWorkspaceInteraction(
                repository_full_name="octocat/hello-world",
                issue_number=42,
                task_uuid=env.task_id,
                target_directory=env.checkout_directory,
            ).render(),
        ),
    )
    assert producer_adapter.submitted_sends() == ()
    assert registrar.session_registrations == [
        {
            "workspace_id": env.workspace_id,
            "task_id": env.task_id,
            "task_agent_session_id": producer_ready[0],
            "connection_id": env.connection_id,
            "external_session_id": "fake-session-1",
        }
    ]
    assert handoff.calls == []
    stage_writes = [
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "planning_bootstrap_stage = %s" in sql
    ]
    assert len(stage_writes) == 1
    receipt_inserts = [
        sql for sql, _ in conn.executed if "insert into openorc.task_hub_run_receipts" in sql
    ]
    assert len(receipt_inserts) == 1


def test_prepare_dispatched_stage_replay_fails_closed() -> None:
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    conn = ScriptedConnection(
        _entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, "prepare_dispatched", _OBSERVED),
            classifier_results=(
                None,  # no outstanding PLANNING_BOOTSTRAP_PREPARE receipt
            ),
        )
    )
    runtime = env.runtime()

    with pytest.raises(PlanningBootstrapUnresolvedError):
        prepare_task_for_planning(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.post_claim_token,
            reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
            observation_registrar=_RecordingRegistrar(),
            completion_handoff=_RecordingCompletionHandoff(),
        )

    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert _statement_indices(conn, "update openorc.") == []


def test_prepare_dispatched_with_outstanding_receipt_replays_without_effect() -> None:
    """The durably dispatched PREPARE's outstanding receipt (issue #334).

    A replayed continuation against a claimed/admitted PREPARE receipt is an
    idempotent no-effect replay: the observed completion owns the
    resumption, nothing is re-dispatched, and no durable write happens.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    receipt_row = _detached_prepare_receipt_row(env, producer_ready[0], claim_token)
    conn = ScriptedConnection(
        _entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, "prepare_dispatched", _OBSERVED),
            classifier_results=(
                receipt_row,  # the outstanding PLANNING_BOOTSTRAP_PREPARE receipt
            ),
        )
    )
    runtime = env.runtime()

    outcome = prepare_task_for_planning(
        _pool(conn),
        runtime,
        workspace_id=env.workspace_id,
        task_id=env.task_id,
        profile_id=env.owner_profile_id,
        expected_state_token=env.post_claim_token,
        reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
        observation_registrar=_RecordingRegistrar(),
        completion_handoff=_RecordingCompletionHandoff(),
    )

    assert isinstance(outcome, TaskPlanningBootstrapDispatchOutcome)
    assert outcome.claim_token == claim_token
    assert runtime.establishment_leases == []
    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert runtime.fake_adapter(WorkflowRole.PRODUCER).detached_admissions() == ()
    assert _statement_indices(conn, "update openorc.") == []


def _refused_receipt_row(receipt_row: tuple[Any, ...], refusal_kind: str) -> tuple[Any, ...]:
    return (
        *receipt_row[:9],
        "refused",
        {"refusal_kind": refusal_kind},
        None,
        _OBSERVED,
        None,
        _OBSERVED,  # the refusal instant
        _OBSERVED,
        None,
        None,
        None,
        None,
    )


def test_detached_prepare_registration_refusal_refuses_the_receipt() -> None:
    """A registration refusal is a proven pre-effect failure (issue #334).

    The just-recorded receipt is marked refused so a never-dispatched turn
    cannot hold the session's single-outstanding slot, and the typed
    pre-effect failure triages under the claim's own family.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    receipt_row = _detached_prepare_receipt_row(env, producer_ready[0], claim_token)

    class _RefusingRegistrar(_RecordingRegistrar):
        def register_session(self, **kwargs: Any) -> None:
            raise ObservationRegistrationRefusedError(
                "the observation host refused the registration",
                reason="observer_unavailable",
            )

    results = [
        *_entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, None, None),
            classifier_results=(
                None,
                None,
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, producer_ready[0], "ext-p"
                ),
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, reviewer_ready[0], "ext-r"
                ),
            ),
        ),
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        *_base_repository_results(producer_ready, env.connection_row, env.repository_row),
        (
            claim_token,
            _OBSERVED,
            None,
            None,
        ),  # the fail-closed pre-check's evidence read# the dispatch transaction's evidence read
        (claim_token, _OBSERVED, None, None),  # the dispatch transaction's evidence read
        True,  # the durable PREPARE-dispatch marker
        receipt_row,  # the detached-turn receipt insert
        _refused_receipt_row(receipt_row, "observation_registration_refused"),
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    runtime.fake_adapter(WorkflowRole.PRODUCER).allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.PRODUCER,
            initialization="seed",
        )
    )

    with pytest.raises(ExternalOperationFailedError):
        prepare_task_for_planning(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.post_claim_token,
            reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
            observation_registrar=_RefusingRegistrar(),
            completion_handoff=_RecordingCompletionHandoff(),
        )

    assert runtime.transitions == []
    assert runtime.fake_adapter(WorkflowRole.PRODUCER).detached_admissions() == ()
    refusal_updates = [
        sql for sql, _ in conn.executed if "set status = 'refused'" in " ".join(sql.split())
    ]
    assert len(refusal_updates) == 1


def test_detached_prepare_admission_refusal_refuses_the_receipt() -> None:
    """A positively identified admission refusal is proven pre-effect.

    The receipt is marked refused and the dispatch fails known through the
    typed operational-failure class the coordinator triages; nothing is
    ever re-dispatched.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    receipt_row = _detached_prepare_receipt_row(env, producer_ready[0], claim_token)
    results = [
        *_entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, None, None),
            classifier_results=(
                None,
                None,
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, producer_ready[0], "ext-p"
                ),
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, reviewer_ready[0], "ext-r"
                ),
            ),
        ),
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        *_base_repository_results(producer_ready, env.connection_row, env.repository_row),
        (
            claim_token,
            _OBSERVED,
            None,
            None,
        ),  # the fail-closed pre-check's evidence read# the dispatch transaction's evidence read
        (claim_token, _OBSERVED, None, None),  # the dispatch transaction's evidence read
        True,  # the durable PREPARE-dispatch marker
        receipt_row,  # the detached-turn receipt insert
        _refused_receipt_row(receipt_row, "admission_refused"),
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    producer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.PRODUCER,
            initialization="seed",
        )
    )
    producer_adapter.queue_detached_admission(AgentRuntimeUnavailableError("refused"))

    with pytest.raises(ExternalOperationFailedError):
        prepare_task_for_planning(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.post_claim_token,
            reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
            observation_registrar=_RecordingRegistrar(),
            completion_handoff=_RecordingCompletionHandoff(),
        )

    assert runtime.transitions == []
    # The fake records the admission attempt the runtime refused.
    assert len(producer_adapter.detached_admissions()) == 1
    refusal_updates = [
        sql for sql, _ in conn.executed if "set status = 'refused'" in " ".join(sql.split())
    ]
    assert len(refusal_updates) == 1


def test_detached_prepare_uncertain_admission_keeps_the_receipt_claimed() -> None:
    """An uncertain admission is never a re-dispatch authorization.

    The receipt stays conservatively claimed (no refusal write), the typed
    uncertainty propagates for the existing Level 2 machinery, and the
    observed completion — if the turn actually runs — still owns the
    settlement.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    receipt_row = _detached_prepare_receipt_row(env, producer_ready[0], claim_token)
    results = [
        *_entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, None, None),
            classifier_results=(
                None,
                None,
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, producer_ready[0], "ext-p"
                ),
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, reviewer_ready[0], "ext-r"
                ),
            ),
        ),
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        *_base_repository_results(producer_ready, env.connection_row, env.repository_row),
        (
            claim_token,
            _OBSERVED,
            None,
            None,
        ),  # the fail-closed pre-check's evidence read# the dispatch transaction's evidence read
        (claim_token, _OBSERVED, None, None),  # the dispatch transaction's evidence read
        True,  # the durable PREPARE-dispatch marker
        receipt_row,  # the detached-turn receipt insert
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    producer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.PRODUCER,
            initialization="seed",
        )
    )
    producer_adapter.queue_detached_admission(AgentRuntimeTimeoutError("timed out"))

    with pytest.raises(ExternalOperationUncertainError):
        prepare_task_for_planning(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.post_claim_token,
            reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
            observation_registrar=_RecordingRegistrar(),
            completion_handoff=_RecordingCompletionHandoff(),
        )

    assert runtime.transitions == []
    refusal_updates = [
        sql for sql, _ in conn.executed if "set status = 'refused'" in " ".join(sql.split())
    ]
    assert refusal_updates == []


def test_settled_bootstrap_prepare_transition_completes_from_prepared_state() -> None:
    """The observed-completion transition continuation (issue #334).

    The settled ``prepared`` stage is the only authority: the continuation
    re-derives the claim, both READY bindings, and the exact checkout from
    committed state, performs the one planning-context transition, records
    ``ready_for_planning``, and returns the prepared-for-planning outcome.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    results = [
        (claim_token, _OBSERVED, "prepared", _OBSERVED),  # the settled stage evidence
        producer_ready,  # the READY producer binding
        reviewer_ready,  # the READY reviewer binding
        env.connection_row,  # the snapshot base recovery
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        True,  # the settled transition settles ``ready_for_planning``
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()

    outcome = complete_settled_bootstrap_prepare_transition(
        _pool(conn),
        runtime,
        workspace_id=env.workspace_id,
        profile_id=env.owner_profile_id,
        task_id=env.task_id,
        expected_state_token=env.post_claim_token,
    )

    assert isinstance(outcome, TaskPlanningPreparationOutcome)
    assert outcome.producer_session.external_session_id == "fake-session-1"
    assert runtime.transitions == [("fake-session-1", env.checkout_directory)]
    stage_writes = [
        index
        for index, (sql, _params) in enumerate(conn.executed)
        if "planning_bootstrap_stage = %s" in sql
    ]
    assert len(stage_writes) == 1


def test_settled_bootstrap_prepare_transition_refuses_a_moved_on_stage() -> None:
    """A stage other than the settled ``prepared`` handoff state fails closed.

    The continuation never re-runs the workspace preparation and never
    transitions from an unsettled or already-transitioned bootstrap.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    conn = ScriptedConnection(
        [
            (claim_token, _OBSERVED, "prepare_dispatched", _OBSERVED),
        ]
    )
    runtime = env.runtime()

    with pytest.raises(ConflictError):
        complete_settled_bootstrap_prepare_transition(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            profile_id=env.owner_profile_id,
            task_id=env.task_id,
            expected_state_token=env.post_claim_token,
        )

    assert runtime.transitions == []
    assert _statement_indices(conn, "update openorc.") == []


def test_settle_observed_bootstrap_prepare_result_records_the_prepared_stage() -> None:
    """The settlement-owned stage recording (issue #334).

    A current coarse-planning subject records the claim-bound forward-only
    ``prepared`` stage; a moved-on subject (a published plan revision) is
    the honest not-applied outcome.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    conn = ScriptedConnection(
        [
            env.post_claim_task_row,
            True,  # the forward-only ``prepared`` stage write
        ]
    )
    assert (
        settle_observed_bootstrap_prepare_result(
            _pool(conn),
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            claim_token=claim_token,
        )
        is True
    )

    published = list(env.post_claim_task_row)
    published[9] = uuid.uuid4()  # current_plan_revision_id
    moved_conn = ScriptedConnection([tuple(published)])
    assert (
        settle_observed_bootstrap_prepare_result(
            _pool(moved_conn),
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            claim_token=claim_token,
        )
        is False
    )
    assert _statement_indices(moved_conn, "update openorc.") == []


# --- the competing-dispatch guard and the coordinated transition failure --------


def test_a_competing_continuation_fails_closed_at_an_already_dispatched_stage() -> None:
    """The marker is the one-shot PREPARE guard across settled receipts.

    A continuation that classified the claim resumable before a competing
    continuation dispatched enters its dispatch transaction against the
    already-committed ``prepare_dispatched`` stage — even after the first
    receipt settled, failed, or was refused — and fails closed without
    another receipt, marker write, registration, or admission.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    results = [
        *_entry_results(
            env.workspace_id,
            env.owner_profile_id,
            env.post_claim_task_row,
            claim=(claim_token,),
            producer_row=producer_ready,
            reviewer_row=reviewer_ready,
            claim_write_result=None,
            post_claim_task_row=None,
            claim_evidence_row=(claim_token, _OBSERVED, None, None),
            classifier_results=(
                None,
                None,
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, producer_ready[0], "ext-p"
                ),
                _settled_initialization_receipt_row(
                    env.workspace_id, env.task_id, reviewer_ready[0], "ext-r"
                ),
            ),
        ),
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        *_base_repository_results(producer_ready, env.connection_row, env.repository_row),
        # The competing continuation committed its dispatch meanwhile.
        (claim_token, _OBSERVED, "prepare_dispatched", _OBSERVED),
    ]
    conn = ScriptedConnection(results)
    runtime = env.runtime()
    producer_adapter = runtime.fake_adapter(WorkflowRole.PRODUCER)
    producer_adapter.allocate_detached_session(
        AgentSessionCreationRequest(
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            role=WorkflowRole.PRODUCER,
            initialization="seed",
        )
    )

    with pytest.raises(ConflictError):
        prepare_task_for_planning(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.post_claim_token,
            reviewer_neutral_directory=_NEUTRAL_DIRECTORY,
            observation_registrar=_RecordingRegistrar(),
            completion_handoff=_RecordingCompletionHandoff(),
        )

    assert runtime.interaction_leases == []
    assert runtime.transitions == []
    assert producer_adapter.detached_admissions() == ()
    assert not any("insert into openorc.task_hub_run_receipts" in sql for sql, _ in conn.executed)
    assert _statement_indices(conn, "update openorc.") == []


class _FailingTransitionRuntime(_ScriptedBootstrapRuntime):
    """The scripted composition whose planning-context reconstruction fails."""

    def enter_planning_checkout(
        self,
        pool: Any,
        *,
        workspace_id: Any,
        task_id: Any,
        external_session_id: str,
        checkout_directory: str,
    ) -> None:
        raise AgentRuntimeUnavailableError("the runtime is unreachable for the reconstruction")


def test_a_coordinated_reconstruction_failure_escalates_to_blocked() -> None:
    """A positively detected reconstruction failure follows Level 1/2/3.

    The observed-completion transition continuation runs under the F5b
    coordinator: the failing reconstruction records the claim's transition
    identity, the coordinator verifies it against the settled ``prepared``
    stage, and the real F4 disposition escalates to the BLOCKED
    installation — never an unreported failure stranded on a prepared
    stage, and never an automatic replay.
    """
    env = _Bootstrap()
    claim_token = uuid.uuid4()
    producer_ready = env.producer_ready_row
    reviewer_ready = env.reviewer_ready_row
    evidence_row = (claim_token, _OBSERVED, "prepared", _OBSERVED)
    episode_id = uuid.uuid4()
    block_id = uuid.uuid4()
    gate_id = uuid.uuid4()
    planning_task_row = task_row(
        id=env.task_id,
        workspace_id=env.workspace_id,
        status="planning",
        state_token=env.post_claim_token,
        source_requirements_fingerprint="a" * 64,
    )
    blocked_task_row = task_row(
        id=env.task_id,
        workspace_id=env.workspace_id,
        status="blocked",
        state_token=uuid.uuid4(),
        current_owner_gate_id=gate_id,
        source_requirements_fingerprint="a" * 64,
    )
    episode_row = (
        episode_id,
        env.workspace_id,
        env.task_id,
        "planning_bootstrap_transition",
        "planning_bootstrap_claim",
        claim_token,
        {"task_id": str(env.task_id), "workspace_id": str(env.workspace_id)},
        None,
        _OBSERVED,
    )
    results = [
        # The transition continuation's prologue transaction.
        evidence_row,
        producer_ready,
        reviewer_ready,
        env.connection_row,
        # The post-PREPARE recheck, then the failing reconstruction.
        *_recheck_results(
            env.post_claim_task_row,
            (claim_token,),
            producer_ready,
            reviewer_ready,
        ),
        # The coordinator's evidence resolution re-read.
        evidence_row,
        # The real F4 disposition: the locked decision flow and escalation.
        planning_task_row,  # get_task_for_update
        None,  # no episode rooted at the claim yet
        None,  # no attempt by the reporting source
        evidence_row,  # the transition family's stage verification
        producer_ready,  # the applicable READY producer binding
        episode_row,  # the episode insert
        planning_task_row,  # the F3 composition's task re-lock
        planning_task_row,  # the F3 composition's task re-read
        (block_id, env.workspace_id, env.task_id, "unknown", {}, None, _OBSERVED),
        (
            gate_id,
            env.workspace_id,
            env.task_id,
            "block_resolution",
            "pending",
            None,
            None,
            None,
            block_id,
            None,
            _OBSERVED,
        ),
        blocked_task_row,  # the guarded blocked-Task mutation
        _event_returned_row("task_blocked"),
        _event_returned_row("owner_gate_created"),
        (*episode_row[:7], block_id, _OBSERVED),  # the episode's escalation stamp
    ]
    conn = ScriptedConnection(results)
    producer, reviewer = env.adapters()
    runtime = _FailingTransitionRuntime(
        {WorkflowRole.PRODUCER: producer, WorkflowRole.REVIEWER: reviewer},
        selected_configuration=dict(_SELECTED_CONFIGURATION),
    )
    dispositions: list[Any] = []

    def invoke_level_one(evidence: InvocationAttemptEvidence) -> None:
        complete_settled_bootstrap_prepare_transition(
            _pool(conn),
            runtime,
            workspace_id=env.workspace_id,
            profile_id=env.owner_profile_id,
            task_id=env.task_id,
            expected_state_token=env.post_claim_token,
            attempt_evidence=evidence,
        )

    def recording_triage(pool: Any, **kwargs: Any) -> Any:
        disposition = triage_operational_failure(pool, **kwargs)
        dispositions.append(disposition)
        return disposition

    with pytest.raises(ExternalOperationFailedError):
        coordinate_task_workflow_operation(
            _pool(conn),
            expected_status=TaskStatus.PLANNING,
            workspace_id=env.workspace_id,
            task_id=env.task_id,
            profile_id=env.owner_profile_id,
            expected_state_token=env.post_claim_token,
            invoke_level_one=invoke_level_one,
            triage=recording_triage,
        )

    assert len(dispositions) == 1
    assert dispositions[0].kind is DispositionKind.BLOCKED
    assert dispositions[0].installation is not None
    assert dispositions[0].installation.task_block.id == block_id
    executed = [" ".join(sql.split()) for sql, _ in conn.executed]
    assert not any("ready_for_planning" in sql for sql in executed)
    assert runtime.transitions == []


class _RecordingCompletionHandoff:
    """Records the ONE post-commit completion handoff invocations (issue #333)."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any, Any]] = []
        self.interrupt_calls: list[dict[str, Any]] = []

    def handoff_settled_completion(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        operation: Any,
        validated_result: Any = None,
    ) -> None:
        self.calls.append((workspace_id, task_id, operation))

    def handoff_settled_interrupt(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        task_agent_session_id: Any,
        external_session_id: str,
    ) -> None:
        self.interrupt_calls.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "task_agent_session_id": task_agent_session_id,
                "external_session_id": external_session_id,
            }
        )


class _RecordingRegistrar:
    def __init__(self) -> None:
        self.session_registrations: list[dict[str, Any]] = []

    def register_establishment_session(self, **kwargs: Any) -> None:
        return None

    def register_session(self, **kwargs: Any) -> None:
        self.session_registrations.append(dict(kwargs))
