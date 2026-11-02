"""The F5c reservation/consumption seam against a real Postgres (issue #263).

The focused real-Postgres checks the new seam owns:

- the full authorized-retry round trip on real rows: after the E6 initial
  attempt fails positively pre-effect, the real F4 triage authorizes one
  bounded retry, and the retry seam reserves Execution #2, claims it
  running, records the Producer-attributed started audit, and binds the F4
  attempt set-once — all in the one admission composition that commits
  before any external effect — then dispatches through the deterministic
  fake runtime and finalizes the new row only;
- F4's bound ``resulting_execution_id`` is the consumption identity on real
  rows: the binding lookup the F5b coordinator uses resolves the exact
  attempt, and a replayed consumption refuses with no second Execution;
- a premature wakeup (before the persisted ``not_before``) refuses before
  any effect and leaves the authorization consumable when due;
- the durable backstops enforce the single consumption: the set-once
  binding rejects a second, different Execution (and rebinds the same one
  idempotently), and the per-Task execution-number uniqueness rejects a
  duplicate attempt number.

Ordinary classification and refusal coverage lives in the scripted
deterministic suite; the Task-row-lock serialization itself is
Postgres-native. The suite consumes the explicitly supplied non-production
branch database per ``tests/integration/conftest.py`` and uses the
deterministic fake Agent Runtime (no live runtime, GitHub, or Supabase).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import IntegrityError

from openorc.adapters.agent_runtime.errors import AgentRuntimeUnavailableError
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import ReviewLoopPurpose, ReviewOutcome
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence import executions as execution_repositories
from openorc.persistence import operational_failures as failure_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import sessions as session_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import ExternalOperationFailedError, StaleOperationError
from openorc.services.task_authorized_implementation_execution import (
    execute_authorized_implementation,
)
from openorc.services.task_authorized_implementation_retry import (
    execute_authorized_implementation_retry,
)
from openorc.services.task_implementation_authorization import (
    approve_implementation_authorization,
    open_implementation_authorization,
)
from openorc.services.task_operational_failure_triage import (
    OperationalFailureOperation,
    OperationalFailureReport,
    triage_operational_failure,
)
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    implementation_result_candidate,
    session_ready_candidate,
)

pytestmark = pytest.mark.integration

_BASE_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_SUMMARY = "Looks good"
_EXTERNAL_SESSION_ID = "fake-session-1"
_BASE_TIME = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_RESULT_CANDIDATE = implementation_result_candidate(
    branch="feat/issue-81",
    summary="Implemented the handoff",
    changes=["feat: handoff"],
    validation=["tests pass"],
)


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
            with self._connection.transaction():
                yield self._connection

        return managed()


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(conn: Connection[Any], owner_profile_id: uuid.UUID) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, "workspace"),
    )
    return workspace_id


def _insert_installation(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, %s, %s, %s)",
        (installation_id, workspace_id, 77_000_007, 77_000_008, "octocat", "Organization"),
    )
    return installation_id


def _insert_project(conn: Connection[Any], workspace_id: uuid.UUID) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    return project_id


def _insert_repository(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    github_installation_id: uuid.UUID,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch, github_installation_id) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            _insert_project(conn, workspace_id),
            workspace_id,
            77_000_009,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            github_installation_id,
        ),
    )
    return repository_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, 7703, 42, "a" * 64),
    )
    return task_id


def _insert_connection(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections "
        "(id, workspace_id, adapter_type, name, safe_config, session_capacity, enabled, "
        "auth_reference) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            connection_id,
            workspace_id,
            "cline",
            "primary cline hub",
            "{}",
            2,
            True,
            "opaque-reference",
        ),
    )
    return connection_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _authorized_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """The exact post-E5-approval durable state, built through the real E5
    presentation and approval seams, plus the admitted READY Producer
    session the E6 handoff consumes."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content=_PLAN_TEXT,
        repository_base_sha=_BASE_SHA,
    )
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    pointed = task_repositories.set_current_plan_revision(
        pool,
        task_id,
        expected_state_token=task.state_token,
        plan_revision_id=revision.id,
    )
    assert pointed is not None
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert iteration is not None
    finalized = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.ACCEPTED,
        summary=_SUMMARY,
        findings=[],
    )
    assert finalized is not None

    presented = open_implementation_authorization(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=pointed.state_token,
    )
    approved = approve_implementation_authorization(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=presented.owner_gate.id,
        expected_state_token=presented.task.state_token,
    )
    assert approved.task.status is TaskStatus.IMPLEMENTING
    assert approved.review_loop.status.name == "CLOSED"

    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    session = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    initialized = session_repositories.initialize_task_agent_session(
        pool,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        external_session_id=_EXTERNAL_SESSION_ID,
        effective_config_snapshot={},
    )
    assert initialized is not None
    assert initialized.lifecycle_status is TaskSessionLifecycleStatus.READY

    return {
        "pool": pool,
        "profile_id": profile_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "revision_id": revision.id,
        "gate_id": presented.owner_gate.id,
        "state_token": approved.task.state_token,
        "session_id": session.id,
        "connection_id": connection_id,
    }


# --- service-path fakes -----------------------------------------------------------


class _ProducerAttachment:
    """Deferred Producer attachment around one fake runtime."""

    def __init__(self, runtime: FakeAgentRuntimeAdapter) -> None:
        self._runtime = runtime
        self.acquisitions = 0

    def __call__(self) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.acquisitions += 1
        return self

    def __enter__(self) -> FakeAgentRuntimeAdapter:
        return self._runtime

    def __exit__(self, *_exc: object) -> bool:
        return False


class _FailingRealizeRuntime(FakeAgentRuntimeAdapter):
    """A fake runtime whose native ACT reconstruction provably fails pre-effect."""

    def realize_control(  # type: ignore[override]
        self,
        session_id: str,
        interaction: Any,  # noqa: ANN401 - mirrors the #67 contract parameter
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> Any:  # noqa: ANN401 - mirrors the #67 contract return
        raise AgentRuntimeUnavailableError("the runtime is unavailable")


def _prepared_runtime(facts: dict[str, Any]) -> FakeAgentRuntimeAdapter:
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=cast(uuid.UUID, facts["workspace_id"]),
            task_id=cast(uuid.UUID, facts["task_id"]),
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert created.external_session_id == _EXTERNAL_SESSION_ID
    adapter.queue_send(_EXTERNAL_SESSION_ID, _RESULT_CANDIDATE)
    adapter.script_realization(_EXTERNAL_SESSION_ID, ControlRealization.NATIVE_ONLY)
    return adapter


def _authorize_one_retry(
    conn: Connection[Any],
    facts: dict[str, Any],
    *,
    execution_id: uuid.UUID,
) -> Any:
    """Drive the real F4 triage to authorize one bounded retry for the
    proven pre-effect implementation-execution failure of ``execution_id``."""
    pool = cast(DatabasePool, facts["pool"])
    disposition = triage_operational_failure(
        pool,
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=cast(uuid.UUID, facts["task_id"]),
        expected_state_token=cast(uuid.UUID, facts["state_token"]),
        expected_status=TaskStatus.IMPLEMENTING,
        expected_current_owner_gate_id=None,
        actor=event_coordination.owner_actor(cast(uuid.UUID, facts["profile_id"])),
        operation=OperationalFailureOperation(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=execution_id,
            subject={
                "workspace_id": str(facts["workspace_id"]),
                "task_id": str(facts["task_id"]),
            },
        ),
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
            reporting_attempt_id=execution_id,
            error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
            effect_evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
        ),
        clock=lambda: _BASE_TIME,
    )
    assert disposition.retry_attempt is not None
    assert disposition.retry_attempt.ordinal == 1
    assert disposition.retry_attempt.not_before == _BASE_TIME + timedelta(seconds=2)
    conn.commit()  # settle the authorized attempt for the retry invocation
    return disposition.retry_attempt


def _retry_call(
    facts: dict[str, Any],
    *,
    retry_attempt_id: uuid.UUID,
    runtime: FakeAgentRuntimeAdapter,
    clock: Callable[[], datetime],
) -> Any:
    return execute_authorized_implementation_retry(
        cast(DatabasePool, facts["pool"]),
        profile_id=cast(uuid.UUID, facts["profile_id"]),
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=cast(uuid.UUID, facts["task_id"]),
        expected_state_token=cast(uuid.UUID, facts["state_token"]),
        retry_attempt_id=retry_attempt_id,
        producer_runtime_factory=_ProducerAttachment(runtime),
        clock=clock,
    )


def _failing_initial_attempt(conn: Connection[Any], facts: dict[str, Any]) -> Any:
    """Run the real E6 initial execution with a provably pre-effect failing
    runtime: the claimed attempt finalizes ``failed_transient`` with the
    safe ``runtime_pre_effect`` audit, and the Task stays implementing."""
    pool = cast(DatabasePool, facts["pool"])
    with pytest.raises(ExternalOperationFailedError):
        execute_authorized_implementation(
            pool,
            profile_id=cast(uuid.UUID, facts["profile_id"]),
            workspace_id=cast(uuid.UUID, facts["workspace_id"]),
            task_id=cast(uuid.UUID, facts["task_id"]),
            expected_state_token=cast(uuid.UUID, facts["state_token"]),
            producer_runtime_factory=_ProducerAttachment(_FailingRealizeRuntime()),
        )
    task = task_repositories.get_task(pool, cast(uuid.UUID, facts["task_id"]))
    assert task is not None and task.status is TaskStatus.IMPLEMENTING
    history = execution_repositories.list_task_executions(pool, task_id=task.id)
    assert len(history) == 1
    assert history[0].status is ExecutionStatus.FAILED_TRANSIENT
    conn.commit()  # settle the failed initial attempt before the triage
    return history[0]


def test_the_authorized_retry_round_trips_and_binds_set_once_end_to_end(
    conn: Connection[Any],
) -> None:
    """The full chain on real rows: proven pre-effect failure → real F4
    authorization → one retry Execution reserved, claimed, audited, and
    bound set-once in the admission composition → successful finalize of the
    new row only; the binding lookup resolves the exact attempt and a replay
    refuses without minting anything."""
    facts = _authorized_fixture(conn)
    failed_execution = _failing_initial_attempt(conn, facts)
    retry_attempt = _authorize_one_retry(
        conn, facts, execution_id=cast(uuid.UUID, failed_execution.id)
    )

    outcome = _retry_call(
        facts,
        retry_attempt_id=retry_attempt.id,
        runtime=_prepared_runtime(facts),
        clock=lambda: _BASE_TIME + timedelta(seconds=3),
    )

    # The new Execution is a distinct row (number 2), successfully finalized
    # with the validated canonical result.
    assert outcome.execution.id != failed_execution.id
    assert outcome.execution.execution_number == 2
    assert outcome.execution.status is ExecutionStatus.SUCCEEDED
    assert outcome.execution.implementation_result == dict(_RESULT_CANDIDATE)

    # The F4 authorization is bound set-once to exactly this Execution: the
    # durable lookup the F5b coordinator relies on resolves it.
    bound = failure_repositories.find_operational_failure_attempt_by_resulting_execution(
        cast(DatabasePool, facts["pool"]), resulting_execution_id=outcome.execution.id
    )
    assert bound is not None
    assert bound.id == retry_attempt.id
    assert bound.resulting_execution_id == outcome.execution.id

    # Same-commit artifacts of the admission composition: the started audit
    # and the completed audit name exactly the new row.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'execution_started' and subject_type = 'execution' "
            "and subject_id = %s",
            (facts["task_id"], outcome.execution.id),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'execution_completed' and subject_type = 'execution' "
            "and subject_id = %s",
            (facts["task_id"], outcome.execution.id),
        )
        == 1
    )

    # A replayed consumption of the same authorization is a no-effect
    # refusal: no second Execution, no adapter acquisition.
    replay_attachment = _ProducerAttachment(_prepared_runtime(facts))
    with pytest.raises(StaleOperationError):
        execute_authorized_implementation_retry(
            cast(DatabasePool, facts["pool"]),
            profile_id=cast(uuid.UUID, facts["profile_id"]),
            workspace_id=cast(uuid.UUID, facts["workspace_id"]),
            task_id=cast(uuid.UUID, facts["task_id"]),
            expected_state_token=cast(uuid.UUID, facts["state_token"]),
            retry_attempt_id=retry_attempt.id,
            producer_runtime_factory=replay_attachment,
            clock=lambda: _BASE_TIME + timedelta(seconds=3),
        )
    assert replay_attachment.acquisitions == 0
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.executions where task_id = %s",
            (facts["task_id"],),
        )
        == 2
    )


def test_a_premature_wakeup_refuses_and_leaves_the_authorization_consumable(
    conn: Connection[Any],
) -> None:
    """The persisted due instant is enforced at actual admission: an early
    invocation refuses before any effect, and the same authorization still
    executes exactly once when due."""
    facts = _authorized_fixture(conn)
    failed_execution = _failing_initial_attempt(conn, facts)
    retry_attempt = _authorize_one_retry(
        conn, facts, execution_id=cast(uuid.UUID, failed_execution.id)
    )

    with pytest.raises(StaleOperationError):
        _retry_call(
            facts,
            retry_attempt_id=retry_attempt.id,
            runtime=_FailingRealizeRuntime(),
            clock=lambda: _BASE_TIME + timedelta(seconds=1),
        )

    # The refusal left no durable trace: still one execution, the
    # authorization still unbound and consumable.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.executions where task_id = %s",
            (facts["task_id"],),
        )
        == 1
    )
    attempt = failure_repositories.get_operational_failure_attempt(
        cast(DatabasePool, facts["pool"]), attempt_id=retry_attempt.id
    )
    assert attempt is not None and attempt.resulting_execution_id is None

    outcome = _retry_call(
        facts,
        retry_attempt_id=retry_attempt.id,
        runtime=_prepared_runtime(facts),
        clock=lambda: _BASE_TIME + timedelta(seconds=3),
    )
    assert outcome.execution.execution_number == 2
    assert outcome.execution.status is ExecutionStatus.SUCCEEDED


def test_the_set_once_binding_and_number_backstops_enforce_the_single_consumption(
    conn: Connection[Any],
) -> None:
    """The durable backstops behind the service's Task-lock serialization:
    the set-once binding rejects a second different Execution and rebinds
    the same one idempotently; the per-Task execution number is unique."""
    facts = _authorized_fixture(conn)
    pool = cast(DatabasePool, facts["pool"])
    workspace_id = cast(uuid.UUID, facts["workspace_id"])
    task_id = cast(uuid.UUID, facts["task_id"])

    episode = failure_repositories.create_operational_failure_episode(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        operation_family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=uuid.uuid4(),
        subject={"workspace_id": str(workspace_id), "task_id": str(task_id)},
    )
    attempt = failure_repositories.create_operational_failure_attempt(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        episode_id=episode.id,
        ordinal=1,
        source_kind=OperationalFailureSourceKind.ORIGINAL,
        source_attempt_id=episode.original_attempt_id,
        error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
        not_before=_BASE_TIME,
    )

    first = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=cast(uuid.UUID, facts["session_id"]),
        execution_number=1,
    )
    second = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=cast(uuid.UUID, facts["session_id"]),
        execution_number=2,
    )

    bound = failure_repositories.bind_retry_attempt_execution(
        pool,
        attempt_id=attempt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        resulting_execution_id=first.id,
    )
    assert bound is not None and bound.resulting_execution_id == first.id
    # Rebinding the same Execution is an idempotent success.
    rebound = failure_repositories.bind_retry_attempt_execution(
        pool,
        attempt_id=attempt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        resulting_execution_id=first.id,
    )
    assert rebound is not None and rebound.resulting_execution_id == first.id
    # Binding a different Execution is the refused set-once conflict.
    assert (
        failure_repositories.bind_retry_attempt_execution(
            pool,
            attempt_id=attempt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            resulting_execution_id=second.id,
        )
        is None
    )

    # The per-Task execution-number uniqueness is the durable backstop
    # against a second reserved attempt with the same identity.
    conn.execute("savepoint number_backstop")
    with pytest.raises(IntegrityError):
        execution_repositories.create_execution(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=cast(uuid.UUID, facts["session_id"]),
            execution_number=1,
        )
    conn.execute("rollback to savepoint number_backstop")
