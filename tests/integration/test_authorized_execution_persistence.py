"""Durable E6 execution seams against a real Postgres (issue #81).

The targeted real-Postgres checks the new seams own:

- the additive ``openorc.executions`` columns round-trip: the validated
  canonical ``implementation_result`` payload and the qualifying-activity
  marker persist with the succeeded finalize, the payload CHECK rejects
  non-object values, and the marker is born with the dispatch claim;
- the guarded one-shot primitives on real rows: the dispatch claim moves
  ``queued → running`` and stamps the marker atomically (a second claim is
  a rejected no-op), the completion persists the payload and stamps
  ``succeeded`` in one statement (a duplicate or post-finalization
  completion is a rejected no-op), and the activity marker advances only
  while the row is running (late callbacks against finalized history are
  dropped);
- the replayed E6 start against the durable attempt history refuses before
  any effect: the first attempt's row, status, and its Producer-attributed
  audit remain untouched, and no second attempt is ever reserved.

The single-attempt exclusivity the service enforces under the Task row lock
is the same durable invariant this replay proves; the row-lock serialization
itself is Postgres-native. The scripted deterministic suite owns
classification and no-write refusal coverage; this suite owns only the new
durable invariants and uses the deterministic fake Agent Runtime and a
scripted D6c activity subscription for the service path (no live runtime,
GitHub, or Supabase access). The suite consumes the explicitly supplied
non-production branch database per ``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import IntegrityError

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.cline.activity import ClineActivityKind, ClineActivityObservation
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.reviews import ReviewLoopPurpose, ReviewOutcome
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence import executions as execution_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import sessions as session_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import IMPLEMENTATION_RESULT_FAMILY
from openorc.services.errors import ConflictError
from openorc.services.task_authorized_implementation_execution import (
    execute_authorized_implementation,
)
from openorc.services.task_implementation_authorization import (
    approve_implementation_authorization,
    open_implementation_authorization,
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

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


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


class _ScriptedActivitySubscription:
    """Delivers scripted observations to the listener on establishment."""

    def __init__(self, observations: Sequence[ClineActivityObservation] = ()) -> None:
        self._observations = list(observations)
        self.listeners: list[Callable[[ClineActivityObservation], None]] = []

    def __call__(
        self,
        _runtime: Any,  # noqa: ANN401
        _session_id: str,
        listener: Callable[[ClineActivityObservation], None],
    ) -> AbstractContextManager[None]:
        self.listeners.append(listener)
        return self

    def __enter__(self) -> None:
        for observation in self._observations:
            self.listeners[-1](observation)
        return None

    def __exit__(self, *_exc: object) -> bool:
        return False


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


def _execute_call(
    pool: DatabasePool,
    facts: dict[str, Any],
    *,
    producer_runtime_factory: Any,  # noqa: ANN401
    activity_subscription_factory: Any | None = None,  # noqa: ANN401
) -> Any:  # noqa: ANN401 - the typed outcome
    return execute_authorized_implementation(
        pool,
        profile_id=cast(uuid.UUID, facts["profile_id"]),
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=cast(uuid.UUID, facts["task_id"]),
        expected_state_token=cast(uuid.UUID, facts["state_token"]),
        producer_runtime_factory=producer_runtime_factory,
        activity_subscription_factory=activity_subscription_factory,
    )


# --- the end-to-end durable round trip --------------------------------------------


def test_the_authorized_execution_round_trips_result_and_marker_end_to_end(
    conn: Connection[Any],
) -> None:
    """One E6 attempt persists the validated result and the activity marker
    with its succeeded finalize, records the matched Producer-attributed
    audit, and leaves the Task/gate/plan authority untouched for E7."""
    facts = _authorized_fixture(conn)
    pool = facts["pool"]
    subscription = _ScriptedActivitySubscription(
        [
            ClineActivityObservation(
                session_id=_EXTERNAL_SESSION_ID, kind=ClineActivityKind.THINKING_STARTED
            )
        ]
    )
    runtime = _prepared_runtime(facts)

    outcome = _execute_call(
        pool,
        facts,
        producer_runtime_factory=_ProducerAttachment(runtime),
        activity_subscription_factory=subscription,
    )

    assert outcome.execution.status is ExecutionStatus.SUCCEEDED
    assert outcome.execution.execution_number == 1
    assert outcome.execution.implementation_result == dict(_RESULT_CANDIDATE)
    assert outcome.execution.last_qualifying_activity_at is not None
    assert outcome.task.status is TaskStatus.IMPLEMENTING
    assert outcome.producer_session.id == facts["session_id"]
    (control,) = runtime.realized_controls()
    assert control.interaction.kind.name == "IMPLEMENT"
    (send,) = runtime.submitted_sends()
    assert send.expected_family == IMPLEMENTATION_RESULT_FAMILY

    # The durable row carries exactly the canonical validated payload.
    persisted = execution_repositories.get_execution(pool, execution_id=outcome.execution.id)
    assert persisted is not None
    assert persisted.status is ExecutionStatus.SUCCEEDED
    assert persisted.implementation_result == dict(_RESULT_CANDIDATE)
    assert persisted.last_qualifying_activity_at is not None

    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'execution_started' and actor_type = 'producer' "
            "and actor_id = %s and subject_type = 'execution' and subject_id = %s",
            (facts["task_id"], str(facts["session_id"]), outcome.execution.id),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'execution_completed' and actor_type = 'producer' "
            "and subject_type = 'execution' and subject_id = %s",
            (facts["task_id"], outcome.execution.id),
        )
        == 1
    )


def test_a_replayed_start_against_durable_history_refuses_before_effects(
    conn: Connection[Any],
) -> None:
    """The replayed E6 start refuses on the first attempt's durable history:
    no second reservation, claim, send, or audit ever happens."""
    facts = _authorized_fixture(conn)
    pool = facts["pool"]
    first = _execute_call(
        pool,
        facts,
        producer_runtime_factory=_ProducerAttachment(_prepared_runtime(facts)),
    )
    assert first.execution.status is ExecutionStatus.SUCCEEDED

    replay_runtime = _prepared_runtime(facts)
    with pytest.raises(ConflictError):
        _execute_call(
            pool,
            facts,
            producer_runtime_factory=_ProducerAttachment(replay_runtime),
        )

    assert replay_runtime.realized_controls() == ()
    assert replay_runtime.submitted_sends() == ()
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.executions where task_id = %s",
            (facts["task_id"],),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type in ('execution_started', 'execution_completed')",
            (facts["task_id"],),
        )
        == 2
    )


# --- the guarded one-shot primitives on real rows ---------------------------------


def test_the_claim_completion_and_marker_conditionals_hold_on_real_rows(
    conn: Connection[Any],
) -> None:
    """The dispatch claim is atomic and one-shot; the completion persists the
    payload and stamps success atomically and one-shot; the activity marker
    advances only while running and is born with the claim."""
    facts = _authorized_fixture(conn)
    pool = facts["pool"]

    execution = execution_repositories.create_execution(
        pool,
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=cast(uuid.UUID, facts["task_id"]),
        producer_session_id=cast(uuid.UUID, facts["session_id"]),
        execution_number=1,
    )
    claimed = execution_repositories.claim_execution_running(pool, execution_id=execution.id)
    assert claimed is not None
    assert claimed.status is ExecutionStatus.RUNNING
    # The marker is born with the dispatch claim.
    assert claimed.last_qualifying_activity_at is not None
    claim_instant = claimed.last_qualifying_activity_at

    # A second claim is a rejected no-op.
    assert execution_repositories.claim_execution_running(pool, execution_id=execution.id) is None

    # The marker advances with database time only while running.
    advanced = execution_repositories.record_execution_activity(pool, execution_id=execution.id)
    assert advanced is not None
    assert advanced.last_qualifying_activity_at is not None
    assert advanced.last_qualifying_activity_at >= claim_instant

    # The completion persists the payload and stamps success in one write.
    payload = dict(_RESULT_CANDIDATE)
    completed = execution_repositories.complete_execution_with_result(
        pool, execution_id=execution.id, implementation_result=payload
    )
    assert completed is not None
    assert completed.status is ExecutionStatus.SUCCEEDED
    assert completed.implementation_result == payload

    # Duplicate completion, post-finalization marker writes, and any
    # further status transition are all rejected no-ops on absorbing
    # history.
    assert (
        execution_repositories.complete_execution_with_result(
            pool, execution_id=execution.id, implementation_result=payload
        )
        is None
    )
    assert execution_repositories.record_execution_activity(pool, execution_id=execution.id) is None
    assert (
        execution_repositories.update_execution_status(
            pool,
            execution_id=execution.id,
            expected_status=ExecutionStatus.SUCCEEDED,
            next_status=ExecutionStatus.RUNNING,
        )
        is None
    )
    reread = execution_repositories.get_execution(pool, execution_id=execution.id)
    assert reread is not None
    assert reread.status is ExecutionStatus.SUCCEEDED
    assert reread.implementation_result == payload


def test_the_result_column_check_rejects_non_object_payloads(conn: Connection[Any]) -> None:
    """The canonical-shape CHECK keeps the result column an object or NULL."""
    facts = _authorized_fixture(conn)
    pool = facts["pool"]
    execution = execution_repositories.create_execution(
        pool,
        workspace_id=cast(uuid.UUID, facts["workspace_id"]),
        task_id=cast(uuid.UUID, facts["task_id"]),
        producer_session_id=cast(uuid.UUID, facts["session_id"]),
        execution_number=1,
    )

    # The canonical-shape CHECK rejects non-object payloads; the savepoint
    # keeps the shared test transaction usable after the rejection.
    conn.execute("savepoint result_check")
    with pytest.raises(IntegrityError):
        conn.execute(
            "update openorc.executions set implementation_result = to_jsonb('x'::text) "
            "where id = %s",
            (execution.id,),
        )
    conn.execute("rollback to savepoint result_check")

    # NULL stays legal, and a canonical object round-trips.
    conn.execute(
        "update openorc.executions set implementation_result = %s::jsonb where id = %s",
        ('{"type": "implementation_result", "schema_version": 1}', execution.id),
    )
    stored = conn.execute(
        "select implementation_result from openorc.executions where id = %s",
        (execution.id,),
    ).fetchone()
    assert stored is not None
    assert stored[0] == {"type": "implementation_result", "schema_version": 1}
