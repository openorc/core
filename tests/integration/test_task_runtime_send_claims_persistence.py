"""Integration-marked persistence tests for the F2 runtime-send claims and
bootstrap stage evidence (issue #87).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
invariants directly against real Postgres:

- the ``task_runtime_send_claims`` partial unique indexes reject a
  same-subject duplicate claim (completed, known-failed, and unresolved
  claims alike) while a distinct permitted next revision claims its own row;
- the composite subject foreign keys reject cross-Task and cross-Workspace
  subjects, so a claim can never reference another Task's session,
  iteration, or gate;
- settlement and known non-delivery are forward-only one-shot transitions
  with a coherent terminal status/settlement stamp;
- the bootstrap stage evidence is forward-only, bound to the exact claim
  token, rejected on archival, and stamped by the database clock.

They are excluded from the ordinary deterministic baseline by the repository
pytest configuration. Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_runtime_send_claims_persistence.py

The suite consumes the database it is given and never provisions one.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation, UniqueViolation
from psycopg.types.json import Jsonb

from openorc.domain.reviews import ReviewLoopPurpose
from openorc.domain.send_claims import TaskRuntimeSendClaimKind
from openorc.domain.tasks import TaskPlanningBootstrapStage
from openorc.persistence.planning import create_plan_revision
from openorc.persistence.reviews import create_review_iteration, create_review_loop
from openorc.persistence.send_claims import (
    create_task_runtime_send_claim,
    get_task_runtime_send_claim_for_subject,
    list_task_runtime_send_claims,
    record_task_runtime_send_claim_known_non_delivery,
    settle_task_runtime_send_claim,
)
from openorc.persistence.tasks import (
    claim_task_planning_bootstrap,
    get_task_planning_bootstrap_evidence,
    record_planning_bootstrap_stage,
)

pytestmark = pytest.mark.integration

_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and
    transaction (the same SAVEPOINT semantics as the other persistence
    suites)."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            with self._connection.transaction():
                yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _insert_task_chain(conn: Connection[Any]) -> tuple[Any, ...]:
    """Insert the Profile/Workspace/Project/Repository/Task chain and return
    (task_id, workspace_id, state_token)."""
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, profile_id, "workspace"),
    )
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private) values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            5_000_000,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
        ),
    )
    task_id = uuid.uuid4()
    state_token = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, "
        "status, source_requirements_fingerprint, state_token) "
        "values (%s, %s, %s, 9001, 42, 'planning', %s, %s)",
        (task_id, workspace_id, repository_id, "a" * 64, state_token),
    )
    return task_id, workspace_id, state_token


def _insert_producer_session(conn: Connection[Any], workspace_id: Any, task_id: Any) -> Any:
    """Insert one READY Producer binding and return its id."""
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name) "
        "values (%s, %s, 'cline', 'staged-hub')",
        (connection_id, workspace_id),
    )
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, initialization_protocol_version, "
        "effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, 'producer', %s, %s, 'ready', '1', %s, now())",
        (
            session_id,
            workspace_id,
            task_id,
            connection_id,
            "ext-" + str(session_id)[:8],
            # The jsonb column needs the explicit psycopg JSONB adapter; a
            # plain dict is not adaptable (the Owner-run --testdb suite's
            # ProgrammingError).
            Jsonb({"adapter": "cline"}),
        ),
    )
    return session_id


def _insert_planning_iteration(
    pool: Any, workspace_id: Any, task_id: Any, revision_number: int
) -> Any:
    """One immutable PlanRevision plus its planning loop and first
    unfinalized iteration; returns the iteration id."""
    revision = create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=revision_number,
        content=f"# Plan {revision_number}\n",
        repository_base_sha="c" * 40,
    )
    loop = create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=3,
    )
    iteration = create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert iteration is not None
    return iteration.id


def _insert_approved_pr_gate(conn: Connection[Any], workspace_id: Any, task_id: Any) -> Any:
    """Insert one durably approved PR_AUTHORIZATION gate; returns its id."""
    gate_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.owner_gates "
        "(id, workspace_id, task_id, gate_type, status, subject_head_sha, decided_at) "
        "values (%s, %s, %s, 'pr_authorization', 'approved', %s, now())",
        (gate_id, workspace_id, task_id, _HEAD_SHA),
    )
    return gate_id


def test_initial_plan_claim_is_written_once_and_duplicates_are_rejected(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id, _state_token = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)

    claim = create_task_runtime_send_claim(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
        task_agent_session_id=session_id,
    )
    assert claim.status.value == "claimed"
    assert claim.claimed_at is not None
    # The database clock stamped the claim instant.
    assert claim.claimed_at.tzinfo is not None

    # A replayed or raced second claim for the same Task is rejected by the
    # partial unique index — completed, known-failed, and unresolved prior
    # claims alike.
    with pytest.raises(UniqueViolation):
        create_task_runtime_send_claim(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
            task_agent_session_id=session_id,
        )

    # The pre-check read sees the committed claim for the exact subject.
    existing = get_task_runtime_send_claim_for_subject(
        pool, task_id=task_id, kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH
    )
    assert existing is not None and existing.id == claim.id

    # The claim row survives in the Task's full claim history.
    assert [row.id for row in list_task_runtime_send_claims(pool, task_id=task_id)] == [claim.id]


def test_planning_revise_claims_are_one_per_iteration_and_the_next_revision_is_permitted(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id, _state_token = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)
    first_iteration = _insert_planning_iteration(pool, workspace_id, task_id, 1)
    second_iteration = _insert_planning_iteration(pool, workspace_id, task_id, 2)

    first_claim = create_task_runtime_send_claim(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
        task_agent_session_id=session_id,
        review_iteration_id=first_iteration,
    )
    # A second claim for the SAME iteration is rejected whatever its outcome;
    # a distinct permitted next revision claims its own row.
    with pytest.raises(UniqueViolation):
        create_task_runtime_send_claim(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
            task_agent_session_id=session_id,
            review_iteration_id=first_iteration,
        )
    second_claim = create_task_runtime_send_claim(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
        task_agent_session_id=session_id,
        review_iteration_id=second_iteration,
    )
    assert first_claim.review_iteration_id == first_iteration
    assert second_claim.review_iteration_id == second_iteration

    # The closed subject-form coherence: a planning-revise claim can never
    # carry a gate subject — the domain model rejects the malformed subject
    # form before any statement runs.
    with pytest.raises(ValueError):
        create_task_runtime_send_claim(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
            task_agent_session_id=session_id,
            review_iteration_id=None,
        )


def test_pr_compose_claim_binds_the_approved_gate_and_head_exactly_once(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id, _state_token = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    gate_id = _insert_approved_pr_gate(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)

    claim = create_task_runtime_send_claim(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
        task_agent_session_id=session_id,
        owner_gate_id=gate_id,
        subject_head_sha=_HEAD_SHA,
    )
    assert claim.subject_head_sha == _HEAD_SHA

    with pytest.raises(UniqueViolation):
        create_task_runtime_send_claim(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
            task_agent_session_id=session_id,
            owner_gate_id=gate_id,
            subject_head_sha=_HEAD_SHA,
        )
    existing = get_task_runtime_send_claim_for_subject(
        pool,
        task_id=task_id,
        kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
        owner_gate_id=gate_id,
    )
    assert existing is not None and existing.id == claim.id


def test_claims_reject_cross_task_and_cross_workspace_subjects(
    conn: Connection[Any],
) -> None:
    first_task, first_workspace, _token = _insert_task_chain(conn)
    second_task, second_workspace, _token2 = _insert_task_chain(conn)
    second_session = _insert_producer_session(conn, second_workspace, second_task)
    pool = _SingleConnectionPool(conn)

    # A claim whose session binding belongs to another Task (or another
    # Workspace) can never commit: the composite subject foreign keys keep
    # every claimed subject in the addressed Task/Workspace scope.
    with pytest.raises(ForeignKeyViolation):
        create_task_runtime_send_claim(
            pool,
            workspace_id=first_workspace,
            task_id=first_task,
            kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
            task_agent_session_id=second_session,
        )
    # A second Workspace's Task may claim its own send independently.
    independent = create_task_runtime_send_claim(
        pool,
        workspace_id=second_workspace,
        task_id=second_task,
        kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
        task_agent_session_id=second_session,
    )
    assert independent.task_id == second_task
    assert independent.workspace_id == second_workspace


def test_settlement_and_known_non_delivery_are_forward_only(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id, _state_token = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)
    claim = create_task_runtime_send_claim(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
        task_agent_session_id=session_id,
    )

    settled = settle_task_runtime_send_claim(
        pool,
        claim_id=claim.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome={"plan_revision_id": str(uuid.uuid4())},
    )
    assert settled is not None
    assert settled.status.value == "settled"
    assert settled.settled_at is not None
    assert "plan_revision_id" in settled.outcome

    # The one-shot transition: a second settlement and a late known
    # non-delivery are rejected no-ops that must not be retried blindly.
    assert (
        settle_task_runtime_send_claim(
            pool,
            claim_id=claim.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome={"plan_revision_id": str(uuid.uuid4())},
        )
        is None
    )
    assert (
        record_task_runtime_send_claim_known_non_delivery(
            pool,
            claim_id=claim.id,
            task_id=task_id,
            workspace_id=workspace_id,
            failure_kind="runtime_pre_effect",
        )
        is None
    )

    # A known non-delivery on a still-claimed claim settles exactly once
    # with its safe failure-kind label.
    second_task, second_workspace, _token2 = _insert_task_chain(conn)
    second_session = _insert_producer_session(conn, second_workspace, second_task)
    second_claim = create_task_runtime_send_claim(
        pool,
        workspace_id=second_workspace,
        task_id=second_task,
        kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
        task_agent_session_id=second_session,
    )
    known = record_task_runtime_send_claim_known_non_delivery(
        pool,
        claim_id=second_claim.id,
        task_id=second_task,
        workspace_id=second_workspace,
        failure_kind="runtime_pre_effect",
    )
    assert known is not None
    assert known.status.value == "known_nondelivery"
    assert known.settled_at is not None
    assert known.outcome == {"failure_kind": "runtime_pre_effect"}
    assert (
        settle_task_runtime_send_claim(
            pool,
            claim_id=second_claim.id,
            task_id=second_task,
            workspace_id=second_workspace,
            outcome={},
        )
        is None
    )
    # A scope-mismatched settlement is a rejected no-op as well.
    assert (
        settle_task_runtime_send_claim(
            pool,
            claim_id=claim.id,
            task_id=uuid.uuid4(),
            workspace_id=workspace_id,
            outcome={},
        )
        is None
    )


def test_bootstrap_stage_evidence_is_forward_only_and_claim_bound(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id, state_token = _insert_task_chain(conn)
    pool = _SingleConnectionPool(conn)

    # Before the claim there is no evidence at all.
    assert get_task_planning_bootstrap_evidence(pool, task_id=task_id) is None

    claim_token = uuid.uuid4()
    post_claim_token = claim_task_planning_bootstrap(
        pool,
        task_id=task_id,
        expected_state_token=state_token,
        claim_token=claim_token,
    )
    assert post_claim_token is not None
    evidence = get_task_planning_bootstrap_evidence(pool, task_id=task_id)
    assert evidence is not None
    assert evidence.claim_token == claim_token
    # The database clock stamped the claim instant with the exactly-once
    # claim write (issue #87).
    assert evidence.claimed_at is not None and evidence.claimed_at.tzinfo is not None
    assert evidence.stage is None and evidence.stage_settled_at is None

    # ``prepared`` applies only from the unsettled stage under the exact
    # claim token.
    assert (
        record_planning_bootstrap_stage(
            pool,
            task_id=task_id,
            claim_token=uuid.uuid4(),
            stage=TaskPlanningBootstrapStage.PREPARED,
        )
        is False
    )  # a foreign claim identity never writes stage history
    assert record_planning_bootstrap_stage(
        pool,
        task_id=task_id,
        claim_token=claim_token,
        stage=TaskPlanningBootstrapStage.PREPARED,
    )
    # Stage history never regresses: ``prepared`` cannot be re-written, and
    # ``ready_for_planning`` applies only from ``prepared``.
    assert (
        record_planning_bootstrap_stage(
            pool,
            task_id=task_id,
            claim_token=claim_token,
            stage=TaskPlanningBootstrapStage.PREPARED,
        )
        is False
    )
    assert record_planning_bootstrap_stage(
        pool,
        task_id=task_id,
        claim_token=claim_token,
        stage=TaskPlanningBootstrapStage.READY_FOR_PLANNING,
    )
    assert (
        record_planning_bootstrap_stage(
            pool,
            task_id=task_id,
            claim_token=claim_token,
            stage=TaskPlanningBootstrapStage.READY_FOR_PLANNING,
        )
        is False
    )

    evidence = get_task_planning_bootstrap_evidence(pool, task_id=task_id)
    assert evidence is not None
    assert evidence.stage is TaskPlanningBootstrapStage.READY_FOR_PLANNING
    assert evidence.stage_settled_at is not None

    # An archived Task never accepts stage evidence.
    conn.execute(
        "update openorc.tasks set status = 'cancelled', archived_at = now(), "
        "state_token = gen_random_uuid() where id = %s",
        (task_id,),
    )
    assert (
        record_planning_bootstrap_stage(
            pool,
            task_id=task_id,
            claim_token=claim_token,
            stage=TaskPlanningBootstrapStage.PREPARED,
        )
        is False
    )
