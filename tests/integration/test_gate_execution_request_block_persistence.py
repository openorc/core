"""Integration-marked persistence tests for gates, executions, runtime
requests, and TaskBlocks (issue #24).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
human-authority/attempt/block invariants directly: a pending OwnerGate
resolves exactly once with one atomic Task effect (gate outcome + pointer
clear + token rotation + timestamp), stale-token and non-current-gate
resolutions apply nothing, install never replaces a current gate, the
settled 4x4 gate vocabularies and per-type exact-subject binding are
enforced, REVIEW_RESOLUTION approval stays a separate durable fact that
never rewrites the Reviewer's historical result, Executions are
non-exclusive attempt history anchored to the Producer session (a Reviewer
session never anchors one; finalized attempts are absorbing), the
RuntimeRequest correlation identity is unique across history with the typed
approved/rejected control, and TaskBlocks use the settled reason vocabulary
with resolved blocks retained as historical evidence. They are excluded
from the ordinary deterministic baseline by the repository pytest
configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_gate_execution_request_block_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and
outside agent responsibility: in the normal Owner local-development flow
the target is the ephemeral non-production Supabase branch that the
Owner-only ``devserver.sh`` command creates and later deletes (agents never
invoke it). The session fixture merely resets the ``openorc`` schema and
applies the committed migrations from scratch within the supplied database,
and the suite skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import (
    CheckViolation,
    ForeignKeyViolation,
    NotNullViolation,
    UniqueViolation,
)

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.executions import ExecutionDomainError, ExecutionStatus
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewOutcome
from openorc.domain.runtime_requests import (
    RuntimeRequestDomainError,
    RuntimeRequestResolution,
)
from openorc.persistence import blocks as block_repositories
from openorc.persistence import executions as execution_repositories
from openorc.persistence import gates as gate_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import runtime_requests as request_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.gates import OwnerGateResolutionOutcome
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("OPENORC_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("OPENORC_TEST_DATABASE_URL is not configured")
    return url


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Reset the openorc schema and apply all committed migrations from scratch."""
    with connect(database_url) as conn:
        conn.execute("drop schema if exists openorc cascade")
        for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            # Committed migration files are trusted repository content applied
            # wholesale; LiteralString is the driver's injection-safe query
            # contract, satisfied here by repository-controlled file text.
            migration_sql = cast(LiteralString, migration_path.read_text(encoding="utf-8"))
            conn.execute(migration_sql)
        conn.commit()
    return database_url


@pytest.fixture
def conn(migrated_database: str) -> Iterator[Connection[Any]]:
    """One connection per test; each test runs inside one rolled-back transaction."""
    with connect(migrated_database) as connection:
        yield connection
        connection.rollback()


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction.

    Repository calls run inside a nested psycopg transaction (a SAVEPOINT on
    the already-active per-test transaction), mirroring psycopg_pool's
    per-checkout transaction semantics: a successful call releases the
    savepoint, and an expected constraint violation rolls back only to it —
    the per-test transaction stays valid so the remaining assertions run
    instead of failing with ``InFailedSqlTransaction``.
    """

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


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(
    conn: Connection[Any], owner_profile_id: uuid.UUID, name: str = "workspace"
) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, name),
    )
    return workspace_id


def _insert_project(
    conn: Connection[Any], workspace_id: uuid.UUID, name: str = "project"
) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, name),
    )
    return project_id


def _insert_repository(
    conn: Connection[Any],
    *,
    project_id: uuid.UUID,
    workspace_id: uuid.UUID,
    github_repository_id: int,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            github_repository_id,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _ownership_chain(
    conn: Connection[Any], *, github_repository_id: int = 74_000_001
) -> tuple[uuid.UUID, uuid.UUID]:
    """Create Profile -> Workspace -> Project -> Repository; return (ws, repo)."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn,
        project_id=project_id,
        workspace_id=workspace_id,
        github_repository_id=github_repository_id,
    )
    return workspace_id, repository_id


def _insert_task(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int,
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan')",
        (task_id, workspace_id, repository_id, github_issue_id, 200),
    )
    return task_id


def _insert_connection(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    name: str = "primary hub",
) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, session_capacity) "
        "values (%s, %s, 'cline', %s, 1)",
        (connection_id, workspace_id, name),
    )
    return connection_id


def _insert_session(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    connection_id: uuid.UUID,
    role: str,
) -> uuid.UUID:
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, lifecycle_status) "
        "values (%s, %s, %s, %s, %s, 'ready')",
        (session_id, workspace_id, task_id, role, connection_id),
    )
    return session_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _fresh_task(
    conn: Connection[Any], *, github_issue_id: int = 7401
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, DatabasePool]:
    """One ownership chain, one Task, and its pool; returns (ws, repo, task, pool)."""
    workspace_id, repository_id = _ownership_chain(conn)
    task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
    )
    return workspace_id, repository_id, task_id, cast(DatabasePool, _SingleConnectionPool(conn))


def _producer_session(
    conn: Connection[Any], *, workspace_id: uuid.UUID, task_id: uuid.UUID
) -> uuid.UUID:
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    return _insert_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
    )


def _install_gate(
    pool: DatabasePool,
    task_id: uuid.UUID,
    *,
    workspace_id: uuid.UUID,
    gate_type: OwnerGateType,
    plan_revision_id: uuid.UUID | None = None,
    subject_head_sha: str | None = None,
) -> tuple[Any, Any]:
    """Create one pending gate, install it as current, return (gate, task)."""
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=gate_type,
        plan_revision_id=plan_revision_id,
        subject_head_sha=subject_head_sha,
    )
    installed = task_repositories.set_current_owner_gate(
        pool, task_id, expected_state_token=task.state_token, owner_gate_id=gate.id
    )
    assert installed is not None
    return gate, installed


def test_pending_gate_resolves_exactly_once_with_one_atomic_task_effect(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
    gate, installed = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=revision.id,
    )
    assert installed.current_owner_gate_id == gate.id

    resolution = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=installed.state_token,
    )

    # One atomic effect: the gate resolves with its stamp, the pointer
    # clears, and the Task token rotates with a real updated_at stamp.
    # The two stamps may legitimately be equal: PostgreSQL now() is stable
    # across the whole outer test transaction (including the nested
    # SAVEPOINT-backed repository calls), so the resolution UPDATE can
    # commit the exact same transaction timestamp the install used.
    assert resolution.outcome is OwnerGateResolutionOutcome.RESOLVED
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.APPROVED
    assert resolution.gate.decided_at is not None
    assert resolution.task is not None
    assert resolution.task.current_owner_gate_id is None
    assert resolution.task.state_token != installed.state_token
    assert resolution.task.updated_at >= installed.updated_at

    # A pending gate resolves only once: the historical record is immutable
    # and never recycled or overwritten.
    second = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.REJECTED,
        expected_task_state_token=resolution.task.state_token,
    )
    assert second.outcome is OwnerGateResolutionOutcome.NO_OP
    reread = gate_repositories.get_owner_gate(pool, owner_gate_id=gate.id)
    assert reread is not None
    assert reread.status is OwnerGateStatus.APPROVED  # the first outcome stands
    assert reread.decided_at is not None


def test_resolving_the_current_gate_with_a_stale_token_applies_nothing(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
    gate, installed = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=revision.id,
    )

    resolution = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=uuid.uuid4(),  # stale token
    )

    # A stale operation applies nothing: the gate stays pending, the Task
    # pointer and token are untouched.
    assert resolution.outcome is OwnerGateResolutionOutcome.STALE
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.PENDING
    assert resolution.gate.decided_at is None
    assert resolution.task is None
    current = task_repositories.get_task(pool, task_id)
    assert current is not None
    assert current.current_owner_gate_id == gate.id
    assert current.state_token == installed.state_token


def test_a_noncurrent_pending_gate_cannot_be_resolved_and_the_lifecycle_recovers(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
    gate, installed = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=revision.id,
    )
    # A second gate created while the first is still current is pending but
    # not current (the install refuses while the pointer is non-NULL).
    second = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="0123456789abcdef0123456789abcdef01234567",
    )
    current = task_repositories.get_task(pool, task_id)
    assert current is not None
    assert (
        task_repositories.set_current_owner_gate(
            pool, task_id, expected_state_token=current.state_token, owner_gate_id=second.id
        )
        is None
    )

    # A non-current pending gate cannot be resolved through a stale Owner
    # action: nothing is applied to either the gate or the Task, and the
    # gate is never rewritten into an outcome after losing currency.
    stale = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=second.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=installed.state_token,
    )
    assert stale.outcome is OwnerGateResolutionOutcome.STALE
    assert stale.gate is not None
    assert stale.gate.status is OwnerGateStatus.PENDING
    assert stale.task is None
    still = task_repositories.get_task(pool, task_id)
    assert still is not None
    assert still.current_owner_gate_id == gate.id
    assert still.state_token == installed.state_token

    # The lifecycle recovers: resolve the current gate (clearing the
    # pointer and rotating the token), then install the second gate.
    resolved = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=installed.state_token,
    )
    assert resolved.outcome is OwnerGateResolutionOutcome.RESOLVED
    assert resolved.task is not None
    installed_second = task_repositories.set_current_owner_gate(
        pool,
        task_id,
        expected_state_token=resolved.task.state_token,
        owner_gate_id=second.id,
    )
    assert installed_second is not None
    assert installed_second.current_owner_gate_id == second.id
    # The historical first gate is untouched by the later gate's install.
    # Assert by identity rather than list position: the (created_at, id)
    # ordering of list_task_owner_gates is deterministic, but under one
    # shared test transaction with a stable now() it is not creation
    # order — equal timestamps fall back to the UUID id tie-break.
    history = gate_repositories.list_task_owner_gates(pool, task_id=task_id)
    assert {g.id: g.status for g in history} == {
        gate.id: OwnerGateStatus.APPROVED,
        second.id: OwnerGateStatus.PENDING,
    }


def test_only_the_settled_gate_vocabularies_are_accepted(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    gate, _ = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="0123456789abcdef0123456789abcdef01234567",
    )
    # Only the four settled types are accepted. (The initial pending status
    # is inserted explicitly: the lifecycle column has no database default.)
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.owner_gates (workspace_id, task_id, gate_type, status) "
            "values (%s, %s, 'owner_question', 'pending')",
            (workspace_id, task_id),
        )
    # Only the four settled statuses are accepted.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.owner_gates set status = 'deferred' where id = %s",
            (gate.id,),
        )


def test_gates_bind_their_exact_subject_and_reject_wrong_subjects(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    # An implementation-authorization gate binds the exact review-cleared
    # PlanRevision of the same Task/Workspace.
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
    gate, _ = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=revision.id,
    )
    fetched = gate_repositories.get_owner_gate(pool, owner_gate_id=gate.id)
    assert fetched is not None
    assert fetched.plan_revision_id == revision.id
    assert fetched.subject_head_sha is None

    # A PlanRevision belonging to a different Task can never become this
    # gate's subject: the composite FK rejects same-Workspace cross-Task
    # subject corruption. The other Task lives inside the same original
    # Workspace/Repository, so only its Task identity differs.
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7402,
    )
    other_revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=other_task_id,
        revision_number=1,
        content="# Other Plan",
        repository_base_sha="base-b",
    )
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        conn.execute(
            "update openorc.owner_gates set plan_revision_id = %s where id = %s",
            (other_revision.id, gate.id),
        )
    # A review_resolution gate binds exactly one subject: both subjects (or
    # neither) violate the coherence CHECK. (The initial pending status is
    # inserted explicitly: the lifecycle column has no database default.)
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.owner_gates "
            "(workspace_id, task_id, gate_type, status, plan_revision_id, subject_head_sha) "
            "values (%s, %s, 'review_resolution', 'pending', %s, %s)",
            (workspace_id, task_id, revision.id, "0123456789abcdef0123456789abcdef01234567"),
        )
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.owner_gates "
            "(workspace_id, task_id, gate_type, status, plan_revision_id, subject_head_sha) "
            "values (%s, %s, 'review_resolution', 'pending', null, null)",
            (workspace_id, task_id),
        )


def test_task_pointer_fk_rejects_cross_task_gate_pointers(conn: Connection[Any]) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    gate, _ = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="0123456789abcdef0123456789abcdef01234567",
    )
    # A different Task in the same Workspace cannot point at this gate: the
    # composite foreign key rejects cross-Task pointer corruption. The other
    # Task lives inside the same original Workspace/Repository, so only its
    # Task identity differs.
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7403,
    )
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        conn.execute(
            "update openorc.tasks set current_owner_gate_id = %s where id = %s",
            (gate.id, other_task_id),
        )


def test_review_resolution_approval_never_rewrites_reviewer_history(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
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
    requested = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="one finding",
        findings=[{"summary": "fix the thing", "details": ["detail"]}],
    )
    assert requested is not None

    # The Owner approves the exhausted planning review through a
    # REVIEW_RESOLUTION gate: a separate durable override fact.
    gate, installed = _install_gate(
        pool,
        task_id,
        workspace_id=workspace_id,
        gate_type=OwnerGateType.REVIEW_RESOLUTION,
        plan_revision_id=revision.id,
    )
    resolution = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=installed.state_token,
    )
    assert resolution.outcome is OwnerGateResolutionOutcome.RESOLVED
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.APPROVED

    # The Reviewer's historical result is never rewritten to ACCEPTED: the
    # override is a distinct fact on the gate, and the iteration keeps its
    # CHANGES_REQUESTED outcome and findings.
    reread = review_repositories.get_review_iteration(pool, review_iteration_id=iteration.id)
    assert reread is not None
    assert reread.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert reread.findings == [{"summary": "fix the thing", "details": ["detail"]}]


def test_multiple_executions_coexist_without_exclusivity(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    session_id = _producer_session(conn, workspace_id=workspace_id, task_id=task_id)

    first = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        execution_number=1,
    )
    running = execution_repositories.update_execution_status(
        pool,
        execution_id=first.id,
        expected_status=ExecutionStatus.QUEUED,
        next_status=ExecutionStatus.RUNNING,
    )
    assert running is not None
    # A second attempt coexists while the first is still active: ordering is
    # identity, never exclusivity, and both rows share the Producer session.
    second = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        execution_number=2,
    )
    history = execution_repositories.list_task_executions(pool, task_id=task_id)
    assert [execution.id for execution in history] == [first.id, second.id]
    assert all(execution.producer_session_id == session_id for execution in history)
    assert history[0].status is ExecutionStatus.RUNNING
    assert history[1].status is ExecutionStatus.QUEUED

    # The per-Task attempt identity is unique; a duplicate number is the
    # durable backstop behind the fresh-attempt behavior.
    with pytest.raises(UniqueViolation), conn.transaction():
        execution_repositories.create_execution(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=session_id,
            execution_number=2,
        )


def test_execution_creation_requires_the_producer_session(conn: Connection[Any]) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    producer_id = _producer_session(conn, workspace_id=workspace_id, task_id=task_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    reviewer_id = _insert_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="reviewer",
    )

    # A Reviewer session never anchors an Execution.
    with pytest.raises(ExecutionDomainError):
        execution_repositories.create_execution(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=reviewer_id,
            execution_number=1,
        )
    # The Producer session does.
    created = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=producer_id,
        execution_number=1,
    )
    assert created.producer_session_id == producer_id

    # A session from a different Task is scope-rejected by the composite FK.
    # The other Task lives inside the same original Workspace/Repository, so
    # only its Task identity differs.
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7404,
    )
    other_session = _producer_session(conn, workspace_id=workspace_id, task_id=other_task_id)
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        execution_repositories.create_execution(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=other_session,
            execution_number=2,
        )


def test_finalized_executions_are_absorbing_history(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    session_id = _producer_session(conn, workspace_id=workspace_id, task_id=task_id)
    execution = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        execution_number=1,
    )
    running = execution_repositories.update_execution_status(
        pool,
        execution_id=execution.id,
        expected_status=ExecutionStatus.QUEUED,
        next_status=ExecutionStatus.RUNNING,
    )
    assert running is not None
    succeeded = execution_repositories.update_execution_status(
        pool,
        execution_id=execution.id,
        expected_status=ExecutionStatus.RUNNING,
        next_status=ExecutionStatus.SUCCEEDED,
    )
    assert succeeded is not None
    finalized_at = succeeded.updated_at

    # A finalized Execution is absorbing: no path rewrites its status to
    # manufacture different history. The retry is a fresh row instead.
    with pytest.raises(ExecutionDomainError):
        execution_repositories.update_execution_status(
            pool,
            execution_id=execution.id,
            expected_status=ExecutionStatus.SUCCEEDED,
            next_status=ExecutionStatus.RUNNING,
        )
    reread = execution_repositories.get_execution(pool, execution_id=execution.id)
    assert reread is not None
    assert reread.status is ExecutionStatus.SUCCEEDED
    assert reread.updated_at == finalized_at  # nothing restamped the record

    retry = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        execution_number=2,
    )
    assert retry.producer_session_id == session_id
    # A transition whose expected status no longer matches is a no-op.
    assert (
        execution_repositories.update_execution_status(
            pool,
            execution_id=retry.id,
            expected_status=ExecutionStatus.PAUSED,
            next_status=ExecutionStatus.RUNNING,
        )
        is None
    )


def test_runtime_request_correlation_identity_is_unique_across_history(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    session_id = _producer_session(conn, workspace_id=workspace_id, task_id=task_id)

    request = request_repositories.create_runtime_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        external_approval_id="cline-approval-1",
    )
    assert request.kind.value == "action_approval"
    resolved = request_repositories.resolve_runtime_request(
        pool,
        runtime_request_id=request.id,
        resolution=RuntimeRequestResolution.APPROVED,
    )
    assert resolved is not None
    assert resolved.status.value == "resolved"
    assert resolved.resolution is RuntimeRequestResolution.APPROVED
    assert resolved.closed_at is not None

    # The correlation identity is unique across all history: one exact
    # external request is one row forever, and resolving it never frees the
    # external identity for a second historical row in the same session.
    with pytest.raises(UniqueViolation), conn.transaction():
        request_repositories.create_runtime_request(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=session_id,
            external_approval_id="cline-approval-1",
        )
    # A different external identifier is a different exact request.
    second = request_repositories.create_runtime_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        external_approval_id="cline-approval-2",
    )
    assert second.id != request.id


def test_runtime_requests_require_the_producer_session(conn: Connection[Any]) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    producer_id = _producer_session(conn, workspace_id=workspace_id, task_id=task_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    reviewer_id = _insert_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="reviewer",
    )

    # Reviewer sessions do not participate in runtime approvals.
    with pytest.raises(RuntimeRequestDomainError):
        request_repositories.create_runtime_request(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=reviewer_id,
            external_approval_id="cline-approval-1",
        )
    created = request_repositories.create_runtime_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=producer_id,
        external_approval_id="cline-approval-1",
    )
    assert created.producer_session_id == producer_id

    # A session from a different Task is scope-rejected by the composite FK.
    # The other Task lives inside the same original Workspace/Repository, so
    # only its Task identity differs.
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7405,
    )
    other_session = _producer_session(conn, workspace_id=workspace_id, task_id=other_task_id)
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        request_repositories.create_runtime_request(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=other_session,
            external_approval_id="cline-approval-2",
        )


def test_terminal_requests_are_immutable_historical_records(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    session_id = _producer_session(conn, workspace_id=workspace_id, task_id=task_id)
    request = request_repositories.create_runtime_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        external_approval_id="cline-approval-1",
    )

    resolved = request_repositories.resolve_runtime_request(
        pool,
        runtime_request_id=request.id,
        resolution=RuntimeRequestResolution.APPROVED,
    )
    assert resolved is not None

    # A terminal request is a historical record: no rewrite, no repurpose,
    # and no second control.
    assert (
        request_repositories.resolve_runtime_request(
            pool,
            runtime_request_id=request.id,
            resolution=RuntimeRequestResolution.REJECTED,
        )
        is None
    )
    assert request_repositories.expire_runtime_request(pool, runtime_request_id=request.id) is None
    assert request_repositories.cancel_runtime_request(pool, runtime_request_id=request.id) is None
    reread = request_repositories.get_runtime_request(pool, runtime_request_id=request.id)
    assert reread is not None
    assert reread.status.value == "resolved"
    assert reread.resolution is RuntimeRequestResolution.APPROVED
    assert reread.closed_at is not None

    # The terminal coherence is CHECK-enforced: a terminal form without its
    # matching facts is rejected durably.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.runtime_requests set status = 'expired' where id = %s",
            (request.id,),
        )
    expired = request_repositories.create_runtime_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=session_id,
        external_approval_id="cline-approval-3",
    )
    expired_request = request_repositories.expire_runtime_request(
        pool, runtime_request_id=expired.id
    )
    assert expired_request is not None
    assert expired_request.status.value == "expired"
    assert expired_request.resolution is None
    assert expired_request.closed_at is not None


def test_task_block_reasons_use_the_settled_vocabulary(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    block = block_repositories.create_task_block(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        reason=TaskBlockReason.STALE_OPERATION,
        context={"expected_subject": "sha-a", "observed_subject": "sha-b"},
    )
    assert block.resolved_at is None

    # Only the settled twelve-value reason vocabulary is accepted.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_blocks (workspace_id, task_id, reason, context) "
            "values (%s, %s, 'review_loop_exhausted', '{}')",
            (workspace_id, task_id),
        )
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_blocks (workspace_id, task_id, reason, context) "
            "values (%s, %s, 'owner_chat', '{}')",
            (workspace_id, task_id),
        )
    # The context is a canonical JSON object: other shapes are rejected.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_blocks (workspace_id, task_id, reason, context) "
            "values (%s, %s, 'unknown', '[]')",
            (workspace_id, task_id),
        )
    # The context is NOT NULL by design: a block always carries its
    # recovery context, and the plain jsonb_typeof CHECK alone would pass
    # SQL NULL. An empty JSON object is the valid empty context.
    with pytest.raises(NotNullViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_blocks (workspace_id, task_id, reason, context) "
            "values (%s, %s, 'unknown', null)",
            (workspace_id, task_id),
        )
    empty_context = block_repositories.create_task_block(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        reason=TaskBlockReason.UNKNOWN,
        context={},
    )
    assert empty_context.context == {}


def test_resolved_blocks_remain_historical_with_context(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    block = block_repositories.create_task_block(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        reason=TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN,
        context={"send_outcome": "unknown", "runtime_event": "approval-send"},
    )

    resolved = block_repositories.resolve_task_block(pool, task_block_id=block.id)
    assert resolved is not None
    assert resolved.resolved_at is not None

    # A resolved block remains historical evidence: reason and recovery
    # context are retained untouched, and resolution happens exactly once.
    assert resolved.reason is TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN
    assert resolved.context == {
        "send_outcome": "unknown",
        "runtime_event": "approval-send",
    }
    assert block_repositories.resolve_task_block(pool, task_block_id=block.id) is None

    # The full history keeps the resolved block; the current view does not.
    history = block_repositories.list_task_blocks(pool, task_id=task_id)
    assert [b.id for b in history] == [block.id]
    assert history[0].resolved_at is not None
    assert block_repositories.list_current_task_blocks(pool, task_id=task_id) == []
