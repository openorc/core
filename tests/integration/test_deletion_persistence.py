"""Integration-marked deletion-persistence tests for the deletion ownership
graph (issue #27).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the destructive
deletion semantics of the complete Phase 1 persistence model: the canonical
``auth.users`` account-root cascade, scoped Workspace/Project/Repository
deletion through the aggregate persistence operations, archived-Task purge
with its full aggregate, and the Connection delete/disconnect boundary. They
are excluded from the ordinary deterministic baseline by the repository
pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \
      .venv/bin/python -m pytest -m integration tests/integration/test_deletion_persistence.py

The suite consumes the database it is given and never provisions one; the
shared session fixture in ``tests/integration/conftest.py`` resets the
``openorc`` schema and replays the committed migrations once per pytest
session. Per-test transaction rollback keeps every deletion isolated.

Deletion must never touch external systems: every scenario here is pure
OpenOrc-database work. Deleting an OpenOrc Repository mapping never deletes
or mutates the GitHub repository, and no OpenOrc deletion deletes or mutates
GitHub issues, branches, commits, pull requests, checks, or comments, or any
runtime-owned credential/configuration/filesystem state — there is no
external call anywhere on a deletion path, and none of these tests invokes
any adapter. The GitHub/runtime neutrality is structural: OpenOrc deletion
is OpenOrc-database work only.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose
from openorc.domain.tasks import Task, TaskDomainError, TaskStatus
from openorc.persistence import (
    blocks as block_repositories,
)
from openorc.persistence import (
    connections as connection_repositories,
)
from openorc.persistence import (
    deletion as deletion_repositories,
)
from openorc.persistence import (
    events as event_repositories,
)
from openorc.persistence import (
    executions as execution_repositories,
)
from openorc.persistence import (
    gates as gate_repositories,
)
from openorc.persistence import (
    planning as planning_repositories,
)
from openorc.persistence import (
    pull_requests as pull_request_repositories,
)
from openorc.persistence import (
    reviews as review_repositories,
)
from openorc.persistence import (
    runtime_requests as request_repositories,
)
from openorc.persistence import (
    sessions as session_repositories,
)
from openorc.persistence import (
    tasks as task_repositories,
)
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration

# One exact 40-hex head SHA for PR subject fixtures.
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"

# Every Phase 1 table scoped by a direct workspace_id. Used by the account
# deletion assertions: after an account-root deletion, no row attributable
# exclusively to the deleted Profile may remain in any of them.
_WORKSPACE_SCOPED_TABLES = (
    "projects",
    "repositories",
    "connections",
    "workflow_role_bindings",
    "tasks",
    "task_agent_sessions",
    "plan_revisions",
    "review_loops",
    "review_iterations",
    "owner_gates",
    "executions",
    "runtime_requests",
    "task_blocks",
    "task_pull_requests",
    "workflow_events",
)

# Task-owned Phase 1 tables, keyed by task_id. The archived-Task purge must
# remove every row from all of them.
_TASK_OWNED_TABLES = (
    "task_agent_sessions",
    "plan_revisions",
    "review_loops",
    "review_iterations",
    "owner_gates",
    "executions",
    "runtime_requests",
    "task_blocks",
    "task_pull_requests",
)


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
        raise AssertionError("deletion tests never close pools")


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    """Insert the Supabase Auth user and its 1:1 OpenOrc Profile.

    ``openorc.profiles.id`` references ``auth.users (id) ON DELETE CASCADE``
    — the single sanctioned Supabase Auth boundary (issue #27). The Auth user
    row is the deletion root for the account-root test; both inserts roll
    back with the test transaction.
    """
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
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
            f"repo-{github_repository_id}",
            f"https://github.com/octocat/repo-{github_repository_id}",
            False,
            "main",
        ),
    )
    return repository_id


def _count_rows(conn: Connection[Any], table: str, column: str, value: Any) -> int:
    """Count rows in one openorc table matching one column value.

    Table and column identifiers come from the repository-controlled static
    tuples in this module; the value is always parameterized.
    """
    query = cast(LiteralString, f"select count(*) from openorc.{table} where {column} = %s")
    row = conn.execute(query, (value,)).fetchone()
    assert row is not None
    return row[0]


def _snapshot_workspace_counts(
    conn: Connection[Any], workspace_ids: list[uuid.UUID]
) -> dict[str, int]:
    """Row counts per workspace-scoped table for the supplied Workspace set."""
    counts: dict[str, int] = {}
    for table in _WORKSPACE_SCOPED_TABLES:
        # Static repository-controlled table identifier; value parameterized.
        query = cast(
            LiteralString,
            f"select count(*) from openorc.{table} where workspace_id = any(%s)",
        )
        row = conn.execute(query, (workspace_ids,)).fetchone()
        assert row is not None
        counts[table] = row[0]
    return counts


def _assert_settled(conn: Connection[Any]) -> None:
    """Force every deferred foreign key to be checked now, inside the test
    transaction: proves the deletion left no pending referential violation
    (the same check the commit boundary would perform)."""
    conn.execute("set constraints all immediate")


def _populate_full_task_aggregate(
    pool: DatabasePool,
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    connection_id: uuid.UUID,
    github_issue_id: int,
    github_issue_number: int,
) -> Task:
    """Create one Task populated across every implemented Task-owned table.

    Built through the Phase 1 persistence primitives, so every row satisfies
    the settled domain/constraint rules: Producer session, PlanRevision (and
    current pointer), planning ReviewLoop/ReviewIteration, IMPLEMENTATION_
    AUTHORIZATION OwnerGate (and current pointer), Execution, RuntimeRequest,
    TaskBlock, TaskPullRequest, PR-subject ReviewIteration and MERGE_DECISION
    OwnerGate, and a Task-scoped WorkflowEvent.
    """
    task = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
        github_issue_number=github_issue_number,
    )
    session = session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    plan_revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        revision_number=1,
        content="# Plan",
        repository_base_sha=_HEAD_SHA,
    )
    task = task_repositories.set_current_plan_revision(
        pool,
        task.id,
        expected_state_token=task.state_token,
        plan_revision_id=plan_revision.id,
    )
    assert task is not None
    planning_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
    )
    review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        review_loop_id=planning_loop.id,
        iteration_number=1,
        plan_revision_id=plan_revision.id,
    )
    implementation_gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=plan_revision.id,
    )
    task = task_repositories.set_current_owner_gate(
        pool,
        task.id,
        expected_state_token=task.state_token,
        owner_gate_id=implementation_gate.id,
    )
    assert task is not None
    execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        producer_session_id=session.id,
        execution_number=1,
    )
    request_repositories.create_runtime_request(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        producer_session_id=session.id,
        external_approval_id="cline-approval-1",
    )
    block_repositories.create_task_block(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        reason=TaskBlockReason.AGENT_SESSION_LOST,
        context={"stage": "plan"},
    )
    task_pull_request = pull_request_repositories.create_task_pull_request(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        repository_id=repository_id,
        github_pr_id=github_issue_id + 900_000,
        github_pr_number=github_issue_number,
        head_ref=f"openorc/task-{github_issue_number}",
        base_ref="main",
        head_sha=_HEAD_SHA,
    )
    pr_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=5,
    )
    review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        review_loop_id=pr_loop.id,
        iteration_number=1,
        task_pull_request_id=task_pull_request.id,
        reviewed_head_sha=_HEAD_SHA,
    )
    gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.MERGE_DECISION,
        subject_head_sha=_HEAD_SHA,
        task_pull_request_id=task_pull_request.id,
    )
    event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
        subject_type="task",
        subject_id=task.id,
        context={"source": "deletion-test"},
    )
    return task


def _populate_light_task(
    pool: DatabasePool,
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    connection_id: uuid.UUID,
    github_issue_id: int,
    github_issue_number: int,
) -> Task:
    """Create one minimal Task with a session, plan revision, and event."""
    task = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
        github_issue_number=github_issue_number,
    )
    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        revision_number=1,
        content="# Plan",
        repository_base_sha=_HEAD_SHA,
    )
    event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
    )
    return task


def _count_auth_users(conn: Connection[Any], user_id: uuid.UUID) -> int:
    """Count the Supabase Auth user row behind one Profile identity."""
    row = conn.execute("select count(*) from auth.users where id = %s", (user_id,)).fetchone()
    assert row is not None
    return row[0]


def test_account_deletion_from_the_auth_users_root_removes_the_complete_openorc_graph(
    conn: Connection[Any],
) -> None:
    """The canonical deep deletion proof: direct Supabase-administrative
    deletion of the auth user removes the complete OpenOrc-owned graph.

    No OpenOrc code runs on this path — the single-statement database cascade
    (``auth.users -> openorc.profiles -> ...``) alone must leave no OpenOrc
    row attributable exclusively to the deleted Profile, must not strand
    orphans, and must not disturb the separate control account.
    """
    target_profile = _insert_profile(conn)
    primary_workspace = _insert_workspace(conn, target_profile, name="primary")
    secondary_workspace = _insert_workspace(conn, target_profile, name="secondary")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    primary_project = _insert_project(conn, primary_workspace, name="primary-project")
    primary_repository = _insert_repository(
        conn,
        project_id=primary_project,
        workspace_id=primary_workspace,
        github_repository_id=71_000_001,
    )
    primary_connection = connection_repositories.create_connection(
        pool,
        workspace_id=primary_workspace,
        adapter=AdapterType.CLINE,
        name="primary-hub",
        auth_reference="auth-ref-primary",
    )
    connection_repositories.set_role_binding(
        pool,
        workspace_id=primary_workspace,
        role=WorkflowRole.PRODUCER,
        connection_id=primary_connection.id,
    )
    event_repositories.record_workflow_event(
        pool,
        workspace_id=primary_workspace,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
    )
    _populate_full_task_aggregate(
        pool,
        workspace_id=primary_workspace,
        repository_id=primary_repository,
        connection_id=primary_connection.id,
        github_issue_id=71_001,
        github_issue_number=1,
    )

    # A second Workspace with its own lighter graph: sibling isolation inside
    # the same account is observable through the removal.
    secondary_project = _insert_project(conn, secondary_workspace, name="secondary-project")
    secondary_repository = _insert_repository(
        conn,
        project_id=secondary_project,
        workspace_id=secondary_workspace,
        github_repository_id=71_000_002,
    )
    secondary_connection = connection_repositories.create_connection(
        pool,
        workspace_id=secondary_workspace,
        adapter=AdapterType.CLINE,
        name="secondary-hub",
        auth_reference="auth-ref-secondary",
    )
    _populate_light_task(
        pool,
        workspace_id=secondary_workspace,
        repository_id=secondary_repository,
        connection_id=secondary_connection.id,
        github_issue_id=71_002,
        github_issue_number=2,
    )

    # Separate control account that must survive untouched.
    control_profile = _insert_profile(conn)
    control_workspace = _insert_workspace(conn, control_profile, name="control")
    control_project = _insert_project(conn, control_workspace, name="control-project")
    control_repository = _insert_repository(
        conn,
        project_id=control_project,
        workspace_id=control_workspace,
        github_repository_id=71_000_009,
    )
    control_connection = connection_repositories.create_connection(
        pool,
        workspace_id=control_workspace,
        adapter=AdapterType.CLINE,
        name="control-hub",
        auth_reference="auth-ref-control",
    )
    _populate_light_task(
        pool,
        workspace_id=control_workspace,
        repository_id=control_repository,
        connection_id=control_connection.id,
        github_issue_id=71_901,
        github_issue_number=901,
    )

    target_workspaces = [primary_workspace, secondary_workspace]
    target_before = _snapshot_workspace_counts(conn, target_workspaces)
    assert target_before["tasks"] == 2
    assert target_before["task_pull_requests"] == 1
    control_before = _snapshot_workspace_counts(conn, [control_workspace])
    assert control_before["tasks"] == 1

    # The account root: direct Supabase-administrative deletion of the auth
    # user. No OpenOrc helper runs; the FK cascade alone removes the graph.
    conn.execute("delete from auth.users where id = %s", (target_profile,))
    _assert_settled(conn)

    assert _count_auth_users(conn, target_profile) == 0
    assert _count_rows(conn, "profiles", "id", target_profile) == 0
    assert _count_rows(conn, "workspaces", "owner_profile_id", target_profile) == 0
    for table, count in _snapshot_workspace_counts(conn, target_workspaces).items():
        assert count == 0, f"{table} retained rows attributable to the deleted account"
    # The control Auth user/Profile/OpenOrc graph remains intact.
    assert _count_auth_users(conn, control_profile) == 1
    assert _count_rows(conn, "profiles", "id", control_profile) == 1
    assert _snapshot_workspace_counts(conn, [control_workspace]) == control_before


def test_scoped_workspace_project_and_repository_deletion_preserves_siblings_and_configuration(
    conn: Connection[Any],
) -> None:
    """Scoped aggregate deletions remove only the owned subtree, preserve the
    owning Profile, sibling Workspaces, sibling Projects, and Workspace-level
    configuration — and never involve GitHub."""
    profile = _insert_profile(conn)
    main_workspace = _insert_workspace(conn, profile, name="main")
    sibling_workspace = _insert_workspace(conn, profile, name="sibling")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    project_one = _insert_project(conn, main_workspace, name="project-one")
    project_two = _insert_project(conn, main_workspace, name="project-two")
    repository_one = _insert_repository(
        conn,
        project_id=project_one,
        workspace_id=main_workspace,
        github_repository_id=72_000_001,
    )
    repository_two = _insert_repository(
        conn,
        project_id=project_two,
        workspace_id=main_workspace,
        github_repository_id=72_000_002,
    )
    sibling_project = _insert_project(conn, sibling_workspace, name="sibling-project")
    sibling_repository = _insert_repository(
        conn,
        project_id=sibling_project,
        workspace_id=sibling_workspace,
        github_repository_id=72_000_009,
    )
    connection = connection_repositories.create_connection(
        pool,
        workspace_id=main_workspace,
        adapter=AdapterType.CLINE,
        name="main-hub",
        auth_reference="auth-ref-main",
    )
    connection_repositories.set_role_binding(
        pool,
        workspace_id=main_workspace,
        role=WorkflowRole.PRODUCER,
        connection_id=connection.id,
    )
    event_repositories.record_workflow_event(
        pool,
        workspace_id=main_workspace,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
    )
    task_one = _populate_light_task(
        pool,
        workspace_id=main_workspace,
        repository_id=repository_one,
        connection_id=connection.id,
        github_issue_id=72_001,
        github_issue_number=1,
    )
    task_two = _populate_light_task(
        pool,
        workspace_id=main_workspace,
        repository_id=repository_two,
        connection_id=connection.id,
        github_issue_id=72_002,
        github_issue_number=2,
    )
    sibling_connection = connection_repositories.create_connection(
        pool,
        workspace_id=sibling_workspace,
        adapter=AdapterType.CLINE,
        name="sibling-hub",
        auth_reference="auth-ref-sibling",
    )
    sibling_task = _populate_light_task(
        pool,
        workspace_id=sibling_workspace,
        repository_id=sibling_repository,
        connection_id=sibling_connection.id,
        github_issue_id=72_901,
        github_issue_number=901,
    )

    workspace_before = _snapshot_workspace_counts(conn, [main_workspace])
    sibling_before = _snapshot_workspace_counts(conn, [sibling_workspace])
    assert workspace_before["tasks"] == 2

    # Deleting one Project removes only its Repository/Task subtree.
    deleted_project = deletion_repositories.delete_project(pool, project_one)
    assert deleted_project is not None and deleted_project.id == project_one
    _assert_settled(conn)
    assert _count_rows(conn, "repositories", "id", repository_one) == 0
    assert _count_rows(conn, "tasks", "id", task_one.id) == 0
    assert _count_rows(conn, "plan_revisions", "task_id", task_one.id) == 0
    assert _count_rows(conn, "projects", "id", project_two) == 1
    assert _count_rows(conn, "repositories", "id", repository_two) == 1
    assert _count_rows(conn, "tasks", "id", task_two.id) == 1
    assert _count_rows(conn, "connections", "id", connection.id) == 1
    assert _count_rows(conn, "workflow_role_bindings", "connection_id", connection.id) == 1

    # Deleting one Repository mapping removes only its OpenOrc Task subtree —
    # OpenOrc's mapping/state for the external GitHub repository only.
    deleted_repository = deletion_repositories.delete_repository(pool, repository_two)
    assert deleted_repository is not None and deleted_repository.id == repository_two
    _assert_settled(conn)
    assert _count_rows(conn, "tasks", "id", task_two.id) == 0
    assert _count_rows(conn, "task_agent_sessions", "task_id", task_two.id) == 0
    assert _count_rows(conn, "projects", "id", project_two) == 1
    assert _count_rows(conn, "connections", "id", connection.id) == 1

    # Deleting the Workspace removes everything exclusively owned by it,
    # including its Connections, bindings, overrides, and events.
    deleted_workspace = deletion_repositories.delete_workspace(pool, main_workspace)
    assert deleted_workspace is not None and deleted_workspace.id == main_workspace
    _assert_settled(conn)
    for table, count in _snapshot_workspace_counts(conn, [main_workspace]).items():
        assert count == 0, f"{table} retained Workspace-owned rows"
    # The owning Profile, the sibling Workspace, and all of its data remain.
    assert _count_rows(conn, "workspaces", "owner_profile_id", profile) == 1
    assert _snapshot_workspace_counts(conn, [sibling_workspace]) == sibling_before
    assert _count_rows(conn, "tasks", "id", sibling_task.id) == 1


def test_purging_an_archived_task_removes_its_complete_aggregate(
    conn: Connection[Any],
) -> None:
    """Purge removes the archived Task and every OpenOrc-owned Task-owned
    descendant; sibling Tasks and the surrounding aggregates stay; current
    (non-archived) Tasks are rejected; GitHub is untouched."""
    profile = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn,
        project_id=project_id,
        workspace_id=workspace_id,
        github_repository_id=73_000_001,
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    connection = connection_repositories.create_connection(
        pool,
        workspace_id=workspace_id,
        adapter=AdapterType.CLINE,
        name="hub",
        auth_reference="auth-ref-purge",
    )
    task = _populate_full_task_aggregate(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        connection_id=connection.id,
        github_issue_id=73_001,
        github_issue_number=1,
    )
    sibling = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=73_002,
        github_issue_number=2,
    )

    # A current (non-archived) Task is never purgeable through this
    # primitive: the normal active lifecycle cancels first.
    with pytest.raises(TaskDomainError):
        deletion_repositories.purge_archived_task(pool, task.id)

    archived = task_repositories.archive_task(
        pool,
        task.id,
        expected_state_token=task.state_token,
        terminal_status=TaskStatus.COMPLETED,
    )
    assert archived is not None and archived.archived_at is not None

    purged = deletion_repositories.purge_archived_task(pool, task.id)
    assert purged is not None and purged.id == task.id
    _assert_settled(conn)
    for table in _TASK_OWNED_TABLES:
        assert _count_rows(conn, table, "task_id", task.id) == 0, (
            f"{table} retained purged-Task rows"
        )
    assert _count_rows(conn, "workflow_events", "task_id", task.id) == 0
    assert _count_rows(conn, "tasks", "id", task.id) == 0
    # Sibling Task, Repository mapping, Project, Workspace, and Connection
    # remain untouched.
    assert _count_rows(conn, "tasks", "id", sibling.id) == 1
    assert _count_rows(conn, "repositories", "id", repository_id) == 1
    assert _count_rows(conn, "connections", "id", connection.id) == 1
    # Purging again finds nothing; the current sibling stays rejected.
    assert deletion_repositories.purge_archived_task(pool, task.id) is None
    with pytest.raises(TaskDomainError):
        deletion_repositories.purge_archived_task(pool, sibling.id)


def test_connection_deletion_is_restricted_and_disconnect_is_explicit(
    conn: Connection[Any],
) -> None:
    """Connection hard deletion is blocked while historical session/bindings
    reference it (never cascading through history); disconnect is the explicit
    atomic revoke; an unreferenced Connection is deletable."""
    profile = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn,
        project_id=project_id,
        workspace_id=workspace_id,
        github_repository_id=74_000_001,
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    connection = connection_repositories.create_connection(
        pool,
        workspace_id=workspace_id,
        adapter=AdapterType.CLINE,
        name="referenced-hub",
        auth_reference="auth-ref-referenced",
    )
    connection_repositories.set_role_binding(
        pool, workspace_id=workspace_id, role=WorkflowRole.PRODUCER, connection_id=connection.id
    )
    task = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=74_001,
        github_issue_number=1,
    )
    session_repositories.ensure_task_agent_session(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection.id,
    )
    archived = task_repositories.archive_task(
        pool,
        task.id,
        expected_state_token=task.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert archived is not None

    # Hard deletion is rejected: historical TaskAgentSession evidence and the
    # role binding reference the Connection, and the Connection-reference
    # foreign keys never cascade through that history. The repository
    # operation forces the deferred Connection-reference constraints
    # IMMEDIATE inside its own transaction, so the rejection is deterministic
    # before the operation returns.
    with pytest.raises(ForeignKeyViolation):
        deletion_repositories.delete_connection(pool, connection.id)
    assert _count_rows(conn, "task_agent_sessions", "connection_id", connection.id) == 1
    assert _count_rows(conn, "workflow_role_bindings", "connection_id", connection.id) == 1
    assert connection_repositories.get_connection(pool, connection.id) is not None

    # Disconnect is the explicit atomic revoke: enabled=false and
    # auth_reference=null together; row identity, bindings, and history are
    # preserved.
    disconnected = deletion_repositories.disconnect_connection(pool, connection.id)
    assert disconnected is not None
    assert disconnected.id == connection.id
    assert disconnected.enabled is False
    assert disconnected.auth_reference is None
    assert (
        len(connection_repositories.list_connection_bindings(pool, connection_id=connection.id))
        == 1
    )
    assert _count_rows(conn, "task_agent_sessions", "connection_id", connection.id) == 1

    # Still referenced: hard deletion remains blocked after disconnecting.
    with pytest.raises(ForeignKeyViolation):
        deletion_repositories.delete_connection(pool, connection.id)

    # An unreferenced (never-used) Connection is deletable: the restriction
    # is FK-driven, not a blanket ban.
    fresh = connection_repositories.create_connection(
        pool, workspace_id=workspace_id, adapter=AdapterType.CLINE, name="fresh"
    )
    deleted = deletion_repositories.delete_connection(pool, fresh.id)
    assert deleted is not None and deleted.id == fresh.id
    _assert_settled(conn)
