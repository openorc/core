"""Integration-marked persistence tests for workflow events (#26, #100).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
invariants directly: the WorkflowEvent locked CHECK vocabularies (the actor
vocabulary uses owner — a ``human`` actor is rejected by the database, and
the event-type CHECK matches the live Python enum after the corrective
removal migration), direct Workspace scope with optional agreeing Task
scope (cross-Workspace Task attachment rejected through the composite
foreign key), the pair-shaped subject reference, canonical JSON-object
context round-trips, and the index-matching read paths. They are excluded
from the ordinary deterministic baseline by the repository pytest
configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_workflow_event_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and
outside agent responsibility: in the normal Owner local-development flow
the target is the ephemeral non-production Supabase branch that the
Owner-only ``devserver.sh`` command creates and later deletes (agents
never invoke it). The session fixture merely resets the ``openorc``
schema and applies the committed migrations from scratch within the
supplied database, and the suite skips cleanly when
``OPENORC_TEST_DATABASE_URL`` is absent.

Append-only for this issue is the persistence-surface contract
(INSERT-only write path, no UPDATE/DELETE/upsert API). These tests do
not assert that arbitrary raw SQL UPDATE/DELETE against the table fails:
Postgres itself is not the enforcement point, and the module-surface
contract is proven by the ordinary suite.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation

from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.persistence import events as event_repositories
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

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
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): every Profile needs its
    # backing Auth user row. The inserts roll back with the test transaction.
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


def _insert_project(conn: Connection[Any], workspace_id: uuid.UUID) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    return project_id


def _insert_repository(
    conn: Connection[Any], *, project_id: uuid.UUID, workspace_id: uuid.UUID
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
            70_200_001,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan')",
        (task_id, workspace_id, repository_id, 7601, 200),
    )
    return task_id


def _fresh_task(
    conn: Connection[Any],
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, DatabasePool]:
    """One ownership chain, one Task, and its pool; returns (ws, repo, task, pool)."""
    workspace_id, pool = _fresh_workspace(conn)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(conn, project_id=project_id, workspace_id=workspace_id)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    return workspace_id, repository_id, task_id, pool


def _fresh_workspace(conn: Connection[Any]) -> tuple[uuid.UUID, DatabasePool]:
    """One Profile -> Workspace chain and its pool; returns (workspace, pool)."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    return workspace_id, cast(DatabasePool, _SingleConnectionPool(conn))


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def test_task_and_workspace_events_flow_through_the_read_paths(conn: Connection[Any]) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    gate_subject_id = uuid.uuid4()

    workspace_event = event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.OWNER_GATE_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
        actor_id=str(uuid.uuid4()),
        subject_type="owner_gate",
        subject_id=gate_subject_id,
        context={"gate_kind": "plan_approval"},
    )
    task_event = event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.EXECUTION_STARTED,
        actor_type=WorkflowEventActor.PRODUCER,
    )

    # Every event carries direct Workspace scope; the Workspace-level
    # event legitimately has no Task.
    assert workspace_event.workspace_id == task_event.workspace_id == workspace_id
    assert workspace_event.task_id is None
    assert task_event.task_id == task_id

    # The index-matched read paths reach both events. Ordering follows the
    # locked read contract (``created_at desc, id desc``), and this
    # fixture's nested savepoints share one transaction timestamp — so
    # tied ``created_at`` values order by event id, never by insertion
    # order. The expectation is computed from the contract, never from
    # insert chronology.
    expected_recent = sorted(
        [workspace_event, task_event],
        key=lambda event: (event.created_at, event.id),
        reverse=True,
    )
    recent = event_repositories.list_recent_workspace_events(
        pool, workspace_id=workspace_id, limit=10
    )
    assert [e.id for e in recent] == [e.id for e in expected_recent]
    task_history = event_repositories.list_task_events(pool, task_id=task_id, limit=10)
    assert [e.id for e in task_history] == [task_event.id]
    by_type = event_repositories.list_recent_events_by_type(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.OWNER_GATE_CREATED,
        limit=10,
    )
    assert [e.id for e in by_type] == [workspace_event.id]
    by_actor = event_repositories.list_recent_events_by_actor(
        pool, workspace_id=workspace_id, actor_type=WorkflowEventActor.PRODUCER, limit=10
    )
    assert [e.id for e in by_actor] == [task_event.id]
    by_subject = event_repositories.find_events_by_subject(
        pool,
        workspace_id=workspace_id,
        subject_type="owner_gate",
        subject_id=gate_subject_id,
        limit=10,
    )
    assert [e.id for e in by_subject] == [workspace_event.id]


def test_a_task_related_event_cannot_carry_disagreeing_scope(conn: Connection[Any]) -> None:
    workspace_a, _repository_a, task_a, pool_a = _fresh_task(conn)
    workspace_b, pool_b = _fresh_workspace(conn)

    # The same Workspace cannot attach a foreign Task to its events: the
    # composite foreign key rejects the disagreeing scope.
    with pytest.raises(ForeignKeyViolation):
        event_repositories.record_workflow_event(
            pool_b,
            workspace_id=workspace_b,
            task_id=task_a,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=WorkflowEventActor.OPENORC,
        )
    # The legitimate task-scoped event still works on its own Workspace.
    event = event_repositories.record_workflow_event(
        pool_a,
        workspace_id=workspace_a,
        task_id=task_a,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
    )
    assert event.task_id == task_a


def test_the_database_actor_vocabulary_uses_owner_and_rejects_human(
    conn: Connection[Any],
) -> None:
    workspace_id, pool = _fresh_workspace(conn)

    # OWNER is the human-authority actor and persists cleanly.
    event = event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.IMPLEMENTATION_AUTHORIZED,
        actor_type=WorkflowEventActor.OWNER,
    )
    assert event.actor_type is WorkflowEventActor.OWNER

    # HUMAN is not an OpenOrc actor anywhere: the CHECK rejects it at the
    # database, and the domain vocabulary cannot express it.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.workflow_events "
            "(workspace_id, event_type, actor_type) values (%s, %s, %s)",
            (workspace_id, "task_created", "human"),
        )


def test_the_event_type_and_subject_pair_checks_reject_invalid_rows(
    conn: Connection[Any],
) -> None:
    workspace_id, pool = _fresh_workspace(conn)
    event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
    )

    # The excluded CRUD-ish event names are not in the locked vocabulary.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.workflow_events "
            "(workspace_id, event_type, actor_type) values (%s, %s, %s)",
            (workspace_id, "task_archived", "openorc"),
        )

    # The generic subject reference is a pair: half a pair is rejected.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.workflow_events "
            "(workspace_id, event_type, actor_type, subject_type) values (%s, %s, %s, %s)",
            (workspace_id, "task_created", "openorc", "task"),
        )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where workspace_id = %s",
            (workspace_id,),
        )
        == 1
    )


def test_event_context_round_trips_as_a_canonical_json_object(conn: Connection[Any]) -> None:
    workspace_id, pool = _fresh_workspace(conn)
    event = event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.OWNER_GATE_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
        subject_type="owner_gate",
        subject_id=uuid.uuid4(),
        context={"gate_kind": "plan_approval", "note": None},
    )
    reread = event_repositories.get_workflow_event(pool, workflow_event_id=event.id)
    assert reread is not None
    assert reread.context == event.context
    assert reread.context["gate_kind"] == "plan_approval"
    assert reread.context["note"] is None
    assert reread.subject_type == "owner_gate"
