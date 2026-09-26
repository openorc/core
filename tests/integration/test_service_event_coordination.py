"""Integration-marked service tests for WorkflowEvent coordination (issue #56).

Real-database coverage for exactly what the deterministic service fakes
cannot prove: the migrated ``workflow_events`` event-type CHECK (the
demonstrated ``workspace_configuration_changed`` extension is accepted;
unknown and removed vocabulary values are rejected), the real atomic
composition of a canonical mutation with its promised event (a failed event
insertion rolls the paired workspace mutation back), and the service-level
event facts committed by the coordinated #53/#54 mutations — including that
guidance prose never reaches the durable event and that stale/no-op paths
and the deliberately non-evented mutations write no events at all.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_service_event_coordination.py

The suite consumes the database it is given and never provisions one;
provisioning and teardown sit outside the test suite and outside agent
responsibility (Owner-only devserver --testdb flow).
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation

from openorc.domain.events import WorkflowEventType
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence import events as event_repositories
from openorc.persistence import gates as gate_repositories
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import task_mutations, workspace_configuration
from openorc.services.errors import StaleOperationError
from openorc.services.event_coordination import owner_actor
from openorc.services.transaction_composition import composed_transaction

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


class _BorrowedConnectionPool:
    """Borrows the fixture-owned connection; adds no transaction of its own.

    The fixture owns the connection and its implicit per-test transaction.
    ``composed_transaction`` establishes the only composition-wide
    transaction (a SAVEPOINT over that transaction), which is exactly what
    makes the all-or-nothing assertions below meaningful: a failed event
    insertion rolls back to the composition's savepoint, the per-test
    transaction stays valid, and the assertions observe that neither the
    mutation nor the event survived.
    """

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    @contextmanager
    def connection(self) -> Any:
        yield self._connection

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _pool(conn: Connection[Any]) -> DatabasePool:
    return cast(DatabasePool, _BorrowedConnectionPool(conn))


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE (issue #27).
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
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _ownership_chain(
    conn: Connection[Any], *, github_repository_id: int = 60_000_001
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """Create Profile -> Workspace -> Project -> Repository.

    Returns ``(profile_id, workspace_id, repository_id)``.
    """
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn,
        project_id=project_id,
        workspace_id=workspace_id,
        github_repository_id=github_repository_id,
    )
    return profile_id, workspace_id, repository_id


def _create_task(
    pool: DatabasePool,
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int = 9001,
) -> Any:
    return task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
        github_issue_number=42,
        source_requirements_fingerprint="a" * 64,
    )


def _workspace_events(pool: DatabasePool, workspace_id: uuid.UUID) -> list[Any]:
    return event_repositories.list_recent_workspace_events(
        pool, workspace_id=workspace_id, limit=50
    )


def test_workspace_configuration_changed_value_round_trips_the_migrated_check(
    conn: Connection[Any],
) -> None:
    """The demonstrated #56 vocabulary extension is accepted by the real
    migrated CHECK and round-trips as an immutable audit fact."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    pool = _pool(conn)

    recorded = event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
        actor_type=owner_actor(profile_id).actor_type,
        actor_id=owner_actor(profile_id).actor_id,
        subject_type="workspace",
        subject_id=workspace_id,
        context={"setting": "review_iteration_limit", "previous": 5, "new": 7},
    )
    reloaded = event_repositories.get_workflow_event(pool, workflow_event_id=recorded.id)
    assert reloaded is not None
    assert reloaded.event_type is WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED
    assert reloaded.task_id is None
    assert reloaded.actor_id == str(profile_id)
    assert reloaded.subject_type == "workspace"
    assert reloaded.subject_id == workspace_id
    assert dict(reloaded.context) == {
        "setting": "review_iteration_limit",
        "previous": 5,
        "new": 7,
    }


def test_the_migrated_check_rejects_removed_and_unknown_event_types(
    conn: Connection[Any],
) -> None:
    """The additive migration kept the #100 narrowed vocabulary: the removed
    prompt-override value and any unknown value are rejected by the database.

    Each expected violation runs inside its own ``conn.transaction()``
    SAVEPOINT: a constraint violation aborts the surrounding transaction
    block it occurred in, so isolating every rejected insert in its own
    savepoint keeps the per-test transaction valid for the next iteration.
    """
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    for rejected in ("prompt_override_changed", "not_a_real_event_type"):
        # Nesting order is load-bearing: psycopg's transaction manager must
        # be the INNER context so its __exit__ sees the CheckViolation and
        # rolls back the SAVEPOINT first; pytest.raises (outer) then consumes
        # the re-raised exception. The reverse order (or a combined `with`)
        # would let pytest.raises consume the exception first, leaving
        # psycopg to RELEASE a savepoint in an aborted transaction state
        # (InFailedSqlTransaction). The noqa keeps ruff's SIM117 from
        # suggesting exactly that wrong combined form.
        with pytest.raises(CheckViolation):  # noqa: SIM117
            with conn.transaction():
                conn.execute(
                    "insert into openorc.workflow_events "
                    "(workspace_id, event_type, actor_type, context) "
                    "values (%s, %s, %s, '{}'::jsonb)",
                    (workspace_id, rejected, "owner"),
                )


def test_set_review_iteration_limit_commits_the_mutation_and_its_event_together(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    pool = _pool(conn)

    result = workspace_configuration.set_review_iteration_limit(
        pool, profile_id=profile_id, workspace_id=workspace_id, review_iteration_limit=7
    )
    assert result.changed is True

    workspace = ownership_repositories.get_workspace(pool, workspace_id=workspace_id)
    assert workspace is not None
    assert workspace.review_iteration_limit == 7
    events = _workspace_events(pool, workspace_id)
    assert len(events) == 1
    event = events[0]
    assert event.event_type is WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED
    assert event.task_id is None
    assert event.actor_type.value == "owner"
    assert event.actor_id == str(profile_id)
    assert event.subject_type == "workspace"
    assert event.subject_id == workspace_id
    assert dict(event.context) == {"setting": "review_iteration_limit", "previous": 5, "new": 7}

    # A same-value write is a no-op and records no further event.
    noop = workspace_configuration.set_review_iteration_limit(
        pool, profile_id=profile_id, workspace_id=workspace_id, review_iteration_limit=7
    )
    assert noop.changed is False
    assert len(_workspace_events(pool, workspace_id)) == 1


def test_guidance_change_records_the_setting_fact_without_the_prose(
    conn: Connection[Any],
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    pool = _pool(conn)
    prose = "Always re-run the full suite before dispatching the Reviewer.\n第二段落。"

    result = workspace_configuration.set_guidance(
        pool, profile_id=profile_id, workspace_id=workspace_id, guidance=prose
    )
    assert result.changed is True

    workspace = ownership_repositories.get_workspace(pool, workspace_id=workspace_id)
    assert workspace is not None
    assert workspace.guidance == prose
    events = _workspace_events(pool, workspace_id)
    assert len(events) == 1
    event = events[0]
    assert event.event_type is WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED
    # The event identifies only the guidance setting change: the Owner's
    # prose never reaches the durable event context.
    assert dict(event.context) == {"setting": "guidance"}
    assert prose not in repr(event.context)

    # Resetting to blank records the same semantic fact, never a prose delta.
    reset = workspace_configuration.set_guidance(
        pool, profile_id=profile_id, workspace_id=workspace_id, guidance=""
    )
    assert reset.changed is True
    events = _workspace_events(pool, workspace_id)
    assert len(events) == 2
    assert dict(events[0].context) == {"setting": "guidance"}
    assert prose not in repr(events[0].context)


def test_failed_event_insertion_rolls_back_the_paired_workspace_mutation(
    conn: Connection[Any],
) -> None:
    """The real all-or-nothing proof: one composed transaction updates the
    Workspace and inserts its event; a scope-violating event insert raises
    the driver exception and the whole composition rolls back together."""
    profile_id, workspace_id, _ = _ownership_chain(conn)
    # A Task of a DIFFERENT Workspace: the (task_id, workspace_id) scope
    # agreement composite foreign key rejects the event at statement time.
    other_profile_id = _insert_profile(conn)
    other_workspace_id = _insert_workspace(conn, other_profile_id)
    other_project_id = _insert_project(conn, other_workspace_id)
    other_repository_id = _insert_repository(
        conn,
        project_id=other_project_id,
        workspace_id=other_workspace_id,
        github_repository_id=60_000_002,
    )
    pool = _pool(conn)
    foreign_task = _create_task(
        pool, workspace_id=other_workspace_id, repository_id=other_repository_id
    )

    with pytest.raises(ForeignKeyViolation), composed_transaction(pool) as transaction_pool:
        updated = ownership_repositories.update_workspace_review_iteration_limit(
            transaction_pool, workspace_id, review_iteration_limit=7
        )
        assert updated is not None
        event_repositories.record_workflow_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=foreign_task.id,
            event_type=WorkflowEventType.TASK_CANCELLED,
            actor_type=owner_actor(profile_id).actor_type,
            actor_id=owner_actor(profile_id).actor_id,
        )

    # Neither side of the composition survived: the Workspace keeps its
    # previous limit and no event row exists for it.
    workspace = ownership_repositories.get_workspace(pool, workspace_id=workspace_id)
    assert workspace is not None
    assert workspace.review_iteration_limit == 5
    assert _workspace_events(pool, workspace_id) == []
    # The per-test transaction stayed valid after the savepoint rollback.
    unchanged = workspace_configuration.set_review_iteration_limit(
        pool, profile_id=profile_id, workspace_id=workspace_id, review_iteration_limit=7
    )
    assert unchanged.changed is True
    assert len(_workspace_events(pool, workspace_id)) == 1


def test_task_archival_commits_its_terminal_event_and_stale_retries_write_none(
    conn: Connection[Any],
) -> None:
    profile_id, workspace_id, repository_id = _ownership_chain(conn)
    pool = _pool(conn)
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)

    archived = task_mutations.archive_task(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=task.state_token,
        terminal_status=TaskStatus.CANCELLED,
        actor=owner_actor(profile_id),
    )
    assert archived.status is TaskStatus.CANCELLED
    assert archived.archived_at is not None

    events = event_repositories.list_task_events(pool, task_id=task.id, limit=50)
    assert len(events) == 1
    event = events[0]
    assert event.event_type is WorkflowEventType.TASK_CANCELLED
    assert event.task_id == task.id
    assert event.actor_type.value == "owner"
    assert event.actor_id == str(profile_id)

    # A stale re-archive (the pre-rotation token) applies nothing and
    # records no further event.
    with pytest.raises(StaleOperationError):
        task_mutations.archive_task(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=task.state_token,
            terminal_status=TaskStatus.COMPLETED,
            actor=owner_actor(profile_id),
        )
    assert len(event_repositories.list_task_events(pool, task_id=task.id, limit=50)) == 1

    # Completion maps to its own terminal event type.
    completed_profile_id = _insert_profile(conn)
    completed_workspace_id = _insert_workspace(conn, completed_profile_id)
    completed_project_id = _insert_project(conn, completed_workspace_id)
    completed_repository_id = _insert_repository(
        conn,
        project_id=completed_project_id,
        workspace_id=completed_workspace_id,
        github_repository_id=60_000_003,
    )
    completed_task = _create_task(
        pool,
        workspace_id=completed_workspace_id,
        repository_id=completed_repository_id,
        github_issue_id=9002,
    )
    task_mutations.archive_task(
        pool,
        workspace_id=completed_workspace_id,
        task_id=completed_task.id,
        expected_state_token=completed_task.state_token,
        terminal_status=TaskStatus.COMPLETED,
        actor=owner_actor(completed_profile_id),
    )
    completed_events = event_repositories.list_task_events(
        pool, task_id=completed_task.id, limit=50
    )
    assert len(completed_events) == 1
    assert completed_events[0].event_type is WorkflowEventType.TASK_COMPLETED


def test_gate_resolution_commits_its_event_only_for_the_resolved_outcome(
    conn: Connection[Any],
) -> None:
    profile_id, workspace_id, repository_id = _ownership_chain(conn)
    pool = _pool(conn)
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)
    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="a" * 40,
    )
    installed = task_mutations.set_current_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=task.state_token,
        owner_gate_id=gate.id,
    )

    resolution = task_mutations.resolve_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=installed.state_token,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        actor=owner_actor(profile_id),
    )
    assert resolution.gate.status is OwnerGateStatus.APPROVED

    events = event_repositories.list_task_events(pool, task_id=task.id, limit=50)
    assert len(events) == 1
    event = events[0]
    assert event.event_type is WorkflowEventType.OWNER_GATE_RESOLVED
    assert event.task_id == task.id
    assert event.actor_id == str(profile_id)
    assert event.subject_type == "owner_gate"
    assert event.subject_id == gate.id
    assert dict(event.context) == {"outcome": "approved"}

    # A second resolution attempt (already-resolved one-shot fact) is stale
    # and records nothing further.
    with pytest.raises(StaleOperationError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=resolution.task.state_token,
            owner_gate_id=gate.id,
            outcome=OwnerGateStatus.REJECTED,
            actor=owner_actor(profile_id),
        )
    assert len(event_repositories.list_task_events(pool, task_id=task.id, limit=50)) == 1


def test_branch_binding_and_gate_installation_remain_deliberately_non_evented(
    conn: Connection[Any],
) -> None:
    _, workspace_id, repository_id = _ownership_chain(conn)
    pool = _pool(conn)
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)

    bound = task_mutations.bind_canonical_branch(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=task.state_token,
        canonical_feature_branch="feat/producer-branch",
    )
    assert bound.canonical_feature_branch == "feat/producer-branch"

    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="b" * 40,
    )
    task_mutations.set_current_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=bound.state_token,
        owner_gate_id=gate.id,
    )

    # Both successful foundational mutations committed, yet the Workspace
    # carries zero events: they are deliberately non-evented Phase 1 facts.
    assert _workspace_events(pool, workspace_id) == []
