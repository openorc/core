"""Integration-marked persistence tests for plan production (issue #77).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
atomicity of the E2 plan-production composition against a real Postgres:
one successful initial production commits the immutable PlanRevision, the
current-plan pointer installation, the rotated Task token, and the
coordinated ``PLAN_REVISION_CREATED`` event as one transaction; the
E3-directed revision continuation commits the next revision, moves the
pointer, leaves revision 1 intact, and records ``PLAN_REVISION_REVISED``;
and the same composed write set — revision insert, pointer installation,
event recording — rolls back together when a later write in the composition
fails. They are excluded from the ordinary deterministic baseline by the
repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_plan_production_persistence.py

The suite consumes the database it is given and never provisions one; the
session fixture resets the ``openorc`` schema and applies the committed
migrations from scratch. External systems (Agent Runtime, GitHub) are the
deterministic fakes — this suite proves the durable composition, not the
external boundaries (those are the ordinary suite's subject).
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubAppClient,
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewLoopPurpose,
    ReviewOutcome,
)
from openorc.persistence import events as event_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.services.task_plan_production import (
    produce_initial_task_plan,
    revise_task_plan,
)
from openorc.services.task_planning_bootstrap import TaskPlanningPreparationOutcome
from openorc.services.transaction_composition import composed_transaction
from tests.fakes.agent_runtime import FakeAgentRuntimeAdapter, session_ready_candidate

pytestmark = pytest.mark.integration

_BASE_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_EXTERNAL_INSTALLATION_ID = 77_000_001
_GITHUB_REPOSITORY_ID = 77_000_002


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


def _insert_repository(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            _insert_project(conn, workspace_id),
            workspace_id,
            _GITHUB_REPOSITORY_ID,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _insert_installation(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, %s, %s, %s)",
        (
            installation_id,
            workspace_id,
            _EXTERNAL_INSTALLATION_ID,
            77_000_003,
            "octocat",
            "Organization",
        ),
    )
    return installation_id


def _ownership_chain(
    conn: Connection[Any],
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID]:
    """Profile -> Workspace -> Repository(with route) -> Task in planning.

    Returns (workspace_id, repository_id, installation_id, task_id, profile_id).
    """
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    repository_id = _insert_repository(conn, workspace_id=workspace_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    conn.execute(
        "update openorc.repositories set github_installation_id = %s where id = %s",
        (installation_id, repository_id),
    )
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, 7701, 42, "a" * 64),
    )
    return workspace_id, repository_id, installation_id, task_id, profile_id


def _insert_connection(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name) "
        "values (%s, %s, 'cline', 'primary hub')",
        (connection_id, workspace_id),
    )
    return connection_id


def _insert_ready_session(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    connection_id: uuid.UUID,
    role: str,
    external_session_id: str,
) -> uuid.UUID:
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, %s, %s, %s, 'ready', '{}'::jsonb, now())",
        (session_id, workspace_id, task_id, role, connection_id, external_session_id),
    )
    return session_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


class _FakeGitHubClient:
    """Scripted observation fake: the fresh address and the committed base SHA."""

    def __init__(self) -> None:
        self.repository_calls: list[tuple[int, int]] = []
        self.branch_calls: list[tuple[int, str, str, str]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        self.repository_calls.append((github_installation_id, github_repository_id))
        return GitHubRepositoryObservation(
            github_repository_id=github_repository_id,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        self.branch_calls.append(
            (github_installation_id, owner_login, repository_name, branch_name)
        )
        return GitHubBranchObservation(branch_name=branch_name, head_sha=_BASE_SHA)


def _prepared_producer_adapter() -> tuple[FakeAgentRuntimeAdapter, str]:
    """One live fake Producer session, as the E1b bootstrap established it."""
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=uuid.UUID(int=0),
            task_id=uuid.UUID(int=0),
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    return adapter, created.external_session_id


def _preparation_for(
    pool: DatabasePool, *, workspace_id: uuid.UUID, task_id: uuid.UUID
) -> TaskPlanningPreparationOutcome:
    """The E1b-shaped handoff built from the durable post-claim facts."""
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    producer = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.PRODUCER)
    reviewer = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    assert producer is not None and reviewer is not None
    return TaskPlanningPreparationOutcome(
        task=task,
        producer_session=producer,
        reviewer_session=reviewer,
        producer_checkout_directory=f"/srv/repositories/{task_id}",
    )


def _planning_fixture(
    conn: Connection[Any],
) -> tuple[DatabasePool, FakeAgentRuntimeAdapter, _FakeGitHubClient, str, dict[str, Any]]:
    """The full E2-ready durable fixture: ownership chain, READY bindings,
    the live fake Producer session, and the prepared handoff facts."""
    workspace_id, _repository_id, _installation_id, task_id, profile_id = _ownership_chain(conn)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    adapter, producer_external_id = _prepared_producer_adapter()
    _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id=producer_external_id,
    )
    _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="reviewer",
        external_session_id="fake-reviewer-1",
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    preparation = _preparation_for(pool, workspace_id=workspace_id, task_id=task_id)
    return (
        pool,
        adapter,
        _FakeGitHubClient(),
        producer_external_id,
        {
            "workspace_id": workspace_id,
            "task_id": task_id,
            "profile_id": profile_id,
            "preparation": preparation,
        },
    )


def test_initial_production_commits_revision_pointer_and_event_atomically(
    conn: Connection[Any],
) -> None:
    pool, adapter, github, producer_external_id, facts = _planning_fixture(conn)
    adapter.queue_send(
        producer_external_id, {"type": "plan_result", "schema_version": 1, "plan": _PLAN_TEXT}
    )

    outcome = produce_initial_task_plan(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        profile_id=facts["profile_id"],
        preparation=facts["preparation"],
    )

    # The exact typed outcome: revision 1 with the branch-observed base and
    # the post-pointer Task token.
    assert outcome.plan_revision.revision_number == 1
    assert outcome.plan_revision.content == _PLAN_TEXT
    assert outcome.plan_revision.repository_base_sha == _BASE_SHA
    assert outcome.task.current_plan_revision_id == outcome.plan_revision.id
    assert outcome.task.state_token != facts["preparation"].task.state_token

    # The three durable facts committed together, exactly once each.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.plan_revisions where task_id = %s",
            (facts["task_id"],),
        )
        == 1
    )
    reloaded = task_repositories.get_task(pool, facts["task_id"])
    assert reloaded is not None
    assert reloaded.current_plan_revision_id == outcome.plan_revision.id
    assert reloaded.state_token == outcome.task.state_token
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_revision_created' and subject_type = 'plan_revision' "
            "and subject_id = %s and actor_type = 'producer'",
            (facts["task_id"], outcome.plan_revision.id),
        )
        == 1
    )
    # The observation used the routed stable identities.
    assert github.repository_calls == [(_EXTERNAL_INSTALLATION_ID, _GITHUB_REPOSITORY_ID)]


def test_revision_production_commits_next_revision_pointer_and_event(
    conn: Connection[Any],
) -> None:
    pool, adapter, github, producer_external_id, facts = _planning_fixture(conn)
    adapter.queue_send(
        producer_external_id, {"type": "plan_result", "schema_version": 1, "plan": _PLAN_TEXT}
    )
    initial = produce_initial_task_plan(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        profile_id=facts["profile_id"],
        preparation=facts["preparation"],
    )

    # The E3-shaped finalized planning change request against revision 1.
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=initial.plan_revision.id,
    )
    assert iteration is not None
    finalized = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="Needs work",
        findings=[{"summary": "Clarify scope", "details": "Say which module changes."}],
    )
    assert finalized is not None

    adapter.queue_send(
        producer_external_id,
        {"type": "plan_result", "schema_version": 1, "plan": _REVISED_PLAN_TEXT},
    )
    revised = revise_task_plan(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        profile_id=facts["profile_id"],
        expected_state_token=initial.task.state_token,
        prior_plan_revision_id=initial.plan_revision.id,
        review_iteration_id=iteration.id,
    )

    assert revised.plan_revision.revision_number == 2
    assert revised.plan_revision.content == _REVISED_PLAN_TEXT
    assert revised.plan_revision.repository_base_sha == _BASE_SHA
    assert revised.task.current_plan_revision_id == revised.plan_revision.id
    assert revised.task.state_token != initial.task.state_token

    # Revision 1 stays intact; the pointer and the revised event moved on.
    first = planning_repositories.get_plan_revision(pool, plan_revision_id=initial.plan_revision.id)
    assert first is not None
    assert first.revision_number == 1
    assert first.content == _PLAN_TEXT
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_revision_created'",
            (facts["task_id"],),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_revision_revised' and subject_id = %s",
            (facts["task_id"], revised.plan_revision.id),
        )
        == 1
    )


def test_composition_rolls_back_revision_pointer_and_event_together(
    conn: Connection[Any],
) -> None:
    workspace_id, _repository_id, _installation_id, task_id, _profile_id = _ownership_chain(conn)
    # A second Workspace's Task: the event insert's (task_id, workspace_id)
    # pair will not exist, so the composition's LAST write fails.
    foreign_profile = _insert_profile(conn)
    foreign_workspace = _insert_workspace(conn, foreign_profile)
    foreign_repository = _insert_repository(conn, workspace_id=foreign_workspace)
    foreign_task = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (foreign_task, foreign_workspace, foreign_repository, 1, 1, "b" * 64),
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = task_repositories.get_task(pool, task_id)
    assert task is not None

    with pytest.raises(ForeignKeyViolation), composed_transaction(pool) as scoped:
        revision = planning_repositories.create_plan_revision(
            scoped,
            workspace_id=workspace_id,
            task_id=task_id,
            revision_number=1,
            content=_PLAN_TEXT,
            repository_base_sha=_BASE_SHA,
        )
        task_repositories.set_current_plan_revision(
            scoped,
            task_id,
            expected_state_token=task.state_token,
            plan_revision_id=revision.id,
        )
        event_repositories.record_workflow_event(
            scoped,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.PLAN_REVISION_CREATED,
            actor_type=WorkflowEventActor.PRODUCER,
            task_id=foreign_task,
            subject_type="plan_revision",
            subject_id=revision.id,
        )

    # The later failure rolled the earlier writes back together: no revision,
    # no pointer, no event, and the Task token never rotated.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.plan_revisions where task_id = %s",
            (task_id,),
        )
        == 0
    )
    reloaded = task_repositories.get_task(pool, task_id)
    assert reloaded is not None
    assert reloaded.current_plan_revision_id is None
    assert reloaded.state_token == task.state_token
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s",
            (task_id,),
        )
        == 0
    )
