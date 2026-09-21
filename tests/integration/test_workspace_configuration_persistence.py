"""Integration-marked tests for Workspace authorization and configuration (issue #53).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
Workspace settings invariants directly: the additive migration's typed
defaults (review_iteration_limit 5, blank guidance), durable positivity
constraint, guidance round-trips, ReviewLoop historical-limit retention with
the Workspace setting supplying future loops, and the service-level
authorization boundary against real data. They are excluded from the
ordinary deterministic baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_workspace_configuration_persistence.py

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
from psycopg.errors import CheckViolation

from openorc.domain.connections import WorkflowRole
from openorc.domain.ownership import GitHubRepositoryIdentity
from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewLoopPurpose,
)
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import workspace_configuration
from openorc.services.errors import NotFoundError
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_task_review_loop,
    require_workspace_project,
)

pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _pool(conn: Connection[Any]) -> DatabasePool:
    return cast(DatabasePool, _SingleConnectionPool(conn))


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE (issue #27).
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
        "html_url, is_private, default_branch) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            4000,
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
        "insert into openorc.tasks (id, workspace_id, repository_id, github_issue_id, "
        "github_issue_number, status) values (%s, %s, %s, %s, %s, 'ready_to_plan')",
        (task_id, workspace_id, repository_id, 4000, 12),
    )
    return task_id


def test_fresh_workspaces_carry_the_configured_boundary_defaults(
    conn: Connection[Any],
) -> None:
    pool = _pool(conn)

    workspace = ownership_repositories.create_workspace(
        pool, owner_profile_id=_insert_profile(conn), name="defaults"
    )

    assert workspace.review_iteration_limit == DEFAULT_REVIEW_LOOP_ITERATION_LIMIT == 5
    assert workspace.guidance == ""

    # A raw insert omitting both columns receives the same durable defaults.
    other_profile = _insert_profile(conn)
    other_workspace_id = uuid.uuid4()
    row = conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) "
        "values (%s, %s, 'raw') returning review_iteration_limit, guidance",
        (other_workspace_id, other_profile),
    ).fetchone()
    assert row is not None
    assert row[0] == DEFAULT_REVIEW_LOOP_ITERATION_LIMIT
    assert row[1] == ""


def test_configuration_updates_round_trip_and_durable_constraint_holds(
    conn: Connection[Any],
) -> None:
    pool = _pool(conn)
    workspace = ownership_repositories.create_workspace(
        pool, owner_profile_id=_insert_profile(conn), name="config"
    )

    # Review-limit update: exact before/after facts, written on change only.
    limit_result = ownership_repositories.update_workspace_review_iteration_limit(
        pool, workspace.id, review_iteration_limit=7
    )
    assert limit_result is not None
    workspace, previous, changed = limit_result
    assert changed is True
    assert previous == DEFAULT_REVIEW_LOOP_ITERATION_LIMIT
    assert workspace.review_iteration_limit == 7

    no_op_result = ownership_repositories.update_workspace_review_iteration_limit(
        pool, workspace.id, review_iteration_limit=7
    )
    assert no_op_result is not None
    _, previous, changed = no_op_result
    assert changed is False
    assert previous == 7

    # Guidance: arbitrary Owner-authored prose round-trips verbatim as the
    # single current value; resetting to blank is itself a change.
    prose = "Always re-run the full suite.\nΟἶναι νόμοι — 多相. ✔\n   "
    guidance_result = ownership_repositories.update_workspace_guidance(
        pool, workspace.id, guidance=prose
    )
    assert guidance_result is not None
    workspace, previous, changed = guidance_result
    assert changed is True
    assert previous == ""
    reloaded = ownership_repositories.get_workspace(pool, workspace.id)
    assert reloaded is not None
    assert reloaded.guidance == prose
    reset_result = ownership_repositories.update_workspace_guidance(
        pool, workspace.id, guidance=""
    )
    assert reset_result is not None
    workspace, previous, changed = reset_result
    assert changed is True
    assert previous == prose
    reloaded = ownership_repositories.get_workspace(pool, workspace.id)
    assert reloaded is not None
    assert reloaded.guidance == ""

    # The durable positive-integer CHECK backstops the domain validation;
    # each violation aborts only its own savepoint inside the test
    # transaction.
    for bad_limit in (0, -3):
        with pytest.raises(CheckViolation), conn.transaction():
            conn.execute(
                "update openorc.workspaces set review_iteration_limit = %s where id = %s",
                (bad_limit, workspace.id),
            )
    # The committed-in-test value is untouched by the aborted savepoints.
    reloaded = ownership_repositories.get_workspace(pool, workspace.id)
    assert reloaded is not None
    assert reloaded.review_iteration_limit == 7


def test_existing_review_loops_keep_their_historical_effective_limit(
    conn: Connection[Any],
) -> None:
    pool = _pool(conn)
    profile_id = _insert_profile(conn)
    workspace = ownership_repositories.create_workspace(
        pool, owner_profile_id=profile_id, name="loops"
    )
    project_id = _insert_project(conn, workspace.id)
    repository_id = _insert_repository(conn, project_id=project_id, workspace_id=workspace.id)
    task_id = _insert_task(conn, workspace_id=workspace.id, repository_id=repository_id)

    # A loop created under the configured-boundary default stores its own
    # effective limit.
    historical_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace.id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    assert historical_loop.iteration_limit == 5

    # Changing the Workspace setting never rewrites historical loops.
    change_result = ownership_repositories.update_workspace_review_iteration_limit(
        pool, workspace.id, review_iteration_limit=7
    )
    assert change_result is not None
    workspace, previous, changed = change_result
    assert changed is True
    assert previous == 5

    retained = review_repositories.get_review_loop(pool, review_loop_id=historical_loop.id)
    assert retained is not None
    assert retained.iteration_limit == 5

    # A newly created loop is supplied the updated Workspace value.
    fresh_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace.id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=7,
    )
    persisted = review_repositories.get_review_loop(pool, review_loop_id=fresh_loop.id)
    assert persisted is not None
    assert persisted.iteration_limit == 7


def test_service_authorization_enforces_workspace_isolation_on_real_data(
    conn: Connection[Any],
) -> None:
    from openorc.persistence.connections import set_role_binding
    from openorc.services.workspace_authorization import require_workspace_repository

    pool = _pool(conn)
    owner_profile = _insert_profile(conn)
    intruder_profile = _insert_profile(conn)
    owner_workspace = ownership_repositories.create_workspace(
        pool, owner_profile_id=owner_profile, name="owner-workspace"
    )
    intruder_workspace = ownership_repositories.create_workspace(
        pool, owner_profile_id=intruder_profile, name="intruder-workspace"
    )
    project_id = _insert_project(conn, owner_workspace.id)
    repository_id = _insert_repository(conn, project_id=project_id, workspace_id=owner_workspace.id)
    task_id = _insert_task(conn, workspace_id=owner_workspace.id, repository_id=repository_id)
    historical_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=owner_workspace.id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
    )

    # The owner resolves and operates on their Workspace.
    resolved = require_profile_workspace(
        pool, profile_id=owner_profile, workspace_id=owner_workspace.id
    )
    assert resolved.id == owner_workspace.id
    assert (
        require_workspace_project(
            pool, profile_id=owner_profile, workspace_id=owner_workspace.id, project_id=project_id
        ).id
        == project_id
    )
    assert (
        require_task_review_loop(
            pool,
            profile_id=owner_profile,
            workspace_id=owner_workspace.id,
            review_loop_id=historical_loop.id,
        ).id
        == historical_loop.id
    )

    # Knowing exact internal UUIDs is not access: the intruder is uniformly
    # refused for the Workspace, its Projects, its Task-owned records, and
    # its configuration mutations.
    for operation in (
        lambda: require_profile_workspace(
            pool, profile_id=intruder_profile, workspace_id=owner_workspace.id
        ),
        lambda: require_workspace_project(
            pool,
            profile_id=intruder_profile,
            workspace_id=intruder_workspace.id,
            project_id=project_id,
        ),
        lambda: require_task_review_loop(
            pool,
            profile_id=intruder_profile,
            workspace_id=intruder_workspace.id,
            review_loop_id=historical_loop.id,
        ),
        lambda: workspace_configuration.set_review_iteration_limit(
            pool,
            profile_id=intruder_profile,
            workspace_id=owner_workspace.id,
            review_iteration_limit=99,
        ),
    ):
        with pytest.raises(NotFoundError):
            operation()

    # The denied configuration mutation changed nothing.
    unchanged = ownership_repositories.get_workspace(pool, owner_workspace.id)
    assert unchanged is not None
    assert unchanged.review_iteration_limit == 5

    # GitHub presentation metadata is never authorization input: resolution
    # is driven by the authenticated Profile UUID alone, not owner_login.
    repository = require_workspace_repository(
        pool,
        profile_id=owner_profile,
        workspace_id=owner_workspace.id,
        repository_id=repository_id,
    )
    assert repository.metadata.owner_login == "octocat"
    assert repository.identity == GitHubRepositoryIdentity(github_repository_id=4000)

    # The role-binding path composes with the same boundary.
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, safe_config, "
        "session_capacity, enabled) values (%s, %s, 'cline', 'route', '{}', 1, true)",
        (connection_id, owner_workspace.id),
    )
    set_role_binding(
        pool,
        workspace_id=owner_workspace.id,
        role=WorkflowRole.PRODUCER,
        connection_id=connection_id,
    )
    assert (
        require_profile_workspace(
            pool, profile_id=owner_profile, workspace_id=owner_workspace.id
        ).id
        == owner_workspace.id
    )
