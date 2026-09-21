"""Integration-marked service tests for authoritative Task mutations (issue #54).

Focused real-database coverage for exactly what the deterministic service
fakes and the existing persistence integration suites cannot prove: the
service-layer translation of actual conditional-write ``None`` outcomes
against real SQL. Within the per-test-transaction isolation model a
concurrently-committed state change is not observable, so stale-token
rejections surface through the currentness guard (also proven here against
real state); the genuine write-``None`` classification path is proven
through the write conditions the guard deliberately does not pre-check —
the already-bound branch and the already-installed current gate — the
repository-wide branch-ownership collision between two current Tasks of
one Repository (the real partial unique index raising the driver
``UniqueViolation`` the service translates), plus the real gate-resolution
``STALE`` outcome and the real ``gen_random_uuid()`` token rotation with
its continuation contract.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_state_services.py

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

import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import task_mutations
from openorc.services.errors import ConflictError, StaleOperationError

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction.

    Service compositions run inside nested psycopg transactions (SAVEPOINTs
    on the already-active per-test transaction), mirroring psycopg_pool's
    per-checkout semantics: a successful composition releases its savepoint,
    and a typed failure inside one rolls back only to it — the per-test
    transaction stays valid so the remaining assertions run instead of
    failing with ``InFailedSqlTransaction``.
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
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): every Profile needs its
    # backing Auth user row. The inserts roll back with the test transaction.
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
    conn: Connection[Any], *, github_repository_id: int = 50_000_001
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


def _create_task(
    pool: DatabasePool,
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int = 9001,
    github_issue_number: int = 42,
) -> Any:
    return task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
        github_issue_number=github_issue_number,
    )


def test_stale_token_is_rejected_by_the_guard_against_real_state(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)

    with pytest.raises(StaleOperationError):
        task_mutations.update_task_status(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=uuid.uuid4(),
            status=TaskStatus.PLANNING,
        )

    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.status is TaskStatus.READY_TO_PLAN
    assert unchanged.state_token == task.state_token


def test_rebinding_conflict_classifies_as_conflict_against_real_sql(
    conn: Connection[Any],
) -> None:
    """The guard deliberately does not pre-check branch binding: the real
    conditional write rejects the rebinding, and the locked classification
    re-read translates it into a typed conflict that applies nothing."""
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)

    bound = task_mutations.bind_canonical_branch(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=task.state_token,
        canonical_feature_branch="feat/producer-branch",
    )
    assert bound.canonical_feature_branch == "feat/producer-branch"

    with pytest.raises(ConflictError):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=bound.state_token,
            canonical_feature_branch="feat/other-branch",
        )

    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.canonical_feature_branch == "feat/producer-branch"
    assert unchanged.state_token == bound.state_token


def test_cross_task_branch_collision_translates_to_conflict_against_real_index(
    conn: Connection[Any],
) -> None:
    """Two current Tasks of one Repository independently choosing the same
    branch name: the repository-wide partial unique index rejects the second
    bind with a driver ``UniqueViolation``, and the service translates it
    into the stable typed conflict without exposing the other Task."""
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task_a = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)
    task_b = _create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=9002,
        github_issue_number=43,
    )

    owned = task_mutations.bind_canonical_branch(
        pool,
        workspace_id=workspace_id,
        task_id=task_a.id,
        expected_state_token=task_a.state_token,
        canonical_feature_branch="feat/shared-branch",
    )
    assert owned.canonical_feature_branch == "feat/shared-branch"

    with pytest.raises(ConflictError):
        task_mutations.bind_canonical_branch(
            pool,
            workspace_id=workspace_id,
            task_id=task_b.id,
            expected_state_token=task_b.state_token,
            canonical_feature_branch="feat/shared-branch",
        )

    # Task B applied nothing: unbound branch, unrotated token.
    unchanged = task_repositories.get_task(pool, task_b.id)
    assert unchanged is not None
    assert unchanged.canonical_feature_branch is None
    assert unchanged.state_token == task_b.state_token


def test_second_gate_install_classifies_as_conflict_against_real_sql(
    conn: Connection[Any],
) -> None:
    """The guard deliberately does not pre-check gate currency: the real
    conditional write refuses to replace a current gate, and the locked
    classification re-read translates it into a typed conflict."""
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)
    first = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="a" * 40,
    )
    second = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="b" * 40,
    )

    installed = task_mutations.set_current_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=task.state_token,
        owner_gate_id=first.id,
    )
    assert installed.current_owner_gate_id == first.id

    with pytest.raises(ConflictError):
        task_mutations.set_current_owner_gate(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=installed.state_token,
            owner_gate_id=second.id,
        )

    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.current_owner_gate_id == first.id
    still_pending = gate_repositories.get_owner_gate(pool, owner_gate_id=second.id)
    assert still_pending is not None
    assert still_pending.status is OwnerGateStatus.PENDING


def test_non_current_gate_resolution_translates_the_stale_outcome(
    conn: Connection[Any],
) -> None:
    """A pending gate that is not the Task's current gate resolves to the
    persistence ``STALE`` outcome under the real row lock; the service
    translates it into a typed stale operation and applies nothing."""
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)
    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="a" * 40,
    )

    with pytest.raises(StaleOperationError):
        task_mutations.resolve_owner_gate(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=task.state_token,
            owner_gate_id=gate.id,
            outcome=OwnerGateStatus.APPROVED,
        )

    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.current_owner_gate_id is None
    assert unchanged.state_token == task.state_token
    still_pending = gate_repositories.get_owner_gate(pool, owner_gate_id=gate.id)
    assert still_pending is not None
    assert still_pending.status is OwnerGateStatus.PENDING


def test_re_archiving_an_archived_task_is_stale(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)

    archived = task_mutations.archive_task(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=task.state_token,
        terminal_status=TaskStatus.CANCELLED,
    )
    assert archived.status is TaskStatus.CANCELLED
    assert archived.archived_at is not None

    with pytest.raises(StaleOperationError):
        task_mutations.archive_task(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=archived.state_token,
            terminal_status=TaskStatus.CANCELLED,
        )

    unchanged = task_repositories.get_task(pool, task.id)
    assert unchanged is not None
    assert unchanged.state_token == archived.state_token


def test_real_token_rotation_continues_through_returned_tokens(
    conn: Connection[Any],
) -> None:
    """The service returns the post-write Task with the database's freshly
    rotated token; the caller continues from that token, and the
    pre-mutation token is stale against the real durable state."""
    workspace_id, repository_id = _ownership_chain(conn)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = _create_task(pool, workspace_id=workspace_id, repository_id=repository_id)
    original_token = task.state_token

    moved = task_mutations.update_task_status(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=original_token,
        status=TaskStatus.PLANNING,
    )
    assert moved.state_token != original_token

    # The pre-mutation token no longer matches the real durable state.
    with pytest.raises(StaleOperationError):
        task_mutations.update_task_status(
            pool,
            workspace_id=workspace_id,
            task_id=task.id,
            expected_state_token=original_token,
            status=TaskStatus.IMPLEMENTING,
        )

    # The returned token is the current one and drives the next mutation.
    continued = task_mutations.update_task_status(
        pool,
        workspace_id=workspace_id,
        task_id=task.id,
        expected_state_token=moved.state_token,
        status=TaskStatus.IMPLEMENTING,
    )
    assert continued.status is TaskStatus.IMPLEMENTING
    assert continued.state_token != moved.state_token

    durable = task_repositories.get_task(pool, task.id)
    assert durable is not None
    assert durable.status is TaskStatus.IMPLEMENTING
    assert durable.state_token == continued.state_token
