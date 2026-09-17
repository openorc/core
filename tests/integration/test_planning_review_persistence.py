"""Integration-marked persistence tests for planning and review (issue #23).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
planning/review invariants directly: PlanRevision immutability and
fresh-revision behavior, repository-base movement not invalidating or
rewriting an accepted/authorized revision, immutable finalized Reviewer
results (outcome, summary, findings, decided_at finalizing atomically and
never rewriting), per-Loop iteration-number uniqueness, the exact v1
purpose vocabulary, effective-iteration-limit retention with the
configured default, current-plan pointer movement with same-Task/Workspace
consistency, cross-Task corruption rejection, and the settled
outcome/findings coherence. They are excluded from the ordinary
deterministic baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_planning_review_persistence.py

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and
outside agent responsibility: in the normal Owner local-development flow the
target is the ephemeral non-production Supabase branch that the Owner-only
``devserver.sh`` command creates and later deletes (agents never invoke it).
The session fixture merely resets the ``openorc`` schema and applies the
committed migrations from scratch within the supplied database, and the suite
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent.
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
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation

from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
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
    conn: Connection[Any], *, github_repository_id: int = 70_000_001
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


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _fresh_task(
    conn: Connection[Any], *, github_issue_id: int = 7301
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


def test_plan_revisions_are_immutable_with_fresh_revision_behavior(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)

    first = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan v1",
        repository_base_sha="base-a",
    )
    second = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=2,
        content="# Plan v2",
        repository_base_sha="base-b",
    )

    # A changed plan is a fresh revision: both revisions exist with their own
    # exact content and base context, and the planning history is complete.
    history = planning_repositories.list_task_plan_revisions(pool, task_id=task_id)
    assert [revision.revision_number for revision in history] == [1, 2]
    assert history[0].content == "# Plan v1" and history[0].repository_base_sha == "base-a"
    assert history[1].content == "# Plan v2" and history[1].repository_base_sha == "base-b"

    # The per-Task version sequence is unique: a duplicate version cannot be
    # created (the durable backstop behind fresh-revision behavior).
    with pytest.raises(UniqueViolation):
        planning_repositories.create_plan_revision(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            revision_number=2,
            content="# Plan v2 again",
            repository_base_sha="base-c",
        )
    # The created revisions are distinct rows: neither is a mutation of the
    # other.
    assert first.id != second.id
    assert first.content == "# Plan v1" and second.content == "# Plan v2"


def test_repository_base_movement_neither_invalidates_nor_rewrites_an_accepted_revision(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)

    revision_v1 = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan v1",
        repository_base_sha="base-old",
    )
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision_v1.id,
    )
    accepted = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.ACCEPTED,
        summary="plan clears",
        findings=[],
    )
    assert accepted is not None and accepted.outcome is ReviewOutcome.ACCEPTED

    # The repository base later moves: the fresh revision carries the new
    # base context and the pointer moves to it. Nothing rewrites revision 1,
    # and the accepted review of the exact earlier subject remains intact.
    revision_v2 = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=2,
        content="# Plan v1 (rebased)",
        repository_base_sha="base-new",
    )
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    moved = task_repositories.set_current_plan_revision(
        pool,
        task_id,
        expected_state_token=task.state_token,
        plan_revision_id=revision_v2.id,
    )
    assert moved is not None and moved.current_plan_revision_id == revision_v2.id

    # Revision 1 is untouched history: same content, same base, still
    # exactly the subject of its recorded ACCEPTED iteration.
    reread_v1 = planning_repositories.get_plan_revision(pool, plan_revision_id=revision_v1.id)
    assert reread_v1 is not None
    assert reread_v1.content == "# Plan v1"
    assert reread_v1.repository_base_sha == "base-old"
    iterations = review_repositories.list_review_loop_iterations(pool, review_loop_id=loop.id)
    assert len(iterations) == 1
    assert iterations[0].plan_revision_id == revision_v1.id
    assert iterations[0].outcome is ReviewOutcome.ACCEPTED
    assert iterations[0].findings == []


def test_finalized_iteration_results_cannot_be_rewritten(conn: Connection[Any]) -> None:
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
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )

    first_result = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="tighten the acceptance criteria",
        findings=[{"summary": "vague step 2", "details": "no measurable outcome"}],
    )
    assert first_result is not None

    # A finalized iteration can never change: the conditional finalize
    # matches no row and returns None (a rejected no-op, never a rewrite).
    assert (
        review_repositories.record_review_iteration_result(
            pool,
            review_iteration_id=iteration.id,
            outcome=ReviewOutcome.ACCEPTED,
            summary="rewritten past",
            findings=[],
        )
        is None
    )
    reread = review_repositories.get_review_iteration(pool, review_iteration_id=iteration.id)
    assert reread is not None
    assert reread.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert reread.summary == "tighten the acceptance criteria"
    assert reread.findings == [{"summary": "vague step 2", "details": "no measurable outcome"}]

    # No partially recorded result can exist at the database level either:
    # setting one result fact alone violates the finalize-coherence CHECK.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.review_iterations set summary = 'orphan fact' where id = %s",
            (iteration.id,),
        )


def test_one_review_loop_cannot_contain_duplicate_iteration_numbers(
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
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )

    # The repository path cannot create a second iteration with the same
    # number in one loop (UniqueViolation), and neither can a direct insert.
    with pytest.raises(UniqueViolation):
        review_repositories.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=1,
            plan_revision_id=revision.id,
        )
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            "insert into openorc.review_iterations "
            "(workspace_id, task_id, review_loop_id, iteration_number, plan_revision_id) "
            "values (%s, %s, %s, 1, %s)",
            (workspace_id, task_id, loop.id, revision.id),
        )
    # A different loop numbers its iterations independently.
    other_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=3,
    )
    review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=other_loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.review_iterations where review_loop_id = %s",
            (loop.id,),
        )
        == 1
    )


def test_review_loop_purpose_accepts_only_planning_and_pr_review(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)

    planning_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    pr_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    assert planning_loop.purpose is ReviewLoopPurpose.PLANNING
    assert pr_loop.purpose is ReviewLoopPurpose.PR_REVIEW

    # v1 accepts no implementation-review loop and no speculative categories.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.review_loops "
            "(workspace_id, task_id, purpose, iteration_limit, status) "
            "values (%s, %s, 'implementation_review', 5, 'open')",
            (workspace_id, task_id),
        )


def test_review_loop_persistence_retains_the_effective_iteration_limit(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)

    default_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    assert default_loop.iteration_limit == DEFAULT_REVIEW_LOOP_ITERATION_LIMIT == 5
    custom_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=3,
    )
    # The effective limit each loop was created with is retained verbatim
    # for historical reconstruction (the configured boundary supplies the
    # value; the database carries no default).
    reread_default = review_repositories.get_review_loop(pool, review_loop_id=default_loop.id)
    reread_custom = review_repositories.get_review_loop(pool, review_loop_id=custom_loop.id)
    assert reread_default is not None and reread_default.iteration_limit == 5
    assert reread_custom is not None and reread_custom.iteration_limit == 3

    # A non-positive limit is rejected by the CHECK.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.review_loops "
            "(workspace_id, task_id, purpose, iteration_limit, status) "
            "values (%s, %s, 'planning', 0, 'open')",
            (workspace_id, task_id),
        )


def test_current_plan_pointer_moves_between_revisions_leaving_history_intact(
    conn: Connection[Any],
) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    revisions = [
        planning_repositories.create_plan_revision(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            revision_number=number,
            content=f"# Plan v{number}",
            repository_base_sha=f"base-{number}",
        )
        for number in (1, 2, 3)
    ]
    task = task_repositories.get_task(pool, task_id)
    assert task is not None

    pointed = task_repositories.set_current_plan_revision(
        pool,
        task_id,
        expected_state_token=task.state_token,
        plan_revision_id=revisions[0].id,
    )
    assert pointed is not None and pointed.current_plan_revision_id == revisions[0].id

    # The pointer moves forward to a newer same-Task revision; every older
    # revision remains intact and listable.
    newer_task = task_repositories.get_task(pool, task_id)
    assert newer_task is not None
    moved = task_repositories.set_current_plan_revision(
        pool,
        task_id,
        expected_state_token=newer_task.state_token,
        plan_revision_id=revisions[2].id,
    )
    assert moved is not None and moved.current_plan_revision_id == revisions[2].id
    history = planning_repositories.list_task_plan_revisions(pool, task_id=task_id)
    assert [revision.revision_number for revision in history] == [1, 2, 3]
    assert [revision.content for revision in history] == ["# Plan v1", "# Plan v2", "# Plan v3"]

    # A stale state token is a rejected no-op (never a blind mutation).
    assert (
        task_repositories.set_current_plan_revision(
            pool,
            task_id,
            expected_state_token=task.state_token,  # the pre-move token
            plan_revision_id=revisions[1].id,
        )
        is None
    )
    after = task_repositories.get_task(pool, task_id)
    assert after is not None and after.current_plan_revision_id == revisions[2].id


def test_cross_task_pointer_corruption_is_rejected(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    # A second Task in a different Workspace owning its own revision.
    other_workspace_id, _ = _ownership_chain(conn, github_repository_id=70_000_002)
    other_repository_id = _insert_repository(
        conn,
        project_id=_insert_project(conn, other_workspace_id),
        workspace_id=other_workspace_id,
        github_repository_id=70_000_003,
    )
    other_task_id = _insert_task(
        conn,
        workspace_id=other_workspace_id,
        repository_id=other_repository_id,
        github_issue_id=7302,
    )
    foreign_revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=other_workspace_id,
        task_id=other_task_id,
        revision_number=1,
        content="# Foreign plan",
        repository_base_sha="foreign-base",
    )
    task = task_repositories.get_task(pool, task_id)
    assert task is not None

    # A pointer to another Task's revision is rejected durably by the
    # composite foreign key; the pointer and the Task are unchanged.
    with pytest.raises(ForeignKeyViolation):
        task_repositories.set_current_plan_revision(
            pool,
            task_id,
            expected_state_token=task.state_token,
            plan_revision_id=foreign_revision.id,
        )
    assert workspace_id != other_workspace_id
    after = task_repositories.get_task(pool, task_id)
    assert after is not None
    assert after.current_plan_revision_id is None


def test_iteration_subject_must_belong_to_the_same_task(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    # A second Task in the same Workspace with its own revision.
    other_repository_id = _insert_repository(
        conn,
        project_id=_insert_project(conn, workspace_id),
        workspace_id=workspace_id,
        github_repository_id=70_000_004,
    )
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=other_repository_id,
        github_issue_id=7303,
    )
    foreign_revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=other_task_id,
        revision_number=1,
        content="# Another task's plan",
        repository_base_sha="base-x",
    )
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )

    # The exact-subject rule is durable: an iteration of task_id's loop
    # cannot bind another Task's revision, even within the same Workspace.
    with pytest.raises(ForeignKeyViolation):
        review_repositories.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=1,
            plan_revision_id=foreign_revision.id,
        )


def test_settled_result_coherence_is_durable_in_both_directions(
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
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )

    def _iteration(number: int) -> uuid.UUID:
        created = review_repositories.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=number,
            plan_revision_id=revision.id,
        )
        return created.id

    # ACCEPTED clears with zero findings.
    accepted = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=_iteration(1),
        outcome=ReviewOutcome.ACCEPTED,
        summary="clears",
        findings=[],
    )
    assert accepted is not None and accepted.findings == []
    # CHANGES_REQUESTED retains at least one finding.
    changes = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=_iteration(2),
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="not yet",
        findings=[{"summary": "s", "details": "d"}],
    )
    assert changes is not None and changes.findings is not None
    assert len(changes.findings) == 1
    # The coherence is CHECK-enforced in both failing directions.
    with pytest.raises(CheckViolation):
        review_repositories.record_review_iteration_result(
            pool,
            review_iteration_id=_iteration(3),
            outcome=ReviewOutcome.ACCEPTED,
            summary="premature acceptance",
            findings=[{"summary": "s", "details": "d"}],
        )
    with pytest.raises(CheckViolation):
        review_repositories.record_review_iteration_result(
            pool,
            review_iteration_id=_iteration(4),
            outcome=ReviewOutcome.CHANGES_REQUESTED,
            summary="empty findings",
            findings=[],
        )


def test_findings_are_persisted_verbatim_as_a_json_array(conn: Connection[Any]) -> None:
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
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    recorded_findings = [
        {"summary": "vague step 2", "details": "no measurable outcome", "lines": [40, 55]},
        {"summary": "missing rollback", "details": "add a rollback step", "lines": []},
    ]
    finalized = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="fix the plan",
        findings=recorded_findings,
    )
    assert finalized is not None
    assert finalized.findings == recorded_findings

    # Read back verbatim through a fresh query: the document is stored as a
    # JSON array and returns exactly as persisted.
    reread = review_repositories.get_review_iteration(pool, review_iteration_id=iteration.id)
    assert reread is not None and reread.findings == recorded_findings
    array_type = conn.execute(
        "select jsonb_typeof(findings) from openorc.review_iterations where id = %s",
        (iteration.id,),
    ).fetchone()
    assert array_type is not None and array_type[0] == "array"

    # A non-array findings document is rejected by the durable shape CHECK.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.review_iterations "
            "set outcome = 'accepted', summary = 's', findings = %s, decided_at = now() "
            "where id = %s",
            ('{"not": "an array"}', iteration.id),
        )


def test_review_loop_close_is_absorbing(conn: Connection[Any]) -> None:
    workspace_id, _, task_id, pool = _fresh_task(conn)
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    closed = review_repositories.close_review_loop(pool, review_loop_id=loop.id)
    assert closed is not None
    assert closed.status is ReviewLoopStatus.CLOSED
    assert closed.closed_at is not None
    # CLOSED is absorbing: a second close is a rejected no-op.
    assert review_repositories.close_review_loop(pool, review_loop_id=loop.id) is None
    reread = review_repositories.get_review_loop(pool, review_loop_id=loop.id)
    assert reread is not None
    assert reread.status is ReviewLoopStatus.CLOSED
    assert reread.closed_at == closed.closed_at


def test_the_same_review_primitives_serve_both_v1_purposes(conn: Connection[Any]) -> None:
    # PR review support later binds one TaskPullRequest plus exact head SHA
    # on this same review-history model (#25): no second model exists, and
    # the PR_REVIEW purpose is already a first-class loop vocabulary.
    workspace_id, _, task_id, pool = _fresh_task(conn)
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
    pr_loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=pr_loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert iteration.review_loop_id == pr_loop.id
    loops = review_repositories.list_task_review_loops(
        pool, task_id=task_id, status=ReviewLoopStatus.OPEN
    )
    assert [loop.purpose for loop in loops] == [ReviewLoopPurpose.PR_REVIEW]
