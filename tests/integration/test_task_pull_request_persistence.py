"""Integration-marked persistence tests for TaskPullRequest (issue #25).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
TaskPullRequest invariants directly: one canonical PR per Task for the
whole v1 lifetime (a second record — even after the first closes unmerged —
raises ``UniqueViolation``), the current observed head changing while the
PR identity stays constant, stored PR-review ``reviewed_head_sha`` history
remaining intact across later head changes, stable GitHub PR identity and
repository-local PR number as distinct persistence concerns, incompatible
Task/Workspace/Repository attachment rejection through the composite
foreign keys, PR ReviewIterations binding the exact TaskPullRequest plus
the exact reviewed head SHA, base-ref movement not rewriting the historical
review subject, ReviewLoop purpose/subject agreement enforced in both
directions, and OwnerGate PR-subject persistence with cross-Task rejection.
They are excluded from the ordinary deterministic baseline by the
repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_pull_request_persistence.py

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

import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation, UniqueViolation

from openorc.domain.gates import OwnerGateType
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewLoopDomainError,
    ReviewLoopPurpose,
    ReviewOutcome,
)
from openorc.persistence import gates as gate_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import pull_requests as pull_request_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence.pool import DatabasePool

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration

_HEAD_A = "1111111111111111111111111111111111111111"
_HEAD_B = "2222222222222222222222222222222222222222"


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
    conn: Connection[Any], *, github_repository_id: int = 70_100_001
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
    conn: Connection[Any], *, github_issue_id: int = 7501
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


def _canonical_pull_request(
    pool: DatabasePool,
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_pr_id: int = 555_000,
    head_sha: str = _HEAD_A,
    base_ref: str = "main",
) -> Any:
    """Insert the Task's canonical PR record through the repository."""
    return pull_request_repositories.create_task_pull_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        repository_id=repository_id,
        github_pr_id=github_pr_id,
        github_pr_number=42,
        head_ref="openorc/task-42",
        base_ref=base_ref,
        head_sha=head_sha,
    )


def _pr_review_subject_count(conn: Connection[Any], task_id: uuid.UUID) -> int:
    return _row_count(
        conn,
        "select count(*) from openorc.review_iterations where task_id = %s",
        (task_id,),
    )


def test_a_second_task_pull_request_for_one_task_is_rejected(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    first = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )

    # One Task has exactly one PR record for the whole v1 lifetime: the
    # full-history ``unique (task_id)`` rejects a second row outright.
    with pytest.raises(UniqueViolation):
        _canonical_pull_request(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            repository_id=repository_id,
            github_pr_id=first.github_pr_id + 1,
        )
    assert (
        _row_count(
            conn, "select count(*) from openorc.task_pull_requests where task_id = %s", (task_id,)
        )
        == 1
    )


def test_the_current_head_changes_while_the_pr_identity_stays_constant(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )
    created_at = record.created_at

    reconciled = pull_request_repositories.update_task_pull_request_observed(
        pool,
        task_pull_request_id=record.id,
        head_ref=record.head_ref,
        base_ref=record.base_ref,
        head_sha=_HEAD_B,
        state=TaskPullRequestState.OPEN,
        merged_at=None,
    )
    assert reconciled is not None
    # The PR identity is unchanged; only the observed head moved.
    assert reconciled.id == record.id
    assert reconciled.github_pr_id == record.github_pr_id
    assert reconciled.github_pr_number == record.github_pr_number
    assert reconciled.head_sha == _HEAD_B
    assert reconciled.head_sha != record.head_sha
    assert reconciled.created_at == record.created_at
    assert reconciled.updated_at >= record.updated_at
    # The per-Task lookup reaches the same single canonical record.
    reread = pull_request_repositories.get_task_pull_request_for_task(pool, task_id=task_id)
    assert reread is not None and reread.id == record.id and reread.head_sha == _HEAD_B
    assert reread.created_at == created_at


def test_stored_reviewed_head_sha_survives_a_current_head_change(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )
    loop = review_repositories.create_review_loop(
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
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=None,
        task_pull_request_id=record.id,
        reviewed_head_sha=_HEAD_A,
    )
    assert iteration is not None
    finalized = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.ACCEPTED,
        summary="accepted the exact head",
        findings=[],
    )
    assert finalized is not None

    # The PR head moves across remediation rounds; the historical review
    # subject — the same TaskPullRequest plus the exact reviewed head —
    # must remain untouched.
    reconciled = pull_request_repositories.update_task_pull_request_observed(
        pool,
        task_pull_request_id=record.id,
        head_ref=record.head_ref,
        base_ref=record.base_ref,
        head_sha=_HEAD_B,
        state=TaskPullRequestState.OPEN,
        merged_at=None,
    )
    assert reconciled is not None and reconciled.head_sha == _HEAD_B

    reread = review_repositories.get_review_iteration(pool, review_iteration_id=iteration.id)
    assert reread is not None
    assert reread.task_pull_request_id == record.id
    assert reread.reviewed_head_sha == _HEAD_A
    assert reread.outcome is ReviewOutcome.ACCEPTED
    assert isinstance(reread.decided_at, datetime)
    # The durable row itself carries the reviewed head, not the PR's
    # current observed head.
    stored = conn.execute(
        "select reviewed_head_sha from openorc.review_iterations where id = %s",
        (iteration.id,),
    ).fetchone()
    assert stored is not None and stored[0] == _HEAD_A


def test_stable_github_identity_and_local_number_are_distinct_concerns(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )

    # The reconciliation lookup is keyed on the stable GitHub identity; the
    # repository-local number rides along as address metadata only.
    found = pull_request_repositories.find_task_pull_request_by_github_identity(
        pool, workspace_id=workspace_id, github_pr_id=record.github_pr_id
    )
    assert found is not None
    assert found.id == record.id
    assert found.github_pr_id != found.github_pr_number

    # Reconciliation updates observed state in place and never rewrites
    # either identity or address metadata.
    reconciled = pull_request_repositories.update_task_pull_request_observed(
        pool,
        task_pull_request_id=record.id,
        head_ref=record.head_ref,
        base_ref="release/2.0",
        head_sha=_HEAD_B,
        state=TaskPullRequestState.OPEN,
        merged_at=None,
    )
    assert reconciled is not None
    assert reconciled.github_pr_id == record.github_pr_id
    assert reconciled.github_pr_number == record.github_pr_number


def test_incompatible_task_workspace_repository_attachment_is_rejected(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    # A second Task in the same Workspace with its own Repository.
    other_repository_id = _insert_repository(
        conn,
        project_id=_insert_project(conn, workspace_id),
        workspace_id=workspace_id,
        github_repository_id=70_100_002,
    )
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=other_repository_id,
        github_issue_id=7502,
    )

    # A PR record cannot attach to the Task's Repository across Tasks: the
    # Task/Repository/Workspace triple FK rejects the attachment.
    with pytest.raises(ForeignKeyViolation):
        _canonical_pull_request(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            repository_id=other_repository_id,
        )
    # The inverse: the PR cannot be created for a foreign Task under the
    # first Task's ownership either.
    with pytest.raises(ForeignKeyViolation):
        _canonical_pull_request(
            pool,
            workspace_id=workspace_id,
            task_id=other_task_id,
            repository_id=repository_id,
        )

    # A different Workspace can never be the scope of a Task's PR record.
    other_workspace_id, _ = _ownership_chain(conn, github_repository_id=70_100_003)
    with pytest.raises(ForeignKeyViolation):
        pull_request_repositories.create_task_pull_request(
            pool,
            workspace_id=other_workspace_id,
            task_id=task_id,
            repository_id=repository_id,
            github_pr_id=555_001,
            github_pr_number=7,
            head_ref="openorc/task-42",
            base_ref="main",
            head_sha=_HEAD_A,
        )


def test_pr_review_iteration_binds_the_exact_task_pull_request_and_head(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        repository_id=repository_id,
    )
    loop = review_repositories.create_review_loop(
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
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=None,
        task_pull_request_id=record.id,
        reviewed_head_sha=_HEAD_A,
    )
    assert iteration is not None
    assert iteration.task_pull_request_id == record.id
    assert iteration.reviewed_head_sha == _HEAD_A
    assert iteration.plan_revision_id is None

    # A PR subject belonging to a different Task can never enter this
    # Task's loop, even within the same Workspace: the composite subject
    # foreign key rejects cross-Task subject corruption.
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=record.repository_id,
        github_issue_id=7503,
    )
    other_pr = pull_request_repositories.create_task_pull_request(
        pool,
        workspace_id=workspace_id,
        task_id=other_task_id,
        repository_id=record.repository_id,
        github_pr_id=555_002,
        github_pr_number=8,
        head_ref="openorc/task-43",
        base_ref="main",
        head_sha=_HEAD_B,
    )
    # The PR subject belongs to the same Task as the iteration: the
    # composite foreign key rejects another Task's PR. The subject foreign
    # key is DEFERRABLE INITIALLY DEFERRED (issue #27 deletion semantics);
    # force the pending check at the assertion point.
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        review_repositories.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=loop.id,
            iteration_number=2,
            plan_revision_id=None,
            task_pull_request_id=other_pr.id,
            reviewed_head_sha=_HEAD_B,
        )
        conn.execute("set constraints openorc.review_iterations_task_pull_request_fk immediate")


def test_base_ref_movement_does_not_rewrite_the_review_subject(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )
    loop = review_repositories.create_review_loop(
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
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=None,
        task_pull_request_id=record.id,
        reviewed_head_sha=_HEAD_A,
    )
    assert iteration is not None

    # The PR target/base moves; the PR's observed base_ref updates while
    # the immutable review subject (TaskPullRequest + exact reviewed head)
    # stays exactly as stored.
    reconciled = pull_request_repositories.update_task_pull_request_observed(
        pool,
        task_pull_request_id=record.id,
        head_ref=record.head_ref,
        base_ref="release/2.0",
        head_sha=_HEAD_A,
        state=TaskPullRequestState.OPEN,
        merged_at=None,
    )
    assert reconciled is not None
    assert reconciled.base_ref == "release/2.0"

    reread = review_repositories.get_review_iteration(pool, review_iteration_id=iteration.id)
    assert reread is not None
    assert reread.task_pull_request_id == record.id
    assert reread.reviewed_head_sha == _HEAD_A
    assert reread.plan_revision_id is None


def test_a_closed_unmerged_pull_request_remains_canonical_and_prevents_replacement(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )
    closed = pull_request_repositories.update_task_pull_request_observed(
        pool,
        task_pull_request_id=record.id,
        head_ref=record.head_ref,
        base_ref=record.base_ref,
        head_sha=record.head_sha,
        state=TaskPullRequestState.CLOSED,
        merged_at=None,
    )
    assert closed is not None
    assert closed.state is TaskPullRequestState.CLOSED
    assert closed.merged_at is None

    # The closed-unmerged PR remains the Task's canonical record through
    # the normal per-Task lookup...
    canonical = pull_request_repositories.get_task_pull_request_for_task(pool, task_id=task_id)
    assert canonical is not None
    assert canonical.id == record.id
    assert canonical.state is TaskPullRequestState.CLOSED

    # ...and it still blocks a replacement PR row for the same Task: there
    # is no replacement-PR history in v1.
    with pytest.raises(UniqueViolation):
        _canonical_pull_request(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            repository_id=record.repository_id,
            github_pr_id=record.github_pr_id + 1,
        )


def test_review_loop_purpose_subject_mismatch_is_rejected_in_both_directions(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
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

    # A PR subject cannot enter a planning loop: the mismatch is a
    # deterministic caller error raised under the loop lock, and nothing
    # is written.
    with pytest.raises(ReviewLoopDomainError):
        review_repositories.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=planning_loop.id,
            iteration_number=1,
            plan_revision_id=None,
            task_pull_request_id=record.id,
            reviewed_head_sha=_HEAD_A,
        )
    assert _pr_review_subject_count(conn, task_id) == 0
    # ...and a PlanRevision subject cannot enter a PR-review loop.
    with pytest.raises(ReviewLoopDomainError):
        review_repositories.create_review_iteration(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            review_loop_id=pr_loop.id,
            iteration_number=1,
            plan_revision_id=revision.id,
        )
    assert _pr_review_subject_count(conn, task_id) == 0

    # The matching purpose accepts its own subject form.
    pr_iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=pr_loop.id,
        iteration_number=1,
        plan_revision_id=None,
        task_pull_request_id=record.id,
        reviewed_head_sha=_HEAD_A,
    )
    assert pr_iteration is not None
    plan_iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=planning_loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert plan_iteration is not None


def test_owner_gate_pr_subjects_persist_and_reject_cross_task_binding(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id, task_id, pool = _fresh_task(conn)
    record = _canonical_pull_request(
        pool, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
    )

    # PR_AUTHORIZATION: the pre-PR exact-head gate — persisted with the
    # head SHA only and no PR binding, per the settled subject mapping.
    pr_authorization = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha=_HEAD_A,
    )
    fetched = gate_repositories.get_owner_gate(pool, owner_gate_id=pr_authorization.id)
    assert fetched is not None
    assert fetched.subject_head_sha == _HEAD_A
    assert fetched.task_pull_request_id is None
    assert fetched.plan_revision_id is None

    # MERGE_DECISION: the canonical TaskPullRequest plus the exact head SHA.
    merge_decision = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.MERGE_DECISION,
        subject_head_sha=_HEAD_A,
        task_pull_request_id=record.id,
    )
    fetched_merge = gate_repositories.get_owner_gate(pool, owner_gate_id=merge_decision.id)
    assert fetched_merge is not None
    assert fetched_merge.task_pull_request_id == record.id
    assert fetched_merge.subject_head_sha == _HEAD_A
    assert fetched_merge.plan_revision_id is None

    # REVIEW_RESOLUTION (PR form): the TaskPullRequest plus the exact head.
    resolution = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.REVIEW_RESOLUTION,
        subject_head_sha=_HEAD_A,
        task_pull_request_id=record.id,
    )
    fetched_resolution = gate_repositories.get_owner_gate(pool, owner_gate_id=resolution.id)
    assert fetched_resolution is not None
    assert fetched_resolution.task_pull_request_id == record.id

    # REVIEW_RESOLUTION (planning form): the PlanRevision only.
    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha="base-a",
    )
    plan_resolution = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.REVIEW_RESOLUTION,
        plan_revision_id=revision.id,
    )
    fetched_plan_resolution = gate_repositories.get_owner_gate(
        pool, owner_gate_id=plan_resolution.id
    )
    assert fetched_plan_resolution is not None
    assert fetched_plan_resolution.plan_revision_id == revision.id
    assert fetched_plan_resolution.subject_head_sha is None
    assert fetched_plan_resolution.task_pull_request_id is None

    # A PR-subject gate cannot bind another Task's PR, even inside the same
    # Workspace: the composite foreign key is the durable exact-subject
    # rule. It is DEFERRABLE INITIALLY DEFERRED (issue #27 deletion
    # semantics); force the pending check at the assertion point.
    other_task_id = _insert_task(
        conn,
        workspace_id=workspace_id,
        repository_id=record.repository_id,
        github_issue_id=7504,
    )
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        gate_repositories.create_owner_gate(
            pool,
            workspace_id=workspace_id,
            task_id=other_task_id,
            gate_type=OwnerGateType.MERGE_DECISION,
            subject_head_sha=_HEAD_A,
            task_pull_request_id=record.id,
        )
        conn.execute("set constraints openorc.owner_gates_task_pull_request_fk immediate")
