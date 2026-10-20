"""Durable atomicity of the E5 implementation-authorization seams (issue #80).

The targeted real-Postgres checks the new seams own: the gate-presentation
composition (one pending exact-plan gate created and installed current, a
still-``planning`` Task moved to ``waiting_for_owner``, token rotation, and
the safe OPENORC audit facts) and the Owner approval composition (gate
APPROVED with OWNER provenance, planning ReviewLoop closed once, Task
``implementing``, and the resolution + authorization events) each commit
atomically, and a forced late failure rolls the whole approval composition
back — the gate stays pending and current, the Task keeps its token and
status, the loop stays OPEN, and no event is written. Owner cancellation
resolves the exact pending gate CANCELLED (never REJECTED) and archives
the Task in one composition, after which approval is impossible.

The scripted deterministic suite proves classification and no-write
failures; the E3c settlement and #219 supersession persistence coverage is
not duplicated here. No live GitHub, runtime, or Supabase access; the suite
consumes the explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import StaleOperationError
from openorc.services.task_implementation_authorization import (
    approve_implementation_authorization,
    cancel_implementation_authorization,
    open_implementation_authorization,
)

pytestmark = pytest.mark.integration

_BASE_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
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


def _insert_installation(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, %s, %s, %s)",
        (installation_id, workspace_id, 77_000_007, 77_000_008, "octocat", "Organization"),
    )
    return installation_id


def _insert_repository(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    github_installation_id: uuid.UUID,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch, github_installation_id) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            _insert_project(conn, workspace_id),
            workspace_id,
            77_000_009,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            github_installation_id,
        ),
    )
    return repository_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, 7703, 42, "a" * 64),
    )
    return task_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _authorization_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """The presentation-ready durable fixture.

    Ownership chain, one current PlanRevision, one snapshotted OPEN planning
    loop whose iteration 1 is the Reviewer ACCEPTED clearance, and the Task
    still ``planning`` with no gate — the ordinary E3b accepted shape.
    """
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content=_PLAN_TEXT,
        repository_base_sha=_BASE_SHA,
    )
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    pointed = task_repositories.set_current_plan_revision(
        pool,
        task_id,
        expected_state_token=task.state_token,
        plan_revision_id=revision.id,
    )
    assert pointed is not None
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
    finalized = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.ACCEPTED,
        summary="Looks good",
        findings=[],
    )
    assert finalized is not None
    return {
        "pool": pool,
        "profile_id": profile_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "revision_id": revision.id,
        "loop_id": loop.id,
        "state_token": pointed.state_token,
    }


def _open_call(pool: DatabasePool, facts: dict[str, Any]) -> Any:
    return open_implementation_authorization(
        pool,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
    )


def _approve_call(pool: DatabasePool, facts: dict[str, Any], *, gate_id: Any) -> Any:
    return approve_implementation_authorization(
        pool,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        owner_gate_id=gate_id,
        expected_state_token=facts["state_token"],
    )


def test_presentation_and_approval_compose_and_replay_is_stale(
    conn: Connection[Any],
) -> None:
    """Presentation installs one pending exact-plan gate (with its OPENORC
    audit), approval closes the loop and moves the Task to ``implementing``
    with OWNER-provenance audit, and both replays with the stale pre-write
    tokens apply nothing."""
    facts = _authorization_fixture(conn)
    pool = facts["pool"]

    presented = _open_call(pool, facts)

    assert presented.owner_gate.status is OwnerGateStatus.PENDING
    assert presented.owner_gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
    assert presented.owner_gate.plan_revision_id == facts["revision_id"]
    gate_id = presented.owner_gate.id
    assert presented.task.status is TaskStatus.WAITING_FOR_OWNER
    assert presented.task.current_owner_gate_id == gate_id
    assert presented.task.state_token != facts["state_token"]
    assert presented.review_loop.status is ReviewLoopStatus.OPEN
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_gate_created' and actor_type = 'openorc' "
            "and subject_type = 'owner_gate' and subject_id = %s",
            (facts["task_id"], gate_id),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_ready' and actor_type = 'openorc' "
            "and subject_type = 'plan_revision' and subject_id = %s",
            (facts["task_id"], facts["revision_id"]),
        )
        == 1
    )

    # A second presentation with the stale pre-presentation token is stale.
    with pytest.raises(StaleOperationError):
        _open_call(pool, facts)

    facts["state_token"] = presented.task.state_token
    authorized = _approve_call(pool, facts, gate_id=gate_id)

    assert authorized.owner_gate.status is OwnerGateStatus.APPROVED
    assert authorized.owner_gate.decided_at is not None
    assert authorized.review_loop.status is ReviewLoopStatus.CLOSED
    assert authorized.review_loop.closed_at is not None
    assert authorized.task.status is TaskStatus.IMPLEMENTING
    assert authorized.task.current_owner_gate_id is None
    assert authorized.task.state_token != presented.task.state_token
    iterations = review_repositories.list_review_loop_iterations(
        pool, review_loop_id=facts["loop_id"]
    )
    assert [iteration.iteration_number for iteration in iterations] == [1]
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_gate_resolved' and actor_type = 'owner' "
            "and actor_id = %s and subject_type = 'owner_gate' and subject_id = %s "
            'and context = \'{"outcome": "approved"}\'::jsonb',
            (facts["task_id"], str(facts["profile_id"]), gate_id),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'implementation_authorized' and actor_type = 'owner' "
            "and subject_type = 'plan_revision' and subject_id = %s",
            (facts["task_id"], facts["revision_id"]),
        )
        == 1
    )

    # A replayed approval with the pre-approval token is stale and rewrites
    # nothing: the resolution is one-shot and E6's contract is not bypassed.
    facts["state_token"] = presented.task.state_token
    with pytest.raises(StaleOperationError):
        _approve_call(pool, facts, gate_id=gate_id)
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=gate_id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.APPROVED
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.IMPLEMENTING
    assert task.state_token == authorized.task.state_token


def test_a_late_approval_failure_rolls_the_whole_composition_back(
    conn: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the gate resolution applies nothing: the gate stays
    pending and current, the Task keeps its token and status, the loop stays
    OPEN, and no approval event was written."""
    facts = _authorization_fixture(conn)
    pool = facts["pool"]
    presented = _open_call(pool, facts)
    gate_id = presented.owner_gate.id
    events_before = _row_count(
        conn,
        "select count(*) from openorc.workflow_events where task_id = %s",
        (facts["task_id"],),
    )

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late approval failure")

    monkeypatch.setattr(event_coordination, "record_implementation_authorized_event", _boom)
    facts["state_token"] = presented.task.state_token
    with pytest.raises(RuntimeError, match="forced late approval failure"):
        _approve_call(pool, facts, gate_id=gate_id)

    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=gate_id)
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id == gate_id
    assert task.state_token == presented.task.state_token
    loop = review_repositories.get_review_loop(pool, review_loop_id=facts["loop_id"])
    assert loop is not None
    assert loop.status is ReviewLoopStatus.OPEN
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s",
            (facts["task_id"],),
        )
        == events_before
    )


def test_owner_cancel_resolves_and_archives_atomically(
    conn: Connection[Any],
) -> None:
    """Cancellation stamps the gate CANCELLED with OWNER provenance (never
    REJECTED), archives the Task with its safe OWNER terminal event, and
    leaves the gate unapprovable afterwards."""
    facts = _authorization_fixture(conn)
    pool = facts["pool"]
    presented = _open_call(pool, facts)
    gate_id = presented.owner_gate.id
    facts["state_token"] = presented.task.state_token

    cancelled = cancel_implementation_authorization(
        pool,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        owner_gate_id=gate_id,
        expected_state_token=facts["state_token"],
    )

    assert cancelled.owner_gate.status is OwnerGateStatus.CANCELLED
    assert cancelled.owner_gate.decided_at is not None
    assert cancelled.task.status is TaskStatus.CANCELLED
    assert cancelled.task.archived_at is not None
    assert cancelled.task.current_owner_gate_id is None
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_gate_resolved' and actor_type = 'owner' "
            'and context = \'{"outcome": "cancelled"}\'::jsonb',
            (facts["task_id"],),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'task_cancelled' and actor_type = 'owner'",
            (facts["task_id"],),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            'and context = \'{"outcome": "rejected"}\'::jsonb',
            (facts["task_id"],),
        )
        == 0
    )

    with pytest.raises(StaleOperationError):
        _approve_call(pool, facts, gate_id=gate_id)
