"""Integration-marked persistence tests for the Hub-run correlation/settlement
seam (issue #322).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
invariants directly against real Postgres:

- the partial unique index enforces the single-outstanding-run invariant per
  authoritative session binding (one unsettled ``claimed``/``admitted``
  receipt), while a settled prior receipt frees the slot for the next run;
- the acknowledged ``runId`` binds at most once per session binding;
- the per-status coherence and outcome-label CHECK constraints reject
  incoherent rows;
- the acknowledgement attach, settlement, and refusal are one-shot
  forward-only conditional transitions (a raced or duplicated transition is
  a rejected no-op);
- the staged early completion is unique per receipt and consumed exactly
  once;
- the composite subject foreign keys reject cross-Task and cross-Workspace
  subjects.

They are excluded from the ordinary deterministic baseline by the repository
pytest configuration. Run explicitly when a target has been made available
(Owner-reserved provisioning):

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_task_hub_run_persistence.py

The suite consumes the database it is given and never provisions one.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation
from psycopg.types.json import Jsonb

from openorc.domain.hub_runs import HubRunObservationKind
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.hub_runs import (
    attach_task_hub_run_acknowledgement,
    consume_task_hub_run_early_completion,
    create_task_hub_run_receipt,
    get_outstanding_task_hub_run_receipt,
    get_task_hub_run_early_completion_for_update,
    mark_task_hub_run_receipt_contradicted,
    refuse_task_hub_run_receipt,
    settle_task_hub_run_receipt,
    stage_task_hub_run_early_completion,
)

pytestmark = pytest.mark.integration


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and
    transaction (the same SAVEPOINT semantics as the other persistence
    suites)."""

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


def _insert_task_chain(conn: Connection[Any]) -> tuple[Any, ...]:
    """Insert the Profile/Workspace/Project/Repository/Task chain and return
    (task_id, workspace_id)."""
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, profile_id, "workspace"),
    )
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private) values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            5_000_000,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
        ),
    )
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, "
        "status, source_requirements_fingerprint, state_token) "
        "values (%s, %s, %s, 9001, 42, 'planning', %s, %s)",
        (task_id, workspace_id, repository_id, "a" * 64, uuid.uuid4()),
    )
    return task_id, workspace_id


def _insert_producer_session(conn: Connection[Any], workspace_id: Any, task_id: Any) -> Any:
    """Insert one READY Producer binding and return its id."""
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name) "
        "values (%s, %s, 'cline', 'staged-hub')",
        (connection_id, workspace_id),
    )
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, 'producer', %s, %s, 'ready', %s, now())",
        (
            session_id,
            workspace_id,
            task_id,
            connection_id,
            "ext-" + str(session_id)[:8],
            Jsonb({"adapter": "cline"}),
        ),
    )
    return session_id


def _create_receipt(pool: Any, workspace_id: Any, task_id: Any, session_id: Any) -> Any:
    return create_task_hub_run_receipt(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        task_agent_session_id=session_id,
        operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=uuid.uuid4(),
        expected_formal_family="implementation_result",
    )


def test_the_single_outstanding_run_invariant_is_durable_per_session(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)

    first = _create_receipt(pool, workspace_id, task_id, session_id)
    assert first.status.value == "claimed"
    assert get_outstanding_task_hub_run_receipt(pool, task_agent_session_id=session_id) is not None

    # A concurrent second dispatch for the same session binding is rejected
    # by the partial unique index while the first run is outstanding.
    with pytest.raises(UniqueViolation):
        _create_receipt(pool, workspace_id, task_id, session_id)

    # Admitting does not free the slot; settling does.
    attached = attach_task_hub_run_acknowledgement(
        pool,
        receipt_id=first.id,
        task_id=task_id,
        workspace_id=workspace_id,
        run_id="run-1",
    )
    assert attached is not None and attached.status.value == "admitted"
    with pytest.raises(UniqueViolation):
        _create_receipt(pool, workspace_id, task_id, session_id)

    settled = settle_task_hub_run_receipt(
        pool,
        receipt_id=first.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome={"settlement": "validated_result", "consumed_sequence": 7},
    )
    assert settled is not None and settled.status.value == "settled"
    assert get_outstanding_task_hub_run_receipt(pool, task_agent_session_id=session_id) is None
    second = _create_receipt(pool, workspace_id, task_id, session_id)
    assert second.id != first.id


def test_the_acknowledged_runid_binds_once_per_session(conn: Connection[Any]) -> None:
    task_id, workspace_id = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)

    first = _create_receipt(pool, workspace_id, task_id, session_id)
    attached = attach_task_hub_run_acknowledgement(
        pool,
        receipt_id=first.id,
        task_id=task_id,
        workspace_id=workspace_id,
        run_id="run-1",
    )
    assert attached is not None
    settle_task_hub_run_receipt(
        pool,
        receipt_id=first.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome={"settlement": "validated_result"},
    )
    second = _create_receipt(pool, workspace_id, task_id, session_id)
    # The same native run identity can never bind twice within one session.
    with pytest.raises(UniqueViolation):
        attach_task_hub_run_acknowledgement(
            pool,
            receipt_id=second.id,
            task_id=task_id,
            workspace_id=workspace_id,
            run_id="run-1",
        )
    rebound = attach_task_hub_run_acknowledgement(
        pool,
        receipt_id=second.id,
        task_id=task_id,
        workspace_id=workspace_id,
        run_id="run-2",
    )
    assert rebound is not None and rebound.run_id == "run-2"


def test_the_forward_only_transitions_are_one_shot_no_ops_when_raced(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)

    receipt = _create_receipt(pool, workspace_id, task_id, session_id)
    assert (
        attach_task_hub_run_acknowledgement(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            run_id="run-1",
        )
        is not None
    )
    # A replayed attach is a rejected no-op that rewrites nothing.
    assert (
        attach_task_hub_run_acknowledgement(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            run_id="run-2",
        )
        is None
    )
    assert (
        settle_task_hub_run_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome={"settlement": "validated_result"},
        )
        is not None
    )
    assert (
        settle_task_hub_run_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome={"settlement": "validated_result"},
        )
        is None
    )

    refused = _create_receipt(pool, workspace_id, task_id, session_id)
    settled_slot = attach_task_hub_run_acknowledgement(
        pool,
        receipt_id=refused.id,
        task_id=task_id,
        workspace_id=workspace_id,
        run_id="run-3",
    )
    assert settled_slot is not None
    # A refusal applies only to a claimed receipt.
    assert (
        refuse_task_hub_run_receipt(
            pool,
            receipt_id=refused.id,
            task_id=task_id,
            workspace_id=workspace_id,
            refusal_kind="hub_refused",
        )
        is None
    )


def test_the_staged_completion_is_unique_per_receipt_and_consumed_once(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)
    receipt = _create_receipt(pool, workspace_id, task_id, session_id)

    staged = stage_task_hub_run_early_completion(
        pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        kind=HubRunObservationKind.RUN_COMPLETED,
        sequence=41,
        result_candidate='{"type": "implementation_result"}',
    )
    assert staged.sequence == 41
    # A second terminal delivery for the same single outstanding run is a
    # duplicate, never a second staged row.
    with pytest.raises(UniqueViolation):
        stage_task_hub_run_early_completion(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            kind=HubRunObservationKind.RUN_FAILED,
            sequence=42,
            result_candidate=None,
        )
    locked = get_task_hub_run_early_completion_for_update(pool, receipt_id=receipt.id)
    assert locked is not None and locked.id == staged.id
    assert consume_task_hub_run_early_completion(
        pool, completion_id=staged.id, receipt_id=receipt.id
    )
    # The exactly-once consumption: a second delete is a rejected no-op.
    assert not consume_task_hub_run_early_completion(
        pool, completion_id=staged.id, receipt_id=receipt.id
    )
    assert get_task_hub_run_early_completion_for_update(pool, receipt_id=receipt.id) is None


def test_the_subject_foreign_keys_reject_cross_task_subjects(conn: Connection[Any]) -> None:
    first_task, first_workspace = _insert_task_chain(conn)
    second_task, second_workspace = _insert_task_chain(conn)
    second_session = _insert_producer_session(conn, second_workspace, second_task)
    pool = _SingleConnectionPool(conn)
    # A receipt referencing another Task's session identity is impossible.
    with pytest.raises(ForeignKeyViolation):
        create_task_hub_run_receipt(
            pool,
            workspace_id=first_workspace,
            task_id=first_task,
            task_agent_session_id=second_session,
            operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=uuid.uuid4(),
            expected_formal_family="implementation_result",
        )


def test_the_contradiction_marker_blocks_settlement(conn: Connection[Any]) -> None:
    task_id, workspace_id = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)
    receipt = _create_receipt(pool, workspace_id, task_id, session_id)
    attach_task_hub_run_acknowledgement(
        pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        run_id="run-1",
    )
    marked = mark_task_hub_run_receipt_contradicted(
        pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
    )
    assert marked is not None and marked.contradicted_at is not None
    # A repeated contradiction is a rejected no-op; the marker is set once.
    assert (
        mark_task_hub_run_receipt_contradicted(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
        )
        is None
    )
    # A contradicted correlation can never settle: the settlement CAS refuses
    # it, so the outstanding run stays honestly unresolved.
    assert (
        settle_task_hub_run_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            outcome={"settlement": "validated_result"},
        )
        is None
    )


def test_the_coherence_and_outcome_label_checks_reject_incoherent_rows(
    conn: Connection[Any],
) -> None:
    task_id, workspace_id = _insert_task_chain(conn)
    session_id = _insert_producer_session(conn, workspace_id, task_id)
    pool = _SingleConnectionPool(conn)
    receipt = _create_receipt(pool, workspace_id, task_id, session_id)
    # An admitted status without a run identity violates the per-status
    # coherence CHECK (the savepoint keeps the shared test transaction usable).
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.task_hub_run_receipts set status = 'admitted' where id = %s",
            (receipt.id,),
        )
    attach_task_hub_run_acknowledgement(
        pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        run_id="run-1",
    )
    # A settled receipt must carry its settlement label.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.task_hub_run_receipts set status = 'settled', "
            "settled_at = now() where id = %s",
            (receipt.id,),
        )
    settled = settle_task_hub_run_receipt(
        pool,
        receipt_id=receipt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome={"settlement": "validated_result"},
    )
    assert settled is not None and settled.outcome["settlement"] == "validated_result"
