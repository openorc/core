"""Deterministic mapping tests for OwnerGate repositories (issue #24).

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the planning/review mapping fakes)
to prove row-to-domain-object mapping, UTC normalization at the persistence
boundary, parameterization, the exact-subject validation before SQL (the
pre-PR PR_AUTHORIZATION head subject, the TaskPullRequest plus exact head
SHA merge-decision subject, and the single-subject REVIEW_RESOLUTION), and —
above all — the shape of the authoritative resolution transaction: the
Task row locked FOR UPDATE before any gate write, the currency/token/
non-archived verification under that lock, the one-shot guarded gate
UPDATE, and the atomic pointer clear + token rotation. Database constraint
behavior is proven against a real database by the integration-marked suite
in ``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.gates import OwnerGateDomainError, OwnerGateStatus, OwnerGateType
from openorc.persistence import gates as gates_module
from openorc.persistence.gates import (
    OwnerGateResolutionOutcome,
    create_owner_gate,
    get_owner_gate,
    list_task_owner_gates,
    resolve_owner_gate,
)
from openorc.persistence.pool import DatabasePool


class FakeCursor:
    """Returns one canned row (and optional canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    """Records executed SQL and returns canned rows, optionally per statement."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        responses: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        # When supplied, each execute() consumes the next canned response —
        # this models multi-statement repositories such as the gate
        # resolution (gate read, Task FOR UPDATE lock read, guarded gate
        # UPDATE, then the guarded Task pointer clear).
        self.responses = list(responses) if responses is not None else None
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if self.responses is not None:
            response = self.responses.pop(0)
            return FakeCursor(response, None if response is None else [response])
        return FakeCursor(self.row, self.rows)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[FakeConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("mapping tests never close pools")


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _gate_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "gate_type": "implementation_authorization",
        "status": "pending",
        "plan_revision_id": uuid.uuid4(),
        "subject_head_sha": None,
        "task_pull_request_id": None,
        "decided_at": None,
        "created_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["gate_type"],
        values["status"],
        values["plan_revision_id"],
        values["subject_head_sha"],
        values["task_pull_request_id"],
        values["decided_at"],
        values["created_at"],
    )


def _task_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "repository_id": uuid.uuid4(),
        "github_issue_id": 24,
        "github_issue_number": 24,
        "status": "waiting_for_owner",
        "archived_at": None,
        "canonical_feature_branch": None,
        "state_token": uuid.uuid4(),
        "current_plan_revision_id": None,
        "current_owner_gate_id": None,
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["repository_id"],
        values["github_issue_id"],
        values["github_issue_number"],
        values["status"],
        values["archived_at"],
        values["canonical_feature_branch"],
        values["state_token"],
        values["current_plan_revision_id"],
        values["current_owner_gate_id"],
        values["created_at"],
        values["updated_at"],
    )


def test_create_owner_gate_inserts_and_maps_the_pending_row() -> None:
    row = _gate_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    gate = create_owner_gate(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=row[5],
    )

    assert gate.id == row[0]
    assert gate.workspace_id == row[1]
    assert gate.task_id == row[2]
    assert gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.plan_revision_id == row[5]
    assert gate.subject_head_sha is None
    assert gate.task_pull_request_id is None
    assert gate.decided_at is None
    assert gate.created_at == _utc_observed_at()
    assert gate.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.owner_gates" in sql
    # The lifecycle columns carry no database default: the initial pending
    # status is explicit in the creation statement.
    assert "workspace_id, task_id, gate_type, status," in sql
    assert "implementation_authorization" not in sql  # enum values are parameters
    assert params == (
        row[1],
        row[2],
        "implementation_authorization",
        "pending",
        row[5],
        None,
        None,
    )
    assert len(fake_conn.executed) == 1


def test_merge_decision_binds_the_task_pull_request_plus_head_sha() -> None:
    # The PR-subject gate form (issue #25): a merge_decision binds the
    # canonical TaskPullRequest plus the exact head SHA — persisted as the
    # durable subject, never as a bare head SHA.
    pr_id = uuid.uuid4()
    head = "0123456789abcdef0123456789abcdef01234567"
    row = _gate_row(
        gate_type="merge_decision",
        plan_revision_id=None,
        subject_head_sha=head,
        task_pull_request_id=pr_id,
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    gate = create_owner_gate(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        gate_type=OwnerGateType.MERGE_DECISION,
        subject_head_sha=head,
        task_pull_request_id=pr_id,
    )

    assert gate.gate_type is OwnerGateType.MERGE_DECISION
    assert gate.task_pull_request_id == pr_id
    assert gate.subject_head_sha == head
    assert gate.plan_revision_id is None
    sql, params = fake_conn.executed[0]
    assert "subject_head_sha, task_pull_request_id)" in sql
    assert params == (row[1], row[2], "merge_decision", "pending", None, head, pr_id)


def test_pr_subject_gate_shapes_are_validated_before_sql() -> None:
    # Subject-shape caller errors never reach SQL (issue #25):
    # MERGE_DECISION requires the TaskPullRequest plus the exact head SHA
    # (never a bare head SHA, never a PlanRevision), and PR_AUTHORIZATION
    # deliberately carries no PR binding — it happens before the canonical
    # PR exists.
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    base: dict[str, Any] = {"workspace_id": uuid.uuid4(), "task_id": uuid.uuid4()}
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(pool, gate_type=OwnerGateType.MERGE_DECISION, **base)
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.MERGE_DECISION,
            subject_head_sha="0123456789abcdef0123456789abcdef01234567",
            **base,
        )
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.MERGE_DECISION,
            plan_revision_id=uuid.uuid4(),
            subject_head_sha="0123456789abcdef0123456789abcdef01234567",
            task_pull_request_id=uuid.uuid4(),
            **base,
        )
    # PR_AUTHORIZATION keeps its pre-PR subject: head SHA only, and a PR
    # binding on it is a caller error.
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.PR_AUTHORIZATION,
            subject_head_sha="0123456789abcdef0123456789abcdef01234567",
            task_pull_request_id=uuid.uuid4(),
            **base,
        )
    assert fake_conn.executed == []


def test_create_owner_gate_validates_type_and_subject_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    base: dict[str, Any] = {"workspace_id": uuid.uuid4(), "task_id": uuid.uuid4()}
    # Wrong vocabulary.
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(pool, gate_type="chat_decision", **base)  # type: ignore[arg-type]
    # implementation_authorization requires exactly the plan-revision subject.
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(pool, gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION, **base)
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
            subject_head_sha="abc",
            **base,
        )
    # pr_authorization requires exactly the head-SHA subject.
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(pool, gate_type=OwnerGateType.PR_AUTHORIZATION, **base)
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.PR_AUTHORIZATION,
            plan_revision_id=uuid.uuid4(),
            subject_head_sha="abc",
            **base,
        )
    # merge_decision requires exactly the TaskPullRequest plus head SHA.
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(pool, gate_type=OwnerGateType.MERGE_DECISION, **base)
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.MERGE_DECISION,
            subject_head_sha="abc",
            **base,
        )
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.MERGE_DECISION,
            plan_revision_id=uuid.uuid4(),
            subject_head_sha="abc",
            task_pull_request_id=uuid.uuid4(),
            **base,
        )
    # PR_AUTHORIZATION deliberately carries no PR binding (pre-PR gate).
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.PR_AUTHORIZATION,
            subject_head_sha="abc",
            task_pull_request_id=uuid.uuid4(),
            **base,
        )
    # review_resolution requires exactly one subject.
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(pool, gate_type=OwnerGateType.REVIEW_RESOLUTION, **base)
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.REVIEW_RESOLUTION,
            plan_revision_id=uuid.uuid4(),
            subject_head_sha="abc",
            **base,
        )
    with pytest.raises(OwnerGateDomainError):
        create_owner_gate(
            pool,
            gate_type=OwnerGateType.REVIEW_RESOLUTION,
            subject_head_sha="abc",
            **base,
        )
    assert fake_conn.executed == []


def test_get_owner_gate_maps_or_returns_none() -> None:
    row = _gate_row()
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    gate = get_owner_gate(pool, owner_gate_id=row[0])
    assert gate is not None
    assert gate.id == row[0]
    assert gate.created_at == _utc_observed_at()

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_owner_gate(empty_pool, owner_gate_id=row[0]) is None


def test_list_task_owner_gates_orders_by_creation() -> None:
    first = _gate_row(status="approved", decided_at=_observed_at())
    second = _gate_row(task_id=first[2], workspace_id=first[1])
    fake_conn = FakeConnection(rows=[first, second])
    pool = cast(DatabasePool, FakePool(fake_conn))

    gates = list_task_owner_gates(pool, task_id=first[2])

    assert [gate.id for gate in gates] == [first[0], second[0]]
    sql, params = fake_conn.executed[0]
    assert "openorc.owner_gates" in sql
    assert "order by created_at, id" in sql
    assert params == (first[2],)


def test_the_module_surface_carries_the_resolution_contract() -> None:
    assert set(gates_module.__all__) == {
        "OwnerGateResolution",
        "OwnerGateResolutionOutcome",
        "create_owner_gate",
        "get_owner_gate",
        "list_task_owner_gates",
        "resolve_owner_gate",
    }


def _resolved_gate_row(base: tuple[Any, ...], outcome: OwnerGateStatus) -> tuple[Any, ...]:
    row = list(base)
    row[4] = outcome.value
    row[8] = _observed_at()
    return tuple(row)


def _resolved_task_row(base: tuple[Any, ...]) -> tuple[Any, ...]:
    row = list(base)
    row[10] = None  # current_owner_gate_id cleared
    row[8] = uuid.uuid4()  # state_token rotated
    row[12] = _observed_at()  # updated_at advanced
    return tuple(row)


def test_resolve_owner_gate_success_applies_one_atomic_task_transition() -> None:
    gate_row = _gate_row()
    task_row = _task_row(
        id=gate_row[2],
        workspace_id=gate_row[1],
        status="waiting_for_owner",
        current_owner_gate_id=gate_row[0],
    )
    resolved_gate_row = _resolved_gate_row(gate_row, OwnerGateStatus.APPROVED)
    cleared_task_row = _resolved_task_row(task_row)
    fake_conn = FakeConnection(responses=[gate_row, task_row, resolved_gate_row, cleared_task_row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    resolution = resolve_owner_gate(
        pool,
        owner_gate_id=gate_row[0],
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=task_row[8],
    )

    assert resolution.outcome is OwnerGateResolutionOutcome.RESOLVED
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.APPROVED
    assert resolution.gate.decided_at == _utc_observed_at()
    assert resolution.task is not None
    assert resolution.task.current_owner_gate_id is None
    assert resolution.task.state_token == cleared_task_row[8]
    assert resolution.task.state_token != task_row[8]

    # The transaction shape: gate read, Task locked FOR UPDATE, one-shot
    # guarded gate UPDATE, then the atomic pointer clear + token rotation.
    assert len(fake_conn.executed) == 4
    gate_read_sql, _ = fake_conn.executed[0]
    assert "from openorc.owner_gates where id = %s" in gate_read_sql
    task_lock_sql, task_lock_params = fake_conn.executed[1]
    assert "from openorc.tasks" in task_lock_sql
    assert "for update" in task_lock_sql
    assert task_lock_params == (gate_row[2],)
    gate_update_sql, gate_update_params = fake_conn.executed[2]
    assert "update openorc.owner_gates" in gate_update_sql
    assert "decided_at = now()" in gate_update_sql
    assert "where id = %s and status = 'pending'" in gate_update_sql
    assert gate_update_params == ("approved", gate_row[0])
    task_update_sql, task_update_params = fake_conn.executed[3]
    assert "update openorc.tasks" in task_update_sql
    assert "current_owner_gate_id = null" in task_update_sql
    assert "state_token = gen_random_uuid(), updated_at = now()" in task_update_sql
    assert "and current_owner_gate_id = %s" in task_update_sql
    assert task_update_params == (gate_row[2], task_row[8], gate_row[0])


def test_resolve_owner_gate_treats_missing_gate_as_no_op() -> None:
    fake_conn = FakeConnection(responses=[None])
    pool = cast(DatabasePool, FakePool(fake_conn))
    resolution = resolve_owner_gate(
        pool,
        owner_gate_id=uuid.uuid4(),
        outcome=OwnerGateStatus.REJECTED,
        expected_task_state_token=uuid.uuid4(),
    )
    assert resolution.outcome is OwnerGateResolutionOutcome.NO_OP
    assert resolution.gate is None and resolution.task is None
    assert len(fake_conn.executed) == 1  # the gate read only; no writes


def test_resolve_owner_gate_treats_already_resolved_gate_as_one_shot_no_op() -> None:
    gate_row = _gate_row(status="approved", decided_at=_observed_at())
    fake_conn = FakeConnection(responses=[gate_row])
    pool = cast(DatabasePool, FakePool(fake_conn))
    resolution = resolve_owner_gate(
        pool,
        owner_gate_id=gate_row[0],
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=uuid.uuid4(),
    )
    assert resolution.outcome is OwnerGateResolutionOutcome.NO_OP
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.APPROVED
    assert resolution.task is None
    assert len(fake_conn.executed) == 1  # the gate read only; no rewrite path


def test_resolve_owner_gate_treats_superseded_gate_as_stale_without_writes() -> None:
    # A pending gate that is no longer the Task's current gate (superseded
    # or never installed) is a stale operation: nothing is applied to either
    # the gate or the Task, and the gate is never rewritten into an outcome.
    gate_row = _gate_row()
    task_row = _task_row(id=gate_row[2], workspace_id=gate_row[1], current_owner_gate_id=None)
    fake_conn = FakeConnection(responses=[gate_row, task_row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    resolution = resolve_owner_gate(
        pool,
        owner_gate_id=gate_row[0],
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=task_row[8],
    )

    assert resolution.outcome is OwnerGateResolutionOutcome.STALE
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.PENDING  # never rewritten
    assert resolution.task is None
    # Only the gate read and the Task lock read ran; neither UPDATE did.
    assert len(fake_conn.executed) == 2


def test_resolve_owner_gate_treats_stale_token_while_current_as_stale() -> None:
    # The gate is current, but the caller's expected Task token no longer
    # matches: a stale operation, applying nothing.
    gate_row = _gate_row()
    task_row = _task_row(
        id=gate_row[2], workspace_id=gate_row[1], current_owner_gate_id=gate_row[0]
    )
    fake_conn = FakeConnection(responses=[gate_row, task_row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    resolution = resolve_owner_gate(
        pool,
        owner_gate_id=gate_row[0],
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=uuid.uuid4(),  # stale token
    )

    assert resolution.outcome is OwnerGateResolutionOutcome.STALE
    assert resolution.gate is not None
    assert resolution.gate.status is OwnerGateStatus.PENDING
    assert resolution.task is None
    assert len(fake_conn.executed) == 2  # no writes


def test_resolve_owner_gate_treats_archived_task_as_stale() -> None:
    gate_row = _gate_row()
    task_row = _task_row(
        id=gate_row[2],
        workspace_id=gate_row[1],
        status="cancelled",
        archived_at=_observed_at(),
        current_owner_gate_id=gate_row[0],
    )
    fake_conn = FakeConnection(responses=[gate_row, task_row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    resolution = resolve_owner_gate(
        pool,
        owner_gate_id=gate_row[0],
        outcome=OwnerGateStatus.CANCELLED,
        expected_task_state_token=task_row[8],
    )

    assert resolution.outcome is OwnerGateResolutionOutcome.STALE
    assert resolution.task is None
    assert len(fake_conn.executed) == 2  # no writes


def test_resolve_owner_gate_requires_a_terminal_outcome_and_uuid_token() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    with pytest.raises(OwnerGateDomainError):
        resolve_owner_gate(
            pool,
            owner_gate_id=uuid.uuid4(),
            outcome=OwnerGateStatus.PENDING,
            expected_task_state_token=uuid.uuid4(),
        )
    with pytest.raises(OwnerGateDomainError):
        resolve_owner_gate(
            pool,
            owner_gate_id=uuid.uuid4(),
            outcome=OwnerGateStatus.APPROVED,
            expected_task_state_token="not-a-uuid",  # type: ignore[arg-type]
        )
    assert fake_conn.executed == []
