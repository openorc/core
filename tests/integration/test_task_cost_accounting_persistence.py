"""Durable cost-accounting facts and the atomic cost-limit block (issue #240).

The targeted real-Postgres checks the Pre-F cost-accounting seam owns:

- the additive migrations carry their durable constraints: the
  nullable-positive Workspace default and Task authorized budgets, the
  coherent nonnegative session cost checkpoint pair, and the additive
  ``cost_limit`` TaskBlock reason;
- the exact-scope high-water session checkpoint accepts only a strictly
  greater amount for the exact binding tuple and is a truthful no-op for
  lower, equal, and foreign observations;
- the retained-cost aggregate is role-agnostic and sums every
  TaskAgentSession matching the Task by ownership only;
- the Owner-authorized ``COST_LIMIT`` block composes atomically through
  the shared E10b machinery; a forced failure rolls the whole decision
  back; an already-blocked Task never receives a duplicate block, duplicate
  intent, or new interrupt.

No live GitHub, runtime, or Supabase access; the suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection

from openorc.domain.blocks import TaskBlockReason
from openorc.persistence import (
    sessions as session_repositories,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.task_abort import (
    CostLimitBlockOutcomeKind,
    establish_owner_authorized_cost_limit_block,
)
from tests.integration.test_task_planning_gate_change_request_persistence import (
    _insert_connection,
    _insert_installation,
    _insert_profile,
    _insert_ready_session,
    _insert_repository,
    _insert_task,
    _insert_workspace,
    _SingleConnectionPool,
)

pytestmark = pytest.mark.integration

_BUDGET = Decimal("25.00")


def _budget_task(conn: Connection[Any], *, task_id: uuid.UUID, budget: Decimal | None) -> None:
    conn.execute(
        "update openorc.tasks set authorized_task_budget_usd = %s where id = %s",
        (budget, task_id),
    )


def _insert_extra_task(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int,
) -> uuid.UUID:
    """One additional Task on the same repository with its own stable issue identity."""
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, github_issue_id, github_issue_id % 1000, "a" * 64),
    )
    return task_id


def _insert_open_execution(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    producer_session_id: uuid.UUID,
) -> uuid.UUID:
    execution_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.executions "
        "(id, workspace_id, task_id, producer_session_id, execution_number, status) "
        "values (%s, %s, %s, %s, 1, 'running')",
        (execution_id, workspace_id, task_id, producer_session_id),
    )
    return execution_id


def _scalar(conn: Connection[Any], sql: LiteralString, params: tuple[Any, ...]) -> Any:
    """One scalar durable read with an asserted row, for state assertions.

    ``sql`` is repository/test-controlled literal SQL (the driver's
    ``LiteralString`` injection-safe query contract), never external input.
    """
    row = conn.execute(sql, params).fetchone()
    assert row is not None
    return row[0]


def _seed_budgeted_task(
    conn: Connection[Any],
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID]:
    """One Workspace, one budgeted ``planning`` Task, one ready Producer session."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    _budget_task(conn, task_id=task_id, budget=_BUDGET)
    producer_session_id = _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id="external-producer-session",
    )
    return workspace_id, task_id, connection_id, producer_session_id


class TestMigrationConstraints:
    def test_the_workspace_default_budget_is_nullable_and_strictly_positive(
        self, conn: Connection[Any]
    ) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        conn.execute(
            "update openorc.workspaces set default_task_budget_usd = %s where id = %s",
            (_BUDGET, workspace_id),
        )
        row = conn.execute(
            "select default_task_budget_usd from openorc.workspaces where id = %s",
            (workspace_id,),
        ).fetchone()
        assert row is not None and row[0] == _BUDGET
        # Zero and negative values are configuration errors, not "unlimited".
        for bad in (Decimal("0.00"), Decimal("-1.00")):
            with pytest.raises(Exception):  # noqa: B017, PT011 - durable CHECK rejection
                conn.execute(
                    "update openorc.workspaces set default_task_budget_usd = %s where id = %s",
                    (bad, workspace_id),
                )

    def test_the_task_authorized_budget_rejects_a_never_started_value(
        self, conn: Connection[Any]
    ) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
        conn.execute("update openorc.tasks set status = 'ready_to_plan' where id = %s", (task_id,))
        # A ready_to_plan row cannot carry an authorization: the capture
        # commits only together with the guarded start transition.
        with pytest.raises(Exception):  # noqa: B017, PT011 - durable CHECK rejection
            conn.execute(
                "update openorc.tasks set authorized_task_budget_usd = %s where id = %s",
                (_BUDGET, task_id),
            )

    def test_the_session_cost_checkpoint_is_a_coherent_nonnegative_pair(
        self, conn: Connection[Any]
    ) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        connection_id = _insert_connection(conn, workspace_id=workspace_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
        session_id = _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            connection_id=connection_id,
            role="producer",
            external_session_id="external-producer-session",
        )
        # Both NULL is the ordinary unobserved state; a zero reading is a
        # legitimate reported value, and a negative one is not.
        conn.execute(
            "update openorc.task_agent_sessions "
            "set retained_cost_usd = %s, cost_observed_at = now() where id = %s",
            (Decimal("0.00"), session_id),
        )
        with pytest.raises(Exception):  # noqa: B017, PT011 - durable CHECK rejection
            conn.execute(
                "update openorc.task_agent_sessions "
                "set retained_cost_usd = %s, cost_observed_at = now() where id = %s",
                (Decimal("-0.01"), session_id),
            )

    def test_cost_limit_is_a_durable_block_reason(self, conn: Connection[Any]) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
        conn.execute(
            "insert into openorc.task_blocks (workspace_id, task_id, reason, context) "
            "values (%s, %s, 'cost_limit', '{}'::jsonb)",
            (workspace_id, task_id),
        )
        with pytest.raises(Exception):  # noqa: B017, PT011 - durable CHECK rejection
            conn.execute(
                "insert into openorc.task_blocks (workspace_id, task_id, reason, context) "
                "values (%s, %s, 'review_iteration_limit', '{}'::jsonb)",
                (workspace_id, task_id),
            )


class TestSessionCostCheckpoint:
    def test_the_high_water_checkpoint_rejects_lower_equal_and_foreign_observations(
        self, conn: Connection[Any]
    ) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        connection_id = _insert_connection(conn, workspace_id=workspace_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
        session_id = _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            connection_id=connection_id,
            role="producer",
            external_session_id="external-producer-session",
        )
        pool = cast(DatabasePool, _SingleConnectionPool(conn))

        first = session_repositories.checkpoint_task_agent_session_cost(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=session_id,
            connection_id=connection_id,
            retained_cost_usd=Decimal("1.50"),
        )
        assert first is not None and first.retained_cost_usd == Decimal("1.50")
        assert first.cost_observed_at is not None

        # A lower (Hub-reset) or equal reading never rewrites the high
        # water and never restamps the observation instant.
        for not_higher in (Decimal("1.50"), Decimal("0.50")):
            assert (
                session_repositories.checkpoint_task_agent_session_cost(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    task_agent_session_id=session_id,
                    connection_id=connection_id,
                    retained_cost_usd=not_higher,
                )
                is None
            )
        reloaded = conn.execute(
            "select retained_cost_usd, cost_observed_at from openorc.task_agent_sessions "
            "where id = %s",
            (session_id,),
        ).fetchone()
        assert reloaded is not None
        assert reloaded[0] == Decimal("1.50")
        assert reloaded[1] is not None

        # A strictly greater reading advances the high water.
        higher = session_repositories.checkpoint_task_agent_session_cost(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=session_id,
            connection_id=connection_id,
            retained_cost_usd=Decimal("2.00"),
        )
        assert higher is not None and higher.retained_cost_usd == Decimal("2.00")

        # Foreign observations (cross-Task) match no row and write nothing.
        other_task = _insert_extra_task(
            conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7703
        )
        foreign = session_repositories.checkpoint_task_agent_session_cost(
            pool,
            workspace_id=workspace_id,
            task_id=other_task,
            task_agent_session_id=session_id,
            connection_id=connection_id,
            retained_cost_usd=Decimal("99.00"),
        )
        assert foreign is None


class TestRoleAgnosticAggregation:
    def test_the_aggregate_sums_every_task_session_by_ownership_only(
        self, conn: Connection[Any]
    ) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        connection_id = _insert_connection(conn, workspace_id=workspace_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
        producer_session_id = _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            connection_id=connection_id,
            role="producer",
            external_session_id="external-producer-session",
        )
        reviewer_session_id = _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            connection_id=connection_id,
            role="reviewer",
            external_session_id="external-reviewer-session",
        )
        pool = cast(DatabasePool, _SingleConnectionPool(conn))
        for session_id, amount in (
            (producer_session_id, Decimal("10.25")),
            (reviewer_session_id, Decimal("5.10")),
        ):
            assert (
                session_repositories.checkpoint_task_agent_session_cost(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    task_agent_session_id=session_id,
                    connection_id=connection_id,
                    retained_cost_usd=amount,
                )
                is not None
            )
        # A third binding row on a foreign Task is never added: ownership is
        # the Task, not a role list. (The v1 durable schema admits exactly
        # one binding per (task, role), so cross-role coverage here is the
        # role-agnosticism proof; the aggregation statement itself carries
        # no role predicate anywhere.)
        other_task = _insert_extra_task(
            conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7704
        )
        foreign_session = _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=other_task,
            connection_id=connection_id,
            role="producer",
            external_session_id="foreign-session",
        )
        assert (
            session_repositories.checkpoint_task_agent_session_cost(
                pool,
                workspace_id=workspace_id,
                task_id=other_task,
                task_agent_session_id=foreign_session,
                connection_id=connection_id,
                retained_cost_usd=Decimal("99.00"),
            )
            is not None
        )

        costs = session_repositories.sum_task_retained_session_costs(pool, task_id=task_id)
        assert costs.known_total_usd == Decimal("15.35")
        assert costs.has_unobserved_sessions is False

    def test_unknown_constituents_stay_unknown_and_never_zero(self, conn: Connection[Any]) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        connection_id = _insert_connection(conn, workspace_id=workspace_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
        producer_session_id = _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            connection_id=connection_id,
            role="producer",
            external_session_id="external-producer-session",
        )
        _insert_ready_session(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            connection_id=connection_id,
            role="reviewer",
            external_session_id="external-reviewer-session",
        )
        pool = cast(DatabasePool, _SingleConnectionPool(conn))
        assert (
            session_repositories.checkpoint_task_agent_session_cost(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session_id,
                connection_id=connection_id,
                retained_cost_usd=Decimal("3.00"),
            )
            is not None
        )

        costs = session_repositories.sum_task_retained_session_costs(pool, task_id=task_id)
        assert costs.known_total_usd == Decimal("3.00")
        # The unobserved Reviewer binding keeps the aggregate truthful: it
        # is a known lower bound with unknown constituents, never billing.
        assert costs.has_unobserved_sessions is True


class TestAtomicCostLimitBlock:
    def test_the_cost_limit_block_composes_atomically_with_owner_attribution(
        self, conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workspace_id, task_id, connection_id, producer_session_id = _seed_budgeted_task(conn)
        owner_profile_id = _scalar(
            conn,
            "select owner_profile_id from openorc.workspaces where id = %s",
            (workspace_id,),
        )
        producer_external = "external-producer-session"
        _insert_open_execution(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            producer_session_id=producer_session_id,
        )
        pool = cast(DatabasePool, _SingleConnectionPool(conn))
        assert (
            session_repositories.checkpoint_task_agent_session_cost(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session_id,
                connection_id=connection_id,
                retained_cost_usd=Decimal("25.00"),
            )
            is not None
        )
        token_before = _scalar(
            conn, "select state_token from openorc.tasks where id = %s", (task_id,)
        )

        result = establish_owner_authorized_cost_limit_block(
            pool, workspace_id=workspace_id, task_id=task_id
        )

        assert result.kind is CostLimitBlockOutcomeKind.BLOCKED_WITH_INTERRUPTS
        assert result.task is not None and result.task.status.value == "blocked"
        assert result.task_block is not None
        assert result.task_block.reason is TaskBlockReason.COST_LIMIT
        assert len(result.interrupt_dispatches) == 1
        intent, external_session_id = result.interrupt_dispatches[0]
        assert external_session_id == producer_external
        assert intent.requested_by_profile_id == owner_profile_id
        assert (
            _scalar(conn, "select state_token from openorc.tasks where id = %s", (task_id,))
            != token_before
        )

        # The committed facts: one current COST_LIMIT block with the safe
        # owner_abort context, one OWNER task_blocked audit, one unsettled
        # exact-session intent requested by the DB-derived Workspace Owner.
        block_row = conn.execute(
            "select reason, context, resolved_at from openorc.task_blocks where task_id = %s",
            (task_id,),
        ).fetchall()
        assert len(block_row) == 1
        assert block_row[0][0] == "cost_limit"
        assert block_row[0][1]["control"] == "owner_abort"
        assert block_row[0][1]["authorized_budget_usd"] == str(_BUDGET)
        assert block_row[0][2] is None
        event_row = conn.execute(
            "select actor_type, actor_id from openorc.workflow_events "
            "where task_id = %s and event_type = 'task_blocked'",
            (task_id,),
        ).fetchall()
        assert event_row == [("owner", str(owner_profile_id))]
        intent_row = conn.execute(
            "select requested_by_profile_id, status from openorc.task_interrupt_intents "
            "where task_id = %s",
            (task_id,),
        ).fetchall()
        assert intent_row == [(owner_profile_id, "unsettled")]

        # A replay (concurrent worker, restarted callback) never duplicates
        # the block, the intent, or the audit.
        replay = establish_owner_authorized_cost_limit_block(
            pool, workspace_id=workspace_id, task_id=task_id
        )
        assert replay.kind is CostLimitBlockOutcomeKind.ALREADY_BLOCKED
        assert replay.interrupt_dispatches == ()
        assert (
            _scalar(conn, "select count(*) from openorc.task_blocks where task_id = %s", (task_id,))
            == 1
        )
        assert (
            _scalar(
                conn,
                "select count(*) from openorc.task_interrupt_intents where task_id = %s",
                (task_id,),
            )
            == 1
        )
        assert (
            _scalar(
                conn,
                "select count(*) from openorc.workflow_events where task_id = %s "
                "and event_type = 'task_blocked'",
                (task_id,),
            )
            == 1
        )

    def test_an_idle_task_blocks_without_gratuitous_intents(self, conn: Connection[Any]) -> None:
        workspace_id, task_id, connection_id, producer_session_id = _seed_budgeted_task(conn)
        pool = cast(DatabasePool, _SingleConnectionPool(conn))
        assert (
            session_repositories.checkpoint_task_agent_session_cost(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session_id,
                connection_id=connection_id,
                retained_cost_usd=Decimal("25.00"),
            )
            is not None
        )

        result = establish_owner_authorized_cost_limit_block(
            pool, workspace_id=workspace_id, task_id=task_id
        )

        # No durably demonstrable in-flight runtime activity: the block
        # composes with zero interrupt intents — no gratuitous Interrupt.
        assert result.kind is CostLimitBlockOutcomeKind.BLOCKED_IDLE
        assert result.interrupt_dispatches == ()
        assert (
            _scalar(
                conn,
                "select count(*) from openorc.task_interrupt_intents where task_id = %s",
                (task_id,),
            )
            == 0
        )
        assert (
            _scalar(conn, "select status from openorc.tasks where id = %s", (task_id,)) == "blocked"
        )

    def test_a_forced_failure_rolls_back_the_whole_decision(
        self, conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workspace_id, task_id, connection_id, producer_session_id = _seed_budgeted_task(conn)
        pool = cast(DatabasePool, _SingleConnectionPool(conn))
        assert (
            session_repositories.checkpoint_task_agent_session_cost(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session_id,
                connection_id=connection_id,
                retained_cost_usd=Decimal("25.00"),
            )
            is not None
        )
        token_before = _scalar(
            conn, "select state_token from openorc.tasks where id = %s", (task_id,)
        )

        def _explode(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("forced audit failure")

        monkeypatch.setattr(event_coordination, "record_task_blocked_event", _explode)
        with pytest.raises(RuntimeError, match="forced audit failure"):
            establish_owner_authorized_cost_limit_block(
                pool, workspace_id=workspace_id, task_id=task_id
            )

        # The whole composition rolled back: no block, no status rotation,
        # no audit, no intent — and the checkpointed cost fact is the only
        # surviving write of the earlier transaction.
        monkeypatch.undo()
        assert (
            _scalar(conn, "select count(*) from openorc.task_blocks where task_id = %s", (task_id,))
            == 0
        )
        assert (
            _scalar(
                conn,
                "select count(*) from openorc.task_interrupt_intents where task_id = %s",
                (task_id,),
            )
            == 0
        )
        task_row = conn.execute(
            "select status, state_token, authorized_task_budget_usd from openorc.tasks "
            "where id = %s",
            (task_id,),
        ).fetchone()
        assert task_row is not None
        assert task_row[0] == "planning"
        assert task_row[1] == token_before
        assert task_row[2] == _BUDGET

    def test_disabled_archived_and_thresholdless_subjects_are_no_ops(
        self, conn: Connection[Any]
    ) -> None:
        profile_id = _insert_profile(conn)
        workspace_id = _insert_workspace(conn, profile_id)
        installation_id = _insert_installation(conn, workspace_id=workspace_id)
        repository_id = _insert_repository(
            conn, workspace_id=workspace_id, github_installation_id=installation_id
        )
        pool = cast(DatabasePool, _SingleConnectionPool(conn))

        # No standing authorization: no policy decision is ever invented.
        unauthorized_task = _insert_task(
            conn, workspace_id=workspace_id, repository_id=repository_id
        )
        assert (
            establish_owner_authorized_cost_limit_block(
                pool, workspace_id=workspace_id, task_id=unauthorized_task
            ).kind
            is CostLimitBlockOutcomeKind.NOT_APPLICABLE
        )

        # A foreign Task identity under the same lock is classified, never
        # guessed around.
        _budget_task(conn, task_id=unauthorized_task, budget=_BUDGET)
        foreign_workspace = _insert_workspace(conn, profile_id)
        assert (
            establish_owner_authorized_cost_limit_block(
                pool, workspace_id=foreign_workspace, task_id=unauthorized_task
            ).kind
            is CostLimitBlockOutcomeKind.NOT_APPLICABLE
        )

        # A threshold no longer true under the lock refuses to manufacture
        # a blocking decision from earlier, weaker observations.
        budgeted_task = _insert_extra_task(
            conn, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7705
        )
        _budget_task(conn, task_id=budgeted_task, budget=_BUDGET)
        assert (
            establish_owner_authorized_cost_limit_block(
                pool, workspace_id=workspace_id, task_id=budgeted_task
            ).kind
            is CostLimitBlockOutcomeKind.NOT_APPLICABLE
        )
