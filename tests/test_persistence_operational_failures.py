"""Deterministic tests for the F12 outstanding-retry lookup repository (issue #316).

The ordinary suite cannot execute Postgres; these tests use scripted SQL
handlers over a fake pool/connection seam to prove the bounded startup
recovery read's contract: the latest-attempt-per-episode enumeration over the
three supported retry families, the per-family outstanding predicate, keyset
paging by episode id, UTC normalization at the boundary, and the read-only
guarantee (no UPDATE/INSERT/DELETE is ever executed).

Database index behavior is covered by the migration convention test; real
database behavior for the surrounding ledger remains with the
integration-marked suites.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.domain.operational_failures import (
    GitHubMergeOperation,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.operational_failures import (
    list_outstanding_authorized_retry_attempts,
)
from openorc.persistence.pool import DatabasePool
from tests.test_persistence_github_issues import FakePool, ScriptedConnection

_OBSERVED = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
_UTC_OBSERVED = datetime(2026, 11, 22, 10, 0, 0, tzinfo=UTC)

_ATTEMPT_ID = uuid4()
_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_EPISODE_ID = uuid4()
_AFTER_EPISODE_ID = uuid4()


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _outstanding_row(
    *,
    attempt_id: Any = _ATTEMPT_ID,
    workspace_id: Any = _WORKSPACE_ID,
    task_id: Any = _TASK_ID,
    episode_id: Any = _EPISODE_ID,
    ordinal: int = 2,
    family: str = "implementation_execution",
    not_before: Any = _OBSERVED,
) -> tuple[Any, ...]:
    return (
        attempt_id,
        workspace_id,
        task_id,
        episode_id,
        ordinal,
        family,
        not_before,
    )


def test_the_lookup_enumerates_the_latest_outstanding_attempt_per_episode() -> None:
    """One bounded read maps each candidate row onto its domain read model."""
    conn = ScriptedConnection()
    conn.on(
        "from openorc.operational_failure_attempts",
        [
            _outstanding_row(),
            _outstanding_row(
                attempt_id=uuid4(),
                task_id=uuid4(),
                episode_id=uuid4(),
                family="exact_head_merge",
            ),
        ],
    )

    candidates = list_outstanding_authorized_retry_attempts(
        _pool(conn), limit=50
    )

    assert len(candidates) == 2
    first = candidates[0]
    assert first.attempt_id == _ATTEMPT_ID
    assert first.workspace_id == _WORKSPACE_ID
    assert first.task_id == _TASK_ID
    assert first.episode_id == _EPISODE_ID
    assert first.ordinal == 2
    assert (
        first.operation_family
        is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    )
    assert first.not_before == _UTC_OBSERVED  # normalized to timezone-aware UTC
    assert candidates[1].operation_family is GitHubMergeOperation.EXACT_HEAD_MERGE

    # The read never mutates the ledger.
    assert all(
        not sql.lstrip().lower().startswith(("update", "insert", "delete"))
        for sql, _params in conn.executed
    )


def test_the_lookup_scopes_the_enumeration_to_the_supported_families() -> None:
    """The family filter is the exact three-family roster with the per-family
    outstanding predicates; the paging cursor and limit travel as parameters."""
    conn = ScriptedConnection()
    conn.on("from openorc.operational_failure_attempts", [])

    list_outstanding_authorized_retry_attempts(_pool(conn), limit=25)

    (sql, params) = conn.executed[0]
    lowered = " ".join(sql.split()).lower()
    assert "escalated_task_block_id is null" in lowered
    assert "a.ordinal = (select max(a2.ordinal)" in lowered
    assert "a.resulting_execution_id is null" in lowered
    assert "a.consumed_at is null" in lowered
    assert params is not None
    assert list(params[0]) == [
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION.value,
        GitHubMergeOperation.EXACT_HEAD_MERGE.value,
        PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION.value,
    ]
    assert params[1] == "implementation_execution"
    assert params[2] == "exact_head_merge"
    assert params[3] == "exact_head_pr_authorization_presentation"
    assert params[4] is None  # no cursor: the first page
    assert params[5] is None
    assert params[6] == 25


def test_the_lookup_pages_by_episode_id_keyset() -> None:
    """``after_episode_id`` is the paging hint carried into the comparison."""
    conn = ScriptedConnection()
    conn.on("from openorc.operational_failure_attempts", [])

    list_outstanding_authorized_retry_attempts(
        _pool(conn), limit=10, after_episode_id=_AFTER_EPISODE_ID
    )

    (_sql, params) = conn.executed[0]
    assert params is not None
    assert params[4] == _AFTER_EPISODE_ID
    assert params[5] == _AFTER_EPISODE_ID
    assert params[6] == 10


def test_an_empty_enumeration_is_an_empty_pass() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.operational_failure_attempts", [])

    assert list_outstanding_authorized_retry_attempts(_pool(conn), limit=50) == []


@pytest.mark.parametrize("limit", [0, -1])
def test_a_non_positive_limit_fails_closed(limit: int) -> None:
    conn = ScriptedConnection()

    with pytest.raises(ValueError, match="positive limit"):
        list_outstanding_authorized_retry_attempts(_pool(conn), limit=limit)
