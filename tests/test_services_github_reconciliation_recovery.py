"""Deterministic fake-seam tests for the GitHub reconciliation/recovery sweep (issue #62).

Covers the required behavioral matrix: bounded current-work derivation (open
tracked projections unioned with current-Task issues; closed historical
issues and terminal-Task PR records never enter the sweep); stable-identity
tick bucketing (deterministic membership, disjoint buckets, exact rotation
coverage without starvation, the operator full sweep); the no-webhook
discovery scenarios (requirements drift against the immutable Task source
baseline, hierarchy change without any blocking consequence, dependency/
blocking change, missed branch head, missed PR head/state, merge, closed-
unmerged, exact-head checks projection); duplicate and out-of-order delivery
recovery convergence; unchanged re-sweep idempotence at canonical-state/
event level; per-unit failure isolation (known, uncertain, unexpected,
access loss); the bounded delivery-recovery batch; and the
no-database-transaction rule across every external invocation boundary (the
persistence reads run real repository statements against a scripted
connection while the fake pool tracks checked-out connections, and every
capability fake asserts none is open). No live GitHub, Valkey, or Postgres.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.config import Settings
from openorc.domain.github_issues import (
    GitHubIssueIdentity,
    GitHubIssueProjection,
    GitHubIssueState,
)
from openorc.domain.ownership import GitHubRepositoryIdentity, Repository, RepositoryMetadata
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.persistence.pool import DatabasePool
from openorc.services import (
    canonical_branch,
    commit_checks_projection,
    github_issue_relations,
    github_reconciliation,
    github_webhook_dispatch,
    task_pull_request_reconciliation,
)
from openorc.services.canonical_branch import CanonicalBranchObservation
from openorc.services.commit_checks_projection import CommitChecksProjection
from openorc.services.errors import (
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
)
from openorc.services.github_issue_relations import DependencySyncResult, HierarchySyncResult
from openorc.services.github_reconciliation import (
    RepositoryIssueReconciliation,
    RepositoryObservationReconciliation,
)
from openorc.services.github_reconciliation_recovery import (
    MAX_RECOVERY_DELIVERY_BATCH,
    GitHubReconciliationSweepFamily,
    GitHubReconciliationSweepResult,
    GitHubReconciliationSweepUnitStatus,
    run_github_reconciliation_sweep,
)
from openorc.services.github_webhook_dispatch import (
    GitHubWebhookDispatchResult,
    GitHubWebhookDispatchRouteOutcome,
    GitHubWebhookDispatchRouteStatus,
)
from openorc.services.task_pull_request_reconciliation import TaskPullRequestReconciliation

_NOW = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_A = uuid.uuid4()
_PROJECT_A = uuid.uuid4()
_REPOSITORY_A = uuid.uuid4()
_INSTALLATION_A = uuid.uuid4()
_GITHUB_REPOSITORY_ID = 456
_EXTERNAL_INSTALLATION_ID = 123
_TASK_A = uuid.uuid4()
_TOKEN_A = uuid.uuid4()
_FINGERPRINT_BASE = "a" * 64
_FINGERPRINT_NEW = "b" * 64
_HEAD_SHA_OLD = "0" * 40
_HEAD_SHA_NEW = "f" * 40

# The sanctioned safe attribute vocabulary (issue #108), derived from the
# boundary module itself: sweep spans may carry only these names.
_SANCTIONED_ATTRIBUTE_NAMES = frozenset(
    {
        "openorc.request_id",
        "openorc.workspace_id",
        "openorc.task_id",
        "openorc.execution_id",
        "openorc.connection_id",
        "openorc.workflow_role",
        "openorc.operation",
        "openorc.github_installation_id",
        "openorc.github_repository",
        "openorc.github_issue_number",
        "openorc.github_pull_request_number",
        "openorc.github_head_sha",
        "openorc.rq_job_id",
    }
)


class FakeCursor:
    """Returns one canned row and/or canned rows, like a psycopg cursor."""

    def __init__(
        self,
        row: tuple[Any, ...] | None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._rows is None:
            return None
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._rows is None:
            return []
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, tuple[Any, ...] | list[tuple[Any, ...]] | None]] = []

    def on(self, sql_marker: str, result: tuple[Any, ...] | list[tuple[Any, ...]] | None) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(result, list):
                    return FakeCursor(None, result)
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Nested psycopg transaction blocks (SAVEPOINTs under composition).
        yield


class FakePool:
    """Pool fake tracking checked-out connections for the transaction proof."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("github reconciliation sweep tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _github_client() -> Any:
    """A typed ``Any`` stand-in for the adapter the capability fakes replace."""
    return object()


class ScriptedCapability:
    """A composed-capability fake asserting no open transaction at the boundary.

    Returns its scripted results in deterministic call order and raises the
    scripted per-call errors, so sweep-unit isolation and failure handling
    are exercised against the real orchestration. A call beyond the scripted
    results fails loudly — an unexpected capability invocation can never
    pass silently.
    """

    def __init__(
        self,
        pool: FakePool,
        *,
        results: Sequence[Any] = (),
        errors: dict[int, Exception] | None = None,
    ) -> None:
        self._pool = pool
        self.results = list(results)
        self.errors = dict(errors or {})
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        assert self._pool.active_connections == 0, (
            "no database transaction may span the external reconciliation invocation"
        )
        index = len(self.calls)
        self.calls.append(dict(kwargs))
        error = self.errors.get(index)
        if error is not None:
            raise error
        if self.results:
            return self.results.pop(0)
        raise AssertionError(f"no scripted result for capability call {index}")


class Harness:
    """Scripted persistence plus scripted composed-capability fakes."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self.conn = conn
        self.pool_fake = FakePool(conn)
        self.pool = cast(DatabasePool, self.pool_fake)
        self.repository_observation = ScriptedCapability(self.pool_fake)
        self.issue_reconcile = ScriptedCapability(self.pool_fake)
        self.dependencies = ScriptedCapability(self.pool_fake)
        self.hierarchy = ScriptedCapability(self.pool_fake)
        self.branch_observation = ScriptedCapability(self.pool_fake)
        self.pull_request_reconcile = ScriptedCapability(self.pool_fake)
        self.checks_projection = ScriptedCapability(self.pool_fake)
        self.dispatch = ScriptedCapability(self.pool_fake)
        self.settings = _settings()


@pytest.fixture
def harness_factory(monkeypatch: pytest.MonkeyPatch):
    def _make(partitions: int = 16) -> Harness:
        harness = Harness(ScriptedConnection())
        harness.settings = _settings(partitions=partitions)
        # Every sweep invocation reconciles each configured repository's
        # metadata (always for the operator's full sweep; on bucket match
        # otherwise); the single-repository tests seed its benign unchanged
        # result by default, and a SECOND repository invocation still fails
        # loudly instead of passing silently.
        harness.repository_observation.results = [
            RepositoryObservationReconciliation(
                repository=_durable_repository(), metadata_changed=False
            )
        ]
        monkeypatch.setattr(
            github_reconciliation,
            "reconcile_repository_observation",
            harness.repository_observation,
        )
        monkeypatch.setattr(
            github_reconciliation, "reconcile_repository_issue", harness.issue_reconcile
        )
        monkeypatch.setattr(
            github_issue_relations,
            "synchronize_repository_issue_dependencies",
            harness.dependencies,
        )
        monkeypatch.setattr(
            github_issue_relations,
            "synchronize_repository_issue_hierarchy",
            harness.hierarchy,
        )
        monkeypatch.setattr(
            canonical_branch,
            "observe_bound_canonical_task_branch",
            harness.branch_observation,
        )
        monkeypatch.setattr(
            task_pull_request_reconciliation,
            "reconcile_task_pull_request",
            harness.pull_request_reconcile,
        )
        monkeypatch.setattr(
            commit_checks_projection, "project_commit_checks", harness.checks_projection
        )
        monkeypatch.setattr(
            github_webhook_dispatch, "dispatch_github_webhook_delivery", harness.dispatch
        )
        return harness

    return _make


def _settings(partitions: int = 16) -> Settings:
    return Settings(
        environment="test",
        api_host="127.0.0.1",
        api_port=3999,
        api_reload=False,
        valkey_url="redis://127.0.0.1:6379/0",
        database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        db_pool_min=1,
        db_pool_max=5,
        db_pool_timeout=5.0,
        github_reconciliation_sweep_partitions=partitions,
    )


def _repository_row(
    workspace_id: uuid.UUID = _WORKSPACE_A, repository_id: uuid.UUID = _REPOSITORY_A
) -> tuple[Any, ...]:
    return (
        repository_id,
        _PROJECT_A,
        workspace_id,
        _GITHUB_REPOSITORY_ID,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _NOW,
        _NOW,
        _INSTALLATION_A,
    )


def _task_row(
    *,
    task_id: uuid.UUID = _TASK_A,
    github_issue_id: int = 502,
    issue_number: int = 52,
    status: str = "implementing",
    canonical_feature_branch: str | None = None,
    fingerprint: str = _FINGERPRINT_BASE,
) -> tuple[Any, ...]:
    return (
        task_id,
        _WORKSPACE_A,
        _REPOSITORY_A,
        github_issue_id,
        issue_number,
        status,
        None,
        canonical_feature_branch,
        _TOKEN_A,
        None,
        None,
        fingerprint,
        _NOW,
        _NOW,
    )


def _open_issue_row(
    *,
    github_issue_id: int,
    issue_number: int,
    state: str = "open",
    fingerprint: str = _FINGERPRINT_BASE,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_A,
        _REPOSITORY_A,
        github_issue_id,
        issue_number,
        "Tracked issue",
        None,
        state,
        fingerprint,
        None,
        _NOW,
        _NOW,
    )


def _pull_request_row(
    *, task_id: uuid.UUID = _TASK_A, state: str = "closed", merged_at: Any = _NOW
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_A,
        task_id,
        _REPOSITORY_A,
        555,
        7,
        "openorc/task-1",
        "main",
        _HEAD_SHA_NEW,
        state,
        merged_at,
        _NOW,
        _NOW,
    )


def _delivery_row(
    *, delivery_guid: str = "guid-1", received_at: datetime = _NOW, processed_at: Any = None
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        delivery_guid,
        "issues",
        "edited",
        "relevant",
        "issue_state",
        "resolved",
        _EXTERNAL_INSTALLATION_ID,
        _GITHUB_REPOSITORY_ID,
        42,
        None,
        received_at,
        processed_at,
    )


def _durable_repository() -> Repository:
    return Repository(
        id=_REPOSITORY_A,
        project_id=_PROJECT_A,
        workspace_id=_WORKSPACE_A,
        identity=GitHubRepositoryIdentity(github_repository_id=_GITHUB_REPOSITORY_ID),
        metadata=RepositoryMetadata(
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        created_at=_NOW,
        updated_at=_NOW,
        github_installation_id=_INSTALLATION_A,
    )


def _durable_issue_projection(
    *,
    github_issue_id: int = 501,
    issue_number: int = 51,
    fingerprint: str = _FINGERPRINT_BASE,
    state: GitHubIssueState = GitHubIssueState.OPEN,
) -> GitHubIssueProjection:
    return GitHubIssueProjection(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_A,
        repository_id=_REPOSITORY_A,
        identity=GitHubIssueIdentity(github_issue_id=github_issue_id),
        issue_number=issue_number,
        title="Tracked issue",
        body=None,
        state=state,
        requirements_fingerprint=fingerprint,
        provider_updated_at=None,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _issue_reconciliation(
    *,
    fingerprint: str = _FINGERPRINT_NEW,
    created: bool = False,
    state_changed: bool = False,
    requirements_changed: bool = True,
    previous_fingerprint: str | None = _FINGERPRINT_BASE,
) -> RepositoryIssueReconciliation:
    return RepositoryIssueReconciliation(
        repository=_durable_repository(),
        repository_metadata_changed=False,
        issue=_durable_issue_projection(fingerprint=fingerprint),
        issue_created=created,
        issue_state_changed=state_changed,
        requirements_changed=requirements_changed,
        previous_fingerprint=previous_fingerprint,
    )


def _durable_task_pull_request(
    *,
    state: TaskPullRequestState = TaskPullRequestState.CLOSED,
    merged_at: datetime | None = _NOW,
) -> TaskPullRequest:
    return TaskPullRequest(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_A,
        task_id=_TASK_A,
        repository_id=_REPOSITORY_A,
        github_pr_id=555,
        github_pr_number=7,
        head_ref="openorc/task-1",
        base_ref="main",
        head_sha=_HEAD_SHA_NEW,
        state=state,
        merged_at=merged_at,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _pr_reconciliation(
    *,
    pull_request: TaskPullRequest | None = None,
    updated: bool = True,
    head_changed: bool = True,
    base_changed: bool = False,
    state_changed: bool = True,
    previous_head_sha: str | None = _HEAD_SHA_OLD,
    previous_state: TaskPullRequestState | None = TaskPullRequestState.OPEN,
) -> TaskPullRequestReconciliation:
    return TaskPullRequestReconciliation(
        pull_request=pull_request if pull_request is not None else _durable_task_pull_request(),
        updated=updated,
        head_changed=head_changed,
        base_changed=base_changed,
        state_changed=state_changed,
        previous_head_sha=previous_head_sha,
        previous_base_ref="main",
        previous_state=previous_state,
    )


def _checks_projection() -> CommitChecksProjection:
    return CommitChecksProjection(
        pull_request=_durable_task_pull_request(),
        head_sha=_HEAD_SHA_NEW,
        check_runs=(),
        combined_status_state="success",
        status_contexts=(),
    )


def _dispatch_result(
    *,
    processed: bool = True,
    route_status: GitHubWebhookDispatchRouteStatus | None = None,
) -> GitHubWebhookDispatchResult:
    return GitHubWebhookDispatchResult(
        delivery=None,
        processed=processed,
        route_outcomes=(
            ()
            if route_status is None
            else (
                GitHubWebhookDispatchRouteOutcome(
                    workspace_id=_WORKSPACE_A,
                    repository_id=_REPOSITORY_A,
                    status=route_status,
                ),
            )
        ),
    )


def _script_one_repo_sweep(harness: Harness, *, tick: int | None = None) -> None:
    """Script the durable reads of one full single-repository sweep invocation."""
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    # Family 2's bounded current-work issue set (open projections).
    conn.on("from openorc.github_issues", [])
    # Family 2's current Tasks.
    conn.on("from openorc.tasks", [])
    # Family 3 re-derives the current Tasks fresh.
    conn.on("from openorc.tasks", [])
    # Family 4's bounded delivery-recovery batch.
    conn.on("order by received_at, id limit %s", [])


def _assert_no_durable_writes(harness: Harness) -> None:
    """The sweep orchestration itself fabricates no durable state change.

    Every write belongs to a composed capability; the sweep's own SQL is
    exactly the deterministic work-set reads.
    """
    for sql, _ in harness.conn.executed:
        lowered = " ".join(sql.split()).lower()
        assert not any(
            keyword in lowered for keyword in ("insert into", "update ", "delete from")
        ), f"the sweep fabricated a durable write: {sql}"


def test_the_full_sweep_services_the_bounded_current_work_set(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    # One open tracked issue (501) plus one Task-backed issue (502) form the
    # issue work set. A closed historical issue with no current Task is in
    # neither source (it is not in the open listing and has no current Task),
    # so it can never enter the sweep.
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on(
        "from openorc.tasks",
        [
            _task_row(
                github_issue_id=502, issue_number=52, canonical_feature_branch="openorc/task-1"
            )
        ],
    )
    conn.on(
        "from openorc.tasks",
        [
            _task_row(
                github_issue_id=502, issue_number=52, canonical_feature_branch="openorc/task-1"
            )
        ],
    )
    # The current Task carries a bound canonical branch but no canonical PR.
    conn.on("where task_id = %s", None)
    conn.on("order by received_at, id limit %s", [])
    harness.repository_observation.results = [
        RepositoryObservationReconciliation(
            repository=_durable_repository(), metadata_changed=False
        )
    ]
    harness.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_BASE, requirements_changed=False),
        _issue_reconciliation(fingerprint=_FINGERPRINT_BASE, requirements_changed=False),
    ]
    harness.dependencies.results = [
        DependencySyncResult(blocked=False, changed=False),
        DependencySyncResult(blocked=False, changed=False),
    ]
    harness.hierarchy.results = [
        HierarchySyncResult(changed=False),
        HierarchySyncResult(changed=False),
    ]
    harness.branch_observation.results = [
        CanonicalBranchObservation(
            task_id=_TASK_A,
            workspace_id=_WORKSPACE_A,
            branch_name="openorc/task-1",
            head_sha=_HEAD_SHA_NEW,
        )
    ]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    assert result.tick is None
    assert result.partitions == 16
    assert [(outcome.family, outcome.status) for outcome in result.outcomes] == [
        (
            GitHubReconciliationSweepFamily.REPOSITORY,
            GitHubReconciliationSweepUnitStatus.RECONCILED,
        ),
        (GitHubReconciliationSweepFamily.ISSUE, GitHubReconciliationSweepUnitStatus.RECONCILED),
        (GitHubReconciliationSweepFamily.RELATIONS, GitHubReconciliationSweepUnitStatus.RECONCILED),
        (GitHubReconciliationSweepFamily.ISSUE, GitHubReconciliationSweepUnitStatus.RECONCILED),
        (GitHubReconciliationSweepFamily.RELATIONS, GitHubReconciliationSweepUnitStatus.RECONCILED),
        (GitHubReconciliationSweepFamily.BRANCH, GitHubReconciliationSweepUnitStatus.RECONCILED),
    ]
    # The issue work set is the open-projection/current-Task union, ordered by
    # stable identity, resolved to their durable address numbers.
    assert [call["issue_number"] for call in harness.issue_reconcile.calls] == [51, 52]
    assert [call["github_issue_id"] for call in harness.dependencies.calls] == [501, 502]
    assert [call["github_issue_id"] for call in harness.hierarchy.calls] == [501, 502]
    assert harness.branch_observation.calls == [{"workspace_id": _WORKSPACE_A, "task_id": _TASK_A}]
    # The current-work filters are the SQL predicates themselves.
    issues_sql = next(sql for sql, _ in conn.executed if "from openorc.github_issues" in sql)
    assert "state = %s" in issues_sql
    tasks_sql = next(sql for sql, _ in conn.executed if "from openorc.tasks" in sql)
    assert "archived_at is null" in tasks_sql
    _assert_no_durable_writes(harness)


def test_tick_bucketing_is_deterministic_disjoint_and_starvation_free(harness_factory) -> None:
    def _run(tick: int) -> list[int]:
        harness = harness_factory(partitions=2)
        conn = harness.conn
        conn.on("from openorc.repositories", [_repository_row()])
        conn.on(
            "from openorc.github_issues",
            [
                _open_issue_row(github_issue_id=issue_id, issue_number=issue_id)
                for issue_id in (1, 2, 3, 4)
            ],
        )
        conn.on("from openorc.tasks", [])
        conn.on("from openorc.tasks", [])
        conn.on("order by received_at, id limit %s", [])
        harness.issue_reconcile.results = [
            _issue_reconciliation(
                fingerprint=_FINGERPRINT_BASE,
                requirements_changed=False,
                previous_fingerprint=None,
            )
            for _ in range(4)
        ]
        harness.dependencies.results = [
            DependencySyncResult(blocked=False, changed=False) for _ in range(4)
        ]
        harness.hierarchy.results = [HierarchySyncResult(changed=False) for _ in range(4)]

        result = run_github_reconciliation_sweep(
            harness.pool, _github_client(), harness.settings, sweep_tick=tick
        )

        assert result.tick == tick
        return [
            outcome.github_issue_id
            for outcome in result.outcomes
            if outcome.family is GitHubReconciliationSweepFamily.ISSUE
            and outcome.github_issue_id is not None
        ]

    tick_zero = _run(0)
    tick_one = _run(1)
    # Deterministic membership: each tick services exactly its stable-identity
    # bucket, the buckets are disjoint, and their union covers every eligible
    # unit — no starvation, no double service per rotation.
    assert tick_zero == [2, 4]
    assert tick_one == [1, 3]
    assert sorted(tick_zero + tick_one) == [1, 2, 3, 4]
    assert not set(tick_zero) & set(tick_one)


def test_no_webhook_requirements_drift_is_detected_without_rewriting_the_baseline(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on(
        "from openorc.tasks",
        [_task_row(github_issue_id=501, issue_number=51, fingerprint=_FINGERPRINT_BASE)],
    )
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_NEW, requirements_changed=True)
    ]
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    )
    # The drift is a reported fact against the immutable Task baseline.
    assert issue_outcome.status is GitHubReconciliationSweepUnitStatus.RECONCILED
    assert issue_outcome.task_baseline_drift is True
    assert issue_outcome.issue_requirements_changed is True
    assert issue_outcome.issue_previous_fingerprint == _FINGERPRINT_BASE
    assert issue_outcome.issue_current_fingerprint == _FINGERPRINT_NEW
    # The immutable baseline is never rewritten: the sweep's own SQL performs
    # no write at all.
    _assert_no_durable_writes(harness)


def test_unchanged_requirements_report_no_drift(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on(
        "from openorc.tasks",
        [_task_row(github_issue_id=501, issue_number=51, fingerprint=_FINGERPRINT_BASE)],
    )
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.results = [
        _issue_reconciliation(
            fingerprint=_FINGERPRINT_BASE, requirements_changed=False, previous_fingerprint=None
        )
    ]
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    )
    assert issue_outcome.task_baseline_drift is False
    assert issue_outcome.issue_requirements_changed is False
    assert issue_outcome.issue_created is False
    _assert_no_durable_writes(harness)


def test_an_issue_without_a_current_task_reports_no_baseline_comparison(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_NEW, requirements_changed=True)
    ]
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    )
    assert issue_outcome.task_baseline_drift is None
    _assert_no_durable_writes(harness)


def test_a_no_webhook_hierarchy_change_is_recovered_without_blocking_consequence(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_BASE, requirements_changed=False)
    ]
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=True)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    relations_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.RELATIONS
    )
    # Hierarchy is presentation-only: the change is recovered and reported
    # with no blocking/executability semantics anywhere in the sweep.
    assert relations_outcome.status is GitHubReconciliationSweepUnitStatus.RECONCILED
    assert relations_outcome.hierarchy_changed is True
    assert relations_outcome.dependency_blocked is False
    assert relations_outcome.dependency_mirror_changed is False
    _assert_no_durable_writes(harness)


def test_a_no_webhook_dependency_blocking_change_is_discovered(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_BASE, requirements_changed=False)
    ]
    harness.dependencies.results = [DependencySyncResult(blocked=True, changed=True)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    relations_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.RELATIONS
    )
    # GitHub newly reports the issue as blocked: the normalized fact is
    # reported; deciding BLOCKED belongs to the later workflow services.
    assert relations_outcome.status is GitHubReconciliationSweepUnitStatus.RECONCILED
    assert relations_outcome.dependency_blocked is True
    assert relations_outcome.dependency_mirror_changed is True
    _assert_no_durable_writes(harness)


def test_a_missed_branch_head_change_is_discovered(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [])
    # Family 2's current Tasks (none — the branch subject enters through
    # family 3's fresh re-derivation only).
    conn.on("from openorc.tasks", [])
    conn.on(
        "from openorc.tasks",
        [_task_row(canonical_feature_branch="openorc/task-1")],
    )
    conn.on("where task_id = %s", None)
    conn.on("order by received_at, id limit %s", [])
    harness.branch_observation.results = [
        CanonicalBranchObservation(
            task_id=_TASK_A,
            workspace_id=_WORKSPACE_A,
            branch_name="openorc/task-1",
            head_sha=_HEAD_SHA_NEW,
        )
    ]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    branch_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.BRANCH
    )
    assert branch_outcome.status is GitHubReconciliationSweepUnitStatus.RECONCILED
    assert branch_outcome.branch_observed is True
    assert branch_outcome.branch_head_sha == _HEAD_SHA_NEW
    # No state-token rotation, no binding, no Task mutation of any kind.
    _assert_no_durable_writes(harness)


def test_missed_pr_head_state_and_merge_are_discovered_with_the_exact_head_checks(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [])
    conn.on("from openorc.tasks", [])
    conn.on(
        "from openorc.tasks",
        [_task_row(github_issue_id=502, issue_number=52)],
    )
    conn.on("where task_id = %s", _pull_request_row(state="closed", merged_at=_NOW))
    harness.pull_request_reconcile.results = [_pr_reconciliation()]
    harness.checks_projection.results = [_checks_projection()]
    conn.on("order by received_at, id limit %s", [])

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    assert [(outcome.family, outcome.status) for outcome in result.outcomes] == [
        (
            GitHubReconciliationSweepFamily.REPOSITORY,
            GitHubReconciliationSweepUnitStatus.RECONCILED,
        ),
        (
            GitHubReconciliationSweepFamily.PULL_REQUEST,
            GitHubReconciliationSweepUnitStatus.RECONCILED,
        ),
        (GitHubReconciliationSweepFamily.CHECKS, GitHubReconciliationSweepUnitStatus.RECONCILED),
    ]
    _, pr_outcome, checks_outcome = result.outcomes
    assert pr_outcome.pull_request_updated is True
    assert pr_outcome.pull_request_head_changed is True
    assert pr_outcome.pull_request_state_changed is True
    assert pr_outcome.pull_request_previous_head_sha == _HEAD_SHA_OLD
    assert pr_outcome.pull_request_previous_state is TaskPullRequestState.OPEN
    assert pr_outcome.pull_request_merged is True
    assert pr_outcome.pull_request_closed_unmerged is False
    # The checks projection addresses the exact current reconciled head.
    assert checks_outcome.checks_projected is True
    assert checks_outcome.checks_head_sha == _HEAD_SHA_NEW
    assert checks_outcome.checks_combined_status_state == "success"
    assert checks_outcome.checks_check_run_count == 0
    assert harness.checks_projection.calls == [{"workspace_id": _WORKSPACE_A, "task_id": _TASK_A}]
    _assert_no_durable_writes(harness)


def test_a_missed_closed_unmerged_pr_is_a_normalized_fact(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [])
    conn.on("from openorc.tasks", [])
    conn.on(
        "from openorc.tasks",
        [_task_row(github_issue_id=502, issue_number=52)],
    )
    conn.on("where task_id = %s", _pull_request_row(state="closed", merged_at=None))
    harness.pull_request_reconcile.results = [
        _pr_reconciliation(pull_request=_durable_task_pull_request(merged_at=None))
    ]
    harness.checks_projection.results = [_checks_projection()]
    conn.on("order by received_at, id limit %s", [])

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    pr_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.PULL_REQUEST
    )
    assert pr_outcome.status is GitHubReconciliationSweepUnitStatus.RECONCILED
    assert pr_outcome.pull_request_merged is False
    assert pr_outcome.pull_request_closed_unmerged is True
    _assert_no_durable_writes(harness)


def test_duplicate_and_out_of_order_deliveries_converge_oldest_first(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    older = datetime(2026, 9, 27, 11, 0, 0, tzinfo=UTC)
    conn.on(
        "order by received_at, id limit %s",
        [
            _delivery_row(delivery_guid="guid-old", received_at=older),
            _delivery_row(delivery_guid="guid-new", received_at=_NOW),
        ],
    )
    harness.dispatch.results = [_dispatch_result(), _dispatch_result()]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    delivery_outcomes = [
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.DELIVERY
    ]
    assert [(o.status, o.delivery_processed) for o in delivery_outcomes] == [
        (GitHubReconciliationSweepUnitStatus.RECONCILED, True),
        (GitHubReconciliationSweepUnitStatus.RECONCILED, True),
    ]
    # The sweep re-enters dispatch with exactly the durable delivery GUID,
    # oldest first — duplication and ordering of the notification history
    # never matter, because every routed reconciliation re-reads fresh
    # authority inside dispatch.
    assert [call["delivery_guid"] for call in harness.dispatch.calls] == ["guid-old", "guid-new"]
    # The sweep itself never touches the delivery inbox state: the bounded
    # processed_at marking belongs to the dispatch boundary alone.
    _assert_no_durable_writes(harness)


def test_a_failed_delivery_dispatch_stays_recoverable_and_uncertainty_has_precedence(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on(
        "order by received_at, id limit %s",
        [_delivery_row(delivery_guid="guid-1"), _delivery_row(delivery_guid="guid-2")],
    )
    harness.dispatch.results = [
        _dispatch_result(
            processed=False, route_status=GitHubWebhookDispatchRouteStatus.FAILED_KNOWN
        ),
        _dispatch_result(
            processed=False, route_status=GitHubWebhookDispatchRouteStatus.FAILED_UNCERTAIN
        ),
    ]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    delivery_outcomes = [
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.DELIVERY
    ]
    assert [o.status for o in delivery_outcomes] == [
        GitHubReconciliationSweepUnitStatus.FAILED_KNOWN,
        GitHubReconciliationSweepUnitStatus.FAILED_UNCERTAIN,
    ]
    assert all(outcome.delivery_processed is False for outcome in delivery_outcomes)
    # Unresolved deliveries stay in the eligible set: the next sweep re-runs
    # them; nothing was guessed or destructively advanced.


def test_the_delivery_recovery_batch_is_bounded(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])

    run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    deliveries_sql, deliveries_params = next(
        (sql, params) for sql, params in conn.executed if "limit %s" in sql
    )
    assert deliveries_params is not None
    assert deliveries_params[0] == "relevant"
    assert deliveries_params[1] == MAX_RECOVERY_DELIVERY_BATCH
    assert "processed_at is null" in deliveries_sql


def test_one_units_failure_does_not_corrupt_independent_units(harness_factory) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on(
        "from openorc.github_issues",
        [
            _open_issue_row(github_issue_id=1, issue_number=1),
            _open_issue_row(github_issue_id=2, issue_number=2),
        ],
    )
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.errors = {0: ExternalOperationFailedError("known provider condition")}
    harness.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_BASE, requirements_changed=False)
    ]
    harness.dependencies.results = [
        DependencySyncResult(blocked=False, changed=False),
        DependencySyncResult(blocked=False, changed=False),
    ]
    harness.hierarchy.results = [
        HierarchySyncResult(changed=False),
        HierarchySyncResult(changed=False),
    ]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcomes = [
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    ]
    relations_outcomes = [
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.RELATIONS
    ]
    # The failed issue unit is classified and the sweep continues: the
    # independent relations unit for the same issue and both units for the
    # second issue all completed. No unit forced another to roll back.
    assert [o.status for o in issue_outcomes] == [
        GitHubReconciliationSweepUnitStatus.FAILED_KNOWN,
        GitHubReconciliationSweepUnitStatus.RECONCILED,
    ]
    assert [o.github_issue_id for o in issue_outcomes] == [1, 2]
    assert [o.status for o in relations_outcomes] == [
        GitHubReconciliationSweepUnitStatus.RECONCILED,
        GitHubReconciliationSweepUnitStatus.RECONCILED,
    ]


def test_an_uncertain_outcome_is_surfaced_without_destructive_guessing(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.errors = {0: ExternalOperationUncertainError("outcome unknown")}
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    )
    assert issue_outcome.status is GitHubReconciliationSweepUnitStatus.FAILED_UNCERTAIN
    # Exactly one attempt: uncertainty is never retried blindly and never
    # turned into a destructive state change.
    assert len(harness.issue_reconcile.calls) == 1


def test_access_loss_is_a_normalized_fact_without_credential_fallback(
    harness_factory,
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.errors = {0: AuthorizationError("access lost")}
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    )
    assert issue_outcome.status is GitHubReconciliationSweepUnitStatus.ACCESS_LOST
    assert issue_outcome.github_issue_id == 501
    # The sweep continues independently; no credential fallback seam exists.
    assert len(harness.issue_reconcile.calls) == 1


def test_an_unexpected_error_is_contained_with_safe_classification(
    harness_factory, caplog: pytest.LogCaptureFixture
) -> None:
    harness = harness_factory()
    conn = harness.conn
    conn.on("from openorc.repositories", [_repository_row()])
    conn.on("from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)])
    conn.on("from openorc.tasks", [])
    conn.on("from openorc.tasks", [])
    conn.on("order by received_at, id limit %s", [])
    harness.issue_reconcile.errors = {0: RuntimeError("boom with secret material")}
    harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
    harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    with caplog.at_level(logging.WARNING, logger="openorc.services.github_reconciliation_recovery"):
        result = run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    issue_outcome = next(
        outcome
        for outcome in result.outcomes
        if outcome.family is GitHubReconciliationSweepFamily.ISSUE
    )
    assert issue_outcome.status is GitHubReconciliationSweepUnitStatus.FAILED_UNEXPECTED
    assert len(harness.issue_reconcile.calls) == 1
    # Only the safe type classification is logged; exception content that may
    # carry secrets or customer content never reaches the log.
    assert "RuntimeError" in caplog.text
    assert "boom with secret material" not in caplog.text


def test_repeated_unchanged_sweep_is_an_idempotent_noop(harness_factory) -> None:
    def _run() -> GitHubReconciliationSweepResult:
        harness = harness_factory()
        conn = harness.conn
        conn.on("from openorc.repositories", [_repository_row()])
        conn.on(
            "from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)]
        )
        conn.on("from openorc.tasks", [])
        conn.on("from openorc.tasks", [])
        conn.on("order by received_at, id limit %s", [])
        harness.issue_reconcile.results = [
            _issue_reconciliation(
                fingerprint=_FINGERPRINT_BASE,
                requirements_changed=False,
                previous_fingerprint=None,
            )
        ]
        harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
        harness.hierarchy.results = [HierarchySyncResult(changed=False)]
        return run_github_reconciliation_sweep(harness.pool, _github_client(), harness.settings)

    first = _run()
    second = _run()

    # Both invocations report the same reconciled outcomes with unchanged
    # facts; the composed capabilities' change-only semantics make a repeated
    # sweep a no-op at the canonical-state/event level, and the sweep itself
    # never fabricates a write or event of its own.
    assert [(outcome.family, outcome.status) for outcome in first.outcomes] == [
        (outcome.family, outcome.status) for outcome in second.outcomes
    ]
    assert all(
        outcome.status is GitHubReconciliationSweepUnitStatus.RECONCILED
        and not outcome.issue_requirements_changed
        and not outcome.issue_state_changed
        and not outcome.dependency_mirror_changed
        and not outcome.hierarchy_changed
        for outcome in second.outcomes
        if outcome.family
        in (GitHubReconciliationSweepFamily.ISSUE, GitHubReconciliationSweepFamily.RELATIONS)
    )


def test_a_partial_failure_sweep_is_safe_to_rerun(harness_factory) -> None:
    def _script(harness: Harness) -> None:
        conn = harness.conn
        conn.on("from openorc.repositories", [_repository_row()])
        conn.on(
            "from openorc.github_issues", [_open_issue_row(github_issue_id=501, issue_number=51)]
        )
        conn.on("from openorc.tasks", [])
        conn.on("from openorc.tasks", [])
        conn.on("order by received_at, id limit %s", [])
        harness.dependencies.results = [DependencySyncResult(blocked=False, changed=False)]
        harness.hierarchy.results = [HierarchySyncResult(changed=False)]

    failing = harness_factory()
    _script(failing)
    failing.issue_reconcile.errors = {0: ExternalOperationFailedError("known provider condition")}

    failed = run_github_reconciliation_sweep(failing.pool, _github_client(), failing.settings)

    recovered = harness_factory()
    _script(recovered)
    recovered.issue_reconcile.results = [
        _issue_reconciliation(fingerprint=_FINGERPRINT_BASE, requirements_changed=False)
    ]

    rerun = run_github_reconciliation_sweep(recovered.pool, _github_client(), recovered.settings)

    first_issue = next(
        o for o in failed.outcomes if o.family is GitHubReconciliationSweepFamily.ISSUE
    )
    second_issue = next(
        o for o in rerun.outcomes if o.family is GitHubReconciliationSweepFamily.ISSUE
    )
    assert first_issue.status is GitHubReconciliationSweepUnitStatus.FAILED_KNOWN
    assert second_issue.status is GitHubReconciliationSweepUnitStatus.RECONCILED
    # The re-run re-derived the same bounded work set from durable state and
    # completed the previously failed unit.
    _assert_no_durable_writes(recovered)


def test_malformed_sweep_arguments_are_rejected_before_any_work(harness_factory) -> None:
    harness = harness_factory()

    for bad_tick in (-1, True, "0", 1.5):
        with pytest.raises(InvalidCommandError):
            run_github_reconciliation_sweep(
                harness.pool,
                _github_client(),
                harness.settings,
                sweep_tick=bad_tick,  # type: ignore[arg-type]
            )
    for bad_partitions in (0, -3):
        settings = _settings(partitions=bad_partitions)
        with pytest.raises(InvalidCommandError):
            run_github_reconciliation_sweep(harness.pool, _github_client(), settings)
    # Validation happens before any durable read.
    assert harness.conn.executed == []
