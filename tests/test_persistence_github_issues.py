"""Deterministic tests for the GitHub issue projection repositories (issue #59).

The ordinary suite cannot execute Postgres; these tests use scripted SQL
handlers over a fake pool/connection seam (mirroring the transaction-boundary
fakes) to prove the serialized reconcile contract:

- the lock-then-compute-then-write shape (``SELECT ... FOR UPDATE`` before
  any fact is computed);
- the true durable no-op: an identical authoritative observation executes no
  UPDATE at all, preserving ``updated_at`` — including identical #375
  presentation facts;
- change-only writes through the conditional ``IS DISTINCT FROM`` update,
  including the #375 presentation facts, whose change never moves the
  requirements fingerprint;
- insert-race classification from re-read durable mappings — including the
  interleaving where the tuple-identical concurrent insert is surfaced
  through the number-unique violation and still converges idempotently —
  never from PostgreSQL's reported constraint name;
- number reuse against a different stable identity as a durable conflict
  with no identity rebind;
- row mapping, UTC normalization, and naive-instant rejection at the
  boundary;
- the #375 authorized read surface: the keyset-paged state-filtered list,
  the scope-checked by-UUID detail lookup, and the relations
  observation-evidence stamps.

Database constraint behavior is proven against a real database by the
integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.github_issues import (
    GitHubIssueDomainError,
    GitHubIssueLabel,
    GitHubIssueMilestone,
    GitHubIssueObservedFacts,
    GitHubIssueProjection,
    GitHubIssueState,
    GitHubIssueUserRef,
    github_issue_requirements_fingerprint,
)
from openorc.persistence.github_issues import (
    GitHubIssuePageState,
    GitHubIssueReconcileOutcome,
    find_github_issue,
    find_github_issue_by_id,
    find_github_issue_by_number,
    list_open_repository_github_issues,
    list_repository_github_issues_page,
    reconcile_github_issue,
    stamp_github_issue_relations_observation,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import NaiveDatetimeError

_OBSERVED_AT = datetime(2026, 9, 23, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
_UTC_OBSERVED = datetime(2026, 9, 23, 10, 0, 0, tzinfo=UTC)

_REPOSITORY_ID = uuid.uuid4()
_WORKSPACE_ID = uuid.uuid4()
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Steps to reproduce")


class FakeCursor:
    def __init__(
        self,
        row: tuple[Any, ...] | None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[
            tuple[str, tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception]
        ] = []

    def on(
        self, sql_marker: str, result: tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception
    ) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, list):
                    return FakeCursor(None, result)
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Nested psycopg transaction blocks (SAVEPOINTs under composition).
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("github issue persistence tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _issue_row(
    *,
    issue_number: int = _ISSUE_NUMBER,
    github_issue_id: int = _GITHUB_ISSUE_ID,
    title: str = "Found a bug",
    body: str | None = "Steps to reproduce",
    state: str = "open",
    fingerprint: str = _FINGERPRINT,
    provider_updated_at: datetime | None = _UTC_OBSERVED,
    updated_at: datetime | None = None,
    state_reason: str | None = None,
    html_url: str | None = None,
    provider_created_at: datetime | None = None,
    provider_closed_at: datetime | None = None,
    user_json: Any = None,
    labels_json: Any = None,
    assignees_json: Any = None,
    milestone_json: Any = None,
    hierarchy_observed_at: datetime | None = None,
    dependencies_observed_at: datetime | None = None,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        github_issue_id,
        issue_number,
        title,
        body,
        state,
        fingerprint,
        provider_updated_at,
        _UTC_OBSERVED,
        updated_at or _UTC_OBSERVED,
        state_reason,
        html_url,
        provider_created_at,
        provider_closed_at,
        user_json,
        labels_json,
        assignees_json,
        milestone_json,
        hierarchy_observed_at,
        dependencies_observed_at,
    )


def _projection(row: GitHubIssueProjection | None) -> GitHubIssueProjection:
    assert row is not None
    return row


def _incoming_kwargs(**overrides: Any) -> dict[str, Any]:
    fact_values: dict[str, Any] = {
        "issue_number": _ISSUE_NUMBER,
        "title": "Found a bug",
        "body": "Steps to reproduce",
        "state": GitHubIssueState.OPEN,
        "requirements_fingerprint": _FINGERPRINT,
        "provider_updated_at": _UTC_OBSERVED,
    }
    outer: dict[str, Any] = {
        "workspace_id": _WORKSPACE_ID,
        "repository_id": _REPOSITORY_ID,
        "github_issue_id": _GITHUB_ISSUE_ID,
    }
    for key, value in overrides.items():
        if key in outer:
            outer[key] = value
        else:
            fact_values[key] = value
    return {**outer, "facts": GitHubIssueObservedFacts(**fact_values)}


def test_row_mapping_normalizes_provider_instants_to_utc() -> None:
    conn = ScriptedConnection()
    row = _issue_row(provider_updated_at=_OBSERVED_AT, updated_at=_OBSERVED_AT)
    conn.on("select", row)
    conn.on("select", row)  # the two find helpers share the select shape

    by_identity = find_github_issue(
        _pool(conn), repository_id=_REPOSITORY_ID, github_issue_id=_GITHUB_ISSUE_ID
    )
    by_number = find_github_issue_by_number(
        _pool(conn), repository_id=_REPOSITORY_ID, issue_number=_ISSUE_NUMBER
    )

    assert _projection(by_identity).provider_updated_at == _UTC_OBSERVED
    assert _projection(by_number).updated_at == _UTC_OBSERVED
    first_sql, first_params = conn.executed[0]
    assert "where repository_id = %s and github_issue_id = %s" in first_sql
    assert first_params == (_REPOSITORY_ID, _GITHUB_ISSUE_ID)
    second_sql, second_params = conn.executed[1]
    assert "where repository_id = %s and issue_number = %s" in second_sql
    assert second_params == (_REPOSITORY_ID, _ISSUE_NUMBER)


def test_a_naive_provider_instant_is_rejected_at_the_boundary() -> None:
    conn = ScriptedConnection()

    with pytest.raises(NaiveDatetimeError):
        reconcile_github_issue(
            _pool(conn),
            **_incoming_kwargs(provider_updated_at=datetime(2026, 9, 23, 12, 0, 0)),
        )

    assert conn.executed == []


def test_a_first_projection_inserts_and_reports_creation() -> None:
    conn = ScriptedConnection()
    conn.on("select", None)  # no durable projection for the stable identity
    conn.on("insert into openorc.github_issues", _issue_row())

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.INSERTED
    assert _projection(result.projection).identity.github_issue_id == _GITHUB_ISSUE_ID
    assert result.previous_fingerprint is None
    assert result.previous_state is None
    select_sql, _ = conn.executed[0]
    assert "from openorc.github_issues" in select_sql
    assert "for update" in select_sql
    insert_sql, insert_params = conn.executed[1]
    assert "insert into openorc.github_issues" in insert_sql
    assert "on conflict" not in insert_sql
    assert insert_params == (
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        "Found a bug",
        "Steps to reproduce",
        "open",
        _FINGERPRINT,
        _UTC_OBSERVED,
        None,  # state_reason: unobserved presentation fact stays honest
        None,  # html_url
        None,  # provider_created_at
        None,  # provider_closed_at
        None,  # user_json
        None,  # labels_json
        None,  # assignees_json
        None,  # milestone_json
    )
    assert len(conn.executed) == 2


def test_an_unchanged_observation_is_a_true_durable_no_op() -> None:
    conn = ScriptedConnection()
    conn.on("select", _issue_row())  # the persisted row matches the observation

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.UNCHANGED
    assert result.previous_fingerprint == _FINGERPRINT
    assert result.previous_state is GitHubIssueState.OPEN
    # No UPDATE statement is executed at all: the durable row (including
    # updated_at) is untouched by an unchanged reconciliation.
    assert len(conn.executed) == 1
    assert not any(sql.lstrip().lower().startswith("update") for sql, _ in conn.executed)


def test_a_changed_observation_updates_conditionally_and_reports_the_transition() -> None:
    previous = _issue_row(title="Found a bug", body="Old body")
    conn = ScriptedConnection()
    conn.on("select", previous)
    conn.on("update openorc.github_issues", _issue_row(body="Steps to reproduce"))

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.UPDATED
    assert _projection(result.projection).body == "Steps to reproduce"
    assert result.previous_fingerprint == _FINGERPRINT
    assert result.previous_state is GitHubIssueState.OPEN
    update_sql, update_params = conn.executed[1]
    assert "is distinct from" in update_sql
    assert "updated_at = now()" in update_sql
    # Stable identity and Workspace scope are never rewritten.
    assert "id = excluded" not in update_sql
    assert "workspace_id = excluded" not in update_sql


def test_a_state_change_reports_the_serialized_pre_image_state() -> None:
    previous = _issue_row(state="open")
    conn = ScriptedConnection()
    conn.on("select", previous)
    conn.on("update openorc.github_issues", _issue_row(state="closed"))

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs(state=GitHubIssueState.CLOSED))

    assert result.outcome is GitHubIssueReconcileOutcome.UPDATED
    # The reported pre-image is the locked before-state, not the post-write
    # state: this is what lets the service distinguish a real open/closed
    # transition from a requirements-only durable write.
    assert result.previous_state is GitHubIssueState.OPEN
    assert _projection(result.projection).state is GitHubIssueState.CLOSED


def test_a_number_recorded_for_a_different_stable_identity_is_a_durable_conflict() -> None:
    conn = ScriptedConnection()
    # The locked stable-identity read finds the SAME stable issue durably
    # recorded under a different number: neither fact is rewritten.
    conn.on("select", _issue_row(issue_number=43))

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.IDENTITY_NUMBER_MISMATCH
    assert _projection(result.projection).identity.github_issue_id == 503
    assert len(conn.executed) == 1  # nothing was written


def test_insert_race_classified_through_the_number_unique_violation_converges() -> None:
    # A tuple-identical concurrent insert violates BOTH unique invariants;
    # this interleaving deliberately surfaces the number-unique violation.
    # Classification must come from the re-read durable mappings: the
    # stable-identity row (same number) exists, so the invocation converges
    # idempotently instead of retrying or conflicting.
    conn = ScriptedConnection()
    conn.on("select", None)  # first locked read: no row (the race is uncommitted)
    conn.on("insert into openorc.github_issues", UniqueViolation("duplicate key"))
    conn.on("select", _issue_row())  # race re-read: the same-identity winner committed
    conn.on("select", _issue_row())  # next loop iteration's locked read: now visible

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.UNCHANGED
    assert result.previous_fingerprint == _FINGERPRINT
    assert result.previous_state is GitHubIssueState.OPEN
    # Exactly one insert attempt happened; no write followed convergence.
    inserts = [sql for sql, _ in conn.executed if "insert into" in sql.lower()]
    assert len(inserts) == 1
    assert len(conn.executed) == 4


def test_insert_race_with_a_divergent_observation_reports_the_true_transition() -> None:
    winner_row = _issue_row(title="Older winner title")
    conn = ScriptedConnection()
    conn.on("select", None)  # first locked read: no row yet
    conn.on("insert into openorc.github_issues", UniqueViolation("duplicate key"))
    conn.on("select", winner_row)  # race classification: same identity, same number
    conn.on("select", winner_row)  # next iteration's locked read finds the winner
    conn.on("update openorc.github_issues", _issue_row())

    result = reconcile_github_issue(
        _pool(conn), **_incoming_kwargs(requirements_fingerprint=_FINGERPRINT)
    )

    # The loser serialized onto the winner's committed row and reports the
    # durable transition it actually established, with the serialized
    # pre-image fingerprint.
    assert result.outcome is GitHubIssueReconcileOutcome.UPDATED
    assert result.previous_fingerprint == _FINGERPRINT
    assert result.previous_state is GitHubIssueState.OPEN


def test_insert_race_with_a_differently_mapped_number_is_the_number_conflict() -> None:
    conn = ScriptedConnection()
    conn.on("select", None)  # first locked read: no stable-identity row
    conn.on("insert into openorc.github_issues", UniqueViolation("duplicate key"))
    conn.on("select", None)  # re-read by stable identity: still absent
    conn.on("select", _issue_row(github_issue_id=999))  # number maps elsewhere

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.NUMBER_CONFLICT
    assert _projection(result.projection).identity.github_issue_id == 999
    # Nothing was rewritten: exactly one insert attempt, no retry, no UPDATE.
    inserts = [sql for sql, _ in conn.executed if "insert into" in sql.lower()]
    assert len(inserts) == 1
    assert not any(sql.lstrip().lower().startswith("update") for sql, _ in conn.executed)


def test_an_unresolvable_race_fails_closed() -> None:
    conn = ScriptedConnection()
    for _ in range(3):
        # Each bounded attempt: the locked read finds nothing, the insert
        # races, and the re-read proves neither durable mapping.
        conn.on("select", None)
        conn.on("insert into openorc.github_issues", UniqueViolation("duplicate key"))
        conn.on("select", None)
        conn.on("select", None)

    result = reconcile_github_issue(_pool(conn), **_incoming_kwargs())

    assert result.outcome is GitHubIssueReconcileOutcome.UNRESOLVABLE
    assert result.projection is None
    assert result.previous_state is None


def test_malformed_command_arguments_are_rejected_before_any_sql() -> None:
    conn = ScriptedConnection()

    with pytest.raises(ValueError):
        reconcile_github_issue(_pool(conn), **_incoming_kwargs(github_issue_id=0))
    with pytest.raises(GitHubIssueDomainError):
        reconcile_github_issue(_pool(conn), **_incoming_kwargs(issue_number=True))
    with pytest.raises(GitHubIssueDomainError):
        reconcile_github_issue(
            _pool(conn),
            **_incoming_kwargs(state="open"),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError):
        reconcile_github_issue(
            _pool(conn),
            **_incoming_kwargs(repository_id="not-a-uuid"),  # type: ignore[arg-type]
        )

    assert conn.executed == []


def test_open_issue_listing_maps_open_rows_deterministically() -> None:
    conn = ScriptedConnection()
    conn.on(
        "from openorc.github_issues",
        [
            _issue_row(github_issue_id=501, issue_number=41),
            _issue_row(github_issue_id=503, issue_number=43),
        ],
    )

    listed = list_open_repository_github_issues(
        _pool(conn), workspace_id=_WORKSPACE_ID, repository_id=_REPOSITORY_ID
    )

    assert [projection.identity.github_issue_id for projection in listed] == [501, 503]
    assert all(projection.state is GitHubIssueState.OPEN for projection in listed)
    sql, params = conn.executed[0]
    # The bounded current-work predicate: only open tracked projections.
    assert "state = %s" in sql
    assert "order by github_issue_id" in sql
    assert params == (_WORKSPACE_ID, _REPOSITORY_ID, GitHubIssueState.OPEN.value)


def test_open_issue_listing_with_no_rows_returns_an_empty_list() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.github_issues", [])

    assert (
        list_open_repository_github_issues(
            _pool(conn), workspace_id=_WORKSPACE_ID, repository_id=_REPOSITORY_ID
        )
        == []
    )


_PRESENTATION_USER_JSON = {"id": 77, "login": "octocat"}
_PRESENTATION_LABELS_JSON = [{"id": 11, "name": "bug", "color": "d73a4a", "description": None}]


def test_row_mapping_restores_the_presentation_facts() -> None:
    conn = ScriptedConnection()
    row = _issue_row(
        state_reason="reopened",
        html_url="https://github.com/octocat/hello-world/issues/42",
        provider_created_at=_UTC_OBSERVED,
        provider_closed_at=None,
        user_json=_PRESENTATION_USER_JSON,
        labels_json=_PRESENTATION_LABELS_JSON,
        assignees_json=[{"id": 88, "login": "hubot"}],
        milestone_json={
            "id": 13,
            "number": 7,
            "title": "v1",
            "description": None,
            "state": "open",
            "html_url": "https://github.com/octocat/hello-world/milestone/7",
            "due_on": "2026-12-01T00:00:00+00:00",
        },
        hierarchy_observed_at=_UTC_OBSERVED,
        dependencies_observed_at=_UTC_OBSERVED,
    )
    conn.on("select", row)

    projection = find_github_issue(
        _pool(conn), repository_id=_REPOSITORY_ID, github_issue_id=_GITHUB_ISSUE_ID
    )

    restored = _projection(projection)
    assert restored.state_reason == "reopened"
    assert restored.html_url == "https://github.com/octocat/hello-world/issues/42"
    assert restored.provider_created_at == _UTC_OBSERVED
    assert restored.provider_closed_at is None
    assert restored.user is not None
    assert restored.user.login == "octocat"
    assert restored.labels is not None
    assert restored.labels[0].name == "bug"
    assert restored.assignees is not None
    assert restored.assignees[0].login == "hubot"
    assert restored.milestone is not None
    assert restored.milestone.due_on == datetime(2026, 12, 1, tzinfo=UTC)
    assert restored.hierarchy_observed_at == _UTC_OBSERVED
    assert restored.dependencies_observed_at == _UTC_OBSERVED


def test_an_identical_observation_including_presentation_facts_is_a_true_no_op() -> None:
    previous = _issue_row(
        state_reason="reopened",
        html_url="https://github.com/octocat/hello-world/issues/42",
        user_json=_PRESENTATION_USER_JSON,
        labels_json=_PRESENTATION_LABELS_JSON,
        assignees_json=[{"id": 88, "login": "hubot"}],
        milestone_json={
            "id": 13,
            "number": 7,
            "title": "v1",
            "description": None,
            "state": "open",
            "html_url": "https://github.com/octocat/hello-world/milestone/7",
            "due_on": None,
        },
    )
    conn = ScriptedConnection()
    conn.on("select", previous)

    result = reconcile_github_issue(
        _pool(conn),
        **_incoming_kwargs(
            state_reason="reopened",
            html_url="https://github.com/octocat/hello-world/issues/42",
            user=GitHubIssueUserRef(id=77, login="octocat"),
            labels=(GitHubIssueLabel(id=11, name="bug", color="d73a4a", description=None),),
            assignees=(GitHubIssueUserRef(id=88, login="hubot"),),
            milestone=GitHubIssueMilestone(
                id=13,
                number=7,
                title="v1",
                description=None,
                state="open",
                html_url="https://github.com/octocat/hello-world/milestone/7",
                due_on=None,
            ),
        ),
    )

    assert result.outcome is GitHubIssueReconcileOutcome.UNCHANGED
    assert len(conn.executed) == 1  # no write at all; updated_at preserved


def test_a_presentation_only_change_writes_but_never_moves_the_fingerprint() -> None:
    previous = _issue_row()  # no presentation facts observed yet
    conn = ScriptedConnection()
    conn.on("select", previous)
    conn.on(
        "update openorc.github_issues",
        _issue_row(
            state_reason="reopened",
            html_url="https://github.com/octocat/hello-world/issues/42",
            user_json=_PRESENTATION_USER_JSON,
        ),
    )

    result = reconcile_github_issue(
        _pool(conn),
        **_incoming_kwargs(
            state_reason="reopened",
            html_url="https://github.com/octocat/hello-world/issues/42",
            user=GitHubIssueUserRef(id=77, login="octocat"),
        ),
    )

    assert result.outcome is GitHubIssueReconcileOutcome.UPDATED
    assert result.previous_fingerprint == _FINGERPRINT
    assert result.previous_state is GitHubIssueState.OPEN
    update_sql, update_params = conn.executed[1]
    assert "requirements_fingerprint = %s" in update_sql
    assert update_params is not None
    # The written fingerprint equals the unchanged requirements digest: the
    # presentation change never moved it.
    assert update_params[4] == _FINGERPRINT
    assert "labels_json = %s" in update_sql
    assert "milestone_json = %s" in update_sql


def test_the_keyset_page_query_filters_states_and_bounds_the_page() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.github_issues", [_issue_row()])

    listed = list_repository_github_issues_page(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        state=GitHubIssuePageState.CLOSED,
        after_issue_number=41,
        limit=25,
    )

    assert len(listed) == 1
    sql, params = conn.executed[0]
    assert "order by issue_number asc limit %s" in sql
    assert "state = %s" in sql
    assert "issue_number > " in sql
    assert params == (_WORKSPACE_ID, _REPOSITORY_ID, "closed", 41, 41, 25)


def test_the_all_page_query_carries_no_state_predicate() -> None:
    conn = ScriptedConnection()
    conn.on("from openorc.github_issues", [])

    list_repository_github_issues_page(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        state=GitHubIssuePageState.ALL,
        after_issue_number=None,
        limit=50,
    )

    sql, params = conn.executed[0]
    assert "state = %s" not in sql
    assert params == (_WORKSPACE_ID, _REPOSITORY_ID, None, None, 50)


def test_the_detail_lookup_is_scoped_to_the_addressed_repository() -> None:
    conn = ScriptedConnection()
    issue_id = uuid.uuid4()
    conn.on("select", _issue_row())

    projection = find_github_issue_by_id(
        _pool(conn), workspace_id=_WORKSPACE_ID, repository_id=_REPOSITORY_ID, issue_id=issue_id
    )

    assert _projection(projection).id is not None
    sql, params = conn.executed[0]
    assert "where id = %s and workspace_id = %s and repository_id = %s" in sql
    assert params == (issue_id, _WORKSPACE_ID, _REPOSITORY_ID)


def test_relations_observation_evidence_stamps_the_requested_families() -> None:
    conn = ScriptedConnection()
    conn.on("update openorc.github_issues", None)

    stamp_github_issue_relations_observation(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=_GITHUB_ISSUE_ID,
        dependencies=True,
    )

    sql, params = conn.executed[0]
    assert "dependencies_observed_at = now()" in sql
    assert "hierarchy_observed_at" not in sql
    assert params == (_WORKSPACE_ID, _REPOSITORY_ID, _GITHUB_ISSUE_ID)


def test_relations_observation_evidence_requires_a_family() -> None:
    conn = ScriptedConnection()

    with pytest.raises(ValueError):
        stamp_github_issue_relations_observation(
            _pool(conn),
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            github_issue_id=_GITHUB_ISSUE_ID,
        )

    assert conn.executed == []
