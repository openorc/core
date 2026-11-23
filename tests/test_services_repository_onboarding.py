"""Deterministic tests for the Project/Repository onboarding services (issue #372).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the Owner-authorized onboarding surface — Project
creation as genuine Owner configuration, the repository claim persisting the
stable identity ALONE (no observation fact in the executed command), the
duplicate-identity conflict, the issue #97 account-deletion barrier, uniform
not-found isolation, and the mapping-status projections (claim state without
observations, observed state, route composition, and fail-closed broken
routes). Durable constraint behavior is proven by the integration-marked
suite and the migration convention tests.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from psycopg.errors import UniqueViolation

from openorc.observability import OPERATION, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services import repository_onboarding
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

_GUARD_OPERATIONAL_ROW = (None, None, None)
_GUARD_DELETION_ACTIVE_ROW = ("active", uuid.uuid4(), _OBSERVED)


class FakeCursor:
    def __init__(self, result: Any) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result if isinstance(self._result, tuple) else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._result) if isinstance(self._result, list) else []


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[Any]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("repository onboarding tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(workspace_id: Any, profile_id: Any) -> tuple[Any, ...]:
    return (workspace_id, profile_id, "platform", _OBSERVED, _OBSERVED, 5, "", None)


def _project_row(project_id: Any, workspace_id: Any) -> tuple[Any, ...]:
    return (project_id, workspace_id, "delivery", _OBSERVED, _OBSERVED)


def _repository_row(
    repository_id: Any,
    project_id: Any,
    workspace_id: Any,
    *,
    route: Any = None,
    claimed: bool = True,
) -> tuple[Any, ...]:
    observation = (
        (None, None, None, None, None)
        if claimed
        else ("octocat", "hello-world", "https://github.com/octocat/hello-world", False, "main")
    )
    return (
        repository_id,
        project_id,
        workspace_id,
        987654321,
        *observation,
        _OBSERVED,
        _OBSERVED,
        route,
    )


def _installation_row(installation_id: Any, workspace_id: Any) -> tuple[Any, ...]:
    return (
        installation_id,
        workspace_id,
        12345678,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def test_create_project_rejects_an_invalid_name_before_any_database_work() -> None:
    conn = ScriptedConnection([])

    for bad in ("", "   ", "p" * 201, None, 7):
        with pytest.raises(InvalidCommandError):
            repository_onboarding.create_workspace_project(
                _pool(conn),
                profile_id=uuid.uuid4(),
                workspace_id=uuid.uuid4(),
                name=bad,  # type: ignore[arg-type]
            )
    assert conn.executed == []


def test_create_project_persists_the_owner_authored_name() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _project_row(project_id, workspace_id),
        ]
    )

    project = repository_onboarding.create_workspace_project(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, name="delivery"
    )

    assert project.id == project_id
    assert project.name == "delivery"
    assert len(conn.executed) == 3
    insert_sql, insert_params = conn.executed[2]
    assert "insert into openorc.projects" in insert_sql
    assert insert_params == (workspace_id, "delivery")


def test_create_project_fails_closed_during_an_account_deletion_attempt() -> None:
    conn = ScriptedConnection([_GUARD_DELETION_ACTIVE_ROW])

    with pytest.raises(ConflictError):
        repository_onboarding.create_workspace_project(
            _pool(conn), profile_id=uuid.uuid4(), workspace_id=uuid.uuid4(), name="delivery"
        )

    assert len(conn.executed) == 1
    assert "openorc.profiles" in conn.executed[0][0]


def test_list_projects_is_ownership_gated_and_workspace_scoped() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    conn = ScriptedConnection(
        [_ws_row(workspace_id, profile_id), [_project_row(project_id, workspace_id)]]
    )

    projects = repository_onboarding.list_workspace_projects(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert len(projects) == 1
    assert projects[0].name == "delivery"
    sql, params = conn.executed[1]
    assert "from openorc.projects where workspace_id = %s order by created_at, id" in sql
    assert params == (workspace_id,)


def test_claim_repository_rejects_an_invalid_identity_before_any_database_work() -> None:
    conn = ScriptedConnection([])

    for bad in (0, -1, True, 1.5, "987654321", None):
        with pytest.raises(InvalidCommandError):
            repository_onboarding.claim_workspace_repository(
                _pool(conn),
                profile_id=uuid.uuid4(),
                workspace_id=uuid.uuid4(),
                project_id=uuid.uuid4(),
                github_repository_id=bad,  # type: ignore[arg-type]
            )
    assert conn.executed == []


def test_claim_repository_persists_the_identity_alone_in_claim_state() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            # require_workspace_project re-enforces ownership before resolving
            # the subject — the Workspace read composes twice, like every
            # composed subject resolver.
            _ws_row(workspace_id, profile_id),
            _project_row(project_id, workspace_id),
            None,
            _repository_row(repository_id, project_id, workspace_id),
        ]
    )

    repository = repository_onboarding.claim_workspace_repository(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        project_id=project_id,
        github_repository_id=987654321,
    )

    # The claim command carries the stable external identity ALONE: the
    # executed insert binds no GitHub observation fact of any kind.
    assert repository.metadata is None
    assert repository.identity.github_repository_id == 987654321
    assert len(conn.executed) == 6
    find_sql, find_params = conn.executed[4]
    assert "where workspace_id = %s and github_repository_id = %s" in find_sql
    assert find_params == (workspace_id, 987654321)
    insert_sql, insert_params = conn.executed[5]
    assert "insert into openorc.repositories" in insert_sql
    assert insert_params == (
        project_id,
        workspace_id,
        987654321,
        None,
        None,
        None,
        None,
        None,
    )


def test_claim_repository_rejects_a_duplicate_identity_as_a_conflict() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _ws_row(workspace_id, profile_id),
            _project_row(project_id, workspace_id),
            _repository_row(uuid.uuid4(), project_id, workspace_id),
        ]
    )

    with pytest.raises(ConflictError):
        repository_onboarding.claim_workspace_repository(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            project_id=project_id,
            github_repository_id=987654321,
        )

    # The duplicate check ran and no insert ever followed it.
    assert len(conn.executed) == 5


def test_claim_repository_translates_the_unique_constraint_backstop() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _ws_row(workspace_id, profile_id),
            _project_row(project_id, workspace_id),
            None,
            UniqueViolation("duplicate key value violates unique constraint"),
        ]
    )

    with pytest.raises(ConflictError):
        repository_onboarding.claim_workspace_repository(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            project_id=project_id,
            github_repository_id=987654321,
        )


def test_claim_repository_rejects_a_foreign_project_uniformly() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _ws_row(workspace_id, profile_id),
            None,
        ]
    )

    with pytest.raises(NotFoundError):
        repository_onboarding.claim_workspace_repository(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            project_id=uuid.uuid4(),
            github_repository_id=987654321,
        )

    # A foreign Project is indistinguishable from an absent one, and no
    # claim insert ever ran.
    assert len(conn.executed) == 4
    assert "from openorc.projects" in conn.executed[3][0]


def test_claim_repository_rejects_an_unowned_workspace_uniformly() -> None:
    conn = ScriptedConnection([_GUARD_OPERATIONAL_ROW, None])

    with pytest.raises(NotFoundError):
        repository_onboarding.claim_workspace_repository(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            project_id=uuid.uuid4(),
            github_repository_id=987654321,
        )


def test_list_repositories_projects_claim_state_without_a_route() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            [_repository_row(repository_id, uuid.uuid4(), workspace_id)],
            [],
        ]
    )

    statuses = repository_onboarding.list_workspace_repositories(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert len(statuses) == 1
    assert statuses[0].repository.metadata is None
    assert statuses[0].installation is None
    repos_sql, repos_params = conn.executed[1]
    assert "where workspace_id = %s order by created_at, id" in repos_sql
    assert repos_params == (workspace_id,)
    installs_sql, installs_params = conn.executed[2]
    assert "from openorc.github_installations" in installs_sql
    assert installs_params == (workspace_id,)


def test_list_repositories_projects_the_route_and_observed_state() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            [
                _repository_row(
                    repository_id,
                    uuid.uuid4(),
                    workspace_id,
                    route=installation_id,
                    claimed=False,
                )
            ],
            [_installation_row(installation_id, workspace_id)],
        ]
    )

    statuses = repository_onboarding.list_workspace_repositories(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert len(statuses) == 1
    assert statuses[0].repository.metadata is not None
    assert statuses[0].repository.metadata.owner_login == "octocat"
    assert statuses[0].installation is not None
    assert statuses[0].installation.id == installation_id


def test_list_repositories_fails_closed_on_a_missing_route_record() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            [_repository_row(uuid.uuid4(), uuid.uuid4(), workspace_id, route=uuid.uuid4())],
            [],
        ]
    )

    with pytest.raises(NotFoundError):
        repository_onboarding.list_workspace_repositories(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id
        )


def test_read_repository_projects_the_recorded_status() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, route=installation_id),
            _installation_row(installation_id, workspace_id),
        ]
    )

    status = repository_onboarding.read_workspace_repository(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        repository_id=repository_id,
    )

    assert status.repository.id == repository_id
    assert status.repository.metadata is None
    assert status.installation is not None
    assert status.installation.id == installation_id


def test_read_repository_fails_closed_on_a_missing_route_record() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _ws_row(workspace_id, profile_id),
            _repository_row(repository_id, uuid.uuid4(), workspace_id, route=uuid.uuid4()),
            None,
        ]
    )

    with pytest.raises(NotFoundError):
        repository_onboarding.read_workspace_repository(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )


def test_read_repository_rejects_an_unowned_workspace_uniformly() -> None:
    conn = ScriptedConnection([None])

    with pytest.raises(NotFoundError):
        repository_onboarding.read_workspace_repository(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            repository_id=uuid.uuid4(),
        )
    assert len(conn.executed) == 1
    assert "from openorc.workspaces" in conn.executed[0][0]


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_claim_repository_opens_its_service_span() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _GUARD_OPERATIONAL_ROW,
            _ws_row(workspace_id, profile_id),
            _ws_row(workspace_id, profile_id),
            _project_row(project_id, workspace_id),
            None,
            _repository_row(uuid.uuid4(), project_id, workspace_id),
        ]
    )

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        repository_onboarding.claim_workspace_repository(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            project_id=project_id,
            github_repository_id=987654321,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "repository_onboarding.claim_workspace_repository"
    attributes = exported.attributes
    assert attributes is not None
    assert set(attributes) == {OPERATION, WORKSPACE_ID}
    assert attributes[WORKSPACE_ID] == str(workspace_id)
