"""Deterministic tests for the Workspace authorization service (issue #53).

The ordinary suite cannot execute Postgres: these tests use canned rows and a
scripted fake connection seam to prove the fail-closed authorization policy —
owner success, non-owner denial, missing-subject normalization, and guessed
cross-Workspace (and cross-Task) UUID denial for every demonstrated record
family — plus the structural guarantee the review demanded: the public
surface is profile-gated end to end, and a directly loaded or hand-constructed
plain ``Workspace`` is never sufficient authority anywhere on it. Database
constraint behavior is proven by the integration-marked suite.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.domain.ownership import Workspace
from openorc.persistence.pool import DatabasePool
from openorc.services import workspace_authorization
from openorc.services.errors import NotFoundError

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("authorization tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(
    workspace_id: Any,
    owner_profile_id: Any,
    *,
    review_iteration_limit: int = 5,
    guidance: str = "",
) -> tuple[Any, ...]:
    return (
        workspace_id,
        owner_profile_id,
        "platform",
        _OBSERVED,
        _OBSERVED,
        review_iteration_limit,
        guidance,
    )


_OBSERVED = _ws_row(uuid.uuid4(), uuid.uuid4())[3]


def test_owner_resolves_their_workspace_and_settings() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    row = _ws_row(workspace_id, profile_id, review_iteration_limit=7, guidance="prose")
    conn = ScriptedConnection([row])

    workspace = workspace_authorization.require_profile_workspace(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id
    )

    assert workspace == Workspace(
        id=workspace_id,
        owner_profile_id=profile_id,
        name="platform",
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        review_iteration_limit=7,
        guidance="prose",
    )
    assert len(conn.executed) == 1
    assert "from openorc.workspaces where id = %s" in conn.executed[0][0]


def test_non_owner_and_missing_workspace_are_uniformly_not_found() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()

    # A Workspace owned by another Profile is not found — never an
    # authorization error that would confirm its existence.
    conn = ScriptedConnection([_ws_row(workspace_id, uuid.uuid4())])
    with pytest.raises(NotFoundError):
        workspace_authorization.require_profile_workspace(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id
        )
    # Only the ownership load ran; no further resolution happened.
    assert len(conn.executed) == 1

    # A missing Workspace is the same observable outcome.
    missing = ScriptedConnection([None])
    with pytest.raises(NotFoundError):
        workspace_authorization.require_profile_workspace(
            _pool(missing), profile_id=profile_id, workspace_id=workspace_id
        )
    assert len(missing.executed) == 1


def test_a_directly_loaded_foreign_workspace_is_not_authority() -> None:
    # Loading a Workspace through persistence is data access, not
    # authorization: the same load feeds the gate, which still fails closed
    # for a Profile that does not own the row.
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    conn = ScriptedConnection([_ws_row(workspace_id, uuid.uuid4())])

    with pytest.raises(NotFoundError):
        workspace_authorization.require_profile_workspace(
            _pool(conn), profile_id=profile_id, workspace_id=workspace_id
        )
    assert [sql for sql, _ in conn.executed] == [
        "select id, owner_profile_id, name, created_at, updated_at, "
        "review_iteration_limit, guidance from openorc.workspaces where id = %s"
    ]


def test_public_authorization_surface_never_accepts_a_workspace_as_authority() -> None:
    # Structural guarantee: no public resolver in either service module
    # accepts a Workspace parameter (or omits profile_id), so a directly
    # loaded or hand-constructed foreign Workspace cannot be supplied as
    # authority anywhere on the public surface.
    foreign_workspace = Workspace(
        id=uuid.uuid4(),
        owner_profile_id=uuid.uuid4(),
        name="foreign",
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        review_iteration_limit=5,
        guidance="",
    )
    modules = (
        workspace_authorization,
        pytest.importorskip("openorc.services.workspace_configuration"),
    )
    for module in modules:
        assert module.__all__ is not None
        for name in module.__all__:
            obj = getattr(module, name)
            if not callable(obj):
                continue
            signature = inspect.signature(obj)
            # The service-operation convention: the first parameter is the
            # database pool. Result dataclasses (audit handoffs) are not
            # operations and are skipped by this convention check.
            first = next(iter(signature.parameters.values()))
            first_annotation = (
                first.annotation
                if isinstance(first.annotation, str)
                else getattr(first.annotation, "__name__", str(first.annotation))
            )
            if first_annotation != "DatabasePool":
                continue
            assert "profile_id" in signature.parameters, name
            assert signature.parameters["profile_id"].kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in signature.parameters.values():
                annotation = (
                    parameter.annotation
                    if isinstance(parameter.annotation, str)
                    else getattr(parameter.annotation, "__name__", str(parameter.annotation))
                )
                assert annotation != "Workspace", (
                    f"{module.__name__}.{name} must not accept a Workspace as authority"
                )
            # Behavioral proof: no public callable has any Workspace-typed
            # parameter, so this hand-constructed foreign Workspace can only
            # be rejected outright by the interpreter.
            with pytest.raises(TypeError):
                obj(  # type: ignore[call-arg]
                    _pool(ScriptedConnection([])),
                    profile_id=uuid.uuid4(),
                    authorized_workspace=foreign_workspace,
                )


def test_authorization_never_consults_github_identity_or_repository_metadata() -> None:
    # Ownership is the authenticated Profile UUID alone: the resolver
    # succeeds with a repository row whose observed GitHub metadata belongs
    # to an unrelated owner, and no executed SQL ever references GitHub
    # presentation columns.
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    project_id = uuid.uuid4()
    ws_row = _ws_row(workspace_id, profile_id)
    repo_row = (
        repository_id,
        project_id,
        workspace_id,
        987654321,
        "totally-unrelated-github-owner",
        "their-repo",
        "https://github.com/totally-unrelated-github-owner/their-repo",
        True,
        "trunk",
        _OBSERVED,
        _OBSERVED,
        None,
    )
    conn = ScriptedConnection([ws_row, repo_row])

    repository = workspace_authorization.require_workspace_repository(
        _pool(conn), profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
    )

    assert repository.id == repository_id
    assert repository.metadata.owner_login == "totally-unrelated-github-owner"
    for sql, _ in conn.executed:
        where_clause = sql.split("where")[-1]
        # The authorization predicates address identity/scope only; GitHub
        # presentation columns never filter access.
        assert "owner_login" not in where_clause
        assert "github_repository_id" not in where_clause


def _family_rows(workspace_id: Any) -> dict[str, tuple[Any, ...]]:
    """One mapper-valid row per record family, scoped to ``workspace_id``."""
    connection_id = uuid.uuid4()
    repository_id = uuid.uuid4()
    project_id = uuid.uuid4()
    task_id = uuid.uuid4()
    session_id = uuid.uuid4()
    return {
        "project": (uuid.uuid4(), workspace_id, "project", _OBSERVED, _OBSERVED),
        "repository": (
            uuid.uuid4(),
            project_id,
            workspace_id,
            1000,
            "octocat",
            "repo",
            "https://github.com/octocat/repo",
            False,
            "main",
            _OBSERVED,
            _OBSERVED,
            None,
        ),
        "connection": (
            connection_id,
            workspace_id,
            "cline",
            "route",
            {},
            1,
            True,
            None,
            None,
            None,
            _OBSERVED,
            _OBSERVED,
        ),
        "binding": (uuid.uuid4(), workspace_id, "producer", connection_id, _OBSERVED, _OBSERVED),
        "task": (
            task_id,
            workspace_id,
            repository_id,
            42,
            7,
            "ready_to_plan",
            None,
            None,
            uuid.uuid4(),
            None,
            None,
            _OBSERVED,
            _OBSERVED,
        ),
        "session": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            "producer",
            connection_id,
            None,
            "connecting",
            None,
            None,
            None,
            None,
            None,
            None,
            _OBSERVED,
            _OBSERVED,
        ),
        "plan revision": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            1,
            "# Plan",
            "a1b2c3d4",
            _OBSERVED,
        ),
        "review loop": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            "planning",
            5,
            "open",
            None,
            _OBSERVED,
        ),
        "review iteration": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            uuid.uuid4(),
            1,
            uuid.uuid4(),
            None,
            None,
            None,
            None,
            None,
            None,
            _OBSERVED,
        ),
        "owner gate": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            "implementation_authorization",
            "pending",
            uuid.uuid4(),
            None,
            None,
            None,
            _OBSERVED,
        ),
        "execution": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            session_id,
            1,
            "queued",
            _OBSERVED,
            _OBSERVED,
        ),
        "runtime request": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            session_id,
            "action_approval",
            "ext-approval-1",
            "pending",
            None,
            None,
            _OBSERVED,
        ),
        "task block": (uuid.uuid4(), workspace_id, task_id, "review_failure", {}, None, _OBSERVED),
        "pull request record": (
            uuid.uuid4(),
            workspace_id,
            task_id,
            repository_id,
            9001,
            5,
            "feature/one",
            "main",
            "abc123",
            "open",
            None,
            _OBSERVED,
            _OBSERVED,
        ),
        "workflow event": (
            uuid.uuid4(),
            workspace_id,
            None,
            "task_created",
            "owner",
            None,
            "task",
            task_id,
            {},
            _OBSERVED,
        ),
    }


def _family_resolvers(profile_id: Any, workspace_id: Any):
    """(label, callable(conn, record_id)) for every UUID-addressed family."""
    wa = workspace_authorization
    return [
        (
            "project",
            lambda conn, rid: wa.require_workspace_project(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, project_id=rid
            ),
        ),
        (
            "repository",
            lambda conn, rid: wa.require_workspace_repository(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, repository_id=rid
            ),
        ),
        (
            "connection",
            lambda conn, rid: wa.require_workspace_connection(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, connection_id=rid
            ),
        ),
        (
            "task",
            lambda conn, rid: wa.require_workspace_task(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, task_id=rid
            ),
        ),
        (
            "plan revision",
            lambda conn, rid: wa.require_task_plan_revision(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, plan_revision_id=rid
            ),
        ),
        (
            "review loop",
            lambda conn, rid: wa.require_task_review_loop(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, review_loop_id=rid
            ),
        ),
        (
            "review iteration",
            lambda conn, rid: wa.require_task_review_iteration(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=workspace_id,
                review_iteration_id=rid,
            ),
        ),
        (
            "owner gate",
            lambda conn, rid: wa.require_task_owner_gate(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, owner_gate_id=rid
            ),
        ),
        (
            "execution",
            lambda conn, rid: wa.require_task_execution(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, execution_id=rid
            ),
        ),
        (
            "runtime request",
            lambda conn, rid: wa.require_task_runtime_request(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=workspace_id,
                runtime_request_id=rid,
            ),
        ),
        (
            "task block",
            lambda conn, rid: wa.require_task_block(
                _pool(conn), profile_id=profile_id, workspace_id=workspace_id, block_id=rid
            ),
        ),
        (
            "pull request record",
            lambda conn, rid: wa.require_task_pull_request(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_pull_request_id=rid,
            ),
        ),
        (
            "workflow event",
            lambda conn, rid: wa.require_workspace_event(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=workspace_id,
                workflow_event_id=rid,
            ),
        ),
    ]


_TASK_OWNED_LABELS = frozenset(
    {
        "plan revision",
        "review loop",
        "review iteration",
        "owner gate",
        "execution",
        "runtime request",
        "task block",
        "pull request record",
    }
)


def test_owner_resolves_every_demonstrated_family_within_their_workspace() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    ws_row = _ws_row(workspace_id, profile_id)
    rows = _family_rows(workspace_id)

    # Workspace-rooted families resolve with [workspace, record] statements;
    # Task-owned UUID families resolve with [workspace, record, task].
    for label, resolve in _family_resolvers(profile_id, workspace_id):
        record_row = rows[label]
        record_id = record_row[0]
        if label in _TASK_OWNED_LABELS:
            conn = ScriptedConnection([ws_row, record_row, rows["task"]])
        else:
            conn = ScriptedConnection([ws_row, record_row])

        record = resolve(conn, record_id)

        assert record is not None
        assert record.id == record_id
        # Only identity/scope predicates were executed; no GitHub join.
        for sql, _ in conn.executed:
            assert "owner_login" not in sql.split("where")[-1]


def test_guessed_cross_workspace_uuid_is_denied_for_every_family() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    ws_row = _ws_row(workspace_id, profile_id)
    foreign_rows = _family_rows(uuid.uuid4())

    for label, resolve in _family_resolvers(profile_id, workspace_id):
        record_id = foreign_rows[label][0]
        # The record exists in another Workspace: guessing its exact UUID
        # from this Workspace is indistinguishable from absence.
        conn = ScriptedConnection([ws_row, foreign_rows[label]])
        with pytest.raises(NotFoundError):
            resolve(conn, record_id)

        # A missing record is the same observable outcome.
        missing = ScriptedConnection([ws_row, None])
        with pytest.raises(NotFoundError):
            resolve(missing, record_id)


def test_task_owned_record_mislinked_to_another_workspaces_task_is_denied() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    ws_row = _ws_row(workspace_id, profile_id)
    rows = _family_rows(workspace_id)
    foreign_task = _family_rows(uuid.uuid4())["task"]

    for label, resolve in _family_resolvers(profile_id, workspace_id):
        if label not in _TASK_OWNED_LABELS:
            continue
        record_id = rows[label][0]
        # The record carries the authorized Workspace scope, but its task_id
        # resolves to a Task of another Workspace: uniformly not found.
        conn = ScriptedConnection([ws_row, rows[label], foreign_task])
        with pytest.raises(NotFoundError):
            resolve(conn, record_id)


def test_session_and_role_binding_resolution_scopes_fail_closed() -> None:
    from openorc.domain.connections import WorkflowRole

    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    ws_row = _ws_row(workspace_id, profile_id)
    rows = _family_rows(workspace_id)
    task_id = rows["task"][0]

    # Owner resolves the Task/role session and the per-role binding.
    ok = ScriptedConnection([ws_row, rows["task"], rows["session"]])
    session = workspace_authorization.require_task_agent_session(
        _pool(ok),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
    )
    assert session.task_id == task_id

    binding = workspace_authorization.require_workspace_role_binding(
        _pool(ScriptedConnection([ws_row, rows["binding"]])),
        profile_id=profile_id,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
    )
    assert binding.workspace_id == workspace_id

    # A session bound to a different Task identity is not found.
    mislinked_session = (
        rows["session"][0],
        workspace_id,
        uuid.uuid4(),
        "producer",
        rows["session"][4],
        None,
        "connecting",
        None,
        None,
        None,
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )
    with pytest.raises(NotFoundError):
        workspace_authorization.require_task_agent_session(
            _pool(ScriptedConnection([ws_row, rows["task"], mislinked_session])),
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
        )

    # A missing binding is not found; so is one from another Workspace.
    foreign_workspace_id = uuid.uuid4()
    foreign_binding = (
        rows["binding"][0],
        foreign_workspace_id,
        "producer",
        uuid.uuid4(),
        _OBSERVED,
        _OBSERVED,
    )
    for bad_binding_row in (None, foreign_binding):
        with pytest.raises(NotFoundError):
            workspace_authorization.require_workspace_role_binding(
                _pool(ScriptedConnection([ws_row, bad_binding_row])),
                profile_id=profile_id,
                workspace_id=workspace_id,
                role=WorkflowRole.PRODUCER,
            )


def test_task_scoped_workflow_event_reads_validate_the_task_linkage() -> None:
    from openorc.domain.events import WorkflowEventActor, WorkflowEventType

    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    ws_row = _ws_row(workspace_id, profile_id)
    task_rows = _family_rows(workspace_id)
    authorized_task_id = task_rows["task"][0]
    workspace_level_event = task_rows["workflow event"]  # task_id=None by construction

    def event_row(task_id: uuid.UUID) -> tuple[Any, ...]:
        return (
            uuid.uuid4(),
            workspace_id,
            task_id,
            WorkflowEventType.TASK_CREATED.value,
            WorkflowEventActor.OWNER.value,
            None,
            "task",
            task_id,
            {},
            _OBSERVED,
        )

    # A Workspace-level event resolves with no Task lookup at all.
    ok_workspace_level = ScriptedConnection([ws_row, workspace_level_event])
    resolved = workspace_authorization.require_workspace_event(
        _pool(ok_workspace_level),
        profile_id=profile_id,
        workspace_id=workspace_id,
        workflow_event_id=workspace_level_event[0],
    )
    assert resolved.task_id is None
    assert len(ok_workspace_level.executed) == 2

    # A task-scoped event of the authorized Workspace resolves through the
    # Task-scope resolver.
    ok_task_scoped = ScriptedConnection([ws_row, event_row(authorized_task_id), task_rows["task"]])
    resolved = workspace_authorization.require_workspace_event(
        _pool(ok_task_scoped),
        profile_id=profile_id,
        workspace_id=workspace_id,
        workflow_event_id=event_row(authorized_task_id)[0],
    )
    assert resolved.task_id == authorized_task_id
    assert len(ok_task_scoped.executed) == 3
    task_lookup_sql = ok_task_scoped.executed[2][0]
    assert "from openorc.tasks where id = %s" in task_lookup_sql

    # A task-scoped event whose Task resolves outside the authorized
    # Workspace is uniformly not found — the composite foreign key is a
    # backstop, not the authorization mechanism.
    foreign_workspace_id = uuid.uuid4()
    foreign_task_id = _family_rows(foreign_workspace_id)["task"][0]
    with pytest.raises(NotFoundError):
        workspace_authorization.require_workspace_event(
            _pool(
                ScriptedConnection(
                    [
                        ws_row,
                        event_row(foreign_task_id),
                        _family_rows(foreign_workspace_id)["task"],
                    ]
                )
            ),
            profile_id=profile_id,
            workspace_id=workspace_id,
            workflow_event_id=event_row(foreign_task_id)[0],
        )
