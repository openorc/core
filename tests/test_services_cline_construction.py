"""Deterministic tests for the durable Cline construction-context resolver
(issue #189, D8a).

A scripted fake connection seam proves the read-only resolution against
canned durable rows: fresh construction from the composition-selected
bootstrap base with the current #162 role configuration, bound resolution
recovering the Producer's original base from the durable READY snapshot,
Reviewer resolution without any repository_base requirement, the fail-closed
refusal matrix, and that bound-path unestablishability is ``None`` — never
session-loss evidence. The adapter-facing failure normalization (fresh
rejection / bound ``None`` through the real adapter before any backend
effect) is proven with the real ``ClineAdapter`` in
``tests/test_services_session_runtime.py`` and is not duplicated here.
Durable concurrency semantics of the underlying reads are already proven by
the existing session persistence suites.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.errors import AgentRuntimeConfigurationRejectedError
from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskAgentSessionDomainError
from openorc.persistence.pool import DatabasePool
from openorc.services.cline_construction import DurableClineConstructionContextResolver

_OBSERVED = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)

# One canned statement outcome: a row tuple, a row list, None (empty result),
# or an exception instance the seam raises instead of returning.
_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception


class FakeCursor:
    """Returns one canned result, like a psycopg cursor (row or row list)."""

    def __init__(self, result: tuple[Any, ...] | list[tuple[Any, ...]] | None) -> None:
        self._result = result

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._result is None:
            return None
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._result is None:
            return []
        if isinstance(self._result, list):
            return list(self._result)
        return [self._result]


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL
    and the connection/transaction lifecycle."""

    def __init__(self, results: Sequence[_CannedResult]) -> None:
        self.results: list[_CannedResult] = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.lifecycle: list[str] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports the repository transaction scopes over the pooled checkouts.
        self.lifecycle.append("tx_enter")
        try:
            yield
        finally:
            self.lifecycle.append("tx_exit")


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        conn = self._conn

        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            conn.lifecycle.append("pool_enter")
            try:
                yield conn
            finally:
                conn.lifecycle.append("pool_exit")

        return managed()

    def close(self) -> None:
        raise AssertionError("resolver tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


# --- canned durable rows -------------------------------------------------


def _binding_row(
    workspace_id: Any,
    task_id: Any,
    connection_id: Any,
    *,
    role: str = "producer",
    lifecycle_status: str = "connecting",
    external_session_id: str | None = None,
    snapshot: Any = None,
) -> tuple[Any, ...]:
    """One canned TaskAgentSession row (domain-valid for the given lifecycle)."""
    return (
        uuid.uuid4(),
        workspace_id,
        task_id,
        role,
        connection_id,
        external_session_id,
        lifecycle_status,
        snapshot if snapshot is not None else ({} if external_session_id is not None else None),
        None,
        None,
        None,
        _OBSERVED if external_session_id is not None else None,
        _OBSERVED if lifecycle_status == "ended" else None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _route_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    role: str = "producer",
    provider: str | None = None,
    model: str | None = None,
    override: str | None = None,
) -> tuple[Any, ...]:
    """One canned WorkflowRoleBinding row (unconfigured by default)."""
    return (
        uuid.uuid4(),
        workspace_id,
        role,
        connection_id,
        provider,
        model,
        override,
        _OBSERVED,
        _OBSERVED,
    )


def _connection_row(
    workspace_id: Any,
    connection_id: Any,
    *,
    enabled: bool = True,
    safe_config: Any = None,
) -> tuple[Any, ...]:
    return (
        connection_id,
        workspace_id,
        "cline",
        "primary cline hub",
        {} if safe_config is None else safe_config,
        1,
        enabled,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _fresh_results(
    *,
    binding_row: tuple[Any, ...] | None,
    connection_row: tuple[Any, ...] | None,
    route_row: tuple[Any, ...] | None,
) -> ScriptedConnection:
    """Canned fresh-path reads: binding, admitted Connection, current route."""
    return ScriptedConnection([binding_row, connection_row, route_row])


def _bound_results(
    *,
    rows: list[tuple[Any, ...]],
    connection_row: tuple[Any, ...] | None,
    route_row: tuple[Any, ...] | None,
) -> ScriptedConnection:
    """Canned bound-path reads: exact-identity rows, Connection, route."""
    return ScriptedConnection([rows, connection_row, route_row])


# --- fresh construction --------------------------------------------------


def test_fresh_producer_resolves_the_selected_bootstrap_base_and_role_configuration() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    # The durable Connection row carries a DIFFERENT repository_base than the
    # composition selection: the selection is authoritative and the current
    # safe_config value is never substituted into the fresh context.
    conn = _fresh_results(
        binding_row=_binding_row(workspace_id, task_id, connection_id),
        connection_row=_connection_row(
            workspace_id, connection_id, safe_config={"repository_base": "/srv/other/base"}
        ),
        route_row=_route_row(
            workspace_id,
            connection_id,
            provider="configured-provider",
            model="configured-model",
            override="# Owner's verbatim override",
        ),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), bootstrap_repository_base="/srv/selected/repositories"
    )

    context = resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.PRODUCER)

    assert context.identity.workspace_id == workspace_id
    assert context.identity.task_id == task_id
    assert context.identity.role is WorkflowRole.PRODUCER
    assert context.identity.connection_id == connection_id
    # The current per-role configuration is resolved verbatim (issue #162).
    assert context.configured_provider == "configured-provider"
    assert context.configured_model == "configured-model"
    assert context.role_prompt_override == "# Owner's verbatim override"
    # The selected bootstrap base is the fresh Producer's working directory;
    # the Connection's current repository_base is never substituted.
    assert context.cwd == "/srv/selected/repositories"
    assert context.workspace_root == "/srv/selected/repositories"


def test_fresh_producer_fails_closed_without_a_selected_bootstrap_base() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _fresh_results(
        binding_row=_binding_row(workspace_id, task_id, connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.PRODUCER)


def test_fresh_producer_fails_closed_for_a_missing_binding() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _fresh_results(
        binding_row=None,
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), bootstrap_repository_base="/srv/selected/repositories"
    )

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.PRODUCER)
    assert len(conn.executed) == 1


def test_fresh_producer_fails_closed_for_a_non_connecting_binding() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
    )
    conn = _fresh_results(
        binding_row=ready_row,
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), bootstrap_repository_base="/srv/selected/repositories"
    )

    # Fresh construction is definitionally the establishment of an admitted
    # CONNECTING binding; a READY/LOST/ENDED binding is never freshly built.
    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.PRODUCER)
    assert len(conn.executed) == 1


def test_fresh_producer_fails_closed_for_an_ineligible_admitted_connection() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    for connection_row in (
        _connection_row(workspace_id, connection_id, enabled=False),
        _connection_row(uuid.uuid4(), connection_id),
    ):
        conn = _fresh_results(
            binding_row=_binding_row(workspace_id, task_id, connection_id),
            connection_row=connection_row,
            route_row=_route_row(workspace_id, connection_id),
        )
        resolver = DurableClineConstructionContextResolver(
            _pool(conn), bootstrap_repository_base="/srv/selected/repositories"
        )

        with pytest.raises(AgentRuntimeConfigurationRejectedError):
            resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.PRODUCER)

        # The refusal happened before the current role configuration was read.
        assert len(conn.executed) == 2


def test_fresh_producer_fails_closed_without_a_current_role_route() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _fresh_results(
        binding_row=_binding_row(workspace_id, task_id, connection_id),
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=None,
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), bootstrap_repository_base="/srv/selected/repositories"
    )

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.PRODUCER)


def test_fresh_reviewer_resolves_the_supplied_neutral_directory() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    # The admitted Connection carries NO repository_base: a fresh Reviewer
    # never requires one and runs at the supplied neutral directory.
    conn = _fresh_results(
        binding_row=_binding_row(workspace_id, task_id, connection_id, role="reviewer"),
        connection_row=_connection_row(workspace_id, connection_id, safe_config={}),
        route_row=_route_row(
            workspace_id,
            connection_id,
            role="reviewer",
            provider="reviewer-provider",
            model="reviewer-model",
        ),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), working_directory="/srv/neutral/review"
    )

    context = resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.REVIEWER)

    assert context.identity.role is WorkflowRole.REVIEWER
    assert context.identity.connection_id == connection_id
    assert context.configured_provider == "reviewer-provider"
    assert context.configured_model == "reviewer-model"
    assert context.cwd == "/srv/neutral/review"
    assert context.workspace_root == "/srv/neutral/review"


def test_fresh_reviewer_fails_closed_without_a_supplied_directory() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _fresh_results(
        binding_row=_binding_row(workspace_id, task_id, connection_id, role="reviewer"),
        connection_row=_connection_row(workspace_id, connection_id, safe_config={}),
        route_row=_route_row(workspace_id, connection_id, role="reviewer"),
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        resolver.resolve_construction_context(workspace_id, task_id, WorkflowRole.REVIEWER)
    assert len(conn.executed) == 3


# --- bound sessions ------------------------------------------------------


def test_bound_producer_resolves_the_original_base_from_the_snapshot() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    original_base = "/srv/original/repositories"
    # The durable Connection row's CURRENT repository_base is edited; the
    # snapshot-pinned original base is authoritative for the bound session.
    conn = _bound_results(
        rows=[
            _binding_row(
                workspace_id,
                task_id,
                connection_id,
                lifecycle_status="ready",
                external_session_id="ext-1",
                snapshot={"adapter": "cline", "configuration": {"repository_base": original_base}},
            )
        ],
        connection_row=_connection_row(
            workspace_id, connection_id, safe_config={"repository_base": "/srv/edited/base"}
        ),
        route_row=_route_row(
            workspace_id, connection_id, provider="configured-provider", model="configured-model"
        ),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), working_directory=f"{original_base}/{task_id}"
    )

    context = resolver.resolve_bound_session_context("ext-1")

    assert context is not None
    assert context.identity.workspace_id == workspace_id
    assert context.identity.task_id == task_id
    assert context.identity.connection_id == connection_id
    assert context.configured_provider == "configured-provider"
    assert context.configured_model == "configured-model"
    # The deterministic isolated Task checkout at the ORIGINAL base is the
    # accepted working context; the edited Connection base never relocates it.
    assert context.cwd == f"{original_base}/{task_id}"
    assert context.workspace_root == f"{original_base}/{task_id}"


def test_bound_producer_accepts_the_original_base_itself() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    original_base = "/srv/original/repositories"
    conn = _bound_results(
        rows=[
            _binding_row(
                workspace_id,
                task_id,
                connection_id,
                lifecycle_status="ready",
                external_session_id="ext-1",
                snapshot={"adapter": "cline", "configuration": {"repository_base": original_base}},
            )
        ],
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn), working_directory=original_base)

    context = resolver.resolve_bound_session_context("ext-1")

    assert context is not None
    assert context.cwd == original_base
    assert context.workspace_root == original_base


def test_bound_producer_refuses_a_foreign_working_directory() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    original_base = "/srv/original/repositories"
    conn = _bound_results(
        rows=[
            _binding_row(
                workspace_id,
                task_id,
                connection_id,
                lifecycle_status="ready",
                external_session_id="ext-1",
                snapshot={"adapter": "cline", "configuration": {"repository_base": original_base}},
            )
        ],
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), working_directory="/srv/somewhere/else"
    )

    # Fail closed with no substitution: a supplied working context that is
    # neither the original base nor its deterministic checkout resolves no
    # construction context — never the current Connection default.
    assert resolver.resolve_bound_session_context("ext-1") is None


def test_bound_producer_passes_through_a_missing_working_directory() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    conn = _bound_results(
        rows=[
            _binding_row(
                workspace_id,
                task_id,
                connection_id,
                lifecycle_status="ready",
                external_session_id="ext-1",
                snapshot={
                    "adapter": "cline",
                    "configuration": {"repository_base": "/srv/original/repositories"},
                },
            )
        ],
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    context = resolver.resolve_bound_session_context("ext-1")

    # The phase has not supplied a working directory yet: identity-only
    # operations still resolve, and the adapter fails closed on any working
    # context it would need. No path is invented.
    assert context is not None
    assert context.cwd is None
    assert context.workspace_root is None


@pytest.mark.parametrize(
    "snapshot",
    [
        pytest.param(None, id="no-snapshot"),
        pytest.param({}, id="empty-snapshot"),
        pytest.param({"adapter": "cline", "configuration": {}}, id="no-repository-base"),
        pytest.param(
            {"adapter": "github", "configuration": {"repository_base": "/srv/base"}},
            id="non-cline-snapshot-adapter",
        ),
    ],
)
def test_bound_producer_fails_closed_on_a_missing_or_invalid_snapshot(snapshot: Any) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        snapshot=snapshot,
    )
    conn = _bound_results(
        rows=[ready_row],
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), working_directory="/srv/original/repositories"
    )

    # Without the pinned original base the bound construction context is not
    # establishable: an establishment miss, never session-loss evidence.
    assert resolver.resolve_bound_session_context("ext-1") is None


def test_bound_reviewer_resolves_without_a_repository_base() -> None:
    """Regression (issue #189 review): a READY Reviewer binding whose admitted
    Connection and snapshot carry no ``repository_base`` resolves normally —
    Reviewer sessions run at their separately supplied neutral non-repository
    directory and never require a Producer base or checkout."""
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        role="reviewer",
        lifecycle_status="ready",
        external_session_id="reviewer-session-1",
        snapshot={"adapter": "cline", "configuration": {}},
    )
    conn = _bound_results(
        rows=[ready_row],
        connection_row=_connection_row(workspace_id, connection_id, safe_config={}),
        route_row=_route_row(
            workspace_id,
            connection_id,
            role="reviewer",
            provider="reviewer-provider",
            model="reviewer-model",
            override="# Reviewer override",
        ),
    )
    resolver = DurableClineConstructionContextResolver(
        _pool(conn), working_directory="/srv/neutral/review"
    )

    context = resolver.resolve_bound_session_context("reviewer-session-1")

    assert context is not None
    assert context.identity.role is WorkflowRole.REVIEWER
    assert context.identity.connection_id == connection_id
    assert context.configured_provider == "reviewer-provider"
    assert context.configured_model == "reviewer-model"
    assert context.role_prompt_override == "# Reviewer override"
    assert context.cwd == "/srv/neutral/review"
    assert context.workspace_root == "/srv/neutral/review"


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param([], id="zero-matches"),
        pytest.param("two-matches", id="ambiguous-across-connections"),
    ],
)
def test_bound_unestablishable_identity_is_none_and_never_loss(rows: Any) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    if rows == "two-matches":
        rows = [
            _binding_row(
                workspace_id,
                task_id,
                connection_id,
                lifecycle_status="ready",
                external_session_id="ext-1",
                snapshot={"adapter": "cline", "configuration": {"repository_base": "/srv/b1"}},
            ),
            _binding_row(
                workspace_id,
                uuid.uuid4(),
                uuid.uuid4(),
                lifecycle_status="ready",
                external_session_id="ext-1",
                snapshot={"adapter": "cline", "configuration": {"repository_base": "/srv/b2"}},
            ),
        ]
    conn = _bound_results(
        rows=rows,
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    # A miss is an identity-establishment miss — never evidence of
    # exact-session loss — and an ambiguous duplicate is never guessed.
    assert resolver.resolve_bound_session_context("ext-1") is None


@pytest.mark.parametrize(
    "lifecycle_row",
    [
        pytest.param("lost", id="lost-binding"),
        pytest.param("ended", id="ended-binding"),
    ],
)
def test_bound_terminal_lifecycle_records_resolve_no_context(lifecycle_row: str) -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    terminal_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status=lifecycle_row,
        external_session_id="ext-1",
    )
    conn = _bound_results(
        rows=[terminal_row],
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=_route_row(workspace_id, connection_id),
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    assert resolver.resolve_bound_session_context("ext-1") is None


def test_bound_fails_closed_for_an_ineligible_admitted_connection() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        snapshot={"adapter": "cline", "configuration": {"repository_base": "/srv/base"}},
    )
    for connection_row in (
        _connection_row(workspace_id, connection_id, enabled=False),
        _connection_row(uuid.uuid4(), connection_id),
    ):
        conn = _bound_results(
            rows=[ready_row],
            connection_row=connection_row,
            route_row=_route_row(workspace_id, connection_id),
        )
        resolver = DurableClineConstructionContextResolver(_pool(conn))

        assert resolver.resolve_bound_session_context("ext-1") is None


def test_bound_fails_closed_without_a_current_role_route() -> None:
    workspace_id = uuid.uuid4()
    task_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    ready_row = _binding_row(
        workspace_id,
        task_id,
        connection_id,
        lifecycle_status="ready",
        external_session_id="ext-1",
        snapshot={"adapter": "cline", "configuration": {"repository_base": "/srv/base"}},
    )
    conn = _bound_results(
        rows=[ready_row],
        connection_row=_connection_row(workspace_id, connection_id),
        route_row=None,
    )
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    assert resolver.resolve_bound_session_context("ext-1") is None


@pytest.mark.parametrize("malformed", ["", "   ", None, 7])
def test_bound_malformed_session_identity_is_none(malformed: Any) -> None:
    conn = ScriptedConnection([])
    resolver = DurableClineConstructionContextResolver(_pool(conn))

    assert resolver.resolve_bound_session_context(malformed) is None
    assert conn.executed == []


# --- composition validation ----------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"bootstrap_repository_base": "srv/relative/base"}, id="relative-base"),
        pytest.param({"bootstrap_repository_base": "   "}, id="blank-base"),
        pytest.param({"working_directory": "   "}, id="blank-directory"),
    ],
)
def test_resolver_construction_fails_closed_on_malformed_composition_inputs(
    kwargs: dict[str, Any],
) -> None:
    conn = ScriptedConnection([])

    with pytest.raises(ValueError):
        DurableClineConstructionContextResolver(_pool(conn), **kwargs)

    assert conn.executed == []


def test_persistence_read_requires_a_session_identity() -> None:
    from openorc.persistence.sessions import list_task_agent_sessions_by_external_session_id

    conn = ScriptedConnection([])

    with pytest.raises(TaskAgentSessionDomainError):
        list_task_agent_sessions_by_external_session_id(_pool(conn), external_session_id="   ")

    assert conn.executed == []
