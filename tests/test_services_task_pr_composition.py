"""Deterministic tests for the E8 PR composition and publication seam (issue #83).

A scripted SQL-connection seam, a routing fake GitHub App client, the shared
deterministic fake Agent Runtime, and a recording publication fake prove the
E8 orchestration seams on their critical paths:

- durable approval admission: the persisted APPROVED historical gate plus its
  single OWNER-attributed ``owner_gate_resolved`` audit uniquely authorize the
  composition; pending/cancelled/rejected/foreign gates, a missing or
  ambiguous audit, a mismatched caller/approver, stale tokens, moved status,
  and an unoperational account refuse before any runtime interaction, any
  send, and any publication;
- the one PR_COMPOSE interaction: the exact pre-existing READY Producer
  binding and the canonical ``PrComposeInteraction`` are used once with the
  current optional Workspace guidance and a validated ``pr_result`` through
  the shared #67 -> #65 funnel; uncertain and known-failed runtime outcomes
  are distinct, never resent, and never published; no Reviewer interaction,
  no new session, and no hidden prompt exists;
- the deterministic closing reference: a missing effective exact-issue
  closing reference is appended once, an existing one is preserved, and
  wrong-issue, other-repository, fenced, inline-code, and keyword-less
  references never satisfy it;
- the B8 handoff: the fully grounded ``PublicationCommand`` is captured and
  the existing primitive is invoked exactly once with the exact approving
  Profile; ``PUBLISHED``, ``PREFLIGHT_STALE``, ``POST_CREATE_HEAD_MISMATCH``,
  and ``POST_CREATE_AUTHORITY_STALE`` classifications yield the correct
  orchestration consequences without an extra create;
- finalization and staleness: only the review-eligible canonical record
  commits ``waiting_for_owner -> reviewing`` plus exactly one ``pr_created``
  event; the narrow continuation of this module's own interrupted publication
  reconciles authoritatively and completes without repeating the composition
  or the create; closed-unmerged, merged, and retargeted records fail closed;
  a forced late event failure rolls back and the continuation completes;
  stale post-create authority blocks recoverably or mutates nothing;
- telemetry carries only the sanctioned safe identifier vocabulary.

No live GitHub, runtime, or Postgres access; the real race matrix of the
publication primitive is deliberately not duplicated here (issue #64's own
suite owns it) and the new SQL composition seams are proven by the
integration-marked suite against a real Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeProtocolFailureError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubPullRequestObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import PR_RESULT_FAMILY
from openorc.services import pull_request_publication
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.pull_request_publication import (
    ExternalOperationReconciliationRequiredError,
    ExternalOperationRecoveryRequiredError,
    PublicationCommand,
    TaskPullRequestPublication,
    TaskPullRequestPublicationOutcome,
)
from openorc.services.task_pr_composition import (
    ProducerRuntimeAttachmentFactory,
    compose_and_publish_task_pr,
)
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    pr_result_candidate,
    session_ready_candidate,
)

_OBSERVED = datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC)
_ACCOUNT_OPERATIONAL_ROW: tuple[Any, ...] = (None, None, None)
_ACCOUNT_DELETION_ACTIVE_ROW: tuple[Any, ...] = ("active", uuid.uuid4(), _OBSERVED)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_OTHER_PROFILE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_PR_GATE_ID = uuid.uuid4()
_INSTALLATION_RECORD = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_ISSUE_ID = 503
_ISSUE_NUMBER = 42
_STATE_TOKEN = uuid.uuid4()
_ROTATED_TOKEN = uuid.uuid4()
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_OTHER_HEAD_SHA = "0123456789abcdef0123456789abcdef89abcdef"
_RACED_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_BRANCH = "openorc/task-42-implementation"
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77
_TITLE = "feat: implement task 42"
_BODY = "Implemented the retry loop."
_EXTERNAL_SESSION_ID = "fake-session-1"

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


def _sql_has(*markers: str) -> Callable[[str, tuple[Any, ...] | None], bool]:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(marker in sql for marker in markers)

    return predicate


def _sql_lacks(
    base: Callable[[str, tuple[Any, ...] | None], bool], *markers: str
) -> Callable[[str, tuple[Any, ...] | None], bool]:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return base(sql, params) and not any(marker in sql for marker in markers)

    return predicate


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


_Predicate = Callable[[str, tuple[Any, ...] | None], bool]
_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | Exception | None


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler.

    Handlers are consumed in registration order unless registered as
    repeating, so same-shaped reads with different scripted outcomes script
    in call order.
    """

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, _CannedResult, bool]] = []

    def on(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, False))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (predicate, result, once) in enumerate(self._handlers):
            if predicate(lowered, params):
                if once:
                    del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, list):
                    return FakeCursor(None, result)
                return FakeCursor(result, None)
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
        raise AssertionError("composition tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_ACCOUNT_BARRIER = _sql_has("from openorc.profiles", "for key share")
_WORKSPACE_READ = _sql_has("from openorc.workspaces")
_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_PR_READ = _sql_has("from openorc.task_pull_requests")
_PR_UPDATE = _sql_has("update openorc.task_pull_requests")
_REPO_READ = _sql_lacks(_sql_has("from openorc.repositories"), "for update")
_REPO_READ_LOCKED = _sql_has("from openorc.repositories", "for update")
_INSTALLATION_READ = _sql_has("from openorc.github_installations")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_EVENTS_BY_SUBJECT = _sql_has("from openorc.workflow_events", "subject_id = %s")
_STATUS_UPDATE = _sql_has("update openorc.tasks set status", "queued_at = case")
_ARCHIVE_UPDATE = _sql_has("update openorc.tasks set status", "archived_at = now()")
_LOOPS_READ = _sql_has("from openorc.review_loops", "status = %s")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _ws_row(*, guidance: str = "") -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, 2, guidance, None)


def _task_row(
    *,
    status: str = "waiting_for_owner",
    state_token: Any = _STATE_TOKEN,
    canonical_branch: Any = _BRANCH,
    archived_at: Any = None,
    terminal_cause: Any = None,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        _GITHUB_ISSUE_ID,
        _ISSUE_NUMBER,
        status,
        archived_at,
        canonical_branch,
        state_token,
        None,
        None,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
        terminal_cause,
        None,
    )


def _gate_row(
    *,
    gate_id: Any = _PR_GATE_ID,
    gate_type: str = "pr_authorization",
    status: str = "approved",
    plan_revision_id: Any = None,
    subject_head_sha: Any = _HEAD_SHA,
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        plan_revision_id,
        subject_head_sha,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _session_row() -> tuple[Any, ...]:
    return (
        _SESSION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "producer",
        uuid.uuid4(),
        _EXTERNAL_SESSION_ID,
        "ready",
        {"repository_base": "/workspace/task-42"},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _repository_row() -> tuple[Any, ...]:
    return (
        _REPOSITORY_ID,
        uuid.uuid4(),
        _WORKSPACE_ID,
        _GITHUB_REPOSITORY_ID,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        _INSTALLATION_RECORD,
    )


def _installation_row() -> tuple[Any, ...]:
    return (
        _INSTALLATION_RECORD,
        _WORKSPACE_ID,
        12345678,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _pr_row(
    *,
    head_ref: str = _BRANCH,
    base_ref: str = "main",
    head_sha: str = _HEAD_SHA,
    state: str = "open",
    merged_at: Any = None,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        _GITHUB_PR_ID,
        _PR_NUMBER,
        head_ref,
        base_ref,
        head_sha,
        state,
        merged_at,
        _OBSERVED,
        _OBSERVED,
    )


def _approved_audit_row(
    *, actor_id: Any = str(_OWNER_PROFILE_ID), outcome: str = "approved"
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "owner_gate_resolved",
        "owner",
        actor_id,
        "owner_gate",
        _PR_GATE_ID,
        {"outcome": outcome},
        _OBSERVED,
    )


def _block_row(*, reason: str = "stale_operation") -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        reason,
        {"authorized_head_sha": _HEAD_SHA},
        None,
        _OBSERVED,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "pr_created",
        "openorc",
        None,
        "task_pull_request",
        uuid.uuid4(),
        {},
        _OBSERVED,
    )


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


def _event_inserts(conn: ScriptedConnection) -> list[tuple[Any, ...]]:
    return [params for sql, params in conn.executed if _EVENT_INSERT(sql, None) and params]


def _event_types(conn: ScriptedConnection) -> list[str]:
    return [str(params[2]) for params in _event_inserts(conn)]


def _event_params_of_type(conn: ScriptedConnection, event_type: str) -> list[tuple[Any, ...]]:
    return [params for params in _event_inserts(conn) if params[2] == event_type]


class _FakeGitHub:
    """A routing fake GitHub App client asserting the no-transaction rule.

    Every method the E8 seams must never call raises ``AssertionError``; the
    reconciliation path's reads are scripted observations.
    """

    def __init__(
        self,
        *,
        pool: FakePool,
        repository_observation: GitHubRepositoryObservation | None = None,
        pull_request_observation: GitHubPullRequestObservation | None = None,
        pull_request_error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._repository_observation = repository_observation
        self._pull_request_observation = pull_request_observation
        self._pull_request_error = pull_request_error
        self.repository_calls: list[dict[str, int]] = []
        self.pull_request_calls: list[dict[str, Any]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.repository_calls.append(
            {
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        assert self._repository_observation is not None
        return self._repository_observation

    def get_repository_pull_request(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
    ) -> GitHubPullRequestObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the external GitHub call"
        )
        self.pull_request_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
                "pull_number": pull_number,
            }
        )
        if self._pull_request_error is not None:
            raise self._pull_request_error
        assert self._pull_request_observation is not None
        return self._pull_request_observation

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not create pull requests directly")

    def get_repository_issue(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not read issues")

    def get_repository_branch(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not observe branches directly")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not request merges")

    def close_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("this fake must not close pull requests")

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("publication credential validation belongs to B8")

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not compose raw validation operations")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not project combined status")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the E8 seam must not compose raw adapter operations")

    def get_issue_blocked_by(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not observe relationships")

    def get_issue_sub_issues(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not observe relationships")

    def get_issue_parent(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not observe relationships")

    def resolve_related_issue_endpoints(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not resolve endpoints")


class _FakeResolver:
    """A scripted #142 resolver recording the exact Profile it resolves for."""

    def __init__(self) -> None:
        self.resolve_calls: list[uuid.UUID] = []

    def resolve(self, pool: DatabasePool, *, profile_id: uuid.UUID) -> Any:
        self.resolve_calls.append(profile_id)
        from openorc.adapters.github import GitHubUserAccessToken

        return GitHubUserAccessToken(value="ghu_owner_user_token", expires_at=_OBSERVED)

    def evict_cached_access_token(self, profile_id: uuid.UUID) -> None:
        raise AssertionError("the E8 seam never evicts credentials")


class _RecordingPublication:
    """A recording fake for the existing publication primitive (issue #64)."""

    def __init__(
        self,
        *,
        result: TaskPullRequestPublication | None = None,
        error: Exception | None = None,
    ) -> None:
        self.commands: list[PublicationCommand] = []
        self._result = result
        self._error = error

    def __call__(
        self,
        pool: DatabasePool,
        github: Any,
        user_token_resolver: Any,
        command: PublicationCommand,
    ) -> TaskPullRequestPublication:
        assert cast(FakePool, pool).active_connections == 0, (
            "the service must never hold a database transaction open across "
            "the publication primitive"
        )
        self.commands.append(command)
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result


def _publication_fake(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: TaskPullRequestPublication | None = None,
    error: Exception | None = None,
) -> _RecordingPublication:
    fake = _RecordingPublication(result=result, error=error)
    monkeypatch.setattr(pull_request_publication, "publish_task_pull_request", fake)
    return fake


def _prepared_adapter(*, send_outcomes: tuple[Any, ...] = ()) -> FakeAgentRuntimeAdapter:
    """One fake runtime holding the live Producer session on the exact id."""
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert created.external_session_id == _EXTERNAL_SESSION_ID
    for outcome in send_outcomes:
        adapter.queue_send(_EXTERNAL_SESSION_ID, outcome)
    return adapter


def _runtime_factory(
    adapter: FakeAgentRuntimeAdapter, invocations: list[int] | None = None
) -> ProducerRuntimeAttachmentFactory:
    @contextmanager
    def factory() -> Iterator[FakeAgentRuntimeAdapter]:
        if invocations is not None:
            invocations.append(1)
        yield adapter

    return factory


# --- scripted sequences (in service call order) --------------------------------------


_CLAIM_ID = uuid.uuid4()
_CLAIM_AT = datetime(2026, 10, 26, 9, 30, 0, tzinfo=UTC)

_CLAIM_PRECHECK = _sql_has("from openorc.task_runtime_send_claims", "task_id = %s and kind = %s")
_CLAIM_INSERT = _sql_has("insert into openorc.task_runtime_send_claims")
_CLAIM_SETTLE = _sql_has("update openorc.task_runtime_send_claims", "settled_at = now()")


def _claim_row(
    *,
    kind: str = "pr_compose_dispatch",
    status: str = "claimed",
    outcome: Any = None,
    settled: bool = False,
    claim_id: Any = _CLAIM_ID,
) -> tuple[Any, ...]:
    settled_at = _CLAIM_AT if settled or status != "claimed" else None
    return (
        claim_id,
        _WORKSPACE_ID,
        _TASK_ID,
        kind,
        _SESSION_ID,
        None,
        _PR_GATE_ID,
        _HEAD_SHA,
        status,
        {} if outcome is None else outcome,
        _CLAIM_AT,
        settled_at,
        _CLAIM_AT,
    )


def _script_pr_compose_claim(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    pr_read: Any = None,
    existing_claim: tuple[Any, ...] | None = None,
) -> None:
    """The PR_COMPOSE claim transaction's canned sequence, in call order.

    The locked Task re-read, the canonical-PR reproof read (still absent on
    the fresh path), the gate/Producer-binding reproofs (the gate and
    session reads ride the repeating reads), the exact-subject pre-check
    (``None`` means unclaimed), and the committed claim row (issue #87)."""
    conn.on(_TASK_READ_LOCKED, task_row if task_row is not None else _task_row())
    conn.on(_PR_READ, pr_read)
    conn.on(_CLAIM_PRECHECK, existing_claim)
    conn.on(_CLAIM_INSERT, _claim_row())


def _script_repeating_reads(
    conn: ScriptedConnection,
    *,
    barrier: tuple[Any, ...] = _ACCOUNT_OPERATIONAL_ROW,
    guidance: str = "",
    gate_row: tuple[Any, ...] | None = None,
    audit_rows: list[tuple[Any, ...]] | None = None,
) -> None:
    """Same-shaped reads whose result never changes across the whole flow."""
    conn.on_repeat(_ACCOUNT_BARRIER, barrier)
    conn.on_repeat(_WORKSPACE_READ, _ws_row(guidance=guidance))
    conn.on_repeat(_GATE_READ, gate_row if gate_row is not None else _gate_row())
    conn.on_repeat(
        _EVENTS_BY_SUBJECT, audit_rows if audit_rows is not None else [_approved_audit_row()]
    )
    conn.on_repeat(_REPO_READ, _repository_row())
    conn.on_repeat(_INSTALLATION_READ, _installation_row())
    conn.on_repeat(_SESSION_READ, _session_row())


def _script_admission_pass(
    conn: ScriptedConnection, *, task_row: tuple[Any, ...], pr_read: Any
) -> None:
    """One admission pass: Workspace/Task guard, lock, currentness, PR, the
    gate/audit reads (repeats), then the approving-Profile re-prove guard —
    the route/session/guidance reads all repeat."""
    conn.on(_TASK_READ, task_row)
    conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_TASK_READ, task_row)
    conn.on(_PR_READ, pr_read)
    conn.on(_TASK_READ, task_row)


def _script_fresh_publication(
    conn: ScriptedConnection,
    *,
    passes: int = 3,
    task_rows: list[tuple[Any, ...]] | None = None,
    pr_reads: list[Any] | None = None,
    guidance: str = "",
    final_task_row: tuple[Any, ...] | None = None,
    fail_final_event: bool = False,
) -> None:
    """The fresh-path composition: `passes` admission passes, then the
    finalization pass (guarded transition + pr_created audit)."""
    _script_repeating_reads(conn, guidance=guidance)
    rows = task_rows if task_rows is not None else [_task_row() for _ in range(passes)]
    prs = pr_reads if pr_reads is not None else [None for _ in range(passes)]
    for index in range(passes):
        _script_admission_pass(conn, task_row=rows[index], pr_read=prs[index])
        if index == 0:
            # The F2 pre-send dispatch claim commits after the first
            # admission pass and strictly before the PR_COMPOSE send
            # (issue #87).
            _script_pr_compose_claim(conn)
    final = final_task_row if final_task_row is not None else _task_row()
    conn.on(_TASK_READ, final)
    conn.on(_TASK_READ_LOCKED, final)
    conn.on(_TASK_READ, final)
    conn.on(_PR_READ, _pr_row())
    if fail_final_event:
        conn.on(_EVENT_INSERT, RuntimeError("forced late event failure"))
    conn.on(_STATUS_UPDATE, _task_row(status="reviewing", state_token=_ROTATED_TOKEN))
    conn.on_repeat(_EVENT_INSERT, _event_row())
    # The validated pr_result settles the send claim exactly once inside the
    # post-interaction revalidation transaction — before publication, and
    # without pretending GitHub publication has completed (issue #87).
    conn.on(_CLAIM_SETTLE, _claim_row(status="settled"))


def _script_continuation(
    conn: ScriptedConnection,
    *,
    observed_pr_row: tuple[Any, ...],
    finalize: str = "reviewing",
) -> None:
    """The continuation path: one admission pass over the persisted record,
    the authoritative reconciliation, then the finalization (or the recover
    block, or the merged refusal)."""
    _script_repeating_reads(conn)
    # --- admission over the persisted record (with the re-prove guard) ---
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_TASK_READ, _task_row())
    # --- reconciliation phase 1: task, canonical PR, route reads ---
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    # --- reconciliation write phase: barrier/repo-lock/observed update ---
    conn.on(_REPO_READ_LOCKED, _repository_row())
    conn.on(_PR_READ, _pr_row())  # the locked pre-image read
    conn.on(_PR_UPDATE, observed_pr_row)
    # --- finalization pass ---
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, observed_pr_row)
    if finalize == "reviewing":
        conn.on(_STATUS_UPDATE, _task_row(status="reviewing", state_token=_ROTATED_TOKEN))
    elif finalize == "block":
        conn.on(_TASK_READ_LOCKED, _task_row())
        conn.on(_TASK_READ, _task_row())
        conn.on(_BLOCK_INSERT, _block_row())
        conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_ROTATED_TOKEN))
    elif finalize == "cancel":
        # The trusted E10c cancellation composition (issue #229) replaces
        # the recoverable block: the workspace-derived account barrier and
        # the barrier reads come from the repeating reads, then the locked
        # Task, the locked canonical PR evidence, the loop reads, the
        # guarded archival with its terminal cause, and the terminal event.
        conn.on(_TASK_READ_LOCKED, _task_row())
        conn.on(_PR_READ, observed_pr_row)
        conn.on(_LOOPS_READ, [])
        conn.on(
            _ARCHIVE_UPDATE,
            _task_row(
                status="cancelled",
                state_token=_ROTATED_TOKEN,
                archived_at=_OBSERVED,
                terminal_cause="pr_closed_unmerged",
            ),
        )
    conn.on_repeat(_EVENT_INSERT, _event_row())


def _compose_call(
    pool: DatabasePool,
    github: Any,
    resolver: Any,
    runtime_factory: ProducerRuntimeAttachmentFactory,
    *,
    profile_id: Any = _OWNER_PROFILE_ID,
    expected_state_token: Any = _STATE_TOKEN,
    owner_gate_id: Any = _PR_GATE_ID,
) -> Any:
    return compose_and_publish_task_pr(
        pool,
        github,
        resolver,
        runtime_factory,
        profile_id=profile_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=owner_gate_id,
        expected_state_token=expected_state_token,
    )


def _publication_result(
    outcome: TaskPullRequestPublicationOutcome,
    *,
    pull_request: Any = None,
) -> TaskPullRequestPublication:
    """The primitive's typed result carrying the mapped canonical record."""
    from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState

    if pull_request is None:
        return TaskPullRequestPublication(outcome=outcome, pull_request=None)
    domain_pr = TaskPullRequest(
        id=pull_request[0],
        workspace_id=pull_request[1],
        task_id=pull_request[2],
        repository_id=pull_request[3],
        github_pr_id=pull_request[4],
        github_pr_number=pull_request[5],
        head_ref=pull_request[6],
        base_ref=pull_request[7],
        head_sha=pull_request[8],
        state=TaskPullRequestState(pull_request[9]),
        merged_at=pull_request[10],
        created_at=pull_request[11],
        updated_at=pull_request[12],
    )
    return TaskPullRequestPublication(outcome=outcome, pull_request=domain_pr)


# --- command classification -----------------------------------------------------------


def test_malformed_commands_are_classified_before_any_state_is_touched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    resolver = _FakeResolver()
    adapter = _prepared_adapter()
    _publication_fake(monkeypatch)

    with pytest.raises(InvalidCommandError):
        _compose_call(pool, github, resolver, _runtime_factory(adapter), profile_id="not-a-uuid")
    with pytest.raises(InvalidCommandError):
        _compose_call(
            pool, github, resolver, _runtime_factory(adapter), expected_state_token="not-a-uuid"
        )
    with pytest.raises(InvalidCommandError):
        compose_and_publish_task_pr(
            pool,
            github,
            resolver,
            "not-callable",  # type: ignore[arg-type]
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_PR_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert conn.executed == []


# --- durable approval admission and attribution ----------------------------------------


def test_the_durable_approval_authorizes_fresh_composition_and_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    resolver = _FakeResolver()
    adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
    publication = _publication_fake(
        monkeypatch,
        result=_publication_result(
            TaskPullRequestPublicationOutcome.PUBLISHED, pull_request=_pr_row()
        ),
    )

    outcome = _compose_call(pool, github, resolver, _runtime_factory(adapter))

    assert outcome.task.status is TaskStatus.REVIEWING
    assert outcome.task.state_token == _ROTATED_TOKEN
    assert outcome.pull_request.github_pr_id == _GITHUB_PR_ID
    assert outcome.pull_request.head_sha == _HEAD_SHA
    # The one fully grounded publication command under the approving Profile.
    (command,) = publication.commands
    assert command.profile_id == _OWNER_PROFILE_ID
    assert command.workspace_id == _WORKSPACE_ID
    assert command.task_id == _TASK_ID
    assert command.authorized_head_sha == _HEAD_SHA
    assert command.canonical_branch == _BRANCH
    assert command.base_ref == "main"
    assert command.state_token == _STATE_TOKEN
    assert command.title == _TITLE
    assert command.body == f"{_BODY}\n\nCloses #{_ISSUE_NUMBER}"
    # The one canonical interaction on the exact pre-existing binding.
    controls = adapter.realized_controls(_EXTERNAL_SESSION_ID)
    assert len(controls) == 1
    assert controls[0].expected_family == PR_RESULT_FAMILY
    assert "octocat/hello-world" in cast(str, controls[0].message)
    assert f"#{_ISSUE_NUMBER}" in cast(str, controls[0].message)
    sends = adapter.submitted_sends(_EXTERNAL_SESSION_ID)
    assert len(sends) == 1 and sends[0].formal
    # Exactly one primitive invocation; the safe finalization composition.
    assert len(publication.commands) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _PR_UPDATE) == 0
    assert _event_types(conn) == ["pr_created"]
    (created,) = _event_params_of_type(conn, "pr_created")
    assert created[3] == "openorc" and created[4] is None
    assert created[5] == "task_pull_request"


def test_workspace_guidance_is_composed_into_the_interaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_fresh_publication(conn, guidance="Ship it.")
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
    _publication_fake(
        monkeypatch,
        result=_publication_result(
            TaskPullRequestPublicationOutcome.PREFLIGHT_STALE, pull_request=None
        ),
    )
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_ROTATED_TOKEN))

    with pytest.raises(StaleOperationError):
        _compose_call(
            pool,
            github,
            _FakeResolver(),
            _runtime_factory(adapter),
        )

    # The current Owner-authored guidance reached the one interaction.
    (control,) = adapter.realized_controls(_EXTERNAL_SESSION_ID)
    assert "Ship it." in cast(str, control.message)


# --- the deterministic closing reference -----------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("Implemented.", f"Implemented.\n\nCloses #{_ISSUE_NUMBER}"),
        (f"Implemented.\n\nCloses #{_ISSUE_NUMBER}", f"Implemented.\n\nCloses #{_ISSUE_NUMBER}"),
        (
            f"Fixes octocat/hello-world#{_ISSUE_NUMBER} and more.",
            f"Fixes octocat/hello-world#{_ISSUE_NUMBER} and more.",
        ),
        ("Resolves #99 and #7.", f"Resolves #99 and #7.\n\nCloses #{_ISSUE_NUMBER}"),
        ("See other/repo#42.", f"See other/repo#42.\n\nCloses #{_ISSUE_NUMBER}"),
        (
            "A bare #42 without a keyword.",
            f"A bare #42 without a keyword.\n\nCloses #{_ISSUE_NUMBER}",
        ),
        (
            "```\nCloses #42\n```",
            "```\nCloses #42\n```" + f"\n\nCloses #{_ISSUE_NUMBER}",
        ),
        (
            "Use `Closes #42` in the description.",
            "Use `Closes #42` in the description." + f"\n\nCloses #{_ISSUE_NUMBER}",
        ),
    ],
)
def test_the_closing_reference_is_normalized_deterministically(body: str, expected: str) -> None:
    from openorc.protocol.models import PrResult
    from openorc.services.task_pr_composition import _normalized_pr_body

    result = PrResult(title=_TITLE, body=body)
    assert (
        _normalized_pr_body(
            result, repository_full_name="octocat/hello-world", issue_number=_ISSUE_NUMBER
        )
        == expected
    )


# --- admission refusals before any external call ----------------------------------------


@pytest.mark.parametrize(
    ("gate_row", "expected"),
    [
        (_gate_row(status="pending"), ConflictError),
        (_gate_row(status="cancelled"), ConflictError),
        (_gate_row(status="rejected"), ConflictError),
        (
            _gate_row(
                gate_type="implementation_authorization",
                plan_revision_id=uuid.uuid4(),
                subject_head_sha=None,
            ),
            ConflictError,
        ),
    ],
)
def test_unapproved_or_foreign_gate_types_refuse_before_any_effect(
    monkeypatch: pytest.MonkeyPatch, gate_row: tuple[Any, ...], expected: type[Exception]
) -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn, gate_row=gate_row, audit_rows=[_approved_audit_row()])
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, None)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    invocations: list[int] = []
    adapter = _prepared_adapter()
    _publication_fake(monkeypatch)

    with pytest.raises(expected):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter, invocations))

    assert invocations == []
    assert adapter.realized_controls() == ()
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _event_types(conn) == []


def test_a_gate_from_another_task_is_uniformly_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(
        conn,
        gate_row=(
            uuid.uuid4(),
            _WORKSPACE_ID,
            uuid.uuid4(),  # a foreign task id
            "pr_authorization",
            "approved",
            None,
            _HEAD_SHA,
            None,
            _OBSERVED,
            _OBSERVED,
        ),
    )
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, None)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    _publication_fake(monkeypatch)

    with pytest.raises(NotFoundError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(_prepared_adapter()))

    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_missing_or_ambiguous_owner_audit_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    github = _FakeGitHub(pool=FakePool(ScriptedConnection()))
    _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    conn = ScriptedConnection()
    _script_repeating_reads(conn, audit_rows=[])
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, None)
    with pytest.raises(ConflictError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))

    conn = ScriptedConnection()
    _script_repeating_reads(conn, audit_rows=[_approved_audit_row(), _approved_audit_row()])
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, None)
    with pytest.raises(ConflictError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))

    assert adapter.realized_controls() == ()
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_mismatched_caller_and_approver_is_rejected_not_substituted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn, audit_rows=[_approved_audit_row(actor_id=str(_OTHER_PROFILE_ID))])
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, None)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    with pytest.raises(AuthorizationError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert adapter.realized_controls() == ()
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_stale_token_or_moved_status_refuses_before_any_external_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    github = _FakeGitHub(pool=FakePool(ScriptedConnection()))
    _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    stale = _task_row(state_token=_ROTATED_TOKEN)
    conn.on(_TASK_READ, stale)
    conn.on(_TASK_READ_LOCKED, stale)
    conn.on(_TASK_READ, stale)
    with pytest.raises(StaleOperationError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))

    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    moved = _task_row(status="reviewing")
    conn.on(_TASK_READ, moved)
    conn.on(_TASK_READ_LOCKED, moved)
    conn.on(_TASK_READ, moved)
    with pytest.raises(ConflictError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))

    assert adapter.realized_controls() == ()
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_committed_duplicate_claim_refuses_before_any_external_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The F2 one-shot claim (issue #87): a raced or replayed PR_COMPOSE
    production for an already-claimed gate raises the typed stale error
    strictly before the send — the durable claim fact is the backstop."""
    github = _FakeGitHub(pool=FakePool(ScriptedConnection()))
    _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    fresh = _task_row()
    conn.on(_TASK_READ, fresh)
    conn.on(_TASK_READ_LOCKED, fresh)
    conn.on(_TASK_READ, fresh)
    conn.on(_PR_READ, None)
    conn.on(_TASK_READ, fresh)
    # The F2 dispatch claim's own locked Task reproof (issue #87).
    conn.on(_TASK_READ_LOCKED, fresh)
    _script_pr_compose_claim(conn, existing_claim=_claim_row())
    with pytest.raises(StaleOperationError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))

    assert adapter.realized_controls() == ()
    assert not any(_CLAIM_INSERT(sql, None) for sql, _ in conn.executed)


def test_an_unoperational_account_refuses_the_whole_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn, barrier=_ACCOUNT_DELETION_ACTIVE_ROW)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    with pytest.raises(ConflictError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert adapter.realized_controls() == ()
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


# --- the narrow continuation of an interrupted publication ------------------------------


def _reconciliation_observation(
    *,
    head_sha: str = _HEAD_SHA,
    state: str = "open",
) -> GitHubPullRequestObservation:
    return GitHubPullRequestObservation(
        github_pr_id=_GITHUB_PR_ID,
        pull_number=_PR_NUMBER,
        head_ref=_BRANCH,
        head_sha=head_sha,
        base_ref="main",
        state=state,
        merged=False,
        merged_at=None,
    )


def test_the_narrow_continuation_completes_without_composition_or_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_continuation(conn, observed_pr_row=_pr_row())
    pool = _pool(conn)
    github = _FakeGitHub(
        pool=cast(FakePool, pool),
        repository_observation=GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        pull_request_observation=_reconciliation_observation(),
    )
    invocations: list[int] = []
    adapter = _prepared_adapter()
    publication = _publication_fake(monkeypatch)

    outcome = _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter, invocations))

    assert outcome.task.status is TaskStatus.REVIEWING
    assert outcome.task.state_token == _ROTATED_TOKEN
    assert outcome.pull_request.head_sha == _HEAD_SHA
    # The composition and the create are never repeated on this path.
    assert invocations == []
    assert adapter.realized_controls() == ()
    assert adapter.submitted_sends() == ()
    assert publication.commands == []
    assert len(github.pull_request_calls) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _event_types(conn) == ["pr_created"]


@pytest.mark.parametrize(
    ("observed_row", "observation", "expected_reason"),
    [
        (_pr_row(base_ref="develop"), _reconciliation_observation(), "stale_operation"),
    ],
)
def test_an_ineligible_reconciled_record_blocks_recoverably(
    monkeypatch: pytest.MonkeyPatch,
    observed_row: tuple[Any, ...],
    observation: GitHubPullRequestObservation,
    expected_reason: str,
) -> None:
    conn = ScriptedConnection()
    _script_continuation(conn, observed_pr_row=observed_row, finalize="block")
    pool = _pool(conn)
    github = _FakeGitHub(
        pool=cast(FakePool, pool),
        repository_observation=GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        pull_request_observation=observation,
    )
    publication = _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    with pytest.raises(StaleOperationError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert publication.commands == []
    assert adapter.realized_controls() == ()
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _event_types(conn) == ["task_blocked"]


@pytest.mark.parametrize(
    ("observed_row", "observation"),
    [
        (_pr_row(state="closed"), _reconciliation_observation(state="closed")),
        (
            # The precedence regression (issue #229): a drifted head SHA
            # must never mask the confirmed closed-unmerged terminal
            # condition as a stale publication subject.
            _pr_row(state="closed", head_sha=_OTHER_HEAD_SHA),
            _reconciliation_observation(state="closed"),
        ),
    ],
)
def test_a_confirmed_closed_unmerged_publication_cancels_the_task_terminally(
    monkeypatch: pytest.MonkeyPatch,
    observed_row: tuple[Any, ...],
    observation: GitHubPullRequestObservation,
) -> None:
    conn = ScriptedConnection()
    _script_continuation(conn, observed_pr_row=observed_row, finalize="cancel")
    pool = _pool(conn)
    github = _FakeGitHub(
        pool=cast(FakePool, pool),
        repository_observation=GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        pull_request_observation=observation,
    )
    publication = _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    with pytest.raises(StaleOperationError, match="terminally cancelled"):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert publication.commands == []
    assert adapter.realized_controls() == ()
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _ARCHIVE_UPDATE) == 1
    assert _event_types(conn) == ["task_cancelled"]


def test_a_confirmed_merge_refuses_without_false_recovery_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_continuation(conn, observed_pr_row=_pr_row(state="closed", merged_at=_OBSERVED))
    pool = _pool(conn)
    github = _FakeGitHub(
        pool=cast(FakePool, pool),
        repository_observation=GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        pull_request_observation=_reconciliation_observation(state="closed"),
    )
    publication = _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    with pytest.raises(ConflictError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert publication.commands == []
    assert adapter.realized_controls() == ()
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


# --- publication outcomes, runtime failures, and atomicity ------------------------------


def test_preflight_stale_blocks_the_task_atomically_and_creates_no_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
    publication = _publication_fake(
        monkeypatch,
        result=_publication_result(
            TaskPullRequestPublicationOutcome.PREFLIGHT_STALE, pull_request=None
        ),
    )
    # The recoverable block composition after the primitive's classification.
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_ROTATED_TOKEN))

    with pytest.raises(StaleOperationError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert len(publication.commands) == 1
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _event_types(conn) == ["task_blocked"]


def test_post_create_stale_preserves_the_record_and_blocks_or_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    github = _FakeGitHub(pool=FakePool(ScriptedConnection()))

    # POST_CREATE_HEAD_MISMATCH with a still-current Task: the recoverable
    # block records the preserved created PR's observed head.
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    pool = _pool(conn)
    _publication_fake(
        monkeypatch,
        result=_publication_result(
            TaskPullRequestPublicationOutcome.POST_CREATE_HEAD_MISMATCH,
            pull_request=_pr_row(head_sha=_RACED_SHA),
        ),
    )
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_ROTATED_TOKEN))
    with pytest.raises(StaleOperationError):
        _compose_call(
            pool,
            github,
            _FakeResolver(),
            _runtime_factory(
                _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
            ),
        )
    assert _count(conn, _PR_UPDATE) == 0
    assert _event_types(conn) == ["task_blocked"]

    # POST_CREATE_AUTHORITY_STALE with a moved-on Task: zero mutation.
    conn = ScriptedConnection()
    _script_fresh_publication(conn, final_task_row=_task_row(state_token=_ROTATED_TOKEN))
    pool = _pool(conn)
    _publication_fake(
        monkeypatch,
        result=_publication_result(
            TaskPullRequestPublicationOutcome.POST_CREATE_AUTHORITY_STALE,
            pull_request=_pr_row(),
        ),
    )
    conn.on(_TASK_READ_LOCKED, _task_row(state_token=_ROTATED_TOKEN))
    conn.on(_TASK_READ, _task_row(state_token=_ROTATED_TOKEN))
    with pytest.raises(StaleOperationError):
        _compose_call(
            pool,
            github,
            _FakeResolver(),
            _runtime_factory(
                _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
            ),
        )
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


@pytest.mark.parametrize(
    "error",
    [
        ExternalOperationFailedError("known provider condition"),
        ExternalOperationUncertainError("unknown outcome"),
        ConflictError("an existing external pull request is never adopted"),
        ExternalOperationReconciliationRequiredError("un-reconciled creation"),
        ExternalOperationRecoveryRequiredError("persistence failed after creation"),
    ],
)
def test_primitive_failures_propagate_without_durable_effects(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
    _publication_fake(monkeypatch, error=error)

    with pytest.raises(type(error)):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _event_types(conn) == []


def test_uncertain_and_failed_runtime_outcomes_never_resend_or_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    github = _FakeGitHub(pool=FakePool(ScriptedConnection()))
    _publication_fake(monkeypatch)

    # Uncertain outcome: the request may have been delivered; never resent.
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    adapter = _prepared_adapter(
        send_outcomes=(AgentRuntimeUncertainOutcomeError("unknown send outcome"),)
    )
    with pytest.raises(ExternalOperationUncertainError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))
    assert len(adapter.submitted_sends(_EXTERNAL_SESSION_ID)) == 1
    assert _event_types(conn) == []
    # The committed claim stays conservatively claimed through uncertainty
    # (issue #87): never reclassified, never settled.
    claim_inserts = [sql for sql, _ in conn.executed if _CLAIM_INSERT(sql, None)]
    assert len(claim_inserts) == 1
    assert not any(_CLAIM_SETTLE(sql, None) for sql, _ in conn.executed)

    # A protocol failure (no valid pr_result) arises only after the send did
    # work: an unresolved outcome, never a proven non-delivery (issue #87).
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    adapter = _prepared_adapter(
        send_outcomes=(AgentRuntimeProtocolFailureError("the reply was not a valid pr_result"),)
    )
    with pytest.raises(ExternalOperationUncertainError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))
    assert len(adapter.submitted_sends(_EXTERNAL_SESSION_ID)) == 1
    assert _event_types(conn) == []
    claim_inserts = [sql for sql, _ in conn.executed if _CLAIM_INSERT(sql, None)]
    assert len(claim_inserts) == 1
    assert not any(_CLAIM_SETTLE(sql, None) for sql, _ in conn.executed)

    # A native-only realization carries no formal result after possible
    # composition work: an unresolved outcome, never a proven non-delivery.
    conn = ScriptedConnection()
    _script_fresh_publication(conn)
    adapter = _prepared_adapter()
    adapter.script_realization(_EXTERNAL_SESSION_ID, ControlRealization.NATIVE_ONLY)
    with pytest.raises(ExternalOperationUncertainError):
        _compose_call(_pool(conn), github, _FakeResolver(), _runtime_factory(adapter))
    assert _event_types(conn) == []
    claim_inserts = [sql for sql, _ in conn.executed if _CLAIM_INSERT(sql, None)]
    assert len(claim_inserts) == 1
    assert not any(_CLAIM_SETTLE(sql, None) for sql, _ in conn.executed)


def test_stale_subjects_after_the_interaction_never_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    rotated = _task_row(state_token=_ROTATED_TOKEN)
    _script_fresh_publication(conn, task_rows=[_task_row(), _task_row(), rotated])
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
    publication = _publication_fake(monkeypatch)

    with pytest.raises(StaleOperationError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    # The interaction ran, but Producer prose is never authorization to
    # publish a moved-on subject.
    assert len(adapter.realized_controls(_EXTERNAL_SESSION_ID)) == 1
    assert publication.commands == []
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_forced_final_event_failure_rolls_back_and_the_continuation_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # First invocation: the finalize's pr_created insert fails; the whole
    # finalization rolls back and the published record stays persisted.
    conn = ScriptedConnection()
    _script_fresh_publication(conn, fail_final_event=True)
    pool = _pool(conn)
    github = _FakeGitHub(pool=cast(FakePool, pool))
    adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
    _publication_fake(
        monkeypatch,
        result=_publication_result(
            TaskPullRequestPublicationOutcome.PUBLISHED, pull_request=_pr_row()
        ),
    )
    with pytest.raises(RuntimeError, match="forced late event failure"):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))
    # The transition statement ran but the paired audit insert raised: the
    # composed transaction rolls back, so the invocation never returns a
    # review-safe result and the recovery invocation below completes it.
    assert _count(conn, _EVENT_INSERT) == 1

    # The immediately following invocation continues on the persisted
    # record: no repeated composition, no second create, and the transition
    # plus its audit complete.
    conn = ScriptedConnection()
    _script_continuation(conn, observed_pr_row=_pr_row())
    pool = _pool(conn)
    github = _FakeGitHub(
        pool=cast(FakePool, pool),
        repository_observation=GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        pull_request_observation=_reconciliation_observation(),
    )
    publication = _publication_fake(monkeypatch)
    outcome = _compose_call(pool, github, _FakeResolver(), _runtime_factory(_prepared_adapter()))
    assert outcome.task.status is TaskStatus.REVIEWING
    assert publication.commands == []
    assert _event_types(conn) == ["pr_created"]


def test_telemetry_carries_only_the_sanctioned_vocabulary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from opentelemetry import trace

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    previous = trace.get_tracer_provider()
    trace.set_tracer_provider(provider)
    try:
        conn = ScriptedConnection()
        _script_fresh_publication(conn)
        pool = _pool(conn)
        github = _FakeGitHub(pool=cast(FakePool, pool))
        adapter = _prepared_adapter(send_outcomes=(pr_result_candidate(title=_TITLE, body=_BODY),))
        _publication_fake(
            monkeypatch,
            result=_publication_result(
                TaskPullRequestPublicationOutcome.PUBLISHED, pull_request=_pr_row()
            ),
        )
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))
    finally:
        trace.set_tracer_provider(previous)

    spans = exporter.get_finished_spans()
    assert "task_pr_composition.compose_and_publish_task_pr" in [span.name for span in spans]
    for span in spans:
        assert set(span.attributes or {}) <= _SANCTIONED_ATTRIBUTE_NAMES


# --- reconciled head-SHA drift (review remediation) -------------------------------------


def test_continuation_reconciliation_observing_a_changed_head_blocks_recoverably(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Head-SHA drift observed by the authoritative continuation
    reconciliation is recoverable stale publication authority: the atomic
    ``stale_operation`` block composition applies where the Task remains
    current, and the canonical record is never recreated or replaced."""
    conn = ScriptedConnection()
    _script_continuation(
        conn,
        observed_pr_row=_pr_row(head_sha=_RACED_SHA),
        finalize="block",
    )
    pool = _pool(conn)
    github = _FakeGitHub(
        pool=cast(FakePool, pool),
        repository_observation=GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        pull_request_observation=_reconciliation_observation(head_sha=_RACED_SHA),
    )
    publication = _publication_fake(monkeypatch)
    adapter = _prepared_adapter()

    with pytest.raises(StaleOperationError):
        _compose_call(pool, github, _FakeResolver(), _runtime_factory(adapter))

    assert publication.commands == []
    assert adapter.realized_controls() == ()
    # Only the reconciliation's own update-only write touched the record:
    # the drift block never recreates or replaces the canonical PR.
    assert _count(conn, _PR_UPDATE) == 1
    assert _count(conn, _BLOCK_INSERT) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _event_types(conn) == ["task_blocked"]
