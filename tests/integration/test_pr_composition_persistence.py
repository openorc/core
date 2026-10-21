"""Durable atomicity of the E8 PR-composition seam (issue #83).

One targeted real-Postgres check the new seam owns end to end: the
E7-approved historical gate and its OWNER-attributed audit admit the
composition, the race-safe primitive persists the canonical record, a forced
late ``pr_created`` failure rolls the finalization back without erasing the
created PR, and the immediately following continuation invocation completes
``waiting_for_owner → reviewing`` plus exactly one ``pr_created`` event —
with a representative stale assertion that a moved-on Task admits nothing
further. The composition and the create are never repeated on the
continuation path.

No live GitHub, runtime, or Supabase access — the GitHub App client is a
local fake returning fixed observations, and the suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubBranchObservation,
    GitHubIssueObservation,
    GitHubPullRequestFacts,
    GitHubPullRequestObservation,
    GitHubRepositoryObservation,
)
from openorc.adapters.github.capabilities import GitHubUserAccessValidation
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import ConflictError
from openorc.services.github_user_authorization import GitHubUserAccessToken
from openorc.services.task_pr_composition import (
    ProducerRuntimeAttachmentFactory,
    compose_and_publish_task_pr,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    pr_result_candidate,
    session_ready_candidate,
)
from tests.integration.test_pr_authorization_persistence import (
    _BRANCH,
    _GITHUB_ISSUE_ID,
    _HEAD_SHA,
    _ISSUE_NUMBER,
    _pr_authorization_fixture,
    approve_pr_authorization,
    open_pr_authorization,
)

pytestmark = pytest.mark.integration

_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77
_TITLE = "feat: implement task 42"
_BODY = "Implemented the retry loop."


class _FakeGitHubAppClient:
    """A local fixed-observation client serving the E8/B8 reads and create."""

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        return GitHubRepositoryObservation(
            github_repository_id=github_repository_id,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        assert issue_number == _ISSUE_NUMBER
        return GitHubIssueObservation(
            github_issue_id=_GITHUB_ISSUE_ID,
            issue_number=issue_number,
            title="Found a bug",
            body="Requirements body",
            state="open",
            provider_updated_at=datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC),
            is_pull_request=False,
        )

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        assert branch_name == _BRANCH
        return GitHubBranchObservation(branch_name=branch_name, head_sha=_HEAD_SHA)

    def create_pull_request(
        self,
        *,
        github_installation_id: int,
        credential: Any,
        owner_login: str,
        repository_name: str,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str | None,
    ) -> GitHubPullRequestFacts:
        assert head_ref == _BRANCH and base_ref == "main"
        assert body is not None and f"Closes #{_ISSUE_NUMBER}" in body
        return GitHubPullRequestFacts(
            github_pr_id=_GITHUB_PR_ID,
            pull_number=_PR_NUMBER,
            head_ref=head_ref,
            head_sha=_HEAD_SHA,
            base_ref=base_ref,
            state="open",
            merged=False,
            merged_at=None,
        )

    def get_repository_pull_request(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
    ) -> GitHubPullRequestObservation:
        assert pull_number == _PR_NUMBER
        return GitHubPullRequestObservation(
            github_pr_id=_GITHUB_PR_ID,
            pull_number=pull_number,
            head_ref=_BRANCH,
            head_sha=_HEAD_SHA,
            base_ref="main",
            state="open",
            merged=False,
            merged_at=None,
        )

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        return GitHubUserAccessValidation(
            github_installation_id=77_000_007,
            github_repository_id=77_000_009,
        )

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

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E8 seam must not request merges")


class _FakeResolver:
    def resolve(self, pool: DatabasePool, *, profile_id: Any) -> GitHubUserAccessToken:
        from datetime import UTC, datetime, timedelta

        return GitHubUserAccessToken(
            value="ghu_owner_user_token",
            expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
        )

    def evict_cached_access_token(self, profile_id: Any) -> None:
        raise AssertionError("the E8 seam never evicts credentials")


def _prepared_adapter(workspace_id: Any, task_id: Any) -> FakeAgentRuntimeAdapter:
    """One fake runtime holding the live Producer session on the exact id."""
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    adapter.queue_send(created.external_session_id, pr_result_candidate(title=_TITLE, body=_BODY))
    return adapter


def _runtime_factory(adapter: FakeAgentRuntimeAdapter) -> ProducerRuntimeAttachmentFactory:
    @contextmanager
    def factory() -> Iterator[FakeAgentRuntimeAdapter]:
        yield adapter

    return factory


def _event_count(conn: Connection[Any], event_type: str) -> int:
    row = conn.execute(
        "select count(*) from openorc.workflow_events where event_type = %s",
        (event_type,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_e8_publication_survives_interrupted_finalization_and_completes(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The full interrupted-finalization arc on a real Postgres: E7 approval
    → E8 composition/publication whose forced late ``pr_created`` failure
    rolls back without erasing the created PR → the continuation invocation
    completes ``reviewing`` plus exactly one ``pr_created`` event → a
    moved-on Task admits nothing further."""
    facts = _pr_authorization_fixture(conn)
    pool = cast(DatabasePool, facts["pool"])
    github = _FakeGitHubAppClient()
    resolver = _FakeResolver()

    # --- E7: present and approve the exact-head gate ---
    opened = open_pr_authorization(
        pool,
        github,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
    )
    approved = approve_pr_authorization(
        pool,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        owner_gate_id=opened.owner_gate.id,
        expected_state_token=opened.task.state_token,
    )
    assert approved.owner_gate.status is OwnerGateStatus.APPROVED
    assert approved.owner_gate.gate_type is OwnerGateType.PR_AUTHORIZATION

    # --- the Producer session identity matches the fake runtime session ---
    adapter = _prepared_adapter(facts["workspace_id"], facts["task_id"])
    conn.execute(
        "update openorc.task_agent_sessions set external_session_id = %s "
        "where task_id = %s and role = 'producer'",
        (adapter.created_session_ids()[0], facts["task_id"]),
    )

    # --- E8 invocation 1: publication succeeds but the final event insert
    # is forced to fail — the finalization rolls back and the created PR is
    # an external fact that is never erased. ---
    def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("forced late pr_created failure")

    # Scope the failure injection to the first invocation only: the real
    # event recorder must be restored before the continuation recovery.
    with monkeypatch.context() as failure_patch:
        failure_patch.setattr(
            event_coordination,
            "record_pr_created_event",
            _boom,
        )
        with pytest.raises(RuntimeError, match="forced late pr_created failure"):
            compose_and_publish_task_pr(
                pool,
                github,
                resolver,
                _runtime_factory(adapter),
                profile_id=facts["profile_id"],
                workspace_id=facts["workspace_id"],
                task_id=facts["task_id"],
                owner_gate_id=approved.owner_gate.id,
                expected_state_token=approved.task.state_token,
            )
    interrupted = task_repositories.get_task(pool, facts["task_id"])
    assert interrupted is not None
    assert interrupted.status is TaskStatus.WAITING_FOR_OWNER
    assert interrupted.state_token == approved.task.state_token
    assert _event_count(conn, "pr_created") == 0
    pr_row = conn.execute(
        "select github_pr_id, head_sha, state from openorc.task_pull_requests where task_id = %s",
        (facts["task_id"],),
    ).fetchone()
    assert pr_row is not None
    assert pr_row[0] == _GITHUB_PR_ID and pr_row[1] == _HEAD_SHA and pr_row[2] == "open"

    # --- E8 invocation 2: the narrow continuation reconciles the persisted
    # record authoritatively and completes the state/event finalization —
    # never repeating the composition or the create. ---
    outcome = compose_and_publish_task_pr(
        pool,
        github,
        resolver,
        _runtime_factory(_prepared_adapter(facts["workspace_id"], facts["task_id"])),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        owner_gate_id=approved.owner_gate.id,
        expected_state_token=approved.task.state_token,
    )
    assert outcome.task.status is TaskStatus.REVIEWING
    assert outcome.pull_request.github_pr_id == _GITHUB_PR_ID
    assert outcome.pull_request.head_sha == _HEAD_SHA
    finalized = task_repositories.get_task(pool, facts["task_id"])
    assert finalized is not None and finalized.status is TaskStatus.REVIEWING
    assert _event_count(conn, "pr_created") == 1
    (event_row,) = conn.execute(
        "select actor_type, actor_id, subject_type from openorc.workflow_events "
        "where event_type = 'pr_created'",
    ).fetchall()
    assert event_row[0] == "openorc" and event_row[1] is None
    assert event_row[2] == "task_pull_request"

    # --- representative stale assertion: the moved-on Task admits nothing ---
    with pytest.raises(ConflictError):
        compose_and_publish_task_pr(
            pool,
            github,
            resolver,
            _runtime_factory(_prepared_adapter(facts["workspace_id"], facts["task_id"])),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            owner_gate_id=approved.owner_gate.id,
            expected_state_token=approved.task.state_token,
        )
    assert _event_count(conn, "pr_created") == 1
