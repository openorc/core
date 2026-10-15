"""Opt-in live D7 integration check (issue #149): the assembled production
Core Cline path exercised against the pinned supported runtime baseline.

What this module is
-------------------

One small, explicitly Owner-operated live validation suite for the frozen
supported trio ``cline 3.0.68`` / ``@cline/core 0.0.90`` / ``@cline/sdk
0.0.90``. It drives the production path end to end —
``attach_cline_runtime`` (#190) with the real ``ClineBridgeProcessBackend``
(#73) over the local Node D3 bridge, the durable D8a construction-context
resolver, and the ``ClineAdapter`` — against a ready, safely reachable
managed ClineBox through an ordinary enabled Core Connection and the #55
trusted credential boundary. It is not a generic certification framework,
not a second fake-backed D2-D8 suite, and never part of CI.

Gating
------

- The ``integration`` marker keeps the module out of the ordinary
  deterministic baseline (the default pytest ``addopts`` deselect it).
- An explicit live opt-in is additionally required: ``OPENORC_CLINE_LIVE=1``.
  Without it every test skips cleanly. With it, a missing prerequisite is a
  clear preflight failure — never a silent skip and never a Box contact.
- ``OPENORC_TEST_DATABASE_URL`` must supply a non-production disposable
  Supabase branch database (the Owner-run ``devserver.sh --testdb`` flow).
  The suite consumes the database it is given and never provisions one.

Remote/local boundary
---------------------

Session working directories live on the remote ClineBox. This module never
asserts remote path existence through its own filesystem. Task-checkout and
cleanup verification always runs on the actual Box through the required
Owner-approved ``OPENORC_CLINE_LIVE_BOX_CHECK_COMMAND`` operator-check hook:
autonomous execution requires genuine Box-side verification, never an
unperformed Owner attestation. Local preflight covers only local
prerequisites plus the dynamic fixture-subject resolution over the public
GitHub API.

The prepared fixture PR's head SHA is resolved from GitHub at every preflight
and pinned for the review interaction; the prepared base and head branches
and the head repository are validated with it, and head movement during the
review is a stop condition, never a silent retarget. After a controlled Hub
restart that rotated the attach credential, the CURRENT credential is
acquired through the test-only credential-supplier command hook (executed
fresh at refresh time — a restart helper cannot update this process's
environment) and installed through the test-only
``_refresh_current_hub_credential`` interface over the existing #55 trusted
Vault rotation service; together they mirror the future Cloud operator
runner's post-restart acquisition and refresh (openorc/cloud#54). Actual
live verification of this suite is performed manually by the Owner after the
Core and Cloud sides are both in place; nothing here runs live inference in
CI or ordinary test runs.

Scenarios (run the module whole; tests rely on module-scoped state in file
order): A fresh attachments/initialization/exact binding; B Producer
workspace lifecycle and original-base stability; C Reviewer verified
GitHub-MCP exact-head review through the D5b tool-evidence gate; D
interruption/reconciliation (adapter-local guard semantics vs.
cross-attachment continuity) plus the required Hub-restart recovery
completion gate; E observational activity/usage; and authorized workspace
cleanup.

Content discipline: raw attach credentials, Cline-native secrets,
transcripts, prompts, and source content never enter test evidence, logs,
exceptions, or telemetry. Recorded evidence carries only safe identifiers,
versions, continuity classifications, and harmless bounded observations.
"""

from __future__ import annotations

import json
import os
import secrets
import shlex
import shutil
import signal
import subprocess
import threading
import time
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import ExitStack, suppress
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from psycopg import Connection
from psycopg_pool import ConnectionPool
from tests.integration.cline_live_support import (
    FIXTURE_CHANGED_PATH,
    FIXTURE_PR_NUMBER,
    FIXTURE_REPOSITORY,
    BoxCheckVerificationError,
    CredentialSupplierError,
    FixtureSubjectError,
    assert_head_unchanged,
    classify_box_check_exit,
    credential_from_supplier_output,
    resolve_fixture_head_sha,
)

from openorc.adapters.agent_runtime.errors import AgentRuntimeUncertainOutcomeError
from openorc.adapters.agent_runtime.results import AgentTextResponse
from openorc.adapters.cline import (
    ClineAccumulatedUsage,
    ClineActivityKind,
    ClineCommentaryActivity,
    ClineConstructionMode,
    ClineDispatchCheckpoint,
    ClineSessionContinuity,
    ConnectionControlEndpointError,
    parse_control_endpoint_url,
)
from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.persistence import connections as connection_repositories
from openorc.persistence import runtime_control_secrets
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import (
    CleanupWorkspaceInteraction,
    ImplementInteraction,
    PrepareWorkspaceInteraction,
    PrReviewSubject,
    ReviewInteraction,
)
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    WORKSPACE_RESULT_FAMILY,
    ImplementationResult,
    PlanResult,
    ReviewResult,
    WorkspaceResult,
)
from openorc.services.cline_attachment import (
    AttachedClineRuntime,
    ClineAttachmentPhase,
    attach_cline_runtime,
)
from openorc.services.connection_credentials import (
    configure_connection_credential,
    rotate_connection_credential,
)
from openorc.services.errors import (
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
)
from openorc.services.session_admission import (
    AdmissionStatus,
    admit_task_agent_sessions,
)
from openorc.services.session_runtime import establish_task_agent_session

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BRIDGE_PACKAGE_DIR = _REPO_ROOT / "packages" / "cline-sdk-bridge"
_BRIDGE_ENTRYPOINT = _BRIDGE_PACKAGE_DIR / "dist" / "cli.js"
_BRIDGE_PACKAGE_JSON = _BRIDGE_PACKAGE_DIR / "package.json"

# The frozen supported runtime trio (issue #149). A mismatch blocks the
# qualification instead of silently changing a pin: changing a version is a
# separate identify/requalify decision, never a D7 fix.
_PINNED_CLINE_VERSION = "3.0.68"
_PINNED_CORE_VERSION = "0.0.90"
_PINNED_SDK_VERSION = "0.0.90"

# The prepared, deliberately unmerged review fixture identity (repository,
# PR number, and sole changed file) lives in
# ``tests/integration/cline_live_support.py`` together with its validation
# rules. The head SHA is NOT fixed: each run resolves it from GitHub and
# pins it for the review interaction.

_OPT_IN_ENV = "OPENORC_CLINE_LIVE"
_RESUME_ENV = "OPENORC_CLINE_LIVE_RESUME"
_ATTESTED_VERSIONS_ENV = "OPENORC_CLINE_LIVE_ATTESTED_VERSIONS"
_CONTROL_ENDPOINT_ENV = "OPENORC_CLINE_LIVE_CONTROL_ENDPOINT"
_HUB_TOKEN_ENV = "OPENORC_CLINE_LIVE_HUB_TOKEN"
_PRODUCER_PROVIDER_ENV = "OPENORC_CLINE_LIVE_PRODUCER_PROVIDER"
_PRODUCER_MODEL_ENV = "OPENORC_CLINE_LIVE_PRODUCER_MODEL"
_REVIEWER_PROVIDER_ENV = "OPENORC_CLINE_LIVE_REVIEWER_PROVIDER"
_REVIEWER_MODEL_ENV = "OPENORC_CLINE_LIVE_REVIEWER_MODEL"
_CREDENTIAL_COMMAND_ENV = "OPENORC_CLINE_LIVE_CREDENTIAL_COMMAND"
_REPOSITORY_BASE_ENV = "OPENORC_CLINE_LIVE_REPOSITORY_BASE"
_REVIEWER_DIRECTORY_ENV = "OPENORC_CLINE_LIVE_REVIEWER_DIRECTORY"
_HANDOFF_ENV = "OPENORC_CLINE_LIVE_HANDOFF_PATH"
_RESTART_COMMAND_ENV = "OPENORC_CLINE_LIVE_RESTART_COMMAND"
_PAUSE_SENTINEL_ENV = "OPENORC_CLINE_LIVE_PAUSE_SENTINEL"
_PAUSE_TIMEOUT_ENV = "OPENORC_CLINE_LIVE_PAUSE_TIMEOUT_SECONDS"
_ROTATE_ENV = "OPENORC_CLINE_LIVE_ROTATE_CREDENTIAL"
_BOX_CHECK_ENV = "OPENORC_CLINE_LIVE_BOX_CHECK_COMMAND"
_DATABASE_URL_ENV = "OPENORC_TEST_DATABASE_URL"

_ACTIVITY_WAIT_SECONDS = 240.0
_TURN_WAIT_SECONDS = 600.0
_RECONNECT_ATTEMPTS = 12
_RECONNECT_DELAY_SECONDS = 10.0
_RESTART_SETTLE_SECONDS = 20.0
_DEFAULT_PAUSE_TIMEOUT_SECONDS = 1800.0
_INTERRUPTED_TURN_TAIL_SECONDS = 45.0

# --- local preflight (before any agent effect) -------------------------------


def _required_env(name: str, purpose: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.fail(f"preflight: {name} is required for {purpose}")
    return value


def _check_bridge_sdk_pin() -> None:
    try:
        manifest = json.loads(_BRIDGE_PACKAGE_JSON.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        pytest.fail(f"preflight: the bridge package manifest is unreadable ({error})")
    dependencies = {**manifest.get("dependencies", {}), **manifest.get("devDependencies", {})}
    pinned = dependencies.get("@cline/sdk")
    if pinned != _PINNED_SDK_VERSION:
        pytest.fail(
            "preflight: the bridge pins @cline/sdk "
            f"{pinned!r}, not the supported {_PINNED_SDK_VERSION!r}; a pin change "
            "is a separate requalification, never a D7 fix"
        )


def _check_attested_remote_versions() -> str:
    """Require the Owner-attested actual Box runtime versions to match the trio.

    The attestation itself is produced by Owner-operated supported means (the
    Cloud cline-box verification flow that checks the CLI pin and fails closed
    on drift, or the recorded current Box facts) — never by this process.
    """
    raw = os.environ.get(_ATTESTED_VERSIONS_ENV, "").strip()
    if not raw:
        pytest.fail(
            "preflight: OPENORC_CLINE_LIVE_ATTESTED_VERSIONS is required; first "
            "verify the live Box's actual runtime versions with the Owner-operated "
            "cline-box verification flow or recorded Box facts, then attest them "
            "here (format: 'cline@3.0.68 core@0.0.90 sdk@0.0.90')"
        )
    attested: dict[str, str] = {}
    for token in raw.split():
        component, separator, version = token.partition("@")
        if separator and component.strip() and version.strip():
            attested[component.strip().lower()] = version.strip()
    expected = {
        "cline": _PINNED_CLINE_VERSION,
        "core": _PINNED_CORE_VERSION,
        "sdk": _PINNED_SDK_VERSION,
    }
    mismatched = [
        f"{component}: attested {attested.get(component)!r} != supported {version!r}"
        for component, version in expected.items()
        if attested.get(component) != version
    ]
    if mismatched:
        pytest.fail(
            "preflight: the attested ClineBox runtime versions do not match the "
            "supported trio; a mismatch blocks qualification instead of changing "
            "pins: " + "; ".join(mismatched)
        )
    return raw


def _fetch_fixture_pull_request() -> tuple[dict[str, object], list[dict[str, object]]]:
    """Fetch the prepared fixture PR and its changed files from the public API.

    Unauthenticated, non-secret reads of the public dummy repository; no
    credential and no runtime involvement.
    """
    headers = {
        "User-Agent": "openorc-d7-live-validation",
        "Accept": "application/vnd.github+json",
    }
    base_url = f"https://api.github.com/repos/{FIXTURE_REPOSITORY}/pulls/{FIXTURE_PR_NUMBER}"

    def _fetch(url: str) -> object:
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=15.0) as response:
                return json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError) as error:
            pytest.fail(
                "preflight: the prepared fixture PR could not be re-read from the "
                f"public GitHub API ({error}); resolve the network route before "
                "the live run"
            )

    payload = _fetch(base_url)
    files = _fetch(f"{base_url}/files")
    if not isinstance(payload, dict) or not isinstance(files, list):
        pytest.fail(
            "preflight: the public GitHub API answered the fixture PR read with an "
            "unexpected payload shape"
        )
    return payload, files


def _resolve_fixture_head() -> str:
    """Resolve and pin the fixture PR's current head for this run.

    The head SHA is resolved from GitHub immediately before the run and
    validated against the prepared PR identity and fixture structure; it is
    then pinned for the review interaction. A moved head is re-resolved on
    the next deliberate run, never silently retargeted mid-run.
    """
    payload, files = _fetch_fixture_pull_request()
    try:
        return resolve_fixture_head_sha(payload, files)
    except FixtureSubjectError as error:
        pytest.fail(
            f"preflight: the prepared fixture PR is not the expected review subject ({error})"
        )


def _preflight(*, resume: bool) -> tuple[str, str]:
    """Local, non-secret preflight; fails closed before any agent effect.

    Local scope only: the Node runtime, built bridge artifacts, the bridge's
    SDK pin, the Owner-attested remote trio, configuration syntax, the
    required Box-side operator-check hook, and the dynamically resolved
    fixture PR subject. Remote Box facts are otherwise never probed from
    this process.
    """
    if shutil.which("node") is None:
        pytest.fail("preflight: node is not on PATH; the D3 bridge child cannot launch")
    if not _BRIDGE_ENTRYPOINT.is_file():
        pytest.fail(
            "preflight: packages/cline-sdk-bridge/dist/cli.js is not built; run "
            "`npm ci && npm run build` in packages/cline-sdk-bridge first"
        )
    _check_bridge_sdk_pin()
    attested = _check_attested_remote_versions()
    # Autonomous execution requires genuine Box-side checkout and cleanup
    # verification: the operator-check hook is a required prerequisite, never
    # a substitutable unperformed Owner attestation.
    _required_env(
        _BOX_CHECK_ENV,
        "genuine Box-side checkout/cleanup verification (an operator-check "
        "command template with a {path} placeholder, exit 0 = path exists)",
    )
    if resume:
        _required_env(_HANDOFF_ENV, "the stage-two handoff record")
    else:
        endpoint = _required_env(_CONTROL_ENDPOINT_ENV, "the Connection control endpoint")
        try:
            parse_control_endpoint_url(endpoint)
        except ConnectionControlEndpointError as error:
            pytest.fail(
                f"preflight: the configured control endpoint is not a valid v1 endpoint ({error})"
            )
        _required_env(_REPOSITORY_BASE_ENV, "the Producer repository base configuration")
        _required_env(_REVIEWER_DIRECTORY_ENV, "the neutral Reviewer directory configuration")
        _required_env(_PRODUCER_PROVIDER_ENV, "the Producer role binding provider configuration")
        _required_env(_PRODUCER_MODEL_ENV, "the Producer role binding model configuration")
        _required_env(_REVIEWER_PROVIDER_ENV, "the Reviewer role binding provider configuration")
        _required_env(_REVIEWER_MODEL_ENV, "the Reviewer role binding model configuration")
        _required_env(_HUB_TOKEN_ENV, "the Connection control credential")
        if os.environ.get(_ROTATE_ENV) == "1":
            # A restart helper cannot update this process's environment after
            # it starts, so a requested post-restart rotation must have a
            # credential supplier command that is executed fresh at refresh
            # time.
            _required_env(
                _CREDENTIAL_COMMAND_ENV,
                "the post-restart current-Hub-credential supplier (executed at "
                "refresh time; its stdout is the current token)",
            )
    head_sha = _resolve_fixture_head()
    return attested, head_sha


# --- deliberate live-test interruption tooling (never production behavior) ---


def _bridge_child_pids() -> list[int]:
    """The pytest process's own D3 bridge children.

    The bridge child is a direct child of this test process; killing it
    mid-turn is the real local-transport interruption scenario D1 requires.
    This is test-driver tooling only and never belongs in production code.
    """
    completed = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,command="],
        capture_output=True,
        text=True,
        timeout=15.0,
        check=False,
    )
    pids: list[int] = []
    for line in completed.stdout.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3 and parts[1] == str(os.getpid()) and "cli.js" in parts[2]:
            with suppress(ValueError):
                pids.append(int(parts[0]))
    return pids


def _kill_bridge_children() -> int:
    killed = 0
    for pid in _bridge_child_pids():
        with suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
            killed += 1
    return killed


def _wait_for(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(1.0)
    pytest.fail(f"timed out after {timeout:.0f}s waiting for {what}")


# --- Owner-approved Hub-restart modes ----------------------------------------


def _run_restart_hook() -> None:
    command = _required_env(_RESTART_COMMAND_ENV, "the Owner-approved controlled Hub restart")
    print(
        "D7 live: executing the Owner-approved controlled ClineBox Hub restart "
        "hook (documented precondition: no unrelated active work depends on this Box).",
        flush=True,
    )
    completed = subprocess.run(shlex.split(command), timeout=600.0, check=False)
    if completed.returncode != 0:
        pytest.fail(
            f"the Owner-approved restart hook exited with {completed.returncode}; "
            "the recovery scenario cannot proceed"
        )
    time.sleep(_RESTART_SETTLE_SECONDS)


def _pause_for_owner_restart() -> None:
    sentinel = Path(_required_env(_PAUSE_SENTINEL_ENV, "the manual-restart pause mode"))
    raw_timeout = os.environ.get(_PAUSE_TIMEOUT_ENV, "").strip()
    timeout = float(raw_timeout) if raw_timeout else _DEFAULT_PAUSE_TIMEOUT_SECONDS
    print(
        "D7 live: PAUSED for the Owner-approved manual Hub restart.\n"
        "  1. Restart the ClineBox Hub now (no unrelated active work may depend on it).\n"
        f"  2. If the Hub attach credential changed, set {_ROTATE_ENV}=1 and provide\n"
        f"     {_CREDENTIAL_COMMAND_ENV} — a command whose stdout is the CURRENT token,\n"
        "     executed fresh at refresh time (the original process environment cannot\n"
        "     be updated after start); the refresh goes through the existing trusted\n"
        "     Vault rotation service, never a raw secret swap.\n"
        f"  3. Create the sentinel file {sentinel} to resume this test.",
        flush=True,
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if sentinel.is_file():
            print("D7 live: restart confirmed; resuming recovery.", flush=True)
            return
        time.sleep(2.0)
    pytest.fail(
        "NOT EXECUTED: the manual Hub restart was not confirmed within the pause "
        "window; the required recovery scenario is not executed and the "
        "corresponding acceptance criterion stays unsatisfied"
    )


# --- module-scoped live state -------------------------------------------------


class _Live:
    """Module-scoped live state: durable fixtures, identities, and evidence.

    The durable fixture rows are committed disposable state on the supplied
    non-production branch database. They are removed at teardown through the
    sanctioned Auth-root cascade unless a stage-two resume is expected (the
    handoff mode), in which case the preserved database keeps the original
    READY binding for the separate recovery invocation.
    """

    def __init__(self, *, resume: bool, database_url: str) -> None:
        self.resume = resume
        self.database_url = database_url
        self.attested_versions = ""
        self.fixture_head_sha: str | None = None
        self._pool: ConnectionPool[Connection[Any]] | None = None
        self.profile_id: UUID | None = None
        self.workspace_id: UUID | None = None
        self.task_id: UUID | None = None
        self.connection_id: UUID | None = None
        self.repository_base: str | None = None
        self.reviewer_directory: str | None = None
        self.producer_session_id: str | None = None
        self.reviewer_session_id: str | None = None
        self.producer_count_before_restart: int | None = None
        self.evidence: dict[str, str] = {}

    @property
    def pool(self) -> DatabasePool:
        assert self._pool is not None
        return cast(DatabasePool, self._pool)

    def record(self, scenario: str, status: str, detail: str = "") -> None:
        self.evidence[scenario] = f"{status}: {detail}" if detail else status

    def prepare(self) -> None:
        pool: ConnectionPool[Connection[Any]] = ConnectionPool(
            conninfo=self.database_url, min_size=0, max_size=4, open=True, name="d7-live"
        )
        self._pool = pool
        if self.resume:
            self._prepare_resume()
        else:
            self._prepare_fresh()

    def _prepare_fresh(self) -> None:
        endpoint = _required_env(_CONTROL_ENDPOINT_ENV, "the Connection control endpoint")
        self.repository_base = _required_env(
            _REPOSITORY_BASE_ENV, "the Producer repository base configuration"
        )
        self.reviewer_directory = _required_env(
            _REVIEWER_DIRECTORY_ENV, "the neutral Reviewer directory configuration"
        )
        producer_provider = _required_env(
            _PRODUCER_PROVIDER_ENV, "the Producer role binding provider configuration"
        )
        producer_model = _required_env(
            _PRODUCER_MODEL_ENV, "the Producer role binding model configuration"
        )
        reviewer_provider = _required_env(
            _REVIEWER_PROVIDER_ENV, "the Reviewer role binding provider configuration"
        )
        reviewer_model = _required_env(
            _REVIEWER_MODEL_ENV, "the Reviewer role binding model configuration"
        )
        token = _required_env(_HUB_TOKEN_ENV, "the Connection control credential")

        profile_id, workspace_id, task_id = self._seed_durable_rows()
        connection = connection_repositories.create_connection(
            self.pool,
            workspace_id=workspace_id,
            adapter=AdapterType.CLINE,
            name="d7 live cline hub",
            safe_config={"control_endpoint": endpoint, "repository_base": self.repository_base},
            session_capacity=2,
            enabled=True,
        )
        for role, provider, model in (
            (WorkflowRole.PRODUCER, producer_provider, producer_model),
            (WorkflowRole.REVIEWER, reviewer_provider, reviewer_model),
        ):
            connection_repositories.set_role_binding(
                self.pool,
                workspace_id=workspace_id,
                role=role,
                connection_id=connection.id,
                configured_provider=provider,
                configured_model=model,
                role_prompt_override=None,
            )
        configure_connection_credential(
            self.pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=connection.id,
            credential=token,
        )
        outcome = admit_task_agent_sessions(
            self.pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            roles=(WorkflowRole.PRODUCER, WorkflowRole.REVIEWER),
        )
        if outcome.status is not AdmissionStatus.ADMITTED:
            pytest.fail(
                "fixture admission did not ADMIT both roles "
                f"(status {outcome.status.value}); the live run cannot start"
            )
        self.profile_id = profile_id
        self.workspace_id = workspace_id
        self.task_id = task_id
        self.connection_id = connection.id

    def _seed_durable_rows(self) -> tuple[UUID, UUID, UUID]:
        """Insert the test-scoped ownership chain, mirroring the existing
        integration suites' sanctioned seeding pattern."""
        profile_id = uuid4()
        workspace_id = uuid4()
        project_id = uuid4()
        repository_id = uuid4()
        task_id = uuid4()
        assert self._pool is not None
        with self._pool.connection() as conn, conn.transaction():
            # profiles.id references auth.users (id) ON DELETE CASCADE: the
            # sanctioned Supabase Auth boundary every Profile needs.
            conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
            conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
            conn.execute(
                "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
                (workspace_id, profile_id, "d7 live validation"),
            )
            conn.execute(
                "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
                (project_id, workspace_id, "d7 live"),
            )
            conn.execute(
                "insert into openorc.repositories "
                "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
                "html_url, is_private, default_branch) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    repository_id,
                    project_id,
                    workspace_id,
                    1_000_000_001,
                    "openorc",
                    "runtime-test-fixture",
                    f"https://github.com/{FIXTURE_REPOSITORY}",
                    False,
                    "main",
                ),
            )
            conn.execute(
                "insert into openorc.tasks "
                "(id, workspace_id, repository_id, github_issue_id, github_issue_number, "
                "status, state_token, source_requirements_fingerprint) "
                "values (%s, %s, %s, %s, %s, 'ready_to_plan', %s, %s)",
                (
                    task_id,
                    workspace_id,
                    repository_id,
                    149,
                    FIXTURE_PR_NUMBER,
                    uuid4(),
                    "d7" * 32,
                ),
            )
        return profile_id, workspace_id, task_id

    def _prepare_resume(self) -> None:
        """Stage two of the preserved-DB restart mode.

        The preserved database is authoritative; the handoff record only
        locates the test-scoped rows. The original READY binding and its
        exact external session identity are re-read from durable state,
        never recreated or re-admitted, and the migrations are never reset
        or re-applied here.
        """
        handoff_path = Path(_required_env(_HANDOFF_ENV, "the stage-two handoff record"))
        try:
            handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            pytest.fail(f"the stage-two handoff record is unreadable ({error})")
        assert self._pool is not None
        with self._pool.connection() as conn:
            present = conn.execute("select to_regclass('openorc.task_agent_sessions')").fetchone()
        if present is None or present[0] is None:
            pytest.fail(
                "resume mode: the supplied preserved database does not carry the "
                "applied migrations; resume mode never resets or re-applies them"
            )
        workspace_id = UUID(str(handoff["workspace_id"]))
        task_id = UUID(str(handoff["task_id"]))
        connection_id = UUID(str(handoff["connection_id"]))
        with self._pool.connection() as conn:
            binding = conn.execute(
                "select lifecycle_status, external_session_id, connection_id, "
                "effective_config_snapshot from openorc.task_agent_sessions "
                "where task_id = %s and role = 'producer'",
                (task_id,),
            ).fetchone()
            owner = conn.execute(
                "select owner_profile_id from openorc.workspaces where id = %s",
                (workspace_id,),
            ).fetchone()
        if binding is None or owner is None:
            pytest.fail(
                "resume mode: the preserved database has no Producer binding or "
                "Workspace for the handoff Task; the original READY session "
                "cannot be resumed"
            )
        lifecycle, external_session_id, persisted_connection_id, snapshot = binding
        if lifecycle != TaskSessionLifecycleStatus.READY.value:
            pytest.fail(
                "resume mode: the preserved Producer binding is not READY; the "
                "original session cannot be resumed"
            )
        if external_session_id != str(handoff["producer_session_id"]):
            pytest.fail(
                "resume mode: the preserved Producer binding's external session "
                "identity disagrees with the handoff record; failing closed"
            )
        configuration = (snapshot or {}).get("configuration", {})
        base = configuration.get("repository_base")
        if not isinstance(base, str) or not base.strip():
            pytest.fail(
                "resume mode: the preserved binding's effective configuration "
                "snapshot does not carry the original repository base"
            )
        if persisted_connection_id != connection_id:
            pytest.fail(
                "resume mode: the preserved binding's Connection disagrees with "
                "the handoff record; failing closed"
            )
        self.profile_id = UUID(str(owner[0]))
        self.workspace_id = workspace_id
        self.task_id = task_id
        self.connection_id = connection_id
        self.repository_base = base
        self.producer_session_id = str(external_session_id)
        self.producer_count_before_restart = int(handoff["producer_transcript_count"])

    def box_check(self, path: str, *, expect_exists: bool) -> str:
        """Verify a Box-side path through the Owner-approved operator hook.

        The hook is a required preflight prerequisite, so this verification
        is always genuine: an explicitly Owner-supplied command template (the
        ``{path}`` placeholder receives the addressed Box-side path) runs
        against the actual Box, and its outcome is recorded as evidence. The
        agent's COMPLETED prose alone is never filesystem authority, and this
        process never probes the remote path itself.
        """
        template = os.environ.get(_BOX_CHECK_ENV, "").strip()
        if not template:
            pytest.fail(
                "the operator box check command is not configured; autonomous "
                "execution requires genuine Box-side checkout/cleanup verification"
            )
        argv = [part.replace("{path}", path) for part in shlex.split(template)]
        completed = subprocess.run(argv, timeout=60.0, capture_output=True, check=False)
        # Explicit outcome contract: exit 0 = exists, exit 1 = absent, any
        # other exit status is a verification failure — a transport,
        # authentication, or remote-execution error is never cleanup evidence.
        try:
            observed = classify_box_check_exit(completed.returncode)
        except BoxCheckVerificationError as error:
            pytest.fail(f"the operator box check for {path!r} failed ({error})")
        if (observed == "exists") is not expect_exists:
            pytest.fail(
                f"the operator box check disagreed about {path!r} "
                f"(expected {'exists' if expect_exists else 'absent'}, observed {observed})"
            )
        return "OPERATOR-CHECKED"

    def finalize(self) -> None:
        try:
            if self.resume:
                self.record(
                    "state",
                    "PRESERVED",
                    "stage-two resume left the preserved database untouched",
                )
            elif os.environ.get(_HANDOFF_ENV, "").strip():
                self._write_handoff()
                self.record(
                    "state",
                    "PRESERVED",
                    "handoff record written; durable rows kept for the stage-two resume",
                )
            else:
                self._cleanup_durable_rows()
                self.record(
                    "state",
                    "CLEANED",
                    "test-scoped durable rows removed through the Auth-root cascade",
                )
        finally:
            self._print_evidence()
            if self._pool is not None:
                self._pool.close()

    def _write_handoff(self) -> None:
        """Stage one's safe handoff record: identifiers and counts only.

        No transcript content, no credentials, no prompt text.
        """
        path = Path(_required_env(_HANDOFF_ENV, "the stage-two handoff record"))
        payload = {
            "workspace_id": str(self.workspace_id),
            "task_id": str(self.task_id),
            "connection_id": str(self.connection_id),
            "producer_session_id": self.producer_session_id,
            "reviewer_session_id": self.reviewer_session_id,
            "producer_transcript_count": self.producer_count_before_restart,
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _cleanup_durable_rows(self) -> None:
        """Best-effort removal of this run's committed rows on a disposable branch."""
        assert self._pool is not None
        if self.connection_id is not None:
            with suppress(Exception):
                with self._pool.connection() as conn:
                    row = conn.execute(
                        "select auth_reference from openorc.connections where id = %s",
                        (self.connection_id,),
                    ).fetchone()
                if row is not None and row[0]:
                    secret_id = runtime_control_secrets.parse_connection_auth_reference(str(row[0]))
                    runtime_control_secrets.delete_runtime_control_secret(
                        self.pool, secret_id=secret_id
                    )
        if self.profile_id is not None:
            with suppress(Exception), self._pool.connection() as conn, conn.transaction():
                conn.execute("delete from auth.users where id = %s", (self.profile_id,))

    def _print_evidence(self) -> None:
        summary = {
            "suite": "D7 live Cline runtime validation (issue #149)",
            "attested_versions": self.attested_versions,
            "fixture_repository": FIXTURE_REPOSITORY,
            "fixture_pr_number": FIXTURE_PR_NUMBER,
            "fixture_pr_head": self.fixture_head_sha,
            "workspace_id": _safe_id(self.workspace_id),
            "task_id": _safe_id(self.task_id),
            "connection_id": _safe_id(self.connection_id),
            "producer_session_id": self.producer_session_id,
            "reviewer_session_id": self.reviewer_session_id,
            "scenarios": self.evidence,
        }
        print(
            "\nD7 LIVE EVIDENCE SUMMARY\n" + json.dumps(summary, indent=2, sort_keys=True),
            flush=True,
        )


def _safe_id(value: UUID | None) -> str | None:
    return None if value is None else str(value)


# --- fixtures and shared helpers ----------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _require_live_opt_in() -> Iterator[None]:
    """Module-scoped so the skip gate runs before the module-scoped live setup."""
    if os.environ.get(_OPT_IN_ENV) != "1":
        pytest.skip(
            "D7 live Cline validation is not enabled; set OPENORC_CLINE_LIVE=1 "
            "to opt in (the ordinary deterministic baseline never contacts the Box)"
        )
    yield


@pytest.fixture(scope="module")
def live(request: pytest.FixtureRequest) -> Iterator[_Live]:
    resume = os.environ.get(_RESUME_ENV) == "1"
    state = _Live(
        resume=resume,
        database_url=_required_env(_DATABASE_URL_ENV, "the non-production branch database"),
    )
    state.attested_versions, state.fixture_head_sha = _preflight(resume=resume)
    if resume:
        # Stage two of the preserved-DB restart mode: the preserved database
        # is consumed as-is; the conftest migration reset is deliberately not
        # requested so the original READY binding survives untouched.
        state.prepare()
    else:
        state.database_url = request.getfixturevalue("migrated_database")
        state.prepare()
    try:
        yield state
    finally:
        state.finalize()


def _fetch_current_hub_credential() -> str:
    """Acquire the CURRENT Hub credential at refresh time (test-only seam).

    A restart helper cannot update this process's environment after it
    starts, so the post-restart refresh never re-reads the original
    ``OPENORC_CLINE_LIVE_HUB_TOKEN`` value: it executes the explicitly
    Owner-supplied credential-supplier command (``OPENORC_CLINE_LIVE_CREDENTIAL_COMMAND``)
    fresh at refresh time and takes its stdout as the current token. The
    value is captured without echo, never logged, never persisted, and never
    passed in command arguments; it goes straight into the trusted Vault
    rotation service. This is the supplier half of the test-side mirror of
    the future Cloud operator runner's post-restart acquisition
    (openorc/cloud#54).
    """
    command = _required_env(
        _CREDENTIAL_COMMAND_ENV,
        "the post-restart current-Hub-credential supplier (executed at refresh "
        "time; its stdout is the current token)",
    )
    completed = subprocess.run(shlex.split(command), timeout=60.0, capture_output=True, check=False)
    if completed.returncode != 0:
        # Only the exit status is reported; the supplier's output can carry
        # the current credential and is never echoed.
        pytest.fail(
            "the credential supplier command failed "
            f"(exit {completed.returncode}); the current Hub credential was "
            "not obtained and the reattachment must not rotate to a stale value"
        )
    try:
        return credential_from_supplier_output(completed.stdout)
    except CredentialSupplierError as error:
        pytest.fail(f"the credential supplier returned no usable credential ({error})")


def _refresh_current_hub_credential(state: _Live, credential: str) -> None:
    """Refresh the test Connection's stored Hub credential (test-only seam).

    The narrow test-only current-Hub-credential refresh interface: after a
    controlled Hub restart that rotated the attach credential, the current
    value is installed through the existing #55 trusted Vault rotation
    service — the Vault secret is updated in place and the durable opaque
    ``auth_reference`` stays stable — so the reattachment resolves the Hub's
    CURRENT credential. Merely reopening the same Connection never refreshes
    a stale secret. This is the test-side mirror of the future Cloud operator
    runner's credential refresh (openorc/cloud#54); it is deliberately not
    production API, not new credential storage, and never logs the value.
    """
    assert state.profile_id is not None
    assert state.workspace_id is not None
    assert state.connection_id is not None
    rotate_connection_credential(
        state.pool,
        profile_id=state.profile_id,
        workspace_id=state.workspace_id,
        connection_id=state.connection_id,
        credential=credential,
    )
    state.record(
        "D3 credential rotation",
        "EXECUTED",
        "current Hub credential refreshed through the trusted Vault rotation service",
    )


def _attach_bound(
    state: _Live,
    stack: ExitStack,
    *,
    role: WorkflowRole,
    external_session_id: str,
    working_directory: str,
) -> AttachedClineRuntime:
    """Enter a BOUND attachment lease, retrying while the Box is restarting.

    Each attempt is a fresh composition invocation with fresh Connection and
    credential reads; a failed attempt disposes its own backend before the
    next one starts. Only attachment-outcome failures are retried; durable
    authorization/configuration refusals propagate unchanged.
    """
    assert state.workspace_id is not None
    assert state.task_id is not None
    last: Exception | None = None
    for _ in range(_RECONNECT_ATTEMPTS):
        lease = attach_cline_runtime(
            state.pool,
            workspace_id=state.workspace_id,
            task_id=state.task_id,
            role=role,
            phase=ClineAttachmentPhase.BOUND,
            external_session_id=external_session_id,
            working_directory=working_directory,
        )
        try:
            return stack.enter_context(lease)
        except (ExternalOperationFailedError, ExternalOperationUncertainError) as error:
            last = error
            time.sleep(_RECONNECT_DELAY_SECONDS)
    raise AssertionError(
        f"the ClineBox did not accept a reattachment after {_RECONNECT_ATTEMPTS} attempts: {last}"
    )


def _establish_fresh(
    state: _Live, stack: ExitStack, *, role: WorkflowRole, working_directory: str | None
) -> TaskAgentSession:
    """One fresh production attachment plus establishment inside one lease."""
    assert state.workspace_id is not None
    assert state.task_id is not None
    lease = attach_cline_runtime(
        state.pool,
        workspace_id=state.workspace_id,
        task_id=state.task_id,
        role=role,
        phase=ClineAttachmentPhase.FRESH,
        working_directory=working_directory,
    )
    runtime = stack.enter_context(lease)
    assert runtime.selected_configuration is not None
    session = establish_task_agent_session(
        state.pool,
        runtime.adapter,
        workspace_id=state.workspace_id,
        task_id=state.task_id,
        role=role,
        effective_configuration=runtime.selected_configuration,
    )
    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id is not None and session.external_session_id.strip()
    return session


# --- scenario A: production attachment, initialization, exact binding --------


def test_a_production_attachments_initialization_and_exact_binding(live: _Live) -> None:
    if live.resume:
        pytest.skip("stage-two resume mode runs only the recovery and cleanup scenarios")
    with ExitStack() as stack:
        producer = _establish_fresh(live, stack, role=WorkflowRole.PRODUCER, working_directory=None)
        reviewer = _establish_fresh(
            live,
            stack,
            role=WorkflowRole.REVIEWER,
            working_directory=live.reviewer_directory,
        )
    assert producer.external_session_id is not None
    assert reviewer.external_session_id is not None
    live.producer_session_id = producer.external_session_id
    live.reviewer_session_id = reviewer.external_session_id
    assert live.producer_session_id != live.reviewer_session_id, (
        "the two roles must hold separate opaque external session identities"
    )

    # A BOUND reattachment from a new backend/adapter lease using the exact
    # persisted Task/role/session/Connection identity continues the same
    # conversation without resending initialization or creating a replacement.
    reviewer_id = live.reviewer_session_id
    assert live.reviewer_directory is not None
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.REVIEWER,
            external_session_id=reviewer_id,
            working_directory=live.reviewer_directory,
        )
        reconciliation = bound.adapter.reconcile_session(reviewer_id)
        assert reconciliation.continuity in (
            ClineSessionContinuity.ACCESSIBLE,
            ClineSessionContinuity.RECOVERABLE,
        )
        reply = bound.adapter.send(reviewer_id, "Reply with the single word: acknowledged.")
        assert isinstance(reply, AgentTextResponse)

    live.record(
        "A attachment/initialization/binding",
        "EXECUTED",
        "fresh Producer (ACT at admitted base) and fresh Reviewer (PLAN at the "
        "neutral directory) established READY through the production attachment "
        "path; distinct external session IDs; bound reattachment reconciled and "
        "continued without reinitialization",
    )


# --- scenario B: Producer workspace lifecycle and original-base stability -----


def test_b_producer_workspace_lifecycle_and_original_base_stability(live: _Live) -> None:
    if live.resume:
        pytest.skip("stage-two resume mode runs only the recovery and cleanup scenarios")
    assert live.producer_session_id is not None
    assert live.repository_base is not None
    assert live.task_id is not None
    assert live.connection_id is not None
    producer_id = live.producer_session_id
    checkout = f"{live.repository_base}/{live.task_id}"

    # PREPARE_WORKSPACE: the Producer creates only its own isolated Task
    # checkout of the dummy repository at the OpenOrc-supplied destination.
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=live.repository_base,
        )
        interaction = PrepareWorkspaceInteraction(
            repository_full_name=FIXTURE_REPOSITORY,
            issue_number=FIXTURE_PR_NUMBER,
            task_uuid=live.task_id,
            target_directory=checkout,
        )
        result = bound.adapter.realize_control(
            producer_id,
            interaction,
            message=interaction.render(),
            expected_family=WORKSPACE_RESULT_FAMILY,
        )
        assert isinstance(result, WorkspaceResult)
        assert result.operation == "PREPARE"
        assert result.status == "COMPLETED"
    checkout_attestation = live.box_check(checkout, expect_exists=True)

    # Same-ID bootstrap-ACT -> PLAN reconstruction at the isolated checkout,
    # then one normal formal planning turn.
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=checkout,
        )
        bound.adapter.reconstruct_session(producer_id, target_mode=ClineConstructionMode.PLAN)
        plan = bound.adapter.send(
            producer_id,
            "Compose the implementation plan for this Task: create one scratch file "
            "named d7-live.txt in this checkout containing a single line of text, "
            "and nothing else. Respond with the canonical plan_result envelope.",
            expected_family=PLAN_RESULT_FAMILY,
        )
        assert isinstance(plan, PlanResult)

    # IMPLEMENT: the authorized same-ID PLAN->ACT reconstruction at that same
    # checkout, then one harmless bounded implementation action. No OpenOrc PR
    # publication, no workflow gates, no push.
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=checkout,
        )
        bound.adapter.realize_control(producer_id, ImplementInteraction())
        implementation = bound.adapter.send(
            producer_id,
            "IMPLEMENT the plan: create the file d7-live.txt in this checkout with "
            "exactly one line of text. Do not commit, push, or publish anything. "
            "Respond with the canonical implementation_result envelope.",
            expected_family=IMPLEMENTATION_RESULT_FAMILY,
        )
        assert isinstance(implementation, ImplementationResult)
        assert implementation.status == "COMPLETED"

    # Original-base stability: a test-scoped edit to the live Connection
    # configuration never relocates the existing Task — the durable READY
    # snapshot pins the original base. The configuration is restored after.
    connection = connection_repositories.get_connection(live.pool, live.connection_id)
    assert connection is not None
    original_config = dict(connection.safe_config)
    moved_config = dict(original_config)
    moved_config["repository_base"] = f"{live.repository_base}/moved"
    assert (
        connection_repositories.update_connection(
            live.pool,
            connection.id,
            name=connection.name,
            safe_config=moved_config,
            session_capacity=connection.session_capacity,
            enabled=True,
            auth_reference=connection.auth_reference,
        )
        is not None
    )
    try:
        with ExitStack() as stack:
            bound = _attach_bound(
                live,
                stack,
                role=WorkflowRole.PRODUCER,
                external_session_id=producer_id,
                working_directory=checkout,
            )
            bound.adapter.reconstruct_session(producer_id, target_mode=ClineConstructionMode.ACT)
            reconciliation = bound.adapter.reconcile_session(producer_id)
            assert reconciliation.continuity in (
                ClineSessionContinuity.ACCESSIBLE,
                ClineSessionContinuity.RECOVERABLE,
            )
    finally:
        assert (
            connection_repositories.update_connection(
                live.pool,
                connection.id,
                name=connection.name,
                safe_config=original_config,
                session_capacity=connection.session_capacity,
                enabled=True,
                auth_reference=connection.auth_reference,
            )
            is not None
        )

    live.record(
        "B producer workspace lifecycle",
        "EXECUTED",
        f"PREPARE/PLAN/IMPLEMENT realized through the production path; checkout "
        f"verification: {checkout_attestation}; the originally pinned base "
        "survived a test-scoped Connection configuration edit (restored)",
    )


# --- scenario C: Reviewer GitHub-MCP exact-head verified review ---------------


def test_c_reviewer_verified_pr_review(live: _Live) -> None:
    if live.resume:
        pytest.skip("stage-two resume mode runs only the recovery and cleanup scenarios")
    assert live.reviewer_session_id is not None
    assert live.reviewer_directory is not None
    assert live.fixture_head_sha is not None
    reviewer_id = live.reviewer_session_id
    # The head SHA was resolved from GitHub at preflight and pinned for this
    # review interaction; the review is bound to that exact resolved head.
    pinned_head = live.fixture_head_sha
    subject = PrReviewSubject(
        repository_full_name=FIXTURE_REPOSITORY,
        pull_request_number=FIXTURE_PR_NUMBER,
        head_sha=pinned_head,
        issue_number=FIXTURE_PR_NUMBER,
    )
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.REVIEWER,
            external_session_id=reviewer_id,
            working_directory=live.reviewer_directory,
        )
        interaction = ReviewInteraction(subject=subject)
        message = (
            f"REVIEW assignment: inspect {FIXTURE_REPOSITORY} PR "
            f"#{FIXTURE_PR_NUMBER} at exact head {pinned_head} with your "
            "native GitHub MCP; read the changed file "
            f"{FIXTURE_CHANGED_PATH} at that SHA with a commit-addressed "
            "get_file_contents read. Then respond with the canonical review_result "
            "envelope."
        )
        # The parsed review_result is released only after the current-turn
        # native tool evidence establishes access to the exact assigned PR and
        # a commit-addressed read at the assigned SHA (D5b gate). No verdict
        # is required for this intentionally trivial PR.
        result = bound.adapter.realize_control(
            reviewer_id,
            interaction,
            message=message,
            expected_family=REVIEW_RESULT_FAMILY,
        )
        assert isinstance(result, ReviewResult)
        # Head-movement rejection: the reviewed subject must still exist at
        # its verified exact head after the turn completed.
        try:
            post_review_payload, _ = _fetch_fixture_pull_request()
            post_review_head = post_review_payload.get("head")
            observed_head = assert_head_unchanged(
                pinned_head,
                post_review_head.get("sha") if isinstance(post_review_head, dict) else None,
            )
        except FixtureSubjectError as error:
            pytest.fail(f"the verified review cannot stand on a moved subject ({error})")
    live.record(
        "C reviewer verified review",
        "EXECUTED",
        f"review_result released through the D5b tool-evidence gate "
        f"(outcome {result.outcome}); subject head {observed_head} unchanged "
        "after the review turn",
    )


# --- scenario D1: adapter-local uncertain-send guard semantics ----------------


def test_d1_uncertain_send_latch_refuses_reconstruction_on_the_same_adapter(
    live: _Live,
) -> None:
    """D1 (adapter-local, one adapter lifetime).

    A real local-transport interruption during an in-flight send latches the
    send-kind uncertain effect on that adapter instance. The implemented D6b
    contract does not refuse ordinary sends on the latch: what it guarantees
    is that same-ID reconstruction/recovery is refused while the uncertainty
    is unresolved, and that settlement requires positive caller attestation.
    The detailed settlement/refusal matrix is owned by the deterministic D6b
    tests; no settlement is performed here with fabricated operation
    accounting.
    """
    if live.resume:
        pytest.skip("stage-two resume mode runs only the recovery and cleanup scenarios")
    assert live.producer_session_id is not None
    assert live.repository_base is not None
    producer_id = live.producer_session_id
    checkout = f"{live.repository_base}/{live.task_id}"
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=checkout,
        )
        adapter = bound.adapter
        pre_interrupt = adapter.reconcile_session(producer_id)
        assert pre_interrupt.transcript_message_count is not None
        live.producer_count_before_restart = pre_interrupt.transcript_message_count

        observations: list[Any] = []
        subscription = adapter.subscribe_activity(producer_id, observations.append)
        outcome: list[BaseException | None] = []

        def _interrupted_turn() -> None:
            try:
                adapter.send(
                    producer_id,
                    "Run the shell command `sleep 20` and then report its exit status.",
                )
            except Exception as error:  # the asserted outcome lives below
                outcome.append(error)

        turn = threading.Thread(target=_interrupted_turn, name="d7-interrupted-turn")
        turn.start()
        _wait_for(
            lambda: bool(observations),
            timeout=_ACTIVITY_WAIT_SECONDS,
            what="the interrupted turn's first activity observation",
        )
        killed = _kill_bridge_children()
        turn.join(timeout=_TURN_WAIT_SECONDS)
        with suppress(Exception):
            subscription.unsubscribe()
        assert not turn.is_alive(), "the interrupted send thread did not finish"
        assert killed > 0, "no D3 bridge child was found to interrupt"
        assert outcome, "the interrupted send completed without raising"
        error = outcome[0]
        assert isinstance(error, AgentRuntimeUncertainOutcomeError), (
            "the interrupted send was not classified as an uncertain outcome"
        )

        # The latch refuses same-ID reconstruction on this adapter instance.
        with pytest.raises(AgentRuntimeUncertainOutcomeError):
            adapter.reconstruct_session(producer_id, target_mode=ClineConstructionMode.ACT)
        # Settlement demands positive caller attestation; without it the
        # refusal raises before any effect.
        with pytest.raises(ValueError):
            adapter.settle_uncertain_send(
                producer_id,
                runtime_work_settled=False,
                operation_accounted_for=False,
            )
    live.record(
        "D1 adapter-local uncertain-send guard",
        "EXECUTED",
        "a real transport interruption latched the send-kind uncertain effect; "
        "same-ID reconstruction was refused and the settlement attestation gate "
        "held on the same adapter instance (no settlement was fabricated)",
    )


# --- scenario D2: cross-attachment continuity ---------------------------------


def test_d2_fresh_attachment_reconciles_the_exact_session(live: _Live) -> None:
    """D2 (cross-attachment continuity).

    A fresh BOUND attachment is a new adapter instance without the previous
    instance's process-local guards by construction. Its empty guard set is
    NOT settlement evidence for the interrupted logical send: reconciliation
    classifies continuity through public reads only, and the attestation duty
    stays with the caller's durable dispatch facts.
    """
    if live.resume:
        pytest.skip("stage-two resume mode runs only the recovery and cleanup scenarios")
    assert live.producer_session_id is not None
    assert live.repository_base is not None
    assert live.producer_count_before_restart is not None
    producer_id = live.producer_session_id
    checkout = f"{live.repository_base}/{live.task_id}"
    checkpoint = ClineDispatchCheckpoint(
        transcript_message_count=live.producer_count_before_restart
    )
    # Wait out the interrupted turn's Hub-side tail before the continuation
    # send: the interrupted work's local outcome was never classified, and
    # this grace keeps the continuation turn from overlapping it.
    time.sleep(_INTERRUPTED_TURN_TAIL_SECONDS)
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=checkout,
        )
        reconciliation = bound.adapter.reconcile_session(producer_id, checkpoint=checkpoint)
        assert reconciliation.continuity in (
            ClineSessionContinuity.ACCESSIBLE,
            ClineSessionContinuity.RECOVERABLE,
        )
        live.record(
            "D2 reconciliation observation",
            "OBSERVED",
            f"continuity={reconciliation.continuity.value}; "
            f"transcript_count={reconciliation.transcript_message_count}; "
            f"beyond_checkpoint={reconciliation.messages_beyond_checkpoint}",
        )
        reply = bound.adapter.send(producer_id, "Reply with the single word: continued.")
        assert isinstance(reply, AgentTextResponse)
    live.record(
        "D2 cross-attachment continuity",
        "EXECUTED",
        "a fresh BOUND attachment (new adapter, no local guards) reconciled the "
        "exact session through public reads and continued it without "
        "reinitialization; the empty local guard set is recorded as NOT "
        "settlement evidence for the interrupted send",
    )


# --- scenario D3: required Hub-restart recovery (completion gate) -------------


def test_d3_hub_restart_recovery(live: _Live) -> None:
    """The required real Hub-restart recovery completion gate.

    Exactly one Owner-controlled mode must execute: (a) an explicitly
    Owner-approved restart hook, (b) an in-invocation Owner-approved manual
    pause, or (c) the preserved-database two-stage resume. With none of them
    the scenario reports NOT EXECUTED — distinct from a runtime UNKNOWN
    continuity classification — and the corresponding acceptance criterion
    stays unsatisfied pending execution or an explicit Owner-approved scope
    exception recorded on the PR. No unapproved restart is ever automated.
    """
    resume = live.resume
    if resume:
        assert live.producer_count_before_restart is not None
    else:
        assert live.producer_session_id is not None
        assert live.repository_base is not None
        producer_id = live.producer_session_id
        checkout = f"{live.repository_base}/{live.task_id}"
        if os.environ.get(_RESTART_COMMAND_ENV, "").strip():
            # Record the pre-restart transcript extent through the ordinary
            # public reads, then perform the Owner-approved restart.
            with ExitStack() as stack:
                bound = _attach_bound(
                    live,
                    stack,
                    role=WorkflowRole.PRODUCER,
                    external_session_id=producer_id,
                    working_directory=checkout,
                )
                reconciliation = bound.adapter.reconcile_session(producer_id)
                assert reconciliation.transcript_message_count is not None
                live.producer_count_before_restart = reconciliation.transcript_message_count
            _run_restart_hook()
        elif os.environ.get(_PAUSE_SENTINEL_ENV, "").strip():
            with ExitStack() as stack:
                bound = _attach_bound(
                    live,
                    stack,
                    role=WorkflowRole.PRODUCER,
                    external_session_id=producer_id,
                    working_directory=checkout,
                )
                reconciliation = bound.adapter.reconcile_session(producer_id)
                assert reconciliation.transcript_message_count is not None
                live.producer_count_before_restart = reconciliation.transcript_message_count
            _pause_for_owner_restart()
        else:
            pytest.fail(
                "NOT EXECUTED: no Hub-restart mode is configured. Set "
                f"{_RESTART_COMMAND_ENV} (Owner-approved restart hook), "
                f"{_PAUSE_SENTINEL_ENV} (Owner-approved manual pause), or run the "
                f"two-stage preserved-database mode ({_RESUME_ENV}=1 with "
                f"{_HANDOFF_ENV}); the required recovery scenario is not executed "
                "and the corresponding acceptance criterion stays unsatisfied"
            )

    # A Hub restart may rotate the attach credential: the current value is
    # refreshed through the test-only credential-refresh interface before the
    # reattachment — merely reopening the same Connection never refreshes a
    # stale secret.
    if os.environ.get(_ROTATE_ENV) == "1":
        _refresh_current_hub_credential(live, _fetch_current_hub_credential())

    # Reconnect through the ordinary Connection path: the fresh attachment
    # re-resolves the Hub's CURRENT attach credential; the prior incarnation
    # may be non-addressable after the restart.
    assert live.producer_session_id is not None
    assert live.repository_base is not None
    producer_id = live.producer_session_id
    checkout = f"{live.repository_base}/{live.task_id}"
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=checkout,
        )
        reconciliation = bound.adapter.reconcile_session(producer_id)
        assert reconciliation.continuity in (
            ClineSessionContinuity.ACCESSIBLE,
            ClineSessionContinuity.RECOVERABLE,
        )
        count = reconciliation.transcript_message_count
        assert count is not None
        assert live.producer_count_before_restart is not None
        # The persisted exact transcript must not shrink across the restart.
        # Growth records the interrupted work's effect landing and stays an
        # observation, never a settlement or a completion claim.
        assert count >= live.producer_count_before_restart, (
            "the persisted transcript shrank across the Hub restart"
        )
        # The authorized same-ID recovery: no initialization resend, no
        # replacement session; the recovery itself positively establishes the
        # surviving transcript before any stop/start and honors a positively
        # identified stop absence if the prior incarnation is non-addressable.
        bound.adapter.recover_session(producer_id, target_mode=ClineConstructionMode.ACT)
        reply = bound.adapter.send(producer_id, "Reply with the single word: recovered.")
        assert isinstance(reply, AgentTextResponse)
    live.record(
        "D3 hub-restart recovery",
        "EXECUTED" if not resume else "EXECUTED (stage-two resume)",
        f"current-credential reattachment, transcript survival "
        f"(pre {live.producer_count_before_restart} / post {count} messages), "
        "and same-ID recover_session with a post-recovery continuation turn",
    )


# --- scenario E: observational activity and usage ------------------------------


def test_e_activity_and_usage_observations(live: _Live) -> None:
    if live.resume:
        pytest.skip("stage-two resume mode runs only the recovery and cleanup scenarios")
    assert live.reviewer_session_id is not None
    assert live.reviewer_directory is not None
    reviewer_id = live.reviewer_session_id
    probe = f"d7{secrets.token_hex(8)}"
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.REVIEWER,
            external_session_id=reviewer_id,
            working_directory=live.reviewer_directory,
        )
        observations: list[Any] = []
        subscription = bound.adapter.subscribe_activity(reviewer_id, observations.append)
        reply = bound.adapter.send(
            reviewer_id,
            f"Run the shell command `echo {probe}` and report its exit status. Then reply done.",
        )
        assert isinstance(reply, AgentTextResponse)
        _wait_for(
            lambda: any(
                observation.kind is ClineActivityKind.TURN_FINISHED for observation in observations
            ),
            timeout=_TURN_WAIT_SECONDS,
            what="the turn_finished activity observation",
        )
        with suppress(Exception):
            subscription.unsubscribe()
        snapshot = bound.adapter.read_activity_snapshot(reviewer_id)
        assert snapshot.session_id == reviewer_id
        # Usage scopes are runtime bookkeeping: absent scopes stay None
        # (unknown) and are never substituted from the other scope. A Hub
        # restart may reset them while the conversation survives; a reset is
        # observational only — never terminal-session-loss evidence and never
        # a reason to invalidate the durable TaskAgentSession.
        assert snapshot.usage is None or isinstance(snapshot.usage, ClineAccumulatedUsage)
        assert snapshot.aggregate_usage is None or isinstance(
            snapshot.aggregate_usage, ClineAccumulatedUsage
        )
    kinds = {observation.kind for observation in observations}
    representative = kinds & {
        ClineActivityKind.TOOL_STARTED,
        ClineActivityKind.TOOL_COMPLETED,
        ClineActivityKind.ITERATION_STARTED,
        ClineActivityKind.COMMENTARY,
        ClineActivityKind.TURN_FINISHED,
    }
    assert representative, "no representative session-scoped activity was observed"
    # Content safety: command/tool inputs and outputs never leave the adapter
    # in projected activity. Completed non-reasoning commentary is permitted
    # content-bearing text by contract, so it is excluded from the scan.
    leaked = [
        type(observation.detail).__name__
        for observation in observations
        if not isinstance(observation.detail, ClineCommentaryActivity)
        and observation.detail is not None
        and probe in repr(observation.detail)
    ]
    assert not leaked, "prompt material leaked into projected activity details"
    assert probe not in repr(snapshot), "prompt material leaked into the usage snapshot"
    live.record(
        "E observational activity/usage",
        "EXECUTED",
        f"session-scoped typed observations observed (kinds: "
        f"{sorted(kind.value for kind in kinds)}); usage scopes optional and "
        "content-safe; prompt material absent from non-commentary projections",
    )


# --- authorized cleanup ---------------------------------------------------------


def test_z_cleanup_workspace_and_final_evidence(live: _Live) -> None:
    assert live.producer_session_id is not None
    assert live.repository_base is not None
    assert live.task_id is not None
    producer_id = live.producer_session_id
    checkout = f"{live.repository_base}/{live.task_id}"
    # The authorized cleanup reconstruction: the same ACT session back at the
    # original base, outside the checkout, before CLEANUP_WORKSPACE runs.
    with ExitStack() as stack:
        bound = _attach_bound(
            live,
            stack,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer_id,
            working_directory=live.repository_base,
        )
        bound.adapter.reconstruct_session(producer_id, target_mode=ClineConstructionMode.ACT)
        interaction = CleanupWorkspaceInteraction(
            repository_full_name=FIXTURE_REPOSITORY,
            issue_number=FIXTURE_PR_NUMBER,
            task_uuid=live.task_id,
            target_directory=checkout,
        )
        result = bound.adapter.realize_control(
            producer_id,
            interaction,
            message=interaction.render(),
            expected_family=WORKSPACE_RESULT_FAMILY,
        )
        assert isinstance(result, WorkspaceResult)
        assert result.operation == "CLEANUP"
        assert result.status == "COMPLETED"
    checkout_attestation = live.box_check(checkout, expect_exists=False)
    base_attestation = live.box_check(live.repository_base, expect_exists=True)
    live.record(
        "cleanup",
        "EXECUTED",
        f"CLEANUP_WORKSPACE realized through the production path; checkout "
        f"removal: {checkout_attestation}; original base intact: {base_attestation}; "
        "the prepared fixture PR stays untouched",
    )
