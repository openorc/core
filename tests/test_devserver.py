"""Focused deterministic coverage for the Python devserver orchestrator.

The orchestration (openorc.devtools.devserver) is exercised through injected
Python-level fakes — a fake process runner, fake child processes, a fake
queue client, and a recording logger — never through fake executable trees.

The wrapper contract (repo-root resolution, .venv requirement, argument
passthrough) is covered by direct invocations of scripts/devserver.sh, and
the real subprocess supervision primitives (session isolation, process-group
signals, reaping) by a small number of real-subprocess tests.

The real Owner-run manual E2E remains authoritative for live interaction
(Supabase branches, Ctrl-C against the real stack, Vite/API/worker startup).
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from openorc.devtools.devserver import (
    DEFAULT_VALKEY_URL,
    NGROK_LOG_PATH,
    Devserver,
    DevserverError,
    HelpRequested,
    ProcessSupervisor,
    RunConfig,
    RunResult,
    SubprocessRunner,
    SupabaseManager,
    UsageError,
    is_local_valkey_url,
    load_env_file,
    parse_args,
    parse_branch_environment,
    redact_url,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
WRAPPER_PATH = REPO_ROOT / "scripts" / "devserver.sh"
MIGRATIONS_TOOL_PATH = REPO_ROOT / "scripts" / "supabase-apply-migrations.sh"
PIN_VERSION = (REPO_ROOT / "supabase" / "cli-version").read_text(encoding="utf-8").strip()

REF_PARENT = "a" * 20
REF_PRODUCTION = "c" * 20

BRANCH_DB_URL = (
    "postgresql://postgres.bbbbbbbb:branchsecret@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
)
BRANCH_DIRECT_DB_URL = "postgresql://postgres:branchsecret@db.bbbbbbbb.supabase.co:5432/postgres"
BRANCH_API_URL = "https://bbbbbbbb.supabase.co"
PUBLISHABLE_KEY = "sb_publishable_test-key"
DEFAULT_SECRET_KEY = "sb_secret_test-key"
ANON_KEY = "legacy-anon-key"
SERVICE_ROLE_KEY = "legacy-service-role-key"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

Event = tuple[str, str, Any]


@dataclass
class RecordingLogger:
    entries: list[tuple[str, str]] = field(default_factory=list)

    def log(self, message: str) -> None:
        self.entries.append(("log", message))

    def warn(self, message: str) -> None:
        self.entries.append(("warn", message))

    def error(self, message: str) -> None:
        self.entries.append(("error", message))

    def text(self) -> str:
        return "\n".join(message for _, message in self.entries)

    def messages_of(self, level: str) -> list[str]:
        return [message for level_, message in self.entries if level_ == level]


@dataclass
class FakeChild:
    label: str
    pid: int
    events: list[Event]
    exit_after_polls: int | None = None
    exit_code: int = 0
    die_on_signals: int = 1
    dead: bool = False
    fail_wait: bool = False
    signals: list[int] = field(default_factory=list)
    waited: bool = False
    wait_timeout: float | None = None
    poll_count: int = field(default=0, init=False)

    def poll(self) -> int | None:
        self.poll_count += 1
        if self.dead:
            return 1
        if len(self.signals) >= self.die_on_signals:
            return self.exit_code
        if self.exit_after_polls is not None and self.poll_count > self.exit_after_polls:
            return self.exit_code
        return None

    def wait(self, timeout: float | None = None) -> int:
        if self.fail_wait:
            raise RuntimeError(f"simulated reap failure for {self.label}")
        self.waited = True
        self.wait_timeout = timeout
        self.events.append(("wait", self.label, "wait"))
        return self.exit_code

    def signal(self, signum: int) -> None:
        self.signals.append(signum)
        self.events.append(("signal", self.label, signum))


@dataclass
class RunCall:
    argv: tuple[str, ...]
    env: dict[str, str] | None
    cwd: Path | None
    capture: bool


@dataclass
class SpawnCall:
    label: str
    argv: tuple[str, ...]
    env: dict[str, str] | None
    cwd: Path | None
    log_path: Path | None
    inherit_stdin: bool


class FakeRunner:
    """ProcessRunner double scripting run results by exact/prefix argv."""

    def __init__(self, events: list[Event]) -> None:
        self.events = events
        self.run_calls: list[RunCall] = []
        self.spawn_calls: list[SpawnCall] = []
        self.run_results: dict[tuple[str, ...], RunResult] = {}
        self.run_prefix_results: list[tuple[tuple[str, ...], RunResult]] = []
        self.default_result = RunResult(returncode=0, stdout="", stderr="")
        self.spawn_children: dict[str, FakeChild] = {}
        self.default_child_exit_after_polls: int | None = None
        self.default_child_exit_code: int = 0
        self._next_pid = 4000

    def calls_with_prefix(self, prefix: tuple[str, ...]) -> list[RunCall]:
        return [call for call in self.run_calls if call.argv[: len(prefix)] == prefix]

    @property
    def create_calls(self) -> list[RunCall]:
        return self.calls_with_prefix(("supabase", "branches", "create"))

    @property
    def delete_calls(self) -> list[RunCall]:
        return self.calls_with_prefix(("supabase", "branches", "delete"))

    def run(
        self,
        argv: Any,
        *,
        env: Any = None,
        cwd: Any = None,
        capture: bool = True,
    ) -> RunResult:
        self.run_calls.append(
            RunCall(
                argv=tuple(argv),
                env=dict(env) if env is not None else None,
                cwd=cwd,
                capture=capture,
            )
        )
        self.events.append(("run", " ".join(str(part) for part in argv[:3]), "run"))
        key = tuple(argv)
        if key in self.run_results:
            return self.run_results[key]
        for prefix, result in self.run_prefix_results:
            if key[: len(prefix)] == prefix:
                return result
        return self.default_result

    def spawn(
        self,
        label: str,
        argv: Any,
        *,
        env: Any = None,
        cwd: Any = None,
        log_path: Any = None,
        inherit_stdin: bool = False,
    ) -> FakeChild:
        self.spawn_calls.append(
            SpawnCall(
                label=label,
                argv=tuple(argv),
                env=dict(env) if env is not None else None,
                cwd=cwd,
                log_path=log_path,
                inherit_stdin=inherit_stdin,
            )
        )
        self.events.append(("spawn", label, "spawn"))
        child = self.spawn_children.pop(label, None)
        if child is None:
            # Only foreground children (spawned with inherit_stdin=True) exit
            # on their own; background/owned children stay alive so supervision
            # and the queue ownership guard observe healthy processes.
            child = FakeChild(
                label=label,
                pid=self._next_pid,
                events=self.events,
                exit_after_polls=self.default_child_exit_after_polls if inherit_stdin else None,
                exit_code=self.default_child_exit_code,
            )
            self._next_pid += 1
        return child


class FakeQueueClient:
    """QueueClient double with scripted readiness and flush recording."""

    def __init__(self, *, responding: bool = True, respond_after_pings: int | None = None) -> None:
        self.responding = responding
        self.respond_after_pings = respond_after_pings
        self.ping_count = 0
        self.flushes = 0
        self.urls: list[str] = []

    def ping(self) -> bool:
        self.ping_count += 1
        if not self.responding:
            if self.respond_after_pings is not None and self.ping_count >= self.respond_after_pings:
                self.responding = True
            else:
                raise RuntimeError("connection refused")
        return True

    def flushdb(self) -> None:
        self.flushes += 1


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def branch_env_output(
    *,
    api_url: str = BRANCH_API_URL,
    pooler_url: str | None = BRANCH_DB_URL,
    direct_url: str | None = None,
    publishable: str | None = PUBLISHABLE_KEY,
    default_key: str | None = DEFAULT_SECRET_KEY,
    extra: dict[str, str] | None = None,
) -> str:
    lines = [f"SUPABASE_URL={api_url}"]
    if pooler_url is not None:
        lines.append(f"POSTGRES_URL={pooler_url}")
    if direct_url is not None:
        lines.append(f"POSTGRES_URL_NON_POOLING={direct_url}")
    if publishable is not None:
        lines.append(f"SUPABASE_PUBLISHABLE_KEY={publishable}")
    if default_key is not None:
        lines.append(f"SUPABASE_DEFAULT_KEY={default_key}")
    if extra:
        lines.extend(f"{key}={value}" for key, value in extra.items())
    return "\n".join(lines) + "\n"


@dataclass
class Harness:
    devserver: Devserver
    runner: FakeRunner
    queue: FakeQueueClient
    log: RecordingLogger
    events: list[Event]
    root: Path
    env: dict[str, str]
    queue_factory_urls: list[str]

    def spawn_labels(self) -> list[str]:
        return [call.label for call in self.runner.spawn_calls]

    def signal_labels(self) -> list[str]:
        return [event[1] for event in self.events if event[0] == "signal"]


def make_harness(
    tmp_path: Path,
    config: RunConfig | None = None,
    *,
    root: Path | None = None,
    env: dict[str, str | None] | None = None,
    seed_sql: bool = False,
    get_output: str | None = None,
    get_returncode: int = 0,
    get_stderr: str = "",
    create_returncode: int = 0,
    queue_responding: bool = False,
    queue_respond_after_pings: int | None = 2,
    foreground_exit_after_polls: int | None = 3,
    foreground_exit_code: int = 0,
    command_resolver: Callable[[str], str | None] | None = None,
) -> Harness:
    repo_root = root if root is not None else tmp_path / "repo"
    (repo_root / "supabase").mkdir(parents=True)
    (repo_root / "scripts").mkdir()
    (repo_root / "supabase" / "cli-version").write_text(PIN_VERSION + "\n", encoding="utf-8")
    (repo_root / ".venv" / "bin").mkdir(parents=True)
    (repo_root / ".venv" / "bin" / "python").write_text("#!/bin/sh\n", encoding="utf-8")
    (repo_root / ".venv" / "bin" / "python").chmod(0o755)
    if seed_sql:
        (repo_root / "supabase" / "seed.sql").write_text("select 1;\n", encoding="utf-8")

    events: list[Event] = []
    runner = FakeRunner(events)
    runner.default_child_exit_after_polls = foreground_exit_after_polls
    runner.default_child_exit_code = foreground_exit_code
    runner.run_results[("supabase", "--version")] = RunResult(0, PIN_VERSION + "\n", "")
    runner.run_prefix_results.append(
        (
            ("supabase", "branches", "create"),
            RunResult(
                create_returncode,
                "",
                "stub: branch create failed" if create_returncode else "",
            ),
        )
    )
    runner.run_prefix_results.append(
        (
            ("supabase", "branches", "get"),
            RunResult(
                get_returncode,
                get_output if get_output is not None else branch_env_output(),
                get_stderr,
            ),
        )
    )
    runner.run_prefix_results.append((("supabase", "migration", "list"), RunResult(0, "", "")))

    queue = FakeQueueClient(
        responding=queue_responding, respond_after_pings=queue_respond_after_pings
    )
    queue_factory_urls: list[str] = []
    log = RecordingLogger()

    run_config = config if config is not None else RunConfig()
    run_env = {
        "OPENORC_SUPABASE_PROJECT_REF": REF_PARENT,
        "OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION,
        "OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS": "2",
        "OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS": "0",
    }
    if env:
        for key, value in env.items():
            if value is None:
                run_env.pop(key, None)
            else:
                run_env[key] = value

    def queue_factory(url: str) -> FakeQueueClient:
        queue_factory_urls.append(url)
        return queue

    def default_command_resolver(name: str) -> str | None:
        return f"/fake/bin/{name}"

    devserver = Devserver(
        config=run_config,
        root=repo_root,
        env=run_env,
        runner=runner,
        log=log,
        queue_client_factory=queue_factory,
        sleep=lambda seconds: None,
        tmp_root=tmp_path / "tmp",
        stop_term_timeout_seconds=0.3,
        command_resolver=(
            command_resolver if command_resolver is not None else default_command_resolver
        ),
    )
    return Harness(
        devserver=devserver,
        runner=runner,
        queue=queue,
        log=log,
        events=events,
        root=repo_root,
        env=run_env,
        queue_factory_urls=queue_factory_urls,
    )


# ---------------------------------------------------------------------------
# Wrapper contract
# ---------------------------------------------------------------------------


def _stripped_env() -> dict[str, str]:
    prefixes = ("OPENORC_", "SUPABASE_", "VALKEY_", "VITE_", "NGROK_", "LOCAL_")
    return {key: value for key, value in os.environ.items() if not key.startswith(prefixes)}


def test_bash_n_passes_for_devserver_wrapper_and_migrations_tool() -> None:
    for script in (WRAPPER_PATH, MIGRATIONS_TOOL_PATH):
        result = subprocess.run(
            ["bash", "-n", str(script)], capture_output=True, text=True, check=False
        )
        assert result.returncode == 0, result.stderr


def test_wrapper_fails_closed_without_venv_with_setup_hint(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy(WRAPPER_PATH, repo / "scripts" / "devserver.sh")
    result = subprocess.run(
        ["bash", str(repo / "scripts" / "devserver.sh"), "--help"],
        capture_output=True,
        text=True,
        check=False,
        env=_stripped_env(),
    )
    assert result.returncode == 1
    assert ".venv/bin/python" in result.stderr
    assert "uv sync" in result.stderr


def test_wrapper_execs_repo_python_from_repo_root_with_all_arguments(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / ".venv" / "bin").mkdir(parents=True)
    shutil.copy(WRAPPER_PATH, repo / "scripts" / "devserver.sh")
    probe = tmp_path / "wrapper-probe.txt"
    python_stub = repo / ".venv" / "bin" / "python"
    # The stub records cwd and argv without executing the entrypoint script.
    python_stub.write_text(
        '#!/bin/bash\nprintf \'%s\\n\' "$PWD" "$@" > "$WRAPPER_PROBE"\n',
        encoding="utf-8",
    )
    python_stub.chmod(0o755)

    result = subprocess.run(
        ["bash", str(repo / "scripts" / "devserver.sh"), "--api-only", "--no-supabase"],
        capture_output=True,
        text=True,
        check=False,
        env=_stripped_env() | {"WRAPPER_PROBE": str(probe)},
        cwd=str(tmp_path),
    )

    assert result.returncode == 0, result.stderr
    lines = probe.read_text(encoding="utf-8").splitlines()
    assert lines[0] == str(repo)
    assert lines[1:] == [str(repo / "scripts" / "devserver.py"), "--api-only", "--no-supabase"]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def test_default_config_is_full_stack() -> None:
    assert parse_args([]) == RunConfig()
    assert parse_args([]).foreground_kind == "app"


def test_app_only_disables_every_other_surface() -> None:
    config = parse_args(["--app-only"])
    assert (config.include_app, config.include_api, config.include_worker) == (True, False, False)
    assert not config.auto_valkey
    assert not config.auto_ngrok
    assert not config.auto_supabase


def test_api_only_keeps_supabase_and_ngrok() -> None:
    config = parse_args(["--api-only"])
    assert (config.include_app, config.include_api, config.include_worker) == (False, True, False)
    assert not config.auto_valkey
    assert config.auto_ngrok
    assert config.auto_supabase


def test_worker_only_keeps_valkey_and_supabase() -> None:
    config = parse_args(["--worker-only"])
    assert (config.include_app, config.include_api, config.include_worker) == (False, False, True)
    assert not config.auto_ngrok
    assert config.auto_valkey
    assert config.auto_supabase


def test_no_flags_disable_individual_automation() -> None:
    config = parse_args(
        ["--no-worker", "--no-valkey", "--no-ngrok", "--no-supabase", "--keep-supabase"]
    )
    assert config.include_worker is False
    assert config.auto_valkey is False
    assert config.auto_ngrok is False
    assert config.auto_supabase is False
    assert config.keep_supabase is True


@pytest.mark.parametrize("flag", ["-h", "--help"])
def test_help_requested(flag: str) -> None:
    with pytest.raises(HelpRequested):
        parse_args([flag])


def test_unknown_parameter_raises_usage_error() -> None:
    with pytest.raises(UsageError) as excinfo:
        parse_args(["--bogus"])
    assert "--bogus" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://127.0.0.1:6379/2", True),
        ("redis://localhost:6379/2", True),
        ("redis://127.0.0.1:6400/9", True),
        ("redis://0.0.0.0:6379/2", False),
        ("redis://example.com:6379/2", False),
        ("redis://127.0.0.1:6379", False),
        ("rediss://127.0.0.1:6379/2", False),
    ],
)
def test_is_local_valkey_url(url: str, expected: bool) -> None:
    assert is_local_valkey_url(url) is expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "postgresql://user:secret@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
            "postgresql://aws-0-us-east-1.pooler.supabase.com",
        ),
        (
            "postgres://user:secret@db.ref.supabase.co:5432/postgres",
            "postgres://db.ref.supabase.co",
        ),
        ("https://ref.supabase.co", "https://ref.supabase.co"),
        ("redis://127.0.0.1:6379/2", "redis://127.0.0.1"),
        ("not-a-url", "<unrecognized-url>"),
        ("postgresql://", "<unrecognized-url>"),
    ],
)
def test_redact_url(url: str, expected: str) -> None:
    assert redact_url(url) == expected


def test_load_env_file_respects_exported_values_and_skips_comments(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        '# comment\n\nFIRST_VALUE=alpha\nQUOTED="quoted value"\n1INVALID=x\nSECOND_VALUE=beta\n',
        encoding="utf-8",
    )
    env = {"SECOND_VALUE": "exported"}
    log = RecordingLogger()

    load_env_file(env_file, env, log)

    assert env["FIRST_VALUE"] == "alpha"
    assert env["QUOTED"] == "quoted value"
    assert env["SECOND_VALUE"] == "exported"
    assert "1INVALID" not in env
    assert any("1INVALID" in message for message in log.messages_of("warn"))


def test_load_env_file_missing_file_is_a_noop(tmp_path: Path) -> None:
    env: dict[str, str] = {}
    log = RecordingLogger()
    load_env_file(tmp_path / "missing.env", env, log)
    assert env == {}


# ---------------------------------------------------------------------------
# Branch environment parsing and key normalization
# ---------------------------------------------------------------------------


def test_branch_environment_modern_keys() -> None:
    parsed = parse_branch_environment(branch_env_output())
    assert parsed.api_url == BRANCH_API_URL
    assert parsed.publishable_key == PUBLISHABLE_KEY
    assert parsed.secret_key == DEFAULT_SECRET_KEY
    assert parsed.database_url == BRANCH_DB_URL
    assert parsed.database_credentials_published


def test_branch_environment_legacy_key_fallback() -> None:
    parsed = parse_branch_environment(
        branch_env_output(
            publishable=None,
            default_key=None,
            extra={"SUPABASE_ANON_KEY": ANON_KEY, "SUPABASE_SERVICE_ROLE_KEY": SERVICE_ROLE_KEY},
        )
    )
    assert parsed.publishable_key == ANON_KEY
    assert parsed.secret_key == SERVICE_ROLE_KEY


def test_branch_environment_mixed_key_generations() -> None:
    parsed = parse_branch_environment(
        branch_env_output(publishable=None, extra={"SUPABASE_ANON_KEY": ANON_KEY})
    )
    assert parsed.publishable_key == ANON_KEY
    assert parsed.secret_key == DEFAULT_SECRET_KEY


def test_branch_environment_modern_keys_win_over_legacy() -> None:
    parsed = parse_branch_environment(
        branch_env_output(
            extra={"SUPABASE_ANON_KEY": ANON_KEY, "SUPABASE_SERVICE_ROLE_KEY": SERVICE_ROLE_KEY}
        )
    )
    assert parsed.publishable_key == PUBLISHABLE_KEY
    assert parsed.secret_key == DEFAULT_SECRET_KEY


def test_branch_environment_prefers_pooler_url_with_direct_fallback() -> None:
    both = parse_branch_environment(branch_env_output(direct_url=BRANCH_DIRECT_DB_URL))
    assert both.database_url == BRANCH_DB_URL
    direct_only = parse_branch_environment(
        branch_env_output(pooler_url=None, direct_url=BRANCH_DIRECT_DB_URL)
    )
    assert direct_only.database_url == BRANCH_DIRECT_DB_URL
    assert direct_only.database_credentials_published


def test_branch_environment_without_database_credentials() -> None:
    parsed = parse_branch_environment(branch_env_output(pooler_url=None))
    assert not parsed.database_credentials_published


# ---------------------------------------------------------------------------
# Supabase CLI pin enforcement
# ---------------------------------------------------------------------------


def make_supabase_manager(
    tmp_path: Path,
    runner: FakeRunner,
    *,
    env: dict[str, str] | None = None,
    with_pin_file: bool = True,
    pin_content: str | None = None,
) -> tuple[SupabaseManager, RecordingLogger]:
    root = tmp_path / "repo"
    (root / "supabase").mkdir(parents=True, exist_ok=True)
    if with_pin_file:
        (root / "supabase" / "cli-version").write_text(
            (pin_content if pin_content is not None else PIN_VERSION) + "\n",
            encoding="utf-8",
        )
    log = RecordingLogger()
    manager = SupabaseManager(
        runner=runner,
        root=root,
        env=env if env is not None else {},
        log=log,
        sleep=lambda seconds: None,
        max_attempts=2,
        sleep_seconds=0.0,
    )
    return manager, log


def version_runner(stdout: str, returncode: int = 0) -> FakeRunner:
    runner = FakeRunner([])
    runner.run_results[("supabase", "--version")] = RunResult(returncode, stdout, "")
    return runner


def test_cli_pin_accepts_pinned_version(tmp_path: Path) -> None:
    manager, log = make_supabase_manager(tmp_path, version_runner(PIN_VERSION + "\n"))
    manager.verify_cli_pin()
    assert any(PIN_VERSION in message for message in log.messages_of("log"))


def test_cli_pin_accepts_brand_and_v_prefixes(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, version_runner(f"Supabase v{PIN_VERSION}"))
    manager.verify_cli_pin()


def test_cli_pin_refuses_unparseable_brand_output(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, version_runner(f"Supabase CLI v{PIN_VERSION}"))
    with pytest.raises(DevserverError, match="unexpected --version output"):
        manager.verify_cli_pin()


def test_cli_pin_refuses_mismatch(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, version_runner("2.116.0"))
    with pytest.raises(DevserverError, match="version mismatch"):
        manager.verify_cli_pin()


def test_cli_pin_refuses_failed_version_probe(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, version_runner("", returncode=7))
    with pytest.raises(DevserverError, match="non-zero status"):
        manager.verify_cli_pin()


def test_cli_pin_requires_pin_file(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, FakeRunner([]), with_pin_file=False)
    with pytest.raises(DevserverError, match="pin file missing"):
        manager.verify_cli_pin()


def test_cli_pin_requires_non_empty_pin(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, FakeRunner([]), pin_content="   ")
    with pytest.raises(DevserverError, match="pin file is empty"):
        manager.verify_cli_pin()


# ---------------------------------------------------------------------------
# Supabase configuration guards
# ---------------------------------------------------------------------------


def test_missing_parent_ref_fails_closed(tmp_path: Path) -> None:
    manager, _ = make_supabase_manager(tmp_path, FakeRunner([]))
    with pytest.raises(DevserverError, match="OPENORC_SUPABASE_PROJECT_REF"):
        manager.verify_configuration()


def test_parent_equal_to_production_warns_but_proceeds(tmp_path: Path) -> None:
    manager, log = make_supabase_manager(
        tmp_path,
        FakeRunner([]),
        env={
            "OPENORC_SUPABASE_PROJECT_REF": REF_PARENT,
            "OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PARENT,
        },
    )
    manager.verify_configuration()
    warnings = log.messages_of("warn")
    assert any("equals the configured production project ref" in message for message in warnings)
    assert any("writes still target only the branch database" in message for message in warnings)


def test_parent_differing_from_production_does_not_warn(tmp_path: Path) -> None:
    manager, log = make_supabase_manager(
        tmp_path,
        FakeRunner([]),
        env={
            "OPENORC_SUPABASE_PROJECT_REF": REF_PARENT,
            "OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION,
        },
    )
    manager.verify_configuration()
    assert log.messages_of("warn") == []


# ---------------------------------------------------------------------------
# Supabase orchestration (through Devserver)
# ---------------------------------------------------------------------------


def test_devserver_level_pin_mismatch_fails_before_branch_creation(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    harness.runner.run_results[("supabase", "--version")] = RunResult(0, "9.9.9", "")
    assert harness.devserver.run() == 1
    assert not harness.runner.create_calls
    assert any("version mismatch" in message for message in harness.log.messages_of("error"))


def test_missing_parent_ref_fails_at_devserver_level(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, env={"OPENORC_SUPABASE_PROJECT_REF": ""})
    assert harness.devserver.run() == 1
    assert not harness.runner.create_calls
    assert any(
        "OPENORC_SUPABASE_PROJECT_REF is required" in message
        for message in harness.log.messages_of("error")
    )


def test_branch_create_failure_does_not_attempt_delete(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, create_returncode=1)
    assert harness.devserver.run() == 1
    assert harness.runner.delete_calls == []
    assert any("creation failed" in message for message in harness.log.messages_of("error"))


def test_readiness_timeout_fails_and_deletes_created_branch(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        get_output=branch_env_output(pooler_url=None, publishable=None, default_key=None),
    )
    assert harness.devserver.run() == 1
    assert any("did not become ready" in message for message in harness.log.messages_of("error"))
    # The bounded wait performed exactly max_attempts attempts.
    assert len(harness.runner.calls_with_prefix(("supabase", "branches", "get"))) == 2
    assert len(harness.runner.delete_calls) == 1
    created_name = harness.runner.create_calls[0].argv[3]
    assert created_name.startswith("openorc-e2e-")
    assert harness.runner.delete_calls[0].argv[3] == created_name
    assert harness.runner.delete_calls[0].argv[4:] == ("--project-ref", REF_PARENT)


def test_auth_failure_fails_immediately_without_retry(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path, get_returncode=1, get_stderr="error: invalid access token (401)"
    )
    assert harness.devserver.run() == 1
    assert len(harness.runner.calls_with_prefix(("supabase", "branches", "get"))) == 1
    assert any(
        "authentication/authorization" in message for message in harness.log.messages_of("error")
    )
    assert len(harness.runner.delete_calls) == 1


def test_migration_failure_fails_hard_and_deletes_branch(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    migrations_prefix = (str(harness.root / "scripts" / "supabase-apply-migrations.sh"),)
    harness.runner.run_prefix_results.append(
        (migrations_prefix, RunResult(1, "", "migration tool refused"))
    )
    assert harness.devserver.run() == 1
    assert any("migration" in message.lower() for message in harness.log.messages_of("error"))
    assert len(harness.runner.delete_calls) == 1


def test_full_supabase_sequence_on_happy_path(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--api-only"]))
    assert harness.devserver.run() == 0

    argvs = [call.argv for call in harness.runner.run_calls]
    create = harness.runner.create_calls[0]
    branch_name = create.argv[3]
    assert branch_name.startswith("openorc-e2e-")
    assert create.argv[4:] == ("--project-ref", REF_PARENT)

    get_call = harness.runner.calls_with_prefix(("supabase", "branches", "get"))[0]
    assert get_call.argv[2:4] == ("get", branch_name)
    assert get_call.argv[-2:] == ("-o", "env")

    probe = harness.runner.calls_with_prefix(("supabase", "migration", "list"))[0]
    assert probe.argv[-1] == BRANCH_DB_URL

    migrations = [
        call
        for call in harness.runner.run_calls
        if call.argv[0].endswith("supabase-apply-migrations.sh")
    ]
    assert migrations[0].argv[1:] == ("--branch", branch_name)

    # No seed.sql exists: the seed step is skipped entirely.
    assert harness.runner.calls_with_prefix(("supabase", "db", "push")) == []

    delete = harness.runner.delete_calls
    assert len(delete) == 1
    assert delete[0].argv[3] == branch_name
    assert argvs.index(delete[0].argv) > argvs.index(migrations[0].argv)


def test_seed_applied_when_seed_sql_present(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, seed_sql=True, config=parse_args(["--api-only"]))
    assert harness.devserver.run() == 0
    push = harness.runner.calls_with_prefix(("supabase", "db", "push"))
    assert len(push) == 1
    assert push[0].argv[3:] == ("--db-url", BRANCH_DB_URL, "--include-seed")


def test_keep_supabase_preserves_branch(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--api-only", "--keep-supabase"]))
    assert harness.devserver.run() == 0
    assert harness.runner.delete_calls == []
    warnings = harness.log.messages_of("warn")
    assert any("--keep-supabase" in message for message in warnings)
    assert any("DELETE IT MANUALLY" in message for message in warnings)


def test_no_supabase_never_invokes_cli(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--api-only", "--no-supabase"]))
    assert harness.devserver.run() == 0
    assert not [call for call in harness.runner.run_calls if call.argv[:1] == ("supabase",)]


def test_unresolvable_publishable_key_fails_closed(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, get_output=branch_env_output(publishable=None))
    assert harness.devserver.run() == 1
    errors = harness.log.messages_of("error")
    assert any("SUPABASE_PUBLISHABLE_KEY" in message for message in errors)
    assert any("SUPABASE_ANON_KEY" in message for message in errors)
    assert len(harness.runner.delete_calls) == 1


def test_unresolvable_secret_key_fails_closed(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, get_output=branch_env_output(default_key=None))
    assert harness.devserver.run() == 1
    errors = harness.log.messages_of("error")
    assert any("SUPABASE_DEFAULT_KEY" in message for message in errors)
    assert any("SUPABASE_SERVICE_ROLE_KEY" in message for message in errors)
    assert len(harness.runner.delete_calls) == 1


# ---------------------------------------------------------------------------
# Valkey / Redis-compatible queue backend rules
# ---------------------------------------------------------------------------


def test_exported_valkey_url_wins_over_default(tmp_path: Path) -> None:
    custom = "redis://127.0.0.1:6400/9"
    harness = make_harness(tmp_path, env={"VALKEY_URL": custom})
    assert harness.devserver.run() == 0
    backend = next(call for call in harness.runner.spawn_calls if call.label == "queue-backend")
    assert backend.argv[1:3] == ("--port", "6400")
    assert set(harness.queue_factory_urls) == {custom}
    worker = next(call for call in harness.runner.spawn_calls if call.label == "worker")
    assert worker.env is not None and worker.env["VALKEY_URL"] == custom


def test_devserver_owned_valkey_started_with_persistence_disabled_and_stopped(
    tmp_path: Path,
) -> None:
    harness = make_harness(tmp_path)
    tmp_dir = tmp_path / "tmp" / f"openorc-redis-{os.getpid()}"
    log_path = tmp_path / "tmp" / f"openorc-redis-{os.getpid()}.log"

    assert harness.devserver.run() == 0

    backend = next(call for call in harness.runner.spawn_calls if call.label == "queue-backend")
    assert backend.argv == (
        "redis-server",
        "--port",
        "6379",
        "--bind",
        "127.0.0.1",
        "--save",
        "",
        "--appendonly",
        "no",
        "--dir",
        str(tmp_dir),
    )
    assert backend.log_path == log_path
    assert harness.queue.flushes == 1
    assert set(harness.queue_factory_urls) == {DEFAULT_VALKEY_URL}
    # The owned server is stopped (TERM) during cleanup and its invocation
    # temp directory is removed; the log file is intentionally left behind.
    assert ("signal", "queue-backend", signal.SIGTERM) in harness.events
    assert not tmp_dir.exists()


def test_preexisting_local_valkey_reused_flushed_never_stopped(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, queue_responding=True)
    assert harness.devserver.run() == 0
    assert not [call for call in harness.runner.spawn_calls if call.label == "queue-backend"]
    assert harness.queue.flushes == 1
    assert "queue-backend" not in harness.signal_labels()
    assert any("externally owned" in message for message in harness.log.messages_of("log"))


def test_valkey_port_theft_guard_fails_closed(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    harness.runner.spawn_children["queue-backend"] = FakeChild(
        label="queue-backend", pid=9999, events=harness.events, dead=True
    )
    assert harness.devserver.run() == 1
    errors = harness.log.messages_of("error")
    assert any("will not adopt" in message for message in errors)
    assert harness.queue.flushes == 0
    assert "queue-backend" not in harness.signal_labels()
    # The Supabase branch was created before the failure and is still deleted.
    assert len(harness.runner.delete_calls) == 1


def test_nonlocal_valkey_url_never_started_flushed_or_stopped(tmp_path: Path) -> None:
    remote = "redis://queue.example.internal:6379/5"
    harness = make_harness(tmp_path, env={"VALKEY_URL": remote})
    assert harness.devserver.run() == 0
    assert not [call for call in harness.runner.spawn_calls if call.label == "queue-backend"]
    assert harness.queue.flushes == 0
    assert harness.queue.ping_count == 0
    assert any("non-local" in message for message in harness.log.messages_of("log"))
    worker = next(call for call in harness.runner.spawn_calls if call.label == "worker")
    assert worker.env is not None and worker.env["VALKEY_URL"] == remote


def test_no_valkey_disables_start_and_flush(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--no-valkey"]))
    assert harness.devserver.run() == 0
    assert not [call for call in harness.runner.spawn_calls if call.label == "queue-backend"]
    assert harness.queue.flushes == 0
    assert any("Auto-Valkey disabled" in message for message in harness.log.messages_of("log"))


# ---------------------------------------------------------------------------
# Lifecycle, modes, and cleanup ordering
# ---------------------------------------------------------------------------


def test_full_lifecycle_stop_order_and_supabase_deletion_last(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    assert harness.devserver.run() == 0

    # Children spawn in dependency order: owned queue backend, API, worker,
    # then the foreground app.
    assert harness.spawn_labels() == ["queue-backend", "api", "worker", "app"]

    app = next(call for call in harness.runner.spawn_calls if call.label == "app")
    assert app.cwd == harness.root / "apps" / "app"
    assert app.argv == ("npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "8081")
    api = next(call for call in harness.runner.spawn_calls if call.label == "api")
    assert api.env is not None and api.env["OPENORC_ENV"] == "development"
    assert api.env["OPENORC_API_PORT"] == "3000"
    worker = next(call for call in harness.runner.spawn_calls if call.label == "worker")
    assert worker.env is not None and worker.env["OPENORC_ENV"] == "development"
    assert worker.env["VALKEY_URL"] == DEFAULT_VALKEY_URL

    # Teardown: the foreground app already exited naturally (no signal), then
    # worker, then API, then the owned queue backend last of the processes.
    assert harness.signal_labels() == ["worker", "api", "queue-backend"]
    assert all(event[2] == signal.SIGTERM for event in harness.events if event[0] == "signal")

    # The worker is fully reaped (RQ teardown) before the queue backend is
    # signaled: Redis is alive for the whole worker shutdown.
    worker_wait = harness.events.index(("wait", "worker", "wait"))
    backend_signal = harness.events.index(("signal", "queue-backend", signal.SIGTERM))
    assert worker_wait < backend_signal

    # The Supabase branch deletion is the very last action of the run.
    delete_events = [event for event in harness.events if "branches delete" in event[1]]
    assert len(delete_events) == 1
    assert harness.events.index(delete_events[0]) == len(harness.events) - 1


def test_foreground_exit_code_propagates(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, foreground_exit_after_polls=2, foreground_exit_code=7)
    assert harness.devserver.run() == 7
    assert len(harness.runner.delete_calls) == 1


def test_worker_only_runs_worker_foreground_without_app_or_api(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--worker-only"]))
    assert harness.devserver.run() == 0
    assert harness.spawn_labels() == ["queue-backend", "worker"]
    worker = harness.runner.spawn_calls[1]
    assert worker.inherit_stdin is True
    assert harness.queue.flushes == 1


def test_api_only_skips_valkey_and_runs_api_foreground(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--api-only"]))
    assert harness.devserver.run() == 0
    assert harness.spawn_labels() == ["api"]
    api = harness.runner.spawn_calls[0]
    assert api.inherit_stdin is True
    assert api.env is not None and api.env["OPENORC_API_PORT"] == "3000"
    assert harness.queue.flushes == 0


def test_app_only_runs_npm_in_app_directory_only(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--app-only"]))
    assert harness.devserver.run() == 0
    assert harness.spawn_labels() == ["app"]
    app = harness.runner.spawn_calls[0]
    assert app.cwd == harness.root / "apps" / "app"
    assert app.argv == ("npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "8081")
    assert not [call for call in harness.runner.run_calls if call.argv[:1] == ("supabase",)]
    assert harness.queue.flushes == 0


def test_ngrok_started_when_configured(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        config=parse_args(["--api-only"]),
        env={"NGROK_RESERVED_URL": "https://openorc-test.ngrok.app"},
    )
    assert harness.devserver.run() == 0
    ngrok = next(call for call in harness.runner.spawn_calls if call.label == "ngrok")
    assert ngrok.argv == ("ngrok", "http", "--url=https://openorc-test.ngrok.app", "3000")
    assert ngrok.log_path == NGROK_LOG_PATH


def test_ngrok_skipped_when_not_configured(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--api-only"]))
    assert harness.devserver.run() == 0
    assert not [call for call in harness.runner.spawn_calls if call.label == "ngrok"]
    assert any(
        "NGROK_RESERVED_URL not set" in message for message in harness.log.messages_of("log")
    )


def test_vite_api_base_url_defaulted_only_when_absent(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    assert harness.devserver.run() == 0
    app = next(call for call in harness.runner.spawn_calls if call.label == "app")
    assert app.env is not None
    assert app.env["VITE_API_BASE_URL"] == "http://127.0.0.1:3000"

    harness2 = make_harness(
        tmp_path,
        root=tmp_path / "case2",
        env={"VITE_API_BASE_URL": "https://api.example.test"},
    )
    assert harness2.devserver.run() == 0
    app2 = next(call for call in harness2.runner.spawn_calls if call.label == "app")
    assert app2.env is not None
    assert app2.env["VITE_API_BASE_URL"] == "https://api.example.test"


def test_env_file_supplies_parent_ref(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    harness = make_harness(tmp_path, root=root, env={"OPENORC_SUPABASE_PROJECT_REF": None})
    (root / ".env").write_text(f"OPENORC_SUPABASE_PROJECT_REF={REF_PARENT}\n", encoding="utf-8")
    assert harness.devserver.run() == 0
    assert harness.runner.create_calls[0].argv[5] == REF_PARENT


def test_exported_parent_ref_wins_over_env_file(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    harness = make_harness(tmp_path, root=root)
    (root / ".env").write_text(f"OPENORC_SUPABASE_PROJECT_REF={'e' * 20}\n", encoding="utf-8")
    assert harness.devserver.run() == 0
    assert harness.runner.create_calls[0].argv[5] == REF_PARENT


def test_missing_venv_python_fails_with_setup_hint(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    (harness.root / ".venv" / "bin" / "python").unlink()
    assert harness.devserver.run() == 1
    assert any("uv sync" in message for message in harness.log.messages_of("error"))
    # Base tool checks precede any Supabase operation.
    assert harness.runner.create_calls == []


def test_nothing_selected_to_run_is_rejected(tmp_path: Path) -> None:
    config = RunConfig(
        include_app=False, include_api=False, include_worker=False, auto_supabase=False
    )
    harness = make_harness(tmp_path, config=config)
    assert harness.devserver.run() == 1
    assert any("Nothing selected to run" in message for message in harness.log.messages_of("error"))


def test_credentials_exported_to_children_never_env_file(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    (harness.root / ".env").write_text("NOTE=untouched\n", encoding="utf-8")
    assert harness.devserver.run() == 0

    api = next(call for call in harness.runner.spawn_calls if call.label == "api")
    worker = next(call for call in harness.runner.spawn_calls if call.label == "worker")
    for call in (api, worker):
        env = call.env or {}
        assert env["SUPABASE_URL"] == BRANCH_API_URL
        assert env["SUPABASE_PUBLISHABLE_KEY"] == PUBLISHABLE_KEY
        assert env["SUPABASE_SECRET_KEY"] == DEFAULT_SECRET_KEY
        assert env["DATABASE_URL"] == BRANCH_DB_URL
        assert env["OPENORC_ENV"] == "development"
    assert api.env is not None and api.env["OPENORC_API_PORT"] == "3000"
    assert worker.env is not None and worker.env["VALKEY_URL"] == DEFAULT_VALKEY_URL

    # Generated branch credentials never reach tracked/generated repo files.
    assert (harness.root / ".env").read_text(encoding="utf-8") == "NOTE=untouched\n"


def test_secrets_never_logged_and_urls_host_only(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    assert harness.devserver.run() == 0
    text = harness.log.text()
    assert PUBLISHABLE_KEY not in text
    assert DEFAULT_SECRET_KEY not in text
    assert "branchsecret" not in text
    assert BRANCH_DB_URL not in text
    assert "postgresql://aws-0-us-east-1.pooler.supabase.com" in text


# ---------------------------------------------------------------------------
# Signals: shutdown requests only; one idempotent cleanup path
# ---------------------------------------------------------------------------


def wait_for_app_spawn(harness: Harness) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if any(call.label == "app" for call in harness.runner.spawn_calls):
            return
        time.sleep(0.01)
    pytest.fail("devserver did not reach the app foreground")


def test_sigint_requests_shutdown_returns_130_and_cleans_up_once(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, foreground_exit_after_polls=None)
    results: list[int] = []
    thread = threading.Thread(target=lambda: results.append(harness.devserver.run()))
    thread.start()
    try:
        wait_for_app_spawn(harness)
        # A second request must not duplicate or override the first.
        harness.devserver.request_shutdown(signal.SIGINT)
        harness.devserver.request_shutdown(signal.SIGTERM)
    finally:
        thread.join(timeout=15)

    assert not thread.is_alive()
    assert results == [130]

    # The single cleanup path stopped the still-running foreground app first,
    # then worker and API, then the owned queue backend, and deleted the
    # branch exactly once as the final action.
    assert harness.signal_labels() == ["app", "worker", "api", "queue-backend"]
    assert len(harness.runner.delete_calls) == 1
    delete_events = [event for event in harness.events if "branches delete" in event[1]]
    assert harness.events.index(delete_events[0]) == len(harness.events) - 1


def test_sigterm_requests_shutdown_returns_143(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, foreground_exit_after_polls=None)
    results: list[int] = []
    thread = threading.Thread(target=lambda: results.append(harness.devserver.run()))
    thread.start()
    try:
        wait_for_app_spawn(harness)
        harness.devserver.request_shutdown(signal.SIGTERM)
    finally:
        thread.join(timeout=15)

    assert not thread.is_alive()
    assert results == [143]
    assert harness.signal_labels() == ["app", "worker", "api", "queue-backend"]


# ---------------------------------------------------------------------------
# Real subprocess supervision primitives
#
# These use real child processes to prove the dangerous contracts: children
# run in their own session, process-group signals reach the child tree, TERM
# escalation to KILL works, and every child is reaped (no zombies).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group supervision")
def test_real_child_is_terminated_and_reaped() -> None:
    log = RecordingLogger()
    supervisor = ProcessSupervisor(log, sleep=time.sleep, stop_term_timeout_seconds=2.0)
    child = supervisor.start(
        "sleeper",
        [sys.executable, "-c", "import time; time.sleep(30)"],
        runner=SubprocessRunner(),
    )
    assert child.poll() is None

    started = time.monotonic()
    supervisor.stop_all_reverse()
    elapsed = time.monotonic() - started

    assert child.poll() is not None
    assert elapsed < 10
    assert any("Stopping sleeper" in message for message in log.messages_of("log"))


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group supervision")
def test_real_child_ignoring_sigterm_is_killed(tmp_path: Path) -> None:
    log = RecordingLogger()
    supervisor = ProcessSupervisor(log, sleep=time.sleep, stop_term_timeout_seconds=0.5)
    ready_marker = tmp_path / "stubborn-ready"
    child = supervisor.start(
        "stubborn",
        [
            sys.executable,
            "-c",
            "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"open({str(ready_marker)!r}, 'w').close(); time.sleep(30)",
        ],
        runner=SubprocessRunner(),
        log_path=tmp_path / "stubborn.log",
    )
    deadline = time.monotonic() + 10.0
    while not ready_marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert ready_marker.exists(), "stubborn child never installed its SIGTERM handler"
    assert child.poll() is None

    supervisor.stop_all_reverse()

    # The child ignored TERM, so only the KILL escalation could end it.
    assert child.poll() is not None
    assert any("forcing" in message for message in log.messages_of("warn"))


# ---------------------------------------------------------------------------
# Regression: owned queue backend is recorded immediately after spawn
#
# Startup failure after branch creation must still invoke cleanup: if the
# ownership guard or readiness wait fails while the spawned child is alive,
# the canonical cleanup path must stop/reap it, remove its temp dir, and
# still delete the invocation-created Supabase branch.
# ---------------------------------------------------------------------------


def test_readiness_failure_stops_owned_redis_removes_temp_dir_and_deletes_branch(
    tmp_path: Path,
) -> None:
    # The queue backend never becomes ready; the spawned child stays alive.
    harness = make_harness(tmp_path, queue_responding=False, queue_respond_after_pings=None)
    tmp_dir = tmp_path / "tmp" / f"openorc-redis-{os.getpid()}"

    assert harness.devserver.run() == 1

    errors = harness.log.messages_of("error")
    assert any("did not become ready" in message for message in errors)
    assert any(call.label == "queue-backend" for call in harness.runner.spawn_calls)

    # The owned child was recorded before readiness failed, so cleanup
    # stopped and reaped it (safe even though it never became ready).
    assert ("signal", "queue-backend", signal.SIGTERM) in harness.events
    assert ("wait", "queue-backend", "wait") in harness.events
    assert not tmp_dir.exists()

    # The branch created before the failure is still deleted.
    assert len(harness.runner.delete_calls) == 1


# ---------------------------------------------------------------------------
# Regression: cleanup stages are best-effort and deletion is always last
# ---------------------------------------------------------------------------


def test_cleanup_stage_failure_does_not_block_branch_deletion(tmp_path: Path) -> None:
    harness = make_harness(tmp_path)
    # Poison one application child (per-child resilience inside stage 1) and
    # the owned queue-backend child (stage-level guard around stage 2).
    harness.runner.spawn_children["worker"] = FakeChild(
        label="worker", pid=4242, events=harness.events, fail_wait=True
    )
    harness.runner.spawn_children["queue-backend"] = FakeChild(
        label="queue-backend", pid=4243, events=harness.events, fail_wait=True
    )
    tmp_dir = tmp_path / "tmp" / f"openorc-redis-{os.getpid()}"

    assert harness.devserver.run() == 0

    warnings = harness.log.messages_of("warn")
    assert any("Failed to stop worker" in message for message in warnings)
    assert any(
        "Cleanup stage 'owned queue-backend shutdown' failed" in message for message in warnings
    )

    # Later cleanup stages still ran where safe.
    assert ("signal", "api", signal.SIGTERM) in harness.events
    assert ("signal", "queue-backend", signal.SIGTERM) in harness.events
    assert not tmp_dir.exists()

    # Supabase deletion was attempted exactly once and remains the last
    # action; the cleanup failure did not mask the run result.
    assert len(harness.runner.delete_calls) == 1
    delete_events = [event for event in harness.events if "branches delete" in event[1]]
    assert harness.events.index(delete_events[0]) == len(harness.events) - 1


# ---------------------------------------------------------------------------
# Regression: command resolution is injected (no host-tool dependencies)
# ---------------------------------------------------------------------------


def test_missing_supabase_cli_fails_closed(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        command_resolver=lambda name: None if name == "supabase" else f"/fake/bin/{name}",
    )
    assert harness.devserver.run() == 1
    assert any(
        "Required command not found on PATH: supabase" in message
        for message in harness.log.messages_of("error")
    )
    assert harness.runner.create_calls == []


def test_missing_npm_fails_closed_when_app_included(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        command_resolver=lambda name: None if name == "npm" else f"/fake/bin/{name}",
    )
    assert harness.devserver.run() == 1
    assert any(
        "Required command not found on PATH: npm" in message
        for message in harness.log.messages_of("error")
    )
    assert harness.runner.create_calls == []


def test_missing_redis_server_fails_closed(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        command_resolver=(lambda name: None if name == "redis-server" else f"/fake/bin/{name}"),
    )
    assert harness.devserver.run() == 1
    errors = harness.log.messages_of("error")
    assert any("redis-server is not on PATH" in message for message in errors)
    # The binary check precedes any spawn attempt.
    assert not [call for call in harness.runner.spawn_calls if call.label == "queue-backend"]
    # The branch created before the failure is still deleted.
    assert len(harness.runner.delete_calls) == 1


def test_ngrok_binary_missing_warns_and_skips(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        config=parse_args(["--api-only"]),
        env={"NGROK_RESERVED_URL": "https://openorc-test.ngrok.app"},
        command_resolver=lambda name: None if name == "ngrok" else f"/fake/bin/{name}",
    )
    assert harness.devserver.run() == 0
    assert not [call for call in harness.runner.spawn_calls if call.label == "ngrok"]
    assert any("ngrok not found on PATH" in message for message in harness.log.messages_of("warn"))


# ---------------------------------------------------------------------------
# --testdb: Owner-only command-against-ephemeral-branch mode
# ---------------------------------------------------------------------------


def test_testdb_parses_command_after_separator() -> None:
    config = parse_args(["--testdb", "--", ".venv/bin/python", "-m", "pytest"])
    assert config.testdb_command == (".venv/bin/python", "-m", "pytest")
    assert config.keep_supabase is False


def test_testdb_composes_with_keep_supabase_before_separator() -> None:
    config = parse_args(["--testdb", "--keep-supabase", "--", "echo", "hi"])
    assert config.testdb_command == ("echo", "hi")
    assert config.keep_supabase is True


def test_testdb_flags_after_separator_belong_to_the_command() -> None:
    config = parse_args(["--testdb", "--", "pytest", "--keep-supabase", "-x"])
    assert config.testdb_command == ("pytest", "--keep-supabase", "-x")


@pytest.mark.parametrize(
    "flag",
    [
        "--app-only",
        "--api-only",
        "--worker-only",
        "--no-worker",
        "--no-valkey",
        "--no-ngrok",
        "--no-supabase",
    ],
)
def test_testdb_rejects_surface_selection_flags(flag: str) -> None:
    with pytest.raises(UsageError, match="cannot be combined with --testdb"):
        parse_args(["--testdb", flag, "--", "true"])


@pytest.mark.parametrize("argv", [["--testdb"], ["--testdb", "--"]])
def test_testdb_without_command_is_a_usage_error(argv: list[str]) -> None:
    with pytest.raises(UsageError, match="command"):
        parse_args(argv)


def test_testdb_command_without_separator_is_a_usage_error() -> None:
    with pytest.raises(UsageError, match="Expected '--'"):
        parse_args(["--testdb", "pytest", "-m", "integration"])


def test_testdb_unknown_option_is_a_usage_error() -> None:
    with pytest.raises(UsageError, match="Unknown parameter"):
        parse_args(["--testdb", "--bogus", "--", "true"])


def test_testdb_provisions_branch_runs_command_and_deletes_branch(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        config=parse_args(
            ["--testdb", "--", ".venv/bin/python", "-m", "pytest", "-m", "integration"]
        ),
    )
    assert harness.devserver.run() == 0

    # Only the testdb command runs: no app, API, worker, queue backend, or
    # ngrok; the queue client is never consulted.
    assert harness.spawn_labels() == ["testdb-command"]
    assert harness.queue.ping_count == 0
    assert harness.queue.flushes == 0
    assert harness.queue_factory_urls == []

    create = harness.runner.create_calls[0]
    branch_name = create.argv[3]
    migrations = [
        call
        for call in harness.runner.run_calls
        if call.argv[0].endswith("supabase-apply-migrations.sh")
    ]
    assert migrations[0].argv[1:] == ("--branch", branch_name)

    child = harness.runner.spawn_calls[0]
    assert child.argv == (".venv/bin/python", "-m", "pytest", "-m", "integration")
    assert child.inherit_stdin is True
    assert child.env is not None
    assert child.env["OPENORC_TEST_DATABASE_URL"] == BRANCH_DB_URL
    # Only the testdb contract variable is injected: the branch URL is never
    # mapped onto application DATABASE_URL or the stack service environment.
    assert "DATABASE_URL" not in child.env
    assert "SUPABASE_URL" not in child.env
    assert "OPENORC_ENV" not in child.env
    assert "VITE_API_BASE_URL" not in child.env

    # Ordering: provisioning+migrations -> child command -> branch deletion,
    # exactly once, last of all.
    kinds = [(event[0], event[1]) for event in harness.events]
    migration_index = next(
        index
        for index, (kind, name) in enumerate(kinds)
        if kind == "run" and "supabase-apply-migrations" in name
    )
    spawn_index = next(
        index
        for index, (kind, name) in enumerate(kinds)
        if kind == "spawn" and name == "testdb-command"
    )
    delete_index = next(
        index
        for index, (kind, name) in enumerate(kinds)
        if kind == "run" and "branches delete" in name
    )
    assert migration_index < spawn_index < delete_index
    assert len(harness.runner.delete_calls) == 1
    assert harness.runner.delete_calls[0].argv[3] == branch_name


def test_testdb_branch_url_overrides_existing_export(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        config=parse_args(["--testdb", "--", "env"]),
        env={"OPENORC_TEST_DATABASE_URL": "postgresql://stale:stale@old-host:5432/stale"},
    )
    assert harness.devserver.run() == 0
    child = harness.runner.spawn_calls[0]
    assert child.env is not None
    # The generated branch URL always wins for the child process.
    assert child.env["OPENORC_TEST_DATABASE_URL"] == BRANCH_DB_URL


def test_testdb_logs_never_contain_credentials_or_full_database_url(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--testdb", "--", "true"]))
    assert harness.devserver.run() == 0
    text = harness.log.text()
    assert "branchsecret" not in text
    assert BRANCH_DB_URL not in text
    assert PUBLISHABLE_KEY not in text
    assert DEFAULT_SECRET_KEY not in text
    # Host-oriented redaction is the only database reference logged.
    assert "postgresql://aws-0-us-east-1.pooler.supabase.com" in text


def test_testdb_child_exit_status_propagates(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path, config=parse_args(["--testdb", "--", "false"]), foreground_exit_code=7
    )
    assert harness.devserver.run() == 7
    # Child failure still deletes the created branch exactly once.
    assert len(harness.runner.delete_calls) == 1


def test_testdb_migration_failure_fails_closed_and_cleans_up(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--testdb", "--", "true"]))
    harness.runner.run_prefix_results.append(
        ((str(harness.root / "scripts" / "supabase-apply-migrations.sh"),), RunResult(1, "", ""))
    )
    assert harness.devserver.run() == 1
    # Ownership was acquired (branch created) before the failure, so the
    # single cleanup path still deletes exactly that branch; the child
    # command never runs.
    assert len(harness.runner.create_calls) == 1
    assert len(harness.runner.delete_calls) == 1
    assert harness.spawn_labels() == []
    assert any(
        "migration application failed" in message for message in harness.log.messages_of("error")
    )


def test_testdb_branch_create_failure_never_deletes_any_branch(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path, config=parse_args(["--testdb", "--", "true"]), create_returncode=1
    )
    assert harness.devserver.run() == 1
    # No branch was created by this invocation, so cleanup deletes nothing.
    assert harness.runner.delete_calls == []
    assert harness.spawn_labels() == []


def test_testdb_seed_is_never_applied(tmp_path: Path) -> None:
    harness = make_harness(tmp_path, config=parse_args(["--testdb", "--", "true"]), seed_sql=True)
    assert harness.devserver.run() == 0
    # --testdb applies migrations only; seeding stays an ordinary-mode step.
    assert harness.runner.calls_with_prefix(("supabase", "db", "push")) == []


def test_testdb_keep_supabase_preserves_created_branch(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path, config=parse_args(["--testdb", "--keep-supabase", "--", "true"])
    )
    assert harness.devserver.run() == 0
    assert harness.runner.delete_calls == []
    assert any("--keep-supabase" in message for message in harness.log.messages_of("warn"))


def test_testdb_requires_supabase_binary_but_not_stack_tools(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        config=parse_args(["--testdb", "--", "true"]),
        command_resolver=(lambda name: None if name == "supabase" else f"/fake/bin/{name}"),
    )
    assert harness.devserver.run() == 1
    assert any(
        "Required command not found on PATH: supabase" in message
        for message in harness.log.messages_of("error")
    )
    # The binary check precedes any Supabase operation.
    assert harness.runner.create_calls == []


def wait_for_testdb_spawn(harness: Harness) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if any(call.label == "testdb-command" for call in harness.runner.spawn_calls):
            return
        time.sleep(0.01)
    pytest.fail("devserver did not reach the testdb command")


def test_testdb_child_stopped_and_branch_deleted_on_sigint(tmp_path: Path) -> None:
    harness = make_harness(
        tmp_path,
        config=parse_args(["--testdb", "--", ".venv/bin/python", "-m", "pytest"]),
        foreground_exit_after_polls=None,
    )
    results: list[int] = []
    thread = threading.Thread(target=lambda: results.append(harness.devserver.run()))
    thread.start()
    try:
        wait_for_testdb_spawn(harness)
        harness.devserver.request_shutdown(signal.SIGINT)
    finally:
        thread.join(timeout=15)

    assert not thread.is_alive()
    assert results == [130]
    # The single cleanup path stops the child first, then deletes the branch
    # created by this invocation exactly once, last of all.
    assert harness.signal_labels() == ["testdb-command"]
    assert len(harness.runner.delete_calls) == 1
    delete_events = [event for event in harness.events if "branches delete" in event[1]]
    assert harness.events.index(delete_events[0]) == len(harness.events) - 1
