"""Deterministic coverage for scripts/devserver.sh.

The script is exercised inside a fake repository root with stub executables
(a fake adapter at a bootstrap boundary):

- a stub `supabase` CLI on PATH records every argv and emits canned branch
  environment output;
- a stub replacement for scripts/supabase-apply-migrations.sh records its
  invocation and selected environment;
- a stub `.venv/bin/python` records process launches and environment;
- stub `npm`, `redis-cli`, `redis-server`, and `brew` binaries record argv;
  `brew` doubles as a canary (devserver must never invoke Homebrew).

Every test is deterministic and requires no live Supabase/Valkey/npm
infrastructure. A developer's local `.env` can never influence results.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DEVSERVER_PATH = REPO_ROOT / "scripts" / "devserver.sh"
MIGRATIONS_TOOL_PATH = REPO_ROOT / "scripts" / "supabase-apply-migrations.sh"
PIN_PATH = REPO_ROOT / "supabase" / "cli-version"

_PIN_VERSION = PIN_PATH.read_text(encoding="utf-8").strip()

# 20-character lowercase alphanumeric Supabase project refs.
REF_PARENT = "a" * 20
REF_PRODUCTION = "c" * 20

REF_BRANCH = "b" * 20

BRANCH_DB_URL = f"postgresql://postgres.{REF_BRANCH}:branchsecret@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
BRANCH_DIRECT_DB_URL = (
    f"postgresql://postgres:branchsecret@db.{REF_BRANCH}.supabase.co:5432/postgres"
)
BRANCH_API_URL = f"https://{REF_BRANCH}.supabase.co"

PUBLISHABLE_KEY = "sb_publishable_test-key"
DEFAULT_SECRET_KEY = "sb_secret_test-key"
ANON_KEY = "legacy-anon-key"
SERVICE_ROLE_KEY = "legacy-service-role-key"

DEFAULT_VALKEY_URL = "redis://127.0.0.1:6379/2"

# Environment prefixes stripped from the calling shell so ambient developer
# configuration can never leak into a test run.
_STRIPPED_PREFIXES = (
    "OPENORC_",
    "SUPABASE_",
    "VALKEY_",
    "VITE_",
    "NGROK_",
    "LOCAL_",
    "REDIS_STUB_",
    "PY_STUB_",
)


# ---------------------------------------------------------------------------
# Stub executables
# ---------------------------------------------------------------------------

_SUPABASE_STUB = '''#!/usr/bin/env python3
"""Deterministic supabase CLI stub; records argv and emits canned output."""

import json
import os
import sys


def record(argv):
    with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({"tool": "supabase", "argv": argv}) + "\\n")


argv = sys.argv[1:]
record(argv)

if argv[:1] == ["--version"]:
    if os.environ.get("SUPABASE_STUB_VERSION_FAIL"):
        print("stub: version probe failed", file=sys.stderr)
        raise SystemExit(7)
    print(os.environ.get("SUPABASE_STUB_VERSION", "__PIN__"))
    raise SystemExit(0)

if argv[:2] == ["branches", "create"]:
    if os.environ.get("SUPABASE_STUB_CREATE_FAIL"):
        print("stub: branch create failed", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(0)

if argv[:2] == ["branches", "get"]:
    mode = os.environ.get("SUPABASE_STUB_BRANCH_GET", "ok")
    if mode == "fail":
        print("stub: branches get failed", file=sys.stderr)
        raise SystemExit(1)
    if mode == "authfail":
        print("ERROR: Invalid API key (HTTP 401)", file=sys.stderr)
        raise SystemExit(1)
    if mode == "nourl":
        # Branch exists but never publishes database credentials.
        raise SystemExit(0)

    def emit(key, value):
        if value:
            print(f\'{key}="{value}"\')

    emit("SUPABASE_URL", os.environ.get("SUPABASE_STUB_BRANCH_API_URL", ""))
    emit("POSTGRES_URL", os.environ.get("SUPABASE_STUB_BRANCH_POOLER_URL", ""))
    emit("POSTGRES_URL_NON_POOLING", os.environ.get("SUPABASE_STUB_BRANCH_DIRECT_URL", ""))
    emit("SUPABASE_PUBLISHABLE_KEY", os.environ.get("SUPABASE_STUB_PUBLISHABLE_KEY", ""))
    emit("SUPABASE_ANON_KEY", os.environ.get("SUPABASE_STUB_ANON_KEY", ""))
    emit("SUPABASE_DEFAULT_KEY", os.environ.get("SUPABASE_STUB_DEFAULT_KEY", ""))
    emit("SUPABASE_SERVICE_ROLE_KEY", os.environ.get("SUPABASE_STUB_SERVICE_ROLE_KEY", ""))
    raise SystemExit(0)

if argv[:2] == ["migration", "list"]:
    if os.environ.get("SUPABASE_STUB_DB_NOTREADY"):
        raise SystemExit(1)
    raise SystemExit(0)

if argv[:2] == ["branches", "delete"]:
    raise SystemExit(0)

if argv[:2] == ["db", "push"]:
    if os.environ.get("SUPABASE_STUB_SEED_FAIL"):
        print("stub: seed push failed", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(0)

raise SystemExit(0)
'''

_NPM_STUB = """#!/usr/bin/env python3
import json, os, sys

with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps({"tool": "npm", "argv": sys.argv[1:]}) + "\\n")
"""

_BREW_STUB = """#!/usr/bin/env python3
import json, os, sys

with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps({"tool": "brew", "argv": sys.argv[1:]}) + "\\n")
print("stub: brew must never be invoked by devserver.sh", file=sys.stderr)
raise SystemExit(3)
"""

_REDIS_CLI_STUB = """#!/usr/bin/env python3
import json, os, sys

argv = sys.argv[1:]
with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps({"tool": "redis-cli", "argv": argv}) + "\\n")

counter_path = os.environ["REDIS_STUB_PING_COUNTER"]
count = 0
if os.path.exists(counter_path):
    with open(counter_path, encoding="utf-8") as fh:
        raw = fh.read().strip()
        count = int(raw) if raw else 0
count += 1
with open(counter_path, "w", encoding="utf-8") as fh:
    fh.write(str(count))

fail_first = int(os.environ.get("REDIS_STUB_PING_FAIL_FIRST_N", "0"))

if argv[-1:] == ["ping"]:
    if count <= fail_first:
        raise SystemExit(1)
    raise SystemExit(0)

raise SystemExit(0)
"""

_REDIS_SERVER_STUB = """#!/usr/bin/env python3
import json, os, signal, sys, time


def record(argv):
    with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({"tool": "redis-server", "argv": argv}) + "\\n")


record(sys.argv[1:])

if os.environ.get("REDIS_STUB_SERVER_DIES") == "1":
    raise SystemExit(0)


def handler(signum, frame):
    record(["--signal", "TERM"])
    raise SystemExit(0)


signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
time.sleep(120)
"""

_PYTHON_STUB = """#!/usr/bin/env python3
import json, os, signal, sys, time

_ENV_KEYS = (
    "VALKEY_URL",
    "OPENORC_ENV",
    "OPENORC_API_PORT",
    "SUPABASE_URL",
    "SUPABASE_PUBLISHABLE_KEY",
    "SUPABASE_SECRET_KEY",
    "DATABASE_URL",
)

argv = [sys.argv[0], *sys.argv[1:]]
entry = {
    "tool": "python",
    "argv": argv,
    "env": {key: os.environ.get(key, "") for key in _ENV_KEYS},
}
with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(entry) + "\\n")

if os.environ.get("PY_STUB_STAY") != "1":
    raise SystemExit(0)


def handler(signum, frame):
    with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({"tool": "python", "argv": ["--signal", "TERM"]}) + "\\n")
    raise SystemExit(0)


signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
time.sleep(120)
"""

_APPLY_MIGRATIONS_STUB = """#!/usr/bin/env python3
import json, os, sys

entry = {
    "tool": "apply-migrations",
    "argv": sys.argv[1:],
    "env": {
        "OPENORC_SUPABASE_PROJECT_REF": os.environ.get("OPENORC_SUPABASE_PROJECT_REF", ""),
    },
}
with open(os.environ["DEVSERVER_STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(entry) + "\\n")
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def read_calls(log_path: Path) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    calls = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            calls.append(json.loads(line))
    return calls


def calls_of(calls: list[dict[str, Any]], tool: str) -> list[list[str]]:
    return [call["argv"] for call in calls if call["tool"] == tool]


def env_of(calls: list[dict[str, Any]], tool: str) -> list[dict[str, str]]:
    return [call.get("env", {}) for call in calls if call["tool"] == tool]


def owned_valkey_temp_dirs() -> list[Path]:
    """Return temp DIRECTORIES created by devserver-owned queue servers.

    Log files under /tmp/openorc-redis-<pid>.log intentionally survive the
    run (like ngrok logs), so only directories are considered leftover state.
    Tests compare against a pre-run baseline so unrelated /tmp state (for
    example from an interrupted run) cannot influence assertions.
    """
    return [path for path in Path("/tmp").glob("openorc-redis-*") if path.is_dir()]


def supabase_env(**overrides: str) -> dict[str, str]:
    base = {
        "OPENORC_SUPABASE_PROJECT_REF": REF_PARENT,
        "OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION,
        # Small deterministic waits; tests never wait on real provisioning.
        "OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS": "2",
        "OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS": "0",
        "SUPABASE_STUB_BRANCH_API_URL": BRANCH_API_URL,
        "SUPABASE_STUB_BRANCH_POOLER_URL": BRANCH_DB_URL,
        "SUPABASE_STUB_PUBLISHABLE_KEY": PUBLISHABLE_KEY,
        "SUPABASE_STUB_DEFAULT_KEY": DEFAULT_SECRET_KEY,
    }
    base.update(overrides)
    return base


@pytest.fixture
def devserver_harness(tmp_path: Path) -> Any:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "supabase").mkdir()
    (repo / "apps" / "app").mkdir(parents=True)

    shutil.copy2(DEVSERVER_PATH, repo / "scripts" / "devserver.sh")
    shutil.copy2(PIN_PATH, repo / "supabase" / "cli-version")
    _write_executable(repo / "scripts" / "supabase-apply-migrations.sh", _APPLY_MIGRATIONS_STUB)

    venv_bin = repo / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_executable(venv_bin / "python", _PYTHON_STUB)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stubs = {
        "supabase": _SUPABASE_STUB,
        "npm": _NPM_STUB,
        "brew": _BREW_STUB,
        "redis-cli": _REDIS_CLI_STUB,
        "redis-server": _REDIS_SERVER_STUB,
    }
    for name, body in stubs.items():
        # The supabase stub's __PIN__ placeholder must match the repository
        # pin exactly, mirroring the strict-equality version check.
        _write_executable(bin_dir / name, body.replace("__PIN__", _PIN_VERSION))

    log_path = tmp_path / "stub-log.jsonl"

    def run(
        *args: str,
        env: dict[str, str] | None = None,
        env_file: dict[str, str] | None = None,
        timeout: float = 60.0,
    ) -> subprocess.CompletedProcess[str]:
        full_env = {
            key: value
            for key, value in os.environ.items()
            if not any(key.startswith(prefix) for prefix in _STRIPPED_PREFIXES)
        }
        full_env["PATH"] = f"{bin_dir}{os.pathsep}{full_env.get('PATH', os.defpath)}"
        full_env["DEVSERVER_STUB_LOG"] = str(log_path)
        full_env["REDIS_STUB_PING_COUNTER"] = str(tmp_path / "redis-ping-counter")

        if env:
            full_env.update(env)

        if env_file:
            (repo / ".env").write_text(
                "".join(f"{key}={value}\n" for key, value in env_file.items()),
                encoding="utf-8",
            )

        return subprocess.run(
            ["bash", str(repo / "scripts" / "devserver.sh"), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=full_env,
            check=False,
        )

    return SimpleNamespace(run=run, repo=repo, log_path=log_path, tmp_path=tmp_path)


# ---------------------------------------------------------------------------
# Static checks
# ---------------------------------------------------------------------------


def test_bash_n_passes_for_both_scripts() -> None:
    for script in (DEVSERVER_PATH, MIGRATIONS_TOOL_PATH):
        result = subprocess.run(
            ["bash", "-n", str(script)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------


def test_help_exits_zero(devserver_harness: Any) -> None:
    result = devserver_harness.run("--help")

    assert result.returncode == 0
    assert "Usage:" in result.stdout
    assert "--keep-supabase" in result.stdout


def test_unknown_parameter_fails(devserver_harness: Any) -> None:
    result = devserver_harness.run("--bogus")

    assert result.returncode != 0
    assert "Unknown parameter" in result.stderr


def test_missing_parent_ref_fails_closed_before_branch_creation(
    devserver_harness: Any,
) -> None:
    result = devserver_harness.run(env=supabase_env(OPENORC_SUPABASE_PROJECT_REF=""))

    assert result.returncode != 0
    assert "OPENORC_SUPABASE_PROJECT_REF is required" in result.stderr
    assert calls_of(read_calls(devserver_harness.log_path), "supabase") == [["--version"]]


def test_missing_venv_python_fails_with_setup_hint(devserver_harness: Any) -> None:
    (devserver_harness.repo / ".venv" / "bin" / "python").unlink()

    result = devserver_harness.run("--api-only", "--no-supabase")

    assert result.returncode != 0
    assert "uv sync" in result.stderr
    assert calls_of(read_calls(devserver_harness.log_path), "python") == []


# ---------------------------------------------------------------------------
# Queue backend: two independent axes
# ---------------------------------------------------------------------------


def test_preexisting_local_valkey_reused_flushed_never_stopped(
    devserver_harness: Any,
) -> None:
    # Default devserver URL (db index 2) is already responding: the server is
    # externally owned (never stopped) but the selected DB is still flushed.
    valkey_dir_baseline = owned_valkey_temp_dirs()
    result = devserver_harness.run("--worker-only", "--no-supabase")

    assert result.returncode == 0, result.stderr
    calls = read_calls(devserver_harness.log_path)

    assert calls_of(calls, "redis-server") == []
    assert calls_of(calls, "brew") == []
    assert ["-u", DEFAULT_VALKEY_URL, "FLUSHDB"] in calls_of(calls, "redis-cli")

    worker_env = env_of(calls, "python")[0]
    assert worker_env["VALKEY_URL"] == DEFAULT_VALKEY_URL

    assert owned_valkey_temp_dirs() == valkey_dir_baseline


def test_exported_valkey_url_wins_over_devserver_default(devserver_harness: Any) -> None:
    url = "redis://127.0.0.1:7777/5"

    result = devserver_harness.run("--worker-only", "--no-supabase", env={"VALKEY_URL": url})

    assert result.returncode == 0, result.stderr
    calls = read_calls(devserver_harness.log_path)

    assert ["-u", url, "FLUSHDB"] in calls_of(calls, "redis-cli")
    assert env_of(calls, "python")[0]["VALKEY_URL"] == url


def test_devserver_owned_valkey_started_with_persistence_disabled_and_stopped(
    devserver_harness: Any,
) -> None:
    valkey_dir_baseline = owned_valkey_temp_dirs()
    result = devserver_harness.run(
        "--worker-only",
        "--no-supabase",
        env={"REDIS_STUB_PING_FAIL_FIRST_N": "1"},
    )

    assert result.returncode == 0, result.stderr
    calls = read_calls(devserver_harness.log_path)
    server_calls = calls_of(calls, "redis-server")

    assert len(server_calls) == 2  # launch + TERM marker
    launch = server_calls[0]
    port_index = launch.index("--port")
    dir_index = launch.index("--dir")
    assert launch[port_index + 1] == "6379"
    assert "--bind" in launch and "127.0.0.1" in launch
    assert "--save" in launch and "" in launch
    assert "--appendonly" in launch and "no" in launch
    assert launch[dir_index + 1].startswith("/tmp/openorc-redis-")

    # Owned server is stopped (TERM marker) and its temp dir removed.
    assert ["--signal", "TERM"] in server_calls
    assert owned_valkey_temp_dirs() == valkey_dir_baseline

    # Reset still happens for the selected local DB, after readiness.
    flush_calls = [argv for argv in calls_of(calls, "redis-cli") if argv[-1] == "FLUSHDB"]
    assert flush_calls == [["-u", DEFAULT_VALKEY_URL, "FLUSHDB"]]


def test_valkey_port_theft_guard_fails_closed(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        "--worker-only",
        "--no-supabase",
        env={
            "REDIS_STUB_PING_FAIL_FIRST_N": "1",
            "REDIS_STUB_SERVER_DIES": "1",
        },
    )

    assert result.returncode != 0
    assert "another server may already own the port" in result.stderr
    # Never adopted, so never flushed.
    redis_cli_calls = calls_of(read_calls(devserver_harness.log_path), "redis-cli")
    assert all(argv[-1] != "FLUSHDB" for argv in redis_cli_calls)


def test_nonlocal_valkey_url_never_flushed_or_managed(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        "--worker-only",
        "--no-supabase",
        env={"VALKEY_URL": "redis://cache.example.com:6379/2"},
    )

    assert result.returncode == 0, result.stderr
    calls = read_calls(devserver_harness.log_path)

    assert calls_of(calls, "redis-server") == []
    assert calls_of(calls, "redis-cli") == []


def test_no_valkey_disables_start_and_reset(devserver_harness: Any) -> None:
    valkey_dir_baseline = owned_valkey_temp_dirs()
    result = devserver_harness.run("--worker-only", "--no-supabase", "--no-valkey")

    assert result.returncode == 0, result.stderr
    calls = read_calls(devserver_harness.log_path)

    assert calls_of(calls, "redis-server") == []
    assert calls_of(calls, "redis-cli") == []
    assert owned_valkey_temp_dirs() == valkey_dir_baseline


def test_brew_is_never_invoked(devserver_harness: Any) -> None:
    result = devserver_harness.run(env=supabase_env())

    assert result.returncode == 0, result.stderr
    assert calls_of(read_calls(devserver_harness.log_path), "brew") == []


# ---------------------------------------------------------------------------
# Supabase lifecycle
# ---------------------------------------------------------------------------


def test_happy_path_full_lifecycle_and_stop_order(devserver_harness: Any) -> None:
    valkey_dir_baseline = owned_valkey_temp_dirs()
    result = devserver_harness.run(
        env=supabase_env(PY_STUB_STAY="1", REDIS_STUB_PING_FAIL_FIRST_N="1"),
    )

    assert result.returncode == 0, result.stderr
    calls = read_calls(devserver_harness.log_path)

    supabase_calls = calls_of(calls, "supabase")
    assert supabase_calls[0] == ["--version"]
    create_call = supabase_calls[1]
    assert create_call[0] == "branches" and create_call[1] == "create"
    branch_name = create_call[2]
    assert branch_name.startswith("openorc-e2e-")
    assert supabase_calls[2][:2] == ["branches", "get"]
    assert ["migration", "list", "--db-url", BRANCH_DB_URL] in supabase_calls

    # Migration tooling invoked with the created branch and parent ref.
    apply_calls = [c for c in calls if c["tool"] == "apply-migrations"]
    assert apply_calls[0]["argv"] == ["--branch", branch_name]
    assert apply_calls[0]["env"]["OPENORC_SUPABASE_PROJECT_REF"] == REF_PARENT

    # Selected local DB flushed before the worker starts.
    assert ["-u", DEFAULT_VALKEY_URL, "FLUSHDB"] in calls_of(calls, "redis-cli")

    # Real process surfaces launched from the repository venv, identified by
    # entrypoint (record order is nondeterministic: stub interpreters boot
    # asynchronously).
    python_entries = [
        call for call in calls if call["tool"] == "python" and call["argv"] != ["--signal", "TERM"]
    ]
    api_entry = next(
        call for call in python_entries if call["argv"][1].endswith("apps/api/main.py")
    )
    worker_entry = next(
        call for call in python_entries if call["argv"][1].endswith("apps/worker/main.py")
    )
    api_env = api_entry["env"]
    worker_env = worker_entry["env"]
    assert api_entry["argv"][0].endswith(".venv/bin/python")
    assert api_env["OPENORC_API_PORT"] == "3000"
    assert api_env["SUPABASE_URL"] == BRANCH_API_URL
    assert api_env["DATABASE_URL"] == BRANCH_DB_URL
    assert worker_env["VALKEY_URL"] == DEFAULT_VALKEY_URL

    npm_calls = calls_of(calls, "npm")
    assert npm_calls == [["run", "dev", "--", "--host", "0.0.0.0", "--port", "8081"]]

    # Stop order: API/worker stop before the owned queue backend; the branch
    # is deleted last.
    python_term_indices = [
        index
        for index, call in enumerate(calls)
        if call["tool"] == "python" and call["argv"] == ["--signal", "TERM"]
    ]
    redis_term_index = next(
        index
        for index, call in enumerate(calls)
        if call["tool"] == "redis-server" and call["argv"] == ["--signal", "TERM"]
    )
    delete_index = next(
        index
        for index, call in enumerate(calls)
        if call["tool"] == "supabase" and call["argv"][:2] == ["branches", "delete"]
    )

    assert len(python_term_indices) == 2
    assert max(python_term_indices) < redis_term_index
    assert redis_term_index < delete_index
    assert delete_index == len(calls) - 1
    assert supabase_calls[-1][:2] == ["branches", "delete"]

    assert owned_valkey_temp_dirs() == valkey_dir_baseline
    assert calls_of(calls, "brew") == []


def test_branch_create_failure_does_not_attempt_delete(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        env=supabase_env(SUPABASE_STUB_CREATE_FAIL="1"),
    )

    assert result.returncode != 0
    assert "branch creation failed" in result.stderr

    supabase_calls = calls_of(read_calls(devserver_harness.log_path), "supabase")
    assert supabase_calls[0] == ["--version"]
    assert supabase_calls[1][:2] == ["branches", "create"]
    # Failure before readiness: no credential fetch, no delete attempt.
    assert not any(argv[:2] == ["branches", "get"] for argv in supabase_calls)
    assert not any(argv[:2] == ["branches", "delete"] for argv in supabase_calls)


def test_readiness_timeout_fails_and_still_deletes_branch(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        env=supabase_env(SUPABASE_STUB_BRANCH_GET="nourl"),
    )

    assert result.returncode != 0
    assert "did not become ready" in result.stderr

    supabase_calls = calls_of(read_calls(devserver_harness.log_path), "supabase")
    assert supabase_calls[-1][:2] == ["branches", "delete"]


def test_keep_supabase_preserves_branch(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        "--keep-supabase",
        env=supabase_env(),
    )

    assert result.returncode == 0, result.stderr
    supabase_calls = calls_of(read_calls(devserver_harness.log_path), "supabase")
    assert not any(argv[:2] == ["branches", "delete"] for argv in supabase_calls)
    # The keep notice is a warning (stderr) naming the preserved branch.
    assert "Keeping Supabase branch by request" in result.stderr
    assert "DELETE IT MANUALLY" in result.stderr


def test_auth_failure_fails_immediately_without_retry(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        env=supabase_env(SUPABASE_STUB_BRANCH_GET="authfail"),
    )

    assert result.returncode != 0
    assert "authentication/authorization" in result.stderr

    get_calls = [
        argv
        for argv in calls_of(read_calls(devserver_harness.log_path), "supabase")
        if argv[:2] == ["branches", "get"]
    ]
    assert len(get_calls) == 1


def test_new_format_keys_exported(devserver_harness: Any) -> None:
    result = devserver_harness.run("--api-only", env=supabase_env())

    assert result.returncode == 0, result.stderr
    api_env = env_of(read_calls(devserver_harness.log_path), "python")[0]

    assert api_env["SUPABASE_URL"] == BRANCH_API_URL
    assert api_env["SUPABASE_PUBLISHABLE_KEY"] == PUBLISHABLE_KEY
    assert api_env["SUPABASE_SECRET_KEY"] == DEFAULT_SECRET_KEY
    assert api_env["DATABASE_URL"] == BRANCH_DB_URL


def test_legacy_key_fallback_exported(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        "--api-only",
        env=supabase_env(
            SUPABASE_STUB_PUBLISHABLE_KEY="",
            SUPABASE_STUB_DEFAULT_KEY="",
            SUPABASE_STUB_ANON_KEY=ANON_KEY,
            SUPABASE_STUB_SERVICE_ROLE_KEY=SERVICE_ROLE_KEY,
        ),
    )

    assert result.returncode == 0, result.stderr
    api_env = env_of(read_calls(devserver_harness.log_path), "python")[0]

    assert api_env["SUPABASE_PUBLISHABLE_KEY"] == ANON_KEY
    assert api_env["SUPABASE_SECRET_KEY"] == SERVICE_ROLE_KEY


def test_publishable_key_unresolvable_fails_closed(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        env=supabase_env(SUPABASE_STUB_PUBLISHABLE_KEY="", SUPABASE_STUB_ANON_KEY=""),
    )

    assert result.returncode != 0
    assert "neither SUPABASE_PUBLISHABLE_KEY nor SUPABASE_ANON_KEY" in result.stderr

    supabase_calls = calls_of(read_calls(devserver_harness.log_path), "supabase")
    assert supabase_calls[-1][:2] == ["branches", "delete"]


def test_secret_key_unresolvable_fails_closed(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        env=supabase_env(SUPABASE_STUB_DEFAULT_KEY="", SUPABASE_STUB_SERVICE_ROLE_KEY=""),
    )

    assert result.returncode != 0
    assert "neither SUPABASE_DEFAULT_KEY nor SUPABASE_SERVICE_ROLE_KEY" in result.stderr

    supabase_calls = calls_of(read_calls(devserver_harness.log_path), "supabase")
    assert supabase_calls[-1][:2] == ["branches", "delete"]


def test_direct_url_fallback_when_pooler_absent(devserver_harness: Any) -> None:
    result = devserver_harness.run(
        "--api-only",
        env=supabase_env(
            SUPABASE_STUB_BRANCH_POOLER_URL="",
            SUPABASE_STUB_BRANCH_DIRECT_URL=BRANCH_DIRECT_DB_URL,
        ),
    )

    assert result.returncode == 0, result.stderr
    api_env = env_of(read_calls(devserver_harness.log_path), "python")[0]
    assert api_env["DATABASE_URL"] == BRANCH_DIRECT_DB_URL


def test_credentials_exported_to_processes_never_files(devserver_harness: Any) -> None:
    result = devserver_harness.run(env=supabase_env())

    assert result.returncode == 0, result.stderr

    # Never in stdout/stderr (URLs are logged host-only).
    for output in (result.stdout, result.stderr):
        assert PUBLISHABLE_KEY not in output
        assert DEFAULT_SECRET_KEY not in output
        assert "branchsecret" not in output

    # Never written anywhere inside the repository.
    secrets = (PUBLISHABLE_KEY, DEFAULT_SECRET_KEY, "branchsecret")
    for path in devserver_harness.repo.rglob("*"):
        if path.is_file():
            content = path.read_text(encoding="utf-8", errors="ignore")
            for secret in secrets:
                assert secret not in content, f"secret leaked into {path}"


def test_no_supabase_flag_never_invokes_cli(devserver_harness: Any) -> None:
    result = devserver_harness.run("--no-supabase")

    assert result.returncode == 0, result.stderr
    assert calls_of(read_calls(devserver_harness.log_path), "supabase") == []


def test_api_only_foreground_mode(devserver_harness: Any) -> None:
    result = devserver_harness.run("--api-only", "--no-supabase")

    assert result.returncode == 0, result.stderr
    python_calls = [
        call["argv"] for call in read_calls(devserver_harness.log_path) if call["tool"] == "python"
    ]
    assert len(python_calls) == 1
    assert python_calls[0][0].endswith(".venv/bin/python")
    assert python_calls[0][1].endswith("apps/api/main.py")
