"""Deterministic smoke coverage for scripts/supabase-apply-migrations.sh.

The script is exercised against a stub `supabase` CLI executable placed at
the front of PATH (a fake adapter at a bootstrap boundary), so every test is
deterministic and requires no live Supabase infrastructure. The stub records
every argv it receives as JSON lines, letting tests assert both outcomes and
exact CLI invocation shapes.

Each test runs the real script inside a fake repository root (the script plus
the committed supabase/cli-version pin are copied there) so a developer's
local `.env` at the repository root can never influence results.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "supabase-apply-migrations.sh"
PIN_PATH = REPO_ROOT / "supabase" / "cli-version"

_PIN_VERSION = PIN_PATH.read_text(encoding="utf-8").strip()

# 20-character lowercase alphanumeric Supabase project refs.
REF_PARENT = "a" * 20
REF_BRANCH = "b" * 20
REF_PRODUCTION = "c" * 20

BRANCH_DB_URL = f"postgresql://postgres.{REF_BRANCH}:branchsecret@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
BRANCH_DIRECT_DB_URL = f"postgresql://postgres:branchsecret@db.{REF_BRANCH}.supabase.co:5432/postgres"
BRANCH_API_URL = f"https://{REF_BRANCH}.supabase.co"
BRANCH_SECRET_TOKEN = "supatest-access-token"

BRANCH_ENV_PREFIXES = ("OPENORC_", "SUPABASE_")

_STUB_TEMPLATE = '''#!/usr/bin/env python3
"""Deterministic supabase CLI stub; records argv and emits canned output."""

import json
import os
import sys

argv = sys.argv[1:]
with open(os.environ["SUPABASE_STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(argv) + "\\n")

if argv[:1] == ["--version"]:
    if os.environ.get("SUPABASE_STUB_VERSION_FAIL"):
        print("stub: version probe failed", file=sys.stderr)
        raise SystemExit(7)
    print(os.environ.get("SUPABASE_STUB_VERSION", "__PIN__"))
    raise SystemExit(0)

if argv[:2] == ["branches", "get"]:
    mode = os.environ.get("SUPABASE_STUB_BRANCH_GET", "ok")
    if mode == "fail":
        print("stub: branches get failed", file=sys.stderr)
        raise SystemExit(1)
    if mode == "authfail":
        # Simulated CLI-unauthenticated/unauthorized state for the branch
        # operation itself.
        print("ERROR: Invalid API key (HTTP 401)", file=sys.stderr)
        raise SystemExit(1)
    if mode == "nourl":
        # Emulate a branch without published database credentials
        # (not ready yet, or the production/main branch).
        raise SystemExit(0)
    api_url = os.environ.get("SUPABASE_STUB_BRANCH_API_URL", "")
    db_url = os.environ.get("SUPABASE_STUB_BRANCH_POSTGRES_URL", "")
    pooler_url = os.environ.get("SUPABASE_STUB_BRANCH_POOLER_URL", "")
    if api_url:
        print(f'SUPABASE_URL="{api_url}"')
    if pooler_url:
        print(f'POSTGRES_URL="{pooler_url}"')
    if db_url:
        print(f'POSTGRES_URL_NON_POOLING="{db_url}"')
    raise SystemExit(0)

if argv[:2] == ["projects", "list"]:
    if os.environ.get("SUPABASE_STUB_AUTH_FAIL"):
        print("stub: not authenticated", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(0)

if argv[:2] == ["migration", "list"]:
    if os.environ.get("SUPABASE_STUB_DB_NOTREADY"):
        print("stub: database not ready", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(0)

if argv[:2] == ["db", "push"]:
    raise SystemExit(0)

print(f"stub: unexpected argv: {argv}", file=sys.stderr)
raise SystemExit(3)
'''.replace("__PIN__", _PIN_VERSION)


@pytest.fixture(name="run_tool")
def run_tool_fixture(tmp_path: Path) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Run scripts/supabase-apply-migrations.sh against the stub CLI.

    The script is executed inside a fake repository root assembled under
    tmp_path so neither the developer's real `.env` nor ambient OPENORC_*/
    SUPABASE_* environment values can leak into a test.
    """

    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    stub = stub_bin / "supabase"
    stub.write_text(_STUB_TEMPLATE, encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)

    log_path = tmp_path / "stub-calls.jsonl"

    def build_fake_repo(*, with_migration: bool) -> Path:
        fake_root = tmp_path / "repo"
        if fake_root.exists():
            shutil.rmtree(fake_root)
        (fake_root / "scripts").mkdir(parents=True)
        supabase_dir = fake_root / "supabase"
        (supabase_dir / "migrations").mkdir(parents=True)
        shutil.copy(SCRIPT_PATH, fake_root / "scripts" / SCRIPT_PATH.name)
        shutil.copy(PIN_PATH, supabase_dir / "cli-version")
        if with_migration:
            (supabase_dir / "migrations" / "20260101000000_probe.sql").write_text(
                "select 1;\n", encoding="utf-8"
            )
        return fake_root

    def run(
        *args: str,
        env: dict[str, str] | None = None,
        env_file: dict[str, str] | None = None,
        with_migration: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        fake_root = build_fake_repo(with_migration=with_migration)

        if env_file is not None:
            lines = [f"{key}={value}" for key, value in env_file.items()]
            (fake_root / ".env").write_text("\n".join(lines) + "\n", encoding="utf-8")

        merged = os.environ.copy()
        for name in list(merged):
            if name.startswith(BRANCH_ENV_PREFIXES):
                del merged[name]

        merged["PATH"] = f"{stub_bin}{os.pathsep}{merged.get('PATH', '')}"
        merged["SUPABASE_STUB_LOG"] = str(log_path)
        merged.setdefault("SUPABASE_STUB_VERSION", _PIN_VERSION)
        if env:
            merged.update(env)

        return subprocess.run(
            [str(fake_root / "scripts" / SCRIPT_PATH.name), *args],
            env=merged,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    return run


def read_calls(tmp_path: Path) -> list[list[str]]:
    """Return every argv the stub received, in order."""
    log_path = tmp_path / "stub-calls.jsonl"
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]


def branch_env(**overrides: str) -> dict[str, str]:
    """Standard environment for --branch mode tests."""
    base: dict[str, str] = {
        "OPENORC_SUPABASE_PROJECT_REF": REF_PARENT,
        "OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION,
        "SUPABASE_ACCESS_TOKEN": BRANCH_SECRET_TOKEN,
        "SUPABASE_STUB_BRANCH_POSTGRES_URL": BRANCH_DIRECT_DB_URL,
        "SUPABASE_STUB_BRANCH_POOLER_URL": BRANCH_DB_URL,
        "SUPABASE_STUB_BRANCH_API_URL": BRANCH_API_URL,
    }
    base.update(overrides)
    return base


BRANCH_ARGS = ["--branch", "e2e"]


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------


def test_help_exits_zero(run_tool: Callable[..., subprocess.CompletedProcess[str]]) -> None:
    result = run_tool("--help")

    assert result.returncode == 0
    assert "Usage:" in result.stdout


def test_no_target_fails_closed(run_tool: Callable[..., subprocess.CompletedProcess[str]]) -> None:
    result = run_tool()

    assert result.returncode != 0
    assert "No target specified" in result.stderr


def test_conflicting_targets_rejected(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        BRANCH_ARGS[0],
        BRANCH_ARGS[1],
        "--db-url",
        "postgresql://u:p@127.0.0.1:54322/postgres",
        env=branch_env(),
    )

    assert result.returncode != 0
    assert "exactly one target" in result.stderr
    assert read_calls(tmp_path) == []


def test_unknown_parameter_rejected(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool("--bogus")

    assert result.returncode != 0
    assert "Unknown parameter" in result.stderr
    assert read_calls(tmp_path) == []


def test_version_mismatch_refused_before_any_target_operation(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        *BRANCH_ARGS,
        env={**branch_env(), "SUPABASE_STUB_VERSION": "2.116.0"},
        with_migration=True,
    )

    assert result.returncode != 0
    assert "version mismatch" in result.stderr
    # The pin is exact: no target resolution or write may happen on mismatch.
    assert read_calls(tmp_path) == [["--version"]]


def test_pin_older_installed_version_also_refused(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        *BRANCH_ARGS,
        env={**branch_env(), "SUPABASE_STUB_VERSION": "2.118.0"},
        with_migration=True,
    )

    assert result.returncode != 0
    assert "version mismatch" in result.stderr
    assert read_calls(tmp_path) == [["--version"]]


def test_prerelease_version_identity_rejected(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    # Regression: a prerelease suffix is part of the reported version
    # identity. The check must not truncate it (e.g. 2.117.0-beta.1 ->
    # 2.117.0) into a false match against the pinned release.
    result = run_tool(
        *BRANCH_ARGS,
        env={**branch_env(), "SUPABASE_STUB_VERSION": "2.117.0-beta.1"},
        with_migration=True,
    )

    assert result.returncode != 0
    assert "version mismatch" in result.stderr
    assert "2.117.0-beta.1" in result.stderr
    assert read_calls(tmp_path) == [["--version"]]


def test_version_probe_failure_fails_closed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        *BRANCH_ARGS,
        env={**branch_env(), "SUPABASE_STUB_VERSION_FAIL": "1"},
        with_migration=True,
    )

    assert result.returncode != 0
    assert "exited with a non-zero status" in result.stderr
    assert read_calls(tmp_path) == [["--version"]]


def test_whitespace_around_reported_version_is_normalized(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    # Only harmless whitespace/prefix normalization is allowed; the pinned
    # identity itself must still match exactly.
    result = run_tool(
        *BRANCH_ARGS,
        env={**branch_env(), "SUPABASE_STUB_VERSION": "  2.117.0  "},
        with_migration=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Migrations applied successfully." in result.stdout


# ---------------------------------------------------------------------------
# Fail-closed target selection
# ---------------------------------------------------------------------------


def test_branch_mode_with_cli_authenticated_state_proceeds_without_exported_token(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    # Simulated CLI-authenticated state: the stub CLI accepts the branch
    # operation, and no SUPABASE_ACCESS_TOKEN is exported. Authentication is
    # delegated to the CLI, so the tooling must proceed and delegate.
    env = branch_env()
    del env["SUPABASE_ACCESS_TOKEN"]

    result = run_tool(*BRANCH_ARGS, env=env, with_migration=True)

    assert result.returncode == 0, result.stderr
    assert "Migrations applied successfully." in result.stdout
    calls = read_calls(tmp_path)
    assert ["branches", "get", "e2e", "--project-ref", REF_PARENT, "-o", "env"] in calls


def test_branch_mode_with_cli_unauthenticated_state_fails_immediately(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    # Simulated CLI-unauthenticated/unauthorized state: the branch operation
    # itself is rejected. The tooling must fail immediately with a useful
    # diagnostic instead of retrying through the whole wait budget.
    env = branch_env(SUPABASE_STUB_BRANCH_GET="authfail")
    del env["SUPABASE_ACCESS_TOKEN"]

    result = run_tool(*BRANCH_ARGS, env=env)

    assert result.returncode != 0
    assert "authentication/authorization" in result.stderr
    assert "Invalid API key" in result.stderr
    # Fail immediately: exactly one branch lookup, no retries, no writes.
    assert read_calls(tmp_path) == [
        ["--version"],
        ["branches", "get", "e2e", "--project-ref", REF_PARENT, "-o", "env"],
    ]


def test_branch_mode_without_parent_ref_fails_closed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    env = branch_env()
    del env["OPENORC_SUPABASE_PROJECT_REF"]

    result = run_tool(*BRANCH_ARGS, env=env)

    assert result.returncode != 0
    assert "OPENORC_SUPABASE_PROJECT_REF" in result.stderr
    assert read_calls(tmp_path) == [["--version"]]


def test_branch_main_refused_without_cli_calls(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool("--branch", "main", env=branch_env())

    assert result.returncode != 0
    assert "production branch identity" in result.stderr
    assert read_calls(tmp_path) == [["--version"]]


def test_branch_resolved_production_identity_refused(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    env = branch_env(
        SUPABASE_STUB_BRANCH_POSTGRES_URL=(
            f"postgresql://postgres:prodsecret@db.{REF_PRODUCTION}.supabase.co:5432/postgres"
        ),
        SUPABASE_STUB_BRANCH_API_URL=f"https://{REF_PRODUCTION}.supabase.co",
    )

    result = run_tool(*BRANCH_ARGS, env=env, with_migration=True)

    assert result.returncode != 0
    assert "configured production project" in result.stderr
    assert [call[:2] for call in read_calls(tmp_path)] == [
        ["--version"],
        ["branches", "get"],
        ["migration", "list"],
    ]
    assert "db push" not in result.stdout


def test_branch_parent_equals_production_warns_but_proceeds(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    # Branching parents may legitimately be the production project; reads
    # resolve the branch while writes still target the branch database only.
    env = branch_env(OPENORC_SUPABASE_PRODUCTION_PROJECT_REF=REF_PARENT)

    result = run_tool(*BRANCH_ARGS, env=env, with_migration=True)

    assert result.returncode == 0
    assert "parent project equals the configured production project" in result.stderr
    assert "Migrations applied successfully." in result.stdout


def test_branch_wait_timeout_fails_closed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    env = branch_env(
        SUPABASE_STUB_BRANCH_GET="nourl",
        OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS="2",
        OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS="0",
    )

    result = run_tool(*BRANCH_ARGS, env=env)

    assert result.returncode != 0
    assert "did not become ready" in result.stderr
    assert "credentials not published yet" in result.stderr
    assert not any(call[:2] == ["db", "push"] for call in read_calls(tmp_path))


def test_branch_database_not_ready_times_out(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    # Regression from live E2E: hosted branches publish credentials BEFORE
    # their database host resolves. Credential presence alone must not be
    # treated as readiness; the wait must poll until the database answers.
    env = branch_env(
        SUPABASE_STUB_DB_NOTREADY="1",
        OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS="2",
        OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS="0",
    )

    result = run_tool(*BRANCH_ARGS, env=env, with_migration=True)

    assert result.returncode != 0
    assert "did not become ready" in result.stderr
    assert "database not answering yet" in result.stderr
    assert not any(call[:2] == ["db", "push"] for call in read_calls(tmp_path))


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_branch_mode_happy_path_applies_migrations(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(*BRANCH_ARGS, env=branch_env(), with_migration=True)

    assert result.returncode == 0
    assert "Migrations applied successfully." in result.stdout

    calls = read_calls(tmp_path)
    assert calls == [
        ["--version"],
        ["branches", "get", "e2e", "--project-ref", REF_PARENT, "-o", "env"],
        ["migration", "list", "--db-url", BRANCH_DB_URL],
        ["db", "push", "--db-url", BRANCH_DB_URL],
    ]
    # Strict push: no history-drift masking flag.
    assert not any("--include-all" in call for call in calls)
    # Pooler preference (live-verified): when both URLs are published, the
    # pooler URL is the apply target even though the direct URL was offered.
    assert calls[-1] == ["db", "push", "--db-url", BRANCH_DB_URL]
    # Generated credentials never reach stdout/stderr.
    assert "branchsecret" not in result.stdout
    assert "branchsecret" not in result.stderr
    assert BRANCH_SECRET_TOKEN not in result.stdout


def test_branch_mode_falls_back_to_direct_url_when_pooler_absent(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    env = branch_env(SUPABASE_STUB_BRANCH_POOLER_URL="")

    result = run_tool(*BRANCH_ARGS, env=env, with_migration=True)

    assert result.returncode == 0, result.stderr
    calls = read_calls(tmp_path)
    assert ["migration", "list", "--db-url", BRANCH_DIRECT_DB_URL] in calls
    assert calls[-1] == ["db", "push", "--db-url", BRANCH_DIRECT_DB_URL]


def test_dry_run_forwarded_to_db_push(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        "--dry-run",
        "--db-url",
        "postgresql://u:p@127.0.0.1:54322/postgres",
        env={},
        with_migration=True,
    )

    assert result.returncode == 0
    assert "Dry run complete; nothing was applied." in result.stdout
    calls = read_calls(tmp_path)
    assert calls[-1] == [
        "db",
        "push",
        "--db-url",
        "postgresql://u:p@127.0.0.1:54322/postgres",
        "--dry-run",
    ]
    assert result.stdout.count("u:p") == 0


def test_empty_migrations_is_clean_noop(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool("--db-url", "postgresql://u:p@127.0.0.1:54322/postgres", env={})

    assert result.returncode == 0
    assert "nothing to apply" in result.stdout
    assert not any(call[:2] == ["db", "push"] for call in read_calls(tmp_path))


# ---------------------------------------------------------------------------
# --db-url mode: provable non-production only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("host", ["127.0.0.1", "127.0.0.99", "localhost"])
def test_db_url_loopback_allowed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]],
    tmp_path: Path,
    host: str,
) -> None:
    url = f"postgresql://postgres:localpw@{host}:54322/postgres"

    result = run_tool("--db-url", url, env={}, with_migration=True)

    assert result.returncode == 0, result.stderr
    assert "Migrations applied successfully." in result.stdout
    assert read_calls(tmp_path)[-1] == ["db", "push", "--db-url", url]
    assert "localpw" not in result.stdout


def test_db_url_remote_ref_differs_from_production_allowed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    url = (
        f"postgresql://postgres.{REF_BRANCH}:branchsecret"
        f"@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
    )

    result = run_tool(
        "--db-url",
        url,
        env={"OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION},
        with_migration=True,
    )

    assert result.returncode == 0, result.stderr
    assert f"Target project ref {REF_BRANCH} verified as non-production." in result.stdout
    assert "branchsecret" not in result.stdout


def test_db_url_production_ref_refused(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    url = f"postgresql://postgres:pw@db.{REF_PRODUCTION}.supabase.co:5432/postgres"

    result = run_tool(
        "--db-url",
        url,
        env={"OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION},
        with_migration=True,
    )

    assert result.returncode != 0
    assert "embeds the configured production project ref" in result.stderr
    assert not any(call[:2] == ["db", "push"] for call in read_calls(tmp_path))


def test_db_url_without_production_guard_fails_closed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        "--db-url",
        f"postgresql://postgres:pw@db.{REF_BRANCH}.supabase.co:5432/postgres",
        env={},
    )

    assert result.returncode != 0
    assert "cannot be proven non-production" in result.stderr
    assert read_calls(tmp_path) == [["--version"]]


def test_db_url_unextractable_ref_fails_closed(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        "--db-url",
        "postgresql://u:p@db.internal.example.com:5432/openorc",
        env={"OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION},
    )

    assert result.returncode != 0
    assert "could not be reliably extracted" in result.stderr
    assert not any(call[:2] == ["db", "push"] for call in read_calls(tmp_path))


# ---------------------------------------------------------------------------
# Environment file loading (same semantics as devserver.sh)
# ---------------------------------------------------------------------------


def test_env_file_supplies_branch_configuration(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        *BRANCH_ARGS,
        env_file={
            "OPENORC_SUPABASE_PROJECT_REF": REF_PARENT,
            "SUPABASE_ACCESS_TOKEN": BRANCH_SECRET_TOKEN,
            "OPENORC_SUPABASE_PRODUCTION_PROJECT_REF": REF_PRODUCTION,
        },
        env={
            "SUPABASE_STUB_BRANCH_POSTGRES_URL": BRANCH_DB_URL,
            "SUPABASE_STUB_BRANCH_API_URL": BRANCH_API_URL,
        },
        with_migration=True,
    )

    assert result.returncode == 0, result.stderr
    calls = read_calls(tmp_path)
    assert calls[1] == ["branches", "get", "e2e", "--project-ref", REF_PARENT, "-o", "env"]


def test_exported_environment_wins_over_env_file(
    run_tool: Callable[..., subprocess.CompletedProcess[str]], tmp_path: Path
) -> None:
    result = run_tool(
        *BRANCH_ARGS,
        env_file={
            "OPENORC_SUPABASE_PROJECT_REF": REF_PRODUCTION,
            "SUPABASE_ACCESS_TOKEN": "from-file",
        },
        env=branch_env(),
        with_migration=True,
    )

    assert result.returncode == 0
    calls = read_calls(tmp_path)
    assert ["branches", "get", "e2e", "--project-ref", REF_PARENT, "-o", "env"] in calls
