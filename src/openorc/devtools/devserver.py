"""Manual local E2E devserver orchestration for OpenOrc.

This module implements the orchestration behind ``scripts/devserver.sh``: the
stable human-facing command stays a tiny Bash wrapper, while the orchestration
lives here so it is importable and deterministically testable.

IMPORTANT:
- This is Owner/developer manual E2E tooling. Agents do not run the real
  hosted flow during ordinary implementation work.
- Ordinary tests substitute the process-runner and Supabase command
  boundaries at the Python level; they never build fake executable trees.

Responsibilities (issue #10 semantics):

- ephemeral Supabase preview branch lifecycle (create, bounded readiness
  wait, credential resolution, delegated migrations, optional seed, and
  deletion of only the branch created by this invocation);
- local Redis-compatible queue backend ownership (start/stop only what this
  process owns, never ``brew services``) and guarded logical-DB reset
  (flush only the explicitly selected localhost-shaped development DB);
- process supervision of the API, RQ worker, Vue dev server, and optional
  ngrok tunnel with dependency-aware teardown;
- signal handling: SIGINT/SIGTERM only request shutdown; exactly one
  idempotent cleanup path performs all teardown (signal/error/natural exit
  -> one cleanup sequence).

Secrets are never logged; URLs are logged host-oriented (scheme + host).
"""

from __future__ import annotations

import contextlib
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, TextIO

import redis

LOG_PREFIX = "[devserver]"

DEFAULT_APP_PORT = 8081
DEFAULT_API_PORT = 3000

# OpenOrc queue naming is defined in src/openorc/workers/queues.py (openorc:
# prefix; canonical default queue openorc:default). The devserver local
# default is Redis DB index 2, the recommended local namespace (see
# .env.example); an exported VALKEY_URL always wins.
DEFAULT_VALKEY_URL = "redis://127.0.0.1:6379/2"

LOCAL_VALKEY_URL_RE = re.compile(r"^redis://(127\.0\.0\.1|localhost):([0-9]+)/([0-9]+)$")

BRANCH_NAME_PREFIX = "openorc-e2e"
BRANCH_WAIT_MAX_ATTEMPTS_ENV = "OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS"
BRANCH_WAIT_SLEEP_SECONDS_ENV = "OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS"
BRANCH_WAIT_MAX_ATTEMPTS_DEFAULT = 60
BRANCH_WAIT_SLEEP_SECONDS_DEFAULT = 5.0

VALKEY_READY_ATTEMPTS = 20
VALKEY_READY_SLEEP_SECONDS = 0.5
OWNERSHIP_GUARD_ATTEMPTS = 10
OWNERSHIP_GUARD_SLEEP_SECONDS = 0.1

STOP_TERM_TIMEOUT_SECONDS = 5.0
STOP_POLL_INTERVAL_SECONDS = 0.1
FOREGROUND_POLL_SECONDS = 0.2

NGROK_LOG_PATH = Path("/tmp/openorc-ngrok.log")


class DevserverError(Exception):
    """Fatal devserver condition; the orchestrator fails closed."""


class UsageError(DevserverError):
    """Invalid command-line usage."""


class HelpRequested(Exception):
    """Raised when -h/--help is requested."""


class ShutdownRequested(Exception):
    """Raised in the main flow when a signal has requested shutdown.

    Signal handlers never clean up; they only record the request. The main
    flow converts the recorded request into this exception (or a foreground
    loop break) so the single finally-based cleanup path runs exactly once.
    """

    def __init__(self, signum: int) -> None:
        super().__init__(f"Shutdown requested by signal {signum}")
        self.signum = signum


class Logger(Protocol):
    """Logging boundary; implementations must never emit secret values."""

    def log(self, message: str) -> None: ...

    def warn(self, message: str) -> None: ...

    def error(self, message: str) -> None: ...


class StreamLogger:
    """Print-based logger writing prefixed lines to stdout/stderr."""

    def __init__(self, stdout: TextIO | None = None, stderr: TextIO | None = None) -> None:
        self._stdout = stdout if stdout is not None else sys.stdout
        self._stderr = stderr if stderr is not None else sys.stderr

    def log(self, message: str) -> None:
        print(f"{LOG_PREFIX} {message}", file=self._stdout, flush=True)

    def warn(self, message: str) -> None:
        print(f"{LOG_PREFIX} WARNING: {message}", file=self._stderr, flush=True)

    def error(self, message: str) -> None:
        print(f"{LOG_PREFIX} ERROR: {message}", file=self._stderr, flush=True)


class QueueClient(Protocol):
    """Minimal Redis-compatible client boundary (readiness + guarded flush)."""

    def ping(self) -> bool: ...

    def flushdb(self) -> None: ...


class ChildProcess(Protocol):
    """A long-lived child process owned by the orchestrator."""

    @property
    def pid(self) -> int: ...

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def signal(self, signum: int) -> None: ...


@dataclass(frozen=True)
class RunResult:
    """Result of a completed short-lived command."""

    returncode: int
    stdout: str
    stderr: str


class ProcessRunner(Protocol):
    """Boundary for all child-process interaction.

    ``run`` executes a short-lived command (Supabase CLI, the migration
    tooling); ``spawn`` starts a long-lived child (API, worker, app, ngrok,
    devserver-owned queue server). Tests substitute this boundary instead of
    constructing fake executable trees.
    """

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        capture: bool = True,
    ) -> RunResult: ...

    def spawn(
        self,
        label: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        log_path: Path | None = None,
        inherit_stdin: bool = False,
    ) -> ChildProcess: ...


# Resolves a command name to an executable path (or None). Real execution
# uses shutil.which; deterministic tests inject a fake so the suite never
# depends on host-installed developer tools (supabase, npm, redis-server,
# ngrok).
CommandResolver = Callable[[str], str | None]


# ---------------------------------------------------------------------------
# Redaction and URL helpers
#
# Credentials and tokens are never logged; URLs are logged host-only.
# ---------------------------------------------------------------------------

_URL_WITH_CREDENTIALS_RE = re.compile(r"((?:postgres(?:ql)?|https?)://[^:/@\s]+):[^@\s]+@")
_URL_SCHEMES = {"postgres", "postgresql", "redis", "rediss", "https", "http"}


def sanitize_cli_error(text: str, env: Mapping[str, str]) -> str:
    """Redact the access token and URL credentials from CLI diagnostics."""
    token = env.get("SUPABASE_ACCESS_TOKEN")
    if token:
        text = text.replace(token, "<redacted-token>")
    return _URL_WITH_CREDENTIALS_RE.sub(r"\1:<redacted>@", text)


def redact_url(url: str) -> str:
    """Return a credential-free, host-oriented form of a URL."""
    scheme, separator, rest = url.partition("://")
    if not separator or scheme not in _URL_SCHEMES:
        return "<unrecognized-url>"
    if "@" in rest:
        rest = rest.rpartition("@")[2]
    host = re.split(r"[/?:]", rest, maxsplit=1)[0]
    host = host.removeprefix("[").removesuffix("]")
    if not host:
        return "<unrecognized-url>"
    return f"{scheme}://{host}"


def is_local_valkey_url(url: str) -> bool:
    """True for the localhost-shaped queue URLs the logical-DB guard covers."""
    return LOCAL_VALKEY_URL_RE.match(url) is not None


# ---------------------------------------------------------------------------
# Command-line configuration
# ---------------------------------------------------------------------------


@dataclass
class RunConfig:
    """Selected surfaces and automation toggles for one devserver run."""

    include_app: bool = True
    include_api: bool = True
    include_worker: bool = True
    auto_valkey: bool = True
    auto_ngrok: bool = True
    auto_supabase: bool = True
    keep_supabase: bool = False

    @property
    def foreground_kind(self) -> str | None:
        """The single surface that runs in the foreground (app > api > worker)."""
        if self.include_app:
            return "app"
        if self.include_api:
            return "api"
        if self.include_worker:
            return "worker"
        return None


USAGE = """Usage: ./devserver.sh [options]

Default:
  Start the local OpenOrc manual E2E stack:
    - ephemeral hosted Supabase branch (current-checkout migrations)
    - local Redis-compatible queue backend (default db index 2)
    - local API
    - local worker
    - local Vue app (foreground)

Options:
  --app-only
      Start only the Vue app.
      Supabase/API/worker/Valkey are not started automatically.

  --api-only
      Start only the API (foreground).
      Supabase is prepared unless --no-supabase is also supplied.

  --worker-only
      Start only the worker (foreground).

  --no-worker
      Do not start worker processes.

  --no-valkey
      Do not auto-start or reset the local queue backend.

  --no-ngrok
      Do not start ngrok.

  --no-supabase
      Do not create an ephemeral Supabase branch.
      Useful only when explicitly pointing the stack at another safe
      non-production environment.

  --keep-supabase
      Debug escape hatch: do not delete the ephemeral Supabase branch on exit.

      This should NOT be the normal workflow.
      Leaving branches running incurs compute cost.

  -h, --help
      Show this help.

Notes:
  - The default Supabase environment is ephemeral; the branch created by a run
    is deleted on exit (including on failure) unless --keep-supabase is given.
  - The production Supabase project must never be used as a development target.
  - A queue server already responding at VALKEY_URL is reused and left running
    on exit; the selected local DB (default redis://127.0.0.1:6379/2) is still
    flushed for a clean start. Non-local URLs are never flushed or managed.
  - A devserver-started queue server runs redis-server directly with
    persistence disabled and is stopped on exit (never via brew services).
  - API/worker processes run from the repository .venv (uv sync); there is no
    PATH python3 fallback.
  - Cline is not expected to run this script during normal implementation.
"""


def parse_args(argv: Sequence[str]) -> RunConfig:
    """Parse devserver arguments into a RunConfig.

    Raises HelpRequested for -h/--help and UsageError for unknown options.
    """
    config = RunConfig()
    for arg in argv:
        if arg == "--app-only":
            config.include_app = True
            config.include_api = False
            config.include_worker = False
            config.auto_valkey = False
            config.auto_ngrok = False
            config.auto_supabase = False
        elif arg == "--api-only":
            config.include_app = False
            config.include_api = True
            config.include_worker = False
            config.auto_valkey = False
        elif arg == "--worker-only":
            config.include_app = False
            config.include_api = False
            config.include_worker = True
            config.auto_ngrok = False
        elif arg == "--no-worker":
            config.include_worker = False
        elif arg == "--no-valkey":
            config.auto_valkey = False
        elif arg == "--no-ngrok":
            config.auto_ngrok = False
        elif arg == "--no-supabase":
            config.auto_supabase = False
        elif arg == "--keep-supabase":
            config.keep_supabase = True
        elif arg in {"-h", "--help"}:
            raise HelpRequested
        else:
            raise UsageError(f"Unknown parameter: {arg}")
    return config


# ---------------------------------------------------------------------------
# Environment file
#
# Optional local configuration file: .env at the repository root (git-ignored).
# Blank lines and # comments are skipped, keys must be valid shell identifiers,
# one surrounding quote pair is stripped from values, and variables already
# exported in the calling shell always win over .env.
# ---------------------------------------------------------------------------

_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def load_env_file(env_file: Path, env: MutableMapping[str, str], log: Logger) -> None:
    """Load the optional root .env into env; exported values always win."""
    if not env_file.is_file():
        log.log("No .env file found; using exported environment and defaults.")
        return

    log.log(f"Loading environment file: {env_file}")
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.lstrip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not _ENV_KEY_RE.match(key):
            log.warn(f"Ignoring invalid .env line: {key}")
            continue
        if key not in env:
            env[key] = _strip_quotes(value)


def _env_int(env: Mapping[str, str], key: str, default: int, log: Logger) -> int:
    raw = env.get(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        log.warn(f"Ignoring non-integer {key}: {raw}")
        return default


def _env_float(env: Mapping[str, str], key: str, default: float, log: Logger) -> float:
    raw = env.get(key)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        log.warn(f"Ignoring non-numeric {key}: {raw}")
        return default


# ---------------------------------------------------------------------------
# Supabase branch environment
#
# Pinned CLI v2.117.0 may expose modern or legacy API key names. The OpenOrc
# runtime contract (see .env.example) always requires SUPABASE_PUBLISHABLE_KEY
# and SUPABASE_SECRET_KEY:
#   - publishable: SUPABASE_PUBLISHABLE_KEY, else legacy SUPABASE_ANON_KEY;
#   - secret: SUPABASE_DEFAULT_KEY, else legacy SUPABASE_SERVICE_ROLE_KEY.
# The CLI never emits a key literally named SUPABASE_SECRET_KEY.
# ---------------------------------------------------------------------------


class _BranchFetchStatus(Enum):
    OK = "ok"
    CLI_FAILED = "cli-failed"
    CREDENTIALS_NOT_PUBLISHED = "credentials-not-published"


@dataclass(frozen=True)
class BranchEnvironment:
    """Parsed, key-normalized output of `supabase branches get -o env`."""

    api_url: str = ""
    pooler_url: str = ""
    direct_url: str = ""
    publishable_key: str = ""
    secret_key: str = ""

    @property
    def database_credentials_published(self) -> bool:
        return bool(self.pooler_url or self.direct_url)

    @property
    def database_url(self) -> str:
        """Pooler URL preferred over the direct (non-pooling) URL."""
        return self.pooler_url or self.direct_url

    @property
    def runtime_overrides(self) -> dict[str, str]:
        return {
            "SUPABASE_URL": self.api_url,
            "DATABASE_URL": self.database_url,
            "SUPABASE_PUBLISHABLE_KEY": self.publishable_key,
            "SUPABASE_SECRET_KEY": self.secret_key,
        }


def parse_branch_environment(raw: str) -> BranchEnvironment:
    """Parse `supabase branches get -o env` output, normalizing API key names."""
    values: dict[str, str] = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = _strip_quotes(value)

    return BranchEnvironment(
        api_url=values.get("SUPABASE_URL", ""),
        pooler_url=values.get("POSTGRES_URL", ""),
        direct_url=values.get("POSTGRES_URL_NON_POOLING", ""),
        publishable_key=values.get("SUPABASE_PUBLISHABLE_KEY")
        or values.get("SUPABASE_ANON_KEY", ""),
        secret_key=values.get("SUPABASE_DEFAULT_KEY")
        or values.get("SUPABASE_SERVICE_ROLE_KEY", ""),
    )


# ---------------------------------------------------------------------------
# Supabase branch lifecycle
#
# The branch created by THIS invocation is the only branch this orchestrator
# is allowed to delete automatically. Credential values are resolved into
# child-process environments and are never logged or written to files.
# ---------------------------------------------------------------------------


def _make_branch_name(env: Mapping[str, str]) -> str:
    """Unique per invocation so two devserver runs never share a database."""
    user_part = re.sub(r"[^a-z0-9-]+", "-", env.get("USER", "owner").lower()) or "owner"
    timestamp = time.strftime("%Y%m%d%H%M%S")
    return f"{BRANCH_NAME_PREFIX}-{user_part}-{timestamp}-{os.getpid()}"


class SupabaseManager:
    """Ephemeral preview-branch lifecycle for one devserver invocation."""

    _AUTH_ERROR_RE = re.compile(
        r"401|403|unauthorized|forbidden|invalid access token|invalid api key"
        r"|not logged in|access token|api key|permission"
    )

    def __init__(
        self,
        *,
        runner: ProcessRunner,
        root: Path,
        env: Mapping[str, str],
        log: Logger,
        sleep: Callable[[float], None],
        max_attempts: int,
        sleep_seconds: float,
    ) -> None:
        self._runner = runner
        self._root = root
        self._env = env
        self._log = log
        self._sleep = sleep
        self._max_attempts = max_attempts
        self._sleep_seconds = sleep_seconds
        self._branch_name: str | None = None
        self._created = False
        self._branch_environment: BranchEnvironment | None = None

    def _parent_ref(self) -> str:
        parent_ref = self._env.get("OPENORC_SUPABASE_PROJECT_REF", "")
        if not parent_ref:
            raise DevserverError("OPENORC_SUPABASE_PROJECT_REF is required for ephemeral Supabase.")
        return parent_ref

    def _require_branch(self) -> str:
        if self._branch_name is None:
            raise DevserverError("No Supabase branch was created by this invocation.")
        return self._branch_name

    def verify_configuration(self) -> None:
        """Fail closed without a parent ref; warn when the parent is production."""
        parent_ref = self._parent_ref()
        production_ref = self._env.get("OPENORC_SUPABASE_PRODUCTION_PROJECT_REF", "")
        if production_ref and parent_ref == production_ref:
            self._log.warn("Branch parent project equals the configured production project ref.")
            self._log.warn(
                "Expected when preview branches are hosted on the production project; "
                "writes still target only the branch database."
            )

    def verify_cli_pin(self) -> None:
        """Enforce strict equality with the supabase/cli-version repository pin."""
        pin_path = self._root / "supabase" / "cli-version"
        if not pin_path.is_file():
            raise DevserverError("CLI version pin file missing: supabase/cli-version")
        pinned = "".join(pin_path.read_text(encoding="utf-8").split())
        if not pinned:
            raise DevserverError("CLI version pin file is empty: supabase/cli-version")

        result = self._runner.run(["supabase", "--version"])
        if result.returncode != 0:
            raise DevserverError(
                "supabase --version exited with a non-zero status; cannot verify the "
                "installed CLI version."
            )

        normalized = result.stdout.strip()
        if normalized.lower().startswith("supabase "):
            normalized = normalized[len("supabase ") :].strip()
        normalized = normalized.removeprefix("v")

        semver_re = r"^[0-9]+(\.[0-9]+)+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$"
        if not normalized or re.match(semver_re, normalized) is None:
            raise DevserverError(
                "Could not determine the installed Supabase CLI version from unexpected "
                f"--version output: {result.stdout.strip() or '<empty>'}."
            )
        if normalized != pinned:
            raise DevserverError(
                f"Supabase CLI version mismatch: installed {normalized}, repository pin {pinned}. "
                "Align your Supabase CLI with the repository pin (see docs/supabase-migrations.md)."
            )
        self._log.log(f"Supabase CLI version OK: {normalized}")

    @property
    def branch_name(self) -> str | None:
        return self._branch_name

    def create_branch(self) -> str:
        """Create the unique ephemeral branch; records it for later deletion."""
        name = _make_branch_name(self._env)
        parent_ref = self._parent_ref()
        self._log.log(f"Creating ephemeral Supabase branch: {name}")
        result = self._runner.run(
            ["supabase", "branches", "create", name, "--project-ref", parent_ref]
        )
        if result.returncode != 0:
            raise DevserverError(f"Supabase branch creation failed for '{name}'.")
        self._branch_name = name
        self._created = True
        return name

    def _fetch_branch_environment(self) -> tuple[_BranchFetchStatus, BranchEnvironment, str]:
        """Fetch branch env output; statuses distinguish retry from failure."""
        branch_name = self._require_branch()
        result = self._runner.run(
            [
                "supabase",
                "branches",
                "get",
                branch_name,
                "--project-ref",
                self._parent_ref(),
                "-o",
                "env",
            ]
        )
        if result.returncode != 0:
            stderr = sanitize_cli_error(result.stderr, self._env)
            return _BranchFetchStatus.CLI_FAILED, BranchEnvironment(), stderr
        branch_environment = parse_branch_environment(result.stdout)
        if not branch_environment.database_credentials_published:
            return _BranchFetchStatus.CREDENTIALS_NOT_PUBLISHED, branch_environment, ""
        return _BranchFetchStatus.OK, branch_environment, ""

    def wait_until_ready(self) -> BranchEnvironment:
        """Bounded wait for credentials publication AND a database that answers.

        Creating the branch does not mean Postgres/Auth/API are immediately
        usable. Hosted branches publish credentials before their database
        host resolves, so readiness requires BOTH signals. The production/
        main branch never publishes database credentials through the API, so
        it can never pass this gate. Authentication/authorization failures
        fail immediately instead of retrying.
        """
        branch_name = self._require_branch()
        self._log.log(f"Waiting for Supabase branch '{branch_name}' to become ready...")
        reason = ""
        for attempt in range(1, self._max_attempts + 1):
            status, branch_environment, stderr = self._fetch_branch_environment()
            if status is _BranchFetchStatus.OK:
                probe = self._runner.run(
                    ["supabase", "migration", "list", "--db-url", branch_environment.database_url]
                )
                if probe.returncode == 0:
                    self._log.log("Supabase branch is ready.")
                    self._branch_environment = branch_environment
                    return branch_environment
                reason = "database not answering yet"
            elif status is _BranchFetchStatus.CREDENTIALS_NOT_PUBLISHED:
                reason = "database credentials not published yet"
            else:
                if self._AUTH_ERROR_RE.search(stderr.lower()):
                    raise DevserverError(
                        f"Supabase rejected the branch operation for '{branch_name}' "
                        f"(authentication/authorization): {stderr or '<no diagnostic>'}\n"
                        "Check that SUPABASE_ACCESS_TOKEN (or your stored CLI login) is valid "
                        "and authorized for the parent project."
                    )
                reason = "branch lookup failed transiently"

            if attempt < self._max_attempts:
                self._log.log(
                    f"Branch '{branch_name}' not ready ({reason}; attempt {attempt}/"
                    f"{self._max_attempts}); waiting {self._sleep_seconds:g}s..."
                )
                self._sleep(self._sleep_seconds)

        raise DevserverError(
            f"Supabase branch '{branch_name}' did not become ready within {self._max_attempts} "
            f"attempts x {self._sleep_seconds:g}s (last status: {reason}). It may still be "
            "provisioning, or it may be the production/main branch (whose database credentials "
            "are never retrievable)."
        )

    def export_runtime_credentials(self, env: MutableMapping[str, str]) -> None:
        """Export resolved branch credentials into the child-process environment.

        Values are never written to .env or any tracked/generated file.
        """
        branch_environment = self._branch_environment
        if branch_environment is None:
            raise DevserverError(
                "Supabase branch credentials were not resolved for this invocation."
            )
        if not branch_environment.api_url:
            raise DevserverError(
                "Supabase branch did not publish an API URL; refusing to start the stack "
                "against an unresolved target."
            )
        if not branch_environment.database_credentials_published:
            raise DevserverError(
                "Supabase branch did not publish database credentials; refusing to start the stack."
            )
        if not branch_environment.publishable_key:
            raise DevserverError(
                "Supabase branch API keys could not be resolved: neither SUPABASE_PUBLISHABLE_KEY "
                "nor SUPABASE_ANON_KEY was present in the branch output."
            )
        if not branch_environment.secret_key:
            raise DevserverError(
                "Supabase branch API keys could not be resolved: neither SUPABASE_DEFAULT_KEY "
                "nor SUPABASE_SERVICE_ROLE_KEY was present in the branch output."
            )
        env.update(branch_environment.runtime_overrides)
        database_host = redact_url(branch_environment.database_url)
        self._log.log(
            f"Branch credentials exported for this process tree "
            f"(API URL: {branch_environment.api_url}, database: {database_host})."
        )

    def apply_migrations(self) -> None:
        """Apply current-checkout migrations through the supported tooling.

        The migration tool owns the strict db push, production identity
        guards, main-branch refusal, and fail-hard behavior; branch
        lifecycle remains this manager's responsibility.
        """
        branch_name = self._require_branch()
        self._log.log(
            f"Applying current repository migrations to Supabase branch '{branch_name}'..."
        )
        result = self._runner.run(
            [str(self._root / "scripts" / "supabase-apply-migrations.sh"), "--branch", branch_name],
            capture=False,
        )
        if result.returncode != 0:
            raise DevserverError(
                f"Supabase migration application failed for branch '{branch_name}'; refusing to "
                "start the stack against a partially migrated target."
            )

    def apply_seed_if_present(self) -> None:
        """Apply representative non-production seed data when seed.sql exists."""
        if not (self._root / "supabase" / "seed.sql").is_file():
            self._log.log("No supabase/seed.sql present; skipping seed.")
            return
        branch_environment = self._branch_environment
        if branch_environment is None or not branch_environment.database_url:
            raise DevserverError(
                "Supabase branch database URL is not resolved; cannot apply seed data."
            )
        self._log.log("Applying Supabase development seed data...")
        # --include-seed applies the seed after the migration-history check;
        # migrations were already applied, so this is a migration no-op plus
        # the seed. Fails hard on error (migration-first policy).
        result = self._runner.run(
            [
                "supabase",
                "db",
                "push",
                "--db-url",
                branch_environment.database_url,
                "--include-seed",
            ],
            capture=False,
        )
        if result.returncode != 0:
            raise DevserverError(
                "Seed data did not apply cleanly; failing hard (migration-first policy)."
            )

    def delete_branch_if_created(self, *, keep: bool) -> None:
        """Delete only the branch created by this invocation; noisy on failure.

        Forgotten branches cost money, so deletion failure is surfaced as a
        loud warning identifying the branch requiring manual cleanup.
        """
        if not self._created or self._branch_name is None:
            return
        if keep:
            self._log.warn(
                f"Keeping Supabase branch by request (--keep-supabase): {self._branch_name}"
            )
            self._log.warn("DELETE IT MANUALLY when finished to avoid continued compute charges.")
            return
        self._log.log(f"Deleting ephemeral Supabase branch: {self._branch_name}")
        result = self._runner.run(
            [
                "supabase",
                "branches",
                "delete",
                self._branch_name,
                "--project-ref",
                self._parent_ref(),
            ],
            capture=False,
        )
        if result.returncode != 0:
            self._log.warn(f"Failed to delete Supabase branch: {self._branch_name}")
            self._log.warn("DELETE IT MANUALLY to avoid continued compute charges.")
            return
        self._created = False


# ---------------------------------------------------------------------------
# Real process runner and child process
#
# Children start in their own session (start_new_session=True), so the child
# PID is the process-group ID and a group-wide signal reaches the whole child
# tree (for example npm and its Vite child). Terminal Ctrl-C reaches only the
# orchestrator, which then performs the single controlled cleanup.
# ---------------------------------------------------------------------------


class PopenChildProcess:
    """ChildProcess implementation over subprocess.Popen."""

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self._process = process

    @property
    def pid(self) -> int:
        return self._process.pid

    def poll(self) -> int | None:
        return self._process.poll()

    def wait(self, timeout: float | None = None) -> int:
        try:
            return self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            returncode = self._process.returncode
            return returncode if returncode is not None else -1

    def signal(self, signum: int) -> None:
        # The process tree may already be gone; poll()/wait() reaps it.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(self._process.pid, signum)


class SubprocessRunner:
    """Real ProcessRunner over the subprocess module."""

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        capture: bool = True,
    ) -> RunResult:
        completed = subprocess.run(
            list(argv),
            env=dict(env) if env is not None else None,
            cwd=str(cwd) if cwd is not None else None,
            stdin=subprocess.DEVNULL,
            capture_output=capture,
            text=capture,
            check=False,
        )
        stdout = completed.stdout if isinstance(completed.stdout, str) else ""
        stderr = completed.stderr if isinstance(completed.stderr, str) else ""
        return RunResult(returncode=completed.returncode, stdout=stdout, stderr=stderr)

    def spawn(
        self,
        label: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        log_path: Path | None = None,
        inherit_stdin: bool = False,
    ) -> ChildProcess:
        output_handle = None
        stdout: int | None = None
        stderr: int | None = None
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            output_handle = log_path.open("ab")
            stdout = output_handle.fileno()
            stderr = subprocess.STDOUT
        try:
            process = subprocess.Popen(
                list(argv),
                env=dict(env) if env is not None else None,
                cwd=str(cwd) if cwd is not None else None,
                stdin=None if inherit_stdin else subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
        finally:
            if output_handle is not None:
                output_handle.close()
        return PopenChildProcess(process)


class RedisQueueClient:
    """QueueClient adapter over the pinned redis client."""

    def __init__(self, url: str) -> None:
        self._client = redis.Redis.from_url(url, socket_connect_timeout=2.0, socket_timeout=2.0)

    def ping(self) -> bool:
        return bool(self._client.ping())

    def flushdb(self) -> None:
        self._client.flushdb()


def default_queue_client_factory(url: str) -> QueueClient:
    return RedisQueueClient(url)


def stop_child(
    log: Logger,
    label: str,
    child: ChildProcess,
    *,
    sleep: Callable[[float], None],
    term_timeout_seconds: float = STOP_TERM_TIMEOUT_SECONDS,
    poll_interval_seconds: float = STOP_POLL_INTERVAL_SECONDS,
) -> None:
    """Stop one child: TERM, bounded wait, escalate to KILL; always reap."""
    if child.poll() is None:
        log.log(f"Stopping {label} (PID {child.pid})...")
        child.signal(signal.SIGTERM)
        for _ in range(int(term_timeout_seconds / poll_interval_seconds)):
            if child.poll() is not None:
                break
            sleep(poll_interval_seconds)
        if child.poll() is None:
            log.warn(f"{label} (PID {child.pid}) did not stop gracefully; forcing.")
            child.signal(signal.SIGKILL)
    # Reap the child so nothing lingers as a zombie.
    child.wait()


@dataclass
class _ManagedProcess:
    label: str
    child: ChildProcess


class ProcessSupervisor:
    """Registry of started children; stops them in reverse start order."""

    def __init__(
        self,
        log: Logger,
        *,
        sleep: Callable[[float], None],
        stop_term_timeout_seconds: float = STOP_TERM_TIMEOUT_SECONDS,
    ) -> None:
        self._log = log
        self._sleep = sleep
        self._stop_term_timeout_seconds = stop_term_timeout_seconds
        self._managed: list[_ManagedProcess] = []

    def start(
        self,
        label: str,
        argv: Sequence[str],
        *,
        runner: ProcessRunner,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        log_path: Path | None = None,
        inherit_stdin: bool = False,
    ) -> ChildProcess:
        child = runner.spawn(
            label, argv, env=env, cwd=cwd, log_path=log_path, inherit_stdin=inherit_stdin
        )
        self._managed.append(_ManagedProcess(label, child))
        return child

    def stop_all_reverse(self) -> None:
        """Stop children newest-first so dependents die before dependencies.

        Registration order is queue-server (owned), API, worker, ngrok,
        foreground service; reverse order stops the foreground service first
        and the queue server last, with the worker fully reaped (RQ teardown)
        before the queue backend is ever touched.
        """
        for managed in reversed(self._managed):
            try:
                stop_child(
                    self._log,
                    managed.label,
                    managed.child,
                    sleep=self._sleep,
                    term_timeout_seconds=self._stop_term_timeout_seconds,
                )
            except Exception as error:
                # One stuck child must not block stopping the remaining ones.
                self._log.warn(f"Failed to stop {managed.label} (PID {managed.child.pid}): {error}")
        self._managed.clear()


# ---------------------------------------------------------------------------
# Local queue backend (Redis-compatible)
#
# Two independent safety questions:
#
# 1. Process ownership (may I stop this server?):
#    - a server already responding at the configured URL is externally owned;
#      it is used as-is and never stopped on cleanup;
#    - a server started by this orchestrator runs the redis-server binary
#      directly as an ephemeral child with persistence disabled and is
#      stopped on cleanup. NEVER `brew services start redis/valkey`: that
#      installs a persistent Homebrew LaunchAgent which outlives the
#      devserver. No Homebrew mutation is ever performed.
#
# 2. Logical DB ownership (may I reset this selected development DB?):
#    the explicitly localhost-shaped VALKEY_URL namespace (default
#    redis://127.0.0.1:6379/2) is flushed before the worker starts in both
#    externally-owned and devserver-owned cases. Non-local URLs are never
#    flushed and never managed. The flush guard is based on the URL, never
#    on server-process ownership.
# ---------------------------------------------------------------------------


class ValkeyManager:
    """Queue backend ownership plus guarded logical-DB reset."""

    def __init__(
        self,
        url: str,
        *,
        runner: ProcessRunner,
        log: Logger,
        sleep: Callable[[float], None],
        queue_client_factory: Callable[[str], QueueClient],
        tmp_root: Path,
        command_resolver: CommandResolver = shutil.which,
    ) -> None:
        self._url = url
        self._runner = runner
        self._log = log
        self._sleep = sleep
        self._queue_client_factory = queue_client_factory
        self._tmp_root = tmp_root
        self._command_resolver = command_resolver
        self._owned_child: ChildProcess | None = None
        self._owned_tmp_dir: Path | None = None

    @property
    def url(self) -> str:
        return self._url

    def _client(self) -> QueueClient:
        return self._queue_client_factory(self._url)

    def _responding(self) -> bool:
        try:
            self._client().ping()
        except Exception:  # any transport/timeout failure means not responding
            return False
        return True

    def ensure(self) -> None:
        """Reuse a responding server or start a devserver-owned one."""
        if not is_local_valkey_url(self._url):
            self._log.log(
                f"Using non-local queue backend from environment: {redact_url(self._url)}"
            )
            return
        if self._responding():
            self._log.log(
                f"Queue backend already responding at {self._url} "
                "(externally owned; devserver will not stop it on exit)"
            )
            return
        self._start_owned()

    def _start_owned(self) -> None:
        if self._command_resolver("redis-server") is None:
            raise DevserverError(
                f"No queue backend is responding at {self._url} and redis-server is not on "
                "PATH. Start a local Redis-compatible server or point VALKEY_URL at a "
                "running instance."
            )
        match = LOCAL_VALKEY_URL_RE.match(self._url)
        if match is None:  # unreachable: ensure() gates on the local URL shape
            raise DevserverError(
                f"Queue backend URL is not a supported local URL: {redact_url(self._url)}"
            )
        port = match.group(2)

        # Invocation-specific paths outside the repository keep concurrent
        # devserver runs independent and leave no repo residue.
        tmp_dir = self._tmp_root / f"openorc-redis-{os.getpid()}"
        log_path = self._tmp_root / f"openorc-redis-{os.getpid()}.log"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        self._log.log(
            f"Starting devserver-owned queue backend (redis-server) on 127.0.0.1:{port}..."
        )
        self._log.log(f"Queue backend logs: {log_path}")
        child = self._runner.spawn(
            "queue-backend",
            [
                "redis-server",
                "--port",
                port,
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                str(tmp_dir),
            ],
            log_path=log_path,
        )
        # Record ownership immediately: if the ownership guard or the
        # readiness wait fails while the child is still alive, the canonical
        # cleanup path must still stop/reap this child and remove this temp
        # dir (startup failure after branch creation still invokes cleanup).
        self._owned_child = child
        self._owned_tmp_dir = tmp_dir

        # Ownership sanity guard: if a foreign server grabbed the port during
        # startup, our instance exits (bind conflict) within milliseconds
        # while the foreign instance answers the ping. Fail loudly instead of
        # silently adopting (and later killing) someone else's server.
        for _ in range(OWNERSHIP_GUARD_ATTEMPTS):
            if child.poll() is not None:
                raise DevserverError(
                    f"Queue backend answered at {self._url} but the devserver-started instance "
                    "exited; another server may already own the port. Devserver will not "
                    f"adopt it; see {log_path}"
                )
            self._sleep(OWNERSHIP_GUARD_SLEEP_SECONDS)

        for _ in range(VALKEY_READY_ATTEMPTS):
            if self._responding():
                self._log.log(
                    f"Queue backend started (devserver-owned PID {child.pid}; "
                    "stopped automatically on exit)"
                )
                return
            if child.poll() is not None:
                raise DevserverError(
                    f"devserver-started queue backend exited before becoming ready at "
                    f"{self._url}; see {log_path}"
                )
            self._sleep(VALKEY_READY_SLEEP_SECONDS)

        raise DevserverError(
            f"devserver-started queue backend did not become ready at {self._url}; see {log_path}"
        )

    def flush_local_db(self) -> None:
        """Flush the explicitly selected local development DB (guarded)."""
        if not is_local_valkey_url(self._url):
            return
        if not self._responding():
            raise DevserverError(
                f"Cannot flush local queue DB because the queue backend is not responding "
                f"at {self._url}."
            )
        self._log.log(f"Flushing local OpenOrc queue DB: {self._url}")
        self._client().flushdb()

    def stop_owned(self) -> None:
        """Stop only the server this invocation started; external servers untouched."""
        if self._owned_child is None:
            return
        child = self._owned_child
        self._owned_child = None
        stop_child(self._log, "queue-backend", child, sleep=self._sleep)

    def remove_temp_dir(self) -> None:
        """Remove this invocation's temp dir (only when devserver created it)."""
        tmp_dir = self._owned_tmp_dir
        if tmp_dir is None:
            return
        self._owned_tmp_dir = None
        if tmp_dir.parent != self._tmp_root or not tmp_dir.name.startswith("openorc-redis-"):
            self._log.warn(f"Refusing to remove unexpected queue temp dir: {tmp_dir}")
            return
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Orchestrator
#
# Exactly one cleanup path exists: run() installs signal handlers that only
# REQUEST shutdown, executes the stack, and performs the entire teardown once
# in a finally block — whether the run ends naturally, with an error, or via
# a shutdown signal. Handlers never terminate children, stop Redis, or delete
# branches themselves.
#
# Cleanup order (dependencies, not registration order):
#   1. app/API/worker/ngrok children (reverse start order: the foreground
#      service stops first and the worker is fully reaped — RQ teardown —
#      while the queue backend is still alive);
#   2. the devserver-owned queue server, then this invocation's temp dir;
#   3. the ephemeral Supabase branch, last.
# ---------------------------------------------------------------------------


class Devserver:
    """Single-run orchestrator wiring Supabase, queue, and process lifecycle."""

    def __init__(
        self,
        *,
        config: RunConfig,
        root: Path,
        env: MutableMapping[str, str] | None = None,
        runner: ProcessRunner | None = None,
        log: Logger | None = None,
        queue_client_factory: Callable[[str], QueueClient] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        tmp_root: Path | None = None,
        stop_term_timeout_seconds: float = STOP_TERM_TIMEOUT_SECONDS,
        command_resolver: CommandResolver | None = None,
    ) -> None:
        self._config = config
        self._root = root
        self._env: MutableMapping[str, str] = env if env is not None else os.environ
        self._log: Logger = log if log is not None else StreamLogger()
        self._runner: ProcessRunner = runner if runner is not None else SubprocessRunner()
        self._queue_client_factory = (
            queue_client_factory
            if queue_client_factory is not None
            else default_queue_client_factory
        )
        self._sleep = sleep
        self._tmp_root = tmp_root if tmp_root is not None else Path(tempfile.gettempdir())
        self._command_resolver: CommandResolver = (
            command_resolver if command_resolver is not None else shutil.which
        )
        self._supervisor = ProcessSupervisor(
            self._log, sleep=self._sleep, stop_term_timeout_seconds=stop_term_timeout_seconds
        )
        self._supabase: SupabaseManager | None = None
        self._valkey: ValkeyManager | None = None
        self._shutdown_signum: int | None = None
        self._cleanup_done = False
        self._previous_signal_handlers: dict[int, Any] = {}

    # -- lifecycle ---------------------------------------------------------

    def run(self) -> int:
        """Run the configured stack; return the process exit code."""
        self._install_signal_handlers()
        try:
            return self._execute()
        except ShutdownRequested as requested:
            return 128 + requested.signum
        except DevserverError as error:
            self._log.error(str(error))
            return 1
        finally:
            self._cleanup_once()

    def request_shutdown(self, signum: int) -> None:
        """Record a shutdown request; the main flow performs all cleanup."""
        if self._shutdown_signum is None:
            self._shutdown_signum = signum

    def _on_signal(self, signum: int, frame: object) -> None:
        self.request_shutdown(signum)

    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            # Tests may drive the orchestrator from a worker thread; signal
            # handlers can only be installed on the main thread. Shutdown can
            # still be requested directly via request_shutdown().
            return
        for signum in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(OSError, ValueError):
                self._previous_signal_handlers[signum] = signal.signal(signum, self._on_signal)

    def _ignore_signals_for_cleanup(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for signum in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(OSError, ValueError):
                signal.signal(signum, signal.SIG_IGN)

    def _restore_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for signum, handler in self._previous_signal_handlers.items():
            with contextlib.suppress(OSError, ValueError, TypeError):
                signal.signal(signum, handler)
        self._previous_signal_handlers.clear()

    def _check_shutdown(self) -> None:
        if self._shutdown_signum is not None:
            raise ShutdownRequested(self._shutdown_signum)

    def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep in slices, converting a shutdown request into an exception.

        Signal handlers only record the request; long startup waits observe
        it here in the main flow so shutdown stays prompt and the single
        finally-based cleanup path runs.
        """
        remaining = seconds
        while remaining > 0:
            self._check_shutdown()
            step = min(remaining, 0.25)
            self._sleep(step)
            remaining -= step
        self._check_shutdown()

    def _cleanup_once(self) -> None:
        """The single, idempotent cleanup path (see module ordering notes).

        Stages are best-effort: a failure in one stage is logged and must
        never short-circuit later stages, so the invocation-owned Supabase
        branch deletion is always attempted last and no cleanup error masks
        the original run result.
        """
        if self._cleanup_done:
            return
        self._cleanup_done = True
        self._ignore_signals_for_cleanup()
        try:
            self._attempt_cleanup_stage(
                "application-child shutdown", self._supervisor.stop_all_reverse
            )
            valkey = self._valkey
            if valkey is not None:
                self._attempt_cleanup_stage("owned queue-backend shutdown", valkey.stop_owned)
                self._attempt_cleanup_stage("queue temp cleanup", valkey.remove_temp_dir)
            supabase = self._supabase
            if supabase is not None:
                keep = self._config.keep_supabase

                def delete_branch() -> None:
                    supabase.delete_branch_if_created(keep=keep)

                # Always attempted last; deletion failure is reported by the
                # manager as a loud warning identifying the branch.
                self._attempt_cleanup_stage("supabase branch deletion", delete_branch)
        finally:
            self._restore_signal_handlers()

    def _attempt_cleanup_stage(self, stage: str, action: Callable[[], None]) -> None:
        """Run one cleanup stage; log its failure without blocking later stages."""
        try:
            action()
        except Exception as error:  # best-effort: keep reaching later stages
            self._log.warn(f"Cleanup stage '{stage}' failed: {error}")

    # -- execution ---------------------------------------------------------

    def _execute(self) -> int:
        self._require_base_tools()
        load_env_file(self._root / ".env", self._env, self._log)
        api_port = self._env_value("LOCAL_API_PORT", str(DEFAULT_API_PORT))
        app_port = self._env_value("LOCAL_APP_PORT", str(DEFAULT_APP_PORT))

        self._prepare_supabase()
        self._prepare_valkey()
        self._export_frontend_defaults(api_port)
        self._start_background_services(api_port)
        return self._run_foreground(app_port, api_port)

    def _env_value(self, key: str, default: str) -> str:
        value = self._env.get(key)
        return value if value else default

    def _require_base_tools(self) -> None:
        config = self._config
        if config.auto_supabase and self._command_resolver("supabase") is None:
            raise DevserverError("Required command not found on PATH: supabase")
        if config.include_app and self._command_resolver("npm") is None:
            raise DevserverError("Required command not found on PATH: npm")
        if config.include_api or config.include_worker:
            python_path = self._root / ".venv" / "bin" / "python"
            if not (python_path.is_file() and os.access(python_path, os.X_OK)):
                # The canonical local Python environment is the repository
                # .venv created from the committed uv contract. Never fall
                # back to an arbitrary system python3.
                raise DevserverError(
                    ".venv/bin/python not found. Run 'uv sync' to create the repository "
                    "virtual environment (see README)."
                )

    def _prepare_supabase(self) -> None:
        if not self._config.auto_supabase:
            self._log.log("Supabase automation disabled.")
            return
        manager = SupabaseManager(
            runner=self._runner,
            root=self._root,
            env=self._env,
            log=self._log,
            sleep=self._interruptible_sleep,
            max_attempts=_env_int(
                self._env,
                BRANCH_WAIT_MAX_ATTEMPTS_ENV,
                BRANCH_WAIT_MAX_ATTEMPTS_DEFAULT,
                self._log,
            ),
            sleep_seconds=_env_float(
                self._env,
                BRANCH_WAIT_SLEEP_SECONDS_ENV,
                BRANCH_WAIT_SLEEP_SECONDS_DEFAULT,
                self._log,
            ),
        )
        self._supabase = manager
        manager.verify_configuration()
        manager.verify_cli_pin()
        manager.create_branch()
        manager.wait_until_ready()
        manager.export_runtime_credentials(self._env)
        manager.apply_migrations()
        manager.apply_seed_if_present()

    def _prepare_valkey(self) -> None:
        if not self._config.include_worker:
            return
        valkey_url = self._env_value("VALKEY_URL", DEFAULT_VALKEY_URL)
        manager = ValkeyManager(
            valkey_url,
            runner=self._runner,
            log=self._log,
            sleep=self._sleep,
            queue_client_factory=self._queue_client_factory,
            tmp_root=self._tmp_root,
            command_resolver=self._command_resolver,
        )
        self._valkey = manager
        if not self._config.auto_valkey:
            self._log.log(f"Auto-Valkey disabled. Expecting queue backend at: {valkey_url}")
        else:
            manager.ensure()
            manager.flush_local_db()
        # Export the resolved URL for the whole child tree (API/worker).
        self._env["VALKEY_URL"] = valkey_url

    def _export_frontend_defaults(self, api_port: str) -> None:
        if not self._config.include_app:
            return
        if self._env.get("VITE_API_BASE_URL"):
            return
        self._env["VITE_API_BASE_URL"] = f"http://127.0.0.1:{api_port}"
        self._log.log(f"Exported VITE_API_BASE_URL={self._env['VITE_API_BASE_URL']}")

    def _service_env(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        merged = dict(self._env)
        merged.setdefault("OPENORC_ENV", "development")
        if extra:
            merged.update(extra)
        return merged

    def _venv_python(self) -> Path:
        return self._root / ".venv" / "bin" / "python"

    def _start_background_services(self, api_port: str) -> None:
        config = self._config
        if config.include_api and config.include_app:
            self._log.log(f"Starting OpenOrc API in background (127.0.0.1:{api_port})...")
            self._supervisor.start(
                "api",
                [str(self._venv_python()), str(self._root / "apps" / "api" / "main.py")],
                runner=self._runner,
                env=self._service_env({"OPENORC_API_PORT": api_port}),
            )
        if config.include_worker and config.include_app:
            self._log.log("Starting OpenOrc worker in background...")
            self._supervisor.start(
                "worker",
                [str(self._venv_python()), str(self._root / "apps" / "worker" / "main.py")],
                runner=self._runner,
                env=self._service_env(),
            )
        if config.include_api:
            self._start_ngrok_if_configured(api_port)

    def _start_ngrok_if_configured(self, api_port: str) -> None:
        if not self._config.auto_ngrok:
            return
        reserved_url = self._env.get("NGROK_RESERVED_URL", "")
        if not reserved_url:
            self._log.log("NGROK_RESERVED_URL not set; skipping ngrok startup.")
            return
        if self._command_resolver("ngrok") is None:
            self._log.warn("ngrok not found on PATH; skipping tunnel startup.")
            return
        self._log.log(f"Starting ngrok tunnel for API: {reserved_url}")
        self._log.log(f"ngrok logs: {NGROK_LOG_PATH}")
        self._supervisor.start(
            "ngrok",
            ["ngrok", "http", f"--url={reserved_url}", api_port],
            runner=self._runner,
            log_path=NGROK_LOG_PATH,
        )

    def _run_foreground(self, app_port: str, api_port: str) -> int:
        kind = self._config.foreground_kind
        if kind is None:
            raise DevserverError("Nothing selected to run.")

        if kind == "app":
            self._log.log(f"Starting OpenOrc app in foreground (127.0.0.1:{app_port})...")
            child = self._supervisor.start(
                "app",
                ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", app_port],
                runner=self._runner,
                cwd=self._root / "apps" / "app",
                env=dict(self._env),
                inherit_stdin=True,
            )
        elif kind == "api":
            self._log.log(f"Running OpenOrc API in foreground (127.0.0.1:{api_port})...")
            child = self._supervisor.start(
                "api",
                [str(self._venv_python()), str(self._root / "apps" / "api" / "main.py")],
                runner=self._runner,
                env=self._service_env({"OPENORC_API_PORT": api_port}),
                inherit_stdin=True,
            )
        else:
            self._log.log("Running OpenOrc worker in foreground...")
            child = self._supervisor.start(
                "worker",
                [str(self._venv_python()), str(self._root / "apps" / "worker" / "main.py")],
                runner=self._runner,
                env=self._service_env(),
                inherit_stdin=True,
            )

        while child.poll() is None:
            self._check_shutdown()
            self._sleep(FOREGROUND_POLL_SECONDS)
        return child.wait()


def main(argv: Sequence[str] | None = None, root: Path | None = None) -> int:
    """Console entrypoint used by scripts/devserver.sh via scripts/devserver.py."""
    repo_root = root if root is not None else Path(__file__).resolve().parents[3]
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        config = parse_args(arguments)
    except HelpRequested:
        print(USAGE, end="")
        return 0
    except UsageError as usage_error:
        print(f"{LOG_PREFIX} ERROR: {usage_error}", file=sys.stderr)
        return 1

    return Devserver(config=config, root=repo_root).run()
