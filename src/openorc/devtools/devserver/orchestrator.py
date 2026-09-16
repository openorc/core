"""High-level devserver lifecycle orchestration.

Exactly one cleanup path exists: run() installs signal handlers that only
REQUEST shutdown, executes the stack, and performs the entire teardown once
in a finally block — whether the run ends naturally, with an error, or via
a shutdown signal.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any

from openorc.devtools.devserver.cli import USAGE, RunConfig, parse_args
from openorc.devtools.devserver.environment import _env_float, _env_int, load_env_file
from openorc.devtools.devserver.errors import (
    DevserverError,
    HelpRequested,
    ShutdownRequested,
    UsageError,
)
from openorc.devtools.devserver.logging import LOG_PREFIX, Logger, StreamLogger, redact_url
from openorc.devtools.devserver.processes import (
    STOP_TERM_TIMEOUT_SECONDS,
    CommandResolver,
    ProcessRunner,
    ProcessSupervisor,
    SubprocessRunner,
)
from openorc.devtools.devserver.supabase import (
    BRANCH_WAIT_MAX_ATTEMPTS_DEFAULT,
    BRANCH_WAIT_MAX_ATTEMPTS_ENV,
    BRANCH_WAIT_SLEEP_SECONDS_DEFAULT,
    BRANCH_WAIT_SLEEP_SECONDS_ENV,
    BranchEnvironment,
    SupabaseManager,
)
from openorc.devtools.devserver.valkey import (
    DEFAULT_VALKEY_URL,
    QueueClient,
    ValkeyManager,
    default_queue_client_factory,
)

DEFAULT_APP_PORT = 8081
DEFAULT_API_PORT = 3000
FOREGROUND_POLL_SECONDS = 0.2

NGROK_LOG_PATH = Path("/tmp/openorc-ngrok.log")


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
        if self._config.testdb_command is not None:
            return self._execute_testdb()
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
        manager = self._build_supabase_manager()
        self._provision_supabase_branch(manager)
        manager.export_runtime_credentials(self._env)
        manager.apply_migrations()
        manager.apply_seed_if_present()

    def _build_supabase_manager(self) -> SupabaseManager:
        """Construct the branch manager; recorded for the single cleanup path."""
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
        return manager

    def _provision_supabase_branch(self, manager: SupabaseManager) -> BranchEnvironment:
        """Run the canonical branch lifecycle through bounded readiness.

        Shared by ordinary stack startup and --testdb. Credentials are
        resolved into the manager and returned; they are never logged.
        """
        manager.verify_configuration()
        manager.verify_cli_pin()
        manager.create_branch()
        return manager.wait_until_ready()

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

    # -- testdb mode -------------------------------------------------------

    def _require_testdb_tools(self) -> None:
        """--testdb needs only the Supabase CLI; stack tooling is irrelevant."""
        if self._command_resolver("supabase") is None:
            raise DevserverError("Required command not found on PATH: supabase")

    def _execute_testdb(self) -> int:
        """Run one Owner command against a freshly provisioned branch.

        --testdb provisions the canonical ephemeral Supabase branch, applies
        the current checkout's committed migrations (seed data is not
        applied), runs the command supplied after '--' with
        OPENORC_TEST_DATABASE_URL pointed at the branch database, and
        deletes the branch on exit via the single cleanup path. Nothing
        else is started: no app, API, worker, queue backend, or ngrok.
        """
        command = list(self._config.testdb_command or ())
        if not command:  # unreachable: parse_args enforces a non-empty command
            raise DevserverError("--testdb requires a command to run.")
        self._require_testdb_tools()
        load_env_file(self._root / ".env", self._env, self._log)
        manager = self._build_supabase_manager()
        branch_environment = self._provision_supabase_branch(manager)
        manager.apply_migrations()

        database_url = branch_environment.database_url
        if not database_url:  # fail closed even though readiness implies it
            raise DevserverError("Supabase branch did not publish database credentials.")
        child_env = dict(self._env)
        # The integration contract is specifically OPENORC_TEST_DATABASE_URL;
        # the branch URL is never mapped onto application DATABASE_URL here.
        child_env["OPENORC_TEST_DATABASE_URL"] = database_url
        self._log.log(
            f"Running testdb command against branch '{manager.branch_name}' "
            f"(database: {redact_url(database_url)})."
        )
        child = self._supervisor.start(
            "testdb-command",
            command,
            runner=self._runner,
            env=child_env,
            inherit_stdin=True,
        )
        while child.poll() is None:
            self._check_shutdown()
            self._sleep(FOREGROUND_POLL_SECONDS)
        return child.wait()


def main(argv: Sequence[str] | None = None, root: Path | None = None) -> int:
    """Console entrypoint used by scripts/devserver.sh via scripts/devserver.py."""
    # Fallback root when invoked without an explicit root: this package sits
    # four directory levels below the repository root.
    repo_root = root if root is not None else Path(__file__).resolve().parents[4]
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
