"""Command-line configuration: RunConfig, usage text, and argument parsing."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from openorc.devtools.devserver.errors import HelpRequested, UsageError

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
    # When set, --testdb mode: run this command against a freshly provisioned
    # ephemeral Supabase branch instead of starting the application stack.
    testdb_command: tuple[str, ...] | None = None

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

  --testdb [--keep-supabase] [-- <command> [args...]]
      Provision an ephemeral hosted Supabase branch, apply the current
      checkout's committed migrations (never seed data), run the
      persistence integration suite with OPENORC_TEST_DATABASE_URL
      pointing at the branch database, and delete the branch on exit
      (including failure or Ctrl-C).

      Bare --testdb runs the canonical suite:

        ./devserver.sh --testdb

      Supply a command after '--' to run something else instead:

        ./devserver.sh --testdb -- \
          .venv/bin/python -m pytest -m integration \
          tests/integration/test_ownership_persistence.py

      Nothing else starts in this mode: no app, API, worker, queue
      backend, or ngrok. Surface-selection flags (--app-only,
      --api-only, --worker-only, --no-worker, --no-valkey, --no-ngrok,
      --no-supabase) cannot be combined with --testdb. --keep-supabase
      keeps the branch after the run (debug escape hatch; incurs
      compute cost). The branch database URL is injected into the child
      environment only; it is never logged or written to files.

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
  - --testdb provisions only the branch database: committed migrations are
    applied, seed data is not, and the integration suite (or the command
    supplied after '--') is the only process started.
  - Cline is not expected to run this script during normal implementation.
"""


# The canonical persistence integration suite run when --testdb is given
# without an explicit command override (Owner workflow default).
DEFAULT_TESTDB_COMMAND: tuple[str, ...] = (
    ".venv/bin/python",
    "-m",
    "pytest",
    "-m",
    "integration",
    "tests/integration/test_ownership_persistence.py",
)

_TESTDB_INCOMPATIBLE_FLAGS = frozenset(
    {
        "--app-only",
        "--api-only",
        "--worker-only",
        "--no-worker",
        "--no-valkey",
        "--no-ngrok",
        "--no-supabase",
    }
)


def parse_args(argv: Sequence[str]) -> RunConfig:
    """Parse devserver arguments into a RunConfig.

    Raises HelpRequested for -h/--help and UsageError for unknown options.
    """
    config = RunConfig()
    arguments = list(argv)
    index = 0
    while index < len(arguments):
        arg = arguments[index]
        index += 1
        if arg == "--testdb":
            config.testdb_command = _parse_testdb_command(config, arguments[index:])
            return config
        elif arg == "--app-only":
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


def _parse_testdb_command(config: RunConfig, rest: Sequence[str]) -> tuple[str, ...]:
    """Parse everything after --testdb: options up to '--', then the command.

    '--' terminates option parsing; every argument after it belongs to the
    child command and is never interpreted by the devserver. A missing or
    empty override means the canonical integration suite runs.
    """
    remaining = list(rest)
    while remaining:
        arg = remaining.pop(0)
        if arg == "--":
            if not remaining:
                return DEFAULT_TESTDB_COMMAND
            return tuple(remaining)
        if arg in {"-h", "--help"}:
            raise HelpRequested
        if arg == "--keep-supabase":
            config.keep_supabase = True
        elif arg in _TESTDB_INCOMPATIBLE_FLAGS:
            raise UsageError(f"{arg} cannot be combined with --testdb.")
        elif arg.startswith("-"):
            raise UsageError(f"Unknown parameter: {arg}")
        else:
            raise UsageError(
                f"Expected '--' before the testdb command; got {arg!r}. "
                "Use: --testdb [--keep-supabase] -- <command> [args...]"
            )
    return DEFAULT_TESTDB_COMMAND
