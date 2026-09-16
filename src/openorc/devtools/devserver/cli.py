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
