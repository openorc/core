"""Manual local E2E devserver orchestration for OpenOrc.

This package implements the orchestration behind ``scripts/devserver.sh``: the
stable human-facing command stays a tiny Bash wrapper, while the orchestration
lives here so it is importable and deterministically testable.

IMPORTANT:
- This is Owner/developer manual E2E tooling. Agents do not run the real
  hosted flow during ordinary implementation work.
- Ordinary tests substitute the process-runner and Supabase command
  boundaries at the Python level; they never build fake executable trees.

Module layout (one responsibility per module):

- ``errors``       shared exception taxonomy;
- ``logging``      secret-safe logger and URL redaction helpers;
- ``environment``  optional .env loading and typed environment parsing;
- ``cli``          RunConfig, usage text, and argument parsing;
- ``processes``    subprocess mechanics and process supervision;
- ``supabase``     ephemeral hosted Supabase preview-branch lifecycle;
- ``valkey``       local queue-backend ownership and guarded logical-DB reset;
- ``orchestrator`` high-level lifecycle, signal coordination, and entrypoint.

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

from openorc.devtools.devserver.cli import DEFAULT_TESTDB_COMMAND, USAGE, RunConfig, parse_args
from openorc.devtools.devserver.environment import load_env_file
from openorc.devtools.devserver.errors import (
    DevserverError,
    HelpRequested,
    ShutdownRequested,
    UsageError,
)
from openorc.devtools.devserver.logging import (
    LOG_PREFIX,
    Logger,
    StreamLogger,
    redact_url,
    sanitize_cli_error,
)
from openorc.devtools.devserver.orchestrator import NGROK_LOG_PATH, Devserver, main
from openorc.devtools.devserver.processes import (
    ChildProcess,
    CommandResolver,
    ProcessRunner,
    ProcessSupervisor,
    RunResult,
    SubprocessRunner,
    stop_child,
)
from openorc.devtools.devserver.supabase import (
    BranchEnvironment,
    SupabaseManager,
    parse_branch_environment,
)
from openorc.devtools.devserver.valkey import (
    DEFAULT_VALKEY_URL,
    QueueClient,
    ValkeyManager,
    default_queue_client_factory,
    is_local_valkey_url,
)

__all__ = [
    "DEFAULT_TESTDB_COMMAND",
    "DEFAULT_VALKEY_URL",
    "LOG_PREFIX",
    "NGROK_LOG_PATH",
    "BranchEnvironment",
    "ChildProcess",
    "CommandResolver",
    "Devserver",
    "DevserverError",
    "HelpRequested",
    "Logger",
    "ProcessRunner",
    "ProcessSupervisor",
    "QueueClient",
    "RunConfig",
    "RunResult",
    "ShutdownRequested",
    "StreamLogger",
    "SubprocessRunner",
    "SupabaseManager",
    "USAGE",
    "UsageError",
    "ValkeyManager",
    "default_queue_client_factory",
    "is_local_valkey_url",
    "load_env_file",
    "main",
    "parse_args",
    "parse_branch_environment",
    "redact_url",
    "sanitize_cli_error",
    "stop_child",
]
