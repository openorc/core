#!/bin/bash

# devserver.sh
#
# Manual local E2E orchestrator for OpenOrc — the stable human-facing command.
#
# IMPORTANT:
# - This script is for the human owner/developer to run manually.
# - Cline is NOT expected to run this during normal implementation work.
#
# The orchestration lives in Python (openorc.devtools.devserver, invoked
# through scripts/devserver.py) so it can be imported and deterministically
# tested. This wrapper stays deliberately small and boring:
#
#   - strict shell mode;
#   - resolve the repository root regardless of the caller's cwd;
#   - require the repository virtualenv interpreter (.venv/bin/python);
#   - exec the Python entrypoint with all arguments unchanged.
#
# Local development model (see README "Manual local E2E" for the full
# lifecycle and safety properties):
#
#   Browser -> Vue app (foreground) -> FastAPI API -> RQ worker
#                                                   -> local Redis-compatible
#                                                      queue backend
#                          API/worker -> Supabase preview branch (ephemeral)
#
# ./scripts/devserver.sh --testdb [-- <command> [args...]] provisions an
# ephemeral branch, applies committed migrations, runs the persistence
# integration suite (or the supplied <command>) with
# OPENORC_TEST_DATABASE_URL, and deletes the branch on exit.
#
# The Supabase branch created by a run is deleted on exit (including Ctrl-C
# and failure) unless --keep-supabase is given. Signals only request
# shutdown; the Python orchestrator performs exactly one dependency-aware
# cleanup sequence (children first, devserver-owned queue server next,
# Supabase branch deletion last of all).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -x "$ROOT_DIR/.venv/bin/python" ]; then
    echo "[devserver] ERROR: .venv/bin/python not found. Run 'uv sync' to create the repository virtual environment (see README)." >&2
    exit 1
fi

exec "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/devserver.py" "$@"
