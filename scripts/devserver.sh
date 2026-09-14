#!/bin/bash

# devserver.sh
#
# Manual local E2E orchestrator for OpenOrc.
#
# IMPORTANT:
# - This script is for the human owner/developer to run manually.
# - Cline is NOT expected to run this during normal implementation work.
# - The script is intentionally a draft. Cline should iterate on it as the
#   OpenOrc app/API/worker/Supabase integration becomes real.
#
# Local development model:
#
#   Browser
#      |
#      v
#   Vue app (local)
#      |
#      v
#   FastAPI API (local)
#      |
#      +--> RQ worker(s) (local)
#      |        |
#      |        v
#      |      Valkey (local)
#      |
#      +--> Supabase preview branch (hosted, ephemeral)
#
# Supabase lifecycle:
#
#   ./devserver.sh
#       |
#       +--> create temporary Supabase branch
#       +--> apply current repo migrations
#       +--> seed non-production data if available
#       +--> export branch-specific credentials
#       +--> start local stack
#       |
#       `--> on ANY exit:
#              stop local processes
#              delete Supabase branch
#
# The Supabase branch MUST NOT survive normal script termination.
# This is deliberate: hosted non-production state should be disposable and
# recreated from the repository, not manually maintained.
#
# Proven localhost behavior patterns:
# - strict `set -euo pipefail`
# - run relative to repo root
# - track background PIDs centrally
# - one foreground process keeps the script alive
# - cleanup through trap
# - start local queue backend automatically when appropriate
# - readiness checks before using local services
# - only perform destructive queue reset when the target is provably local
# - optional ngrok support for webhook/callback testing
#
# Things intentionally NOT finalized yet:
# - final env-file layout
# - final API/app ports
# - devserver wiring for the defined queue names (openorc: prefix,
#   canonical openorc:default) and the ownership-aware Redis lifecycle
#   (start/stop only devserver-owned instances; recommended local
#   namespace is Redis DB index 2)
# - exact Valkey launch command
# - exact Supabase CLI JSON parsing
# - exact Supabase branch migration command
# - final ngrok requirement
# - final app/API/worker entrypoints
#
# These should be filled in from the actual implementation rather than guessed.

set -euo pipefail


# ---------------------------------------------------------------------------
# Repository paths
# ---------------------------------------------------------------------------

# The script lives in <repo>/scripts; resolve the repository root one level up.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

MODE="dev"

INCLUDE_APP=true
INCLUDE_API=true
INCLUDE_WORKER=true

AUTO_VALKEY=true
AUTO_NGROK=true
AUTO_SUPABASE=true

# Temporary Supabase branch must normally be deleted when this script exits.
DELETE_SUPABASE_BRANCH_ON_EXIT=true

DEFAULT_APP_PORT=8081
DEFAULT_API_PORT=3000

# OpenOrc queue naming is defined in src/openorc/workers/queues.py
# (openorc: prefix; canonical default queue openorc:default). The effective
# default here is unchanged until the devserver work switches the local
# default to Redis DB index 2 together with ownership-aware Redis
# startup/teardown (see .env.example for the recommended local value).
DEFAULT_VALKEY_URL="redis://127.0.0.1:6379/0"

NGROK_LOG_PATH="/tmp/openorc-ngrok.log"


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

BACKGROUND_PIDS=()
BACKGROUND_LABELS=()

SUPABASE_BRANCH_CREATED=false
SUPABASE_BRANCH_NAME=""
SUPABASE_BRANCH_REF=""

# Parent hosted project: "OpenOrc Cloud".
#
# This should be supplied by local environment/configuration.
# Do NOT hardcode the real production project ref into this script.
SUPABASE_PARENT_PROJECT_REF="${OPENORC_SUPABASE_PROJECT_REF:-}"

# Safety guard.
#
# If we later store the production project ref separately, destructive branch
# operations should explicitly refuse to target it.
SUPABASE_PRODUCTION_PROJECT_REF="${OPENORC_SUPABASE_PRODUCTION_PROJECT_REF:-}"


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

log() {
    echo "[devserver] $*"
}

warn() {
    echo "[devserver] WARNING: $*" >&2
}

die() {
    echo "[devserver] ERROR: $*" >&2
    exit 1
}


# ---------------------------------------------------------------------------
# Environment file
#
# Optional local configuration file: `.env` at the repository root.
#
# - `.env` is git-ignored; copy `.env.example` to `.env` and fill in values.
# - Variables already exported in the calling shell always win over `.env`.
# - Branch-generated Supabase credentials are exported later at runtime and
#   are never read from `.env`.
#
# Defined after the logging helpers so the loader can use them.
# ---------------------------------------------------------------------------

load_env_file() {
    local env_file="$ROOT_DIR/.env"
    local line key value

    if [ ! -f "$env_file" ]; then
        log "No .env file found; using exported environment and defaults."
        return 0
    fi

    log "Loading environment file: $env_file"

    while IFS= read -r line || [ -n "$line" ]; do
        # Trim leading whitespace.
        line="${line#"${line%%[![:space:]]*}"}"

        # Skip blank lines and comments.
        case "$line" in
            ''|'#'*) continue ;;
        esac

        key="${line%%=*}"
        value="${line#*=}"

        # Accept only valid shell variable names.
        case "$key" in
            ''|[0-9]*|*[!A-Za-z0-9_]*)
                warn "Ignoring invalid .env line: $key"
                continue
                ;;
        esac

        # Strip one pair of surrounding quotes when present.
        if [ "${#value}" -ge 2 ]; then
            case "$value" in
                \"*\") value="${value#\"}"; value="${value%\"}" ;;
                \'*\') value="${value#\'}"; value="${value%\'}" ;;
            esac
        fi

        # Variables already exported in the calling shell win over `.env`.
        if [ -z "${!key+x}" ]; then
            export "$key=$value"
        fi
    done < "$env_file"
}

load_env_file


# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: ./devserver.sh [options]

Default:
  Start the local OpenOrc manual E2E stack:
    - ephemeral hosted Supabase branch
    - local Valkey
    - local API
    - local worker(s)
    - local Vue app

Options:
  --app-only
      Start only the Vue app.
      Supabase/API/worker/Valkey are not started automatically.

  --api-only
      Start only the API.
      Supabase is prepared unless --no-supabase is also supplied.

  --worker-only
      Start only the worker(s).

  --no-worker
      Do not start worker processes.

  --no-valkey
      Do not auto-start or reset local Valkey.

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
  - The default Supabase environment is ephemeral.
  - The production Supabase project must never be used as a development target.
  - Any destructive local Valkey reset must only happen for localhost URLs.
  - Cline is not expected to run this script during normal implementation.
USAGE
}


# ---------------------------------------------------------------------------
# Basic tool checks
# ---------------------------------------------------------------------------

require_command() {
    local command_name="$1"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        die "Required command not found on PATH: $command_name"
    fi
}

require_base_tools() {
    if [ "$AUTO_SUPABASE" = true ]; then
        require_command supabase
    fi

    if [ "$INCLUDE_APP" = true ]; then
        require_command npm
    fi

    if [ "$INCLUDE_API" = true ] || [ "$INCLUDE_WORKER" = true ]; then
        require_command python3
    fi
}


# ---------------------------------------------------------------------------
# Background process tracking
#
# Every background child registers here; cleanup terminates all of them.
# ---------------------------------------------------------------------------

register_background_pid() {
    local pid="$1"
    local label="$2"

    BACKGROUND_PIDS+=("$pid")
    BACKGROUND_LABELS+=("$label")
}

stop_background_processes() {
    local idx

    for idx in "${!BACKGROUND_PIDS[@]}"; do
        local pid="${BACKGROUND_PIDS[$idx]}"
        local label="${BACKGROUND_LABELS[$idx]}"

        if kill -0 "$pid" >/dev/null 2>&1; then
            log "Stopping $label (PID $pid)..."
            kill "$pid" >/dev/null 2>&1 || true
        fi
    done
}


# ---------------------------------------------------------------------------
# Supabase safety helpers
# ---------------------------------------------------------------------------

require_supabase_configuration() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    if [ -z "$SUPABASE_PARENT_PROJECT_REF" ]; then
        die "OPENORC_SUPABASE_PROJECT_REF is required for ephemeral Supabase."
    fi

    # Defense against future configuration accidents.
    if [ -n "$SUPABASE_PRODUCTION_PROJECT_REF" ] \
        && [ "$SUPABASE_PARENT_PROJECT_REF" != "$SUPABASE_PRODUCTION_PROJECT_REF" ]; then
        warn "Configured parent project differs from production project ref."
        warn "Verify that this is intentional."
    fi
}


# ---------------------------------------------------------------------------
# Supabase branch naming
#
# Branch names are unique per invocation so two devserver processes do not
# accidentally share the same database.
# ---------------------------------------------------------------------------

make_supabase_branch_name() {
    local user_part="${USER:-owner}"

    user_part="$(
        printf '%s' "$user_part" \
            | tr '[:upper:]' '[:lower:]' \
            | tr -cs 'a-z0-9-' '-'
    )"

    printf 'openorc-e2e-%s-%s-%s' \
        "$user_part" \
        "$(date +%Y%m%d%H%M%S)" \
        "$$"
}


# ---------------------------------------------------------------------------
# Supabase branch creation
#
# TODO:
# The precise CLI flags/output should be confirmed against the Supabase CLI
# version we pin for OpenOrc.
#
# Prefer machine-readable output (JSON) once available so this script does not
# scrape human-formatted CLI output.
# ---------------------------------------------------------------------------

create_supabase_branch() {
    if [ "$AUTO_SUPABASE" != true ]; then
        log "Supabase automation disabled."
        return
    fi

    require_supabase_configuration

    SUPABASE_BRANCH_NAME="$(make_supabase_branch_name)"

    log "Creating ephemeral Supabase branch: $SUPABASE_BRANCH_NAME"

    # TODO:
    # Confirm exact invocation and add --output json when supported/appropriate.
    #
    # Expected semantic operation:
    #   supabase branches create "$SUPABASE_BRANCH_NAME" \
    #       --project-ref "$SUPABASE_PARENT_PROJECT_REF"
    #
    supabase branches create "$SUPABASE_BRANCH_NAME" \
        --project-ref "$SUPABASE_PARENT_PROJECT_REF"

    SUPABASE_BRANCH_CREATED=true

    # TODO:
    # Parse and store the branch ref if creation returns it.
    #
    # SUPABASE_BRANCH_REF="..."
}


# ---------------------------------------------------------------------------
# Supabase readiness
#
# Creating the branch does not necessarily mean Postgres/Auth/API are
# immediately ready.
#
# A health endpoint returning success does not guarantee the service is
# actually usable; wait for a genuine readiness signal.
#
# TODO:
# Determine the most reliable Supabase CLI/API readiness signal.
# ---------------------------------------------------------------------------

wait_for_supabase_branch() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    log "Waiting for Supabase branch to become ready..."

    # TODO:
    #
    # Preferred eventual implementation:
    # - query branch status using machine-readable CLI output
    # - wait for a terminal READY/HEALTHY state
    # - use a bounded timeout
    #
    # Example shape only:
    #
    # local attempts=60
    # local attempt=1
    #
    # while [ "$attempt" -le "$attempts" ]; do
    #     status="$(...)"
    #
    #     if [ "$status" = "READY" ]; then
    #         return
    #     fi
    #
    #     sleep 2
    #     attempt=$((attempt + 1))
    # done
    #
    # die "Supabase branch did not become ready."
    :
}


# ---------------------------------------------------------------------------
# Supabase credentials
#
# Each preview branch has its own API/database credentials.
#
# These values should be fetched dynamically and exported only for this
# devserver process tree.
#
# Do NOT write generated branch credentials back into tracked env files.
#
# TODO:
# Confirm exact CLI/API mechanism for:
# - API URL
# - publishable/anon key
# - backend secret/service-role equivalent
# - Postgres URL / pooler URL
# ---------------------------------------------------------------------------

load_supabase_branch_credentials() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    log "Loading Supabase branch credentials..."

    # TODO:
    # Fetch branch details using JSON output and export values, e.g.:
    #
    # export SUPABASE_URL="..."
    # export SUPABASE_PUBLISHABLE_KEY="..."
    # export SUPABASE_SECRET_KEY="..."
    # export DATABASE_URL="..."
    #
    # Naming must follow the final OpenOrc application configuration model.
    :
}


# ---------------------------------------------------------------------------
# Apply migrations
#
# Critical architectural rule:
#
#   repo migration files
#       -> ephemeral test branch
#       -> review
#       -> main
#       -> production
#
# The remote branch exists to validate the current checkout.
#
# It must never become an alternate source of schema truth.
# ---------------------------------------------------------------------------

apply_supabase_migrations() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    log "Applying current repository migrations to Supabase branch..."

    # TODO:
    # Use the supported Supabase CLI flow to apply:
    #
    #   supabase/migrations/*
    #
    # from the CURRENT CHECKOUT to the newly created preview branch.
    #
    # This should not depend on production having already received the same
    # migration.
    #
    # The final implementation should fail hard if migrations do not apply
    # cleanly.
    :
}


# ---------------------------------------------------------------------------
# Seed development data
#
# If supabase/seed.sql exists, apply representative fake/test data only.
#
# Never pull or clone production user data into this environment by default.
# ---------------------------------------------------------------------------

seed_supabase_branch() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    if [ ! -f "$ROOT_DIR/supabase/seed.sql" ]; then
        log "No supabase/seed.sql present; skipping seed."
        return
    fi

    log "Applying Supabase development seed data..."

    # TODO:
    # Wire to final supported seed command.
    :
}


# ---------------------------------------------------------------------------
# Supabase deletion
#
# This runs from the global cleanup trap.
#
# The branch created by THIS invocation is the only branch this script is
# allowed to delete automatically.
#
# Failure to delete is noisy because forgotten branches cost money.
# ---------------------------------------------------------------------------

delete_supabase_branch() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    if [ "$DELETE_SUPABASE_BRANCH_ON_EXIT" != true ]; then
        if [ "$SUPABASE_BRANCH_CREATED" = true ]; then
            warn "Keeping Supabase branch by request: $SUPABASE_BRANCH_NAME"
        fi
        return
    fi

    if [ "$SUPABASE_BRANCH_CREATED" != true ]; then
        return
    fi

    if [ -z "$SUPABASE_BRANCH_NAME" ]; then
        warn "Supabase branch was marked created but branch name is empty."
        return
    fi

    log "Deleting ephemeral Supabase branch: $SUPABASE_BRANCH_NAME"

    if ! supabase branches delete "$SUPABASE_BRANCH_NAME" \
        --project-ref "$SUPABASE_PARENT_PROJECT_REF"; then

        warn "Failed to delete Supabase branch: $SUPABASE_BRANCH_NAME"
        warn "DELETE IT MANUALLY to avoid continued compute charges."
        return
    fi

    SUPABASE_BRANCH_CREATED=false
}


# ---------------------------------------------------------------------------
# Cleanup
#
# Order matters:
# - local processes stop first so they no longer depend on Supabase
# - Supabase branch is deleted last
#
# Preserve the original process exit code.
# ---------------------------------------------------------------------------

cleanup() {
    local exit_code=$?

    # Prevent recursive trap invocation while cleanup itself runs.
    trap - EXIT INT TERM

    stop_background_processes
    delete_supabase_branch

    exit "$exit_code"
}

trap cleanup EXIT INT TERM


# ---------------------------------------------------------------------------
# Valkey / Redis-compatible local queue backend
#
# Safety model:
# - use configured URL
# - recognize localhost explicitly
# - try existing service first
# - use Homebrew when available
# - verify readiness with redis-cli
# - flush only the local DB
#
# TODO:
# Confirm whether the local command is `valkey-server`, Homebrew service
# `valkey`, or Redis-compatible fallback on this machine.
# ---------------------------------------------------------------------------

resolve_worker_valkey_url() {
    printf '%s' "${VALKEY_URL:-$DEFAULT_VALKEY_URL}"
}

is_local_valkey_url() {
    local valkey_url="$1"

    [[ "$valkey_url" =~ ^redis://(127\.0\.0\.1|localhost):([0-9]+)/([0-9]+)$ ]]
}

wait_for_valkey() {
    local valkey_url="$1"
    local attempts=20
    local attempt=1

    if ! command -v redis-cli >/dev/null 2>&1; then
        warn "redis-cli not found; cannot actively verify Valkey readiness."
        return 0
    fi

    while [ "$attempt" -le "$attempts" ]; do
        if redis-cli -u "$valkey_url" ping >/dev/null 2>&1; then
            return 0
        fi

        sleep 0.5
        attempt=$((attempt + 1))
    done

    return 1
}

ensure_local_valkey() {
    local valkey_url="$1"

    if [ "$AUTO_VALKEY" != true ]; then
        log "Auto-Valkey disabled. Expecting queue backend at: $valkey_url"
        return
    fi

    if ! is_local_valkey_url "$valkey_url"; then
        log "Using non-local queue backend from environment: $valkey_url"
        return
    fi

    if command -v redis-cli >/dev/null 2>&1; then
        if redis-cli -u "$valkey_url" ping >/dev/null 2>&1; then
            log "Valkey/Redis already responding at $valkey_url"
            return
        fi
    fi

    # TODO:
    # Prefer the actual OpenOrc local installation once established.
    #
    # Likely candidates:
    #
    #   brew services start valkey
    #
    # or possibly a Redis-compatible fallback during bootstrap.
    #
    if command -v brew >/dev/null 2>&1; then
        log "Attempting to start local Valkey via Homebrew..."

        if brew services start valkey >/dev/null 2>&1; then
            if wait_for_valkey "$valkey_url"; then
                log "Valkey started successfully."
                return
            fi

            die "Homebrew reported Valkey started, but it did not become ready."
        fi

        warn "Unable to start Valkey using Homebrew."
    fi

    die "Local Valkey is not responding at $valkey_url."
}

flush_local_valkey_db() {
    local valkey_url="$1"

    if [ "$AUTO_VALKEY" != true ]; then
        return
    fi

    # Never flush anything that is not unambiguously localhost.
    if ! is_local_valkey_url "$valkey_url"; then
        return
    fi

    require_command redis-cli

    if ! redis-cli -u "$valkey_url" ping >/dev/null 2>&1; then
        die "Cannot flush local queue DB because Valkey is not responding."
    fi

    log "Flushing local OpenOrc queue DB: $valkey_url"

    redis-cli -u "$valkey_url" FLUSHDB >/dev/null
}


# ---------------------------------------------------------------------------
# ngrok
#
# Requirements:
# - optional
# - only start when API is running
# - only start if a reserved URL is configured
#
# Whether OpenOrc ultimately requires ngrok depends on webhook/runtime callback
# behavior during manual E2E.
# ---------------------------------------------------------------------------

start_ngrok_background() {
    if [ "$AUTO_NGROK" != true ] || [ "$INCLUDE_API" != true ]; then
        return
    fi

    local ngrok_url="${NGROK_RESERVED_URL:-}"

    if [ -z "$ngrok_url" ]; then
        log "NGROK_RESERVED_URL not set; skipping ngrok startup."
        return
    fi

    if ! command -v ngrok >/dev/null 2>&1; then
        warn "ngrok not found on PATH; skipping tunnel startup."
        return
    fi

    mkdir -p "$(dirname "$NGROK_LOG_PATH")"
    : > "$NGROK_LOG_PATH"

    local api_port="${LOCAL_API_PORT:-$DEFAULT_API_PORT}"

    log "Starting ngrok tunnel for API: $ngrok_url"
    log "ngrok logs: $NGROK_LOG_PATH"

    ngrok http \
        --url="$ngrok_url" \
        "$api_port" \
        >>"$NGROK_LOG_PATH" 2>&1 < /dev/null &

    register_background_pid "$!" "ngrok"
}


# ---------------------------------------------------------------------------
# API
#
# TODO:
# Replace entrypoint after apps/api bootstrap exists.
# ---------------------------------------------------------------------------

start_api_background() {
    local api_port="${LOCAL_API_PORT:-$DEFAULT_API_PORT}"

    log "Starting OpenOrc API in background..."

    (
        export APP_ENV="${APP_ENV:-development}"

        # Placeholder.
        #
        # Expected eventual shape:
        #
        # exec "$ROOT_DIR/.venv/bin/python" -m uvicorn \
        #     openorc.api.app:app \
        #     --host 0.0.0.0 \
        #     --port "$api_port" \
        #     --reload \
        #     --log-level debug
        #
        warn "API startup is not implemented yet."
        sleep infinity
    ) &

    register_background_pid "$!" "api"
}


# ---------------------------------------------------------------------------
# Worker
#
# TODO:
# Replace with final RQ worker entrypoint and queue names.
# ---------------------------------------------------------------------------

start_worker_background() {
    log "Starting OpenOrc worker in background..."

    (
        export APP_ENV="${APP_ENV:-development}"

        # Placeholder.
        #
        # Expected eventual shape:
        #
        # exec "$ROOT_DIR/.venv/bin/python" -m openorc.worker
        #
        warn "Worker startup is not implemented yet."
        sleep infinity
    ) &

    register_background_pid "$!" "worker"
}


# ---------------------------------------------------------------------------
# Vue application
#
# Run the primary frontend in the foreground.
# Ctrl-C naturally tears down the whole stack through the cleanup trap.
# ---------------------------------------------------------------------------

run_app_foreground() {
    local app_port="${LOCAL_APP_PORT:-$DEFAULT_APP_PORT}"

    log "Starting OpenOrc app in foreground..."

    # TODO:
    # Adjust working directory / package script once apps/app is initialized.
    #
    # Expected eventual shape:
    #
    # (
    #     cd "$ROOT_DIR/apps/app"
    #     exec npm run dev -- --host 0.0.0.0 --port "$app_port"
    # )
    #
    die "Vue app startup is not implemented yet."
}


# ---------------------------------------------------------------------------
# Supabase environment preparation
# ---------------------------------------------------------------------------

prepare_supabase_environment() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    create_supabase_branch
    wait_for_supabase_branch
    load_supabase_branch_credentials
    apply_supabase_migrations
    seed_supabase_branch
}


# ---------------------------------------------------------------------------
# CLI options
# ---------------------------------------------------------------------------

while [[ "$#" -gt 0 ]]; do
    case "$1" in

        --app-only)
            MODE="app-only"
            INCLUDE_APP=true
            INCLUDE_API=false
            INCLUDE_WORKER=false

            AUTO_VALKEY=false
            AUTO_NGROK=false
            AUTO_SUPABASE=false
            ;;

        --api-only)
            MODE="api-only"
            INCLUDE_APP=false
            INCLUDE_API=true
            INCLUDE_WORKER=false

            AUTO_VALKEY=false
            ;;

        --worker-only)
            MODE="worker-only"
            INCLUDE_APP=false
            INCLUDE_API=false
            INCLUDE_WORKER=true

            AUTO_NGROK=false
            ;;

        --no-worker)
            INCLUDE_WORKER=false
            ;;

        --no-valkey)
            AUTO_VALKEY=false
            ;;

        --no-ngrok)
            AUTO_NGROK=false
            ;;

        --no-supabase)
            AUTO_SUPABASE=false
            ;;

        --keep-supabase)
            DELETE_SUPABASE_BRANCH_ON_EXIT=false
            ;;

        -h|--help)
            print_usage
            exit 0
            ;;

        *)
            die "Unknown parameter: $1"
            ;;
    esac

    shift
done


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

require_base_tools

prepare_supabase_environment


# Queue backend

if [ "$INCLUDE_WORKER" = true ]; then
    WORKER_VALKEY_URL="$(resolve_worker_valkey_url)"

    ensure_local_valkey "$WORKER_VALKEY_URL"
    flush_local_valkey_db "$WORKER_VALKEY_URL"

    export VALKEY_URL="$WORKER_VALKEY_URL"
fi


# Start background services

if [ "$INCLUDE_API" = true ] && [ "$INCLUDE_APP" = true ]; then
    start_api_background
fi

if [ "$INCLUDE_WORKER" = true ] && [ "$INCLUDE_APP" = true ]; then
    start_worker_background
fi

if [ "$INCLUDE_API" = true ]; then
    start_ngrok_background
fi


# ---------------------------------------------------------------------------
# Foreground process selection
#
# One service stays in the foreground so:
#
#   Ctrl-C
#      -> shell trap
#      -> stop children
#      -> delete Supabase branch
#
# No daemon supervisor required for manual E2E.
# ---------------------------------------------------------------------------

if [ "$INCLUDE_APP" = true ]; then
    run_app_foreground
    exit 0
fi

if [ "$INCLUDE_API" = true ]; then
    # TODO:
    # Add run_api_foreground once API bootstrap exists.
    die "API-only foreground mode is not implemented yet."
fi

if [ "$INCLUDE_WORKER" = true ]; then
    # TODO:
    # Add run_worker_foreground once worker bootstrap exists.
    die "Worker-only foreground mode is not implemented yet."
fi

die "Nothing selected to run."