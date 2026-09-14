#!/bin/bash

# devserver.sh
#
# Manual local E2E orchestrator for OpenOrc.
#
# IMPORTANT:
# - This script is for the human owner/developer to run manually.
# - Cline is NOT expected to run this during normal implementation work.
#
# Local development model:
#
#   Browser
#      |
#      v
#   Vue app (local, foreground)
#      |
#      v
#   FastAPI API (local)
#      |
#      +--> RQ worker (local)
#      |        |
#      |        v
#      |      Redis-compatible queue backend (local)
#      |
#      +--> Supabase preview branch (hosted, ephemeral)
#
# Supabase lifecycle:
#
#   ./devserver.sh
#       |
#       +--> create a temporary Supabase branch (unique per invocation)
#       +--> wait for genuine branch readiness (published credentials plus
#       |    a live database, with a bounded timeout)
#       +--> export branch credentials to the process tree only (never to
#       |    tracked files). API keys are normalized for the pinned CLI:
#       |    SUPABASE_PUBLISHABLE_KEY falls back to SUPABASE_ANON_KEY, and
#       |    SUPABASE_SECRET_KEY falls back to SUPABASE_DEFAULT_KEY, then
#       |    SUPABASE_SERVICE_ROLE_KEY (the pinned CLI never emits a key
#       |    literally named SUPABASE_SECRET_KEY).
#       +--> apply current-checkout migrations through the supported tooling
#       |    (scripts/supabase-apply-migrations.sh); fail hard on error
#       +--> apply supabase/seed.sql when present
#       +--> start the local stack
#       |
#       `--> on ANY exit:
#              stop local processes (foreground first, then background,
#              queue backend last)
#              delete the Supabase branch created by this invocation
#
# The Supabase branch MUST NOT survive normal script termination.
# This is deliberate: hosted non-production state should be disposable and
# recreated from the repository, not manually maintained.
#
# Queue backend lifecycle (two independent questions):
#
# - Server process ownership answers "may I stop this server?".
#   A Redis-compatible server already responding at the configured local
#   VALKEY_URL is used as-is and is NEVER stopped. When nothing is
#   responding, this script starts the installed `redis-server` binary
#   directly as an ephemeral child with persistence disabled
#   (`--save '' --appendonly no`). It never uses `brew services`, whose
#   persistent LaunchAgent state outlives the devserver.
#
# - The guarded VALKEY_URL namespace answers "may I reset this selected
#   development DB?".
#   The explicitly selected local database (default
#   redis://127.0.0.1:6379/2) is flushed before the worker starts in both
#   externally-owned and devserver-owned cases. Non-local URLs are never
#   flushed and never managed.
#
# Proven localhost behavior patterns:
# - strict `set -euo pipefail`
# - run relative to repo root
# - `.env` at the repo root (git-ignored); exported environment wins
# - track background PIDs centrally; stop in reverse start order so the
#   foreground process dies first and the queue backend stops after the
#   worker/API that depend on it
# - one foreground process keeps the script alive, run as a tracked child so
#   the parent shell always owns the cleanup trap
# - readiness checks before using local services
# - optional ngrok support for webhook/callback testing

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
# (openorc: prefix; canonical default queue openorc:default). The devserver
# local default is Redis DB index 2, the recommended local namespace (see
# .env.example); an exported VALKEY_URL always wins.
DEFAULT_VALKEY_URL="redis://127.0.0.1:6379/2"

# Invocation-specific queue-backend temp paths. Never shared between runs and
# never inside the repository.
VALKEY_TMP_DIR="/tmp/openorc-redis-$$/"
VALKEY_LOG_PATH="/tmp/openorc-redis-$$.log"

NGROK_LOG_PATH="/tmp/openorc-ngrok.log"

# Bounded wait for a freshly created branch to become genuinely usable
# (credentials published AND a database that answers). The timing constants
# are overridable for deterministic tests; the defaults are the supported
# developer values and are not part of the .env contract.
SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS="${OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS:-60}"
SUPABASE_BRANCH_WAIT_SLEEP_SECONDS="${OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS:-5}"


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

BACKGROUND_PIDS=()
BACKGROUND_LABELS=()

VALKEY_EXTERNALLY_OWNED=false
VALKEY_TMP_DIR_CREATED=false

SUPABASE_BRANCH_CREATED=false
SUPABASE_BRANCH_NAME=""

# Parent hosted project ("OpenOrc Cloud").
#
# Supplied by local environment/configuration. Do NOT hardcode the real
# production project ref into this script.
SUPABASE_PARENT_PROJECT_REF="${OPENORC_SUPABASE_PROJECT_REF:-}"

# Safety guard.
#
# The branch PARENT project may legitimately be the hosted production project
# (that is how Supabase preview branching works); only branch resolution
# touches the parent. The production project itself is never a development
# target: the stack only ever consumes credentials of the branch created by
# this invocation, and the migration tooling re-verifies that identity.
SUPABASE_PRODUCTION_PROJECT_REF="${OPENORC_SUPABASE_PRODUCTION_PROJECT_REF:-}"

# Branch credentials fetched from `supabase branches get -o env`.
BRANCH_ENV_API_URL=""
BRANCH_ENV_POOLER_URL=""
BRANCH_ENV_DIRECT_URL=""
BRANCH_ENV_PUBLISHABLE_KEY=""
BRANCH_ENV_SECRET_KEY=""
BRANCH_GET_STDERR=""


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
        # The canonical local Python environment is the repository .venv
        # created from the committed uv contract. Never fall back to an
        # arbitrary system python3.
        if [ ! -x "$ROOT_DIR/.venv/bin/python" ]; then
            die ".venv/bin/python not found. Run 'uv sync' to create the repository virtual environment (see README)."
        fi
    fi
}


# ---------------------------------------------------------------------------
# Background process tracking
#
# Every background child registers here, including the foreground service and
# the devserver-owned queue backend. Cleanup stops them in REVERSE start
# order: the foreground process (registered last) stops first; the queue
# backend (registered first) stops after the worker/API that depend on it.
# ---------------------------------------------------------------------------

register_background_pid() {
    local pid="$1"
    local label="$2"

    BACKGROUND_PIDS+=("$pid")
    BACKGROUND_LABELS+=("$label")
}

stop_background_processes() {
    local idx pid label waited

    for ((idx=${#BACKGROUND_PIDS[@]} - 1; idx >= 0; idx--)); do
        pid="${BACKGROUND_PIDS[$idx]}"
        label="${BACKGROUND_LABELS[$idx]}"

        if kill -0 "$pid" >/dev/null 2>&1; then
            log "Stopping $label (PID $pid)..."
            kill "$pid" >/dev/null 2>&1 || true

            waited=0
            while kill -0 "$pid" >/dev/null 2>&1 && [ "$waited" -lt 50 ]; do
                sleep 0.1
                waited=$((waited + 1))
            done

            if kill -0 "$pid" >/dev/null 2>&1; then
                warn "$label (PID $pid) did not stop gracefully; forcing."
                kill -9 "$pid" >/dev/null 2>&1 || true
            fi
        fi
    done

    BACKGROUND_PIDS=()
    BACKGROUND_LABELS=()
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
# Supabase CLI version pin (strict equality)
#
# The same shared machine-wide CLI binary is used for branch operations and
# by scripts/supabase-apply-migrations.sh, so the repository pin in
# supabase/cli-version is enforced here with the same strict-equality check
# (see docs/supabase-migrations.md). Any other version is refused.
# ---------------------------------------------------------------------------

# Trim leading and trailing whitespace.
trim() {
    local value="$1"

    value="${value#"${value%%[![:space:]]*}"}"
    printf '%s' "${value%"${value##*[![:space:]]}"}"
}

require_pinned_supabase_cli() {
    local pinned installed_raw normalized
    local semver_re='^[0-9]+(\.[0-9]+)+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$'

    if [ ! -f "$ROOT_DIR/supabase/cli-version" ]; then
        die "CLI version pin file missing: supabase/cli-version"
    fi

    pinned="$(tr -d '[:space:]' < "$ROOT_DIR/supabase/cli-version")"

    if [ -z "$pinned" ]; then
        die "CLI version pin file is empty: supabase/cli-version"
    fi

    if ! installed_raw="$(supabase --version)"; then
        die "supabase --version exited with a non-zero status; cannot verify the installed CLI version."
    fi

    normalized="$(trim "$installed_raw")"

    case "$normalized" in
        [Ss]upabase\ *)
            normalized="$(trim "${normalized#[Ss]upabase }")"
            ;;
    esac

    normalized="${normalized#v}"

    if [ -z "$normalized" ] || [[ ! "$normalized" =~ $semver_re ]]; then
        die "Could not determine the installed Supabase CLI version from unexpected --version output: ${installed_raw:-<empty>}."
    fi

    if [ "$normalized" != "$pinned" ]; then
        die "Supabase CLI version mismatch: installed $normalized, repository pin $pinned.
Align your Supabase CLI with the repository pin (see docs/supabase-migrations.md)."
    fi

    log "Supabase CLI version OK: $normalized"
}


# ---------------------------------------------------------------------------
# CLI diagnostics handling
#
# Credentials and tokens are never logged; URLs are logged host-only.
# ---------------------------------------------------------------------------

# Redact credentials from CLI diagnostics before surfacing them.
sanitize_cli_error() {
    local text="$1"

    if [ -n "${SUPABASE_ACCESS_TOKEN:-}" ]; then
        text="${text//$SUPABASE_ACCESS_TOKEN/<redacted-token>}"
    fi
    text="$(printf '%s\n' "$text" | sed -E 's#((postgres(ql)?|https?)://[^:/@ ]+):[^@ ]+@#\1:<redacted>@#g')"
    printf '%s' "$text"
}

# Returns 0 when the CLI error output looks like an authentication or
# authorization failure for the branch operation itself.
branch_get_error_is_auth() {
    local text

    text="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"

    printf '%s' "$text" | grep -Eq '401|403|unauthorized|forbidden|invalid access token|invalid api key|not logged in|access token|api key|permission'
}

# Print a database/queue URL with credentials removed (scheme + host only).
redact_url() {
    local url="$1" rest host

    case "$url" in
        postgresql://*|postgres://*|rediss://*|redis://*|https://*)
            rest="${url#*://}"
            ;;
        *)
            printf '<unrecognized-url>'
            return
            ;;
    esac

    case "$rest" in
        *@*) rest="${rest#*@}" ;;
    esac

    host="${rest%%[/?:]*}"
    host="${host#[}"
    host="${host%]}"

    if [ -z "$host" ]; then
        printf '<unrecognized-url>'
        return
    fi

    printf '%s://%s' "${url%%://*}" "$host"
}

# Local queue URLs never carry credentials, so they may be shown in full.
redact_valkey_url() {
    local valkey_url="$1"

    if is_local_valkey_url "$valkey_url"; then
        printf '%s' "$valkey_url"
    else
        redact_url "$valkey_url"
    fi
}


# ---------------------------------------------------------------------------
# Supabase branch creation
# ---------------------------------------------------------------------------

create_supabase_branch() {
    if [ "$AUTO_SUPABASE" != true ]; then
        log "Supabase automation disabled."
        return
    fi

    require_pinned_supabase_cli
    require_supabase_configuration

    SUPABASE_BRANCH_NAME="$(make_supabase_branch_name)"

    log "Creating ephemeral Supabase branch: $SUPABASE_BRANCH_NAME"

    if ! supabase branches create "$SUPABASE_BRANCH_NAME" \
        --project-ref "$SUPABASE_PARENT_PROJECT_REF" \
        </dev/null; then
        die "Supabase branch creation failed for '$SUPABASE_BRANCH_NAME'."
    fi

    SUPABASE_BRANCH_CREATED=true
}


# ---------------------------------------------------------------------------
# Supabase readiness + credential fetch
#
# Creating the branch does not mean Postgres/Auth/API are immediately usable.
# Machine-readable CLI output only (`-o env`); readiness requires BOTH:
#   1. the branch has published its database credentials, and
#   2. the branch database actually answers (`supabase migration list`).
# Publishing credentials alone is NOT sufficient: hosted branches expose
# credentials before their database host resolves.
#
# The production/main branch never publishes database credentials through
# the API, so it can never pass this gate.
# ---------------------------------------------------------------------------

# Populates from `supabase branches get ... -o env`:
#   BRANCH_ENV_API_URL          https API URL of the branch (SUPABASE_URL)
#   BRANCH_ENV_POOLER_URL       POSTGRES_URL (published pooler URL)
#   BRANCH_ENV_DIRECT_URL       POSTGRES_URL_NON_POOLING (direct URL)
#   BRANCH_ENV_PUBLISHABLE_KEY  SUPABASE_PUBLISHABLE_KEY, else SUPABASE_ANON_KEY
#   BRANCH_ENV_SECRET_KEY       SUPABASE_DEFAULT_KEY, else SUPABASE_SERVICE_ROLE_KEY
#   BRANCH_GET_STDERR           sanitized stderr from the last CLI invocation
# Returns:
#   0  credentials resolved
#   1  CLI invocation failed (BRANCH_GET_STDERR holds the sanitized stderr)
#   2  branch exists but database credentials are not published yet
fetch_branch_environment() {
    local branch_name="$1"
    local err_file raw line key value status=0
    local publishable_key="" anon_key="" default_key="" service_role_key=""

    err_file="$(mktemp "${TMPDIR:-/tmp}/openorc-branch-get.XXXXXX")"
    raw="$(supabase branches get "$branch_name" \
        --project-ref "$SUPABASE_PARENT_PROJECT_REF" \
        -o env 2>"$err_file" </dev/null)" || status=$?
    BRANCH_GET_STDERR="$(sanitize_cli_error "$(cat "$err_file")")"
    rm -f "$err_file"

    if [ "$status" != 0 ]; then
        return 1
    fi

    BRANCH_ENV_API_URL=""
    BRANCH_ENV_POOLER_URL=""
    BRANCH_ENV_DIRECT_URL=""

    while IFS= read -r line || [ -n "$line" ]; do
        key="${line%%=*}"
        value="${line#*=}"

        if [ "${#value}" -ge 2 ]; then
            case "$value" in
                \"*\") value="${value#\"}"; value="${value%\"}" ;;
                \'*\') value="${value#\'}"; value="${value%\'}" ;;
            esac
        fi

        case "$key" in
            POSTGRES_URL)              BRANCH_ENV_POOLER_URL="$value" ;;
            POSTGRES_URL_NON_POOLING)  BRANCH_ENV_DIRECT_URL="$value" ;;
            SUPABASE_URL)              BRANCH_ENV_API_URL="$value" ;;
            SUPABASE_PUBLISHABLE_KEY)  publishable_key="$value" ;;
            SUPABASE_ANON_KEY)         anon_key="$value" ;;
            SUPABASE_DEFAULT_KEY)      default_key="$value" ;;
            SUPABASE_SERVICE_ROLE_KEY) service_role_key="$value" ;;
        esac
    done <<< "$raw"

    if [ -z "$BRANCH_ENV_POOLER_URL" ] && [ -z "$BRANCH_ENV_DIRECT_URL" ]; then
        return 2
    fi

    # Key normalization for the pinned CLI (v2.117.0): new-format keys are
    # preferred, legacy anon/service-role values are the documented fallback.
    # The CLI does not emit a key literally named SUPABASE_SECRET_KEY; the
    # new-format default secret is named SUPABASE_DEFAULT_KEY.
    BRANCH_ENV_PUBLISHABLE_KEY="${publishable_key:-$anon_key}"
    BRANCH_ENV_SECRET_KEY="${default_key:-$service_role_key}"

    return 0
}

# Bounded wait around the branch operation. Distinguishes:
#   - authentication/authorization failure  -> fail immediately;
#   - credentials not published yet         -> retry;
#   - credentials published, DB not ready   -> retry;
#   - database answers                      -> proceed.
wait_for_supabase_branch() {
    local branch_name="$1"
    local attempt=1 reason="" status=0

    log "Waiting for Supabase branch '$branch_name' to become ready..."

    while [ "$attempt" -le "$SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS" ]; do
        status=0
        fetch_branch_environment "$branch_name" || status=$?

        case "$status" in
            0)
                if supabase migration list \
                    --db-url "${BRANCH_ENV_POOLER_URL:-$BRANCH_ENV_DIRECT_URL}" \
                    >/dev/null 2>&1 </dev/null; then
                    log "Supabase branch is ready."
                    return 0
                fi
                reason="database not answering yet"
                ;;
            2)
                reason="database credentials not published yet"
                ;;
            *)
                if branch_get_error_is_auth "$BRANCH_GET_STDERR"; then
                    die "Supabase rejected the branch operation for '$branch_name' (authentication/authorization): ${BRANCH_GET_STDERR:-<no diagnostic>}
Check that SUPABASE_ACCESS_TOKEN (or your stored CLI login) is valid and authorized for the parent project."
                fi
                reason="branch lookup failed transiently"
                ;;
        esac

        if [ "$attempt" -lt "$SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS" ]; then
            log "Branch '$branch_name' not ready ($reason; attempt $attempt/$SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS); waiting ${SUPABASE_BRANCH_WAIT_SLEEP_SECONDS}s..."
            sleep "$SUPABASE_BRANCH_WAIT_SLEEP_SECONDS"
        fi

        attempt=$((attempt + 1))
    done

    die "Supabase branch '$branch_name' did not become ready within $SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS attempts x ${SUPABASE_BRANCH_WAIT_SLEEP_SECONDS}s (last status: $reason). It may still be provisioning, or it may be the production/main branch (whose database credentials are never retrievable)."
}


# ---------------------------------------------------------------------------
# Supabase credentials
#
# Each preview branch has its own API/database credentials. Values are
# fetched dynamically and exported only for this devserver process tree.
# They are NEVER written to `.env` or any tracked/generated file, and key
# values are never logged.
#
# Runtime contract (see .env.example):
#   SUPABASE_URL              branch https API URL
#   SUPABASE_PUBLISHABLE_KEY  new-format publishable key (else legacy anon)
#   SUPABASE_SECRET_KEY       new-format default secret (else legacy service-role)
#   DATABASE_URL              branch pooler URL (direct URL fallback)
# ---------------------------------------------------------------------------

load_supabase_branch_credentials() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    log "Loading Supabase branch credentials..."

    if [ -z "$BRANCH_ENV_API_URL" ]; then
        die "Supabase branch did not publish an API URL; refusing to start the stack against an unresolved target."
    fi

    if [ -z "$BRANCH_ENV_POOLER_URL" ] && [ -z "$BRANCH_ENV_DIRECT_URL" ]; then
        die "Supabase branch did not publish database credentials; refusing to start the stack."
    fi

    # Fail closed when the required API keys cannot be resolved: the runtime
    # contract requires both a publishable key and a secret key.
    if [ -z "$BRANCH_ENV_PUBLISHABLE_KEY" ]; then
        die "Supabase branch API keys could not be resolved: neither SUPABASE_PUBLISHABLE_KEY nor SUPABASE_ANON_KEY was present in the branch output."
    fi

    if [ -z "$BRANCH_ENV_SECRET_KEY" ]; then
        die "Supabase branch API keys could not be resolved: neither SUPABASE_DEFAULT_KEY nor SUPABASE_SERVICE_ROLE_KEY was present in the branch output."
    fi

    export SUPABASE_URL="$BRANCH_ENV_API_URL"
    export DATABASE_URL="${BRANCH_ENV_POOLER_URL:-$BRANCH_ENV_DIRECT_URL}"
    export SUPABASE_PUBLISHABLE_KEY="$BRANCH_ENV_PUBLISHABLE_KEY"
    export SUPABASE_SECRET_KEY="$BRANCH_ENV_SECRET_KEY"

    log "Branch credentials exported for this process tree (API URL: $BRANCH_ENV_API_URL, database: $(redact_url "$DATABASE_URL"))."
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
# The remote branch exists to validate the current checkout. It must never
# become an alternate source of schema truth.
#
# The supported tooling (scripts/supabase-apply-migrations.sh) owns the
# strict `db push`, the production identity guards, the `main`-branch
# refusal, and fail-hard error behavior; branch creation/deletion and
# credential export for the local stack remain this script's responsibility.
# ---------------------------------------------------------------------------

apply_supabase_migrations() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    log "Applying current repository migrations to Supabase branch '$SUPABASE_BRANCH_NAME'..."

    "$ROOT_DIR/scripts/supabase-apply-migrations.sh" --branch "$SUPABASE_BRANCH_NAME"
}


# ---------------------------------------------------------------------------
# Seed development data
#
# If supabase/seed.sql exists, apply representative fake/test data only.
# Never pull or clone production user data into this environment.
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

    # `--include-seed` applies the configured seed file after the migration
    # history check; migrations were already applied above, so this is a
    # migration no-op followed by the seed. Fails hard on error.
    if ! supabase db push --db-url "$DATABASE_URL" --include-seed </dev/null; then
        die "Seed data did not apply cleanly; failing hard (migration-first policy)."
    fi
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
            warn "Keeping Supabase branch by request (--keep-supabase): $SUPABASE_BRANCH_NAME"
            warn "DELETE IT MANUALLY when finished to avoid continued compute charges."
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
        --project-ref "$SUPABASE_PARENT_PROJECT_REF" \
        </dev/null; then

        warn "Failed to delete Supabase branch: $SUPABASE_BRANCH_NAME"
        warn "DELETE IT MANUALLY to avoid continued compute charges."
        return
    fi

    SUPABASE_BRANCH_CREATED=false
}


# ---------------------------------------------------------------------------
# Cleanup
#
# Order matters (dependencies, not registration order):
#   1. foreground/API/worker/ngrok stop (reverse start order, so the
#      foreground process stops first and the queue backend stops after the
#      worker/API that depend on it);
#   2. the devserver-owned queue server stops (a pre-existing server is
#      never registered and is never touched), then this invocation's temp
#      directory is removed;
#   3. the Supabase branch is deleted last.
#
# The original process exit code is preserved.
# ---------------------------------------------------------------------------

cleanup() {
    local exit_code=$?

    # Prevent recursive trap invocation while cleanup itself runs.
    trap - EXIT INT TERM

    stop_background_processes
    remove_valkey_temp_dir
    delete_supabase_branch

    exit "$exit_code"
}

trap cleanup EXIT INT TERM


# ---------------------------------------------------------------------------
# Valkey / Redis-compatible local queue backend
#
# Two independent safety questions:
#
# 1. Server process ownership (may I stop this server?):
#    - a server already responding at the configured URL is externally owned;
#      it is used as-is and is never stopped on cleanup;
#    - a server started by this script runs the `redis-server` binary
#      directly as an ephemeral child with persistence disabled and is
#      stopped on cleanup.
#
#    NEVER `brew services start redis/valkey`: that installs a persistent
#    Homebrew LaunchAgent (RunAtLoad/KeepAlive) which outlives devserver and
#    survives shutdown. No Homebrew mutation is performed here.
#
# 2. Logical DB reset (may I reset this selected development DB?):
#    the explicitly selected local VALKEY_URL namespace (default
#    redis://127.0.0.1:6379/2) is flushed before the worker starts in both
#    externally-owned and devserver-owned cases. Non-local URLs are never
#    flushed. `--no-valkey` disables both automatic start and reset.
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
        warn "redis-cli not found; cannot actively verify queue backend readiness."
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
    local valkey_port

    if [ "$AUTO_VALKEY" != true ]; then
        log "Auto-Valkey disabled. Expecting queue backend at: $(redact_valkey_url "$valkey_url")"
        return
    fi

    if ! is_local_valkey_url "$valkey_url"; then
        log "Using non-local queue backend from environment: $(redact_valkey_url "$valkey_url")"
        return
    fi

    if command -v redis-cli >/dev/null 2>&1; then
        if redis-cli -u "$valkey_url" ping >/dev/null 2>&1; then
            log "Queue backend already responding at $valkey_url (externally owned; devserver will not stop it on exit)"
            VALKEY_EXTERNALLY_OWNED=true
            return
        fi
    fi

    # Nothing is responding: start the installed redis-server binary directly
    # as an ephemeral devserver-owned process.
    #
    # - running it in the foreground of this subshell with `exec` makes "$!"
    #   the real redis-server PID, so the shared register_background_pid/stop
    #   machinery owns exactly this process (reverse-order cleanup stops it
    #   after the worker/API that depend on it);
    # - persistence is disabled (`--save ''`, `--appendonly no`) and all
    #   transient files stay in this invocation's /tmp/openorc-redis-$$/
    #   namespace, never inside the repository;
    # - the ownership sanity guard below fails loudly if a foreign server
    #   grabbed the port during startup, instead of silently adopting (and
    #   later killing) someone else's server.
    if ! command -v redis-server >/dev/null 2>&1; then
        die "No queue backend is responding at $(redact_valkey_url "$valkey_url") and redis-server is not on PATH. Start a local Redis-compatible server or point VALKEY_URL at a running instance."
    fi

    valkey_port="${BASH_REMATCH[2]}"

    # redis-server --dir requires the directory to exist.
    mkdir -p "$VALKEY_TMP_DIR"
    VALKEY_TMP_DIR_CREATED=true
    : > "$VALKEY_LOG_PATH"

    log "Starting devserver-owned queue backend (redis-server) on 127.0.0.1:$valkey_port..."
    log "Queue backend logs: $VALKEY_LOG_PATH"

    (
        exec redis-server \
            --port "$valkey_port" \
            --bind 127.0.0.1 \
            --save '' \
            --appendonly no \
            --dir "$VALKEY_TMP_DIR" \
            >>"$VALKEY_LOG_PATH" 2>&1 < /dev/null
    ) &
    VALKEY_OWNED_PID="$!"
    register_background_pid "$VALKEY_OWNED_PID" "queue-backend"

    if ! wait_for_valkey "$valkey_url"; then
        die "devserver-started queue backend did not become ready at $(redact_valkey_url "$valkey_url"); see $VALKEY_LOG_PATH"
    fi

    # Ownership sanity guard: if a foreign server grabbed the port during
    # startup, our instance would exit (bind conflict) within milliseconds
    # while the foreign instance answers the ping. Poll briefly so that
    # death is observable, then fail loudly instead of silently adopting
    # (and later killing) someone else's server. A healthy instance simply
    # survives the window.
    guard_attempt=0
    while [ "$guard_attempt" -lt 10 ]; do
        if ! kill -0 "$VALKEY_OWNED_PID" >/dev/null 2>&1; then
            die "Queue backend answered at $(redact_valkey_url "$valkey_url") but the devserver-started instance exited; another server may already own the port. Devserver will not adopt it; see $VALKEY_LOG_PATH"
        fi
        sleep 0.1
        guard_attempt=$((guard_attempt + 1))
    done

    log "Queue backend started (devserver-owned PID $VALKEY_OWNED_PID; stopped automatically on exit)"
}

flush_local_valkey_db() {
    local valkey_url="$1"

    if [ "$AUTO_VALKEY" != true ]; then
        return
    fi

    # Never flush anything that is not unambiguously localhost: server
    # process ownership never grants flush rights; only the explicitly
    # selected local VALKEY_URL namespace does.
    if ! is_local_valkey_url "$valkey_url"; then
        return
    fi

    require_command redis-cli

    if ! redis-cli -u "$valkey_url" ping >/dev/null 2>&1; then
        die "Cannot flush local queue DB because the queue backend is not responding at $(redact_valkey_url "$valkey_url")."
    fi

    log "Flushing local OpenOrc queue DB: $valkey_url"

    redis-cli -u "$valkey_url" FLUSHDB >/dev/null
}

remove_valkey_temp_dir() {
    # Remove only this invocation's directory, only when this script created
    # it (a devserver-owned queue server). Invocation-specific paths make
    # concurrent devserver runs independent.
    if [ "$VALKEY_TMP_DIR_CREATED" != true ]; then
        return
    fi

    case "$VALKEY_TMP_DIR" in
        /tmp/openorc-redis-*/) : ;;
        *)
            warn "Refusing to remove unexpected queue temp dir: $VALKEY_TMP_DIR"
            return
            ;;
    esac

    rm -rf "$VALKEY_TMP_DIR"
    VALKEY_TMP_DIR_CREATED=false
}


# ---------------------------------------------------------------------------
# ngrok
#
# Requirements:
# - optional
# - only start when the API is included
# - only start if a reserved URL is configured
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

    (
        exec ngrok http \
            --url="$ngrok_url" \
            "$api_port" \
            >>"$NGROK_LOG_PATH" 2>&1 < /dev/null
    ) &

    register_background_pid "$!" "ngrok"
}


# ---------------------------------------------------------------------------
# API
#
# Real entrypoint established by the Python control-plane bootstrap:
# .venv/bin/python apps/api/main.py (uvicorn serving openorc.api.app:app,
# configured through OPENORC_API_HOST / OPENORC_API_PORT / OPENORC_API_RELOAD).
# The interpreter is always the repository .venv; require_base_tools fails
# closed with setup instructions when it is absent.
# ---------------------------------------------------------------------------

start_api_background() {
    local api_port="${LOCAL_API_PORT:-$DEFAULT_API_PORT}"

    log "Starting OpenOrc API in background (127.0.0.1:$api_port)..."

    (
        export OPENORC_ENV="${OPENORC_ENV:-development}"
        export OPENORC_API_PORT="$api_port"
        exec "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/apps/api/main.py"
    ) &

    register_background_pid "$!" "api"
}


# ---------------------------------------------------------------------------
# Worker
#
# Real entrypoint established by the RQ foundation: .venv/bin/python
# apps/worker/main.py (RQ worker over the canonical openorc: queues,
# backend selected by VALKEY_URL).
# ---------------------------------------------------------------------------

start_worker_background() {
    log "Starting OpenOrc worker in background..."

    (
        export OPENORC_ENV="${OPENORC_ENV:-development}"
        exec "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/apps/worker/main.py"
    ) &

    register_background_pid "$!" "worker"
}


# ---------------------------------------------------------------------------
# Foreground process selection
#
# The frontend (or API/worker in single-surface modes) runs as a tracked
# FOREGROUND CHILD, never via a top-level exec: the parent devserver shell
# must stay alive because it owns the EXIT/INT/TERM trap that stops children
# and deletes the ephemeral Supabase branch. `exec` inside the subshell
# replaces only the subshell, so the registered PID is the real service PID
# and the parent regains control as soon as the child exits or signals.
# ---------------------------------------------------------------------------

run_app_foreground() {
    local app_port="${LOCAL_APP_PORT:-$DEFAULT_APP_PORT}"
    local foreground_pid

    log "Starting OpenOrc app in foreground (0.0.0.0:$app_port)..."

    (
        cd "$ROOT_DIR/apps/app"
        exec npm run dev -- --host 0.0.0.0 --port "$app_port"
    ) &
    foreground_pid="$!"

    register_background_pid "$foreground_pid" "app-foreground"
    wait "$foreground_pid"
}

run_api_foreground() {
    local api_port="${LOCAL_API_PORT:-$DEFAULT_API_PORT}"
    local foreground_pid

    log "Running OpenOrc API in foreground (127.0.0.1:$api_port)..."

    (
        export OPENORC_ENV="${OPENORC_ENV:-development}"
        export OPENORC_API_PORT="$api_port"
        exec "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/apps/api/main.py"
    ) &
    foreground_pid="$!"

    register_background_pid "$foreground_pid" "api-foreground"
    wait "$foreground_pid"
}

run_worker_foreground() {
    local foreground_pid

    log "Running OpenOrc worker in foreground..."

    (
        export OPENORC_ENV="${OPENORC_ENV:-development}"
        exec "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/apps/worker/main.py"
    ) &
    foreground_pid="$!"

    register_background_pid "$foreground_pid" "worker-foreground"
    wait "$foreground_pid"
}


# ---------------------------------------------------------------------------
# Supabase environment preparation
# ---------------------------------------------------------------------------

prepare_supabase_environment() {
    if [ "$AUTO_SUPABASE" != true ]; then
        return
    fi

    create_supabase_branch
    wait_for_supabase_branch "$SUPABASE_BRANCH_NAME"
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


# Frontend API base: Vite exposes VITE_-prefixed variables from the process
# environment, and an exported value wins over apps/app/.env. Export the
# default only when the caller has not already provided one.

if [ "$INCLUDE_APP" = true ] && [ -z "${VITE_API_BASE_URL:-}" ]; then
    export VITE_API_BASE_URL="http://127.0.0.1:${LOCAL_API_PORT:-$DEFAULT_API_PORT}"
    log "Exported VITE_API_BASE_URL=$VITE_API_BASE_URL"
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
# Foreground process
#
# One service runs as a tracked foreground child so:
#
#   Ctrl-C / child exit / TERM
#      -> shell trap (parent devserver shell stays alive)
#      -> stop children (foreground first, queue backend last)
#      -> delete Supabase branch
#
# No daemon supervisor required for manual E2E.
# ---------------------------------------------------------------------------

if [ "$INCLUDE_APP" = true ]; then
    run_app_foreground
    exit 0
fi

if [ "$INCLUDE_API" = true ]; then
    run_api_foreground
    exit 0
fi

if [ "$INCLUDE_WORKER" = true ]; then
    run_worker_foreground
    exit 0
fi

die "Nothing selected to run."
