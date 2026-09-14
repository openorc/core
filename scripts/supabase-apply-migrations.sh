#!/bin/bash

# supabase-apply-migrations.sh
#
# Repository-native developer command for applying the CURRENT CHECKOUT's
# supabase/migrations/* to an explicitly designated NON-PRODUCTION Supabase
# target.
#
# Migration-first policy (authoritative: supabase/AGENTS.md):
#
#   supabase/migrations/ is the single source of schema evolution.
#   Migrations are authored here first, reviewed with the code change, and
#   then applied to deployed environments through supported tooling.
#
#   This script never mutates a production target. Any target that cannot
#   be reliably proven non-production is refused (fail closed). There is no
#   override flag; production application belongs to openorc/cloud, not to
#   this repository's development workflow.
#
# Designed for direct developer use and for later invocation by
# scripts/devserver.sh (issue #10). Branch creation/deletion and credential
# export for the local stack remain devserver responsibilities.
#
# Usage:
#   scripts/supabase-apply-migrations.sh [--dry-run] (--branch NAME | --db-url URL)
#
# Environment (contract documented in .env.example; .env at the repo root is
# loaded first, exported variables win):
#
#   SUPABASE_ACCESS_TOKEN                    optional; a stored `supabase
#                                            login` also authenticates the
#                                            CLI for --branch mode
#   OPENORC_SUPABASE_PROJECT_REF             parent project for --branch mode
#   OPENORC_SUPABASE_PRODUCTION_PROJECT_REF  production identity guard;
#                                            required for non-loopback
#                                            --db-url targets
#
# The exact Supabase CLI version pinned in supabase/cli-version is enforced
# with strict equality; any other version is refused.

set -euo pipefail


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI_VERSION_FILE="$ROOT_DIR/supabase/cli-version"

cd "$ROOT_DIR"


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# Bounded wait for a freshly created branch to publish its database
# credentials (a reliable readiness signal: production/main branches never
# expose database credentials through the API).
# The timing constants are overridable for deterministic tests; the defaults
# are the supported developer values and are not part of the .env contract.
BRANCH_WAIT_MAX_ATTEMPTS="${OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS:-60}"
BRANCH_WAIT_SLEEP_SECONDS="${OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS:-5}"

DRY_RUN=false


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log() {
    echo "[supabase-migrations] $*"
}

warn() {
    echo "[supabase-migrations] WARNING: $*" >&2
}

die() {
    echo "[supabase-migrations] ERROR: $*" >&2
    exit 1
}


# ---------------------------------------------------------------------------
# Environment file
#
# Same semantics as scripts/devserver.sh:
#   - `.env` at the repository root is git-ignored.
#   - Blank lines and `#` comments are skipped.
#   - Variables already exported in the calling shell win over `.env`.
# ---------------------------------------------------------------------------

load_env_file() {
    local env_file="$ROOT_DIR/.env"
    local line key value

    if [ ! -f "$env_file" ]; then
        return 0
    fi

    while IFS= read -r line || [ -n "$line" ]; do
        line="${line#"${line%%[![:space:]]*}"}"

        case "$line" in
            ''|'#'*) continue ;;
        esac

        key="${line%%=*}"
        value="${line#*=}"

        case "$key" in
            ''|[0-9]*|*[!A-Za-z0-9_]*)
                warn "Ignoring invalid .env line: $key"
                continue
                ;;
        esac

        if [ "${#value}" -ge 2 ]; then
            case "$value" in
                \"*\") value="${value#\"}"; value="${value%\"}" ;;
                \'*\') value="${value#\'}"; value="${value%\'}" ;;
            esac
        fi

        if [ -z "${!key+x}" ]; then
            export "$key=$value"
        fi
    done < "$env_file"
}

load_env_file


# ---------------------------------------------------------------------------
# CLI version pin (strict equality)
#
# supabase/cli-version pins the exact supported Supabase CLI version,
# verified against the upstream stable release at pin time. Per repository
# dependency policy this is an exact pin, not a minimum-version floor: any
# other installed version is refused.
#
# The installed CLI must self-report the pinned version exactly. Only
# harmless normalization is applied to the reported identity:
#   - surrounding whitespace is trimmed;
#   - one fixed optional prefix is stripped (the binary's self-name
#     "supabase"/"Supabase", or a leading "v").
# The full semantic version identity — including prerelease/build suffixes
# such as 2.117.0-beta.1 — must otherwise match the pin exactly, so a
# prerelease is never truncated into a matching release. A failing
# `supabase --version` is a hard failure.
# ---------------------------------------------------------------------------

require_command() {
    local command_name="$1"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        die "Required command not found on PATH: $command_name"
    fi
}

# Trim leading and trailing whitespace.
trim() {
    local value="$1"

    value="${value#"${value%%[![:space:]]*}"}"
    printf '%s' "${value%"${value##*[![:space:]]}"}"
}

require_pinned_cli_version() {
    local pinned installed_raw normalized
    local semver_re='^[0-9]+(\.[0-9]+)+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$'

    if [ ! -f "$CLI_VERSION_FILE" ]; then
        die "CLI version pin file missing: supabase/cli-version"
    fi

    pinned="$(tr -d '[:space:]' < "$CLI_VERSION_FILE")"

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
# URL helpers
#
# Only shapes that allow reliable identity reasoning are recognized.
# Supabase project refs are 20-character lowercase alphanumeric IDs.
# Generated credentials are never logged; URLs are printed redacted.
# ---------------------------------------------------------------------------

url_userinfo() {
    local url="$1"
    local rest

    case "$url" in
        postgresql://*|postgres://*|https://*)
            rest="${url#*://}"
            ;;
        *)
            printf ''
            return
            ;;
    esac

    case "$rest" in
        *@*)
            printf '%s' "${rest%%@*}"
            ;;
        *)
            printf ''
            ;;
    esac
}

url_host() {
    local url="$1"
    local rest host

    case "$url" in
        postgresql://*|postgres://*|https://*)
            rest="${url#*://}"
            ;;
        *)
            printf ''
            return
            ;;
    esac

    case "$rest" in
        *@*) rest="${rest#*@}" ;;
    esac

    host="${rest%%[/?:]*}"
    host="${host#[}"
    host="${host%]}"

    printf '%s' "$host"
}

is_loopback_host() {
    local host="$1"

    case "$host" in
        localhost|::1|127.0.0.1) return 0 ;;
    esac

    [[ "$host" =~ ^127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]] && return 0

    return 1
}

# Extract the Supabase project ref embedded in a URL when the URL shape makes
# it unambiguous:
#   - pooler userinfo:  postgres.<ref>@...
#   - direct host:      db.<ref>.supabase.co
#   - api host:         <ref>.supabase.co
# Prints nothing when no ref can be reliably extracted.
extract_supabase_ref() {
    local url="$1"
    local host userinfo

    host="$(url_host "$url")"
    userinfo="$(url_userinfo "$url")"

    if [[ "$userinfo" =~ ^postgres\.([a-z0-9]{20})(:|$) ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
        return
    fi

    if [[ "$host" =~ ^db\.([a-z0-9]{20})\.supabase\.co$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
        return
    fi

    if [[ "$host" =~ ^([a-z0-9]{20})\.supabase\.co$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
        return
    fi

    printf ''
}

redact_url() {
    local url="$1"
    local scheme host

    case "$url" in
        postgresql://*) scheme="postgresql://" ;;
        postgres://*)   scheme="postgres://" ;;
        https://*)      scheme="https://" ;;
        *) printf '<unrecognized-url>'; return ;;
    esac

    host="$(url_host "$url")"

    if [ -z "$host" ]; then
        printf '%s<redacted>' "$scheme"
        return
    fi

    printf '%s%s' "$scheme" "$host"
}


# ---------------------------------------------------------------------------
# Production guard
#
# The resolved write target must be provably non-production:
#   - loopback targets are non-production by construction;
#   - hosted targets are checked against
#     OPENORC_SUPABASE_PRODUCTION_PROJECT_REF.
#
# The branch PARENT project may legitimately be the hosted production
# project (that is how Supabase preview branching works); only reads
# (branch resolution) touch the parent, and that case is warned about.
# Migrations are always written to the branch database itself, never to the
# parent project.
# ---------------------------------------------------------------------------

require_branch_configuration() {
    if [ -z "${OPENORC_SUPABASE_PROJECT_REF:-}" ]; then
        die "OPENORC_SUPABASE_PROJECT_REF is required for --branch mode."
    fi

    # Authentication is delegated entirely to the Supabase CLI. An exported
    # SUPABASE_ACCESS_TOKEN is the intended non-interactive path (Cline Hub /
    # self-hosted); a stored `supabase login` also works for interactive use.
    # No account-wide probe is performed here: authentication and
    # authorization failures surface through the actual branch operation
    # (see wait_for_branch_database), so scoped tokens work.

    if [ -n "${OPENORC_SUPABASE_PRODUCTION_PROJECT_REF:-}" ] \
        && [ "${OPENORC_SUPABASE_PROJECT_REF:-}" = "${OPENORC_SUPABASE_PRODUCTION_PROJECT_REF:-}" ]; then
        warn "Branch parent project equals the configured production project ref."
        warn "Expected when preview branches are hosted on the production project; writes still target only the branch database."
    fi
}

require_non_production_branch() {
    local branch_name="$1"
    local branch_project_ref="$2"
    local production_ref="${OPENORC_SUPABASE_PRODUCTION_PROJECT_REF:-}"

    if [ -n "$production_ref" ] && [ "$branch_project_ref" = "$production_ref" ]; then
        die "Refusing to apply migrations: the resolved branch belongs to the configured production project ($production_ref)."
    fi
}

require_non_production_db_url() {
    local db_url="$1"
    local host target_ref production_ref="${OPENORC_SUPABASE_PRODUCTION_PROJECT_REF:-}"

    host="$(url_host "$db_url")"

    if is_loopback_host "$host"; then
        log "Target is loopback ($host): non-production by construction."
        return
    fi

    if [ -z "$production_ref" ]; then
        die "Refusing non-loopback --db-url target: OPENORC_SUPABASE_PRODUCTION_PROJECT_REF is not configured, so the target cannot be proven non-production. Set it in your environment (see .env.example)."
    fi

    target_ref="$(extract_supabase_ref "$db_url")"

    if [ -z "$target_ref" ]; then
        die "Refusing non-loopback --db-url target: a project ref could not be reliably extracted from $(redact_url "$db_url"), so the target cannot be proven non-production."
    fi

    if [ "$target_ref" = "$production_ref" ]; then
        die "Refusing non-loopback --db-url target: it embeds the configured production project ref."
    fi

    log "Target project ref $target_ref verified as non-production."
}


# ---------------------------------------------------------------------------
# Branch target resolution
#
# Machine-readable CLI output only (`-o env`). The branch's database URL is
# the write target; the branch's own project identity is checked against the
# production guard. Generated branch credentials are consumed in-process and
# are never printed or written anywhere.
#
# Readiness is verified with an actual database connection (`supabase
# migration list`): publishing credentials alone is NOT sufficient - hosted
# branches expose credentials before their database host resolves.
# ---------------------------------------------------------------------------

# Populates from `supabase branches get ... -o env`:
#   BRANCH_POSTGRES_URL  direct (non-pooling) Postgres URL of the branch
#   BRANCH_API_URL       https API URL of the branch
#   BRANCH_PROJECT_REF   the branch's own project ref
#   BRANCH_GET_STDERR    sanitized stderr from the last CLI invocation
# Returns:
#   0  credentials and identity resolved
#   1  CLI invocation failed (BRANCH_GET_STDERR holds the sanitized stderr)
#   2  branch exists but database credentials are not published yet
#   3  credentials published but the branch identity could not be determined
fetch_branch_environment() {
    local branch_name="$1"
    local err_file raw line key value status=0

    err_file="$(mktemp "${TMPDIR:-/tmp}/openorc-branch-get.XXXXXX")"
    raw="$(supabase branches get "$branch_name" \
        --project-ref "${OPENORC_SUPABASE_PROJECT_REF:-}" \
        -o env 2>"$err_file")" || status=$?
    BRANCH_GET_STDERR="$(sanitize_cli_error "$(cat "$err_file")")"
    rm -f "$err_file"

    if [ "$status" != 0 ]; then
        return 1
    fi

    BRANCH_POSTGRES_URL=""
    BRANCH_API_URL=""

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
            POSTGRES_URL_NON_POOLING) BRANCH_POSTGRES_URL="$value" ;;
            SUPABASE_URL)             BRANCH_API_URL="$value" ;;
        esac
    done <<< "$raw"

    if [ -z "$BRANCH_POSTGRES_URL" ]; then
        return 2
    fi

    BRANCH_PROJECT_REF="$(extract_supabase_ref "${BRANCH_API_URL:-$BRANCH_POSTGRES_URL}")"

    if [ -z "$BRANCH_PROJECT_REF" ]; then
        return 3
    fi
}

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

# Bounded wait around the branch operation. Distinguishes:
#   - authentication/authorization failure  -> fail immediately;
#   - credentials not published yet         -> retry;
#   - credentials published, DB not ready   -> retry;
#   - database answers                      -> proceed.
wait_for_branch_database() {
    local branch_name="$1"
    local attempt=1 reason="" status=0

    while [ "$attempt" -le "$BRANCH_WAIT_MAX_ATTEMPTS" ]; do
        status=0
        fetch_branch_environment "$branch_name" || status=$?

        case "$status" in
            0)
                if supabase migration list --db-url "$BRANCH_POSTGRES_URL" >/dev/null 2>&1 </dev/null; then
                    return 0
                fi
                reason="database not answering yet"
                ;;
            2)
                reason="database credentials not published yet"
                ;;
            3)
                reason="branch identity not determinable from CLI output"
                ;;
            *)
                if branch_get_error_is_auth "$BRANCH_GET_STDERR"; then
                    die "Supabase rejected the branch operation for '$branch_name' (authentication/authorization): ${BRANCH_GET_STDERR:-<no diagnostic>}
Check that SUPABASE_ACCESS_TOKEN (or your stored CLI login) is valid and authorized for the parent project."
                fi
                reason="branch lookup failed transiently"
                ;;
        esac

        if [ "$attempt" -lt "$BRANCH_WAIT_MAX_ATTEMPTS" ]; then
            log "Branch '$branch_name' not ready ($reason; attempt $attempt/$BRANCH_WAIT_MAX_ATTEMPTS); waiting ${BRANCH_WAIT_SLEEP_SECONDS}s..."
            sleep "$BRANCH_WAIT_SLEEP_SECONDS"
        fi

        attempt=$((attempt + 1))
    done

    die "Branch '$branch_name' did not become ready within $((BRANCH_WAIT_MAX_ATTEMPTS * BRANCH_WAIT_SLEEP_SECONDS))s (last status: $reason). It may still be provisioning, or it may be the production/main branch (whose database credentials are never retrievable)."
}

resolve_branch_target() {
    local branch_name="$1"

    # Refuse the production branch identity before any CLI call.
    if [ "$branch_name" = "main" ]; then
        die "Refusing to apply migrations to branch 'main': that is the production branch identity."
    fi

    require_branch_configuration

    log "Resolving branch '$branch_name' under parent project ${OPENORC_SUPABASE_PROJECT_REF}..."

    wait_for_branch_database "$branch_name"

    require_non_production_branch "$branch_name" "$BRANCH_PROJECT_REF"

    TARGET_URL="$BRANCH_POSTGRES_URL"
    log "Branch target resolved: $(redact_url "$TARGET_URL")"
}


# ---------------------------------------------------------------------------
# Migration application
#
# Strict `db push`:
#   - no --include-all (it would silently mask remote/local history drift);
#   - stdin closed so interactive prompts fail instead of hanging;
#   - an empty local migration set is a clean no-op (migration-first: the
#     current checkout has nothing to apply).
# ---------------------------------------------------------------------------

apply_migrations() {
    local target_url="$1"
    local dry_run_flags=()

    if [ "$DRY_RUN" = true ]; then
        dry_run_flags=(--dry-run)
        log "Dry run: showing migrations that would be applied to $(redact_url "$target_url")..."
    else
        log "Applying supabase/migrations from the current checkout to $(redact_url "$target_url")..."
    fi

    if [ ! -d "$ROOT_DIR/supabase/migrations" ]; then
        die "supabase/migrations/ not found in the current checkout."
    fi

    if ! compgen -G "$ROOT_DIR/supabase/migrations/*.sql" >/dev/null; then
        log "No migration files in supabase/migrations/; nothing to apply."
        return 0
    fi

    # The guarded expansion keeps `set -u` happy on bash 3.2 (macOS default)
    # when the dry-run array is empty.
    if ! supabase db push \
        --db-url "$target_url" \
        ${dry_run_flags[@]+"${dry_run_flags[@]}"} \
        </dev/null; then
        die "Migrations did not apply cleanly; failing hard (migration-first policy)."
    fi

    if [ "$DRY_RUN" = true ]; then
        log "Dry run complete; nothing was applied."
    else
        log "Migrations applied successfully."
    fi
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: scripts/supabase-apply-migrations.sh [--dry-run] (--branch NAME | --db-url URL)

Apply the current checkout's supabase/migrations/* to an explicitly
designated NON-PRODUCTION Supabase target.

Options:
  --dry-run       Show migrations that would be applied; apply nothing.
  --branch NAME   Target the named preview branch hosted under
                  OPENORC_SUPABASE_PROJECT_REF.
  --db-url URL    Target a Postgres URL that is provably non-production
                  (loopback host, or Supabase-hosted URL whose embedded
                  project ref differs from
                  OPENORC_SUPABASE_PRODUCTION_PROJECT_REF).
  -h, --help      Show this help.

Environment:
  SUPABASE_ACCESS_TOKEN                    intended non-interactive auth path
                                           (Cline Hub / self-hosted); a stored
                                           `supabase login` also works
  OPENORC_SUPABASE_PROJECT_REF             parent project for --branch mode
  OPENORC_SUPABASE_PRODUCTION_PROJECT_REF  production identity guard;
                                           required for non-loopback
                                           --db-url targets

The exact Supabase CLI version pinned in supabase/cli-version is enforced.
See docs/supabase-migrations.md for the full workflow.
USAGE
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --dry-run)
                DRY_RUN=true
                ;;
            --branch)
                [ $# -ge 2 ] || die "--branch requires a name argument."
                BRANCH_TARGET="$2"
                shift
                ;;
            --branch=*)
                BRANCH_TARGET="${1#--branch=}"
                ;;
            --db-url)
                [ $# -ge 2 ] || die "--db-url requires a connection string."
                DB_URL_TARGET="$2"
                shift
                ;;
            --db-url=*)
                DB_URL_TARGET="${1#--db-url=}"
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

    if [ -n "${BRANCH_TARGET:-}" ] && [ -n "${DB_URL_TARGET:-}" ]; then
        die "Provide exactly one target: --branch or --db-url."
    fi

    if [ -z "${BRANCH_TARGET:-}" ] && [ -z "${DB_URL_TARGET:-}" ]; then
        die "No target specified. Provide --branch NAME or --db-url URL; refusing to guess a target (fail closed)."
    fi

    require_command supabase
    require_pinned_cli_version

    if [ -n "${BRANCH_TARGET:-}" ]; then
        resolve_branch_target "$BRANCH_TARGET"
    else
        require_non_production_db_url "$DB_URL_TARGET"
        TARGET_URL="$DB_URL_TARGET"
    fi

    apply_migrations "$TARGET_URL"
}

main "$@"