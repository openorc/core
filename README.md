# OpenOrc

OpenOrc turns Cline Hub into a governed autonomous software-engineering workflow.

Give OpenOrc a GitHub issue and it coordinates persistent Cline Producer and Reviewer sessions through planning, independent review, human authorization, implementation, pull-request review, remediation, and merge.

Cline remains responsible for the agent runtime, model/provider choice, tools, filesystem, and execution environment. OpenOrc is responsible for engineering workflow, authority, exact review subjects, GitHub reconciliation, and durable audit state. OpenOrc is deliberately Cline-first in v1 and runtime-neutral by architecture.

## Repository status

Phase 0 repository bootstrap and Phase 1 domain/persistence foundation are complete. The development environment, app/API/worker shells, CI baseline, durable control-plane model, Supabase migration workflow, Redis-compatible queue integration, and ephemeral local dev stack are established and operational.

Phase 2 is the current implementation focus: building the headless backend control plane and Cline/GitHub integration. OpenOrc remains under active development and is not yet a complete end-to-end product.

`openorc/core` is the complete source-available product. Self-hosting must remain complete.

```text
Phase 0  Repository foundation           ✓ complete
Phase 1  Domain + persistence            ✓ complete
Phase 2  Backend control plane            ← current
Phase 3  Real headless E2E
Phase 4  Frontend control surface
Phase 5  Product E2E + hardening
```

## How OpenOrc works

OpenOrc governs an issue-to-merge engineering workflow while leaving coding execution to connected agent runtimes and repository policy to GitHub.

```text
GitHub issue
↓
Producer prepares an implementation strategy
↓
Producer ↔ Reviewer bounded review loop
↓
Owner authorizes implementation
↓
Producer implements
↓
Owner authorizes PR creation
↓
OpenOrc creates the pull request
↓
Producer ↔ Reviewer review/remediate committed state
↓
Owner requests merge
↓
GitHub confirms merge
```

Automation handles coordination between those boundaries; consequential progression remains explicit Owner authority. Producer and Reviewer are independent logical roles with separate Task-scoped sessions. v1 deliberately uses Cline Hub for both roles: Cline provides persistent agent sessions, model/provider flexibility, tools, and execution, while OpenOrc governs how those sessions participate in the GitHub engineering lifecycle. The Connection/adapter boundary keeps the workflow architecture runtime-neutral without making adapter breadth a v1 objective.

The ownership split is deliberate: GitHub remains the durable engineering record for issues, committed code, pull requests, CI, and merge outcomes. OpenOrc owns deterministic workflow/control truth such as Task state, reviews, Owner gates, runtime-session bindings, and audit history. Connected agent runtimes own their conversational and execution context, tools, filesystem state, and runtime-owned credentials. In v1 those runtimes are bring-your-own, Owner-controlled execution infrastructure rather than part of the OpenOrc control plane.

## Architectural shape

```text
GitHub
  durable engineering record
        ⇅
OpenOrc control plane
  workflow / review / authority
  Vue + FastAPI + RQ
        │
  Postgres / Valkey
        │
   ┌────┴────┐
   ↓         ↓
Producer   Reviewer
   ↓         ↓
ClineAdapter
   ↓         ↓
 Cline Hub / Cline Hub
   agent runtime + execution
```

The repository structure is intentionally explicit:

```text
apps/app/                  Vue product SPA
apps/api/                  thin FastAPI process/bootstrap
apps/worker/               thin RQ process/bootstrap
src/openorc/domain/        product semantics and invariants
src/openorc/services/      shared application/use-case orchestration
src/openorc/api/           HTTP transport
src/openorc/workers/       queue transport
src/openorc/adapters/      external-system boundaries
src/openorc/persistence/   durable storage implementation
packages/cline-sdk-bridge/ thin Node/TypeScript @cline/sdk compatibility bridge
supabase/                  product-owned schema/migrations
tests/                     cross-cutting test suites
docs/                      design/bootstrap/reference documentation
```

API routers and worker jobs remain thin transports over shared services. Workflow meaning belongs in domain/services code. Adapters own external mechanics, not product authority. Durable workflow state belongs in Postgres, not queues or agent transcripts.

## Local development (Python)

The Python control plane targets Python 3.13. The repository commits a portable toolchain contract that developer workstations, agent runtimes, CI, and hosted deployment environments consume identically; only environment bootstrap differs:

- `pyproject.toml` — direct dependencies (exact-pinned) and tool configuration
- `uv.lock` — the committed, solver-resolved transitive dependency graph
- `.python-version` — the exact default interpreter version
- uv itself is version-pinned through `[tool.uv] required-version`; environments running lock or build operations must use that uv version

### One-time setup

```bash
# Install the uv version pinned by the repository (see [tool.uv] required-version).
uv --version   # must satisfy the repository requirement
uv sync        # creates or updates the local .venv from uv.lock (dev group included)
```

### Canonical commands

```bash
.venv/bin/python -m pytest                 # tests
.venv/bin/python -m ruff check .           # lint
.venv/bin/python -m ruff format --check .  # format check
.venv/bin/pyright                          # static type checks
.venv/bin/python apps/api/main.py          # API process (GET /healthz)
.venv/bin/python apps/worker/main.py       # RQ worker process
```

uv is the dependency/build tool, not a runtime dependency: environments that receive a prepared environment run the same entrypoints with their own interpreter (`python apps/api/main.py`). API and worker configuration comes from environment variables (see `.env.example`); neither process requires persistent local filesystem state.

### Queue backend

The worker connects to any Redis-compatible backend selected through `VALKEY_URL`; when unset, the application falls back to its built-in default (`redis://127.0.0.1:6379/0`).

- `openorc:` is the stable application-level RQ queue prefix. Deployment isolation is provided by the Redis/Valkey namespace selected by `VALKEY_URL`. In the current reference/local setup this may be a Redis DB index; another deployment may use a dedicated instance/service.
- The canonical default queue is `openorc:default` (defined in `src/openorc/workers/queues.py`); future OpenOrc queues derive from the same prefix.
- For local OpenOrc development, `VALKEY_URL=redis://127.0.0.1:6379/2` is the recommended namespace (see `.env.example`), keeping the local stack out of a Redis namespace shared with other local development tooling.
- No server binary or server version is pinned: the queue backend contract is Redis-compatibility through `VALKEY_URL`. The exact-pinned `rq` and `redis` Python packages are application dependencies under the repository dependency policy, not server-version indicators.
- RQ is asynchronous execution machinery, not workflow truth; durable workflow state belongs in Postgres, not queue payloads.

### Postgres (persistence)

API and worker processes access OpenOrc's dedicated `openorc` Postgres schema through shared persistence code (`src/openorc/persistence/`), using synchronous psycopg 3 with a bounded `psycopg_pool.ConnectionPool`. In devserver runs, `DATABASE_URL` is exported per-run from the ephemeral Supabase branch; when unset, the application falls back to the documented Supabase local-stack default (`postgresql://postgres:postgres@127.0.0.1:54322/postgres`).

- Pool bounds are **per process**, not deployment-wide: `OPENORC_DB_POOL_MIN` / `OPENORC_DB_POOL_MAX` (defaults 1 / 10) and `OPENORC_DB_POOL_TIMEOUT` seconds (default 30). Size deployments for process count × pool size.
- Connections are pooler-neutral by default (`prepare_threshold=None`; no session-local state), so direct, session-pooler, and transaction-pooler endpoints work identically.
- A pool belongs to exactly one OS process and is created by the process that uses it — never inherited or reused across an RQ fork boundary. The durable invariants live in `src/openorc/persistence/AGENTS.md`.

### Authentication (Supabase Auth)

API and worker authentication verifies Supabase Auth access tokens against the project's asymmetric signing keys (JWKS) and resolves the verified user to the canonical OpenOrc `Profile` (idempotently bootstrapped on first use; see `src/openorc/services/authentication.py`).

- `SUPABASE_URL` — the Supabase project URL; the Auth issuer (`<url>/auth/v1`) and JWKS source (`<url>/auth/v1/.well-known/jwks.json`) derive from it. In devserver runs it is exported per-run from the ephemeral branch; production deployments must supply it, and `OPENORC_ENV=production` fails startup when it is missing or malformed.
- `OPENORC_SUPABASE_JWT_AUDIENCE` — the expected authenticated audience (default `authenticated`).
- `SUPABASE_SECRET_KEY` — the deployment's Supabase secret API key (`sb_secret_...`), the non-JWT administrative credential for the server-side Auth Admin boundary (permanent account deletion; see `src/openorc/services/account_lifecycle.py`). Deliberately optional on the shared environment surface: it is required only by the component that constructs the Auth Admin client (that construction fails fast without it), so no other process — the worker included — needs to receive it. It travels on the `apikey` request header only, is never browser-visible, never persisted in `openorc.*` tables or Vault, and never logged.
- v1 is GitHub-only sign-in: configure Supabase Auth with GitHub as the only enabled user sign-in provider and asymmetric JWT signing keys (ES256/RS256). The verifier enforces the trusted `app_metadata` GitHub-origin checks as defense-in-depth and has no symmetric/HS256 shared-secret path.

### GitHub integration (OpenOrc GitHub App)

Durable repository automation authenticates as the deployed OpenOrc GitHub App and mints short-lived installation access tokens for the exact installation routed to each Repository (see `src/openorc/adapters/github/` and `src/openorc/services/github_repository_access.py`). Human GitHub sign-in is identity only: there is no PAT or human OAuth token fallback for repository operations anywhere in the boundary.

- `OPENORC_GITHUB_APP_ID` — the deployed OpenOrc GitHub App's numeric App ID. Optional on the shared environment surface: the requirement is enforced where the GitHub App client is constructed (`HttpGitHubAppClient.from_settings`), which fails fast without it, so unrelated processes never need it.
- `OPENORC_GITHUB_APP_PRIVATE_KEY` — the GitHub App's PEM private key. Deployment/bootstrap secret material: never Workspace data, never stored in `openorc.*` tables or Supabase Vault, never returned through ordinary APIs, never logged or attached to telemetry. Optional on the shared environment surface for the same construction-point reason; a supplied-but-blank value fails closed without echoing it.
- `OPENORC_GITHUB_WEBHOOK_SECRET` — the GitHub App's webhook secret. It authenticates inbound webhook deliveries at `POST /api/github/webhooks`: the exact raw request bytes are verified against the configured secret (SHA-256, constant-time comparison) before any payload-derived effect. The same secret-material discipline as the App private key applies (never Workspace data, never `openorc.*` tables or Vault, never returned, never logged or attached to telemetry; optional on the shared surface and enforced at the verification boundary, so an unconfigured deployment rejects webhook deliveries rather than accepting unverified ones). A re-delivered GitHub delivery GUID is acknowledged idempotently and durably deduplicated.
- The supported GitHub REST API version is pinned in one adapter location (`SUPPORTED_GITHUB_API_VERSION` in `src/openorc/adapters/github/transport.py`) and sent on every request through `X-GitHub-Api-Version`.
- Installation tokens and the App private key are held in memory only, redacted in ordinary representations, and never persisted in domain objects, events, logs, or telemetry attributes.

### Application observability (OpenTelemetry)

OpenOrc's own Python control-plane code emits operational telemetry through OpenTelemetry with OTLP as the vendor-neutral export boundary (`OpenOrc API / worker / services / adapters → OpenTelemetry traces + logs + metrics → OTLP → deployment-selected collector/backend`). The destination is deployment configuration, not a core dependency; core ships no backend-specific SDK.

- `OPENORC_OTLP_ENDPOINT` — optional OTLP collector base URL (http/https). Per-signal paths (`/v1/traces`, `/v1/logs`, `/v1/metrics`) are derived from it. Unset means unconfigured telemetry: processes run with the local stderr log baseline and no OpenTelemetry export runtime. Malformed values fail startup like other configuration errors.
- API and worker processes share one contract with distinct service/resource identity (`openorc-api` / `openorc-worker`, `service.version`, deployment environment). Initialization happens exactly once per serving process and is terminal after shutdown; runtime export failures never crash the application.
- Every API response carries an `x-openorc-request-id` header — a safe opaque correlation ID for operator/frontend diagnostics that is observational only (never authority or idempotency). Ordinary Python `logging` participates in the configured pipeline and correlates with the active trace/span context.
- Durable `WorkflowEvent` audit history remains in Postgres and is not operational logging; connected-runtime telemetry stays runtime-owned. See `src/openorc/AGENTS.md` for the canonical rules.

## Local development (frontend)

`apps/app` is the product SPA (Vue 3 + Vite + TypeScript) and targets Node 24. Its toolchain contract mirrors the Python one:

- `apps/app/package.json` — direct dependencies (exact-pinned), scripts, and toolchain declarations
- `apps/app/package-lock.json` — the committed, resolved transitive dependency graph
- `apps/app/.node-version` — the exact Node 24 development/default patch
- npm-native `devEngines` enforces the supported Node 24 line and the exact npm version before `install`, `ci`, and `run` commands; `"packageManager"` is ecosystem metadata only
- `apps/app/.npmrc` pins `save-exact=true` so dependencies stay exact-pinned

### One-time setup

```bash
# Use the Node 24 patch from apps/app/.node-version (for example via nvm).
node --version   # must satisfy apps/app/package.json engines/devEngines
cd apps/app
npm ci           # installs exactly from package-lock.json
```

### Canonical commands

```bash
cd apps/app
npm run dev        # development server
npm run build      # type-check + production build (dist/)
npm run typecheck  # vue-tsc strict type checks
npm test           # Vitest
```

### API base configuration

The SPA never assumes it is served by the API process or that API calls are same-origin. The API base URL is externalized through `VITE_API_BASE_URL` (see `apps/app/.env.example`); an unset or invalid value fails closed when the application needs the API base URL rather than silently falling back to same-origin behavior.

## Local development (Supabase migrations)

`supabase/` owns the product Postgres schema. Repository migrations are the authoritative schema-evolution source (see `supabase/AGENTS.md`); the supported developer workflow is documented in [`docs/supabase-migrations.md`](docs/supabase-migrations.md).

The Supabase CLI version is pinned exactly in `supabase/cli-version` and is enforced (strict equality, no version floor) by the tooling:

```bash
supabase migration new <name>                                       # author a migration
scripts/supabase-apply-migrations.sh --dry-run --branch my-branch   # validate (no writes)
scripts/supabase-apply-migrations.sh --branch my-branch             # apply to a non-production branch
```

The tooling refuses any target that cannot be reliably proven non-production; applying migrations to production is intentionally outside this developer workflow. Configuration (project refs, access token) comes from the environment (see `.env.example`); credentials are never committed.

## Continuous integration

The `CI` GitHub Actions workflow (`.github/workflows/ci.yml`) runs the same canonical commands developers run locally. It triggers on pull requests to `main`, pushes to `main`, and manual dispatch. Baseline runs are deterministic: they require no secrets and no live Supabase, Valkey, Cline Hub, GitHub-integration, or other external infrastructure.

The workflow has two jobs. Their display names are the stable names to configure as required status checks when branch protection is enabled:

- **Python checks** (`python-checks`) — `uv sync --locked` (verifies the committed `pyproject.toml`/`uv.lock` pair is consistent rather than re-resolving it), then pytest, `ruff check`, `ruff format --check`, and pyright. The uv and Python versions come from the repository toolchain contract: the setup-uv action reads `[tool.uv] required-version` from `pyproject.toml`, and uv provisions the interpreter from `.python-version`.
- **Frontend checks** (`frontend-checks`) — bootstraps the exact npm version declared in `apps/app/package.json` (`devEngines`) before any app-local npm command, runs `npm ci` from the committed lockfile, then `typecheck`, Vitest, and the production build. Node resolves from `apps/app/.node-version`.

Third-party actions are pinned to the immutable commit SHA of their current stable release, with the release version noted in a comment. Dependency caches (uv/npm) accelerate runs but are never required for correctness: installation always verifies against the committed lockfiles.

The ordinary pytest baseline excludes tests marked `integration` by default; `pytest -m integration` overrides that explicitly (see [`tests/README.md`](tests/README.md)).

## Manual local E2E (devserver)

`scripts/devserver.sh` starts the full local stack against a fresh ephemeral hosted Supabase branch. It is a **human-run** tool for the Owner's manual E2E verification; agents do not run it during ordinary implementation work.

The orchestration is implemented in Python (`src/openorc/devtools/devserver/`, a package of focused modules: CLI, environment, logging, subprocess mechanics, Supabase branch lifecycle, queue-backend ownership, and the orchestrator); `scripts/devserver.sh` is a deliberately small stable wrapper that resolves the repository root, requires the repository `.venv` (`uv sync`), and execs the Python entrypoint with all arguments unchanged. Signals (Ctrl-C/SIGTERM) only request shutdown — exactly one dependency-aware cleanup path performs all teardown.

```bash
bash scripts/devserver.sh                 # full stack: Supabase branch + queue + API + worker + app (foreground)
bash scripts/devserver.sh --help          # component modes, --testdb, --no-* controls, --keep-supabase
```

What a default run does:

1. creates a unique ephemeral Supabase branch from `OPENORC_SUPABASE_PROJECT_REF`;
2. waits for genuine branch readiness (published credentials plus a live database, bounded timeout);
3. exports branch credentials (`SUPABASE_URL`, `SUPABASE_PUBLISHABLE_KEY`, `SUPABASE_SECRET_KEY`, `DATABASE_URL`) to the stack process tree only — never to tracked files;
4. applies current-checkout migrations through `scripts/supabase-apply-migrations.sh` (fails hard on error) and `supabase/seed.sql` when present;
5. ensures the local Redis-compatible queue backend (`VALKEY_URL`, default `redis://127.0.0.1:6379/2`) and flushes the selected local DB before the worker starts;
6. starts the API and RQ worker from the repository `.venv` (`uv sync`), and runs the Vue dev server in the foreground;
7. on any exit (including Ctrl-C or failure) runs a single dependency-aware cleanup: the app/worker/API stop first, the worker is allowed to finish its RQ shutdown while the queue backend is still alive, a devserver-started queue server stops only after that, and only the branch created by that run is deleted.

Lifecycle and safety properties:

- A queue server already responding at `VALKEY_URL` is reused and left running; the selected local DB is still flushed. A devserver-started queue server runs `redis-server` directly (never `brew services`) with persistence disabled and is stopped on exit. Non-local URLs are never flushed or managed.
- The production Supabase project is never a development target: the stack only consumes credentials of the branch created by the run, and the migration tooling re-verifies non-production identity.
- `--keep-supabase` preserves the branch for debugging and says so loudly (branches cost compute; delete them manually).

Prerequisites: pinned Supabase CLI (see `docs/supabase-migrations.md`), `.env` with `OPENORC_SUPABASE_PROJECT_REF` (and optionally `OPENORC_SUPABASE_PRODUCTION_PROJECT_REF`, `SUPABASE_ACCESS_TOKEN`), `.venv` from `uv sync`, local `redis-server`, npm dependencies, and optionally `NGROK_RESERVED_URL` for webhook/callback testing.

### Running the integration suite against an ephemeral branch (`--testdb`)

`--testdb` reuses the same ephemeral branch lifecycle for an explicit Owner-controlled persistence workflow: it provisions a non-production branch, applies committed migrations (never seed data), runs the canonical persistence integration suite, and deletes the branch on exit:

```bash
./scripts/devserver.sh --testdb
```

Supply a command after `--` to run something else against the same branch instead (for example, one integration module):

```bash
./scripts/devserver.sh --testdb -- \
  .venv/bin/python -m pytest -o addopts=--strict-markers \
  -m integration tests/integration/test_task_agent_session_persistence.py
```

The built-in suite command targets the whole `tests/integration` directory and replaces the repository pytest marker defaults (`-o addopts=--strict-markers`) so the integration tests are actually selected while strict marker checking is preserved; ordinary runs keep excluding them and remain DB-free. Future integration modules are included by the directory automatically.

The command receives `OPENORC_TEST_DATABASE_URL` (the branch database URL, injected into its environment only and never logged). Nothing else starts in this mode: no app, API, worker, queue backend, or ngrok. Surface-selection flags cannot be combined with `--testdb`; `--keep-supabase` keeps the branch after the run as the existing debug escape hatch. See `tests/README.md` for the integration-suite contract.

## Agent context

Repository implementation guidance is carried by the root `AGENTS.md` plus nested `AGENTS.md` files at architectural boundaries. Agents working across boundaries must read every applicable local guide before editing.

## License

Copyright © 2026 Oliver Cheung. OpenOrc is licensed under the Elastic License 2.0 (ELv2). You may use, modify, redistribute, and self-host OpenOrc, including for commercial internal use and to build commercial products. Providing OpenOrc itself as a hosted or managed service requires a separate commercial license. See `LICENSE`.
