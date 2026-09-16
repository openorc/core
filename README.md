# OpenOrc

OpenOrc is an open, provider-neutral control plane for governed agentic software development.

It keeps the engineering workflow stable while coding runtimes, models, inference providers, and review systems remain replaceable. OpenOrc coordinates GitHub-backed work, independent Producer/Reviewer roles, explicit human authority, Task-scoped runtime sessions, deterministic workflow state, pull-request review, and audit history. It does not replace coding agents, GitHub, CI, or observability systems.

## Repository status

Phase 0 repository bootstrap is complete. The development environment, app/API/worker shells, CI baseline, Supabase migration workflow, Redis-compatible queue integration, and ephemeral local dev stack are established and operational.

Phase 1 is the current implementation focus: building OpenOrc's durable domain and persistence foundation. OpenOrc remains under active development and is not yet a complete end-to-end product.

`openorc/core` is the complete source-available product. Self-hosting must remain complete.

```text
Phase 0  Repository foundation           ✓ complete
Phase 1  Domain + persistence            ← current
Phase 2  Backend control plane
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

Automation handles coordination between those boundaries; consequential progression remains explicit Owner authority. Producer and Reviewer are independent logical roles with separate Task-scoped sessions. v1 uses Cline Hub for both roles through `ClineAdapter`, while the workflow architecture keeps the connected runtime replaceable behind the Connection/adapter boundary.

The ownership split is deliberate: GitHub remains the durable engineering record for issues, committed code, pull requests, CI, and merge outcomes. OpenOrc owns deterministic workflow/control truth such as Task state, reviews, Owner gates, runtime-session bindings, and audit history. Connected agent runtimes own their conversational and execution context, tools, filesystem state, and runtime-owned credentials. In v1 those runtimes are bring-your-own, Owner-controlled execution infrastructure rather than part of the OpenOrc control plane.

## Architectural shape

```text
GitHub
  durable engineering record
        ⇅
OpenOrc control plane
  Vue + FastAPI + RQ
        │
  Postgres / Valkey
        │
Connection + adapter layer
        │
Agent runtimes
  v1: Cline Hub via ClineAdapter
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

The orchestration is implemented in Python (`src/openorc/devtools/devserver.py`); `scripts/devserver.sh` is a deliberately small stable wrapper that resolves the repository root, requires the repository `.venv` (`uv sync`), and execs the Python entrypoint with all arguments unchanged. Signals (Ctrl-C/SIGTERM) only request shutdown — exactly one dependency-aware cleanup path performs all teardown.

```bash
bash scripts/devserver.sh                 # full stack: Supabase branch + queue + API + worker + app (foreground)
bash scripts/devserver.sh --help          # component-only modes, --no-* controls, --keep-supabase
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

## Agent context

Repository implementation guidance is carried by the root `AGENTS.md` plus nested `AGENTS.md` files at architectural boundaries. Agents working across boundaries must read every applicable local guide before editing.

## License

Copyright © 2026 Oliver Cheung. OpenOrc is licensed under the Elastic License 2.0 (ELv2). You may use, modify, redistribute, and self-host OpenOrc, including for commercial internal use and to build commercial products. Providing OpenOrc itself as a hosted or managed service requires a separate commercial license. See `LICENSE`.
