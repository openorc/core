# OpenOrc

OpenOrc is an open, provider-neutral control plane for governed agentic software development.

It keeps the engineering workflow stable while coding runtimes, models, inference providers, and review systems remain replaceable. OpenOrc coordinates GitHub-backed work, independent Producer/Reviewer roles, explicit human authority, Task-scoped runtime sessions, deterministic workflow state, pull-request review, and audit history. It does not replace coding agents, GitHub, CI, or observability systems.

## Repository status

This repository is in the initial bootstrap phase. The architecture and repository context are being established before substantive implementation begins.

`openorc/core` is the complete open-source product. Self-hosting must remain complete. The private `openorc/cloud` repository may compose and operate core, but core must never depend on Cloud-only code or infrastructure.

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
Agent runtimes such as Cline Hub
```

The starting repository structure is intentionally explicit:

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

## Agent context

Repository implementation guidance is carried by the root `AGENTS.md` plus nested `AGENTS.md` files at architectural boundaries. Agents working across boundaries must read every applicable local guide before editing.

## License

Apache License 2.0. See `LICENSE`.
