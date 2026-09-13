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

## Agent context

Repository implementation guidance is carried by the root `AGENTS.md` plus nested `AGENTS.md` files at architectural boundaries. Agents working across boundaries must read every applicable local guide before editing.

## License

Apache License 2.0. See `LICENSE`.
