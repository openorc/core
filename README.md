# OpenOrc

**Turn Cline into a persistent, governed software factory.**

OpenOrc coordinates Cline-powered engineering work from GitHub issue through planning, independent review, implementation, and human approval to a reviewed pull request and Owner-controlled merge. It handles the handoffs, durable workflow state, and recovery so you can delegate substantial work without manually managing the Producer/Reviewer loop.

**Keep your runtime, models, and tools.** Cline remains the coding runtime and owns model/provider configuration, credentials, tools, and execution. GitHub remains the durable engineering record. OpenOrc governs the workflow between them. It is Cline-first in v1, with runtime-neutral architecture boundaries for future integrations.

## From issue to merge

```text
GitHub issue
    ↓
Producer prepares a plan
    ↓
Reviewer evaluates the plan
    ↓
Owner authorizes implementation
    ↓
Producer implements and pushes code
    ↓
Owner authorizes PR creation
    ↓
OpenOrc publishes the PR
    ↓
Reviewer evaluates the committed PR head
    ↕
Producer remediates findings as needed
    ↓
Owner requests merge
    ↓
GitHub confirms merge
```

Producer and Reviewer are **separate logical roles with persistent, Task-scoped sessions**. They may share one Cline Hub while keeping independent session context and role configuration. The workflow uses bounded review loops, exact plan/commit review subjects, explicit Owner authority, and safe handling of retries, stale operations, and uncertain external outcomes.

The goal is to automate coordination *between* human decisions, not to automate away the human's responsibility for them.

## Your Cline environment, two ways

- **Bring your own Cline Hub:** connect an externally managed Cline runtime and keep operating its environment, providers, credentials, and tools yourself. The full self-hosted OpenOrc Core workflow does not depend on OpenOrc Cloud.
- **Managed ClineBox (planned OpenOrc Cloud option):** use a persistent, Workspace-scoped development environment running Cline, with repositories, installed toolchains, dependencies, caches, and native runtime configuration retained across Tasks. Eligible idle environments can be hibernated and restored instead of being rebuilt for every task.

**The environment persists; Task conversations do not cross Task boundaries.** Each new Task creates its own isolated Producer and Reviewer sessions and working contexts. Within a Task, those sessions persist across planning, implementation, and review. OpenOrc Cloud manages the ClineBox host lifecycle; Cline remains the owner of its provider credentials and native configuration.

## Headless by design

**OpenOrc Core is the product; no particular frontend is the product boundary.**

Core exposes a client-independent control plane built around shared application services and a canonical HTTP/OpenAPI command-and-query surface. The same workflow authority, authorization checks, and durable state apply regardless of how an Owner interacts with it.

```text
                 Client interfaces
      Vue web UI · API integrations · CLI/SDK
             MCP / conversational clients
                         │
                    OpenOrc API
                         │
               Shared workflow services
                   │             │
             Postgres         Integrations
          workflow state      │         │
                            GitHub    Cline
                                     runtime
```

The Vue interface, direct API consumers, future CLI/SDK tools, and MCP-based clients (including interactive MCP Apps) are ways to operate the same backend, **not separate workflow engines**. Client availability evolves with the project; this diagram describes the architectural boundary, not a claim that every interface is already shipped.

## Architecture and ownership

OpenOrc separates three sources of truth:

- **GitHub:** engineering requirements and dependencies, branches and commits, pull requests, CI, and final merge outcomes.
- **OpenOrc Core / Postgres:** Tasks, plans, reviews, Owner decisions, runtime-session bindings, workflow transitions, and audit history.
- **Connected agent runtimes:** agent conversations, tools, model execution, and working files.

GitHub changes are reconciled against GitHub's authoritative state. Agent output is validated and tied to the exact subject OpenOrc assigned. Neither a queue message nor an agent transcript gets to decide whether a Task is authorized to advance.

Core is **Cline-first in v1, runtime-neutral at its architecture boundaries**. OpenOrc does not implement its own coding agent or model runtime.

### Execution modes

The v1 execution design provides two deployment choices over **one shared workflow implementation**, selected through `OPENORC_EXECUTION_MODE`:

- **Regular mode (default):** a bounded local executor owned by the API serving process, without a separate worker service or Valkey. Capacity is bounded (`OPENORC_LOCAL_EXECUTOR_WORKERS`, default 2; `OPENORC_LOCAL_EXECUTOR_PENDING`, default 8), and saturation or shutdown refuses before acceptance.
- **Queue mode:** RQ/Valkey delivers the same operations to independently deployed workers through an explicitly supplied `VALKEY_URL` (no implicit default backend).

Both modes submit the same typed operation invocations through one shared dispatcher, and the RQ worker refuses to start in regular mode. Postgres remains the durable authority in both modes; neither local memory nor queue IDs are workflow truth. GitHub webhook dispatch and the on-demand reconciliation sweep are mode neutral: both are registered shared-operation handlers, and the authenticated webhook route and the explicit sweep submission seam use the serving process's active submitter in either mode.

## Project status

**Phase 2: headless backend control plane, in progress.**

OpenOrc is under active development and is not yet a complete end-to-end product. The repository contains the source-available Core implementation, including its domain and persistence foundations, GitHub integration, and Cline runtime integration. Other client interfaces and workflow capabilities continue to evolve. The managed ClineBox option is not yet generally available.

## Development

Python 3.13 and Node 24 are the primary toolchain runtimes. The detailed setup, environment, test, and local service instructions live in the **[developer guide](docs/development.md)**.

A typical backend setup starts with:

```bash
uv sync
cp .env.example .env
# Configure the required Supabase/development environment values.
./scripts/devserver.sh
```

The devserver provisions an ephemeral Supabase branch for local verification. See the guide for its prerequisites, supported modes, and cleanup behavior.

Additional references: [Supabase migrations](docs/supabase-migrations.md), [testing](tests/README.md), and [repository contributor guidance](AGENTS.md).

## License

Copyright © 2026 Oliver Cheung. OpenOrc is licensed under the **Elastic License 2.0 (ELv2)**. It is source-available and may be self-hosted under that license; providing OpenOrc itself as a hosted or managed service requires a separate commercial license. See [LICENSE](LICENSE).
