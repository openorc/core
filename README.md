# OpenOrc

**OpenOrc is a headless control plane for governed AI software engineering.**

Give OpenOrc a GitHub issue and it coordinates the engineering work from planning to merge: a Producer proposes and implements, an independent Reviewer evaluates, and a human Owner authorizes consequential steps. OpenOrc keeps the workflow, decisions, and recovery state durable instead of leaving them scattered across agent conversations and ad hoc handoffs.

**OpenOrc coordinates agents; it does not replace them.** Cline remains the v1 coding runtime, including its model choices, tools, and working environment. GitHub remains the system of record for issues, code, pull requests, checks, and merges. OpenOrc owns the workflow between them.

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

The v1 execution design provides two deployment choices over **one shared workflow implementation**:

- **Regular mode (default design):** a bounded local executor, without a separate worker service or Valkey.
- **Queue mode:** RQ/Valkey delivers the same operations to independently deployed workers.

Postgres remains the durable authority in both modes. Mode-neutral delivery is being completed in Phase 2; existing local development tooling still uses RQ/Valkey.

## Project status

**Phase 2: headless backend control plane, in progress.**

OpenOrc is under active development and is not yet a complete end-to-end product. The repository contains the source-available Core implementation, including its domain and persistence foundations, GitHub integration, and Cline runtime integration. Other client interfaces and workflow capabilities continue to evolve.

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
