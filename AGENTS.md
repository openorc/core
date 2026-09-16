# OpenOrc Core Agent Context

## Product and repository identity

OpenOrc is an open, provider-neutral control plane for governed agentic software development. It orchestrates engineering workflow; it does not replace coding runtimes, models, GitHub, CI, or observability systems.

`openorc/core` is the complete Elastic License 2.0 (ELv2) source-available product. Self-hosting must remain complete. `openorc/cloud` may depend on core; core must never import, require, or encode Cloud-only billing or infrastructure assumptions.

Supabase persistence/auth integration and the official v1 Cline adapter belong in core. Concrete OpenOrc Cloud deployment and Polar billing do not.

## Sources of truth

- **GitHub** owns durable engineering intent, issue hierarchy/dependencies, committed code, pull requests, CI/check state, branch protection, merge policy, and merge outcomes.
- **OpenOrc/Postgres** owns deterministic workflow/control truth: Tasks, session bindings, reviews, gates, executions, runtime requests, block state, and audit/workflow events.
- **Connected Agent Runtimes** own conversational context, private execution state, model/provider/tool execution, and runtime-owned credentials.
- GitHub webhooks are notifications that trigger authoritative reconciliation; webhook payloads are not a second GitHub truth source.
- Agent transcripts and RQ jobs are never durable workflow authority.

## Global architecture rules

- API routers and RQ jobs are thin transports over shared application services.
- Workflow/domain semantics belong in `src/openorc/domain/` and `src/openorc/services/`.
- Adapters own provider/runtime/API mechanics and normalization; they do not decide workflow meaning or authority.
- Persistence owns durable representation, not product semantics.
- Workflow state belongs in Postgres, never ephemeral queue state or runtime transcripts.
- Browser-facing live OpenOrc updates use authorized SSE. Runtime telemetry is optional adapter capability and is not automatically durable workflow history.
- Core must remain provider/runtime neutral above adapter boundaries.
- Use supported official external abstractions when they satisfy the contract; do not casually reimplement provider/runtime internals.
- Authentication ownership follows direct consumption. OpenOrc is not a universal vault for runtime-owned provider, MCP, Git, tool, or account credentials.

## Dependency policy

- Do not choose dependency or tool versions from model memory.
- When adding or upgrading a dependency or development tool, determine the latest stable release from its authoritative package registry or upstream release source at implementation time.
- Verify compatibility with OpenOrc's pinned runtime/toolchain versions and all supported target environments.
- Pin the selected direct dependency/tool version using the repository's established dependency and lockfile conventions.
- Transitive dependencies must be captured by the repository lock convention; do not rely on floating environment resolution.
- Avoid prerelease, nightly, RC, beta, or development versions unless the task explicitly requires one.

## Cross-cutting workflow invariants

- One GitHub issue has at most one current/non-archived OpenOrc Task.
- Task boundaries are conversation boundaries. Producer and Reviewer use separate Task-scoped sessions and working contexts.
- Allowed conversational topology is Owner ↔ Reviewer ↔ Producer. There is no free-form Owner ↔ Producer chat.
- Human implementation authorization and merge decisions are explicit authority boundaries.
- Prompt customization may change instructions, never protocol, authority, session semantics, or state-machine meaning.
- Repository review is bound to exact committed subjects; PR acceptance is exact-head addressed.
- Stale workflow-changing operations must never be applied to newer subjects or gates.
- Lost Task role sessions are blocking/recovery conditions; never silently replace them.
- Cancellation ends OpenOrc orchestration without implicitly deleting or mutating GitHub issue/branch/commit/PR/CI artifacts.

## Security and quality baseline

- Never commit secrets or log sensitive values.
- Workspace isolation is a security boundary, not merely billing scope.
- Raw OpenOrc-owned secrets do not belong in ordinary application tables.
- Fail closed at authorization and workflow-authority boundaries.
- New behavior ships with appropriate automated tests in the same change.
- Bug fixes ship with regression coverage.
- Ordinary tests should be deterministic and should not require live external infrastructure.

## Repository workflow

During initial repository bootstrap, direct administrative writes to `main` may occur before governance is enabled. After bootstrap governance is configured, normal implementation work must use task branches and pull requests to protected `main`.

Do not assume merge-commit, squash, rebase-merge, or branch-update policy from another repository. Follow the actual GitHub settings documented in `.github/AGENTS.md` once established. Agents do not merge implementation PRs unless repository policy is explicitly changed.

## Context perimeter map

Read the nearest relevant guide before editing, and read all relevant guides for cross-boundary work:

- `.github/AGENTS.md`
- `apps/app/AGENTS.md`
- `src/openorc/AGENTS.md`
- `src/openorc/domain/AGENTS.md`
- `src/openorc/services/AGENTS.md`
- `src/openorc/api/AGENTS.md`
- `src/openorc/workers/AGENTS.md`
- `src/openorc/adapters/AGENTS.md`
- `src/openorc/adapters/github/AGENTS.md`
- `src/openorc/adapters/cline/AGENTS.md`
- `src/openorc/persistence/AGENTS.md`
- `packages/cline-sdk-bridge/AGENTS.md`
- `supabase/AGENTS.md`
- `tests/AGENTS.md`

Keep this root file as the repository entry vector. Local implementation rules belong in the nearest relevant `AGENTS.md`; task-specific scope belongs in GitHub issues; explanatory detail belongs in README/docs rather than bloating standing agent context.
