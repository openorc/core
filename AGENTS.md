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
- Operational workflow failures use OpenOrc's shared Level 1 (authorized operation/evidence) → Level 2 (failure disposition) → Level 3 (exceptional blocking/gate selection) path where applicable. Do not invent per-workflow retry or escalation machinery; read `src/openorc/services/AGENTS.md` before adding a failure path. Direct controls/guards may enter Level 3 without Level 2.
- Lost Task role sessions are blocking/recovery conditions; never silently replace them.
- Session establishment (issue #324): in regular mode the mandatory Producer/Reviewer initialization is an enqueued, independently completed agent turn — promptless `start`, one observation registration, one detached-turn admission through the runtime-neutral detached-turn boundary (issue #348; native enqueue mechanics stay inside the Cline integration) — dispatched through the same #130 establishment validation and composition phases. The binding stays CONNECTING with no bound external identity until a genuinely attributable terminal response validates the canonical `session_ready` and the existing lifecycle service atomically binds the identity, snapshot, and initialized timestamp. Outstanding initialization receipts make a repeated dispatch an idempotent no-op; a CONNECTING binding without one is the honest unknown that is never re-dispatched; readiness finalization resumes the bootstrap through the existing continuation machinery. Mere Hub/transport unavailability is never session loss. Queue mode keeps the blocking handshake until its own parity leaf (#328).
- Detached initial implementation (issue #333): in regular mode the E5-authorized initial IMPLEMENT is likewise an enqueued, independently completed agent turn — the same E6 reservation, R1/R2 authority guards, and proven/uncertain failure classification, with the dispatching worker exiting after the durable admission — and the independently observed completion settles the Execution through the shared finalize before the ONE post-commit completion handoff (`task_completion_handoff`) routes by explicit operation family to the owning service and presents the exact-head PR_AUTHORIZATION from committed state. Queue mode keeps the blocking composition until its own parity leaf (#328).
- Detached workspace PREPARE (issue #334): in regular mode the bootstrap's canonical `PrepareWorkspaceInteraction` is likewise one enqueued, independently completed agent turn on the original Producer session at the immutable repository base — the `prepare_dispatched` stage marker and the `planning_bootstrap_prepare` receipt commit in one short transaction strictly before the positive observation registration and the verbatim admission, and the dispatching worker exits after the acknowledgement/refusal/uncertainty. The independently observed completion validates the attributable `workspace_result` and settles only the original bootstrap stage (the claim-bound `prepared` recording inside the settlement transaction); the ONE post-commit handoff then performs the existing safe same-external-session PLAN-mode reconstruction into the exact Task-UUID checkout (no extra initialization, no mode-notice prompt; the runtime's own same-session serialization guards it) and arranges the next typed initial-PLAN invocation through the shared continuation seam. An outstanding PREPARE receipt makes a replayed continuation an idempotent no-effect replay; a durably dispatched PREPARE is the Level 2 bootstrap family's still-current failed operation, so a confirmed failure (refusal, observed run failure, failed/invalid workspace result) escalates through the existing machinery to the BLOCKED disposition instead of a stale no-op, and an unavailable/invalid/ambiguous result can never mark the workspace prepared or authorize a new turn. Queue mode keeps the blocking PREPARE until its own parity leaf (#328).
- Detached plan-result production (issue #335): in regular mode the Producer's initial PLAN and each routed planning REVISE are likewise one enqueued, independently completed agent turn on the exact prepared Producer session — the same one-shot F2 send claim (now also recording the dispatch-observed repository-base head, issue #335's `dispatch_repository_base_sha`) and the `plan_result`-expecting detached-turn receipt commit in one short transaction strictly before the positive observation registration and the verbatim admission of the composed canonical instruction, and the dispatching worker exits after the acknowledgement/refusal/uncertainty. The independently observed completion validates the attributable `plan_result` and atomically creates the next immutable PlanRevision from the claim's recorded base context, rotates the current revision/token, settles the claim and receipt, and emits the existing events — missing, invalid, or stale completions never create a revision. The ONE post-commit handoff then submits the existing typed planning REVIEW from committed state. Queue mode keeps the blocking production until its own parity leaf (#328); the Owner-discussion resolution paths keep their blocking revise sends until #339.
- Detached planning REVIEW decision (issue #336): in regular mode the planning REVIEW is likewise one enqueued, independently completed agent turn on the exact READY Reviewer session — the same atomic reservation of exactly one next-numbered unfinalized ReviewIteration (the OPEN planning loop, the no-unresolved-attempt and no-iteration-for-current-revision guards) commits before the effect, and the iteration's detached-turn receipt companion (operation `reviewer_review`, attempt kind `review_iteration`, expecting the canonical `review_result` family) commits in one short transaction strictly before the positive observation registration and the verbatim admission of the composed canonical REVIEW message; the dispatching worker exits after the acknowledgement/refusal/uncertainty. The independently observed completion validates the attributable `review_result` and finalizes the decision through the owning E3b settlement (`settle_observed_planning_review`) — the same one-shot conditional result recording, persisted-state budget and sticky Owner-mediation derivation, audit events, and exhausted-decision gate, all atomic with the receipt settlement; a moved-on subject settles result-not-applied, and a duplicate, late, or stale REVIEW can never decide twice. The ONE post-commit handoff owns the workflow continuation from committed state: ACCEPTED presents the exact-plan IMPLEMENTATION_AUTHORIZATION (never approved) under the F5b coordinator through the exact-plan presentation family — the finalized ACCEPTED iteration is its durable attribution identity and a detected presentation failure escalates durably through the existing Level 2/3 machinery instead of stranding the accepted review — an in-budget CHANGES_REQUESTED dispatches the new authorized Producer REVISE through the #335 detached service using the persisted iteration, and an exhausted or sticky change request's REVIEW_RESOLUTION gate stands with no Producer prompt. The decision consumes durable identity, never a process-local `TaskPlanningReviewDispatchOutcome`, and never a blocking Producer adapter; queue mode keeps the blocking REVIEW dispatch and in-process decision until its own parity leaf (#328).
- Detached PR composition and canonical GitHub publication (issue #337): in regular mode the approved-head Producer PR_COMPOSE is likewise one enqueued, independently completed agent turn on the exact READY Producer session — the ordinary E8 admission (the persisted approved `PR_AUTHORIZATION` gate by id, its exact head-SHA subject, the OWNER-attributed approving-Profile audit) commits with the one-shot `PR_COMPOSE_DISPATCH` claim and the `pr_result`-expecting detached-turn receipt companion in one short transaction strictly before the positive observation registration and the verbatim admission, and the dispatching worker exits after the acknowledgement/refusal/uncertainty. The independently observed completion validates the attributable `pr_result` and settles the claim (safe head identity only) and receipt through the owning settlement (`settle_observed_pr_compose_result`), atomic with the receipt settlement and serialized on the exclusive Task row lock (the settlement performs no Task write of its own, so the lock is what excludes a concurrent Owner Abort/blocking/cancellation transition); moved-on subjects settle result-not-applied. The publication is a deliberately bounded post-settlement consequence in the SAME completion-processing invocation — the ONE post-commit handoff's PR_COMPOSE route invokes the existing owning service's `publish_settled_task_pr_composition` (full authorization re-proof under the re-derived approving Profile in one short transaction, then the existing bounded GitHub preflight/create/reconcile/finalize with no transaction across the I/O) and then submits the existing typed PR_REVIEW via F1 from committed state. The validated title/body are v1-deliberately never persisted: the `PrResult` is carried in memory from the settlement to that invocation's publication, a process death after the settlement but before any create is the explicitly accepted narrow v1 liveness/stranding limitation (the claimed turn is never repeated, no title/body invented), and a design needing a cross-process publication invocation requires a separately agreed durable result contract (STOP, no ephemeral-value handoff). The narrow interrupted-publication continuation owns no detached turn. Queue mode keeps the blocking compose-publish-review interim until #328.
- Detached PR-review decision and Producer remediation (issue #338): in regular mode the PR REVIEW is likewise one enqueued, independently completed agent turn on the exact READY Reviewer session — the same shared reservation of exactly one next-numbered unfinalized PR/head ReviewIteration for the freshly reconciled canonical PR/head (the OPEN PR loop, the no-unresolved-attempt and no-iteration-for-current-head guards) commits before the effect, and the iteration's detached-turn receipt companion (operation `reviewer_review`, attempt kind `review_iteration`, expecting the canonical `review_result` family) commits in one short transaction strictly before the positive observation registration and the verbatim admission; the dispatching worker exits after the acknowledgement/refusal/uncertainty, and the worker's former `while True` dependency on a nonserialized dispatch outcome is gone. The independently observed completion validates the attributable `review_result` and finalizes the decision through the owning PR-review settlement (`settle_observed_pr_review`) — the same one-shot conditional result recording, persisted-state budget and sticky Owner-mediation derivation, audit events, and gate commits, all atomic with the receipt settlement, with the correlation seam resolving the `REVIEWER_REVIEW` family's expected failure status by the reserved attempt's durable subject flavor; a moved-on subject settles result-not-applied, and a duplicate, late, or stale REVIEW can never decide twice. The ONE post-commit handoff owns the continuations from committed state: an in-budget CHANGES_REQUESTED dispatches the new authorized E9 remediation as one detached agent turn on the exact Task checkout through the existing E9 service (the one-shot claim and its `implementation_result`-expecting receipt companion commit in one short transaction strictly before the registration and the verbatim admission of the persisted finalized iteration's `review_result` JSON), whose independently observed completion settles through the existing #322 E9 execution-settlement route; the remediation-completion handoff then re-proves the settled round from committed state, verifies the branch claim against the canonical branch and the SAME canonical PR's GitHub-confirmed NEW head through the existing B7 reconciliation under the F5b coordinator, and only then submits the next typed PR_REVIEW through F1 for that confirmed head. ACCEPTED presents the existing MERGE_DECISION gate and never merges autonomously; exhaustion/sticky mediation produces the existing REVIEW_RESOLUTION; duplicate/stale/ambiguous output never advances a newer head. Queue mode keeps the blocking dispatch/decision/remediation interim until its own parity leaf (#328).
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
- `src/openorc/protocol/AGENTS.md`
- `src/openorc/domain/AGENTS.md`
- `src/openorc/services/AGENTS.md`
- `src/openorc/api/AGENTS.md`
- `src/openorc/workers/AGENTS.md`
- `src/openorc/adapters/AGENTS.md`
- `src/openorc/adapters/agent_runtime/AGENTS.md`
- `src/openorc/adapters/github/AGENTS.md`
- `src/openorc/adapters/cline/AGENTS.md`
- `src/openorc/adapters/supabase/AGENTS.md`
- `src/openorc/persistence/AGENTS.md`
- `packages/cline-sdk-bridge/AGENTS.md`
- `supabase/AGENTS.md`
- `tests/AGENTS.md`

Keep this root file as the repository entry vector. Local implementation rules belong in the nearest relevant `AGENTS.md`; task-specific scope belongs in GitHub issues; explanatory detail belongs in README/docs rather than bloating standing agent context.
