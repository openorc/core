# Application Services Agent Context

## Boundary

Services own deterministic use-case orchestration: authorization, current-subject validation, domain/persistence composition, adapter invocation, external-outcome classification, durable effects, and workflow consequences.

Services do not own HTTP DTOs, RQ job semantics, provider wire mechanics, Vue state, or Cloud infrastructure.

## Canonical flow

FastAPI/RQ entrypoint → application service → authorization/current-subject checks → persistence/domain/adapters → deterministic result or typed application error.

API routers and RQ jobs remain thin callers. Provider-specific mechanics remain in adapters. Persistence remains representation/data access, not workflow policy.

## Service organization

- Organize modules around coherent capabilities, not one global workflow service.
- Keep distinct responsibilities in distinct modules and avoid generic helper buckets.
- Reuse focused helpers only for genuine cross-service concerns.
- Validate command shape before authorization, persistence mutation, telemetry annotation, or external calls when malformed caller data could otherwise escape into those boundaries.
- Expected failures use the typed ApplicationError vocabulary. Transports map those errors; service errors must carry only safe fields.

## Transactions and external I/O

- composed_transaction(pool) is the supported atomic-composition primitive for database-only service work. The explicit outer transaction is required; nested repository transactions become SAVEPOINTs.
- Keep database transactions short.
- Never hold a database transaction open across GitHub, Supabase Auth HTTP, Cline/runtime, model, or any other external call.
- After an external call, reload/reconcile current durable state before a consequential follow-up mutation.
- Known success, known failure/non-delivery, and uncertain outcome are distinct. Timeout/connection loss is not automatically safe to retry.
- The LLM is never an idempotency mechanism.

## Workspace authorization

- Owner authorization is exact: workspace.owner_profile_id must match the authenticated Profile ID.
- Public Workspace-scoped resolvers take authenticated profile_id plus the addressed identity and perform ownership gating themselves. Passing a Workspace object is not reusable proof of authorization.
- Validate direct workspace_id and Task linkage for Task-owned records; database composite foreign keys are durable backstops, not the authorization mechanism.
- Missing, cross-Workspace, and cross-Task subjects are uniformly NotFoundError where exposing the distinction would create a probing oracle.
- AuthorizationError is for cases where scope is already established and the action itself is unauthorized.
- Workspace configuration operations are ownership-gated, row-locked, same-value no-ops, and emit audit only for actual changes.
- A changed Workspace review limit affects future ReviewLoops only.

## Exact-subject and stale-operation safety

- Delayed/replayed workflow-changing work always reloads current durable state.
- Verify Task state_token plus the exact PlanRevision, OwnerGate, PR/head, RuntimeRequest, or equivalent authority subject before applying effects.
- A stale subject/token never applies to newer state and never manufactures a replacement token.
- Use focused guards/resolvers for concrete subject families; do not create a generic subject/reflection framework.
- PR authority uses the canonical TaskPullRequest and exact reconciled head SHA. Base-branch movement alone is not an OpenOrc stale-review condition.
- Persistence conditional-write rejection is classified from current locked durable state into the typed application vocabulary; fail closed when the exact reason cannot be proven.

## Task mutations and workflow events

- The focused Task mutation surface is update status, archive terminal Task, bind canonical branch, install current PlanRevision, install current OwnerGate, and resolve exact current OwnerGate. Do not introduce a generic Task patch API.
- These primitives enforce currentness/scope. Higher-level workflow capabilities decide whether a transition is allowed at a given stage.
- Successful mutations return the post-write Task with its rotated state_token.
- Services decide when consequential workflow facts deserve WorkflowEvents. Persistence only appends the record.
- Canonical mutation and its promised WorkflowEvent commit atomically in the same composed transaction.
- Failed/stale/no-op mutations emit no event.
- WorkflowActorContext contains only safe logical actor identity. Event context is small, event-specific, and never contains credentials, raw request bodies, guidance prose, prompts/templates, or duplicated canonical state.

## Canonical branch/PR/checks/merge operations (issue #63)

- Canonical branch verification turns a Producer-reported branch claim into canonical state through GitHub only: the exact committed head SHA comes from the documented branch read for the Task's exact Repository, addressed through the freshly observed repository address; binding goes through the Phase 2A `bind_canonical_branch` primitive (exact currentness/state-token, bind-once, repository-wide ownership exclusivity). Once bound, later remediation continues on the canonical branch rather than replacing it — a claim naming a different branch is a conflict, never a silent rebind. A missing/deleted branch or access failure is the normalized `AuthorizationError` integration condition, never permission to trust runtime-local Git state.
- TaskPullRequest reconciliation addresses the one already-created canonical record and is strictly update-only (`reconcile_task_pull_request_observed`); a missing record is the uniform `NotFoundError`, and a PR number reporting a different stable `github_pr_id` is a conflict, never a rebind or a replacement row. The returned before→current facts distinguish a changed head (the review subject changed) from a base-only change (which never invalidates exact-head acceptance). Reconciliation emits no WorkflowEvent and decides no Task-state transition.

## Race-safe PR publication (issue #64)

- `pull_request_publication.publish_task_pull_request` is the external-operation primitive invoked after `PR_AUTHORIZATION`. It owns command-shape validation, the replay/currentness guard, exact-head preflight, exactly one create attempt, immediate authoritative post-create re-read, canonical-record persistence, and the post-create durable currentness recheck. It never resolves the `PR_AUTHORIZATION` OwnerGate, never transitions Task state, never emits WorkflowEvents, and never dispatches the Reviewer — those belong to the later workflow layer consuming its typed result.
- Clean success requires ALL of: preflight head == authorized head, GitHub create known-success, post-create reconciled head == the same authorized head, and the Task `state_token`/route still current inside the write transaction. Any failure of the authority/currentness conditions after known GitHub creation preserves and persists the created PR (the external fact is never erased or rolled back) and returns a distinct stale classification (`POST_CREATE_HEAD_MISMATCH`, `POST_CREATE_AUTHORITY_STALE`) that is never review-safe.
- Exactly one create attempt per invocation. Uncertain create outcomes (timeout, connection loss) are `ExternalOperationUncertainError` with zero automatic resend. A durable-persistence failure after known GitHub success propagates as the recovery-required condition — never an automatic second create. GitHub's "already exists" answer while no canonical record exists is an explicit non-adoption conflict; v1 never adopts external PRs.
- No database transaction spans preflight, create, or post-create GitHub calls; the write phase is one short transaction whose first lock is the account-deletion barrier, whose Task row is re-read under an exclusive row lock (`get_task_for_update`) so the authority recheck serializes against concurrent Task mutations through the canonical-record persistence, and whose Repository route is revalidated under lock. Persistence failures and known reconciliation failures AFTER known GitHub creation are the typed `ExternalOperationRecoveryRequiredError` / `ExternalOperationReconciliationRequiredError` conditions — never an infrastructure leak, never a denial of the known creation, never an automatic second create. Telemetry carries only safe identifiers — never PR title/body, issue content, provider response bodies, or tokens.
- The checks/commit-status projection is read-only for the exact durable reconciled head SHA, persists nothing, and presents GitHub-owned facts only — never an OpenOrc merge-policy engine.
- The merge primitive binds authority through `require_task_pull_request_head` in one short transaction that closes before any GitHub call, sends the documented `sha` expected-head parameter, classifies the documented head-mismatch answer as `StaleOperationError` and other definitive rejections as known `ExternalOperationFailedError` (GitHub owns the policy), and — only on known success — immediately reconciles the PR authoritatively to persist the durable merged facts. An uncertain merge is never automatically replayed; recovery belongs to the later reconciliation/recovery layer. No Task-state transition (including `COMPLETED`) or Owner merge-decision policy lives here.

## TaskAgentSession runtime establishment and lifecycle (issue #130)

- `session_runtime.establish_task_agent_session` is the ONE logical readiness operation over an admitted CONNECTING binding: exact-binding validation (already-READY binding is idempotent success with no runtime call; LOST/ENDED are terminal conflicts that never trigger replacement; the persisted ADMITTED Connection must exist, be Workspace-scoped, and be enabled — a later role-binding route change applies to future Task admissions only and never invalidates an already-admitted Task/role/Connection, and the operation never silently switches to the new route, issue #189) → canonical #66 initialization composition and deliberate non-secret effective-configuration snapshot assembly (the composition-selected admitted-Connection configuration supplied as the required `effective_configuration` — read and selected ONCE by the composition before this service and shared with the construction-context resolver — plus the durably-read adapter type; the current Connection `safe_config` is never re-read, so fresh construction and the durable READY snapshot agree by construction; never credentials, initialization Markdown, rendered schemas, Workspace guidance, control/prompt bodies, or transcripts) → exactly one #67 `create_session` readiness handshake with no database transaction held → durable CONNECTING -> READY through the atomic persistence primitive, whose conditional-write rejection is reclassified from the reloaded binding (same exact READY identity idempotent, different identity continuity conflict, LOST/ENDED terminal conflicts, missing/mis-scoped fail closed).
- Known adapter failure and uncertain outcome both leave the binding CONNECTING — neither is ever session loss, never a partial external identity, and never replayed. A durable persistence failure after known external readiness is the typed `SessionEstablishmentRecoveryRequiredError`: the known readiness is never denied and no second session is created automatically.
- Genuine loss recording requires the normalized exact-session-loss evidence (`AgentSessionNotFoundError`); unavailability/timeouts/configuration rejections are invalid commands, never loss. LOST/ENDED records are lifecycle history on the SAME binding (identity, Connection, and initialization facts preserved; no successor session), repeat loss/end are idempotent, and both lifecycle records write facts only — later workflow services decide TaskBlock/recovery consequences, and this module emits no WorkflowEvents.

## Durable Cline construction-context resolution (issue #189)

- `services/cline_construction.py` is the production implementation of the #74 injected read-only `ClineConstructionContextResolver` seam — the bridge between durable Task/role/Connection facts and `ClineAdapter` construction and exact-session continuity. It is read-only (no persistence mutation, no workflow authorization, no WorkflowEvents) with no Cloud imports, network I/O, SSH, or Cline Hub access, and it is constructed at the process-invocation/composition boundary with the durable pool plus the composition-selected construction facts (`bootstrap_repository_base`, `working_directory`).
- Fresh resolution (`resolve_construction_context`) requires the exact durable binding for the addressed Task/role to exist, be Workspace-scoped, and be CONNECTING (fresh construction is definitionally the establishment of an admitted binding), verifies the ADMITTED Connection — never the mutable role-binding route — as an available, enabled Cline Connection of that Workspace BEFORE supplying any construction input, resolves the current #162 role configuration (provider/model pair, verbatim prompt override), and supplies the fresh Producer's composition-selected bootstrap base or the fresh Reviewer's composition-supplied neutral directory. Failures are normalized adapter-boundary configuration rejections (`AgentRuntimeConfigurationRejectedError`); uniform NotFoundError stays with the establishment service's own subject validation.
- Bound resolution (`resolve_bound_session_context`) keys on the exact opaque external session ID: exactly one durable READY binding, an available enabled Cline admitted Connection, and the current role configuration are required, else the answer is `None` — an identity-establishment miss the adapter fails closed on, never session-loss evidence (ambiguous cross-Connection duplicates and terminal LOST/ENDED records resolve no context). The Producer's original base is recovered from the binding's own `effective_config_snapshot` (a later Connection edit never relocates the Task) and a supplied working directory is accepted only as that original base or the deterministic `<original base>/<task id>` checkout; a Reviewer never requires a repository_base and resolves with its neutral directory verbatim. A `None` working directory passes through so identity-only operations still resolve while reconstruction fails closed adapter-side.
- The one-authoritative-selection contract: fresh construction uses the `bootstrap_repository_base` the composition selected once from the admitted Connection, and `establish_task_agent_session` pins the same selected configuration (`effective_configuration`) into the durable READY snapshot. This resolver never sources construction paths from the current `Connection.safe_config.repository_base`. D8b composition owns performing that single read; E1 owns durable path selection.

## Connection credentials

- Credential configure/rotate is Owner-authorized and operates under the Connection row lock.
- Credential input must be a non-empty string before authorization or mutation. Preserve accepted credential bytes verbatim; do not trim/normalize secret material.
- Configure creates the Vault secret and installs the opaque auth_reference atomically. Configure over an existing credential is a conflict.
- Rotate updates the existing Vault secret in place and preserves the opaque reference. Missing, dangling, or malformed references fail closed.
- Runtime credential resolution is the trusted adapter invocation path. It still enforces Workspace scope and fails closed for disabled Connections or unresolved credentials.
- ControlEndpointSecret is the narrow secret-bearing in-memory boundary. Its repr/str are redacted and secret_value() is reserved for the trusted adapter/authentication call. Never put it in domain objects, DTOs, events, logs, telemetry attributes, or snapshots.

## Observability

- Manual spans belong at meaningful public use-case boundaries, not every guard/helper/repository call.
- Use observability.tracing.application_span/application_tracer and only the safe attribute vocabulary.
- Failure telemetry is safe classification only. Do not export arbitrary exception messages/stacktraces because they may contain secrets or customer content.
- Operational logging is selective: degraded dependencies, uncertain outcomes, protocol failures, stale/rejected async work when operationally relevant, and unexpected durable-state invariant failures. Expected product outcomes and successful operations do not need per-call logs.
- Telemetry is observational. It never changes typed error semantics, transaction scope, authorization, WorkflowEvent content, or workflow authority.

## Administrative deletion and account lifecycle

- Authenticated Owner mutations that carry profile_id compose the account-operational barrier first, before Workspace authorization or destructive mutation. This prevents new Owner mutations from racing past a claimed account deletion.
- Administrative aggregate deletion is fail closed: ownership/scope first, then the destructive database effect. It never mutates GitHub or runtime-owned artifacts.
- Disconnect/hard-delete cleanup removes only the OpenOrc-owned Vault secret referenced by the current auth_reference, while holding the Connection lock. Malformed/dangling references are conflicts and the transaction rolls back.
- Only archived Tasks are administratively purgeable. Restrictive foreign-key failure is translated to a conflict rather than deleting history to make the operation succeed.
- Permanent account deletion is the one service flow that crosses the Supabase Auth Admin boundary: claim the exact durable deletion attempt and revoke OpenOrc-owned Connection credentials in one short transaction, commit, then perform the Auth Admin call with no database transaction open.
- Hard Auth-user deletion is required. Do not create a competing manual graph delete or use Supabase soft deletion as account deletion.
- Fresh active deletion attempts are single-flight. Expired active or uncertain attempts reconcile through the Admin read surface before replay.
- Reconciliation and compare-and-swap always bind to the exact attempt UUID being reconciled. If it has gone stale, reload/reclassify rather than replaying against newer state.
- The active-attempt lease is derived from bounded Admin request timeouts plus safety margin and compared against the database clock.
- Confirmed already-absent Auth users are an idempotent success state. A present Auth user with no corresponding Profile at entry is inconsistent and fails closed.
- Account deletion itself does not compose the account-operational guard because it must remain retryable/reconcilable.
- SUPABASE_SECRET_KEY is required only where the Auth Admin client is constructed; unrelated processes may boot without it.

## Profile-scoped GitHub user authorization (issue #142)

- `github_user_authorization.py` owns the same-human binding proof and the Profile-scoped user-token lifecycle. Establishment proves the trusted Supabase GitHub identity through the Auth Admin boundary — exactly one entry whose `provider_id` strictly normalizes to the stable numeric GitHub user ID; zero, multiple, malformed, or absent identity state fails closed without guessing — then exchanges the post-correlation code (forwarding the PKCE `code_verifier` when the correlated authorization request used PKCE) and requires the expiring-token capability (never a silently persisted long-lived token), resolves the authorized GitHub account authoritatively, and compares stable numeric IDs; a mismatch stores nothing. The sign-in provider token is never consumed, copied, persisted, refreshed, or reused as a repository credential, and `AuthenticatedPrincipal` is never broadened with mutable provider presentation data. Reauthorization reactivates a revoked row in place with its generation advanced (no superseded-secret delete attempt on a NULL reference) — the recovery path after revocation and after a failed account-deletion attempt.
- Refresh rotation is guarded by the durable generation: read the current generation, resolve the Vault credential wrapped IMMEDIATELY in the redacted secret-bearing carrier (the raw value crosses the persistence boundary only there and reaches a raw representation only at the adapter's final request-construction boundary), perform exactly one GitHub refresh exchange with NO database transaction open, then in one short transaction compose `require_account_operational` FIRST, lock/reload, and install the rotation only when the durable generation still matches and the row is still active. A stale process discards its returned token pair and reloads; a definitive rejection fails closed without durable mutation (durable state is reloaded first so a committed concurrent rotation is reported as moved-on); a capability-less refresh answer and an uncertain outcome both fail closed through the typed application conditions — never a raw adapter exception — and never replay. Missing/revoked/expired/unusable authorization is an explicit typed condition (`GitHubUserAuthorizationUnavailableError` with a condition constant) — there is no installation-token fallback.
- The access-token cache is process-memory only, bounded, and keyed to the exact `(Profile, generation)` pair; every lifecycle transition advances the generation atomically, so no cache key can outlive its credential. The resolver's cache eviction is the ONLY sanctioned bounded-recovery trigger (issue #143): after a DEFINITIVE 401-style rejection under the resolved user token, the write service evicts and re-resolves (exactly one refresh exchange) and retries the failed step exactly once — never after uncertain outcomes, policy rejections, or stale operations.

## Owner-accountable GitHub writes (issue #143)

- `github_owner_write_authorization.py` is the shared composed boundary every Owner-accountable GitHub write passes through BEFORE credential resolution or external GitHub I/O: the account-operational barrier (`require_account_operational`) composes FIRST in one short transaction, then `require_workspace_task` proves the exact Profile owns the addressed Workspace/Task, then the service's own subject/currentness validation (state token, canonical branch, exact PR head), then `resolve_owner_write_credential` resolves that Profile's user credential and proves the user × installation × repository intersection for the exact routed identities.
- Every Owner-accountable write command carries the accountable `profile_id` explicitly; it comes from the trusted workflow/Owner authorization context and is never guessed from Workspace ownership, mutable GitHub metadata, repository membership, or current-user heuristics. A caller-supplied or stale `profile_id` is never sufficient authority by itself, and another Profile's valid credential is never reusable even if that user can access the same repository.
- The user × installation × repository intersection failure is the typed `GitHubUserRepositoryAccessError` (condition constant) — never another installation, another Profile, an installation-token write, or a PAT; GitHub's later definitive permission/policy rejection on the write itself remains a normal known provider outcome. Write-failure translation (`translate_owner_write_failure`) is honest about the accountable Profile's user credential and never describes a user-authenticated write failure as a routed-installation credential failure; installation-flavored error text belongs only to installation-authenticated reads/reconciliation.
- Account deletion composes the GitHub-user cleanup in the same revocation transaction as the deletion-attempt barrier: the refresh secret is deleted and the row is made non-dangling (revoked, reference NULL, generation advanced) BEFORE the external Auth delete, idempotent on retry, and failing closed on malformed/dangling references before any destructive external call.

## Role-binding configuration and the authenticated API dependency (issue #162)

- `role_binding_configuration.py` owns the Owner-authorized read/update of
  the per-role runtime-session configuration. Reads and mutations establish
  Workspace ownership through the shared resolvers (`require_workspace_role_binding`
  / `require_profile_workspace`) — authentication alone never grants Workspace
  access, and missing/foreign subjects are uniformly `NotFoundError`.
- The mutation composes the account-operational barrier first, validates the
  command shape before authorization/persistence (pair completeness; the
  override is `None` or any verbatim string, including empty), writes the
  whole record through the conditional upsert, and records
  `WORKSPACE_CONFIGURATION_CHANGED` only for actual changes (including
  initial creation) inside the same transaction. The event helper accepts no
  provider/model/prompt parameter, so those values have no path into event
  context. Configuration edits compose no `task_agent_sessions` work: an
  initialized session, its Connection boundary, and its historical snapshot
  are never rewritten.
- `api/dependencies/authentication.py` is the first authenticated transport
  dependency: bearer-token extraction plus the #52 `authenticate` service,
  run off the event loop (sync dependency), with a process-level verifier so
  the adapter's JWKS cache works across requests. `api/dependencies/application_errors.py`
  is the one ApplicationError → HTTP mapping (401/403/404/409/422/503);
  routers stay thin and raise/propagate the typed vocabulary.

## Task start, durable FIFO admission, and the planning claim (issue #76)

- `task_start.py` owns Task start and the durable capacity queue. The Owner
  start command runs three strictly ordered phases: a short authorization
  transaction (account-operational barrier FIRST, exact Workspace/Task
  ownership, subject load and the startability precheck) closed before any
  external I/O; the fresh authoritative GitHub revalidation (route, B3
  reconcile, B4 dependency observation) with no database transaction open —
  Start is the commitment point that freshly reconciles the requirements
  fingerprint a successful start freezes (issue #233: the Task carries NO
  baseline before Start, so pre-start issue edits are not drift), while a
  blocked/closed issue, unobservable blocking state, or lost route is NOT
  capacity waiting and fails closed as a typed non-start condition; and one
  short arbitration transaction (barrier FIRST, the per-Workspace
  arbitration advisory lock, the locked re-read bound to the caller's exact
  `state_token`, the durable-projection eligibility guard against the
  fingerprint being frozen, the structural feasibility verdict, one bounded
  FIFO establishment pass, then the target's claim or queue entry — which
  freezes the freshly verified fingerprint atomically with the transition;
  a losing, stale, or failed start leaves no baseline).
- Structural feasibility comes from the complete required role→Connection
  multiset (`assess_structural_route_feasibility`, the read-only
  session-admission seam): existing valid Task reservations count toward the
  total requirement, other Tasks' occupancy does not. An impossible
  configuration (both roles on one capacity-1 Connection, with or without an
  existing reservation) is the actionable `TaskCapacityConfigurationError`
  BEFORE any queue write — never an indefinite capacity wait. Temporary
  occupation stays #68's all-or-nothing decision.
- The durable eligibility guard revalidates every claimed Task against the
  durable GitHub projection alone (identity, OPEN state, fingerprint equality
  against the compared baseline — the fingerprint being frozen for a starting
  target, the Task's own frozen baseline for a queued candidate — and empty
  mirrored blocked-by set) with no GitHub network call: a Task that queued
  while eligible can become ineligible while waiting, and a capacity wakeup
  must not advance it. TaskBlock creation and generalized source-drift
  recovery remain #85.
- The bounded establishment pass advances admissible older candidates in
  durable `(queued_at, id)` order (each claim atomic: both reservations +
  planning + cleared `queued_at` + one `planning_started` event) before the
  target decides; where the fixed work budget cannot establish no-bypass,
  the target durably queues itself with the observable
  `ARBITRATION_DEFERRED` reason. FIFO safety rests on the serialization
  boundary, never on assuming earlier scans stay valid. A queued Task is a
  scheduling state: no `task_queued` event exists, failed capacity checks
  preserve the original `queued_at`, and `reevaluate_queued_tasks` (system
  invocation, Workspace-durable-Owner barrier, OPENORC-actor claims)
  surfaces per-candidate conditions without stalling remaining candidates.
- The explicitly started Task's claim records the OWNER actor;
  arbitration-advanced and sweep-claimed Tasks record the OPENORC actor.
  `planning` here means capacity admitted and startup claimed — never READY
  sessions, workspace preparation, or a successful PLAN (issues #203/#77).

## Planning bootstrap continuation (issue #203)

- `task_planning_bootstrap.prepare_task_for_planning` is the ONE logical E1b
  continuation for a Task claimed into coarse `planning` by the #76 start
  capability: entry/claim (account barrier FIRST → the per-Workspace
  arbitration lock → #53 Owner scope → the #54 exact current `planning` Task
  → the one-shot claim fact → both durable role bindings) → establishment
  (per-role branch BEFORE any lease: READY is reused with no runtime effect,
  CONNECTING goes through #130 with the injected fresh lease's selected
  configuration) → the pre-PREPARE exact-subject recheck → one typed #181
  PREPARE to the exact bound Producer (only `PREPARE`/`COMPLETED` enables
  planning) → the post-PREPARE recheck → the runtime-neutral
  planning-context transition (production Cline: the #74/#190 same-ID
  reconstruction into PLAN at the selected checkout) → the final recheck →
  the E2 (#77) handoff outcome. It never admits capacity again, rebinds a
  route, recreates a Task, replaces a session, clones in Core, or sends a
  PLAN control.
- The one-shot `tasks.planning_bootstrap_claim_token` fact is the
  replay-safety discriminator, written exactly once (conditional on the
  exact `state_token`, a non-archived row, and a still-NULL token)
  immediately BEFORE the first creation dispatch and never rewritten.
  Presence means a prior claim's establishment is unresolved: the
  continuation fails closed (`PlanningBootstrapUnresolvedError`) with zero
  runtime effects even when the caller supplies the refreshed current token
  — a creation may have been dispatched with an unknown outcome, and the
  merged seams cannot prove another creation safe. Absence with both
  bindings CONNECTING is the one provably safe first establishment. This is
  Task-level claim state, not #87/#88 dispatch identity: no outbox, job
  queue, scheduler, recovery engine, or process-local single-flight exists
  here, and uncertain external operations are never retried, downgraded, or
  blindly replayed (Owner/recovery handling is later-layer work; #85/#87/
  #88 and F2/F4 own the general machinery).
- The continuation composes the injected runtime-neutral
  `TaskBootstrapRuntimeComposition` seam (`establishment_lease` /
  `interaction_lease` / `enter_planning_checkout`); the production Cline
  composition is `cline_planning_bootstrap.ClineBootstrapRuntimeComposition`
  over the #190 FRESH/BOUND attachment leases and the #74
  `reconstruct_session` PLAN transition. `ClineConstructionMode`, SDK steps,
  and mode authority never cross into the workflow service, and the
  universal `AgentRuntimeAdapter` surface is never expanded.
- Every recheck (pre-PREPARE, post-PREPARE, post-transition) verifies the
  exact current Task/token, the continuation's own claim identity, and both
  READY bindings' exact external identities before and after each
  consequential external effect; the Producer checkout target is recovered
  from the READY binding's own durable `effective_config_snapshot` (never
  the live `Connection.safe_config`) and is exactly
  `<original base>/<Task UUID>`; the Reviewer stays in its separately
  supplied neutral non-repository directory; there is no checkout registry,
  no new session fields, and no agent-reported path is ever authority.

## E3a: exact-plan REVIEW dispatch (issue #209)

- `task_planning_review_dispatch.dispatch_planning_review` is the ONE
  runtime-facing, exact-plan REVIEW attempt seam: it consumes the exact
  successful E2 handoff (`TaskPlanProductionOutcome` — the persisted current
  PlanRevision and the post-update Task token), and returns the reserved
  exact-subject attempt plus the validated typed `ReviewResult` as an
  internal continuation input for E3b's decision finalization. It never
  finalizes a verdict, records an iteration result, rotates the Task token,
  or decides budget/exhaustion; REVIEW_RESOLUTION and durable
  dispatch/replay are later-layer work (#85/#87–#90).
- Entry composition (one short `composed_transaction`): account barrier →
  exact Owner Workspace/Task scope → the Task row locked `FOR UPDATE`
  (serializing concurrent dispatches for the same Task through existing
  persistence — no new lock, index, or queue machinery) → the #54 exact
  current `planning` Task under the E2 token → the supplied revision proven
  exactly current and content-identical to its persisted row (a stale or
  foreign plan refuses dispatch) → the exact admitted READY Reviewer
  binding resolved by `(Task, role)` (never rebound, recreated, or
  reinitialized) → the durable Repository/issue identity.
- Loop/iteration discipline: on first review exactly one OPEN
  `ReviewLoopPurpose.PLANNING` loop is established snapshotting the
  Workspace's current positive `review_iteration_limit`; subsequent plans
  and Owner-mediated continuation reuse the same loop (the stored budget
  survives Workspace setting edits). More than one planning loop, or a
  closed planning loop while the Task is still planning, fails closed. With
  the loop row locked `FOR UPDATE`, any unfinalized iteration in the loop
  (an unresolved earlier attempt) refuses another ordinary REVIEW, and the
  next-numbered unfinalized ReviewIteration is then reserved atomically for
  the exact current PlanRevision before any external send (unique per-loop
  numbering and the loop-OPEN creation guard are the durable backstops; a
  competing reservation is a stale operation). No PR-head fields.
- The ONE REVIEW send: a deterministic inline-composed model-visible
  message carrying exactly the persisted PlanRevision content verbatim plus
  the durable repository/issue identity — agreeing exactly with the
  `ReviewInteraction(subject=PlanReviewSubject(...))` sent to the exact
  Reviewer session through the #67
  `realize_control(..., expected_family=REVIEW_RESULT_FAMILY)` funnel, with
  NO database transaction open. No separate REVIEW Markdown, no semantic
  rubric, no protocol instruction prose, no Cline-specific path, no new
  prompt template or control asset.
- Known failure and uncertain runtime outcome are distinct typed errors;
  an uncertain send leaves the reserved attempt unresolved — no verdict is
  inferred, no replacement attempt is created, nothing is blindly resent,
  and only a schema-valid exact-subject typed `ReviewResult` is ever
  returned to E3b, which repeats the exact Task/token/current-plan/session
  checks when it records the result and its workflow consequence in one
  transaction.

## E3b: bounded planning review decisions (issue #210)

- `task_planning_review_decision.finalize_planning_review_decision` is the
  ONE planning-loop decision seam: it consumes E3a's internal continuation
  input (`TaskPlanningReviewDispatchOutcome`) and, in one short
  `composed_transaction`, rechecks the exact authorized current non-archived
  `planning` Task under the caller's expected token, the exact current
  PlanRevision, the re-locked OPEN planning loop, the still-unfinalized
  reserved attempt, and the exact READY Reviewer binding — then records the
  result once (one-shot conditional write; a duplicate or raced delivery is
  a stale operation, never a second result/event/gate), records the safe
  REVIEWER `PLAN_REVIEWED` event, and commits the durable next decision.
  It never assesses plan semantics and never settles gates (E3c).
- Decision facts are derived from durable state, never caller input and
  never new persistent counters: the budget state comes from
  `openorc.domain.reviews.count_changes_requested` /
  `autonomous_budget_exhausted` over the loop's snapshotted
  `iteration_limit` (the number of autonomous Producer revisions, not a cap
  on ReviewIterations; the Workspace setting is never consulted for an
  existing loop), and Owner-mediated stickiness comes from durable
  REVIEW_RESOLUTION and IMPLEMENTATION_AUTHORIZATION gate history
  (`list_task_owner_gates`; issue #219 extends the sticky source to the
  superseded accepted-plan implementation gate).
- ACCEPTED leaves the single planning ReviewLoop OPEN (issue #219) and
  yields the accepted PlanRevision/iteration evidence for E5 without
  creating IMPLEMENTATION_AUTHORIZATION, mutating the Task, or closing the
  loop — E5's Owner approval closes it atomically. An in-budget
  CHANGES_REQUESTED keeps the Task PLANNING and the loop OPEN and routes
  the exact finalized findings to merged E2 `revise_task_plan` OUTSIDE any
  transaction — only E2's newly persisted revision and rotated token may
  launch E3a's next ordinary REVIEW, and an uncertain E2 operation is never
  resent here. Exhaustion creates and installs the exact-plan pending
  REVIEW_RESOLUTION gate and moves the Task to `waiting_for_owner` in the
  same transaction.
- `REVIEW_LIMIT_REACHED` is recorded on the FIRST exhaustion only — the
  exhaustion that occurs with no prior REVIEW_RESOLUTION or
  IMPLEMENTATION_AUTHORIZATION gate in durable history. Later Owner-mediated
  disagreements create fresh gates with
  `PLAN_REVIEWED`/`OWNER_GATE_CREATED` without re-representing the
  already-exhausted budget. Exhausted findings are never forwarded into
  gates or events. The amended E3a admission guard
  (`_require_no_iteration_for_current_revision`) refuses another ordinary
  REVIEW of a revision the loop already reviewed; E3c's formal
  Owner-mediated evaluations are a separate later seam and are exempt.
- Event mappings follow the resolved-gate precedent: `PLAN_REVIEWED`
  subjects the finalized ReviewIteration with an `outcome`-value context;
  `OWNER_GATE_CREATED` subjects the new gate; `REVIEW_LIMIT_REACHED`
  subjects the ReviewLoop. No event shadows canonical records.

## E3c: Reviewer-driven REVIEW_RESOLUTION settlement (issue #211)

- `task_planning_review_resolution.settle_planning_review_resolution` is the
  ONE planning settlement seam: it consumes E4's future formal-result
  recognition input — the validated typed `ReviewResult` recognized at the
  Task's pending current REVIEW_RESOLUTION gate (no chat transport or parser
  here) — and, in one short `composed_transaction`, rechecks the exact
  authorized current non-archived `waiting_for_owner` Task under the caller's
  expected token, the exact current pending plan-subject REVIEW_RESOLUTION
  gate addressed through the Task's own current-gate pointer, the unchanged
  exact current PlanRevision, the re-locked OPEN planning loop, and the exact
  existing admitted READY Reviewer binding (verified after E4's external
  send) — then allocates and finalizes a NEW next-numbered ReviewIteration
  for the unchanged revision (no pre-reservation for advisory chat), records
  the REVIEWER `PLAN_REVIEWED` event, stamps the gate `SETTLED` through the
  shared conditional `resolve_owner_gate` persistence primitive (gate stamp +
  pointer clear + token rotation), and records the REVIEWER
  `OWNER_GATE_RESOLVED` event whose context carries `outcome=settled` plus
  the settling `review_iteration_id`/`review_outcome` references.
- Formal ACCEPTED keeps the single planning ReviewLoop OPEN (issue #219)
  and yields the unchanged PlanRevision/iteration evidence for E5 — no
  IMPLEMENTATION_AUTHORIZATION, no implementation authority, no loop
  closure (E5's Owner approval closes it atomically), and no Task status
  change (the Task stays `waiting_for_owner` for E5's flow). Formal
  CHANGES_REQUESTED moves the
  Task `waiting_for_owner` → `planning` under the post-settlement token and
  routes ONLY the newly finalized findings to merged E2 `revise_task_plan`
  OUTSIDE any transaction (the post-settlement token, the unchanged prior
  revision, the new iteration); the historic exhausted findings are never
  rerouted and an uncertain E2 operation is never resent. Only E2's newly
  persisted revision and rotated token may launch E3a's next ordinary
  REVIEW; the next disagreement immediately creates a NEW gate (settled
  gates keep Owner mediation sticky through durable gate history) without
  resetting the spent autonomous budget.
- Fail closed: a stale/foreign/replayed settlement, a non-REVIEW_RESOLUTION
  or PR/head-subject gate, a changed current PlanRevision, a non-READY or
  missing Reviewer binding, a closed loop, and a non-formal
  (non-`ReviewResult`) input apply nothing. The Owner-facing
  `task_mutations.resolve_owner_gate` refuses `SETTLED` — it is never
  Owner-attributed. PR/head-subject settlement belongs to E9 (#84), which
  reuses the same purpose-independent persistence primitive.
- E4 coordination (issue #79): `record_discussion_message_events=True` records
  E4's two direction-only `OWNER_REVIEWER_DISCUSSION_MESSAGE` message facts
  INSIDE the settlement transaction, so the exchange audit commits or rolls
  back atomically with the settlement. The E2 continuation dependencies (the
  runtime adapter and GitHub client) are validated at entry exactly when the
  validated result routes to E2 — the same no-stranding guarantee, scoped to
  the routing outcome; a formal ACCEPTED legitimately needs neither.
- The same module owns issue #219's sibling seam:
  `supersede_implementation_authorization_with_changes` is the ONE G1
  change-request handoff: at a still-current plan-subject
  IMPLEMENTATION_AUTHORIZATION gate, a validated typed CHANGES_REQUESTED
  `ReviewResult` (never an ACCEPTED reply — that stays E4's advisory
  conversation) rechecks the same exact subjects in one short composed
  transaction, allocates and finalizes the NEW next-numbered same-plan
  ReviewIteration, records the REVIEWER `PLAN_REVIEWED` event, supersedes
  the gate through the narrow conditional `supersede_current_owner_gate`
  persistence primitive (pointer clear + `waiting_for_owner` → `planning` +
  token rotation; the gate row stays pending history — no Owner stamp, no
  SETTLED, no `OWNER_GATE_RESOLVED` event), and routes ONLY the new
  findings to E2 outside the transaction on the post-supersession token.
  The Producer/GitHub dependencies are validated at entry (the only formal
  outcome routes to E2), the Task row lock serializes a raced Owner
  approval/cancellation, a closed loop refuses the handoff, and E3b's
  sticky rule treats the gate's durable history as Owner mediation.

## E4: Owner ↔ Reviewer discussion across gates (issue #79)

- `task_owner_reviewer_discussion.send_owner_reviewer_discussion_message` is
  the ONE Owner-authenticated discussion transport: at every pending current
  OwnerGate of a non-archived `waiting_for_owner` Task it sends the Owner's
  nonblank message verbatim to the exact admitted READY Reviewer session as
  ONE ordinary `send(external_session_id, message, expected_family=None)`
  interaction. All four pending gate types admit discussion against their
  real subject form (exact current PlanRevision; the recorded pre-PR
  committed head with no PR invented; canonical PR + exact reconciled head).
- Authorization precedes EVERY adapter acquisition: the service takes
  deferred attachment factories (`reviewer_runtime_factory(binding)` and a
  lazily invoked `producer_runtime_factory()`), and production composition
  binds the #190 BOUND `attach_cline_runtime` to the Phase-1 external
  session identity. The pre-send revalidation re-proves currentness after
  acquisition; the post-send recheck transaction FIRST takes the Task row
  `FOR UPDATE` (the same lock an Owner gate resolution takes) before
  rechecking currentness and writing effects, so a still-current decision
  and its audit/dispatch can never interleave with a concurrent resolution;
  the lock lives only inside that short transaction. No database
  transaction spans an attachment or the send.
- Recognition is deliberately stricter than the #65 parser's
  one-object-amid-prose tolerance: the whole normalized reply must be one
  standalone unfenced JSON object or exactly one fenced JSON object with
  nothing outside the fence; only that candidate is validated through the
  existing #65 `parse_formal_response`, and only a `review_result` family
  result becomes formal. Recognition runs at REVIEW_RESOLUTION gates and —
  issue #219 — at still-current plan-subject IMPLEMENTATION_AUTHORIZATION
  gates, where only a `CHANGES_REQUESTED` result is a workflow trigger and
  a schema-valid ACCEPTED reply (like ordinary prose) stays advisory with
  the gate still approvable. All other gates never invoke recognition —
  all content there is advisory.
- Audit is two direction-only `OWNER_REVIEWER_DISCUSSION_MESSAGE` events per
  still-current exchange (Owner message, Reviewer reply) through the #56
  event-coordination helper — direction-only context, never text. For a
  recognized formal result, E4 dispatches the authoritative continuation
  seam with the audit flag so the message facts commit atomically with it:
  E3c's settlement at a plan-subject REVIEW_RESOLUTION gate, and issue
  #219's E3 G1 change-request handoff at an implementation gate (only a
  CHANGES_REQUESTED result ever reaches that dispatch); a PR/head
  REVIEW_RESOLUTION result fails closed until E9 (#84). Uncertain sends,
  stale races, and audit failures return typed safe errors and never
  present a failed exchange as success; recovery/durable dispatch belongs
  to #85/#87–#90, never to this service.

## E5: implementation authorization and the planning close boundary (issue #80)

- `task_implementation_authorization.open_implementation_authorization` is
  the ONE gate-presentation seam: an Owner-authorized continuation of
  accepted formal review that derives every clearance fact from the
  canonical tables inside one short composed transaction — the account
  operational barrier, exact Owner Workspace/Task scope, the Task row
  locked `FOR UPDATE` under the caller's expected token, the pre-gate
  state (`planning` after ordinary E3b acceptance, or `waiting_for_owner`
  with no current gate after E3c settlement), the exact current
  PlanRevision, the re-locked single OPEN planning ReviewLoop, and the
  latest adjudicating finalized Reviewer ACCEPTED bound to that exact
  revision — then creates and installs one pending exact-plan
  IMPLEMENTATION_AUTHORIZATION gate, moves a still-`planning` Task to
  `waiting_for_owner`, and records the safe OPENORC `OWNER_GATE_CREATED` +
  `PLAN_READY` facts. It never trusts a caller's free-text acceptance or a
  detached E3b/E3c outcome object, never dispatches the Producer, never
  appends a Reviewer iteration, and a replay/race can never duplicate the
  current gate. After #219 retires a presented gate, a distinct
  Reviewer-accepted revision earns a NEW gate through the same path.
- `approve_implementation_authorization` is the one irreversible planning
  boundary: the exact current pending gate resolves APPROVED with truthful
  OWNER provenance, the planning ReviewLoop closes exactly once (only
  now), the Task moves `waiting_for_owner` to `implementing` under a
  rotated token, and the `OWNER_GATE_RESOLVED` (approved) +
  `IMPLEMENTATION_AUTHORIZED` events commit atomically with them — a
  forced late failure leaves no partial facts. It returns the durable
  Task/gate/revision/iteration/loop handoff for E6 (#81) and never
  realizes or sends anything: E6 owns all runtime interaction, including
  addressing the READY Producer binding. A post-approval replay is stale
  (the resolution is one-shot), never a second approval.
- `cancel_implementation_authorization` is the narrow cancel-at-gate
  orchestration: the exact pending gate resolves CANCELLED with OWNER
  provenance (never REJECTED — there is no Owner "Decline") and the Task
  archives CANCELLED with its safe OWNER `TASK_CANCELLED` event in one
  composition, so an exact pending gate cannot survive its Task's
  cancellation. Broad cross-stage cancellation belongs to E10 (#85); a
  Task with no current gate cancels through the existing `archive_task`
  primitive.
- E4's `send_owner_reviewer_discussion_message` stays the only discussion
  transport at the pending gate; E5 reads no discussion state and adds no
  reconsideration command, so ordinary discussion keeps the gate pending
  and approvable, and an advisory standalone Reviewer ACCEPTED at the gate
  never re-derives clearance. New #56 event mappings:
  `record_plan_ready_event` (OPENORC, plan-revision subject) and
  `record_implementation_authorized_event` (OWNER, plan-revision subject);
  both accept no content parameter, so no canonical fact is shadowed into
  event context.

## E6: authorized Producer execution and the accepted-review handoff (issue #81)

- `task_authorized_implementation_execution.execute_authorized_implementation`
  is the ONE execution seam, and it automates exactly the proven manual
  handoff: committed E5 Owner approval → the exact existing Producer session
  PLAN→ACT → one send of the persisted accepted `review_result` JSON →
  validate one typed `ImplementationResult` → record the Execution attempt
  and outcome. Reviewer ACCEPTED is the message delivered to the Producer;
  Owner APPROVED is permission to deliver it — a Reviewer result alone never
  starts implementation. The caller cannot supply a free-text acceptance or
  choose a Producer session.
- Admission (one short transaction, fails closed before any write) derives
  every authority fact from canonical state under the Task row `FOR UPDATE`:
  account-operational + exact Owner Workspace/Task scope, the caller's exact
  `state_token`, a non-archived `implementing` Task with no current gate and
  an unchanged current PlanRevision, the E5-approved historical
  IMPLEMENTATION_AUTHORIZATION gate bound to exactly that plan, the CLOSED
  planning ReviewLoop whose latest finalized iteration for that revision is
  ACCEPTED, and the existing READY Producer binding (never allocated,
  replaced, reinitialized, or rebound). Initial-implementation exclusivity
  refuses on ANY prior attempt history — concurrent/replayed starts,
  earlier successes, unresolved dispatches, and older failed attempts alike;
  retries belong to the later recovery layer (#85/#87–#89), never to a
  second dispatch from this seam. `create_execution` reserves attempt 1 and
  the focused claim moves it `queued → running` with the F5
  `last_qualifying_activity_at` marker born in the same statement,
  committing the dispatch claim before any effectful ACT transition or send,
  with the Producer-attributed `EXECUTION_STARTED` audit recorded atomically.
- The external phase is authority-gated with NO transaction open: the
  deferred Producer attachment is acquired first, then the exact subject set
  is revalidated (R1) before the native `ImplementInteraction` realization
  on the same external session ID; after realization the optional D6c
  activity subscription is established, the subject set is revalidated again
  (R2) immediately before the one send, and the send carries the canonical
  accepted envelope (shared `review_result_envelope.serialize_review_result`;
  the ACCEPTED loader is `accepted_review_result_from_iteration`) with
  `expected_family=IMPLEMENTATION_RESULT_FAMILY`. A schema-valid result is
  necessarily `status: COMPLETED` (the schema pins the const). No
  `implement.md`, no appended instructions, no plan re-send, no Reviewer
  runtime action, no transcript synthesis.
- The finalize (one short transaction) re-proves the exact subject set and
  the still-running attempt under the Task row lock, then persists the
  validated canonical `implementation_result` payload and the `succeeded`
  stamp in one conditional statement with the Producer-attributed
  `EXECUTION_COMPLETED` audit. The result's `branch` is a Producer
  discovery/routing claim only — never canonical Task branch or GitHub head
  state; E7 (#82) owns reconciliation and PR_AUTHORIZATION, and the Task is
  never advanced here. Additive `openorc.executions` columns carry only the
  nullable canonical result payload and the nullable activity marker — no
  branch/SHA field, transcript, prompt, or dispatch table, ever.
- Failure classification is truthful: a known failure that provably occurred
  before any runtime effect (known attachment failure, known realization
  failure, the R1 stale-authority refusal) finalizes
  `failed_transient`/`failed_final` with a safe `execution_failed` audit
  (`failure_kind` context token only); every post-claim uncertainty —
  uncertain attachment/reconstruction, timeout/disconnect, session loss,
  invalid formal completion after possible work, the R2 stale-authority
  refusal, or a finalize that cannot commit an already-returned result —
  leaves the attempt conservatively `running` (the durable
  potentially-dispatched record), reports a typed content-safe error, and
  never resends, never fabricates success, and never grants retry
  permission. Settlement belongs to the later recovery layer; no abort or
  retry engine is installed here.
- The activity marker is advisory F5 fuel (issue #90 consumes it): the
  injected subscription factory adapts the Cline-local D6c
  `subscribe_activity` seam, the service owns the qualification filter
  (thinking, iteration/work polling, tool lifecycle, non-formal commentary
  qualify; notices, turn-finish, runtime status/error/ended, and usage
  never do), and the listener stamps `last_qualifying_activity_at` with
  database time through a short conditional write that is a rejected no-op
  on finalized history. No activity content is ever persisted; unavailable
  telemetry, other sessions, and subscription failure alter nothing.
  `execution_started`/`execution_completed`/`execution_failed` #56 recorders
  are Producer-attributed (`producer_actor`) with execution-subject
  references and empty or failure-kind-only context — no canonical fact is
  shadowed into event context.

## E7: the exact-head PR authorization gate (issue #82)

- `task_pr_authorization.open_pr_authorization` is the ONE
  `PR_AUTHORIZATION` presentation seam, and `approve_pr_authorization` /
  `cancel_pr_authorization` are its only Owner actions — Approve and
  Cancel, nothing else. The merged generic E4 discussion service is never
  called at this gate type, and no Discuss, Decline/Reject,
  changes-request, formal Reviewer iteration, or Reviewer pre-PR review
  path exists here.
- The trigger is durable, never conversational: only the persisted
  successful current E6 Execution (the Task's latest attempt, `SUCCEEDED`,
  non-NULL `implementation_result` whose persisted envelope re-derives as
  the `implementation_result` family at the pinned schema version with
  `status: COMPLETED` and a usable `branch` routing claim) may open the
  gate. No webhook, runtime text, or caller-supplied branch/SHA/outcome is
  authority; the original Producer binding is re-proven; an optional
  caller `execution_id` must be exactly the latest attempt.
- Composition: admission+authority (one short transaction under the Task
  row lock: exact token, non-archived `implementing`, no current gate, no
  canonical `TaskPullRequest`, the E5-approved exact-plan evidence, the
  CLOSED planning loop with its Reviewer acceptance, the qualifying
  execution) → fresh GitHub verification with NO transaction open (B3
  `reconcile_repository_issue` on the Task's routed repository, then the
  shared read-only `canonical_branch.observe_routed_repository_branch` B7
  seam for the branch/head — GitHub alone supplies the committed SHA) →
  the final short transaction re-proves the authenticated Owner's
  account-operational barrier FIRST (the admission check does not remain
  authoritative across the intervening GitHub calls), then every admission
  fact under the
  lock and commits the one-time `bind_canonical_branch`, one pending
  exact-head gate (`subject_head_sha` only — no PlanRevision, no PR
  binding), `implementing → waiting_for_owner` under token rotation, and
  the OPENORC `OWNER_GATE_CREATED` audit together. Replay/race can never
  create a second gate or an orphaned bind; a bound same-branch
  continuation re-verifies without rebinding; a different-branch claim is
  a conflict, never adoption; a repository-wide branch-ownership collision
  (`UniqueViolation`) is translated into the same stable `ConflictError`
  with the whole composition rolled back.
- Source invalidity is reason-specific: ONLY a confirmed requirements
  fingerprint change against the Task's immutable baseline takes the
  atomic `BLOCKED / GITHUB_SOURCE_CHANGED` path — one short transaction
  re-proving the account barrier first and then the drift durably under
  the Task lock, then
  `create_task_block` + `implementing → blocked` (token rotation) + the
  `task_blocked` audit (the first `record_task_blocked_event` mapping,
  task_block subject, no context shadowing the block row) commit or roll
  back together, with no gate. A closed source issue, a stable-identity
  mismatch, access loss, known provider failure, and uncertain outcomes
  are typed fail-closed refusals (`ConflictError` / `AuthorizationError` /
  `ExternalOperation*Error`) that fabricate no drift, no block, and no
  gate. Recovery from `blocked` belongs to later layers, not this leaf.
- Approve: one short transaction proving the account barrier, exact Owner
  scope, the current non-archived `waiting_for_owner` Task under the
  expected token, the exact current pending `PR_AUTHORIZATION` gate with
  its head-SHA subject, the bound canonical branch, and coherent E6
  success/approved-plan evidence (the persisted branch claim equals the
  bound branch); resolves ONLY that gate APPROVED one-shot (pointer clear
  + token rotation) with the OWNER-attributed `OWNER_GATE_RESOLVED` audit
  (authenticated Profile `actor_id` — E8's durable approving-Profile
  recovery). The Task stays `waiting_for_owner`: no `reviewing`
  transition, no PR creation, no dispatch, ever. Cancel: the E5
  cancellation pattern — the gate CANCELLED with OWNER provenance and
  terminal `cancelled` archival with `TASK_CANCELLED` in one transaction;
  no GitHub artifact is touched.
- E7 never calls `publish_task_pull_request`, `pr_compose`, GitHub
  create-PR, Reviewer PR review, or merge, and never calls the Producer;
  E8 (#83) owns publication. No database transaction spans GitHub I/O;
  telemetry carries only the safe identifier vocabulary.

## E8: Producer PR composition and race-safe canonical PR publication (issue #83)

- `task_pr_composition.compose_and_publish_task_pr` is the ONE publication
  seam after E7's durable approval. Admission is one short DB-only
  transaction that re-derives the persisted APPROVED historical
  `PR_AUTHORIZATION` gate — the current-pointer guard is deliberately NOT
  used (E7 intentionally cleared the pointer on approval) — its exact
  head-SHA subject, and the single OWNER-attributed `owner_gate_resolved`
  audit whose Profile is the only publication identity; a mismatched
  caller/approver is rejected, never substituted. Fresh-path admission
  additionally resolves the READY Producer binding (never created or
  replaced), the current Workspace guidance, and the routed Repository
  presentation context (full name and the workflow base from the durable
  routed record — never from the Producer).
- The fresh path runs ONE `PR_COMPOSE` interaction on the exact binding
  through the #67 realize-control funnel with R1 (post-acquisition) and
  post-interaction revalidations; uncertain outcomes are never resent and
  known runtime/protocol failures stay distinct. The validated
  `pr_result` title/body are presentation only: a missing effective
  closing-keyword reference to the Task's exact issue in the authoritative
  repository is appended once (fenced/inline-code examples, other issues,
  and other repositories never satisfy it), and presentation can never
  override the authoritative repository, branch, base, head SHA, or
  approving Profile.
- Publication goes through `pull_request_publication.publish_task_pull_request`
  (B8) exactly once — never reimplemented, wrapped, or fallen back — and
  only `PUBLISHED` (or a still-valid reconciled continuation record)
  finalizes `waiting_for_owner → reviewing` with token rotation plus
  exactly one `PR_CREATED` audit (subject the canonical TaskPullRequest,
  truthful OPENORC actor, empty context). E8 never dispatches review;
  #84 owns that.
- The review-eligibility guard is re-proved from persistence in the
  finalize: open + unmerged, the Task's canonical `head_ref`, the
  authorized `base_ref`, the exact approved head SHA, and the stable
  GitHub PR identity. Closed-unmerged records take the atomic
  `pr_closed_unmerged` block; a confirmed merge refuses with zero durable
  mutation (merge confirmation/completion belongs to E10 — never a false
  unmerged-closure block). Reconciled head-SHA drift routes through the
  same recoverable stale guard: `stale_operation` TaskBlock + `blocked`
  transition + `task_blocked` audit where the Task remains current, zero
  mutation where it has moved on — never a PR recreation or replacement.
- The narrow continuation completes E8's own interrupted publication: an
  admitted record carrying the exact approved head is reconciled
  authoritatively through `reconcile_task_pull_request` and finalized —
  never repeating the composition, re-creating, or adopting an unrelated
  PR. `PREFLIGHT_STALE` and the post-create stale classifications use the
  same recoverable block shape. No transaction spans the runtime
  interaction, the reconciliation, or any GitHub call; telemetry carries
  only the safe identifier vocabulary.

## E9: commit-addressed PR review, remediation, and merge decision (issue #84)

- The PR review conversation continues over the one canonical GitHub PR and
  its exact reconciled head. `task_pr_review_dispatch.dispatch_pr_review` is
  the ONE ordinary dispatch seam: Phase A0 is a short database-only
  authenticated admission (account barrier + exact Owner Workspace/Task
  scope) that is closed BEFORE the system-capable
  `reconcile_task_pull_request` runs, so a foreign/cross-Owner invocation
  can never trigger a GitHub read or mutate the target PR's persisted
  reconciliation snapshot; Phase B re-proves everything under the Task row
  `FOR UPDATE` plus the loop row `FOR UPDATE` (non-archived `REVIEWING` at
  the expected token, no current OwnerGate, canonical feature branch, the
  exact reconciled head, the exact admitted READY Reviewer session, one
  OPEN `PR_REVIEW` loop established once with the Workspace limit snapshot,
  no unresolved earlier attempts, no duplicate dispatch for an
  already-reviewed head) and reserves exactly one next-numbered PR/head
  iteration; Phase C re-reads the persisted reconciled head immediately
  before the send (a moved head is a never-dispatched dead letter whose
  reserved iteration stays unfinalized); Phase D sends ONE
  `ReviewInteraction(PrReviewSubject)` with
  `expected_family=REVIEW_RESULT_FAMILY` whose entire message is the
  canonical structural JSON of the persisted `implementation_result` (no
  prose, no SHA text, no rubric).
- Durable completion evidence is the latest successful Execution with a
  non-NULL `implementation_result`: the initial E6 result is valid exactly
  while the reconciled head equals the published head; a changed head
  requires a successful remediation attempt claiming the latest-numbered
  finalized CHANGES_REQUESTED iteration of the PR loop. Stale evidence
  never dispatches; a GitHub-observed changed head never fabricates a
  Producer completion; base-branch movement alone is never a stale
  review-subject condition.
- `task_pr_review_decision` owns the atomic next decisions in one short
  Task-locked transaction: finalize once + safe REVIEWER `PR_REVIEWED`
  (subject the ReviewIteration; outcome value only in context), then a
  within-budget `CHANGES_REQUESTED` keeps `REVIEWING` and routes the
  remediation outside the transaction; an exhausted/sticky disagreement
  installs the exact PR/head `REVIEW_RESOLUTION` (`REVIEWING` to
  `WAITING_FOR_OWNER`, `REVIEW_LIMIT_REACHED` on the first exhaustion only);
  an `ACCEPTED` installs the exact-head `MERGE_DECISION` with the loop left
  OPEN (only E10 closes it). PR-scoped stickiness derives only from this
  canonical PR's `REVIEW_RESOLUTION`/`MERGE_DECISION` gate history —
  deliberately narrower than E3b's all-gates helper; planning gates never
  consult into the PR budget.
- `task_pr_remediation.route_pr_remediation` is the PR-specific Producer
  continuation: a durable one-shot claim (`executions.review_iteration_id`
  + partial unique index + composite-FK backstop — at most one attempt can
  ever claim one finalized CHANGES_REQUESTED iteration, token-independent,
  classified stale on `UniqueViolation`), the durable-before-send ordering,
  no token rotation at claim, ONE `ReviseInteraction` with
  `serialize_review_result` and `expected_family=IMPLEMENTATION_RESULT_FAMILY`
  on the original Producer ACT session, E6's exact known-failure versus
  uncertain/potentially-dispatched result discipline, then branch-claim
  verification, B7 reconciliation of the SAME canonical PR, a
  GitHub-confirmed NEW head, and the re-entered ordinary dispatch. Missing
  push, wrong branch, closed/unmerged, or conflicting identity fails
  closed — never invented success, never another PR.
- `task_owner_reviewer_discussion` routes the PR/head formal continuations:
  a standalone valid `review_result` at a PR/head `REVIEW_RESOLUTION`
  dispatches `settle_pr_review_resolution` (new same-head finalized
  iteration + `SETTLED` provenance via the #211 primitive; ACCEPTED means a
  new MERGE_DECISION; CHANGES_REQUESTED means back to `REVIEWING` plus
  remediation), and at a pending PR/head `MERGE_DECISION` only a standalone
  valid `CHANGES_REQUESTED` is actionable —
  `retire_pr_merge_decision_with_changes` retires G1 through the guarded
  `supersede_current_owner_gate` `reviewing` destination (pending history,
  never Owner-stamped, never SETTLED, no false gate-resolution event) while
  a standalone ACCEPTED — like ordinary prose — stays advisory. The
  remediation dependencies are validated at entry exactly when the result
  routes to remediation (the #219 no-stranding guarantee), and the
  discussion-audit flag commits the exchange events atomically with the
  authoritative continuation.

## E10a: exact-head merge authorization and GitHub-confirmed completion (issue #227)

- `task_merge_lifecycle.request_task_merge` is the ONE Owner merge command:
  at the Task's exact current pending `MERGE_DECISION` gate it re-proves the
  same Profile's Workspace/Task Owner authority, the gate's exact canonical
  `TaskPullRequest` + Reviewer-accepted head binding, and the OPEN
  `PR_REVIEW` loop; authoritative B7 reconciliation (outside any
  transaction) routes a confirmed merged PR to the shared completion path, a
  confirmed closed-unmerged PR refuses without consuming the gate (#229 owns
  it), and a changed head applies the narrow `stale_operation` block
  consequence; the routed installation's issue-open guard refuses a
  GitHub-closed source issue without consuming the pending gate. One short
  composed transaction resolves the gate APPROVED and commits the OWNER
  `OWNER_GATE_RESOLVED` + `MERGE_REQUESTED` audit pair — the durable
  one-shot claim — and `pull_request_merge.request_pull_request_merge` is
  called exactly once with the gate's SHA, the approving Profile, and the
  `claim_revalidation` hook (the primitive's final pre-send transaction
  re-proves the claim without granting a second merge authority).
- Outcome classification states which external effect occurred, by type
  only: a confirmed merge completes through the shared seam; a GitHub-owned
  refusal (`ExternalOperationRejectedError`, the machine-readable subtype
  the merge primitive raises for the `REJECTED` outcome) records
  `MERGE_REJECTED_BY_GITHUB`, while the plain `ExternalOperationFailedError`
  (credential resolution, authentication/authorization rejection, rate
  limiting — proven pre-delivery non-delivery) records no GitHub-refusal
  event; after an authoritative no-merge proof on the unchanged accepted
  head a NEW pending exact-head `MERGE_DECISION` gate requires fresh Owner
  authorization; a confirmed merge whose post-merge reconciliation fails —
  or succeeds without yet confirming the merge durably — raises
  `MergeConfirmedReconciliationPendingError` (evidence preserved, mutation
  never repeated, one reconciliation re-attempt); uncertain
  outcomes block `EXTERNAL_OPERATION_UNCERTAIN` with durable safe context
  and are never repeated; an expected-head mismatch blocks
  `STALE_OPERATION` and never transfers Reviewer acceptance to the
  unreviewed head.
- `task_merge_lifecycle.complete_confirmed_task_merge` is the ONE
  GitHub-authoritative completion seam, invoked by the Owner path AND — as
  the narrow B6 consequence — by `github_webhook_dispatch._dispatch_pull_request`
  and the sweep's PR-unit orchestration after a successfully reconciled
  merged canonical PR. It reloads the durable merged facts under lock
  (webhook inputs are never trusted), closes the OPEN `PR_REVIEW` loop
  (absent is legitimate; the planning loop is never touched), archives
  `COMPLETED` through the guarded archival primitive (which clears the
  current-gate pointer without inventing Owner gate events), and commits
  `PR_MERGED` (GITHUB) + `TASK_COMPLETED` (OPENORC) exactly once. Archived
  `CANCELLED` history is immutable; an already-`COMPLETED` archive is the
  idempotent `ALREADY_COMPLETED` no-op. Reconciliation stays strictly
  update-only; the consequence call duplicates no GitHub read, webhook
  dedupe, polling, or scheduling.

## E10b: Owner Abort and the unified Task cancellation (issue #228)

- `task_abort.abort_task_activity` is the ONE Owner-authorized Abort
  command: stop OpenOrc first, then interrupt ClineBox. Its dispatch phase
  (current Task in `planning`/`implementing`/`reviewing`, or at a pending
  OwnerGate with a demonstrably in-flight runtime interaction) proves the
  account/Owner/currentness guards before any Cline I/O, and resolves the
  exact existing bound `TaskAgentSession` targets. Eligibility is
  phase-specific: the autonomous phases (`planning`/`implementing`/
  `reviewing`) are eligible against their appropriate existing bound
  runtime session, while a WAITING_FOR_OWNER Task must demonstrate in-flight
  activity — an ordinary pending gate does not establish active work, so a
  Producer target must own a durably OPEN Execution anchored to that exact
  session (`producer_session_id`) and a Reviewer target an OPEN ReviewLoop
  carrying an unfinalized ReviewIteration; an idle gate refuses. The atomic
  composition then applies the
  `owner_action_required` TaskBlock with the safe `owner_abort` context, the
  `blocked` transition under the rotated token (a pending gate stays
  truthful pending history, unusable while blocked), the OWNER
  `task_blocked` audit, and the durable one-shot exact-session Interrupt
  intents. AFTER the commit, `ClineAdapter.abort_activity` is invoked
  exactly once per exact target, and each safe outcome
  (acknowledged / definitively failed / uncertain) is recorded on its
  durable intent. An SDK acknowledgement is recorded in place and never
  settles the intent; a definitive non-delivery records `failed`; an
  uncertain outcome records `unknown` and is never replayed. The
  settlement phase (the Task already `blocked` by this control) never
  re-interrupts: without positive caller-attested D6b evidence
  (`TaskInterruptSettlement`) it refuses and surfaces the unresolved
  interruption; with it, `ClineAdapter.settle_abort` runs outside any
  transaction and a positively settled intent is the only state in which
  Cancel becomes available. The settlement records and the exact-attempt
  Execution accounting commit in ONE short transaction after the positive
  cessation was established: the Task's durably open Execution attempts
  transition to the existing `cancelled` final status through the guarded
  conditional primitive (truthful history preserved — no invented
  completion result, no interruption state machine), so a crash can never
  leave a settled interruption with an unaccounted open Execution vetoing
  Cancel while the settlement entrypoint could not run again; the
  still-unsettled durable state remains truthfully re-settleable instead.
  The Task stays
  `blocked` regardless of the Interrupt outcome; late formal results can
  never advance it; terminal session identities are preserved (no
  `stop`/`start`/`send`, no replacement session, no prompt).
- `task_cancellation.cancel_task` is the ONE Owner-authorized unified
  Cancel command from a current safely inactive Task (`ready_to_plan`,
  `queued`, the exact current pending OwnerGate of any type, or `blocked`
  with every interruption settled). Ordering is load-bearing: the
  available authoritative canonical-PR facts are established BEFORE any
  irreversible mutation — a PR already authoritatively confirmed merged at
  the point of decision routes to E10a's `complete_confirmed_task_merge`
  (never an Owner `CANCELLED` claim), and a read failure still allows
  cancellation with a durable unresolved PR-close obligation. The final
  decision transaction revalidates those facts under the Task row lock
  BEFORE any mutation: the durable canonical PR identity must still match
  the pre-decision record, and a merge B7 reconciliation confirmed between
  the observation and the lock (no Task token rotation involved) routes to
  E10a's completion service instead of an Owner `CANCELLED` claim. One
  shared short transaction (`task_cancellation.cancel_task_composition`, the
  ONE common cancellation composition to which E5's
  `cancel_implementation_authorization` and E7's `cancel_pr_authorization`
  now delegate, keeping their exact-addressed gate-type discipline)
  re-proves authorization/currentness/the accounted-runtime veto under the
  Task row lock, then atomically resolves the gate `CANCELLED` with OWNER
  provenance, closes every OPEN ReviewLoop, archives `CANCELLED` (which
  clears `queued_at` and the current-gate pointer and releases the issue's
  one-current-Task mapping), commits the one OWNER `TASK_CANCELLED` event,
  and records the one-shot PR-close intent when the canonical open/unmerged
  PR may require closure. The veto composes the durable outstanding-work
  facts available in v1 (interrupt intents not positively settled, open
  Executions, pending RuntimeRequests); per-send durable dispatch identity
  arrives with F2 (#87/#89). Autonomous progression is inapplicable — the
  Owner uses Abort first.
- The one-shot post-archival closure attempt runs strictly after the
  commit: a fresh B7 observation; already closed-unmerged records
  `no_action` with no write; a PR observed merged after terminal
  cancellation preserves the archived outcome and never rewrites history;
  an open PR at the same stable identity is closed exactly once through
  the new `GitHubAppClient.close_pull_request` primitive under the
  recorded Profile's user credential (#143 machinery, bounded definitive-401
  recovery included), and the observed outcome is confirmed through
  reconciliation — never the close answer alone. Pre-delivery conditions
  keep the obligation pending for the later reconciliation machinery; a
  definitive rejection records `failed`; an unconfirmable outcome records
  `unknown` — never blindly replayed. The durable intent records live in
  `openorc.task_interrupt_intents` and
  `openorc.task_pull_request_close_intents` (issue #228 migration); audit
  events alone are not a work queue, and no general outbound-operation
  framework exists here (F2/F4 own delivery identity; E10c #229 owns the
  externally closed-unmerged terminal cause; F4b #230 owns non-cancellation
  recovery).

## E10c: the trusted PR-closure cancellation (issue #229)

- `task_cancellation.cancel_task_for_confirmed_pr_closure` is the ONE trusted
  GitHub-observation cancellation entrypoint: when existing authoritative B7
  reconciliation has durably confirmed a Task's canonical PR CLOSED without
  merge, the still-current Task is terminally cancelled through the shared
  E10b composition (`_cancel_composition`'s second trusted admission mode —
  two distinct trusted admission paths over one archival core). It takes no
  caller-selected actor, cause, PR number, Profile identity, or state token,
  and performs no external I/O: the evidence precondition is the durable
  canonical PR state itself, re-proved under the Task row lock AND under the
  canonical PR row lock (`require_canonical_task_pull_request_for_update`,
  the focused `FOR UPDATE` read) held through the terminal archival, so a
  concurrent B7 write phase can never commit a confirmed merge between the
  evidence decision and the cancellation. A raced confirmed merge — or any
  other non-evidence durable state — refuses fail-closed and applies
  nothing; the retryable read/reconciliation paths re-classify and route
  merged facts to E10a instead.
- Unlike Owner Cancel, the externally confirmed terminal closure is not
  gated on runtime idleness (no accounted-runtime veto), admits every
  non-archived status, resolves no OwnerGate (the pending gate stays
  untouched history and the archival clears the current-gate pointer per
  E10a's historical-provenance policy), and records no PR-close intent —
  the external closure already happened, so no Profile user credential is
  required. The terminal `task_cancelled` event carries truthful GITHUB
  provenance with no actor identity, and the archival stamps the canonical
  `pr_closed_unmerged` terminal cause on the Task (`terminal_cause` — a
  Task-owned archived fact, deliberately separate from an active TaskBlock;
  the `TaskBlockReason` value survives only for historical rows and no new
  live TaskBlock of that reason is ever written). Archived history is
  immutable: an already-archived Task (`CANCELLED` by Owner/GitHub or
  `COMPLETED` by merge) is the idempotent `ALREADY_TERMINAL` no-op.
- The consequence is wired only into the existing observation paths:
  `github_webhook_dispatch._dispatch_pull_request` applies it after a
  successful B7 reconciliation of a closed-unmerged canonical PR (parallel
  to the E10a merged branch; failures flow through the route failure
  classification and keep the delivery recoverable — never marked processed
  when the archival failed), and `github_reconciliation_recovery` applies it
  at the sweep's PR-unit orchestration point through
  `_cancel_closed_unmerged_task_consequence` (failures are contained as the
  sweep's own typed outcome so the consequence gap stays visible and
  re-runnable). E7's post-publication eligibility routes the confirmed
  closed-unmerged branch of `_finalize_or_block` to this entrypoint after
  the rejecting finalize transaction has rolled back — the eligibility
  classification deliberately precedes the exact head-SHA drift check so an
  externally established terminal PR state is never masked as a stale
  publication subject, while `STALE_OPERATION` blocking for open-PR
  mismatches and the confirmed-merge refusal are unchanged. No generic
  blocking service, no GitHub PR-close write, no session/checkout mutation,
  and no F2/F4 recovery belong here.


Read domain plus relevant persistence/adapter/API/worker guides for cross-boundary changes.
