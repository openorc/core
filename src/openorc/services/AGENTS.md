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
  a changed source, blocked/closed issue, unobservable blocking state, or
  lost route is NOT capacity waiting and fails closed as a typed non-start
  condition; and one short arbitration transaction (barrier FIRST, the
  per-Workspace arbitration advisory lock, the locked re-read bound to the
  caller's exact `state_token`, the durable-projection eligibility guard,
  the structural feasibility verdict, one bounded FIFO establishment pass,
  then the target's claim or queue entry).
- Structural feasibility comes from the complete required role→Connection
  multiset (`assess_structural_route_feasibility`, the read-only
  session-admission seam): existing valid Task reservations count toward the
  total requirement, other Tasks' occupancy does not. An impossible
  configuration (both roles on one capacity-1 Connection, with or without an
  existing reservation) is the actionable `TaskCapacityConfigurationError`
  BEFORE any queue write — never an indefinite capacity wait. Temporary
  occupation stays #68's all-or-nothing decision.
- The durable eligibility guard revalidates every claimed Task against the
  durable GitHub projection alone (identity, OPEN state, fingerprint
  equality, empty mirrored blocked-by set) with no GitHub network call: a
  Task that queued while eligible can become ineligible while waiting, and a
  capacity wakeup must not advance it. TaskBlock creation and generalized
  source-drift recovery remain #85.
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
  REVIEW_RESOLUTION gate history (`list_task_owner_gates`).
- ACCEPTED closes the loop and yields the accepted PlanRevision/iteration
  evidence for E5 without creating IMPLEMENTATION_AUTHORIZATION or
  mutating the Task. An in-budget CHANGES_REQUESTED keeps the Task
  PLANNING and the loop OPEN and routes the exact finalized findings to
  merged E2 `revise_task_plan` OUTSIDE any transaction — only E2's newly
  persisted revision and rotated token may launch E3a's next ordinary
  REVIEW, and an uncertain E2 operation is never resent here. Exhaustion
  creates and installs the exact-plan pending REVIEW_RESOLUTION gate and
  moves the Task to `waiting_for_owner` in the same transaction.
- `REVIEW_LIMIT_REACHED` is recorded on the FIRST exhaustion only — the
  exhaustion that occurs with no prior REVIEW_RESOLUTION gate in durable
  history. Later Owner-mediated disagreements create fresh gates with
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
- Formal ACCEPTED closes the loop and yields the unchanged
  PlanRevision/iteration evidence for E5 — no IMPLEMENTATION_AUTHORIZATION,
  no implementation authority, and no Task status change (the Task stays
  `waiting_for_owner` for E5's flow). Formal CHANGES_REQUESTED moves the
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
  acquisition; the post-send recheck re-proves it before any effect. No
  database transaction spans an attachment or the send.
- Recognition is deliberately stricter than the #65 parser's
  one-object-amid-prose tolerance: the whole normalized reply must be one
  standalone unfenced JSON object or exactly one fenced JSON object with
  nothing outside the fence; only that candidate is validated through the
  existing #65 `parse_formal_response`, and only a `review_result` family
  result becomes formal. Ordinary gates never invoke recognition — all
  content there is advisory.
- Audit is two direction-only `OWNER_REVIEWER_DISCUSSION_MESSAGE` events per
  still-current exchange (Owner message, Reviewer reply) through the #56
  event-coordination helper — direction-only context, never text. For a
  recognized formal result, E4 dispatches E3c's settlement with the audit
  flag so the message facts commit atomically with settlement; a PR/head
  REVIEW_RESOLUTION result fails closed until E9 (#84). Uncertain sends,
  stale races, and audit failures return typed safe errors and never
  present a failed exchange as success; recovery/durable dispatch belongs
  to #85/#87–#90, never to this service.

Read domain plus relevant persistence/adapter/API/worker guides for cross-boundary changes.
