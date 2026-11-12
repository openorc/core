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
## Shared operational-failure path (Levels 1–3)

- **Level 1 — operation service:** Execute one authorized logical attempt against its exact current Task/subject; own durable one-shot claims, typed failure/effect evidence, and ordinary success/finalization. Workflow-semantic/stale refusals are not automatically operational failures. Never treat an uncertain external effect as proven non-delivery.
- **Level 2 — operational disposition:** For applicable workflow invocations, `task_workflow_operation_coordinator` carries the failing Level 1 invocation's own durable attempt identity, currentness, and typed evidence to `task_operational_failure_triage`. This service alone decides bounded, positively safe automatic retries as distinct authorized attempts or terminal escalation. Workers, adapters, and individual operation services do not select retries or manufacture replacement attempts.
- **Level 3 — exceptional blocking:** `task_level3_blocking_catalog.classify_blocking_case` selects a supported Type A case from positively evidenced typed conditions, otherwise the Type B `BLOCK_RESOLUTION` diagnostic fallback. `task_exceptional_blocking` installs the exact TaskBlock, selected OwnerGate, BLOCKED transition, and audit atomically. Callers supply conditions/evidence, never choose gate types themselves. Direct blocking controls/guards (such as Owner Abort and cost enforcement) reach Level 3 without pretending to be Level 2 operational failures.
- **Extension rule:** Before adding a new operational failure path, inspect `domain/operational_failures.py`, the Level 1 attempt-evidence seam, the coordinator's supported families/currentness checks, and triage eligibility. Registering a worker `OperationKind` alone does **not** enroll its failures in Level 2. Reuse these services; account explicitly for pre-invocation submission failures and uncertain effects rather than inventing local retry/blocking logic.


- composed_transaction(pool) is the supported atomic-composition primitive for database-only service work. The explicit outer transaction is required; nested repository transactions become SAVEPOINTs.
- Keep database transactions short.
- Never hold a database transaction open across GitHub, Supabase Auth HTTP, Cline/runtime, model, or any other external call.
- After an external call, reload/reconcile current durable state before a consequential follow-up mutation.
- Known success, known failure/non-delivery, and uncertain outcome are distinct. Timeout/connection loss is not automatically safe to retry.
- The LLM is never an idempotency mechanism.

## Terminalization and the shared terminal finalization

- One terminalization decision, one finalizer: every terminal authority (the unified Owner Cancel command and the delegating E5/E7 gate-cancellation entrypoints, the trusted GitHub closed-unmerged cancellation, and the confirmed-merge completion) commits its existing decision composition MINUS the inline archival, PLUS the exact-Task terminalization commitment, and then hands off to the shared finalization service (`task_terminal_finalization`) strictly after that commit.
- The commitment is the explicit authority/currentness barrier across ordinary Task interactions: `require_current_task` refuses operations against a committed, not-yet-archived Task (typed stale classification), and the commitment's token rotation stales every in-flight command. Only the finalizer acts on a committed Task, rechecking the commitment directly from durable state.
- The finalizer owns the optional bounded Cline cleanup (one `CLEANUP_WORKSPACE` request on the original eligible Producer session under the one-shot claim) and the mandatory archival: arbitration advisory lock FIRST, the guarded archival committing exactly the committed outcome (with the trusted closed-unmerged terminal cause), the terminal event with the origin-selected truthful actor provenance, and the absorbing ENDED transition of the remaining active session bindings — one composed transaction, never holding it across external calls. An unsettled cleanup claim defers the archival to its owning attempt; a duplicate finalizer never ends a session mid-attempt.
- A KNOWN-REFUSED finalization delivery is an observed delivery failure, not the accepted silent-worker-death limitation: it falls back in-process to the same authoritative archive-only finalization (no cleanup attempt). Uncertain submissions are handled conservatively with no resubmission or polling. A silently dead worker may leave a committed terminalization unfinished — the accepted system-wide v1 limitation recorded on issue #317; no deadline, recovery, or discovery mechanism exists for it, and none may be built here.

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
- Exceptional blocked-Task entries (one unresolved TaskBlock plus its pending exceptional OwnerGate) install only through the shared atomic blocking service (`task_exceptional_blocking`), and the installed exceptional gate case is selected only by the Level 3 blocking-case catalog (`classify_blocking_case`): originating blockers and Level 2 triage declare typed blocking conditions with sanitized evidence, and no caller hardcodes an exceptional gate type. Unsupported or insufficiently evidenced conditions fail closed to the Type B `BLOCK_RESOLUTION` diagnostic fallback. Since F8e (issue #290) the proven PR-publication and merge-head authority-mismatch conditions deliberately classify to that same Type B fallback — the legacy `GITHUB_STATE_MISMATCH` type is never newly selected or installed (the installer rejects it), and its menu is Cancel Task only.
- WorkflowActorContext contains only safe logical actor identity. Event context is small, event-specific, and never contains credentials, raw request bodies, guidance prose, prompts/templates, or duplicated canonical state.

## External repository operations

- Producer-reported branches are claims, not GitHub truth. Bind the one canonical branch only after an authoritative read of the addressed Repository; it is exclusive among current Tasks and never silently rebound. Remediation uses that branch.
- Reconciliation updates the existing canonical TaskPullRequest only; stable PR identity cannot be rebound. Head changes invalidate review acceptance; base-only movement does not.
- PR creation is a one-attempt external effect: validate the exact authorized branch head before creation, persist the known external creation, then reconcile the created PR/head immediately. A mismatch or changed Task authority is stale and never review-safe; uncertain creation or persistence after known creation must not trigger a second create.
- Merge requests use the exact Reviewer-accepted head SHA, with GitHub's expected-head guard. GitHub-confirmed merge, not a request or agent claim, completes the Task. Checks/status are GitHub observations, not OpenOrc merge policy.
- Owner-accountable GitHub writes require the exact Profile's active GitHub App user authorization, resolved only after account-operational, Workspace ownership, and subject/currentness checks. Never fall back to installation credentials, PATs, or another human's token. Refresh rotates under the durable Profile generation guard; tokens remain secret and short-lived.

## Sessions, admission, and planning

- Owner Start rechecks current GitHub source/eligibility, freezes the requirements fingerprint only on successful transition, and atomically admits the Task to planning or durable FIFO capacity waiting. Never use hierarchy as a blocking predicate.
- Connection capacity is enforced across processes on durable/locked Connection state. A Task sharing one Connection for two roles needs two slots. Insufficient structural capacity is a configuration error, not a queue wait; queued order uses the durable queue-entry instant.
- Runtime establishment uses the already-admitted Task/role/Connection binding. A later role-route edit does not migrate a Task. A READY binding is idempotent; LOST/ENDED never create replacements. Persist one coherent readiness identity/configuration after validated `session_ready`; known external readiness plus failed persistence requires reconciliation, not another session.
- Runtime construction context is resolved from durable exact Task/role/binding facts. The caller selects the repository base/Task checkout once; the resolver does not authorize operations, read Cloud infrastructure, invent paths, or trust an agent-reported location. Live role configuration may be re-resolved on legitimate same-ID reconstruction without changing the admitted Connection.
- Workspace preparation and cleanup are explicit workflow-authorized controls, separate from session initialization. The Producer owns checkout changes. A terminal Task authorizes bounded cleanup of only its original dedicated checkout; failure/uncertainty does not reverse terminal history.
- Planning REVIEW dispatch binds the exact current immutable PlanRevision and Reviewer session. Only a validated Reviewer `review_result` can settle a review; malformed output/timeout is a protocol/operational failure, never an implicit verdict.
- The ReviewLoop limit counts autonomous Producer revisions, not Reviewer evaluations. On exhaustion, pause at REVIEW_RESOLUTION; Owner-mediated mode remains sticky. A new Reviewer result is appended to history, not retroactively substituted for earlier findings. Reviewer ACCEPTED opens the ordinary next Owner gate; CHANGES_REQUESTED routes its new findings to Producer.
- Owner ↔ Reviewer discussion uses only current permitted gates. Ordinary chat never grants authority. At pending IMPLEMENTATION_AUTHORIZATION/MERGE_DECISION, a standalone valid Reviewer CHANGES_REQUESTED retires the pending gate without forging an Owner decision and triggers a new planning/PR-review cycle; PR_AUTHORIZATION has no discussion.
- Implementation authorization closes planning only for the exact review-cleared PlanRevision. Producer execution is in the existing session; `implementation_result` is semantic completion, never proof of GitHub committed head. PR_AUTHORIZATION binds the reconciled exact head. The Producer composes `pr_result`; OpenOrc, not the Producer, creates the PR.

## Owner controls and terminal lifecycle

- Owner Abort first durably blocks Task progression, then requests Interrupt of the exact active session; an acknowledgement does not prove execution has settled. Record one-shot interrupt intent/outcome and do not mistake Abort for Cancel or session deletion.
- Owner Cancel applies only to an eligible safely inactive current Task/gate, a settled BLOCKED Task, or a Task whose interrupted Executions are positively accounted by the current blocking episode: the SETTLED interrupt intent must be linked (task_block_id) to the exact current TaskBlock covering the affected Producer session. A historical settlement from an earlier episode on a reused session never authorizes later unaccounted work, and an open Execution without that linkage still vetoes fail-closed. The rule is cause-independent — Cancel never inspects TaskBlock reasons. Cancel finalizes accounted Executions with terminal CANCELLED accounting (execution_cancelled audit) in the same decision transaction; a conditional finalize that does not positively re-verify terminal status fails closed and rolls back. It archives/releases the Task and preserves history; if a canonical PR is open and unmerged, record the separate one-shot Owner-attributed closure obligation. An uncertain close is never blindly retried.
- GitHub-confirmed closure of the canonical PR without merge causes terminal cancellation with `pr_closed_unmerged` provenance; it is not a recoverable TaskBlock or an Owner cancellation. External reopening never reactivates the archived Task.
- Task state transitions, gate settlements, and corresponding audit events commit atomically under exact subject/token guards. External effects occur outside database transactions. Do not let delayed worker results advance a blocked, cancelled, or superseded Task.

## Credentials, deletion, and telemetry
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
## Cross-boundary references

Read the relevant domain, persistence, protocol, adapter, API, and worker guides when changing their contracts. Existing focused services, migrations, and tests are the source for implementation detail; do not append issue-by-issue implementation histories here.
