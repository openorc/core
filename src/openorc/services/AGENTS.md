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

Read domain plus relevant persistence/adapter/API/worker guides for cross-boundary changes.
