# Application Services Agent Context

## Boundary

Services own deterministic use-case orchestration: applying domain rules, loading/updating durable state, validating current subject/authority context, calling persistence/adapters, deciding workflow consequences, and scheduling follow-up work.

Services do not own HTTP DTOs, RQ job semantics, Cline/GitHub wire mechanics, Vue state, or Cloud infrastructure.

## Canonical flow

```text
FastAPI request / RQ job
→ application service
→ domain/current-subject checks + durable state
→ adapter/persistence operation
→ external system when required
```

External results/errors return through the adapter to services, which decide the deterministic workflow consequence and durable effects.

## Service organization

- Organize service modules around coherent OpenOrc capabilities and use cases, not one global workflow/application service.
- A service operation should orchestrate one coherent use case or closely related use-case family. Split distinct workflow responsibilities instead of accumulating unrelated phases into one module.
- API routers and RQ jobs remain thin callers of services; do not duplicate or relocate application decisions into transport code for convenience.
- Services coordinate domain, persistence, protocol results, and adapters. Provider-specific GitHub/Cline transport mechanics remain in adapters, while persistence modules remain responsible for durable representation rather than workflow policy.
- Reuse focused application helpers only when they encode a genuine cross-service application concern. Do not create generic helper buckets as an alternative to clear capability ownership.

## Exact-subject and replay safety

Before any delayed/replayed workflow-changing action:

- reload current durable state;
- verify the exact subject, OwnerGate, Execution, PR head, RuntimeRequest, or equivalent context still matches;
- if stale, do not apply the effect;
- persist/surface recovery context rather than guessing.

Distinguish known success, known non-delivery/failure, and uncertain outcome. Timeout or connection loss is not automatically safe to retry. Reconcile authoritatively where possible; use bounded retry only for known-safe cases; otherwise block. The LLM is never the idempotency mechanism.

Task state carries the same discipline through the opaque `state_token`: reload the current durable Task and its token before any delayed/replayed workflow-changing action, apply mutations only through the conditional persistence primitives that rotate the token, and treat a stale token as a stale operation — never applied, surfaced as recovery context instead.

## Workflow ownership

Services decide when workflow policy permits plan publication, PR composition/publication, Reviewer dispatch, runtime continuation, and merge invocation. Adapters provide mechanics only.

ReviewLoop exhaustion creates Owner intervention rather than silently extending the loop; Owner override remains distinct from Reviewer acceptance.

## Established service foundation (Phase 2A)

These conventions are established by issue #51; later Phase 2 leaves inherit them. Changing one is a deliberate architecture decision, not an implementation detail.

- Expected application failures use the transport-neutral typed vocabulary in `errors.py` (`ApplicationError` base): authentication, authorization, not found, invalid command/input, conflict, stale operation (`StaleOperationError`, a distinct `ConflictError` subtype), known external-operation failure (`ExternalOperationFailedError`), and uncertain external-operation outcome (`ExternalOperationUncertainError`). Errors are message-typed and gain only explicit safe keyword fields when a capability demonstrates the need — never credentials/tokens, never a second copy of domain state. Transports own status/outcome mapping.
- `transaction_composition.py:composed_transaction(pool)` is the one production-supported atomic-composition primitive: it explicitly enters one outer transaction on the process-local pool (`with pool.connection() as conn, conn.transaction()`) and yields a transaction-scoped `DatabasePool` view over that connection, so existing repository `transaction(...)` scopes become nested psycopg SAVEPOINTs and the whole composition commits or rolls back together. `pool.connection()` alone establishes no transaction; the explicit outer block is load-bearing.
- The connection-bound pool view is transaction-scoped only: it never closes the underlying connection or process pool and expires with the outer transaction that yielded it.
- The external-I/O rule is executable here: database-only validation/mutation composes inside one short transaction; GitHub, Supabase Auth HTTP, Cline/runtime, model, or any other external call occurs with no database transaction open; after an external call, reload/reconcile current durable state before a consequential follow-up mutation; timeout/connection loss is never reclassified as success or a safe retry.

## Workspace authorization foundation (issue #53)

These conventions are established by `workspace_authorization.py` and `workspace_configuration.py`; later Phase 2 leaves inherit them. Changing one is a deliberate architecture decision, not an implementation detail.

- The canonical v1 rule is exact: `workspace.owner_profile_id == authenticated_profile.id`, where the authenticated Profile UUID comes from the #52 authentication boundary (`AuthenticatedUser.profile.id`). Authorization is never inferred from GitHub identity, repository metadata, Connection identity, object existence alone, or client-supplied Workspace claims.
- The public authorization surface is profile-gated end to end: every public resolver takes the authenticated `profile_id` (keyword-only) plus the addressed identity and internally composes `require_profile_workspace` before any scope check. No public resolver accepts a plain `Workspace` object, a bare `workspace_id` alone, or any pre-authorized object as authority — a `Workspace` is ordinary durable data that persistence can load and application code can construct, so passing one proves only object shape, never that the Profile was checked. Plain Workspace objects returned by the gate are resource data, never reusable authority.
- Scoped resolution validates each record's direct `workspace_id` (never a join through mutable external metadata) and, for Task-owned records, additionally validates the `task_id` linkage against a Task of the authorized Workspace — including task-scoped WorkflowEvent reads, whose present `task_id` is resolved through the same private Task-scope resolver (Workspace-level events with `task_id=None` need no Task lookup). Addressing quirks are honored as-is: sessions are `(Task, role)`-addressed, role bindings are `(Workspace, role)`-addressed. The database composite foreign keys remain durable backstops, never the authorization mechanism.
- An inaccessible subject is uniformly `NotFoundError` — missing, cross-Workspace, and cross-Task are the same observable outcome, so probing exact internal UUIDs leaks nothing. `AuthorizationError` is reserved for contexts where scope is already established to the caller. Scope-validation policy lives in module-private helpers; public resolvers name the record family and return the loaded record.
- Workspace configuration operations (`workspace_configuration.py`) take `profile_id`, gate ownership first, and compose their DB-only read/validate/mutate inside `composed_transaction`. The persistence updates use a deliberate `SELECT ... FOR UPDATE` plus conditional write, so the returned audit handoff (#56) — exact previous/new limit values, and for guidance only the semantic `changed` fact, never the prose as event context — never comes from a racy re-read. Same-value writes are no-ops. The module records no WorkflowEvents (#56 owns the event stream) and introduces no generic settings/audit framework.
- A change to the Workspace review limit affects future ReviewLoops only; existing loops' stored effective limits are immutable historical configuration.

Read domain plus relevant adapter/persistence/API/worker guides for cross-boundary changes.
