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

Read domain plus relevant adapter/persistence/API/worker guides for cross-boundary changes.
