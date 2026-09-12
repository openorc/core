# Cline SDK Bridge Agent Context

## Boundary

This package is a thin Node/TypeScript language-compatibility shim:

```text
Python ClineAdapter
→ internal backend contract
→ long-lived local Node subprocess
→ JSON-RPC 2.0 over stdin/stdout
→ official @cline/sdk
→ remote Cline Hub
```

## It may own

Official SDK invocation, request/reply correlation, plain-JSON translation, SDK connection lifecycle, asynchronous event forwarding, approvals/usage/runtime-state forwarding, and bridge-local protocol/version compatibility.

## It must never own

Tasks, PlanRevisions, ReviewLoops, OwnerGates, Executions, workflow RuntimeRequests, OpenOrc retry/idempotency policy, formal OpenOrc response validation, GitHub operations, authorization, durable OpenOrc state, workflow transitions, an independent network service endpoint, or deployment/scaling identity.

## Process and version rules

- Python starts/lifecycle-manages the bridge locally; it is not a separately deployed service.
- Bridge failure/restart never implies replacement of the external Task session.
- Keep the bridge replaceable by a future native Python SDK implementation without changes above the backend boundary.
- Pin a released `@cline/sdk` semantic version and validate it as part of an explicit supported CLI + SDK release pair.
- Contract tests cover request/reply correlation, async event forwarding, and restart behavior.
