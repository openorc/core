# FastAPI Transport Agent Context

## Boundary

This subtree owns HTTP routing, auth/session dependency extraction, request parsing/validation, response serialization, HTTP error mapping, wire DTOs, and authorized SSE endpoints.

It does not own workflow transitions, review/session lifecycle decisions, GitHub reconciliation policy, retry/idempotency policy, persistence rules, or direct external orchestration that bypasses services.

## Rules

- Routers call shared application services.
- Do not duplicate business/domain validation that must also apply to workers or future transports.
- Resolve authentication and Workspace scope before invoking application operations.
- Browser clients never call Agent Runtimes directly through hidden API shortcuts.
- Runtime/provider URLs exposed to the UI are navigation affordances only.
- SSE is live delivery, not durable workflow truth.
- API contracts consumed by the SPA require coordinated changes with `apps/app/AGENTS.md`.

Read services/domain guidance and persistence guidance where relevant.
