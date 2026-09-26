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
- FastAPI `BackgroundTasks` is not used for deferred or workflow execution; that work goes through RQ/Valkey queues and shared application services.
- v1 persistence is synchronous: async/event-loop handlers must not perform blocking database I/O directly.
- Provider webhook ingress routes (e.g. `POST /api/github/webhooks`, issue #61) are authenticated by the provider's own signature material, not by Supabase Auth: the route reads the exact raw request bytes and the required provider headers, enforces a bounded request size, delegates the blocking intake service off the event loop (threadpool), and maps only typed intake outcomes/errors to HTTP. Signature failures map to a uniform, detail-free rejection; the route contains no reconciliation, queue, or workflow logic.
- API contracts consumed by the SPA require coordinated changes with `apps/app/AGENTS.md`.

Read services/domain guidance and persistence guidance where relevant.
