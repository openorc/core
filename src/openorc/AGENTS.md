# Shared Python Control Plane Agent Context

## Boundary

This package owns shared backend application/domain implementation used by API and worker process surfaces.

It does not own the Vue UI, Cloud deployment/billing, or Node-specific Cline SDK compatibility mechanics.

## Dependency direction

Conceptually preserve:

```text
domain
↑
services
↑
API / workers

services → persistence + adapters
Agent Runtime adapters → protocol
adapters/persistence → external systems
```

`src/openorc/protocol/` is the low-level runtime-independent formal-contract boundary between OpenOrc and connected agent runtimes: versioned formal response families, envelopes, protocol parsing/validation helpers, and shared typed protocol models. It imports no concrete adapters, services, domain, persistence, API, workers, or runtime-specific packages. Services may consume protocol result types returned by Agent Runtime adapters.

Forbidden dependency directions include:

- domain → FastAPI, RQ, Supabase SDK, GitHub SDK, Cline SDK, or Node bridge mechanics;
- services → FastAPI DTOs or RQ job objects;
- routers/jobs → duplicated business logic;
- protocol → concrete adapters, services, domain, persistence, API, workers, or runtime-specific packages.

Domain/services must be usable without an HTTP request or queue job object.

## Shared rules

- Use typed internal/domain/application errors for expected failures; transports map them to transport behavior.
- Preserve explicit Workspace isolation in all Workspace-scoped operations.
- Validate the exact current workflow subject/authority context before consequential effects.
- Never infer workflow meaning from database shape or provider-native response shape.
- External-operation uncertainty is neither success nor known failure.

Read the applicable domain, services, transport, adapter, persistence, and Supabase guides for cross-boundary changes.
