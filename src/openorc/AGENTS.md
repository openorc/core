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

## Module organization

- Organize Python modules around cohesive capabilities and responsibilities rather than broad catch-all files.
- Keep unrelated use cases and independently changing responsibilities in separate modules.
- Prefer specific module names that communicate ownership and purpose. Do not use generic dumping-ground modules such as `utils.py`, `helpers.py`, or `common.py` for unrelated behavior.
- Preserve architectural boundaries when extracting code: moving logic into another file must not move workflow authority into transports, adapters, or persistence.
- Keep tests organized around the capability or module they exercise rather than allowing unrelated scenarios to accumulate in one catch-all test module.

## Shared rules

- Use typed internal/domain/application errors for expected failures; transports map them to transport behavior.
- Preserve explicit Workspace isolation in all Workspace-scoped operations.
- Validate the exact current workflow subject/authority context before consequential effects.
- Never infer workflow meaning from database shape or provider-native response shape.
- External-operation uncertainty is neither success nor known failure.

## Application observability

OpenOrc application telemetry is OpenTelemetry with OTLP as the vendor-neutral export boundary, owned by the focused boundary in `openorc.observability/`. Durable `WorkflowEvent` audit history stays in Postgres and is never operational logging; connected-runtime telemetry stays runtime-owned and separate.

Process lifecycle (terminal):

- Telemetry initializes **at most once per process** through `initialize_observability(settings, surface)`: repeated same-identity calls are no-ops, conflicting identities raise a typed error, and initialization after terminal shutdown raises. Global OpenTelemetry providers are set-once and shutdown is terminal — there is no same-process provider reset, and SDK private globals are never touched.
- The API lifespan triggers initialization inside the actually-serving process (uvicorn reload workers included), never in a launcher parent. Lifespan teardown never shuts the global runtime down; the terminal flush belongs to the registered process-exit hook. The worker surface performs terminal shutdown in `run_worker`'s `finally` because that process is ending there.
- Constructing apps (`create_app`) or importing modules installs nothing. Unconfigured telemetry (`OPENORC_OTLP_ENDPOINT` unset) installs no OpenTelemetry runtime — only the process logging baseline. Malformed configuration fails fast like other configuration errors; runtime export failures never crash the application.
- OpenOrc boundary spans acquire tracers only through `observability.tracing.application_tracer` (default: the global runtime; tests inject local, non-global providers — that seam, not processor/exporter injection alone, is the test-isolation primitive). Initialization fails closed when a foreign global OpenTelemetry provider is already installed: the OpenTelemetry setters are set-once without raising on override attempts, so a pre-instrumented process never silently runs with the wrong runtime, and each installed global provider is verified by identity.

Logging contract:

- Ordinary Python `logging` is the only logging API — no bespoke OpenOrc logger wrapper. Logging stays selective: process lifecycle, degraded dependencies, external-operation uncertainty, protocol failures, stale/rejected asynchronous work, and unexpected invariant failures. Expected successful helper/repository operations do not log individual lines.
- The bootstrap installs the OpenTelemetry `LoggingHandler` on configured runs so records correlate with the active trace/span context, scoped to the `openorc` logger hierarchy — dependency, framework, and connected-runtime loggers reach only the local stderr baseline and never become OpenOrc operational telemetry. The API request ID reaches correlated log records through the boundary-owned ContextVar plus enrichment filter (the middleware sets it and resets it in `finally`).

Traces, spans, and attributes:

- Manual spans belong at meaningful boundaries only: application/service operations, external adapter operations, API request and worker job entrypoints, queue handoff where trace context propagates safely, and external HTTP through supported instrumentation or explicit adapter spans. Never instrument every helper, SQL statement, or dataclass conversion. Open every OpenOrc span through `observability.tracing.application_span`: automatic exception recording is disabled and failure telemetry is a safe classification only — an ERROR status whose description is the exception type name. `exception.message`, `exception.stacktrace`, and exception-text status descriptions must never be exported; arbitrary exception text may carry secret-bearing content.
- The API request middleware owns exactly one canonical request-entry span per request; any future framework instrumentation must be reconciled so exactly one request-entry span exists. Unhandled exceptions keep the framework's server-error semantics: they propagate to Starlette's outermost ServerErrorMiddleware (registered Exception handler, debug-mode precedence, re-raise for server-side logging), and the application-registered handler attaches the same server-generated request ID to the final response from request scope — no second request span and no client-supplied ID.
- Attach only the safe vocabulary from `observability.attributes` (Workspace/Task/Execution/Connection IDs, workflow role, OpenOrc operation name, GitHub stable repository/installation identifiers and issue/PR numbers and exact head SHA, RQ job identity, request ID). Attributes are contextual diagnostics: not workflow authority and not a second copy of canonical state.
- Never emit raw secrets, bearer/authorization tokens, Supabase secret/admin keys, GitHub installation tokens, runtime credentials, Workspace guidance prose, prompt bodies, agent transcripts/private reasoning, or arbitrary request/response bodies.

Read the applicable domain, services, transport, adapter, persistence, and Supabase guides for cross-boundary changes.
