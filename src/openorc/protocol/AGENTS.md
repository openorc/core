# Protocol Agent Context

## Boundary

`protocol/` is the canonical home for runtime-independent OpenOrc agent protocol contracts and pure protocol helpers: the versioned v1 formal response families (`session_ready`, `plan_result`, `review_result`, `implementation_result`, `pr_result`), formal request/protocol envelopes, session-initialization protocol content and protocol metadata, runtime-independent parsing/validation helpers for formal protocol objects, and shared typed protocol models/schemas.

This package is not a network service, runtime adapter, workflow engine, or general agent-to-agent messaging framework.

## Runtime/provider neutrality

- Protocol contracts are independent of Cline or any other runtime/provider.
- No Cline SDK types, Hub transport details, approval identifiers, telemetry types, runtime modes, subprocess/JSON-RPC mechanics, or provider-specific configuration belong here.
- Runtime/provider-specific extraction and transport remain adapter concerns.

## Protocol ownership

- OpenOrc owns the protocol. Connected agents/runtimes implement or satisfy it; they do not redefine it.
- Protocol schemas and protocol versions are explicit and stable enough for deterministic validation and audit reconstruction.
- Workspace prompt customization may change instructions/emphasis but must not redefine formal schemas, authority semantics, session semantics, exact review-subject identity, or workflow transitions.

## Formal response semantics

- Formal agent interactions use typed, versioned OpenOrc contracts rather than prose inference.
- Harmless presentation normalization is permitted where needed, for example removing Markdown code fences around otherwise valid JSON.
- Parsing/validation never invents missing fields, infers an outcome from free-form prose, or converts an invalid response into a semantic success.
- A malformed or schema-invalid formal response is a protocol failure for the caller to handle, not permission for protocol code to guess intended meaning.

## Authority and subject identity

- OpenOrc supplies and retains authoritative Task, PlanRevision, PR, commit SHA, OwnerGate, and other workflow identities.
- Agents do not become authoritative merely by echoing identifiers back in a response.
- Protocol models carry only semantic fields the agent is responsible for producing. Machine/workflow identity belongs to OpenOrc state and the calling context unless a later contract explicitly establishes otherwise.

## Separation from workflow/domain and adapters

- Protocol code defines and validates the language OpenOrc speaks with agents; it does not decide workflow consequences. Task state transitions, Owner authority, ReviewLoop behavior, retry/recovery policy, durable persistence effects, and other workflow semantics remain in services/domain code.
- A validated `review_result`, for example, is a typed protocol result. Deciding whether it advances a Task, creates another review iteration, or opens an OwnerGate is outside this package.
- Domain validation remains responsible for domain entities/value objects and workflow invariants. Formal agent-response schema validation belongs here; do not duplicate it in `domain/`.
- Agent Runtime adapters own transport/session mechanics, response extraction, provider/runtime-specific normalization, and provider/runtime error classification. They invoke protocol parsing/validation when translating provider-native responses into typed OpenOrc results and do not duplicate formal OpenOrc schemas locally. Other external-system adapters (e.g. GitHub) do not depend on this package.

## Dependency direction

Distinguish message flow from Python dependency direction. Typical message flow is external runtime → adapter → protocol validation/typed result → application service → domain/workflow consequence.

Code dependency rules:

```text
services → domain
services → adapters + persistence
Agent Runtime adapters → protocol
services may consume protocol result types returned by Agent Runtime adapters
protocol → no concrete adapters, services, domain, persistence, API, workers, or runtime-specific packages
API / workers → services
```

`protocol/` is a low-level runtime-independent contract package. Do not introduce reverse dependencies from it into workflow or transport implementations.

## Scope restraint

- Do not turn this package into a generic inter-agent protocol, peer-to-peer messaging layer, provider abstraction framework, or capability marketplace.
- Producer and Reviewer do not communicate directly. OpenOrc-managed workflow/services route protocol artifacts and findings between their separate Task-scoped sessions.
- This package is the small OpenOrc semantic protocol boundary, nothing broader.
