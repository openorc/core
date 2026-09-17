# Cline Adapter Agent Context

## Boundary

Cline v1 support is an official Agent Runtime adapter in core. The Python adapter depends on an internal language-neutral SDK backend implemented initially by `packages/cline-sdk-bridge`.

Use the official Cline SDK path; do not reimplement the Cline Hub wire protocol or fork runtime internals when the official abstraction satisfies the validated contract.

## Session invariants

- Preserve one exact Producer and one exact Reviewer external session per Task for the Task lifetime.
- Session creation is idempotent for `(Task, role)` and usable only after the OpenOrc readiness handshake succeeds.
- Producer and Reviewer have isolated working contexts even on one Hub.
- Reviewer remains review-only/PLAN mode in v1; Producer begins PLAN and switches to ACT only after implementation authorization.
- Runtime-internal rebuild under the same external session identity is not an OpenOrc session replacement.
- Unexpected loss of the bound context blocks; never silently create a new one.

## Runtime ownership

Cline owns provider authentication/catalogs, MCP/tool credentials, Git/runtime credentials, filesystem/tool execution, runtime persistence, and private conversational state. OpenOrc owns only the Connection/session routing and workflow semantics.

OpenOrc Connection rows never model Cline-owned credentials: runtime-owned provider/MCP/tool credentials remain runtime-owned and are not stored as generic OpenOrc credential records. OpenOrc-owned authentication reaches the runtime only through an opaque reference boundary (`connections.auth_reference`), never as a persisted secret.

At the validated v1 baseline OpenOrc does not pretend to select provider/model remotely; new sessions use the Hub's configured/default choice until a supported credential-safe discovery/configuration surface exists. `connections.reported_provider`/`reported_model` are nullable runtime-reported observations only — never configuration authority, never enums. Connection `enabled` and `session_capacity` are OpenOrc admission/eligibility semantics: they say nothing about runtime reachability, health, or lifecycle, and capacity is Owner-configured, never discovered from Cline.

## Delivery / telemetry

Use supported runtime events/state for delivery reconciliation, scoped approvals, cancellation, and optional telemetry. Do not blindly replay uncertain sends. Runtime telemetry is not workflow authority.

Read `packages/cline-sdk-bridge/AGENTS.md` for bridge changes and services/domain guidance for semantic changes.
