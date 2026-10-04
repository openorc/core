# Cline Adapter Agent Context

## Boundary

Cline v1 support is an official Agent Runtime adapter in core. The Python adapter depends on an internal language-neutral SDK backend implemented initially by `packages/cline-sdk-bridge`.

Use the official Cline SDK path; do not reimplement the Cline Hub wire protocol, private on-disk state, or runtime internals when the documented public abstraction satisfies the contract.

OpenOrc orchestrates **persistent Cline sessions assigned to workflow roles**. It does not own Cline's internal agent topology or native runtime configuration.

## Documentation-first rule

Before source archaeology, packet probing, or live experiments, read the current official Cline SDK documentation relevant to the change. At minimum for Cline adapter/bridge work consult:

- https://docs.cline.bot/sdk/overview
- https://docs.cline.bot/sdk/clinecore
- https://docs.cline.bot/sdk/architecture/overview
- API reference:
  - https://docs.cline.bot/sdk/reference/cline-core
  - https://docs.cline.bot/sdk/reference/agent
  - https://docs.cline.bot/sdk/reference/gateway
  - https://docs.cline.bot/sdk/reference/tools-api
  - https://docs.cline.bot/sdk/reference/events
  - https://docs.cline.bot/sdk/reference/types
- https://docs.cline.bot/sdk/events
- https://docs.cline.bot/sdk/tools
- https://docs.cline.bot/sdk/plugins
- https://docs.cline.bot/sdk/guides/permission-handling
- https://docs.cline.bot/sdk/guides/going-to-production

Treat documented public surfaces as the candidate contract. Use source inspection or live qualification only for pinned-version behavior, remote Cline Box semantics, or genuine ambiguity. Do not experimentally rediscover documented APIs.

## SDK/package boundary

- `@cline/sdk` / `ClineCore` is the primary OpenOrc runtime-session integration surface.
- Lower-level `Agent` / `AgentRuntime` APIs are not the OpenOrc integration layer.
- The documented `@cline/llms` catalog surface may be consumed for provider/model selection.
- Do not mirror ClineCore wholesale into Python. Expose only the session/configuration/control/event operations OpenOrc actually needs.

## Session invariants

- Preserve one exact Producer and one exact Reviewer external session per Task for the Task lifetime.
- Session creation is idempotent for `(Task, role)` and usable only after the OpenOrc readiness handshake succeeds.
- Producer and Reviewer have isolated working contexts even on one Hub.
- Reviewer remains review-only/PLAN mode in v1; Producer begins PLAN and switches to ACT only after implementation authorization. Use only the supported public same-session mode-control contract qualified for the pinned Cline baseline.
- Runtime-internal rebuild under the same external session identity is not an OpenOrc session replacement.
- Unexpected loss of the bound context blocks; never silently create a new one.

## Runtime ownership

OpenOrc owns Task/role binding, exact external-session routing, workflow semantics, and the role/session configuration it deliberately selects.

For Cline v1, OpenOrc selects provider/model identifiers per role/session from the documented public catalog and records effective runtime-reported identity where available.

Cline owns provider authentication and native runtime configuration, including tools, MCP, plugins, skills, sub-agents/teams, permission/auto-approval configuration, filesystem/tool execution, runtime persistence, private conversational state, and other native behavior inside the session. A managed Cline Box does not transfer those concerns into OpenOrc merely because Cloud owns the host lifecycle.

Do not parse private Cline files or invent a shadow configured-provider registry. If a selected provider/model is unusable because native Cline configuration/authentication is missing, surface the supported runtime failure.

OpenOrc Connection rows never model Cline-owned provider/MCP/tool credentials. OpenOrc-owned authentication reaches the runtime only through an opaque reference boundary (`connections.auth_reference`), never as a persisted secret. Connection `enabled` and `session_capacity` remain OpenOrc admission/eligibility semantics, not runtime health or discovered Cline capacity.

## Delivery / telemetry

Use supported ClineCore session events/state for delivery reconciliation, cancellation/control, usage/cost, effective model identity, and optional runtime telemetry. Do not consume lower-level AgentRuntime events when the ClineCore session surface supplies the needed fact.

Cline-native tool approval remains a runtime concern unless the qualified remote-session contract demonstrably requires a minimal OpenOrc-side callback/pass-through. Do not make OpenOrc the Cline permission UI by assumption.

Do not blindly replay uncertain sends. Runtime telemetry is not workflow authority.

Read `packages/cline-sdk-bridge/AGENTS.md` for bridge changes and services/domain guidance for semantic changes.
