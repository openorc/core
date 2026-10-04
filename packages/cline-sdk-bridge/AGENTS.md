# Cline SDK Bridge Agent Context

## Boundary

This package is a thin Node/TypeScript language-compatibility shim:

```text
Python ClineAdapter
→ internal backend contract
→ long-lived local Node subprocess
→ JSON-RPC 2.0 over stdin/stdout
→ official @cline/sdk / ClineCore
→ remote Cline Hub
```

The qualified managed path attaches in ClineCore remote mode to the same persistent CLI-owned Hub used by the native Cline Box dashboard.

## Documentation-first rule

Before changing SDK integration code, read the current official Cline SDK documentation relevant to the operation. Start with:

- https://docs.cline.bot/sdk/overview
- https://docs.cline.bot/sdk/clinecore
- https://docs.cline.bot/sdk/architecture/overview
- https://docs.cline.bot/sdk/reference/cline-core
- https://docs.cline.bot/sdk/reference/gateway
- https://docs.cline.bot/sdk/reference/events
- https://docs.cline.bot/sdk/reference/types
- https://docs.cline.bot/sdk/events
- https://docs.cline.bot/sdk/tools
- https://docs.cline.bot/sdk/plugins
- https://docs.cline.bot/sdk/guides/permission-handling
- https://docs.cline.bot/sdk/guides/going-to-production

Documentation defines the candidate public contract. Source inspection/live probes are for pinned-version or remote-Hub qualification, not API discovery.

## Package policy

- Prefer `@cline/sdk` / `ClineCore` for session lifecycle, persistence, messages, events, usage, and controls.
- `@cline/llms` may be consumed directly for the documented provider/model catalog used by OpenOrc role configuration.
- Do not drop to `@cline/agents` / `AgentRuntime` merely because a lower-level method exists.
- Do not import private Cline packages, private Hub protocol, or private on-disk configuration as product contracts.

## It may own

Official SDK invocation, request/reply correlation, plain-JSON translation, SDK connection lifecycle, asynchronous ClineCore session-event forwarding, usage/effective-runtime-state forwarding, provider/model catalog translation, and any minimal approval/question callback forwarding only where the validated public remote contract requires OpenOrc participation.

## It must never own

Tasks, PlanRevisions, ReviewLoops, OwnerGates, Executions, OpenOrc retry/idempotency policy, formal OpenOrc response validation, GitHub operations, authorization, durable OpenOrc state, workflow transitions, an independent network service endpoint, deployment/scaling identity, provider credentials, Cline plugins/MCP/skills/tools/sub-agent configuration, or a second permission system.

## Process and version rules

- Python starts/lifecycle-manages the bridge locally; it is not a separately deployed service.
- Bridge failure/restart never implies replacement of the external Task session.
- Keep the bridge replaceable by a future native Python SDK implementation without changes above the backend boundary.
- Pin released Cline package versions and validate them as one supported CLI/core/SDK/catalog baseline.
- Contract tests cover only OpenOrc-owned bridge behavior such as request/reply correlation, async event forwarding, translation, and restart behavior; do not recreate Cline's SDK test suite.
