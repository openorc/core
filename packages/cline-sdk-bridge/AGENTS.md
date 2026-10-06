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
- Do not add an `@cline/llms` or other provider/model catalog dependency. OpenOrc passes opaque configured provider/model identifiers through the SDK surface and does not enumerate Cline-native provider state.
- Do not drop to `@cline/agents` / `AgentRuntime` merely because a lower-level method exists.
- Do not import private Cline packages, private Hub protocol, or private on-disk configuration as product contracts.

## It may own

Official SDK invocation, request/reply correlation, plain-JSON translation, SDK connection lifecycle, public session/message/history reads, asynchronous ClineCore event forwarding, usage/runtime-state forwarding, and the SDK control/reconstruction calls required by the backend contract.

## It must never own

Tasks, PlanRevisions, ReviewLoops, OwnerGates, Executions, OpenOrc retry/idempotency policy, formal OpenOrc response validation, GitHub operations, authorization, durable OpenOrc state, workflow transitions, an independent network service endpoint, deployment/scaling identity, provider credentials, Cline plugins/MCP/skills/tools/sub-agent configuration, or a second permission system.

## Process and version rules

- Python starts/lifecycle-manages the bridge locally; it is not a separately deployed service.
- Bridge failure/restart never implies replacement of the external Task session.
- Keep the bridge replaceable by a future native Python SDK implementation without changes above the backend boundary.
- Pin the supported `@cline/sdk` version selected by the Phase 2D baseline; compatibility is qualified against the corresponding supported CLI/core/SDK baseline.
- Do not register interactive approval/question executors or turn Cline-native tool approvals into an OpenOrc bridge protocol. Contract tests cover only OpenOrc-owned bridge behavior such as request/reply correlation, translation/event forwarding, and restart behavior; do not recreate Cline's SDK test suite.
