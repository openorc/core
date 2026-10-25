# Cline Adapter Agent Context

## Boundary

Use the official pinned `@cline/sdk` / `ClineCore` public remote-session surface through the narrow Python backend/Node bridge. Do not reimplement Hub wire protocol, parse private Cline files, or mirror the entire SDK into OpenOrc. Consult current official Cline SDK documentation before source archaeology or live qualification; upgrade the qualified CLI/core/SDK baseline deliberately together, with affected remote-runtime checks.

## Sessions, authority, and working context

- Preserve exact Task-scoped Producer/Reviewer sessions and distinct working contexts. Initialize once through validated `session_ready`; never replace an established external identity because a send, Hub, or bridge failed.
- Cline-native PLAN/ACT construction mode realizes the authorized role workflow. Reviewer stays PLAN; Producer may ACT only for authorized checkout preparation, post-implementation-authorization work, and terminal checkout cleanup. Mode/working-directory changes use the qualified same-ID reconstruction with the raw existing messages; do not create another OpenOrc session or resend initialization.
- The caller owns durable Task/role/Connection routing, selected base/checkout identity, and workflow authority. Adapter construction resolves current safe role configuration and only the already-selected exact working context; runtime/provider IDs are opaque and Cline owns provider authentication and native settings.
- Never infer workflow acceptance, session loss, delivery, or completion from Cline bookkeeping status or optional telemetry. Confirm lost context using supported positive evidence, not missing events or an unavailable Hub.
- Native tool approval is Cline-owned. v1 auto-approves enabled tools, disables `ask_question`, and registers no OpenOrc interactive approval executor; do not set spawn-agent/team flags.

## Delivery, review, and telemetry

- Unknown send/reconstruction/interrupt outcomes require public-read reconciliation, not blind replay. Bridge/client disconnect may leave work completing Hub-side; restart may reset usage and require same-ID reconstruction.
- Planning REVIEW sends the exact canonical PlanRevision; PR REVIEW supplies canonical PR and reconciled head SHA to the existing Reviewer session. Formal result validation and workflow consequences remain outside the Cline backend. Do not demand a private Reviewer checkout or certify the Reviewer's tool inspection.
- Activity projections are explicitly read-only, Task/session-bound allowlists. Suppress reasoning, prompts, formal envelopes, tool/MCP inputs and outputs, raw errors, and credential-bearing data; only permitted completed assistant commentary and safe usage/status facts reach authorized clients. Usage is advisory and not evidence of success or continuity.
- Connection `control_endpoint` is a validated `ws://`/`wss://` shape in safe config, never a credential or an assertion that its destination is safe. Deployment network isolation owns egress/SSRF protection.

Read `packages/cline-sdk-bridge/AGENTS.md` for bridge changes and services/domain guidance for workflow changes.
