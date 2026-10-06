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
- Supported baseline: CLI `3.0.68`, Hub/Core `0.0.90`, SDK `0.0.90`; deliberate qualification precedes upgrades.
- Provider/model choices are opaque configured IDs. No catalog, configured-provider enumeration or required effective identity readback belongs in v1.
- Do not mirror ClineCore wholesale into Python. Expose only the session/configuration/control/event operations OpenOrc actually needs.

## Session invariants

- Preserve one exact Producer and one exact Reviewer external session per Task for the Task lifetime.
- Session creation is idempotent for `(Task, role)` and usable only after the OpenOrc readiness handshake succeeds.
- Producer and Reviewer have isolated working contexts even on one Hub.
- Reviewer stays PLAN; Producer begins PLAN and enters ACT only after implementation authorization. PLAN permits read-only commands while guarding mutations. On `0.0.90`, mode is construction-time: per-turn send(mode) is tag-only. Use serialized readMessages → await stop → same-ID start with raw initialMessages and the complete target-mode construction bundle. Never re-initialize or invent a mode-notice prompt; original record metadata.mode is not current-mode truth.
- Runtime-internal rebuild under the same external session identity is not an OpenOrc session replacement.
- Unexpected loss of the bound context blocks; never silently create a new one.

## Runtime ownership

OpenOrc owns Task/role binding, exact external-session routing, workflow semantics, and the role/session configuration it deliberately selects.

Per-role construction supplies opaque providerId/modelId, role Markdown in config.rules, blank systemPrompt, the role checkout as both cwd/workspaceRoot, workflow mode, enableTools=true, enableSpawnAgent=false, enableAgentTeams=false, interactive=true and ask_question disabled. Register no interactive executors or approval callbacks. Re-supply all fields on rebuild; selected Task-session IDs remain distinct from optional runtime-reported provenance.

OpenOrc owns default role Markdown and optional Owner overrides; NULL uses the shipped default, separate from Workspace guidance and thin initialization. Material role-prompt edits/reset or routing changes that alter the prompt are blocked during dependent nonterminal Tasks, including CONNECTING/recovery gaps. Default changes wait for affected Tasks to terminate. No Task-session prompt copies/history are persisted.

Root repository AGENTS.md is naturally consumed from the role checkout; no native rule-file hydration. Separate checkouts provide working-context isolation, not Workspace security isolation.

Cline owns provider authentication and native runtime configuration, including tools, MCP, plugins, skills, sub-agents/teams, permission/auto-approval configuration, filesystem/tool execution, runtime persistence, private conversational state, and other native behavior inside the session. A managed Cline Box does not transfer those concerns into OpenOrc merely because Cloud owns the host lifecycle.

Do not parse private Cline files or invent a shadow configured-provider registry. start() acceptance does not prove usability: a first-turn selection/auth failure before session_ready is a construction/configuration failure. Remote start({prompt}) does not execute it; initialization is the first send and readiness is validated exactly once.

OpenOrc Connection rows never model Cline-owned provider/MCP/tool credentials. OpenOrc-owned authentication reaches the runtime only through an opaque reference boundary (`connections.auth_reference`), never as a persisted secret. Connection `enabled` and `session_capacity` remain OpenOrc admission/eligibility semantics, not runtime health or discovered Cline capacity.

## Delivery / telemetry

Use public ClineCore records/messages/history/usage and durable OpenOrc dispatch state for reconciliation. Fresh-client plain subscriptions do not replay running-turn events on 0.0.90; only the original surviving client auto-resumed. Record status is bookkeeping. Usage persists through same-ID rebuild only within one Hub lifetime; Hub restart retains transcript but resets usage and requires same-ID reconstruction. Unknown send/start acceptance blocks, never replays. Missing addressability alone is not loss: distinguish readable restart state from absent/deleted records. delete() is a demonstrated terminal boundary, not the only possible one.

Abort interrupts a turn and permits later authorized ordinary interaction in the same session; stop releases a runtime without proving Task cancellation/session loss. No fabricated native resume API. Core consumes current endpoint/control auth through the existing Connection seam; Cloud/private credential discovery is not a Core contract.

Cline-native approvals/questions do not cross the supported v1 seam; create no Cline RuntimeRequests and register no approval/question callbacks or autoApprove:false policy. The runtime-neutral RuntimeRequest model remains separate.

Do not blindly replay uncertain sends. Runtime telemetry is not workflow authority.

Read `packages/cline-sdk-bridge/AGENTS.md` for bridge changes and services/domain guidance for semantic changes.
