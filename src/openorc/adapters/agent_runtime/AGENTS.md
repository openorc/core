# Agent Runtime Adapter Contract Agent Context

## Boundary

`agent_runtime/` is the shared runtime-neutral Python contract between
OpenOrc application services and concrete Agent Runtime adapters (the
deterministic fake runtime #69, the official Cline adapter #73). It defines
the universal v1 session operations (`AgentRuntimeAdapter`), their typed
normalized results, and the compact normalized failure taxonomy.

This package owns no runtime mechanics of its own: it is the contract and
the shared formal-validation funnel, not a runtime client. It imports only
the standard library, `openorc.protocol`, and the narrow `WorkflowRole`
domain vocabulary.

## Universal surface discipline

- Exactly three universal operations: `create_session` (one readiness
  handshake), `send` (exact-session formal or non-formal interaction), and
  `realize_control` (already-authorized semantic #129 control). No universal
  close/end/session-status polling operation is ever added; runtime-specific
  cancel/resume, inspection, telemetry, and lifecycle mechanics stay
  optional/local and never expand this interface.
- No provider/model selection, no Cline modes or any runtime's native action
  vocabulary, no telemetry streams, no approvals, no filesystem/Git
  operations, and no capability marketplace on the universal interface.
- Protocol schemas stay in `openorc.protocol` (#65): concrete adapters strip
  their runtime-native outer envelope and funnel candidates through the
  shared `_parse_formal_candidate`; no adapter-local schema copies.
- Semantic controls (#129) arrive distinctly from rendered prose; adapters
  dispatch locally on the semantic control kind and never decide
  authorization. The interaction-to-expected-family mapping stays with the
  application layer.

## Semantics

- Successful `create_session` means fresh exact context + canonical
  initialization + valid `session_ready`; an allocated-but-unconfirmed
  external ID is internal runtime fact, never a successful result. Creation
  mutates no durable persistence.
- `send` with an expected family is formal: envelope-stripped candidate →
  shared #65 parsing → typed result or normalized protocol failure; never
  repaired, reinterpreted, or retried. `send` without an expected family is
  ordinary exact-session conversation (advisory Owner ↔ Reviewer discussion
  uses the existing Reviewer session): the reply returns as
  `AgentTextResponse` and is never protocol-parsed.
- Exact-session discipline: operations address the exact opaque external
  session ID; adapters never silently replace, redirect, merge, or reuse
  sessions. Exact-session loss is a distinct normalized error, never
  conflated with runtime unavailability. Timeout is an uncertain outcome,
  never known non-delivery.
- Normalized messages/results never expose credentials/tokens, Workspace
  guidance, prompt/initialization bodies, raw responses, transcripts, or
  provider-native exception text.

Read the parent adapters guide, the protocol guide, and services/domain
guides for cross-boundary changes.
