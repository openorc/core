# @openorc/cline-sdk-bridge

The OpenOrc D3 Cline SDK bridge: a long-lived local Node/TypeScript subprocess
that invokes the pinned, official `@cline/sdk` `ClineCore` remote client and
exposes the D2 backend operation surface (`src/openorc/adapters/cline/`)
through **JSON-RPC 2.0 over stdin/stdout**.

```text
Python ClineAdapter
→ internal backend contract (D2)
→ long-lived local Node subprocess (this package)
→ JSON-RPC 2.0 over stdin/stdout
→ official @cline/sdk / ClineCore (pinned 0.0.90, backendMode "remote")
→ remote Cline Hub
```

It is a replaceable language-compatibility shim. It owns no workflow, no
persistence, no GitHub, no authorization, no retry policy, and no external
session ownership. See `AGENTS.md` for the binding boundary rules.

## Wire contract (fixed)

- UTF-8 **newline-delimited JSON-RPC 2.0**: one request/response/notification
  per line; stdin is inbound protocol, stdout is outbound protocol, and stdout
  carries protocol frames only (all `console.*` output is silenced).
- Request methods: `connect`, `start`, `send`, `stop`, `abort`, `get`,
  `read_messages`, `list_history`, `get_accumulated_usage`, `subscribe`,
  `unsubscribe`, `dispose`. Parameters are the flattened D2 wire shapes
  (`ClineRemoteConfig` for `connect`, `ClineStartRequest` for `start`).
- `subscribe` returns `{"subscription_id": "<bridge-local opaque id>"}`;
  asynchronous SDK events are emitted as `cline.event` notifications with
  `{subscription_id, session_id, kind, payload}`. `unsubscribe` is wire-only,
  idempotent, and stops only its own handle (realizing D2's
  `Subscription.unsubscribe()`, which cannot cross JSON).
- Responses preserve request IDs, including out-of-order completion. Event
  notifications and responses may interleave; every stdout line is one
  complete frame.
- Backend failures use a fixed harmless `error.message`
  (`cline sdk backend operation failed`) with a constrained `error.data`
  classification: `{kind}` of
  `remote_attachment_rejected` / `remote_attachment_unavailable` /
  `uncertain_outcome` / `session_not_found` / `sdk_operation_rejected` (the
  latter carrying a validated safe machine `code` such as
  `hub_connection_closed` / `hub_connect_failed` and optionally a numeric
  `close_code`). Codes are preserved, never interpreted. Raw SDK/provider
  exception text, stacks, URLs, tokens, prompts, results, and transcripts
  never appear in error frames, stderr, logs, or error chains.
- Wire-protocol failures (parse errors, invalid requests, unknown methods,
  invalid params, operations issued before `connect`) use standard JSON-RPC
  error codes with `data.kind: "wire_protocol"` and a bounded `reason`, so D4
  recognizes them separately from backend errors.
- Requests issued before `connect` completes answer
  `wire_protocol/not_attached`; the caller sequences `connect` first. The
  bridge does not queue, replay, or recover on the caller's behalf.

## Mapping notes

- `connect` builds `ClineCore.create({backendMode: "remote", remote: …})`;
  the opaque demonstrated `remote_options` object is forwarded verbatim and
  the explicit D2 fields (`endpoint`, `auth_token` → `authToken`,
  `client_identity` → `clientType`) are authoritative. A re-`connect`
  disposes the previous attachment and creates a replacement; the bridge
  never launches/restarts a Hub, discovers credentials, or falls back to
  `auto`/`local` execution.
- `start` maps the complete D2 construction bundle onto
  `ClineCore.start({config, interactive, toolPolicies, initialMessages?})`:
  `providerId`, `modelId`, `config.mode` (`plan`/`act`), additive
  `config.rules`, explicit `config.systemPrompt`, `cwd`, `workspaceRoot`,
  `enableTools` — and, for same-ID reconstruction, the exact ID as
  `config.sessionId` plus the unmodified raw `readMessages` array as
  `initialMessages`. `enableSpawnAgent`/`enableAgentTeams` are deliberately
  never synthesized (D2 forbids it; the pinned runtime treats absent flags as
  disabled), no approval executors are registered, and no executable initial
  prompt is sent. `start` is allocation only.
- SDK-native results are forwarded as plain JSON with safe native date
  serialization (ISO strings); `undefined` SDK responses map to JSON `null`.
  Nested message arrays survive `read_messages → wire → start` unchanged.
- `dispose` releases bridge/client resources only; `stop` and `abort` remain
  distinct SDK calls; `delete` is never exposed. An SDK disconnect triggers
  no automatic `start`/`send`/reconnect/replay inside the bridge.

## Commands

```sh
npm ci          # locked install from the committed package-lock.json
npm run build   # tsc → dist/ (entrypoint: dist/cli.js)
npm run typecheck
npm test        # builds first (pretest), then vitest run
```

The D4 Python process layer launches the built stdio entrypoint
(`node dist/cli.js` or the `cline-sdk-bridge` bin) with stdin/stdout pipes.

## Tests

Deterministic vitest suites with no model inference, remote Hub, or any live
infrastructure:

- in-process tests over a scripted fake SDK client
  (`tests/helpers/fake-client.ts`) covering construction mapping, native
  return shapes, dispatch/control, subscriptions/concurrency, and safe
  failure classification with sentinel-leak checks;
- spawned-subprocess tests driving the **built** `dist/bridge.js` core over
  real stdio through `tests/spawn-harness.mjs` (scenario-driven fake client
  via `OPENORC_BRIDGE_FAKE_SCRIPT` / `OPENORC_BRIDGE_FAKE_RECORD` env files),
  covering framing, split/coalesced lines, stdout purity, and subprocess
  stderr hygiene;
- smoke tests for the real `dist/cli.js` entrypoint (loads the pinned SDK,
  no attach, exit 0 at end of input).

## Version policy

`@cline/sdk` is pinned to the D1-qualified `0.0.90` exactly. Dependabot
surfaces newer stable releases (including `0.0.x`) as individual,
never-auto-merged compatibility-signal PRs; adopting one is a separate
documented qualification decision, not a routine bump.