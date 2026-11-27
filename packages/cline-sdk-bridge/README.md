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
→ official @cline/sdk / ClineCore (pinned 0.2.1, backendMode "remote")
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
  `unsubscribe`, `dispose`, plus the Hub-native admission/observation methods
  `enqueue`, `list_runs`, and `subscribe_hub` (issue #321). Parameters are the
  flattened D2 wire shapes (`ClineRemoteConfig` for `connect`,
  `ClineStartRequest` for `start`).
- `subscribe` returns `{"subscription_id": "<bridge-local opaque id>"}`;
  asynchronous SDK events are emitted as `cline.event` notifications with
  `{subscription_id, session_id, kind, payload}`. `subscribe_hub` returns the
  same handle shape and asynchronous Hub event envelopes are emitted as
  `cline.hub_event` notifications with
  `{subscription_id, session_id, kind, sequence, payload}` — every field is
  the Hub envelope's own value, forwarded verbatim. `unsubscribe` is
  wire-only, idempotent, and stops only its own handle (realizing D2's
  `Subscription.unsubscribe()`, which cannot cross JSON).
- `enqueue` (issue #321) is nonblocking Hub-native run admission through the
  public `NodeHubClient.command("run.enqueue", {prompt}, sessionId, options)`
  surface: the bridge forwards exactly `{prompt}` on the Hub envelope (no delivery
  mode is ever constructed), applies the documented public
  `retryAfterDispatch: false` command option (issue #407 — consequential
  admission is never retransmitted by SDK transport recovery, and the option is
  a per-command argument no remote configuration can override), verifies the
  ack, and answers with `{run_id, accepted_at, queue_position}` — transport
  evidence for later exact correlation, never a workflow decision. `list_runs`
  answers with the status-only `runs` array from the `run.list` reply, opaque
  and uninterpreted; a missing or malformed array is an `uncertain_outcome`
  error, never an empty list. Admission is neither delivery nor execution;
  dispatch timeouts (`hub_command_timeout`) and Hub transport failures
  (`hub_connection_closed` and the other pinned transport codes) classify as
  `uncertain_outcome` for these hub operations — never as refusals, and
  never authorizing a resend. Positive Hub refusals keep their safe machine
  code (for example `run_admission_rejected`, `hub_draining`).
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
- The hub-native operations (issue #321) run through one officially exported
  `NodeHubClient` per attachment, constructed lazily on the first hub
  operation from the same admitted endpoint, authentication, and client
  identity as the `ClineCore` attachment, reused for the attachment
  lifetime, and disposed with it. The pinned SDK's declaration file cannot
  re-export the class through `@cline/sdk` under this package's NodeNext
  resolution (still true of the pinned 0.2.1 root declaration), so
  `sdk-client.ts` extracts the public constructor, URL normalizer, and
  per-command options through the package namespace with one documented
  boundary cast; no new dependency, no private shape, and no tsconfig change.
- `start` maps the complete D2 fresh-construction bundle onto
  `ClineCore.start({config, interactive, toolPolicies})`: `providerId`,
  `modelId`, `config.mode` (`plan`/`act`), additive `config.rules`, explicit
  `config.systemPrompt`, `cwd`, `workspaceRoot`, `enableTools`.
  `enableSpawnAgent`/`enableAgentTeams` are deliberately never synthesized
  (D2 forbids it; the pinned runtime treats absent flags as disabled), no
  approval executors are registered, and no executable initial prompt is
  sent. `start` is allocation only, fresh construction only.
- `resume` (issue #408) is the native same-ID reconstruction: one public
  `NodeHubClient.command("session.resume", payload, sessionId,
  {retryAfterDispatch: false})` call carrying the complete construction
  bundle for the exact existing session — the Hub performs the saved-history
  loading and no transcript is ever uploaded (the former
  `read_messages → start(initialMessages)` re-upload path is removed). The
  payload mirrors the pinned remote session-creation shape: `sessionConfig`
  (`sessionId`, `providerId`, `modelId`, `cwd`, `workspaceRoot`,
  `systemPrompt`, `mode`, `rules`, `enableTools`), `metadata`
  (`interactive`, `provider`, `model`, `enableTools`), `runtimeOptions`, and
  `toolPolicies`. The reply's restored identity is extracted from the pinned
  reply shapes (`payload.snapshot.sessionId`, `payload.session.sessionId`, or
  `payload.sessionId`) and verified against the requested exact ID — a
  mismatch throws a safe machine-coded error that classifies as an unproven
  uncertain outcome. After verification, one read-only
  `core.pendingPrompts.list({sessionId})` consultation establishes the
  documented session-event subscription wiring for the restored incarnation;
  its failure is contained and never reclassifies the verified resume.
  Native refusals (`session_already_exists`, `session_not_found`,
  `session_messages_not_found`, `unsupported_command`) and transport
  failures/timeouts classify at the boundary; a lost reply never authorizes
  another resume.
- SDK-native results are forwarded as plain JSON with safe native date
  serialization (ISO strings); `undefined` SDK responses map to JSON `null`.
  `read_messages` serves the conversation-read/reconciliation capabilities
  and positive continuity evidence — never a transcript upload.
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

`@cline/sdk` is pinned to the SDK1/SDK2a-qualified `0.2.1` exactly (matched to
the CLI 3.0.75 Hub; openorc/cloud#57, core issue #407). Dependabot surfaces
newer stable releases (including `0.0.x`) as individual, never-auto-merged
compatibility-signal PRs; adopting one is a separate documented qualification
decision, not a routine bump. The qualification record — static pinned-source
findings, the deterministic controlled-transport tests, and the live ClineBox
probe evidence — lives in [QUALIFICATION-0-2-1.md](QUALIFICATION-0-2-1.md).

Consequential-dispatch discipline (issue #407): every effectful Hub command
the bridge issues passes `retryAfterDispatch: false`; read-only queries keep
the SDK default. Effectful `ClineCore` operations (`start`/`send`/`stop`/
`abort`) expose no supported suppression option at their public surface, and
the pinned SDK's transport-recovery path is additionally gated on a
remembered local-Hub URL the bridge never registers — the SDK-internal
retransmission evidence and any remaining limitation are recorded for the
adapter qualification (#413).