# SDK2a qualification — `@cline/sdk` 0.2.1 bridge (core issue #407)

Status: **qualified** for the SDK2a scope — the deliberate 0.2.1 checkout is
the supported bridge baseline, safe for consequential dispatch and remote
attachment. Adapter-level (ClineAdapter/projection) qualification is NOT
included here and remains owned by #413. Evidence recorded 2026-11-27.

Parent: #404 (Phase 2 Cline upgrade). Baseline pairing evidence:
[SDK1](https://github.com/openorc/cloud/blob/main/deployment/cline/research/sdk1-clinebox-3-0-75.md)
(openorc/cloud#57). Targeted live qualification ran on the existing isolated
SDK1 reference ClineBox (`openorc-cline-runtime-51c919e6`, Droplet 623822957,
CLI 3.0.75 / core 0.2.1, account auth persisted from SDK1) through the
operator SSH loopback forward. No Hub restart, no machine-global change, no
provider secret injection, no paid inference; the verified free pair
(`cline` / `cline-free/nemotron-3.5-super-vl`) carried the only inference.
Disposable probe sessions only; the box finished with zero active runs
(read-back: 5 runs total, all `completed`). The probe tool is committed at
`tools/sdk2a-live-probe.mjs` (sanitized JSON phases; the Hub credential never
left the mode-600 ephemeral file, deleted after use).

## Static findings (pinned-package source/declaration analysis)

1. **Public suppression option**: 0.2.1 `NodeHubClient.command(command,
   payload, sessionId, options?: HubCommandOptions)` adds
   `retryAfterDispatch?: boolean` ("Whether transport recovery may resend a
   request after dispatch"). Without it, the client's retry loop performs one
   transport-recovery resend after dispatch.
2. **Recovery gating**: the resend only happens when
   `recoverLocalHubTransport` succeeds, which requires (a) the current URL to
   be in the remembered local-Hub URL set (`rememberRecoverableLocalHubUrl`)
   and (b) the failure to be a `HubTransportError` — command timeouts
   (`hub_command_timeout`) never trigger it. Every `remember…` call site
   lives in local-Hub adoption/daemon machinery (ensure/discovery/daemon
   startup); remote-mode construction never registers the endpoint.
3. **Runtime-host sharing**: the `ClineCore` remote runtime host issues
   `session.create`, `run.start`, `run.abort`, and `session.detach` through
   the same internal client `command()` mechanism (identical retry loop).
   Its SDK-internal `session.create` deadlock retry additionally requires a
   `hub_command_timeout` for `session.create` plus the remembered-URL check.
   **No suppression option exists at the `ClineCore.start/send/stop/abort`
   public surface** (only `session.mcp_apps.call_tool` is internally
   suppressed).
4. **Unchanged classification contracts**: `HubTransportErrorCode` union
   (`hub_connect_timeout` / `hub_connect_failed` / `hub_connection_closed` /
   `hub_connection_not_open`), `hub_command_timeout`, and the
   `SessionNotFoundError` `session_not_found` shape are unchanged from
   0.0.90. `CoreRuntimeFeatures` still requires
   `enableSpawnAgent`/`enableAgentTeams` (the no-synthesis start cast
   remains). The root declaration still does not re-export
   `NodeHubClient`/`normalizeHubWebSocketUrl` through NodeNext (the
   boundary cast remains).

## Deterministic controlled-transport tests (real pinned client)

`tests/transport.test.ts` drives the **real** installed SDK 0.2.1 against a
local stub Hub over real WebSockets (no bridge wrapper, no fake client):

- With `retryAfterDispatch: false`, a consequential `run.enqueue` whose
  transport dies after dispatch is dispatched **exactly once** (stub Hub
  observes one command frame); the command rejects with the pinned transport
  error shape. The ack path still resolves the Hub reply payload.
- The **real `ClineCore` remote runtime path** (representative consequential
  operation `run.abort`, the shared command mechanism) behaves identically:
  exactly one dispatch, no retransmission, `hub_connection_closed` outcome.
- Read-only subscriptions reconnect to the same admitted endpoint
  (re-register + subscription re-issue) with no local-Hub repoint.

## Live evidence (observed behavior, sanitized probe transcript)

Probe run (committed tool, phases in order):

```json
{"phase":"attach","ok":true,"historyEntries":9}
{"phase":"quiescence","ok":true,"existingRuns":3}
{"phase":"allocate","ok":true,"session_allocated":true}
{"phase":"enqueue_ack","ok":true,"has_run_id":true,"has_accepted_at":true,"has_queue_position":true}
{"phase":"event_forwarding","hub_events":19,"hub_event_kinds":["agent.done","assistant.delta","assistant.finished","cloud.event","iteration.finished","reasoning.delta","reasoning.finished","run.completed","session.updated","usage.updated"],"session_events":24,"session_event_kinds":["agent_event","session_snapshot","status"],"run_statuses":["completed"],"window_timed_out":false}
{"phase":"allocate_fault_session","ok":true}
{"phase":"fault_dispatch","dispatched":true}
{"phase":"fault_transport_outcome","rejected":true,"code":"hub_connection_closed"}
{"phase":"fault_probe","rejected_code":"hub_connection_closed","session2_run_count":1}
{"phase":"cleanup","ok":true}
```

Observed conclusions:

- **Authenticated remote attachment** through the exact admitted endpoint,
  native current Hub credential, and the bridge's own production client
  factory (`dist/sdk-client.js`): `listHistory` and status-only `run.list`
  resolve over the SSH loopback forward.
- **Promptless allocation** (no inference) on the free pair succeeds through
  `ClineCore.start`; the bridge's `SdkStartInput` mapping is compatible.
- **`run.enqueue` acknowledgement** carries the qualified shape
  (`runId` non-empty string, safe-integer `acceptedAt`, non-negative
  safe-integer `queuePosition`) — the bridge's `verifyRunAdmission` contract
  is unchanged and every real admission verifies.
- **Native event/status forwarding**: hub event envelopes
  (`agent.done`, `assistant.delta`, `assistant.finished`, `cloud.event`,
  `iteration.finished`, `reasoning.delta`, `reasoning.finished`,
  `run.completed`, `session.updated`, `usage.updated`) and session events
  (`agent_event`, `session_snapshot`, `status`) forward through the
  subscription surfaces; the status-only `run.list` records carry
  `runId/sessionId/state/acceptedAt/startedAt` (state field, records stay
  opaque). A free-model run completed within the bounded window.
- **Disconnect-after-dispatch safety (live)**: a consequential `run.enqueue`
  dispatched through the probe's own ephemeral tunnel, destroyed immediately
  after dispatch: the command rejected with `hub_connection_closed`
  (closeCode 1006) and a fresh attachment observed **exactly one** run for
  the disposable session — no retransmission of the admitted command.

## Retained behavior and limitations (recorded, not worked around)

- `retryAfterDispatch: false` is applied authoritatively at the real
  `enqueue` call site. Effectful `ClineCore` operations (`start`/`send`/
  `stop`/`abort`) expose no suppression option; their guarantee rests on the
  never-remembered-endpoint gating plus the observed non-retransmission
  behavior above. Any future SDK change to that gating is a compatibility
  signal requiring re-qualification.
- The pinned client retry loops and the `reconnectSubscribedTransport` path
  consult the remembered-URL set for repointing; the bridge never registers
  its endpoint, so no local-Hub fallback/start is reachable.
- Probe sessions remain as disposable history records on the qualification
  Box (no active sessions/runs at the end); no Hub restart was performed.
- `hub_command_timeout` default for unaugmented commands is 8s (unchanged
  semantics: client-side dispatch timeout = uncertain outcome).

## Scope boundary

This leaf qualifies the bridge transport only. D7 Owner database-backed runs
remain SDK2c work (this change never provisions or executes `--testdb`);
Cloud runner defaults/overrides remain openorc/cloud#64; native
`session.resume` remains #408; adapter/session/usage/interrupt qualification
remains #413.
