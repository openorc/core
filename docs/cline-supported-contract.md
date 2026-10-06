# Supported Cline integration contract

R5 normalized Core contract, 2026-10-06. This document defines the supported implementation baseline; the grounding document defines product/design authority. Empirical provenance and rejected assumptions remain in the [Cloud R5 research handoff](https://github.com/openorc/cloud/blob/main/deployment/cline/research/r5-synthesis.md). Core implementation has no runtime dependency on Cloud artifacts or infrastructure.

## Supported baseline

| Layer | Exact supported version | Pin owner |
| --- | --- | --- |
| Official CLI distribution `cline` | `3.0.68` | Cloud managed Box config; BYO operator supplies compatible runtime |
| Hub/runtime `@cline/core` | `0.0.90` | CLI release pairing; verify effective Hub version |
| Public client `@cline/sdk` | `0.0.90` | Core D3 actual npm manifest and lockfile |
| JavaScript runtime | existing Node 24 line; qualification used `24.21.0` | existing OpenOrc toolchain |

SDK is the published Core alias/re-export. Core needs no direct `@cline/llms`, helper, Bun, platform CLI, or extra catalog package. Capture transitive packages in the real lockfile; verify resolved Core `0.0.90`. D1 records the supported baseline without adding a synthetic npm package; D3 owns its actual manifest/lockfile. Cloud already pins CLI `3.0.68` in `deployment/cline/cline-box/cline-box.yaml`. Every managed CLI launch uses `CLINE_NO_AUTO_UPDATE=1` and provisioning verifies the installed version. These are supported versions, not a claim about today's latest registry release.

## Infrastructure-neutral Core contract

### Integration and construction

Python ClineAdapter → replaceable ClineSdkBackend → local long-lived Node JSON-RPC subprocess → public `@cline/sdk` / `ClineCore(remote)` → existing Hub. Bridge owns SDK lifecycle/translation only. Core takes a usable endpoint and control credential through its Connection/auth boundary; it neither provisions the Hub nor discovers private credentials.

Required SDK subset: `create` remote options (`clientName`, `backendMode: remote`, `remote.endpoint`, optional authentication/client identity), `start`, `send`, `get`, `readMessages`, `listHistory` (or qualified equivalent history read), `getAccumulatedUsage`, `subscribe`/unsubscribe, `abort`, `stop`, and client `dispose`. Transport only needed events/results/errors. No wholesale ClineCore mirror, catalog, configured-provider list, checkpoint fork, dynamic model switching, pending-prompt steering, interactive executor, or approval callback API is required. `delete` is qualification evidence for loss, not an automatic recovery/cleanup control.

Every fresh construction and same-ID rebuild supplies:

```text
config.providerId / modelId = opaque per-role Owner selections
config.rules = resolved runtime-neutral OpenOrc role Markdown
config.mode = workflow-derived plan|act
config.systemPrompt = ""
config.cwd = config.workspaceRoot = that Task role's repository checkout
config.enableTools = true
config.enableSpawnAgent = false
config.enableAgentTeams = false
interactive = true
toolPolicies.ask_question.enabled = false
sessionMetadata = safe role/correlation metadata where used
capabilities / interactive executors / approval callbacks = not registered
```

Producer starts PLAN, changes to ACT only on an already-authorized IMPLEMENT control, remains ACT for remediation. Reviewer stays PLAN. R3's ACT Producer probe is not authorization to start workflow Producers in ACT. PLAN permits read-only commands and guards mutating commands; it is not a blanket command-execution ban or a security sandbox.

Keep the seven concerns distinct: formal JSON schemas; configurable runtime-neutral role prompts; non-overridable thin initialization; non-overridable workflow controls; optional Workspace guidance; repository-owned root AGENTS.md/context; runtime-native system/mode/tool harness. OpenOrc owns default Producer/Reviewer Markdown plus optional Owner overrides on role bindings. NULL override resolves the current default at fresh construction; never copy defaults into ordinary Workspace configuration. Role prompts cannot redefine schemas, authority, subject identity, communication, or lifecycle. Defaults are product assets, not research token prompts. Workspace guidance retains its existing separate composition seam.

Owner decision in R5: role prompts cannot change mid-Task. Reject material override edits/reset and any routing change that would alter the resolved prompt while an already-started nonterminal Task depends on the Workspace role. Serialize edits with admission so CONNECTING and recovery gaps are covered; same-value writes are no-ops. Shipped default changes wait until affected Tasks finish/cancel before activation. This permits recovery to re-supply the unchanged prompt without Task-session prompt copies, hashes, versions or history. Provider/model changes remain future-session choices; recovery uses initialized Task-session selected IDs. Dedicated Core [#160](https://github.com/openorc/core/issues/160) owns this configuration seam and its minimal guard.

Provider/model values are opaque strings, not catalog entries or enums. Native UI supplies the model ID; provider ID is a separate Cline identifier and is not assumed discoverable there. `start` accepts unusable selections; first-send failure before valid `session_ready` is a construction/configuration failure. Requested identity is not verified effective identity; reported provenance stays nullable and opportunistic. Cline owns provider credentials and native settings; Core never passes provider API keys or parses native provider files.

### Initialization, response and observation

Fresh `start` allocates but its prompt does not execute remotely. Deliver canonical initialization as the FIRST `send`, validate the existing `session_ready` schema, then expose READY. Set `interactive: true`. Compose initialization above the adapter using the existing protocol assets; no second handshake in the service and no role handbook added to initialization.

`send` resolves an AgentResult (or undefined on its public type); extract the actual reply and pass formal results through existing protocol validation. Undefined/empty/invalid results never become semantic success. Finish reasons are `completed|max_iterations|aborted|mistake_limit|error`; completion is not itself a valid OpenOrc result. `abort` resolves the outstanding send with `aborted` and preserves later turns. `stop` releases the runtime and finalizes bookkeeping; it does not prove Task/session loss. Continuation uses the next authorized ordinary interaction in the same context, not replay of the aborted input or a made-up native resume API.

Qualified event union: `chunk`, `agent_event`, `team_progress`, `pending_prompts`, `pending_prompt_submitted`, `session_snapshot`, `ended`, `hook`, `status`. Relevant nested agent events include content/iteration updates, usage, notice, done and error. D2 exposes only what consumers need, not teammate/steer frameworks. Snapshot/status/ended interleave; record `completed|failed|cancelled` is runtime bookkeeping, never TaskAgentSession lifecycle or workflow success. Persisted messages and usage may lag to assistant/turn boundaries.

### Same identity, replaceable runtime

Settled same-ID rebuild:

```text
serialize work on exact session; reconcile/quiesce outstanding turn
→ readMessages(existing sessionId)
→ await stop(existing sessionId)
→ start(full construction bundle,
        config.sessionId = existing sessionId,
        config.mode = target workflow mode,
        initialMessages = raw readMessages array verbatim)
→ next ordinary authorized interaction
```

Preserve message IDs, roles, typed content and timestamps; do not parse/re-wrap the transcript. Preserve external ID, TaskAgentSession identity, original record start time, prior transcript order and one-time initialization. No initialization resend or invented continuation/mode-notice prompt. Re-supply all fields even where omission happened to fall back to old record values. Per-turn `send(mode)` only tags user input; it cannot change runtime mode. Original `metadata.mode` is stale after rebuild, so current mode comes from OpenOrc's authorized workflow/construction decision. No public effective prompt/mode readback is required.

Separate ordinary Task-role checkouts provide working context isolation on one Hub. Reviewer checkout is prepared at the exact committed subject before review; never review Producer uncommitted state. Root AGENTS.md is naturally discovered; no rule-file hydration. GitHub owns canonical pushed branch/head facts. Directory separation is not cross-Workspace security isolation. Nested AGENTS discovery and production private-repository Git credential delivery were not qualified.

### Reconciliation and loss bounds

| Qualified condition | Public evidence | Safe Core consequence |
| --- | --- | --- |
| Bridge dies during send | no pending return; Hub finishes in qualified run; record/transcript/usage readable | reconcile exact logical dispatch; do not resend |
| Live transport cut | pending call rejects `hub_connection_closed`, closeCode `1006`; no session-event loss notification | reconnect and reconcile; outcome is uncertain until resolved |
| Fresh client attaches mid-turn | no live events via either plain subscribe form | use public reads, not assumed replay; original surviving client auto-resumed stream |
| Stop resolved, bridge died before start | record and raw transcript survive | reconcile known stop/start progress, then same-ID full rebuild; no new identity |
| Settled-session Hub restart | record/history/transcript survive; send may say `session not found`; usage resets | reattach using current credentials supplied by runtime owner; reconcile, then same-ID rebuild |
| Deleted bound session | get undefined, history absent, reads/sends `Unknown session: <id>` | genuine loss; fail closed, never synthesize replacement |
| Attach failure | `hub_connect_failed`, potentially empty message | unavailable/auth/connect-level condition, not loss; cannot distinguish bad token from unreachable Hub |

Read `get`, canonical `readMessages`, usage, and history with durable OpenOrc dispatch/control state. Missing events or temporary missing runtime addressability never suffice to classify loss. R4 qualified stop→loss before start, not ambiguous acceptance of a replacement start. If send/start acceptance, complete transcript or target construction cannot be established, remain blocked/uncertain rather than replaying. In-flight Hub restart was not qualified; do not promise transparent recovery or invent a new fault matrix. Usage continuity holds within one Hub lifetime only; no cross-restart accumulation or invented totals. Public status can re-project after restart; own durable OpenOrc state retains workflow truth.

Cline approvals are absent from v1 OpenOrc integration. Do not create Cline RuntimeRequests, register interactive executors, or set `autoApprove:false` policies. Existing runtime-neutral RuntimeRequest domain remains available for a future demonstrably required adapter contract. Native configuration UI stays Cline-owned.


## Implementation sequence

D1 #70 adopts the frozen baseline and affected-upgrade gate. #160 adds typed role inputs/default assets and active prompt-edit guard before D5 #74; backend/bridge D2–D4 can proceed independently. D6a #75 owns activity abort/continuation, D6b #148 public reconciliation, D7 #149 completed-stack qualification. D3 owns the actual exact SDK npm manifest/lockfile and all-stable-release Dependabot signal, not D1 placeholder dependencies.

## Future supported-version qualification

1. Read current official SDK documentation for changed public seams before archaeology or experiments.
2. Compare released public types/behavior to this supported contract; release notification is only a signal, never upgrade authorization.
3. Run affected representative checks on a remote single CLI-owned Hub, using disposable fixture roles and no production Task side effects. Baseline checks cover blank native harness/additive role rules, first-send readiness, PLAN guard and authorized ACT rebuild, transcript/one-init continuity, usable/misconfigured selection, and affected abort/loss/reconciliation seams.
4. Do not repeat unrelated protocol/persistence/error/lifecycle matrices. Revalidate Cloud snapshot/auth/readiness only when the changed release affects those boundaries.
5. Update pins/lockfile, decision ledger and affected contracts deliberately; then run D7's completed-stack integration. Keep private protocol, state files and dashboard internals out of Core.

