# SDK3 qualification — native same-session reconstruction (core issue #408)

Status: **implemented and deterministically verified**; the targeted
no-database live qualification and the controlled-restart validation are
recorded as **pending** below with reproducible execution instructions.
Parent: #404. Consumes [SDK2a bridge
qualification](https://github.com/openorc/core/blob/main/packages/cline-sdk-bridge/QUALIFICATION-0-2-1.md)
(core PR #414) and [SDK2c adapter
qualification](https://github.com/openorc/core/blob/main/src/openorc/adapters/cline/QUALIFICATION-0-2-1.md)
(core PR #415); no part of either matrix is repeated here. Baseline pins:
`cline 3.0.75` / `@cline/core 0.2.1` / `@cline/sdk 0.2.1` (unchanged).

## What changed (implementation, not a decision)

Native public `NodeHubClient.command("session.resume", …)` is the SOLE
same-session reconstruction mechanism. The client-side
`read_messages → stop → start(initialMessages)` transcript re-upload
workaround is removed — no fallback, no compatibility switch. One serialized
reconstruction seam (`ClineAdapter._run_same_id_reconstruction`) is shared by
ordinary mode/context changes and authorized stopped recovery:

- **Ordinary live reconstruction** (IMPLEMENT PLAN→ACT, PREPARE ACT→PLAN,
  terminal-cleanup ACT reconstruction): the qualified `stop` releases the
  exact quiescent incarnation, then the native `resume` restores the exact
  session ID with the resolver-resolved CURRENT authorized construction
  bundle (role provider/model and role Markdown resolved at reconstruction
  time — never historical role configuration or a changed Connection's
  repository default). Hub-side saved-history loading; no transcript is ever
  uploaded.
- **Authorized stopped recovery** (`recover_session`): the existing positive
  surviving-history requirement is PRESERVED — the serialized
  `read_messages` positively establishes the exact persisted conversation
  before any effect (an empty read on an established bound session refuses;
  a history listing, empty read, echoed session ID, or transport failure
  alone never establishes recoverability) — then the positively absent
  incarnation (tolerated stop absence only) precedes the native `resume`.
  The guard was not weakened merely to eliminate a read; the read is
  positive-continuity evidence, not an upload.
- **Resident refusal**: a native resume of a live incarnation
  (`session_already_exists`) is a positively classified known refusal; it
  never authorizes stopping another incarnation, and there is no silent
  replacement or automatic stop-and-resume fallback.
- **Consequential classification**: `resume` is an effectful incarnation
  change in BOTH bridge (`HUB_COMMAND_OPERATIONS`) and adapter
  (`EFFECTFUL_OPERATIONS`) classification. Unproven rejection, malformed or
  mismatched acknowledgement after possible dispatch, timeout, or lost reply
  fail closed through the existing uncertain-effect protection with a
  dedicated `resume` effect kind released ONLY by the new
  `settle_uncertain_resume` caller-attested settlement — never cleared via
  send/abort settlement or inferred quiescence. Only positively qualified
  pre-effect refusals (`session_already_exists`,
  `session_messages_not_found`, `invalid_resume`, `unsupported_command`,
  and exact saved-record absence via `session_not_found` →
  `AgentSessionNotFoundError`) are known refusals. A lost reply never
  authorizes another resume.
- **Post-resume addressing**: after the reply identity verification, one
  read-only `core.pendingPrompts.list({sessionId})` consultation establishes
  the DOCUMENTED session-event subscription wiring for the restored
  incarnation (declared `hasSessionSubscription` contract: "In hub mode
  ClineCore subscribes to a session when it starts, sends to, or lists
  pending prompts for it, and unsubscribes on stop"), so observation, reads,
  authorized turns, and later stop/reconstruction address the restored
  incarnation through the ordinary production backend. The consultation is
  auxiliary and contained: the incarnation change already took effect, its
  failure never reclassifies the verified resume, and the documented
  lifecycle self-heals at the next ordinary send. All other host operations
  are stateless sessionId-addressed Hub commands (`run.abort`,
  `session.detach`, `session.get`, `session.messages`), and the blocking
  send path (`runTurn`) re-ensures the subscription itself.
- No shared application/domain/persistence/neutral-contract change; no
  Owner Resume/Continue semantics change; no initialization replay, no mode
  notice, no replacement conversation.

## Static contract (pinned 0.2.1; public API distinguished from internal examples)

Documentation gap recorded first: docs.cline.bot documents neither
`NodeHubClient.command` per-command options nor `session.resume` (its
"Gateway" page is the `@cline/llms` provider gateway; the ClineCore page
documents no resume/reconstruct operation). The command name is PUBLIC
(exported `HubCommandName` union); the payload/response/error semantics below
come from the pinned implementation and are live-verified per the pending
procedure. No private wire protocol, private runtime files, or private
package imports are used.

- **Hub-side handler** (pinned `cU` → shared creation handler `$g`):
  requires `payload.sessionConfig.sessionId` (trimmed, non-empty; else
  `invalid_resume`); requires a saved session record (else
  `session_not_found`, "The saved session was not found."); requires
  non-empty saved messages (else `session_messages_not_found`, "The saved
  conversation is empty or unavailable."); then runs the shared
  create/resume/restore creation routine with the saved messages injected as
  `initialMessages`. The pinned source therefore POSITIVELY requires and
  restores the exact saved conversation: a missing record or an
  empty/unavailable conversation can never resume successfully.
- **Honored construction fields** (`$g`): `sessionConfig` (sessionId,
  providerId, modelId, cwd, workspaceRoot, systemPrompt, mode, maxIterations,
  enableTools, enableSpawnAgent, enableAgentTeams, checkpoint — the whole
  sessionConfig object is spread through to the runtime config, which is how
  the additive role `rules` reach the runtime, matching the pinned CLI
  session-client create contract), payload `metadata` (`interactive` gates
  `startSession.interactive`; provider/model fallbacks; source), payload
  `toolPolicies` (→ `autoApproveTools`), top-level `workspaceRoot`/`cwd`,
  and `initialCompactionState`. Runtime-side feature flags resolve from mode
  presets with config overrides only when boolean — the bridge mirrors the
  SDK's own remote create payload shape (same fields, same omissions), so
  defaults resolve identically for the resumed incarnation.
- **Reply identity**: the pinned reply carries the restored identity as
  `payload.snapshot.sessionId`, `payload.session.sessionId`, or
  `payload.sessionId` (all three shapes verified against the pinned create
  and resume call sites); the bridge extracts exactly these shapes, trims,
  and fails closed (`resume_identity_mismatch`, an unproven uncertain
  outcome) on anything else.
- **Internal example provenance**: the SDK's own cloud-handoff
  `restoreSavedInnerSession` resumes with
  `{workspaceRoot, cwd, metadata{…, interactive:true}, sessionConfig{…},
  runtimeOptions, toolPolicies?}`, treats `session_already_exists` as
  residency, and falls back to `session.messages` +
  `session.create(initialMessages)` on `unsupported_command` — the fallback
  being exactly the workaround this leaf removes. This is an internal
  example, not the OpenOrc contract; the OpenOrc bridge issues the command
  directly through the public `NodeHubClient` with the full construction
  bundle.
- **Consequential classification**: `session.resume` is in the pinned
  SDK's mutating/hub-draining command set; per the SDK2a handoff the
  documented public `retryAfterDispatch: false` option is applied
  authoritatively at the real call site (a per-command argument, never
  derivable from `remote_options`).
- **Subscription/addressing facts**: the host's session-event delivery is
  wired only by its per-session hub subscription
  (`ensureSessionSubscription`), triggered by start/send/pending-prompts;
  `subscribe()` is a pure local filter; `handleHubEvent` has no global
  subscription. `readLiveMessages` is declared public with the documented
  "seeding a replacement session during a plan/act mode switch" use — the
  public surface anticipates mode-switch reconstruction, and the resume
  payload maps onto the same wire contract that surface implies.

## Deterministic evidence (all passing in this change)

- Real call-site contract (`sdk-client-options.test.ts`): `session.resume`
  issued with `retryAfterDispatch: false` and the exact full payload
  (`sessionConfig` incl. `rules`, `metadata.interactive`, `runtimeOptions`,
  `toolPolicies`); every pinned reply identity shape accepted (trimmed);
  wrong-identity reply fails closed with `resume_identity_mismatch` and
  never consults the wiring surface; a failed wiring consultation never
  reclassifies the verified resume; `run.list` keeps the SDK default; the
  enqueue safety-option matrix is reused, not repeated (same
  `NodeHubClient.command` mechanism — SDK2a transport evidence carries
  over).
- Bridge mapping/wire (`mapping.test.ts`, `failures.test.ts`,
  `returns.test.ts`, scripted-child and real-seam
  `test_adapters_cline_bridge_process.py`): the `resume` wire operation
  forwards the complete bundle verbatim with no `initial_messages` anywhere
  on the wire; the verified identity is the only success value; fresh
  `start` carries no session ID; reply/error mapping unchanged elsewhere.
- Adapter seam (`test_adapters_cline_adapter.py`, 170 tests): ordinary
  reconstruction sequence `stop → resume` with the full bundle and NO read;
  wrong returned identity fails closed; uncertain stop never reaches the
  resume; resident/absent refusal classifications
  (`session_already_exists`/`session_messages_not_found`/
  `unsupported_command` → known refusals; `session_not_found` →
  session absence); uncertain resume latches the dedicated kind, blocks
  every later effect (sends, detached admissions, reconstructions), and is
  released only by `settle_uncertain_resume` with caller attestation — never
  by send/abort settlement; recovery keeps the positive surviving-history
  precondition (absent read and empty read refuse before any effect;
  positively-absent stop tolerated) then resumes natively; Reviewer-PLAN
  enforcement, currentness/binding resolution, send/abort concurrency, and
  settlement separation reuse the existing suites unchanged.
- Values/contract/classification units: `ClineResumeRequest`/
  `ClineResumeResult` validation, sensitive-field repr withholding,
  refusal-code taxonomy.
- Full deterministic baseline: 4648 passed (ordinary suites; the
  `integration`-marked D7 module stays Owner-run).

## Pending: targeted no-database live qualification (Owner-provided Box access)

The live run requires the isolated SDK1 reference ClineBox
(`openorc-cline-runtime-51c919e6`, Droplet 623822957) through the operator
SSH loopback forward with the CURRENT Hub credential from the trusted
supplier (`cline hub ensure --json` over trusted SSH into a mode-600
ephemeral file, deleted after use) — the same operator inputs the committed
SDK2a/SDK2c probe tools consumed. This session's environment has none of
them (no SSH config entry, no `cline` CLI, no loopback forward), so the run
is recorded here as pending with reproducible instructions; it is not a
#408 implementation/merge prerequisite per the issue.

Procedure (drive the PRODUCTION path — `ClineBridgeProcessBackend` over the
built bridge and `ClineAdapter` with explicit per-role contexts, exactly the
committed `packages/cline-sdk-bridge/tools/sdk2c-adapter-live-probe.py`
discipline; free pair `cline` / `cline-free/nemotron-3.5-super-vl` only;
disposable sessions registered for cleanup, stopped, quiescence verified via
status-only `run.list`; no database, no `--testdb`, no paid inference; every
verdict below determines success):

1. **Attach/allocate**: connect the production backend; promptless
   allocation of a Producer ACT session at a disposable repository base
   through `allocate_detached_session`.
2. **Baseline turn**: admit one detached turn (free pair); verify the
   attributable terminal; read the saved history (non-empty) and the
   aggregate usage (record honestly; the free pair reports `totalCost = 0`).
3. **Ordinary native reconstruction**: bind the checkout context and run
   `reconstruct_session(producer_id, target_mode=PLAN)`; verify the exact
   identity, the private verbatim saved-history comparison before/after
   (no legitimate runtime change intervening), and the usage observation.
4. **Effective construction**: admit a PLAN turn requesting a file write;
   the plan guard must prevent the mutation (verdict computed in-probe;
   reply text never emitted) and the Box-side marker must not exist through
   the Owner-approved operator-check hook. Then reconstruct ACT at the
   checkout and ACT back at the original base, each with the private
   verbatim comparison; Reviewer PLAN in its separate neutral context where
   affected. A compliant model response, echoed config, or same ID alone is
   insufficient.
5. **Resident refusal**: attempt `reconstruct_session` while the incarnation
   is live; require the positively classified refusal (never a
   stop-and-retry).
6. **Stopped recovery**: `stop` the incarnation; require the record
   present, run `recover_session` (the read precondition is the adapter's),
   verify the exact identity, the verbatim restored conversation, and a
   usable continuation turn.
7. **Usage**: record aggregate usage before/after reconstruction and
   recovery; continuity, a genuine reset, and absence are each distinct
   observations — never spend or loss evidence.

## Pending: controlled Hub restart (final Owner-run D7 campaign — the single campaign)

The restart evidence for the native production mechanism comes from the ONE
final Owner-run ordered database-backed D7 campaign
(`tests/integration/test_cline_live_runtime.py`, issue #149) against the
completed SDK3 baseline and the qualified Cloud runner — the #404 closure
gate. This change adds focused, deterministic D7 support (never executed by
Cline; `--testdb` is never provisioned or run by Cline):

- D3 now captures the same-session aggregate usage before the controlled
  restart (both restart modes, and through the two-stage preserved-database
  handoff) and after the recovery, recording continuity/reset/absence
  honestly in the scenario evidence (never spend or loss claims; application
  cost accounting unchanged).
- D3's record now names the native same-ID recovery with a post-recovery
  continuation turn as the recovery evidence; the surviving-transcript and
  credential-rotation assertions are unchanged. The existing supported
  Owner-controlled restart modes (hook, manual pause, two-stage resume) are
  preserved.
- Effective post-restart Producer construction remains evidenced by the D7
  Producer workflow scenarios (B/C/D-series) over the recovered session;
  the recovery seam itself is the native path.

A separate targeted no-database restart is NOT planned: it would duplicate
the D7 campaign. If live qualification surfaces a concrete native behavior
uncertainty that only a controlled restart can resolve, document the
distinct gap and obtain explicit Owner authorization first.

## Owner handoff

- Run the pending live procedure (inputs: `OPENORC_SDK3_TOKEN_FILE`,
  `OPENORC_SDK3_ENDPOINT`; optional `OPENORC_SDK3_ENTRYPOINT` /
  `OPENORC_SDK3_NODE`) and record the sanitized phase evidence in this file.
- Run the final D7 campaign (Owner-authorized restart) and record its D3
  evidence; resolve any critical failures.
- Until then: the supported behavior is the static contract plus the
  deterministic evidence above; the live confirmation and restart evidence
  are pending, not passed.
