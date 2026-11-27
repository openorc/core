# SDK3 qualification — native same-session reconstruction (core issue #408)

Status: **implemented, deterministically verified, and live-qualified**
(the targeted no-database native qualification ran through the production
path on the reference Box, evidence below); the controlled-restart
validation is the single final Owner-run D7 campaign, recorded as pending
with its prepared focused additions.
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

## Live qualification (executed 2026-11-27; production path, reference Box)

Executed through the committed production instrument
`packages/cline-sdk-bridge/tools/sdk3-native-resume-probe.py` against the
isolated SDK1 reference ClineBox (`openorc-cline-runtime-51c919e6`,
Droplet 623822957, CLI 3.0.75 / core 0.2.1) through the operator SSH
loopback forward, with the CURRENT Hub credential from the trusted supplier
(`cline hub ensure --json` over trusted SSH into a mode-600 ephemeral file,
deleted after use). Discipline: the probe drives the production path
(`ClineBridgeProcessBackend` over the built bridge, `ClineAdapter` with
explicit per-role construction contexts, `ClineHubTurnObservationSource`
attached before any enqueue); free pair only
(`cline` / `cline-free/nemotron-3.5-super-vl`); disposable sessions
registered at allocation, stopped before disposal, quiescence verified
through the status-only `run.list` surface (0 active runs); scratch
directories provisioned and removed through the Owner-approved SSH hook;
Box-side existence verified through the Owner-approved operator hook; every
verdict below determined success; no database, no `--testdb`, no paid
inference. Sanitized phase evidence:

```text
preflight/attach        ok (25 history entries; 25 existing runs)
allocate_producer       promptless ACT allocation at the disposable base
observe_producer        exact-session observation subscribed BEFORE enqueue
baseline_turn           attributed start, completed fresh terminal
usage_before            aggregate=cost_zero root=cost_zero (real zero, never unknown)
reconstruct_plan        ACT/base -> PLAN/checkout: verbatim (2 -> 2)
usage_after_plan        aggregate=cost_zero root=cost_zero
effective_plan_turn     attributed write request under PLAN (completed)
effective_plan_constr.  PLAN EFFECTIVELY held: the Box-side marker was NOT
                        written (operator-hook verified) — effective
                        construction demonstrated, never inferred
reconstruct_act         PLAN/checkout -> ACT/checkout: verbatim (4 -> 4)
effective_act_turn      the same write request under ACT (completed)
effective_act_constr.   ACT EFFECTIVELY performed the authorized scratch
                        write: the Box-side marker EXISTS (operator-hook
                        verified)
reconstruct_act_base    ACT/checkout -> ACT/base: verbatim (10 -> 10)
resident_resume         native resume DIRECTLY on the live idle incarnation:
                        observed accepted_idempotent on this baseline (the
                        identity-verified reply resolved; no refusal), the
                        conversation preserved verbatim (10 -> 10), and the
                        incarnation kept serving an attributable turn —
                        no stop was ever issued
stopped_recovery        stop -> record present while stopped -> recover:
                        verbatim (12 -> 12 against the latest pre-stop read)
usage_after_recovery    aggregate=cost_zero root=cost_zero
recovery_continuation   attributable completed turn on the restored
                        incarnation (usable same-ID continuation)
allocate_reviewer       promptless PLAN allocation at the neutral directory
reviewer_baseline_turn  attributed completed terminal
reviewer_plan_recovery  Reviewer PLAN same-ID native recovery: record
                        present while stopped, verbatim (2 -> 2)
cleanup                 both sessions stopped; quiescence 0 active runs;
                        Box scratch removed; probe_complete ok
```

Qualification conclusions:

- **Identity and continuity**: every authorized quiescent incarnation
  replacement (three mode/context transitions, one stopped recovery, one
  Reviewer PLAN recovery) preserved the exact session ID and the complete
  saved conversation verbatim (private in-probe comparisons; counts
  emitted). Continuity is distinguished from an empty/new conversation by
  the verbatim comparison, not the echoed ID. Compaction semantics were not
  separately exercised: the pinned runtime owns compaction and the probe
  sessions stayed under compaction scale — recorded as unproven rather than
  claimed.
- **Effective construction**: demonstrated, not inferred — PLAN actually
  prevented the mutating write and ACT actually performed the authorized
  scratch write at the checkout, each verified Box-side through the
  operator hook. The full ACT base -> PLAN checkout -> ACT checkout -> ACT
  base matrix ran with verbatim continuity across every transition. The
  provider/model input was exercised as the same free pair carried verbatim
  through the native resume; an actual override pair would require
  purchased inference and is recorded as NOT ESTABLISHED (a limitation, not
  a failure — the opaque IDs are forwarded verbatim by the same mapping the
  deterministic tests pin).
- **Stopped vs resident and loss**: the authorized stopped recovery
  proceeded on positive surviving history with a positively absent
  incarnation and restored the exact conversation verbatim. The resident
  behavior on this baseline is the recorded finding below. Absent/unusable
  saved context is established by the pinned handler source (the Hub
  refuses `session_not_found` / `session_messages_not_found`) and by the
  deterministic refusal tests; a live disposable probe of absent context
  was not separately staged.
- **RESIDENT finding (recorded, adapted into the contract)**: on the
  qualified ClineBox baseline, the native resume of a LIVE IDLE incarnation
  is accepted idempotently — no `session_already_exists` refusal is
  observed (that code is the cloud-hub behavior the pinned SDK's internal
  flow anticipates); the identity-verified reply resolved, the conversation
  was preserved verbatim, and the incarnation continued serving. OpenOrc
  never relies on resident refusal: the adapter's reconstruction seam
  always releases the quiescent incarnation through `stop` first, and no
  OpenOrc path invokes the native resume on a live session — so the
  observed idempotent acceptance is a safety property (identity,
  conversation, and service survive even direct misuse), and the
  `session_already_exists` classification stays as defensive mapping for
  hubs that do refuse. A resident resume over an ACTIVE run was
  deliberately not probed: no OpenOrc path can reach it (the D6a guards
  refuse reconstruction while work is outstanding), and probing it would
  risk disturbing in-flight disposable work.
- **Usage**: aggregate and root scopes both read a genuine zero before and
  after every reconstruction/recovery (real evidence, never unknown), with
  the aggregate scope distinguished from root readings; a reset across a
  Hub lifetime remains contractually possible and is recorded as such.
- **Post-resume addressing**: demonstrated — after the identity-verified
  native resume, the observation subscription and subsequent detached
  turns, reads, and the later stop/reconstruction all addressed the
  restored incarnation through the ordinary production backend (the
  qualifying-activity wiring rides the documented pending-prompts
  consultation; the qualifying-pulse attestation is the SDK2c matrix, not
  repeated here).

## Pending: controlled Hub restart (final Owner-run D7 campaign — the single campaign)

The restart evidence for the native production mechanism comes from the ONE
final Owner-run ordered database-backed D7 campaign
(`tests/integration/test_cline_live_runtime.py`, issue #149) against the
completed SDK3 baseline and the qualified Cloud runner — the #404 closure
gate. This change adds focused, deterministic D7 support (never executed by
Cline; `--testdb` is never provisioned or run by Cline):

- D3 now captures, before the controlled restart (both restart modes and
  the two-stage preserved-database handoff), the private conversation
  IDENTITY of the surviving Producer conversation — a canonical digest over
  the supported `read_conversation` projection plus its count, carried
  through the stage-one/stage-two handoff — and after the native recovery
  verifies that the projected conversation EXTENDS the pre-restart
  projection verbatim (prefix identity; growth from the interrupted work's
  effects landing is a legitimate intervening change). Content never leaves
  the process: only counts, digests, and the identity verdict are recorded.
- D3 now establishes EFFECTIVE post-recovery construction inside the
  scenario itself: after the native recovery at the checkout, the restored
  ACT incarnation must actually create the authorized uniquely named scratch
  DIRECTORY inside the checkout, verified Box-side through the existing
  Owner-approved operator hook (`live.box_check`, whose Cloud-runner
  existence check is directory-shaped) — never inferred from a reply, a
  same ID, a transcript count, or the earlier (pre-restart) workflow
  scenarios. The scratch directory is removed by the existing authorized
  workspace cleanup; no separate cleanup exists.
- D3's usage recording now distinguishes the aggregate scope's absence from
  a root-scope reading per scope (`aggregate=<state> root=<state>`), with a
  genuine zero staying zero.
- The existing supported Owner-controlled restart modes (hook, manual
  pause, two-stage resume), the surviving-transcript and
  credential-rotation assertions, and the lifecycle/recovery assertions are
  unchanged; no new stateful suite or duplicate restart matrix.

A separate targeted no-database restart is NOT planned: it would duplicate
the D7 campaign. If post-campaign evidence surfaces a concrete native
behavior uncertainty that only a further controlled restart can resolve,
document the distinct gap and obtain explicit Owner authorization first.

## Owner handoff

- Run the final D7 campaign (Owner-authorized controlled restart) against
  this SDK3 baseline and the qualified Cloud runner; the D3 additions carry
  the surviving conversation identity and effective post-recovery
  construction evidence. Resolve any critical failures.
- Until then: the supported behavior is the static contract, the
  deterministic evidence, and the executed live qualification above; the
  restart evidence is pending, not passed.
