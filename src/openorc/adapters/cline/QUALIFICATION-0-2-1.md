# SDK2c qualification — ClineAdapter on `@cline/sdk` 0.2.1 (core issue #413)

Status: **qualified** for the SDK2c scope — the existing `ClineAdapter` and
detached-turn observation integration are compatible with the SDK2a-qualified
0.2.1 bridge and the SDK1 reference CLI 3.0.75 / Core+SDK 0.2.1 environment,
with one demonstrated adaptation (the abort-terminated run event, below).
Bridge transport qualification is NOT repeated here (see
`packages/cline-sdk-bridge/QUALIFICATION-0-2-1.md`, core PR #414); Cloud
runner qualification is openorc/cloud#64. Evidence recorded 2026-11-27.

Environment: the existing isolated SDK1 reference ClineBox
(`openorc-cline-runtime-51c919e6`, Droplet 623822957) through the operator
SSH loopback forward, the trusted current credential supplier
(`cline hub ensure --json` over trusted SSH; the current token never left a
mode-600 ephemeral file, deleted after use), native account auth, and the
SDK2b-verified free pair (`cline` / `cline-free/nemotron-3.5-super-vl`) as
the only inference. Disposable probe sessions only; no Hub restart, no
machine-global change, no provider secret injection, no paid inference. The
probe tool is committed at
`packages/cline-sdk-bridge/tools/sdk2c-adapter-live-probe.py` (sanitized JSON
phases) so that the qualification record distinguishes what was demonstrated
from what remains pending. The probe drives the production Python path —
`ClineBridgeProcessBackend` over the built bridge, `ClineAdapter` with
explicit per-role construction contexts, and `ClineHubTurnObservationSource`
— never a fake backend, never a database, never `--testdb`.

Discipline: every required verdict (attribution, ordering freshness, terminal
outcome, formal validation, settlement, transcript preservation, cleanup
quiescence) determines probe success — a wrong or missing verdict fails the
run instead of being merely printed. Both the interrupted turn and the
authorized continuation are enqueued through `admit_detached_turn` (the
continuation content is the shared #343 fixed `RESUMPTION_INSTRUCTION`), and
every admitted reference is classified against independently observed start
and terminal evidence with the session's consumed markers chained across
turns. Every allocated session is registered for bounded best-effort stop at
allocation; stops run before attachment disposal with each outcome emitted,
and the success path verifies quiescence through the status-only `run.list`
surface before disposal.

## Demonstrated difference and adaptation: the abort-terminated run event

Live observation (single-subscription diagnostic during this qualification,
sanitized; durable `run.list` corroboration): an interrupted detached run on
the qualified trio terminates with the Hub event **`run.aborted`** — not
`run.completed`/`run.failed`. The envelope carries its own session identity
and durable-log sequence, its payload keys are `reason` / `result` /
`snapshot`, and its `result.finishReason` is exactly the pinned interruption
marker `aborted`. The durable run-list record's state token is `aborted`.

The 0.0.90-qualified terminal-event recognition set (`run.completed` /
`run.failed`) did not recognize `run.aborted`, so the production observation
path delivered no terminal evidence for an interrupted detached turn — the
first live probe run timed out waiting for a terminal after an acknowledged
abort while the durable record already read `aborted` — and the #342
positive interrupted-turn evidence was unreachable from the event stream.

Adaptation (entirely inside the concrete Cline integration):
`hub_projection._TERMINAL_EVENT_KINDS` recognizes `run.aborted`, mapped onto
the neutral vocabulary's failure encoding with its `result` read exactly
like the other terminal events — the pinned interruption finish reason stays
the ONLY positive interrupted evidence, an aborted envelope without it
classifies as an observed failure (never a success, never an invented
verdict), and the shared seam's interrupted routing already precedes every
family routing, so no shared application/domain/persistence change is made.
Focused projection regressions pin both shapes; the live probe re-run then
delivered the interrupted terminal on the first attempt and the full
settlement/resume path passed.

## Live probe evidence (sanitized probe phases, observed 2026-11-27)

```text
preflight/attach       ok (history_entries 20; existing_runs 12)
allocate_reviewer      promptless allocation ok (independent per-role context)
observe_reviewer       exact-session observation subscribed BEFORE the enqueue
enqueue_initialization verified ack carried the turn reference
initialization_turn    run.started attributed to the outstanding reference
                       (pre-enqueue subscription); terminal completed, fresh
                       ordering; session_ready validated through the shared
                       protocol funnel
status_consultation    completion_without_result_evidence (status alone is
                       never success or result evidence)
second_turn            idle-separated admitted turn: attributed start, fresh
                       terminal, result present (sequence/replay verdict)
reviewer_stopped       stopped incarnation with the persisted record still
                       present — stopped is never session loss
allocate_producer      promptless allocation ok (ACT at the probe base)
interrupt_turn_1       start attributed to the admitted reference, abort
                       acknowledged, terminal observed promptly and fresh
                       against the chained consumed markers: outcome failed,
                       interrupted TRUE (positive interrupted terminal
                       evidence, not the ack)
settle_abort           settled (positive terminal evidence attested)
resume_activity        the #343 fixed authorized resume instruction enqueued
                       through the production detached admission seam
                       (admit_detached_turn): the admitted reference was
                       verified against the attributed start, and the
                       resumed turn completed fresh on the same session over
                       the retained conversation
usage_attestation      aggregate_total_usd observed 0 (the free pair reports
                       totalCost = 0 — real evidence, never unknown);
                       qualifying model-backed activity pulses observed (32)
reconstruction         authorized context transition (ACT→PLAN) through the
                       serialized read_messages → stop → start seam; exact
                       session identity; the transcript compared privately
                       before/after and preserved verbatim. The effective
                       construction behavior of a mode transition is
                       attributed to the Owner-run D7 suite (below), not
                       claimed from this probe
stopped_recovery       record present while stopped; exact stopped-session
                       recovery proceeded through the same-ID start with the
                       transcript preserved verbatim (private comparison)
cleanup_session        both allocated sessions registered at allocation and
                       stopped before attachment disposal (outcomes emitted)
cleanup_quiescence     the probe's own sessions verified quiescent through
                       the status-only run.list surface before disposal
                       (0 active runs)
probe_complete         ok; all probe sessions stopped, attachments disposed
```

Observed zero-cost usage: the qualified free pair reports `totalCost = 0`.
The adapter preserves an observed zero as real evidence in both aggregation
scopes (never an unknown placeholder, never substituted across scopes), the
consultation returns the exact decimal zero, and the cost service accepts it
as a truthful non-negative reading. Representative zero-cost regressions were
added (snapshot mapping + observation consultation) — none existed before.

## Static findings (pinned 0.2.1 declaration analysis)

Every shape the adapter integration consumes is unchanged from the 0.0.90
qualification:

- `AgentResult` (`finishReason: AgentFinishReason`, reply `text`), and the
  `AgentFinishReason` union (`completed` / `max_iterations` / `aborted` /
  `mistake_limit` / `error`) — `_extract_reply_candidate`,
  `SDK_INTERRUPTED_FINISH_REASON`, and `ClineTurnFinishReason` are unchanged.
- `SessionUsageSummary` (`usage?` / `aggregateUsage?`) and
  `SessionAccumulatedUsage` (`inputTokens` / `outputTokens` /
  `cacheReadTokens` / `cacheWriteTokens` / `totalCost`) —
  `project_usage_summary` and `aggregate_usage_cost_usd` are unchanged.
- `AgentEvent` union and `AgentContentType` (`text` / `reasoning` / `media` /
  `tool`; `content_update` remains tool-only), the `AgentUsageEvent` fields
  (including `totalCost?`), and the `AgentNoticeEvent` notice types and
  reason vocabulary (exactly the pinned `_NOTICE_REASONS`) — the D6c
  projection allowlist is unchanged.
- `CoreSessionEvent` still carries `chunk` / `agent_event` /
  `session_snapshot` / `ended` / `status`; 0.2.1 ADDS `team_progress` /
  `pending_prompts` / `pending_prompt_submitted` / `hook` kinds. These are
  deliberately unprojected: the allowlist already yields no observation for
  unknown kinds, so no adapter change is required or made.
- `HubEventEnvelope` (`event`, `sequence?`, `sessionId?`, `payload?`) and the
  `HubEventName` union still include `run.started` / `run.completed` /
  `run.failed` (plus further non-terminal kinds that stay unqualified);
  envelope-owned session identity and durable-log sequence semantics are
  unchanged.
- `CoreRuntimeFeatures` still marks `enableSpawnAgent`/`enableAgentTeams` as
  required booleans (the bridge's no-synthesis start cast remains), and the
  SDK root declaration still does not re-export `NodeHubClient` /
  `normalizeHubWebSocketUrl` through NodeNext (the boundary cast remains).

## Observed tooling notes (recorded, not worked around)

- A parallel hub subscription on one attachment under-delivers: during the
  first probe run a parallel raw-kind tap missed `run.completed` while the
  production observation source delivered and classified that terminal. The
  committed probe therefore uses only the production observation path; the
  raw-kind inventory for the `run.aborted` finding came from a temporary
  single-subscription diagnostic (not committed).
- An abort dispatched after the turn had already completed produced no event
  and left the completed state — an abort acknowledgement alone is never
  terminal evidence, consistent with the #342 discipline.

## Retained behavior and limitations

Unchanged from the SDK2a bridge qualification: remote-only attachment (the
bridge never registers a local-Hub endpoint), explicit
`retryAfterDispatch: false` for the consequential `run.enqueue` admission
(read-only `run.list` keeps the SDK default), no suppression option at the
`ClineCore.start/send/stop/abort` public surface (the never-remembered-
endpoint gating plus observed non-retransmission own that guarantee),
`hub_command_timeout` default 8s with unchanged uncertain-outcome semantics,
and the existing uncertainty/no-replay/stale guards. Any future SDK change
to that gating is a compatibility signal requiring re-qualification.

## Scope boundary: demonstrated versus pending

Demonstrated by this qualification (see the evidence above): the detached
allocation/admission/observation/attribution mechanics with chained ordering
verdicts; validated `session_ready` from the attributable terminal; the
status-only consultation semantics; the abort-terminated run event
adaptation with positive interrupted evidence and settlement; the detached
Resume admission completing on the same session; verbatim transcript
preservation across same-ID reconstruction and stopped-session recovery;
zero-cost usage evidence and content-free activity attestation; bounded
session cleanup with verified quiescence.

Attributed to the Owner-run database-backed D7 suite
(`tests/integration/test_cline_live_runtime.py`), not demonstrated by this
probe: the effective construction behavior of a mode transition inside the
full workflow scenarios, and the complete D7 stateful scenario matrix —
executed by the Owner with the deliberate compatible Core checkout and the
qualified Cloud runner (openorc/cloud#64); Cline never provisions or
executes `--testdb`.

Outstanding and pending Owner disposition or execution:

- The usage reset/continuity and exact stopped-session recovery across an
  Owner-authorized controlled Hub restart remains an OUTSTANDING
  qualification requirement: it requires Owner authorization and quiescence
  and was not executed in this session (the recovery seam itself was
  exercised against a stopped session without a restart, above). Its
  evidence must be recorded before issue closure unless the Owner explicitly
  dispositions the outstanding requirement.
- No interactive authentication was needed (the native account auth from
  SDK1 persisted; the trusted supplier resolved the current credential). No
  paid inference was used or authorized.
