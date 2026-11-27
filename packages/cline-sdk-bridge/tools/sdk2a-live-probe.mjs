/** SDK2a live qualification probe (core issue #407): bounded, sanitized,
 * database-independent transport evidence for the upgraded `@cline/sdk` 0.2.1
 * bridge against the SDK1 reference ClineBox (CLI 3.0.75 / core 0.2.1) through
 * the operator SSH loopback forward.
 *
 * The probe drives the bridge's own production client factory
 * (`dist/sdk-client.js`, the exact code the Python backend launches) plus one
 * direct `NodeHubClient` fault client built with the same construction
 * semantics as the bridge's hub attachment.
 *
 * Inputs (environment, never logged):
 * - OPENORC_SDK2A_TOKEN_FILE  path of the mode-600 file carrying the CURRENT
 *   Hub attach credential (resolved by the trusted operator supplier over
 *   SSH; the file is deleted by the caller afterwards).
 * - OPENORC_SDK2A_ENDPOINT    the admitted Hub WebSocket endpoint (the
 *   loopback forward, e.g. ws://127.0.0.1:25463/hub).
 * - OPENORC_SDK2A_SSH_KEY / OPENORC_SDK2A_KNOWN_HOSTS / OPENORC_SDK2A_HOST /
 *   OPENORC_SDK2A_USER / OPENORC_SDK2A_HUB_PORT  the operator SSH facts used
 *   ONLY to establish and destroy the probe's own ephemeral fault tunnel.
 *
 * Output: one JSON line per phase with sanitized fields only (versions,
 * counts, ids, statuses, event kinds, timings, error codes). Prompts are the
 * fixed harmless probe strings below; credentials, raw SDK errors, payloads,
 * and transcripts are never printed.
 *
 * Inference: the explicitly configured SDK1-verified free pair
 * (provider `cline`, model `cline-free/nemotron-3.5-super-vl`) using the
 * native account auth; no provider secrets are injected and no paid
 * fallback exists in this probe.
 */

import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

const bridgePackageRoot = new URL("..", import.meta.url).pathname;
const { createRealClientFactory, buildHubClientOptions } = await import(
  `${bridgePackageRoot}dist/sdk-client.js`
);

const TOKEN_FILE = process.env.OPENORC_SDK2A_TOKEN_FILE;
const ENDPOINT = process.env.OPENORC_SDK2A_ENDPOINT;
const SSH_KEY = process.env.OPENORC_SDK2A_SSH_KEY;
const KNOWN_HOSTS = process.env.OPENORC_SDK2A_KNOWN_HOSTS;
const SSH_HOST = process.env.OPENORC_SDK2A_HOST;
const SSH_USER = process.env.OPENORC_SDK2A_USER;
const HUB_PORT = process.env.OPENORC_SDK2A_HUB_PORT ?? "25463";
for (const [name, value] of Object.entries({
  OPENORC_SDK2A_TOKEN_FILE: TOKEN_FILE,
  OPENORC_SDK2A_ENDPOINT: ENDPOINT,
  OPENORC_SDK2A_SSH_KEY: SSH_KEY,
  OPENORC_SDK2A_KNOWN_HOSTS: KNOWN_HOSTS,
  OPENORC_SDK2A_HOST: SSH_HOST,
  OPENORC_SDK2A_USER: SSH_USER,
})) {
  if (!value) {
    console.log(JSON.stringify({ phase: "preflight", ok: false, missing: name }));
    process.exit(2);
  }
}

const PROVIDER = "cline";
const MODEL = "cline-free/nemotron-3.5-super-vl";
const CLIENT_IDENTITY = "openorc-core/sdk2a-live";
const PROBE_A = "SDK2a transport probe A: reply with the single word ok";
const PROBE_B = "SDK2a transport probe B: reply with the single word ok";
const EVENT_WINDOW_MS = 90_000;
const FAULT_TUNNEL_PORT = 25464;

function emit(phase, data) {
  console.log(JSON.stringify({ phase, ...data }));
}

function withDeadline(promise, ms, label) {
  return Promise.race([
    promise,
    sleep(ms).then(() => {
      throw new Error(`deadline exceeded: ${label}`);
    }),
  ]);
}

/** Error evidence is reduced to the safe machine shape only. */
function errorCode(error) {
  return typeof error === "object" && error !== null && "code" in error
    ? String(error.code)
    : "unclassified";
}

function startFaultTunnel() {
  return spawn(
    "ssh",
    [
      "-i", SSH_KEY,
      "-o", `UserKnownHostsFile=${KNOWN_HOSTS}`,
      "-o", "ExitOnForwardFailure=yes",
      "-o", "ServerAliveInterval=30",
      "-N",
      "-L", `127.0.0.1:${FAULT_TUNNEL_PORT}:127.0.0.1:${HUB_PORT}`,
      `${SSH_USER}@${SSH_HOST}`,
    ],
    { stdio: "ignore" },
  );
}

async function waitForTcpPort(port, ms) {
  const net = await import("node:net");
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const open = await new Promise((resolve) => {
      const socket = net.connect(port, "127.0.0.1");
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => resolve(false));
    });
    if (open) return true;
    await sleep(200);
  }
  return false;
}

const remoteConfig = {
  endpoint: ENDPOINT,
  client_identity: CLIENT_IDENTITY,
  auth_token: readFileSync(TOKEN_FILE, "utf8").trim(),
};

const factory = createRealClientFactory();
const client = await factory(remoteConfig);

// --- phase 1: authenticated remote attachment -------------------------------
let history;
try {
  history = await withDeadline(client.listHistory(), 30_000, "listHistory");
  emit("attach", { ok: true, historyEntries: Array.isArray(history) ? history.length : null });
} catch (error) {
  emit("attach", { ok: false, code: errorCode(error) });
  process.exit(3);
}

// --- phase 2: quiescence (no unrelated sessions affected) --------------------
let preRuns;
try {
  preRuns = await withDeadline(client.listRuns(null), 30_000, "listRuns/quiescence");
  const runs = Array.isArray(preRuns?.runs) ? preRuns.runs : null;
  emit("quiescence", { ok: runs !== null, existingRuns: runs === null ? null : runs.length });
} catch (error) {
  emit("quiescence", { ok: false, code: errorCode(error) });
  process.exit(3);
}

// --- phase 3: representative promptless allocation (no inference) ------------
// The SDK-shape start input the bridge's `SdkStartInput` mapping produces
// (the wire's snake_case bundle is mapped by `params.ts` before this layer).
const START_BUNDLE = {
  config: {
    providerId: PROVIDER,
    modelId: MODEL,
    mode: "plan",
    rules: "",
    systemPrompt: "",
    cwd: "/home/openorc",
    workspaceRoot: "/home/openorc",
    enableTools: true,
  },
  interactive: true,
  toolPolicies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
};

let session1;
try {
  const started = await withDeadline(client.start({ ...START_BUNDLE }), 30_000, "start/session-1");
  session1 = started.sessionId;
  emit("allocate", { ok: typeof session1 === "string", session_allocated: typeof session1 === "string" });
} catch (error) {
  emit("allocate", { ok: false, code: errorCode(error) });
  process.exit(3);
}

// --- phase 4: run.enqueue acknowledgement ------------------------------------
let ack;
try {
  ack = await withDeadline(client.enqueue({ sessionId: session1, prompt: PROBE_A }), 30_000, "enqueue/ack");
  emit("enqueue_ack", {
    ok: ack !== null && typeof ack === "object",
    has_run_id: ack !== null && typeof ack.runId === "string" && ack.runId.trim().length > 0,
    has_accepted_at: ack !== null && typeof ack.acceptedAt === "number" && Number.isSafeInteger(ack.acceptedAt),
    has_queue_position: ack !== null && typeof ack.queuePosition === "number" && Number.isSafeInteger(ack.queuePosition),
  });
} catch (error) {
  emit("enqueue_ack", { ok: false, code: errorCode(error) });
  process.exit(3);
}

// --- phase 5: native event/status forwarding ---------------------------------
const hubEventKinds = new Set();
const sessionEventKinds = new Set();
let hubEventCount = 0;
let sessionEventCount = 0;
const unsubscribeHub = await client.subscribeHub((event) => {
  hubEventCount += 1;
  if (event && typeof event.event === "string") hubEventKinds.add(event.event);
}, { sessionId: session1 });
const unsubscribeSession = client.subscribe((event) => {
  sessionEventCount += 1;
  if (event && typeof event.type === "string") sessionEventKinds.add(event.type);
}, { sessionId: session1 });

const observed = await (async () => {
  const deadline = Date.now() + EVENT_WINDOW_MS;
  while (Date.now() < deadline) {
    try {
      const runs = await withDeadline(client.listRuns(session1), 15_000, "listRuns/status");
      const runList = Array.isArray(runs?.runs) ? runs.runs : [];
      const statuses = runList
        .map((run) => (run !== null && typeof run === "object" && typeof run.state === "string" ? run.state : "uninterpretable"))
        .filter((status) => status !== "uninterpretable");
      const settled = statuses.length > 0 && statuses.every((s) => s !== "running" && s !== "queued" && s !== "pending");
      if (settled && statuses.length > 0) {
        return { statuses, timedOut: false };
      }
    } catch {
      // status polling never aborts the probe; the bounded window decides
    }
    await sleep(2000);
  }
  return { statuses: null, timedOut: true };
})();
unsubscribeHub();
unsubscribeSession();
emit("event_forwarding", {
  hub_events: hubEventCount,
  hub_event_kinds: [...hubEventKinds].sort(),
  session_events: sessionEventCount,
  session_event_kinds: [...sessionEventKinds].sort(),
  run_statuses: observed.statuses,
  window_timed_out: observed.timedOut,
});

// --- phase 6: disconnect-after-dispatch fault probe ---------------------------
// A disposable second session; the fault client attaches through the probe's
// own ephemeral tunnel, which the probe destroys immediately after the
// command dispatch is observed (requestId assigned, before any reply).
let session2;
try {
  const started = await withDeadline(client.start({ ...START_BUNDLE }), 30_000, "start/session-2");
  session2 = started.sessionId;
  emit("allocate_fault_session", { ok: typeof session2 === "string" });
} catch (error) {
  emit("allocate_fault_session", { ok: false, code: errorCode(error) });
  process.exit(3);
}

const tunnel = startFaultTunnel();
const tunnelReady = await waitForTcpPort(FAULT_TUNNEL_PORT, 15_000);
if (!tunnelReady) {
  emit("fault_probe", { ok: false, reason: "fault tunnel did not open" });
  tunnel.kill();
  process.exit(3);
}

const faultOptions = buildHubClientOptions({
  endpoint: `ws://127.0.0.1:${FAULT_TUNNEL_PORT}/hub`,
  client_identity: CLIENT_IDENTITY,
  auth_token: remoteConfig.auth_token,
});
const { NodeHubClient } = await import("@cline/sdk");
const faultClient = new NodeHubClient(faultOptions);
await faultClient.connect();

let dispatched = false;
const dispatchObserved = new Promise((resolve) => {
  const check = setInterval(() => {
    if (dispatched) {
      clearInterval(check);
      resolve();
    }
  }, 20);
});
const faultCommand = faultClient.command(
  "run.enqueue",
  { prompt: PROBE_B },
  session2,
  { retryAfterDispatch: false, onDispatch: () => { dispatched = true; } },
);
// The rejection may fire as soon as the tunnel dies — attach a handler
// immediately so the transport outcome can never crash the probe as an
// unhandled rejection before the awaited observation below.
faultCommand.catch(() => {});
await withDeadline(dispatchObserved, 10_000, "fault dispatch");
emit("fault_dispatch", { dispatched: true });

// Destroy the probe's own tunnel: transport loss strictly after dispatch.
tunnel.kill();
await sleep(1500);

let faultOutcome;
try {
  await withDeadline(faultCommand, 30_000, "fault command outcome");
  faultOutcome = { rejected: false, code: null };
} catch (error) {
  faultOutcome = { rejected: true, code: errorCode(error) };
}
emit("fault_transport_outcome", faultOutcome);

// Reconnect through the surviving main tunnel and count session-2 runs:
// exactly one admitted run proves no post-dispatch retransmission.
let postFaultRuns = null;
try {
  const runs = await withDeadline(client.listRuns(session2), 30_000, "listRuns/post-fault");
  postFaultRuns = Array.isArray(runs?.runs) ? runs.runs.length : null;
} catch (error) {
  emit("post_fault_status", { ok: false, code: errorCode(error) });
}
emit("fault_probe", { rejected_code: faultOutcome.code, session2_run_count: postFaultRuns });

// --- phase 7: bounded cleanup (disposable sessions) ---------------------------
for (const sessionId of [session1, session2]) {
  try {
    await withDeadline(client.stop(sessionId), 15_000, `stop/${sessionId}`);
  } catch (error) {
    emit("cleanup", { ok: false, code: errorCode(error) });
  }
}
await client.dispose().catch(() => {});
await faultClient.dispose().catch(() => {});
emit("cleanup", { ok: true });