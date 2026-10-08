#!/usr/bin/env node
/** Subprocess test harness: runs the BUILT bridge core (`dist/bridge.js`,
 * SDK-free) over real stdio with a scenario-driven fake client, so framing,
 * framing robustness, and safe-failure behavior are exercised through actual
 * child processes without any live SDK, Hub, or network.
 *
 * Scenario inputs (test-only):
 * - `OPENORC_BRIDGE_FAKE_SCRIPT` — JSON path: `{ outcomes: {<method>:
 *   [{ok}|{error:{code?,closeCode?,sentinel?},delayMs?}]...}, events:
 *   [{afterSubscribe, sessionId, kind, payload, delayMs?}] }`.
 * - `OPENORC_BRIDGE_FAKE_RECORD` — JSONL path recording every client call
 *   (exact args) for parent assertions.
 */

import { BridgeCore } from "../dist/bridge.js";
import { appendFileSync, readFileSync } from "node:fs";

const script = JSON.parse(readFileSync(process.env.OPENORC_BRIDGE_FAKE_SCRIPT, "utf8"));
const recordPath = process.env.OPENORC_BRIDGE_FAKE_RECORD ?? null;

function record(call) {
  if (recordPath !== null) {
    appendFileSync(recordPath, `${JSON.stringify(call)}\n`);
  }
}

function nextOutcome(method) {
  const queue = script["outcomes"]?.[method];
  if (!Array.isArray(queue) || queue.length === 0) {
    return undefined;
  }
  return queue.shift();
}

function nativeError(spec) {
  const error = new Error(spec.sentinel ?? "scripted native failure");
  if (spec.code !== undefined) {
    error.code = spec.code;
  }
  if (spec.closeCode !== undefined) {
    error.details = { closeCode: spec.closeCode, closeReason: spec.closeReason ?? "closed" };
  }
  return error;
}

function notFound(sessionId) {
  const error = new Error(`session not found: ${sessionId}`);
  error.code = "session_not_found";
  return error;
}

async function apply(method, fallback) {
  const outcome = nextOutcome(method);
  if (outcome === undefined) {
    return fallback();
  }
  if (outcome.delayMs !== undefined) {
    await new Promise((resolve) => {
      setTimeout(resolve, outcome.delayMs);
    });
  }
  if (outcome.error !== undefined) {
    throw nativeError(outcome.error);
  }
  return outcome.ok;
}

const sessions = new Map();
let sessionCounter = 0;
const subscriptions = [];
let subscribeCount = 0;

function scheduleScriptedEvents() {
  subscribeCount += 1;
  for (const event of script["events"] ?? []) {
    if (event["afterSubscribe"] !== subscribeCount) {
      continue;
    }
    setTimeout(() => {
      for (const entry of subscriptions) {
        if (!entry.active) {
          continue;
        }
        if (entry.filter !== null && entry.filter !== event["sessionId"]) {
          continue;
        }
        entry.listener({ type: event["kind"], payload: event["payload"] });
      }
    }, event["delayMs"] ?? 10);
  }
}

const fake = {
  async start(input) {
    record({ method: "start", args: input });
    return await apply("start", () => {
      const sessionId = input.config.sessionId ?? `cline-session-${++sessionCounter}`;
      sessions.set(sessionId, input.initialMessages === undefined ? [] : structuredClone(input.initialMessages));
      return { sessionId };
    });
  },
  async send(input) {
    record({ method: "send", args: input });
    return await apply("send", () => {
      if (!sessions.has(input.sessionId)) {
        throw notFound(input.sessionId);
      }
      return undefined;
    });
  },
  async stop(sessionId) {
    record({ method: "stop", args: { sessionId } });
    return await apply("stop", () => {
      if (!sessions.has(sessionId)) {
        throw notFound(sessionId);
      }
      return undefined;
    });
  },
  async abort(sessionId, reason) {
    record({ method: "abort", args: { sessionId, reason: reason === undefined ? null : reason } });
    return await apply("abort", () => {
      if (!sessions.has(sessionId)) {
        throw notFound(sessionId);
      }
      return undefined;
    });
  },
  async get(sessionId) {
    record({ method: "get", args: { sessionId } });
    return await apply("get", () => {
      if (!sessions.has(sessionId)) {
        throw notFound(sessionId);
      }
      return undefined;
    });
  },
  async readMessages(sessionId) {
    record({ method: "read_messages", args: { sessionId } });
    return await apply("read_messages", () => {
      if (!sessions.has(sessionId)) {
        throw notFound(sessionId);
      }
      return structuredClone(sessions.get(sessionId) ?? []);
    });
  },
  async listHistory() {
    record({ method: "list_history", args: {} });
    return await apply("list_history", () => []);
  },
  async getAccumulatedUsage(sessionId) {
    record({ method: "get_accumulated_usage", args: { sessionId } });
    return await apply("get_accumulated_usage", () => {
      if (!sessions.has(sessionId)) {
        throw notFound(sessionId);
      }
      return undefined;
    });
  },
  subscribe(listener, options) {
    record({ method: "subscribe", args: { filter: options?.sessionId ?? null } });
    const entry = { filter: options?.sessionId ?? null, listener, active: true };
    subscriptions.push(entry);
    scheduleScriptedEvents();
    return () => {
      entry.active = false;
    };
  },
  async dispose() {
    record({ method: "dispose", args: {} });
    for (const entry of subscriptions) {
      entry.active = false;
    }
    return undefined;
  },
};

const factory = async (remote) => {
  record({ method: "factory", args: remote });
  const outcome = nextOutcome("connect");
  if (outcome === undefined) {
    return fake;
  }
  if (outcome.delayMs !== undefined) {
    await new Promise((resolve) => {
      setTimeout(resolve, outcome.delayMs);
    });
  }
  if (outcome.error !== undefined) {
    throw nativeError(outcome.error);
  }
  return fake;
};

const core = new BridgeCore({
  clientFactory: factory,
  writeFrame: (frame) => {
    process.stdout.write(`${JSON.stringify(frame)}\n`);
  },
});

process.stdin.on("data", (chunk) => {
  core.handleData(chunk);
});
process.stdin.on("end", () => {
  core.handleEnd();
  process.exit(0);
});