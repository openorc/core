/** Wire/launch tests over a real spawned bridge subprocess (issue #72 test 1):
 * multiple newline-delimited JSON-RPC calls on one process (including split
 * and coalesced lines), correct IDs/results, protocol-only stdout, and
 * bounded payload-free errors for malformed/unknown input. */

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawnHarness, until, type SpawnedBridge } from "./helpers/spawn.js";

const CONNECT = { endpoint: "wss://hub.example/remote", client_identity: "openorc-core/test" };

const START = {
  provider_id: "provider-a",
  model_id: "model-a",
  mode: "plan",
  rules: "# Role rules",
  system_prompt: "",
  cwd: "/workspace",
  workspace_root: "/workspace",
  enable_tools: true,
  interactive: true,
  tool_policies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
};

let bridge: SpawnedBridge;

beforeEach(async () => {
  bridge = await spawnHarness({
    outcomes: {
      connect: [{ ok: null }],
      start: [{ ok: { sessionId: "s1" } }, { ok: { sessionId: "s1" } }],
      get: [{ ok: { sessionId: "s1", status: "active" } }],
      get_accumulated_usage: [{ ok: null }],
    },
  });
});

afterEach(async () => {
  await bridge.cleanup();
});

function responseFor(id: string | number): { result?: unknown; error?: unknown } | undefined {
  return bridge.stdoutFrames.find(
    (frame) => frame["id"] === id && (frame["result"] !== undefined || frame["error"] !== undefined),
  ) as { result?: unknown; error?: unknown } | undefined;
}

describe("wire framing over a spawned bridge subprocess", () => {
  it("answers coalesced and split newline-delimited calls with correct ids and results", async () => {
    const connect = `${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "connect", params: CONNECT })}\n`;
    const start = JSON.stringify({ jsonrpc: "2.0", id: "alpha", method: "start", params: START });
    const get = `${JSON.stringify({ jsonrpc: "2.0", id: 7, method: "get", params: { session_id: "s1" } })}\n`;

    bridge.write(connect);
    await until(() => responseFor(1));
    expect(responseFor(1)?.result).toBeNull();

    // The start request is split across two writes; get is coalesced behind it.
    bridge.write(start.slice(0, 30));
    bridge.write(`${start.slice(30)}\n${get}`);

    await until(() => responseFor(7));
    expect(responseFor("alpha")?.result).toEqual({ session_id: "s1", result: null });
    expect(responseFor(7)?.result).toEqual({ sessionId: "s1", status: "active" });

    for (const line of bridge.stdoutLines) {
      const frame = JSON.parse(line) as Record<string, unknown>;
      expect(frame["jsonrpc"]).toBe("2.0");
      expect(
        frame["result"] !== undefined || frame["error"] !== undefined || typeof frame["method"] === "string",
      ).toBe(true);
    }
  });

  it("keeps stdout protocol-only across many calls on one process", async () => {
    bridge.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "connect", params: CONNECT })}\n`);
    await until(() => responseFor(1));
    bridge.write(
      [
        JSON.stringify({ jsonrpc: "2.0", id: 2, method: "list_history" }),
        JSON.stringify({ jsonrpc: "2.0", id: 3, method: "get_accumulated_usage", params: { session_id: "s1" } }),
      ].join("\n") + "\n",
    );
    await until(() => responseFor(3));
    expect(responseFor(2)?.result).toEqual([]);
    expect(responseFor(3)?.result).toBeNull();
    expect(bridge.stdoutLines.length).toBe(3);
    for (const line of bridge.stdoutLines) {
      expect(() => JSON.parse(line)).not.toThrow();
    }
  });

  it("answers malformed and unknown input with bounded JSON-RPC errors and never echoes it", async () => {
    const sentinel = "PAYLOAD-SENTINEL-do-not-echo";
    bridge.write(
      [
        JSON.stringify({ jsonrpc: "2.0", id: 1, method: "definitely_not_a_method" }),
        `${sentinel} not json`,
        JSON.stringify(["a", "batch"]),
        JSON.stringify({ jsonrpc: "1.0", id: 4, method: "connect" }),
        JSON.stringify({ jsonrpc: "2.0", method: "connect" }),
      ]
        .map((line) => `${line}\n`)
        .join(""),
    );
    await until(() => (bridge.stdoutFrames.length >= 4 ? true : undefined));

    const methodError = bridge.stdoutFrames.find((frame) => frame["id"] === 1) as {
      error: { code: number; message: string };
    };
    expect(methodError.error.code).toBe(-32601);
    expect(methodError.error.message).toBe("method not found");

    const parseError = bridge.stdoutFrames.find(
      (frame) => frame["id"] === null && (frame["error"] as { code?: number })?.code === -32700,
    ) as { error: { code: number; message: string } };
    expect(parseError.error.message).toBe("parse error");
    expect(JSON.stringify(parseError)).not.toContain(sentinel);

    const batchError = bridge.stdoutFrames.find(
      (frame) => frame["id"] === null && (frame["error"] as { code?: number })?.code === -32600,
    ) as { error: { code: number } };
    expect(batchError.error.code).toBe(-32600);

    const batchAndVersionErrors = bridge.stdoutFrames.filter(
      (frame) => frame["id"] === null && (frame["error"] as { code?: number })?.code === -32600,
    );
    expect(batchAndVersionErrors.length).toBe(2);

    // The inbound notification (no id) is consumed without any response.
    expect(bridge.stdoutFrames.length).toBe(4);
    expect(bridge.stderr).not.toContain(sentinel);
  });

  it("distinguishes wire-protocol errors for invalid params and not-attached state", async () => {
    bridge.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "start", params: START })}\n`);
    await until(() => responseFor(1));
    const notAttached = responseFor(1)?.error as { code: number; data: { kind: string; reason: string } };
    expect(notAttached.code).toBe(-32602);
    expect(notAttached.data).toEqual({ kind: "wire_protocol", reason: "not_attached" });

    bridge.write(`${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "connect", params: CONNECT })}\n`);
    await until(() => responseFor(2));
    expect(responseFor(2)?.result).toBeNull();

    bridge.write(
      `${JSON.stringify({ jsonrpc: "2.0", id: 3, method: "start", params: { ...START, mode: "yolo" } })}\n`,
    );
    await until(() => responseFor(3));
    expect((responseFor(3)?.error as { code: number; data: { reason: string } }).code).toBe(-32602);
    expect((responseFor(3)?.error as { data: { reason: string } }).data.reason).toBe("invalid_params");

    bridge.write(`${JSON.stringify({ jsonrpc: "2.0", id: 4, method: "start", params: { mode: "plan" } })}\n`);
    await until(() => responseFor(4));
    expect((responseFor(4)?.error as { data: { reason: string } }).data.reason).toBe("invalid_params");
  });

  it("completes out-of-order responses while preserving request ids", async () => {
    const slow = await spawnHarness({
      outcomes: {
        connect: [{ ok: null }],
        start: [{ ok: { sessionId: "s1" } }],
        send: [{ delayMs: 400, ok: { text: "done" } }],
        get: [{ ok: { sessionId: "s1" } }],
      },
    });
    const slowResponseFor = (id: string | number): { result?: unknown; error?: unknown } | undefined =>
      slow.stdoutFrames.find(
        (frame) => frame["id"] === id && (frame["result"] !== undefined || frame["error"] !== undefined),
      ) as { result?: unknown; error?: unknown } | undefined;
    try {
      slow.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "connect", params: CONNECT })}\n`);
      await until(() => slowResponseFor(1));
      slow.write(
        [JSON.stringify({ jsonrpc: "2.0", id: 2, method: "start", params: { ...START, mode: "act" } })].join("\n") + "\n",
      );
      await until(() => slowResponseFor(2));
      // send parks for 400ms while get completes immediately on the same process.
      slow.write(
        [
          JSON.stringify({ jsonrpc: "2.0", id: 3, method: "send", params: { session_id: "s1", prompt: "work" } }),
          JSON.stringify({ jsonrpc: "2.0", id: 4, method: "get", params: { session_id: "s1" } }),
        ].join("\n") + "\n",
      );
      await until(() => (slowResponseFor(3) !== undefined && slowResponseFor(4) !== undefined ? true : undefined));
      const indexOf = (id: string | number): number =>
        slow.stdoutFrames.findIndex(
          (frame) => frame["id"] === id && (frame["result"] !== undefined || frame["error"] !== undefined),
        );
      expect(indexOf(4)).toBeLessThan(indexOf(3));
      expect(slowResponseFor(4)?.result).toEqual({ sessionId: "s1" });
      expect(slowResponseFor(3)?.result).toEqual({ text: "done" });
      expect(slowResponseFor(2)?.result).toEqual({ session_id: "s1", result: null });
    } finally {
      await slow.cleanup();
    }
  });
});