/** Safe failure tests (issue #72 test 6): the D2-relevant failure classes
 * are parameterized against native-shaped SDK errors, a sensitive sentinel
 * planted in native errors never reaches any output frame, stderr, or
 * diagnostic, and wire-protocol failures stay separately recognizable. */

import { describe, expect, it } from "vitest";
import { BACKEND_ERROR_MESSAGE } from "../src/errors.js";
import type { BridgeClientFactory } from "../src/bridge.js";
import { FakeClineCoreClient } from "./helpers/fake-client.js";
import { spawnHarness, until } from "./helpers/spawn.js";
import { errorOf, requestLine, startTestBridge, until as untilFrame } from "./helpers/test-bridge.js";

const CONNECT = { endpoint: "wss://hub.example/remote", client_identity: "openorc-core/test" };

const START = {
  provider_id: "p",
  model_id: "m",
  mode: "plan",
  rules: "",
  system_prompt: "",
  cwd: "/w",
  workspace_root: "/w",
  enable_tools: true,
  interactive: true,
  tool_policies: {},
};

const SENTINEL = "SENSITIVE-SENTINEL-9f2b-never-export";

function errorWithCode(code: string, closeCode?: number): Error {
  const error = new Error(`${SENTINEL}: raw native explanation`);
  (error as { code?: string }).code = code;
  if (closeCode !== undefined) {
    (error as { details?: unknown }).details = { closeCode, closeReason: `${SENTINEL}-close-reason` };
  }
  return error;
}

async function failingOperation(
  method: string,
  nativeError: unknown,
): Promise<ReturnType<typeof startTestBridge>> {
  const fake = new FakeClineCoreClient();
  const bridge = startTestBridge({ clientFactory: async () => fake, writeFrame: () => undefined });
  bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
  await untilFrame(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  fake.script(method, [{ error: nativeError }]);
  if (method === "start") {
    bridge.feed(`${requestLine(2, "start", START)}\n`);
  } else {
    bridge.feed(
      `${requestLine(2, method, {
        session_id: "cline-session-1",
        ...(method === "send" ? { prompt: "go" } : {}),
        ...(method === "abort" ? { reason: "why" } : {}),
      })}\n`,
    );
  }
  await untilFrame(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
  return bridge;
}

function throwingBridge(method: string, nativeError: unknown): ReturnType<typeof startTestBridge> {
  const factory: BridgeClientFactory =
    method === "connect"
      ? async () => {
          throw nativeError;
        }
      : async () => new FakeClineCoreClient();
  return startTestBridge({ clientFactory: factory, writeFrame: () => undefined });
}

describe("safe failure classification", () => {
  const cases: Array<{
    name: string;
    method: string;
    native: unknown;
    kind: string;
    code?: string;
    closeCode?: number;
  }> = [
    {
      name: "coded hub transport rejection keeps its safe code and bounded close_code",
      method: "connect",
      native: errorWithCode("hub_connection_closed", 1006),
      kind: "sdk_operation_rejected",
      code: "hub_connection_closed",
      closeCode: 1006,
    },
    {
      name: "connect failure with a bare hub_connect_failed is preserved without interpretation",
      method: "connect",
      native: errorWithCode("hub_connect_failed"),
      kind: "sdk_operation_rejected",
      code: "hub_connect_failed",
    },
    {
      name: "node system codes classify connect as unavailable without forwarding the code",
      method: "connect",
      native: errorWithCode("ECONNREFUSED"),
      kind: "remote_attachment_unavailable",
    },
    {
      name: "connect input rejection maps to attachment_rejected",
      method: "connect",
      native: new TypeError(`${SENTINEL}: bad remote options`),
      kind: "remote_attachment_rejected",
    },
    {
      name: "unclassified connect failure maps to attachment_unavailable",
      method: "connect",
      native: new Error(`${SENTINEL}: unexplained`),
      kind: "remote_attachment_unavailable",
    },
    {
      name: "positively identified session absence maps to session_not_found",
      method: "send",
      native: errorWithCode("session_not_found"),
      kind: "session_not_found",
    },
    {
      name: "unclassified in-flight send failure stays uncertain",
      method: "send",
      native: new Error(`${SENTINEL}: connection dropped mid-turn`),
      kind: "uncertain_outcome",
    },
    {
      name: "coded SDK rejections on operations keep their safe machine code",
      method: "get",
      native: errorWithCode("command_failed"),
      kind: "sdk_operation_rejected",
      code: "command_failed",
    },
    {
      name: "system-coded read failures stay uncertain rather than unavailable",
      method: "get",
      native: errorWithCode("ECONNRESET"),
      kind: "uncertain_outcome",
    },
  ];

  for (const testCase of cases) {
    it(`${testCase.name} (${testCase.method})`, async () => {
      const throwing = throwingBridge(testCase.method, testCase.native);
      if (testCase.method === "connect") {
        throwing.feed(`${requestLine(1, "connect", CONNECT)}\n`);
        await untilFrame(() => (throwing.frames.some((frame) => frame["id"] === 1) ? true : undefined));
        const error = errorOf(throwing.frames, 1);
        expect(error.message).toBe(BACKEND_ERROR_MESSAGE);
        expect(error.data.kind).toBe(testCase.kind);
        if (testCase.code !== undefined) {
          expect(error.data.code).toBe(testCase.code);
        } else {
          expect(error.data.code).toBeUndefined();
        }
        if (testCase.closeCode !== undefined) {
          expect(error.data.close_code).toBe(testCase.closeCode);
        } else {
          expect(error.data.close_code).toBeUndefined();
        }
        expect(JSON.stringify(throwing.frames)).not.toContain(SENTINEL);
      } else {
        const bridge = await failingOperation(testCase.method, testCase.native);
        const error = errorOf(bridge.frames, 2);
        expect(error.message).toBe(BACKEND_ERROR_MESSAGE);
        expect(error.data.kind).toBe(testCase.kind);
        if (testCase.code !== undefined) {
          expect(error.data.code).toBe(testCase.code);
        }
        expect(JSON.stringify(bridge.frames)).not.toContain(SENTINEL);
      }
    });
  }
});

describe("wire errors stay separately recognizable", () => {
  it("reports not-attached and invalid frames as wire-protocol errors", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = startTestBridge({ clientFactory: async () => fake, writeFrame: () => undefined });
    bridge.feed(`${requestLine(1, "start", START)}\n`);
    await untilFrame(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
    const notAttached = errorOf(bridge.frames, 1);
    expect(notAttached.data).toEqual({ kind: "wire_protocol", reason: "not_attached" });
    expect(notAttached.code).toBe(-32602);

    bridge.feed(`${"definitely not json"}\n`);
    await untilFrame(() => (bridge.frames.length === 2 ? true : undefined));
    const parseError = bridge.frames[1] as { id: unknown; error: { code: number; data: { kind: string } } };
    expect(parseError.error.code).toBe(-32700);
    expect(parseError.error.data.kind).toBe("wire_protocol");
  });

  it("never leaks a sensitive sentinel from native errors into frames or stderr", async () => {
    const child = await spawnHarness({
      outcomes: {
        connect: [{ ok: null }],
        start: [{ ok: { sessionId: "s1" } }],
        send: [{ error: { code: "hub_connection_closed", closeCode: 1006, sentinel: SENTINEL } }],
      },
    });
    const childResponseFor = (id: number): { result?: unknown; error?: unknown } | undefined =>
      child.stdoutFrames.find(
        (frame) => frame["id"] === id && (frame["result"] !== undefined || frame["error"] !== undefined),
      ) as { result?: unknown; error?: unknown } | undefined;
    try {
      child.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "connect", params: CONNECT })}\n`);
      await until(() => childResponseFor(1));
      child.write(`${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "start", params: START })}\n`);
      await until(() => childResponseFor(2));
      child.write(`${JSON.stringify({ jsonrpc: "2.0", id: 3, method: "send", params: { session_id: "s1", prompt: "go" } })}\n`);
      await until(() => childResponseFor(3));
      const error = childResponseFor(3)?.error as {
        code: number;
        message: string;
        data: { kind: string; code?: string; close_code?: number };
      };
      expect(error.message).toBe(BACKEND_ERROR_MESSAGE);
      expect(error.data).toEqual({ kind: "sdk_operation_rejected", code: "hub_connection_closed", close_code: 1006 });
      expect(child.stdoutLines.join("\n")).not.toContain(SENTINEL);
      expect(child.stderr).not.toContain(SENTINEL);
    } finally {
      await child.cleanup();
    }
  });
});