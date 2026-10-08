/** Real built-entrypoint smoke tests (issue #72 acceptance): `dist/cli.js`
 * loads the pinned SDK, keeps stdout protocol-only, answers bounded wire
 * errors without any attach, and exits 0 at end of input. */

import { afterEach, describe, expect, it } from "vitest";
import { spawnRealEntrypoint, until, type SpawnedBridge } from "./helpers/spawn.js";

let bridge: SpawnedBridge | undefined;

afterEach(async () => {
  await bridge?.cleanup();
  bridge = undefined;
});

describe("built stdio entrypoint (dist/cli.js)", () => {
  it("answers unknown methods and parse errors without a live Hub", async () => {
    bridge = await spawnRealEntrypoint();
    bridge.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "nope" })}\n`);
    bridge.write("definitely not json\n");
    await until(() => (bridge?.stdoutFrames.length === 2 ? true : undefined));
    const methodError = bridge.stdoutFrames.find((frame) => frame["id"] === 1) as {
      error: { code: number; message: string };
    };
    expect(methodError.error.code).toBe(-32601);
    expect(methodError.error.message).toBe("method not found");
    const parseError = bridge.stdoutFrames.find(
      (frame) => frame["id"] === null && (frame["error"] as { code?: number })?.code === -32700,
    ) as { error: { code: number; message: string } };
    expect(parseError.error.message).toBe("parse error");
    for (const line of bridge.stdoutLines) {
      const frame = JSON.parse(line) as Record<string, unknown>;
      expect(frame["jsonrpc"]).toBe("2.0");
    }
  });

  it("reports not-attached as a wire error before any SDK attach", async () => {
    bridge = await spawnRealEntrypoint();
    bridge.write(
      `${JSON.stringify({ jsonrpc: "2.0", id: 5, method: "send", params: { session_id: "s1", prompt: "hi" } })}\n`,
    );
    await until(() => (bridge?.stdoutFrames.length === 1 ? true : undefined));
    const error = bridge.stdoutFrames[0] as { id: unknown; error: { code: number; message: string; data: { kind: string; reason: string } } };
    expect(error.error.code).toBe(-32602);
    expect(error.error.data).toEqual({ kind: "wire_protocol", reason: "not_attached" });
    expect(error.error.message).toBe("invalid params");
  });

  it("exits 0 on end of input", async () => {
    bridge = await spawnRealEntrypoint();
    await bridge.end();
    const code = await bridge.exited;
    expect(code).toBe(0);
  });
});