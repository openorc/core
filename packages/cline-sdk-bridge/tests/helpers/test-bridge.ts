/** In-process bridge test harness: collects outbound frames synchronously,
 * feeds inbound lines/chunks, and polls deterministically for expected
 * frames. No network, no real SDK, no sleeps beyond bounded polling. */

import { BridgeCore } from "../../src/bridge.js";
import type { BridgeClientFactory, BridgeCoreOptions } from "../../src/bridge.js";
import type { JsonObject } from "../../src/types.js";

export interface TestBridge {
  core: BridgeCore;
  /** Every outbound JSON-RPC frame, in write order. */
  frames: JsonObject[];
  /** Feed one or more raw text lines (with or without trailing newline). */
  feed(text: string): void;
  /** Flush a trailing partial line (end of input). */
  end(): void;
}

export function startTestBridge(options: BridgeCoreOptions): TestBridge {
  const frames: JsonObject[] = [];
  const core = new BridgeCore({
    clientFactory: options.clientFactory,
    writeFrame: (frame) => {
      frames.push(frame);
    },
  });
  return {
    core,
    frames,
    feed: (text: string) => {
      core.handleData(Buffer.from(text, "utf8"));
    },
    end: () => {
      core.handleEnd();
    },
  };
}

export function requestLine(id: string | number, method: string, params?: unknown): string {
  return JSON.stringify(params === undefined ? { jsonrpc: "2.0", id, method } : { jsonrpc: "2.0", id, method, params });
}

export function responseOf(frames: JsonObject[], id: string | number): { result?: unknown; error?: unknown } {
  const frame = frames.find((candidate) => candidate["id"] === id && (candidate["result"] !== undefined || candidate["error"] !== undefined));
  if (frame === undefined) {
    throw new Error(`no response frame for id ${String(id)}`);
  }
  return { result: frame["result"], error: frame["error"] };
}

export function errorOf(frames: JsonObject[], id: string | number): { code: number; message: string; data: JsonObject } {
  const response = responseOf(frames, id);
  if (response.error === undefined) {
    throw new Error(`expected error response for id ${String(id)}, got result: ${JSON.stringify(response.result)}`);
  }
  return response.error as { code: number; message: string; data: JsonObject };
}

export function resultOf(frames: JsonObject[], id: string | number): unknown {
  const response = responseOf(frames, id);
  if ("error" in response && response.error !== undefined) {
    throw new Error(`expected result response for id ${String(id)}, got error: ${JSON.stringify(response.error)}`);
  }
  return response.result;
}

export function eventsOf(frames: JsonObject[]): Array<{ method: string; params: JsonObject }> {
  return frames
    .filter((frame) => typeof frame["method"] === "string")
    .map((frame) => ({ method: frame["method"] as string, params: frame["params"] as JsonObject }));
}

/** Poll until the probe returns a defined value; bounded, small steps. */
export async function until<T>(probe: () => T | undefined, timeoutMs = 2000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = probe();
    if (value !== undefined) {
      return value;
    }
    if (Date.now() > deadline) {
      throw new Error("condition not met within timeout");
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

/** A factory that always yields the supplied client (and records remotes). */
export function fixedFactory(client: unknown, remotes?: unknown[]): BridgeCoreOptions {
  return {
    clientFactory: async (remote) => {
      remotes?.push(remote);
      return client as Awaited<ReturnType<BridgeClientFactory>>;
    },
    writeFrame: () => undefined,
  };
}