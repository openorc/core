/** Real SDK call-site option tests (issue #407): the actual `sdk-client.ts`
 * production module drives a scripted SDK boundary, proving the documented
 * public `retryAfterDispatch: false` per-command option is applied
 * authoritatively at the consequential `run.enqueue` call site — never
 * derivable from, or overridable through, the opaque `remote_options` — and
 * that read-only `run.list` keeps the SDK default. The fake SDK here is the
 * mock boundary for the *option contract*; the SDK-internal retransmission
 * behavior itself is established by `transport.test.ts` over the real
 * pinned client. */

import { beforeEach, describe, expect, it, vi } from "vitest";

const sdkMock = vi.hoisted(() => {
  const commandCalls: Array<{
    command: string;
    payload: unknown;
    sessionId: unknown;
    options: unknown;
  }> = [];
  const constructionOptions: unknown[] = [];

  class FakeHubClient {
    readonly options: unknown;

    constructor(options: unknown) {
      constructionOptions.push(options);
      this.options = options;
    }

    async connect(): Promise<void> {}

    async command(
      command: string,
      payload?: Record<string, unknown>,
      sessionId?: string,
      options?: unknown,
    ): Promise<{ payload?: Record<string, unknown> }> {
      commandCalls.push({ command, payload, sessionId, options });
      return { payload: { recorded: true } };
    }

    subscribe(_listener: unknown, _options?: unknown): () => void {
      return () => {};
    }

    async dispose(): Promise<void> {}
  }

  return { commandCalls, constructionOptions, FakeHubClient };
});

vi.mock("@cline/sdk", () => ({
  ClineCore: {
    create: async () => ({ dispose: async () => {} }),
  },
  noopBasicLogger: () => {},
  NodeHubClient: sdkMock.FakeHubClient,
  normalizeHubWebSocketUrl: (url: string) => url,
}));

import { createRealClientFactory } from "../src/sdk-client.js";
import type { WireRemoteConfig } from "../src/types.js";

const REMOTE: WireRemoteConfig = {
  endpoint: "wss://hub.example/remote",
  client_identity: "openorc-core/test",
  auth_token: "attach-token",
};

beforeEach(() => {
  sdkMock.commandCalls.length = 0;
  sdkMock.constructionOptions.length = 0;
});

describe("consequential dispatch option contract (issue #407)", () => {
  it("passes retryAfterDispatch:false at the real run.enqueue call site", async () => {
    const client = await createRealClientFactory()(REMOTE);
    await client.enqueue({ sessionId: "exact-session-1", prompt: "turn" });

    expect(sdkMock.commandCalls).toHaveLength(1);
    expect(sdkMock.commandCalls[0]).toEqual({
      command: "run.enqueue",
      payload: { prompt: "turn" },
      sessionId: "exact-session-1",
      options: { retryAfterDispatch: false },
    });
  });

  it("keeps run.list on the SDK default (no per-command options)", async () => {
    const client = await createRealClientFactory()(REMOTE);
    await client.listRuns("exact-session-2");

    expect(sdkMock.commandCalls).toHaveLength(1);
    expect(sdkMock.commandCalls[0]).toEqual({
      command: "run.list",
      payload: {},
      sessionId: "exact-session-2",
      options: undefined,
    });
  });

  it("the safety option survives an opaque remote_options override attempt", async () => {
    // The opaque demonstrated remote_options object is construction-surface
    // only (spread verbatim into the hub client options); the per-command
    // safety option is applied after it at the call site, so remote
    // configuration can never re-enable post-dispatch retransmission.
    const client = await createRealClientFactory()({
      ...REMOTE,
      remote_options: { retryAfterDispatch: true, workspaceRoot: "/workspace" },
    });
    await client.enqueue({ sessionId: "exact-session-3", prompt: "turn" });

    // Construction options carry the verbatim spread with the explicit D2
    // fields authoritative; the per-command options remain suppressed.
    expect(sdkMock.constructionOptions[0]).toEqual({
      retryAfterDispatch: true,
      workspaceRoot: "/workspace",
      url: "wss://hub.example/remote",
      clientType: "openorc-core/test",
      authToken: "attach-token",
    });
    expect(sdkMock.commandCalls[0]?.options).toEqual({ retryAfterDispatch: false });
  });
});