/** Real SDK call-site option tests (issue #407; native reconstruction issue
 * #408): the actual `sdk-client.ts` production module drives a scripted SDK
 * boundary, proving the documented public `retryAfterDispatch: false`
 * per-command option is applied authoritatively at the consequential
 * `run.enqueue` and `session.resume` call sites — never derivable from, or
 * overridable through, the opaque `remote_options` — that read-only `run.list`
 * keeps the SDK default, and that the native resume verifies the exact
 * restored identity and consults the declared pending-prompts surface to
 * establish the documented session-event wiring. The fake SDK here is the
 * mock boundary for the *option contract*; the SDK-internal retransmission
 * behavior itself is established by `transport.test.ts` over the real pinned
 * client. */

import { beforeEach, describe, expect, it, vi } from "vitest";

const sdkMock = vi.hoisted(() => {
  const commandCalls: Array<{
    command: string;
    payload: unknown;
    sessionId: unknown;
    options: unknown;
  }> = [];
  const constructionOptions: unknown[] = [];
  const pendingPromptsCalls: Array<{ sessionId: unknown }> = [];
  let commandReply: { payload?: Record<string, unknown> } = { payload: { recorded: true } };
  let failPendingPrompts = false;

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
      return commandReply;
    }

    subscribe(_listener: unknown, _options?: unknown): () => void {
      return () => {};
    }

    async dispose(): Promise<void> {}
  }

  const fakeCore = {
    dispose: async () => {},
    pendingPrompts: {
      list: async (input: { sessionId: string }) => {
        pendingPromptsCalls.push({ sessionId: input.sessionId });
        if (failPendingPrompts) {
          throw new Error("consultation transport failure");
        }
        return [];
      },
    },
  };

  return {
    commandCalls,
    constructionOptions,
    pendingPromptsCalls,
    commandReply: () => commandReply,
    setCommandReply: (reply: { payload?: Record<string, unknown> }) => {
      commandReply = reply;
    },
    setPendingPromptsFailure: (value: boolean) => {
      failPendingPrompts = value;
    },
    FakeHubClient,
    fakeCore,
  };
});

vi.mock("@cline/sdk", () => ({
  ClineCore: {
    create: async () => sdkMock.fakeCore,
  },
  noopBasicLogger: () => {},
  NodeHubClient: sdkMock.FakeHubClient,
  normalizeHubWebSocketUrl: (url: string) => url,
}));

import { createRealClientFactory } from "../src/sdk-client.js";
import type { SdkResumeInput } from "../src/bridge.js";
import type { WireRemoteConfig } from "../src/types.js";

const REMOTE: WireRemoteConfig = {
  endpoint: "wss://hub.example/remote",
  client_identity: "openorc-core/test",
  auth_token: "attach-token",
};

const RESUME_INPUT: SdkResumeInput = {
  sessionId: "exact-session-1",
  construction: {
    providerId: "provider-a",
    modelId: "model-a",
    mode: "act",
    rules: "# Producer rules\nBe careful.",
    systemPrompt: "",
    cwd: "/workspace/producer",
    workspaceRoot: "/workspace",
    enableTools: true,
    interactive: true,
  },
  toolPolicies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
};

const EXPECTED_RESUME_PAYLOAD = {
  workspaceRoot: "/workspace",
  cwd: "/workspace/producer",
  sessionConfig: {
    sessionId: "exact-session-1",
    providerId: "provider-a",
    modelId: "model-a",
    cwd: "/workspace/producer",
    workspaceRoot: "/workspace",
    systemPrompt: "",
    mode: "act",
    rules: "# Producer rules\nBe careful.",
    enableTools: true,
  },
  metadata: {
    interactive: true,
    provider: "provider-a",
    model: "model-a",
    enableTools: true,
  },
  runtimeOptions: {},
  toolPolicies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
};

beforeEach(() => {
  sdkMock.commandCalls.length = 0;
  sdkMock.constructionOptions.length = 0;
  sdkMock.pendingPromptsCalls.length = 0;
  sdkMock.setPendingPromptsFailure(false);
  sdkMock.setCommandReply({ payload: { recorded: true } });
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

describe("native session.resume call-site contract (issue #408)", () => {
  it("passes retryAfterDispatch:false and the full construction bundle at the real session.resume call site", async () => {
    sdkMock.setCommandReply({ payload: { snapshot: { sessionId: "exact-session-1" } } });
    const client = await createRealClientFactory()(REMOTE);
    const result = await client.resume(RESUME_INPUT);

    expect(sdkMock.commandCalls).toHaveLength(1);
    expect(sdkMock.commandCalls[0]).toEqual({
      command: "session.resume",
      payload: EXPECTED_RESUME_PAYLOAD,
      sessionId: "exact-session-1",
      options: { retryAfterDispatch: false },
    });
    expect(result).toEqual({ sessionId: "exact-session-1" });
    // The documented session-event wiring consultation addresses the
    // restored incarnation through the ordinary production surface.
    expect(sdkMock.pendingPromptsCalls).toEqual([{ sessionId: "exact-session-1" }]);
  });

  it("accepts every pinned reply identity shape", async () => {
    const client = await createRealClientFactory()(REMOTE);
    for (const payload of [
      { snapshot: { sessionId: "exact-session-1" } },
      { session: { sessionId: "exact-session-1" } },
      { sessionId: "exact-session-1" },
      { session: { sessionId: " exact-session-1 " } },
    ]) {
      sdkMock.setCommandReply({ payload });
      await expect(client.resume(RESUME_INPUT)).resolves.toEqual({ sessionId: "exact-session-1" });
    }
    expect(sdkMock.commandCalls).toHaveLength(4);
  });

  it("fails closed on a reply that does not carry the exact requested identity", async () => {
    const client = await createRealClientFactory()(REMOTE);
    sdkMock.setCommandReply({ payload: { session: { sessionId: "other-session" } } });
    await expect(client.resume(RESUME_INPUT)).rejects.toMatchObject({
      code: "resume_identity_mismatch",
    });
    // A wrong-identity reply never consults the wiring surface.
    expect(sdkMock.pendingPromptsCalls).toEqual([]);
  });

  it("survives a failed wiring consultation without reclassifying the verified resume", async () => {
    const client = await createRealClientFactory()(REMOTE);
    sdkMock.setCommandReply({ payload: { sessionId: "exact-session-1" } });
    sdkMock.setPendingPromptsFailure(true);

    const result = await client.resume(RESUME_INPUT);

    expect(result).toEqual({ sessionId: "exact-session-1" });
    expect(sdkMock.pendingPromptsCalls).toEqual([{ sessionId: "exact-session-1" }]);
  });
});