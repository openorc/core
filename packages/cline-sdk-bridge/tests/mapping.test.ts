/** Construction mapping tests (issue #72 test 2; native reconstruction issue
 * #408): every D2 construction field maps onto the actual qualified SDK
 * argument shape — `config.mode`, additive rules, blank system prompt,
 * working roots, tool policies, and the native `resume` reconstruction
 * operation carrying the complete bundle with no transcript — with no
 * synthesized flags, approval callbacks, or initial prompt. */

import { describe, expect, it } from "vitest";
import { FakeClineCoreClient, type RecordedCall } from "./helpers/fake-client.js";
import { fixedFactory, requestLine, resultOf, startTestBridge, until } from "./helpers/test-bridge.js";
import { buildHubClientOptions, buildRemoteOptions } from "../src/sdk-client.js";
import type { JsonObject } from "../src/types.js";

const CONNECT = { endpoint: "wss://hub.example/remote", client_identity: "openorc-core/test", auth_token: "tok" };

const START = {
  provider_id: "provider-a",
  model_id: "model-a",
  mode: "plan",
  rules: "# Producer rules\nBe careful.",
  system_prompt: "",
  cwd: "/workspace/producer",
  workspace_root: "/workspace",
  enable_tools: true,
  interactive: true,
  tool_policies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
};

const RESUME = {
  session_id: "exact-session-9",
  ...START,
  mode: "act",
};

function expectedStartInput(mode = "plan"): JsonObject {
  return {
    config: {
      providerId: "provider-a",
      modelId: "model-a",
      mode,
      rules: "# Producer rules\nBe careful.",
      systemPrompt: "",
      cwd: "/workspace/producer",
      workspaceRoot: "/workspace",
      enableTools: true,
    },
    interactive: true,
    toolPolicies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
  };
}

function expectedResumeInput(): JsonObject {
  return {
    sessionId: "exact-session-9",
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
}

async function wireCalls(
  fake: FakeClineCoreClient,
  method: string,
  request: JsonObject,
): Promise<RecordedCall[]> {
  const bridge = startTestBridge(fixedFactory(fake));
  bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  bridge.feed(`${requestLine(2, method, request)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
  return fake.callsOf(method);
}

describe("start construction mapping", () => {
  it("maps every D2 fresh-construction field onto the SDK start input", async () => {
    const fake = new FakeClineCoreClient();
    const calls = await wireCalls(fake, "start", START as JsonObject);
    expect(calls.length).toBe(1);
    expect(calls[0]?.args).toEqual(expectedStartInput());
    const config = calls[0]?.args["config"] as JsonObject;
    expect(Object.hasOwn(config, "sessionId")).toBe(false);
    expect(Object.hasOwn(config, "enableSpawnAgent")).toBe(false);
    expect(Object.hasOwn(config, "enableAgentTeams")).toBe(false);
    expect(Object.hasOwn(calls[0]?.args as JsonObject, "prompt")).toBe(false);
    expect(Object.hasOwn(calls[0]?.args as JsonObject, "initialMessages")).toBe(false);
    expect(Object.hasOwn(calls[0]?.args as JsonObject, "capabilities")).toBe(false);
  });

  it("maps the native resume operation with the full construction bundle and no transcript", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("resume", [{ ok: { sessionId: "exact-session-9" } }]);
    const calls = await wireCalls(fake, "resume", RESUME as JsonObject);
    expect(calls.length).toBe(1);
    expect(calls[0]?.args).toEqual(expectedResumeInput());
    expect(Object.hasOwn(calls[0]?.args as JsonObject, "initialMessages")).toBe(false);
    expect(Object.hasOwn(calls[0]?.args["construction"] as JsonObject, "sessionId")).toBe(false);
  });

  it("round-trips a resume reply's verified identity as the wire session_id", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("resume", [{ ok: { sessionId: "exact-session-9" } }]);
    const bridge = startTestBridge(fixedFactory(fake));
    bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
    bridge.feed(`${requestLine(2, "resume", RESUME as JsonObject)}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(resultOf(bridge.frames, 2)).toEqual({ session_id: "exact-session-9" });
  });

  it("returns the exact SDK session id and the native result or JSON null", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("start", [{ ok: { sessionId: "returned-exact", result: { finishReason: "completed" } } }]);
    const bridge = startTestBridge(fixedFactory(fake));
    bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
    bridge.feed(`${requestLine(2, "start", START as JsonObject)}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(resultOf(bridge.frames, 2)).toEqual({ session_id: "returned-exact", result: { finishReason: "completed" } });

    const plain = new FakeClineCoreClient();
    const secondBridge = startTestBridge(fixedFactory(plain));
    secondBridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
    await until(() => (secondBridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
    secondBridge.feed(`${requestLine(2, "start", START as JsonObject)}\n`);
    await until(() => (secondBridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(resultOf(secondBridge.frames, 2)).toEqual({ session_id: "cline-session-1", result: null });
  });
});

describe("connect remote-options mapping", () => {
  it("applies explicit D2 fields after the verbatim opaque remote options", () => {
    const options = buildRemoteOptions({
      endpoint: "wss://hub.example/remote",
      client_identity: "openorc-core/test",
      auth_token: "attach-token",
      remote_options: { workspaceRoot: "/workspace", cwd: "/workspace", extra: { nested: true } },
    });
    expect(options).toEqual({
      workspaceRoot: "/workspace",
      cwd: "/workspace",
      extra: { nested: true },
      endpoint: "wss://hub.example/remote",
      clientType: "openorc-core/test",
      authToken: "attach-token",
    });
  });

  it("omits authToken when the D2 config carries none", () => {
    const options = buildRemoteOptions({ endpoint: "wss://hub.example/remote", client_identity: "openorc-core/test" });
    expect(options.endpoint).toBe("wss://hub.example/remote");
    expect(options.clientType).toBe("openorc-core/test");
    expect(Object.hasOwn(options, "authToken")).toBe(false);
  });
});

describe("hub client options mapping (issue #321)", () => {
  it("maps the same admitted endpoint, auth, and identity onto the hub client", () => {
    const options = buildHubClientOptions({
      endpoint: "https://hub.example/ws",
      client_identity: "openorc-core/test",
      auth_token: "attach-token",
      remote_options: { workspaceRoot: "/workspace", clientType: "overridden" },
    });
    // The endpoint is normalized to its WebSocket scheme for the hub client;
    // explicit D2 fields stay authoritative after the verbatim spread.
    expect(options.url).toBe("wss://hub.example/ws");
    expect(options.clientType).toBe("openorc-core/test");
    expect(options.authToken).toBe("attach-token");
    expect(options).toHaveProperty("workspaceRoot", "/workspace");
  });

  it("omits authToken when the D2 config carries none", () => {
    const options = buildHubClientOptions({ endpoint: "wss://hub.example/remote", client_identity: "openorc-core/test" });
    expect(options.url).toBe("wss://hub.example/remote");
    expect(options.clientType).toBe("openorc-core/test");
    expect(Object.hasOwn(options, "authToken")).toBe(false);
  });
});