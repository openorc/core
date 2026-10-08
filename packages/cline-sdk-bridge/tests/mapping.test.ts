/** Construction mapping tests (issue #72 test 2): every D2 construction field
 * maps onto the actual qualified SDK argument shape — `config.sessionId`,
 * `config.mode`, additive rules, blank system prompt, working roots, tool
 * policies, and the exact nested `read_messages → initial_messages` round
 * trip — with no synthesized flags, approval callbacks, or initial prompt. */

import { describe, expect, it } from "vitest";
import { FakeClineCoreClient, type RecordedCall } from "./helpers/fake-client.js";
import { fixedFactory, requestLine, resultOf, startTestBridge, until } from "./helpers/test-bridge.js";
import { buildRemoteOptions } from "../src/sdk-client.js";
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

function expectedStartInput(sessionId?: string, initialMessages?: JsonObject[], mode = "plan"): JsonObject {
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
      ...(sessionId !== undefined ? { sessionId } : {}),
    },
    interactive: true,
    toolPolicies: { "*": { autoApprove: true }, ask_question: { enabled: false } },
    ...(initialMessages !== undefined ? { initialMessages } : {}),
  };
}

async function startCalls(fake: FakeClineCoreClient, request: JsonObject): Promise<RecordedCall[]> {
  const bridge = startTestBridge(fixedFactory(fake));
  bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  bridge.feed(`${requestLine(2, "start", request)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
  return fake.callsOf("start");
}

describe("start construction mapping", () => {
  it("maps every D2 fresh-construction field onto the SDK start input", async () => {
    const fake = new FakeClineCoreClient();
    const calls = await startCalls(fake, START as JsonObject);
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

  it("maps same-ID starts through config.sessionId with the unmodified raw message array", async () => {
    const fake = new FakeClineCoreClient();
    const messages: JsonObject[] = [
      { role: "user", content: "first", metadata: { nested: { deep: [1, 2, { key: "value" }] } } },
      { role: "assistant", content: "reply", metadata: { ts: 123, partial: false } },
    ];
    const calls = await startCalls(fake, {
      ...START,
      mode: "act",
      session_id: "exact-session-9",
      initial_messages: messages,
    } as JsonObject);
    expect(calls[0]?.args).toEqual(expectedStartInput("exact-session-9", messages, "act"));
    expect((calls[0]?.args["config"] as JsonObject)["mode"]).toBe("act");
    expect((calls[0]?.args["config"] as JsonObject)["sessionId"]).toBe("exact-session-9");
  });

  it("round-trips read_messages through a later same-ID start without structural loss", async () => {
    const fake = new FakeClineCoreClient();
    const storedMessages: JsonObject[] = [
      { role: "user", content: "hello", metadata: { a: 1 } },
      { role: "assistant", content: "hi", metadata: { b: { c: [true, null, 2.5] }, kept: "as-is" } },
    ];
    fake.script("read_messages", [{ ok: storedMessages }]);
    const bridge = startTestBridge(fixedFactory(fake));
    bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
    bridge.feed(`${requestLine(2, "start", START as JsonObject)}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    bridge.feed(`${requestLine(3, "read_messages", { session_id: "cline-session-1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    const read = resultOf(bridge.frames, 3) as JsonObject[];
    expect(read).toEqual(storedMessages);

    bridge.feed(`${requestLine(4, "start", { ...START, session_id: "cline-session-1", initial_messages: read })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 4) ? true : undefined));
    const sameIdStart = fake.callsOf("start")[1];
    expect(sameIdStart?.args["initialMessages"]).toEqual(storedMessages);
    expect(sameIdStart?.args["initialMessages"]).not.toBe(storedMessages);
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