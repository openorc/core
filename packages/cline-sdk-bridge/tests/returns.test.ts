/** Native return-shape tests (issue #72 test 3): representative SDK-native
 * results and permitted nulls cross as unchanged plain JSON — send results,
 * undefined → null, nullable reads, history, and usage, with safe native
 * date serialization. */

import { describe, expect, it } from "vitest";
import { FakeClineCoreClient } from "./helpers/fake-client.js";
import { fixedFactory, requestLine, resultOf, startTestBridge, until } from "./helpers/test-bridge.js";

const CONNECT = { endpoint: "wss://hub.example/remote", client_identity: "openorc-core/test" };

async function call(method: string, params: unknown, script?: Array<Record<string, unknown>>): Promise<unknown> {
  const fake = new FakeClineCoreClient();
  if (script !== undefined) {
    fake.script(method, script);
  }
  const bridge = startTestBridge(fixedFactory(fake));
  bridge.feed(`${requestLine(1, "connect", CONNECT)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  bridge.feed(`${requestLine(2, "start", {
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
  })}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
  bridge.feed(`${requestLine(3, method, params)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
  return resultOf(bridge.frames, 3);
}

describe("native return shapes", () => {
  it("forwards a representative send AgentResult as plain JSON", async () => {
    const native = {
      text: "final answer",
      iterations: 3,
      finishReason: "completed",
      usage: { inputTokens: 10, outputTokens: 5 },
      messages: [{ role: "assistant", content: "final answer", metadata: { nested: true } }],
      model: { id: "model-a", provider: "provider-a" },
    };
    const result = await call("send", { session_id: "cline-session-1", prompt: "go" }, [{ ok: native }]);
    expect(result).toEqual(native);
  });

  it("maps the SDK's legitimate undefined send response to JSON null", async () => {
    const result = await call("send", { session_id: "cline-session-1", prompt: "go" }, [{ ok: undefined }]);
    expect(result).toBeNull();
  });

  it("serializes native dates as safe ISO strings", async () => {
    const result = await call(
      "get",
      { session_id: "cline-session-1" },
      [{ ok: { sessionId: "cline-session-1", startedAt: new Date("2026-01-02T03:04:05.678Z") } }],
    );
    expect(result).toEqual({ sessionId: "cline-session-1", startedAt: "2026-01-02T03:04:05.678Z" });
  });

  it("preserves permitted null/absent reads", async () => {
    expect(await call("get", { session_id: "cline-session-1" })).toBeNull();
    expect(await call("get_accumulated_usage", { session_id: "cline-session-1" })).toBeNull();
  });

  it("returns representative usage summaries unchanged", async () => {
    const usage = {
      usage: { inputTokens: 100, outputTokens: 40, cacheReadTokens: 0, cacheWriteTokens: 0, totalCost: 0.02 },
      aggregateUsage: { inputTokens: 150, outputTokens: 60, cacheReadTokens: 5, cacheWriteTokens: 1, totalCost: 0.03 },
    };
    expect(await call("get_accumulated_usage", { session_id: "cline-session-1" }, [{ ok: usage }])).toEqual(usage);
  });

  it("returns history arrays unchanged", async () => {
    const history = [
      { sessionId: "a", status: "completed", title: "First" },
      { sessionId: "b", status: "active" },
    ];
    expect(await call("list_history", undefined, [{ ok: history }])).toEqual(history);
  });

  it("returns message arrays with full nested structure", async () => {
    const messages = [
      { role: "user", content: "q", metadata: { deep: { list: [1, { x: null }] } } },
      { role: "assistant", content: "a", metadata: { toolCalls: [{ name: "read_file", args: { path: "/w" } }] } },
    ];
    expect(await call("read_messages", { session_id: "cline-session-1" }, [{ ok: messages }])).toEqual(messages);
  });
});