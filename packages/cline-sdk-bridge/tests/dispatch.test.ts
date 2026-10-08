/** Session dispatch/control tests (issue #72 test 4): exact target IDs and
 * verbatim send text, abort/stop/dispose kept distinct, and no hidden
 * delete, automatic start, retry, or resend. */

import { describe, expect, it } from "vitest";
import { FakeClineCoreClient, sessionNotFoundError } from "./helpers/fake-client.js";
import { errorOf, fixedFactory, requestLine, startTestBridge, until } from "./helpers/test-bridge.js";

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

async function connectedBridge(fake: FakeClineCoreClient) {
  const bridge = startTestBridge(fixedFactory(fake));
  bridge.feed(`${requestLine(0, "connect", CONNECT)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 0) ? true : undefined));
  bridge.feed(`${requestLine(1, "start", { ...START, session_id: "session-target" })}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  return bridge;
}

describe("session dispatch and control", () => {
  it("addresses the exact supplied session IDs with verbatim send text", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    const prompt = "Implement the login flow.\nLine two with unicode ✓ — keep verbatim.";
    bridge.feed(`${requestLine(2, "send", { session_id: "session-target", prompt })}\n`);
    bridge.feed(`${requestLine(3, "get", { session_id: "other-session" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    const send = fake.callsOf("send")[0];
    expect(send?.args).toEqual({ sessionId: "session-target", prompt });
    const get = fake.callsOf("get")[0];
    expect(get?.args).toEqual({ sessionId: "other-session" });
  });

  it("keeps abort, stop, and local dispose distinct", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "abort", { session_id: "session-target", reason: "owner interrupt" })}\n`);
    bridge.feed(`${requestLine(3, "abort", { session_id: "session-target" })}\n`);
    bridge.feed(`${requestLine(4, "stop", { session_id: "session-target" })}\n`);
    bridge.feed(`${requestLine(5, "dispose")}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 5) ? true : undefined));
    expect(fake.callsOf("abort")).toEqual([
      { method: "abort", args: { sessionId: "session-target", reason: "owner interrupt" } },
      { method: "abort", args: { sessionId: "session-target", reason: null } },
    ]);
    expect(fake.callsOf("stop")).toEqual([{ method: "stop", args: { sessionId: "session-target" } }]);
    expect(fake.callsOf("dispose")).toEqual([{ method: "dispose", args: {} }]);
    expect(fake.disposeCount).toBe(1);
  });

  it("keeps dispose idempotent and free of session decisions", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "dispose")}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    bridge.feed(`${requestLine(3, "dispose")}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    expect(fake.disposeCount).toBe(1);
    // No replacement session is ever allocated by dispose.
    expect(fake.callsOf("start").length).toBe(1);
  });

  it("never exposes a delete surface or automatic retries", async () => {
    const fake = new FakeClineCoreClient();
    expect(Object.hasOwn(fake, "delete")).toBe(false);
    expect((fake as unknown as Record<string, unknown>)["delete"]).toBeUndefined();
    const bridge = await connectedBridge(fake);
    fake.script("send", [{ error: sessionNotFoundError("session-target") }]);
    bridge.feed(`${requestLine(2, "send", { session_id: "session-target", prompt: "one" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(errorOf(bridge.frames, 2).data.kind).toBe("session_not_found");
    // The failed send was not retried or resent, and no start was issued.
    expect(fake.callsOf("send").length).toBe(1);
    expect(fake.callsOf("start").length).toBe(1);
    expect(fake.callsOf("stop").length).toBe(0);
    expect(fake.callsOf("abort").length).toBe(0);
  });
});