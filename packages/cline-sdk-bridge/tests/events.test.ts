/** Subscription and concurrency tests (issue #72 test 5): filtered/unfiltered
 * SDK events become correlated JSON-RPC notifications, unsubscribe is
 * idempotent and stops only its own handle, and a parked in-flight send does
 * not block event delivery or an independent abort/read. */

import { describe, expect, it } from "vitest";
import { FakeClineCoreClient } from "./helpers/fake-client.js";
import { eventsOf, fixedFactory, requestLine, resultOf, startTestBridge, until } from "./helpers/test-bridge.js";

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
  bridge.feed(`${requestLine(1, "start", { ...START, session_id: "s1" })}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  return bridge;
}

describe("event subscriptions", () => {
  it("forwards unfiltered events as correlated cline.event notifications", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(resultOf(bridge.frames, 2)).toEqual({ subscription_id: "sub-1" });

    fake.emit("s1", "status", { sessionId: "s1", status: "running" });
    fake.emit("s2", "status", { sessionId: "s2", status: "running" });
    const events = eventsOf(bridge.frames);
    expect(events.length).toBe(2);
    expect(events[0]?.params).toEqual({
      subscription_id: "sub-1",
      session_id: "s1",
      kind: "status",
      payload: { sessionId: "s1", status: "running" },
    });
    expect(events[1]?.params).toEqual({
      subscription_id: "sub-1",
      session_id: "s2",
      kind: "status",
      payload: { sessionId: "s2", status: "running" },
    });
  });

  it("preserves exact event session identity for session-scoped subscriptions", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe", { session_id: "s1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(fake.callsOf("subscribe")[0]?.args).toEqual({ filter: "s1" });

    fake.emit("s2", "status", { sessionId: "s2", status: "running" });
    expect(eventsOf(bridge.frames).length).toBe(0);
    fake.emit("s1", "status", { sessionId: "s1", status: "running" });
    const events = eventsOf(bridge.frames);
    expect(events.length).toBe(1);
    expect(events[0]?.params["session_id"]).toBe("s1");
    expect(events[0]?.params["subscription_id"]).toBe("sub-1");
  });

  it("stops only its own subscription on idempotent unsubscribe", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe", { session_id: "s1" })}\n`);
    bridge.feed(`${requestLine(3, "subscribe", { session_id: "s2" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));

    bridge.feed(`${requestLine(4, "unsubscribe", { subscription_id: "sub-1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 4) ? true : undefined));
    bridge.feed(`${requestLine(5, "unsubscribe", { subscription_id: "sub-1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 5) ? true : undefined));
    expect(resultOf(bridge.frames, 5)).toBeNull();

    fake.emit("s1", "status", { sessionId: "s1", status: "running" });
    fake.emit("s2", "status", { sessionId: "s2", status: "running" });
    const events = eventsOf(bridge.frames);
    expect(events.length).toBe(1);
    expect(events[0]?.params["session_id"]).toBe("s2");
    expect(events[0]?.params["subscription_id"]).toBe("sub-2");
  });

  it("keeps event delivery and independent abort/read live while a send is parked", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));

    fake.script("send", [{ deferred: "parked-send" }]);
    bridge.feed(`${requestLine(3, "send", { session_id: "s1", prompt: "long work" })}\n`);
    await until(() => (fake.callsOf("send").length > 0 ? true : undefined));

    fake.emit("s1", "status", { sessionId: "s1", status: "running" });
    await until(() => (eventsOf(bridge.frames).length > 0 ? true : undefined));

    bridge.feed(`${requestLine(4, "abort", { session_id: "s1", reason: "owner interrupt" })}\n`);
    bridge.feed(`${requestLine(5, "get", { session_id: "s1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 5) ? true : undefined));
    expect(resultOf(bridge.frames, 5)).toBeNull();
    expect(bridge.frames.some((frame) => frame["id"] === 4)).toBe(true);
    expect(bridge.frames.some((frame) => frame["id"] === 3 && frame["result"] !== undefined)).toBe(false);

    fake.deferred("parked-send").resolve({ text: "parked finished" });
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    expect(resultOf(bridge.frames, 3)).toEqual({ text: "parked finished" });
    const indexOf = (id: number): number =>
      bridge.frames.findIndex((frame) => frame["id"] === id && frame["result"] !== undefined);
    expect(indexOf(5)).toBeLessThan(indexOf(3));
  });

  it("stops all forwarding on dispose without deleting sessions", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    bridge.feed(`${requestLine(3, "dispose")}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    fake.emit("s1", "status", { sessionId: "s1", status: "running" });
    expect(eventsOf(bridge.frames).length).toBe(0);
    expect(fake.callsOf("start").length).toBe(1);
  });
});