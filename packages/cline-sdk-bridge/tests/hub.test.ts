/** Hub-native admission and observation tests (issue #321): `enqueue` maps to
 * exactly the public `run.enqueue` command with a verified ack, admission
 * refusals, dispatch timeouts, and transport losses classify per the three-way
 * admission discipline with no blind resend, `run.list` extraction never
 * fabricates an empty observation, and Hub event envelopes forward verbatim
 * through their own structural subscription path. */

import { describe, expect, it } from "vitest";
import {
  FakeClineCoreClient,
  hubCommandRefusal,
  hubCommandTimeoutError,
  hubTransportError,
} from "./helpers/fake-client.js";
import { errorOf, eventsOf, fixedFactory, requestLine, resultOf, startTestBridge, until } from "./helpers/test-bridge.js";

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
  // A scripted start binds the tests' fixed "s1" identity (fresh starts
  // allocate their own IDs on the fake).
  fake.script("start", [{ ok: { sessionId: "s1" } }]);
  const bridge = startTestBridge(fixedFactory(fake));
  bridge.feed(`${requestLine(0, "connect", CONNECT)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 0) ? true : undefined));
  bridge.feed(`${requestLine(1, "start", START)}\n`);
  await until(() => (bridge.frames.some((frame) => frame["id"] === 1) ? true : undefined));
  return bridge;
}

describe("hub run admission (enqueue)", () => {
  it("maps enqueue onto exactly the public run.enqueue command and verifies the ack", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "enqueue", { session_id: "s1", prompt: "detached turn" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));

    const calls = fake.callsOf("enqueue");
    expect(calls.length).toBe(1);
    // Exactly sessionId + prompt: no delivery mode is ever carried into the
    // Hub enqueue input.
    expect(calls[0]?.args).toEqual({ sessionId: "s1", prompt: "detached turn" });
    expect(resultOf(bridge.frames, 2)).toEqual({
      run_id: "hub-run-1",
      accepted_at: 1794712612935,
      queue_position: 0,
    });
  });

  it("surfaces known admission refusals with the Hub code and no resend", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("enqueue", [
      { error: hubCommandRefusal("run_admission_rejected") },
      { error: hubCommandRefusal("hub_draining") },
    ]);
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));

    const error = errorOf(bridge.frames, 2);
    expect(error.data).toEqual({ kind: "sdk_operation_rejected", code: "run_admission_rejected" });
    // One dispatch, no blind resend of the refused admission.
    expect(fake.callsOf("enqueue").length).toBe(1);

    bridge.feed(`${requestLine(3, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    expect(errorOf(bridge.frames, 3).data).toEqual({ kind: "sdk_operation_rejected", code: "hub_draining" });
    expect(fake.callsOf("enqueue").length).toBe(2);
  });

  it("classifies a positive session_not_found answer as exact-session absence", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("enqueue", [{ error: hubCommandRefusal("session_not_found") }]);
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(errorOf(bridge.frames, 2).data).toEqual({ kind: "session_not_found" });
  });

  it("classifies a dispatch timeout as an uncertain outcome, never a refusal", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("enqueue", [{ error: hubCommandTimeoutError() }]);
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(errorOf(bridge.frames, 2).data).toEqual({ kind: "uncertain_outcome" });
    expect(fake.callsOf("enqueue").length).toBe(1);
  });

  it("classifies a pending enqueue rejected by connection loss as uncertain", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("enqueue", [{ deferred: "parked-enqueue" }]);
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
    await until(() => (fake.callsOf("enqueue").length > 0 ? true : undefined));

    // The socket closes while the command is in flight: the pinned client
    // rejects the pending command with a transport error after possible
    // dispatch, which is never proof of non-delivery or refusal.
    fake.deferred("parked-enqueue").reject(hubTransportError("hub_connection_closed", 1006));
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(errorOf(bridge.frames, 2).data).toEqual({ kind: "uncertain_outcome" });
    expect(fake.callsOf("enqueue").length).toBe(1);
  });

  it("keeps a malformed ok acknowledgement uncertain instead of inventing a run handle", async () => {
    const malformedAcks: unknown[] = [
      {},
      { runId: "", acceptedAt: 1, queuePosition: 0 },
      { runId: "r", acceptedAt: 1.5, queuePosition: 0 },
      { runId: "r", acceptedAt: 1, queuePosition: -1 },
      { runId: "r", acceptedAt: 1 },
    ];
    const fake = new FakeClineCoreClient();
    fake.script("enqueue", malformedAcks.map((ok) => ({ ok })));
    const bridge = await connectedBridge(fake);
    for (let index = 0; index < malformedAcks.length; index += 1) {
      bridge.feed(`${requestLine(10 + index, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
      await until(() => (bridge.frames.some((frame) => frame["id"] === 10 + index) ? true : undefined));
      expect(errorOf(bridge.frames, 10 + index).data).toEqual({ kind: "uncertain_outcome" });
    }
  });

  it("rejects enqueue before an attachment exists as a wire protocol error", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = startTestBridge(fixedFactory(fake));
    bridge.feed(`${requestLine(0, "enqueue", { session_id: "s1", prompt: "turn" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 0) ? true : undefined));
    expect(errorOf(bridge.frames, 0).data).toEqual({ kind: "wire_protocol", reason: "not_attached" });
    expect(fake.callsOf("enqueue").length).toBe(0);
  });
});

describe("hub run observation (list_runs)", () => {
  it("returns the status-only runs array verbatim for filtered and unfiltered reads", async () => {
    const fake = new FakeClineCoreClient();
    fake.script("list_runs", [
      { ok: { runs: [{ runId: "r1", state: "queued" }, { runId: "r2", state: "failed" }] } },
      { ok: { runs: [] } },
    ]);
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "list_runs", { session_id: "s1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(resultOf(bridge.frames, 2)).toEqual([
      { runId: "r1", state: "queued" },
      { runId: "r2", state: "failed" },
    ]);
    expect(fake.callsOf("list_runs")[0]?.args).toEqual({ session_id: "s1" });

    bridge.feed(`${requestLine(3, "list_runs", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    expect(resultOf(bridge.frames, 3)).toEqual([]);
    expect(fake.callsOf("list_runs")[1]?.args).toEqual({ session_id: null });
  });

  it("treats a missing or malformed runs array as unavailable, never empty evidence", async () => {
    const malformedPayloads: unknown[] = [{}, { runs: "nope" }, { runs: [1] }, { runs: null }];
    const fake = new FakeClineCoreClient();
    fake.script("list_runs", malformedPayloads.map((ok) => ({ ok })));
    const bridge = await connectedBridge(fake);
    for (let index = 0; index < malformedPayloads.length; index += 1) {
      bridge.feed(`${requestLine(20 + index, "list_runs", { session_id: null })}\n`);
      await until(() => (bridge.frames.some((frame) => frame["id"] === 20 + index) ? true : undefined));
      expect(errorOf(bridge.frames, 20 + index).data).toEqual({ kind: "uncertain_outcome" });
    }
  });
});

describe("hub event envelopes (subscribe_hub)", () => {
  it("forwards Hub envelopes verbatim with envelope-owned identity and sequence", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe_hub", { session_id: "s1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    expect(resultOf(bridge.frames, 2)).toEqual({ subscription_id: "sub-1" });
    expect(fake.callsOf("subscribe_hub")[0]?.args).toEqual({ filter: "s1" });

    fake.emitHub({
      event: "run.started",
      sessionId: "s1",
      sequence: 7,
      payload: { requestId: "hub-run-1", snapshot: { prompt: "ignored" } },
    });
    let events = eventsOf(bridge.frames);
    expect(events.length).toBe(1);
    expect(events[0]?.method).toBe("cline.hub_event");
    expect(events[0]?.params).toEqual({
      subscription_id: "sub-1",
      session_id: "s1",
      kind: "run.started",
      sequence: 7,
      payload: { requestId: "hub-run-1", snapshot: { prompt: "ignored" } },
    });

    // Terminal envelopes carry neither runId nor requestId; nothing is
    // synthesized and the AgentResult passes through unchanged.
    fake.emitHub({
      event: "run.completed",
      sessionId: "s1",
      sequence: 8,
      payload: { reason: "finished", result: { text: "done" }, snapshot: {} },
    });
    events = eventsOf(bridge.frames);
    expect(events[1]?.params).toEqual({
      subscription_id: "sub-1",
      session_id: "s1",
      kind: "run.completed",
      sequence: 8,
      payload: { reason: "finished", result: { text: "done" }, snapshot: {} },
    });
  });

  it("drops unattributable envelopes for filtered subscriptions and nulls absent identity", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe_hub", { session_id: "s1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));

    fake.emitHub({ event: "run.started", sessionId: "s2", sequence: 1, payload: {} });
    expect(eventsOf(bridge.frames).length).toBe(0);

    // An unfiltered subscription forwards an envelope without session
    // identity with null identity; absent sequence forwards as null too.
    bridge.feed(`${requestLine(3, "subscribe_hub", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    fake.emitHub({ event: "usage.updated", payload: { tokens: 5 } });
    const events = eventsOf(bridge.frames);
    expect(events.length).toBe(1);
    expect(events[0]?.params).toEqual({
      subscription_id: "sub-2",
      session_id: null,
      kind: "usage.updated",
      sequence: null,
      payload: { tokens: 5 },
    });
  });

  it("stops only its own hub subscription on idempotent unsubscribe", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe_hub", { session_id: "s1" })}\n`);
    bridge.feed(`${requestLine(3, "subscribe_hub", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));

    bridge.feed(`${requestLine(4, "unsubscribe", { subscription_id: "sub-1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 4) ? true : undefined));
    bridge.feed(`${requestLine(5, "unsubscribe", { subscription_id: "sub-1" })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 5) ? true : undefined));
    expect(resultOf(bridge.frames, 5)).toBeNull();

    fake.emitHub({ event: "run.started", sessionId: "s1", sequence: 1, payload: {} });
    const events = eventsOf(bridge.frames);
    expect(events.length).toBe(1);
    expect(events[0]?.params["subscription_id"]).toBe("sub-2");
  });

  it("stops all hub forwarding on dispose", async () => {
    const fake = new FakeClineCoreClient();
    const bridge = await connectedBridge(fake);
    bridge.feed(`${requestLine(2, "subscribe_hub", { session_id: null })}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 2) ? true : undefined));
    bridge.feed(`${requestLine(3, "dispose")}\n`);
    await until(() => (bridge.frames.some((frame) => frame["id"] === 3) ? true : undefined));
    fake.emitHub({ event: "run.started", sessionId: "s1", sequence: 1, payload: {} });
    expect(eventsOf(bridge.frames).length).toBe(0);
    expect(fake.callsOf("start").length).toBe(1);
  });
});
