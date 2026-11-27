/** Controlled transport-fault evidence (issue #407): the **real pinned**
 * `NodeHubClient` from the installed `@cline/sdk` 0.2.1 exercises a local
 * stub Hub over real WebSockets, establishing SDK-internal retransmission
 * behavior — not bridge-wrapper or fake-client behavior.
 *
 * Evidence recorded here:
 * - A consequential `run.enqueue` issued with the documented public
 *   `retryAfterDispatch: false` option is dispatched exactly once even when
 *   the transport dies after dispatch: the command rejects with the pinned
 *   transport error shape and the stub Hub observes exactly one command
 *   frame. A bridge-level wrapper counter or a fake-client timeout can never
 *   prove this SDK-internal property.
 * - The acknowledged-admission path still resolves with the Hub reply
 *   payload through the same real client.
 * - Read-only subscriptions keep the safe SDK reconnect behavior: after a
 *   transport loss the client reconnects to the same admitted endpoint
 *   (re-registration plus subscription re-issue) and never repoints to a
 *   local-Hub discovery URL — the pinned recovery path is gated on a
 *   remembered local-Hub URL the bridge never registers, so no local Hub
 *   fallback/start can occur.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { RawData, WebSocket as StubSocket } from "ws";
import { WebSocketServer } from "ws";
import * as ClineSdk from "@cline/sdk";

type Envelope = Record<string, unknown>;
type Frame = { kind?: string; envelope?: Envelope };

interface StubHubState {
  registrations: number;
  enqueueFrames: Envelope[];
  abortFrames: Envelope[];
  scenario: "ack" | "drop-after-dispatch";
}

interface StubHubHandle {
  url: string;
  state: StubHubState;
  /** Server-initiated close of every attached client socket (transport loss). */
  drop(): void;
  close(): Promise<void>;
}

async function startStubHub(): Promise<StubHubHandle> {
  const state: StubHubState = { registrations: 0, enqueueFrames: [], abortFrames: [], scenario: "ack" };
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  const clients = new Set<StubSocket>();
  // The server binds asynchronously: wait for the ephemeral port before use.
  await new Promise<void>((resolve, reject) => {
    wss.once("listening", () => resolve());
    wss.once("error", reject);
  });

  wss.on("connection", (socket: StubSocket) => {
    clients.add(socket);
    socket.on("close", () => clients.delete(socket));
    socket.on("message", (data: RawData) => {
      let frame: Frame;
      try {
        frame = JSON.parse(String(data)) as Frame;
      } catch {
        return;
      }
      const envelope = frame.envelope;
      if (frame.kind !== "command" || envelope === undefined) return;
      const command = envelope["command"] as string;
      if (command === "client.register") {
        state.registrations += 1;
        socket.send(
          JSON.stringify({ kind: "reply", envelope: { requestId: envelope["requestId"], command, ok: true, payload: {} } }),
        );
        return;
      }
      if (command === "run.enqueue") {
        state.enqueueFrames.push(envelope);
        if (state.scenario === "drop-after-dispatch") {
          socket.close();
          return;
        }
        socket.send(
          JSON.stringify({
            kind: "reply",
            envelope: {
              requestId: envelope["requestId"],
              command,
              ok: true,
              payload: { run_id: "run-1", accepted_at: "2026-01-01T00:00:00Z", queue_position: 0 },
            },
          }),
        );
        return;
      }
      if (command === "run.abort") {
        state.abortFrames.push(envelope);
        if (state.scenario === "drop-after-dispatch") {
          socket.close();
          return;
        }
        socket.send(
          JSON.stringify({ kind: "reply", envelope: { requestId: envelope["requestId"], command, ok: true, payload: {} } }),
        );
      }
    });
  });

  const address = wss.address();
  if (address === null || typeof address === "string") {
    await new Promise<void>((resolve) => wss.close(() => resolve()));
    throw new Error("stub hub did not bind an ephemeral TCP port");
  }
  const url = `ws://127.0.0.1:${address.port}/hub`;
  return {
    url,
    state,
    drop: () => {
      for (const socket of clients) socket.close();
    },
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of clients) socket.close();
        wss.close(() => resolve());
      }),
  };
}

/** The documented boundary cast shape (same discipline as sdk-client.ts):
 * the pinned root declaration does not re-export the class through
 * NodeNext, so the public constructor is extracted through the namespace. */
interface StubHubClient {
  connect(): Promise<void>;
  command(
    command: string,
    payload?: Record<string, unknown>,
    sessionId?: string,
    options?: { retryAfterDispatch?: boolean },
  ): Promise<{ payload?: Record<string, unknown>; ok?: boolean }>;
  subscribe(listener: (event: Envelope) => void, options?: { sessionId?: string }): () => void;
  isConnected(): boolean;
  dispose(): Promise<void>;
}

const { NodeHubClient } = ClineSdk as unknown as {
  NodeHubClient: new (options: { url: string; clientType?: string }) => StubHubClient;
};

/** Every client created through {@linkcode makeClient} is disposed by the
 * afterEach hook before the stub servers close, so no client keeps
 * reconnecting against a closed server. */
/** Core instances track only disposal in the shared afterEach cleanup. */
const openCores: Array<{ dispose(): Promise<void> }> = [];
const openClients: StubHubClient[] = [];
const openHubs: StubHubHandle[] = [];

function makeClient(hub: StubHubHandle): StubHubClient {
  const client = new NodeHubClient({ url: hub.url, clientType: "openorc-core/test" });
  openClients.push(client);
  return client;
}

afterEach(async () => {
  for (const core of openCores.splice(0)) {
    await core.dispose().catch(() => {});
  }
  for (const client of openClients.splice(0)) {
    await client.dispose().catch(() => {});
  }
  while (openHubs.length > 0) {
    await (openHubs.pop() as StubHubHandle).close();
  }
});

describe("real pinned NodeHubClient transport behavior (issue #407)", () => {
  it("resolves an acknowledged run.enqueue with the Hub reply payload", async () => {
    const hub = await startStubHub();
    openHubs.push(hub);
    const client = makeClient(hub);

    const reply = await client.command(
      "run.enqueue",
      { prompt: "turn" },
      "exact-session-1",
      { retryAfterDispatch: false },
    );

    expect(reply.ok).toBe(true);
    expect(reply.payload).toEqual({ run_id: "run-1", accepted_at: "2026-01-01T00:00:00Z", queue_position: 0 });
    expect(hub.state.registrations).toBe(1);
    expect(hub.state.enqueueFrames).toHaveLength(1);
  });

  it("dispatches a consequential run.enqueue exactly once when the transport dies after dispatch", async () => {
    const hub = await startStubHub();
    openHubs.push(hub);
    hub.state.scenario = "drop-after-dispatch";
    const client = makeClient(hub);

    const pending = client.command(
      "run.enqueue",
      { prompt: "turn" },
      "exact-session-2",
      { retryAfterDispatch: false },
    );
    await expect(pending).rejects.toMatchObject({ code: "hub_connection_closed" });

    // The decisive SDK-internal property: exactly one dispatch, no
    // transport-recovery retransmission of the already-dispatched command.
    expect(hub.state.enqueueFrames).toHaveLength(1);
    expect(hub.state.registrations).toBe(1);
  });

  it("keeps read-only subscriptions reconnecting to the same admitted endpoint", async () => {
    const hub = await startStubHub();
    openHubs.push(hub);
    const client = makeClient(hub);
    await client.connect();
    client.subscribe(() => {}, { sessionId: "exact-session-3" });

    const firstUrl = hub.url;
    // Force a real transport loss with an active subscription: the pinned
    // client schedules a reconnect (backoff base 250ms, ±50% jitter) and must
    // come back to the same URL — re-register and re-issue the subscription —
    // without consulting local-Hub discovery.
    hub.drop();
    const reconnected = await new Promise<number>((resolve, reject) => {
      const deadline = Date.now() + 5000;
      const poll = (): void => {
        if (Date.now() > deadline) {
          reject(new Error("client did not reconnect in time"));
          return;
        }
        if (hub.state.registrations >= 2) {
          resolve(hub.state.registrations);
          return;
        }
        setTimeout(poll, 50);
      };
      setTimeout(poll, 100);
    });

    expect(reconnected).toBeGreaterThanOrEqual(2);
    expect(firstUrl).toBe(hub.url);
    expect(hub.state.enqueueFrames).toHaveLength(0);
  });
});

/** The real pinned `ClineCore` remote runtime path (issue #407 review
 * evidence): the runtime host's effectful commands (`run.start`, `run.abort`,
 * `session.detach`, `session.create`) all issue through the same internal
 * Hub client `command()` mechanism as the exported `NodeHubClient` (verified
 * against the pinned bundle), so one representative consequential operation
 * — `abort`, the control command with the strongest effect — exercises the
 * shared transport mechanism end to end. `ClineCore` exposes no suppression
 * option at its public surface; the established non-retransmission guarantee
 * rests on the same never-remembered-endpoint gating proven above plus this
 * observed behavior. `telemetry` is omitted so the SDK default is a no-op,
 * and HOME is isolated so no user Cline state is touched. */
describe("real pinned ClineCore remote-path transport behavior (issue #407)", () => {
  let isolatedHome: string | undefined;

  beforeAll(() => {
    isolatedHome = mkdtempSync(join(tmpdir(), "sdk2a-cline-home-"));
    process.env.HOME = isolatedHome;
  });

  afterAll(() => {
    if (isolatedHome !== undefined) {
      rmSync(isolatedHome, { recursive: true, force: true });
    }
  });

  it("dispatches a consequential run.abort exactly once when the transport dies after dispatch", async () => {
    const hub = await startStubHub();
    openHubs.push(hub);
    const { ClineCore } = ClineSdk as unknown as {
      ClineCore: {
        create(options: {
          backendMode: "remote";
          remote: { endpoint: string; clientType?: string };
        }): Promise<{
          abort(sessionId: string, reason?: unknown): Promise<void>;
          dispose(): Promise<void>;
        }>;
      };
    };
    const core = await ClineCore.create({
      backendMode: "remote",
      remote: { endpoint: hub.url, clientType: "openorc-core/test" },
    });
    openCores.push({ dispose: () => core.dispose() });

    hub.state.scenario = "drop-after-dispatch";
    const pending = core.abort("exact-session-4", "sdk2a transport probe");
    await expect(pending).rejects.toMatchObject({ code: "hub_connection_closed" });

    // The ClineCore-path counterpart of the decisive property: exactly one
    // dispatch of the consequential control command, no retransmission.
    expect(hub.state.abortFrames).toHaveLength(1);
    expect(hub.state.registrations).toBe(1);
  });
});