/** Real SDK client adapter: the pinned `@cline/sdk` `ClineCore` remote client
 * plus the officially exported `NodeHubClient` behind the bridge's structural
 * surface (issue #72 §1, §3; issue #321).
 *
 * This module is the only place that imports `@cline/sdk`. It maps the D2
 * remote attachment and construction inputs onto the actual qualified public
 * SDK fields, forwards SDK-native results through unchanged (dates serialize
 * as ISO strings), and throws native failures uncaught for boundary
 * classification. No `@cline/llms` dependency, no `Agent`/`AgentRuntime`, no
 * approval executors, no provider catalog, and no synthesized flags.
 *
 * Hub-native operations (issue #321) run through one officially exported
 * `NodeHubClient` per attachment generation: constructed lazily on the first
 * hub operation from the same admitted endpoint, authentication, and client
 * identity as the existing `ClineCore` attachment, reused for the attachment
 * lifetime, and disposed with it. This reuses the public SDK command surface
 * for the already-running Hub; no new Hub, no callback server, and no
 * private Hub frame construction exists here. No `delivery` mode is ever
 * synthesized into hub command payloads.
 */

import { ClineCore, noopBasicLogger } from "@cline/sdk";
import type { ClineCoreOptions, ClineCoreStartInput, CoreSessionEvent, RemoteOptions } from "@cline/sdk";
import * as ClineSdk from "@cline/sdk";
import type {
  BridgeClientFactory,
  BridgeSdkClient,
  SdkHubEvent,
  SdkSessionEvent,
  SdkStartInput,
  SdkStartResult,
} from "./bridge.js";
import type { WireRemoteConfig } from "./types.js";

/** Structural surface of the officially exported `NodeHubClient` (issue
 * #321).
 *
 * The pinned SDK's declaration file re-exports the class through directory
 * star-exports (`export * from "./hub"`, `export * from "./client"`) that
 * this package's pinned NodeNext module resolution cannot follow, so the
 * public constructor and URL normalizer are extracted through the package
 * namespace with one documented boundary cast — the same discipline as the
 * start-input cast in `start`. The runtime object is the unmodified SDK
 * class (`@cline/sdk` star-re-exports `@cline/core`, whose public surface
 * exports both symbols); the structural surface covers exactly the
 * documented members the bridge consumes, and no private shape is assumed. */
interface HubClientOptionsInput {
  url: string;
  authToken?: string;
  clientType?: string;
}

interface HubCommandReplyLike {
  payload?: Record<string, unknown> | undefined;
}

interface NodeHubClientLike {
  connect(): Promise<void>;
  command(
    command: string,
    payload?: Record<string, unknown>,
    sessionId?: string,
  ): Promise<HubCommandReplyLike>;
  subscribe(
    listener: (event: SdkHubEvent) => void,
    options?: { sessionId?: string },
  ): () => void;
  dispose(): Promise<void>;
}

const { NodeHubClient, normalizeHubWebSocketUrl } = ClineSdk as unknown as {
  NodeHubClient: new (options: HubClientOptionsInput) => NodeHubClientLike;
  normalizeHubWebSocketUrl: (url: string) => string;
};

class ClineCoreBridgeClient implements BridgeSdkClient {
  /** The lazily established hub-client attachment for this generation:
   * created on the first hub operation, reused thereafter, disposed with
   * the attachment. Concurrent hub operations share one attach attempt;
   * a failed attempt resets so the next operation retries its own attach. */
  private hubAttach: Promise<NodeHubClientLike> | null = null;

  constructor(
    private readonly core: ClineCore,
    private readonly remote: WireRemoteConfig,
  ) {}

  async start(input: SdkStartInput): Promise<SdkStartResult> {
    // Single documented boundary cast: the D2 contract forbids synthesizing
    // `enableSpawnAgent`/`enableAgentTeams`, which the SDK config type marks
    // as required; the pinned runtime treats absent flags as disabled.
    const result = await this.core.start(input as unknown as ClineCoreStartInput);
    return result.result === undefined
      ? { sessionId: result.sessionId }
      : { sessionId: result.sessionId, result: result.result };
  }

  async send(input: { sessionId: string; prompt: string }): Promise<unknown> {
    return await this.core.send({ sessionId: input.sessionId, prompt: input.prompt });
  }

  async stop(sessionId: string): Promise<void> {
    await this.core.stop(sessionId);
  }

  async abort(sessionId: string, reason?: unknown): Promise<void> {
    await this.core.abort(sessionId, reason);
  }

  async get(sessionId: string): Promise<unknown> {
    return await this.core.get(sessionId);
  }

  async readMessages(sessionId: string): Promise<unknown[]> {
    return await this.core.readMessages(sessionId);
  }

  async listHistory(): Promise<unknown[]> {
    return await this.core.listHistory();
  }

  async getAccumulatedUsage(sessionId: string): Promise<unknown> {
    return await this.core.getAccumulatedUsage(sessionId);
  }

  subscribe(listener: (event: SdkSessionEvent) => void, options?: { sessionId?: string }): () => void {
    const forward = (event: CoreSessionEvent): void => {
      listener({ type: event.type, payload: event.payload });
    };
    return this.core.subscribe(forward, options);
  }

  private requireHub(): Promise<NodeHubClientLike> {
    if (this.hubAttach === null) {
      const attach = (async () => {
        const hub = new NodeHubClient(buildHubClientOptions(this.remote));
        await hub.connect();
        return hub;
      })();
      attach.catch(() => {
        if (this.hubAttach === attach) {
          this.hubAttach = null;
        }
      });
      this.hubAttach = attach;
    }
    return this.hubAttach;
  }

  async enqueue(input: { sessionId: string; prompt: string }): Promise<unknown> {
    const hub = await this.requireHub();
    const reply = await hub.command("run.enqueue", { prompt: input.prompt }, input.sessionId);
    return reply.payload ?? {};
  }

  async listRuns(sessionId: string | null): Promise<unknown> {
    const hub = await this.requireHub();
    const reply = await hub.command("run.list", {}, sessionId ?? undefined);
    return reply.payload ?? {};
  }

  async subscribeHub(
    listener: (event: SdkHubEvent) => void,
    options?: { sessionId?: string },
  ): Promise<() => void> {
    const hub = await this.requireHub();
    // The pinned HubEventEnvelope is structurally assignable to the bridge's
    // envelope-only SdkHubEvent (event name, session identity, sequence,
    // payload), so forwarding stays verbatim with no re-shaping.
    return hub.subscribe(listener, options);
  }

  async dispose(): Promise<void> {
    const hubAttach = this.hubAttach;
    this.hubAttach = null;
    try {
      if (hubAttach !== null) {
        const hub = await hubAttach.catch(() => null);
        if (hub !== null) {
          await hub.dispose();
        }
      }
    } finally {
      // Local resource release of both attachments; containment and any
      // failure decision stay at the bridge boundary.
      await this.core.dispose();
    }
  }
}

/** Build the SDK remote options from the D2 remote attachment inputs.
 *
 * The opaque demonstrated `remote_options` object is forwarded verbatim; the
 * explicit D2 fields are authoritative and are applied after the spread.
 * `client_identity` maps to the Hub registration identity field
 * (`clientType`). Values are never logged or echoed. Exported for contract
 * tests; this is the complete connect mapping apart from the
 * `ClineCore.create` call itself.
 */
export function buildRemoteOptions(remote: WireRemoteConfig): RemoteOptions {
  const options = {
    ...(remote.remote_options ?? {}),
    endpoint: remote.endpoint,
    clientType: remote.client_identity,
  } as RemoteOptions;
  if (remote.auth_token !== undefined) {
    options.authToken = remote.auth_token;
  }
  return options;
}

/** Build the hub client options from the D2 remote attachment inputs (issue
 * #321).
 *
 * The same admitted endpoint, authentication, and client identity as the
 * `ClineCore` attachment: the opaque demonstrated `remote_options` object is
 * forwarded verbatim (spread first) and the explicit D2 fields are
 * authoritative and applied after the spread. `client_identity` maps to the
 * Hub registration identity field (`clientType`); the endpoint is normalized
 * with the SDK's exported URL normalizer so both attachments accept the same
 * configured endpoint. Values are never logged or echoed. Exported for
 * contract tests. */
export function buildHubClientOptions(remote: WireRemoteConfig): HubClientOptionsInput {
  const options = {
    ...(remote.remote_options ?? {}),
    url: normalizeHubWebSocketUrl(remote.endpoint),
    clientType: remote.client_identity,
  } as HubClientOptionsInput;
  if (remote.auth_token !== undefined) {
    options.authToken = remote.auth_token;
  }
  return options;
}

/** The production client factory: one `ClineCore` remote-mode attachment for
 * the supplied already-running Hub, carrying the hub-native operations of
 * issue #321 through its own lazily established `NodeHubClient`.
 * `noopBasicLogger` keeps SDK-side operational diagnostics out of the
 * protocol streams. No `clientName` is synthesized; the caller's
 * `client_identity` is the attach identity. */
export function createRealClientFactory(): BridgeClientFactory {
  return async (remote: WireRemoteConfig) => {
    const options: ClineCoreOptions = {
      backendMode: "remote",
      logger: noopBasicLogger,
      remote: buildRemoteOptions(remote),
    };
    const core = await ClineCore.create(options);
    return new ClineCoreBridgeClient(core, remote);
  };
}