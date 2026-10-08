/** Real SDK client adapter: the pinned `@cline/sdk` `ClineCore` remote client
 * behind the bridge's structural surface (issue #72 §1, §3).
 *
 * This module is the only place that imports `@cline/sdk`. It maps the D2
 * remote attachment and construction inputs onto the actual qualified public
 * SDK fields, forwards SDK-native results through unchanged (dates serialize
 * as ISO strings), and throws native failures uncaught for boundary
 * classification. No `@cline/llms` dependency, no `Agent`/`AgentRuntime`, no
 * approval executors, no provider catalog, and no synthesized flags.
 */

import { ClineCore, noopBasicLogger } from "@cline/sdk";
import type { ClineCoreOptions, ClineCoreStartInput, CoreSessionEvent, RemoteOptions } from "@cline/sdk";
import type { BridgeClientFactory, BridgeSdkClient, SdkSessionEvent, SdkStartInput, SdkStartResult } from "./bridge.js";
import type { WireRemoteConfig } from "./types.js";

class ClineCoreBridgeClient implements BridgeSdkClient {
  constructor(private readonly core: ClineCore) {}

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

  async dispose(): Promise<void> {
    await this.core.dispose();
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

/** The production client factory: one `ClineCore` remote-mode attachment for
 * the supplied already-running Hub. `noopBasicLogger` keeps SDK-side
 * operational diagnostics out of the protocol streams. No `clientName` is
 * synthesized; the caller's `client_identity` is the attach identity. */
export function createRealClientFactory(): BridgeClientFactory {
  return async (remote: WireRemoteConfig) => {
    const options: ClineCoreOptions = {
      backendMode: "remote",
      logger: noopBasicLogger,
      remote: buildRemoteOptions(remote),
    };
    const core = await ClineCore.create(options);
    return new ClineCoreBridgeClient(core);
  };
}