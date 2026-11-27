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
 * Consequential-dispatch safety (issue #407): every effectful Hub command
 * this bridge issues — `run.enqueue` admission and the native `session.resume`
 * reconstruction (issue #408) — passes the documented public
 * `retryAfterDispatch: false` command option (issue #407), so the pinned
 * SDK's transport recovery can never retransmit an already-dispatched
 * consequential command. Read-only queries (`run.list`, the post-resume
 * pending-prompts consultation) keep the SDK default.
 * Read-only queries may retain safe SDK reconnect behavior; a retransmitted
 * admission is never acceptable because dispatch alone does not confirm
 * Hub-side acceptance.
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
  SdkResumeInput,
  SdkResumeResult,
  SdkSessionEvent,
  SdkStartInput,
  SdkStartResult,
} from "./bridge.js";
import type { WireRemoteConfig } from "./types.js";

/** Structural surface of the officially exported `NodeHubClient` (issue
 * #321).
 *
 * The pinned SDK's declaration file re-exports the class through directory
 * star-exports that this package's pinned NodeNext module resolution cannot
 * follow — verified against the pinned 0.2.1 root declaration, which still
 * re-exports only the derived API factories, not the class or the URL
 * normalizer — so the public constructor, URL normalizer, and command
 * options are extracted through the package namespace with one documented
 * boundary cast — the same discipline as the start-input cast in `start`.
 * The runtime object is the unmodified SDK class (`@cline/sdk` star-re-exports
 * `@cline/core`, whose public surface exports both symbols); the structural
 * surface covers exactly the documented members the bridge consumes, and no
 * private shape is assumed. */
interface HubClientOptionsInput {
  url: string;
  authToken?: string;
  clientType?: string;
}

/** Structural surface of the pinned SDK's documented per-command options
 * (issue #407). Only the members the bridge consumes are listed; the SDK's
 * own `HubCommandOptions` type is deliberately not exported through the
 * declaration entry point, so it is carried through the same boundary cast. */
interface HubCommandOptionsInput {
  retryAfterDispatch?: boolean;
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
    options?: HubCommandOptionsInput,
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

/** A verified `session.resume` reply that does not carry the exact requested
 * session identity. Safe machine code only; no native text is carried. The
 * downstream classification keeps this an unproven-effect uncertain outcome:
 * the command was dispatched and answered, so the incarnation state is never
 * claimed known. */
class ResumeIdentityMismatchError extends Error {
  readonly code = "resume_identity_mismatch";

  constructor() {
    super("the native resume reply did not carry the requested session identity");
    this.name = "ResumeIdentityMismatchError";
  }
}

/** Extract the restored session identity from one verified `session.resume`
 * reply payload. The pinned 0.2.1 reply shapes carry the identity as
 * `payload.snapshot.sessionId`, `payload.session.sessionId`, or
 * `payload.sessionId`; any other shape is unusable and returns undefined so
 * the caller fails closed. Reply contents are never interpreted beyond this
 * bounded identity extraction. */
function extractResumeSessionId(payload: unknown): string | undefined {
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    return undefined;
  }
  const record = payload as { snapshot?: unknown; session?: unknown; sessionId?: unknown };
  for (const candidate of [
    (record.snapshot as { sessionId?: unknown } | null | undefined)?.sessionId,
    (record.session as { sessionId?: unknown } | null | undefined)?.sessionId,
    record.sessionId,
  ]) {
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate.trim();
    }
  }
  return undefined;
}

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
    // `enableSpawnAgent`/`enableAgentTeams`, which the pinned 0.2.1 SDK config
    // type (`CoreRuntimeFeatures`) still marks as required booleans; the
    // pinned runtime treats absent flags as disabled.
    const result = await this.core.start(input as unknown as ClineCoreStartInput);
    return result.result === undefined
      ? { sessionId: result.sessionId }
      : { sessionId: result.sessionId, result: result.result };
  }

  /** Native same-session reconstruction (issue #408): one public
   * `NodeHubClient.command("session.resume", …)` call carrying the complete
   * construction bundle for the exact existing session. The Hub performs
   * Hub-side saved-history loading — no transcript is ever uploaded, and
   * there is no fallback to a transcript-carrying re-creation.
   *
   * Contract established against the pinned 0.2.1 surface (static analysis
   * + live qualification, SDK3): the Hub's resume handler requires
   * `payload.sessionConfig.sessionId`, refuses a missing saved record
   * (`session_not_found`), refuses an empty/unavailable saved conversation
   * (`session_messages_not_found`), refuses an already-resident session
   * (`session_already_exists`), and otherwise runs the shared session
   * creation routine with the saved messages injected as
   * `initialMessages`; the reply carries the restored identity as
   * `payload.snapshot.sessionId` or `payload.session.sessionId`. The
   * sessionConfig fields mirror the SDK's own remote session-creation
   * payload (the full construction config plus the pinned session ID), so
   * runtime-side defaults resolve identically for the resumed incarnation.
   *
   * Consequential-dispatch safety (issue #407 handoff): `retryAfterDispatch:
   * false` is applied authoritatively at this real call site — a per-command
   * options argument, never part of the construction options, so neither the
   * opaque `remote_options` nor any remote configuration can override it.
   * A lost reply or transport failure is an uncertain outcome that never
   * authorizes another resume.
   *
   * After the identity verification, one read-only
   * `core.pendingPrompts.list({sessionId})` consultation establishes the
   * documented session-event subscription wiring for the restored
   * incarnation ("In hub mode ClineCore subscribes to a session when it
   * starts, sends to, or lists pending prompts for it" — declared
   * `hasSessionSubscription` contract), so ordinary observation, reads, and
   * authorized turns address the restored incarnation through the ordinary
   * production backend. The consultation is auxiliary: it runs after the
   * incarnation change took effect, so its failure is contained (the wiring
   * self-heals at the next ordinary send) and can never reclassify the
   * already-verified resume outcome. */
  async resume(input: SdkResumeInput): Promise<SdkResumeResult> {
    const hub = await this.requireHub();
    const construction = input.construction;
    const payload = {
      workspaceRoot: construction.workspaceRoot,
      cwd: construction.cwd,
      sessionConfig: {
        sessionId: input.sessionId,
        providerId: construction.providerId,
        modelId: construction.modelId,
        cwd: construction.cwd,
        workspaceRoot: construction.workspaceRoot,
        systemPrompt: construction.systemPrompt,
        mode: construction.mode,
        rules: construction.rules,
        enableTools: construction.enableTools,
      },
      metadata: {
        interactive: construction.interactive,
        provider: construction.providerId,
        model: construction.modelId,
        enableTools: construction.enableTools,
      },
      runtimeOptions: {},
      toolPolicies: input.toolPolicies,
    };
    const reply = await hub.command(
      "session.resume",
      payload,
      input.sessionId,
      { retryAfterDispatch: false },
    );
    const restored = extractResumeSessionId(reply.payload);
    if (restored !== input.sessionId) {
      // Fail closed: a reply that does not carry the exact requested
      // identity never counts as a successful reconstruction.
      throw new ResumeIdentityMismatchError();
    }
    try {
      await this.core.pendingPrompts.list({ sessionId: input.sessionId });
    } catch {
      // Contained auxiliary wiring consultation only: the incarnation
      // change already took effect and was identity-verified, and the
      // documented subscription lifecycle re-establishes delivery at the
      // next ordinary send. This failure never reclassifies the resume.
    }
    return { sessionId: restored };
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
    // Consequential admission (issue #407): `retryAfterDispatch: false` is
    // applied authoritatively at the real SDK call site — it is a per-command
    // options argument, never part of the construction options, so neither
    // the opaque `remote_options` nor any remote configuration can override
    // it. The pinned SDK performs one transport-recovery resend after
    // dispatch unless this option is passed; a retransmitted admission is
    // never acceptable because dispatch alone does not confirm Hub-side
    // acceptance and the bridge's uncertain-outcome discipline (never a
    // resend) owns the recovery decision.
    const reply = await hub.command(
      "run.enqueue",
      { prompt: input.prompt },
      input.sessionId,
      { retryAfterDispatch: false },
    );
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