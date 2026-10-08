/** The bridge core: fixed JSON-RPC 2.0 method surface over an injected SDK
 * client (issue #72 §2).
 *
 * `BridgeCore` is SDK-free: it consumes the structural `BridgeSdkClient`
 * surface, maps wire parameters onto it, forwards SDK observations as plain
 * JSON, classifies native failures into the D2 error classes, and preserves
 * request IDs across out-of-order completion. It never serializes all SDK
 * work behind one operation: requests are dispatched independently, event
 * forwarding stays live during in-flight sends, and the bridge makes no
 * retry, resend, reconnect, or session-replacement decisions.
 *
 * Wire methods (fixed): `connect`, `start`, `send`, `stop`, `abort`, `get`,
 * `read_messages`, `list_history`, `get_accumulated_usage`, `subscribe`,
 * `unsubscribe`, `dispose`. Asynchronous SDK events are emitted as
 * `cline.event` notifications carrying the subscription handle, exact event
 * session identity, safe machine kind, and the unchanged JSON payload.
 */

import {
  classifyNativeError,
  WireBackendError,
  WireProtocolError,
  wireErrorBody,
  NOT_ATTACHED_ERROR_CODE,
} from "./errors.js";
import {
  validateAbortParams,
  validateConnectParams,
  validateSendParams,
  validateStartParams,
  validateSubscribeParams,
  validateUnsubscribeParams,
} from "./params.js";
import { errorResponseFrame, eventNotificationFrame, LineSplitter, parseLine, responseFrame } from "./protocol.js";
import type { JsonObject, JsonValue, WireRemoteConfig, WireSessionEventParams } from "./types.js";

/** Notification method carrying asynchronous SDK session events. */
export const EVENT_NOTIFICATION_METHOD = "cline.event";

/** SDK start config mapped from the D2 construction bundle.
 *
 * `enableSpawnAgent` / `enableAgentTeams` are deliberately absent: the D2
 * contract forbids synthesizing them and the pinned 0.0.90 runtime treats
 * absent runtime-feature flags as disabled (its own remote session-creation
 * path omits them too).
 */
export interface SdkStartConfigInput {
  providerId: string;
  modelId: string;
  mode: "plan" | "act";
  rules: string;
  systemPrompt: string;
  cwd: string;
  workspaceRoot: string;
  enableTools: boolean;
  sessionId?: string;
}

/** SDK start input mapped from the D2 `ClineStartRequest`. */
export interface SdkStartInput {
  config: SdkStartConfigInput;
  interactive: boolean;
  toolPolicies: JsonObject;
  initialMessages?: unknown[];
}

/** SDK start result observed through the structural client surface. */
export interface SdkStartResult {
  sessionId: string;
  result?: unknown;
}

/** SDK session event observed through the structural client surface. */
export interface SdkSessionEvent {
  type: string;
  payload: unknown;
}

/** Structural SDK client surface the bridge consumes (fake-friendly).
 *
 * Implementations return SDK-native values; the bridge converts them to
 * plain JSON. Native failures are thrown uncaught and classified by the
 * bridge boundary.
 */
export interface BridgeSdkClient {
  start(input: SdkStartInput): Promise<SdkStartResult>;
  send(input: { sessionId: string; prompt: string }): Promise<unknown>;
  stop(sessionId: string): Promise<void>;
  abort(sessionId: string, reason?: unknown): Promise<void>;
  get(sessionId: string): Promise<unknown>;
  readMessages(sessionId: string): Promise<unknown[]>;
  listHistory(): Promise<unknown[]>;
  getAccumulatedUsage(sessionId: string): Promise<unknown>;
  subscribe(listener: (event: SdkSessionEvent) => void, options?: { sessionId?: string }): () => void;
  dispose(): Promise<void>;
}

/** Creates one SDK client attachment for the supplied remote inputs. */
export type BridgeClientFactory = (remote: WireRemoteConfig) => Promise<BridgeSdkClient>;

export interface BridgeCoreOptions {
  clientFactory: BridgeClientFactory;
  /** Writes one complete outbound JSON-RPC frame (called once per frame). */
  writeFrame: (frame: JsonObject) => void;
}

interface BridgeSubscription {
  id: string;
  filter: string | null;
  sdkUnsubscribe: () => void;
}

type JsonRpcId = string | number | null;

export class BridgeCore {
  private readonly clientFactory: BridgeClientFactory;
  private readonly writeFrame: (frame: JsonObject) => void;
  private readonly splitter = new LineSplitter();
  private client: BridgeSdkClient | null = null;
  private readonly subscriptions = new Map<string, BridgeSubscription>();
  private subscriptionCounter = 0;

  constructor(options: BridgeCoreOptions) {
    this.clientFactory = options.clientFactory;
    this.writeFrame = options.writeFrame;
  }

  /** Feed one raw inbound chunk (handles split and coalesced lines). */
  handleData(chunk: Uint8Array): void {
    for (const line of this.splitter.push(chunk)) {
      this.handleLine(line);
    }
  }

  /** Flush any trailing partial line at end of input. */
  handleEnd(): void {
    for (const line of this.splitter.flush()) {
      this.handleLine(line);
    }
  }

  private handleLine(line: string): void {
    const parsed = parseLine(line);
    if (parsed.type === "notification") {
      return;
    }
    if (parsed.type === "error") {
      this.writeFrame(errorResponseFrame(null, wireErrorBody(parsed.error)));
      return;
    }
    const { id, method, params } = parsed.request;
    void this.dispatch(id, method, params);
  }

  private respond(id: JsonRpcId, result: JsonValue): void {
    this.writeFrame(responseFrame(id, result));
  }

  private fail(id: JsonRpcId, error: unknown): void {
    this.writeFrame(errorResponseFrame(id, wireErrorBody(error)));
  }

  private async dispatch(id: JsonRpcId, method: string, params: unknown): Promise<void> {
    try {
      const result = await this.invoke(method, params);
      this.respond(id, result);
    } catch (error) {
      this.fail(id, error);
    }
  }

  private requireClient(): BridgeSdkClient {
    if (this.client === null) {
      throw new WireProtocolError("not_attached", NOT_ATTACHED_ERROR_CODE);
    }
    return this.client;
  }

  private async invoke(method: string, params: unknown): Promise<JsonValue> {
    switch (method) {
      case "connect":
        return await this.connect(params);
      case "start":
        return await this.start(params);
      case "send":
        return await this.send(params);
      case "stop":
        return await this.stop(params);
      case "abort":
        return await this.abort(params);
      case "get":
        return await this.get(params);
      case "read_messages":
        return await this.readMessages(params);
      case "list_history":
        return await this.listHistory();
      case "get_accumulated_usage":
        return await this.getAccumulatedUsage(params);
      case "subscribe":
        return await this.subscribe(params);
      case "unsubscribe":
        return await this.unsubscribe(params);
      case "dispose":
        return await this.dispose();
      default:
        throw new WireProtocolError("method_not_found");
    }
  }

  private async guarded<T>(operation: string, work: () => Promise<T>): Promise<T> {
    try {
      return await work();
    } catch (error) {
      if (error instanceof WireBackendError || error instanceof WireProtocolError) {
        throw error;
      }
      throw classifyNativeError(operation, error);
    }
  }

  private async connect(params: unknown): Promise<JsonValue> {
    const remote = validateConnectParams(params);
    const previous = this.client;
    this.client = null;
    if (previous !== null) {
      // Releasing the replaced attachment is a local resource action; its
      // failure must not block the replacement and never affects external
      // Task sessions.
      await previous.dispose().catch(() => undefined);
    }
    await this.guarded("connect", async () => {
      this.client = await this.clientFactory(remote);
    });
    return null;
  }

  private async start(params: unknown): Promise<JsonValue> {
    const request = validateStartParams(params);
    const client = this.requireClient();
    const input: SdkStartInput = {
      config: {
        providerId: request.provider_id,
        modelId: request.model_id,
        mode: request.mode,
        rules: request.rules,
        systemPrompt: request.system_prompt,
        cwd: request.cwd,
        workspaceRoot: request.workspace_root,
        enableTools: request.enable_tools,
        ...(request.session_id !== undefined ? { sessionId: request.session_id } : {}),
      },
      interactive: request.interactive,
      toolPolicies: request.tool_policies,
      ...(request.initial_messages !== undefined ? { initialMessages: request.initial_messages } : {}),
    };
    return await this.guarded("start", async () => {
      const result = await client.start(input);
      return {
        session_id: result.sessionId,
        result: result.result === undefined ? null : toPlainJson(result.result),
      };
    });
  }

  private async send(params: unknown): Promise<JsonValue> {
    const request = validateSendParams(params);
    const client = this.requireClient();
    return await this.guarded("send", async () => {
      const result = await client.send({ sessionId: request.session_id, prompt: request.prompt });
      return result === undefined ? null : toPlainJson(result);
    });
  }

  private async stop(params: unknown): Promise<JsonValue> {
    const sessionId = requireSessionIdParam(params);
    const client = this.requireClient();
    await this.guarded("stop", () => client.stop(sessionId));
    return null;
  }

  private async abort(params: unknown): Promise<JsonValue> {
    const request = validateAbortParams(params);
    const client = this.requireClient();
    await this.guarded("abort", () => client.abort(request.session_id, request.reason));
    return null;
  }

  private async get(params: unknown): Promise<JsonValue> {
    const sessionId = requireSessionIdParam(params);
    const client = this.requireClient();
    return await this.guarded("get", async () => {
      const record = await client.get(sessionId);
      return record === undefined ? null : toPlainJson(record);
    });
  }

  private async readMessages(params: unknown): Promise<JsonValue> {
    const sessionId = requireSessionIdParam(params);
    const client = this.requireClient();
    return await this.guarded("read_messages", async () => toPlainJson(await client.readMessages(sessionId)));
  }

  private async listHistory(): Promise<JsonValue> {
    const client = this.requireClient();
    return await this.guarded("list_history", async () => toPlainJson(await client.listHistory()));
  }

  private async getAccumulatedUsage(params: unknown): Promise<JsonValue> {
    const sessionId = requireSessionIdParam(params);
    const client = this.requireClient();
    return await this.guarded("get_accumulated_usage", async () => {
      const usage = await client.getAccumulatedUsage(sessionId);
      return usage === undefined ? null : toPlainJson(usage);
    });
  }

  private async subscribe(params: unknown): Promise<JsonValue> {
    const request = validateSubscribeParams(params);
    const client = this.requireClient();
    this.subscriptionCounter += 1;
    const subscription: BridgeSubscription = {
      id: `sub-${this.subscriptionCounter}`,
      filter: request.session_id,
      sdkUnsubscribe: () => undefined,
    };
    subscription.sdkUnsubscribe = client.subscribe(
      (event) => this.forwardEvent(subscription, event),
      request.session_id !== null ? { sessionId: request.session_id } : undefined,
    );
    this.subscriptions.set(subscription.id, subscription);
    return { subscription_id: subscription.id };
  }

  private async unsubscribe(params: unknown): Promise<JsonValue> {
    const request = validateUnsubscribeParams(params);
    const subscription = this.subscriptions.get(request.subscription_id);
    if (subscription !== undefined) {
      this.subscriptions.delete(request.subscription_id);
      // Idempotent, and only ever stops this one handle.
      subscription.sdkUnsubscribe();
    }
    return null;
  }

  private async dispose(): Promise<JsonValue> {
    this.clearSubscriptions();
    const client = this.client;
    this.client = null;
    if (client !== null) {
      try {
        await this.guarded("dispose", () => client.dispose());
      } catch {
        // Local resource release; containment here is the safe behavior and
        // never implies any external session decision.
      }
    }
    return null;
  }

  /** Best-effort local shutdown used at end of input; never a session
   * decision. Errors are contained; the caller bounds any hang. */
  async disposeForExit(): Promise<void> {
    this.clearSubscriptions();
    const client = this.client;
    this.client = null;
    if (client === null) {
      return;
    }
    try {
      await client.dispose();
    } catch {
      // Local resource release at process exit; nothing safe to report.
    }
  }

  private clearSubscriptions(): void {
    for (const subscription of this.subscriptions.values()) {
      try {
        subscription.sdkUnsubscribe();
      } catch {
        // Local unsubscribe must never block shutdown.
      }
    }
    this.subscriptions.clear();
  }

  private forwardEvent(subscription: BridgeSubscription, event: SdkSessionEvent): void {
    if (typeof event.type !== "string" || event.type.length === 0) {
      return;
    }
    const payload = event.payload;
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
      return;
    }
    const sessionId = (payload as { sessionId?: unknown }).sessionId;
    if (typeof sessionId !== "string" || sessionId.length === 0) {
      return;
    }
    if (subscription.filter !== null && sessionId !== subscription.filter) {
      return;
    }
    const params: WireSessionEventParams = {
      subscription_id: subscription.id,
      session_id: sessionId,
      kind: event.type,
      // The payload was verified to be a JSON object above; its JSON
      // round-trip is therefore an object too.
      payload: toPlainJson(payload) as JsonObject,
    };
    this.writeFrame(eventNotificationFrame(EVENT_NOTIFICATION_METHOD, params));
  }
}

function requireSessionIdParam(params: unknown): string {
  if (params === null || typeof params !== "object" || Array.isArray(params)) {
    throw new WireProtocolError("invalid_params");
  }
  const sessionId = (params as { session_id?: unknown }).session_id;
  if (typeof sessionId !== "string" || sessionId.trim().length === 0) {
    throw new WireProtocolError("invalid_params");
  }
  return sessionId;
}

/** Convert an SDK-native value to plain JSON. Dates serialize as ISO strings;
 * `undefined`-valued object properties drop, which is stable JSON behavior.
 * Non-serializable values fail closed into backend error classification. */
export function toPlainJson(value: unknown): JsonValue {
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}