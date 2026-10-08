/** Deterministic in-process fake SDK client for bridge tests (issue #72
 * required tests). Mirrors the D2 fake philosophy: FIFO scripted outcomes
 * consumed exactly once, exact-ID session semantics, deep-copied recorded
 * arguments, synchronous event delivery in subscription order, and no
 * workflow/persistence/retry semantics of any kind.
 *
 * Native failures are thrown in the pinned SDK's native shapes (an `Error`
 * with a machine `code`, optional transport `details.closeCode`) so the
 * bridge boundary classification is exercised end to end.
 */

import type { BridgeSdkClient, SdkSessionEvent, SdkStartInput, SdkStartResult } from "../../src/bridge.js";
import type { JsonObject } from "../../src/types.js";

export interface ScriptedOutcome {
  ok?: unknown;
  error?: unknown;
  delayMs?: number;
  /** Park the call until the test resolves the matching deferred. */
  deferred?: string;
}

export interface RecordedCall {
  method: string;
  args: JsonObject;
}

export class DeferredController {
  private settled = false;

  constructor(
    private readonly resolveFn: (value: unknown) => void,
    private readonly rejectFn: (error: unknown) => void,
  ) {}

  resolve(value: unknown): void {
    if (!this.settled) {
      this.settled = true;
      this.resolveFn(value);
    }
  }

  reject(error: unknown): void {
    if (!this.settled) {
      this.settled = true;
      this.rejectFn(error);
    }
  }
}

export class FakeClineCoreClient implements BridgeSdkClient {
  readonly calls: RecordedCall[] = [];
  disposeCount = 0;

  private readonly scripts = new Map<string, ScriptedOutcome[]>();
  private readonly deferreds = new Map<string, DeferredController>();
  private readonly sessions = new Map<string, unknown[]>();
  private readonly subscriptions: Array<{
    filter: string | null;
    listener: (event: SdkSessionEvent) => void;
    active: boolean;
  }> = [];
  private nextSessionNumber = 0;

  script(method: string, outcomes: ScriptedOutcome[]): void {
    this.scripts.set(method, [...outcomes]);
  }

  /** Resolve the parked deferred outcome for a scripted key. */
  deferred(key: string): DeferredController {
    const existing = this.deferreds.get(key);
    if (existing === undefined) {
      throw new Error(`no parked deferred for key ${key}`);
    }
    return existing;
  }

  private nextOutcome(method: string): ScriptedOutcome | undefined {
    const queue = this.scripts.get(method);
    if (queue === undefined || queue.length === 0) {
      return undefined;
    }
    return queue.shift();
  }

  private record(method: string, args: unknown): void {
    this.calls.push({ method, args: structuredClone(args) as JsonObject });
  }

  callsOf(method: string): RecordedCall[] {
    return this.calls.filter((call) => call.method === method);
  }

  private async apply<T>(method: string, fallback: () => T): Promise<T> {
    const outcome = this.nextOutcome(method);
    if (outcome === undefined) {
      return fallback();
    }
    if (outcome.deferred !== undefined) {
      const key = outcome.deferred;
      return await new Promise<T>((resolve, reject) => {
        this.deferreds.set(key, new DeferredController(resolve as (value: unknown) => void, reject));
      });
    }
    if (outcome.delayMs !== undefined) {
      await delay(outcome.delayMs);
    }
    if (outcome.error !== undefined) {
      throw outcome.error;
    }
    return outcome.ok as T;
  }

  private sessionNotFound(sessionId: string): Error {
    const error = new Error(`session not found: ${sessionId}`);
    (error as { code?: string }).code = "session_not_found";
    return error;
  }

  private requireKnown(sessionId: string): void {
    if (!this.sessions.has(sessionId)) {
      throw this.sessionNotFound(sessionId);
    }
  }

  async start(input: SdkStartInput): Promise<SdkStartResult> {
    this.record("start", input);
    return await this.apply("start", () => {
      const sessionId = input.config.sessionId ?? `cline-session-${this.nextSessionNumber + 1}`;
      if (input.config.sessionId === undefined) {
        this.nextSessionNumber += 1;
      }
      this.sessions.set(sessionId, input.initialMessages === undefined ? [] : structuredClone(input.initialMessages));
      return { sessionId };
    });
  }

  async send(input: { sessionId: string; prompt: string }): Promise<unknown> {
    this.record("send", input);
    return await this.apply("send", () => {
      this.requireKnown(input.sessionId);
      return undefined;
    });
  }

  async stop(sessionId: string): Promise<void> {
    this.record("stop", { sessionId });
    await this.apply("stop", () => {
      this.requireKnown(sessionId);
      return undefined;
    });
  }

  async abort(sessionId: string, reason?: unknown): Promise<void> {
    this.record("abort", { sessionId, reason: reason === undefined ? null : reason });
    await this.apply("abort", () => {
      this.requireKnown(sessionId);
      return undefined;
    });
  }

  async get(sessionId: string): Promise<unknown> {
    this.record("get", { sessionId });
    return await this.apply("get", () => {
      this.requireKnown(sessionId);
      return undefined;
    });
  }

  async readMessages(sessionId: string): Promise<unknown[]> {
    this.record("read_messages", { sessionId });
    return await this.apply("read_messages", () => {
      this.requireKnown(sessionId);
      return structuredClone(this.sessions.get(sessionId) ?? []);
    });
  }

  async listHistory(): Promise<unknown[]> {
    this.record("list_history", {});
    return await this.apply("list_history", () => []);
  }

  async getAccumulatedUsage(sessionId: string): Promise<unknown> {
    this.record("get_accumulated_usage", { sessionId });
    return await this.apply("get_accumulated_usage", () => {
      this.requireKnown(sessionId);
      return undefined;
    });
  }

  subscribe(listener: (event: SdkSessionEvent) => void, options?: { sessionId?: string }): () => void {
    this.record("subscribe", { filter: options?.sessionId ?? null });
    const entry = { filter: options?.sessionId ?? null, listener, active: true };
    this.subscriptions.push(entry);
    return () => {
      entry.active = false;
    };
  }

  /** Synchronous test-side event delivery in subscription order. */
  emit(sessionId: string, kind: string, payload: JsonObject): void {
    for (const entry of this.subscriptions) {
      if (!entry.active) {
        continue;
      }
      if (entry.filter !== null && entry.filter !== sessionId) {
        continue;
      }
      entry.listener({ type: kind, payload });
    }
  }

  async dispose(): Promise<void> {
    this.record("dispose", {});
    this.disposeCount += 1;
    for (const entry of this.subscriptions) {
      entry.active = false;
    }
  }
}

/** A native-shaped session-not-found error. */
export function sessionNotFoundError(sessionId: string): Error {
  const error = new Error(`session not found: ${sessionId}`);
  (error as { code?: string }).code = "session_not_found";
  return error;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}