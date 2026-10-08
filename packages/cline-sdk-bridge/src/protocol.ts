/** JSON-RPC 2.0 newline-delimited stdio framing (issue #72 §2).
 *
 * One request/response/notification per UTF-8 line. Inbound lines may be
 * split across or coalesced into arbitrary chunks; the splitter handles
 * multibyte UTF-8 boundaries. Every outbound frame is written as one
 * complete line in a single write call so interleaved responses and event
 * notifications never tear.
 */

import type { JsonObject, JsonValue } from "./types.js";
import { WireProtocolError } from "./errors.js";

/** Streams decoder for newline-delimited UTF-8 frames. */
export class LineSplitter {
  private readonly decoder = new TextDecoder("utf-8");
  private buffer = "";

  /** Push one raw chunk; returns the complete lines it completes. */
  push(chunk: Uint8Array): string[] {
    this.buffer += this.decoder.decode(chunk, { stream: true });
    return this.drain(false);
  }

  /** Flush any trailing partial line at end-of-stream. */
  flush(): string[] {
    this.buffer += this.decoder.decode();
    return this.drain(true);
  }

  private drain(final: boolean): string[] {
    const lines: string[] = [];
    for (;;) {
      const newlineAt = this.buffer.indexOf("\n");
      if (newlineAt === -1) {
        break;
      }
      const line = this.buffer.slice(0, newlineAt);
      this.buffer = this.buffer.slice(newlineAt + 1);
      lines.push(stripCarriageReturn(line));
    }
    if (final && this.buffer.length > 0) {
      const line = this.buffer;
      this.buffer = "";
      lines.push(stripCarriageReturn(line));
    }
    return lines;
  }
}

function stripCarriageReturn(line: string): string {
  return line.endsWith("\r") ? line.slice(0, -1) : line;
}

export type JsonRpcId = string | number | null;

export interface ParsedRequest {
  id: JsonRpcId;
  method: string;
  params: unknown;
}

export type ParsedLine =
  | { type: "request"; request: ParsedRequest }
  | { type: "notification" }
  | { type: "error"; error: WireProtocolError };

function validId(value: unknown): value is JsonRpcId {
  return value === null || typeof value === "string" || typeof value === "number";
}

/** Parse one protocol line into a request, an ignorable notification, or a
 * bounded protocol error. Never echoes input content into error messages. */
export function parseLine(line: string): ParsedLine {
  if (line.trim().length === 0) {
    return { type: "notification" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return { type: "error", error: new WireProtocolError("parse_error") };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { type: "error", error: new WireProtocolError("invalid_request") };
  }
  const object = parsed as Record<string, unknown>;
  if (object["jsonrpc"] !== "2.0" || typeof object["method"] !== "string") {
    return { type: "error", error: new WireProtocolError("invalid_request") };
  }
  const hasId = Object.prototype.hasOwnProperty.call(object, "id");
  if (hasId && !validId(object["id"])) {
    return { type: "error", error: new WireProtocolError("invalid_request") };
  }
  if (!hasId) {
    // Inbound notifications are not part of the D3 surface; D4 sends
    // requests only. They are consumed without response, per JSON-RPC 2.0.
    return { type: "notification" };
  }
  return {
    type: "request",
    request: { id: object["id"] as JsonRpcId, method: object["method"], params: object["params"] },
  };
}

/** Build a JSON-RPC success response frame. */
export function responseFrame(id: JsonRpcId, result: unknown): JsonObject {
  return { jsonrpc: "2.0", id, result: (result ?? null) as JsonValue };
}

/** Build a JSON-RPC error response frame. */
export function errorResponseFrame(
  id: JsonRpcId,
  body: { code: number; message: string; data: JsonValue },
): JsonObject {
  return { jsonrpc: "2.0", id, error: body };
}

/** Build an asynchronous event notification frame. */
export function eventNotificationFrame(method: string, params: JsonObject): JsonObject {
  return { jsonrpc: "2.0", method, params };
}