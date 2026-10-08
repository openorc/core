/** Safe wire error types and native-failure classification (issue #72 §4).
 *
 * The bridge classifies native SDK failures into the five D2 backend error
 * classes using only machine-checkable evidence, and never forwards raw
 * SDK/provider exception messages, stacks, URLs, tokens, prompts, result
 * bodies, or transcripts. A fixed harmless message plus a constrained
 * `error.data` classification is all that crosses the wire.
 *
 * Classification discipline (evidence-based, in order):
 *
 * 1. Positively identified exact-session absence: the SDK's session-not-found
 *    shape (`error.code === "session_not_found"`, the pinned 0.0.90 class
 *    constant) maps to `session_not_found`.
 * 2. A safe machine code on `error.code` (lowercase snake_case, bounded) maps
 *    to `sdk_operation_rejected` carrying that code — e.g. the qualified
 *    hub transport codes `hub_connect_failed` / `hub_connection_closed` — plus
 *    a numeric `close_code` when the SDK exposes one. Codes are preserved for
 *    downstream classification and never interpreted here: a bare
 *    `hub_connect_failed` does not identify bad credentials versus a down Hub.
 * 3. A Node system code on `error.code` (uppercase OS shape, not expressible
 *    in the D2 safe-code shape) is positive network/OS-level failure
 *    evidence: `remote_attachment_unavailable` for connect, otherwise
 *    `uncertain_outcome`. The raw code is never forwarded.
 * 4. Connect failures throwing JS built-in type/range/syntax errors indicate
 *    input/configuration rejection before any attachment could exist:
 *    `remote_attachment_rejected`.
 * 5. Everything else: connect failures produce no client attachment, so they
 *    map to `remote_attachment_unavailable`; every other operation may have
 *    reached the Hub with unproven non-effect and maps to `uncertain_outcome`.
 *    Uncertain outcomes are never relabeled as safe non-delivery.
 */

import type { JsonValue, WireBackendErrorKind, WireProtocolReason } from "./types.js";

/** Fixed harmless JSON-RPC `error.message` for backend failures. */
export const BACKEND_ERROR_MESSAGE = "cline sdk backend operation failed";

/** Standard JSON-RPC 2.0 protocol error codes. */
export const PROTOCOL_ERROR_CODES = {
  parse_error: -32700,
  invalid_request: -32600,
  method_not_found: -32601,
  invalid_params: -32602,
} as const;

export const NOT_ATTACHED_ERROR_CODE = PROTOCOL_ERROR_CODES.invalid_params;

export const PROTOCOL_ERROR_MESSAGES: Record<WireProtocolReason, string> = {
  parse_error: "parse error",
  invalid_request: "invalid request",
  method_not_found: "method not found",
  invalid_params: "invalid params",
  not_attached: "invalid params",
};

/** Constrained `error.data` of backend failures. */
export type WireBackendErrorData = {
  kind: WireBackendErrorKind;
  code?: string;
  close_code?: number;
};

/** A classified backend failure, thrown by bridge operation handlers. */
export class WireBackendError extends Error {
  readonly data: WireBackendErrorData;

  constructor(data: WireBackendErrorData) {
    super(BACKEND_ERROR_MESSAGE);
    this.name = "WireBackendError";
    this.data = data;
  }
}

/** A wire-protocol failure (framing, envelope, params, attachment state). */
export class WireProtocolError extends Error {
  readonly jsonRpcCode: number;
  readonly reason: WireProtocolReason;

  constructor(reason: WireProtocolReason, jsonRpcCode?: number) {
    super(PROTOCOL_ERROR_MESSAGES[reason]);
    this.name = "WireProtocolError";
    this.reason = reason;
    this.jsonRpcCode = jsonRpcCode ?? PROTOCOL_ERROR_CODES[reason as Exclude<WireProtocolReason, "not_attached">];
  }
}

const SAFE_CODE_PATTERN = /^[a-z][a-z0-9_]*$/;
const SYSTEM_CODE_PATTERN = /^[A-Z][A-Z0-9_]*$/;
const MAX_CODE_LENGTH = 64;

function codeOf(error: unknown): string | undefined {
  const candidate = (error as { code?: unknown } | null | undefined)?.code;
  return typeof candidate === "string" ? candidate : undefined;
}

/** The forwardable safe machine code of a native error, if any. */
function safeCodeOf(error: unknown): string | undefined {
  const code = codeOf(error);
  if (code === undefined || code.length > MAX_CODE_LENGTH || !SAFE_CODE_PATTERN.test(code)) {
    return undefined;
  }
  return code;
}

/** A Node system error code (uppercase OS shape), if present. */
function systemCodeOf(error: unknown): string | undefined {
  const code = codeOf(error);
  if (code === undefined || code.length > MAX_CODE_LENGTH || !SYSTEM_CODE_PATTERN.test(code)) {
    return undefined;
  }
  return code;
}

/** The numeric WebSocket close code exposed by the pinned SDK transport
 * errors (`HubTransportError.details.closeCode`), if present. The SDK's
 * `closeReason` string is never forwarded. */
function closeCodeOf(error: unknown): number | undefined {
  const details = (error as { details?: unknown } | null | undefined)?.details;
  const candidates: unknown[] = [details, error];
  for (const candidate of candidates) {
    if (candidate !== null && typeof candidate === "object") {
      const closeCode = (candidate as { closeCode?: unknown }).closeCode;
      if (typeof closeCode === "number" && Number.isFinite(closeCode)) {
        return closeCode;
      }
    }
  }
  return undefined;
}

/** Classify one native SDK failure for one bridge operation. */
export function classifyNativeError(operation: string, error: unknown): WireBackendError {
  if (codeOf(error) === "session_not_found") {
    return new WireBackendError({ kind: "session_not_found" });
  }

  const safeCode = safeCodeOf(error);
  if (safeCode !== undefined) {
    const closeCode = closeCodeOf(error);
    const data: WireBackendErrorData = { kind: "sdk_operation_rejected", code: safeCode };
    if (closeCode !== undefined) {
      data.close_code = closeCode;
    }
    return new WireBackendError(data);
  }

  if (systemCodeOf(error) !== undefined) {
    return new WireBackendError({
      kind: operation === "connect" ? "remote_attachment_unavailable" : "uncertain_outcome",
    });
  }

  if (operation === "connect" && (error instanceof TypeError || error instanceof RangeError || error instanceof SyntaxError)) {
    return new WireBackendError({ kind: "remote_attachment_rejected" });
  }

  return new WireBackendError({
    kind: operation === "connect" ? "remote_attachment_unavailable" : "uncertain_outcome",
  });
}

/** JSON `error.data` for wire-protocol errors: bounded reason only. */
export function protocolErrorData(reason: WireProtocolReason): { kind: "wire_protocol"; reason: WireProtocolReason } {
  return { kind: "wire_protocol", reason };
}

/** Build one JSON-RPC error frame body from a bridge error. */
export function wireErrorBody(error: unknown): { code: number; message: string; data: JsonValue } {
  if (error instanceof WireBackendError) {
    return { code: -32000, message: BACKEND_ERROR_MESSAGE, data: error.data };
  }
  if (error instanceof WireProtocolError) {
    return { code: error.jsonRpcCode, message: error.message, data: protocolErrorData(error.reason) };
  }
  // Unclassified bridge-internal failure: conservative backend uncertainty
  // with no detail. Never forward native text from unexpected bridge errors.
  return { code: -32000, message: BACKEND_ERROR_MESSAGE, data: { kind: "uncertain_outcome" } };
}