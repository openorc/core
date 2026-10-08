/** Wire parameter validation for the fixed JSON-RPC method surface (#72 §2).
 *
 * Validation is bridge-local and structural: it never interprets prompt,
 * rule, transcript, or option contents beyond the declared JSON shapes, and
 * failures produce bounded payload-free wire errors without echoing inputs.
 */

import {
  type WireRemoteConfig,
  type WireStartRequest,
  isJsonObject,
  isNonEmptyString,
  type JsonObject,
} from "./types.js";
import { WireProtocolError } from "./errors.js";

function invalidParams(): WireProtocolError {
  return new WireProtocolError("invalid_params");
}

function requireObject(params: unknown): JsonObject {
  if (!isJsonObject(params)) {
    throw invalidParams();
  }
  return params;
}

function requireNonEmptyString(params: JsonObject, key: string): string {
  const value = params[key];
  if (!isNonEmptyString(value)) {
    throw invalidParams();
  }
  return value;
}

function requireString(params: JsonObject, key: string): string {
  const value = params[key];
  if (typeof value !== "string") {
    throw invalidParams();
  }
  return value;
}

function requireBoolean(params: JsonObject, key: string): boolean {
  const value = params[key];
  if (typeof value !== "boolean") {
    throw invalidParams();
  }
  return value;
}

function optionalString(params: JsonObject, key: string): string | undefined {
  const value = params[key];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isNonEmptyString(value)) {
    throw invalidParams();
  }
  return value;
}

function requireSessionId(params: JsonObject): string {
  return requireNonEmptyString(params, "session_id");
}

/** `connect` params: the flattened D2 `ClineRemoteConfig`. */
export function validateConnectParams(params: unknown): WireRemoteConfig {
  const object = requireObject(params);
  const endpoint = requireNonEmptyString(object, "endpoint");
  const clientIdentity = requireNonEmptyString(object, "client_identity");
  const authToken = optionalString(object, "auth_token");
  const remoteOptionsValue = object["remote_options"];
  let remoteOptions: JsonObject | undefined;
  if (remoteOptionsValue !== undefined && remoteOptionsValue !== null) {
    if (!isJsonObject(remoteOptionsValue)) {
      throw invalidParams();
    }
    remoteOptions = remoteOptionsValue;
  }
  const config: WireRemoteConfig = { endpoint, client_identity: clientIdentity };
  if (authToken !== undefined) {
    config.auth_token = authToken;
  }
  if (remoteOptions !== undefined) {
    config.remote_options = remoteOptions;
  }
  return config;
}

/** `start` params: the flattened D2 `ClineStartRequest`. */
export function validateStartParams(params: unknown): WireStartRequest {
  const object = requireObject(params);
  const mode = requireString(object, "mode");
  if (mode !== "plan" && mode !== "act") {
    throw invalidParams();
  }
  const toolPolicies = object["tool_policies"];
  if (!isJsonObject(toolPolicies)) {
    throw invalidParams();
  }
  const initialMessagesValue = object["initial_messages"];
  let initialMessages: JsonObject[] | undefined;
  if (initialMessagesValue !== undefined && initialMessagesValue !== null) {
    if (!Array.isArray(initialMessagesValue)) {
      throw invalidParams();
    }
    initialMessages = initialMessagesValue.filter(isJsonObject);
    if (initialMessages.length !== initialMessagesValue.length) {
      throw invalidParams();
    }
  }
  const request: WireStartRequest = {
    provider_id: requireNonEmptyString(object, "provider_id"),
    model_id: requireNonEmptyString(object, "model_id"),
    mode,
    rules: requireString(object, "rules"),
    system_prompt: requireString(object, "system_prompt"),
    cwd: requireNonEmptyString(object, "cwd"),
    workspace_root: requireNonEmptyString(object, "workspace_root"),
    enable_tools: requireBoolean(object, "enable_tools"),
    interactive: requireBoolean(object, "interactive"),
    tool_policies: toolPolicies,
  };
  const sessionId = optionalString(object, "session_id");
  if (sessionId !== undefined) {
    request.session_id = sessionId;
  }
  if (initialMessages !== undefined) {
    request.initial_messages = initialMessages;
  }
  return request;
}

/** `send` params: exact target session ID and verbatim prompt. */
export function validateSendParams(params: unknown): { session_id: string; prompt: string } {
  const object = requireObject(params);
  return { session_id: requireSessionId(object), prompt: requireString(object, "prompt") };
}

/** `abort` params: exact target session ID with optional opaque reason. */
export function validateAbortParams(params: unknown): { session_id: string; reason?: string | null } {
  const object = requireObject(params);
  const sessionId = requireSessionId(object);
  const reasonValue = object["reason"];
  let reason: string | null | undefined;
  if (reasonValue !== undefined) {
    if (reasonValue === null) {
      reason = null;
    } else if (typeof reasonValue === "string") {
      reason = reasonValue;
    } else {
      throw invalidParams();
    }
  }
  return reason === undefined ? { session_id: sessionId } : { session_id: sessionId, reason };
}

/** `subscribe` params: exact session ID or null for unfiltered delivery. */
export function validateSubscribeParams(params: unknown): { session_id: string | null } {
  const object = requireObject(params);
  const value = object["session_id"];
  if (value === null) {
    return { session_id: null };
  }
  return { session_id: requireSessionId(object) };
}

/** `unsubscribe` params: the bridge-local opaque subscription ID. */
export function validateUnsubscribeParams(params: unknown): { subscription_id: string } {
  const object = requireObject(params);
  return { subscription_id: requireNonEmptyString(object, "subscription_id") };
}