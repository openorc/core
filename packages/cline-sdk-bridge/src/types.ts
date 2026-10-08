/** Shared wire-level JSON types of the Cline SDK bridge (issue #72).
 *
 * Everything crossing the JSON-RPC 2.0 stdio boundary is a JSON value. The
 * D2 Python contract (`src/openorc/adapters/cline/values.py`) is the source
 * of the wire shapes; the bridge transports them verbatim and never
 * interprets their contents.
 */

export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;

export interface JsonObject {
  [key: string]: JsonValue;
}

export type JsonArray = JsonValue[];

/** The five D2 backend failure classifications carried in `error.data.kind`.
 *
 * These map one-to-one onto `openorc.adapters.cline.errors`:
 *
 * - `remote_attachment_rejected`       -> ClineRemoteAttachmentRejectedError
 * - `remote_attachment_unavailable`    -> ClineRemoteAttachmentUnavailableError
 * - `uncertain_outcome`                -> ClineBackendUncertainOutcomeError
 * - `session_not_found`                -> ClineSessionNotFoundError
 * - `sdk_operation_rejected`           -> ClineSdkOperationRejectedError
 */
export type WireBackendErrorKind =
  | "remote_attachment_rejected"
  | "remote_attachment_unavailable"
  | "uncertain_outcome"
  | "session_not_found"
  | "sdk_operation_rejected";

/** Bounded reasons of wire-protocol errors. These never carry native error
 * text; D4 recognizes them as wire errors rather than backend errors. */
export type WireProtocolReason =
  | "parse_error"
  | "invalid_request"
  | "method_not_found"
  | "invalid_params"
  | "not_attached";

/** Wire shape of `ClineRemoteConfig` (connect params, flattened). */
export type WireRemoteConfig = {
  endpoint: string;
  client_identity: string;
  auth_token?: string;
  remote_options?: JsonObject;
};

/** Wire shape of `ClineStartRequest` (start params, flattened). */
export type WireStartRequest = {
  provider_id: string;
  model_id: string;
  mode: "plan" | "act";
  rules: string;
  system_prompt: string;
  cwd: string;
  workspace_root: string;
  enable_tools: boolean;
  interactive: boolean;
  tool_policies: JsonObject;
  session_id?: string;
  initial_messages?: JsonObject[];
};

/** Wire shape of the D2 `ClineStartResult`. */
export type WireStartResult = {
  session_id: string;
  result: JsonObject | null;
};

/** Wire params of the asynchronous `cline.event` notification. */
export type WireSessionEventParams = {
  subscription_id: string;
  session_id: string;
  kind: string;
  payload: JsonObject;
};

export function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}