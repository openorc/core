"""JSON-compatible value types of the Cline SDK backend contract (issue #71).

``ClineSdkBackend`` (:mod:`openorc.adapters.cline.contract`) is the
language-neutral seam between the future Python Cline adapter (#74) and
whatever executes the qualified v1 ``@cline/sdk`` / ``ClineCore`` remote
operations — initially the Node JSON-RPC bridge (#72/#73), later possibly a
native Python SDK. Everything that crosses the eventual stdio bridge is a
JSON-compatible Python value: no SDK object references, JS classes, Node
handles, Python process objects, or provider-specific error instances appear
in the public backend API.

Inputs OpenOrc actually controls — the complete v1 construction bundle and
the remote attachment inputs — are narrow typed fields. SDK-native
observations and results whose exact nested contents D5/D6 must interpret
without loss stay opaque as :class:`JsonObject` values: forwarded verbatim,
never interpreted, projected, summarized, or re-shaped here.

Sensitive handling: the remote attachment auth token and SDK-native event
payloads are excluded from ``repr`` and never allowed into errors, logging,
events, or call diagnostics. The token is supplied by the caller — Cloud
resolves/reissues managed-Hub attach credentials and owns host/tunnel
lifecycle; this boundary only transports the current inputs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

__all__ = [
    "ClineConstructionMode",
    "ClineRemoteConfig",
    "ClineSessionEvent",
    "ClineStartRequest",
    "ClineStartResult",
    "JsonObject",
    "Subscription",
]

type JsonObject = dict[str, Any]


class ClineConstructionMode(StrEnum):
    """The effective construction mode of a v1 session.

    Restricted to the two modes the pinned baseline demonstrates for v1
    construction. This is the ``config.mode`` construction input,
    deliberately distinct from any SDK top-level session-source ``mode`` or
    a per-turn mode tag.
    """

    PLAN = "plan"
    ACT = "act"


def _require_non_empty_string(owner: str, name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{owner}.{name} must be a non-empty string")


def _require_json_object(owner: str, name: str, value: object) -> None:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{owner}.{name} must be None or a JSON object")


@dataclass(frozen=True)
class ClineRemoteConfig:
    """Explicit public remote-client inputs for one backend attachment.

    ``connect`` establishes or re-establishes the backend's client
    attachment to the already-running remote Cline Hub described here. Only
    required explicit public remote-client inputs are carried:

    - ``endpoint`` — the remote Hub endpoint;
    - ``auth_token`` — the current attach token when the remote requires
      one. Sensitive: excluded from ``repr`` and never allowed into errors,
      logging, events, or call diagnostics. Cloud — not this backend —
      resolves or reissues current managed-Hub attach credentials and owns
      host/tunnel lifecycle;
    - ``client_identity`` — the attaching client's explicit identity;
    - ``remote_options`` — opaque JSON object of demonstrated remote
      working-context options, forwarded verbatim and never interpreted
      here. Excluded from ``repr`` because its SDK-native contents may be
      sensitive.
    """

    endpoint: str
    client_identity: str
    auth_token: str | None = None
    remote_options: JsonObject | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string("ClineRemoteConfig", "endpoint", self.endpoint)
        _require_non_empty_string("ClineRemoteConfig", "client_identity", self.client_identity)
        if self.auth_token is not None and (
            not isinstance(self.auth_token, str) or not self.auth_token
        ):
            raise ValueError("ClineRemoteConfig.auth_token must be None or a non-empty string")
        if self.remote_options is not None:
            _require_json_object("ClineRemoteConfig", "remote_options", self.remote_options)

    def __repr__(self) -> str:
        return (
            f"ClineRemoteConfig(endpoint={self.endpoint!r}, "
            f"client_identity={self.client_identity!r}, auth_token=<redacted>, "
            f"remote_options=<"
            f"{'set' if self.remote_options is not None else 'absent'}>)"
        )


@dataclass(frozen=True)
class ClineStartRequest:
    """The complete v1 session-construction bundle, forwarded unchanged.

    Supplied by the adapter layer (D5a) and transported verbatim; the
    backend neither authors nor defaults any field:

    - ``provider_id``/``model_id`` — opaque identifiers OpenOrc selects;
    - ``mode`` — the effective construction mode (plan or act only); this
      is ``config.mode``, deliberately distinct from the SDK top-level
      session-source ``mode`` or a per-turn mode tag;
    - ``rules`` — role Markdown, and ``system_prompt`` — always explicitly
      present (the adapter supplies the required empty string);
    - ``cwd``/``workspace_root`` — the per-role working context;
    - ``enable_tools``/``interactive`` — the v1 construction booleans;
    - ``tool_policies`` — the full per-session tool-policy JSON object;
    - ``session_id`` — optional same external session ID for same-ID
      reconstruction (fresh construction leaves it absent);
    - ``initial_messages`` — optional raw persisted message array from
      ``read_messages``, round-tripped with its entire JSON structure,
      ordering, and nested fields preserved. Never summarized, projected,
      flattened, redacted in flight, or rebuilt from other observations.

    Nothing else is synthesized here: no ``enableSpawnAgent``/
    ``enableAgentTeams``, provider catalog, provider credentials, native
    tools/executors, approval callbacks, session prompt overrides, session
    history snapshots, or generic SDK configuration bags.
    """

    provider_id: str
    model_id: str
    mode: ClineConstructionMode
    rules: str
    system_prompt: str
    cwd: str
    workspace_root: str
    enable_tools: bool
    interactive: bool
    tool_policies: JsonObject
    session_id: str | None = None
    initial_messages: list[JsonObject] | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string("ClineStartRequest", "provider_id", self.provider_id)
        _require_non_empty_string("ClineStartRequest", "model_id", self.model_id)
        if not isinstance(self.mode, ClineConstructionMode):
            raise ValueError("ClineStartRequest.mode must be a ClineConstructionMode (plan or act)")
        if not isinstance(self.rules, str):
            raise ValueError("ClineStartRequest.rules must be a string")
        if not isinstance(self.system_prompt, str):
            raise ValueError(
                "ClineStartRequest.system_prompt must be a string and is always "
                "explicitly present (the adapter supplies the required empty string)"
            )
        _require_non_empty_string("ClineStartRequest", "cwd", self.cwd)
        _require_non_empty_string("ClineStartRequest", "workspace_root", self.workspace_root)
        if not isinstance(self.enable_tools, bool) or not isinstance(self.interactive, bool):
            raise ValueError("ClineStartRequest.enable_tools and .interactive must be booleans")
        _require_json_object("ClineStartRequest", "tool_policies", self.tool_policies)
        if self.session_id is not None:
            _require_non_empty_string("ClineStartRequest", "session_id", self.session_id)
        if self.initial_messages is not None and not (
            isinstance(self.initial_messages, list)
            and all(isinstance(item, dict) for item in self.initial_messages)
        ):
            raise ValueError(
                "ClineStartRequest.initial_messages must be None or a list of "
                "JSON objects carrying the raw persisted message array"
            )


@dataclass(frozen=True)
class ClineStartResult:
    """The SDK's allocation outcome for one ``start`` call.

    ``session_id`` is the exact opaque external session identifier the
    backend observed; same-ID reconstruction must keep it exactly. Backend
    ``start`` is allocation only, never OpenOrc readiness: ``session_ready``
    is not validated, the optional SDK ``result`` is not assumed to have
    executed an initial turn, and no ``AgentSessionCreated`` is produced.
    ``result`` is the optional SDK-native plain-JSON result object — or None
    for the SDK's legitimate undefined response — kept opaque.
    """

    session_id: str
    result: JsonObject | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string("ClineStartResult", "session_id", self.session_id)
        if self.result is not None:
            _require_json_object("ClineStartResult", "result", self.result)


@dataclass(frozen=True)
class ClineSessionEvent:
    """One forwarded ``CoreSessionEvent`` observation, retained verbatim.

    ``session_id`` is the event's own session identifier, ``kind`` its safe
    machine event kind, and ``payload`` the opaque JSON event payload. The
    payload is forwarded without interpretation — lifecycle, usage
    continuity, and workflow state are never derived here — and is excluded
    from ``repr`` because its SDK-native contents may be sensitive.
    """

    session_id: str
    kind: str
    payload: JsonObject

    def __post_init__(self) -> None:
        _require_non_empty_string("ClineSessionEvent", "session_id", self.session_id)
        _require_non_empty_string("ClineSessionEvent", "kind", self.kind)
        _require_json_object("ClineSessionEvent", "payload", self.payload)

    def __repr__(self) -> str:
        return (
            f"ClineSessionEvent(session_id={self.session_id!r}, "
            f"kind={self.kind!r}, payload=<withheld>)"
        )


class Subscription:
    """Handle for one explicit event subscription.

    Returned by ``ClineSdkBackend.subscribe``; ``unsubscribe`` is explicit
    and idempotent. Subscriptions are an event-forwarding contract, not a
    durable replay guarantee: on the pinned baseline a fresh client did not
    replay an already-running turn's missed mid-flight stream, and nothing
    here promises otherwise.
    """

    __slots__ = ("_active", "_cancel")

    def __init__(self, cancel: Callable[[], None]) -> None:
        self._cancel = cancel
        self._active = True

    def unsubscribe(self) -> None:
        """Stop event delivery for this subscription; safe to call again."""
        if self._active:
            self._active = False
            self._cancel()

    @property
    def active(self) -> bool:
        """Whether delivery is still active for this subscription."""
        return self._active
