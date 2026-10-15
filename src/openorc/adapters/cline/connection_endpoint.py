"""Cline Connection control-endpoint configuration boundary (issue #188).

The v1 Cline Connection schema defines the non-secret Owner configuration a
Connection's ``safe_config`` carries: the existing ``repository_base`` plus
one optional, validated ``control_endpoint`` — the reachable control endpoint
the **Core process** attaches to through the public SDK (a complete
``ws://`` or ``wss://`` URL with an explicit host and a valid endpoint path,
e.g. the pinned Hub shape ``ws://127.0.0.1:25463/hub``). This module is the
single parsing/validation primitive that the future configuration mutation
and read boundaries (G1) and the attachment wiring (D8b) compose; nothing
else interprets the endpoint value.

Contract:

- The endpoint key is ``control_endpoint`` (one spelling, everywhere). It is
  optional: a Connection whose ``safe_config`` carries no endpoint remains a
  valid stored Connection — it is simply not attachable until the Owner
  configures one. No endpoint value is ever invented for legacy rows.
- Validation is structural, and the validated value is preserved **verbatim**:
  no normalization, no rewriting, no host/case folding. What the Owner
  configured is exactly what persists and what the attachment leaf reads.
- Secret-bearing URL components are rejected outright: userinfo
  (``user:pass@``), query strings (a common credential carrier), and
  fragments. Only ``scheme://host[:port]/path`` is representable, with
  ``ws``/``wss`` the only supported schemes. Whitespace and control
  characters anywhere in the value are rejected (a WebSocket endpoint never
  legitimately carries them), as are malformed hosts and ports.
- Parsing is **never a safety property**. A syntactically valid endpoint says
  nothing about where it actually leads; reaching it must be bounded by
  dial-time egress/SSRF policy at the later attachment leaf (issue #188).
  This module validates shape only and performs no network action of any
  kind.
- Errors carry only safe content: they name the failing aspect (scheme, host,
  path, component) and never echo the raw configured value, so a malformed
  URL cannot leak secret-bearing material through an error message.
"""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

__all__ = [
    "CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY",
    "ConnectionControlEndpointError",
    "parse_control_endpoint_url",
    "resolve_connection_control_endpoint",
]

#: The one safe_config key carrying the Cline control endpoint (issue #188).
CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY = "control_endpoint"

_SUPPORTED_SCHEMES = ("ws", "wss")


class ConnectionControlEndpointError(ValueError):
    """The configured Connection control endpoint is absent in shape or malformed."""


def parse_control_endpoint_url(value: object) -> str:
    """Validate one complete Cline control-endpoint URL and return it verbatim.

    The value must be a string carrying no whitespace or control characters
    and parsing as ``ws://`` or ``wss://`` with an explicit host, a valid
    endpoint path, and no secret-bearing URL components (userinfo, query, or
    fragment). The validated value is returned exactly as supplied — the
    parse never normalizes or rewrites it.
    """
    if not isinstance(value, str):
        raise ConnectionControlEndpointError("the control endpoint must be a string")
    if not value or any(
        character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F
        for character in value
    ):
        raise ConnectionControlEndpointError(
            "the control endpoint must be a non-empty URL without whitespace or control characters"
        )
    try:
        split = urlsplit(value)
    except ValueError:
        raise ConnectionControlEndpointError(
            "the control endpoint is not a parseable URL"
        ) from None
    if split.scheme not in _SUPPORTED_SCHEMES:
        raise ConnectionControlEndpointError(
            "the control endpoint scheme is unsupported; only ws:// and wss:// "
            "endpoints are supported"
        )
    try:
        # Deliberate validation read: urlsplit raises for a malformed or
        # out-of-range port.
        _ = split.port
    except ValueError:
        raise ConnectionControlEndpointError(
            "the control endpoint port is malformed or out of range"
        ) from None
    host = split.hostname
    if not host:
        raise ConnectionControlEndpointError("the control endpoint must include an explicit host")
    path = split.path
    if not path.startswith("/") or not path.lstrip("/"):
        raise ConnectionControlEndpointError(
            "the control endpoint must include a valid endpoint path"
        )
    if split.username is not None or split.password is not None:
        raise ConnectionControlEndpointError(
            "the control endpoint must not embed credentials in userinfo"
        )
    if split.query:
        raise ConnectionControlEndpointError(
            "the control endpoint must not carry a query component"
        )
    if split.fragment:
        raise ConnectionControlEndpointError(
            "the control endpoint must not carry a fragment component"
        )
    return value


def resolve_connection_control_endpoint(safe_config: Mapping[str, object]) -> str | None:
    """Resolve the Cline control endpoint from one Connection's ``safe_config``.

    Returns the validated endpoint string when configured, or ``None`` when
    the key is absent — an absent endpoint is a valid stored-but-not-
    attachable Connection state, never a fabricated default. A present value
    that is not a string, or that fails URL validation, is a typed rejection
    consumed at the mutation/read/attachment boundaries (issue #188).
    """
    if not isinstance(safe_config, Mapping):
        raise ConnectionControlEndpointError("the connection safe_config must be a mapping")
    if CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY not in safe_config:
        return None
    configured = safe_config[CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY]
    if not isinstance(configured, str):
        raise ConnectionControlEndpointError("a configured control endpoint must be a string")
    return parse_control_endpoint_url(configured)
