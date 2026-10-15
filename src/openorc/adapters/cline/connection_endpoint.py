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
  legitimately carries them), as are malformed ports and malformed hosts:
  the host must be a supported DNS name (letters/digits/hyphen labels,
  no empty or hyphen-edge labels), a valid dotted-quad IPv4 literal, or a
  bracketed IPv6 literal — ``urlsplit`` alone does not validate hostname
  syntax, so dedicated checks reject malformed host characters and
  non-ASCII/IDN forms (IDNA is not a v1 endpoint capability).
- Parsing is **never a safety property**. A syntactically valid endpoint says
  nothing about where it actually leads. This module validates shape only and
  performs no network action of any kind; the SDK attachment (D8b, issue
  #190) performs no application-level egress/SSRF screening — protecting
  internal services and other tenants from Owner-configured endpoints is a
  deployment-operator obligation (infrastructure isolation and network
  controls).
- Errors carry only safe content: they name the failing aspect (scheme, host,
  path, component) and never echo the raw configured value, so a malformed
  URL cannot leak secret-bearing material through an error message.
"""

from __future__ import annotations

import ipaddress
import re
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


# One DNS label: letters/digits with interior hyphens only, 1-63 characters.
# The host arrives lowercased through urlsplit, so A-Z tolerance is harmless.
_HOSTNAME_LABEL_PATTERN = re.compile(r"(?!-)[A-Za-z0-9-]{1,63}(?<!-)")


def _validate_hostname(host: str) -> None:
    """Reject malformed hostname syntax urlsplit itself does not check.

    urlsplit decomposes URLs without validating hostname characters, so
    values like ``foo<script>`` or ``foo\\evil`` survive decomposition with a
    non-empty hostname. Supported v1 host forms are DNS names, dotted-quad
    IPv4 literals, and bracketed IPv6 literals; anything else — malformed
    host characters, non-ASCII/IDN forms, empty or hyphen-edge labels,
    invalid address literals — is a typed rejection. Errors name the failing
    form and never echo the host value.
    """
    if ":" in host:
        # Bracketed IPv6 literals arrive unbracketed through split.hostname;
        # a colon outside brackets fails port/host parsing earlier or lands
        # here as an invalid literal.
        try:
            ipaddress.IPv6Address(host)
        except ValueError:
            raise ConnectionControlEndpointError(
                "the control endpoint host is not a valid bracketed IPv6 literal"
            ) from None
        return
    if all(character in "0123456789." for character in host):
        # All-numeric/dotted forms are IPv4 attempts: require a valid
        # dotted-quad literal (rejects out-of-range and leading-zero octets).
        try:
            ipaddress.IPv4Address(host)
        except ValueError:
            raise ConnectionControlEndpointError(
                "the control endpoint host is not a valid IPv4 literal"
            ) from None
        return
    labels = host.split(".")
    if len(host) > 253 or any(_HOSTNAME_LABEL_PATTERN.fullmatch(label) is None for label in labels):
        raise ConnectionControlEndpointError(
            "the control endpoint host is not a valid DNS hostname"
        )


def parse_control_endpoint_url(value: object) -> str:
    """Validate one complete Cline control-endpoint URL and return it verbatim.

    The value must be a string carrying no whitespace or control characters
    and parsing as ``ws://`` or ``wss://`` with an explicit host in a
    supported form (DNS name, IPv4 literal, or bracketed IPv6 literal), a
    valid endpoint path, and no secret-bearing URL components (userinfo,
    query, or fragment). The validated value is returned exactly as
    supplied — the parse never normalizes or rewrites it.
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
    _validate_hostname(host)
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
