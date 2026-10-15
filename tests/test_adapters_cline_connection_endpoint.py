"""Cline Connection control-endpoint parsing/validation tests (issue #188).

Ordinary deterministic tests over the adapter-boundary primitive: the
validated ``ws``/``wss`` shapes with verbatim preservation, the rejections
(unsupported schemes, missing host or endpoint path, secret-bearing URL
components, whitespace/control characters, malformed ports, malformed IPv6
literals), the safe_config resolver contract (an absent key is a valid
stored-but-unattachable Connection state, never a fabricated default), and
that error messages name only the failing aspect and never echo the raw
configured value.
"""

from __future__ import annotations

from typing import Any

import pytest

from openorc.adapters.cline.connection_endpoint import (
    CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY,
    ConnectionControlEndpointError,
    parse_control_endpoint_url,
    resolve_connection_control_endpoint,
)


@pytest.mark.parametrize(
    "endpoint",
    [
        "ws://127.0.0.1:25463/hub",  # the pinned Hub shape
        "wss://hub.example.com/hub",
        "wss://hub.example.com:8443/hub",
        "ws://[2001:db8::1]:25463/hub",  # bracketed IPv6 host
        "ws://hub.example.com/hub/",
        "wss://hub.example.com/deep/endpoint/path",
    ],
)
def test_valid_endpoints_are_accepted_and_preserved_verbatim(endpoint: str) -> None:
    # Validation is structural only: the validated value is returned exactly
    # as supplied — no normalization, no rewriting, no case folding.
    assert parse_control_endpoint_url(endpoint) == endpoint


@pytest.mark.parametrize(
    "endpoint",
    ["WS://hub.example.com/hub", "WsS://hub.example.com/hub"],
)
def test_scheme_comparison_is_case_insensitive_and_value_stays_verbatim(
    endpoint: str,
) -> None:
    assert parse_control_endpoint_url(endpoint) == endpoint


@pytest.mark.parametrize(
    "bad_endpoint",
    [
        "",  # empty
        "   ",  # blank
        "ws://host/hub ",  # trailing whitespace
        " ws://host/hub",  # leading whitespace
        "ws://ho st/hub",  # interior whitespace
        "ws://host/hub\x00",  # control character
        7,  # non-string
        None,
        b"ws://host/hub",
        "http://hub.example.com/hub",  # unsupported schemes
        "https://hub.example.com/hub",
        "tcp://hub.example.com/hub",
        "ws:///hub",  # no explicit host
        "ws://:25463/hub",  # empty host component
        "ws://host",  # no path at all
        "ws://host/",  # slash-only path: no endpoint path
        "ws://user:pass@hub.example.com/hub",  # secret-bearing userinfo
        "ws://user@hub.example.com/hub",  # bare userinfo
        "ws://@hub.example.com/hub",  # empty userinfo
        "ws://hub.example.com/hub?token=super-secret-value",  # query component
        "ws://hub.example.com/hub#fragment",  # fragment component
        "ws://hub.example.com:abc/hub",  # malformed port
        "ws://hub.example.com:99999/hub",  # out-of-range port
    ],
)
def test_invalid_endpoints_are_rejected(bad_endpoint: Any) -> None:
    with pytest.raises(ConnectionControlEndpointError):
        parse_control_endpoint_url(bad_endpoint)


def test_malformed_ipv6_literal_is_a_typed_rejection() -> None:
    # urlsplit itself raises for a malformed IPv6 URL; the primitive
    # reclassifies it as the typed configuration error.
    with pytest.raises(ConnectionControlEndpointError):
        parse_control_endpoint_url("ws://[::1/hub")


def test_error_messages_never_echo_a_query_component() -> None:
    secretish = "ws://hub.example.com/hub?token=super-secret-value"
    with pytest.raises(ConnectionControlEndpointError) as excinfo:
        parse_control_endpoint_url(secretish)
    assert "super-secret-value" not in str(excinfo.value)
    assert secretish not in str(excinfo.value)


def test_error_messages_never_echo_userinfo_credentials() -> None:
    secretish = "ws://owner:topsecret@hub.example.com/hub"
    with pytest.raises(ConnectionControlEndpointError) as excinfo:
        parse_control_endpoint_url(secretish)
    assert "topsecret" not in str(excinfo.value)
    assert secretish not in str(excinfo.value)


def test_resolver_returns_absent_for_a_connection_without_an_endpoint() -> None:
    # A legacy Connection whose safe_config carries no endpoint is a valid
    # stored-but-unattachable state: None, never a fabricated default.
    assert resolve_connection_control_endpoint({}) is None
    assert (
        resolve_connection_control_endpoint({"repository_base": "/srv/openorc/repositories"})
        is None
    )


def test_resolver_returns_the_validated_value_verbatim() -> None:
    endpoint = "ws://127.0.0.1:25463/hub"
    resolved = resolve_connection_control_endpoint(
        {
            "repository_base": "/srv/openorc/repositories",
            CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY: endpoint,
        }
    )
    assert resolved == endpoint


def test_resolver_rejects_non_string_and_malformed_configured_values() -> None:
    with pytest.raises(ConnectionControlEndpointError):
        resolve_connection_control_endpoint({CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY: 7})
    with pytest.raises(ConnectionControlEndpointError):
        resolve_connection_control_endpoint(
            {CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY: "http://hub.example.com/hub"}
        )
    with pytest.raises(ConnectionControlEndpointError):
        resolve_connection_control_endpoint(
            {CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY: "ws://user:pass@host/hub"}
        )


def test_resolver_requires_a_mapping() -> None:
    with pytest.raises(ConnectionControlEndpointError):
        resolve_connection_control_endpoint(["not", "a", "mapping"])  # type: ignore[arg-type]


def test_the_config_key_is_the_single_control_endpoint_spelling() -> None:
    assert CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY == "control_endpoint"
