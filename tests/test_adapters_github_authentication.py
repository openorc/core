"""Deterministic tests for the GitHub App authentication boundary (issue #58).

A generated RSA keypair, an injected clock, and a scripted transport seam
prove the App JWT contract (RS256, App-ID issuer, bounded lifetime with
clock-drift allowance), the fail-fast construction rules, exact installation
routing for token minting, the bounded in-memory cache honoring GitHub's
expiry semantics at the clock seam, eviction on authentication rejection,
token redaction on every surface, and secret-safe errors. No live GitHub
network access.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from openorc.adapters.github.authentication import (
    INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS,
    GitHubAppAuthenticator,
)
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubOutcomeUncertainError,
)
from openorc.config import ConfigurationError

# The App ID and private key are deployment/bootstrap secret material; the
# tests generate a dedicated keypair and treat the PEM as the secret under
# test: every assertion proves it stays out of representations and errors.
_APP_ID = 12345
_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PRIVATE_KEY_PEM = _PRIVATE_KEY.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
).decode()

# The deterministic clock seam: mint tests read expiry relative to this instant.
_NOW = 1_000_000.0


class FakeClock:
    """Deterministic clock seam the cache expiry tests advance explicitly."""

    def __init__(self, start: float = _NOW) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class FakeFetcher:
    """Scripted transport seam recording calls; serves queued results."""

    def __init__(self, results: list[tuple[int, Mapping[str, str], bytes] | Exception]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str, dict[str, str], float]] = []

    def __call__(
        self, url: str, method: str, headers: Mapping[str, str], timeout_seconds: float
    ) -> tuple[int, Mapping[str, str], bytes]:
        self.calls.append((url, method, dict(headers), timeout_seconds))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _mint_body(
    token: str = "ghs_minted_installation_token", lifetime_seconds: float = 3000
) -> bytes:
    expires_at = datetime.fromtimestamp(_NOW + lifetime_seconds, tz=UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return json.dumps({"token": token, "expires_at": expires_at}).encode()


def _authenticator(fetch: FakeFetcher, clock: FakeClock | None = None) -> GitHubAppAuthenticator:
    return GitHubAppAuthenticator(
        app_id=_APP_ID,
        private_key_pem=_PRIVATE_KEY_PEM,
        clock=clock if clock is not None else FakeClock(),
        fetch=fetch,
    )


def test_app_jwt_carries_the_expected_contract_without_secret_leakage() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body())])
    authenticator = _authenticator(fetch)

    authenticator.mint_installation_token(4242)

    url, method, headers, _ = fetch.calls[0]
    assert method == "POST"
    assert url.endswith("/app/installations/4242/access_tokens")
    assert url.startswith("https://api.github.com/")
    authorization = headers["Authorization"]
    assert authorization.startswith("Bearer ")
    presented_jwt = authorization.removeprefix("Bearer ")
    # The App private key never travels anywhere but the JWT signature.
    assert "PRIVATE KEY" not in authorization
    claims = jwt.decode(presented_jwt, options={"verify_signature": False})
    assert claims["iss"] == str(_APP_ID)
    assert claims["iat"] == int(_NOW) - 60
    assert claims["exp"] == int(_NOW) + 540
    assert claims["exp"] - claims["iat"] == 600  # inside GitHub's 10-minute maximum
    header = jwt.get_unverified_header(presented_jwt)
    assert header["alg"] == "RS256"


def test_app_jwt_creation_performs_no_transport_call() -> None:
    fetch = FakeFetcher([])

    _authenticator(fetch).app_jwt()

    assert fetch.calls == []


def test_mint_parses_the_documented_token_response() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body(token="ghs_exact_token"))])

    token = _authenticator(fetch).mint_installation_token(4242)

    assert token.token_value() == "ghs_exact_token"
    assert token.expires_at == datetime.fromtimestamp(_NOW + 3000, tz=UTC)


def test_construction_fails_fast_on_invalid_app_identity() -> None:
    bad_app_ids: list[Any] = [0, -1, True, "1", None]
    for bad_app_id in bad_app_ids:
        with pytest.raises(ConfigurationError):
            GitHubAppAuthenticator(app_id=bad_app_id, private_key_pem=_PRIVATE_KEY_PEM)


def test_construction_fails_fast_on_blank_or_unparseable_private_keys() -> None:
    for bad_pem in ("", "   ", "not-a-key"):
        with pytest.raises(ConfigurationError):
            GitHubAppAuthenticator(app_id=_APP_ID, private_key_pem=bad_pem)


def test_private_key_failure_messages_stay_secret_safe() -> None:
    with pytest.raises(ConfigurationError) as caught:
        GitHubAppAuthenticator(app_id=_APP_ID, private_key_pem="not-a-key")

    assert "not-a-key" not in str(caught.value)
    assert "BEGIN" not in str(caught.value)


def test_mint_targets_the_exact_installation_id() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body())])

    _authenticator(fetch).mint_installation_token(987654)

    assert fetch.calls[0][0].endswith("/app/installations/987654/access_tokens")


def test_cached_token_serves_within_validity_without_reminting() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body())])
    clock = FakeClock()
    authenticator = _authenticator(fetch, clock)

    first = authenticator.installation_token(4242)
    second = authenticator.installation_token(4242)

    assert first is second
    assert len(fetch.calls) == 1


def test_expired_token_is_reminted_at_the_clock_seam() -> None:
    fetch = FakeFetcher(
        [
            (201, {}, _mint_body(lifetime_seconds=3000)),
            (201, {}, _mint_body(lifetime_seconds=3000)),
        ]
    )
    clock = FakeClock()
    authenticator = _authenticator(fetch, clock)

    first = authenticator.installation_token(4242)
    clock.now += 3000 + INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS
    second = authenticator.installation_token(4242)

    assert first is not second
    assert len(fetch.calls) == 2


def test_safety_margin_keeps_a_token_unusable_near_expiry() -> None:
    # A token expiring within the safety margin is never served from cache:
    # a live request could otherwise present a token that expires mid-flight.
    fetch = FakeFetcher([(201, {}, _mint_body(lifetime_seconds=30)), (201, {}, _mint_body())])
    authenticator = _authenticator(fetch)

    authenticator.installation_token(4242)
    authenticator.installation_token(4242)

    assert len(fetch.calls) == 2


def test_invalidate_evicts_the_cached_token() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body()), (201, {}, _mint_body())])
    clock = FakeClock()
    authenticator = _authenticator(fetch, clock)

    first = authenticator.installation_token(4242)
    authenticator.invalidate_installation_token(4242)
    second = authenticator.installation_token(4242)

    assert first is not second
    assert len(fetch.calls) == 2


def test_installations_are_cached_independently_by_stable_id() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body()), (201, {}, _mint_body())])
    authenticator = _authenticator(fetch)

    first = authenticator.installation_token(1)
    second = authenticator.installation_token(2)

    assert first is not second
    assert {call[0].rsplit("/", 2)[-2] for call in fetch.calls} == {"1", "2"}


def test_installation_tokens_are_redacted_on_every_ordinary_surface() -> None:
    fetch = FakeFetcher([(201, {}, _mint_body(token="ghs_redaction_probe"))])
    authenticator = _authenticator(fetch)

    token = authenticator.mint_installation_token(4242)

    assert "ghs_redaction_probe" not in repr(token)
    assert "ghs_redaction_probe" not in str(token)
    assert "PRIVATE KEY" not in repr(authenticator)
    assert "PRIVATE KEY" not in str(authenticator)


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b'["unexpected"]',
        b'{"expires_at": "2026-09-24T12:00:00Z"}',
        b'{"token": "ghs_x"}',
        b'{"token": "ghs_x", "expires_at": "not-a-date"}',
        b'{"token": "ghs_x", "expires_at": "2026-09-24T12:00:00"}',
    ],
)
def test_uninterpretable_mint_responses_classify_as_uncertain(body: bytes) -> None:
    fetch = FakeFetcher([(201, {}, body)])

    with pytest.raises(GitHubOutcomeUncertainError):
        _authenticator(fetch).mint_installation_token(4242)


def test_mint_authentication_rejections_are_known_failures_with_safe_messages() -> None:
    fetch = FakeFetcher([(401, {}, b'{"message": "ghs_secret"}')])

    with pytest.raises(GitHubAuthenticationRejectedError) as caught:
        _authenticator(fetch).mint_installation_token(4242)

    assert "ghs_secret" not in str(caught.value)
    assert "PRIVATE KEY" not in str(caught.value)


def test_nonpositive_installation_ids_are_caller_contract_violations() -> None:
    authenticator = _authenticator(FakeFetcher([]))

    with pytest.raises(ValueError):
        authenticator.mint_installation_token(0)
