"""Deterministic tests for the Supabase Auth access-token verifier (issue #52).

Ordinary tests never touch the network: ES256/RSA keypairs are generated with
the pinned ``cryptography`` dev dependency, a deterministic fake
``JwksClient`` serves the locally built JWKS, and ``HttpJwksClient`` is
exercised through injected ``fetch``/``clock`` fakes. The suite proves the
verifier's token-level contract: asymmetric algorithm allowlist, signature/
issuer/audience/role validation, explicit anonymous rejection, the GitHub-only
trusted ``app_metadata`` policy, safe error content, and the JWKS cache
boundary.
"""

from __future__ import annotations

import json
import time
import urllib.error
from typing import Any, cast
from uuid import UUID, uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt import PyJWK
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

from openorc.adapters.supabase import (
    HttpJwksClient,
    SupabaseAccessTokenRejectedError,
    SupabaseAccessTokenVerifier,
    SupabaseJwksOutcomeUnknownError,
    SupabaseJwksUnavailableError,
)
from openorc.adapters.supabase.auth import DEFAULT_JWKS_CACHE_TTL_SECONDS
from openorc.config import ConfigurationError
from openorc.domain.identity import AuthenticatedPrincipal

PROJECT_URL = "https://example.supabase.co"
ISSUER = "https://example.supabase.co/auth/v1"
JWKS_URL = ISSUER + "/.well-known/jwks.json"
AUDIENCE = "authenticated"


def _now() -> int:
    # Real clock time: exp/nbf are validated against the system clock by the
    # JWT library, and tokens are verified immediately in these tests.
    return int(time.time())


def _ec_private_key() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(ec.SECP256R1())


def _rsa_private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwk_entry(private_key: Any, kid: str, algorithm: str) -> dict[str, Any]:
    """Build the JWKS entry for a keypair via the JWT library's serializer."""
    if algorithm.startswith("ES"):
        entry = ECAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    else:
        entry = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    entry.update({"kid": kid, "alg": algorithm, "use": "sig"})
    return entry


class FakeJwksClient:
    """Deterministic in-memory JwksClient for the verifier boundary."""

    def __init__(self, entries: list[dict[str, Any]] | None = None) -> None:
        self._entries = list(entries) if entries is not None else []
        self.requested_kids: list[str | None] = []

    def get_signing_key(self, kid: str | None) -> PyJWK:
        self.requested_kids.append(kid)
        matches = [entry for entry in self._entries if entry.get("kid") == kid]
        if len(matches) != 1:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token was not signed by a "
                "known project signing key"
            )
        return PyJWK(matches[0])


class FailingJwksClient:
    """JwksClient that fails retrieval with the given normalized error."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    def get_signing_key(self, kid: str | None) -> PyJWK:
        raise self._error


def _base_claims() -> dict[str, Any]:
    """Claims of a valid GitHub-originated authenticated Supabase user."""
    return {
        "iss": ISSUER,
        "sub": str(uuid4()),
        "aud": AUDIENCE,
        "role": "authenticated",
        "exp": _now() + 300,
        "iat": _now(),
        "session_id": str(uuid4()),
        "is_anonymous": False,
        "app_metadata": {"provider": "github", "providers": ["github"]},
    }


def _sign(
    private_key: Any,
    claims: dict[str, Any],
    *,
    kid: str = "key-1",
    algorithm: str = "ES256",
) -> str:
    return jwt.encode(claims, private_key, algorithm=algorithm, headers={"kid": kid})


def _verifier(entries: list[dict[str, Any]] | None = None) -> SupabaseAccessTokenVerifier:
    return SupabaseAccessTokenVerifier(
        project_url=PROJECT_URL,
        audience=AUDIENCE,
        jwks_client=FakeJwksClient(entries),
    )


def _expect_rejection(token: str, verifier: SupabaseAccessTokenVerifier) -> str:
    with pytest.raises(SupabaseAccessTokenRejectedError) as excinfo:
        verifier.verify(token)
    return str(excinfo.value)


def test_valid_token_resolves_the_sub_uuid_principal() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    verifier = _verifier([_jwk_entry(private_key, "key-1", "ES256")])

    principal = verifier.verify(_sign(private_key, claims))

    assert principal == AuthenticatedPrincipal(user_id=UUID(claims["sub"]))


def test_valid_rsa_signed_token_resolves_the_principal() -> None:
    private_key = _rsa_private_key()
    claims = _base_claims()
    verifier = _verifier([_jwk_entry(private_key, "key-1", "RS256")])

    principal = verifier.verify(_sign(private_key, claims, algorithm="RS256"))

    assert principal.user_id == UUID(claims["sub"])


def test_principal_identity_ignores_presentation_metadata_changes() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    # Mutable presentation claims must never influence OpenOrc identity.
    claims["email"] = "brand-new-email@example.com"
    claims["user_metadata"] = {
        "user_name": "renamed-login",
        "full_name": "Renamed Person",
        "email": "renamed@example.com",
    }
    verifier = _verifier([_jwk_entry(private_key, "key-1", "ES256")])

    principal = verifier.verify(_sign(private_key, claims))

    assert principal.user_id == UUID(claims["sub"])


def test_expired_token_is_rejected() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["exp"] = _now() - 1
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "expired" in message


def test_wrong_issuer_is_rejected() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["iss"] = "https://other-project.supabase.co/auth/v1"
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "issuer" in message


def test_wrong_audience_is_rejected() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["aud"] = "some-other-audience"
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "audience" in message


@pytest.mark.parametrize("role", ["anon", "service_role", "authenticated_user"])
def test_wrong_role_is_rejected(role: str) -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["role"] = role
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "role" in message


def test_anonymous_token_with_authenticated_role_is_rejected() -> None:
    # Supabase anonymous users can carry the authenticated role; the explicit
    # trusted is_anonymous check rejects them regardless of role/audience or
    # any other claims.
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["is_anonymous"] = True
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "anonymous" in message


def test_malformed_subject_is_rejected() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["sub"] = "not-a-uuid"
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "subject" in message


def test_invalid_signature_is_rejected() -> None:
    real_key = _ec_private_key()
    forger_key = _ec_private_key()
    token = _sign(forger_key, _base_claims())

    message = _expect_rejection(token, _verifier([_jwk_entry(real_key, "key-1", "ES256")]))

    assert "signature" in message


def test_unknown_signing_key_is_rejected() -> None:
    private_key = _ec_private_key()
    token = _sign(private_key, _base_claims(), kid="rotated-away")

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "signing key" in message


def test_hs256_token_is_rejected_by_the_algorithm_allowlist() -> None:
    # Header alg is never trusted: HS256 shared-secret verification has no
    # code path at all, whatever the key material. The key is long enough to
    # avoid PyJWT's HMAC key-length warning; rejection happens on alg alone.
    private_key = _ec_private_key()
    token = _sign(
        "a-shared-secret-that-is-long-enough-for-hs256",
        _base_claims(),
        kid="key-1",
        algorithm="HS256",
    )

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "asymmetric" in message


def test_missing_github_provider_metadata_is_rejected() -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    del claims["app_metadata"]
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "account-origin" in message


@pytest.mark.parametrize(
    "app_metadata",
    [
        {"provider": "google", "providers": ["google"]},
        {"provider": "google", "providers": ["github"]},  # providers include github
        {"provider": "github"},  # missing providers list
        {"providers": ["github"]},  # missing provider
        {"providers": "github"},  # providers is not a list
    ],
)
def test_non_github_originated_accounts_are_rejected(app_metadata: dict[str, Any]) -> None:
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["app_metadata"] = app_metadata
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "GitHub-originated" in message


def test_github_account_with_additional_linked_providers_is_accepted() -> None:
    # The trusted primary provider is what matters; additional linked
    # providers do not invalidate GitHub origin.
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["app_metadata"] = {"provider": "github", "providers": ["github", "google"]}
    verifier = _verifier([_jwk_entry(private_key, "key-1", "ES256")])

    principal = verifier.verify(_sign(private_key, claims))

    assert principal.user_id == UUID(claims["sub"])


def test_hostile_user_metadata_cannot_grant_github_access() -> None:
    # Editable user_metadata never grants account origin: only trusted
    # app_metadata is consulted, and a non-GitHub trusted origin stays
    # rejected no matter what user_metadata claims.
    private_key = _ec_private_key()
    claims = _base_claims()
    claims["app_metadata"] = {"provider": "google", "providers": ["google"]}
    claims["user_metadata"] = {
        "provider": "github",
        "providers": ["github"],
        "user_name": "impersonated-login",
    }
    token = _sign(private_key, claims)

    message = _expect_rejection(token, _verifier([_jwk_entry(private_key, "key-1", "ES256")]))

    assert "GitHub-originated" in message


def test_empty_token_is_rejected() -> None:
    message = _expect_rejection("", _verifier())

    assert "empty" in message


@pytest.mark.parametrize(
    ("jwks_error", "expected_type"),
    [
        (
            SupabaseJwksUnavailableError("the signing-key source rejected the lookup"),
            SupabaseJwksUnavailableError,
        ),
        (
            SupabaseJwksOutcomeUnknownError("the signing-key source could not be reached"),
            SupabaseJwksOutcomeUnknownError,
        ),
    ],
)
def test_jwks_retrieval_failure_is_not_a_token_rejection(
    jwks_error: Exception, expected_type: type[Exception]
) -> None:
    private_key = _ec_private_key()
    token = _sign(private_key, _base_claims())
    verifier = SupabaseAccessTokenVerifier(
        project_url=PROJECT_URL,
        audience=AUDIENCE,
        jwks_client=FailingJwksClient(jwks_error),
    )

    with pytest.raises(expected_type) as excinfo:
        verifier.verify(token)

    assert not isinstance(excinfo.value, SupabaseAccessTokenRejectedError)
    assert "signing-key source" in str(excinfo.value)


def test_rejections_never_contain_the_bearer_token() -> None:
    private_key = _ec_private_key()
    verifier = _verifier([_jwk_entry(private_key, "key-1", "ES256")])
    expired = _sign(private_key, {**_base_claims(), "exp": _now() - 1})
    forged = _sign(_ec_private_key(), _base_claims())
    anonymous = _sign(private_key, {**_base_claims(), "is_anonymous": True})
    hostile = _sign(
        private_key,
        {**_base_claims(), "app_metadata": {"provider": "google", "providers": ["google"]}},
    )

    for token in (expired, forged, anonymous, hostile):
        message = _expect_rejection(token, verifier)
        assert token not in message
        assert token[:10] not in message
        assert token[-10:] not in message


def test_verifier_rejects_malformed_construction_configuration() -> None:
    with pytest.raises(ConfigurationError, match="project URL"):
        SupabaseAccessTokenVerifier(
            project_url="not-a-url", audience=AUDIENCE, jwks_client=FakeJwksClient([])
        )
    with pytest.raises(ConfigurationError, match="audience"):
        SupabaseAccessTokenVerifier(
            project_url=PROJECT_URL, audience="", jwks_client=FakeJwksClient([])
        )


def test_verifier_derives_the_issuer_and_jwks_url_from_the_project_url() -> None:
    # The verifier derives the Auth issuer/JWKS endpoints; a trailing slash on
    # the project URL must not change the issuer. The fake JWKS client serves
    # a real key so verification reaches claim validation.
    private_key = _ec_private_key()
    verifier = SupabaseAccessTokenVerifier(
        project_url=PROJECT_URL + "/",
        audience=AUDIENCE,
        jwks_client=FakeJwksClient([_jwk_entry(private_key, "key-1", "ES256")]),
    )

    claims = _base_claims()
    claims["iss"] = "https://elsewhere.example.org/auth/v1"
    message = _expect_rejection(_sign(private_key, claims), verifier)

    assert "issuer" in message


class FakeFetch:
    """Injectable JWKS fetcher recording calls; serves queued documents.

    When the queue is exhausted the last document is served indefinitely, so
    tests needing repeated identical fetches need not queue copies. An
    ``errors`` queue (settable at any time) makes subsequent calls raise the
    queued exception once each, before serving documents again.
    """

    def __init__(self, documents: list[bytes], errors: list[Exception] | None = None) -> None:
        self._documents = list(documents)
        self.errors: list[Exception] = list(errors) if errors is not None else []
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, timeout: float) -> bytes:
        self.calls.append((url, timeout))
        if self.errors:
            raise self.errors.pop(0)
        if len(self._documents) > 1:
            return self._documents.pop(0)
        return self._documents[0]


class FakeClock:
    """Deterministic monotonic clock."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def _jwks_document(private_key: Any, kid: str) -> bytes:
    return json.dumps({"keys": [_jwk_entry(private_key, kid, "ES256")]}).encode()


def _http_failure() -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        JWKS_URL, 503, "Service Unavailable", cast(Any, None), cast(Any, None)
    )


def test_http_client_caches_the_document_between_lookups() -> None:
    private_key = _ec_private_key()
    fetch = FakeFetch([_jwks_document(private_key, "key-1")])
    client = HttpJwksClient(JWKS_URL, clock=FakeClock(), fetch=fetch, cache_ttl_seconds=300.0)

    first = client.get_signing_key("key-1")
    second = client.get_signing_key("key-1")

    assert len(fetch.calls) == 1  # no live call per request: cache boundary
    assert fetch.calls[0][0] == JWKS_URL
    assert first.key.public_numbers() == second.key.public_numbers()


def test_http_client_refetches_after_ttl_expiry() -> None:
    fetch = FakeFetch([_jwks_document(_ec_private_key(), "key-1")])
    clock = FakeClock()
    client = HttpJwksClient(JWKS_URL, cache_ttl_seconds=10.0, clock=clock, fetch=fetch)

    client.get_signing_key("key-1")
    clock.now += 10.0
    client.get_signing_key("key-1")

    assert len(fetch.calls) == 2


def test_http_client_forces_a_single_refresh_for_unknown_kid_then_rejects() -> None:
    private_key = _ec_private_key()
    fetch = FakeFetch([_jwks_document(private_key, "key-1")])
    client = HttpJwksClient(JWKS_URL, clock=FakeClock(), fetch=fetch)

    with pytest.raises(SupabaseAccessTokenRejectedError, match="signing key"):
        client.get_signing_key("rotated-away")
    assert len(fetch.calls) == 2  # stale-cache safety: exactly one forced refresh

    # After the refresh confirms the key is still absent, the same lookup
    # rejects without further fetches.
    with pytest.raises(SupabaseAccessTokenRejectedError):
        client.get_signing_key("rotated-away")
    assert len(fetch.calls) == 2


@pytest.mark.parametrize(
    ("failures", "expected_type"),
    [
        (
            [
                _http_failure(),
                _http_failure(),
            ],
            SupabaseJwksUnavailableError,
        ),
        (
            [
                urllib.error.URLError("connection refused"),
                urllib.error.URLError("connection refused"),
            ],
            SupabaseJwksOutcomeUnknownError,
        ),
    ],
    ids=["known_failure", "unknown_outcome"],
)
def test_failed_forced_refresh_is_bounded_and_keeps_the_failure_classification(
    failures: list[Exception], expected_type: type[Exception]
) -> None:
    # Regression: the forced-refresh slot must be consumed by a FAILED
    # refresh too. Otherwise a JWKS outage during unknown-kid lookups is
    # amplified into a live fetch per token while the ordinary cache is still
    # fresh, and the outage gets misreported as invalid credentials.
    private_key = _ec_private_key()
    fetch = FakeFetch([_jwks_document(private_key, "key-1")])
    clock = FakeClock()
    client = HttpJwksClient(JWKS_URL, clock=clock, fetch=fetch)
    client.get_signing_key("key-1")
    assert len(fetch.calls) == 1  # cache warmed by one successful fetch

    fetch.errors = failures  # every fetch now fails, one error each

    # Unknown kid: the epoch's forced refresh fails with the retrieval error.
    with pytest.raises(expected_type):
        client.get_signing_key("rotated-away")
    assert len(fetch.calls) == 2

    # The failed refresh consumed the epoch: the next unknown-kid lookup
    # re-reports the same classified outcome WITHOUT another fetch — never
    # reclassified as a token rejection.
    with pytest.raises(expected_type):
        client.get_signing_key("rotated-away")
    assert len(fetch.calls) == 2

    # A known kid is still served from the last good document during the
    # failure epoch, without a fetch and without a failure.
    client.get_signing_key("key-1")
    assert len(fetch.calls) == 2

    # A new epoch permits exactly one new load attempt, bounded again.
    clock.now += DEFAULT_JWKS_CACHE_TTL_SECONDS
    with pytest.raises(expected_type):
        client.get_signing_key("rotated-away")
    assert len(fetch.calls) == 3
    with pytest.raises(expected_type):
        client.get_signing_key("rotated-away")
    assert len(fetch.calls) == 3


def test_failed_natural_load_is_bounded_per_epoch() -> None:
    # Regression: a failed natural (TTL-expired) load consumes its epoch the
    # same way, so an outage cannot issue one fetch per request.
    fetch = FakeFetch(
        [],
        errors=[
            urllib.error.URLError("connection refused"),
            urllib.error.URLError("connection refused"),
        ],
    )
    clock = FakeClock()
    client = HttpJwksClient(JWKS_URL, clock=clock, fetch=fetch)

    with pytest.raises(SupabaseJwksOutcomeUnknownError):
        client.get_signing_key("key-1")
    assert len(fetch.calls) == 1

    # Repeated lookups during the failure epoch report the cached outcome.
    for _ in range(3):
        with pytest.raises(SupabaseJwksOutcomeUnknownError):
            client.get_signing_key("key-1")
    assert len(fetch.calls) == 1


def test_http_client_rejects_kidless_tokens_against_ambiguous_sets() -> None:
    first = _jwk_entry(_ec_private_key(), "key-1", "ES256")
    second = _jwk_entry(_ec_private_key(), "key-2", "ES256")
    fetch = FakeFetch([json.dumps({"keys": [first, second]}).encode()])
    client = HttpJwksClient(JWKS_URL, clock=FakeClock(), fetch=fetch)

    with pytest.raises(SupabaseAccessTokenRejectedError):
        client.get_signing_key(None)  # ambiguous: never guessed


def test_http_client_maps_http_failure_to_known_retrieval_failure() -> None:
    def fetch(url: str, timeout: float) -> bytes:
        raise urllib.error.HTTPError(
            url, 503, "Service Unavailable", cast(Any, None), cast(Any, None)
        )

    client = HttpJwksClient(JWKS_URL, clock=FakeClock(), fetch=fetch)

    with pytest.raises(SupabaseJwksUnavailableError):
        client.get_signing_key("key-1")


@pytest.mark.parametrize(
    "raised",
    [
        urllib.error.URLError("connection refused"),
        TimeoutError("timed out"),
        OSError("unreachable"),
    ],
    ids=["url_error", "timeout", "os_error"],
)
def test_http_client_maps_transport_failure_to_unknown_outcome(raised: Exception) -> None:
    def fetch(url: str, timeout: float) -> bytes:
        raise raised

    client = HttpJwksClient(JWKS_URL, clock=FakeClock(), fetch=fetch)

    with pytest.raises(SupabaseJwksOutcomeUnknownError):
        client.get_signing_key("key-1")


@pytest.mark.parametrize("raw", [b"not json", b'{"nope": 1}', b'{"keys": []}', b'{"keys": ["x"]}'])
def test_unusable_documents_are_known_retrieval_failures(raw: bytes) -> None:
    client = HttpJwksClient(JWKS_URL, clock=FakeClock(), fetch=lambda url, timeout: raw)

    with pytest.raises(SupabaseJwksUnavailableError):
        client.get_signing_key("key-1")


def test_http_client_validates_construction_arguments() -> None:
    with pytest.raises(ConfigurationError):
        HttpJwksClient("")
    with pytest.raises(ConfigurationError, match="timeout"):
        HttpJwksClient(JWKS_URL, timeout_seconds=0, clock=FakeClock(), fetch=lambda u, t: b"{}")
    with pytest.raises(ConfigurationError, match="TTL"):
        HttpJwksClient(JWKS_URL, cache_ttl_seconds=0, clock=FakeClock(), fetch=lambda u, t: b"{}")


def test_end_to_end_verification_through_the_production_jwks_client() -> None:
    # Full verify() path with the real HTTP JWKS client behind a fake fetch:
    # real PyJWT key parsing and signature verification, no network.
    private_key = _ec_private_key()
    client = HttpJwksClient(
        JWKS_URL, clock=FakeClock(), fetch=lambda url, timeout: _jwks_document(private_key, "key-1")
    )
    verifier = SupabaseAccessTokenVerifier(
        project_url=PROJECT_URL, audience=AUDIENCE, jwks_client=client
    )
    claims = _base_claims()

    principal = verifier.verify(_sign(private_key, claims))

    assert principal.user_id == UUID(claims["sub"])


def test_supabase_adapter_never_imports_services() -> None:
    # Dependency direction: adapters never import application services. The
    # adapter-local error types are the normalized boundary; the service
    # translates them into the typed application vocabulary.
    import inspect
    import sys

    adapter_modules = [
        sys.modules[name]
        for name in sys.modules
        if name == "openorc.adapters.supabase" or name.startswith("openorc.adapters.supabase.")
    ]
    assert adapter_modules, "the supabase adapter modules must be importable"

    for module in adapter_modules:
        for line in inspect.getsource(module).splitlines():
            stripped = line.strip()
            if stripped.startswith(("import ", "from ")):
                assert "openorc.services" not in stripped, stripped
