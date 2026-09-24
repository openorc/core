"""Deterministic tests for the GitHub capability contract (issue #58).

Pure contract tests prove the semantic capability mapping (GitHub permission
dictionaries mapped inside the adapter boundary), the least-privilege
required v1 capability set (including that exact-head merge authority
derives from ``contents: write`` and never from the pull-request
permission), the webhook-event requirement, and the stable-ID normalization
primitives. Client-flow tests prove the two strictly separated validation
fact sources: the JWT-authenticated installation object supplies
permission/event/suspension authority while the paginated
``GET /installation/repositories`` listing proves repository membership by
stable ID only. No live GitHub network access.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from openorc.adapters.github.authentication import GitHubAppAuthenticator
from openorc.adapters.github.capabilities import (
    REQUIRED_V1_WEBHOOK_EVENTS,
    REQUIRED_V1_WORKFLOW_CAPABILITIES,
    GitHubAccessValidation,
    GitHubWorkflowCapability,
    map_installation_permissions,
    missing_required_capabilities,
    missing_required_webhook_events,
    parse_installation_payload,
    parse_installation_repositories_page,
    require_positive_int,
)
from openorc.adapters.github.client import HttpGitHubAppClient
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
)
from openorc.adapters.github.transport import GITHUB_API_BASE_URL, HttpGitHubRestClient

_NOW = 1_000_000.0


class FakeClock:
    """Deterministic clock seam."""

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


def _key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _json(
    status: int, headers: Mapping[str, str], payload: Any
) -> tuple[int, Mapping[str, str], bytes]:
    return status, dict(headers), json.dumps(payload).encode()


def _mint_response(token: str = "ghs_listing_token") -> tuple[int, Mapping[str, str], bytes]:
    expires_at = datetime.fromtimestamp(_NOW + 3000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return _json(201, {}, {"token": token, "expires_at": expires_at})


# The full documented v1 permission set: exactly what the settled workflow
# needs, at least privilege (no administration write, no broad admin).
_FULL_V1_PERMISSIONS = {
    "issues": "write",
    "contents": "write",
    "pull_requests": "write",
    "checks": "read",
    "metadata": "read",
}
_FULL_V1_EVENTS = [
    "issues",
    "issue_comment",
    "pull_request",
    "push",
    "status",
    "check_run",
    "check_suite",
]


def _installation_payload(
    permissions: dict[str, str] | None = None,
    events: list[str] | None = None,
    suspended_at: str | None = None,
    installation_id: int = 4242,
) -> dict[str, Any]:
    return {
        "id": installation_id,
        "permissions": _FULL_V1_PERMISSIONS if permissions is None else permissions,
        "events": _FULL_V1_EVENTS if events is None else events,
        "suspended_at": suspended_at,
    }


def _listing_payload(
    repository_ids: list[int], extra_permissions: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "total_count": len(repository_ids),
        "repositories": [
            {
                "id": repository_id,
                "node_id": "R_dummy",
                "name": "hello-world",
                # The ordinary repository access shape — deliberately present
                # in fixtures to prove it is never consumed as capability
                # authority.
                "permissions": extra_permissions
                if extra_permissions is not None
                else {"admin": False, "push": False, "pull": True},
            }
            for repository_id in repository_ids
        ],
    }


def _client(fetch: FakeFetcher, clock: FakeClock) -> HttpGitHubAppClient:
    authenticator = GitHubAppAuthenticator(
        app_id=12345, private_key_pem=_key_pem(), clock=clock, fetch=fetch
    )
    transport = HttpGitHubRestClient(fetch=fetch)
    return HttpGitHubAppClient(authenticator=authenticator, transport=transport)


def test_required_v1_capability_set_is_least_privilege_and_includes_contents_write() -> None:
    assert GitHubWorkflowCapability.CONTENTS_WRITE in REQUIRED_V1_WORKFLOW_CAPABILITIES
    assert GitHubWorkflowCapability.METADATA_READ in REQUIRED_V1_WORKFLOW_CAPABILITIES
    assert GitHubWorkflowCapability.ISSUE_WRITE in REQUIRED_V1_WORKFLOW_CAPABILITIES
    assert GitHubWorkflowCapability.PULL_REQUEST_WRITE in REQUIRED_V1_WORKFLOW_CAPABILITIES
    assert GitHubWorkflowCapability.CHECKS_READ in REQUIRED_V1_WORKFLOW_CAPABILITIES
    assert GitHubWorkflowCapability.REPOSITORY_READ in REQUIRED_V1_WORKFLOW_CAPABILITIES
    # Least privilege: exactly the six settled v1 capabilities, no more.
    assert len(REQUIRED_V1_WORKFLOW_CAPABILITIES) == 6


def test_full_v1_permission_set_satisfies_the_required_capabilities() -> None:
    capabilities = map_installation_permissions(_FULL_V1_PERMISSIONS)

    assert missing_required_capabilities(capabilities) == frozenset()


def test_merge_authority_requires_contents_write_not_the_pull_request_permission() -> None:
    # GitHub's documented exact-head merge contract requires Contents write;
    # the pull-request permission alone must never supply merge authority.
    capabilities = map_installation_permissions({"pull_requests": "write", "contents": "read"})

    assert GitHubWorkflowCapability.PULL_REQUEST_WRITE in capabilities
    assert GitHubWorkflowCapability.CONTENTS_WRITE not in capabilities
    missing = missing_required_capabilities(capabilities)
    assert GitHubWorkflowCapability.CONTENTS_WRITE in missing


def test_contents_write_grants_repository_read_and_merge_authority() -> None:
    capabilities = map_installation_permissions({"contents": "write"})

    assert GitHubWorkflowCapability.REPOSITORY_READ in capabilities
    assert GitHubWorkflowCapability.CONTENTS_WRITE in capabilities


def test_unknown_permission_keys_grant_no_semantic_capability() -> None:
    # Least privilege: keys GitHub reports that OpenOrc's v1 workflow does not
    # consume (including administration) never map into semantic capabilities.
    capabilities = map_installation_permissions(
        {"administration": "write", "deployments": "write", "unknown_key": "write"}
    )

    assert capabilities == frozenset()


def test_non_string_permission_values_are_uninterpretable() -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        map_installation_permissions({"issues": 2})


def test_required_webhook_events_are_a_single_explicit_set() -> None:
    assert "issues" in REQUIRED_V1_WEBHOOK_EVENTS
    assert "pull_request" in REQUIRED_V1_WEBHOOK_EVENTS

    assert missing_required_webhook_events(frozenset(_FULL_V1_EVENTS)) == frozenset()
    missing = missing_required_webhook_events(frozenset({"issues", "pull_request"}))
    assert "check_run" in missing
    assert "push" in missing


def test_installation_payload_normalization_binds_to_the_exact_identity() -> None:
    capabilities = parse_installation_payload(
        _installation_payload(suspended_at="2026-09-24T01:02:03Z"), github_installation_id=4242
    )

    assert capabilities.github_installation_id == 4242
    assert missing_required_capabilities(capabilities.capabilities) == frozenset()
    assert capabilities.subscribed_events == frozenset(_FULL_V1_EVENTS)
    assert capabilities.suspended_at == datetime(2026, 9, 24, 1, 2, 3, tzinfo=UTC)


def test_installation_payload_rejects_a_reported_identity_mismatch() -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_installation_payload(
            _installation_payload(installation_id=999), github_installation_id=4242
        )


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"id": True, "permissions": {}, "events": []},
        {"id": 4242, "events": []},
        {"id": 4242, "permissions": {}, "events": "not-a-list"},
        {"id": 4242, "permissions": {}, "events": [1]},
        {"id": 4242, "permissions": {}, "events": [], "suspended_at": 5},
        {"id": 4242, "permissions": {}, "events": [], "suspended_at": "not-a-date"},
        {"id": 4242, "permissions": {}, "events": [], "suspended_at": "2026-09-24T01:02:03"},
    ],
)
def test_uninterpretable_installation_payloads_classify_as_uncertain(
    payload: dict[str, Any],
) -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_installation_payload(payload, github_installation_id=4242)


def test_repository_listing_normalization_returns_only_stable_ids() -> None:
    page = parse_installation_repositories_page(_listing_payload([111, 222]))

    assert page == [111, 222]


def test_repository_listing_ignores_the_per_entry_permissions_member() -> None:
    # Regression guard for the capability-source separation: the listing's
    # ordinary repository access shape is never capability authority.
    page = parse_installation_repositories_page(
        _listing_payload([111], extra_permissions={"admin": True, "push": True, "pull": True})
    )

    assert page == [111]
    assert all(isinstance(repository_id, int) for repository_id in page)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"repositories": "not-a-list"},
        {"repositories": ["not-a-dict"]},
        {"repositories": [{"name": "x"}]},
        {"repositories": [{"id": True}]},
        {"repositories": [{"id": "111"}]},
        {"repositories": [{"id": 0}]},
    ],
)
def test_uninterpretable_listing_pages_classify_as_uncertain(payload: dict[str, Any]) -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_installation_repositories_page(payload)


@pytest.mark.parametrize("value", [0, -1, True, False, "1", 1.5, None])
def test_require_positive_int_rejects_non_positive_integers(value: Any) -> None:
    with pytest.raises(ValueError):
        require_positive_int(value, "github_installation_id")


def test_require_positive_int_accepts_stable_identifiers() -> None:
    assert require_positive_int(4242, "github_installation_id") == 4242


def test_validation_targets_the_exact_routed_installation_and_documented_paths() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(200, {}, _listing_payload([424200, 987654321])),
        ]
    )

    _client(fetch, FakeClock()).validate_installation_repository_access(
        github_installation_id=4242, github_repository_id=987654321
    )

    lookup_url, lookup_method, _, _ = fetch.calls[0]
    mint_url, mint_method, _, _ = fetch.calls[1]
    listing_url, listing_method, _, _ = fetch.calls[2]
    # Exact installation routing: the token is minted for the addressed
    # installation only, and the listing proves membership for it.
    assert lookup_url == f"{GITHUB_API_BASE_URL}/app/installations/4242"
    assert lookup_method == "GET"
    assert mint_url.endswith("/app/installations/4242/access_tokens")
    assert mint_method == "POST"
    assert listing_url == f"{GITHUB_API_BASE_URL}/installation/repositories?per_page=100"
    assert listing_method == "GET"


def test_validation_returns_the_granted_facts_through_paginated_membership() -> None:
    page_two_url = f"{GITHUB_API_BASE_URL}/installation/repositories?per_page=100&page=2"
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            # The expected repository appears on the second documented page.
            _json(200, {"Link": f'<{page_two_url}>; rel="next"'}, _listing_payload([111, 222])),
            _json(200, {}, _listing_payload([987654321])),
        ]
    )

    validation = _client(fetch, FakeClock()).validate_installation_repository_access(
        github_installation_id=4242, github_repository_id=987654321
    )

    assert isinstance(validation, GitHubAccessValidation)
    assert validation.github_installation_id == 4242
    assert validation.github_repository_id == 987654321
    assert missing_required_capabilities(validation.capabilities) == frozenset()
    assert validation.subscribed_events == frozenset(_FULL_V1_EVENTS)
    # The fetch call sequence: installation lookup (App JWT), token mint,
    # listing page 1, then listing page 2 via the documented Link target.
    assert fetch.calls[3][0] == page_two_url


def test_validation_credential_presentation_follows_the_documented_contract() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(200, {}, _listing_payload([987654321])),
        ]
    )

    _client(fetch, FakeClock()).validate_installation_repository_access(
        github_installation_id=4242, github_repository_id=987654321
    )

    _, _, lookup_headers, _ = fetch.calls[0]
    _, _, mint_headers, _ = fetch.calls[1]
    _, _, listing_headers, _ = fetch.calls[2]
    assert lookup_headers["Authorization"].startswith("Bearer ey")
    assert mint_headers["Authorization"].startswith("Bearer ey")
    assert listing_headers["Authorization"] == "Bearer ghs_listing_token"


def test_membership_absence_after_the_complete_listing_is_a_classified_denial() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(200, {}, _listing_payload([111])),
        ]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )


def test_suspension_short_circuits_before_token_minting_or_listing() -> None:
    fetch = FakeFetcher(
        [_json(200, {}, _installation_payload(suspended_at="2026-09-24T01:02:03Z"))]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )

    assert len(fetch.calls) == 1


def test_capability_shortfall_is_a_classified_denial_before_listing() -> None:
    # The installation object lacks contents:write — the documented merge
    # authority — so validation fails closed regardless of membership.
    fetch = FakeFetcher(
        [
            _json(
                200,
                {},
                _installation_payload(permissions={"issues": "write", "contents": "read"}),
            )
        ]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )

    assert len(fetch.calls) == 1


def test_missing_webhook_subscription_is_a_classified_denial() -> None:
    fetch = FakeFetcher([_json(200, {}, _installation_payload(events=["issues"]))])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )

    assert len(fetch.calls) == 1


def test_listing_entry_permissions_never_grant_capability_authority() -> None:
    # Even when the listing entries carry permissive access shapes, a
    # capability shortfall on the installation object denies the validation:
    # the listing proves membership only.
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload(permissions={"metadata": "read"})),
            _mint_response(),
            _json(200, {}, _listing_payload([987654321], extra_permissions={"admin": True})),
        ]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )

    # The listing was never reached: the capability shortfall denied first.
    assert len(fetch.calls) == 1


def test_authentication_rejection_on_a_listing_evicts_and_remints_once() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(token="ghs_stale_token"),
            (401, {}, b'{"message": "ghs_secret"}'),
            _mint_response(token="ghs_fresh_token"),
            _json(200, {}, _listing_payload([987654321])),
        ]
    )

    validation = _client(fetch, FakeClock()).validate_installation_repository_access(
        github_installation_id=4242, github_repository_id=987654321
    )

    assert validation.github_repository_id == 987654321
    # The bounded recovery: exactly one eviction and one re-mint.
    assert len(fetch.calls) == 5
    assert fetch.calls[2][2]["Authorization"] == "Bearer ghs_stale_token"
    assert fetch.calls[4][2]["Authorization"] == "Bearer ghs_fresh_token"


def test_a_second_authentication_rejection_is_a_known_failure() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            (401, {}, b"{}"),
            _mint_response(),
            (401, {}, b"{}"),
        ]
    )

    with pytest.raises(GitHubAuthenticationRejectedError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )

    assert len(fetch.calls) == 5


def test_pagination_targets_outside_the_api_boundary_classify_as_uncertain() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(
                200,
                {"Link": '<https://evil.example/steal>; rel="next"'},
                _listing_payload([111]),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )


def test_exceeding_the_bounded_listing_page_count_classifies_as_uncertain() -> None:
    # A pagination chain that never ends cannot answer the access question:
    # an incomplete listing is neither access granted nor denied.
    page_url = f"{GITHUB_API_BASE_URL}/installation/repositories?per_page=100&page=2"
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            *(
                _json(200, {"Link": f'<{page_url}>; rel="next"'}, _listing_payload([111]))
                for _ in range(101)
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch, FakeClock()).validate_installation_repository_access(
            github_installation_id=4242, github_repository_id=987654321
        )


def test_validation_results_never_carry_credential_material() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(token="ghs_secret_probe"),
            _json(200, {}, _listing_payload([987654321])),
        ]
    )

    validation = _client(fetch, FakeClock()).validate_installation_repository_access(
        github_installation_id=4242, github_repository_id=987654321
    )

    assert "ghs_secret_probe" not in repr(validation)
    assert "ghs_secret_probe" not in str(validation)
