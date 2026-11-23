"""Deterministic tests for the pending GitHub authorization flow domain (issue #373).

The ordinary suite cannot execute Postgres: these tests prove the non-secret
domain invariants — the authorize/reauthorize purpose vocabulary, the
non-negative fence evidence, the bounded expiry window, and that no
credential-bearing field exists on the durable record.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlow,
    GitHubUserAuthorizationFlowDomainError,
    GitHubUserAuthorizationFlowPurpose,
)

_STARTED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


def _flow(**overrides: object) -> GitHubUserAuthorizationFlow:
    values: dict[str, object] = {
        "profile_id": uuid.uuid4(),
        "purpose": GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
        "state": "a" * 43,
        "pkce_verifier_reference": "openorc:github-user-flow-verifier:v1:vault:"
        + str(uuid.uuid4()),
        "observed_revocation_count": 0,
        "started_at": _STARTED,
        "expires_at": _STARTED + timedelta(seconds=600),
    }
    values.update(overrides)
    return GitHubUserAuthorizationFlow(
        profile_id=values["profile_id"],  # type: ignore[arg-type]
        purpose=values["purpose"],  # type: ignore[arg-type]
        state=values["state"],  # type: ignore[arg-type]
        pkce_verifier_reference=values["pkce_verifier_reference"],  # type: ignore[arg-type]
        observed_revocation_count=values["observed_revocation_count"],  # type: ignore[arg-type]
        started_at=values["started_at"],  # type: ignore[arg-type]
        expires_at=values["expires_at"],  # type: ignore[arg-type]
    )


def test_a_pending_flow_represents_its_correlation_facts() -> None:
    flow = _flow(observed_revocation_count=2)

    assert flow.purpose is GitHubUserAuthorizationFlowPurpose.AUTHORIZE
    assert flow.observed_revocation_count == 2
    assert flow.expires_at > flow.started_at


def test_the_record_carries_no_verifier_or_credential_value_fields() -> None:
    # The pending flow is the non-secret correlation boundary: the PKCE
    # verifier value and every credential have no field — only the opaque
    # Vault reference pointer.
    fields = GitHubUserAuthorizationFlow.__dataclass_fields__
    assert "pkce_verifier_reference" in fields
    assert not any(field == "verifier" or "token" in field for field in fields)


def test_both_purposes_are_representable() -> None:
    authorize = _flow(purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE)
    reauthorize = _flow(purpose=GitHubUserAuthorizationFlowPurpose.REAUTHORIZE)

    assert authorize.purpose.value == "authorize"
    assert reauthorize.purpose.value == "reauthorize"


@pytest.mark.parametrize(
    "overrides",
    [
        {"purpose": "authorize"},  # type: ignore[dict-item]
        {"state": ""},
        {"pkce_verifier_reference": ""},
        {"observed_revocation_count": -1},
        {"observed_revocation_count": True},
        {"expires_at": _STARTED},  # not strictly after started_at
        {"expires_at": _STARTED - timedelta(seconds=1)},
    ],
)
def test_invalid_flow_invariants_fail_closed(overrides: dict[str, object]) -> None:
    with pytest.raises(GitHubUserAuthorizationFlowDomainError):
        _flow(**overrides)
