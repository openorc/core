"""Deterministic tests for the GitHub webhook signature verification boundary (issue #61).

Covers the required signature matrix over deterministic raw-body/header
fixtures: valid SHA-256 signatures, missing/malformed/incorrect signatures,
and the body-byte-change invalidation. Secret-safety assertions prove the
secret and signature material never appear in typed error messages or in the
holder's representation.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import pytest

from openorc.adapters.github.webhook_verification import (
    GitHubWebhookSecret,
    GitHubWebhookSignatureInvalidError,
    GitHubWebhookSignatureMalformedError,
    GitHubWebhookSignatureMissingError,
    GitHubWebhookSignatureVerifier,
)

SECRET_VALUE = "webhook-secret-material"
BODY = b'{"zen": "Keep it logically awesome."}'
OTHER_BODY = b'{"zen": "changed bytes"}'


def _signature(body: bytes, secret: str = SECRET_VALUE) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _verifier() -> GitHubWebhookSignatureVerifier:
    return GitHubWebhookSignatureVerifier(GitHubWebhookSecret(SECRET_VALUE))


def test_valid_sha256_signature_verifies() -> None:
    _verifier().verify(BODY, _signature(BODY))


def test_missing_signature_is_rejected() -> None:
    with pytest.raises(GitHubWebhookSignatureMissingError):
        _verifier().verify(BODY, None)


@pytest.mark.parametrize(
    "header",
    [
        "sha256",
        "sha256=",
        "sha1=" + "a" * 40,
        "md5=not-hex-at-all-what",
        "sha256=" + "G" * 64,
        "sha256=" + "a" * 63,
        "sha256=" + "a" * 65,
        "b" * 64,
    ],
)
def test_malformed_signatures_are_rejected(header: str) -> None:
    with pytest.raises(GitHubWebhookSignatureMalformedError):
        _verifier().verify(BODY, header)


def test_incorrect_signature_is_rejected() -> None:
    with pytest.raises(GitHubWebhookSignatureInvalidError):
        _verifier().verify(BODY, _signature(BODY, secret="wrong-secret"))


def test_body_byte_change_invalidates_the_signature() -> None:
    signature = _signature(BODY)

    with pytest.raises(GitHubWebhookSignatureInvalidError):
        _verifier().verify(OTHER_BODY, signature)


def test_a_wrong_secret_rejects_a_well_formed_signature() -> None:
    verifier = GitHubWebhookSignatureVerifier(GitHubWebhookSecret("deployment-secret"))
    signature = _signature(BODY, secret="attacker-secret")

    with pytest.raises(GitHubWebhookSignatureInvalidError):
        verifier.verify(BODY, signature)


def test_verification_is_constant_time_primitively_bound() -> None:
    # The comparison path is the standard library constant-time primitive:
    # the module binds to hmac.compare_digest rather than an equality check.
    import pathlib

    import openorc.adapters.github.webhook_verification as verification_module

    source = pathlib.Path(verification_module.__file__).read_text(encoding="utf-8")
    assert "hmac.compare_digest" in source


@pytest.mark.parametrize(
    ("error_type", "verify_call"),
    [
        (GitHubWebhookSignatureMissingError, lambda v: v.verify(BODY, None)),
        (GitHubWebhookSignatureMalformedError, lambda v: v.verify(BODY, "sha256=zz")),
        (
            GitHubWebhookSignatureInvalidError,
            lambda v: v.verify(BODY, _signature(BODY, secret="attacker-secret")),
        ),
    ],
)
def test_rejection_messages_carry_no_secret_or_signature_material(
    error_type: type[Exception], verify_call: Any
) -> None:
    signature = _signature(BODY)

    with pytest.raises(error_type) as exc_info:
        verify_call(_verifier())

    message = str(exc_info.value)
    assert SECRET_VALUE not in message
    assert signature not in message
    assert BODY.decode() not in message


def test_the_secret_holder_never_represents_its_value() -> None:
    secret = GitHubWebhookSecret(SECRET_VALUE)

    assert SECRET_VALUE not in repr(secret)
    assert SECRET_VALUE not in str(secret)
    assert secret.value() == SECRET_VALUE


def test_an_empty_secret_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError):
        GitHubWebhookSecret("")
