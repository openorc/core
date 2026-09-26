"""Authenticated GitHub webhook signature verification boundary (issue #61).

The focused ingress-authentication boundary required by the thin FastAPI
webhook route. Verification runs over the EXACT raw request bytes against the
deployment's configured GitHub App webhook secret, using the standard library
``hmac``/``hashlib`` primitives with constant-time comparison
(``hmac.compare_digest``) — before any payload is trusted or semantically
parsed and before any application effect can occur.

- A valid signature is GitHub's ``X-Hub-Signature-256`` header:
  ``sha256=`` followed by the lowercase hex HMAC-SHA256 digest of the raw
  body keyed with the webhook secret.
- Missing, malformed, and invalid signatures are distinct typed adapter-local
  rejections the intake service translates into the typed application
  vocabulary (:mod:`openorc.services.errors`); the transport maps them to a
  uniform, detail-free rejection.
- The webhook secret is deployment/bootstrap secret material. It is never
  Workspace data, never ``openorc.*`` table state, never Vault content, and
  never logged, returned, persisted, or attached to telemetry. Error
  messages are deliberately safe by authoring: they never contain the secret
  or the presented signature value, and the secret holder's ``repr``/``str``
  are redacted.
"""

from __future__ import annotations

import hashlib
import hmac

__all__ = [
    "GitHubWebhookRejectedError",
    "GitHubWebhookSecret",
    "GitHubWebhookSignatureInvalidError",
    "GitHubWebhookSignatureMalformedError",
    "GitHubWebhookSignatureMissingError",
    "GitHubWebhookSignatureVerifier",
]

# The documented GitHub webhook signature scheme: HMAC-SHA256 over the exact
# raw request bytes, hex-encoded behind the ``sha256=`` scheme prefix.
_SIGNATURE_SCHEME_PREFIX = "sha256="
_HEX_DIGEST_LENGTH = 64
_HEX_DIGITS = frozenset("0123456789abcdef")


class GitHubWebhookRejectedError(Exception):
    """A GitHub webhook delivery was rejected at the ingress authentication boundary."""


class GitHubWebhookSignatureMissingError(GitHubWebhookRejectedError):
    """The delivery presented no webhook signature header."""


class GitHubWebhookSignatureMalformedError(GitHubWebhookRejectedError):
    """The presented signature header is not a well-formed SHA-256 signature."""


class GitHubWebhookSignatureInvalidError(GitHubWebhookRejectedError):
    """The presented signature does not match the delivery bytes."""


class GitHubWebhookSecret:
    """Redacted holder of the deployment's GitHub App webhook secret.

    The value is preserved verbatim (credential bytes are meaningful). The
    holder's ``repr``/``str`` are redacted so the secret has no
    representation path into logs, errors, or telemetry.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("the GitHub webhook secret must be a non-empty string")
        self._value = value

    def value(self) -> str:
        """Return the secret verbatim, reserved for the verification call."""
        return self._value

    def __repr__(self) -> str:
        return "GitHubWebhookSecret(<redacted>)"

    def __str__(self) -> str:
        return repr(self)


class GitHubWebhookSignatureVerifier:
    """Verifies GitHub's SHA-256 webhook signature over the exact raw bytes."""

    def __init__(self, secret: GitHubWebhookSecret) -> None:
        self._secret = secret

    def verify(self, raw_body: bytes, signature_header: str | None) -> None:
        """Verify the delivery, or raise one of the typed rejections.

        ``raw_body`` must be the exact raw request bytes (never a
        re-serialized or otherwise normalized payload): the digest binds to
        the bytes GitHub signed. Any rejection is raised BEFORE the caller
        can derive any application effect from the payload.
        """
        if not isinstance(raw_body, bytes):
            raise ValueError("the webhook delivery must be verified over exact raw bytes")
        if signature_header is None:
            raise GitHubWebhookSignatureMissingError(
                "the webhook signature header is missing; the delivery is unauthenticated"
            )
        if not signature_header.startswith(_SIGNATURE_SCHEME_PREFIX):
            raise GitHubWebhookSignatureMalformedError("the webhook signature header is malformed")
        digest = signature_header[len(_SIGNATURE_SCHEME_PREFIX) :]
        if len(digest) != _HEX_DIGEST_LENGTH or any(
            character not in _HEX_DIGITS for character in digest
        ):
            raise GitHubWebhookSignatureMalformedError("the webhook signature header is malformed")
        expected = hmac.new(
            self._secret.value().encode("utf-8"), raw_body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, digest):
            raise GitHubWebhookSignatureInvalidError(
                "the webhook signature does not match the delivery bytes"
            )
