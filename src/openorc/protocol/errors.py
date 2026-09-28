"""Typed protocol-failure taxonomy for OpenOrc formal responses (issue #65).

Every failure of formal-response parsing/validation is one of these typed
errors. Classification is deterministic, and a Reviewer ``CHANGES_REQUESTED``
outcome is never a protocol failure — it is a valid semantic outcome that
parses into a ``ReviewResult``.

Exception messages are safe for logs and telemetry: they name the response
family, the failing schema location, and the failing keyword, and never embed
the raw agent response, Workspace guidance, or other uncontrolled payload
content.
"""

from __future__ import annotations


class ProtocolError(Exception):
    """Base of the OpenOrc formal-response protocol-failure taxonomy."""


class FormalObjectNotFoundError(ProtocolError):
    """The response contains no syntactically identifiable JSON object."""


class AmbiguousFormalResponseError(ProtocolError):
    """The response contains more than one JSON-object candidate.

    Protocol parsing never chooses among candidates, so the response fails
    closed.
    """


class MalformedFormalObjectError(ProtocolError):
    """An isolated JSON-object payload is not valid JSON.

    Malformed JSON is never repaired, and valid-looking nested objects inside
    a malformed span are never salvaged.
    """


class UnknownResponseTypeError(ProtocolError):
    """The response ``type`` does not name a canonical v1 response family."""


class UnexpectedResponseTypeError(ProtocolError):
    """The response family does not match the family the caller expected."""


class UnsupportedSchemaVersionError(ProtocolError):
    """The response ``schema_version`` is not the supported canonical v1 value."""


class SchemaInvalidPayloadError(ProtocolError):
    """The response object does not satisfy its canonical v1 JSON Schema."""


class ResponseCoherenceError(ProtocolError):
    """A code-enforced semantic invariant beyond the canonical schema failed.

    Reserved slot: v1 coherence (for example the ``review_result``
    outcome/findings pairing) is currently enforced by the canonical schema
    itself. This class exists so any future code-enforced invariant has a
    deterministic classification instead of reusing schema-invalid errors or
    being confused with a Reviewer outcome.
    """
