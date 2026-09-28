"""Deterministic parsing and validation of OpenOrc formal responses (issue #65).

``parse_formal_response`` is the shared boundary every OpenOrc-facing agent
response passes through: it isolates exactly one explicit JSON object from the
response text, classifies its response family and schema version, validates it
against the canonical v1 JSON Schema asset, and returns a typed protocol
model.

Harmless presentation tolerance only:

- surrounding whitespace;
- a single Markdown code fence around an otherwise valid JSON object;
- exactly one independently parseable JSON object amid non-JSON prose.

Extraction is defined on JSON-object candidates, not raw brace-balanced text:
a brace-balanced span is a candidate if and only if it parses as a complete
JSON object. Brace-like prose (``{big}``, ``{name}``) is harmless noise that
neither creates ambiguity nor gets repaired. Nested objects inside an outer
span never become separate candidates, and valid objects are never salvaged
out of malformed outer spans.

The surrounding prose is never semantic input: it cannot supply, repair,
override, or disambiguate any field of the formal object.

Classification precedence is deterministic: unknown ``type``, then unsupported
``schema_version``, then expected-family mismatch, then canonical-schema
validation. A Reviewer ``CHANGES_REQUESTED`` outcome is a valid protocol
result, never a protocol failure.

Runtime/provider-specific outer envelopes are not handled here: an Agent
Runtime adapter strips its own transport wrapper before invoking this parser
(issue #67).
"""

from __future__ import annotations

import json
from typing import Any

from jsonschema import Draft202012Validator

from openorc.protocol import schema_assets
from openorc.protocol.errors import (
    AmbiguousFormalResponseError,
    FormalObjectNotFoundError,
    MalformedFormalObjectError,
    SchemaInvalidPayloadError,
    UnexpectedResponseTypeError,
    UnknownResponseTypeError,
    UnsupportedSchemaVersionError,
)
from openorc.protocol.models import (
    FORMAL_RESPONSE_FAMILIES,
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    SCHEMA_VERSION,
    SESSION_READY_FAMILY,
    FormalResponse,
    ImplementationResult,
    PlanResult,
    PrResult,
    ReviewFinding,
    ReviewResult,
    SessionReady,
)

_MAX_REPORTED_SCHEMA_ERRORS = 5


def parse_formal_response(
    raw: str | dict[str, Any],
    *,
    expected_family: str | None = None,
) -> FormalResponse:
    """Parse one formal response from response text or an already-isolated object.

    Returns the typed protocol result for the response family. Raises a
    ``ProtocolError`` subclass when the response fails the protocol contract;
    the error is a protocol failure for the caller to handle, never a semantic
    outcome.
    """
    payload: dict[str, Any] = raw if isinstance(raw, dict) else _extract_formal_object(raw)
    family = _response_family(payload)
    _response_schema_version(payload)
    if expected_family is not None and family != expected_family:
        raise UnexpectedResponseTypeError(
            f"formal response family {family!r} does not match the expected response family"
        )
    _validate_against_canonical_schema(family, payload)
    return _build_result(family, payload)


def _extract_formal_object(text: str) -> dict[str, Any]:
    """Isolate exactly one explicit JSON object without semantic inference."""
    stripped = text.strip()
    if not stripped:
        raise FormalObjectNotFoundError("formal response contains no JSON object")

    parsed_candidates: list[dict[str, Any]] = []
    unparsable_span_count = 0
    for span in _top_level_object_spans(stripped):
        parsed = _parse_json_object(span)
        if parsed is None:
            unparsable_span_count += 1
        else:
            parsed_candidates.append(parsed)

    if len(parsed_candidates) > 1:
        raise AmbiguousFormalResponseError(
            "formal response contains multiple JSON-object candidates; "
            "protocol parsing never chooses among them"
        )
    if len(parsed_candidates) == 1:
        return parsed_candidates[0]
    if unparsable_span_count:
        raise MalformedFormalObjectError("the isolated JSON-object payload is not valid JSON")
    raise FormalObjectNotFoundError(
        "formal response contains no syntactically identifiable JSON object"
    )


def _parse_json_object(span: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(span)
    except ValueError:  # json.JSONDecodeError is a ValueError
        return None
    return parsed if isinstance(parsed, dict) else None


def _top_level_object_spans(text: str) -> list[str]:
    """Return the outermost brace-balanced spans of the text.

    The scan tracks JSON string literals and escapes so braces inside strings
    never break span boundaries. Unmatched braces in surrounding prose are
    ignored, and nested braces belong to their outermost span.
    """
    spans: list[str] = []
    depth = 0
    start = -1
    in_string = False
    escaped = False
    for index, char in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            if depth == 0:
                start = index
            depth += 1
        elif char == "}" and depth > 0:
            depth -= 1
            if depth == 0:
                spans.append(text[start : index + 1])
                start = -1
    return spans


def _response_family(payload: dict[str, Any]) -> str:
    family = payload.get("type")
    if not isinstance(family, str) or family not in FORMAL_RESPONSE_FAMILIES:
        raise UnknownResponseTypeError(
            "formal response 'type' does not name a canonical v1 response family"
        )
    return family


def _response_schema_version(payload: dict[str, Any]) -> int:
    version = payload.get("schema_version")
    if type(version) is not int or version != SCHEMA_VERSION:
        raise UnsupportedSchemaVersionError(
            f"formal response 'schema_version' is not the supported canonical "
            f"v{SCHEMA_VERSION} value"
        )
    return version


def _validate_against_canonical_schema(family: str, payload: dict[str, Any]) -> None:
    schema = schema_assets.load_schema(family)
    errors = sorted(
        Draft202012Validator(schema).iter_errors(payload),
        key=lambda error: (error.json_path, str(error.validator)),
    )
    if errors:
        reported = "; ".join(
            f"{error.json_path} failed '{error.validator}'"
            for error in errors[:_MAX_REPORTED_SCHEMA_ERRORS]
        )
        raise SchemaInvalidPayloadError(
            f"{family} payload failed canonical v{SCHEMA_VERSION} schema validation: {reported}"
        )


def _build_result(family: str, payload: dict[str, Any]) -> FormalResponse:
    if family == SESSION_READY_FAMILY:
        return SessionReady(status=payload["status"])
    if family == PLAN_RESULT_FAMILY:
        return PlanResult(plan=payload["plan"])
    if family == REVIEW_RESULT_FAMILY:
        findings = tuple(
            ReviewFinding(summary=finding["summary"], details=finding["details"])
            for finding in payload["findings"]
        )
        return ReviewResult(
            outcome=payload["outcome"], summary=payload["summary"], findings=findings
        )
    if family == IMPLEMENTATION_RESULT_FAMILY:
        return ImplementationResult(
            status=payload["status"],
            branch=payload["branch"],
            summary=payload["summary"],
            changes=tuple(payload["changes"]),
            validation=tuple(payload["validation"]),
            notes=payload.get("notes"),
        )
    # PR_RESULT_FAMILY is the only remaining canonical family.
    return PrResult(title=payload["title"], body=payload["body"])
