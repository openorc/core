"""Formal-response protocol tests for issue #65.

Ordinary deterministic tests: no database, no network. These prove the
canonical v1 formal-response contract: typed results per response family,
canonical-schema enforcement (closed fields, required/non-empty values, exact
type/version, findings coherence), deterministic failure classification, and
formal-object extraction that tolerates harmless presentation without ever
inferring or repairing semantics.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from openorc.protocol import parsing
from openorc.protocol.errors import (
    AmbiguousFormalResponseError,
    FormalObjectNotFoundError,
    MalformedFormalObjectError,
    ProtocolError,
    ResponseCoherenceError,
    SchemaInvalidPayloadError,
    UnexpectedResponseTypeError,
    UnknownResponseTypeError,
    UnsupportedSchemaVersionError,
)
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_OUTCOME_CHANGES_REQUESTED,
    REVIEW_RESULT_FAMILY,
    SESSION_READY_FAMILY,
    ImplementationResult,
    PlanResult,
    PrResult,
    ReviewFinding,
    ReviewResult,
    SessionReady,
)

_RAW_PAYLOAD_MARKER = "UNTRUSTED-AGENT-PAYLOAD-MARKER-42"


def _valid_payload(family: str) -> dict[str, Any]:
    if family == SESSION_READY_FAMILY:
        return {"type": "session_ready", "schema_version": 1, "status": "READY"}
    if family == PLAN_RESULT_FAMILY:
        return {
            "type": "plan_result",
            "schema_version": 1,
            "plan": "# Plan\n\n1. Implement the thing.\n",
        }
    if family == REVIEW_RESULT_FAMILY:
        return {
            "type": "review_result",
            "schema_version": 1,
            "outcome": "ACCEPTED",
            "summary": "Implementation matches the plan.",
            "findings": [],
        }
    if family == IMPLEMENTATION_RESULT_FAMILY:
        return {
            "type": "implementation_result",
            "schema_version": 1,
            "status": "COMPLETED",
            "branch": "feat/implement-the-thing",
            "summary": "Implemented the thing with tests.",
            "changes": ["Added the thing.", "Added tests for the thing."],
            "validation": [".venv/bin/python -m pytest", ".venv/bin/python -m ruff check ."],
            "notes": None,
        }
    assert family == PR_RESULT_FAMILY
    return {
        "type": "pr_result",
        "schema_version": 1,
        "title": "Implement the thing",
        "body": "Closes #1\n\nAdds the thing with tests.",
    }


def _changes_requested_payload() -> dict[str, Any]:
    return {
        "type": "review_result",
        "schema_version": 1,
        "outcome": "CHANGES_REQUESTED",
        "summary": "Needs work before this can merge.",
        "findings": [
            {
                "summary": "Missing tests.",
                "details": "Add deterministic tests for the parser boundary.",
            }
        ],
    }


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload)


def _fenced(payload_text: str, *, language: str = "json") -> str:
    return f"```{language}\n{payload_text}\n```"


def test_session_ready_parses_into_a_typed_result():
    result = parsing.parse_formal_response(_json(_valid_payload(SESSION_READY_FAMILY)))
    assert isinstance(result, SessionReady)
    assert result.status == "READY"


def test_plan_result_parses_into_a_typed_result():
    result = parsing.parse_formal_response(_json(_valid_payload(PLAN_RESULT_FAMILY)))
    assert isinstance(result, PlanResult)
    assert result.plan == "# Plan\n\n1. Implement the thing.\n"


def test_review_result_parses_into_a_typed_result():
    result = parsing.parse_formal_response(_json(_valid_payload(REVIEW_RESULT_FAMILY)))
    assert isinstance(result, ReviewResult)
    assert result.outcome == "ACCEPTED"
    assert result.summary == "Implementation matches the plan."
    assert result.findings == ()


def test_implementation_result_parses_into_a_typed_result():
    result = parsing.parse_formal_response(_json(_valid_payload(IMPLEMENTATION_RESULT_FAMILY)))
    assert isinstance(result, ImplementationResult)
    assert result.status == "COMPLETED"
    assert result.branch == "feat/implement-the-thing"
    assert result.changes == ("Added the thing.", "Added tests for the thing.")
    assert result.validation == (
        ".venv/bin/python -m pytest",
        ".venv/bin/python -m ruff check .",
    )
    assert result.notes is None


def test_pr_result_parses_into_a_typed_result():
    result = parsing.parse_formal_response(_json(_valid_payload(PR_RESULT_FAMILY)))
    assert isinstance(result, PrResult)
    assert result.title == "Implement the thing"
    assert result.body == "Closes #1\n\nAdds the thing with tests."


def test_changes_requested_is_a_valid_protocol_result_not_a_failure():
    result = parsing.parse_formal_response(_json(_changes_requested_payload()))
    assert isinstance(result, ReviewResult)
    assert result.outcome == REVIEW_OUTCOME_CHANGES_REQUESTED
    assert result.findings == (
        ReviewFinding(
            summary="Missing tests.",
            details="Add deterministic tests for the parser boundary.",
        ),
    )


def test_pre_parsed_object_input_is_accepted():
    result = parsing.parse_formal_response(_valid_payload(SESSION_READY_FAMILY))
    assert isinstance(result, SessionReady)


def test_bare_json_object_with_surrounding_whitespace_parses():
    text = f"  \n{_json(_valid_payload(PLAN_RESULT_FAMILY))}\n  "
    result = parsing.parse_formal_response(text)
    assert isinstance(result, PlanResult)


def test_single_code_fence_around_a_valid_object_parses():
    text = _fenced(_json(_valid_payload(SESSION_READY_FAMILY)))
    result = parsing.parse_formal_response(text, expected_family=SESSION_READY_FAMILY)
    assert isinstance(result, SessionReady)


def test_code_fence_without_language_is_tolerated():
    text = f"```\n{_json(_valid_payload(PR_RESULT_FAMILY))}\n```"
    result = parsing.parse_formal_response(text)
    assert isinstance(result, PrResult)


def test_exactly_one_object_inside_prose_parses():
    text = (
        "I've reviewed the implementation. Here is my formal result.\n"
        f"{_json(_valid_payload(REVIEW_RESULT_FAMILY))}\n"
        "Thanks!"
    )
    result = parsing.parse_formal_response(text)
    assert isinstance(result, ReviewResult)


def test_non_json_brace_prose_does_not_create_false_ambiguity():
    text = (
        "I think {big} things look fine. Best, {name}\n"
        f"{_fenced(_json(_valid_payload(REVIEW_RESULT_FAMILY)))}"
    )
    result = parsing.parse_formal_response(text, expected_family=REVIEW_RESULT_FAMILY)
    assert isinstance(result, ReviewResult)


def test_unparsable_span_next_to_one_valid_candidate_is_tolerated():
    text = f"Note: {{'a': 1}} is not JSON.\n{_json(_valid_payload(PLAN_RESULT_FAMILY))}"
    result = parsing.parse_formal_response(text)
    assert isinstance(result, PlanResult)


def test_braces_inside_json_strings_never_break_candidate_extraction():
    payload = _valid_payload(PLAN_RESULT_FAMILY)
    payload["plan"] = 'Plan with braces {in "strings"} and \\"escapes\\" inside.'
    result = parsing.parse_formal_response(_json(payload))
    assert isinstance(result, PlanResult)
    assert "escapes" in result.plan


@pytest.mark.parametrize(
    ("text", "expected_error"),
    [
        pytest.param("no formal object here at all", FormalObjectNotFoundError, id="prose-only"),
        pytest.param("   \n\t  ", FormalObjectNotFoundError, id="whitespace-only"),
        pytest.param(
            "I could not produce a result.",
            FormalObjectNotFoundError,
            id="prose-without-braces",
        ),
        pytest.param(
            f"Results: {{oops, {_json(_valid_payload(PLAN_RESULT_FAMILY))}}}",
            MalformedFormalObjectError,
            id="malformed-outer-span-never-salvages-inner-object",
        ),
        pytest.param(_fenced("{not json}"), MalformedFormalObjectError, id="malformed-fence"),
        pytest.param(_fenced("{not json"), FormalObjectNotFoundError, id="unterminated-fence"),
        pytest.param("[]", FormalObjectNotFoundError, id="json-array-without-object"),
        pytest.param('"just a string"', FormalObjectNotFoundError, id="json-string"),
        pytest.param("5", FormalObjectNotFoundError, id="json-number"),
    ],
)
def test_formal_object_extraction_failures_deterministically(
    text: str, expected_error: type[ProtocolError]
):
    with pytest.raises(expected_error):
        parsing.parse_formal_response(text)


def test_two_fenced_objects_are_ambiguous():
    first = _fenced(_json(_valid_payload(PLAN_RESULT_FAMILY)))
    second = _fenced(_json(_valid_payload(REVIEW_RESULT_FAMILY)))
    with pytest.raises(AmbiguousFormalResponseError):
        parsing.parse_formal_response(f"{first}\n{second}")


def test_prose_object_plus_a_second_valid_object_is_ambiguous():
    first = _json(_valid_payload(PLAN_RESULT_FAMILY))
    second = _json(_valid_payload(REVIEW_RESULT_FAMILY))
    with pytest.raises(AmbiguousFormalResponseError):
        parsing.parse_formal_response(f"First {first} then {second}. Nothing to choose.")


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"type": "surprise", "schema_version": 1}, id="unknown-family"),
        pytest.param({"schema_version": 1}, id="missing-type"),
        pytest.param({"type": 42, "schema_version": 1}, id="non-string-type"),
        pytest.param({"type": None, "schema_version": 1}, id="null-type"),
    ],
)
def test_unknown_or_missing_response_type_fails(payload: dict[str, Any]):
    with pytest.raises(UnknownResponseTypeError):
        parsing.parse_formal_response(payload)


@pytest.mark.parametrize("version", [2, "1", None, True, 1.0])
def test_unsupported_or_wrong_typed_schema_version_fails(version: Any):
    payload = {"type": PLAN_RESULT_FAMILY, "schema_version": version, "plan": "# Plan"}
    with pytest.raises(UnsupportedSchemaVersionError):
        parsing.parse_formal_response(payload)


def test_unknown_type_takes_precedence_over_version():
    with pytest.raises(UnknownResponseTypeError):
        parsing.parse_formal_response({"type": "nope", "schema_version": 2})


def test_unsupported_version_takes_precedence_over_expected_family():
    with pytest.raises(UnsupportedSchemaVersionError):
        parsing.parse_formal_response(
            {"type": PLAN_RESULT_FAMILY, "schema_version": 2, "plan": "# Plan"},
            expected_family=PLAN_RESULT_FAMILY,
        )


def test_unsupported_version_takes_precedence_over_payload_schema_errors():
    with pytest.raises(UnsupportedSchemaVersionError):
        parsing.parse_formal_response(
            {"type": PLAN_RESULT_FAMILY, "schema_version": 2, "plan": "", "extra": True}
        )


def test_expected_family_mismatch_fails():
    with pytest.raises(UnexpectedResponseTypeError):
        parsing.parse_formal_response(
            _json(_valid_payload(PLAN_RESULT_FAMILY)), expected_family=REVIEW_RESULT_FAMILY
        )


@pytest.mark.parametrize(
    ("family", "payload"),
    [
        pytest.param(
            SESSION_READY_FAMILY,
            {"type": "session_ready", "schema_version": 1, "status": "ready"},
            id="wrong-status-const",
        ),
        pytest.param(
            PLAN_RESULT_FAMILY, {"type": "plan_result", "schema_version": 1}, id="missing-plan"
        ),
        pytest.param(
            PLAN_RESULT_FAMILY,
            {"type": "plan_result", "schema_version": 1, "plan": ""},
            id="empty-plan",
        ),
        pytest.param(
            PLAN_RESULT_FAMILY,
            {"type": "plan_result", "schema_version": 1, "plan": 7},
            id="non-string-plan",
        ),
        pytest.param(
            PR_RESULT_FAMILY,
            {"type": "pr_result", "schema_version": 1, "title": "T"},
            id="missing-body",
        ),
        pytest.param(
            IMPLEMENTATION_RESULT_FAMILY,
            {**_valid_payload(IMPLEMENTATION_RESULT_FAMILY), "changes": ["ok", 3]},
            id="non-string-change",
        ),
        pytest.param(
            REVIEW_RESULT_FAMILY,
            {
                "type": "review_result",
                "schema_version": 1,
                "outcome": "ACCEPTED",
                "summary": "Implementation matches the plan.",
                "findings": [{"summary": "S", "details": "D"}],
            },
            id="accepted-with-findings",
        ),
        pytest.param(
            REVIEW_RESULT_FAMILY,
            {
                "type": "review_result",
                "schema_version": 1,
                "outcome": "CHANGES_REQUESTED",
                "summary": "Needs work.",
                "findings": [],
            },
            id="changes-requested-without-findings",
        ),
        pytest.param(
            REVIEW_RESULT_FAMILY,
            {
                "type": "review_result",
                "schema_version": 1,
                "outcome": "CHANGES_REQUESTED",
                "summary": "Needs work.",
                "findings": [{"summary": "S"}],
            },
            id="finding-missing-details",
        ),
    ],
)
def test_canonical_schema_validation_is_enforced(family: str, payload: dict[str, Any]):
    with pytest.raises(SchemaInvalidPayloadError):
        parsing.parse_formal_response(payload)


def test_extra_provenance_field_fails_closed():
    payload = {**_valid_payload(PLAN_RESULT_FAMILY), "provenance": "agent-claims"}
    with pytest.raises(SchemaInvalidPayloadError):
        parsing.parse_formal_response(payload)


def test_schema_invalid_error_does_not_echo_payload_values():
    payload = {**_valid_payload(PLAN_RESULT_FAMILY), "provenance": _RAW_PAYLOAD_MARKER}
    with pytest.raises(SchemaInvalidPayloadError) as exc_info:
        parsing.parse_formal_response(payload)
    assert _RAW_PAYLOAD_MARKER not in str(exc_info.value)


def test_unknown_type_error_does_not_echo_payload_values():
    with pytest.raises(UnknownResponseTypeError) as exc_info:
        parsing.parse_formal_response({"type": _RAW_PAYLOAD_MARKER, "schema_version": 1})
    assert _RAW_PAYLOAD_MARKER not in str(exc_info.value)


def test_malformed_error_does_not_echo_payload_text():
    with pytest.raises(MalformedFormalObjectError) as exc_info:
        parsing.parse_formal_response(f"Result: {{ {_RAW_PAYLOAD_MARKER} }}")
    assert _RAW_PAYLOAD_MARKER not in str(exc_info.value)


def test_ambiguous_error_does_not_echo_payload_values():
    marker_plan = {**_valid_payload(PLAN_RESULT_FAMILY), "plan": _RAW_PAYLOAD_MARKER}
    with pytest.raises(AmbiguousFormalResponseError) as exc_info:
        parsing.parse_formal_response(
            f"{_json(marker_plan)}\n{_json(_valid_payload(REVIEW_RESULT_FAMILY))}"
        )
    assert _RAW_PAYLOAD_MARKER not in str(exc_info.value)


def test_protocol_failure_taxonomy_is_typed():
    for error_type in (
        FormalObjectNotFoundError,
        AmbiguousFormalResponseError,
        MalformedFormalObjectError,
        UnknownResponseTypeError,
        UnexpectedResponseTypeError,
        UnsupportedSchemaVersionError,
        SchemaInvalidPayloadError,
        ResponseCoherenceError,
    ):
        assert issubclass(error_type, ProtocolError)
