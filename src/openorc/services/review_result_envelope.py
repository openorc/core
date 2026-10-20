"""The shared canonical ``review_result`` envelope for Producer dispatch.

E2's revision dispatch (issue #75) and E6's implementation handoff (issue
#81) both deliver the exact persisted Reviewer ``review_result`` formal
envelope to the Producer session: structural serialization only — the
Reviewer's substantive fields preserved verbatim, with no instructions, no
template, no plan re-send, and no rewriting of free-text fields. This tiny
shared module owns that one pure serializer plus the accepted-result
reconstruction from a finalized ReviewIteration. The CHANGES_REQUESTED
reconstruction deliberately stays E2-local: its canonical findings shape is
the revision input, not a handoff envelope.
"""

from __future__ import annotations

import json

from openorc.domain.reviews import ReviewIteration, ReviewOutcome
from openorc.protocol.models import (
    REVIEW_RESULT_FAMILY,
    SCHEMA_VERSION,
    ReviewResult,
)
from openorc.services.errors import ConflictError

__all__ = ["accepted_review_result_from_iteration", "serialize_review_result"]


def serialize_review_result(review_result: ReviewResult) -> str:
    """The substantive Producer message: structural serialization only.

    The routed artifact is the exact canonical ``review_result`` formal
    envelope the Reviewer produced — no new remediation template, no
    inferred instructions, and no rewriting of free-text fields.
    """
    payload = {
        "type": REVIEW_RESULT_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "outcome": review_result.outcome,
        "summary": review_result.summary,
        "findings": [
            {"summary": finding.summary, "details": finding.details}
            for finding in review_result.findings
        ],
    }
    return json.dumps(payload)


def accepted_review_result_from_iteration(iteration: ReviewIteration) -> ReviewResult:
    """Rebuild the persisted ACCEPTED review result verbatim (issue #81).

    The finalized accepted iteration's summary is the handoff envelope's
    substantive content and is preserved exactly, with no interpretation
    or rewriting. An accepted Reviewer result carries no findings: a
    persisted accepted iteration that carries any finding is not the
    canonical accepted shape and fails closed instead of being repaired.
    """
    if iteration.decided_at is None:
        raise ConflictError(
            "the review iteration is not finalized; no accepted-result handoff is produced"
        )
    if iteration.outcome is not ReviewOutcome.ACCEPTED:
        raise ConflictError(
            "the finalized review iteration is not a Reviewer acceptance; "
            "no accepted-result handoff is produced"
        )
    if iteration.findings:
        raise ConflictError(
            "the persisted accepted review iteration carries findings, which is not the "
            "canonical accepted review_result shape; no handoff envelope is produced"
        )
    if iteration.summary is not None and not isinstance(iteration.summary, str):
        raise ConflictError(
            "the persisted review summary is not a string; no handoff envelope is produced"
        )
    return ReviewResult(
        outcome=iteration.outcome,
        summary=iteration.summary or "",
        findings=(),
    )
