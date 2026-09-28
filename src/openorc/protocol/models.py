"""Typed results for the canonical v1 OpenOrc formal response families.

Issue #65: successful formal-response validation returns small immutable typed
values so adapters and services never pass arbitrary dictionaries. These
models preserve exactly the semantic payload validated against the canonical
JSON Schema assets in ``openorc/protocol/schemas/``; they do not duplicate
domain entities, perform persistence, decide workflow transitions, or mirror
caller-owned authority context.

The structural ``type`` and ``schema_version`` fields are deliberately not
repeated as model fields: the Python type identifies the response family and
``SCHEMA_VERSION`` is the supported structural version. Models carry only the
semantic fields the agent is responsible for producing.

This module imports only the standard library, so the typed result surface
stays usable from dependency-free installed-package artifact checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

SESSION_READY_FAMILY: Final = "session_ready"
PLAN_RESULT_FAMILY: Final = "plan_result"
REVIEW_RESULT_FAMILY: Final = "review_result"
IMPLEMENTATION_RESULT_FAMILY: Final = "implementation_result"
PR_RESULT_FAMILY: Final = "pr_result"

FORMAL_RESPONSE_FAMILIES: Final[frozenset[str]] = frozenset(
    {
        SESSION_READY_FAMILY,
        PLAN_RESULT_FAMILY,
        REVIEW_RESULT_FAMILY,
        IMPLEMENTATION_RESULT_FAMILY,
        PR_RESULT_FAMILY,
    }
)

SCHEMA_VERSION: Final = 1

REVIEW_OUTCOME_ACCEPTED: Final = "ACCEPTED"
REVIEW_OUTCOME_CHANGES_REQUESTED: Final = "CHANGES_REQUESTED"


@dataclass(frozen=True)
class SessionReady:
    """Validated ``session_ready`` formal response.

    ``status`` is enforced to be exactly ``"READY"`` by the canonical schema
    during validation.
    """

    status: str


@dataclass(frozen=True)
class PlanResult:
    """Validated ``plan_result`` formal response."""

    plan: str


@dataclass(frozen=True)
class ReviewFinding:
    """One validated finding inside a ``review_result``."""

    summary: str
    details: str


@dataclass(frozen=True)
class ReviewResult:
    """Validated ``review_result`` formal response.

    Coherence (``ACCEPTED`` implies zero findings, ``CHANGES_REQUESTED``
    implies at least one finding) is enforced by the canonical schema during
    validation. ``CHANGES_REQUESTED`` is a valid protocol result, never a
    protocol failure.
    """

    outcome: str
    summary: str
    findings: tuple[ReviewFinding, ...]


@dataclass(frozen=True)
class ImplementationResult:
    """Validated ``implementation_result`` formal response.

    ``branch`` is a discovery/routing claim reported by the agent; it is never
    authoritative GitHub state and is reconciled by workflow code elsewhere.
    """

    status: str
    branch: str
    summary: str
    changes: tuple[str, ...]
    validation: tuple[str, ...]
    notes: str | None


@dataclass(frozen=True)
class PrResult:
    """Validated ``pr_result`` formal response (presentation content only).

    The title/body are presentation material; they are never GitHub authority.
    """

    title: str
    body: str


FormalResponse = SessionReady | PlanResult | ReviewResult | ImplementationResult | PrResult
