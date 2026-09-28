"""The closed runtime-neutral v1 semantic interaction/control model (issue #129).

OpenOrc drives the proven v1 Producer/Reviewer flow through exactly five
semantic interactions: ``PLAN``, ``REVIEW``, ``REVISE``, ``IMPLEMENT``, and
``PR_COMPOSE``. The vocabulary is closed — every other name is rejected —
because workflow semantics do not imply prompt prose:

- PLAN renders the canonical ``controls/plan.md`` asset deterministically.
- PR_COMPOSE renders the canonical ``controls/pr_compose.md`` asset
  deterministically; it is presentation-only and never authorizes or performs
  GitHub PR creation.
- REVIEW is exact-subject: the OpenOrc-supplied planning or canonical PR/head
  subject is the artifact the returned ``review_result`` is bound to, kept
  structurally distinct from agent-authored output. There is no implementation
  review subject in v1; implementation completion is not a ReviewLoop.
- REVISE routes an already-validated ``CHANGES_REQUESTED`` ``ReviewResult`` to
  the Producer verbatim as opaque substantive input. This layer never inspects,
  classifies, keyword-matches, scores, rewrites, or infers meaning from the
  free-text fields of formal results; #65 owns their machine-contract
  validation.
- IMPLEMENT carries runtime-neutral semantic intent only. It has no canonical
  prose in v1; realizing it (for Cline, the native PLAN→ACT transition) is
  adapter/runtime behavior (#67 and later adapters), never protocol content.

The models carry no Cline SDK types, Hub identifiers, provider configuration,
or Cline action names. Free-form Owner ↔ Reviewer advisory discussion uses the
existing Reviewer session but is deliberately not represented here: it causes
no workflow transition and cannot count as formal acceptance.

Workspace guidance may only ride alongside PLAN or PR_COMPOSE rendering through
the controlled #66 subordinate-guidance boundary; it can never wrap, rewrite,
mutate, or semantically reinterpret a review subject or a routed formal result.

Standard library imports only, like the rest of the protocol package.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from openorc.protocol import controls_assets
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    ReviewResult,
)

REVIEW_OUTCOME_CHANGES_REQUESTED: Final = "CHANGES_REQUESTED"


class InteractionKind(StrEnum):
    """The closed v1 semantic interaction vocabulary."""

    PLAN = "PLAN"
    REVIEW = "REVIEW"
    REVISE = "REVISE"
    IMPLEMENT = "IMPLEMENT"
    PR_COMPOSE = "PR_COMPOSE"


INTERACTION_KINDS: Final[frozenset[str]] = frozenset(kind.value for kind in InteractionKind)


@dataclass(frozen=True)
class PlanReviewSubject:
    """The exact current PlanRevision OpenOrc asks the Reviewer to evaluate.

    Carries the OpenOrc-supplied authoritative PlanRevision identity and the
    exact reviewable planning content (the validated ``plan`` content from the
    originating ``plan_result``). Reviewer output never establishes authority
    by echoing these identifiers.
    """

    plan_revision_id: str
    repository_full_name: str
    issue_number: int
    plan: str


@dataclass(frozen=True)
class PrReviewSubject:
    """The exact canonical PR identity plus exact reconciled head SHA.

    Every value comes from authoritative OpenOrc/GitHub state, never from
    Producer or runtime-local claims.
    """

    repository_full_name: str
    pull_request_number: int
    head_sha: str
    issue_number: int


@dataclass(frozen=True)
class PlanInteraction:
    """PLAN: render the canonical planning control asset."""

    kind: Final[InteractionKind] = InteractionKind.PLAN
    repository_full_name: str = ""
    issue_number: int = 0

    def render(self) -> str:
        return controls_assets.render_control_asset(
            controls_assets.PLAN_CONTROL_ASSET,
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
        )


@dataclass(frozen=True)
class ReviewInteraction:
    """REVIEW: exact-subject review against one OpenOrc-supplied subject.

    ``subject`` is the exact artifact to evaluate; ``expected_response_family``
    is always ``review_result`` and is explicit rather than implied.
    """

    kind: Final[InteractionKind] = InteractionKind.REVIEW
    subject: ReviewSubject | None = None
    expected_response_family: Final[str] = REVIEW_RESULT_FAMILY

    def __post_init__(self) -> None:
        if self.subject is None:
            raise ValueError("REVIEW requires an exact OpenOrc-supplied review subject")


@dataclass(frozen=True)
class ReviseInteraction:
    """REVISE: route a validated CHANGES_REQUESTED review result as revision
    input, verbatim and opaque.

    Planning revision expects another ``plan_result``; PR revision expects
    Producer work on the canonical Task branch followed by another
    ``implementation_result``. The ``review_result`` object itself is the
    routed artifact and is never copied into prose, rewritten, or inspected.
    """

    kind: Final[InteractionKind] = InteractionKind.REVISE
    subject: ReviewSubject | None = None
    review_result: ReviewResult | None = None
    expected_response_family: str = ""

    def __post_init__(self) -> None:
        if self.subject is None:
            raise ValueError("REVISE requires the exact review subject under revision")
        if self.review_result is None:
            raise ValueError("REVISE requires the validated review_result being routed")
        if self.review_result.outcome != REVIEW_OUTCOME_CHANGES_REQUESTED:
            raise ValueError(
                "REVISE routes a CHANGES_REQUESTED review_result; "
                f"received outcome {self.review_result.outcome!r}"
            )
        expected = (
            PLAN_RESULT_FAMILY
            if isinstance(self.subject, PlanReviewSubject)
            else IMPLEMENTATION_RESULT_FAMILY
        )
        if self.expected_response_family == "":
            object.__setattr__(self, "expected_response_family", expected)
        elif self.expected_response_family != expected:
            raise ValueError("REVISE expected_response_family does not match the review subject")


@dataclass(frozen=True)
class ImplementInteraction:
    """IMPLEMENT: semantic implementation-authorization intent only.

    No canonical prose exists for IMPLEMENT in v1 and this model carries no
    rendering, no payload, and no provider-specific action names. The adapter
    realizing it owns the native mechanism (for Cline, PLAN→ACT).
    """

    kind: Final[InteractionKind] = InteractionKind.IMPLEMENT


@dataclass(frozen=True)
class PrComposeInteraction:
    """PR_COMPOSE: request presentation title/body through ``pr_result``.

    Presentation-only: it exposes composition content and carries no GitHub
    publication, authorization, or template-discovery action.
    """

    kind: Final[InteractionKind] = InteractionKind.PR_COMPOSE
    repository_full_name: str = ""
    issue_number: int = 0
    expected_response_family: Final[str] = PR_RESULT_FAMILY

    def render(self) -> str:
        return controls_assets.render_control_asset(
            controls_assets.PR_COMPOSE_CONTROL_ASSET,
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
        )


Interaction = (
    PlanInteraction
    | ReviewInteraction
    | ReviseInteraction
    | ImplementInteraction
    | PrComposeInteraction
)
ReviewSubject = PlanReviewSubject | PrReviewSubject
