"""The closed runtime-neutral v1 semantic interaction/control model (issues
#129 and #181).

OpenOrc drives the proven v1 Producer/Reviewer flow through exactly seven
semantic interactions: ``PLAN``, ``REVIEW``, ``REVISE``, ``IMPLEMENT``,
``PR_COMPOSE``, ``PREPARE_WORKSPACE``, and ``CLEANUP_WORKSPACE``. The
vocabulary is closed — every other name is rejected — because workflow
semantics do not imply prompt prose:

- PLAN renders the canonical ``controls/plan.md`` asset deterministically.
- PR_COMPOSE renders the canonical ``controls/pr_compose.md`` asset
  deterministically; it is presentation-only and never authorizes or performs
  GitHub PR creation.
- PREPARE_WORKSPACE and CLEANUP_WORKSPACE (issue #181) render the canonical
  ``controls/prepare_workspace.md`` and ``controls/cleanup_workspace.md``
  assets deterministically: they ask the Producer to prepare its own isolated
  Task checkout at the exact OpenOrc-supplied destination by cloning the
  authoritative repository there, or to remove only that Task checkout after
  the calling workflow has separately authorized cleanup. They carry the exact
  caller-supplied authoritative context (repository, issue, Task UUID, target
  checkout directory) and expect a ``workspace_result``, whose agent-reported
  operational status is never authority for Task identity, checkout location,
  merge state, or session binding; operation matching and workflow
  consequences belong to calling services.
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

Workspace guidance rides alongside PLAN or PR_COMPOSE rendering only through
the controlled #66 subordinate-guidance boundary (``append_subordinate_guidance``);
it can never wrap, rewrite, mutate, or semantically reinterpret a review
subject or a routed formal result, which this module never exposes to guidance.

Standard library imports only, like the rest of the protocol package.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from openorc.protocol import controls_assets
from openorc.protocol.initialization_assets import append_subordinate_guidance
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    WORKSPACE_RESULT_FAMILY,
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
    PREPARE_WORKSPACE = "PREPARE_WORKSPACE"
    CLEANUP_WORKSPACE = "CLEANUP_WORKSPACE"


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
    """PLAN: render the canonical planning control asset, with optional
    Owner-authored subordinate Workspace guidance composed through the
    controlled #66 boundary."""

    kind: InteractionKind = field(default=InteractionKind.PLAN)
    repository_full_name: str = ""
    issue_number: int = 0
    guidance: str | None = None

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.PLAN:
            raise ValueError(
                f"PlanInteraction is the PLAN interaction; received kind {self.kind!r}"
            )

    def render(self) -> str:
        rendered = controls_assets.render_control_asset(
            controls_assets.PLAN_CONTROL_ASSET,
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
        )
        return append_subordinate_guidance(rendered, self.guidance)


@dataclass(frozen=True)
class ReviewInteraction:
    """REVIEW: exact-subject review against one OpenOrc-supplied subject.

    ``subject`` must be exactly one of the supported review subjects
    (``PlanReviewSubject`` or ``PrReviewSubject``); anything else — including
    an ``ImplementationResult`` — is rejected, because the closed REVIEW-subject
    model has no implementation subject variant in v1.
    ``expected_response_family`` is always ``review_result`` and is explicit
    rather than implied.
    """

    kind: InteractionKind = field(default=InteractionKind.REVIEW)
    subject: PlanReviewSubject | PrReviewSubject | None = None
    expected_response_family: str = REVIEW_RESULT_FAMILY

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.REVIEW:
            raise ValueError(
                f"ReviewInteraction is the REVIEW interaction; received kind {self.kind!r}"
            )
        if not isinstance(self.subject, (PlanReviewSubject, PrReviewSubject)):
            subject_kind = type(self.subject).__name__
            raise ValueError(
                "REVIEW requires an exact OpenOrc-supplied review subject of a "
                f"supported planning or canonical PR variant; received {subject_kind}"
            )
        if self.expected_response_family != REVIEW_RESULT_FAMILY:
            raise ValueError(
                "REVIEW expects the review_result response family; received "
                f"{self.expected_response_family!r}"
            )


@dataclass(frozen=True)
class ReviseInteraction:
    """REVISE: route a validated CHANGES_REQUESTED review result as revision
    input, verbatim and opaque.

    ``subject`` must be exactly one of the supported review subjects; anything
    else is rejected. Planning revision expects another ``plan_result``; PR
    revision expects Producer work on the canonical Task branch followed by
    another ``implementation_result``. The ``review_result`` object itself is
    the routed artifact and is never copied into prose, rewritten, or
    inspected.
    """

    kind: InteractionKind = field(default=InteractionKind.REVISE)
    subject: PlanReviewSubject | PrReviewSubject | None = None
    review_result: ReviewResult | None = None
    expected_response_family: str = ""

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.REVISE:
            raise ValueError(
                f"ReviseInteraction is the REVISE interaction; received kind {self.kind!r}"
            )
        if not isinstance(self.subject, (PlanReviewSubject, PrReviewSubject)):
            subject_kind = type(self.subject).__name__
            raise ValueError(
                "REVISE requires the exact review subject under revision of a "
                f"supported planning or canonical PR variant; received {subject_kind}"
            )
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

    kind: InteractionKind = field(default=InteractionKind.IMPLEMENT)

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.IMPLEMENT:
            raise ValueError(
                f"ImplementInteraction is the IMPLEMENT interaction; received kind {self.kind!r}"
            )


@dataclass(frozen=True)
class PrComposeInteraction:
    """PR_COMPOSE: request presentation title/body through ``pr_result``, with
    optional Owner-authored subordinate Workspace guidance composed through the
    controlled #66 boundary.

    Presentation-only: it exposes composition content and carries no GitHub
    publication, authorization, or template-discovery action.
    """

    kind: InteractionKind = field(default=InteractionKind.PR_COMPOSE)
    repository_full_name: str = ""
    issue_number: int = 0
    guidance: str | None = None
    expected_response_family: str = PR_RESULT_FAMILY

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.PR_COMPOSE:
            raise ValueError(
                f"PrComposeInteraction is the PR_COMPOSE interaction; received kind {self.kind!r}"
            )
        if self.expected_response_family != PR_RESULT_FAMILY:
            raise ValueError(
                "PR_COMPOSE expects the pr_result response family; received "
                f"{self.expected_response_family!r}"
            )

    def render(self) -> str:
        rendered = controls_assets.render_control_asset(
            controls_assets.PR_COMPOSE_CONTROL_ASSET,
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
        )
        return append_subordinate_guidance(rendered, self.guidance)


def _validate_workspace_context(
    *,
    repository_full_name: str,
    issue_number: int,
    task_uuid: uuid.UUID,
    target_directory: str,
) -> None:
    """Validate only the input shapes of the caller-supplied authoritative
    workspace context. The protocol never checks filesystem existence, never
    invents a default base, never computes a Task path, and never infers
    authorization from a field."""
    if not isinstance(repository_full_name, str) or not repository_full_name.strip():
        raise ValueError("workspace lifecycle interactions require a nonblank repository full name")
    if type(issue_number) is not int or issue_number <= 0:
        raise ValueError("workspace lifecycle interactions require a positive issue number")
    if not isinstance(task_uuid, uuid.UUID):
        raise ValueError("workspace lifecycle interactions require a typed Task UUID")
    if not isinstance(target_directory, str) or not target_directory.strip():
        raise ValueError(
            "workspace lifecycle interactions require a nonblank target checkout directory"
        )


@dataclass(frozen=True)
class PrepareWorkspaceInteraction:
    """PREPARE_WORKSPACE: ask the Producer to prepare its own isolated Task
    checkout at the exact OpenOrc-supplied destination, cloning the
    authoritative repository into that directory (issue #181).

    The caller supplies and owns the authoritative context; the target
    checkout directory is an input authorized by the caller, never an
    agent-generated path. The returned ``workspace_result`` is agent-reported
    operational status, never authority.
    """

    repository_full_name: str
    issue_number: int
    task_uuid: uuid.UUID
    target_directory: str
    kind: InteractionKind = field(default=InteractionKind.PREPARE_WORKSPACE)
    expected_response_family: str = WORKSPACE_RESULT_FAMILY

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.PREPARE_WORKSPACE:
            raise ValueError(
                "PrepareWorkspaceInteraction is the PREPARE_WORKSPACE interaction; "
                f"received kind {self.kind!r}"
            )
        _validate_workspace_context(
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
            task_uuid=self.task_uuid,
            target_directory=self.target_directory,
        )
        if self.expected_response_family != WORKSPACE_RESULT_FAMILY:
            raise ValueError(
                "PREPARE_WORKSPACE expects the workspace_result response family; "
                f"received {self.expected_response_family!r}"
            )

    def render(self) -> str:
        return controls_assets.render_control_asset(
            controls_assets.PREPARE_WORKSPACE_CONTROL_ASSET,
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
            task_uuid=self.task_uuid,
            target_directory=self.target_directory,
        )


@dataclass(frozen=True)
class CleanupWorkspaceInteraction:
    """CLEANUP_WORKSPACE: ask the Producer to remove only the designated
    Task checkout after the calling workflow has separately authorized
    cleanup (issue #181).

    The caller supplies and owns the authoritative context and establishes
    cleanup authorization; the protocol neither infers authorization from a
    field nor performs any deletion. The returned ``workspace_result`` is
    agent-reported operational status, never authority.
    """

    repository_full_name: str
    issue_number: int
    task_uuid: uuid.UUID
    target_directory: str
    kind: InteractionKind = field(default=InteractionKind.CLEANUP_WORKSPACE)
    expected_response_family: str = WORKSPACE_RESULT_FAMILY

    def __post_init__(self) -> None:
        if self.kind is not InteractionKind.CLEANUP_WORKSPACE:
            raise ValueError(
                "CleanupWorkspaceInteraction is the CLEANUP_WORKSPACE interaction; "
                f"received kind {self.kind!r}"
            )
        _validate_workspace_context(
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
            task_uuid=self.task_uuid,
            target_directory=self.target_directory,
        )
        if self.expected_response_family != WORKSPACE_RESULT_FAMILY:
            raise ValueError(
                "CLEANUP_WORKSPACE expects the workspace_result response family; "
                f"received {self.expected_response_family!r}"
            )

    def render(self) -> str:
        return controls_assets.render_control_asset(
            controls_assets.CLEANUP_WORKSPACE_CONTROL_ASSET,
            repository_full_name=self.repository_full_name,
            issue_number=self.issue_number,
            task_uuid=self.task_uuid,
            target_directory=self.target_directory,
        )


Interaction = (
    PlanInteraction
    | ReviewInteraction
    | ReviseInteraction
    | ImplementInteraction
    | PrComposeInteraction
    | PrepareWorkspaceInteraction
    | CleanupWorkspaceInteraction
)
ReviewSubject = PlanReviewSubject | PrReviewSubject
