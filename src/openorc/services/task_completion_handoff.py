"""The ONE post-commit detached-completion handoff seam (issues #325/#333).

An independently completed detached turn routes to its original owning
service through exactly this one seam. After the shared settlement commits
its durable result/decision, the process that observed the settlement
invokes this handoff with the settled receipt's explicit operation family.
The handoff re-derives every next-step authority and input from committed
durable state — never from a transient settlement object — and then either
submits typed F1 follow-on work or invokes the existing owning service. A
failed or uncertain follow-on submission is not success and never permits a
blind re-enqueue; a successful run receipt is not proof of a subsequent
GitHub/gate effect.

The seam is deliberately narrow: an explicit family route per settled
operation family, implemented by the executing process's composition —
never a plugin/callback framework, never per-family workflow callbacks
accumulated on the observer, and never gate or workflow policy inside the
observer or the Hub-correlation layer. The observation host, the
dispatching service's in-dispatch settlement, and the post-dispatch
bootstrap tail all invoke the same ONE handoff, so a settled completion
triggers its continuation exactly once regardless of which path settled
it, and a family without a wired route fails closed until its own
detached migration wires it. The settled-interrupted-turn route
(``handoff_settled_interrupt``, issue #342) is the same ONE handoff's
second method: both settlement-observing paths — the observation host's
terminal routing and the dispatching service's acknowledgement attach
that consumed a staged early completion — route the post-commit
Interrupt settlement through it identically.
"""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.protocol.models import FormalResponse

__all__ = ["DetachedTurnCompletionHandoff"]


class DetachedTurnCompletionHandoff(Protocol):
    """The ONE post-commit completion handoff of the detached-turn boundary."""

    def handoff_settled_completion(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        operation: PendingTaskRuntimeInteractionOperation,
        validated_result: FormalResponse | None = None,
    ) -> None:
        """Route one settled validated completion by its explicit family.

        Invoked strictly after the settlement transaction committed. The
        implementation re-derives the next-step authority from committed
        durable state and routes only a family with a wired continuation
        route; every failure is contained by the caller and leaves the
        durable state to the existing recovery semantics.

        ``validated_result`` is the deliberately narrow #337 exception: the
        PR_COMPOSE family's validated ``pr_result`` is presentation material
        that the v1 persistence decision deliberately does not store, so it
        is carried in memory to the same invocation's bounded publication
        continuation. Every other family re-derives its continuation from
        committed durable state and receives ``None``; the PR route fails
        closed without a genuine ``PrResult``.
        """

    def handoff_settled_interrupt(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        external_session_id: str,
    ) -> None:
        """Route one settled interrupted turn to its Interrupt settlement
        (issue #342).

        Invoked strictly after the settlement composition that committed the
        receipt's ``interrupted_turn`` settlement and the exact session's
        intent observation — by the process that observed the settlement:
        the observation host's terminal routing, or the dispatching
        service's acknowledgement-attach path that consumed a staged early
        completion. The implementation re-derives the annotated unsettled
        intent from committed durable state, releases the attached adapter's
        D6b guard discipline with no database transaction across it, and
        records the intent's outcome through the same one-shot conditional
        write the Owner settlement branch uses.

        Contained best-effort by contract: the implementation never raises;
        a failed settlement leaves the durable annotated intent exactly as
        committed and the Owner-facing settlement branch remains the
        authority.
        """
        ...
