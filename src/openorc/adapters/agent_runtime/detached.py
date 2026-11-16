"""The optional runtime-neutral detached-turn capability (issues #324/#348).

Beside the universal ``AgentRuntimeAdapter`` contract — never expanding its
three required blocking v1 operations — concrete adapters may implement the
detached capability structurally: allocate a fresh external session without
any model turn, then admit composed content as one detached turn through the
runtime's own nonblocking admission. ``AgentTurnAdmission``'s
``turn_reference`` is the opaque externally admitted turn reference the
runtime acknowledged: transport evidence for later exact correlation, never
a workflow decision, never proof of execution, formal-result delivery, or
success. Admission is deliberately distinct from a ``send`` outcome and from
readiness, and an uncertain admission (a timeout or transport loss) is a
typed normalized uncertainty that never authorizes a resend.

Application services duck-type the capability fail-closed before dispatching
detached work; a runtime without the capability keeps the blocking
readiness handshake. The runtime's native admission mechanics, event
vocabulary, and attribution interpretation stay behind the concrete adapter
boundary (issue #348); nothing SDK-shaped crosses here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from openorc.adapters.agent_runtime.results import (
    AgentSessionAllocation,
    AgentSessionCreationRequest,
)

__all__ = ["AgentTurnAdmission", "DetachedTurnRuntime"]


@dataclass(frozen=True)
class AgentTurnAdmission:
    """One admitted detached turn.

    ``turn_reference`` is the exact opaque externally admitted turn
    reference the runtime's nonblocking admission acknowledged. It is
    transport evidence for later exact correlation — never a workflow
    decision, never proof of execution, formal-result delivery, or success.
    Admission is deliberately distinct from a ``send`` outcome and from
    readiness.
    """

    turn_reference: str

    def __post_init__(self) -> None:
        if not isinstance(self.turn_reference, str) or not self.turn_reference.strip():
            raise ValueError("AgentTurnAdmission.turn_reference must be a non-empty string")


class DetachedTurnRuntime(Protocol):
    """Structural protocol of the optional detached-turn capability.

    Concrete adapters implement the two capability methods (the universal
    ``AgentRuntimeAdapter`` surface is never expanded); application services
    duck-type the capability fail-closed before dispatching detached work.
    """

    def allocate_detached_session(
        self, request: AgentSessionCreationRequest
    ) -> AgentSessionAllocation:
        """Allocate the fresh external session; no model turn, no readiness."""
        ...

    def admit_detached_turn(self, session_id: str, content: str) -> AgentTurnAdmission:
        """Admit the composed content as one detached turn on the exact session."""
        ...
