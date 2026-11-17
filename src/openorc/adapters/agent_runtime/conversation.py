"""The optional runtime-neutral session-conversation read capability (issue #339).

Beside the universal ``AgentRuntimeAdapter`` contract — never expanding its
three required blocking v1 operations — concrete adapters may implement the
session-conversation read capability structurally: one pure observational
read of an exact external session's current conversation, projected into a
small neutral record shape. Application services duck-type the capability
fail-closed before reading; a runtime without the capability keeps its
conversation runtime-owned and unread by OpenOrc.

The read is deliberately transient: it takes no external effect, latches no
uncertain-effect state, and records nothing durable. The projected records
are the runtime's own conversation content made visible to the authorized
Owner-facing client — never workflow authority, never a formal response, and
never an OpenOrc transcript store. A message shaped beyond the projection is
honestly projected as unknown (``None`` kind/text) rather than failing the
whole read or being guessed into a shape the runtime never declared.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

__all__ = ["ConversationMessage", "SessionConversationReader"]


@dataclass(frozen=True)
class ConversationMessage:
    """One projected conversation message of an observed runtime session.

    ``index`` is the message's zero-based position in the session's own
    conversation order. ``kind`` is the runtime message's own category token
    when its native record carries one as a string (the runtime owns its
    conversation taxonomy; the projection never reinterprets it). ``text`` is
    the message's textual content when its native record carries one as a
    string. The projection is a transient read of the runtime-owned
    conversation — never workflow authority, never durable OpenOrc history,
    and never a transcript store.
    """

    index: int
    kind: str | None
    text: str | None

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0:
            raise ValueError("ConversationMessage.index must be a non-negative integer")
        if self.kind is not None and (not isinstance(self.kind, str) or not self.kind.strip()):
            raise ValueError("ConversationMessage.kind must be None or a non-empty string")
        if self.text is not None and (not isinstance(self.text, str) or not self.text.strip()):
            raise ValueError("ConversationMessage.text must be None or a non-empty string")


class SessionConversationReader(Protocol):
    """Structural protocol of the optional conversation-read capability.

    Concrete adapters implement the capability method (the universal
    ``AgentRuntimeAdapter`` surface is never expanded); application services
    duck-type the capability fail-closed before reading.
    """

    def read_conversation(self, session_id: str) -> list[ConversationMessage]:
        """Project the session's current conversation records, in order."""
        ...
