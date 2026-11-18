"""Runtime-neutral shared instruction content for agent-runtime turns.

The fixed authorized resumption instruction (issue #343) is OpenOrc's own
workflow content, not provider mechanics: the same constant is the content
the Cline adapter's blocking same-session resume send realizes and the
content the detached Owner-authorized Resume dispatch admits verbatim as
one independent detached turn. Defining it once keeps the authorized
resumption literally one instruction across execution modes and delivery
mechanics; prompt customization may change instructions, never protocol,
authority, session semantics, or state-machine meaning.
"""

from typing import Final

__all__ = ["RESUMPTION_INSTRUCTION"]


RESUMPTION_INSTRUCTION: Final[str] = "[TASK RESUMPTION] Please continue where you left off."
