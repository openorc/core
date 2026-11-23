"""Wire DTOs for the Task discussion and conversation surface (issue #339).

Thin transport models only: the Owner-authorized application services own
every semantic rule (discussion admission authority, gate currentness,
receipt lifecycle, conversation identity) and these models never duplicate
service/domain validation — FastAPI parses the declared shapes and the
router delegates. No model ever carries conversation text durably: the
request message travels to the operation invocation, and the conversation
read projects the runtime-owned messages transiently.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from openorc.domain.detached_turns import TaskDetachedTurnReceiptStatus

__all__ = [
    "ArchivedTaskPurge",
    "DiscussionAdmissionRead",
    "DiscussionMessageCreate",
    "DiscussionStatusRead",
    "ReviewerConversationMessageRead",
    "ReviewerConversationRead",
]


class DiscussionMessageCreate(BaseModel):
    """One verbatim Owner message for the addressed current OwnerGate.

    Bounded to the service's own discussion message bound — the wire bound
    mirrors it so an oversized body is rejected at the boundary before any
    service effect. The message is Owner-authored conversational command
    input: transient on the wire and in the operation invocation, never
    durably stored by OpenOrc.
    """

    message: str = Field(min_length=1, max_length=20_000)


class DiscussionAdmissionRead(BaseModel):
    """The 202 acknowledgement of one authorized-and-queued discussion message.

    This is explicitly NOT delivery and carries no model reply: the receipt
    is committed and claimed, the admission operation was accepted for
    delivery, and the receipt-status projection reports the actual
    admission, refusal, or recorded exchange lifecycle.
    """

    receipt_id: UUID
    owner_gate_id: UUID
    reviewer_session_id: UUID
    task_id: UUID
    status: TaskDetachedTurnReceiptStatus
    claimed_at: datetime


class DiscussionStatusRead(BaseModel):
    """The Owner-visible lifecycle of one gate-bound discussion exchange.

    ``admission_attempted_at`` lets a ``claimed`` status distinguish an
    authorized message whose admission has not yet been attempted from one
    whose attempt is unresolved. ``outcome_label`` is the safe settlement or
    refusal label exactly as recorded (``None`` while claimed or admitted).
    """

    receipt_id: UUID
    status: TaskDetachedTurnReceiptStatus
    claimed_at: datetime
    admission_attempted_at: datetime | None
    admitted_at: datetime | None
    settled_at: datetime | None
    outcome_label: str | None


class ReviewerConversationMessageRead(BaseModel):
    """One projected conversation message (the runtime's own record shape).

    ``index`` is the zero-based conversation position; ``kind`` is the
    runtime message's own category token when present; ``text`` is its
    textual content when present. A message shaped beyond the projection
    carries ``None`` fields — honestly unknown, never guessed.
    """

    index: int
    kind: str | None
    text: str | None


class ReviewerConversationRead(BaseModel):
    """The transient projection of the Task's Reviewer session conversation.

    Read through the runtime-native transcript surface at request time; the
    runtime owns the conversation and OpenOrc records no transcript.
    """

    task_id: UUID
    reviewer_session_id: UUID
    messages: list[ReviewerConversationMessageRead]


class ArchivedTaskPurge(BaseModel):
    """The settled result of one archived-Task administrative purge (issue #374).

    Identifies the purged archived attempt within its Workspace. Only
    archived (terminal) attempts are purgeable — a current Task is a typed
    conflict, and normal workflow cancellation is the supported route. The
    purge is internal-state deletion only: the backing GitHub issue,
    branch, and PR are never touched.
    """

    workspace_id: UUID
    task_id: UUID
