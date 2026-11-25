"""Authorized WorkflowEvent audit read services (issue #377).

The read-side composition behind the authorized WorkflowEvent audit
surface — the non-live counterpart to G4's authorized SSE: Owner-authorized,
database-only, and strictly side-effect free. Two scopes share one contract:
the Workspace's visible events (its Task-scoped facts plus its Workspace-level
facts) and one exact Task's event history (current or archived attempt). Both
are bounded, deterministically ordered newest-first by the durable
``(created_at, id)`` key, and advanced by an opaque keyset cursor bound to
the exact scope and filter set.

Historical ordering is not commit order: ``workflow_events`` UUID/created_at
values cannot prove the instant a transaction actually committed, so the page
cursor is a stable continuation over visible committed records — never a
forward SSE high-water mark that provably prevents missing late commits,
never a frozen multi-request snapshot, and never a commit-order/replay
sequence. G4 (issue #94) owns the bounded reconnect/replay mechanism and any
additional durable ordering evidence it needs; this surface adds no
sequencing system. G4 also owns the audit/invalidation distinction: a
WorkflowEvent is a consequential committed audit fact, not a generic
notification for every durable mutation, and no event type exists for UI
reactivity.

The safe public projection: every projected ``context`` passes through the
per-event-type established shape inventory — the exact context keys the
write-side producers (``event_coordination`` helpers and the direct writer
call sites) record, validated by both key and value. Established safe
historical values are preserved verbatim; unknown or unsupported keys, and
values outside a key's established safe shape, are omitted fail-closed — an
allowlisted key alone never makes an arbitrary string value safe. Raw
provider/runtime payloads, credentials, prompts, transcripts, and
foreign-Workspace content have no parameter anywhere in this surface. Actor
and subject provenance are projected exactly as stored; nothing is mutated
or invented.

Cross-Workspace isolation is absolute (the G2a/G2b rule): a foreign-Workspace
or absent subject is the uniform not-found outcome, never a probing oracle.
Reads never mutate state, query external providers, initiate recovery, or
reconstruct canonical Task state from events.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Final
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.events import (
    WorkflowEvent,
    WorkflowEventActor,
    WorkflowEventType,
)
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.reviews import ReviewOutcome
from openorc.observability import annotate_span, application_span
from openorc.persistence.events import (
    list_task_events_page,
    list_workspace_events_page,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.services.errors import InvalidCommandError
from openorc.services.event_coordination import OwnerReviewerDiscussionDirection
from openorc.services.transaction_composition import composed_read_transaction
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_task,
)

__all__ = [
    "EVENT_PAGE_LIMIT",
    "EVENT_PAGE_LIMIT_MAX",
    "EventPageCursor",
    "EventPageFilters",
    "PublicWorkflowEvent",
    "decode_event_page_cursor",
    "encode_event_page_cursor",
    "list_task_events_for_owner",
    "list_workspace_events_for_owner",
    "project_public_context",
]

# The bounded page size (the established G2b read default) and the shared
# API paging maximum it aligns with (issue #377: default 50; the established
# shared bound governs the maximum).
EVENT_PAGE_LIMIT = 50
EVENT_PAGE_LIMIT_MAX = 200

_EVENTS_TRACER_SCOPE: Final[str] = "openorc.services.event_reads"
_EVENTS_LIST_SPAN_NAME: Final[str] = "event_reads.list_events_for_owner"

_WORKSPACE_SCOPE_KEY: Final[str] = "w"
_TASK_SCOPE_KEY: Final[str] = "t"
_EVENT_CURSOR_PREFIX: Final[str] = "e"


@dataclass(frozen=True, slots=True)
class EventPageFilters:
    """One event listing's exact filter binding (issue #377).

    Every value is already validated and UTC-normalized. The cursor binds
    these exact values, so a continuation can never silently change the
    filter set or the scope it pages.
    """

    event_type: WorkflowEventType | None
    actor_type: WorkflowEventActor | None
    from_created_at: datetime | None
    to_created_at: datetime | None


@dataclass(frozen=True, slots=True)
class EventPageCursor:
    """One decoded opaque event page cursor (issue #377)."""

    scope_key: str
    scope_id: UUID
    event_type: WorkflowEventType | None
    actor_type: WorkflowEventActor | None
    from_created_at: datetime | None
    to_created_at: datetime | None
    after_created_at: datetime
    after_event_id: UUID


@dataclass(frozen=True, slots=True)
class PublicWorkflowEvent:
    """One projected public WorkflowEvent audit record (issue #377).

    The safe public shape of one committed audit fact: the locked
    vocabularies plus scope, subject, and actor provenance exactly as
    stored, and the context filtered through the event type's established
    safe shape inventory (safe historical values preserved verbatim;
    unsupported keys and unsafe values omitted fail-closed).
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID | None
    event_type: str
    actor_type: str
    actor_id: str | None
    subject_type: str | None
    subject_id: UUID | None
    context: Mapping[str, object]
    created_at: datetime


def _micros_key(value: datetime | None) -> str:
    """Render one UTC instant as its opaque epoch-micros cursor part."""
    if value is None:
        return ""
    return str(int(value.astimezone(UTC).timestamp() * 1_000_000))


def _micros_instant(value: str) -> datetime | None:
    """Restore one epoch-micros cursor part to its UTC instant."""
    if value == "":
        return None
    return datetime.fromtimestamp(int(value) / 1_000_000, tz=UTC)


def _enum_key(value: WorkflowEventType | WorkflowEventActor | None) -> str:
    return "" if value is None else value.value


def encode_event_page_cursor(
    scope_key: str,
    scope_id: UUID,
    filters: EventPageFilters,
    last_created_at: datetime,
    last_event_id: UUID,
) -> str:
    """Return the opaque keyset cursor for one page's last ``(created_at, id)`` key.

    The cursor binds the exact listing scope (Workspace or Task) and the
    exact validated filter set, so a continuation is only ever a
    continuation of the same listing.
    """
    raw = (
        f"{_EVENT_CURSOR_PREFIX}:{scope_key}:{scope_id}"
        f":{_enum_key(filters.event_type)}:{_enum_key(filters.actor_type)}"
        f":{_micros_key(filters.from_created_at)}:{_micros_key(filters.to_created_at)}"
        f":{_micros_key(last_created_at)}:{last_event_id}"
    ).encode("ascii")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_event_page_cursor(cursor: str) -> EventPageCursor:
    """Decode one opaque event page cursor, or raise the typed invalid command."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("ascii")
        parts = raw.split(":")
        if len(parts) != 9 or parts[0] != _EVENT_CURSOR_PREFIX:
            raise ValueError("unexpected cursor shape")
        scope_key = parts[1]
        if scope_key not in (_WORKSPACE_SCOPE_KEY, _TASK_SCOPE_KEY):
            raise ValueError("unexpected cursor scope")
        after_created_at = _micros_instant(parts[7])
        if after_created_at is None:
            raise ValueError("missing continuation key")
        return EventPageCursor(
            scope_key=scope_key,
            scope_id=UUID(parts[2]),
            event_type=None if parts[3] == "" else WorkflowEventType(parts[3]),
            actor_type=None if parts[4] == "" else WorkflowEventActor(parts[4]),
            from_created_at=_micros_instant(parts[5]),
            to_created_at=_micros_instant(parts[6]),
            after_created_at=after_created_at,
            after_event_id=UUID(parts[8]),
        )
    except (ValueError, UnicodeDecodeError, TypeError) as error:
        raise InvalidCommandError("the cursor is not a valid event page cursor") from error


# ---------------------------------------------------------------------------
# The safe public projection (issue #377).
#
# The per-event-type established context shape inventory: the exact keys the
# write-side producers record (the ``event_coordination`` helpers and the
# direct writer call sites in ``task_intake`` and ``github_issue_relations``),
# each validated by key AND value. A key's validator returns the safe value
# verbatim, or the omission sentinel when the value does not match the key's
# established safe shape — an allowlisted key alone never makes an arbitrary
# string value safe.
# ---------------------------------------------------------------------------

_OMIT: Final[object] = object()


def _safe_vocabulary(values: frozenset[str]) -> Callable[[object], object]:
    def validate(value: object) -> object:
        if isinstance(value, str) and value in values:
            return value
        return _OMIT

    return validate


def _safe_uuid_text(value: object) -> object:
    """Accept only a UUID-shaped string (the established identity-text shape)."""
    if not isinstance(value, str):
        return _OMIT
    try:
        UUID(value)
    except ValueError:
        return _OMIT
    return value


def _safe_int(value: object) -> object:
    if isinstance(value, bool) or not isinstance(value, int):
        return _OMIT
    return value


def _safe_decimal_text(value: object) -> object:
    """Accept only a finite exact-decimal string (the established money shape)."""
    if not isinstance(value, str):
        return _OMIT
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return _OMIT
    if not parsed.is_finite():
        return _OMIT
    return value


def _safe_config_value(value: object) -> object:
    """Accept the established configuration-change value shape.

    ``None`` (a legitimate audited disabling value), an exact integer (the
    review-iteration limit), or a finite exact-decimal string (the default
    Task budget). Nothing else was ever written by a producer.
    """
    if value is None:
        return None
    if isinstance(value, (bool, float)):
        return _OMIT
    if isinstance(value, int):
        return value
    return _safe_decimal_text(value)


_FAILURE_KIND_TOKENS: Final[frozenset[str]] = frozenset(
    {
        # The locked, content-safe classification tokens the existing
        # producers pass to ``record_execution_failed_event``:
        # ``authority_stale``/``runtime_pre_effect``/
        # ``observed_run_failure`` (task_authorized_implementation_execution,
        # task_pr_remediation, task_runtime_send_claims,
        # task_workflow_operation_coordinator) and
        # ``malformed_run_result``/``failed_workspace_result``
        # (task_detached_turn_correlation). A failure kind is one of these
        # stable classification tokens and nothing else: a short
        # credential-like string, an exception message, prompt or transcript
        # prose, or any other unsupported value is omitted fail-closed from
        # the public projection. A new producer token extends this set
        # deliberately (its alignment is asserted by the read tests).
        "authority_stale",
        "runtime_pre_effect",
        "observed_run_failure",
        "malformed_run_result",
        "failed_workspace_result",
    }
)


_SETTING_TOKENS: Final[frozenset[str]] = frozenset(
    {
        "review_iteration_limit",
        "default_task_budget_usd",
        "guidance",
        "workflow_role_binding",
        "connection",
    }
)

_GATE_OUTCOMES: Final[frozenset[str]] = frozenset(status.value for status in OwnerGateStatus)
_REVIEW_OUTCOMES: Final[frozenset[str]] = frozenset(outcome.value for outcome in ReviewOutcome)
_ROLE_VALUES: Final[frozenset[str]] = frozenset(role.value for role in WorkflowRole)
_DIRECTION_VALUES: Final[frozenset[str]] = frozenset(
    direction.value for direction in OwnerReviewerDiscussionDirection
)

# The established shapes, per event type. Event types absent from this
# mapping carry no context by design and project to the empty object; a
# mapping entry with an absent key omits that key; a present key whose value
# fails its validator omits that key. Nothing is ever passed through
# unvalidated.
_SAFE_CONTEXT_SHAPES: Final[dict[WorkflowEventType, dict[str, Callable[[object], object]]]] = {
    WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED: {
        "setting": _safe_vocabulary(_SETTING_TOKENS),
        "previous": _safe_config_value,
        "new": _safe_config_value,
        "role": _safe_vocabulary(_ROLE_VALUES),
        "connection_id": _safe_uuid_text,
    },
    WorkflowEventType.OWNER_GATE_RESOLVED: {
        "outcome": _safe_vocabulary(_GATE_OUTCOMES),
        "review_iteration_id": _safe_uuid_text,
        "review_outcome": _safe_vocabulary(_REVIEW_OUTCOMES),
        "cost_effective_threshold_usd": _safe_decimal_text,
        "cost_observed_usd": _safe_decimal_text,
        "cost_authorized_budget_usd": _safe_decimal_text,
    },
    WorkflowEventType.OWNER_REVIEWER_DISCUSSION_MESSAGE: {
        "direction": _safe_vocabulary(_DIRECTION_VALUES),
    },
    WorkflowEventType.EXECUTION_FAILED: {
        "failure_kind": _safe_vocabulary(_FAILURE_KIND_TOKENS),
    },
    WorkflowEventType.PLAN_REVIEWED: {
        "outcome": _safe_vocabulary(_REVIEW_OUTCOMES),
    },
    WorkflowEventType.PR_REVIEWED: {
        "outcome": _safe_vocabulary(_REVIEW_OUTCOMES),
    },
    WorkflowEventType.MERGE_REQUESTED: {
        "owner_gate_id": _safe_uuid_text,
    },
    WorkflowEventType.TASK_RELATIONSHIP_SYNCED: {
        "repository_id": _safe_uuid_text,
        "github_issue_id": _safe_int,
    },
    WorkflowEventType.TASK_DEPENDENCY_SYNCED: {
        "repository_id": _safe_uuid_text,
        "github_issue_id": _safe_int,
    },
}


def project_public_context(event: WorkflowEvent) -> dict[str, object]:
    """Project one event's stored context through its established safe shape.

    The one read-side sanitization boundary: unknown event types (which
    carry no context by design), unknown context keys, and values outside a
    key's established safe shape all fail closed to omission. Established
    safe historical values are preserved verbatim.
    """
    shape = _SAFE_CONTEXT_SHAPES.get(event.event_type)
    if shape is None:
        return {}
    projected: dict[str, object] = {}
    for key, validate in shape.items():
        if key not in event.context:
            continue
        safe = validate(event.context[key])
        if safe is not _OMIT:
            projected[key] = safe
    return projected


def _public_event(event: WorkflowEvent) -> PublicWorkflowEvent:
    """Project one committed event into its safe public shape."""
    return PublicWorkflowEvent(
        id=event.id,
        workspace_id=event.workspace_id,
        task_id=event.task_id,
        event_type=event.event_type.value,
        actor_type=event.actor_type.value,
        actor_id=event.actor_id,
        subject_type=event.subject_type,
        subject_id=event.subject_id,
        context=project_public_context(event),
        created_at=event.created_at,
    )


def _validated_filters(
    event_type: WorkflowEventType | None,
    actor_type: WorkflowEventActor | None,
    from_created_at: datetime | None,
    to_created_at: datetime | None,
) -> EventPageFilters:
    """Validate the canonical filter values and UTC-normalize the bounds."""
    if event_type is not None and not isinstance(event_type, WorkflowEventType):
        raise InvalidCommandError("the event_type filter must be a canonical WorkflowEventType")
    if actor_type is not None and not isinstance(actor_type, WorkflowEventActor):
        raise InvalidCommandError("the actor_type filter must be a canonical WorkflowEventActor")
    from_utc = _validated_instant(from_created_at, "from")
    to_utc = _validated_instant(to_created_at, "to")
    if from_utc is not None and to_utc is not None and from_utc >= to_utc:
        raise InvalidCommandError("the from bound must be earlier than the to bound")
    return EventPageFilters(
        event_type=event_type,
        actor_type=actor_type,
        from_created_at=from_utc,
        to_created_at=to_utc,
    )


def _validated_instant(value: datetime | None, name: str) -> datetime | None:
    """Require a timezone-aware instant, normalized to UTC (never naive)."""
    if value is None:
        return None
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.tzinfo.utcoffset(value) is None
    ):
        raise InvalidCommandError(f"the {name} bound must be a timezone-aware timestamp")
    return normalize_utc(value)


def _validated_limit(limit: int) -> int:
    """Require the bounded page size (the shared API paging maximum)."""
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= EVENT_PAGE_LIMIT_MAX
    ):
        raise InvalidCommandError(
            f"the limit must be an integer between 1 and {EVENT_PAGE_LIMIT_MAX}"
        )
    return limit


def _validated_cursor(
    cursor: str | None,
    *,
    scope_key: str,
    scope_id: UUID,
    filters: EventPageFilters,
) -> EventPageCursor | None:
    """Accept only a cursor bound to this exact scope and filter set."""
    if cursor is None:
        return None
    decoded = decode_event_page_cursor(cursor)
    if decoded.scope_key != scope_key or decoded.scope_id != scope_id:
        raise InvalidCommandError("the cursor does not match this event listing")
    if (
        decoded.event_type != filters.event_type
        or decoded.actor_type != filters.actor_type
        or decoded.from_created_at != filters.from_created_at
        or decoded.to_created_at != filters.to_created_at
    ):
        raise InvalidCommandError("the cursor does not match this listing's filters")
    return decoded


def _page(
    pool: DatabasePool,
    *,
    scope_key: str,
    scope_id: UUID,
    filters: EventPageFilters,
    page_cursor: EventPageCursor | None,
    limit: int,
    read_page: Callable[..., list[WorkflowEvent]],
) -> tuple[list[PublicWorkflowEvent], str | None]:
    """Compose one bounded page behind the already-applied authorization.

    Database-only and side-effect free: the limit+1 probe decides has-more,
    the page truncates to the requested bound, and the next cursor binds the
    exact scope and filter set. The response reflects historical ordering
    over visible committed records — not a frozen multi-request snapshot and
    not a commit-order/replay sequence.
    """
    with application_span(_EVENTS_TRACER_SCOPE, _EVENTS_LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_EVENTS_LIST_SPAN_NAME)
        with composed_read_transaction(pool) as tx_view:
            events = read_page(
                tx_view,
                after_created_at=None if page_cursor is None else page_cursor.after_created_at,
                after_event_id=None if page_cursor is None else page_cursor.after_event_id,
                limit=limit + 1,
                event_type=filters.event_type,
                actor_type=filters.actor_type,
                from_created_at=filters.from_created_at,
                to_created_at=filters.to_created_at,
            )
            has_more = len(events) > limit
            events = events[:limit]
            next_cursor = (
                encode_event_page_cursor(
                    scope_key, scope_id, filters, events[-1].created_at, events[-1].id
                )
                if has_more and events
                else None
            )
        return [_public_event(event) for event in events], next_cursor


def list_workspace_events_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    event_type: WorkflowEventType | None = None,
    actor_type: WorkflowEventActor | None = None,
    from_created_at: datetime | None = None,
    to_created_at: datetime | None = None,
    cursor: str | None = None,
    limit: int = EVENT_PAGE_LIMIT,
) -> tuple[list[PublicWorkflowEvent], str | None]:
    """List one Workspace's visible events for the authenticated Owner (#377).

    Owner-authorized, database-only, and side-effect free: every visible
    event within the Workspace — its Task-scoped facts and its
    Workspace-level facts — bounded, deterministically ordered newest-first
    by ``(created_at, id)``, and advanced by the opaque ``next_cursor`` when
    more items exist. The cursor is bound to this exact scope and filter
    set; a foreign-Workspace subject is the uniform not-found outcome.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    filters = _validated_filters(event_type, actor_type, from_created_at, to_created_at)
    page_cursor = _validated_cursor(
        cursor, scope_key=_WORKSPACE_SCOPE_KEY, scope_id=workspace_id, filters=filters
    )
    limit = _validated_limit(limit)
    require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)

    def read_page(tx_view: DatabasePool, **page_command: object) -> list[WorkflowEvent]:
        return list_workspace_events_page(
            tx_view,  # type: ignore[arg-type]
            workspace_id=workspace_id,  # type: ignore[arg-type]
            **page_command,  # type: ignore[arg-type]
        )

    return _page(
        pool,
        scope_key=_WORKSPACE_SCOPE_KEY,
        scope_id=workspace_id,
        filters=filters,
        page_cursor=page_cursor,
        limit=limit,
        read_page=read_page,
    )


def list_task_events_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    event_type: WorkflowEventType | None = None,
    actor_type: WorkflowEventActor | None = None,
    from_created_at: datetime | None = None,
    to_created_at: datetime | None = None,
    cursor: str | None = None,
    limit: int = EVENT_PAGE_LIMIT,
) -> tuple[list[PublicWorkflowEvent], str | None]:
    """List one exact Task's events for the authenticated Owner (#377).

    Owner-authorized, database-only, and side-effect free: the exact
    authorized Task attempt's events — current or archived — with the same
    filters, ordering, and cursor binding as the Workspace scope. The
    Task-scope guard resolves the exact subject; a foreign-Workspace or
    absent Task is the uniform not-found outcome. Reads never reconstruct
    canonical Task state from events.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    filters = _validated_filters(event_type, actor_type, from_created_at, to_created_at)
    page_cursor = _validated_cursor(
        cursor, scope_key=_TASK_SCOPE_KEY, scope_id=task_id, filters=filters
    )
    limit = _validated_limit(limit)
    require_workspace_task(pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id)

    def read_page(tx_view: DatabasePool, **page_command: object) -> list[WorkflowEvent]:
        return list_task_events_page(
            tx_view,  # type: ignore[arg-type]
            task_id=task_id,  # type: ignore[arg-type]
            **page_command,  # type: ignore[arg-type]
        )

    return _page(
        pool,
        scope_key=_TASK_SCOPE_KEY,
        scope_id=task_id,
        filters=filters,
        page_cursor=page_cursor,
        limit=limit,
        read_page=read_page,
    )


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any read runs."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
