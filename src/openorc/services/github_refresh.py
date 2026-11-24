"""Authorized Workspace/Repository GitHub refresh admission and status
services (issue #375).

The Owner-facing boundary of the scoped on-demand GitHub refresh. The
service owns authorization and admission; routers stay thin transports.

- **Admission** composes the authenticated Owner authorization (the
  #52/#57 Workspace-ownership guards, uniform anti-probing not-found) with
  the atomic durable admission (the per-scope active-overlap containment,
  expired-record recovery, bounded retention) and the F1 execution
  submission. HTTP 202 acknowledges authorization and established
  submission acceptance only: a F1 submission the operation boundary
  refused before delivery is a durable terminal ``failed`` record and a
  typed failure — never a 202; an uncertain submission outcome is never
  converted into a known non-delivery, the record stays ``accepted``
  (recoverable through its bounded lease), and the typed uncertainty is
  raised.
- **Status** re-derives Workspace ownership and presents the stored record
  truthfully: an expired ``accepted``/``running`` record projects as
  ``unknown`` (lost completion evidence is never success). Reads never
  mutate.
- The authoritative refresh execution re-derives its scope from durable
  state in the executor; this boundary never queues authorization facts.

No generic job framework, no second workflow/event ledger, no periodic
scheduler: the minimum durable refresh-specific execution tracking lives in
``openorc.github_refreshes`` and its recovery is strictly on-demand.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from openorc.domain.github_refreshes import (
    GitHubRefreshRecord,
    GitHubRefreshStatus,
    GitHubRefreshSummary,
    GitHubRefreshUnitError,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_refreshes import (
    admit_github_refresh,
    fail_unclaimed_github_refresh,
    get_github_refresh,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_repository,
)

__all__ = [
    "GitHubRefreshProjection",
    "RefreshExecutionSubmission",
    "RefreshSubmissionOutcome",
    "get_workspace_github_refresh_for_owner",
    "request_repository_github_refresh_for_owner",
    "request_workspace_github_refresh_for_owner",
]

# Application-service span boundary (issues #108/#109): one span per public
# use-case operation. Only the safe attribute vocabulary is attachable.
_SERVICE_TRACER_SCOPE = "openorc.services.github_refresh"
_ADMIT_WS_SPAN_NAME = "github_refresh.request_workspace_github_refresh_for_owner"
_ADMIT_REPO_SPAN_NAME = "github_refresh.request_repository_github_refresh_for_owner"
_STATUS_SPAN_NAME = "github_refresh.get_workspace_github_refresh_for_owner"


class RefreshSubmissionOutcome(StrEnum):
    """The explicit submission outcome of one refresh execution enqueue."""

    ACCEPTED = "accepted"
    KNOWN_REFUSED = "known_refused"
    UNCERTAIN = "uncertain"


class RefreshExecutionSubmission(Protocol):
    """The F1-backed execution-submission seam the admission composes.

    Mode neutral by construction: the transport supplies the process-local
    seam (the bounded local executor in regular execution mode, the
    canonical queue submission in queue mode). Acceptance is never
    completion; a known refusal and an uncertain outcome are distinct and
    never retried or replayed here.
    """

    def submit(self, *, refresh_id: UUID) -> RefreshSubmissionOutcome: ...


@dataclass(frozen=True, slots=True)
class GitHubRefreshProjection:
    """One refresh record's truthfully presented status projection.

    The stored record presented verbatim, except that an expired
    ``accepted``/``running`` record projects as ``unknown``: its completion
    evidence is lost, and lost completion evidence is never success. Reads
    never mutate the durable row.
    """

    id: UUID
    workspace_id: UUID
    repository_id: UUID | None
    status: GitHubRefreshStatus
    requested_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    summary: GitHubRefreshSummary | None
    errors: tuple[GitHubRefreshUnitError, ...]


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _admit_and_submit(
    pool: DatabasePool,
    submission: RefreshExecutionSubmission,
    *,
    workspace_id: UUID,
    repository_id: UUID | None,
) -> GitHubRefreshRecord:
    """Run the atomic admission and the execution submission, truthfully."""
    admission = admit_github_refresh(pool, workspace_id=workspace_id, repository_id=repository_id)
    if admission.workspace_missing:
        # The Workspace vanished between authorization and admission (a
        # deletion race): the uniform not-found outcome, never a probing
        # oracle.
        raise NotFoundError("the requested workspace is not available")
    if admission.conflict or admission.record is None:
        raise ConflictError(
            "an active github refresh already covers this scope; retry after it resolves"
        )
    record = admission.record
    outcome = submission.submit(refresh_id=record.id)
    if outcome is RefreshSubmissionOutcome.ACCEPTED:
        # Established submission acceptance: the only 202-acknowledged fact.
        # Acceptance is never completion.
        return record
    if outcome is RefreshSubmissionOutcome.KNOWN_REFUSED:
        # A definitive refusal before delivery: the refresh will not run.
        # The durable record is truthfully terminal ``failed`` (summary
        # unset — no observation was ever attempted) and the typed failure
        # propagates; never a 202, never a replay.
        refused = fail_unclaimed_github_refresh(
            pool,
            refresh_id=record.id,
            errors=(
                GitHubRefreshUnitError(
                    family="admission",
                    code="submission_refused",
                    workspace_id=record.workspace_id,
                    repository_id=record.repository_id,
                ),
            ),
        )
        assert refused is not None  # the record was just admitted as accepted
        raise ExternalOperationFailedError(
            "the github refresh execution could not be accepted for delivery"
        )
    # An uncertain submission outcome is never a known non-delivery: the
    # work may still be delivered and claimed normally, so the record stays
    # ``accepted`` and its bounded admission lease owns recovery if the
    # delivery never happened. The typed uncertainty propagates.
    raise ExternalOperationUncertainError(
        "the outcome of the github refresh submission is unknown; the durable "
        "record remains the caller-visible truth"
    )


def request_workspace_github_refresh_for_owner(
    pool: DatabasePool,
    submission: RefreshExecutionSubmission,
    *,
    profile_id: UUID,
    workspace_id: UUID,
) -> GitHubRefreshRecord:
    """Admit a Workspace-scope refresh under the authenticated Owner boundary."""
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    with application_span(_SERVICE_TRACER_SCOPE, _ADMIT_WS_SPAN_NAME) as span:
        annotate_span(span, operation=_ADMIT_WS_SPAN_NAME, workspace_id=str(workspace_id))
        require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
        return _admit_and_submit(pool, submission, workspace_id=workspace_id, repository_id=None)


def request_repository_github_refresh_for_owner(
    pool: DatabasePool,
    submission: RefreshExecutionSubmission,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
) -> GitHubRefreshRecord:
    """Admit a Repository-scope refresh under the authenticated Owner boundary."""
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    with application_span(_SERVICE_TRACER_SCOPE, _ADMIT_REPO_SPAN_NAME) as span:
        annotate_span(span, operation=_ADMIT_REPO_SPAN_NAME, workspace_id=str(workspace_id))
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        return _admit_and_submit(
            pool, submission, workspace_id=workspace_id, repository_id=repository_id
        )


def get_workspace_github_refresh_for_owner(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    refresh_id: UUID,
) -> GitHubRefreshProjection:
    """Present one refresh record's truthful status under the Owner boundary.

    The addressed record must belong to the authenticated Owner's Workspace
    (a foreign or absent refresh is the uniform not-found outcome, never a
    probing oracle). An expired ``accepted``/``running`` record projects as
    ``unknown``; reads never mutate.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(refresh_id, "refresh_id")
    with application_span(_SERVICE_TRACER_SCOPE, _STATUS_SPAN_NAME) as span:
        annotate_span(span, operation=_STATUS_SPAN_NAME, workspace_id=str(workspace_id))
        require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
        record = get_github_refresh(pool, refresh_id=refresh_id)
        if record is None or record.workspace_id != workspace_id:
            raise NotFoundError("the requested github refresh is not available in this workspace")
        presented_status = record.status
        if (
            presented_status in (GitHubRefreshStatus.ACCEPTED, GitHubRefreshStatus.RUNNING)
            and record.lease_expires_at is not None
            and record.lease_expires_at < datetime.now(tz=UTC)
        ):
            # Lost completion evidence is never success, and a stuck accepted
            # or running record is truthfully unknown until the next
            # overlapping-scope admission recovers it durably.
            presented_status = GitHubRefreshStatus.UNKNOWN
        return GitHubRefreshProjection(
            id=record.id,
            workspace_id=record.workspace_id,
            repository_id=record.repository_id,
            status=presented_status,
            requested_at=record.requested_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
            summary=record.summary,
            errors=record.errors,
        )
