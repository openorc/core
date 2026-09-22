"""Safe OpenOrc telemetry attribute vocabulary (issue #108).

Telemetry attributes are contextual diagnostics: never workflow authority and
never a second copy of canonical state. This module is the single stable
vocabulary for the safe OpenOrc identifiers and context that may appear on
spans and log records, plus the typed helper that applies it.

The redaction discipline is structural: the annotation helper accepts only
the explicit safe keyword arguments defined here, so raw secrets, bearer
tokens, authorization headers, Supabase secret/admin keys, GitHub
installation tokens, runtime credentials, Workspace guidance prose, prompt
bodies, agent transcripts, or arbitrary payload bodies have no supported
path into telemetry attributes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.trace import Span

REQUEST_ID = "openorc.request_id"
WORKSPACE_ID = "openorc.workspace_id"
TASK_ID = "openorc.task_id"
EXECUTION_ID = "openorc.execution_id"
CONNECTION_ID = "openorc.connection_id"
WORKFLOW_ROLE = "openorc.workflow_role"
OPERATION = "openorc.operation"
GITHUB_REPOSITORY = "openorc.github_repository"
GITHUB_ISSUE_NUMBER = "openorc.github_issue_number"
GITHUB_PULL_REQUEST_NUMBER = "openorc.github_pull_request_number"
GITHUB_HEAD_SHA = "openorc.github_head_sha"
RQ_JOB_ID = "openorc.rq_job_id"


def annotate_span(
    span: Span,
    *,
    operation: str | None = None,
    workspace_id: str | None = None,
    task_id: str | None = None,
    execution_id: str | None = None,
    connection_id: str | None = None,
    workflow_role: str | None = None,
    github_repository: str | None = None,
    github_issue_number: int | None = None,
    github_pull_request_number: int | None = None,
    github_head_sha: str | None = None,
    rq_job_id: str | None = None,
) -> None:
    """Attach only the provided safe vocabulary attributes to ``span``.

    ``None`` values are skipped. Callers pass stable identifiers as strings
    (``str(uuid)``). The helper has no free-form context parameter, so
    secret-bearing values cannot be attached through this boundary.
    """
    candidates: tuple[tuple[str, str | int | None], ...] = (
        (OPERATION, operation),
        (WORKSPACE_ID, workspace_id),
        (TASK_ID, task_id),
        (EXECUTION_ID, execution_id),
        (CONNECTION_ID, connection_id),
        (WORKFLOW_ROLE, workflow_role),
        (GITHUB_REPOSITORY, github_repository),
        (GITHUB_ISSUE_NUMBER, github_issue_number),
        (GITHUB_PULL_REQUEST_NUMBER, github_pull_request_number),
        (GITHUB_HEAD_SHA, github_head_sha),
        (RQ_JOB_ID, rq_job_id),
    )
    for name, value in candidates:
        if value is not None:
            span.set_attribute(name, value)
