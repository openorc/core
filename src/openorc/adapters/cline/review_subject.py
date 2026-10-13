"""The Cline PR-review subject-verification seam (issue #163, D5b).

When the Cline adapter realizes an already-authorized REVIEW control whose
subject is the runtime-neutral ``PrReviewSubject`` (exact repository, PR
number, and OpenOrc/GitHub-reconciled head SHA), the parsed
``review_result`` is only released after small current-turn tool evidence
establishes that the persistent Reviewer session actually accessed that
exact subject through its native GitHub MCP. This is a Cline-specific
subject-access/verification seam over the public ``ClineSdkBackend``
``read_messages`` surface — not a cryptographic attestation, not a universal
Reviewer workspace contract, and not GitHub/workflow authority.

Evidence discipline (grounded in the Owner qualification of 2026-10-13 on
the pinned ``cline 3.0.68`` / ``@cline/core 0.0.90`` / ``@cline/sdk 0.0.90``
baseline):

- Verification reads only the raw public transcript the backend already
  exposes: ``tool_use`` blocks (tool name, call id, verbatim input) paired
  by tool-call ID with ``tool_result`` blocks (MCP result content). The
  live ``subscribe`` event stream is not a complete record on the pinned
  baseline and is never an input here.
- Only the two qualified native GitHub MCP shapes count:
  ``Github__pull_request_read(method="get")`` for the assigned repository
  and PR, and ``Github__get_file_contents`` with ``ref`` set to the
  assigned head SHA. Every other tool observation is ignored: this is
  deliberately not a generic MCP/tool-call auditor, and no content
  markers, expected blob-hash registry, or exhaustive changed-file
  verification exists here.
- The current-turn boundary comes from the adapter's pre-send transcript
  snapshot and post-send read: the post-send transcript must be an exact
  value extension of the snapshot, and only the appended suffix may
  satisfy a new assignment. Evidence from earlier review revisions on the
  persistent session can never verify a new one. A failed post-send
  transcript read likewise withholds the review result: a completed send
  is never reclassified as a pre-effect outcome.
- The PR-head check uses final-relevant-observation semantics: among the
  successfully parsed assigned-PR reads of the turn, the last one must
  report ``head.sha`` equal to the assigned SHA. An earlier stale read is
  tolerated when later successful evidence corrects it (qualified T3); a
  final contradicting observation is an unresolved mismatch and fails
  closed (qualified T2). Missing, failed, malformed, wrong-PR, and
  wrong-commit observations are rejected evidence: they never verify the
  subject and never contradict it.
- The file-read check requires at least one successful commit-addressed
  read: ``Github__get_file_contents`` for the assigned repository with
  ``ref == assigned SHA`` whose result binds the same commit through its
  structured ``resource.uri`` field — ``repo://<owner>/<repo>/sha/<sha>/
  contents/<path>`` (the qualified baseline exposes exactly this shape).
  Returned text — including repository-controlled file content — is
  untrusted and is never scanned for bindings: only the structured
  resource URI counts. The blob-SHA text binding is deliberately unused
  here: verifying it requires a per-file expected blob-hash registry,
  which this seam excludes.
- Model prose is never evidence. A syntactically valid ``review_result``
  with failing subject evidence is not released: the adapter raises
  :class:`ClineReviewSubjectUnverifiedError`, a safe non-content-bearing
  ``AgentRuntimeError``. Nothing here logs, persists, attaches to traces,
  or places raw transcripts, source content, credentials, or MCP payloads
  into errors.

Workflow authority stays above this seam: the assigned SHA comes from
OpenOrc/GitHub's canonical PR state, and the existing GitHub reconciliation
and exact-head subject guard own every subsequent current-head check. This
module implements no second GitHub client, no independent PR-head
authority, no persistence, and no workflow transitions.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Final

from openorc.adapters.agent_runtime.errors import AgentRuntimeError
from openorc.adapters.cline.values import JsonObject
from openorc.protocol.interaction import PrReviewSubject

__all__ = [
    "ClineReviewSubjectUnverifiedError",
    "UNREADABLE_EVIDENCE",
    "verify_pr_review_turn",
]

_TOOL_USE_BLOCK: Final = "tool_use"
_TOOL_RESULT_BLOCK: Final = "tool_result"
_IS_ERROR_FIELD: Final = "is_error"

# The two qualified native GitHub MCP tool shapes (Owner qualification,
# 2026-10-13). Deliberately exact: this seam is not a generic MCP auditor.
_PR_READ_TOOL: Final = "Github__pull_request_read"
_PR_READ_METHOD: Final = "get"
_FILE_READ_TOOL: Final = "Github__get_file_contents"

_FULL_SHA: Final = re.compile(r"[0-9a-f]{40}")
# The qualified commit-binding resource URI of a commit-addressed file read.
_COMMIT_URI: Final = re.compile(r"repo://([^/\"\\\s]+)/([^/\"\\\s]+)/sha/([0-9a-f]{40})/contents/")

# Bound on the observed MCP/JSON wrapper nesting of a tool result payload.
_MAX_RESULT_LAYERS: Final = 4

# Safe machine reasons, static by construction: no transcript, tool input or
# output content, repository path, SHA, or credential ever enters them.
_UNREADABLE_SUBJECT: Final = (
    "the assigned review subject's repository identity could not be interpreted"
)
_MISSING_PR_READ: Final = (
    "no successful read of the assigned pull request was observed in the review turn"
)
_HEAD_MISMATCH: Final = (
    "the assigned pull request's observed head does not resolve to the assigned commit"
)
_MISSING_FILE_READ: Final = (
    "no successful commit-addressed read at the assigned head was observed in the review turn"
)
_UNSAFE_BOUNDARY: Final = "the current-turn transcript boundary could not be safely established"

# The one safe reason the adapter itself raises: a completed review send's
# verification transcript could not be read, so the result is withheld
# without any pre-effect classification of the completed send.
UNREADABLE_EVIDENCE: Final = (
    "the review turn completed but its verification transcript could not be read"
)


class ClineReviewSubjectUnverifiedError(AgentRuntimeError):
    """The PR review turn completed but its assigned-subject evidence failed.

    The review send completed and may even have produced a syntactically
    valid ``review_result``, but the current-turn ``read_messages`` tool
    evidence required by the D5b seam did not establish access to the exact
    assigned PR subject, so the formal result is not released as a verified
    review. A known failure after a completed send: never an uncertainty,
    never a trigger for a blind retry, and never a reason to replace or
    reconstruct the exact bound session. Messages and representations are
    safe by authoring and carry a static machine reason only — never
    transcript, tool input/output, source content, credentials, or MCP
    payloads.
    """


@dataclass(frozen=True)
class _ToolObservation:
    """One current-turn ``tool_use``/``tool_result`` pair, kept raw.

    ``tool_input`` and ``result_content`` stay opaque JSON values: they are
    inspected in memory for the two qualified shapes only and are never
    logged, persisted, or surfaced.
    """

    tool_name: object
    tool_input: object
    result_content: object


def _message_content(message: object) -> object:
    """The message's content payload, tolerating the SDK's wrapped shape.

    The public transcript carries ``role``/``content`` either directly or
    nested under a ``message`` object; anything else is opaque and ignored.
    """
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if content is None:
        inner = message.get("message")
        if isinstance(inner, dict):
            content = inner.get("content")
    return content


def _iter_content_blocks(content: object) -> Iterator[JsonObject]:
    """The JSON-object blocks of one message content list, if any."""
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                yield block


def _flatten_result_content(content: object) -> str:
    """Flatten one MCP tool-result content payload into its text form.

    Mirrors the qualified MCP nesting without interpreting the payload: a
    plain string, a list of items, a ``{"content": [...]}`` wrapper, ``text``
    items, and ``resource`` URIs. Everything else flattens to empty text and
    is treated as malformed evidence by the checks.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            piece for piece in (_flatten_result_content(item) for item in content) if piece
        )
    if isinstance(content, dict):
        inner = content.get("content")
        if inner is not None:
            return _flatten_result_content(inner)
        text = content.get("text")
        if isinstance(text, str):
            return text
        resource = content.get("resource")
        if isinstance(resource, dict) and isinstance(resource.get("uri"), str):
            return resource["uri"]
    return ""


def _extract_head_sha(result_content: object) -> str | None:
    """The ``head.sha`` of one PR-read result, or None when unparseable.

    Unwraps the observed MCP/JSON wrapper layers to a bounded depth and
    requires the strict ``head.sha`` 40-hex field on the parsed payload;
    anything else is malformed evidence and is rejected, never interpreted.
    """
    current = _flatten_result_content(result_content)
    for _ in range(_MAX_RESULT_LAYERS):
        if not current.strip():
            return None
        try:
            parsed = json.loads(current)
        except ValueError:
            return None
        if not isinstance(parsed, dict):
            return None
        head = parsed.get("head")
        if isinstance(head, dict):
            sha = head.get("sha")
            if isinstance(sha, str) and _FULL_SHA.fullmatch(sha) is not None:
                return sha
        deeper = _flatten_result_content(parsed)
        if not deeper.strip() or deeper == current:
            return None
        current = deeper
    return None


def _collect_resource_uris(content: object) -> list[str]:
    """The structured ``resource.uri`` values of one tool-result content payload.

    Only structured MCP resource items count: the ``resource.uri`` field of a
    resource item, reached through ``content`` wrapper lists/objects. Returned
    text — including repository-controlled file content — is untrusted and is
    never scanned for bindings, and a ``text`` item is never parsed. A
    JSON-encoded MCP wrapper string is unwrapped to its structured fields,
    since the pinned baseline encodes the result wrapper that way.
    """
    if isinstance(content, str):
        stripped = content.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except ValueError:
            return []
        if isinstance(parsed, dict | list):
            return _collect_resource_uris(parsed)
        return []
    if isinstance(content, list):
        uris: list[str] = []
        for item in content:
            uris.extend(_collect_resource_uris(item))
        return uris
    if isinstance(content, dict):
        inner = content.get("content")
        if inner is not None:
            return _collect_resource_uris(inner)
        resource = content.get("resource")
        if isinstance(resource, dict) and isinstance(resource.get("uri"), str):
            return [resource["uri"]]
    return []


def _result_binds_commit(result_content: object, owner: str, repo: str, sha: str) -> bool:
    """Whether one file-read result's structured resource URI binds the commit.

    The qualified binding is the result's structured ``resource.uri`` field —
    ``repo://<owner>/<repo>/sha/<sha>/contents/<path>`` — with the URI's
    repository and commit equal to the assignment. Returned text is never
    scanned: a matching URI appearing as ordinary tool-returned text (for
    example inside repository-controlled file content) is not tool evidence
    of the binding.
    """
    for uri in _collect_resource_uris(result_content):
        match = _COMMIT_URI.match(uri)
        if (
            match is not None
            and match.group(1) == owner
            and match.group(2) == repo
            and match.group(3) == sha
        ):
            return True
    return False


def _repository_parts(subject: PrReviewSubject) -> tuple[str, str] | None:
    """The ``(owner, repo)`` halves of the assigned repository identity."""
    owner, separator, repo = subject.repository_full_name.partition("/")
    if not separator or not owner or not repo:
        return None
    return owner, repo


def _collect_observations(turn_suffix: Sequence[JsonObject]) -> list[_ToolObservation]:
    """Pair the suffix's ``tool_use`` and ``tool_result`` blocks by call ID.

    Only pairs whose call and result both appear in the suffix count; a
    result referencing an unknown call is unattributable and ignored, and a
    call without its result yields no observation. Error-flagged results are
    failed evidence and never observations. Order is transcript order.
    """
    calls: dict[str, tuple[object, object]] = {}
    observations: list[_ToolObservation] = []
    for message in turn_suffix:
        for block in _iter_content_blocks(_message_content(message)):
            kind = block.get("type")
            if kind == _TOOL_USE_BLOCK:
                call_id = block.get("id")
                if not isinstance(call_id, str) or not call_id or call_id in calls:
                    continue
                tool_input = block.get("input")
                if tool_input is None:
                    tool_input = block.get("arguments")
                calls[call_id] = (block.get("name"), tool_input)
            elif kind == _TOOL_RESULT_BLOCK:
                if block.get(_IS_ERROR_FIELD) is True:
                    continue
                call_id = block.get("tool_use_id")
                if not isinstance(call_id, str) or call_id not in calls:
                    continue
                tool_name, tool_input = calls[call_id]
                observations.append(
                    _ToolObservation(
                        tool_name=tool_name,
                        tool_input=tool_input,
                        result_content=block.get("content"),
                    )
                )
    return observations


def _pr_head_verified(
    subject: PrReviewSubject,
    parts: tuple[str, str],
    observations: list[_ToolObservation],
) -> tuple[bool, str]:
    """Final-relevant-observation verdict for the assigned PR's head."""
    owner, repo = parts
    last_observed_head: str | None = None
    for observation in observations:
        if observation.tool_name != _PR_READ_TOOL:
            continue
        if not isinstance(observation.tool_input, dict):
            continue
        tool_input = observation.tool_input
        if tool_input.get("method") != _PR_READ_METHOD:
            continue
        if tool_input.get("owner") != owner or tool_input.get("repo") != repo:
            continue
        if tool_input.get("pullNumber") != subject.pull_request_number:
            continue
        head_sha = _extract_head_sha(observation.result_content)
        if head_sha is None:
            continue
        last_observed_head = head_sha
    if last_observed_head is None:
        return False, _MISSING_PR_READ
    if last_observed_head != subject.head_sha:
        return False, _HEAD_MISMATCH
    return True, ""


def _file_read_verified(
    subject: PrReviewSubject,
    parts: tuple[str, str],
    observations: list[_ToolObservation],
) -> tuple[bool, str]:
    """Existence verdict for a successful commit-addressed read at the head."""
    owner, repo = parts
    for observation in observations:
        if observation.tool_name != _FILE_READ_TOOL:
            continue
        if not isinstance(observation.tool_input, dict):
            continue
        tool_input = observation.tool_input
        if tool_input.get("owner") != owner or tool_input.get("repo") != repo:
            continue
        if tool_input.get("ref") != subject.head_sha:
            continue
        if _result_binds_commit(observation.result_content, owner, repo, subject.head_sha):
            return True, ""
    return False, _MISSING_FILE_READ


def _turn_suffix(
    pre_send: Sequence[JsonObject], post_send: Sequence[JsonObject]
) -> list[JsonObject] | None:
    """The appended current-turn suffix, or None when the boundary is unsafe.

    The post-send transcript must be an exact value extension of the
    pre-send snapshot: anything rewritten, reordered, or shortened means the
    current-turn boundary cannot be safely established and the review must
    not be verified.
    """
    if len(post_send) < len(pre_send):
        return None
    if list(post_send[: len(pre_send)]) != list(pre_send):
        return None
    return list(post_send[len(pre_send) :])


def verify_pr_review_turn(
    subject: PrReviewSubject,
    pre_send: Sequence[JsonObject],
    post_send: Sequence[JsonObject],
) -> None:
    """Verify current-turn tool evidence for one assigned PR review subject.

    ``pre_send`` is the transcript snapshot taken before the review send and
    ``post_send`` the transcript read after the completed review turn; only
    their appended suffix may satisfy this assignment, so evidence from
    earlier review revisions on the persistent session never verifies a new
    one. Raises :class:`ClineReviewSubjectUnverifiedError` when the boundary
    is unsafe, when the assigned repository identity is unreadable, when no
    successful assigned-PR read was observed, when the final relevant PR
    observation contradicts the assigned head, or when no successful
    commit-addressed file read at the assigned SHA was observed. Returns
    quietly when both qualified checks hold; the caller may then release
    its already-parsed ``review_result``.

    All parsing is in-memory and content never leaves this call: failures
    carry only the static machine reasons above.
    """
    suffix = _turn_suffix(pre_send, post_send)
    if suffix is None:
        raise ClineReviewSubjectUnverifiedError(_UNSAFE_BOUNDARY)
    parts = _repository_parts(subject)
    if parts is None:
        raise ClineReviewSubjectUnverifiedError(_UNREADABLE_SUBJECT)
    observations = _collect_observations(suffix)
    verified, reason = _pr_head_verified(subject, parts, observations)
    if not verified:
        raise ClineReviewSubjectUnverifiedError(reason)
    verified, reason = _file_read_verified(subject, parts, observations)
    if not verified:
        raise ClineReviewSubjectUnverifiedError(reason)
