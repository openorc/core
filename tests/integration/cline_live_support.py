"""Pure validation helpers for the D7 live Cline suite (issue #149).

Test-only support logic for ``tests/integration/test_cline_live_runtime.py``,
kept in a separate network-free module so the subject-validation and
head-movement rules stay deterministically testable in the ordinary suite
(``tests/test_integration_cline_live_support.py``). Nothing here performs
I/O, imports pytest, or touches production code: callers fetch the public
GitHub payloads and feed them to these functions.

The prepared fixture is the repository ``openorc/runtime-test-fixture``'s
deliberately unmerged PR #2 whose sole changed file is
``fixture/d7-review-subject.md``. The head SHA is NOT fixed: each run
resolves the current head from GitHub and pins it for the review
interaction, so the prepared subject stays valid across deliberate
re-preparations without code changes. A moved head between resolution and
review is a stop condition, never a silent retarget.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Final

FIXTURE_REPOSITORY: Final = "openorc/runtime-test-fixture"
FIXTURE_PR_NUMBER: Final = 2
FIXTURE_CHANGED_PATH: Final = "fixture/d7-review-subject.md"
FIXTURE_BASE_BRANCH: Final = "main"
FIXTURE_HEAD_BRANCH: Final = "qual/d7-cline-live-review"

_SHA_PATTERN: Final = re.compile(r"^[0-9a-f]{40}$")

__all__ = [
    "FIXTURE_BASE_BRANCH",
    "FIXTURE_CHANGED_PATH",
    "FIXTURE_HEAD_BRANCH",
    "FIXTURE_PR_NUMBER",
    "FIXTURE_REPOSITORY",
    "BoxCheckVerificationError",
    "CredentialSupplierError",
    "FixtureSubjectError",
    "assert_head_unchanged",
    "classify_box_check_exit",
    "credential_from_supplier_output",
    "resolve_fixture_head_sha",
]


class FixtureSubjectError(ValueError):
    """The prepared fixture PR is not the expected review subject.

    Safe failure vocabulary only: the messages carry public fixture
    identifiers (repository, PR number, changed path, commit SHAs) and never
    content, credentials, or transcripts.
    """


class BoxCheckVerificationError(ValueError):
    """The operator box check could not actually verify the Box-side path.

    The outcome contract is explicit: exit 0 = path exists, exit 1 = path
    absent, and ANY other exit status is a verification failure — a transport,
    authentication, or remote-execution error is never cleanup evidence.
    """


class CredentialSupplierError(ValueError):
    """The credential supplier produced no usable current credential.

    The message carries only the safe structural fact; the supplied value
    itself never enters the error text, logs, or evidence.
    """


def _require_sha(value: object, what: str) -> str:
    if not isinstance(value, str) or not _SHA_PATTERN.match(value):
        raise FixtureSubjectError(f"the fixture PR's {what} is not a 40-hex commit SHA")
    return value


def resolve_fixture_head_sha(
    pull_request: Mapping[str, object],
    files: Sequence[Mapping[str, object]],
) -> str:
    """Validate the prepared fixture PR payload and return its head SHA.

    ``pull_request`` is the public ``GET /repos/{repo}/pulls/{number}`` body
    and ``files`` the ``GET /repos/{repo}/pulls/{number}/files`` body. The
    expected identity is the prepared repository and PR number in an open,
    non-draft state; the expected structure is exactly one changed file at
    the prepared path. Any divergence is a fixture-integrity failure that
    blocks the run instead of silently retargeting the test subject.
    """
    base = pull_request.get("base")
    base_repo = base.get("repo") if isinstance(base, Mapping) else None
    full_name = base_repo.get("full_name") if isinstance(base_repo, Mapping) else None
    if full_name != FIXTURE_REPOSITORY:
        raise FixtureSubjectError(
            f"the pull request's base repository is {full_name!r}, not the prepared "
            f"{FIXTURE_REPOSITORY!r}"
        )
    base_ref = base.get("ref") if isinstance(base, Mapping) else None
    if base_ref != FIXTURE_BASE_BRANCH:
        raise FixtureSubjectError(
            f"the prepared fixture pull request targets base {base_ref!r}, not the "
            f"prepared {FIXTURE_BASE_BRANCH!r}"
        )
    number = pull_request.get("number")
    if number != FIXTURE_PR_NUMBER:
        raise FixtureSubjectError(
            f"the pull request number is {number!r}, not the prepared {FIXTURE_PR_NUMBER!r}"
        )
    if pull_request.get("state") != "open":
        raise FixtureSubjectError(
            f"the prepared fixture pull request is not open (state {pull_request.get('state')!r})"
        )
    if pull_request.get("draft") is not False:
        raise FixtureSubjectError("the prepared fixture pull request is a draft")
    head = pull_request.get("head")
    head_ref = head.get("ref") if isinstance(head, Mapping) else None
    if head_ref != FIXTURE_HEAD_BRANCH:
        raise FixtureSubjectError(
            f"the prepared fixture pull request originates from head branch {head_ref!r}, "
            f"not the prepared {FIXTURE_HEAD_BRANCH!r}"
        )
    head_repo = head.get("repo") if isinstance(head, Mapping) else None
    head_repo_name = head_repo.get("full_name") if isinstance(head_repo, Mapping) else None
    if head_repo_name != FIXTURE_REPOSITORY:
        raise FixtureSubjectError(
            f"the prepared fixture pull request's head repository is {head_repo_name!r}, "
            f"not the prepared {FIXTURE_REPOSITORY!r}"
        )
    head_sha = head.get("sha") if isinstance(head, Mapping) else None
    sha = _require_sha(head_sha, "head SHA")
    if len(files) != 1:
        raise FixtureSubjectError(
            f"the prepared fixture pull request changes {len(files)} file(s), not exactly "
            f"the one prepared changed file"
        )
    filename = files[0].get("filename")
    if filename != FIXTURE_CHANGED_PATH:
        raise FixtureSubjectError(
            f"the prepared fixture pull request's changed file is {filename!r}, not the "
            f"prepared {FIXTURE_CHANGED_PATH!r}"
        )
    return sha


def assert_head_unchanged(pinned: str, observed: object) -> str:
    """Reject head movement against the SHA pinned for the review interaction.

    Returns the observed SHA when it still matches the pinned subject head;
    any other observed value — a different SHA, a moved or force-pushed head,
    or a malformed response — raises instead of letting the review stand on a
    subject that no longer exists at its verified identity.
    """
    _require_sha(pinned, "pinned head SHA")
    observed_sha = _require_sha(observed, "re-read head SHA")
    if observed_sha != pinned:
        raise FixtureSubjectError(
            f"the fixture PR's head moved during the review (pinned {pinned}, observed "
            f"{observed_sha}); the exact review subject is no longer the prepared head"
        )
    return observed_sha


def classify_box_check_exit(returncode: int) -> str:
    """Classify one operator box-check exit status under the explicit contract.

    ``0`` means the addressed Box-side path exists and ``1`` means it does
    not; ANY other exit status — an SSH transport failure, an authentication
    error, or a remote-execution failure — is a verification failure that
    must never be mistaken for evidence that a path is absent. The live
    driver requires the operator-check command to preserve this distinction.
    """
    if returncode == 0:
        return "exists"
    if returncode == 1:
        return "absent"
    raise BoxCheckVerificationError(
        f"the operator box check could not verify the Box-side path (exit {returncode}); "
        "a transport, authentication, or remote-execution failure is never "
        "absence or cleanup evidence"
    )


def credential_from_supplier_output(raw: object) -> str:
    """Extract the current Hub credential from a supplier command's stdout.

    The supplier is the narrow test-only hook the post-restart refresh uses:
    its stdout is the current attach credential. Only the structural fact is
    validated here; the credential value itself never enters error text,
    logs, evidence, or command arguments anywhere downstream.
    """
    if isinstance(raw, bytes):
        text = raw.decode("utf-8")
    elif isinstance(raw, str):
        text = raw
    else:
        raise CredentialSupplierError(
            "the credential supplier produced no decodable stdout payload"
        )
    credential = text.strip()
    if not credential:
        raise CredentialSupplierError(
            "the credential supplier produced an empty current credential"
        )
    return credential
