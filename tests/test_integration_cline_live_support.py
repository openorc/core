"""Deterministic regression tests for the D7 live-suite support helpers
(issue #149).

The live ClineBox validation suite resolves the prepared fixture PR's head
SHA dynamically per run and rejects head movement during the review. These
ordinary tests pin the pure validation contract of
``tests/integration/cline_live_support.py`` — the expected PR identity, the
expected one-changed-file fixture structure, the SHA shape requirements, and
the head-movement rejection — without any network access, so a future
re-preparation of the fixture or a validation regression cannot silently
loosen the review subject.
"""

from __future__ import annotations

import pytest
from tests.integration.cline_live_support import (
    FIXTURE_CHANGED_PATH,
    FIXTURE_PR_NUMBER,
    FIXTURE_REPOSITORY,
    FixtureSubjectError,
    assert_head_unchanged,
    resolve_fixture_head_sha,
)

_SHA = "81204ee712cc0627221d1024af3d2434e0006c0f"
_OTHER_SHA = "b" * 40


def _payload(
    *,
    full_name: str = FIXTURE_REPOSITORY,
    number: int = FIXTURE_PR_NUMBER,
    state: str = "open",
    draft: bool = False,
    head_sha: object = _SHA,
) -> dict[str, object]:
    return {
        "number": number,
        "state": state,
        "draft": draft,
        "base": {"repo": {"full_name": full_name}},
        "head": {"ref": "qual/d7-cline-live-review", "sha": head_sha},
    }


def _files(*names: str) -> list[dict[str, str]]:
    return [{"filename": name} for name in names or ()]


def test_valid_fixture_payload_resolves_the_pinned_head() -> None:
    assert resolve_fixture_head_sha(_payload(), _files(FIXTURE_CHANGED_PATH)) == _SHA


def test_base_repository_must_be_the_prepared_fixture_repository() -> None:
    with pytest.raises(FixtureSubjectError, match="base repository"):
        resolve_fixture_head_sha(_payload(full_name="openorc/other"), _files(FIXTURE_CHANGED_PATH))


def test_pull_request_number_must_be_the_prepared_subject() -> None:
    with pytest.raises(FixtureSubjectError, match="pull request number"):
        resolve_fixture_head_sha(_payload(number=3), _files(FIXTURE_CHANGED_PATH))


def test_closed_fixture_pull_request_is_rejected() -> None:
    with pytest.raises(FixtureSubjectError, match="not open"):
        resolve_fixture_head_sha(_payload(state="closed"), _files(FIXTURE_CHANGED_PATH))


def test_draft_fixture_pull_request_is_rejected() -> None:
    with pytest.raises(FixtureSubjectError, match="draft"):
        resolve_fixture_head_sha(_payload(draft=True), _files(FIXTURE_CHANGED_PATH))


def test_missing_or_malformed_head_sha_is_rejected() -> None:
    with pytest.raises(FixtureSubjectError, match="head SHA"):
        resolve_fixture_head_sha(_payload(head_sha=None), _files(FIXTURE_CHANGED_PATH))
    with pytest.raises(FixtureSubjectError, match="head SHA"):
        resolve_fixture_head_sha(_payload(head_sha="81204ee"), _files(FIXTURE_CHANGED_PATH))
    with pytest.raises(FixtureSubjectError, match="head SHA"):
        resolve_fixture_head_sha(_payload(head_sha=_SHA.upper()), _files(FIXTURE_CHANGED_PATH))


def test_changed_file_structure_must_be_exactly_the_prepared_file() -> None:
    with pytest.raises(FixtureSubjectError, match="changes 0 file"):
        resolve_fixture_head_sha(_payload(), [])
    with pytest.raises(FixtureSubjectError, match="changes 2 file"):
        resolve_fixture_head_sha(_payload(), _files(FIXTURE_CHANGED_PATH, "README.md"))
    with pytest.raises(FixtureSubjectError, match="changed file is"):
        resolve_fixture_head_sha(_payload(), _files("fixture/other.md"))


def test_head_unchanged_accepts_the_pinned_subject() -> None:
    assert assert_head_unchanged(_SHA, _SHA) == _SHA


def test_head_movement_is_rejected() -> None:
    with pytest.raises(FixtureSubjectError, match="head moved during the review"):
        assert_head_unchanged(_SHA, _OTHER_SHA)


def test_malformed_observed_head_is_rejected() -> None:
    with pytest.raises(FixtureSubjectError, match="re-read head SHA"):
        assert_head_unchanged(_SHA, None)
    with pytest.raises(FixtureSubjectError, match="re-read head SHA"):
        assert_head_unchanged(_SHA, "not-a-sha")


def test_a_malformed_pinned_sha_is_rejected_before_comparison() -> None:
    with pytest.raises(FixtureSubjectError, match="pinned head SHA"):
        assert_head_unchanged("", _SHA)
