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
    FIXTURE_BASE_BRANCH,
    FIXTURE_CHANGED_PATH,
    FIXTURE_HEAD_BRANCH,
    FIXTURE_PR_NUMBER,
    FIXTURE_REPOSITORY,
    BoxCheckVerificationError,
    CredentialSupplierError,
    FixtureSubjectError,
    assert_head_unchanged,
    classify_box_check_exit,
    credential_from_supplier_output,
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
    base_ref: str = FIXTURE_BASE_BRANCH,
    head_ref: str = FIXTURE_HEAD_BRANCH,
    head_repo: str = FIXTURE_REPOSITORY,
    head_sha: object = _SHA,
) -> dict[str, object]:
    return {
        "number": number,
        "state": state,
        "draft": draft,
        "base": {"ref": base_ref, "repo": {"full_name": full_name}},
        "head": {"ref": head_ref, "repo": {"full_name": head_repo}, "sha": head_sha},
    }


def _files(*names: str) -> list[dict[str, str]]:
    return [{"filename": name} for name in names or ()]


def test_valid_fixture_payload_resolves_the_pinned_head() -> None:
    assert resolve_fixture_head_sha(_payload(), _files(FIXTURE_CHANGED_PATH)) == _SHA


def test_base_repository_must_be_the_prepared_fixture_repository() -> None:
    with pytest.raises(FixtureSubjectError, match="base repository"):
        resolve_fixture_head_sha(_payload(full_name="openorc/other"), _files(FIXTURE_CHANGED_PATH))


def test_base_branch_must_be_the_prepared_base() -> None:
    with pytest.raises(FixtureSubjectError, match="targets base"):
        resolve_fixture_head_sha(_payload(base_ref="develop"), _files(FIXTURE_CHANGED_PATH))


def test_head_branch_must_be_the_prepared_branch() -> None:
    with pytest.raises(FixtureSubjectError, match="head branch"):
        resolve_fixture_head_sha(_payload(head_ref="feature/other"), _files(FIXTURE_CHANGED_PATH))


def test_head_repository_must_be_the_prepared_repository() -> None:
    with pytest.raises(FixtureSubjectError, match="head repository"):
        resolve_fixture_head_sha(_payload(head_repo="openorc/fork"), _files(FIXTURE_CHANGED_PATH))


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


# --- operator box-check outcome contract --------------------------------------


def test_box_check_exit_zero_means_the_path_exists() -> None:
    assert classify_box_check_exit(0) == "exists"


def test_box_check_exit_one_means_the_path_is_absent() -> None:
    assert classify_box_check_exit(1) == "absent"


def test_any_other_box_check_exit_is_a_verification_failure() -> None:
    for returncode in (2, 255, 130, -1):
        with pytest.raises(BoxCheckVerificationError, match="never"):
            classify_box_check_exit(returncode)


# --- credential supplier output ------------------------------------------------


def test_supplier_output_supplies_the_stripped_credential() -> None:
    assert credential_from_supplier_output("current-token-value\n") == "current-token-value"
    assert credential_from_supplier_output(b"current-token-value\r\n") == "current-token-value"
    assert credential_from_supplier_output("  padded-current-token  ") == "padded-current-token"


def test_empty_supplier_output_is_rejected_without_echoing_the_value() -> None:
    with pytest.raises(CredentialSupplierError, match="empty current credential"):
        credential_from_supplier_output("   \n")
    with pytest.raises(CredentialSupplierError, match="no decodable stdout"):
        credential_from_supplier_output(None)
