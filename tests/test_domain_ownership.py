"""Unit tests for ownership domain models (Phase 1, issue #19)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    OwnershipError,
    RepositoryMetadata,
    Workspace,
)


def _workspace(**overrides: object) -> Workspace:
    values: dict[str, object] = {
        "id": uuid.uuid4(),
        "owner_profile_id": uuid.uuid4(),
        "name": "platform",
        "created_at": datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC),
        "review_iteration_limit": 5,
        "guidance": "",
    }
    values.update(overrides)
    return Workspace(**values)  # type: ignore[arg-type]


def test_workspace_round_trips_the_first_class_configuration_settings() -> None:
    # Blank guidance is valid and means no Workspace-specific guidance.
    assert _workspace().guidance == ""
    assert _workspace().review_iteration_limit == 5

    # Arbitrary Owner-authored prose round-trips verbatim.
    prose = "Always run the full suite.\nΟἶναι νόμοι — 多相. ✔\n   "
    workspace = _workspace(guidance=prose, review_iteration_limit=12)
    assert workspace.guidance == prose
    assert workspace.review_iteration_limit == 12


def test_workspace_requires_a_positive_iteration_limit() -> None:
    for bad in (0, -1, 1.5, "5", True):
        with pytest.raises(OwnershipError):
            _workspace(review_iteration_limit=bad)


def test_workspace_guidance_must_be_a_string() -> None:
    for bad in (None, 5, ["prose"], {"prose": True}):
        with pytest.raises(OwnershipError):
            _workspace(guidance=bad)


def test_github_repository_identity_requires_a_positive_id() -> None:
    identity = GitHubRepositoryIdentity(github_repository_id=12345)

    assert identity.github_repository_id == 12345
    with pytest.raises(OwnershipError):
        GitHubRepositoryIdentity(github_repository_id=0)
    with pytest.raises(OwnershipError):
        GitHubRepositoryIdentity(github_repository_id=-1)


def test_repository_metadata_rejects_blank_observed_fields() -> None:
    RepositoryMetadata(
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch=None,
    )
    RepositoryMetadata(
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )

    with pytest.raises(OwnershipError):
        RepositoryMetadata(
            owner_login=" ",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch=None,
        )
    with pytest.raises(OwnershipError):
        RepositoryMetadata(
            owner_login="octocat",
            name="",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch=None,
        )
    with pytest.raises(OwnershipError):
        RepositoryMetadata(
            owner_login="octocat",
            name="hello-world",
            html_url="  ",
            is_private=False,
            default_branch=None,
        )
    with pytest.raises(OwnershipError):
        RepositoryMetadata(
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch=" ",
        )
