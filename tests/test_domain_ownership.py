"""Unit tests for ownership domain models (Phase 1, issue #19)."""

from __future__ import annotations

import pytest

from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    OwnershipError,
    RepositoryMetadata,
)


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
