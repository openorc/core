"""Ownership and repository-identity domain models.

Profile, Workspace, Project, and Repository form the ownership hierarchy that
scopes all later Workspace-owned OpenOrc state (Phase 1).

- Profile is the canonical OpenOrc application identity. Its identifier is
  deliberately the corresponding Supabase Auth user UUID (1:1 by value); it is
  not a foreign key into Supabase-managed auth infrastructure, and ordinary
  OpenOrc domain references point at Profile.
- A Workspace is owned by exactly one Profile in v1. Membership, invitation,
  and RBAC concepts do not exist.
- A Project belongs to one Workspace. A Repository belongs to one Project and
  carries its Workspace directly; the direct scope must agree with the parent
  Project's Workspace.
- A Repository record's identity is its OpenOrc UUID; the GitHub repository ID
  is the stable external identity used for reconciliation and per-Workspace
  canonicalization. Owner login, name, URL, visibility, and similar fields are
  mutable observed metadata and never identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

__all__ = [
    "GitHubRepositoryIdentity",
    "OwnershipError",
    "Profile",
    "Project",
    "Repository",
    "RepositoryMetadata",
    "Workspace",
]


class OwnershipError(Exception):
    """Raised when an ownership domain invariant is violated."""


@dataclass(frozen=True, slots=True)
class Profile:
    """Canonical OpenOrc application identity.

    ``id`` is the corresponding Supabase Auth user UUID by value (a deliberate
    1:1 infrastructure identity), not a foreign key into Supabase-managed
    schemas.
    """

    id: UUID
    created_at: datetime


@dataclass(frozen=True, slots=True)
class Workspace:
    """A Workspace owned by exactly one Profile in v1."""

    id: UUID
    owner_profile_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Project:
    """A Project belonging to one Workspace."""

    id: UUID
    workspace_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class GitHubRepositoryIdentity:
    """Stable external identity of a GitHub repository.

    GitHub repository IDs survive renames and ownership transfers; deleted and
    recreated repositories receive new IDs. This value — not owner/name
    presentation — is the stable external identity OpenOrc uses for
    reconciliation and per-Workspace canonicalization.
    """

    github_repository_id: int

    def __post_init__(self) -> None:
        if self.github_repository_id <= 0:
            raise OwnershipError(
                f"github_repository_id must be a positive integer, got {self.github_repository_id}"
            )


@dataclass(frozen=True, slots=True)
class RepositoryMetadata:
    """Mutable observed GitHub repository presentation metadata.

    Never identity: updating these observations does not change which external
    repository a Repository record refers to, nor its OpenOrc identity.
    ``default_branch`` is ``None`` until observed (empty repositories have
    none).
    """

    owner_login: str
    name: str
    html_url: str
    is_private: bool
    default_branch: str | None

    def __post_init__(self) -> None:
        for field_name in ("owner_login", "name", "html_url"):
            if not getattr(self, field_name).strip():
                raise OwnershipError(f"RepositoryMetadata.{field_name} must be a non-empty string")
        if self.default_branch is not None and not self.default_branch.strip():
            raise OwnershipError(
                "RepositoryMetadata.default_branch must be None or a non-empty string"
            )


@dataclass(frozen=True, slots=True)
class Repository:
    """One canonical OpenOrc Repository record within one Workspace.

    The OpenOrc ``id`` is the domain-record identity. ``identity`` is the
    stable external GitHub repository identity used for reconciliation and
    per-Workspace canonicalization. ``workspace_id`` carries the direct
    Workspace scope and must agree with the owning Project's Workspace.
    """

    id: UUID
    project_id: UUID
    workspace_id: UUID
    identity: GitHubRepositoryIdentity
    metadata: RepositoryMetadata
    created_at: datetime
    updated_at: datetime
