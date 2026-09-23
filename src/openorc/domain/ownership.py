"""Ownership and repository-identity domain models.

Profile, Workspace, Project, and Repository form the ownership hierarchy that
scopes all later Workspace-owned OpenOrc state (Phase 1).

- Profile is the canonical OpenOrc application identity. Its identifier is
  deliberately the corresponding Supabase Auth user UUID (1:1 by value) and is
  enforced by the single sanctioned identity/deletion foreign key
  ``openorc.profiles.id -> auth.users (id) ON DELETE CASCADE`` (issue #27): the
  OpenOrc-owned application graph can never outlive its account identity, and
  a Profile can never exist without its backing Auth user. Ordinary OpenOrc
  domain references still point at Profile, never into Supabase-managed
  schemas.
- A Workspace is owned by exactly one Profile in v1. Membership, invitation,
  and RBAC concepts do not exist.
- A Project belongs to one Workspace. A Repository belongs to one Project and
  carries its Workspace directly; the direct scope must agree with the parent
  Project's Workspace.
- A Repository record's identity is its OpenOrc UUID; the GitHub repository ID
  is the stable external identity used for reconciliation and per-Workspace
  canonicalization. Owner login, name, URL, visibility, and similar fields are
  mutable observed metadata and never identity.
- A Repository may carry an explicit route (issue #57) to one Workspace-scoped
  GitHubInstallation record (:mod:`openorc.domain.github_installations`): the
  route is a durable configuration fact establishing which installation later
  GitHub operations must use — never an authorization claim, never inferred
  from mutable metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

__all__ = [
    "AccountDeletionAttemptState",
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
    1:1 infrastructure identity), enforced by the single sanctioned identity/
    deletion foreign key ``openorc.profiles.id -> auth.users (id) ON DELETE
    CASCADE`` (issue #27): a Profile can never exist without its backing Auth
    user. Ordinary OpenOrc domain references never point into Supabase-managed
    schemas.
    """

    id: UUID
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AccountDeletionAttemptState:
    """The durable account-deletion attempt state of one Profile (issue #97).

    Present only while an account-deletion attempt is durably unresolved:
    ``active`` while one attempt is in flight (single-flight durable lock
    against concurrent invocations), ``uncertain`` after a prior Auth Admin
    delete outcome could not be classified (the next explicit invocation must
    reconcile through the Admin read surface before any replay). The attempt
    UUID is the ownership identity of the state: every durable transition and
    clear is conditional on this exact UUID, so one invocation can never clear
    or move another invocation's protection. ``started_at`` is the
    database-clock establishment time and the basis of the active-attempt
    lease. All three facts are non-NULL together or all NULL (the composite
    CHECK makes any other tuple unrepresentable), and the whole row cascades
    away with the Profile when permanent deletion succeeds.
    """

    profile_id: UUID
    state: str
    attempt_id: UUID
    started_at: datetime


@dataclass(frozen=True, slots=True)
class Workspace:
    """A Workspace owned by exactly one Profile in v1.

    ``review_iteration_limit`` is the first-class Workspace setting (issue
    #53) supplying the effective iteration limit for future ReviewLoops; the
    limit stored on each existing ReviewLoop at creation is immutable
    historical configuration and is never rewritten when this setting
    changes. ``guidance`` is one current, Owner-authored prose value — blank
    means no Workspace-specific guidance. It is current mutable Workspace
    configuration with no template, version, hash, snapshot, or history
    semantics, and it can never redefine formal schemas, initialization
    semantics, workflow controls, authority, communication topology, exact
    review-subject identity, session boundaries, or state transitions.
    """

    id: UUID
    owner_profile_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime
    review_iteration_limit: int
    guidance: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.review_iteration_limit, bool)
            or not isinstance(self.review_iteration_limit, int)
            or self.review_iteration_limit <= 0
        ):
            raise OwnershipError(
                "Workspace.review_iteration_limit must be a positive integer, "
                f"got {self.review_iteration_limit!r}"
            )
        if not isinstance(self.guidance, str):
            raise OwnershipError(
                f"Workspace.guidance must be a string, got {type(self.guidance).__name__}"
            )


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
    ``github_installation_id`` is the explicit route (issue #57) to the one
    Workspace-scoped GitHubInstallation record that later GitHub operations
    must use: ``None`` means no route is configured, so the Repository is a
    valid historical record but is not usable for GitHub operations. The route
    is a durable configuration fact addressed by the OpenOrc installation
    record UUID — never inferred from mutable GitHub metadata — and it does
    not itself assert current repository access; later GitHub reconciliation
    establishes current access and permissions.
    """

    id: UUID
    project_id: UUID
    workspace_id: UUID
    identity: GitHubRepositoryIdentity
    metadata: RepositoryMetadata
    created_at: datetime
    updated_at: datetime
    github_installation_id: UUID | None = None
