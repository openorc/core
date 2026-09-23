"""GitHub App installation domain models (issue #57).

A GitHubInstallation is the durable, Workspace-scoped record of one GitHub App
installation available to a Workspace. Installations are a separate integration
concept from Agent Runtime Connections (issue #20): a Connection routes OpenOrc
agent-runtime work, an installation routes GitHub operations.

- The record carries only durable routing and observation facts: the OpenOrc
  UUID identity, the direct Workspace scope, the stable external installation
  and account IDs, the mutable observed account login/type, and the mutable
  observed ``suspended_at`` fact. A Workspace may hold several installations
  (its repositories may span multiple GitHub accounts), and the same external
  installation may be represented independently in several Workspaces —
  Workspace isolation stays explicit.
- Raw GitHub App private-key material, installation access tokens, human OAuth
  tokens, PATs, and any other credential material never appear in these
  models. GitHub App credentials are not OpenOrc application-table state at
  all.
- ``suspended_at`` is an observation carried verbatim: no derived
  usability/authorization claim is attached to it anywhere in OpenOrc. The
  record and the Repository route establish WHICH installation later GitHub
  operations must use; they do not assert current access to any repository or
  effective permissions. GitHub transport and authoritative reconciliation are
  outside this leaf — later capability work establishes current access before
  any GitHub operation runs.
- ``type`` is observed presentation text (GitHub reports ``"User"`` or
  ``"Organization"``); it is deliberately not an enum: observed metadata is
  never configuration authority.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from uuid import UUID

__all__ = [
    "GitHubInstallation",
    "GitHubInstallationAccount",
    "GitHubInstallationDomainError",
    "GitHubInstallationIdentity",
    "github_installation_field_names",
]


class GitHubInstallationDomainError(Exception):
    """Raised when a GitHubInstallation invariant is violated."""


@dataclass(frozen=True, slots=True)
class GitHubInstallationIdentity:
    """Stable external identity of a GitHub App installation.

    GitHub installation IDs are stable and globally unique across accounts;
    this value — not the account login or any URL — is the external identity
    OpenOrc keys Workspace installation records on.
    """

    github_installation_id: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.github_installation_id, bool)
            or not isinstance(self.github_installation_id, int)
            or self.github_installation_id <= 0
        ):
            raise GitHubInstallationDomainError(
                "github_installation_id must be a positive integer, "
                f"got {self.github_installation_id!r}"
            )


@dataclass(frozen=True, slots=True)
class GitHubInstallationAccount:
    """The GitHub account an installation targets.

    ``github_account_id`` is the stable external account identity that
    survives renames; ``login`` and ``type`` are mutable observed presentation
    metadata — never identity, never routing authority.
    """

    github_account_id: int
    login: str
    type: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.github_account_id, bool)
            or not isinstance(self.github_account_id, int)
            or self.github_account_id <= 0
        ):
            raise GitHubInstallationDomainError(
                f"github_account_id must be a positive integer, got {self.github_account_id!r}"
            )
        for field_name in ("login", "type"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise GitHubInstallationDomainError(
                    f"GitHubInstallationAccount.{field_name} must be a non-empty string"
                )


@dataclass(frozen=True, slots=True)
class GitHubInstallation:
    """One GitHub App installation record within one Workspace.

    Durable routing and observation facts only. ``suspended_at`` is the
    observed installation state carried verbatim (``None`` means no
    suspension is observed); deciding whether the installation currently
    grants the required repository access or permissions is later GitHub
    reconciliation work, never a property of this record.
    """

    id: UUID
    workspace_id: UUID
    identity: GitHubInstallationIdentity
    account: GitHubInstallationAccount
    suspended_at: datetime | None
    created_at: datetime
    updated_at: datetime


def github_installation_field_names() -> frozenset[str]:
    """Return the exact field set a GitHubInstallation exposes.

    Lets tests prove the record carries no credential-bearing field: raw
    GitHub App private keys, installation access tokens, human OAuth tokens,
    and PATs never appear in domain or persistence objects.
    """
    return frozenset(field.name for field in fields(GitHubInstallation))
