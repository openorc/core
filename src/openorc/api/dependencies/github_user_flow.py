"""User-flow boundary composition for the GitHub authorization API (issue #373).

The HTTP composition root for the OAuth flow's external boundaries: the
Supabase Auth Admin client (the trusted GitHub provider-identity lookup the
#142 same-human binding proof needs) and the GitHub user-token client (the
documented code exchange, refresh, and current-user mechanics). Construction
fails closed with the typed integration-unconfigured vocabulary when the
deployment has not supplied the required credentials — the component that
needs a credential must be the one that possesses it. Clients are constructed
per request (no cache semantics) and never expose their secrets; the
compositions are deployment configuration, never caller-supplied material.

The module also carries the Profile user-token resolver composition the
Owner merge decision command (issue #379) needs: the same GitHub App
user-flow credential pair, checked narrowly so the merge command never
fails closed on, or forces, the #373 flow's unrelated Supabase Admin
boundary.
"""

from __future__ import annotations

from dataclasses import dataclass

from openorc.adapters.github import (
    GitHubUserTokenClient,
    HttpGitHubUserTokenClient,
)
from openorc.adapters.supabase import (
    HttpSupabaseAuthAdminClient,
    SupabaseAuthAdminClient,
)
from openorc.config import Settings
from openorc.services.errors import IntegrationNotConfiguredError
from openorc.services.github_user_authorization import (
    GitHubUserAccessTokenResolver,
    ProfileUserAccessTokenResolver,
)

__all__ = [
    "GitHubUserFlowComposition",
    "resolve_profile_user_token_resolver",
    "resolve_user_flow_composition",
]


@dataclass(frozen=True, slots=True)
class GitHubUserFlowComposition:
    """One request's user-flow external boundaries and public client ID."""

    admin_client: SupabaseAuthAdminClient
    token_client: GitHubUserTokenClient
    user_flow_client_id: str


def resolve_user_flow_composition(settings: Settings) -> GitHubUserFlowComposition:
    """Build the user-flow boundaries for one request, failing closed.

    The GitHub App user-flow client ID/secret and the Supabase Auth Admin
    boundary (project URL + secret key) are deployment credentials: an
    unconfigured deployment refuses the flow operations with the typed
    integration-unconfigured error (mapped to a fail-closed 503), never with
    a partially constructed client. Blank values cannot reach here —
    Settings fails closed on them at boot.
    """
    client_id = settings.github_app_user_flow_client_id
    client_secret = settings.github_app_user_flow_client_secret
    project_url = settings.supabase_url
    secret_key = settings.supabase_secret_key
    if (
        not isinstance(client_id, str)
        or not client_id
        or not isinstance(client_secret, str)
        or not client_secret
    ):
        raise IntegrationNotConfiguredError(
            "the GitHub App user-flow credentials are not configured for this process"
        )
    if (
        not isinstance(project_url, str)
        or not project_url
        or not isinstance(secret_key, str)
        or not secret_key
    ):
        raise IntegrationNotConfiguredError(
            "the Supabase Auth Admin boundary is not configured for this process"
        )
    return GitHubUserFlowComposition(
        admin_client=HttpSupabaseAuthAdminClient(project_url=project_url, secret_key=secret_key),
        token_client=HttpGitHubUserTokenClient(client_id=client_id, client_secret=client_secret),
        user_flow_client_id=client_id,
    )


def resolve_profile_user_token_resolver(settings: Settings) -> ProfileUserAccessTokenResolver:
    """Build the Profile user-token resolver for one request, failing closed.

    The Owner-accountable merge decision command (issue #379) resolves the
    approving Profile's GitHub user access token through the same deployment
    user-flow credentials the #373 authorization flows use: an unconfigured
    deployment refuses the command with the typed integration-unconfigured
    error (mapped to a fail-closed 503) before any service effect — the
    component that needs a credential must be the one that possesses it.
    The composition deliberately checks ONLY the user-flow credentials,
    never the Supabase Auth Admin boundary the #373 flow composition above
    also needs, so the merge command never fails closed on, or forces,
    unrelated credentials. The resolver is constructed per request (no
    cache semantics) and never exposes its secrets; the composition is
    deployment configuration, never caller-supplied material.
    """
    client_id = settings.github_app_user_flow_client_id
    client_secret = settings.github_app_user_flow_client_secret
    if (
        not isinstance(client_id, str)
        or not client_id
        or not isinstance(client_secret, str)
        or not client_secret
    ):
        raise IntegrationNotConfiguredError(
            "the GitHub App user-flow credentials are not configured for this process"
        )
    return GitHubUserAccessTokenResolver(
        token_client=HttpGitHubUserTokenClient(client_id=client_id, client_secret=client_secret)
    )
