"""GitHub App boundary composition for the Owner Task commands (issue #378).

The HTTP composition root for the intake and Start command services' one
external boundary: the GitHub App installation client the fresh
authoritative GitHub revalidation composes. Construction fails closed with
the typed integration-unconfigured vocabulary when the deployment has not
supplied the required GitHub App identity — the component that needs a
credential must be the one that possesses it. The client is constructed per
request (no cache semantics); the composition is deployment configuration,
never caller-supplied material.

This composition is deliberately separate from the GitHub user-flow
composition (issue #373) and the account-deletion composition (issue #374):
the Task commands require only the GitHub App identity and must never fail
closed on, or force, unrelated credentials.
"""

from __future__ import annotations

from openorc.adapters.github import GitHubAppClient, HttpGitHubAppClient
from openorc.config import ConfigurationError, Settings
from openorc.services.errors import IntegrationNotConfiguredError

__all__ = ["resolve_github_app_client"]


def resolve_github_app_client(settings: Settings) -> GitHubAppClient:
    """Build the GitHub App boundary for one request, failing closed.

    The GitHub App identity (app id + private key) is a deployment
    credential: an unconfigured deployment refuses the command with the
    typed integration-unconfigured error (mapped to a fail-closed 503),
    never with a partially constructed client. The adapter's own
    ``ConfigurationError`` for absent identity is the authoritative check;
    the transport boundary converts it to the typed application vocabulary
    so routers map it through the established application-error handler.
    """
    try:
        return HttpGitHubAppClient.from_settings(settings)
    except ConfigurationError as error:
        raise IntegrationNotConfiguredError(
            "the GitHub App identity is not configured for this process"
        ) from error
