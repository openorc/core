"""Account-deletion boundary composition for the OpenOrc API (issue #374).

The HTTP composition root for the permanent account-deletion operation's
one external boundary: the Supabase Auth Admin client the issue #97
account-lifecycle service composes. Construction fails closed with the
typed integration-unconfigured vocabulary when the deployment has not
supplied the required credentials — the component that needs a credential
must be the one that possesses it. The client is constructed per request
(no cache semantics) and never exposes its secret; the composition is
deployment configuration, never caller-supplied material.

This composition is deliberately separate from the GitHub user-flow
composition (issue #373): account deletion requires only the Supabase
Auth Admin boundary and must never fail closed on the unrelated GitHub
user-flow credentials.
"""

from __future__ import annotations

from openorc.adapters.supabase import (
    HttpSupabaseAuthAdminClient,
    SupabaseAuthAdminClient,
)
from openorc.config import Settings
from openorc.services.errors import IntegrationNotConfiguredError

__all__ = ["resolve_account_admin_client"]


def resolve_account_admin_client(settings: Settings) -> SupabaseAuthAdminClient:
    """Build the account-deletion Admin boundary for one request, failing closed.

    The Supabase Auth Admin boundary (project URL + secret key) is a
    deployment credential: an unconfigured deployment refuses the account
    deletion operation with the typed integration-unconfigured error (mapped
    to a fail-closed 503), never with a partially constructed client. Blank
    values cannot reach here — Settings fails closed on them at boot.
    """
    project_url = settings.supabase_url
    secret_key = settings.supabase_secret_key
    if (
        not isinstance(project_url, str)
        or not project_url
        or not isinstance(secret_key, str)
        or not secret_key
    ):
        raise IntegrationNotConfiguredError(
            "the Supabase Auth Admin boundary is not configured for this process"
        )
    return HttpSupabaseAuthAdminClient(project_url=project_url, secret_key=secret_key)
