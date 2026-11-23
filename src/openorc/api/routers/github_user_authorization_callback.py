"""Unauthenticated GitHub user-authorization provider callback ingress (issue #373).

The GitHub App's configured Callback URL points at this route: GitHub
redirects the Owner's browser here with the post-consent query parameters.
The route lives OUTSIDE the versioned ``/api/v1`` public namespace and is
excluded from the published OpenAPI schema, exactly like the webhook
ingress — it is provider/browser transport, not a public client contract.

Documented callback authentication strategy (the issue #373 decision): the
route is unauthenticated by design — a browser redirect cannot carry the
Owner's Supabase session JWT to this origin, and no browser-supplied actor
ID is ever trusted. Completion authority comes exclusively from the durable
correlation the authenticated start operation created: the exact ``state``
was bound to the initiating Profile and intended flow purpose, is single-use
(the durable delete IS the replay barrier), expires on a bounded window, is
fenced by the captured revocation evidence at the final credential
installation, and completes only through the #142 same-human binding proof
against that Profile's trusted Supabase sign-in identity. The state alone
never proves Owner identity; wrong human, replay, expiry, and intervening
revocation all fail closed with nothing usable stored.

Non-disclosure contract: the route bounds and shape-checks ``code`` and
``state`` before any service call, never echoes any query value (including
GitHub's ``error``/``error_description``) into any response, log, or
telemetry attribute, and answers the browser with one of two static,
credential-free outcome bodies. The serving boundary's access logging
redacts this route's request target (``openorc.api.access_logging``), and
the deployment's reverse proxy must not persist the query string either
(``docs/development.md``). No credential value, verifier, reference, or
correlation state ever appears in any response.

Blocking work (consume, token exchange, identity proof) runs off the event
loop (sync handler; the v1 persistence surface is synchronous).
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import Response

from openorc.api.dependencies.github_user_flow import resolve_user_flow_composition
from openorc.config import Settings
from openorc.persistence.pool import get_database_pool
from openorc.services import github_user_authorization_flows
from openorc.services.errors import ApplicationError

router = APIRouter(tags=["github-user-authorization-callback"])

# --- route identity and safe landing bodies ----------------------------------

# The deployment points the GitHub App's Callback URL at this route (see
# docs/development.md). The access-log boundary redacts this path's request
# target so the credential-bearing query never reaches logs.
GITHUB_USER_AUTHORIZATION_CALLBACK_PATH = "/api/github/user-authorization/callback"

# Bounded query inputs: GitHub authorization codes and correlation states are
# far smaller; an oversized value is a uniform rejection without any service
# call. The service re-validates shapes before its own durable reads.
_MAX_CALLBACK_QUERY_VALUE_CHARS = 2048

_SUCCESS_LANDING = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>OpenOrc GitHub authorization</title></head>
<body>
<h1>GitHub authorization completed</h1>
<p>OpenOrc received GitHub's authorization answer. You can return to OpenOrc.</p>
</body>
</html>
"""

_FAILURE_LANDING = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>OpenOrc GitHub authorization</title></head>
<body>
<h1>GitHub authorization was not completed</h1>
<p>OpenOrc could not complete this GitHub authorization. You can close this
page and start again from OpenOrc.</p>
</body>
</html>
"""

_NO_STORE_HEADERS = {"Cache-Control": "no-store"}


def _usable(value: str | None) -> bool:
    """Shape-check one callback query value before any service call."""
    return isinstance(value, str) and bool(value) and len(value) <= _MAX_CALLBACK_QUERY_VALUE_CHARS


def _landing(content: str, status_code: int) -> Response:
    """One static, credential-free browser landing (never cached)."""
    return Response(
        content=content,
        media_type="text/html; charset=utf-8",
        status_code=status_code,
        headers=_NO_STORE_HEADERS,
    )


# --- the ingress route -------------------------------------------------------


@router.get(
    GITHUB_USER_AUTHORIZATION_CALLBACK_PATH,
    include_in_schema=False,
    response_class=Response,
)
def complete_user_authorization_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
) -> Response:
    """Receive GitHub's post-consent redirect and complete or abort the flow.

    Provider-terminated flows (GitHub's ``error`` answer) consume the
    correlation so it can never be completed later. Successful correlations
    complete through the flow service's documented strategy; every typed
    failure — unknown/expired/replayed state, wrong GitHub human, definitive
    rejection, uncertainty, or the revocation fence — answers the uniform
    failure landing with nothing usable stored. Unexpected failures keep the
    framework's server-error semantics. No query value is ever reflected
    into any response.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    if error is not None:
        # The provider terminated the flow (for example, the Owner denied
        # access at the consent step): invalidate the correlation so it can
        # never be completed later, and answer the uniform failure landing.
        # GitHub's error text is never echoed anywhere.
        if isinstance(state, str) and _usable(state):
            github_user_authorization_flows.abort_pending_github_user_authorization_flow(
                pool, state=state
            )
        return _landing(_FAILURE_LANDING, 400)
    if not _usable(state) or not _usable(code):
        return _landing(_FAILURE_LANDING, 400)
    try:
        composition = resolve_user_flow_composition(settings)
        github_user_authorization_flows.complete_github_user_authorization_flow(
            pool,
            state=state,  # type: ignore[arg-type]
            code=code,  # type: ignore[arg-type]
            admin_client=composition.admin_client,
            token_client=composition.token_client,
        )
    except ApplicationError:
        # The uniform failure landing: typed application failures are safe
        # classifications, and the browser never learns which one occurred.
        return _landing(_FAILURE_LANDING, 400)
    return _landing(_SUCCESS_LANDING, 200)
