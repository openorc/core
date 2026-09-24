"""GitHub repository installation-access validation services (issue #58).

The owner-scoped application service primitive that proves, before any
GitHub operation runs, that the exact installation resolved from the #57
route currently grants access to the intended stable Repository identity and
exposes the permissions/subscriptions the settled v1 GitHub workflow needs.

Composition and discipline:

- Owner authorization composes the existing Workspace authorization boundary:
  the #57 resolver :func:`require_configured_repository_installation_route`
  re-enforces ownership and fails closed — an unconfigured Repository route
  is the uniform not-found outcome, and OpenOrc never guesses an
  installation.
- No GitHub/network call ever occurs while a database transaction is open:
  route resolution completes its short transactions before the adapter is
  invoked, and the adapter performs the external calls with no transaction
  open.
- Service translation uses the existing typed application-error vocabulary:
  adapter authorization absence (lost repository access, capability or
  event shortfall, suspension) is a safe classified ``AuthorizationError``;
  other known GitHub rejections are ``ExternalOperationFailedError``;
  timeout/connection loss/uninterpretable answers are
  ``ExternalOperationUncertainError``. No GitHub-shaped workflow exception
  hierarchy exists, and uncertain outcomes are never replayed here.

Credential discipline: the adapter surface carries no credential parameter
of any kind — there is no PAT or human OAuth token fallback. Authentication
always derives from the deployment-held GitHub App identity and the exact
routed installation. Human GitHub sign-in is identity only.
"""

from __future__ import annotations

import logging
from uuid import UUID

from openorc.adapters.github import (
    GitHubAccessValidation,
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
)
from openorc.services.github_installations import (
    require_configured_repository_installation_route,
)

__all__ = ["validate_repository_installation_access"]

logger = logging.getLogger(__name__)

# Application-service span boundary (issues #108/#109): the public use-case
# operation opens one span at the established service boundary. Only safe
# vocabulary attributes are attachable, so credential material and provider
# content have no supported path into telemetry.
_SERVICE_TRACER_SCOPE = "openorc.services.github_repository_access"
_VALIDATE_SPAN_NAME = "github_repository_access.validate_repository_installation_access"


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def validate_repository_installation_access(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
) -> GitHubAccessValidation:
    """Validate the routed installation's current access to one Repository.

    Resolves the configured installation route through the owner-gated #57
    resolver (uniform not-found outcome for an unowned Workspace, an absent
    repository, or an unconfigured route), then — with no database
    transaction open — composes the adapter: a token minted for the exact
    routed installation, stable-ID repository membership validation through
    the documented installation-repositories listing, and the required v1
    capability/event/suspension validation from the JWT-authenticated
    installation object.

    Raises ``AuthorizationError`` when the routed installation does not
    currently grant the access or capabilities the repository's OpenOrc
    operations require — a known integration/authorization condition that
    never triggers a fallback to a human credential. Raises
    ``ExternalOperationFailedError`` for other known GitHub rejections
    (including rate limits, which are deliberately not misread as lost
    authorization) and ``ExternalOperationUncertainError`` when the outcome
    is unknown; uncertain outcomes are never replayed.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _VALIDATE_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(repository_id, "repository_id")
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(span, operation=_VALIDATE_SPAN_NAME, workspace_id=str(workspace_id))
        # Route resolution (owner-gated, fail closed) runs its own short
        # database transactions and completes before the adapter is invoked,
        # so no GitHub/network call ever happens with a transaction open.
        repository, installation = require_configured_repository_installation_route(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
        try:
            return github.validate_installation_repository_access(
                github_installation_id=installation.identity.github_installation_id,
                github_repository_id=repository.identity.github_repository_id,
            )
        except GitHubAuthorizationRejectedError as error:
            raise AuthorizationError(
                "the routed GitHub installation does not currently grant the "
                "access or capabilities this repository's OpenOrc operations "
                "require"
            ) from error
        except GitHubRateLimitedError as error:
            # A rate limit is a known provider condition, deliberately NOT
            # classified as lost authorization.
            raise ExternalOperationFailedError(
                "the GitHub installation access validation was rate limited"
            ) from error
        except GitHubAuthenticationRejectedError as error:
            raise ExternalOperationFailedError(
                "GitHub rejected the OpenOrc GitHub App authentication"
            ) from error
        except GitHubRequestRejectedError as error:
            raise ExternalOperationFailedError(
                "GitHub rejected the installation access validation"
            ) from error
        except GitHubOutcomeUncertainError as error:
            raise ExternalOperationUncertainError(
                "the outcome of the GitHub installation access validation is unknown"
            ) from error
