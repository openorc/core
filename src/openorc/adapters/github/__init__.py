"""GitHub adapter: OpenOrc GitHub App authentication and REST mechanics.

Owns the GitHub App authentication boundary (App JWT creation, short-lived
installation access-token minting), the focused GitHub REST transport, and
the documented capability/access validation operations for the routed
installation (issue #58). Application services decide all workflow meaning;
this boundary returns normalized typed facts and raises the adapter-local
classified errors only. There is no PAT or human OAuth token fallback
anywhere in this boundary: human GitHub sign-in is identity only.
"""

from openorc.adapters.github.authentication import (
    INSTALLATION_TOKEN_CACHE_MAX_ENTRIES,
    INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS,
    GitHubAppAuthenticator,
    InstallationAccessToken,
)
from openorc.adapters.github.capabilities import (
    GITHUB_INSTALLATION_PATH,
    GITHUB_INSTALLATION_REPOSITORIES_PATH,
    GITHUB_MINT_INSTALLATION_TOKEN_PATH,
    REQUIRED_V1_WEBHOOK_EVENTS,
    REQUIRED_V1_WORKFLOW_CAPABILITIES,
    GitHubAccessValidation,
    GitHubInstallationCapabilities,
    GitHubWorkflowCapability,
    map_installation_permissions,
    missing_required_capabilities,
    missing_required_webhook_events,
    parse_installation_payload,
    parse_installation_repositories_page,
    require_positive_int,
)
from openorc.adapters.github.client import (
    GitHubAppClient,
    HttpGitHubAppClient,
)
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)
from openorc.adapters.github.transport import (
    DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    GITHUB_API_BASE_URL,
    GITHUB_JSON_ACCEPT_HEADER,
    SUPPORTED_GITHUB_API_VERSION,
    GitHubFetcher,
    GitHubHttpResponse,
    HttpGitHubRestClient,
    http_fetch,
)

__all__ = [
    "DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS",
    "GITHUB_API_BASE_URL",
    "GITHUB_INSTALLATION_PATH",
    "GITHUB_INSTALLATION_REPOSITORIES_PATH",
    "GITHUB_JSON_ACCEPT_HEADER",
    "GITHUB_MINT_INSTALLATION_TOKEN_PATH",
    "INSTALLATION_TOKEN_CACHE_MAX_ENTRIES",
    "INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS",
    "REQUIRED_V1_WEBHOOK_EVENTS",
    "REQUIRED_V1_WORKFLOW_CAPABILITIES",
    "SUPPORTED_GITHUB_API_VERSION",
    "GitHubAccessValidation",
    "GitHubAppAuthenticator",
    "GitHubAppClient",
    "GitHubFetcher",
    "GitHubHttpResponse",
    "GitHubInstallationCapabilities",
    "GitHubWorkflowCapability",
    "HttpGitHubAppClient",
    "HttpGitHubRestClient",
    "InstallationAccessToken",
    "GitHubAuthenticationRejectedError",
    "GitHubAuthorizationRejectedError",
    "GitHubOutcomeUncertainError",
    "GitHubRateLimitedError",
    "GitHubRequestRejectedError",
    "map_installation_permissions",
    "missing_required_capabilities",
    "missing_required_webhook_events",
    "parse_installation_payload",
    "parse_installation_repositories_page",
    "http_fetch",
    "require_positive_int",
]
