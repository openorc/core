"""GitHub adapter: OpenOrc GitHub App authentication and REST mechanics.

Owns the GitHub App authentication boundary (App JWT creation, short-lived
installation access-token minting), the focused GitHub REST transport, and
the documented capability/access validation operations for the routed
installation (issue #58). Application services decide all workflow meaning;
this boundary returns normalized typed facts and raises the adapter-local
classified errors only.

Authentication is two-mode by operation semantics (issues #141/#142/#143):
installation authentication is the credential for App-level installation
reads, #57 routing, discovery/validation, authoritative repository/issue/
branch/PR/check/status reads, webhook reconciliation, and recovery —
including the one read-only GraphQL POST surface. Profile-scoped GitHub App
user-to-server authorization is the accountable Owner credential for
engineering-record GitHub writes: the write operations require the exact
Profile-bound user credential in their signatures (no installation-token
mutation path is reachable from them). There is no PAT or human-OAuth-token
fallback anywhere in this boundary, and the public surface accepts no raw
credential value — the user credential travels as the redacted secret-
bearing carrier the #142 lifecycle resolves.
"""

from openorc.adapters.github.authentication import (
    INSTALLATION_TOKEN_CACHE_MAX_ENTRIES,
    INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS,
    GitHubAppAuthenticator,
    InstallationAccessToken,
)
from openorc.adapters.github.capabilities import (
    GITHUB_COMMIT_CHECK_RUNS_PATH,
    GITHUB_COMMIT_COMBINED_STATUS_PATH,
    GITHUB_GRAPHQL_PATH,
    GITHUB_INSTALLATION_PATH,
    GITHUB_INSTALLATION_REPOSITORIES_PATH,
    GITHUB_ISSUE_DEPENDENCIES_BLOCKED_BY_PATH,
    GITHUB_ISSUE_SUB_ISSUES_PATH,
    GITHUB_MINT_INSTALLATION_TOKEN_PATH,
    GITHUB_PULL_REQUEST_MERGE_PATH,
    GITHUB_REPOSITORY_BRANCH_PATH,
    GITHUB_REPOSITORY_ISSUE_PATH,
    GITHUB_REPOSITORY_PULL_REQUEST_PATH,
    REQUIRED_V1_WEBHOOK_EVENTS,
    REQUIRED_V1_WORKFLOW_CAPABILITIES,
    V1_CLASSIFIED_WEBHOOK_EVENTS,
    GitHubAccessValidation,
    GitHubInstallationCapabilities,
    GitHubWorkflowCapability,
    map_installation_permissions,
    missing_required_capabilities,
    missing_required_webhook_events,
    parse_installation_payload,
    parse_installation_repositories_page,
    parse_instant,
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
    GitHubPullRequestExistsError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
    GitHubUserTokenRefreshCapabilityMissingError,
    GitHubUserTokenRejectedError,
)
from openorc.adapters.github.observations import (
    GitHubIssueObservation,
    GitHubRepositoryObservation,
    parse_installation_repository_entry,
    parse_issue_payload,
)
from openorc.adapters.github.observations_branch import (
    GitHubBranchObservation,
    parse_branch_payload,
)
from openorc.adapters.github.observations_checks import (
    GitHubCheckRunObservation,
    GitHubCommitStatusesProjection,
    GitHubStatusContextObservation,
    parse_check_runs_page,
    parse_combined_status_page,
)
from openorc.adapters.github.observations_pull_request import (
    GitHubMergeRequestOutcome,
    GitHubMergeRequestResult,
    GitHubPullRequestFacts,
    GitHubPullRequestObservation,
    body_reports_pull_request_already_exists,
    parse_merge_response_payload,
    parse_pull_request_facts,
    parse_pull_request_payload,
)
from openorc.adapters.github.observations_relations import (
    GitHubIssueParentObservation,
    GitHubRelatedIssueEndpoint,
    GitHubRelatedIssueObservation,
    parse_graphql_issue_parent,
    parse_related_issue_payload,
    parse_related_issue_payloads,
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
from openorc.adapters.github.user_tokens import (
    GITHUB_TOKEN_ENDPOINT_URL,
    GitHubCurrentUser,
    GitHubProfileUserAccessToken,
    GitHubUserAccessToken,
    GitHubUserRefreshSecret,
    GitHubUserTokenClient,
    GitHubUserTokenGrant,
    HttpGitHubUserTokenClient,
)

__all__ = [
    "DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS",
    "GITHUB_API_BASE_URL",
    "GITHUB_COMMIT_CHECK_RUNS_PATH",
    "GITHUB_COMMIT_COMBINED_STATUS_PATH",
    "GITHUB_INSTALLATION_PATH",
    "GITHUB_INSTALLATION_REPOSITORIES_PATH",
    "GITHUB_ISSUE_DEPENDENCIES_BLOCKED_BY_PATH",
    "GITHUB_ISSUE_SUB_ISSUES_PATH",
    "GITHUB_JSON_ACCEPT_HEADER",
    "GITHUB_MINT_INSTALLATION_TOKEN_PATH",
    "GITHUB_GRAPHQL_PATH",
    "GITHUB_PULL_REQUEST_MERGE_PATH",
    "GITHUB_REPOSITORY_BRANCH_PATH",
    "GITHUB_REPOSITORY_ISSUE_PATH",
    "GITHUB_REPOSITORY_PULL_REQUEST_PATH",
    "GITHUB_TOKEN_ENDPOINT_URL",
    "GITHUB_USER_INSTALLATIONS_PATH",
    "GITHUB_USER_INSTALLATION_REPOSITORIES_PATH",
    "INSTALLATION_TOKEN_CACHE_MAX_ENTRIES",
    "INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS",
    "REQUIRED_V1_WEBHOOK_EVENTS",
    "REQUIRED_V1_WORKFLOW_CAPABILITIES",
    "V1_CLASSIFIED_WEBHOOK_EVENTS",
    "SUPPORTED_GITHUB_API_VERSION",
    "GitHubAccessValidation",
    "GitHubAppAuthenticator",
    "GitHubAppClient",
    "GitHubBranchObservation",
    "GitHubCheckRunObservation",
    "GitHubCommitStatusesProjection",
    "GitHubCurrentUser",
    "GitHubFetcher",
    "GitHubHttpResponse",
    "GitHubInstallationCapabilities",
    "GitHubIssueObservation",
    "GitHubIssueParentObservation",
    "GitHubMergeRequestOutcome",
    "GitHubMergeRequestResult",
    "GitHubPullRequestFacts",
    "GitHubProfileUserAccessToken",
    "GitHubPullRequestObservation",
    "body_reports_pull_request_already_exists",
    "GitHubRelatedIssueEndpoint",
    "GitHubRelatedIssueObservation",
    "GitHubRepositoryObservation",
    "GitHubStatusContextObservation",
    "GitHubUserAccessToken",
    "GitHubUserAccessValidation",
    "GitHubUserRefreshSecret",
    "GitHubUserTokenClient",
    "GitHubUserTokenGrant",
    "GitHubWorkflowCapability",
    "HttpGitHubAppClient",
    "HttpGitHubRestClient",
    "HttpGitHubUserTokenClient",
    "InstallationAccessToken",
    "GitHubAuthenticationRejectedError",
    "GitHubAuthorizationRejectedError",
    "GitHubOutcomeUncertainError",
    "GitHubPullRequestExistsError",
    "GitHubRateLimitedError",
    "GitHubRequestRejectedError",
    "GitHubUserTokenRefreshCapabilityMissingError",
    "GitHubUserTokenRejectedError",
    "map_installation_permissions",
    "missing_required_capabilities",
    "missing_required_webhook_events",
    "parse_branch_payload",
    "parse_check_runs_page",
    "parse_combined_status_page",
    "parse_instant",
    "parse_installation_payload",
    "parse_installation_repository_entry",
    "parse_installation_repositories_page",
    "parse_user_installations_page",
    "parse_graphql_issue_parent",
    "parse_issue_payload",
    "parse_merge_response_payload",
    "parse_pull_request_facts",
    "parse_pull_request_payload",
    "parse_related_issue_payload",
    "parse_related_issue_payloads",
    "http_fetch",
    "require_positive_int",
]
