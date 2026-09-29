"""The composed OpenOrc GitHub App client (issue #58).

The public adapter facade application services consume: it composes the
GitHub App authenticator and the REST transport and implements the concrete
documented operations this leaf owns, returning normalized typed facts and
raising the adapter-local classified errors. It decides nothing about Task
state, Owner authority, retry policy, or workflow transitions — the services
above own all workflow meaning.

Two strictly separated validation fact sources (see
:mod:`openorc.adapters.github.capabilities`):

1. ``GET /app/installations/{installation_id}`` under a fresh App JWT — the
   documented installation object supplying the fine-grained ``permissions``
   dictionary, the subscribed ``events``, and the ``suspended_at`` state
   consumed by capability validation.
2. ``GET /installation/repositories`` under the installation access token —
   the documented paginated listing used solely to prove repository
   membership by matching the numeric stable repository ``id``. Its
   per-entry ``permissions`` member is the ordinary repository access shape
   and is never consumed as capability authority.

Pagination follows GitHub's documented ``Link`` header (``rel="next"``) and
is exercised by this concrete operation; the listing page count is bounded
so a broken pagination chain can never loop silently — exceeding the bound
classifies as an uncertain outcome because the access question cannot be
answered from an incomplete listing.

Credential discipline (two-mode, issues #141/#142/#143): the credential
mode follows the operation's semantics, never its HTTP verb. Installation
authentication remains the credential for every infrastructure/read
operation — the installation repository/branch/PR/check/status reads, the
read-only GraphQL observations (a POST to the GraphQL endpoint is a read
there), webhook reconciliation, and recovery mechanics — with installation
tokens minted by the authenticator for the exact installation, presented as
``Authorization: Bearer`` headers on the immediate transport call, and
evicted with a single bounded re-mint when GitHub rejects the presented
token with 401. Owner-accountable engineering-record writes (canonical PR
creation, exact-head merge) require the exact Profile-bound GitHub App
user-to-server credential (:class:`GitHubProfileUserAccessToken`) in their
signatures: there is no installation-token mutation path reachable from
them, no installation-token fallback, and no PAT or human-OAuth fallback
anywhere. The user-credential lifecycle (resolution, refresh, eviction, and
the bounded 401 recovery) is owned by the application-service resolver;
this adapter never resolves, stores, or refreshes a user token. Uncertain
outcomes are never replayed. No credential material ever crosses this
facade into service code: the public surface returns typed facts and
raises typed adapter errors only.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from typing import Protocol
from urllib.parse import quote

from openorc.adapters.github.authentication import GitHubAppAuthenticator
from openorc.adapters.github.capabilities import (
    GITHUB_COMMIT_CHECK_RUNS_PATH,
    GITHUB_COMMIT_COMBINED_STATUS_PATH,
    GITHUB_GRAPHQL_PATH,
    GITHUB_INSTALLATION_PATH,
    GITHUB_INSTALLATION_REPOSITORIES_PATH,
    GITHUB_ISSUE_DEPENDENCIES_BLOCKED_BY_PATH,
    GITHUB_ISSUE_SUB_ISSUES_PATH,
    GITHUB_PULL_REQUEST_MERGE_PATH,
    GITHUB_REPOSITORY_BRANCH_PATH,
    GITHUB_REPOSITORY_ISSUE_PATH,
    GITHUB_REPOSITORY_PULL_REQUEST_PATH,
    GITHUB_REPOSITORY_PULL_REQUESTS_COLLECTION_PATH,
    GITHUB_USER_INSTALLATION_REPOSITORIES_PATH,
    GITHUB_USER_INSTALLATIONS_PATH,
    GitHubAccessValidation,
    GitHubInstallationCapabilities,
    GitHubUserAccessValidation,
    missing_required_capabilities,
    missing_required_webhook_events,
    parse_installation_payload,
    parse_installation_repositories_page,
    parse_user_installations_page,
    require_positive_int,
)
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubPullRequestExistsError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
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
    GitHubRelatedIssueObservation,
    parse_graphql_issue_parent,
    parse_related_issue_payloads,
)
from openorc.adapters.github.transport import (
    DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    GitHubFetcher,
    GitHubHttpResponse,
    HttpGitHubRestClient,
    github_repository_api_address,
    is_github_api_origin,
)
from openorc.adapters.github.user_tokens import GitHubProfileUserAccessToken
from openorc.config import (
    GITHUB_APP_ID_VAR,
    GITHUB_APP_PRIVATE_KEY_VAR,
    ConfigurationError,
    Settings,
)
from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.observability import annotate_span, application_span

__all__ = [
    "GitHubAppClient",
    "HttpGitHubAppClient",
]

logger = logging.getLogger(__name__)

# Representative external-adapter span boundaries (issues #108/#109): one
# instrumented external operation per GitHub call sequence. Only the safe
# attribute vocabulary is attachable — the operation name, the stable
# installation identifier, and the stable repository identifier. The App
# JWT, installation tokens, provider URLs, and response bodies have no
# supported path into telemetry.
_TRACER_SCOPE = "openorc.adapters.github.client"
_INSTALLATION_LOOKUP_SPAN_NAME = "github.get_installation_capabilities"
_LISTING_SPAN_NAME = "github.list_installation_repositories"
_ISSUE_SPAN_NAME = "github.get_repository_issue"
_BLOCKED_BY_SPAN_NAME = "github.get_issue_blocked_by"
_SUB_ISSUES_SPAN_NAME = "github.get_issue_sub_issues"
_PARENT_SPAN_NAME = "github.get_issue_parent"
_RELATED_ENDPOINTS_SPAN_NAME = "github.resolve_related_issue_endpoints"
_BRANCH_SPAN_NAME = "github.get_repository_branch"
_PULL_REQUEST_SPAN_NAME = "github.get_repository_pull_request"
_CHECK_RUNS_SPAN_NAME = "github.get_commit_check_runs"
_COMBINED_STATUS_SPAN_NAME = "github.get_commit_combined_status"
_MERGE_SPAN_NAME = "github.merge_pull_request"
_CREATE_PULL_REQUEST_SPAN_NAME = "github.create_pull_request"
_USER_ACCESS_SPAN_NAME = "github.validate_user_installation_repository_access"

# The installation repository listing requests the maximum documented page
# size (100) and is bounded so a broken pagination chain cannot loop.
_LISTING_PAGE_SIZE = 100
_MAX_LISTING_PAGES = 100

# The minimal documented GraphQL query for the one parent fact REST cannot
# authoritatively express: the nullable ``Issue.parent`` field plus the
# documented ``databaseId`` stable numeric identifiers of the parent issue
# and its repository (GitHub GraphQL schema). Nothing else is selected. The
# owner/name strings are embedded as escaped GraphQL string literals by the
# caller (the escape helper below); the issue number is a validated integer.
_ISSUE_PARENT_QUERY_TEMPLATE = (
    "query { repository(owner: __OWNER__, name: __NAME__) {"
    " issue(number: __NUMBER__) {"
    " parent { databaseId repository { databaseId } }"
    " }"
    " }"
)


def _issue_parent_query(*, owner_login: str, repository_name: str, issue_number: int) -> str:
    """Build the minimal documented parent query for one addressed issue."""
    return (
        _ISSUE_PARENT_QUERY_TEMPLATE.replace("__OWNER__", _graphql_string_literal(owner_login))
        .replace("__NAME__", _graphql_string_literal(repository_name))
        .replace("__NUMBER__", str(issue_number))
    )


def _graphql_string_literal(value: str) -> str:
    """Escape one caller-supplied string as a GraphQL string literal.

    Only the two documented string values (owner login, repository name)
    pass through here; backslashes and double quotes are escaped so the
    value cannot break out of the literal.
    """
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class GitHubAppClient(Protocol):
    """The GitHub adapter boundary application services depend on.

    Typed seam so services never import the concrete HTTP client. The
    credential mode follows the operation's semantics (issue #143): every
    infrastructure/read operation authenticates through the exact routed
    installation, while the Owner-accountable engineering-record writes
    (``create_pull_request``/``merge_pull_request``) require the exact
    Profile-bound GitHub App user-to-server credential in their signatures —
    never a raw token, never an installation token, and no PAT or human-OAuth
    fallback anywhere in this boundary.
    """

    def validate_installation_repository_access(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubAccessValidation:
        """Validate current repository access and the required v1 capabilities."""
        ...

    def validate_user_installation_repository_access(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        github_repository_id: int,
    ) -> GitHubUserAccessValidation:
        """Prove the user × installation × repository intersection for one Owner write."""
        ...

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        """Return the normalized current observation of the stable repository."""
        ...

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        """Return the normalized current observation of one repository issue."""
        ...

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        """Return the normalized current observation of one repository branch."""
        ...

    def get_repository_pull_request(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
    ) -> GitHubPullRequestObservation:
        """Return the normalized current observation of one repository pull request."""
        ...

    def get_commit_check_runs(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        head_sha: str,
    ) -> list[GitHubCheckRunObservation]:
        """Return the exhaustively paginated check-run projection for one exact head."""
        ...

    def get_commit_combined_status(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        head_sha: str,
    ) -> GitHubCommitStatusesProjection:
        """Return the exhaustively paginated combined-status projection for one exact head."""
        ...

    def merge_pull_request(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
        expected_head_sha: str,
    ) -> GitHubMergeRequestResult:
        """Request one exact-head merge of a pull request under GitHub's sha guard."""
        ...

    def create_pull_request(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str | None,
    ) -> GitHubPullRequestFacts:
        """Create one branch-addressed pull request and report its stable identity."""
        ...

    def get_installation_capabilities(
        self, github_installation_id: int
    ) -> GitHubInstallationCapabilities:
        """Return the normalized capability facts for the exact installation."""
        ...

    def get_issue_blocked_by(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        """Return the authoritative blocked-by dependency listing of one issue."""
        ...

    def get_issue_sub_issues(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        """Return the authoritative sub-issue listing of one issue."""
        ...

    def get_issue_parent(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueParentObservation:
        """Return the authoritative parent-or-no-parent fact of one issue."""
        ...

    def resolve_related_issue_endpoints(
        self,
        *,
        github_installation_id: int,
        local_github_repository_id: int,
        fresh_owner_login: str,
        fresh_repository_name: str,
        related: list[GitHubRelatedIssueObservation],
    ) -> list[RelatedIssueEndpoint]:
        """Resolve related-issue repository references into stable endpoint identities."""
        ...


class HttpGitHubAppClient:
    """The concrete composed GitHub App client for the documented operations.

    Constructed from the deployment-held GitHub App identity (typically via
    :meth:`from_settings`) with an injectable fetch seam and clock for
    deterministic tests.
    """

    def __init__(
        self,
        *,
        authenticator: GitHubAppAuthenticator,
        transport: HttpGitHubRestClient,
    ) -> None:
        self._authenticator = authenticator
        self._transport = transport

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        clock: Callable[[], float] | None = None,
        fetch: GitHubFetcher | None = None,
        timeout_seconds: float = DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    ) -> HttpGitHubAppClient:
        """Construct the client from deployment configuration, failing fast.

        The GitHub App identity requirement is enforced here — at the point
        the GitHub client is constructed — so unrelated processes booting
        through shared settings never need the private key. Absent identity
        configuration raises ``ConfigurationError``.
        """
        if settings.github_app_id is None or settings.github_app_private_key is None:
            raise ConfigurationError(
                "the GitHub App client requires the deployed GitHub App identity: "
                f"{GITHUB_APP_ID_VAR} and {GITHUB_APP_PRIVATE_KEY_VAR} must be configured"
            )
        authenticator = GitHubAppAuthenticator(
            app_id=settings.github_app_id,
            private_key_pem=settings.github_app_private_key,
            clock=clock,
            fetch=fetch,
            timeout_seconds=timeout_seconds,
        )
        transport = HttpGitHubRestClient(timeout_seconds=timeout_seconds, fetch=fetch)
        return cls(authenticator=authenticator, transport=transport)

    def get_installation_capabilities(
        self, github_installation_id: int
    ) -> GitHubInstallationCapabilities:
        """Fetch the installation object's capability facts (App-JWT authenticated).

        The documented installation object is the source of the fine-grained
        ``permissions`` dictionary, the subscribed ``events``, and the
        ``suspended_at`` state. Facts bind to the exact stable installation
        identity addressed.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        with application_span(_TRACER_SCOPE, _INSTALLATION_LOOKUP_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_INSTALLATION_LOOKUP_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            response = self._transport.request(
                GITHUB_INSTALLATION_PATH.format(installation_id=github_installation_id),
                method="GET",
                authorization=f"Bearer {self._authenticator.app_jwt()}",
            )
            return parse_installation_payload(
                _json_body(response), github_installation_id=github_installation_id
            )

    def validate_installation_repository_access(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubAccessValidation:
        """Validate repository access and required v1 capabilities for the exact installation.

        Composes the two separated validation facts: the installation object
        (permissions, subscribed events, suspension state) and the paginated
        stable-ID repository membership listing. Any authorization absence —
        suspension, a capability or event shortfall, or absence of the
        stable repository identity from the installation's accessible set —
        raises the classified authorization rejection. It never triggers a
        fallback to a human credential.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(github_repository_id, "github_repository_id")
        capabilities = self._require_validated_capabilities(github_installation_id)
        self._require_installation_repository_entry(github_installation_id, github_repository_id)
        return GitHubAccessValidation(
            github_installation_id=github_installation_id,
            github_repository_id=github_repository_id,
            capabilities=capabilities.capabilities,
            subscribed_events=capabilities.subscribed_events,
        )

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        """Return the authoritative observation of the stable repository.

        Performs the full v1 access validation for the exact routed
        installation (suspension, capabilities, subscribed events) and, in
        the same bounded walk of the documented
        ``GET /installation/repositories`` listing, returns the normalized
        observation of the entry matching the exact stable repository ID.
        There is no documented REST operation to fetch a repository by
        stable ID, and a stored owner/name address breaks exactly when a
        rename or ownership transfer must be reconciled, so the stable-ID
        listing entry is the observation source. Absence of the stable
        identity raises the classified authorization rejection; the
        bounded-page exhaustion raises the uncertain outcome.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(github_repository_id, "github_repository_id")
        self._require_validated_capabilities(github_installation_id)
        entry = self._require_installation_repository_entry(
            github_installation_id, github_repository_id
        )
        return parse_installation_repository_entry(entry, github_repository_id=github_repository_id)

    def validate_user_installation_repository_access(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        github_repository_id: int,
    ) -> GitHubUserAccessValidation:
        """Prove the effective user/App/installation/repository intersection.

        The two documented App-scoped user-to-server listings under the
        exact Profile-bound user access token (issue #143): the paginated
        ``GET /user/installations`` walk proves the authorized user can act
        through the exact routed installation of this GitHub App, and the
        paginated ``GET /user/installations/{id}/repositories`` walk proves
        that installation currently grants the user access to the exact
        stable repository identity. Absence of either stable identity after
        the complete listing raises the classified authorization rejection;
        a bounded-page exhaustion or any uninterpretable answer raises the
        uncertain outcome. This proof never falls back to another
        installation, another Profile, or an installation-token write, and
        a later definitive permission/policy rejection from GitHub on the
        write itself remains a normal known provider outcome.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(github_repository_id, "github_repository_id")
        with application_span(_TRACER_SCOPE, _USER_ACCESS_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_USER_ACCESS_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            path = f"{GITHUB_USER_INSTALLATIONS_PATH}?per_page={_LISTING_PAGE_SIZE}"
            page_count = 0
            installation_proven = False
            while page_count < _MAX_LISTING_PAGES:
                response = self._request_with_user_access_token(credential, path)
                if github_installation_id in parse_user_installations_page(_json_body(response)):
                    installation_proven = True
                    break
                next_url = self._next_page_url(response)
                if next_url is None:
                    raise GitHubAuthorizationRejectedError(
                        "the accountable Profile's GitHub user authorization cannot act "
                        "through the routed installation"
                    )
                path = next_url
                page_count += 1
            if not installation_proven:
                raise GitHubOutcomeUncertainError(
                    "the user installation listing could not be completed within the "
                    "bounded page count: the access outcome is unknown"
                )
            repositories_path = GITHUB_USER_INSTALLATION_REPOSITORIES_PATH.format(
                installation_id=github_installation_id
            )
            path = f"{repositories_path}?per_page={_LISTING_PAGE_SIZE}"
            page_count = 0
            while page_count < _MAX_LISTING_PAGES:
                response = self._request_with_user_access_token(credential, path)
                if github_repository_id in parse_installation_repositories_page(
                    _json_body(response)
                ):
                    return GitHubUserAccessValidation(
                        github_installation_id=github_installation_id,
                        github_repository_id=github_repository_id,
                    )
                next_url = self._next_page_url(response)
                if next_url is None:
                    raise GitHubAuthorizationRejectedError(
                        "the accountable Profile's GitHub user authorization cannot reach "
                        "the routed repository through the routed installation"
                    )
                path = next_url
                page_count += 1
            raise GitHubOutcomeUncertainError(
                "the user installation repository listing could not be completed within "
                "the bounded page count: the access outcome is unknown"
            )

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        """Return the authoritative observation of one repository issue.

        The documented ``GET /repos/{owner}/{repo}/issues/{issue_number}``
        operation under the installation access token. ``owner_login`` and
        ``repository_name`` must be the freshly observed repository address
        (from :meth:`get_installation_repository`), never stored mutable
        metadata: a rename or ownership transfer is reconciled through the
        fresh stable-ID observation first, so the issue read is addressed
        consistently. A documented ``pull_request`` member is carried
        through as a typed discriminator; application services decide its
        workflow meaning.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(issue_number, "issue_number")
        if not isinstance(owner_login, str) or not owner_login.strip():
            raise ValueError("owner_login must be a non-empty string")
        if not isinstance(repository_name, str) or not repository_name.strip():
            raise ValueError("repository_name must be a non-empty string")
        with application_span(_TRACER_SCOPE, _ISSUE_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_ISSUE_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_issue_number=issue_number,
            )
            path = GITHUB_REPOSITORY_ISSUE_PATH.format(
                owner=owner_login, repo=repository_name, issue_number=issue_number
            )
            response = self._request_with_installation_token(github_installation_id, path)
            return parse_issue_payload(
                _json_body(response),
                owner_login=owner_login,
                repository_name=repository_name,
                issue_number=issue_number,
            )

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        """Return the authoritative observation of one repository branch.

        The documented ``GET /repos/{owner}/{repo}/branches/{branch}``
        operation under the installation access token, addressed through the
        freshly observed repository address. ``branch_name`` is the exact
        branch the caller addresses (percent-encoded as one path segment);
        the response binds to the addressed name, and the normalized
        observation carries the exact GitHub-committed head SHA — the
        committed-state fact runtime-local HEAD can never establish. A 404
        answer (a missing/deleted branch or absent repository access) is the
        transport's classified authorization absence: the normalized
        integration condition, never permission to trust runtime-local state.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        _require_non_empty_command_str(branch_name, "branch_name")
        with application_span(_TRACER_SCOPE, _BRANCH_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_BRANCH_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            path = GITHUB_REPOSITORY_BRANCH_PATH.format(
                owner=owner_login,
                repo=repository_name,
                branch=quote(branch_name, safe=""),
            )
            response = self._request_with_installation_token(github_installation_id, path)
            return parse_branch_payload(_json_body(response), branch_name=branch_name)

    def get_repository_pull_request(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
    ) -> GitHubPullRequestObservation:
        """Return the authoritative observation of one repository pull request.

        The documented ``GET /repos/{owner}/{repo}/pulls/{pull_number}``
        operation under the installation access token, addressed through the
        freshly observed repository address — never stored mutable metadata.
        The response binds to the addressed subject through its own
        ``number``/``url`` members; the normalized observation carries the
        stable PR identity and the mutable head/base/lifecycle facts the
        canonical TaskPullRequest reconciliation updates in place.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(pull_number, "pull_number")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        with application_span(_TRACER_SCOPE, _PULL_REQUEST_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_PULL_REQUEST_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_pull_request_number=pull_number,
            )
            path = GITHUB_REPOSITORY_PULL_REQUEST_PATH.format(
                owner=owner_login, repo=repository_name, pull_number=pull_number
            )
            response = self._request_with_installation_token(github_installation_id, path)
            return parse_pull_request_payload(
                _json_body(response),
                owner_login=owner_login,
                repository_name=repository_name,
                pull_number=pull_number,
            )

    def get_commit_check_runs(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        head_sha: str,
    ) -> list[GitHubCheckRunObservation]:
        """Return the exhaustive check-run projection for one exact head.

        The documented ``GET /repos/{owner}/{repo}/commits/{ref}/check-runs``
        operation under the installation access token (``checks`` read),
        addressed by the exact head SHA — never a branch name. The listing is
        walked through GitHub's documented ``Link`` headers within the bounded
        page count, and the collected check runs are proven complete against
        the documented ``total_count``: any bound overrun, broken pagination
        chain, count mismatch, or uninterpretable page is an uncertain
        outcome — never a silently partial projection of GitHub-owned facts.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        _require_non_empty_command_str(head_sha, "head_sha")
        with application_span(_TRACER_SCOPE, _CHECK_RUNS_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_CHECK_RUNS_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_head_sha=head_sha,
            )
            path = (
                GITHUB_COMMIT_CHECK_RUNS_PATH.format(
                    owner=owner_login, repo=repository_name, ref=head_sha
                )
                + f"?per_page={_LISTING_PAGE_SIZE}"
            )
            collected: list[GitHubCheckRunObservation] = []
            total_count: int | None = None
            page_count = 0
            while path is not None:
                if page_count >= _MAX_LISTING_PAGES:
                    raise GitHubOutcomeUncertainError(
                        "the check runs listing could not be completed within the "
                        "bounded page count: the outcome is unknown"
                    )
                response = self._request_with_installation_token(github_installation_id, path)
                page_total, page_runs = parse_check_runs_page(_json_body(response))
                if total_count is None:
                    total_count = page_total
                elif page_total != total_count:
                    raise GitHubOutcomeUncertainError(
                        "the check runs listing is not interpretable: its pages report "
                        "different totals"
                    )
                collected.extend(page_runs)
                path = self._next_page_url(response)
                page_count += 1
            if total_count is None or len(collected) != total_count:
                raise GitHubOutcomeUncertainError(
                    "the check runs listing is incomplete: the documented total was "
                    "not reached, so the projection cannot be proven complete"
                )
            return collected

    def get_commit_combined_status(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        head_sha: str,
    ) -> GitHubCommitStatusesProjection:
        """Return the exhaustive combined-status projection for one exact head.

        The documented ``GET /repos/{owner}/{repo}/commits/{ref}/status``
        operation under the installation access token, addressed by the exact
        head SHA. The combined-status listing is itself paginated: every page
        binds its documented ``sha`` member to the addressed head and reports
        the provider-owned aggregate ``state``/``total_count``, and the walk
        collects every status context until the documented total is reached
        within the bounded page count. Any incompleteness is an uncertain
        outcome — the aggregate state is GitHub's own answer and is returned
        only together with the proven-complete context listing.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        _require_non_empty_command_str(head_sha, "head_sha")
        with application_span(_TRACER_SCOPE, _COMBINED_STATUS_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_COMBINED_STATUS_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_head_sha=head_sha,
            )
            path = (
                GITHUB_COMMIT_COMBINED_STATUS_PATH.format(
                    owner=owner_login, repo=repository_name, ref=head_sha
                )
                + f"?per_page={_LISTING_PAGE_SIZE}"
            )
            contexts: list[GitHubStatusContextObservation] = []
            aggregate_state: str | None = None
            total_count: int | None = None
            page_count = 0
            while path is not None:
                if page_count >= _MAX_LISTING_PAGES:
                    raise GitHubOutcomeUncertainError(
                        "the combined status listing could not be completed within the "
                        "bounded page count: the outcome is unknown"
                    )
                response = self._request_with_installation_token(github_installation_id, path)
                page_state, page_total, page_contexts = parse_combined_status_page(
                    _json_body(response), head_sha=head_sha
                )
                if aggregate_state is None:
                    aggregate_state, total_count = page_state, page_total
                elif page_state != aggregate_state or page_total != total_count:
                    raise GitHubOutcomeUncertainError(
                        "the combined status listing is not interpretable: its pages "
                        "report different aggregates"
                    )
                contexts.extend(page_contexts)
                path = self._next_page_url(response)
                page_count += 1
            if total_count is None or len(contexts) != total_count:
                raise GitHubOutcomeUncertainError(
                    "the combined status listing is incomplete: the documented total "
                    "was not reached, so the projection cannot be proven complete"
                )
            assert aggregate_state is not None
            return GitHubCommitStatusesProjection(state=aggregate_state, statuses=tuple(contexts))

    def merge_pull_request(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
        expected_head_sha: str,
    ) -> GitHubMergeRequestResult:
        """Request one exact-head merge under GitHub's documented sha guard.

        The documented ``PUT /repos/{owner}/{repo}/pulls/{pull_number}/merge``
        operation under the exact Profile-bound user-to-server credential
        (issue #143): this Owner-accountable engineering-record mutation
        carries the accountable Profile's user authorization — an
        installation token can never reach it and there is no
        installation-token fallback. Merge authority still derives from the
        App's ``contents: write`` validated at route/access time — never
        from a pull-request permission — and the routed installation
        identity remains part of the operation's address. The request
        carries the documented ``sha`` parameter — the provider's
        expected-head facility — so a concurrent head change can never
        satisfy this merge request: GitHub answers the documented 409
        head-mismatch rejection, normalized here as ``HEAD_MISMATCH``. Any
        other definitive rejection is the known ``REJECTED`` outcome (GitHub
        owns the merge policy: required checks, conflicts, branch
        protection). Authentication/access absence, rate limits, and
        uncertain outcomes (timeout, connection loss, redirect, 5xx,
        uninterpretable answer) raise the adapter's classified errors — an
        uncertain merge outcome is never silently replayed, and a definitive
        401-style rejection propagates for the service's single bounded
        user-token recovery pass (issue #142 lifecycle).
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(pull_number, "pull_number")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        _require_non_empty_command_str(expected_head_sha, "expected_head_sha")
        with application_span(_TRACER_SCOPE, _MERGE_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_MERGE_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_pull_request_number=pull_number,
                github_head_sha=expected_head_sha,
            )
            path = GITHUB_PULL_REQUEST_MERGE_PATH.format(
                owner=owner_login, repo=repository_name, pull_number=pull_number
            )
            body = json.dumps({"sha": expected_head_sha}).encode("utf-8")
            try:
                response = self._put_with_user_access_token(credential, path, body)
            except (
                GitHubAuthenticationRejectedError,
                GitHubAuthorizationRejectedError,
                GitHubRateLimitedError,
            ):
                raise
            except GitHubRequestRejectedError as error:
                # The definitive rejections of this documented operation are
                # classified from GitHub's own documented response semantics:
                # 409 is the expected-head mismatch; every other definitive
                # non-success is GitHub-owned policy/state refusal. The merge
                # applied nothing in either case.
                if error.status_code == 409:
                    return GitHubMergeRequestResult(
                        outcome=GitHubMergeRequestOutcome.HEAD_MISMATCH, merge_commit_sha=None
                    )
                return GitHubMergeRequestResult(
                    outcome=GitHubMergeRequestOutcome.REJECTED, merge_commit_sha=None
                )
            return parse_merge_response_payload(_json_body(response))

    def create_pull_request(
        self,
        *,
        credential: GitHubProfileUserAccessToken,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str | None,
    ) -> GitHubPullRequestFacts:
        """Create one branch-addressed pull request (the documented create).

        The documented ``POST /repos/{owner}/{repo}/pulls`` operation under
        the exact Profile-bound user-to-server credential (issue #143): the
        canonical PR creation is an Owner-accountable engineering-record
        write, externally attributed to the accountable Profile's authorized
        GitHub account — an installation token can never reach it and there
        is no installation-token fallback. GitHub's create operation is
        branch-addressed and provides no atomic expected-head guard — the
        exact-head race is closed above this adapter (preflight + immediate
        post-create reconciliation), never here. The routed installation
        identity remains part of the operation's address; the request
        carries only the presentation title/body and the branch routing
        facts; the response is normalized into the stable PR identity and
        facts.

        The documented 'pull request already exists' definitive rejection
        (a branch already having an open PR toward the base) raises the
        typed :class:`GitHubPullRequestExistsError`; every other definitive
        rejection raises the generic classified error. Authentication,
        access, rate-limit, and uncertain outcomes (timeout, connection
        loss, refused redirect on this mutating request, 5xx) raise the
        adapter's classified errors — an uncertain create is never replayed
        by this adapter and never reclassified as a success or a known
        failure, and a definitive 401-style rejection propagates for the
        service's single bounded user-token recovery pass (issue #142
        lifecycle).
        """
        require_positive_int(github_installation_id, "github_installation_id")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        _require_non_empty_command_str(head_ref, "head_ref")
        _require_non_empty_command_str(base_ref, "base_ref")
        _require_non_empty_command_str(title, "title")
        if body is not None and not isinstance(body, str):
            raise ValueError("body must be None or a string")
        with application_span(_TRACER_SCOPE, _CREATE_PULL_REQUEST_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_CREATE_PULL_REQUEST_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            path = GITHUB_REPOSITORY_PULL_REQUESTS_COLLECTION_PATH.format(
                owner=owner_login, repo=repository_name
            )
            payload: dict[str, str] = {
                "head": head_ref,
                "base": base_ref,
                "title": title,
            }
            if body is not None:
                payload["body"] = body
            request_body = json.dumps(payload).encode("utf-8")
            try:
                response = self._post_with_user_access_token(credential, path, request_body)
            except (
                GitHubAuthenticationRejectedError,
                GitHubAuthorizationRejectedError,
                GitHubPullRequestExistsError,
                GitHubRateLimitedError,
            ):
                raise
            except GitHubRequestRejectedError as error:
                if error.status_code == 422 and body_reports_pull_request_already_exists(
                    error.response_body or b""
                ):
                    # The documented 'pull request already exists' answer:
                    # a branch-addressed create refused because an open PR
                    # already exists for the head/base. Classified from the
                    # documented response members only — a bare 422 is the
                    # endpoint's general validation failure and stays the
                    # ordinary definitive rejection, because a non-duplicate
                    # validation refusal must never become the workflow's
                    # duplicate-PR conflict.
                    raise GitHubPullRequestExistsError(
                        "a pull request already exists for the addressed branch and base",
                        status_code=422,
                    ) from error
                raise
            return parse_pull_request_facts(_json_body(response))

    def get_issue_blocked_by(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        """Return the authoritative blocked-by dependency listing of one issue.

        The documented
        ``GET /repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocked_by``
        operation under the installation access token (Issues read). The
        listing is observed through the freshly observed repository address,
        never stored mutable metadata. An uninterpretable listing raises the
        classified uncertain outcome; a failed answer raises its classified
        error — neither is ever returned as an authoritative (possibly
        empty) dependency fact.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(issue_number, "issue_number")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        with application_span(_TRACER_SCOPE, _BLOCKED_BY_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_BLOCKED_BY_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_issue_number=issue_number,
            )
            path = (
                GITHUB_ISSUE_DEPENDENCIES_BLOCKED_BY_PATH.format(
                    owner=owner_login, repo=repository_name, issue_number=issue_number
                )
                + f"?per_page={_LISTING_PAGE_SIZE}"
            )
            entries: list[object] = []
            self._collect_paginated_entries(github_installation_id, path, entries)
            return parse_related_issue_payloads(entries)

    def get_issue_sub_issues(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> list[GitHubRelatedIssueObservation]:
        """Return the authoritative sub-issue listing of one issue.

        The documented
        ``GET /repos/{owner}/{repo}/issues/{issue_number}/sub_issues``
        operation under the installation access token (Issues read), with
        the same fail-closed discipline as the blocked-by listing.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(issue_number, "issue_number")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        with application_span(_TRACER_SCOPE, _SUB_ISSUES_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_SUB_ISSUES_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_issue_number=issue_number,
            )
            path = (
                GITHUB_ISSUE_SUB_ISSUES_PATH.format(
                    owner=owner_login, repo=repository_name, issue_number=issue_number
                )
                + f"?per_page={_LISTING_PAGE_SIZE}"
            )
            entries: list[object] = []
            self._collect_paginated_entries(github_installation_id, path, entries)
            return parse_related_issue_payloads(entries)

    def get_issue_parent(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueParentObservation:
        """Return the authoritative parent-or-no-parent fact of one issue.

        The documented GitHub GraphQL query reading the nullable
        ``Issue.parent`` field (with the documented ``databaseId`` fields of
        the parent issue and its repository) under the installation access
        token. A null parent inside a well-formed answer is the
        authoritative no-parent fact — the REST parent endpoint documents
        only 200/301/404/410 and can never authoritatively express absence,
        so a REST 404 is never reinterpreted as ``NO_PARENT``. A GraphQL
        error answer, or any uninterpretable shape, raises the classified
        uncertain outcome and leaves any durable mirror untouched.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(issue_number, "issue_number")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        with application_span(_TRACER_SCOPE, _PARENT_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_PARENT_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_issue_number=issue_number,
            )
            query = _issue_parent_query(
                owner_login=owner_login,
                repository_name=repository_name,
                issue_number=issue_number,
            )
            body = json.dumps({"query": query}).encode("utf-8")
            response = self._post_graphql_query_with_installation_token(
                github_installation_id, body
            )
            return parse_graphql_issue_parent(_json_body(response))

    def resolve_related_issue_endpoints(
        self,
        *,
        github_installation_id: int,
        local_github_repository_id: int,
        fresh_owner_login: str,
        fresh_repository_name: str,
        related: list[GitHubRelatedIssueObservation],
    ) -> list[RelatedIssueEndpoint]:
        """Resolve related-issue references into stable endpoint identities.

        Each related-issue observation carries GitHub's documented opaque
        repository API reference. This operation owns every URL semantic
        (issue #122): the reference is validated against the exact trusted
        HTTPS GitHub API origin and followed verbatim through the transport —
        never decomposed into owner/name parts — and the stable identity
        comes from the returned repository object's documented numeric
        ``id``. The addressed repository's own documented API address is
        established here from its freshly observed owner/name fields, so
        same-repository references reuse the local stable identity with zero
        extra reads, and each distinct reference resolves at most once per
        call. A reference that cannot establish a stable identity classifies
        as an uncertain outcome. Stable numeric IDs remain the only
        authority; mutable owner/name data never crosses this boundary as
        identity, and the resolved references never enter telemetry.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        require_positive_int(local_github_repository_id, "local_github_repository_id")
        _require_non_empty_command_str(fresh_owner_login, "fresh_owner_login")
        _require_non_empty_command_str(fresh_repository_name, "fresh_repository_name")
        with application_span(_TRACER_SCOPE, _RELATED_ENDPOINTS_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_RELATED_ENDPOINTS_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            local_address = github_repository_api_address(
                owner_login=fresh_owner_login, repository_name=fresh_repository_name
            ).lower()
            resolved: list[RelatedIssueEndpoint] = []
            resolved_references: dict[str, int] = {}
            for observation in related:
                reference = observation.repository_url
                if not is_github_api_origin(reference):
                    raise GitHubOutcomeUncertainError(
                        "the related issue's repository reference is not resolvable "
                        "to a stable GitHub repository identity"
                    )
                if reference.lower() == local_address:
                    github_repository_id = local_github_repository_id
                else:
                    cached = resolved_references.get(reference.lower())
                    if cached is None:
                        payload = _json_body(
                            self._request_with_installation_token(github_installation_id, reference)
                        )
                        reported_id = payload.get("id")
                        if (
                            isinstance(reported_id, bool)
                            or not isinstance(reported_id, int)
                            or reported_id <= 0
                        ):
                            raise GitHubOutcomeUncertainError(
                                "the GitHub response is not interpretable: "
                                "the repository identity is missing"
                            )
                        github_repository_id = reported_id
                        resolved_references[reference.lower()] = github_repository_id
                    else:
                        github_repository_id = cached
                resolved.append(
                    RelatedIssueEndpoint(
                        github_repository_id=github_repository_id,
                        github_issue_id=observation.github_issue_id,
                    )
                )
            return resolved

    def _require_validated_capabilities(
        self, github_installation_id: int
    ) -> GitHubInstallationCapabilities:
        """Return installation capability facts after full v1 validation.

        The JWT-authenticated documented installation lookup; a suspended
        installation, or one lacking the required v1 capabilities or
        subscribed events, raises the classified authorization rejection.
        """
        capabilities = self.get_installation_capabilities(github_installation_id)
        if capabilities.suspended_at is not None:
            raise GitHubAuthorizationRejectedError(
                "the routed GitHub installation is currently suspended"
            )
        if missing_required_capabilities(
            capabilities.capabilities
        ) or missing_required_webhook_events(capabilities.subscribed_events):
            raise GitHubAuthorizationRejectedError(
                "the routed GitHub installation does not currently grant the "
                "GitHub App permissions the required OpenOrc repository "
                "operations need"
            )
        return capabilities

    def _require_installation_repository_entry(
        self, github_installation_id: int, github_repository_id: int
    ) -> dict[str, object]:
        """Return the documented listing entry proving stable-ID repository access.

        The documented ``GET /installation/repositories`` operation under the
        installation access token, paginated through GitHub's documented
        ``Link`` headers, matching each entry's numeric stable ``id``. The
        listing is used solely for membership and repository observation: its
        per-entry ``permissions`` member is the ordinary repository access
        shape and is never consumed as capability authority. Absence of the
        stable identity after the complete listing — or exceeding the bounded
        page count, which leaves the question unanswered — classifies
        accordingly.
        """
        with application_span(_TRACER_SCOPE, _LISTING_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_LISTING_SPAN_NAME,
                github_installation_id=str(github_installation_id),
                github_repository=str(github_repository_id),
            )
            path = f"{GITHUB_INSTALLATION_REPOSITORIES_PATH}?per_page={_LISTING_PAGE_SIZE}"
            page_count = 0
            while page_count < _MAX_LISTING_PAGES:
                response = self._request_with_installation_token(github_installation_id, path)
                payload = _json_body(response)
                if github_repository_id in parse_installation_repositories_page(payload):
                    return _matching_repository_entry(payload, github_repository_id)
                next_url = self._next_page_url(response)
                if next_url is None:
                    raise GitHubAuthorizationRejectedError(
                        "the routed GitHub installation does not currently "
                        "grant access to the addressed repository"
                    )
                path = next_url
                page_count += 1
            raise GitHubOutcomeUncertainError(
                "the installation repository listing could not be completed "
                "within the bounded page count: the access outcome is unknown"
            )

    def _request_with_installation_token(
        self, github_installation_id: int, path: str
    ) -> GitHubHttpResponse:
        """Perform one installation-token request with bounded 401 recovery.

        On an authentication rejection the cached token is evicted and a
        fresh token is minted once. A second rejection is a known failure;
        uncertain outcomes are never replayed.
        """
        token = self._authenticator.installation_token(github_installation_id)
        try:
            return self._transport.request(
                path, method="GET", authorization=f"Bearer {token.token_value()}"
            )
        except GitHubAuthenticationRejectedError:
            self._authenticator.invalidate_installation_token(github_installation_id)
            token = self._authenticator.installation_token(github_installation_id)
            return self._transport.request(
                path, method="GET", authorization=f"Bearer {token.token_value()}"
            )

    def _post_graphql_query_with_installation_token(
        self, github_installation_id: int, body: bytes
    ) -> GitHubHttpResponse:
        """Perform the installation-authenticated read-only GraphQL query POST.

        The one installation-token POST surface this adapter owns, and it is
        a READ: the documented GraphQL endpoint answering the issue-parent
        observation query (issue #60). Credential mode follows the
        operation's semantics, never the HTTP verb (issue #143): this POST
        reads, so installation authentication remains authoritative for it;
        no mutating operation may use this helper. The same bounded recovery
        contract as the GET helper: one token eviction and re-mint on an
        authentication rejection; uncertain outcomes are never replayed.
        """
        token = self._authenticator.installation_token(github_installation_id)
        try:
            return self._transport.request(
                GITHUB_GRAPHQL_PATH,
                method="POST",
                authorization=f"Bearer {token.token_value()}",
                body=body,
            )
        except GitHubAuthenticationRejectedError:
            self._authenticator.invalidate_installation_token(github_installation_id)
            token = self._authenticator.installation_token(github_installation_id)
            return self._transport.request(
                GITHUB_GRAPHQL_PATH,
                method="POST",
                authorization=f"Bearer {token.token_value()}",
                body=body,
            )

    def _request_with_user_access_token(
        self, credential: GitHubProfileUserAccessToken, path: str
    ) -> GitHubHttpResponse:
        """Perform one user-token GET under the exact Profile-bound credential.

        The read surface of the Owner-accountable credential path (issue
        #143): the App-scoped user-to-server listing walks that prove the
        user × installation × repository intersection. The adapter never
        resolves, stores, or refreshes a user token — the application-service
        resolver owns that lifecycle, and the service owns the bounded 401
        recovery: a definitive authentication rejection propagates unchanged.
        Uncertain outcomes are never replayed.
        """
        return self._transport.request(
            path, method="GET", authorization=f"Bearer {credential.access_token.token_value()}"
        )

    def _post_with_user_access_token(
        self, credential: GitHubProfileUserAccessToken, path: str, body: bytes
    ) -> GitHubHttpResponse:
        """Perform one Owner-accountable user-token POST (canonical PR creation).

        The raw token value is read only here, at the final
        request-construction boundary. A definitive 401-style rejection
        propagates to the service, which owns the single bounded recovery
        pass allowed by the #142 token lifecycle; uncertain outcomes are
        never replayed.
        """
        return self._transport.request(
            path,
            method="POST",
            authorization=f"Bearer {credential.access_token.token_value()}",
            body=body,
        )

    def _put_with_user_access_token(
        self, credential: GitHubProfileUserAccessToken, path: str, body: bytes
    ) -> GitHubHttpResponse:
        """Perform one Owner-accountable user-token PUT (exact-head merge request).

        The same contract as :meth:`_post_with_user_access_token`: the raw
        value is read only at this final request-construction boundary, a
        definitive 401-style rejection propagates for the service's bounded
        recovery, and uncertain outcomes are never replayed.
        """
        return self._transport.request(
            path,
            method="PUT",
            authorization=f"Bearer {credential.access_token.token_value()}",
            body=body,
        )

    def _collect_paginated_entries(
        self, github_installation_id: int, initial_path: str, entries: list[object]
    ) -> None:
        """Walk one bounded paginated JSON-array listing into ``entries``.

        Follows the documented ``Link`` headers with the established
        origin-validated pagination discipline; exceeding the bounded page
        count classifies as an uncertain outcome because the authoritative
        listing cannot be completed.
        """
        path: str | None = initial_path
        page_count = 0
        while path is not None:
            if page_count >= _MAX_LISTING_PAGES:
                raise GitHubOutcomeUncertainError(
                    "the GitHub relationship listing could not be completed "
                    "within the bounded page count: the outcome is unknown"
                )
            response = self._request_with_installation_token(github_installation_id, path)
            entries.extend(_json_array_body(response))
            path = self._next_page_url(response)
            page_count += 1

    def _next_page_url(self, response: GitHubHttpResponse) -> str | None:
        """Extract the documented ``Link``-header ``rel="next"`` target.

        A next target is validated against the exact HTTPS GitHub API origin
        through the centralized parsed-origin rule (never a string-prefix
        check, which hostname-prefix or userinfo URLs would bypass); a target
        outside that origin classifies as an uninterpretable outcome instead
        of silently moving the Authorization credential boundary to another
        host.
        """
        link = _header_value(response.headers, "Link")
        if link is None:
            return None
        next_url = _parse_link_next_target(link)
        if next_url is None:
            return None
        if not is_github_api_origin(next_url):
            raise GitHubOutcomeUncertainError(
                "the installation repository listing is not interpretable: "
                "the pagination target leaves the exact GitHub API origin"
            )
        return next_url


def _json_body(response: GitHubHttpResponse) -> dict[str, object]:
    """Decode a 2xx GitHub response body as a JSON object.

    An uninterpretable 2xx body classifies as an uncertain outcome: the
    request reached GitHub but the answer cannot be trusted as a fact.
    """
    try:
        payload = json.loads(response.body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise GitHubOutcomeUncertainError("the GitHub response is not interpretable") from exc
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    return payload


def _json_array_body(response: GitHubHttpResponse) -> list[object]:
    """Decode a 2xx GitHub response body as a JSON array.

    The documented blocked-by and sub-issue listings answer arrays; an
    uninterpretable 2xx body classifies as an uncertain outcome.
    """
    try:
        payload = json.loads(response.body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise GitHubOutcomeUncertainError("the GitHub response is not interpretable") from exc
    if not isinstance(payload, list):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the listing shape is unexpected"
        )
    return payload


def _require_non_empty_command_str(value: object, name: str) -> None:
    """Reject a malformed caller-supplied address string (fail closed)."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _matching_repository_entry(
    payload: dict[str, object], github_repository_id: int
) -> dict[str, object]:
    """Return the raw listing entry for the validated stable repository identity.

    ``parse_installation_repositories_page`` has already validated the page
    shape and matched the identity, so the entry must exist and be
    well-formed; anything else is an inconsistency that cannot be trusted
    as a fact.
    """
    repositories = payload.get("repositories")
    if isinstance(repositories, list):
        for entry in repositories:
            if (
                isinstance(entry, dict)
                and not isinstance(entry.get("id"), bool)
                and isinstance(entry.get("id"), int)
                and entry["id"] == github_repository_id
            ):
                return entry
    raise GitHubOutcomeUncertainError(
        "the installation repository listing is not interpretable: "
        "the matched repository entry is malformed"
    )


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive response-header lookup."""
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


def _parse_link_next_target(link_value: str) -> str | None:
    """Parse the ``rel="next"`` target from a documented GitHub Link header.

    GitHub pagination uses RFC 5988-style ``Link`` headers:
    ``<url>; rel="next", <url>; rel="last"``. A header without a next
    relation ends the pagination. Malformed entries are ignored rather than
    trusted.
    """
    for entry in link_value.split(","):
        parts = entry.split(";")
        url_part: str | None = None
        relation: str | None = None
        for part in parts:
            stripped = part.strip()
            if stripped.startswith("<") and stripped.endswith(">"):
                url_part = stripped[1:-1]
            elif stripped.lower().startswith("rel="):
                relation = stripped[4:].strip().strip('"').strip("'").lower()
        if relation == "next" and url_part is not None:
            return url_part
    return None
