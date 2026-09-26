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

Credential discipline: installation tokens are minted by the authenticator
for the exact installation, presented as ``Authorization: Bearer`` headers
on the immediate transport call, and evicted with a single bounded re-mint
when GitHub rejects the presented token with 401. Uncertain outcomes are
never replayed. No credential material ever crosses this facade into
service code: the public surface returns typed facts and raises typed
adapter errors only.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from typing import Protocol

from openorc.adapters.github.authentication import GitHubAppAuthenticator
from openorc.adapters.github.capabilities import (
    GITHUB_GRAPHQL_PATH,
    GITHUB_INSTALLATION_PATH,
    GITHUB_INSTALLATION_REPOSITORIES_PATH,
    GITHUB_ISSUE_DEPENDENCIES_BLOCKED_BY_PATH,
    GITHUB_ISSUE_SUB_ISSUES_PATH,
    GITHUB_REPOSITORY_ISSUE_PATH,
    GITHUB_REPOSITORY_PATH,
    GitHubAccessValidation,
    GitHubInstallationCapabilities,
    missing_required_capabilities,
    missing_required_webhook_events,
    parse_installation_payload,
    parse_installation_repositories_page,
    require_positive_int,
)
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
)
from openorc.adapters.github.observations import (
    GitHubIssueObservation,
    GitHubRepositoryObservation,
    parse_installation_repository_entry,
    parse_issue_payload,
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
    is_github_api_origin,
)
from openorc.config import (
    GITHUB_APP_ID_VAR,
    GITHUB_APP_PRIVATE_KEY_VAR,
    ConfigurationError,
    Settings,
)
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
_REPOSITORY_RESOLVE_SPAN_NAME = "github.get_repository_by_address"

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
    surface carries no credential parameter of any kind: there is no PAT,
    human OAuth token, or other human-credential fallback anywhere in this
    boundary — authentication always derives from the deployment-held GitHub
    App identity and the exact routed installation.
    """

    def validate_installation_repository_access(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubAccessValidation:
        """Validate current repository access and the required v1 capabilities."""
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

    def get_repository_by_address(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
    ) -> int:
        """Return the stable numeric repository ID at one owner/name address."""
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
            response = self._post_with_installation_token(
                github_installation_id, GITHUB_GRAPHQL_PATH, body
            )
            return parse_graphql_issue_parent(_json_body(response))

    def get_repository_by_address(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
    ) -> int:
        """Return the stable numeric repository ID at one owner/name address.

        The documented ``GET /repos/{owner}/{repo}`` operation under the
        installation access token: the REST-only resolution step for
        cross-repository related-repository identity. The mutable
        owner/name address is used transiently within one fresh observation
        to establish the stable identity — it is never an identity itself
        and never persisted as one.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        _require_non_empty_command_str(owner_login, "owner_login")
        _require_non_empty_command_str(repository_name, "repository_name")
        with application_span(_TRACER_SCOPE, _REPOSITORY_RESOLVE_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_REPOSITORY_RESOLVE_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            path = GITHUB_REPOSITORY_PATH.format(owner=owner_login, repo=repository_name)
            payload = _json_body(
                self._request_with_installation_token(github_installation_id, path)
            )
            return require_positive_int(payload.get("id"), "repository id")

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

    def _post_with_installation_token(
        self, github_installation_id: int, path: str, body: bytes
    ) -> GitHubHttpResponse:
        """Perform one installation-token POST with bounded 401 recovery.

        The same bounded recovery contract as the GET helper: one token
        eviction and re-mint on an authentication rejection; uncertain
        outcomes are never replayed.
        """
        token = self._authenticator.installation_token(github_installation_id)
        try:
            return self._transport.request(
                path,
                method="POST",
                authorization=f"Bearer {token.token_value()}",
                body=body,
            )
        except GitHubAuthenticationRejectedError:
            self._authenticator.invalidate_installation_token(github_installation_id)
            token = self._authenticator.installation_token(github_installation_id)
            return self._transport.request(
                path,
                method="POST",
                authorization=f"Bearer {token.token_value()}",
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
