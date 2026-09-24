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
    GITHUB_INSTALLATION_PATH,
    GITHUB_INSTALLATION_REPOSITORIES_PATH,
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
from openorc.adapters.github.transport import (
    DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    GITHUB_API_BASE_URL,
    GitHubFetcher,
    GitHubHttpResponse,
    HttpGitHubRestClient,
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

# The installation repository listing requests the maximum documented page
# size (100) and is bounded so a broken pagination chain cannot loop.
_LISTING_PAGE_SIZE = 100
_MAX_LISTING_PAGES = 100


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

    def get_installation_capabilities(
        self, github_installation_id: int
    ) -> GitHubInstallationCapabilities:
        """Return the normalized capability facts for the exact installation."""
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
        self._require_repository_membership(github_installation_id, github_repository_id)
        return GitHubAccessValidation(
            github_installation_id=github_installation_id,
            github_repository_id=github_repository_id,
            capabilities=capabilities.capabilities,
            subscribed_events=capabilities.subscribed_events,
        )

    def _require_repository_membership(
        self, github_installation_id: int, github_repository_id: int
    ) -> None:
        """Prove the installation currently grants access to the stable repository.

        The documented ``GET /installation/repositories`` operation under the
        installation access token, paginated through GitHub's documented
        ``Link`` headers, matching each entry's numeric stable ``id``. The
        listing is used solely for membership: its per-entry ``permissions``
        member is the ordinary repository access shape and is never consumed
        as capability authority. Absence of the stable identity after the
        complete listing — or exceeding the bounded page count, which leaves
        the question unanswered — classifies accordingly.
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
                if github_repository_id in parse_installation_repositories_page(
                    _json_body(response)
                ):
                    return
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

    def _next_page_url(self, response: GitHubHttpResponse) -> str | None:
        """Extract the documented ``Link``-header ``rel="next"`` target.

        A next target outside the GitHub API base classifies as an
        uninterpretable outcome instead of silently moving the credential
        boundary to another host.
        """
        link = _header_value(response.headers, "Link")
        if link is None:
            return None
        next_url = _parse_link_next_target(link)
        if next_url is None:
            return None
        if not next_url.startswith(GITHUB_API_BASE_URL):
            raise GitHubOutcomeUncertainError(
                "the installation repository listing is not interpretable: "
                "the pagination target leaves the GitHub API boundary"
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
