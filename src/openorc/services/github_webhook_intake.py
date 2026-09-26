"""Authenticated GitHub webhook intake services (issue #61).

The secure, durable notification-intake boundary behind the thin FastAPI
webhook route. A signed webhook is never GitHub truth and never workflow
authority: this boundary verifies the exact raw request, records the provider
delivery identity exactly once, resolves the payload's stable
installation/repository identity through the explicit Workspace
GitHubInstallation/Repository routing (#57/#61 routing resolution), and
acknowledges duplicates idempotently. It does NOT invoke authoritative
reconciliation — dispatch (#120) begins from the durably retained routing
metadata and re-validates current durable state before any effect.

Order of operations (each step precedes any payload-derived application
effect):

1. verification — the exact raw bytes are verified once against the
   deployment's configured webhook secret (missing/malformed/invalid
   signatures are typed rejections translated to
   :class:`AuthenticationError`; the secret itself is unconfigured for this
   process -> :class:`IntegrationNotConfiguredError`);
2. classification — only the verified payload is parsed, and only far enough
   to classify it and extract safe stable routing identity (adapter-local;
   provider event names/actions never leak above the GitHub boundary);
3. durable recording — one short, database-only transaction records the
   GUID-keyed delivery, resolves relevant deliveries through the
   webhook-direction B1 routing seam, and persists the resolved Workspace
   routing linkages; a duplicate GUID is acknowledged idempotently with no
   second accepted record (zero/mismatched routes are bounded,
   non-authoritative observations that never pierce Workspace isolation and
   never guess from mutable names).

Every public operation returns a typed result; expected outcomes (invalid
signature, duplicate, ignored, unusable, unmapped) are typed handling rather
than noisy exception logging. The raw payload, signature material, and
customer content never enter domain objects, persistence, events, logs, or
telemetry attributes.
"""

from __future__ import annotations

from dataclasses import dataclass

from openorc.adapters.github.webhook_classification import (
    GitHubWebhookDeliveryFacts,
    classify_github_webhook_delivery,
)
from openorc.adapters.github.webhook_verification import (
    GitHubWebhookRejectedError,
    GitHubWebhookSecret,
    GitHubWebhookSignatureVerifier,
)
from openorc.config import Settings
from openorc.domain.github_webhooks import (
    GitHubWebhookDelivery,
    GitHubWebhookDeliveryClassification,
    GitHubWebhookDeliveryIntake,
    GitHubWebhookRouteResolution,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_webhook_deliveries import (
    record_github_webhook_delivery,
    record_webhook_delivery_routes,
)
from openorc.persistence.github_webhook_routing import resolve_github_webhook_routes
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    AuthenticationError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = ["GitHubWebhookIntake", "intake_github_webhook"]

_SERVICE_TRACER_SCOPE = "openorc.services.github_webhook_intake"
_INTAKE_SPAN_NAME = "github_webhook_intake.intake_github_webhook"


@dataclass(frozen=True, slots=True)
class GitHubWebhookIntake:
    """The typed outcome of one webhook intake invocation.

    ``delivery`` is present only when this invocation durably recorded the
    delivery (the first sighting of its GUID); ``duplicate`` is true when the
    GUID was already durably accepted (idempotent acknowledgement, no second
    record); ``ignored``/``unusable`` mark the bounded non-relevant
    classifications of the accepted delivery.
    """

    delivery: GitHubWebhookDelivery | None
    duplicate: bool = False
    ignored: bool = False
    unusable: bool = False


def _webhook_secret(settings: Settings) -> GitHubWebhookSecret:
    """Build the verification secret, failing closed when unconfigured.

    The requirement is enforced at the point the verification boundary is
    constructed (the #58 pattern): unrelated processes never need the
    secret, and a misconfigured deployment surfaces here, at the boundary,
    rather than silently accepting unauthenticated deliveries.
    """
    value = settings.github_webhook_secret
    if value is None:
        raise IntegrationNotConfiguredError(
            "the GitHub webhook secret is not configured; webhook intake cannot verify deliveries"
        )
    return GitHubWebhookSecret(value)


def _translate_signature_rejection(error: GitHubWebhookRejectedError) -> AuthenticationError:
    # Messages are deliberately safe by authoring (no signature material, no
    # secret); the transport maps the typed error to a uniform rejection.
    return AuthenticationError(str(error))


def _verified_facts(
    verifier: GitHubWebhookSignatureVerifier,
    *,
    raw_body: bytes,
    signature_header: str | None,
    event_name: str,
) -> GitHubWebhookDeliveryFacts:
    """Verify the exact raw bytes once, then classify the verified payload."""
    try:
        verifier.verify(raw_body, signature_header)
    except GitHubWebhookRejectedError as error:
        raise _translate_signature_rejection(error) from error
    return classify_github_webhook_delivery(event_name=event_name, payload_bytes=raw_body)


def _intake_facts(
    facts: GitHubWebhookDeliveryFacts,
    delivery_guid: str,
    resolution: GitHubWebhookRouteResolution | None,
) -> GitHubWebhookDeliveryIntake:
    """Compose the durable intake candidate from the classified facts."""
    relevant = facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    return GitHubWebhookDeliveryIntake(
        delivery_guid=delivery_guid,
        event_name=facts.event_name,
        action=facts.action,
        classification=facts.classification,
        routing_target=facts.routing_target,
        routing_resolution=(resolution.resolution if resolution is not None and relevant else None),
        github_installation_id=facts.github_installation_id,
        github_repository_id=facts.github_repository_id,
        github_issue_number=facts.github_issue_number,
        github_pull_request_number=facts.github_pull_request_number,
    )


def _record_delivery(
    pool: DatabasePool,
    facts: GitHubWebhookDeliveryFacts,
    delivery_guid: str,
) -> GitHubWebhookIntake:
    """Resolve, record, and link the delivery inside ONE short transaction.

    All work here is in-process database work on already-verified bytes, so
    the transaction never crosses an external call. Routing resolution runs
    before the insert (a read; a concurrent duplicate that wins the unique
    constraint simply discards it); a duplicate GUID persists nothing
    further and is acknowledged idempotently.
    """
    relevant = facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    with composed_transaction(pool) as tx_pool:
        resolution: GitHubWebhookRouteResolution | None = None
        if relevant and facts.github_installation_id and facts.github_repository_id:
            resolution = resolve_github_webhook_routes(
                tx_pool,
                github_installation_id=facts.github_installation_id,
                github_repository_id=facts.github_repository_id,
            )
        delivery = record_github_webhook_delivery(
            tx_pool, _intake_facts(facts, delivery_guid, resolution)
        )
        if delivery is None:
            # A concurrent sighting of the same GUID committed first: the
            # durable unique constraint won. Nothing else is written.
            return GitHubWebhookIntake(delivery=None, duplicate=True)
        if resolution is not None and resolution.routes:
            record_webhook_delivery_routes(
                tx_pool, delivery_id=delivery.id, routes=resolution.routes
            )
        return GitHubWebhookIntake(
            delivery=delivery,
            ignored=facts.classification is GitHubWebhookDeliveryClassification.IGNORED,
            unusable=facts.classification is GitHubWebhookDeliveryClassification.UNUSABLE,
        )


def intake_github_webhook(
    pool: DatabasePool,
    settings: Settings,
    *,
    raw_body: bytes,
    signature_header: str | None,
    event_name: str,
    delivery_guid: str,
) -> GitHubWebhookIntake:
    """Intake one authenticated GitHub webhook delivery (issue #61).

    Verifies the exact raw request bytes against the configured webhook
    secret, classifies the verified payload, resolves relevant deliveries
    through the explicit Workspace routing, and durably records the delivery
    exactly once per provider delivery GUID. Raises
    :class:`IntegrationNotConfiguredError` when the webhook secret is
    unconfigured, :class:`AuthenticationError` on a missing, malformed, or
    invalid signature, and :class:`InvalidCommandError` when the delivery
    identity header is missing or malformed.
    """
    if not isinstance(delivery_guid, str) or not delivery_guid.strip():
        raise InvalidCommandError("the GitHub delivery identity header is missing or malformed")
    with application_span(_SERVICE_TRACER_SCOPE, _INTAKE_SPAN_NAME) as span:
        # Only the safe vocabulary: the OpenOrc operation name and stable
        # provider identifiers; never the payload, signature, or secret.
        annotate_span(span, operation=_INTAKE_SPAN_NAME)
        verifier = GitHubWebhookSignatureVerifier(_webhook_secret(settings))
        facts = _verified_facts(
            verifier,
            raw_body=raw_body,
            signature_header=signature_header,
            event_name=event_name,
        )
        if facts.classification is GitHubWebhookDeliveryClassification.RELEVANT:
            annotate_span(
                span,
                github_installation_id=str(facts.github_installation_id),
                github_repository=str(facts.github_repository_id),
                github_issue_number=facts.github_issue_number,
                github_pull_request_number=facts.github_pull_request_number,
            )
        return _record_delivery(pool, facts, delivery_guid)
