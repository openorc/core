# GitHub Adapter Agent Context

## Boundary

This adapter owns GitHub API/webhook transport, normalization, stable GitHub identifiers, and authoritative-state reconciliation mechanics. Application services decide workflow consequences.

## Authentication boundary (issue #58)

- Durable repository automation authenticates only as the deployed OpenOrc GitHub App: App JWTs for app-level reads and short-lived installation access tokens for the exact installation the #57 route resolved. Human GitHub sign-in is identity only; no PAT or human OAuth token fallback exists anywhere in this boundary, and the public surface accepts no credential parameter.
- The GitHub App ID and private key are deployment/bootstrap secret material, optional on the shared Settings surface and required at the point the client/authenticator is constructed (fail fast; never Workspace data, never `openorc.*` tables, never Vault, never logged or returned).
- Installation access tokens live in memory only: a cache keyed by the stable external installation ID honors GitHub expiry semantics with a safety margin, is pruned opportunistically of expired entries, and is LRU-bounded at a documented entry count, so expired secrets are never retained indefinitely and cardinality cannot grow without limit. Eviction on an authentication rejection is a single bounded re-mint, and uncertain outcomes are never replayed. Tokens and key material never enter domain objects, persistence, events, logs, error messages, or telemetry attributes; `repr`/`str` of the secret-bearing types are redacted.

## Documented-operation discipline (issue #58)

- The supported GitHub REST API version is pinned in exactly one location (`SUPPORTED_GITHUB_API_VERSION` in `transport.py`) and sent on every request through `X-GitHub-Api-Version`.
- Validation uses documented operations only. The installation object (`GET /app/installations/{installation_id}`, App-JWT authenticated) is the sole source of the fine-grained permission dictionary, subscribed events, and suspension state used by capability validation. The installation-repositories listing (`GET /installation/repositories`, installation token, `Link`-header paginated with a bounded page count) proves repository membership by matching the numeric stable repository `id` — its per-entry `permissions` member is the ordinary repository access shape and is never capability authority.
- Reconciliation reads (issue #59) use documented operations only: the repository observation comes from the same stable-ID installation-repositories listing entry (there is no documented repo-by-ID read, and a stored owner/name address breaks exactly when a rename/transfer must be reconciled), and the issue observation from `GET /repos/{owner}/{repo}/issues/{issue_number}`, addressed by the freshly observed owner/name and bound to the addressed subject by the response's `number`/`repository_url`. The documented `pull_request` member is the response's own discriminator and is carried as a typed fact; services decide its meaning.
- Capability semantics (including that exact-head merge authority derives from `contents: write` plus the merge endpoint's `sha` exact-head parameter, never from the pull-request permission) live inside the adapter/config boundary; workflow code asks the semantic question and never inspects provider-native permission dictionaries.
- Known failures (definitive rejections, including rate limits — classified apart from authorization absence) and uncertain outcomes (timeout, connection loss, a redirect the safe redirect policy refuses, uninterpretable response) remain observably distinct adapter-local errors; services translate them into the typed application vocabulary.

## Canonical branch/PR/checks/merge operations (issue #63)

- The documented operations are: the branch read (`GET /repos/{owner}/{repo}/branches/{branch}` — the branch name is percent-encoded as one path segment and the response binds to the addressed `name` member, carrying the exact committed head SHA), the PR read (`GET /repos/{owner}/{repo}/pulls/{pull_number}` — bound to the addressed subject by the response's own `number`/`url` members; stable PR `id` is the reconciliation identity), the check-run listing (`GET /repos/{owner}/{repo}/commits/{ref}/check-runs`), the combined commit status (`GET /repos/{owner}/{repo}/commits/{ref}/status`), and the exact-head merge (`PUT /repos/{owner}/{repo}/pulls/{pull_number}/merge` with the documented `sha` expected-head parameter).
- Both CI/status surfaces are addressed by the exact head SHA — never a branch name — and are **exhaustively paginated**: the combined-status listing is itself paginated (`per_page`/`page`, `Link` headers), so every page is walked within the bounded page count and completeness is proven against the documented `total_count` (combined-status pages additionally bind their `sha` member to the addressed head and must agree on the aggregate). Any bound overrun, broken pagination chain, count mismatch, or uninterpretable page is an uncertain outcome — a silently partial projection is never returned.
- The checks/status projection is a typed read model of GitHub-owned facts. It never becomes an OpenOrc merge-policy engine and never synthesizes a universal pass/fail gate that would disagree with GitHub branch protection.
- The merge request normalizes GitHub's documented response classes: the successful merge (with the merge commit SHA GitHub reports), the documented 409 expected-head mismatch (`HEAD_MISMATCH` — a concurrent head change can never satisfy a stale merge request), and any other definitive rejection (`REJECTED` — required checks, conflicts, branch protection: GitHub owns the policy). Authentication/access absence, rate limits, and uncertain outcomes raise the classified errors; an uncertain merge is never silently replayed by this adapter.
- The definitive-rejection classification for the merge endpoint uses the bare HTTP status carried on `GitHubRequestRejectedError.status_code` — adapter-internal request mechanics only: application services never branch on raw provider status codes, and the attribute never carries provider content.


## Pull-request create operation (issue #64)

- The documented `POST /repos/{owner}/{repo}/pulls` create operation is branch-addressed and carries no atomic expected-head guard: the exact-head race is closed by the publication service above (exact preflight + immediate post-create reconciliation), never inside this adapter. The operation carries only the validated presentation title/body and the branch/base routing facts; the response is normalized into the stable PR identity/fact set (`GitHubPullRequestFacts`), and an uninterpretable response classifies as uncertain.
- The documented "pull request already exists" definitive rejection (422) is classified adapter-locally as `GitHubPullRequestExistsError` carrying only the bare status — application services never branch on raw provider status codes, and whether that condition means a conflict or a replay is workflow meaning decided strictly above the adapter. Uncertain create outcomes are never replayed by this adapter.
- A bare 422 is the create endpoint's GENERAL documented validation failure (invalid base/head, malformed title, endpoint abuse), never by itself the duplicate condition: the duplicate classification requires the documented response members (the explicit "a pull request already exists" validation message, or the validation-failed shape whose errors array names the `base` field), matched content-safely over a bounded body via `body_reports_pull_request_already_exists`. Unrelated 422 responses stay ordinary definitive `GitHubRequestRejectedError`s; the bounded body attached to definitive rejections is adapter-internal classification input and is never echoed into messages, logs, or telemetry.

## Provider hardening (issue #122)

## Provider hardening (issue #122)

- REST redirects are followed only when the request is read-style
  (`GET`/`HEAD`) and the resolved target stays on the exact trusted HTTPS
  GitHub API origin (centralized parsed-origin validation); the followed
  count and per-target repeats are bounded. Cross-origin, malformed, loop,
  excess, and non-read-method redirect outcomes classify as uncertain and
  never forward credentials off the trusted origin; a redirect answer to a
  consequential (mutating) operation is never auto-replayed.
- Classified rate-limit errors carry normalized safe scheduling facts when
  GitHub supplies them (`Retry-After` as normalized seconds,
  `X-RateLimit-Reset` as a UTC instant). Provider headers never leak above
  the adapter, and the adapter never sleeps, queues, or retries: scheduling
  policy stays in services/deployment, and uncertain operations are
  reconciled, never blindly replayed.
- GitHub-returned API URLs are opaque navigation references: the adapter
  validates them against the exact trusted origin and follows them verbatim;
  stable identity comes from the returned object's documented fields, and
  repository owner/name stays observed address/presentation fact. No OpenOrc
  code above the adapter constructs, parses, or provider-normalizes GitHub
  API URLs.
- Webhook subscriptions: `issue_comment` is deliberately NOT required
  (issue #122). Comments are GitHub-owned presentation/discussion data;
  publishing OpenOrc's own issue comments rides the `issues` write
  permission, not the subscription. The `issues` family stays required.
- Request pacing: future high-concurrency GitHub work must keep provider
  traffic bounded, avoid indiscriminately concurrent REST mutations, respect
  GitHub secondary-rate-limit guidance (including pacing high-volume
  mutative requests), and coordinate pacing at an appropriate
  installation/provider boundary rather than scattering sleeps through
  adapter calls. No scheduler or semaphore framework exists by design.

## Authority and reconciliation

- GitHub remains authoritative for issue requirements/relationships, committed code, PR/head state, CI/checks, branch protection, merge policy, and merge result.
- Webhook deliveries are notifications, not final truth. Validate signatures, deduplicate by delivery identity, then reconcile relevant state through GitHub APIs.
- Reconciliation must tolerate replay, missed delivery, and out-of-order delivery.
- Stable object IDs and exact commit SHAs matter more than mutable names/URLs.

## PR / review safety

- Canonical PR publication is an OpenOrc operation after explicit authority; the Producer supplies presentation content only.
- PR creation must use preflight branch-head validation and immediate post-create head reconciliation because GitHub creation lacks an atomic expected-head guard.
- Reviewer acceptance is exact-head addressed.
- Merge requests carry the exact reviewed/Owner-overridden head as expected SHA.
- CI/check projection presents GitHub-owned facts and must not become a second merge-policy engine.

## Webhook ingress boundary (issues #61 and #120)

- Inbound GitHub webhook deliveries verify GitHub's SHA-256 signature
  (`X-Hub-Signature-256`, a `sha256=`-prefixed hex HMAC-SHA256 digest) over
  the exact raw request bytes through
  `webhook_verification.GitHubWebhookSignatureVerifier` — stdlib `hmac` with
  constant-time `hmac.compare_digest` — before any payload is trusted or
  semantically parsed and before any payload-derived effect. Missing,
  malformed, and invalid signatures are distinct adapter-local typed
  rejections the intake service translates to `AuthenticationError`; the
  transport maps them to a uniform, detail-free rejection. The raw body is
  verified exactly once and is never persisted.
- The webhook secret is deployment/bootstrap secret material on the shared
  Settings surface (`OPENORC_GITHUB_WEBHOOK_SECRET`, representation-safe,
  supplied-blank fails closed, #58 discipline). It is required at the point
  the verification boundary is constructed (unconfigured deployments fail
  closed rather than accept unverified deliveries) and is never Workspace
  data, `openorc.*` state, Vault content, log/error text, or telemetry.
- Payload semantics stay inside `webhook_classification`: a verified payload
  is parsed only far enough to classify it — a relevant classified v1 event
  family (`V1_CLASSIFIED_WEBHOOK_EVENTS`, a strict superset of the App's
  subscription contract `REQUIRED_V1_WEBHOOK_EVENTS`, adding the
  default-delivered, non-subscribable `installation` and
  `installation_repositories` families, #120) with sufficient stable routing
  identity maps onto the normalized `GitHubWebhookRoutingTarget` vocabulary.
  Two identity shapes exist: repository-scoped deliveries carry the stable
  installation ID, repository ID, and (where the family addresses a provider
  object) the issue or PR number; installation-scoped deliveries (the two
  installation families, whose notifications affect repository sets or the
  whole installation rather than one singular repository) carry only the
  stable installation ID, and dispatch fans out over the installation's
  explicitly routed repositories. `sub_issues`/`issue_dependencies`
  deliberately extract no issue number (their related issues may live in
  other repositories); valid-but-irrelevant deliveries (including
  `issue_comment` — no v1 capability reads comments) are safely ignored;
  structurally unusable payloads are safely classified without inventing
  authority. Only stable identity members (installation/repository IDs,
  issue/PR numbers) are ever extracted; titles, bodies, and content members
  have no read path, and provider event names/actions never leak above this
  boundary into services/domain code.

Do not let GitHub comments, labels, or webhook payloads become implicit Producer authority.
