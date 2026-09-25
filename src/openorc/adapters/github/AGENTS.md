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
- Known failures (definitive rejections, including rate limits — classified apart from authorization absence) and uncertain outcomes (timeout, connection loss, unfollowed redirect, uninterpretable response) remain observably distinct adapter-local errors; services translate them into the typed application vocabulary.

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

Do not let GitHub comments, labels, or webhook payloads become implicit Producer authority.
